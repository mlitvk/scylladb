/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

#include "replica/logstor/chunk_builder.hh"
#include "replica/logstor/record_writer.hh"

namespace replica {

namespace logstor {

class logstor_group;

struct write_target {
    logstor_group* cg = nullptr;
    // Keeps the group from being reported as empty while the write is in flight: until the record
    // is in a segment and in the index, the group's own state does not account for it, and a tablet
    // split ACKs a pre-split group once it looks empty.
    seastar::gate::holder non_empty_holder;
    // Keeps the group from finishing its stop() while the write is in flight. It is released only
    // when the write target is dropped, which for a separator-eligible record is after the record
    // was handed to the group's separator buffer, so a group that stopped has nothing left to come.
    seastar::gate::holder alive_holder;
};

// A record that a write buffer wrote, once its chunk is in a segment. `buffer_holder` keeps the
// buffer open, for follow-up work such as the index update.
struct written_record {
    record_location location;
    seastar::gate::holder buffer_holder;
};

// A record of a mixed buffer that the separator rewrites once the buffer is written.
struct separator_record {
    log_record_writer writer;
    frame_extent frame;
    write_target target;

    record_location location(segment_position chunk_position) const noexcept {
        return locate_record(chunk_position, frame);
    }
};

// A chunk builder that tracks the writes of its records: write() appends a record and returns a
// future that resolves to where the record ends up, once the chunk is written to a segment and
// complete_writes() is called. The buffer_holder of the written_record keeps the buffer open, for
// follow-up work such as index updates.
//
// mixed_write_buffer and full_write_buffer take different records and seal different headers, and
// this class holds what they share.
class write_buffer {
    chunk_builder _builder;
    shared_promise<segment_position> _chunk_position;
    seastar::gate _write_gate;

public:
    write_buffer(const write_buffer&) = delete;
    write_buffer& operator=(const write_buffer&) = delete;

    // Ends the appends, see chunk_builder::finish().
    size_t finish() { return _builder.finish(); }

    // Completes the tracked writes once the chunk of the buffer is written at `chunk_position`, and
    // waits for the holders of the written records.
    future<> complete_writes(segment_position chunk_position);

    // Waits for the holders of the written records. A buffer is reset only once it is closed.
    future<> close();
    bool is_closed() const noexcept;

    template <log_record_writer_concept Writer>
    bool can_fit(const Writer& writer) const noexcept { return _builder.can_fit(writer); }
    bool has_data() const noexcept { return _builder.has_data(); }

    size_t serialized_size() const noexcept { return _builder.serialized_size(); }
    size_t buffer_size() const noexcept { return _builder.buffer_size(); }
    size_t record_bytes() const noexcept { return _builder.record_bytes(); }
    size_t record_count() const noexcept { return _builder.record_count(); }

protected:
    write_buffer(size_t buffer_size, segment_kind kind);
    ~write_buffer() = default;

    write_buffer(write_buffer&&) noexcept = default;
    write_buffer& operator=(write_buffer&&) noexcept = default;

    // Returns the future of where the record whose frame is at `frame` ends up. It holds the write
    // gate until the record's write completes, and passes the holder to the caller.
    future<written_record> track(frame_extent frame);

    // Fails the tracked writes with `ex`, for a chunk that is not written, and closes the buffer.
    future<> abort_writes(std::exception_ptr ex);

    void reset();

    chunk_builder& builder() noexcept { return _builder; }
    const chunk_builder& builder() const noexcept { return _builder; }
};

// The write buffer of a mixed segment, which takes the user writes. It keeps the records it took
// until its chunk is written, for the separator to rewrite them into full segments.
class mixed_write_buffer final : public write_buffer {
    std::vector<separator_record> _separator_records;

public:
    explicit mixed_write_buffer(size_t buffer_size);

    future<written_record> write(log_record_writer writer, write_target target);

    bytes_view seal(segment_sequence segment_seq) {
        return builder().seal(segment_seq);
    }

    // The records of the buffer, once its chunk is written, for the separator.
    std::vector<separator_record> take_separator_records();

    future<> abort_writes(std::exception_ptr ex);

    void reset();
};

// The write buffer of a full segment, which compaction and the separator write.
class full_write_buffer final : public write_buffer {
public:
    explicit full_write_buffer(size_t buffer_size)
        : write_buffer(buffer_size, segment_kind::full)
    {}

    template <log_record_writer_concept Writer>
    future<written_record> write(const Writer& writer) {
        return track(builder().append(writer));
    }

    bytes_view seal(segment_sequence segment_seq, table_id table) {
        return builder().seal(segment_seq, table);
    }

    using write_buffer::abort_writes;
    using write_buffer::reset;

    size_t schema_table_size() const noexcept { return builder().schema_table_size(); }
};

}
}
