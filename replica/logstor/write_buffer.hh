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

// Tracks asynchronous write completion on top of chunk_builder.
//
// This is the buffer type used by the segment manager, buffered_writer,
// compaction, and separator flows. write() appends a record to the underlying
// chunk builder and returns a future that resolves to the final record_location once
// the buffer is flushed. The returned gate holder keeps the buffer alive for
// follow-up work such as index updates. For mixed buffers it also keeps copies
// of appended records so separator rewriting can replay them after the flush.
class write_buffer {
private:
    chunk_builder _builder;
    shared_promise<segment_position> _chunk_position;
    seastar::gate _write_gate;

    std::vector<separator_record> _separator_records;

public:

    write_buffer(size_t buffer_size, segment_kind kind);

    void reset();

    write_buffer(const write_buffer&) = delete;
    write_buffer& operator=(const write_buffer&) = delete;

    write_buffer(write_buffer&&) noexcept = default;
    write_buffer& operator=(write_buffer&&) noexcept = default;

    future<> close();
    bool is_closed() const noexcept;

    size_t serialized_size() const noexcept { return _builder.serialized_size(); }

    size_t buffer_size() const noexcept { return _builder.buffer_size(); }

    template <log_record_writer_concept Writer>
    bool can_fit(const Writer& writer) const noexcept { return _builder.can_fit(writer); }
    bool has_data() const noexcept { return _builder.has_data(); }

    size_t record_bytes() const noexcept { return _builder.record_bytes(); }
    size_t record_count() const noexcept { return _builder.record_count(); }

    size_t finish() { return _builder.finish(); }

    bytes_view seal(segment_sequence segment_seq, std::optional<table_id> table) {
        return _builder.seal(segment_seq, table);
    }

    size_t schema_table_size() const noexcept { return _builder.schema_table_size(); }

    // Write a record to the buffer.
    // Returns a future that will be resolved with the record location once flushed and a gate holder
    // that keeps the write buffer open. The gate should be held for index updates after the write
    // is done.
    template <log_record_writer_concept Writer>
    future<written_record> write(Writer writer, write_target target = {});

    // Complete all tracked writes with their locations once the buffer's chunk is written at chunk_position
    future<> complete_writes(segment_position chunk_position);
    future<> abort_writes(std::exception_ptr);

private:
    bool keeps_separator_records() const noexcept {
        return _builder.kind() == segment_kind::mixed;
    }

    std::vector<separator_record> take_separator_records();

    friend class buffered_writer;
    friend class segment_manager_impl;
    friend struct separator_buffer;
};

extern template future<written_record> write_buffer::write<log_record_writer>(log_record_writer, write_target);
extern template future<written_record> write_buffer::write<log_record_bytes_writer>(log_record_bytes_writer, write_target);

}
}
