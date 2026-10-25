/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// A record stream moves the live records of a compaction group to another node, for a tablet
// migration. The sender reads the segments of the group and sends the records that the index
// still points at. The receiver packs the records into full segments of its own, and adds them to
// its index.
//
// The stream is a sequence of chunks of segment_kind::full, one for each segment of the sender
// that has live records. The sender seals a chunk at ondisk::record_alignment, so a chunk has no
// padding to a block. The segment sequence of a chunk has no meaning on the wire, and the receiver
// does not read it. The receiver checks each chunk with its CRCs, as recovery checks a chunk on
// disk.

#pragma once

#include <deque>
#include <optional>
#include <span>
#include <stdexcept>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/noncopyable_function.hh>

#include "replica/logstor/compaction.hh"
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/write_buffer.hh"

namespace replica::logstor {

// A chunk of a stream is at most this long, because it holds the records of one segment of the
// sender, and no segment is larger than this.
inline constexpr size_t max_stream_chunk_size = segment_descriptor_hist_options.max_size;

// The sink throws this error when a record stream is not valid, for example when a chunk does not
// parse, a record is not of the table of the stream, or the stream ends inside a chunk.
class record_stream_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Cuts the bytes of a stream into chunks. The bytes arrive in buffers of any size, so a chunk can
// start in one buffer and end in a later one.
class stream_chunk_assembler {
public:
    void push(temporary_buffer<char>);

    // Returns the next whole chunk, or nothing if not all bytes of the chunk arrived yet. Throws
    // record_stream_error if the chunk header is not valid, is not of a full segment, or gives a
    // chunk longer than max_stream_chunk_size. This function does not check the CRCs, because
    // parse_chunk() checks them.
    std::optional<temporary_buffer<char>> next_chunk();

    // Returns true if bytes arrived that are not in a chunk that next_chunk() returned.
    bool has_partial_chunk() const noexcept {
        return _size > 0;
    }

private:
    std::deque<temporary_buffer<char>> _buffers;
    size_t _size = 0;
    // Holds the size of the chunk at the front after its chunk header arrived.
    std::optional<size_t> _chunk_size;

    size_t read_chunk_size() const;
    void copy_front(char* out, size_t n) const;
    temporary_buffer<char> take_front(size_t n);
};

// The receiver of a record stream. It packs the records into full segments of their compaction
// groups, as compaction does, and adds each record to the index of its group when its segment is
// written. A record that is older than the record of its key in the index stays in its segment
// as dead space.
//
// The segment manager makes the sink. The sink holds a gate holder of the segment manager and a
// buffer of its pool until close() or abort(). The caller must call one of the two, and must not
// start a call while another call is in progress.
class record_stream_sink {
public:
    // Returns the compaction group of a token, with the holders that a write to the group takes.
    using target_resolver = noncopyable_function<write_target(dht::token)>;
    // Writes the buffer as a full segment of the group, and completes the writes of the buffer.
    using segment_writer = noncopyable_function<future<>(write_buffer&, logstor_group&)>;

    record_stream_sink(table_id table, target_resolver resolve_target, owned_write_buffer buffer,
            segment_writer write_segment, seastar::gate::holder segment_manager_holder);
    record_stream_sink(const record_stream_sink&) = delete;
    record_stream_sink& operator=(const record_stream_sink&) = delete;
    ~record_stream_sink();

    // Writes the records of each whole chunk in `data`. After a put() fails, the sink refuses
    // put() and close(), and the caller must call abort().
    future<> put(std::span<temporary_buffer<char>> data);

    // Writes the records that are in the buffer. Fails and writes nothing if the stream ends inside
    // a chunk or if a put() failed. A close() that fails releases the sink as abort() does, so the
    // caller does not have to call abort() after it.
    future<> close();

    // Drops the records that are in the buffer. The segments that the sink wrote stay in their
    // groups.
    future<> abort();

private:
    table_id _table;
    target_resolver _resolve_target;
    owned_write_buffer _buffer;
    segment_writer _write_segment;
    seastar::gate::holder _segment_manager_holder;
    stream_chunk_assembler _assembler;
    // Holds the group of the records in the buffer, with its holders. The holders stay until the
    // segment of the records is in the group, as for a normal write.
    write_target _target;
    std::vector<future<>> _index_updates;
    // Is true while put() runs, and stays true if put() fails. A failed put() can leave part of a
    // chunk in the buffer and in the assembler, so the sink must not take more data after it.
    bool _failed = false;

    void throw_if_failed() const;
    future<> write_chunk(temporary_buffer<char> bytes);
    future<> write_record(const chunk_record& record);
    future<> write_buffered_records();
    future<> flush();
    future<> release_buffer() noexcept;
};

}
