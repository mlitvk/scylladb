/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <deque>
#include <optional>
#include <span>
#include <stdexcept>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/noncopyable_function.hh>

#include "replica/logstor/compaction.hh"
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/write_buffer.hh"
#include "utils/chunked_vector.hh"
#include "utils/phased_barrier.hh"

namespace replica::logstor {

class table_catalog;

// A chunk of a stream is at most this long, because it holds the records of one segment of the
// sender, and no segment is larger than this.
inline constexpr size_t max_stream_chunk_size = segment_descriptor_hist_options.max_size;

// The sink and the source throw this error when a record stream is not valid, for example when a
// chunk does not parse, a record is not of the table of the stream, the stream ends inside a chunk,
// or a segment of the sender is not a full segment of the table.
class record_stream_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// The sender of a record stream. It gives one chunk for each segment in its list that has live
// records, and then an empty buffer. A record is live if the index points at it when the source
// reads its segment.
//
// The source owns a guard that keeps compaction of the group disabled for its life, so the
// segments in the list stay in the group and their records do not move.
// A tablet merge can move the segments of the group to its merge target during the stream, so the
// source also owns a guard that keeps compaction of the merge target disabled, and the records do
// not move after the merge either.
// A segment that has no valid chunk, or that is not a full segment of the table, fails the
// stream, so that the stream does not lose records without notice.
//
// Apart from a corruption on disk, such a segment is a bug, because a listed segment can change
// like that only when it is freed and used again during the stream. Compaction and
// discard_segments() free the segments of a group. The guards keep compaction from running, and a
// discard runs only at a truncate and at a tablet cleanup, which the topology coordinator does not
// run on a tablet while the tablet streams.
class record_stream_source final : public data_source_impl {
public:
    // Calls the function with each chunk of the segment, in order.
    using chunk_reader = noncopyable_function<future<>(log_segment_id, noncopyable_function<future<>(const scanned_chunk&)>)>;

    record_stream_source(table_id table, const primary_index& index, compaction_reenabler compaction_guard,
            compaction_reenabler merge_target_guard, utils::chunked_vector<log_segment_id> segments, size_t segment_size,
            chunk_reader read_chunks);

    future<temporary_buffer<char>> get() override;

private:
    table_id _table;
    const primary_index& _index;
    compaction_reenabler _compaction_guard;
    compaction_reenabler _merge_target_guard;
    utils::chunked_vector<log_segment_id> _segments;
    size_t _next_segment = 0;
    chunk_reader _read_chunks;
    raw_write_buffer _buffer;

    future<> collect_live_records(log_segment_id segment_id);
    temporary_buffer<char> take_chunk();
};

// Cuts the bytes of a stream into chunks. The bytes arrive in buffers of any size, so a chunk can
// start in one buffer and end in a later one.
class stream_chunk_assembler {
    std::deque<temporary_buffer<char>> _buffers;
    size_t _size = 0;
    // Holds the size of the chunk at the front after its chunk header arrived.
    std::optional<size_t> _chunk_size;

    size_t read_chunk_size() const;
    void copy_front(char* out, size_t n) const;
    temporary_buffer<char> take_front(size_t n);

public:
    void push(temporary_buffer<char>);

    // Returns the next whole chunk, or nothing if not all bytes of the chunk arrived yet.
    // Throws record_stream_error if the chunk header is not valid, is not of a full segment, has a
    // format version or a flag that this release does not read, or gives a chunk longer than
    // max_stream_chunk_size.
    // This function does not check the CRCs, because parse_chunk() checks them.
    std::optional<temporary_buffer<char>> next_chunk();

    // Returns true if bytes arrived that are not in a chunk that next_chunk() returned.
    bool has_partial_chunk() const noexcept {
        return _size > 0;
    }
};

// A record stream sink has a buffer for each of two compaction groups, because a storage group in
// split mode gives the records of one tablet to two groups, and the sender can send the records of
// the two groups mixed.
inline constexpr size_t record_stream_sink_buffers = 2;

// The receiver of a record stream. It packs the records of each compaction group into full
// segments of that group, as compaction does, and adds each record to the index of its group when
// its segment is written. A record that is older than the record of its key in the index stays in
// its segment as dead space.
//
// The segment manager makes the sink. The sink finds the compaction group of each record in the
// table catalog of the segment manager.
// The sink keeps three things until close() or abort(): a gate holder of the segment manager,
// record_stream_sink_buffers buffers of its pool, and `table_stream`, which keeps the stream in
// progress for the table.
// The caller must call one of the two, and must not start a call while another call is in
// progress.
class record_stream_sink {
public:
    // Writes the buffer as a full segment of the group, and completes the writes of the buffer.
    using segment_writer = noncopyable_function<future<>(write_buffer&, logstor_group&)>;

    // `buffers` holds record_stream_sink_buffers buffers.
    record_stream_sink(table_id table, table_catalog& catalog, std::vector<owned_write_buffer> buffers,
            segment_writer write_segment, seastar::gate::holder segment_manager_holder,
            utils::phased_barrier::operation table_stream);
    record_stream_sink(const record_stream_sink&) = delete;
    record_stream_sink& operator=(const record_stream_sink&) = delete;
    ~record_stream_sink();

    // Writes the records of each whole chunk in `data`. After a put() fails, the sink refuses
    // put() and close(), and the caller must call abort().
    future<> put(std::span<temporary_buffer<char>> data);

    // Writes the records that are in the buffers. Fails and writes nothing if the stream ends inside
    // a chunk or if a put() failed. A close() that fails releases the sink as abort() does, so the
    // caller does not have to call abort() after it.
    future<> close();

    // Drops the records that are in the buffers. The segments that the sink wrote stay in their
    // groups.
    // A close() or an abort() releases the sink, and the caller must not call put() or close()
    // after it.
    future<> abort();

private:
    // The records of one compaction group that wait for their segment.
    struct group_buffer {
        owned_write_buffer buffer;
        // Holds the group of the records in the buffer, with its holders, as a normal write holds
        // them until its segment is in the group.
        // The buffer keeps its group after a flush only when the next record goes to the same group,
        // so the holders stay only while the buffer holds records of the group, and until close()
        // returns.
        write_target target;
        std::vector<future<>> index_updates;
    };

    table_id _table;
    table_catalog& _catalog;
    segment_writer _write_segment;
    seastar::gate::holder _segment_manager_holder;
    // Keeps the stream in progress for the table.
    utils::phased_barrier::operation _table_stream;
    // Each buffer goes back to the pool of the segment manager at its destruction, so the
    // declaration of the buffers comes after the gate holder.
    std::array<group_buffer, record_stream_sink_buffers> _buffers;
    // Holds the index in `_buffers` of the buffer of the last record.
    size_t _last_used = 0;
    stream_chunk_assembler _assembler;
    // This flag is true after a put() fails. A failed put() can leave part of a chunk in the buffer
    // and in the assembler, so the sink must not take more data after the failure.
    bool _failed = false;
    // This flag is true after release(), which gives the buffers back to the pool.
    bool _released = false;

    void throw_if_failed() const;
    // Calls on_internal_error() after release(), because a sink without buffers cannot write.
    void check_not_released() const;
    future<> write_chunk(temporary_buffer<char> bytes);
    future<> write_record(const chunk_record& record);
    // Returns the compaction group of the token, with the holders that a write to the group takes.
    // Throws if the table catalog gives no group that takes a write of the token.
    write_target find_target(dht::token token);
    future<group_buffer*> buffer_for(write_target target);
    future<> write_buffered_records();
    future<> flush(group_buffer& gb);
    future<> release() noexcept;
};

}
