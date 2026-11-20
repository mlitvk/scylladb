/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <algorithm>
#include <concepts>
#include <exception>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/expiring_fifo.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/timer.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/log.hh>

#include "replica/logstor/index_update.hh"
#include "replica/logstor/ondisk.hh"
#include "timeout_config.hh"

namespace replica {

namespace logstor {

extern seastar::logger logstor_logger;

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

// Writer for log records that handles serialization and size computation
class log_record_writer {
    log_record _record;

public:
    explicit log_record_writer(log_record record)
        : _record(std::move(record))
    {}

    const log_record& record() const { return _record; }
    const record_header& header() const { return _record.header; }

    size_t header_size() const { return ondisk::record_header_size(_record.header); }
    size_t value_size() const { return _record.value.size(); }
    // The record without its frame header: the record header and the value.
    size_t record_size() const { return header_size() + value_size(); }

    // Write the record's frame - frame header, record header, value - to an output stream.
    void write_frame(seastar::simple_memory_output_stream& out) const;
};

// A record writer for a record whose header and value are already serialized. The writer copies the
// bytes as they are, so that a rewrite of a record does not decode the record.
class log_record_bytes_writer {
    record_header _header;
    bytes_view _header_bytes;
    bytes_view _value_bytes;

public:
    log_record_bytes_writer(record_header header, log_record_bytes_view record_view)
        : _header(std::move(header))
        , _header_bytes(record_view.header)
        , _value_bytes(record_view.value)
    {}

    const record_header& header() const { return _header; }

    size_t header_size() const { return _header_bytes.size(); }
    size_t value_size() const { return _value_bytes.size(); }
    size_t record_size() const { return header_size() + value_size(); }

    void write_frame(seastar::simple_memory_output_stream& out) const;
};

template <typename T>
concept log_record_writer_concept = requires(const T& w, seastar::simple_memory_output_stream& out) {
    { w.header() } -> std::convertible_to<const record_header&>;
    { w.header_size() } -> std::convertible_to<size_t>;
    { w.value_size() } -> std::convertible_to<size_t>;
    { w.record_size() } -> std::convertible_to<size_t>;
    { w.write_frame(out) };
};

template <log_record_writer_concept Writer>
size_t record_frame_size(const Writer& writer) noexcept {
    return ondisk::record_frame_size(writer.header_size(), writer.value_size());
}

// Holds the offset of a record frame in a chunk and the frame_size of the frame, as record_location
// does.
struct frame_extent {
    uint32_t offset;
    uint32_t size;
};

// Returns the location of the record whose frame is at `frame` in a chunk that is written at
// `chunk_position`.
inline record_location locate_record(segment_position chunk_position, frame_extent frame) noexcept {
    return record_location {
        .segment = chunk_position.segment,
        .offset = chunk_position.offset + frame.offset,
        .size = frame.size,
    };
}

namespace chunk_layout {

// Returns the largest record_size that fits an empty chunk of `kind` in a buffer of `buffer_size`
// bytes. The chunk header of a full segment is larger than the chunk header of a mixed segment, so
// a record can fit a chunk of one kind and not a chunk of the other kind.
constexpr size_t max_record_size(size_t buffer_size, segment_kind kind) noexcept {
    const size_t overhead = ondisk::chunk_header_size(kind) + ondisk::record_frame_header_size;
    return buffer_size > overhead ? buffer_size - overhead : 0;
}

// Returns the largest record_size that fits an empty chunk of each kind in a buffer of
// `buffer_size` bytes. The separator rewrites the records of a mixed segment into full segments, so
// a record that logstor accepts must fit both kinds.
constexpr size_t max_record_size_any_kind(size_t buffer_size) noexcept {
    return std::min(max_record_size(buffer_size, segment_kind::mixed),
                    max_record_size(buffer_size, segment_kind::full));
}

}

// Builds one chunk in memory, in a buffer of buffer_size() bytes:
//
//   0                     +------------------+
//                         |   chunk header   |  seal()
//   chunk_header_size()   +------------------+
//                         |   record frame   |  append(), one frame per record,
//                         |   record frame   |  each padded to record_alignment
//                         |       ...        |
//   + records_size        +------------------+
//                         |   zero padding   |  finish(), to block_alignment
//   chunk size            +------------------+
//                         |      unused      |
//   buffer_size()         +------------------+
//
// reset() reserves the space for the header, so the records start at chunk_header_size().
// The header of a full segment also holds the table and the token range of the records. The
// record frames are the body of the chunk, which the body CRC covers.
//
// finish() computes the body CRC and pads the chunk, and the size that finish() returns does not
// change after this. seal() then writes the chunk header, which holds the segment_seq of the
// segment that receives the chunk. So a caller can reserve room for the chunk in a segment before
// it knows the segment_seq.
class chunk_builder {
public:
    chunk_builder(size_t buffer_size, segment_kind kind);

    chunk_builder(const chunk_builder&) = delete;
    chunk_builder& operator=(const chunk_builder&) = delete;

    chunk_builder(chunk_builder&&) noexcept = default;
    chunk_builder& operator=(chunk_builder&&) noexcept = default;

    // Empties the buffer, to build a new chunk in it.
    void reset();

    // Appends the frame of the record of `writer`.
    // Throws std::runtime_error when the record does not fit, see fits(). An append that throws
    // leaves the builder unchanged.
    template <log_record_writer_concept Writer>
    frame_extent append(const Writer& writer);

    // Ends the appends, pads the chunk to block_alignment and computes the body CRC. Returns the
    // size of the chunk, which does not change after this.
    size_t finish();

    // Finishes the chunk if finish() was not called, writes the chunk header and returns the chunk.
    // The first form seals a chunk of a mixed segment, and the second form seals a chunk of a full
    // segment, whose chunk header holds `table`.
    bytes_view seal(segment_sequence segment_seq);
    bytes_view seal(segment_sequence segment_seq, table_id table);

    template <log_record_writer_concept Writer>
    bool fits(const Writer& writer) const noexcept {
        return fits_frame(record_frame_size(writer));
    }

    bool has_data() const noexcept { return _record_count > 0; }

    size_t buffer_size() const noexcept { return _buffer_size; }

    size_t chunk_header_size() const noexcept {
        return ondisk::chunk_header_size(_segment_kind);
    }

    size_t record_bytes() const noexcept { return _record_bytes; }
    size_t record_count() const noexcept { return _record_count; }

    // Returns the bytes of an appended record frame, without the padding after the frame.
    bytes_view frame_bytes(frame_extent frame) const noexcept {
        return bytes_view(reinterpret_cast<const int8_t*>(_buffer.get() + frame.offset), frame.size);
    }

private:
    using ostream = seastar::simple_memory_output_stream;

    size_t _buffer_size;
    std::unique_ptr<char[], free_deleter> _buffer;
    segment_kind _segment_kind;
    ostream _stream;

    // The frame sizes of the appended records summed, their padding excluded.
    size_t _record_bytes{0};
    size_t _record_count{0};
    std::optional<dht::token> _min_token;
    std::optional<dht::token> _max_token;

    // Holds the values that finish() computes: the size of the chunk, and the size and the CRC of
    // the chunk body for the chunk header.
    struct finished_body {
        size_t chunk_size;
        uint32_t records_size;
        uint32_t body_crc;
    };
    std::optional<finished_body> _finished_body;
    bool _sealed{false};

    // Fails with an internal error if the chunk is finished, or if a record frame header cannot
    // describe the record header of `writer`.
    // Throws std::runtime_error if the buffer has no room for the record frame of `writer` and its
    // padding.
    template <log_record_writer_concept Writer>
    void validate_append(const Writer& writer) const;

    bool fits_frame(size_t frame_size) const noexcept;

    // Returns the chunk offset at which the next record frame or padding goes.
    size_t write_offset() const noexcept { return _buffer_size - _stream.size(); }

    bytes_view do_seal(segment_sequence segment_seq, std::optional<table_id> table);
};

// A record of a mixed buffer, which the separator rewrites after the buffer is flushed.
struct separator_record {
    log_record_writer writer;
    frame_extent frame;
    write_target target;

    record_location location(segment_position chunk_position) const noexcept {
        return locate_record(chunk_position, frame);
    }
};

// A write completion holds the work that a write buffer does for one record after the buffer tries
// to write its chunk. The buffer calls one function of the completion of each record, once, in the
// order of the records:
// - At a chunk that is written to a segment, the buffer calls on_written() with the location of the
//   record.
// - At a chunk that is not written, the buffer calls on_failed().
template <typename Completion>
concept write_completion = std::is_nothrow_move_constructible_v<Completion>
        && std::is_nothrow_move_assignable_v<Completion>
        && requires (Completion& c, record_location location, std::exception_ptr ex) {
    { c.on_written(location) } noexcept;
    { c.on_failed(ex) } noexcept;
};

// A chunk builder that keeps a write completion for each record that it takes. The buffer completes
// the writes after its chunk is written to a segment, and fails the writes when the chunk is not
// written.
//
// mixed_write_buffer and full_write_buffer take different records and seal different chunk headers,
// and this class holds the parts that they share.
template <write_completion Completion>
class write_buffer {
    struct entry {
        Completion completion;
        frame_extent frame{};

        explicit entry(Completion&& c) noexcept
            : completion(std::move(c))
        {}
    };

    chunk_builder _builder;
    // The buffer clears the vector and does not free it, so that a reused buffer allocates nothing.
    std::vector<entry> _entries;

public:
    write_buffer(const write_buffer&) = delete;
    write_buffer& operator=(const write_buffer&) = delete;

    // Ends the appends, see chunk_builder::finish().
    size_t finish() { return _builder.finish(); }

    template <log_record_writer_concept Writer>
    bool fits(const Writer& writer) const noexcept { return _builder.fits(writer); }
    bool has_data() const noexcept { return _builder.has_data(); }

    size_t buffer_size() const noexcept { return _builder.buffer_size(); }
    size_t record_bytes() const noexcept { return _builder.record_bytes(); }
    size_t record_count() const noexcept { return _builder.record_count(); }

    // Returns how many writes are neither complete nor aborted.
    size_t pending_write_count() const noexcept { return _entries.size(); }

protected:
    write_buffer(size_t buffer_size, segment_kind kind)
        : _builder(buffer_size, kind)
    {}

    // A completion that is destroyed and not called leaves its write without an outcome.
    ~write_buffer() {
        if (!_entries.empty()) {
            on_internal_error_noexcept(logstor_logger, fmt::format("Destroying a write buffer with {} writes that are not complete", _entries.size()));
        }
    }

    write_buffer(write_buffer&&) noexcept = default;
    write_buffer& operator=(write_buffer&&) noexcept = default;

    // Appends the record, and keeps `completion` for complete_writes(). The completion goes into
    // the buffer before the record, so that nothing can fail after the record is in the chunk. When
    // the append throws, `completion` stays with the caller.
    template <log_record_writer_concept Writer>
    frame_extent append(const Writer& writer, Completion&& completion) {
        // emplace_back() allocates before it moves `completion`, so after a failed allocation
        // `completion` also stays with the caller.
        auto& e = _entries.emplace_back(std::move(completion));
        try {
            e.frame = _builder.append(writer);
        } catch (...) {
            completion = std::move(e.completion);
            _entries.pop_back();
            throw;
        }
        return e.frame;
    }

    // Calls on_written() of each completion, once the chunk of the buffer is written at
    // `chunk_position`.
    future<> complete_writes(segment_position chunk_position) {
        for (auto& e : _entries) {
            e.completion.on_written(locate_record(chunk_position, e.frame));
            co_await coroutine::maybe_yield();
        }
        _entries.clear();
    }

    // Calls on_failed() of each completion with `ex`, for a chunk that is not written.
    void abort_writes(std::exception_ptr ex) noexcept {
        for (auto& e : _entries) {
            e.completion.on_failed(ex);
        }
        _entries.clear();
    }

    // Empties the buffer for a new chunk.
    // The caller must complete or abort the writes of the last chunk first.
    void reset() {
        if (!_entries.empty()) {
            on_internal_error(logstor_logger, fmt::format("Cannot reset a write buffer with {} writes that are not complete", _entries.size()));
        }
        _builder.reset();
    }

    chunk_builder& builder() noexcept { return _builder; }
    const chunk_builder& builder() const noexcept { return _builder; }
};

// The write buffer of a mixed segment, which takes the user writes. The buffer keeps the records
// that it took until its chunk is written, so that the separator can rewrite them into full
// segments.
template <write_completion Completion>
class mixed_write_buffer final : public write_buffer<Completion> {
    using base = write_buffer<Completion>;

    // The separator takes the vector with each chunk, so write() reserves the record count of the
    // last chunk for the next one.
    std::vector<separator_record> _separator_records;
    size_t _last_record_count = 0;

public:
    explicit mixed_write_buffer(size_t buffer_size)
        : base(buffer_size, segment_kind::mixed)
    {}

    // Appends the record, see write_buffer::append().
    void write(log_record_writer writer, write_target target, Completion&& completion) {
        if (_separator_records.capacity() == 0) {
            _separator_records.reserve(_last_record_count);
        }
        auto& record = _separator_records.emplace_back(separator_record{
            .writer = std::move(writer),
            .target = std::move(target),
        });
        try {
            record.frame = base::append(record.writer, std::move(completion));
        } catch (...) {
            _separator_records.pop_back();
            throw;
        }
    }

    bytes_view seal(segment_sequence segment_seq) {
        return base::builder().seal(segment_seq);
    }

    // Completes the writes, and only then returns the records of the chunk for the separator, so
    // that the index already points at each record when the separator moves the record.
    future<std::vector<separator_record>> complete_writes(segment_position chunk_position) {
        co_await base::complete_writes(chunk_position);
        _last_record_count = _separator_records.size();
        co_return std::move(_separator_records);
    }

    void abort_writes(std::exception_ptr ex) noexcept {
        // The separator never gets these records, so drop them and the holders of their targets.
        _separator_records.clear();
        base::abort_writes(std::move(ex));
    }

    void reset() {
        base::reset();
        _separator_records.clear();
    }
};

// The write buffer of a full segment, which takes the records of compaction and the separator.
template <write_completion Completion>
class full_write_buffer final : public write_buffer<Completion> {
    using base = write_buffer<Completion>;

public:
    explicit full_write_buffer(size_t buffer_size)
        : base(buffer_size, segment_kind::full)
    {}

    // Appends the record, see write_buffer::append().
    template <log_record_writer_concept Writer>
    void write(const Writer& writer, Completion&& completion) {
        base::append(writer, std::move(completion));
    }

    bytes_view seal(segment_sequence segment_seq, table_id table) {
        return base::builder().seal(segment_seq, table);
    }

    using base::complete_writes;
    using base::abort_writes;
    using base::reset;
};

// The write buffer of the user writes, whose completion inserts the record into the index.
using user_write_buffer = mixed_write_buffer<index_insert>;

// The write buffer of compaction and the separator, whose completion points the index entry of the
// record at its new location.
using rewrite_buffer = full_write_buffer<index_relocation>;

extern template frame_extent chunk_builder::append<log_record_writer>(const log_record_writer&);
extern template frame_extent chunk_builder::append<log_record_bytes_writer>(const log_record_bytes_writer&);

class write_buffer_pool;

// Gives a borrowed rewrite_buffer back to its pool, along with the pool unit acquired for it.
class write_buffer_returner {
    write_buffer_pool* _pool = nullptr;
    seastar::semaphore_units<> _units;

public:
    write_buffer_returner() = default;
    write_buffer_returner(write_buffer_pool& pool, seastar::semaphore_units<> units) noexcept
        : _pool(&pool), _units(std::move(units)) {}

    void operator()(rewrite_buffer* wb) noexcept;
};

// A rewrite_buffer borrowed from a write_buffer_pool. reset() and destruction give it back, so the
// accounting of the pool holds on every path, also when the owner throws.
using owned_write_buffer = std::unique_ptr<rewrite_buffer, write_buffer_returner>;

// A pool of rewrite_buffers. It hands out at most `capacity` buffers at once, builds them on demand,
// and keeps up to `max_cached` of them for reuse. The owner must call stop() after all users stop.
class write_buffer_pool {
public:
    struct config {
        // The most buffers the pool may have out at once.
        size_t capacity{0};
        size_t buffer_size{0};
        // Buffers built at construction rather than at the point of use. Must not exceed max_cached,
        // or they would not all be kept.
        size_t preallocate{0};
        // The most returned buffers kept for reuse. Must not exceed capacity.
        size_t max_cached{0};
    };

    struct stats {
        uint64_t allocation_waits{0};
        uint64_t buffers_dropped{0};
        uint64_t buffers_created{0};
    };

private:
    size_t _buffer_size;
    size_t _max_cached;
    size_t _capacity;

    // Buffers ready to be handed out. Never longer than _max_cached, and reserved for it up front,
    // so that returning a buffer to it never allocates - it has to be infallible, being the last
    // step of an owner's destructor.
    std::vector<std::unique_ptr<rewrite_buffer>> _free;
    // Buffers that are currently out with an owner.
    size_t _in_use{0};

    seastar::semaphore _available_sem;
    bool _stopped{false};
    stats _stats;

public:

    explicit write_buffer_pool(config);

    future<> stop();

    future<owned_write_buffer> allocate(abort_source& as);
    future<std::vector<owned_write_buffer>> allocate_many(size_t count, abort_source& as);

    void return_buffer(rewrite_buffer* wb, seastar::semaphore_units<> pool_units) noexcept;

    // Changes the number of buffers the pool may have out at once. Lowering it does not take back
    // any of the buffers that are already out - it only makes the pool hand out that many fewer
    // before the returns catch up - but it does free the ones it no longer needs to keep.
    void set_capacity(size_t capacity);

    size_t capacity() const noexcept { return _capacity; }

    size_t memory_usage() const noexcept { return allocated_buffer_count() * _buffer_size; }

    // Buffers that are currently out with an owner.
    size_t used_buffer_count() const noexcept { return _in_use; }

    // Returns the number of buffers that exist, which gives the memory that the pool holds.
    size_t allocated_buffer_count() const noexcept {
        return _in_use + _free.size();
    }

    const stats& get_stats() const noexcept {
        return _stats;
    }

private:
    bool would_wait(size_t count) const noexcept {
        return _available_sem.available_units() < static_cast<ssize_t>(count) || _available_sem.waiters() > 0;
    }
};

// Configuration passed to write_buffer_ring constructor (excluding the flush function).
struct write_buffer_ring_config {
    size_t buffer_size;
    // The scheduling group that the ring runs its flushes in.
    seastar::scheduling_group flush_sg;
    // The number of buffers in the ring, which allows at most `ring_size - 1` flushes in flight.
    // With a sync period of 0, records collect in a buffer only while all flushes are in flight, so
    // a larger ring writes more and smaller chunks, each padded to a block, and adds no throughput.
    // The default allows four flushes in flight, which are sufficient to overlap the disk write with
    // the index and separator work.
    size_t ring_size{5};
    // The time that the ring waits after the first record of a head buffer that is not full, before
    // the head advances past the buffer. A longer period writes fewer and larger chunks at low load,
    // but each write then waits up to the period. With the default of 0, records still collect in a
    // buffer while all flushes are in flight, so the chunks are not small under load.
    std::chrono::milliseconds sync_period{0};
};

// Manages a circular ring of mixed write buffers. The ring reports the outcome of each write
// through its index_insert, also when the ring refuses the write or the queue time of the write
// ends.
//
// Writers append to the head buffer.  A single consumer coroutine drains the
// tail.  The head advances when the current head buffer is full (can't fit the
// next write) or when the consumer advances it to flush the head buffer.
// Writers wait if the ring is full (all buffers are pending flush).
class write_buffer_ring {
    seastar::scheduling_group _flush_sg;
    size_t _buffer_size;
    size_t _ring_size;
    std::chrono::milliseconds _sync_period;
    seastar::noncopyable_function<future<>(user_write_buffer&)> _flush_func;

    // The ring of buffers, indexed modulo _ring_size.
    std::vector<user_write_buffer> _ring;

    // Monotonically increasing indices; the actual slot is idx % _ring_size.
    // _head: next slot writers append to.
    // _dispatch_tail: next slot the consumer will dispatch via _flush_func.
    // _tail: oldest slot not yet safe to reuse.
    // Invariant: _head >= _dispatch_tail >= _tail && _head - _tail < _ring_size.
    size_t _head{0};
    size_t _dispatch_tail{0};
    size_t _tail{0};

    struct in_flight_write {
        std::optional<future<>> completion;

        bool idle() const noexcept {
            return !completion;
        }

        bool ready() const noexcept {
            return completion && completion->available();
        }

        void start(future<> f) {
            completion.emplace(std::move(f));
        }

        future<> take_completion() {
            auto f = std::move(*completion);
            completion.reset();
            return f;
        }

        void reset() noexcept {
            completion.reset();
        }
    };

    std::vector<in_flight_write> _in_flight;

    struct queued_write {
        log_record_writer writer;
        write_target target;
        index_insert completion;
        // Keeps the ring from finishing its stop() while the record is on the queue. It is held
        // by the request rather than by whoever waits for it, so that queueing a write needs no
        // coroutine of its own.
        seastar::gate::holder async_gate_holder;
        db::timeout_clock::time_point timeout;
        uint64_t id;
        size_t write_size;

        queued_write(log_record_writer writer, write_target target, index_insert completion, seastar::gate::holder async_gate_holder,
                db::timeout_clock::time_point timeout, uint64_t id, size_t write_size)
            : writer(std::move(writer))
            , target(std::move(target))
            , completion(std::move(completion))
            , async_gate_holder(std::move(async_gate_holder))
            , timeout(timeout)
            , id(id)
            , write_size(write_size) {
        }
    };

    struct on_queued_write_expiry {
        write_buffer_ring* owner{};

        void operator()(queued_write& w) noexcept {
            owner->on_queued_write_removed(w);
            owner->fail_queued_write(w, std::make_exception_ptr(seastar::timed_out_error{}));
            owner->on_queued_writes_changed();
        }
    };

    // Wakes the consumer whenever a state change may let it make forward
    // progress again: queued writes arrive, the head buffer gains data or
    // advances, a flush completes, a tail is reclaimed, or shutdown begins.
    seastar::condition_variable _consumer_progress_cv;

    // Notified when queued writes are removed or expired, so flush() can wait
    // for the pre-flush queue boundary to advance.
    seastar::condition_variable _queued_writes_changed;

    // Notified when the tail is advanced by the consumer.
    seastar::condition_variable _tail_advanced;

    seastar::expiring_fifo<queued_write, on_queued_write_expiry, db::timeout_clock> _queued_writes;
    size_t _queued_write_bytes{0};
    uint64_t _total_queued_writes{0};
    uint64_t _failed_writes{0};

    seastar::timer<db::timeout_clock> _head_flush_timer;
    bool _head_deadline_expired = false;

    // Monotonically increasing id assigned to queued writes. flush() snapshots
    // this counter and waits until all queued writes with lower ids are gone.
    uint64_t _next_queued_write_id = 0;

    seastar::gate _async_gate;

    // The single flush-consumer fiber, running for the lifetime of the ring.
    future<> _consumer{make_ready_future<>()};

    auto&& head_buf(this auto&& self) noexcept { return self._ring[self._head % self._ring_size]; }
    auto&& tail_buf(this auto&& self) noexcept { return self._ring[self._tail % self._ring_size]; }

    auto&& tail_write(this auto&& self) noexcept { return self._in_flight[self._tail % self._ring_size]; }
    auto&& dispatch_tail_write(this auto&& self) noexcept { return self._in_flight[self._dispatch_tail % self._ring_size]; }

    // The ring is full when all _ring_size slots are occupied. Advancing the
    // head further would make the new head slot collide with the tail slot.
    bool ring_full() const noexcept { return _head - _tail == _ring_size - 1; }

    bool has_pending_buffers() const noexcept;
    bool should_rotate_head_for_flush() const noexcept;
    bool maybe_advance_head() noexcept;

    // Appends the record to the head buffer, and advances the head first when the record does not
    // fit there. Returns false, with the arguments left as they are, when the ring is full.
    bool append_to_head_buffer(log_record_writer&, write_target&, index_insert&);

    // Puts a record that found no room in the ring on the queue of writes, where the consumer moves
    // it into a buffer. The queue has no size limit, so a write waits until its timeout, as a
    // memtable write waits for dirty memory.
    void queue_write(log_record_writer, index_insert, db::timeout_clock::time_point timeout, write_target,
            seastar::gate::holder) noexcept;

    bool try_dispatch_next_buffer();
    future<> run_dispatched_write(size_t idx);
    future<bool> reclaim_completed_tails();

    future<bool> drain_queued_writes();

    void on_queued_write_removed(const queued_write&) noexcept;
    void fail_write(index_insert&, std::exception_ptr) noexcept;
    void fail_queued_write(queued_write&, std::exception_ptr) noexcept;
    void on_queued_writes_changed() noexcept;

    void arm_head_flush_timer();
    void on_head_flush_timer() noexcept;
    void cancel_head_flush_timer() noexcept;

    future<> consumer_loop();

public:
    explicit write_buffer_ring(write_buffer_ring_config cfg,
            seastar::noncopyable_function<future<>(user_write_buffer&)> flush_func);

    write_buffer_ring(const write_buffer_ring&) = delete;
    write_buffer_ring& operator=(const write_buffer_ring&) = delete;

    future<> start();
    future<> stop();
    future<> flush();

    // Takes a record, and reports the outcome of the write through `completion`.
    void write(log_record_writer, index_insert completion, db::timeout_clock::time_point timeout, write_target target = {}) noexcept;

    size_t queued_write_count() const noexcept { return _queued_writes.size(); }
    size_t queued_write_bytes() const noexcept { return _queued_write_bytes; }
    uint64_t total_queued_writes() const noexcept { return _total_queued_writes; }

    // Returns how many writes the ring failed: refused, timed out on the queue, or in a buffer whose
    // flush failed.
    uint64_t failed_write_count() const noexcept { return _failed_writes; }

    // Returns the memory of the ring buffers. The queued writes are not counted, because the limits
    // on open requests already hold their memory, and their memory is freed when the flushes continue.
    size_t memory_usage() const noexcept { return _ring_size * _buffer_size; }

};

}
}
