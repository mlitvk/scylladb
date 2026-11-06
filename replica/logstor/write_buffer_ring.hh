/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <chrono>
#include <optional>
#include <vector>

#include <seastar/core/condition-variable.hh>
#include <seastar/core/expiring_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>

#include "replica/logstor/write_buffer.hh"
#include "timeout_config.hh"

namespace replica {

namespace logstor {

// A record that the ring took into a buffer. `persisted` resolves once the buffer is in a segment.
struct accepted_write {
    future<written_record> persisted;
};

struct write_buffer_ring_config {
    size_t buffer_size;
    // The number of buffers in the ring, which must be at least 2.
    size_t ring_size;
    // The scheduling group of the consumer, and so of the flushes.
    seastar::scheduling_group flush_sg;
    // The most record bytes that the queue of writes holds, and 0 for no limit. A write that would
    // go above it fails with replica::rate_limit_exception.
    size_t max_queued_write_bytes{0};
    // How long the head buffer takes records after its first record before it advances, when it
    // does not fill first.
    std::chrono::milliseconds sync_period{0};
};

// A ring of mixed write buffers, which takes the user writes.
//
// A write goes into the head buffer. The head advances when the next record does not fit, when the
// sync period ends after the first record, and when flush() or stop() asks for it. A write that
// finds the ring full waits in a queue, with its timeout and within max_queued_write_bytes.
//
// A single consumer fiber gives each buffer behind the head to the flush function, reclaims the
// flushed buffers from the tail in ring order, and moves queued writes into the free buffers.
class write_buffer_ring {
    seastar::scheduling_group _flush_sg;
    size_t _buffer_size;
    size_t _ring_size;
    std::chrono::milliseconds _sync_period;
    seastar::noncopyable_function<future<>(mixed_write_buffer&)> _flush_func;

    // The ring of buffers, indexed modulo _ring_size.
    std::vector<mixed_write_buffer> _ring;

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
        seastar::promise<accepted_write> accepted_pr; // written to buffer
        seastar::promise<written_record> persisted_pr; // written to a segment
        log_record_writer writer;
        write_target target;
        // Keeps the ring from finishing its stop() while the record is on the queue. The request
        // holds it, so that queueing a write needs no coroutine.
        seastar::gate::holder ring_holder;
        db::timeout_clock::time_point timeout;
        uint64_t id;
        size_t write_size;

        queued_write(log_record_writer writer, write_target target, seastar::gate::holder ring_holder,
                db::timeout_clock::time_point timeout, uint64_t id, size_t write_size)
            : writer(std::move(writer))
            , target(std::move(target))
            , ring_holder(std::move(ring_holder))
            , timeout(timeout)
            , id(id)
            , write_size(write_size) {
        }

        void fail_timeout() noexcept {
            auto ep = std::make_exception_ptr(seastar::timed_out_error{});
            accepted_pr.set_exception(ep);
            persisted_pr.set_exception(ep);
        }
    };

    struct on_queued_write_expiry {
        write_buffer_ring* owner{};

        void operator()(queued_write& w) noexcept {
            owner->on_queued_write_removed(w);
            w.fail_timeout();
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
    size_t _max_queued_write_bytes{0};

    seastar::timer<db::timeout_clock> _sync_timer;
    bool _sync_deadline_expired = false;

    // Monotonically increasing id assigned to queued writes. flush() snapshots
    // this counter and waits until all queued writes with lower ids are gone.
    uint64_t _next_queued_write_id = 0;

    seastar::gate _async_gate;

    future<> _consumer{make_ready_future<>()};

    auto&& head_buf(this auto&& self) noexcept { return self._ring[self._head % self._ring_size]; }
    auto&& tail_buf(this auto&& self) noexcept { return self._ring[self._tail % self._ring_size]; }

    auto&& tail_write(this auto&& self) noexcept { return self._in_flight[self._tail % self._ring_size]; }
    auto&& dispatch_tail_write(this auto&& self) noexcept { return self._in_flight[self._dispatch_tail % self._ring_size]; }

    // The ring is full when all _ring_size slots are occupied. Advancing the
    // head further would make the new head slot collide with the tail slot.
    bool ring_full() const noexcept { return _head - _tail == _ring_size - 1; }

    bool has_pending_buffers() const noexcept;
    bool should_advance_head() const noexcept;
    bool maybe_advance_head() noexcept;

    std::optional<future<written_record>> append_to_head_buffer(log_record_writer&, write_target&);

    // Puts a record that found no room in the ring on the queue of writes. Returns a future that
    // resolves once the consumer moves the record into a buffer.
    future<accepted_write> queue_write(log_record_writer, db::timeout_clock::time_point timeout, write_target,
            seastar::gate::holder);

    bool try_dispatch_next_buffer();
    future<> run_dispatched_write(size_t idx);
    future<bool> reclaim_completed_tails();

    future<bool> drain_queued_writes();

    void on_queued_write_removed(const queued_write&) noexcept;
    void fail_queued_write(queued_write&, std::exception_ptr) noexcept;
    void on_queued_writes_changed() noexcept;

    void arm_sync_timer();
    void on_sync_timer() noexcept;
    void cancel_sync_timer() noexcept;

    future<> consumer_loop();

public:
    explicit write_buffer_ring(write_buffer_ring_config cfg,
            seastar::noncopyable_function<future<>(mixed_write_buffer&)> flush_func);

    write_buffer_ring(const write_buffer_ring&) = delete;
    write_buffer_ring& operator=(const write_buffer_ring&) = delete;

    future<> start();
    future<> stop();
    future<> flush();

    // Takes a record. The future it returns resolves once the record is in a buffer, and carries a
    // second future that resolves once that buffer is in a segment, which is where the record's
    // location comes from.
    future<accepted_write> write_to_buffer(log_record_writer, db::timeout_clock::time_point timeout, write_target target = {}) noexcept;

    // Resolves once the record is in a segment. Not a coroutine, so it adds no frame per write.
    future<written_record> write(log_record_writer writer, db::timeout_clock::time_point timeout, write_target target = {}) noexcept {
        return write_to_buffer(std::move(writer), timeout, std::move(target)).then([] (accepted_write a) {
            return std::move(a.persisted);
        });
    }

    size_t queued_write_count() const noexcept { return _queued_writes.size(); }

    size_t memory_usage() const noexcept { return _ring_size * _buffer_size + _queued_write_bytes; }

};

}
}
