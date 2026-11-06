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

struct buffered_write_result {
    future<written_record> persisted;
};

// Configuration passed to buffered_writer constructor (excluding the flush function).
struct buffered_writer_config {
    size_t buffer_size;
    size_t ring_size;
    seastar::scheduling_group flush_sg;
    size_t max_queued_write_bytes{0};
    std::chrono::milliseconds sync_period{0};
};

// Manages a circular ring of write_buffers.
//
// Writers append to the head buffer.  A single consumer coroutine drains the
// tail.  The head advances when the current head buffer is full (can't fit the
// next write) or when the consumer seals it.  Writers wait if the ring is full
// (all buffers are pending flush).
class buffered_writer {
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
        seastar::promise<buffered_write_result> accepted_pr; // written to buffer
        seastar::promise<written_record> persisted_pr; // written to a segment
        log_record_writer writer;
        write_target target;
        // Keeps the writer from finishing its stop() while the record is on the queue. It is held
        // by the request rather than by whoever waits for it, so that queueing a write needs no
        // coroutine of its own.
        seastar::gate::holder async_gate_holder;
        db::timeout_clock::time_point timeout;
        uint64_t id;
        size_t write_size;

        queued_write(log_record_writer writer, write_target target, seastar::gate::holder async_gate_holder,
                db::timeout_clock::time_point timeout, uint64_t id, size_t write_size)
            : writer(std::move(writer))
            , target(std::move(target))
            , async_gate_holder(std::move(async_gate_holder))
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
        buffered_writer* owner{};

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

    seastar::timer<db::timeout_clock> _head_flush_timer;
    bool _head_deadline_expired = false;

    // Monotonically increasing id assigned to queued writes. flush() snapshots
    // this counter and waits until all queued writes with lower ids are gone.
    uint64_t _next_queued_write_id = 0;

    seastar::gate _async_gate;

    // The single flush-consumer fiber, running for the lifetime of the writer.
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

    std::optional<future<written_record>> append_to_head_buffer(log_record_writer&, write_target&);

    // Puts a record that found no room in the ring on the queue of writes waiting for some, and
    // waits there for it to be taken into a buffer.
    future<buffered_write_result> queue_write(log_record_writer, db::timeout_clock::time_point timeout, write_target,
            seastar::gate::holder);

    bool try_dispatch_next_buffer();
    future<> run_dispatched_write(size_t idx);
    future<bool> reclaim_completed_tails();

    future<bool> drain_queued_writes();

    void on_queued_write_removed(const queued_write&) noexcept;
    void fail_queued_write(queued_write&, std::exception_ptr) noexcept;
    void on_queued_writes_changed() noexcept;

    void arm_head_flush_timer();
    void on_head_flush_timer() noexcept;
    void cancel_head_flush_timer() noexcept;

    future<> consumer_loop();

public:
    explicit buffered_writer(buffered_writer_config cfg,
            seastar::noncopyable_function<future<>(mixed_write_buffer&)> flush_func);

    buffered_writer(const buffered_writer&) = delete;
    buffered_writer& operator=(const buffered_writer&) = delete;

    future<> start();
    future<> stop();
    future<> flush();

    // Takes a record. The future it returns resolves once the record is in a buffer, and carries a
    // second future that resolves once that buffer is in a segment, which is where the record's
    // location comes from.
    future<buffered_write_result> write_to_buffer(log_record_writer, db::timeout_clock::time_point timeout, write_target target = {}) noexcept;

    size_t queued_write_count() const noexcept { return _queued_writes.size(); }

    size_t memory_usage() const noexcept { return _ring_size * _buffer_size + _queued_write_bytes; }

};

}
}
