/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/write_buffer_ring.hh"

#include <chrono>

#include <seastar/core/on_internal_error.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>

#include "replica/exceptions.hh"
#include "replica/logstor/logstor.hh"

namespace replica::logstor {

write_buffer_ring::write_buffer_ring(write_buffer_ring_config cfg, seastar::noncopyable_function<future<>(mixed_write_buffer&)> flush_func)
        : _flush_sg(cfg.flush_sg)
        , _buffer_size(cfg.buffer_size)
        , _ring_size(cfg.ring_size)
        , _sync_period(cfg.sync_period)
        , _flush_func(std::move(flush_func))
        , _ring()
        , _in_flight(cfg.ring_size)
        , _queued_writes(on_queued_write_expiry{this})
        , _max_queued_write_bytes(cfg.max_queued_write_bytes)
        , _sync_timer([this] { on_sync_timer(); }) {

    if (_ring_size < 2) {
        on_internal_error(logstor_logger, fmt::format("write_buffer_ring ring_size must be >= 2 (got {})", _ring_size));
    }

    if (_buffer_size == 0) {
        on_internal_error(logstor_logger, "write_buffer_ring buffer_size must be > 0");
    }

    _ring.reserve(_ring_size);
    for (size_t i = 0; i < _ring_size; ++i) {
        _ring.emplace_back(_buffer_size);
    }
}

bool write_buffer_ring::has_pending_buffers() const noexcept {
    return _tail < _head || head_buf().has_data();
}

bool write_buffer_ring::should_advance_head() const noexcept {
    return _dispatch_tail == _head && head_buf().has_data() && (_async_gate.is_closed() || _sync_deadline_expired);
}

void write_buffer_ring::arm_sync_timer() {
    if (_sync_deadline_expired || _sync_timer.armed()) {
        return;
    }
    if (_sync_period <= std::chrono::milliseconds(0)) {
        on_sync_timer();
        return;
    }
    _sync_timer.arm(db::timeout_clock::now() + _sync_period);
}

void write_buffer_ring::on_sync_timer() noexcept {
    _sync_deadline_expired = true;
    _consumer_progress_cv.signal();
}

void write_buffer_ring::cancel_sync_timer() noexcept {
    _sync_timer.cancel();
    _sync_deadline_expired = false;
}

void write_buffer_ring::on_queued_writes_changed() noexcept {
    _queued_writes_changed.broadcast();
}

void write_buffer_ring::on_queued_write_removed(const queued_write& w) noexcept {
    _queued_write_bytes -= w.write_size;
}

// Fails only the accepted promise. The future of the persisted promise is taken only when the write
// is accepted, so a failed write never has one, and a promise that holds an exception and has no
// future reports the exception as ignored when it is destroyed.
void write_buffer_ring::fail_queued_write(queued_write& request, std::exception_ptr ep) noexcept {
    request.accepted_pr.set_exception(std::move(ep));
}

bool write_buffer_ring::maybe_advance_head() noexcept {
    if (ring_full() || !head_buf().has_data()) {
        return false;
    }
    cancel_sync_timer();
    ++_head;
    _consumer_progress_cv.signal();
    return true;
}

// The target is taken by reference and moved from only once the record is in a buffer: a caller
// whose append does not happen goes on to queue the record, and the write target it queues has to
// be the one it came with, holders and all.
std::optional<future<written_record>> write_buffer_ring::append_to_head_buffer(log_record_writer& writer, write_target& target) {
    if (!head_buf().can_fit(writer) && !maybe_advance_head()) {
        return std::nullopt;
    }

    bool was_empty = !head_buf().has_data();
    auto persisted = head_buf().write(std::move(writer), std::move(target));
    if (was_empty) {
        arm_sync_timer();
        _consumer_progress_cv.signal();
    }
    return persisted;
}

bool write_buffer_ring::try_dispatch_next_buffer() {
    auto idx = _dispatch_tail;
    auto& state = dispatch_tail_write();
    auto& buf = _ring[idx % _ring_size];

    if (_dispatch_tail >= _head || !state.idle() || !buf.has_data()) {
        return false;
    }

    try {
        state.start(run_dispatched_write(idx));
    } catch (...) {
        state.start(make_exception_future<>(std::current_exception()));
    }
    ++_dispatch_tail;
    return true;
}

future<> write_buffer_ring::run_dispatched_write(size_t idx) {
    auto& buf = _ring[idx % _ring_size];
    auto f = co_await coroutine::as_future(_flush_func(buf));
    _consumer_progress_cv.signal();
    if (f.failed()) {
        co_await coroutine::return_exception_ptr(f.get_exception());
    }
}

future<bool> write_buffer_ring::reclaim_completed_tails() {
    bool reclaimed = false;
    while (_tail < _dispatch_tail) {
        auto& state = tail_write();
        if (!state.ready()) {
            break;
        }

        auto completion = state.take_completion();
        auto f = co_await coroutine::as_future(std::move(completion));

        if (f.failed()) {
            auto ex = f.get_exception();
            auto abort_f = co_await coroutine::as_future(tail_buf().abort_writes(std::move(ex)));
            if (abort_f.failed()) {
                logstor_logger.warn("Failed to abort the writes of a buffer whose flush failed: {}. Ignoring.", abort_f.get_exception());
            }
        }

        tail_buf().reset();
        state.reset();
        ++_tail;
        reclaimed = true;
        _tail_advanced.broadcast();

        co_await coroutine::maybe_yield();
    }

    co_return reclaimed;
}

future<bool> write_buffer_ring::drain_queued_writes() {
    bool removed_queued_writes = false;
    while (!_queued_writes.empty()) {
        auto& next = _queued_writes.front();

        if (!head_buf().can_fit(next.writer) && !maybe_advance_head()) {
            break;
        }

        auto request = std::move(next);
        _queued_writes.pop_front();
        on_queued_write_removed(request);
        removed_queued_writes = true;
        try {
            auto persisted = append_to_head_buffer(request.writer, request.target).value();
            request.accepted_pr.set_value(accepted_write{request.persisted_pr.get_future()});
            std::move(persisted).forward_to(std::move(request.persisted_pr));
        } catch (...) {
            fail_queued_write(request, std::current_exception());
        }
        co_await coroutine::maybe_yield();
    }

    if (removed_queued_writes) {
        on_queued_writes_changed();
    }

    co_return removed_queued_writes;
}

future<> write_buffer_ring::start() {
    logstor_logger.info("Starting write buffer ring");
    _consumer = with_scheduling_group(_flush_sg, [this] {
        return consumer_loop();
    });
    co_return;
}

future<> write_buffer_ring::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping write buffer ring");

    cancel_sync_timer();

    // Wake the consumer so it can observe the closing gate and exit.
    auto close_fut = _async_gate.close();
    _consumer_progress_cv.broadcast();
    co_await std::move(close_fut);
    co_await std::move(_consumer);

    for (auto& state : _in_flight) {
        if (state.completion) {
            auto f = co_await coroutine::as_future(std::move(*state.completion));
            state.completion.reset();
            if (f.failed()) {
                logstor_logger.warn("A buffer flush failed during stop: {}. Ignoring.", f.get_exception());
            }
        }
    }

    logstor_logger.info("Write buffer ring stopped");
}

future<> write_buffer_ring::flush() {
    auto holder = _async_gate.hold();

    // Snapshot the queue insertion boundary. New writes pushed after this point
    // get higher ids and are not our responsibility.
    const uint64_t target_next_id = _next_queued_write_id;

    auto preflush_queued_writes_gone = [this, target_next_id] {
        return _queued_writes.empty() || _queued_writes.front().id >= target_next_id;
    };

    // Drain or let expire all writes that were queued when flush() was called.
    while (!preflush_queued_writes_gone()) {
        co_await drain_queued_writes();
        co_await _queued_writes_changed.wait(preflush_queued_writes_gone);
    }

    // If the current head buffer is non-empty, flush() must first move _head
    // past it so the consumer can dispatch that buffer. The head slot itself
    // is always the buffer that writers are still appending to, so as long as
    // _head stays unchanged that buffer is not yet eligible for dispatch.
    //
    // There are two cases:
    // 1. The head buffer is empty. There is nothing new to seal, so flush()
    //    only needs to wait for buffers that were already dispatched or are
    //    already behind _head.
    // 2. The head buffer is non-empty. flush() must rotate _head to the next
    //    ring slot. After that rotation, the old head becomes dispatchable and
    //    the new _head value is the tail boundary that proves the old head was
    //    fully flushed and reclaimed.
    auto head_before_forced_rotation = _head;
    while (_head == head_before_forced_rotation && head_buf().has_data()) {
        if (maybe_advance_head()) {
            break;
        }
        // Ring is full; wait for tail flush to free a slot.
        co_await _tail_advanced.wait();
    }

    // Wait until all buffers that flush() is responsible for are flushed and
    // reclaimed. If the head was empty, this is just the current _head. If the
    // head had data, this is the new post-rotation _head, which means the old
    // head buffer is now behind the tail.
    co_await _tail_advanced.when([this, flush_tail_goal = _head] {
        return _tail >= flush_tail_goal;
    });
}

// Not a coroutine, because a record that goes straight into the head buffer waits for nothing, and
// a coroutine would allocate a frame per write for no use. So the function gives each failure as a
// failed future, and the try costs nothing on the path that does not throw.
future<accepted_write> write_buffer_ring::write_to_buffer(log_record_writer writer, db::timeout_clock::time_point timeout, write_target target) noexcept {
    try {
        auto holder = _async_gate.hold();

        // The record has to fit the mixed buffer it goes into here and the full segment the separator
        // later rewrites it into, so it is bounded by whichever of the two takes less: one that only
        // fits the buffer it is written to first would be accepted here and then never fit anywhere the
        // separator could put it.
        const size_t max_size = chunk_layout::max_record_size_any_kind(head_buf().buffer_size(), writer.mapping());
        if (writer.record_size() > max_size) [[unlikely]] {
            return make_exception_future<accepted_write>(std::runtime_error(fmt::format(
                    "Write size {} exceeds the maximum record size {} of a {} byte buffer, whose schema table takes {} bytes for schema version {}",
                    writer.record_size(), max_size, head_buf().buffer_size(),
                    chunk_layout::single_version_schema_table_size(writer.mapping()), writer.header().schema_version)));
        }
        const size_t key_size = writer.header_size() - ondisk::record_header_fixed_size;
        if (key_size > ondisk::max_key_size) [[unlikely]] {
            return make_exception_future<accepted_write>(std::runtime_error(
                    fmt::format("Partition key size {} exceeds the maximum {}", key_size, ondisk::max_key_size)));
        }

        // fast path - if there are no queued writes and there is space in the current head buffer or the next, advance the
        // head buffer if needed and write to it.
        if (_queued_writes.empty()) {
            if (auto persisted = append_to_head_buffer(writer, target)) {
                return make_ready_future<accepted_write>(accepted_write{std::move(*persisted)});
            }
        }

        // either there are queued writes or there is no space in the head buffer and ring is full - queue the write.
        return queue_write(std::move(writer), timeout, std::move(target), std::move(holder));
    } catch (...) {
        return current_exception_as_future<accepted_write>();
    }
}

future<accepted_write> write_buffer_ring::queue_write(log_record_writer writer, db::timeout_clock::time_point timeout,
        write_target target, seastar::gate::holder holder) {
    if (_max_queued_write_bytes != 0 && _queued_write_bytes + writer.record_size() > _max_queued_write_bytes) {
        return make_exception_future<accepted_write>(replica::rate_limit_exception());
    }

    const bool queue_was_empty = _queued_writes.empty();
    const auto write_size = writer.record_size();
    queued_write request(std::move(writer), std::move(target), std::move(holder), timeout, _next_queued_write_id++, write_size);
    auto accepted = request.accepted_pr.get_future();
    _queued_write_bytes += write_size;
    _queued_writes.push_back(std::move(request), timeout);
    if (queue_was_empty) {
        _consumer_progress_cv.signal();
    }
    return accepted;
}

// Each pass reclaims the flushed buffers at the tail, moves queued writes into the free buffers,
// advances the head when its sync deadline ended or the ring stops, and dispatches the buffers
// behind the head. The loop waits for a signal when a pass makes no progress, and ends when the ring
// stops and holds no record.
future<> write_buffer_ring::consumer_loop() {
    try {
        while (true) {
            bool progressed = false;

            progressed |= co_await reclaim_completed_tails();

            progressed |= co_await drain_queued_writes();

            if (should_advance_head()) {
                progressed |= maybe_advance_head();
            }

            while (try_dispatch_next_buffer()) {
                progressed = true;
                co_await coroutine::maybe_yield();
            }

            if (_async_gate.is_closed() && _queued_writes.empty() && !has_pending_buffers()) {
                break;
            }

            if (!progressed) {
                co_await _consumer_progress_cv.wait();
            }

            co_await coroutine::maybe_yield();
        }
    } catch (...) {
        on_internal_error(logstor_logger, fmt::format("write_buffer_ring consumer loop aborted unexpectedly: {:t}", std::current_exception()));
    }
}

}
