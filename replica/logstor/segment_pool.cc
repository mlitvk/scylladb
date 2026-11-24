/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/segment_pool.hh"

#include <algorithm>
#include <stdexcept>

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/try_future.hh>
#include <seastar/util/log.hh>
#include <seastar/util/optimized_optional.hh>

#include "utils/exceptions.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

namespace {

// Returns the wait for the next signal of `cv`. The condition variable allocates the waiter in a
// noexcept function, and the refill and the allocator cannot handle a failed allocation of the
// waiter. So the allocation is critical, and the allocation failure injector does not fail it.
future<> wait_for_signal(seastar::condition_variable& cv) noexcept {
    memory::scoped_critical_alloc_section dfg;
    return cv.wait();
}

// Returns the same wait as wait_for_signal(cv), which fails with condition_variable_timed_out at
// `timeout`.
future<> wait_for_signal(seastar::condition_variable& cv, timer<>::time_point timeout) noexcept {
    memory::scoped_critical_alloc_section dfg;
    return cv.wait(timeout);
}

} // anonymous namespace

// The allocator goes to the next new segment only after the current new segment is allocated, so
// after a failure the next attempt tries the same segment again. A skipped segment can be the first
// segment of a file, and then the allocation of the next file fails.
// The file of the new segment can be unavailable for a while, because its format failed or is not
// done. Then this uses a free segment if there is one, so that the store continues with the files
// it has, and waits for the format only if there is no free segment.
future<segment_allocator::allocate_result> segment_allocator::allocate() {
    while (true) {
        const log_segment_id new_seg_id(_next_new_segment_id);
        const bool allocate_new = new_count() > 0 && (_free_segments.empty() || _opener.is_ready(new_seg_id));
        if (allocate_new) {
            auto seg_f = co_await coroutine::as_future(futurize_invoke([this, new_seg_id] { return _opener.open(new_seg_id); }));
            if (!seg_f.failed()) {
                ++_next_new_segment_id;
                co_return seg_f.get();
            }
            auto ex = seg_f.get_exception();
            if (_free_segments.empty()) {
                co_await coroutine::return_exception_ptr(std::move(ex));
            }
            static thread_local logger::rate_limit new_segment_failure_rate_limit(std::chrono::seconds(10));
            logstor_logger.log(log_level::warn, new_segment_failure_rate_limit, "Cannot allocate the new {}: {:t}. Using a free segment", new_seg_id, ex);
        }

        if (!_free_segments.empty()) {
            // If the allocation fails, the segment goes back to the free list. futurize_invoke()
            // turns a failed allocation of the frame of open() into a failed future, so that the
            // segment goes back in that case too.
            auto seg_id = _free_segments.front();
            _free_segments.pop_front();
            auto seg_f = co_await coroutine::as_future(futurize_invoke([this, seg_id] { return _opener.open(seg_id); }));
            if (seg_f.failed()) {
                _free_segments.push_back(seg_id);
                co_await coroutine::return_exception_ptr(seg_f.get_exception());
            }
            co_return seg_f.get();
        }

        if (co_await wait_for_free_segment()) {
            co_return stopped{};
        }
    }
}

// Waits for a freed segment. The usual result is the timeout, so the wait returns the timeout as a
// value and does not throw.
future<bool> segment_allocator::wait_for_free_segment() {
    const auto timeout = timer<>::clock::now() + free_segment_wait_timeout;
    while (!_stop_waiting && _free_segments.empty()) {
        auto f = co_await coroutine::as_future(wait_for_signal(_segment_freed, timeout));
        if (f.failed()) {
            auto ep = f.get_exception();
            if (!try_catch<seastar::condition_variable_timed_out>(ep)) {
                co_await coroutine::return_exception_ptr(std::move(ep));
            }
            break;
        }
    }
    co_return _stop_waiting;
}

void segment_pool::start(scheduling_group sg) {
    _refill = with_scheduling_group(sg, [this] {
        return refill();
    });
}

future<> segment_pool::stop() {
    _stopping = true;
    _refill_needed.signal();
    _allocator.stop_waiting();
    co_await std::move(_refill);
    if (std::ranges::any_of(_waiters, [] (const waiter_list& waiters) { return !waiters.empty(); })) [[unlikely]] {
        on_internal_error_noexcept(logstor_logger, "A consumer waits for a segment while the segment pool stops");
        fail_waiters(std::make_exception_ptr(std::runtime_error("The segment pool is stopped")));
    }
    _segments.clear();
}

future<seg_ptr> segment_pool::take(write_source src, abort_source* as) noexcept {
    try {
        if (_stopping) [[unlikely]] {
            on_internal_error(logstor_logger, "Cannot take a segment from a stopped segment pool");
        }
        const auto t = tier_of(src);
        if (_waiters[static_cast<size_t>(t)].empty() && can_take(t)) {
            return make_ready_future<seg_ptr>(pop(src));
        }
        _stats.segments_wait[static_cast<size_t>(src)]++;
        return wait_for_segment(src, as);
    } catch (...) {
        return current_exception_as_future<seg_ptr>();
    }
}

// Each wait of the refill also ends when the pool stops. Thus the refill stops after the allocation
// that it does at that time, and does not start a new one.
future<> segment_pool::refill() {
    while (true) {
        while (!_stopping && full()) {
            co_await wait_for_signal(_refill_needed);
        }
        if (_stopping) {
            break;
        }

        // futurize_invoke() turns a failed allocation of the frame of allocate() into a failed
        // future, so that the refill tries again.
        auto seg_f = co_await coroutine::as_future(futurize_invoke([this] { return _allocator.allocate(); }));
        if (seg_f.failed()) {
            auto ex = seg_f.get_exception();
            ++_stats.refill_failures;
            static thread_local logger::rate_limit refill_failure_rate_limit(std::chrono::seconds(10));
            logstor_logger.log(log_level::warn, refill_failure_rate_limit, "Cannot refill the segment pool: {:t}. Trying again", ex);
            fail_waiters(std::move(ex));
            // The usual result of the wait is the timeout, and the loop continues after any result,
            // so ignore it.
            const auto retry_time = timer<>::clock::now() + refill_retry_delay;
            while (!_stopping) {
                auto wait_f = co_await coroutine::as_future(wait_for_signal(_refill_needed, retry_time));
                if (wait_f.failed()) {
                    wait_f.ignore_ready_future();
                    break;
                }
            }
            continue;
        }

        auto result = seg_f.get();
        auto* seg = std::get_if<seg_ptr>(&result);
        if (!seg) {
            break;
        }
        put(std::move(*seg));
    }

    logstor_logger.debug("Segment pool refill stopped");
}

void segment_pool::put(seg_ptr seg) {
    if (full()) [[unlikely]] {
        on_internal_error(logstor_logger, "Attempted to put a segment into a full segment pool");
    }
    _segments.push_back(std::move(seg));
    _stats.segments_put++;
    serve_waiters();
}

void segment_pool::serve_waiters() noexcept {
    for (size_t i = 0; i < tier_count; ++i) {
        auto& waiters = _waiters[i];
        while (!waiters.empty() && can_take(tier(i))) {
            auto& w = waiters.front();
            waiters.pop_front();
            w.pr.set_value(pop(w.src));
        }
    }
}

seg_ptr segment_pool::pop(write_source src) noexcept {
    _stats.segments_taken[static_cast<size_t>(src)]++;
    auto seg = std::move(_segments.front());
    _segments.pop_front();
    _refill_needed.signal();
    return seg;
}

future<seg_ptr> segment_pool::wait_for_segment(write_source src, abort_source* as) {
    auto& waiters = _waiters[static_cast<size_t>(tier_of(src))];
    waiter w(src);
    auto f = w.pr.get_future();
    seastar::optimized_optional<abort_source::subscription> abort_subscription;
    if (as) {
        abort_subscription = as->subscribe([&waiters, &w] () noexcept {
            if (w.is_linked()) {
                waiters.erase(waiters.iterator_to(w));
                w.pr.set_exception(abort_requested_exception());
            }
        });
        if (!abort_subscription) {
            co_await coroutine::return_exception(abort_requested_exception());
        }
    }
    waiters.push_back(w);
    co_return co_await coroutine::try_future(std::move(f));
}

void segment_pool::fail_waiters(std::exception_ptr ex) noexcept {
    for (auto& waiters : _waiters) {
        while (!waiters.empty()) {
            auto& w = waiters.front();
            waiters.pop_front();
            w.pr.set_exception(ex);
        }
    }
}

} // namespace replica::logstor
