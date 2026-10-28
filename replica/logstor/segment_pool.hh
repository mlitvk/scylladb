/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <string_view>
#include <variant>

#include <boost/intrusive/list.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/try_future.hh>
#include <seastar/util/log.hh>
#include <seastar/util/optimized_optional.hh>

#include "replica/logstor/segment_types.hh"
#include "seastarx.hh"
#include "utils/exceptions.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

// Names the consumer that takes a segment from the segment pool and writes to it.
enum class write_source {
    normal,
    compaction,
    separator,
    streaming,
};

inline constexpr size_t write_source_count = 4;

inline std::string_view write_source_to_string(write_source src) noexcept {
    switch (src) {
        case write_source::normal: return "normal";
        case write_source::compaction: return "compaction";
        case write_source::separator: return "separator";
        case write_source::streaming: return "streaming";
    }
    return "unknown";
}

// Chooses the segment that the segment pool allocates next: a new segment while there are
// configured segments that were never allocated, else a free segment, else it waits for a segment
// to be freed. Only the refill of the pool allocates, so one allocate() runs at a time.
//
// `Segment` is the type of an open segment. The allocator and the pool hold segments, and do not
// use them, so a test gives them a segment type of its own.
template <typename Segment>
class basic_segment_allocator {
public:
    using seg_ptr = lw_shared_ptr<Segment>;

    // The allocator uses this interface to check and open segments.
    class source {
    public:
        virtual ~source() = default;
        // Returns true if open() can open the segment without a wait for the format of its file.
        virtual bool is_ready(log_segment_id) = 0;
        // Opens a segment for writes, and waits for the format of the file of a new segment. It can
        // fail, for example when the file cannot be allocated.
        virtual future<seg_ptr> open(log_segment_id) = 0;
    };

    // allocate() returns this when stop_waiting() ends its wait for a free segment.
    struct stopped {};
    using allocate_result = std::variant<seg_ptr, stopped>;

private:
    // Compaction can free segments later, and the format of a file can complete.
    // Thus a wait for a free segment ends after this timeout, and allocate() tries again.
    static constexpr auto free_segment_wait_timeout = std::chrono::seconds(5);

    const uint64_t _configured_segment_count;
    uint64_t _next_new_segment_id{0};
    // Holds only segments below the configured count, and the constructor reserves room for all of
    // them.
    seastar::circular_buffer<log_segment_id> _free_segments;
    seastar::condition_variable _segment_freed;
    bool _stop_waiting{false};
    source& _source;

public:
    basic_segment_allocator(uint64_t configured_segment_count, source& src)
        : _configured_segment_count(configured_segment_count)
        , _source(src)
    {
        _free_segments.reserve(static_cast<size_t>(_configured_segment_count));
    }

    future<allocate_result> allocate();

    // Puts the segment on the free list, and returns true. A segment at or above the configured
    // count is retired, and this returns false.
    bool free(log_segment_id segment_id) noexcept {
        if (segment_id.value >= _configured_segment_count) {
            return false;
        }
        // The free list has room for every configured segment, so this does not allocate.
        _free_segments.push_back(segment_id);
        _segment_freed.signal();
        return true;
    }

    // Sets the count of segments that recovery found. These segments are not new, so the next new
    // segment comes after them.
    void set_found_segment_count(uint64_t found_segment_count) noexcept {
        _next_new_segment_id = found_segment_count;
    }

    // Ends the wait of allocate() for a free segment, and the waits after it.
    void stop_waiting() noexcept {
        _stop_waiting = true;
        _segment_freed.signal();
    }

    uint64_t free_count() const noexcept {
        return _free_segments.size();
    }

    // Returns the count of configured segments that were never allocated.
    uint64_t new_count() const noexcept {
        return _next_new_segment_id < _configured_segment_count ? _configured_segment_count - _next_new_segment_id : 0;
    }

    // Returns the count of segments that the allocator can still give: the free ones and the new
    // ones.
    uint64_t available_count() const noexcept {
        return free_count() + new_count();
    }

private:
    // Returns true if stop_waiting() ended the wait.
    future<bool> wait_for_free_segment();
};

// Allocates the next segment. The allocator goes to the next new segment only after the current new
// segment is allocated, so after a failure the next attempt tries the same segment again. A skipped
// segment can be the first segment of a file, and then the allocation of the next file fails. The
// file of the new segment can be unavailable for a while, because its format failed or is not done.
// Then this uses a free segment if there is one, so that the store continues with the files it has,
// and waits for the format only if there is no free segment.
template <typename Segment>
future<typename basic_segment_allocator<Segment>::allocate_result> basic_segment_allocator<Segment>::allocate() {
    while (true) {
        const log_segment_id new_seg_id(_next_new_segment_id);
        const bool allocate_new = new_count() > 0 && (_free_segments.empty() || _source.is_ready(new_seg_id));
        if (allocate_new) {
            auto seg_f = co_await coroutine::as_future(_source.open(new_seg_id));
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
            // If the allocation fails, the segment goes back to the free list.
            auto seg_id = _free_segments.front();
            _free_segments.pop_front();
            auto seg_f = co_await coroutine::as_future(_source.open(seg_id));
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
template <typename Segment>
future<bool> basic_segment_allocator<Segment>::wait_for_free_segment() {
    auto f = co_await coroutine::as_future(_segment_freed.wait(free_segment_wait_timeout, [this] {
        return _stop_waiting || !_free_segments.empty();
    }));
    if (f.failed()) {
        auto ep = f.get_exception();
        if (!try_catch<seastar::condition_variable_timed_out>(ep)) {
            co_await coroutine::return_exception_ptr(std::move(ep));
        }
    }
    co_return _stop_waiting;
}

// Holds the segments that are ready for writes. The pool refills itself in the background from
// the segment allocator, and keeps itself full. The last segments in the pool are the compaction
// reserve, and only compaction takes them, because compaction must always get a segment to free
// segments. The segments above the compaction reserve are the separator reserve, which normal
// writes and streaming do not take (see separator_segment_reserve). A consumer that cannot take a
// segment waits in the queue of its tier, and a new segment goes to the first consumer that can
// take it, compaction first and then the separator. A consumer that waits fails when an attempt to
// refill the pool fails, so that a write does not wait for a segment that possibly does not come. A
// compaction that fails this way runs again later.
template <typename Segment>
class basic_segment_pool {
public:
    using seg_ptr = lw_shared_ptr<Segment>;
    using allocator_type = basic_segment_allocator<Segment>;

    struct stats {
        uint64_t segments_put{0};
        std::array<uint64_t, write_source_count> segments_taken{0};
        std::array<uint64_t, write_source_count> segments_wait{0};
        uint64_t refill_failures{0};
    };

private:
    // After a failed refill, the next attempt comes after this delay.
    static constexpr auto refill_retry_delay = std::chrono::seconds(1);

    allocator_type& _allocator;
    seastar::circular_buffer<seg_ptr> _segments;
    size_t _capacity;
    size_t _reserved_for_compaction;
    size_t _reserved_for_separator;

    // A tier groups the consumers that take segments under the same reserve. The pool serves the
    // tiers in the order of this enum.
    enum class tier : size_t { compaction, separator, other };
    static constexpr size_t tier_count = 3;

    // A waiter is a consumer that waits for a segment. It lives in the frame of wait_for_segment(),
    // and it is in the queue of its tier until its promise is set. The hook is in safe_link mode,
    // so is_linked() is valid.
    struct waiter : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::safe_link>> {
        write_source src;
        promise<seg_ptr> pr;

        explicit waiter(write_source src) noexcept : src(src) {}
    };
    using waiter_list = boost::intrusive::list<waiter, boost::intrusive::constant_time_size<false>>;

    // Holds one queue per tier of the consumers that wait for a segment, in arrival order. No
    // consumer waits in a tier while the pool holds more segments than the reserve of the tier.
    std::array<waiter_list, tier_count> _waiters;

    future<> _refill{make_ready_future<>()};
    bool _stopping{false};
    // Wakes the refill. Signal it when a segment goes out of the pool, and when the pool stops.
    seastar::condition_variable _refill_needed;

    stats _stats;

    static tier tier_of(write_source src) noexcept {
        switch (src) {
        case write_source::compaction: return tier::compaction;
        case write_source::separator: return tier::separator;
        case write_source::normal:
        case write_source::streaming: return tier::other;
        }
        return tier::other;
    }

    // Returns the count of segments that a consumer of this tier must leave in the pool.
    size_t reserve_of(tier t) const noexcept {
        switch (t) {
        case tier::compaction: return 0;
        case tier::separator: return _reserved_for_compaction;
        case tier::other: return _reserved_for_compaction + _reserved_for_separator;
        }
        return _reserved_for_compaction + _reserved_for_separator;
    }

    seg_ptr pop(write_source src) noexcept {
        _stats.segments_taken[static_cast<size_t>(src)]++;
        auto seg = std::move(_segments.front());
        _segments.pop_front();
        _refill_needed.signal();
        return seg;
    }

    // Gives the segments to the waiting consumers that can take them, in tier order.
    void serve_waiters() noexcept {
        for (size_t i = 0; i < tier_count; ++i) {
            auto& waiters = _waiters[i];
            while (!waiters.empty() && _segments.size() > reserve_of(tier(i))) {
                auto& w = waiters.front();
                waiters.pop_front();
                w.pr.set_value(pop(w.src));
            }
        }
    }

    bool full() const noexcept {
        return _segments.size() >= _capacity;
    }

    future<> refill();

    void put(seg_ptr seg) {
        if (full()) [[unlikely]] {
            on_internal_error(logstor_logger, "Attempted to put a segment into a full segment pool");
        }
        _segments.push_back(std::move(seg));
        _stats.segments_put++;
        serve_waiters();
    }

    // Fails each consumer that waits for a segment now. The refill calls this when it cannot
    // allocate a segment, and a consumer that starts to wait after this call waits for the next
    // attempt.
    void fail_waiters(std::exception_ptr ex) noexcept {
        for (auto& waiters : _waiters) {
            while (!waiters.empty()) {
                auto& w = waiters.front();
                waiters.pop_front();
                w.pr.set_exception(ex);
            }
        }
    }

    static std::exception_ptr stopped_error() {
        return std::make_exception_ptr(std::runtime_error("The segment pool is stopped"));
    }

    future<seg_ptr> wait_for_segment(write_source src, abort_source* as) {
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

public:
    // The pool refers to the allocator, which must outlive it.
    basic_segment_pool(size_t pool_size, size_t reserved_for_compaction, size_t reserved_for_separator, allocator_type& allocator)
        : _allocator(allocator)
        , _capacity(pool_size)
        , _reserved_for_compaction(reserved_for_compaction)
        , _reserved_for_separator(reserved_for_separator)
    {}

    // Returns the count of segments that a consumer of this source must leave in the pool.
    size_t reserve_for(write_source src) const noexcept {
        return reserve_of(tier_of(src));
    }

    // Starts the refill in the background.
    void start(scheduling_group sg) {
        _refill = with_scheduling_group(sg, [this] {
            return refill();
        });
    }

    // Stops the refill after the allocation that the refill does at this time. Call this after all
    // the consumers stop. A call to take() after stop() starts, or a wait that remains when the
    // refill stops, is a bug in the caller, and it fails and does not wait forever.
    future<> stop() {
        _stopping = true;
        _refill_needed.signal();
        _allocator.stop_waiting();
        co_await std::move(_refill);
        fail_waiters(stopped_error());
        _segments.clear();
    }

    // Takes a segment, or waits for one that this source can take. If the caller gives `as` and
    // `as` is aborted, the wait ends and take() fails with abort_requested_exception.
    future<seg_ptr> take(write_source src, abort_source* as = nullptr) noexcept {
        try {
            if (_stopping) [[unlikely]] {
                return make_exception_future<seg_ptr>(stopped_error());
            }
            const auto t = tier_of(src);
            if (_waiters[static_cast<size_t>(t)].empty() && _segments.size() > reserve_of(t)) {
                return make_ready_future<seg_ptr>(pop(src));
            }
            _stats.segments_wait[static_cast<size_t>(src)]++;
            return wait_for_segment(src, as);
        } catch (...) {
            return current_exception_as_future<seg_ptr>();
        }
    }

    // Returns the count of segments that a consumer of this source can take now.
    size_t available_segment_count(write_source src) const noexcept {
        const size_t reserved = reserve_of(tier_of(src));
        return _segments.size() > reserved ? _segments.size() - reserved : 0;
    }

    // Returns the count of segments in the pool, including the reserves.
    size_t size() const noexcept {
        return _segments.size();
    }

    const stats& get_stats() const noexcept {
        return _stats;
    }
};

// Each wait of the refill also ends when the pool stops. Thus the refill stops after the allocation
// that it does at that time, and does not start a new one.
template <typename Segment>
future<> basic_segment_pool<Segment>::refill() {
    while (true) {
        co_await _refill_needed.wait([this] { return _stopping || !full(); });
        if (_stopping) {
            break;
        }

        auto seg_f = co_await coroutine::as_future(_allocator.allocate());
        if (seg_f.failed()) {
            auto ex = seg_f.get_exception();
            ++_stats.refill_failures;
            static thread_local logger::rate_limit refill_failure_rate_limit(std::chrono::seconds(10));
            logstor_logger.log(log_level::warn, refill_failure_rate_limit, "Cannot refill the segment pool: {:t}. Trying again", ex);
            fail_waiters(std::move(ex));
            // The usual result of the wait is the timeout, and the loop continues after any result,
            // so ignore it.
            auto wait_f = co_await coroutine::as_future(_refill_needed.wait(refill_retry_delay, [this] {
                return _stopping;
            }));
            wait_f.ignore_ready_future();
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

} // namespace replica::logstor
