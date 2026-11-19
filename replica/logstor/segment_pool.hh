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
#include <string_view>
#include <variant>

#include <boost/intrusive/list.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>

#include "replica/logstor/segment_file.hh"
#include "replica/logstor/segment_types.hh"
#include "seastarx.hh"

namespace replica::logstor {

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
// to be freed.
class segment_allocator {
public:
    // The allocator uses this interface to check and open segments.
    class segment_opener {
    public:
        virtual ~segment_opener() = default;
        // Returns true if open() can open the segment without a wait for the format of its file.
        virtual bool is_ready(log_segment_id) = 0;
        // Opens a segment for writes, and waits for the format of the file of a new segment. It can
        // fail, for example when the file cannot be allocated.
        virtual future<seg_ptr> open(log_segment_id) = 0;
    };

    // allocate() returns this when stop_waiting() ends its wait for a free segment.
    struct stopped {};
    using allocate_result = std::variant<seg_ptr, stopped>;

    segment_allocator(uint64_t configured_segment_count, segment_opener& opener)
        : _configured_segment_count(configured_segment_count)
        , _opener(opener)
    {
        _free_segments.reserve(static_cast<size_t>(_configured_segment_count));
    }

    // Sets the count of segments that recovery found. These segments are not new, so the next new
    // segment comes after them.
    void set_found_segment_count(uint64_t found_segment_count) noexcept {
        _next_new_segment_id = found_segment_count;
    }

    // Returns the next segment in the order of the class comment, opened for writes. Returns
    // `stopped` when stop_waiting() ends the wait for a free segment.
    // Fails with the error of segment_opener::open() when the new segment fails and there is no
    // free segment, or when a free segment fails, which then goes back to the free list.
    // The caller must not call allocate() again before the previous call ends.
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
    segment_opener& _opener;

    // Returns true if stop_waiting() ended the wait.
    future<bool> wait_for_free_segment();
};

// Holds the segments that are ready for writes. The pool refills itself in the background from
// the segment allocator, and keeps itself full.
// The last segments in the pool are the compaction reserve, and only compaction takes them, because
// compaction must always get a segment to free segments. The segments above the compaction reserve
// are the separator reserve, which normal writes and streaming do not take (see
// separator_segment_reserve).
// A consumer that cannot take a segment waits in the queue of its tier. Each segment that the
// refill puts in the pool goes to the first consumer that can take it, compaction first and then
// the separator.
// A consumer that waits fails when an attempt to refill the pool fails, so that a write does not
// wait for a segment that possibly does not come.
class segment_pool {
public:
    struct config {
        size_t capacity;
        size_t compaction_reserve = 0;
        size_t separator_reserve = 0;
    };

    struct stats {
        uint64_t segments_put{0};
        std::array<uint64_t, write_source_count> segments_taken{0};
        std::array<uint64_t, write_source_count> segments_wait{0};
        uint64_t refill_failures{0};
    };

    // The pool refers to the allocator, which must outlive it.
    segment_pool(config cfg, segment_allocator& allocator)
        : _allocator(allocator)
        , _cfg(cfg)
    {
        _segments.reserve(_cfg.capacity);
    }

    // Starts the refill in the background.
    void start(scheduling_group sg);

    // Stops the refill after the allocation that the refill does at this time.
    // The caller must stop all the consumers first. A take() after stop() starts, or a wait that
    // remains when the refill stops, is an internal error, and it fails and does not wait forever.
    future<> stop();

    // Takes a segment, or waits for one that this source can take. If the caller gives `as` and
    // `as` is aborted, the wait ends and take() fails with abort_requested_exception.
    future<seg_ptr> take(write_source src, abort_source* as = nullptr) noexcept;

    // Returns the count of segments that a consumer of this source must leave in the pool.
    size_t reserve_for(write_source src) const noexcept {
        return reserve_of(tier_of(src));
    }

    // Returns the count of available segments: the segments in the pool, the free segments and the
    // segments that were never allocated.
    uint64_t available_segment_count() const noexcept {
        return _allocator.free_count() + _allocator.new_count() + _segments.size();
    }

    // Returns the count of available segments that a consumer of this source can use, which does
    // not include the reserve of the source.
    uint64_t available_segment_count(write_source src) const noexcept {
        const size_t reserve = reserve_for(src);
        const size_t above_reserve = _segments.size() > reserve ? _segments.size() - reserve : 0;
        return _allocator.free_count() + _allocator.new_count() + above_reserve;
    }

    // Returns the count of segments in the pool, including the reserves.
    size_t size() const noexcept {
        return _segments.size();
    }

    const stats& get_stats() const noexcept {
        return _stats;
    }

private:
    // After a failed refill, the next attempt comes after this delay.
    static constexpr auto refill_retry_delay = std::chrono::seconds(1);

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

    segment_allocator& _allocator;
    const config _cfg;
    seastar::circular_buffer<seg_ptr> _segments;

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
        case tier::separator: return _cfg.compaction_reserve;
        case tier::other: return _cfg.compaction_reserve + _cfg.separator_reserve;
        }
        return _cfg.compaction_reserve + _cfg.separator_reserve;
    }

    bool can_take(tier t) const noexcept {
        return _segments.size() > reserve_of(t);
    }

    bool full() const noexcept {
        return _segments.size() >= _cfg.capacity;
    }

    future<> refill();

    void put(seg_ptr seg);

    // Gives the segments to the waiting consumers that can take them, in tier order.
    void serve_waiters() noexcept;

    seg_ptr pop(write_source src) noexcept;

    future<seg_ptr> wait_for_segment(write_source src, abort_source* as);

    // Fails each consumer that waits for a segment now. A consumer that starts to wait after this
    // call is not failed.
    void fail_waiters(std::exception_ptr ex) noexcept;
};

} // namespace replica::logstor
