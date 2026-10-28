/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <limits>
#include <stdexcept>
#include <variant>
#include <vector>

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "replica/logstor/segment_pool.hh"
#include "test/lib/eventually.hh"

using namespace replica::logstor;

namespace {

// The pool holds segments and does not use them, so a segment is only its id here.
struct test_segment {
    log_segment_id id;
    explicit test_segment(log_segment_id id) noexcept : id(id) {}
};

using seg_ptr = lw_shared_ptr<test_segment>;
using allocator = basic_segment_allocator<test_segment>;
using pool = basic_segment_pool<test_segment>;

// Gives segments over fake files, and records the segments that it opens.
struct test_source final : allocator::source {
    static constexpr uint64_t none = std::numeric_limits<uint64_t>::max();

    std::vector<log_segment_id> opens;
    size_t need_free_calls = 0;
    // A segment at or above this id has a file whose format is not done.
    uint64_t not_ready_from = none;
    // open() fails for a segment at or above this id.
    uint64_t fail_open_from = none;
    // open() fails for every segment.
    bool fail_open = false;

    bool is_ready(log_segment_id id) override {
        return id.value < not_ready_from;
    }

    future<seg_ptr> open(log_segment_id id) override {
        opens.push_back(id);
        if (fail_open || id.value >= fail_open_from) {
            return make_exception_future<seg_ptr>(std::runtime_error("open failed"));
        }
        return make_ready_future<seg_ptr>(make_lw_shared<test_segment>(id));
    }

    void need_free_segments() override {
        ++need_free_calls;
    }
};

// Returns the segment of an allocation, and fails the test if the allocation did not give a
// segment.
seg_ptr segment_of(allocator::allocate_result result) {
    BOOST_REQUIRE(std::holds_alternative<seg_ptr>(result));
    return std::get<seg_ptr>(std::move(result));
}

struct test_allocator {
    test_source src;
    allocator alloc;

    explicit test_allocator(uint64_t configured_segment_count)
        : alloc(configured_segment_count, src)
    {}

    seg_ptr allocate() {
        return segment_of(alloc.allocate().get());
    }
};

// Starts a pool over a test allocator. With `configured_segment_count` equal to the capacity, the
// first refill fills the pool from the new segments, and after that a segment comes only when the
// test frees one.
struct test_pool {
    test_allocator ta;
    pool p;

    test_pool(uint64_t configured_segment_count, size_t capacity, size_t reserved_for_compaction, size_t reserved_for_separator = 0)
        : ta(configured_segment_count)
        , p(capacity, reserved_for_compaction, reserved_for_separator, ta.alloc)
    {
        p.start(default_scheduling_group());
        BOOST_REQUIRE(eventually_true([&] { return p.size() == capacity; }));
    }

    ~test_pool() {
        p.stop().get();
    }

    seg_ptr take_now(write_source src) {
        auto f = p.take(src);
        BOOST_REQUIRE(f.available());
        return f.get();
    }

    // Returns the segment to the allocator, as the release of its last reference does.
    void free(seg_ptr seg) {
        auto id = seg->id;
        seg = nullptr;
        ta.alloc.free(id);
    }

    void wait_for_size(size_t size) {
        BOOST_REQUIRE(eventually_true([&] { return p.size() == size; }));
    }
};

// The wait is a coroutine, which the reactor resumes after the abort, so this function
// waits for the result.
bool is_aborted(future<seg_ptr>& f) {
    BOOST_REQUIRE(eventually_true([&] { return f.available(); }));
    BOOST_REQUIRE(f.failed());
    try {
        f.get();
    } catch (const abort_requested_exception&) {
        return true;
    } catch (...) {
    }
    return false;
}

} // namespace

// Checks that the allocator opens the new segments in order, then the free ones, and waits for a
// free segment when both are used up.
SEASTAR_THREAD_TEST_CASE(test_segment_allocator_allocates_new_then_free_then_waits) {
    test_allocator ta(2);
    auto s0 = ta.allocate();
    auto s1 = ta.allocate();
    BOOST_REQUIRE_EQUAL(s0->id.value, 0);
    BOOST_REQUIRE_EQUAL(s1->id.value, 1);
    BOOST_REQUIRE_EQUAL(ta.alloc.new_count(), 0);
    BOOST_REQUIRE_EQUAL(ta.src.need_free_calls, 0);

    auto waiting = ta.alloc.allocate();
    BOOST_REQUIRE(!waiting.available());
    BOOST_REQUIRE_EQUAL(ta.src.need_free_calls, 1);

    auto id0 = s0->id;
    s0 = nullptr;
    BOOST_REQUIRE(ta.alloc.free(id0));
    BOOST_REQUIRE_EQUAL(segment_of(waiting.get())->id.value, 0);
    BOOST_REQUIRE_EQUAL(ta.alloc.free_count(), 0);
}

// Checks that a new segment whose file is not ready is not used, and the allocator uses a free
// segment without a call to open(), and that the new segment is allocated when its file is ready.
SEASTAR_THREAD_TEST_CASE(test_segment_allocator_uses_a_free_segment_when_the_new_one_is_not_ready) {
    test_allocator ta(3);
    auto s0 = ta.allocate();
    auto id0 = s0->id;
    s0 = nullptr;
    ta.alloc.free(id0);

    ta.src.not_ready_from = 1;
    BOOST_REQUIRE_EQUAL(ta.allocate()->id.value, 0);
    BOOST_REQUIRE_EQUAL(ta.src.opens.size(), 2);
    BOOST_REQUIRE_EQUAL(ta.alloc.new_count(), 2);

    ta.src.not_ready_from = test_source::none;
    BOOST_REQUIRE_EQUAL(ta.allocate()->id.value, 1);
    BOOST_REQUIRE_EQUAL(ta.alloc.new_count(), 1);
}

// Checks that a new segment whose file is not ready is opened, which waits for the format, when no
// free segment can be used instead.
SEASTAR_THREAD_TEST_CASE(test_segment_allocator_opens_a_new_segment_that_is_not_ready_when_nothing_is_free) {
    test_allocator ta(2);
    ta.src.not_ready_from = 0;
    BOOST_REQUIRE_EQUAL(ta.allocate()->id.value, 0);
    BOOST_REQUIRE_EQUAL(ta.alloc.new_count(), 1);
}

// Checks that a new segment for which open() fails is not used, and the allocator uses a free
// segment and tries the new segment again later.
SEASTAR_THREAD_TEST_CASE(test_segment_allocator_uses_a_free_segment_when_the_new_one_fails) {
    test_allocator ta(3);
    auto s0 = ta.allocate();
    auto id0 = s0->id;
    s0 = nullptr;
    ta.alloc.free(id0);

    ta.src.fail_open_from = 1;
    BOOST_REQUIRE_EQUAL(ta.allocate()->id.value, 0);
    BOOST_REQUIRE_EQUAL(ta.src.opens.size(), 3);
    BOOST_REQUIRE_EQUAL(ta.src.opens[1].value, 1);
    BOOST_REQUIRE_EQUAL(ta.alloc.new_count(), 2);

    ta.src.fail_open_from = test_source::none;
    BOOST_REQUIRE_EQUAL(ta.allocate()->id.value, 1);
    BOOST_REQUIRE_EQUAL(ta.alloc.new_count(), 1);
}

// Checks that a new segment whose open fails fails the allocation when no free segment can be used
// instead.
SEASTAR_THREAD_TEST_CASE(test_segment_allocator_fails_when_the_new_segment_fails_and_nothing_is_free) {
    test_allocator ta(2);
    ta.src.fail_open = true;
    BOOST_REQUIRE_THROW(ta.alloc.allocate().get(), std::runtime_error);
    BOOST_REQUIRE_EQUAL(ta.alloc.new_count(), 2);
}

// Checks that a free segment whose open fails goes back to the free list.
SEASTAR_THREAD_TEST_CASE(test_segment_allocator_returns_a_free_segment_whose_open_fails) {
    test_allocator ta(1);
    auto s0 = ta.allocate();
    auto id0 = s0->id;
    s0 = nullptr;
    ta.alloc.free(id0);

    ta.src.fail_open = true;
    BOOST_REQUIRE_THROW(ta.alloc.allocate().get(), std::runtime_error);
    BOOST_REQUIRE_EQUAL(ta.alloc.free_count(), 1);
    ta.src.fail_open = false;
    BOOST_REQUIRE_EQUAL(ta.allocate()->id.value, 0);
}

// Checks that stop_waiting() ends a wait for a free segment, and the waits after it.
SEASTAR_THREAD_TEST_CASE(test_segment_allocator_stop_waiting_ends_the_wait) {
    test_allocator ta(0);
    auto waiting = ta.alloc.allocate();
    BOOST_REQUIRE(!waiting.available());
    ta.alloc.stop_waiting();
    BOOST_REQUIRE(std::holds_alternative<allocator::stopped>(waiting.get()));
    BOOST_REQUIRE(std::holds_alternative<allocator::stopped>(ta.alloc.allocate().get()));
}

// Checks that the pool fills itself to its capacity, and refills after a call to take().
SEASTAR_THREAD_TEST_CASE(test_segment_pool_refills_itself) {
    test_pool tp(6, 4, 1);
    BOOST_REQUIRE_EQUAL(tp.ta.src.opens.size(), 4);
    auto seg = tp.take_now(write_source::normal);
    tp.wait_for_size(4);
    BOOST_REQUIRE_EQUAL(tp.ta.src.opens.size(), 5);
    BOOST_REQUIRE_EQUAL(tp.p.get_stats().segments_put, 5);
}

// Checks that normal writes leave the compaction reserve in the pool, and that compaction takes it.
SEASTAR_THREAD_TEST_CASE(test_segment_pool_keeps_the_compaction_reserve) {
    test_pool tp(4, 4, 2);
    auto n0 = tp.take_now(write_source::normal);
    auto n1 = tp.take_now(write_source::normal);
    BOOST_REQUIRE_EQUAL(tp.p.available_segment_count(write_source::normal), 0);
    BOOST_REQUIRE_EQUAL(tp.p.available_segment_count(write_source::compaction), 2);

    auto waiting = tp.p.take(write_source::normal);
    BOOST_REQUIRE(!waiting.available());
    BOOST_REQUIRE_EQUAL(tp.p.get_stats().segments_wait[static_cast<size_t>(write_source::normal)], 1);

    auto c0 = tp.take_now(write_source::compaction);
    auto c1 = tp.take_now(write_source::compaction);
    BOOST_REQUIRE_EQUAL(tp.p.size(), 0);
    BOOST_REQUIRE(!waiting.available());

    // The pool must hold more than the reserve before the normal write gets a segment.
    tp.free(std::move(c0));
    tp.free(std::move(c1));
    tp.wait_for_size(2);
    BOOST_REQUIRE(!waiting.available());
    tp.free(std::move(n0));
    BOOST_REQUIRE(eventually_true([&] { return waiting.available(); }));
    // The pool is a queue, and the first segment that the test freed was the first compaction
    // segment.
    BOOST_REQUIRE_EQUAL(waiting.get()->id.value, 2);
    BOOST_REQUIRE_EQUAL(tp.p.size(), 2);
}

// Checks that the separator takes from the separator reserve and leaves the compaction reserve, and
// that normal writes leave both.
SEASTAR_THREAD_TEST_CASE(test_segment_pool_keeps_the_separator_reserve) {
    test_pool tp(4, 4, 1, 1);
    BOOST_REQUIRE_EQUAL(tp.p.reserve_for(write_source::normal), 2);
    BOOST_REQUIRE_EQUAL(tp.p.reserve_for(write_source::separator), 1);
    BOOST_REQUIRE_EQUAL(tp.p.reserve_for(write_source::compaction), 0);

    auto n0 = tp.take_now(write_source::normal);
    auto n1 = tp.take_now(write_source::normal);
    auto nw = tp.p.take(write_source::normal);
    BOOST_REQUIRE(!nw.available());
    BOOST_REQUIRE_EQUAL(tp.p.available_segment_count(write_source::separator), 1);

    auto s0 = tp.take_now(write_source::separator);
    auto sw = tp.p.take(write_source::separator);
    BOOST_REQUIRE(!sw.available());
    BOOST_REQUIRE_EQUAL(tp.p.get_stats().segments_wait[static_cast<size_t>(write_source::separator)], 1);

    auto c0 = tp.take_now(write_source::compaction);
    BOOST_REQUIRE_EQUAL(tp.p.size(), 0);

    // The pool serves the separator above the compaction reserve, and the normal write above both
    // reserves.
    tp.free(std::move(c0));
    tp.wait_for_size(1);
    BOOST_REQUIRE(!sw.available() && !nw.available());
    tp.free(std::move(s0));
    BOOST_REQUIRE(eventually_true([&] { return sw.available(); }));
    sw.get();
    BOOST_REQUIRE_EQUAL(tp.p.size(), 1);
    BOOST_REQUIRE(!nw.available());
    tp.free(std::move(n0));
    tp.wait_for_size(2);
    BOOST_REQUIRE(!nw.available());
    tp.free(std::move(n1));
    BOOST_REQUIRE(eventually_true([&] { return nw.available(); }));
    nw.get();
}

// Checks that a new segment goes to a waiting compaction before a waiting separator flush, and to
// the flush before a waiting normal write, and that the waiters of a tier are served in arrival
// order.
SEASTAR_THREAD_TEST_CASE(test_segment_pool_serves_the_tiers_in_order_and_then_in_arrival_order) {
    test_pool tp(2, 2, 1);
    auto n = tp.take_now(write_source::normal);
    auto c = tp.take_now(write_source::compaction);

    auto w1 = tp.p.take(write_source::normal);
    auto sw = tp.p.take(write_source::separator);
    auto cw = tp.p.take(write_source::compaction);
    auto w2 = tp.p.take(write_source::streaming);
    BOOST_REQUIRE(!w1.available() && !sw.available() && !cw.available() && !w2.available());

    tp.free(std::move(n));
    BOOST_REQUIRE(eventually_true([&] { return cw.available(); }));
    auto c2 = cw.get();
    BOOST_REQUIRE(!w1.available() && !sw.available() && !w2.available());

    // Here the separator and the normal writes take segments above the same reserve, and the pool
    // serves the separator first.
    tp.free(std::move(c));
    tp.wait_for_size(1);
    BOOST_REQUIRE(!w1.available() && !sw.available());

    tp.free(std::move(c2));
    BOOST_REQUIRE(eventually_true([&] { return sw.available(); }));
    auto s = sw.get();
    BOOST_REQUIRE(!w1.available() && !w2.available());

    tp.free(std::move(s));
    BOOST_REQUIRE(eventually_true([&] { return w1.available(); }));
    auto n1 = w1.get();
    BOOST_REQUIRE(!w2.available());

    tp.free(std::move(n1));
    BOOST_REQUIRE(eventually_true([&] { return w2.available(); }));
    w2.get();
}

// Checks that an abort ends only the wait of the aborted consumer, and that a call to take() with
// an abort that was requested before fails at once.
SEASTAR_THREAD_TEST_CASE(test_segment_pool_abort_ends_only_the_aborted_wait) {
    test_pool tp(2, 2, 1);
    auto n = tp.take_now(write_source::normal);
    auto c = tp.take_now(write_source::compaction);

    abort_source as;
    auto w1 = tp.p.take(write_source::compaction, &as);
    auto w2 = tp.p.take(write_source::compaction);
    BOOST_REQUIRE(!w1.available() && !w2.available());

    as.request_abort();
    BOOST_REQUIRE(is_aborted(w1));
    BOOST_REQUIRE(!w2.available());

    auto w3 = tp.p.take(write_source::compaction, &as);
    BOOST_REQUIRE(is_aborted(w3));

    tp.free(std::move(c));
    BOOST_REQUIRE(eventually_true([&] { return w2.available(); }));
    w2.get();
}

// Checks that a failed refill fails the consumers that wait at that time, and that a consumer that
// starts to wait after it waits for the next attempt.
SEASTAR_THREAD_TEST_CASE(test_segment_pool_failed_refill_fails_the_waiters) {
    test_pool tp(2, 2, 1);
    auto n = tp.take_now(write_source::normal);
    auto c = tp.take_now(write_source::compaction);
    auto w1 = tp.p.take(write_source::normal);
    auto cw = tp.p.take(write_source::compaction);

    tp.ta.src.fail_open = true;
    tp.free(std::move(n));
    BOOST_REQUIRE(eventually_true([&] { return w1.available() && cw.available(); }));
    BOOST_REQUIRE_THROW(w1.get(), std::runtime_error);
    BOOST_REQUIRE_THROW(cw.get(), std::runtime_error);
    BOOST_REQUIRE_EQUAL(tp.p.get_stats().refill_failures, 1);

    auto w2 = tp.p.take(write_source::compaction);
    BOOST_REQUIRE(!w2.available());
    tp.ta.src.fail_open = false;
    // The next attempt, after the retry delay, succeeds.
    BOOST_REQUIRE(eventually_true([&] { return w2.available(); }));
    w2.get();
}

// Checks that a call to take() after the pool stopped fails at once, and that a consumer that waits
// when the pool stops fails.
SEASTAR_THREAD_TEST_CASE(test_segment_pool_take_after_stop_fails) {
    test_allocator ta(1);
    pool p(1, 0, 0, ta.alloc);
    p.start(default_scheduling_group());
    BOOST_REQUIRE(eventually_true([&] { return p.size() == 1; }));
    auto seg = p.take(write_source::normal).get();
    auto waiting = p.take(write_source::normal);
    BOOST_REQUIRE(!waiting.available());

    p.stop().get();
    BOOST_REQUIRE_THROW(waiting.get(), std::runtime_error);
    BOOST_REQUIRE_THROW(p.take(write_source::normal).get(), std::runtime_error);
}
