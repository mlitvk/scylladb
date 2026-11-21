/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>

#include <seastar/core/scheduling.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>

#include "utils/logalloc.hh"

namespace tests {

// Configures the LSA tracker of this shard with a background reclaimer, as main.cc does, and
// returns a guard that stops the background reclaimer. The background reclaimer serves the
// compactions that logalloc::region::request_compaction() requests.
// The caller must run in a seastar thread.
inline auto configure_lsa_background_reclaim() {
    using namespace std::chrono_literals;
    auto sg = seastar::create_scheduling_group("background_reclaim", 100).get();
    logalloc::tracker::config cfg;
    cfg.defragment_on_idle = false;
    cfg.abort_on_lsa_bad_alloc = false;
    cfg.lsa_reclamation_step = 1;
    cfg.background_reclaim_sched_group = sg;
    cfg.background_reclaim_shares_adjust_period = 1ms;
    logalloc::shard_tracker().configure(cfg);
    return seastar::defer([sg] () noexcept {
        logalloc::shard_tracker().stop().get();
        seastar::destroy_scheduling_group(sg).get();
    });
}

// Waits up to 10 s for `done` to return true, and returns its last result.
// The caller must run in a seastar thread.
inline bool wait_for_background_reclaim(std::invocable auto done) {
    using namespace std::chrono_literals;
    for (int i = 0; i < 10000 && !done(); ++i) {
        seastar::sleep(1ms).get();
    }
    return done();
}

} // namespace tests
