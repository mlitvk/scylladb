/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_rate_controller.hh"
#include <algorithm>

namespace replica::logstor {

compaction_backlog reclaimable_backlog(const utilization_histogram& utilization, double threshold) noexcept {
    // A bucket whose upper bound is the threshold counts in full, in spite of rounding.
    constexpr double bound_tolerance = 1e-9;
    compaction_backlog backlog;
    // Bucket i holds the segments with utilization in (i/N, (i+1)/N], see utilization_bucket_of().
    const auto max_utilization = std::clamp(threshold, 0.0, 1.0);
    for (size_t i = 0; i < utilization_bucket_count; ++i) {
        const auto lower = double(i) / utilization_bucket_count;
        const auto upper = double(i + 1) / utilization_bucket_count;
        auto counted_upper = upper;
        if (i > 0 && upper > max_utilization + bound_tolerance) {
            if (lower >= max_utilization) {
                break;
            }
            counted_upper = max_utilization;
        }
        const auto fraction = (counted_upper - lower) / (upper - lower);
        const auto counted = double(utilization[i]) * fraction;
        const auto mean_utilization = (lower + counted_upper) / 2;
        backlog.candidate_segments += counted;
        backlog.reclaimable_segments += counted * (1 - mean_utilization);
    }
    return backlog;
}

void compaction_rate_controller::tick(const sample& s) noexcept {
    // A sample with no period has no rate, so the tick ignores it.
    if (!(s.dt > 0)) {
        return;
    }

    _pressure = compaction_space_pressure(s.available_segments, s.curve);
    _threshold = effective_utilization_threshold(s.available_segments, s.curve);
    _backlog = reclaimable_backlog(s.utilization, _threshold);
    _bypassed = _pressure >= 1;

    // The allocation rate is not smoothed. The credit bucket integrates it, so the next period
    // corrects the error of one period, and a filter would only add delay when the write rate
    // changes.
    const auto alloc_rate = double(s.segments_allocated) / s.dt;
    _rate = alloc_rate + _backlog.reclaimable_segments / _tuning.drain_time;
    // The accrual of the period is always added in full. Only what the bucket keeps between periods
    // is limited, in both directions, because a limit on the accrual would limit the rate to one
    // batch per period. The debt comes from jobs that start on partial credit. While the throttle
    // is off the bucket keeps no debt, because otherwise compaction would stall on that debt when
    // the throttle comes back on.
    const auto floor = _bypassed ? 0.0 : -s.burst_cap;
    if (_credit < floor) {
        _forgiven = std::min(_charged, _forgiven + (floor - _credit));
    }
    _credit = std::clamp(_credit, floor, s.burst_cap) + _rate * s.dt;
}

void compaction_rate_controller::settle_job(double estimate, double reclaimed) noexcept {
    _charged = std::max(0.0, _charged - estimate);
    auto refund = estimate - reclaimed;
    if (refund > 0) {
        const auto paid = std::min(refund, _forgiven);
        _forgiven -= paid;
        refund -= paid;
    }
    _credit += refund;
    _forgiven = std::min(_forgiven, _charged);
}

} // namespace replica::logstor
