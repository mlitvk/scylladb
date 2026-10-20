/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_rate_controller.hh"
#include <algorithm>
#include <cmath>

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

// An exponential moving average of a rate sampled over `dt`, with `tau` as its time constant. The
// weight is computed for the actual period, so a late tick does not over-weight its sample.
static double smooth_rate(double smoothed, double sample, double dt, double tau) noexcept {
    const auto alpha = 1 - std::exp(-dt / tau);
    return smoothed + alpha * (sample - smoothed);
}

void compaction_rate_controller::tick(const sample& s) noexcept {
    // The driver gives a period of zero to the next tick. This only protects the division.
    if (!(s.dt > 0)) {
        return;
    }

    // The threshold is a live-updatable config value with no range check at the config layer, so it
    // can be negative, NaN, or 1 and above. Anything outside [0, 1) disables automatic compaction.
    _enabled = std::isfinite(s.curve.threshold) && s.curve.threshold >= 0 && s.curve.threshold < 1;
    if (!_enabled) {
        ++_generation;
        _rate = 0;
        _credit = 0;
        _unspent_rate = 0;
        _backlog = {};
        _pressure = 0;
        _threshold = 1;
        _demand = 0;
        _bypassed = false;
        return;
    }

    _pressure = compaction_space_pressure(s.available_segments, s.curve);
    _threshold = effective_utilization_threshold(s.available_segments, s.curve);
    _backlog = reclaimable_backlog(s.utilization, _threshold);
    // At the space floor the throttle must not hold compaction back.
    _bypassed = _pressure >= 1;

    // The allocation rate is not smoothed. The credit bucket integrates it, so the next period
    // corrects the error of one period, and a filter would only add delay when the write rate
    // changes.
    const auto alloc_rate = double(s.segments_allocated) / s.dt;
    _rate = alloc_rate + _backlog.reclaimable_segments / _tuning.drain_time;
    // Credit over the cap is rate that compaction did not use. Measure it before the cap applies.
    // It shows that compaction needs more CPU only while there are candidates and the throttle does
    // not hold them back. Otherwise, when candidates come back, the spill of the idle time would
    // increase the demand.
    const auto may_use_cpu = !s.candidates_empty && !s.throttled && !_bypassed;
    const auto spilled = may_use_cpu ? std::max(0.0, _credit - s.burst_cap) : 0.0;
    _unspent_rate = smooth_rate(_unspent_rate, spilled / s.dt, s.dt, compaction_rate_measurement_time);
    // The accrual of the period is always added in full. Only what the bucket keeps between periods
    // is limited, in both directions, because a limit on the accrual would limit the rate to one
    // batch per period. The debt comes from jobs that start on partial credit. While the throttle
    // is off the bucket keeps no debt, because otherwise compaction would stall on that debt when
    // the throttle comes back on.
    const auto floor = _bypassed ? 0.0 : -s.burst_cap;
    if (_credit < floor) {
        ++_generation;
    }
    _credit = std::clamp(_credit, floor, s.burst_cap) + _rate * s.dt;

    // No candidates, or a job that waited for credit: more CPU does not help, so the demand
    // decreases. Otherwise the demand increases by the spilled fraction of the rate and decreases
    // slowly. The controller measures the spill and not the delivered rate, because a job delivers
    // its segments only when it ends, and a shard with one group cannot wait for credit while its
    // job runs.
    const auto idle = s.candidates_empty || !(_rate > 0);
    if (idle || s.throttled) {
        _demand -= _demand * s.dt / _tuning.shares_decay_time;
    } else {
        const auto unspent_fraction = std::min(1.0, _unspent_rate / _rate);
        _demand += unspent_fraction * s.dt / _tuning.shares_rise_time - _demand * s.dt / _tuning.shares_hold_decay_time;
    }
    // The space pressure is the lowest demand, see shares_demand(). It is not kept in `_demand`, so
    // that the demand falls when the pressure ends.
    _demand = std::clamp(_demand, 0.0, 1.0);
}

} // namespace replica::logstor
