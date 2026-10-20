/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/compaction_policy.hh"
#include "replica/logstor/segment_stats.hh"
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace replica::logstor {

// The segments under the utilization threshold, and the net number of segments that a compaction of
// all of them frees, estimated from a utilization histogram. When the threshold cuts a bucket, the
// estimate takes the segments of the bucket as spread evenly over its range. So if the threshold
// cuts a bucket at 30% of its range, 30% of its segments count. The lowest bucket always counts in
// full, so that a threshold of 0 still finds the dead segments. The rate controller drains this
// estimate and the driver watches it, but the selection reads the segments.
struct compaction_backlog {
    double candidate_segments{0};
    double reclaimable_segments{0};
};
compaction_backlog reclaimable_backlog(const utilization_histogram& utilization, double threshold) noexcept;

// The period at which the reclaim rate controller samples the shard.
constexpr std::chrono::milliseconds compaction_rate_control_period{250};

// The time constant, in seconds, over which the rate controller drains the backlog. A burst of
// garbage, for example from a bulk delete or a tablet that leaves, is removed gradually and not in
// one burst of jobs, and the backlog is drained also when there are no writes.
constexpr double compaction_backlog_drain_time = 60.0;

// The time constant, in seconds, over which the unspent rate is smoothed. The allocation rate is
// not smoothed, see compaction_rate_controller::tick().
constexpr double compaction_rate_measurement_time = 2.0;

// The most credit that a batch waits for, in reclaimed segments. A batch is charged all that it
// reclaims, so a batch that starts on less credit leaves a debt, which delays the next job. The
// limit stops the best candidate, which can be a large batch of dead segments, from holding the
// driver for a long time while other groups have smaller batches.
constexpr size_t compaction_admission_credit = min_segments_per_compaction;

// The time constants of the shares demand, in seconds: how fast it increases while compaction
// cannot use its rate, how fast it decreases while compaction is throttled or has nothing to do,
// and how fast it decreases while compaction uses its rate. See compaction_rate_controller.
constexpr double compaction_shares_rise_time = 5.0;
constexpr double compaction_shares_decay_time = 20.0;
constexpr double compaction_shares_hold_decay_time = 120.0;

// Paces automatic compaction and decides how much CPU to ask for it. The engine and
// test/manual/logstor_compaction_sim.cc share it, so the simulated dynamics are the controller's own.
//
// The utilization threshold decides which segments to compact, and this controller decides how
// fast. It commands a net reclaim rate, `n_in - n_out` per second:
//
//     R = A + B / T_drain
//
// `A` is the allocation rate of the segments that compaction must reclaim, which keeps the free
// level where it is. `B / T_drain` drains the backlog `B` under the threshold, so that compaction
// also runs when there are no writes. There is no target free level, so there is no error to
// integrate.
//
// The rate fills a credit bucket of reclaimed segments. A job starts when the bucket holds what its
// batch reclaims, or compaction_admission_credit if that is less, and the job is charged all of it.
// When the job ends, the driver charges the difference between what it reclaimed and the estimate,
// so the bucket holds the charge of a job for as long as the job runs. Between periods the bucket
// keeps at most one batch of credit and of debt, so an idle period saves at most one job, and a job
// that starts on debt delays only the next one.
//
// The CPU shares follow the bucket. Credit that spills over the cap while there are candidates is
// rate that compaction could not use, and only more CPU can change that, so the shares demand
// increases by the spilled fraction of the rate. A job that waits for credit, or no candidates,
// means that more CPU does not help, so the demand decreases. While the bucket is spent as fast as
// it fills, the demand decreases slowly, so that it finds the lowest shares that use the rate.
//
// Below the anchor of the threshold curve the shares demand is at least the space pressure, and at
// the space floor the throttle is off.
class compaction_rate_controller {
public:
    // The time constants are parameters, so that a test can change them. The engine uses the
    // defaults.
    struct tuning {
        double drain_time = compaction_backlog_drain_time;
        double shares_rise_time = compaction_shares_rise_time;
        double shares_decay_time = compaction_shares_decay_time;
        double shares_hold_decay_time = compaction_shares_hold_decay_time;
    };

    compaction_rate_controller() = default;
    explicit compaction_rate_controller(tuning t) noexcept : _tuning(t) {}

    // One sampling period. The counts are since the previous tick, and the controller smooths them.
    struct sample {
        uint64_t available_segments;
        // The threshold as a function of the free level, from the configuration. A threshold
        // outside [0, 1) disables automatic compaction and the controller.
        utilization_threshold_curve curve;
        // The segments of the groups of the shard, by utilization, for the backlog.
        const utilization_histogram& utilization;
        // The segments allocated since the previous tick that compaction must reclaim: the output
        // of the separator and the streamed segments. Not the mixed segments, which the separator
        // frees, and not the output of compaction, because a job is charged `n_in - n_out`. With
        // the mixed segments the controller commands twice the rate that compaction can deliver.
        uint64_t segments_allocated;
        // The most credit, and the most debt, that the bucket keeps from one period to the next, in
        // segments. The driver gives one batch.
        double burst_cap;
        double dt;
        // Whether the last ranking found no batch with a net gain, and whether a job waited for
        // credit since the previous tick. More CPU does not help in either case, so the shares
        // demand decreases.
        bool candidates_empty;
        bool throttled;
    };

    void tick(const sample&) noexcept;

    // Whether a batch estimated to reclaim `reclaimed` segments may start now: the bucket holds
    // what it reclaims, or compaction_admission_credit if that is less.
    bool can_afford(size_t reclaimed) const noexcept {
        return _bypassed || _credit >= double(std::min(reclaimed, compaction_admission_credit));
    }
    // Takes `segments` from the bucket, or gives them back when negative. A disabled controller
    // keeps no credit.
    void charge(double segments) noexcept {
        if (_enabled) {
            _credit -= segments;
        }
    }
    // Changes when a tick drops debt: when the controller is disabled, or when the debt is more
    // than the bucket keeps. A job that started in an earlier generation must not settle its
    // charge, because a refund would give back credit for a charge that the bucket dropped.
    uint64_t generation() const noexcept { return _generation; }

    // Returns true when the configured threshold enables automatic compaction at all.
    bool enabled() const noexcept { return _enabled; }
    // The commanded net reclaim rate, in segments per second.
    double rate() const noexcept { return _rate; }
    double credit() const noexcept { return _credit; }
    // The commanded rate that compaction did not use, that is the credit that spilled over the cap,
    // in segments per second, smoothed.
    double unspent_rate() const noexcept { return _unspent_rate; }
    const compaction_backlog& backlog() const noexcept { return _backlog; }
    double space_pressure() const noexcept { return _pressure; }
    // The threshold that the selection applies now, which is the curve at the current free level.
    double utilization_threshold() const noexcept { return _threshold; }
    // The fewest candidates that a job runs on now, see effective_min_batch().
    size_t min_batch() const noexcept { return effective_min_batch(_pressure); }
    // The fraction of the range between the minimum and the maximum shares that compaction asks
    // for.
    double shares_demand() const noexcept { return std::max(_demand, _pressure); }
    // Whether the throttle is off, at the space floor. It is false before the first tick, so that a
    // controller without a rate starts no job.
    bool bypassed() const noexcept { return _bypassed; }

private:
    tuning _tuning;
    bool _enabled = false;
    double _rate = 0;
    double _credit = 0;
    double _unspent_rate = 0;
    compaction_backlog _backlog;
    double _pressure = 0;
    double _threshold = 1;
    double _demand = 0;
    bool _bypassed = false;
    uint64_t _generation = 0;
};

} // namespace replica::logstor
