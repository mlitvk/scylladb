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

// The time constant, in seconds, over which the rate controller drains the backlog. A burst of dead
// records, for example from a bulk delete or a tablet that leaves, is removed gradually and not in
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

// Paces automatic compaction and decides how much CPU to ask for it.
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
// When the job ends, the driver settles the difference between what it reclaimed and the estimate,
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

    // Holds the inputs of one sampling period. The counts are since the previous tick.
    struct sample {
        uint64_t available_segments;
        // The threshold as a function of the free level, from the configuration.
        utilization_threshold_curve curve;
        // The segments of the groups of the shard, by utilization, for the backlog.
        const utilization_histogram& utilization;
        // The segments allocated since the previous tick that compaction must reclaim: the output
        // of the separator and the streamed segments. The count leaves out the mixed segments,
        // which the separator frees, and the output of compaction, because a job is charged
        // `n_in - n_out`. With the mixed segments the controller commands twice the rate that
        // compaction can deliver.
        uint64_t segments_allocated;
        // The most credit, and the most debt, in segments, that the bucket keeps from one period to
        // the next.
        double burst_cap;
        // The time since the previous tick, in seconds.
        double dt;
        // `candidates_empty` is true when the last ranking found no batch with a net gain, and
        // `throttled` is true when a job waited for credit since the previous tick. More CPU does
        // not help in either case, so the shares demand decreases.
        bool candidates_empty;
        bool throttled;
    };

    compaction_rate_controller() = default;
    explicit compaction_rate_controller(tuning t) noexcept : _tuning(t) {}

    void tick(const sample&) noexcept;

    // Returns true when the bucket holds the credit for a batch that reclaims `reclaimed` segments,
    // or compaction_admission_credit when that is less.
    bool can_afford(size_t reclaimed) const noexcept {
        return _bypassed || _credit >= double(std::min(reclaimed, compaction_admission_credit));
    }
    // Takes the `estimate` of a job that starts from the bucket. The charge holds until
    // settle_job().
    void charge_job(double estimate) noexcept {
        _credit -= estimate;
        _charged += estimate;
    }
    // Settles the charge of a job that ended: a job that reclaimed more than its `estimate` adds
    // debt, and a job that reclaimed less gets a refund.
    //
    // A refund does not take the credit over the cap of the last tick, so that a job that fails or
    // reclaims less than its estimate does not look like rate that compaction could not use. A
    // refund also does not give back the part of a charge that a tick already dropped as debt.
    void settle_job(double estimate, double reclaimed) noexcept;

    // Returns the net reclaim rate that the controller commands, in segments per second.
    double rate() const noexcept { return _rate; }
    double credit() const noexcept { return _credit; }
    // Returns the rate that compaction did not use, which is the credit that spilled over the cap
    // per second, smoothed.
    double unspent_rate() const noexcept { return _unspent_rate; }
    const compaction_backlog& backlog() const noexcept { return _backlog; }
    double space_pressure() const noexcept { return _pressure; }
    // The threshold that the selection applies now, which is the curve at the current free level.
    double utilization_threshold() const noexcept { return _threshold; }
    // The fewest candidates that a group must have now for a job, see effective_min_batch().
    size_t min_batch() const noexcept { return effective_min_batch(_pressure); }
    // Returns the part of the range from the minimum to the maximum compaction shares that
    // compaction asks for, in [0, 1]. It is at least the space pressure.
    double shares_demand() const noexcept { return std::max(_demand, _pressure); }
    // Returns true when the free level is at the space floor, where the throttle is off. It returns
    // false before the first tick, so that a controller without a rate starts no job.
    bool bypassed() const noexcept { return _bypassed; }

private:
    tuning _tuning;
    double _rate = 0;
    double _credit = 0;
    double _unspent_rate = 0;
    compaction_backlog _backlog;
    double _pressure = 0;
    double _threshold = 1;
    double _demand = 0;
    bool _bypassed = false;
    // The burst cap of the last tick, which limits a refund.
    double _burst_cap = 0;
    // The sum of the charges of the jobs in flight.
    double _charged = 0;
    // The debt that a tick dropped while jobs held their charges. A refund pays it first, so that a
    // charge that the bucket dropped does not come back as credit. It is at most `_charged`,
    // because only the jobs in flight can get a refund.
    double _forgiven = 0;
};

} // namespace replica::logstor
