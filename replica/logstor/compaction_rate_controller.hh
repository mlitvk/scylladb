/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "backlog_controller.hh"
#include "replica/logstor/compaction_policy.hh"
#include "replica/logstor/segment_stats.hh"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

namespace replica::logstor {

// The segments under the utilization threshold, and the net number of segments that a compaction of
// all of them frees, estimated from a utilization histogram. When the threshold cuts a bucket, the
// estimate takes the segments of the bucket as spread evenly over its range. So if the threshold
// cuts a bucket at 30% of its range, 30% of its segments count. The lowest bucket always counts in
// full, so that a threshold of 0 still finds the dead segments. The driver watches this estimate
// for new candidates, but the selection reads the segments.
struct compaction_backlog {
    double candidate_segments{0};
    double reclaimable_segments{0};
};
compaction_backlog reclaimable_backlog(const utilization_histogram& utilization, double threshold) noexcept;

// The period at which the rate controller samples the shard.
constexpr std::chrono::milliseconds compaction_rate_control_period{250};

// Decides what automatic compaction takes and how much CPU it asks for.
//
// The utilization threshold curve decides which segments to compact. The CPU shares decide how fast
// compaction runs, and nothing else throttles it. The shares demand is the space pressure: 0 at or
// above the anchor of the curve, so compaction runs at the minimum shares, and 1 at the space floor.
// When compaction does not keep up with the writes at the minimum shares, the free level decreases
// below the anchor, and the shares increase until compaction keeps up.
class compaction_rate_controller {
public:
    // One sampling period.
    struct sample {
        uint64_t available_segments;
        // The threshold as a function of the free level, from the configuration.
        utilization_threshold_curve curve;
        // The segments of the groups of the shard, by utilization, for the backlog.
        const utilization_histogram& utilization;
    };

    void tick(const sample&) noexcept;

    const compaction_backlog& backlog() const noexcept { return _backlog; }
    double space_pressure() const noexcept { return _pressure; }
    // The threshold that the selection applies now, which is the curve at the current free level.
    double utilization_threshold() const noexcept { return _threshold; }
    // The fewest candidates that a group must have now for a job, see effective_min_batch().
    size_t min_batch() const noexcept { return effective_min_batch(_pressure); }

private:
    compaction_backlog _backlog;
    double _pressure = 0;
    double _threshold = 1;
};

// Maps the shares demand of the rate controller, in [0, 1], to CPU shares: min_shares at 0, at or
// above the threshold anchor, and logstor_compaction_max_shares at 1, at the space floor. The
// function is linear, because the demand already has the shape of the shares ramp, see
// compaction_rate_controller.
class compaction_shares_controller : public backlog_controller {
public:
    // The shares at or above the threshold anchor. It is the floor that the sstable compaction
    // controller uses.
    static constexpr float min_shares = 50.0f;

    compaction_shares_controller(scheduling_group sg, float static_shares, float max_shares,
            std::chrono::milliseconds interval, std::function<float()> current_backlog)
        : backlog_controller(std::move(sg), interval, make_control_points(max_shares),
                std::move(current_backlog), static_shares) {
    }

    void set_max_shares(float max_shares) {
        _control_points = make_control_points(max_shares);
    }

private:
    static std::vector<control_point> make_control_points(float max_shares);
};

} // namespace replica::logstor
