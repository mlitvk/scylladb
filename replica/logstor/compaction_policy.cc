/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_policy.hh"
#include "replica/logstor/compaction.hh"
#include <algorithm>
#include <cmath>
#include <optional>

namespace replica::logstor {

free_segment_watermarks make_free_segment_watermarks(uint64_t segment_count, double target_fraction) noexcept {
    // target_fraction is a live-updatable config value with no range check at the config layer, so
    // it may arrive negative, NaN or above 1.0; treat anything outside (0, 1] as disabling the trigger.
    if (!std::isfinite(target_fraction) || target_fraction <= 0) {
        return {0, 0};
    }

    const auto fraction = std::min(target_fraction, 1.0);
    const auto by_fraction = static_cast<uint64_t>(std::ceil(segment_count * fraction));
    // The target has to cover what compaction holds while it works: a job takes up to a batch of
    // segments before it gives any back (see make_compaction_limits()), and the segment pool holds
    // back segments that normal writes cannot take, so a target of less than two batches leaves the
    // write path nothing to make progress with. The floor is itself capped, so that it cannot claim
    // an unreasonable share of a small disk.
    const auto min_target = std::min<uint64_t>(2 * min_segments_per_compaction, std::max<uint64_t>(1, segment_count / 8));
    const auto low = std::min(segment_count, std::max(by_fraction, min_target));
    // Relative hysteresis, so that the band does not grow out of proportion as the target shrinks.
    // Capped at segment_count so a high fraction can't push `high` out of reach of available_segments,
    // which would leave should_run_auto_compaction() unable to ever stop.
    const auto high = std::min(segment_count, low + std::max<uint64_t>(1, low / 4));
    return {low, high};
}

bool auto_compaction_wanted(bool running, uint64_t available_segments, free_segment_watermarks watermarks) noexcept {
    return available_segments < (running ? watermarks.high : watermarks.low);
}

uint64_t compaction_space_floor(uint64_t segment_count) noexcept {
    // Two batches at least, so that a job can hold one while the write path still has the other.
    // The minimum is itself capped, so that the space floor is not most of a tiny disk.
    const auto minimum = std::min<uint64_t>(2 * min_segments_per_compaction, std::max<uint64_t>(1, segment_count / 8));
    return std::max<uint64_t>(segment_count / 32, minimum);
}

compaction_limits make_compaction_limits(uint64_t space_floor, size_t max_batch_cap) noexcept {
    // A space floor below one batch cannot hold the relation; a batch of
    // min_segments_per_compaction is the least that can reclaim anything, so run at that rather
    // than at nothing.
    const auto budget = std::max<uint64_t>(space_floor, min_segments_per_compaction);
    // Spend the budget on concurrency first, up to the number of jobs the buffer pool can carry,
    // then give what is left to the batch. Both directions of the clamp matter: a batch below
    // min_segments_per_compaction cannot reclaim, and one above max_batch_cap makes a single job
    // hold too much of the space floor.
    const auto parallelism = std::clamp<uint64_t>(budget / min_segments_per_compaction, 1, max_auto_compaction_parallelism);
    const auto batch_cap = std::clamp<uint64_t>(budget / parallelism, min_segments_per_compaction, std::max<uint64_t>(max_batch_cap, min_segments_per_compaction));

    return {
        .auto_parallelism = static_cast<size_t>(parallelism),
        .batch_cap = static_cast<size_t>(batch_cap)
    };
}

utilization_threshold_curve make_utilization_threshold_curve(uint64_t segment_count, uint64_t space_floor,
        double threshold, double anchor_fraction, double ceiling_fraction) noexcept {
    // A fraction of the disk, rounded to whole segments; anything that is not a usable fraction
    // means "not configured".
    auto segments_of = [segment_count] (double fraction) -> std::optional<uint64_t> {
        if (!std::isfinite(fraction) || fraction <= 0) {
            return std::nullopt;
        }
        return static_cast<uint64_t>(std::llround(std::min(fraction, 1.0) * double(segment_count)));
    };
    // The anchor has to be above the space floor for the pressure band to have a width, and the
    // ceiling above the anchor for the fall of the threshold to have one; a configuration that says
    // otherwise gets the narrowest band that works rather than a division by zero.
    const auto anchor = std::max<uint64_t>(segments_of(anchor_fraction).value_or(2 * space_floor), space_floor + 1);
    const auto ceiling = segments_of(ceiling_fraction).transform([anchor] (uint64_t c) {
        return std::max<uint64_t>(c, anchor + 1);
    }).value_or(0);
    return utilization_threshold_curve{
        .space_floor = space_floor,
        .anchor = anchor,
        .ceiling = ceiling,
        .threshold = threshold,
    };
}

compaction_limits make_compaction_limits(free_segment_watermarks watermarks, size_t max_batch_cap) noexcept {
    // With the trigger disabled there is no target to protect, and automatic compaction never runs;
    // the limits still have to be sane for compaction submitted explicitly, so fall back to the
    // smallest batch rather than to zero.
    return make_compaction_limits(std::max<uint64_t>(watermarks.low, min_segments_per_compaction), max_batch_cap);
}

double compaction_space_pressure(uint64_t available_segments, const utilization_threshold_curve& curve) noexcept {
    if (curve.anchor <= curve.space_floor) {
        return available_segments <= curve.space_floor ? 1 : 0;
    }
    if (available_segments >= curve.anchor) {
        return 0;
    }
    if (available_segments <= curve.space_floor) {
        return 1;
    }
    return double(curve.anchor - available_segments) / double(curve.anchor - curve.space_floor);
}

double effective_utilization_threshold(uint64_t available_segments, const utilization_threshold_curve& curve) noexcept {
    if (available_segments < curve.anchor) {
        return curve.threshold + (1 - curve.threshold) * compaction_space_pressure(available_segments, curve);
    }
    if (curve.ceiling == 0 || curve.ceiling <= curve.anchor) {
        return curve.threshold;
    }
    if (available_segments >= curve.ceiling) {
        return 0;
    }
    const auto rise = double(available_segments - curve.anchor) / double(curve.ceiling - curve.anchor);
    return std::max(0.0, curve.threshold * (1 - rise));
}

size_t effective_min_batch(double pressure) noexcept {
    const auto scaled = std::llround(double(min_segments_per_compaction) * (1 - std::clamp(pressure, 0.0, 1.0)));
    return std::max<size_t>(1, static_cast<size_t>(scaled));
}

bool compaction_candidate_score::operator<(const compaction_candidate_score& other) const noexcept {
    if (live_bytes == 0 || other.live_bytes == 0) {
        // A batch that copies nothing has infinite efficiency.
        if ((live_bytes == 0) != (other.live_bytes == 0)) {
            return live_bytes != 0;
        }
    } else {
        // efficiency() < other.efficiency(), with the common segment_size factor cancelled out.
        const auto lhs = reclaimed() * other.live_bytes;
        const auto rhs = other.reclaimed() * live_bytes;
        if (lhs != rhs) {
            return lhs < rhs;
        }
    }

    return reclaimed() < other.reclaimed();
}

// Decides how many segments to compact, where `prefix_scores[i]` scores the batch made of the first
// i+1 candidate segments in ascending utilization order. Returns 0 if no prefix reclaims a segment.
// The most efficient prefix is extended to the longest prefix that stays within
// `extension_tolerance` of it, trading a little efficiency for fewer and larger jobs.
static size_t select_compaction_prefix(std::span<const compaction_candidate_score> prefix_scores, double extension_tolerance) noexcept {
    size_t best = 0;
    for (size_t i = 0; i < prefix_scores.size(); ++i) {
        if (prefix_scores[i].reclaimed() == 0) {
            continue;
        }
        // Strictly better, so that the shortest of equally efficient prefixes wins: extending is
        // only worth it when the tolerance below says so.
        if (best == 0 || prefix_scores[best - 1] < prefix_scores[i]) {
            best = i + 1;
        }
    }

    if (best == 0) {
        return 0;
    }

    // Efficiency along the prefix is a sawtooth - it jumps whenever one more segment is reclaimed
    // and decays as live bytes accumulate without reclaiming - so the longest prefix within the
    // tolerance is not necessarily the one just before the first prefix that falls outside it.
    for (size_t i = prefix_scores.size(); i > best; --i) {
        if (prefix_scores[i - 1].efficiency_at_least(prefix_scores[best - 1], extension_tolerance)) {
            return i;
        }
    }
    return best;
}

std::optional<compaction_batch> select_compaction_batch(const segment_set& segments, uint64_t segment_size,
        const compaction_batch_limits& limits) {
    std::vector<const segment_descriptor*> candidates;
    // prefix_scores[i] scores the batch made of candidates[0..i].
    std::vector<compaction_candidate_score> prefix_scores;
    candidates.reserve(limits.batch_cap);
    prefix_scores.reserve(limits.batch_cap);

    // A segment is a candidate while its utilization is at most the threshold, that is while it has
    // at least this much free. The histogram is ordered by free space a bucket at a time, so the
    // first segment over the threshold ends the candidate set for this group only once the buckets
    // it is ordered by have passed it: inside the bucket the threshold cuts through, the segments are
    // in no particular order and each is judged on its own.
    const auto max_record_bytes = static_cast<uint64_t>(std::floor(std::clamp(limits.max_utilization, 0.0, 1.0) * double(segment_size)));
    const auto min_free_space = segment_size - max_record_bytes;
    const auto threshold_bucket = min_free_space > 0 ? segment_descriptor_hist_options.bucket_of(min_free_space) : 0;

    uint64_t accum_record_bytes = 0;
    uint64_t accum_record_count = 0;
    for (const auto& desc : segments.by_free_space()) {
        if (candidates.size() >= limits.batch_cap) {
            break;
        }
        if (min_free_space > 0 && desc.free_space < min_free_space) {
            if (segment_descriptor_hist_options.bucket_of(desc.free_space) < threshold_bucket) {
                break;
            }
            continue;
        }
        candidates.push_back(&desc);

        accum_record_bytes += desc.record_bytes(segment_size);
        accum_record_count += desc.record_count;

        const auto score = compaction_candidate_score{
            .n_in = candidates.size(),
            .n_out = raw_write_buffer::estimate_required_segments(accum_record_bytes, accum_record_count, segment_size, segment_kind::full),
            .live_bytes = accum_record_bytes,
        };
        prefix_scores.push_back(score);
    }

    // Fewer candidates than a job is worth running on answers for the whole group: nothing below
    // the threshold is going to be added to make the batch up.
    if (candidates.size() < limits.min_batch) {
        return std::nullopt;
    }

    const auto selected_count = select_compaction_prefix(prefix_scores, limits.extension_tolerance);
    if (selected_count == 0) {
        return std::nullopt;
    }

    candidates.resize(selected_count);

    return compaction_batch{
        .segments = std::move(candidates),
        .score = prefix_scores[selected_count - 1],
    };
}

float compaction_shares_pressure(uint64_t available_segments, free_segment_watermarks watermarks) noexcept {
    // Also covers the trigger being disabled, where both watermarks are zero: with no free-segment
    // target there is no space-driven demand for shares.
    if (available_segments >= watermarks.high) {
        return 0.0f;
    }
    // Reaching the maximum shares while half the target is still in hand keeps the equilibrium of a
    // saturated controller away from the point where normal writes stall.
    const auto saturation_point = watermarks.low / 2;
    if (available_segments <= saturation_point) {
        return 1.0f;
    }
    return float(watermarks.high - available_segments) / float(watermarks.high - saturation_point);
}

compaction_backlog reclaimable_backlog(const utilization_histogram& utilization, double threshold) noexcept {
    compaction_backlog backlog;
    // Bucket i holds the segments with utilization in (i/N, (i+1)/N], see utilization_bucket_of();
    // every segment of it is under the threshold when the bucket's upper bound is at most the
    // threshold, and the bucket the threshold cuts through holds candidates up to it, taken as spread
    // uniformly. The lowest bucket always counts whole, dead segments being candidates at any
    // threshold.
    const auto max_utilization = std::clamp(threshold, 0.0, 1.0);
    for (size_t i = 0; i < utilization_bucket_count; ++i) {
        const auto lower = double(i) / utilization_bucket_count;
        const auto upper = double(i + 1) / utilization_bucket_count;
        auto counted_upper = upper;
        if (i > 0 && upper > max_utilization + 1e-9) {
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
    if (!(s.dt > 0)) {
        return;
    }

    // The measurements run whether or not the controller is enabled, so that the gauges say what
    // the shard is doing either way.
    _alloc_rate = double(s.segments_allocated) / s.dt;
    _delivered_rate = smooth_rate(_delivered_rate, double(s.segments_reclaimed) / s.dt, s.dt,
            compaction_rate_measurement_time);

    // The threshold is a live-updatable config value with no range check at the config layer, so it
    // may arrive negative, NaN or 1 and above; anything outside [0, 1) disables automatic compaction.
    _enabled = std::isfinite(s.curve.threshold) && s.curve.threshold >= 0 && s.curve.threshold < 1;
    if (!_enabled) {
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
    // At the space floor compaction is losing, and the throttle must not be the reason.
    _bypassed = _pressure >= 1;

    // The allocation rate is unsmoothed: it is spent through the credit bucket, which is an
    // integrator, so what a period over- or under-estimates the next one gives back. A filter in
    // front of it would only add lag, and lag is what the level pays for during a step in the write
    // rate.
    _rate = _alloc_rate + _backlog.reclaimable_segments / _tuning.drain_time;
    // What the bucket carries over its cap is rate that was commanded and not spent. That is the
    // one measurement the shares are driven by, so it is taken before the cap is applied.
    const auto spilled = std::max(0.0, _credit - s.burst_cap);
    _unspent_rate = smooth_rate(_unspent_rate, spilled / s.dt, s.dt, compaction_rate_measurement_time);
    // The period's accrual is always added in full, and it is what the bucket carries between
    // periods that is capped, both ways: trimming the accrual itself would silently cap the
    // sustained rate at a batch per period, which on a fast disk is well below what the level asks
    // for, and a debt is what jobs admitted on partial credit leave behind - bounded to a batch, so
    // that it paces the next job and no more. Jobs run while the throttle is bypassed are charged
    // as well, and were the debt kept, compaction would come out of bypass owing everything it just
    // reclaimed and stall until the level had fallen back to the space floor: a relay around it.
    // So while bypassed the bucket owes nothing.
    const auto floor = _bypassed ? 0.0 : -s.burst_cap;
    _credit = std::clamp(_credit, floor, s.burst_cap) + _rate * s.dt;

    // Which of the three states compaction is in is read off the bucket and the driver. Nothing to
    // run, or a job waiting for credit, is a state more CPU would not change, and the demand decays:
    // the shares bought are not being used. Credit spilling over the cap while there is something to
    // run is rate compaction was given and could not spend - jobs in flight not finishing fast enough
    // for the next to be submitted - which is the one state more CPU could change, and the demand
    // climbs by the spilled fraction of the rate. That the spill is what is measured, rather than the
    // delivered rate against the commanded one, is deliberate: a job delivers what it reclaims at its
    // end, so the delivered rate is lumpy at any smoothing, and a shard with a single group cannot
    // wait for credit while its one job runs, so the absence of a wait says nothing. In between - the
    // bucket being spent as fast as it fills - compaction keeps up on the shares it has, and the demand
    // leaks slowly, so that it drifts down from any transient's high-water mark until a little of the
    // rate goes unspent, and settles where the climb and the leak balance.
    const auto idle = s.candidates_empty || !(_rate > 0);
    if (idle || s.throttled) {
        _demand -= _demand * s.dt / _tuning.shares_decay_time;
    } else {
        const auto unspent_fraction = std::min(1.0, _unspent_rate / _rate);
        _demand += unspent_fraction * s.dt / _tuning.shares_rise_time - _demand * s.dt / _tuning.shares_hold_decay_time;
    }
    // Space pressure is a floor: a disk this short of segments needs the CPU whatever the rate
    // deficit says.
    _demand = std::clamp(std::max(_demand, _pressure), 0.0, 1.0);
}

} // namespace replica::logstor
