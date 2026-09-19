/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>

// How automatic compaction decides when to run, how much to take on at once, and which segments
// to compact. None of it depends on how segments or compaction are implemented, so that tuning it
// rebuilds only the files that use it.

namespace replica::logstor {

struct segment_descriptor;
class segment_set;

// The number of compaction jobs that may run concurrently on a shard, counted in output buffers:
// a normal compaction takes one, a split compaction takes two. It sizes the compaction buffer pool,
// which is what enforces it, and the segment pool's compaction reserve, which is what keeps an
// output segment available for every concurrent flush.
constexpr size_t max_compaction_parallelism = 8;

// The buffers a split compaction takes, and therefore the slots automatic compaction leaves free so
// that a tablet split is never queued behind it.
constexpr size_t split_compaction_buffers = 2;

// The largest number of automatic compactions that can be in flight at once. The dynamic limit is
// derived per disk by make_compaction_limits(); this is the bound the semaphore is sized for.
constexpr size_t max_auto_compaction_parallelism = std::min<size_t>(4, max_compaction_parallelism - split_compaction_buffers);

// The smallest batch worth compacting. A batch reclaims only when its mean utilization is below
// `1 - 1/n_in`, so a smaller batch could not reclaim anything from a disk that is even moderately
// packed. It also anchors the minimum of the space floor.
constexpr size_t min_segments_per_compaction = 8;

// How much reclamation efficiency a batch may give up in exchange for being larger. On its own the
// efficiency score picks short batches, which is per-job overhead for no write-amplification
// benefit; extending within this tolerance trades about 1% of efficiency for half as many jobs.
constexpr double compaction_batch_extension_tolerance = 0.8;

// How much of the best batch's reclamation efficiency a batch has to keep to be started while
// another compaction job is already running. The first job is always admitted, so this bounds the
// marginal write amplification that parallelism may buy throughput at, rather than being able to
// stall compaction. When the groups of a shard hold comparable garbage every candidate is close to
// the best one and the gate never fires; it is unequal groups it exists for, see
// run_auto_compaction().
//
// The bar has to be relative. An absolute floor of one segment reclaimed per segment copied refuses
// everything a disk at 75% utilization can offer, where even the best batch is below it, and
// compaction would stop instead of running at the efficiency the disk allows.
constexpr double compaction_marginal_admission_ratio = 0.75;

// Watermarks, in available segments, that drive automatic compaction. It starts once the number of
// available segments drops below `low` - the free-segment target - and stops once it is back at
// `high`. Both are zero when the trigger is disabled.
struct free_segment_watermarks {
    uint64_t low;
    uint64_t high;
};

// The free-segment target is a fraction of the disk, which is the dominant write-amplification
// knob, with an absolute floor that keeps the target meaningful on small disks, where a fraction of
// the disk rounds down to a segment or two.  `target_fraction` is
// logstor_compaction_trigger_threshold; 0 disables the trigger.
free_segment_watermarks make_free_segment_watermarks(uint64_t segment_count, double target_fraction) noexcept;

// Whether the free-segment level wants automatic compaction running, given whether it is running
// now. The two watermarks are a hysteresis band: compaction starts once the free-segment target is
// breached and runs until the disk is back at the stop watermark, rather than stopping again at the
// first write that crosses back over the target. Both watermarks are zero when the trigger is
// disabled, which answers "no" at any number of available segments.
//
// This is only the space half of the decision; whether compaction may run at all is the caller's.
bool auto_compaction_wanted(bool running, uint64_t available_segments, free_segment_watermarks watermarks) noexcept;

// The space floor: the free segments that compaction keeps for itself. It is what its jobs in
// flight may hold, and the level of free segments at which space pressure has fully overridden the
// utilization threshold that decides what is worth compacting, see compaction_space_pressure(). A
// thirty-second of the disk, with a minimum of two batches so that it stays meaningful on a small
// disk, where a fraction rounds down to a segment or two. Not capped above: on a large disk the
// space floor is the room the write path has to absorb a burst while compaction catches up, and a
// few seconds of writes is not much room.
uint64_t compaction_space_floor(uint64_t segment_count) noexcept;

// How much automatic compaction may take on at once, derived from the space floor.
// The defaults are the most conservative limits make_compaction_limits() can return, so that a
// value that has not been computed yet still admits compaction rather than blocking it.
struct compaction_limits {
    // Automatic compaction jobs kept in flight.
    size_t auto_parallelism = 1;
    // Input segments one job may take.
    size_t batch_cap = min_segments_per_compaction;
};

// A compaction job allocates its output segments as it goes but frees its inputs only when it is
// done, so every job in flight can hold up to `batch_cap` segments of the space floor at once.
// Deriving both limits from the space floor is what keeps
// `auto_parallelism * batch_cap <= space_floor`: without it, concurrent jobs can consume the whole
// space floor and then wait for a segment that only one of them could free. `max_batch_cap` is
// segment_manager_config::max_segments_per_compaction, the upper bound the derived cap is clamped
// to; on a disk whose space floor is above what the largest allowed set of jobs can hold, the
// limits are simply the largest allowed.
//
// The invariant cannot hold on a disk so small that the space floor is below one batch; there the
// minimum of compaction_space_floor() has already been capped by the disk size and a batch of
// `min_segments_per_compaction` is the least that can reclaim anything.
compaction_limits make_compaction_limits(uint64_t space_floor, size_t max_batch_cap) noexcept;
// The same, with the free-segment target as the budget. Drives the engine until the threshold
// controller does.
compaction_limits make_compaction_limits(free_segment_watermarks watermarks, size_t max_batch_cap) noexcept;

// The utilization threshold as a function of the free level: a piecewise-linear curve through
// three points, in available segments.
//
//     available <= space floor          -> 1         any batch with a net gain; the throttle is off
//     space floor < available < anchor  -> 1 .. u    space pressure relaxes the threshold
//     available == anchor               -> u         the configured threshold
//     anchor < available < ceiling      -> u .. 0    more free space lowers it
//     available >= ceiling              -> 0         only dead segments qualify
//
// The slope of the curve is what selects the policy. Flat above the anchor - no ceiling - it is a
// threshold on the segment: the victim utilization is fixed at `u`, write amplification is a
// property of `u` alone, and the free level floats with what the disk holds. Steep - the ceiling
// just above the anchor - it holds the free level at the anchor whatever the disk holds, choosing
// at every utilization the threshold that leaves that much free, which is what the free-segment
// target the engine used to hold did, made continuous. Everything between is a soft target, and the
// default ceiling at a quarter of the disk is one: the level settles above the anchor by less, the
// steeper the curve. It is proportional
// control of the free level with the threshold as the actuator - lowering the threshold when the disk
// is emptier compacts less, garbage accumulates and the free level falls - with a unique equilibrium
// at any slope, since `u_mean(u) = (1 - u) / ln(1 / u)` rises as `u` rises. It has no integral term,
// so nothing to wind up or reset.
//
// A fixed threshold caps the utilization a disk can run at: with compaction taking only segments
// below `u` the occupied ones average `u_mean(u)` - 0.72 at `u = 0.5` - and the disk runs out of free
// segments once its utilization passes roughly 0.70, full of half-used segments the threshold
// refuses. The segment of the curve below the anchor is what keeps such a disk alive.
struct utilization_threshold_curve {
    uint64_t space_floor;
    uint64_t anchor;
    // 0 for no ceiling: the threshold stays at `threshold` above the anchor.
    uint64_t ceiling;
    // The configured threshold, logstor_compaction_utilization_threshold. Anything outside [0, 1)
    // disables automatic compaction.
    double threshold;

    bool operator==(const utilization_threshold_curve&) const noexcept = default;
};

// The curve for a disk of `segment_count` segments with compaction_space_floor() `space_floor`,
// from the configuration: `anchor_fraction` and `ceiling_fraction` are fractions of the disk, 0 for
// an anchor at twice the space floor and for no ceiling, which is the flat curve. The anchor is
// kept above the space floor and the ceiling above the anchor whatever the configuration says.
utilization_threshold_curve make_utilization_threshold_curve(uint64_t segment_count, uint64_t space_floor,
        double threshold, double anchor_fraction, double ceiling_fraction) noexcept;

// How short of segments the write path is, in [0, 1]: 0 at the anchor or above, 1 at the space
// floor or below, linear in between. Below the anchor the threshold relaxes towards one, the
// minimum batch is one and the shares demand is floored at the pressure; at 1 the throttle is off
// as well.
double compaction_space_pressure(uint64_t available_segments, const utilization_threshold_curve&) noexcept;

// The fewest candidate segments a job is run on: min_segments_per_compaction while the disk is not
// short of space, so that a segment crossing the threshold waits for a batch's worth of company
// rather than being compacted on its own, falling linearly with the space pressure to one at the
// space floor, where waiting is not affordable. Under a threshold that runs continuously the
// candidates of a group arrive one at a time, and measured without this a job averaged under three
// segments. The fall is gradual because a disk past the threshold's ceiling settles inside the
// pressure band, so a step to one at the anchor would make single-segment jobs its normal state.
size_t effective_min_batch(double pressure) noexcept;

// The utilization threshold compaction actually applies at this free level: the curve, evaluated.
double effective_utilization_threshold(uint64_t available_segments, const utilization_threshold_curve&) noexcept;

// The cost of one candidate compaction batch: `n_in` segments are read and rewritten into `n_out`
// segments, copying `live_bytes` of live data.
struct compaction_candidate_score {
    size_t n_in;
    size_t n_out;
    uint64_t live_bytes;

    size_t reclaimed() const noexcept {
        return n_in > n_out ? n_in - n_out : 0;
    }

    // Segments reclaimed per segment-worth of data copied, which is exactly the reciprocal of the
    // batch's marginal write amplification. Comparisons cancel the segment_size factor, so it is
    // needed only where the absolute value is wanted, i.e. for logging.
    double efficiency(uint64_t segment_size) const noexcept {
        return live_bytes == 0
                ? std::numeric_limits<double>::infinity()
                : double(reclaimed()) * double(segment_size) / double(live_bytes);
    }

    // Whether this batch's efficiency is at least `fraction` of `other`'s.
    bool efficiency_at_least(const compaction_candidate_score& other, double fraction) const noexcept {
        if (live_bytes == 0) {
            return true;
        }
        if (other.live_bytes == 0) {
            return false;
        }
        return double(reclaimed()) * double(other.live_bytes)
                >= fraction * double(other.reclaimed()) * double(live_bytes);
    }

    bool operator==(const compaction_candidate_score& other) const noexcept = default;

    // Orders batches by reclamation efficiency, so that the score is the reciprocal of the write
    // amplification the batch would incur, and ties by how much the batch reclaims.
    bool operator<(const compaction_candidate_score& other) const noexcept;
};

// The batch select_compaction_batch() chose: the segments to compact, least utilized first, and the
// score of the batch as a whole - which is also the score its group is ranked by, so that the batch
// that wins the ranking is the one that would run.
struct compaction_batch {
    std::vector<const segment_descriptor*> segments;
    compaction_candidate_score score;
};

// What bounds the batch select_compaction_batch() picks.
struct compaction_batch_limits {
    // Input segments one job may take, compaction_limits::batch_cap.
    size_t batch_cap;
    // The fewest candidate segments a group has to hold for a batch to be picked at all: a job has
    // fixed costs - a scan, a barrier on the reads in flight, a group serialized behind it, an
    // output segment sealed part full - so a job of two segments is not worth running when the
    // disk is not short of space. See effective_min_batch().
    size_t min_batch = 1;
    // Only a segment whose utilization is at most this is a candidate: the effective utilization
    // threshold, see effective_utilization_threshold(). 1 makes every segment a candidate.
    double max_utilization = 1;
    // A parameter only so that the compaction simulator can sweep it; every caller in the engine
    // takes the default.
    double extension_tolerance = compaction_batch_extension_tolerance;
};

// Picks the segments one compaction job should read, out of the ones a group owns.
//
// The set's free-space histogram is already ordered by ascending utilization, which is near-optimal
// for victim ordering, so the only decision left is where to stop: the candidate set is the
// histogram's prefix of segments under the utilization threshold, at most `batch_cap` of them, and
// select_compaction_prefix() picks a prefix of that. Returns nothing when no prefix reclaims a
// segment, which answers for the whole group rather than only for this batch, since a longer prefix
// would only add fuller segments.
std::optional<compaction_batch> select_compaction_batch(const segment_set& segments,
        uint64_t segment_size, const compaction_batch_limits& limits);

// The best `capacity` compaction candidates seen so far, ranked by compaction_candidate_score.
//
// Kept as a min heap, so that a candidate that cannot make the set costs a single comparison, which
// is what the scan over a shard's groups does for all but a few of them.
//
// `Candidate` is anything carrying a `score`: the compaction manager ranks its groups by the batch
// each would run, and the compaction simulator (test/manual/logstor_compaction_sim.cc) ranks its own
// groups with the same code.
template <typename Candidate>
requires requires (const Candidate& c) { { c.score } -> std::convertible_to<compaction_candidate_score>; }
class top_compaction_candidates {
    std::vector<Candidate> _candidates;
    size_t _capacity;

    static bool worse(const Candidate& lhs, const Candidate& rhs) noexcept {
        return rhs.score < lhs.score;
    }

public:
    explicit top_compaction_candidates(size_t capacity)
        : _capacity(capacity) {
        _candidates.reserve(capacity);
    }

    void add(Candidate candidate) {
        if (_candidates.size() < _capacity) {
            _candidates.push_back(std::move(candidate));
            std::ranges::push_heap(_candidates, worse);
        } else if (!_candidates.empty() && _candidates.front().score < candidate.score) {
            std::ranges::pop_heap(_candidates, worse);
            _candidates.back() = std::move(candidate);
            std::ranges::push_heap(_candidates, worse);
        }
    }

    // The candidates ranked worst first, so that a caller taking them one at a time from the back
    // takes the best one first.
    std::vector<Candidate> take() && {
        std::ranges::sort(_candidates, std::less<>{}, &Candidate::score);
        return std::move(_candidates);
    }
};

// What compaction_marginal_admission_ratio measures a candidate against: the best batch of a
// ranking that copies anything, out of `candidates` ranked worst first as top_compaction_candidates
// leaves them. Nothing when every candidate copies nothing, which admits them all.
//
// A batch of fully dead segments copies nothing and so reclaims at infinite efficiency, which no
// batch that copies can be a fraction of. Letting one set the bar would refuse every marginal job
// exactly when reclaiming is at its cheapest.
template <typename Candidate>
requires requires (const Candidate& c) { { c.score } -> std::convertible_to<compaction_candidate_score>; }
std::optional<compaction_candidate_score> marginal_admission_bar(const std::vector<Candidate>& candidates) noexcept {
    for (const auto& candidate : candidates | std::views::reverse) {
        if (candidate.score.live_bytes != 0) {
            return candidate.score;
        }
    }
    return std::nullopt;
}

// Where the free-segment target falls on the compaction_shares_pressure() ramp. It is a property of
// the watermarks rather than a tunable: with the relative hysteresis of make_free_segment_watermarks()
// the target sits a third of the way up the ramp on any disk size, up to the rounding of the two
// watermarks to whole segments. Exported so that the shares controller can put a control point on it.
constexpr float compaction_shares_pressure_at_target = 1.0f / 3.0f;

// Space pressure driving the compaction shares controller: 0 at or above the high watermark, where
// automatic compaction stops and there is no space demand at all, compaction_shares_pressure_at_target
// at the free-segment target - the intended steady-state operating point - and 1 once half the
// target has been consumed, linear in between.
//
// The free-segment count is the integral of the mismatch between the write rate and the rate at
// which compaction reclaims, so mapping it proportionally to shares is integral control on that
// mismatch: the loop settles at whatever shares match the write rate, and the slope only decides
// where in the free-segment range it settles. That is why pressure saturates while half the target
// is still in hand instead of at the point where writes stall: a workload that needs the maximum
// shares must be able to settle somewhere that still leaves the write path room.
float compaction_shares_pressure(uint64_t available_segments, free_segment_watermarks watermarks) noexcept;

} // namespace replica::logstor
