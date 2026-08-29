/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "types.hh"
#include "segment_stats.hh"
#include "schema/schema_fwd.hh"
#include "utils/chunked_vector.hh"
#include "write_buffer.hh"
#include "utils/log_heap.hh"
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "mutation_writer/token_group_based_splitting_writer.hh"
#include <algorithm>
#include <array>
#include <limits>
#include <ranges>
#include <optional>
#include <vector>
#include <functional>
#include <utility>

namespace replica {
class compaction_group;
} // namespace replica

namespace replica::logstor {

extern seastar::logger logstor_logger;

class primary_index;
struct segment_descriptor;
struct segment_set;
struct separator_buffer;
class segment_ref;
class logstor_group;
class writeable_segment;
class segment_manager_impl;
class compaction_manager_impl;

using split_target_group = std::function<logstor_group&(log_segment_id, dht::token first_token, dht::token last_token)>;

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

// The separator buffer pool is sized for one buffer per compaction group - the one the group writes
// into - plus this reserve, which covers the second buffer a group holds while its flush is writing
// the first one out. It therefore bounds how many groups can be flushing while every other group
// still holds the buffer it writes into; beyond that a group waits for a buffer to come back, which
// is the same back-pressure it already takes when its own flush is in flight.
constexpr size_t separator_flush_reserve = 16;

// The smallest batch worth compacting. A batch reclaims only when its mean utilization is below
// `1 - 1/n_in`, so a smaller batch could not reclaim anything from a disk that is even moderately
// packed. It also anchors the free-segment target's absolute floor.
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

// How many decisions in a row a group has to be found writing less than the threshold before its
// direct write buffers are taken back. One is not enough: a group whose load dips for a moment
// would give its buffers up and take them again, and each round trip leaves a partly filled segment
// behind.
constexpr unsigned direct_underfilled_periods_before_demotion = 2;

// How many flushes of a group's direct write buffers may fail in a row before the group is put back
// on the ordinary path. A failed buffer is kept in memory for good - its records were acknowledged
// and it is the only copy of them left - and its segment is never reclaimed, so a device that keeps
// failing would cost the shard a buffer and a segment every period for as long as the group stays
// hot. The ordinary path reports a write failure to the caller instead of taking that on.
constexpr unsigned direct_flush_failures_before_demotion = 3;

// Whether a group that writes this many bytes is worth giving write buffers of its own. Below the
// threshold - half a segment per sync period - the partly filled segment such a group leaves at the
// end of every period occupies more of the pool than the second write it saves is worth, and the
// ordinary path is the better place for it.
//
// `periods` is how many sync periods the bytes were counted over, which is one unless a controller
// pass ran long enough for the next one to be skipped. The threshold is per period, so a decision
// that covers more of them has to ask for proportionally more.
bool direct_promotion_wanted(uint64_t bytes, uint64_t hot_threshold_bytes, unsigned periods) noexcept;

// Whether a group that has its own write buffers has gone quiet for long enough to take them back.
// `underfilled_decisions` counts the decisions it has been under the threshold for, this one
// included; each of them covers `periods` sync periods.
bool direct_demotion_wanted(uint64_t bytes, uint64_t hot_threshold_bytes, unsigned periods,
        unsigned underfilled_decisions) noexcept;

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

// How much automatic compaction may take on at once, derived from the free-segment target.
// The defaults are the most conservative limits make_compaction_limits() can return, so that a
// value that has not been refreshed yet still admits compaction rather than blocking it.
struct compaction_limits {
    // Automatic compaction jobs kept in flight.
    size_t auto_parallelism = 1;
    // Input segments one job may take.
    size_t batch_cap = min_segments_per_compaction;
};

// A compaction job allocates its output segments as it goes but frees its inputs only when it is
// done, so every job in flight can hold up to `batch_cap` segments of the free-segment target at
// once. Deriving both limits from `low` is what keeps `auto_parallelism * batch_cap <= low`: without
// it, concurrent jobs can consume the whole target and then wait for a segment that only one of them
// could free. `max_batch_cap` is segment_manager_config::max_segments_per_compaction, the upper bound
// the derived cap is clamped to.
//
// The invariant cannot hold on a disk so small that the target is below one batch; there the floor
// of make_free_segment_watermarks() has already been capped by the disk size and a batch of
// `min_segments_per_compaction` is the least that can reclaim anything.
compaction_limits make_compaction_limits(free_segment_watermarks watermarks, size_t max_batch_cap) noexcept;

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

// Picks the segments one compaction job should read, out of the ones a group owns.
//
// The set's free-space histogram is already ordered by ascending utilization, which is near-optimal
// for victim ordering, so the only decision left is where to stop: the candidate set is the
// histogram's first `batch_cap` segments, and select_compaction_prefix() picks a prefix of it.
// Returns nothing when no prefix reclaims a segment, which answers for the whole group rather than
// only for this batch, since a longer prefix would only add fuller segments.
//
// `extension_tolerance` is a parameter only so that the compaction simulator can sweep it; every
// caller in the engine takes the default.
std::optional<compaction_batch> select_compaction_batch(const segment_set& segments,
        uint64_t segment_size, size_t batch_cap,
        double extension_tolerance = compaction_batch_extension_tolerance);

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

inline constexpr log_heap_options segment_descriptor_hist_options(4 * 1024, 3, 128 * 1024);

struct segment_descriptor : public log_heap_hook<segment_descriptor_hist_options> {
    // free_space = segment_size - net_data_size
    // initially set to segment_size
    // when writing records, decrease by total net data size
    // when freeing a record, increase by the record's net data size
    size_t free_space{0};
    size_t record_count{0};
    segment_set* owner{nullptr}; // non-owning, set when added to a segment_set
    int ref_count{0};
    // Position in segment_set::_segment_list, set to no_index when the segment has no owner.
    static constexpr uint32_t no_index = std::numeric_limits<uint32_t>::max();
    uint32_t index_in_set{no_index};

    void reset(size_t segment_size) noexcept {
        free_space = segment_size;
        record_count = 0;
    }

    size_t net_data_size(size_t segment_size) const noexcept {
        return segment_size - free_space;
    }

    void on_write(size_t net_data_size, size_t cnt = 1) noexcept {
        free_space -= net_data_size;
        record_count += cnt;
    }

    void on_write(log_location loc) noexcept {
        on_write(loc.size);
    }

    void on_free(size_t net_data_size, size_t cnt = 1) noexcept {
        free_space += net_data_size;
        record_count -= cnt;
    }

    void on_free(log_location loc) noexcept {
        on_free(loc.size);
    }
};

using segment_descriptor_hist = log_heap<segment_descriptor, segment_descriptor_hist_options>;

struct segment_set {
    // Segments ordered by free space, used to pick the segments to compact.
    segment_descriptor_hist _segments;
    // The same segments, in no particular order. Used to iterate efficiently and safely over all segments.
    utils::chunked_vector<segment_descriptor*> _segment_list;

    // `parent` is the level of the segment statistics rollup this set accounts into, the table of the
    // group owning it, or nothing for a set that stands on its own and is only asked about itself.
    explicit segment_set(uint64_t segment_size, segment_stats_node* parent = nullptr) noexcept
        : _segment_size(segment_size)
        // The segments of a set all belong to one compaction group, so the set is what counts as a
        // group wherever its statistics are summed.
        , _stats(parent, 1) {
        // The utilization of every segment is a fraction of this, so a set without a segment size
        // could not account for anything it holds.
        if (_segment_size == 0) {
            on_fatal_internal_error(logstor_logger, "segment_set created with a zero segment size");
        }
    }

    // Descriptors point back at their set and are linked into its containers, so a set cannot
    // be copied or moved without leaving all of them dangling.
    segment_set(const segment_set&) = delete;
    segment_set& operator=(const segment_set&) = delete;
    segment_set(segment_set&&) = delete;
    segment_set& operator=(segment_set&&) = delete;

    ~segment_set() {
        clear();
    }

    future<> merge(segment_set& other) {
        while (!other.empty()) {
            // Make room before unlinking, so that failing to grow leaves the segment in `other`.
            // Reserving per segment rather than for all of them up front also covers `other`
            // growing while this yields, which link() could not report, being noexcept.
            reserve_one();
            auto& desc = *other._segment_list.back();
            other.unlink(desc);
            link(desc);
            co_await coroutine::maybe_yield();
        }
    }

    // Throws if the set cannot grow, in which case nothing is modified.
    void add_segment(segment_descriptor& desc) {
        if (desc.owner) {
            on_internal_error(logstor_logger, "add_segment called for segment that has an owner");
        }
        reserve_one();
        link(desc);
        ++desc.ref_count;
    }

    // Accounts a record that was freed from a segment of this set. `freed_bytes` is the space the
    // record gave back, which the caller has already added to the free space of the descriptor.
    void update_segment(segment_descriptor& desc, uint64_t freed_bytes) noexcept {
        const auto live_bytes = desc.net_data_size(_segment_size);
        _stats.free_from_segment(freed_bytes,
                utilization_bucket_of(live_bytes + freed_bytes, _segment_size),
                utilization_bucket_of(live_bytes, _segment_size));
        _segments.adjust_up(desc);
    }

    void remove_segment(segment_descriptor& desc) noexcept {
        unlink(desc);
        --desc.ref_count;
    }

    // unlink all segments for shutdown.
    // don't decrement ref_count because the segments are still owned
    // by this group and we don't want to free them.
    void clear() noexcept {
        while (!empty()) {
            unlink(*_segment_list.back());
        }
    }

    size_t segment_count() const noexcept {
        return _segment_list.size();
    }

    // Bytes of the live records held by the segments of this set, which is the data the group owns
    // rather than the space it takes: the rest of the space the segments hold is dead records, which
    // compaction will reclaim.
    uint64_t live_bytes() const noexcept {
        return stats().live_bytes;
    }

    bool empty() const noexcept {
        return _segment_list.empty();
    }

    // What this set holds, its distribution by utilization included, and one group - itself. Kept up
    // to date as segments are linked, unlinked and freed from, so reading it is O(1) and exact, unlike
    // a scan of the set, which would have to yield and could miss segments that move while it does.
    // The same change is accounted into the levels above this set, see segment_stats_node.
    const segment_stats& stats() const noexcept {
        return _stats.stats();
    }

    // Recomputes the statistics from the segments themselves, for a test to check the maintained ones
    // against. Every path that changes the set has to leave the two equal.
    segment_stats recompute_stats_for_test() const noexcept {
        segment_stats stats;
        stats.group_count = 1;
        for (const auto* desc : _segment_list) {
            const auto live_bytes = desc->net_data_size(_segment_size);
            ++stats.segment_count;
            stats.live_bytes += live_bytes;
            ++stats.utilization[utilization_bucket_of(live_bytes, _segment_size)];
        }
        return stats;
    }

private:
    // The size of a segment, needed to turn the free space of a descriptor into a utilization.
    uint64_t _segment_size;
    // What this set holds, and the levels above it that the same is accounted into.
    segment_stats_node _stats;

    // Makes room for one more segment, so that the following link() cannot fail. Only useful when
    // nothing can be added to the set in between, since the room is not reserved for a caller.
    void reserve_one() {
        static constexpr size_t min_reservation = 16;
        static constexpr size_t chunk_capacity = decltype(_segment_list)::max_chunk_capacity();
        const size_t capacity = _segment_list.capacity();
        if (_segment_list.size() < capacity) {
            return;
        }
        // While the list is under one chunk, grow geometrically: reserving a single element
        // reallocates the partially filled last chunk and migrates it, which would make appending
        // segments quadratic. Once the capacity is a whole number of chunks there is nothing to
        // migrate, so add exactly one chunk. Clamping to chunk_capacity keeps the capacity chunk
        // aligned, which is what makes the following reservations migration free.
        _segment_list.reserve(capacity < chunk_capacity
                ? std::min(std::max(capacity * 2, min_reservation), chunk_capacity)
                : capacity + chunk_capacity);
    }

    // The caller must have made room for the segment, see reserve_one() and merge().
    // Linking must not fail, or the segment would be left owned by a set that doesn't hold it.
    void link(segment_descriptor& desc) noexcept {
        _segment_list.push_back(&desc);
        desc.owner = this;
        desc.index_in_set = _segment_list.size() - 1;
        _segments.push(desc);
        const auto live_bytes = desc.net_data_size(_segment_size);
        _stats.add_segment(live_bytes, utilization_bucket_of(live_bytes, _segment_size));
    }

    // Validates the invariants of every removal path, and aborts rather than throwing, both
    // because it cannot leave the set half-updated and because it runs from the destructor.
    void unlink(segment_descriptor& desc) noexcept {
        if (desc.owner != this) {
            on_fatal_internal_error(logstor_logger, "unlinking a segment from a set that is not its owner");
        }
        if (desc.index_in_set >= _segment_list.size() || _segment_list[desc.index_in_set] != &desc) {
            on_fatal_internal_error(logstor_logger, "segment is not at its recorded position in its set");
        }
        _segments.erase(desc);
        const auto live_bytes = desc.net_data_size(_segment_size);
        _stats.remove_segment(live_bytes, utilization_bucket_of(live_bytes, _segment_size));
        // Keep the list compact by moving the last segment into the freed slot.
        auto* last = _segment_list.back();
        _segment_list[desc.index_in_set] = last;
        last->index_in_set = desc.index_in_set;
        _segment_list.pop_back();
        desc.owner = nullptr;
        desc.index_in_set = segment_descriptor::no_index;
    }
};

class segment_ref {
    struct state {
        log_segment_id id;
        std::function<void()> on_last_release;
        std::function<void()> on_failure;
        bool flush_failure{false};
        ~state() {
            if (!flush_failure) {
                if (on_last_release) on_last_release();
            } else {
                if (on_failure) on_failure();
            }
        }
    };
    lw_shared_ptr<state> _state;
public:
    segment_ref() = default;

    // Copyable: copying increments the shared ref count
    segment_ref(const segment_ref&) = default;
    segment_ref& operator=(const segment_ref&) = default;
    segment_ref(segment_ref&&) noexcept = default;
    segment_ref& operator=(segment_ref&&) noexcept = default;

    log_segment_id id() const noexcept { return _state->id; }
    bool empty() const noexcept { return !_state; }

    void set_flush_failure() noexcept { if (_state) _state->flush_failure = true; }

private:
    friend class segment_manager_impl;
    explicit segment_ref(log_segment_id id, std::function<void()> on_last_release, std::function<void()> on_failure)
        : _state(make_lw_shared<state>(id, std::move(on_last_release), std::move(on_failure)))
    {}
};

struct separator_buffer {
    owned_write_buffer buf;
    utils::chunked_vector<segment_ref> held_segments;
    std::optional<segment_sequence> min_seq_num;

    separator_buffer() = default;

    separator_buffer(const separator_buffer&) = delete;
    separator_buffer& operator=(const separator_buffer&) = delete;

    separator_buffer(separator_buffer&&) noexcept = default;
    separator_buffer& operator=(separator_buffer&&) noexcept = default;

    ~separator_buffer();

    template <log_record_writer_concept Writer>
    void write(segment_ref seg_ref, std::optional<segment_sequence> segment_seq_num, Writer writer,
            primary_index& index, log_location prev_location) {
        // The separator buffer holds a reference to the source segment until its updates are durable.
        if (held_segments.empty() || held_segments.back().id() != seg_ref.id()) {
            held_segments.push_back(std::move(seg_ref));
        }

        if (segment_seq_num && (!min_seq_num || *segment_seq_num < *min_seq_num)) {
            min_seq_num = *segment_seq_num;
        }

        // A separator buffer is a full segment buffer bound to a segment by the flush that writes it
        // out, so a record in it is located from where the buffer landed. The append says where in
        // the buffer that is, and the index update the record owes waits with the buffer rather than
        // on a future of its own.
        const auto appended = buf->append_synchronously(writer);
        buf->add_index_update(separator_index_update{
            .index = &index,
            .key = std::move(writer).take_key(),
            .prev_location = prev_location,
            .offset_in_buffer = appended.record_header_offset,
            .size = appended.total_size,
        });
    }

    bool allocated() const noexcept {
        return bool(buf);
    }

    template <log_record_writer_concept Writer>
    bool can_fit(const Writer& writer) const noexcept {
        return buf && buf->can_fit(writer);
    }

    bool can_fit(size_t write_size) const noexcept {
        return buf && buf->can_fit(write_size);
    }

    bool empty() const noexcept {
        return !buf || !buf->has_data();
    }

    size_t offset_in_buffer() const noexcept {
        return buf ? buf->offset_in_buffer() : 0;
    }

    future<> close() {
        return buf ? buf->close() : make_ready_future<>();
    }

    // Gives the write buffer back to the pool and drops what belonged to it, which releases the
    // source segments it was holding. The buffer is expected to have been flushed, or aborted,
    // before this.
    future<> release();

    future<> abort(std::exception_ptr);
};

// A write buffer bound to a segment of the group from the moment it is allocated, which is what
// lets a write into it be placed without a second pass. A full segment holds exactly one buffer, at
// offset zero, so the final location of a record follows from the segment and the record's offset
// in the buffer as soon as it is appended - the index can be updated with it there and then, and
// the write acknowledged, rather than after the buffer has reached the disk. The buffer is written
// out and its segment linked into the group when the buffer fills or its deadline expires.
//
// The segment is unwritten and belongs to no segment set while the buffer fills, exactly like the
// output of the separator or of a compaction, so nothing can pick it, scan it or free it before it
// holds what the index says it holds.
struct direct_write_buffer {
    owned_write_buffer buf;
    // The segment this buffer is going to be written into. writeable_segment is private to
    // segment_manager.cc, which is where the special members below are defined.
    seastar::lw_shared_ptr<writeable_segment> seg;
    // When the first record went into the buffer, which is what the sync deadline is measured from.
    seastar::lowres_clock::time_point first_append{};

    direct_write_buffer() noexcept;
    direct_write_buffer(direct_write_buffer&&) noexcept;
    direct_write_buffer& operator=(direct_write_buffer&&) noexcept;
    ~direct_write_buffer();

    direct_write_buffer(const direct_write_buffer&) = delete;
    direct_write_buffer& operator=(const direct_write_buffer&) = delete;

    // Whether the buffer can take records: it needs both a buffer and a segment to put it in.
    bool bound() const noexcept {
        return buf && seg;
    }

    // Whether it holds records that have not reached the disk.
    bool empty() const noexcept {
        return !buf || !buf->has_data();
    }
};

class compaction_reenabler {
    std::function<void()> _release;
public:
    compaction_reenabler() = default;
    explicit compaction_reenabler(std::function<void()> release)
        : _release(std::move(release)) {}
    ~compaction_reenabler() { if (_release) _release(); }

    compaction_reenabler(compaction_reenabler&&) = default;
    compaction_reenabler& operator=(compaction_reenabler&&) = default;
    compaction_reenabler(const compaction_reenabler&) = delete;
    compaction_reenabler& operator=(const compaction_reenabler&) = delete;
};

class compaction_manager {
public:
    virtual ~compaction_manager() = default;

    virtual future<> flush_separator_buffer(separator_buffer&, logstor_group&) = 0;

    virtual future<owned_write_buffer> allocate_separator_buffer() = 0;

    // A group owns its direct write buffers, but the segments they are bound to and the pool the
    // buffers come from belong to the segment manager, so the group asks for them to be moved.
    // Rotating hands the full buffer over to be written out in the background and leaves the group
    // writing into its spare; it requires that no flush of this group is already in flight.
    virtual void rotate_direct_buffer(logstor_group&) = 0;
    // Gives both of the group's direct buffers and the segments they are bound to back. The buffers
    // are expected to have been flushed first, since their records are nowhere else.
    virtual future<> release_direct_buffers(logstor_group&) = 0;
    // Gives the group its buffers now, rather than waiting for the controller to find that it
    // writes fast enough to deserve them.
    virtual future<> promote_direct_writes_for_test(logstor_group&) = 0;

    virtual void add(logstor_group&) = 0;
    virtual future<> remove(logstor_group&) = 0;

    // Whether the group is registered here, and therefore whether this manager still holds a pointer
    // to it. Lets an owner of a group check that it was removed before the group is destroyed.
    virtual bool contains(logstor_group&) const noexcept = 0;

    // The shard level of the segment statistics rollup, which the tables of this shard account into.
    // Its statistics are those of the segments owned by the groups of every table on the shard.
    virtual segment_stats_node& shard_segment_stats() noexcept = 0;

    virtual void submit(logstor_group&) = 0;

    virtual future<> submit_split_compaction(logstor_group& src, mutation_writer::classify_by_token_group, split_target_group) = 0;

    virtual future<> stop_ongoing_compactions(logstor_group&) = 0;

    virtual future<compaction_reenabler> disable_compaction(logstor_group&) = 0;
    virtual compaction_reenabler disable_compaction_no_wait(logstor_group&) = 0;
};

// A compaction group in logstor.
//
// The group owns a `segment_set` of segments whose records all belong to it, and therefore to a
// single table and token range. Compaction rewrites the live records of a batch of those segments
// into new segments of the same group.
//
// The group is also the target of a write: the separator hands it the records of the shared active
// segment that belong to it, and buffers them in its separator buffer, which is flushed into a new
// segment in the group.
class logstor_group {
    segment_set _logstor_segments;
    separator_buffer _active_buffer;
    separator_buffer _flushing_buffer;
    uint64_t _separator_generation{1};
    shared_future<> _separator_flush{make_ready_future<>()};
    // Whether the separator may write to this group. The group must be registered with the compaction
    // manager in order for it to be enabled.
    bool _separator_enabled{false};

    // The direct write path. The group writes into _direct_active and rotates into _direct_spare
    // when that fills, so a write never has to wait for a buffer. _direct_flush is the one
    // asynchronous step the direct path of this group may have in flight - a flush, a bind or a
    // release - and everything that has to see the group settled waits on it.
    direct_write_buffer _direct_active;
    direct_write_buffer _direct_spare;
    shared_future<> _direct_flush{make_ready_future<>()};
    // Whether writes of this group go into its own buffer instead of the shared active segment.
    bool _direct_enabled{false};
    // What the group has written since the controller last looked, which is what it promotes and
    // demotes the group on. Fed by every write of the group, direct or not. The controller knows
    // how many sync periods that covers, which is one unless a pass of it ran long.
    uint64_t _direct_bytes_this_period{0};
    unsigned _direct_underfilled_periods{0};
    // Flushes of this group's buffers that failed in a row, see
    // direct_flush_failures_before_demotion.
    unsigned _direct_flush_failures{0};

    void switch_active_separator_buffer();

    future<> allocate_active_separator_buffer();

    // The part of write_to_separator() that waits: for a buffer to be allocated to the group, or for
    // the flush of the buffer that is full to finish. Split out so that a record the active buffer
    // takes as it is pays no coroutine frame.
    template <log_record_writer_concept Writer>
    future<> wait_and_write_to_separator(Writer, segment_ref, std::optional<segment_sequence>, log_location prev_location);

    // Waits until nothing is in flight on the group's direct path.
    future<> await_direct_settled();

protected:
    // `table_stats` is the level of the segment statistics rollup of the table this group belongs to,
    // which the segments of the group are accounted into.
    explicit logstor_group(uint64_t segment_size, segment_stats_node* table_stats) noexcept
        : _logstor_segments(segment_size, table_stats) {
    }

    virtual compaction_manager& logstor_compaction_manager() noexcept = 0;

public:
    virtual ~logstor_group() = default;

    virtual table_id table_id() const noexcept = 0;

    virtual primary_index& logstor_index() noexcept = 0;
    virtual const primary_index& logstor_index() const noexcept = 0;

    // The statistics of the segments this group owns are read through the set, see
    // segment_set::stats(). They are already included in those of the table this group belongs to,
    // which is where a caller that wants a whole table reads them rather than summing its groups.
    segment_set& logstor_segments() noexcept {
        return _logstor_segments;
    }
    const segment_set& logstor_segments() const noexcept {
        return _logstor_segments;
    }

    void add_logstor_segment(segment_descriptor& desc) {
        _logstor_segments.add_segment(desc);
    }

    void clear_segments() {
        _logstor_segments.clear();
    }

    template <log_record_writer_concept Writer>
    // `prev_location` is where the record being rewritten is now, which is what the index entry the
    // group owes an update for has to be pointing at for the rewrite to take effect.
    future<> write_to_separator(Writer, segment_ref, std::optional<segment_sequence>, log_location prev_location);

    future<> flush_separator(std::optional<segment_sequence> seq_num = std::nullopt);

    // Lets the separator write to this group. Called by the compaction manager when the group is
    // registered with it, which is what makes the group's buffers flushable.
    void enable_separator_writes() noexcept {
        _separator_enabled = true;
    }

    // Stops taking separator writes and gives up what the separator has already buffered for this
    // group, handing its buffers back to the pool. What is dropped here was never written out, so the
    // source segments the buffers hold are marked as failed rather than released: the index still
    // points at those records there. A group is expected to have been flushed before it is closed, so
    // anything this finds costs the segments it is holding, and it says so.
    future<> close_separator();

    // Writes out what the group has taken directly but not yet put on disk, and waits for it to be
    // in a segment of the group. This is the drain every tablet operation goes through.
    future<> flush_direct_writes();

    // Waits for the flush the group already has in flight, so that a write which found the group's
    // buffer full can offer its record again instead of taking the ordinary path, which would write
    // it to the disk twice. Fails once `timeout` has passed, which the caller answers by taking the
    // ordinary path after all - with the same timeout, which reports it.
    future<> await_direct_flush(db::timeout_clock::time_point timeout);

    // Stops taking direct writes, flushes what is already buffered and gives the buffers and their
    // segments back. Unlike close_separator() this flushes rather than discards: a record here is
    // nowhere else, and the index already points at the segment the buffer has yet to be written
    // into, so dropping it would lose it outright.
    future<> close_direct_writes();

    bool empty() const noexcept {
        return _logstor_segments.empty()
            && _active_buffer.empty() && _flushing_buffer.empty()
            && _separator_flush.available()
            && _direct_active.empty() && _direct_spare.empty()
            && _direct_flush.available();
    }

    bool separator_has_data() const noexcept {
        return !_active_buffer.empty() || !_flushing_buffer.empty();
    }

    size_t separator_held_segment_count() const noexcept {
        return _active_buffer.held_segments.size() + _flushing_buffer.held_segments.size();
    }

    bool direct_writes_enabled() const noexcept {
        return _direct_enabled;
    }

    bool direct_has_data() const noexcept {
        return !_direct_active.empty() || !_direct_spare.empty();
    }

    // Whether the group actually has a buffer to write into. A group can be taking direct writes
    // and have none - the shard had nothing to give its last flush - and until it is given one it
    // writes the ordinary way, see try_write_direct().
    bool direct_writes_bound() const noexcept {
        return _direct_active.bound();
    }

    // The direct write path is driven from the segment manager, which owns the buffers' segments
    // and decides which groups are hot enough to have them.
    friend class segment_manager_impl;
    friend class compaction_manager_impl;
};

} // namespace replica::logstor
