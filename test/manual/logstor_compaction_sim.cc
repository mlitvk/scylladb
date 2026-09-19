/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// A simulator of logstor compaction.
//
// It drives the engine's own segment selection over a modeled segment pool, so that the effect of a
// parameter - the free-segment target, the batch cap, the disk size, the workload - can be measured
// in write amplification without running a cluster.
//
// What is the real thing, linked from replica/logstor:
//
//   - segment_descriptor, segment_set and its log_heap free-space histogram, so that victims are
//     ordered exactly as they are in the engine, the bucketing of the histogram included;
//   - select_compaction_batch(), and with it the efficiency score, the batch extension tolerance and
//     estimate_required_segments();
//   - top_compaction_candidates, which ranks the groups;
//   - make_free_segment_watermarks() and make_compaction_limits(), so that the free-segment target,
//     the batch cap and the parallelism are derived per disk as they are in the engine;
//   - auto_compaction_wanted(), which decides when compaction runs;
//   - the ondisk:: sizes and alignments, so that what a segment holds and what it wastes are real.
//
// What is modeled here, because the engine's version of it is IO or future bound: the primary index
// (a key to location map), the compaction job, the driver that keeps jobs in flight, the segment
// pool, and the write path.
//
// The write path is deliberately abstracted. In the engine a user record lands in a shared mixed
// active segment and the separator then copies it into a full segment of its compaction group; here
// it goes straight into an open segment of its group, as if the separator were instantaneous. That
// leaves out the mixed segments and the separator buffers, which cost space and one more copy of
// every user byte, but not the part compaction sees: what the group's segments hold, how they are
// laid out, and how they age. The device therefore writes one copy more per user byte than the
// device write amplification reported below.
//
// The simulation has no clock. Compaction runs whenever the free-segment level asks for it and is
// taken to keep up, so what is measured is the steady state at the free level the watermarks
// produce. Compaction falling behind is a property of the device and of the shares controller and is
// out of scope; --free-band pins the free level, which is how configurations that reclaim at
// different rates are compared at the same U_eff.
//
// Examples:
//
//   logstor_compaction_sim --utilization 0.75 --trigger-threshold 0.1
//   logstor_compaction_sim --free-band 1 --utilization 0.85 --batch-cap 8

#include <seastar/core/align.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "replica/logstor/compaction.hh"
#include "replica/logstor/compaction_policy.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/types.hh"
#include "replica/logstor/write_buffer.hh"

using namespace replica::logstor;
using namespace seastar;

namespace {

// The fixed part of a record header as the simulator models it: the engine's fixed fields, and the
// length of the one partition key component, which the key's representation holds ahead of the
// key_size bytes of the key itself.
constexpr size_t record_header_fixed_size = ondisk::record_header_fixed_size + sizeof(uint16_t);

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

enum class workload_kind {
    uniform,
    zipf,
    hot_cold,
};

enum class value_size_kind {
    fixed,
    uniform,
    lognormal,
};

// Which prefix of the candidate segments a job takes. `efficiency` is the implemented rule and runs
// the engine's own select_compaction_batch(); the other two are simulator-local reimplementations of
// alternative rules, and exist to size what the implemented one buys.
enum class strategy_kind {
    efficiency,
    absolute,
    random_victim,
};

struct sim_params {
    uint64_t disk_size = 128 * 1024 * 1024;
    uint64_t segment_size = default_segment_size;
    uint64_t file_size = default_file_size;
    size_t groups = 1;
    // How unevenly the keys are spread over the groups: 0 spreads them evenly, higher values give
    // the earlier groups proportionally more, as a shard carrying tablets of unequal size would.
    double group_skew = 0;

    // Live bytes as a fraction of the segment pool.
    double utilization = 0.75;
    double trigger_threshold = 0.05;
    // Replaces the hysteresis band with a fixed number of segments, so that configurations which
    // reclaim at different rates are still compared at the same free level, and therefore at the
    // same U_eff. 0 keeps the band make_free_segment_watermarks() derives.
    uint64_t free_band = 0;
    // segment_manager_config::max_segments_per_compaction, the bound the derived cap is clamped to.
    size_t batch_cap = 32;
    // Replaces the parallelism make_compaction_limits() derives. 0 keeps the derived one.
    size_t parallelism = 0;
    strategy_kind strategy = strategy_kind::efficiency;
    // Compaction jobs one group may run at once. The engine serializes them per group, so a shard
    // whose garbage sits in few groups has to spend its parallelism on groups that hold little.
    // Above 1 a job's inputs are reserved out of the candidate set when it starts, which is what
    // would let two jobs of one group pick disjoint batches.
    size_t max_jobs_per_group = 1;

    workload_kind workload = workload_kind::uniform;
    double zipf_theta = 0.99;
    double hot_key_fraction = 0.1;
    double hot_write_fraction = 0.9;

    value_size_kind value_size_dist = value_size_kind::fixed;
    uint64_t value_size = 1024;
    uint64_t value_size_min = 256;
    uint64_t value_size_max = 4096;
    double value_size_sigma = 1.0;
    uint64_t key_size = 16;

    // User record writes to simulate, 0 to derive from the dataset size, and the fraction of them
    // that is warm-up. Measurement covers the rest.
    uint64_t writes = 0;
    double warmup = 0.5;
    uint32_t seed = 1;
};

// ---------------------------------------------------------------------------
// Parameter registry: the one place that names, parses and prints a parameter, so that the command
// line and the report of a run speak the same names.
// ---------------------------------------------------------------------------

uint64_t parse_size(const std::string& s) {
    size_t consumed = 0;
    const auto value = std::stod(s, &consumed);
    const auto suffix = std::string_view(s).substr(consumed);
    double multiplier = 1;
    if (suffix == "k" || suffix == "K" || suffix == "KiB") {
        multiplier = 1024;
    } else if (suffix == "m" || suffix == "M" || suffix == "MiB") {
        multiplier = 1024 * 1024;
    } else if (suffix == "g" || suffix == "G" || suffix == "GiB") {
        multiplier = 1024 * 1024 * 1024;
    } else if (!suffix.empty()) {
        throw std::invalid_argument(fmt::format("unknown size suffix '{}'", suffix));
    }
    return static_cast<uint64_t>(value * multiplier);
}

template <typename Enum, size_t N>
Enum parse_enum(const std::string& name, const std::pair<std::string_view, Enum> (&values)[N]) {
    for (const auto& [n, v] : values) {
        if (n == name) {
            return v;
        }
    }
    throw std::invalid_argument(fmt::format("unknown value '{}', expected one of {}", name,
            values | std::views::transform([] (const auto& v) { return v.first; })));
}

template <typename Enum, size_t N>
std::string_view enum_name(Enum value, const std::pair<std::string_view, Enum> (&values)[N]) {
    for (const auto& [n, v] : values) {
        if (v == value) {
            return n;
        }
    }
    return "?";
}

constexpr std::pair<std::string_view, workload_kind> workload_names[] = {
    {"uniform", workload_kind::uniform},
    {"zipf", workload_kind::zipf},
    {"hot-cold", workload_kind::hot_cold},
};

constexpr std::pair<std::string_view, value_size_kind> value_size_names[] = {
    {"fixed", value_size_kind::fixed},
    {"uniform", value_size_kind::uniform},
    {"lognormal", value_size_kind::lognormal},
};

constexpr std::pair<std::string_view, strategy_kind> strategy_names[] = {
    {"efficiency", strategy_kind::efficiency},
    {"absolute", strategy_kind::absolute},
    {"random", strategy_kind::random_victim},
};

struct param_desc {
    const char* name;
    const char* description;
    void (*set)(sim_params&, const std::string&);
    std::string (*get)(const sim_params&);
};

const std::vector<param_desc>& all_params() {
    static const std::vector<param_desc> params = {
        {"disk-size", "size of the logstor disk, with an optional K/M/G suffix",
            [] (sim_params& p, const std::string& v) { p.disk_size = parse_size(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.disk_size); }},
        {"segment-size", "segment size",
            [] (sim_params& p, const std::string& v) { p.segment_size = parse_size(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.segment_size); }},
        {"file-size", "size of the files the segments are cut from",
            [] (sim_params& p, const std::string& v) { p.file_size = parse_size(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.file_size); }},
        {"groups", "compaction groups on the shard",
            [] (sim_params& p, const std::string& v) { p.groups = std::stoul(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.groups); }},
        {"group-skew", "how unevenly the keys are spread over the groups, 0 for evenly",
            [] (sim_params& p, const std::string& v) { p.group_skew = std::stod(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.group_skew); }},
        {"utilization", "live bytes as a fraction of the segment pool",
            [] (sim_params& p, const std::string& v) { p.utilization = std::stod(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.utilization); }},
        {"trigger-threshold", "free-segment target, as a fraction of the disk",
            [] (sim_params& p, const std::string& v) { p.trigger_threshold = std::stod(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.trigger_threshold); }},
        {"free-band", "pin the hysteresis band to this many segments, 0 for the derived one",
            [] (sim_params& p, const std::string& v) { p.free_band = std::stoull(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.free_band); }},
        {"batch-cap", "max_segments_per_compaction, the bound the derived cap is clamped to",
            [] (sim_params& p, const std::string& v) { p.batch_cap = std::stoul(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.batch_cap); }},
        {"parallelism", "compaction jobs in flight, 0 for the derived one",
            [] (sim_params& p, const std::string& v) { p.parallelism = std::stoul(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.parallelism); }},
        {"strategy", "candidate scoring rule: efficiency, absolute or random",
            [] (sim_params& p, const std::string& v) { p.strategy = parse_enum<strategy_kind>(v, strategy_names); },
            [] (const sim_params& p) { return std::string(enum_name(p.strategy, strategy_names)); }},
        {"jobs-per-group", "compaction jobs one group may run at once",
            [] (sim_params& p, const std::string& v) { p.max_jobs_per_group = std::stoul(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.max_jobs_per_group); }},
        {"workload", "key distribution: uniform, zipf or hot-cold",
            [] (sim_params& p, const std::string& v) { p.workload = parse_enum<workload_kind>(v, workload_names); },
            [] (const sim_params& p) { return std::string(enum_name(p.workload, workload_names)); }},
        {"zipf-theta", "zipf skew",
            [] (sim_params& p, const std::string& v) { p.zipf_theta = std::stod(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.zipf_theta); }},
        {"hot-key-fraction", "fraction of the keys that are hot",
            [] (sim_params& p, const std::string& v) { p.hot_key_fraction = std::stod(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.hot_key_fraction); }},
        {"hot-write-fraction", "fraction of the writes that go to the hot keys",
            [] (sim_params& p, const std::string& v) { p.hot_write_fraction = std::stod(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.hot_write_fraction); }},
        {"value-size-dist", "record value size distribution: fixed, uniform or lognormal",
            [] (sim_params& p, const std::string& v) { p.value_size_dist = parse_enum<value_size_kind>(v, value_size_names); },
            [] (const sim_params& p) { return std::string(enum_name(p.value_size_dist, value_size_names)); }},
        {"value-size", "record value size, the median of the lognormal distribution",
            [] (sim_params& p, const std::string& v) { p.value_size = parse_size(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.value_size); }},
        {"value-size-min", "smallest record value",
            [] (sim_params& p, const std::string& v) { p.value_size_min = parse_size(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.value_size_min); }},
        {"value-size-max", "largest record value",
            [] (sim_params& p, const std::string& v) { p.value_size_max = parse_size(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.value_size_max); }},
        {"value-size-sigma", "sigma of the lognormal value size distribution",
            [] (sim_params& p, const std::string& v) { p.value_size_sigma = std::stod(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.value_size_sigma); }},
        {"key-size", "partition key size",
            [] (sim_params& p, const std::string& v) { p.key_size = parse_size(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.key_size); }},
        {"writes", "user record writes to simulate, 0 to derive from the dataset size",
            [] (sim_params& p, const std::string& v) { p.writes = parse_size(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.writes); }},
        {"warmup", "fraction of the writes that is warm-up",
            [] (sim_params& p, const std::string& v) { p.warmup = std::stod(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.warmup); }},
        {"seed", "random seed",
            [] (sim_params& p, const std::string& v) { p.seed = std::stoul(v); },
            [] (const sim_params& p) { return fmt::format("{}", p.seed); }},
    };
    return params;
}

// ---------------------------------------------------------------------------
// The modeled disk
// ---------------------------------------------------------------------------

constexpr uint32_t no_segment = std::numeric_limits<uint32_t>::max();

// Where the index says the record of a key is. A slot rather than an offset: the two identify a
// record equally well, and the slot is what a scan of a segment iterates.
struct sim_location {
    uint32_t segment = no_segment;
    uint32_t slot = 0;

    bool operator==(const sim_location&) const noexcept = default;
    explicit operator bool() const noexcept { return segment != no_segment; }
};

// A record as the scan of a segment sees it: every record ever written to that segment, live or
// dead, in write order.
struct sim_record {
    uint32_t key;
    uint32_t frame_size;
};

// A record in a compaction output buffer, which is not on disk yet. `src` is where the record still
// is while the buffer holds a copy of it: the index update at flush time checks it, so that a record
// the workload overwrote in the meantime is not moved, which is what update_record_location()
// returning false leaves behind.
struct sim_buffered_record {
    uint32_t key;
    uint32_t frame_size;
    sim_location src;
};

// Mirrors raw_write_buffer's space accounting without holding any bytes: a record takes the same
// frame header and 8 byte alignment, the buffer the same chunk headers, and sealing it the same 4K
// padding, so that what a segment ends up holding and what it wastes are what the engine produces.
class sim_write_buffer {
    uint64_t _buffer_size;
    uint64_t _offset;
    uint64_t _record_bytes = 0;
    std::vector<sim_buffered_record> _records;

    // Every segment a group owns is a full segment, whatever wrote it.
    static constexpr size_t chunk_headers_size = raw_write_buffer::chunk_headers_size(segment_kind::full);

public:
    explicit sim_write_buffer(uint64_t buffer_size)
        : _buffer_size(buffer_size)
        , _offset(chunk_headers_size) {
    }

    void reset() noexcept {
        _offset = chunk_headers_size;
        _record_bytes = 0;
        _records.clear();
    }

    bool can_fit(uint64_t record_size) const noexcept {
        const auto aligned = align_up(ondisk::record_frame_header_size + record_size, size_t(ondisk::record_alignment));
        return aligned <= _buffer_size - _offset;
    }

    // Appends a record whose header and value take `record_size` bytes, and returns its frame size:
    // what record_location::size holds and what the segment descriptor accounts for.
    uint32_t append(uint32_t key, uint64_t record_size, sim_location src) {
        const auto frame_size = static_cast<uint32_t>(ondisk::record_frame_header_size + record_size);
        _offset += align_up(uint64_t(frame_size), uint64_t(ondisk::record_alignment));
        _record_bytes += frame_size;
        _records.push_back(sim_buffered_record{.key = key, .frame_size = frame_size, .src = src});
        return frame_size;
    }

    bool has_data() const noexcept { return _offset > chunk_headers_size; }

    uint64_t chunk_size() const noexcept {
        return align_up(_offset, uint64_t(ondisk::block_alignment));
    }

    uint64_t record_bytes() const noexcept { return _record_bytes; }
    size_t record_count() const noexcept { return _records.size(); }
    const std::vector<sim_buffered_record>& records() const noexcept { return _records; }
};

struct sim_segment {
    std::vector<sim_record> records;
};

// A compaction group: the segments it owns, in the engine's own segment_set, and the segment the
// write path is filling for it, which belongs to the group only once it is sealed.
class sim_group {
public:
    explicit sim_group(uint64_t segment_size)
        : segments(segment_size)
        , open_buffer(segment_size) {
    }

    segment_set segments;
    sim_write_buffer open_buffer;
    uint32_t open_segment = no_segment;
    size_t jobs_in_flight = 0;
};

// The batch one compaction job reads, in ascending utilization order.
struct sim_batch {
    std::vector<uint32_t> segments;
    compaction_candidate_score score;
};

// A group ranked by the batch it would run, which is what top_compaction_candidates orders.
struct sim_candidate {
    sim_group* group = nullptr;
    compaction_candidate_score score;
};

// One compaction job in flight. It is stepped one input segment at a time, which is the granularity
// the engine reads inputs at, so that the output segments a job holds before it frees its inputs are
// held for as much of the run as they are in the engine.
struct sim_job {
    sim_group* group = nullptr;
    std::vector<uint32_t> inputs;
    size_t next_input = 0;
    compaction_candidate_score score;
    sim_write_buffer output;

    sim_job(sim_group& g, std::vector<uint32_t> in, compaction_candidate_score s, uint64_t segment_size)
        : group(&g)
        , inputs(std::move(in))
        , score(s)
        , output(segment_size) {
    }
};

// Which write a segment was taken for. Mirrors write_source in segment_manager.cc; the simulator has
// no separator stage of its own, so a user write takes a segment the way the separator's flush does.
enum class write_source {
    user_write,
    compaction,
};

struct sim_stats {
    uint64_t user_records = 0;
    uint64_t user_data_bytes = 0;
    uint64_t user_bytes_written = 0;

    uint64_t compaction_bytes_written = 0;
    uint64_t compaction_data_bytes = 0;
    uint64_t compaction_bytes_read = 0;

    uint64_t compaction_jobs = 0;
    uint64_t compaction_segments_in = 0;
    uint64_t compaction_segments_out = 0;
    uint64_t compaction_segments_reclaimed = 0;
    uint64_t compaction_records_rewritten = 0;
    uint64_t compaction_records_skipped = 0;
    uint64_t segments_allocated = 0;
    uint64_t segments_freed = 0;
    uint64_t empty_candidate_scans = 0;

    double victim_utilization_sum = 0;
    uint64_t victim_segments = 0;

    // Executed batches by input count, and by the efficiency they ran at - the observed marginal
    // write amplification, which is the reciprocal of it.
    std::vector<uint64_t> batch_size_hist;
    std::array<uint64_t, 8> efficiency_hist{};

    // Sampled once per user record write.
    uint64_t samples = 0;
    uint64_t free_segments_sum = 0;
    uint64_t free_segments_min = std::numeric_limits<uint64_t>::max();
    uint64_t free_segments_max = 0;
    double live_bytes_sum = 0;

    // The distribution of the segments by utilization, which is what says how much space there is to
    // reclaim and how cheaply, and what the segments a group owns hold. Sampled rarely, since these
    // are shapes rather than rates, and cost a walk over the groups.
    uint64_t utilization_samples = 0;
    utilization_histogram utilization{};
    uint64_t sealed_segments_sum = 0;
    double sealed_live_bytes_sum = 0;
};

// The upper bounds of the efficiency histogram buckets, in segments reclaimed per segment-worth of
// data copied. The last bucket takes everything above the last bound.
constexpr double efficiency_bucket_bounds[] = {0.125, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0};

struct sim_result {
    sim_params params;
    sim_stats stats;
    uint64_t segment_count = 0;
    size_t batch_cap = 0;
    size_t parallelism = 0;
    free_segment_watermarks watermarks{};
    uint64_t keys = 0;
    uint64_t writes = 0;
    bool out_of_space = false;

    double wa_gc() const noexcept {
        return stats.user_data_bytes ? double(stats.compaction_data_bytes) / double(stats.user_data_bytes) : 0;
    }
    // Every byte the disk takes for a user byte, less the mixed segment pass, which is not modeled.
    double device_wa() const noexcept {
        const auto written = stats.user_bytes_written + stats.compaction_bytes_written;
        return stats.user_data_bytes ? double(written) / double(stats.user_data_bytes) : 0;
    }
    double read_per_user_byte() const noexcept {
        return stats.user_data_bytes ? double(stats.compaction_bytes_read) / double(stats.user_data_bytes) : 0;
    }
    double mean_victim_utilization() const noexcept {
        return stats.victim_segments ? stats.victim_utilization_sum / double(stats.victim_segments) : 0;
    }
    double mean_free_fraction() const noexcept {
        return stats.samples ? double(stats.free_segments_sum) / double(stats.samples) / double(segment_count) : 0;
    }
    double mean_utilization() const noexcept {
        return stats.samples
                ? stats.live_bytes_sum / double(stats.samples) / double(segment_count * params.segment_size)
                : 0;
    }
    // The utilization the occupied segments are packed to, which is what sets write amplification.
    double effective_utilization() const noexcept {
        const auto free = mean_free_fraction();
        return free < 1 ? mean_utilization() / (1 - free) : 0;
    }
    double segments_per_job() const noexcept {
        return stats.compaction_jobs ? double(stats.compaction_segments_in) / double(stats.compaction_jobs) : 0;
    }
    // The segments a group owns, which are the ones compaction can pick from. The rest of what the
    // disk holds is the open segment each group is filling, which is not a candidate until it is
    // sealed and is on average half empty.
    double mean_sealed_segments() const noexcept {
        return stats.utilization_samples
                ? double(stats.sealed_segments_sum) / double(stats.utilization_samples) : 0;
    }
    double mean_open_segments() const noexcept {
        return std::max(0.0, double(segment_count) * (1 - mean_free_fraction()) - mean_sealed_segments());
    }
    // The utilization of the segments compaction picks from, which is what its cost is set by. It is
    // above the utilization of the disk by whatever the open segments are holding back.
    double sealed_utilization() const noexcept {
        const auto sealed = mean_sealed_segments();
        return sealed > 0 && stats.utilization_samples
                ? stats.sealed_live_bytes_sum / double(stats.utilization_samples) / (sealed * double(params.segment_size))
                : 0;
    }
};

class out_of_space_error : public std::runtime_error {
public:
    out_of_space_error() : std::runtime_error("the disk ran out of segments") {}
};

// ---------------------------------------------------------------------------
// The simulator
// ---------------------------------------------------------------------------

class compaction_sim {
    sim_params _p;
    uint64_t _segment_count;
    free_segment_watermarks _watermarks;
    size_t _batch_cap;
    size_t _parallelism;

    std::vector<segment_descriptor> _descs;
    std::vector<sim_segment> _segments;
    std::deque<uint32_t> _free_segments;
    std::vector<std::unique_ptr<sim_group>> _groups;

    // The primary index: where the record of a key is, or nothing while the key has none.
    std::vector<sim_location> _index;
    // The value size of a key, drawn once, so that overwriting a key does not change how much of the
    // disk the dataset takes and the measurement runs at a fixed utilization.
    std::vector<uint32_t> _key_value_size;
    std::vector<uint32_t> _key_group;

    std::vector<sim_job> _jobs;
    std::vector<sim_candidate> _pending_candidates;
    bool _in_compaction_poll = false;

    uint64_t _live_bytes = 0;
    sim_stats _stats;

    std::mt19937 _rng;
    std::vector<double> _zipf_cdf;

public:
    explicit compaction_sim(sim_params p)
        : _p(std::move(p))
        , _segment_count((_p.disk_size / _p.file_size) * (_p.file_size / _p.segment_size))
        , _watermarks(make_free_segment_watermarks(_segment_count, _p.trigger_threshold))
        , _rng(_p.seed) {
        // segment_descriptor_hist indexes a fixed-size bucket array without checking its bounds, so
        // a segment larger than the histogram's maximum computes an out of range bucket for a fully
        // free segment. The segment manager rejects such a configuration; without the same check
        // here such a segment size silently corrupts memory.
        if (_p.segment_size > segment_descriptor_hist_options.max_size) {
            throw std::invalid_argument(fmt::format("a segment of {} is above the {} the free-space"
                    " histogram is built for", _p.segment_size, segment_descriptor_hist_options.max_size));
        }
        if (_p.file_size < _p.segment_size || _p.disk_size < _p.file_size) {
            throw std::invalid_argument("the disk holds whole files and a file holds whole segments");
        }
        if (_segment_count < 4 * min_segments_per_compaction) {
            throw std::invalid_argument(fmt::format("a disk of {} segments is too small to compact", _segment_count));
        }
        if (_p.groups == 0) {
            throw std::invalid_argument("a shard has at least one compaction group");
        }
        if (_p.max_jobs_per_group == 0) {
            throw std::invalid_argument("a group runs at least one compaction job at a time");
        }
        if (_p.free_band > 0) {
            _watermarks.high = std::min(_segment_count, _watermarks.low + _p.free_band);
        }
        const auto limits = make_compaction_limits(_watermarks, _p.batch_cap);
        _parallelism = _p.parallelism != 0 ? _p.parallelism : limits.auto_parallelism;
        // The batch cap follows the parallelism through the relation make_compaction_limits()
        // derives both from, so that overriding the parallelism to study one of them still spends
        // the free-segment target on concurrency first and gives the rest to the batch.
        const auto budget = std::max<uint64_t>(_watermarks.low, min_segments_per_compaction);
        _batch_cap = _p.parallelism != 0
                ? std::clamp<uint64_t>(budget / _parallelism, min_segments_per_compaction,
                        std::max<uint64_t>(_p.batch_cap, min_segments_per_compaction))
                : limits.batch_cap;

        _descs.resize(_segment_count);
        _segments.resize(_segment_count);
        for (uint64_t i = 0; i < _segment_count; ++i) {
            _descs[i].reset(_p.segment_size);
            _free_segments.push_back(static_cast<uint32_t>(i));
        }
        _groups.reserve(_p.groups);
        for (size_t i = 0; i < _p.groups; ++i) {
            _groups.push_back(std::make_unique<sim_group>(_p.segment_size));
        }
        _stats.batch_size_hist.resize(_batch_cap + 1);
        build_dataset();
    }

    sim_result run() {
        sim_result result{
            .params = _p,
            .segment_count = _segment_count,
            .batch_cap = _batch_cap,
            .parallelism = _parallelism,
            .watermarks = _watermarks,
            .keys = _index.size(),
        };

        const auto writes = _p.writes != 0 ? _p.writes : 4 * _index.size();
        const auto warmup = static_cast<uint64_t>(double(writes) * std::clamp(_p.warmup, 0.0, 0.99));
        result.writes = writes;

        try {
            populate();
            run_writes(warmup);
            check_invariants();
            reset_stats();
            run_writes(writes - warmup);
            check_invariants();
        } catch (const out_of_space_error&) {
            result.out_of_space = true;
        }

        result.stats = _stats;
        return result;
    }

private:
    // --- the segment pool ---

    uint64_t available_segments(write_source src) const noexcept {
        const auto free = _free_segments.size();
        if (src == write_source::compaction) {
            return free;
        }
        // Everything but compaction leaves the pool a segment per concurrent compaction job, which
        // is what segment_pool's compaction reserve keeps back.
        return free > max_compaction_parallelism ? free - max_compaction_parallelism : 0;
    }

    uint32_t allocate_segment(write_source src) {
        // allocate_segment() schedules automatic compaction before it reuses a freed segment, and
        // that is what makes compaction run at all once the disk has been written through.
        poll_compaction();
        if (available_segments(src) == 0) {
            throw out_of_space_error();
        }
        const auto id = _free_segments.front();
        _free_segments.pop_front();
        _segments[id].records.clear();
        _descs[id].reset(_p.segment_size);
        ++_stats.segments_allocated;
        return id;
    }

    void free_segment(uint32_t id) {
        const auto& desc = _descs[id];
        if (desc.record_bytes(_p.segment_size) != 0) {
            throw std::logic_error(fmt::format("freeing segment {} that has {} bytes of data",
                    id, desc.record_bytes(_p.segment_size)));
        }
        if (desc.ref_count != 0) {
            throw std::logic_error(fmt::format("freeing segment {} that is still referenced", id));
        }
        _segments[id].records.clear();
        _free_segments.push_back(id);
        ++_stats.segments_freed;
    }

    uint32_t desc_to_id(const segment_descriptor& desc) const noexcept {
        return static_cast<uint32_t>(&desc - _descs.data());
    }

    // --- the index, mirroring what primary_index and the segment manager account on a record ---

    void free_record(sim_location loc) noexcept {
        const auto frame_size = _segments[loc.segment].records[loc.slot].frame_size;
        auto& desc = _descs[loc.segment];
        desc.on_free(frame_size);
        _live_bytes -= frame_size;
        if (desc.owner) {
            desc.owner->update_segment(desc, frame_size);
        }
    }

    sim_location add_record(uint32_t seg_id, uint32_t key, uint32_t frame_size) {
        auto& seg = _segments[seg_id];
        seg.records.push_back(sim_record{.key = key, .frame_size = frame_size});
        _descs[seg_id].on_write(frame_size);
        _live_bytes += frame_size;
        return sim_location{.segment = seg_id, .slot = static_cast<uint32_t>(seg.records.size() - 1)};
    }

    // --- the write path ---

    uint64_t record_size(uint32_t key) const noexcept {
        return record_header_fixed_size + _p.key_size + _key_value_size[key];
    }

    sim_group& group_of(uint32_t key) noexcept {
        return *_groups[_key_group[key]];
    }

    // A user record goes straight into the open segment of its group, which stands for the separator
    // having already copied it out of the mixed active segment. The record is indexed where it
    // lands, so an overwrite of it while the segment is still open kills it there, exactly as an
    // overwrite of a record the separator has already written out does.
    void write_record(uint32_t key) {
        auto& g = group_of(key);
        const auto size = record_size(key);
        if (g.open_segment != no_segment && !g.open_buffer.can_fit(size)) {
            seal_open_segment(g);
        }
        if (g.open_segment == no_segment) {
            g.open_segment = allocate_segment(write_source::user_write);
            g.open_buffer.reset();
        }
        const auto frame_size = g.open_buffer.append(key, size, sim_location{});
        const auto old = _index[key];
        if (old) {
            free_record(old);
        }
        _index[key] = add_record(g.open_segment, key, frame_size);

        ++_stats.user_records;
        _stats.user_data_bytes += frame_size;
        sample();
    }

    // A segment joins its group once it is sealed, which is what makes it a compaction candidate.
    void seal_open_segment(sim_group& g) {
        _stats.user_bytes_written += g.open_buffer.chunk_size();
        g.segments.add_segment(_descs[g.open_segment]);
        g.open_segment = no_segment;
        g.open_buffer.reset();
    }

    // --- compaction ---

    // Scores every prefix of `candidates` the way select_compaction_batch() does, so that a strategy
    // other than the implemented one can pick a different prefix of the same candidates.
    std::vector<compaction_candidate_score> score_prefixes(std::span<const segment_descriptor* const> candidates) const {
        std::vector<compaction_candidate_score> scores;
        scores.reserve(candidates.size());
        uint64_t live_bytes = 0;
        uint64_t records = 0;
        for (size_t i = 0; i < candidates.size(); ++i) {
            live_bytes += candidates[i]->record_bytes(_p.segment_size);
            records += candidates[i]->record_count;
            scores.push_back(compaction_candidate_score{
                .n_in = i + 1,
                .n_out = raw_write_buffer::estimate_required_segments(live_bytes, records, _p.segment_size, segment_kind::full),
                .live_bytes = live_bytes,
            });
        }
        return scores;
    }

    std::optional<sim_batch> select_batch(sim_group& g) {
        switch (_p.strategy) {
        case strategy_kind::efficiency:
            return select_batch_efficiency(g);
        case strategy_kind::absolute:
            return select_batch_absolute(g);
        case strategy_kind::random_victim:
            return select_batch_random(g);
        }
        return std::nullopt;
    }

    // The implemented rule, run by the engine's own code.
    std::optional<sim_batch> select_batch_efficiency(sim_group& g) {
        auto batch = select_compaction_batch(g.segments, _p.segment_size, compaction_batch_limits{
            .batch_cap = _batch_cap,
        });
        if (!batch) {
            return std::nullopt;
        }
        return make_batch(batch->segments, batch->score);
    }

    // The rule the efficiency score replaced: rank by the segments a batch reclaims first, so that
    // it keeps growing into fuller segments for one more reclaimed segment however much that costs.
    std::optional<sim_batch> select_batch_absolute(sim_group& g) {
        std::vector<const segment_descriptor*> candidates;
        candidates.reserve(_batch_cap);
        for (const auto& desc : g.segments.by_free_space()) {
            if (candidates.size() >= _batch_cap) {
                break;
            }
            candidates.push_back(&desc);
        }
        const auto scores = score_prefixes(candidates);
        size_t best = 0;
        for (size_t i = 0; i < scores.size(); ++i) {
            if (scores[i].reclaimed() == 0) {
                continue;
            }
            // Segments reclaimed first, then reclaimed per live byte, which is what operator< ranked
            // by before it became the efficiency score.
            const auto better = best == 0
                    || scores[best - 1].reclaimed() < scores[i].reclaimed()
                    || (scores[best - 1].reclaimed() == scores[i].reclaimed()
                            && !scores[i].efficiency_at_least(scores[best - 1], 1.0));
            if (better) {
                best = i + 1;
            }
        }
        if (best == 0) {
            return std::nullopt;
        }
        return make_batch(std::span(candidates).first(best), scores[best - 1]);
    }

    // The textbook baseline: victims picked at random rather than by utilization, which is what the
    // free-space histogram buys over.
    std::optional<sim_batch> select_batch_random(sim_group& g) {
        const auto count = std::min<size_t>(_batch_cap, g.segments.segment_count());
        if (count == 0) {
            return std::nullopt;
        }
        // Sampled by position, which picks the segments that sampling the set itself would.
        std::vector<size_t> positions(g.segments.segment_count());
        std::iota(positions.begin(), positions.end(), size_t(0));
        std::vector<size_t> picked;
        picked.reserve(count);
        std::sample(positions.begin(), positions.end(), std::back_inserter(picked), count, _rng);
        std::vector<const segment_descriptor*> candidates;
        candidates.reserve(count);
        for (const auto i : picked) {
            candidates.push_back(&g.segments.segment_at(i));
        }
        const auto scores = score_prefixes(candidates);
        for (size_t i = 0; i < scores.size(); ++i) {
            if (scores[i].reclaimed() > 0) {
                return make_batch(std::span(candidates).first(i + 1), scores[i]);
            }
        }
        return std::nullopt;
    }

    sim_batch make_batch(std::span<const segment_descriptor* const> candidates, compaction_candidate_score score) const {
        std::vector<uint32_t> ids;
        ids.reserve(candidates.size());
        for (const auto* desc : candidates) {
            ids.push_back(desc_to_id(*desc));
        }
        return sim_batch{.segments = std::move(ids), .score = score};
    }

    // Ranks the groups by the batch each would run, as find_top_compaction_candidates does, with the
    // same top-k helper.
    std::vector<sim_candidate> rank_candidates(size_t max_candidates) {
        top_compaction_candidates<sim_candidate> best(max_candidates);
        for (const auto& g : _groups) {
            if (g->jobs_in_flight >= _p.max_jobs_per_group) {
                continue;
            }
            if (auto batch = select_batch(*g)) {
                best.add(sim_candidate{.group = g.get(), .score = batch->score});
            }
        }
        return std::move(best).take();
    }

    void poll_compaction() {
        // A compaction job allocates its output segments and allocate_segment() polls; the guard is
        // what keeps that from recursing, rather than any decision of the engine's.
        if (_in_compaction_poll) {
            return;
        }
        _in_compaction_poll = true;
        auto guard = defer([this] () noexcept { _in_compaction_poll = false; });

        // `running` is the state of run_auto_compaction()'s fiber, not of the jobs it has in flight:
        // once started it runs to the stop watermark, rather than stopping the moment the free level
        // is back at the target.
        bool running = false;
        while (auto_compaction_wanted(running, available_segments(write_source::user_write), _watermarks)) {
            running = true;
            start_jobs();
            if (_jobs.empty()) {
                // No group holds a batch with a net gain, so the disk is out of reclaimable space
                // rather than merely between batches.
                ++_stats.empty_candidate_scans;
                break;
            }
            step_jobs();
        }
    }

    void start_jobs() {
        while (_jobs.size() < _parallelism) {
            if (_pending_candidates.empty()) {
                _pending_candidates = rank_candidates(_parallelism);
                if (_pending_candidates.empty()) {
                    return;
                }
            }
            const auto candidate = _pending_candidates.back();
            _pending_candidates.pop_back();
            auto& g = *candidate.group;
            if (g.jobs_in_flight >= _p.max_jobs_per_group) {
                continue;
            }
            // do_compaction() discards the batch the ranking chose and selects again, so a candidate
            // that has been queued for a while runs the batch its group holds now.
            auto batch = select_batch(g);
            if (!batch) {
                continue;
            }
            // The engine reads the inputs of a job in segment id order.
            std::ranges::sort(batch->segments);
            ++g.jobs_in_flight;
            _jobs.emplace_back(g, std::move(batch->segments), batch->score, _p.segment_size);
            account_batch(_jobs.back());
            for (const auto id : _jobs.back().inputs) {
                // Two jobs of one group can only pick disjoint batches if a job's inputs leave the
                // candidate set when it starts. With one job per group the engine has no such need,
                // and the inputs stay in the set until the job frees them.
                if (_p.max_jobs_per_group > 1) {
                    g.segments.remove_segment(_descs[id]);
                }
            }
        }
    }

    void account_batch(const sim_job& job) {
        ++_stats.compaction_jobs;
        _stats.compaction_segments_in += job.inputs.size();
        _stats.compaction_segments_reclaimed += job.score.reclaimed();
        if (job.inputs.size() < _stats.batch_size_hist.size()) {
            ++_stats.batch_size_hist[job.inputs.size()];
        }
        const auto efficiency = job.score.efficiency(_p.segment_size);
        size_t bucket = 0;
        while (bucket < std::size(efficiency_bucket_bounds) && efficiency >= efficiency_bucket_bounds[bucket]) {
            ++bucket;
        }
        ++_stats.efficiency_hist[bucket];
        for (const auto id : job.inputs) {
            _stats.victim_utilization_sum +=
                    double(_descs[id].record_bytes(_p.segment_size)) / double(_p.segment_size);
            ++_stats.victim_segments;
        }
    }

    void step_jobs() {
        for (size_t i = 0; i < _jobs.size();) {
            step_job(_jobs[i]);
            if (_jobs[i].next_input == _jobs[i].inputs.size()) {
                finish_job(_jobs[i]);
                _jobs.erase(_jobs.begin() + i);
            } else {
                ++i;
            }
        }
    }

    // Scans one input segment, which is what the engine reads at a time, copying the records that
    // are still live into the output buffer.
    void step_job(sim_job& job) {
        const auto seg_id = job.inputs[job.next_input++];
        if (_descs[seg_id].record_bytes(_p.segment_size) == 0) {
            // A segment with nothing live in it is not read at all, which is what the nonempty
            // filter in do_compaction() leaves out.
            return;
        }
        _stats.compaction_bytes_read += _p.segment_size;

        const auto& records = _segments[seg_id].records;
        for (uint32_t slot = 0; slot < records.size(); ++slot) {
            const auto loc = sim_location{.segment = seg_id, .slot = slot};
            if (_index[records[slot].key] != loc) {
                ++_stats.compaction_records_skipped;
                continue;
            }
            const auto size = uint64_t(records[slot].frame_size) - ondisk::record_frame_header_size;
            if (!job.output.can_fit(size)) {
                flush_output(job);
            }
            job.output.append(records[slot].key, size, loc);
        }
    }

    void flush_output(sim_job& job) {
        if (!job.output.has_data()) {
            return;
        }
        const auto seg_id = allocate_segment(write_source::compaction);
        _stats.compaction_bytes_written += job.output.chunk_size();
        _stats.compaction_data_bytes += job.output.record_bytes();
        // A record the workload overwrote while the buffer held it is written out but never indexed,
        // so the space it takes is dead the moment it lands.
        for (const auto& rec : job.output.records()) {
            if (_index[rec.key] != rec.src) {
                ++_stats.compaction_records_skipped;
                continue;
            }
            free_record(rec.src);
            _index[rec.key] = add_record(seg_id, rec.key, rec.frame_size);
            ++_stats.compaction_records_rewritten;
        }
        job.group->segments.add_segment(_descs[seg_id]);
        job.output.reset();
        ++_stats.compaction_segments_out;
    }

    void finish_job(sim_job& job) {
        auto& g = *job.group;
        // compaction_buffer::close() flushes whatever is left as a partly filled segment, which is
        // the output residual every job leaves behind.
        flush_output(job);
        for (const auto id : job.inputs) {
            if (_p.max_jobs_per_group == 1) {
                g.segments.remove_segment(_descs[id]);
            }
            if (_descs[id].ref_count == 0) {
                free_segment(id);
            }
        }
        --g.jobs_in_flight;
    }

    // --- the workload ---

    void build_dataset() {
        const auto pool_bytes = _segment_count * _p.segment_size;
        const auto target_live = static_cast<uint64_t>(double(pool_bytes) * _p.utilization);
        if (target_live == 0) {
            throw std::invalid_argument("the dataset is empty, raise --utilization");
        }
        // A record has to leave room for the headers of the chunk it is written into, and the
        // engine only takes one that fits a segment of either kind.
        const auto max_value = raw_write_buffer::max_record_size_any_kind(_p.segment_size)
                - record_header_fixed_size - _p.key_size;

        std::vector<double> group_cdf(_p.groups);
        double weight_sum = 0;
        for (size_t i = 0; i < _p.groups; ++i) {
            weight_sum += std::pow(double(i + 1), -_p.group_skew);
            group_cdf[i] = weight_sum;
        }
        for (auto& c : group_cdf) {
            c /= weight_sum;
        }

        std::uniform_real_distribution<double> unit(0.0, 1.0);
        uint64_t live = 0;
        while (live < target_live) {
            uint64_t value_size = _p.value_size;
            switch (_p.value_size_dist) {
            case value_size_kind::fixed:
                break;
            case value_size_kind::uniform:
                value_size = std::uniform_int_distribution<uint64_t>(_p.value_size_min, _p.value_size_max)(_rng);
                break;
            case value_size_kind::lognormal:
                value_size = static_cast<uint64_t>(
                        std::lognormal_distribution<double>(std::log(double(_p.value_size)), _p.value_size_sigma)(_rng));
                break;
            }
            value_size = std::clamp<uint64_t>(value_size, 1, max_value);
            _key_value_size.push_back(static_cast<uint32_t>(value_size));

            const auto u = unit(_rng);
            const auto group = std::ranges::lower_bound(group_cdf, u) - group_cdf.begin();
            _key_group.push_back(static_cast<uint32_t>(std::min<size_t>(group, _p.groups - 1)));

            live += ondisk::record_frame_header_size + record_header_fixed_size + _p.key_size + value_size;
        }
        _index.resize(_key_value_size.size());

        if (_p.workload == workload_kind::zipf) {
            _zipf_cdf.resize(_index.size());
            double sum = 0;
            for (size_t i = 0; i < _index.size(); ++i) {
                sum += 1.0 / std::pow(double(i + 1), _p.zipf_theta);
                _zipf_cdf[i] = sum;
            }
            for (auto& c : _zipf_cdf) {
                c /= sum;
            }
        }
    }

    uint32_t next_key() {
        const auto keys = static_cast<uint32_t>(_index.size());
        switch (_p.workload) {
        case workload_kind::uniform:
            return std::uniform_int_distribution<uint32_t>(0, keys - 1)(_rng);
        case workload_kind::zipf: {
            const auto u = std::uniform_real_distribution<double>(0.0, 1.0)(_rng);
            const auto it = std::ranges::lower_bound(_zipf_cdf, u);
            return static_cast<uint32_t>(std::min<size_t>(it - _zipf_cdf.begin(), keys - 1));
        }
        case workload_kind::hot_cold: {
            const auto hot_keys = std::clamp<uint32_t>(static_cast<uint32_t>(keys * _p.hot_key_fraction), 1, keys - 1);
            const auto u = std::uniform_real_distribution<double>(0.0, 1.0)(_rng);
            return u < _p.hot_write_fraction
                    ? std::uniform_int_distribution<uint32_t>(0, hot_keys - 1)(_rng)
                    : std::uniform_int_distribution<uint32_t>(hot_keys, keys - 1)(_rng);
        }
        }
        return 0;
    }

    // Writes every key once, in a random order, so that the keys of a group are not laid out in the
    // segments in key order.
    void populate() {
        std::vector<uint32_t> keys(_index.size());
        std::iota(keys.begin(), keys.end(), 0u);
        std::ranges::shuffle(keys, _rng);
        for (const auto key : keys) {
            write_record(key);
            maybe_yield();
        }
    }

    void run_writes(uint64_t count) {
        for (uint64_t i = 0; i < count; ++i) {
            write_record(next_key());
            maybe_yield();
        }
    }

    // The simulation is a long stretch of computation with no IO in it, so it has to give the
    // reactor a turn now and then to keep it from reporting a stall.
    void maybe_yield() {
        if ((_stats.user_records & 0xffff) == 0) {
            seastar::thread::maybe_yield();
        }
    }

    void sample() {
        const auto free = _free_segments.size();
        ++_stats.samples;
        _stats.free_segments_sum += free;
        _stats.free_segments_min = std::min(_stats.free_segments_min, free);
        _stats.free_segments_max = std::max(_stats.free_segments_max, free);
        _stats.live_bytes_sum += double(_live_bytes);

        static constexpr uint64_t utilization_sample_period = 1024;
        if (_stats.samples % utilization_sample_period == 0) {
            ++_stats.utilization_samples;
            for (const auto& g : _groups) {
                const auto& stats = g->segments.stats();
                _stats.sealed_segments_sum += stats.segment_count;
                _stats.sealed_live_bytes_sum += double(stats.live_record_bytes);
                for (size_t i = 0; i < utilization_bucket_count; ++i) {
                    _stats.utilization[i] += stats.utilization[i];
                }
            }
        }
    }

    void reset_stats() {
        const auto batch_hist_size = _stats.batch_size_hist.size();
        _stats = sim_stats{};
        _stats.batch_size_hist.resize(batch_hist_size);
    }

    // Every accounting path the simulator shares with the engine has to leave these equal. Checking
    // the maintained segment statistics against the segments themselves is the engine's own check.
    void check_invariants() const {
        uint64_t live = 0;
        for (uint64_t i = 0; i < _segment_count; ++i) {
            live += _descs[i].record_bytes(_p.segment_size);
        }
        if (live != _live_bytes) {
            throw std::logic_error(fmt::format("live bytes {} do not match the segment descriptors {}",
                    _live_bytes, live));
        }

        uint64_t indexed = 0;
        for (const auto loc : _index) {
            if (loc) {
                indexed += _segments[loc.segment].records[loc.slot].frame_size;
            }
        }
        if (indexed != _live_bytes) {
            throw std::logic_error(fmt::format("live bytes {} do not match the index {}", _live_bytes, indexed));
        }

        for (const auto& g : _groups) {
            const auto recomputed = g->segments.recompute_stats_for_test();
            const auto& maintained = g->segments.stats();
            if (recomputed.segment_count != maintained.segment_count
                    || recomputed.live_record_bytes != maintained.live_record_bytes
                    || recomputed.utilization != maintained.utilization) {
                throw std::logic_error("the maintained segment statistics do not match the segments");
            }
        }
    }
};

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

std::string format_bytes(uint64_t bytes) {
    static constexpr const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    double value = double(bytes);
    size_t unit = 0;
    while (value >= 1024 && unit + 1 < std::size(units)) {
        value /= 1024;
        ++unit;
    }
    return fmt::format("{:.6g} {}", value, units[unit]);
}

void print_report(const sim_result& r) {
    const auto& p = r.params;
    const auto& s = r.stats;

    fmt::print("Configuration\n");
    fmt::print("  disk           {} in {} segments of {}\n",
            format_bytes(p.disk_size), r.segment_count, format_bytes(p.segment_size));
    fmt::print("  groups         {}{}\n", p.groups,
            p.group_skew != 0 ? fmt::format(", skew {}", p.group_skew) : "");
    fmt::print("  free target    {:.4g}% -> low {} high {}{}\n", p.trigger_threshold * 100,
            r.watermarks.low, r.watermarks.high, p.free_band ? " (band pinned)" : "");
    fmt::print("  compaction     batch cap {}, parallelism {}, tolerance {}, strategy {}\n",
            r.batch_cap, r.parallelism, compaction_batch_extension_tolerance, enum_name(p.strategy, strategy_names));
    if (p.max_jobs_per_group > 1) {
        fmt::print("                 {} jobs per group\n", p.max_jobs_per_group);
    }
    fmt::print("  workload       {}, {} keys, values {} ~{}\n",
            enum_name(p.workload, workload_names), r.keys,
            enum_name(p.value_size_dist, value_size_names), format_bytes(p.value_size));
    fmt::print("  writes         {}, {:.0f}% warm-up, seed {}\n", r.writes, p.warmup * 100, p.seed);

    if (r.out_of_space) {
        fmt::print("\n  *** the disk ran out of segments: compaction could not hold the free-segment"
                   " target while the write path needed a segment ***\n");
        if (s.user_records == 0) {
            return;
        }
    }

    fmt::print("\nSpace\n");
    fmt::print("  mean utilization U            {:.4f}\n", r.mean_utilization());
    fmt::print("  mean free segments            {:.1f} ({:.3f}% of the disk), min {} max {}\n",
            r.mean_free_fraction() * double(r.segment_count), r.mean_free_fraction() * 100,
            s.free_segments_min, s.free_segments_max);
    fmt::print("  effective utilization U_eff   {:.4f}\n", r.effective_utilization());
    fmt::print("  segments a group owns         {:.1f}, holding {:.4f} of themselves\n",
            r.mean_sealed_segments(), r.sealed_utilization());
    fmt::print("  open segments                 {:.1f} ({:.3f}% of the disk), one per group\n",
            r.mean_open_segments(), r.mean_open_segments() / double(r.segment_count) * 100);

    fmt::print("\nWrite amplification\n");
    fmt::print("  WA_gc      compaction data / user data     {:.3f}\n", r.wa_gc());
    fmt::print("  device WA  bytes written / user data       {:.3f}\n", r.device_wa());
    fmt::print("  read/user  compaction read / user data     {:.3f}\n", r.read_per_user_byte());
    fmt::print("  random victim baseline U_eff/(1-U_eff)     {:.3f}\n",
            r.effective_utilization() < 1 ? r.effective_utilization() / (1 - r.effective_utilization()) : 0);
    fmt::print("  (the mixed segment pass the separator feeds from is not modeled, so the device\n"
               "   writes one more copy of every user byte than the device WA above)\n");

    fmt::print("\nCompaction\n");
    fmt::print("  jobs {}, segments in {}, out {}, reclaimed {}\n",
            s.compaction_jobs, s.compaction_segments_in, s.compaction_segments_out, s.compaction_segments_reclaimed);
    fmt::print("  segments per job {:.2f}, mean victim utilization {:.3f}\n",
            r.segments_per_job(), r.mean_victim_utilization());
    fmt::print("  records rewritten {}, skipped {}\n", s.compaction_records_rewritten, s.compaction_records_skipped);
    if (s.empty_candidate_scans) {
        fmt::print("  scans that found no candidate with a net gain: {}\n", s.empty_candidate_scans);
    }

    fmt::print("\n  executed batch size\n");
    for (size_t i = 0; i < s.batch_size_hist.size(); ++i) {
        if (s.batch_size_hist[i]) {
            fmt::print("    {:3} segments  {:8} jobs\n", i, s.batch_size_hist[i]);
        }
    }
    fmt::print("\n  executed batch efficiency (segments reclaimed per segment copied, 1/marginal WA)\n");
    for (size_t i = 0; i < s.efficiency_hist.size(); ++i) {
        if (!s.efficiency_hist[i]) {
            continue;
        }
        const auto lo = i == 0 ? 0.0 : efficiency_bucket_bounds[i - 1];
        if (i == s.efficiency_hist.size() - 1) {
            fmt::print("    >= {:<9.4g} {:8} jobs\n", lo, s.efficiency_hist[i]);
        } else {
            fmt::print("    {:.4g} - {:<6.4g} {:8} jobs\n", lo, efficiency_bucket_bounds[i], s.efficiency_hist[i]);
        }
    }

    if (s.utilization_samples) {
        fmt::print("\n  segments by utilization, averaged over the run\n");
        for (size_t i = 0; i < utilization_bucket_count; ++i) {
            const auto count = double(s.utilization[i]) / double(s.utilization_samples);
            if (count < 0.05) {
                continue;
            }
            fmt::print("    {:.2f} - {:.2f}  {:8.1f} segments\n",
                    double(i) / utilization_bucket_count, double(i + 1) / utilization_bucket_count, count);
        }
    }

    fmt::print("\nBytes\n");
    fmt::print("  user data   {}\n", format_bytes(s.user_data_bytes));
    fmt::print("  user writes {}\n", format_bytes(s.user_bytes_written));
    fmt::print("  compaction  {} written, {} read\n",
            format_bytes(s.compaction_bytes_written), format_bytes(s.compaction_bytes_read));
    fmt::print("  segments allocated {}, freed {}\n", s.segments_allocated, s.segments_freed);
}

} // namespace

int main(int argc, char** argv) {
    app_template::config app_cfg;
    app_cfg.name = "logstor_compaction_sim";
    app_cfg.description = "simulates logstor compaction over a modeled segment pool";
    app_template app(std::move(app_cfg));

    auto add = app.add_options();
    for (const auto& param : all_params()) {
        add(param.name, boost::program_options::value<std::string>(), param.description);
    }

    return app.run(argc, argv, [&app] {
        return async([&app] {
            const auto& opts = app.configuration();
            sim_params base;
            for (const auto& param : all_params()) {
                if (opts.contains(param.name)) {
                    param.set(base, opts[param.name].as<std::string>());
                }
            }

            print_report(compaction_sim(base).run());
            return 0;
        });
    });
}
