/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <seastar/core/metrics_types.hh>

namespace replica::logstor {

// The utilization of a segment is the fraction of it held by live records: 1 for a segment none of
// whose records were overwritten or deleted, and 0 for one whose records are all dead. Only sealed
// segments have a utilization - a segment joins a compaction group once it is fully written, and
// from there its live bytes only shrink.
//
// The histogram splits the utilization into equal buckets, so bucket i covers [i/N, (i+1)/N) and a
// fully utilized segment lands in the last bucket. Compaction reclaims from the low buckets, so the
// shape of the histogram is what says how much space there is to reclaim and how cheaply: mass at
// the bottom is space compaction can give back by copying little, mass at the top is space that is
// genuinely in use.
//
// The bucket count is a trade-off with the cardinality of the metric that exports it.
constexpr size_t utilization_bucket_count = 16;

using utilization_histogram = std::array<uint64_t, utilization_bucket_count>;

inline size_t utilization_bucket_of(uint64_t live_bytes, uint64_t segment_size) noexcept {
    // A segment that is entirely live belongs in the last bucket rather than in one of its own.
    return std::min<size_t>(live_bytes * utilization_bucket_count / segment_size, utilization_bucket_count - 1);
}

// Statistics of the segments owned by a set of compaction groups. Every field is a sum over the
// groups, so the statistics of a table are the sum over its groups, those of a shard the sum over
// its tables, and those of a table on a node the sum over the shards.
struct segment_stats {
    uint64_t group_count{0};
    uint64_t segment_count{0};
    // Bytes of the live records held by those segments. The rest of the space the segments take is
    // held by records that were overwritten or deleted, and is what compaction reclaims. It is at
    // most the live record bytes of the table, which also cover the records that are not in a
    // segment of a group yet, being still in the active segment or in a separator buffer.
    uint64_t live_bytes{0};
    utilization_histogram utilization{};

    segment_stats& operator+=(const segment_stats& other) noexcept {
        group_count += other.group_count;
        segment_count += other.segment_count;
        live_bytes += other.live_bytes;
        for (size_t i = 0; i < utilization_bucket_count; ++i) {
            utilization[i] += other.utilization[i];
        }
        return *this;
    }
};

// Exports the utilization histogram as a Prometheus histogram, whose buckets are cumulative and
// labelled by the utilization they reach up to.
//
// Unlike a latency histogram this is a snapshot of a distribution rather than a count of events, so
// its buckets fall as well as rise, as segments are written, freed from and compacted. A dashboard
// has to read the buckets as they are - a rate() over them is meaningless.
inline seastar::metrics::histogram to_metrics_histogram(const segment_stats& stats, uint64_t segment_size) {
    seastar::metrics::histogram res;
    res.buckets.reserve(utilization_bucket_count);

    uint64_t cumulative_count = 0;
    for (size_t i = 0; i < utilization_bucket_count; ++i) {
        cumulative_count += stats.utilization[i];
        res.buckets.push_back(seastar::metrics::histogram_bucket{
            .count = cumulative_count,
            .upper_bound = double(i + 1) / utilization_bucket_count,
        });
    }

    res.sample_count = cumulative_count;
    // The utilizations of the segments summed up, so that the mean utilization is the sum over the
    // count, as it is for any other histogram.
    res.sample_sum = segment_size ? double(stats.live_bytes) / segment_size : 0.0;

    return res;
}

// Everything the logstor of one shard has to say about one table: the segments its groups own, its
// live data, and the shard wide segment counts, which say how much room it has left to grow into.
// Summing these over the shards gives the numbers for the whole node.
struct table_logstor_stats {
    segment_stats segments;
    // Bytes of the live records of the table, taken from its index. Covers the records that are not
    // in a segment of a group yet, being still in the active segment or in a separator buffer, so it
    // is at least segments.live_bytes.
    uint64_t live_record_bytes{0};
    // Shard wide, across all the tables: segments that can still be allocated for writing, and the
    // number of segment slots the shard has.
    uint64_t free_segments{0};
    uint64_t total_segments{0};

    table_logstor_stats& operator+=(const table_logstor_stats& other) noexcept {
        segments += other.segments;
        live_record_bytes += other.live_record_bytes;
        free_segments += other.free_segments;
        total_segments += other.total_segments;
        return *this;
    }
};

}
