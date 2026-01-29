/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/scheduling.hh>
#include "types.hh"
#include "utils/serialized_action.hh"

namespace replica::log_structured {

class segment_manager;
class log_index;
class write_buffer;

/// Configuration for log compaction
struct compaction_config {
    bool compaction_enabled = true;
    /// Minimum live ratio threshold to trigger compaction (0.0 - 1.0)
    double min_live_ratio = 0.3;
    /// How often to check for compaction candidates
    std::chrono::seconds compaction_interval{2};
    /// Maximum number of segments to compact in one iteration
    size_t max_segments_per_compaction = 8;

    seastar::scheduling_group compaction_sg;
};

/// Compacts log segments by rewriting live records from segments with low live ratios.
/// This reclaims disk space occupied by deleted or overwritten records.
class compaction_manager {
    segment_manager& _sm;
    log_index& _index;
    compaction_config _config;

    seastar::gate _async_gate;
    serialized_action _compaction_action;
    bool _running{false};

    struct stats {
        uint64_t compaction_started{0};
        uint64_t segments_compacted{0};
        uint64_t records_rewritten{0};
        uint64_t records_skipped{0};
        uint64_t bytes_written{0};
    };

    stats _stats;

    seastar::metrics::metric_groups _metrics;

public:
    explicit compaction_manager(segment_manager& sm, log_index& index,
                          compaction_config config = {});

    compaction_manager(const compaction_manager&) = delete;
    compaction_manager& operator=(const compaction_manager&) = delete;

    future<> start();
    future<> stop();

    void enable_auto_compaction();
    future<> disable_auto_compaction();

    future<> trigger_compaction();

private:
    future<> compact();

    /// Compact a set of segments by rewriting their live records
    future<> compact_segments(std::vector<log_segment_id> segments);

    /// Flush the compaction buffer to segment manager
    future<> flush_compaction_buffer(write_buffer&);
};

}
