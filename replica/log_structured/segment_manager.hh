/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <cstdint>
#include <filesystem>
#include <seastar/core/shared_future.hh>
#include <seastar/core/file.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/shared_ptr.hh>
#include "types.hh"

namespace replica::log_structured {

/// Configuration for the segment manager
struct segment_manager_config {
    /// Base directory for storage files
    std::filesystem::path base_dir;
    /// Size of each segment (default: 128KB)
    size_t segment_size = 128 * 1024;
    /// Size of each file (default: 32MB)
    size_t file_size = 32 * 1024 * 1024;
    /// Total disk size limit
    size_t disk_size = 512 * 1024 * 1024;

    seastar::scheduling_group sched_group;
};

class segment_manager_impl;

class segment_manager {
    std::unique_ptr<segment_manager_impl> _impl;
private:
    segment_manager_impl& get_impl() noexcept;
    const segment_manager_impl& get_impl() const noexcept;
public:
    static constexpr size_t record_alignment = 8;
    static constexpr size_t disk_alignment = 4096;

    using compaction_trigger_fn = std::function<void()>;

    explicit segment_manager(segment_manager_config config);
    ~segment_manager();

    segment_manager(const segment_manager&) = delete;
    segment_manager& operator=(const segment_manager&) = delete;

    future<> start();
    future<> stop();

    future<log_location> write(bytes_view data, int64_t live_size);

    future<log_location> write(bytes_view data) {
        return write(data, data.size());
    }

    size_t get_write_alignment() const noexcept {
        return disk_alignment;
    }

    size_t get_segment_size() const noexcept;

    future<log_structured_segment_record> read(log_location location);

    void free_record(log_location location);

    /// Find segments suitable for compaction based on live ratio
    std::vector<log_segment_id> find_segments_for_compaction(double min_live_ratio, size_t max_segments);

    /// Free an entire segment (used after compaction)
    future<> free_segment(log_segment_id segment_id);

    /// Read all records from the specified segments
    future<> for_each_record(std::vector<log_segment_id> segments,
                            std::function<future<>(log_location, log_structured_segment_record)> callback);

    void set_compaction_trigger(compaction_trigger_fn fn);

    friend class segment_manager_impl;

};

}
