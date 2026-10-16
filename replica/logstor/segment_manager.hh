/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <seastar/core/shared_future.hh>
#include <seastar/core/file.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/bool_class.hh>
#include "bytes_fwd.hh"
#include "mutation_writer/token_group_based_splitting_writer.hh"
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/write_buffer.hh"
#include "replica/logstor/compaction.hh"
#include "replica/logstor/record_stream.hh"
#include "replica/logstor/segment_stats.hh"
#include "types.hh"
#include "utils/updateable_value.hh"

namespace replica {

namespace logstor {

class compaction_manager;
class segment_set;
class primary_index;
class index_read_guard;
class recovery_catalog;

static constexpr uint64_t default_segment_size = 128 * 1024;
static constexpr uint64_t default_file_size = 32 * 1024 * 1024;
static constexpr double default_compaction_min_utilization_threshold = 0.1;

/// Configuration for the segment manager
struct segment_manager_config {
    std::filesystem::path base_dir;
    uint64_t segment_size = default_segment_size;
    uint64_t file_size = default_file_size;
    uint64_t disk_size;
    bool format_on_startup = true;
    // Create files sparse: extend them to their nominal size without
    // preallocating or zero-filling. Saves disk space and I/O, at the cost of
    // fragmentation and of not reserving space up front.
    bool sparse_files = false;
    bool compaction_enabled = true;
    // The space floor as a fraction of the disk, see compaction_space_floor().
    utils::updateable_value<double> compaction_space_floor{0.03};
    // A segment is a candidate for automatic compaction while its utilization is at most this.
    // A value outside [0, 1] applies as the nearest bound.
    utils::updateable_value<double> compaction_utilization_threshold{0.5};
    // The threshold at the ceiling of the threshold curve and above, see
    // utilization_threshold_curve. At or above compaction_utilization_threshold the threshold is
    // constant above the anchor.
    utils::updateable_value<double> compaction_min_utilization_threshold{default_compaction_min_utilization_threshold};
    // The pool utilization above which the shard refuses streamed tablets.
    utils::updateable_value<double> critical_utilization_level{0.85};
    seastar::scheduling_group compaction_sg;
    utils::updateable_value<float> compaction_static_shares;
    // The shares of the compaction scheduling group at the space floor, see
    // compaction_rate_controller.
    utils::updateable_value<float> compaction_max_shares{2000};
    seastar::scheduling_group separator_sg;
    seastar::scheduling_group split_compaction_sg;
    // A shard receives at most this many record streams at once. Each stream holds
    // record_stream_sink_buffers write buffers for its life, so a stream above the limit waits for
    // one to end.
    size_t max_record_stream_sinks = 2;
};

// What a recovery found on disk, and what it did with it.
struct recovery_summary {
    uint64_t found_segments{0};
    // The segments that hold a record of an index. Recovery gives them back to their compaction
    // groups.
    uint64_t used_segments{0};
    // The segments that recovery puts on the free list: the found segments below the configured
    // count that hold no record of an index.
    uint64_t free_segments{0};
    // The records of a tablet that is not on this shard, which recovery drops.
    uint64_t dropped_records{0};
    // The full segments that fit no single compaction group of this shard. start() separates their
    // records one by one.
    uint64_t separated_segments{0};
    // The sequence of the first segment that is allocated after recovery.
    segment_sequence next_segment_seq{0};
};

// What the logstor of one shard is using. Every field is shard wide and covers every table, unlike
// the statistics of the segments a compaction group owns. Kept in one struct, read through a single
// accessor, so that the shard wide usage numbers stay in one place and another one can join them
// without an accessor of its own.
struct segment_manager_usage {
    // Bytes of the files allocated for segments. This is the space logstor holds on disk, which is
    // more than the space its data takes: it also covers the segments that are free, and files are
    // never given back during normal operation, only retired on recovery.
    uint64_t disk_usage{0};
    // Bytes of the segments that hold records, which is the space the data of this shard takes:
    // the segments the compaction groups own, the one being written and the ones a read still
    // refers to. Less than disk_usage by the free segments and by the room in the files holding
    // them, and the logstor part of the load a node reports.
    uint64_t segment_bytes_in_use{0};
    // The space the segments take from the filesystem: disk_usage, or segment_bytes_in_use with
    // sparse files, whose blocks are free until a segment writes them. A freed segment keeps the
    // blocks it wrote, which this does not count.
    uint64_t space_on_disk{0};
    // Memory the segment manager holds: the descriptors of every segment and the buffers of the
    // compaction, separator and record stream pools. The indexes of the tables are not part of it,
    // nor is the buffered writer of the write path, which logstor holds rather than the segment
    // manager.
    size_t memory_usage{0};
};

class segment_manager_impl;
class log_index;

class segment_manager : public space_accounting_subscriber {
    std::unique_ptr<segment_manager_impl> _impl;
private:
    segment_manager_impl& get_impl() noexcept;
    const segment_manager_impl& get_impl() const noexcept;
public:

    explicit segment_manager(segment_manager_config config);
    ~segment_manager();

    segment_manager(const segment_manager&) = delete;
    segment_manager& operator=(const segment_manager&) = delete;

    // The catalog must live until start() returns: start() separates through it the records of
    // the segments that recovery could not give whole to a compaction group.
    future<recovery_summary> do_recovery(recovery_catalog&);

    // The index a record found on disk goes into, or nullptr to drop the record.
    using index_resolver = std::function<primary_index*(const record_header&)>;

    // Rebuilds indexes from the records on disk the way that recovery does. The rebuild scans every
    // segment of the allocated files and offers each record to the index that the resolver names,
    // and the index decides by its conflict resolution rules which entry to keep. Nothing may
    // write, separate or compact while the rebuild runs.
    future<> rebuild_index_from_disk(index_resolver);

    future<> start();
    future<> stop();

    future<> write(write_buffer& wb);

    // Reads the record at a location from the index. Keep the guard of the location until the
    // read is complete, so that the segment stays allocated.
    future<log_record> read(const index_read_guard&, record_location location);

    void on_add_record(record_location location) noexcept override;
    void on_free_record(record_location location) noexcept override;

    compaction_manager& get_compaction_manager() noexcept;
    const compaction_manager& get_compaction_manager() const noexcept;

    uint64_t get_segment_size() const noexcept;

    // The fraction of the configured pool of this shard that the live records take, the records
    // the separator has not moved yet included, because they take the pool too. After a shrink the
    // segments above the configured pool hold records until they are retired, so the fraction can
    // exceed 1: the data does not fit the pool.
    double pool_utilization() const noexcept;
    // Whether the live records take more of the pool than critical_utilization_level, above which
    // the rest of the pool is the space compaction needs to run.
    bool pool_critical() const noexcept;

    // The statistics of the segments that the compaction groups of this shard own, with their
    // distribution by utilization. The segment_set of each group adds its changes to them. A group
    // gives a pointer to them to the constructor of its segment_set.
    segment_stats& shard_segment_stats() noexcept;
    const segment_stats& shard_segment_stats() const noexcept;

    // Returns the path of the file holding the given segment (for debug/introspection).
    // The path is computed from the segment id, the segment is not looked up, so the file
    // does not have to hold a live segment. The file names are shard local, so this has to
    // be called on the shard owning the segment.
    sstring get_segment_file_path(log_segment_id) const;

    // Removes all the segments of the group and frees them. Waits for an ongoing compaction of
    // the group and keeps compaction disabled while discarding, so the caller doesn't have to.
    // The index must be cleared first, so that no record of the group is reachable.
    future<> discard_segments(logstor_group&);

    segment_manager_usage get_usage() const noexcept;

    future<> await_pending_writes();

    // Returns the ids of the segments that the group owns, in no particular order, for tests.
    utils::chunked_vector<log_segment_id> segment_ids_for_test(const logstor_group&) const;

    // Calls `fn` with each record of the segment, also with the dead records, for tests.
    future<> for_each_record_for_test(log_segment_id, noncopyable_function<future<>(const chunk_record&)> fn);

    const record_stream_stats& stream_stats() const noexcept;

    // Makes a source that sends the live records of the group as a record stream. Returns nullptr
    // if the group has no live record. Compaction of the group stays disabled until the source is
    // destroyed.
    future<std::unique_ptr<record_stream_source>> make_record_stream_source(logstor_group&);

    // Makes a sink that receives a record stream of the table. The sink is work in progress of the
    // segment manager until it is closed or aborted: stop() waits for it, and after stop() starts
    // this fails with gate_closed_exception. Waits while max_record_stream_sinks sinks are open.
    future<std::unique_ptr<record_stream_sink>> make_record_stream_sink(table_id, record_stream_sink::target_resolver);

    friend class segment_manager_impl;

};

}
}