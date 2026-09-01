/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <cstdint>
#include <filesystem>
#include <seastar/core/shared_future.hh>
#include <seastar/core/file.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/bool_class.hh>
#include "bytes_fwd.hh"
#include "mutation_writer/token_group_based_splitting_writer.hh"
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/write_buffer.hh"
#include "replica/logstor/compaction.hh"
#include "types.hh"
#include "utils/updateable_value.hh"

namespace replica {

class database;

namespace logstor {

class compaction_manager;
class segment_set;
class primary_index;

static constexpr uint64_t default_segment_size = 128 * 1024;
static constexpr uint64_t default_file_size = 32 * 1024 * 1024;
// Two buffers of a segment each for every one of eight hot groups, at the default segment size.
static constexpr size_t default_direct_write_memory = 2 * 1024 * 1024;

// Whether a record is being offered to the direct write path for the first time, or again after
// its caller waited for the group's flush. The second offer does not count the record against the
// group's write rate a second time, and is never asked to wait again.
enum class direct_write_attempt {
    first,
    after_flush,
};

// What the direct write path did with a record, see segment_manager::try_write_direct().
struct direct_write_result {
    // Where the record landed, when the path took it.
    std::optional<log_location> location;
    // Set instead when the group could have taken the record but its buffer is full and the one
    // before it is still being written out. Waiting for that flush and offering the record again
    // costs at most one segment write - about what the ordinary path would wait for anyway, the
    // disk being the same - and saves the record the second trip to the disk that path gives it.
    // See logstor_group::await_direct_flush().
    bool retry_after_flush = false;
};

/// Configuration for the segment manager
struct segment_manager_config {
    std::filesystem::path base_dir;
    uint64_t segment_size = default_segment_size;
    uint64_t file_size = default_file_size;
    uint64_t disk_size;
    bool format_on_startup = true;
    bool compaction_enabled = true;
    size_t max_segments_per_compaction = 32;
    utils::updateable_value<double> trigger_compaction_threshold{0.05};
    seastar::scheduling_group compaction_sg;
    utils::updateable_value<float> compaction_static_shares;
    utils::updateable_value<float> compaction_max_shares;
    seastar::scheduling_group separator_sg;
    seastar::scheduling_group split_compaction_sg;
    // Whether a group that writes fast enough may write into a segment of its own instead of the
    // shared active segment, which takes its records to the disk once rather than twice. The
    // records of such a write are acknowledged before they are on the disk, so this is only on in
    // the periodic commitlog sync mode, whose semantics it matches.
    bool direct_group_writes = false;
    // Whether groups that write fast enough may actually use the path, which an operator can change
    // on a running node - unlike the two above, which are what the shard is built for. Turning it
    // off stops records from being taken directly at once and returns the buffers of the groups
    // that hold them within a sync period; turning it on lets the controller give them out again.
    utils::updateable_value<bool> direct_writes_enabled{true};
    // How long a partly filled direct buffer may wait for more records before it is written out,
    // which is what bounds the loss window.
    std::chrono::milliseconds direct_sync_period{10000};
    // What a group has to write in one sync period to be given direct buffers. Zero means half a
    // segment, which is the point below which a partly filled segment costs more in occupied pool
    // slots than the second write it saves.
    uint64_t direct_hot_threshold_bytes{0};
    // The memory a shard may hold in direct buffers. Each hot group holds two buffers of one
    // segment each, so this is what bounds how many groups may be hot at once, and with it the
    // records the direct path can lose. Less than two segments' worth leaves no room for a single
    // hot group, which turns the direct path off.
    size_t direct_write_memory{default_direct_write_memory};
};

// What the logstor of one shard is using and what it has to use. Every field is shard wide and
// covers every table, unlike the statistics of the segments a compaction group owns. Read together
// through a single accessor, since a caller that wants one of them usually wants the others to
// compare it against.
struct segment_manager_usage {
    // Segments that can still be allocated for writing, and the number of segment slots the shard
    // has, unlike the segments a compaction group owns.
    uint64_t free_segments{0};
    uint64_t total_segments{0};
    // Bytes of the segment pool the shard is configured to hold, which is the space its tables can
    // grow into. Unlike total_segments, which counts the slots that are there right now, this
    // ignores the segments recovery found beyond the configured limit: those are retired as they are
    // freed and cannot be allocated again, so they are not room to grow into.
    uint64_t segment_pool_size{0};
    // Bytes of the files allocated for segments. This is the space logstor takes on disk, which is
    // more than the segments the tables own take: it also covers the segments that are free, and
    // files are never given back during normal operation, only retired on recovery.
    uint64_t disk_usage{0};
    // Memory the segment manager holds for its own bookkeeping. The indexes of the tables are not
    // part of it.
    size_t memory_usage{0};
};

struct segment_snapshot {
    log_segment_id segment_id;
    segment_ref seg_ref;
    noncopyable_function<future<seastar::input_stream<char>>(const file_input_stream_options&)> source;
};

class segment_stream_sink {
public:
    virtual ~segment_stream_sink() = default;
    virtual log_segment_id segment_id() const noexcept = 0;
    virtual future<output_stream<char>> output() = 0;
    virtual future<> close() = 0;
    virtual future<> abort() = 0;
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

    future<> do_recovery(replica::database&);
    future<> do_recovery_for_test();

    future<> start();
    future<> stop();

    future<> write(write_buffer& wb);

    // Takes a record straight into the buffer of the group it belongs to, and says where in the
    // group's segments it will be. The record is in memory only until that buffer is written out,
    // so a caller that inserts this location into the index is acknowledging the write before it is
    // durable - see direct_group_writes.
    //
    // Gives back neither a location nor a reason to wait when the group is not taking direct
    // writes, when it has no buffer to take the record right now, or when the record does not fit
    // one. The caller then writes it the ordinary way. Never waits itself.
    //
    // It takes the record's parts rather than a writer over them, and builds the writer itself once
    // it knows the group can be written into: the caller has no record of its own yet - the record
    // that goes through the shared buffer is built only if this declines the write - and a
    // configuration with direct writes off must not pay for a writer nothing will use.
    direct_write_result try_write_direct(logstor_group&, const log_record_header_view&, bytes_view value,
            direct_write_attempt = direct_write_attempt::first);

    // The bytes of one record, as they are on disk: the read of a record does not parse its
    // header, which holds a key the read already has, and decodes its value straight out of
    // this buffer rather than copying it.
    //
    // The disk read is issued over the block-aligned range that covers the record and trimmed back
    // to it, which costs bandwidth and saves latency - see the read path in the implementation.
    future<temporary_buffer<char>> read_record_bytes(log_location location);

    void on_add_record(log_location location) noexcept override;
    void on_free_record(log_location location) noexcept override;

    compaction_manager& get_compaction_manager() noexcept;
    const compaction_manager& get_compaction_manager() const noexcept;

    uint64_t get_segment_size() const noexcept;

    // The times a logstor file was opened. Files are opened once and held open for the life of the
    // shard, so this stops moving once the shard has started.
    uint64_t files_opened() const noexcept;

    // Removes all the segments of the group and frees them. Waits for an ongoing compaction of
    // the group and keeps compaction disabled while discarding, so the caller doesn't have to.
    // The index must be cleared first, so that no record of the group is reachable.
    future<> discard_segments(logstor_group&);

    segment_manager_usage get_usage() const noexcept;

    future<> await_pending_writes();

    future<utils::chunked_vector<segment_snapshot>> make_snapshot(logstor_group& cg);

    // Create an output stream to write a segment (for receiving from remote node)
    // Allocates a new local segment and returns an output stream for writing to the segment.
    future<std::unique_ptr<segment_stream_sink>> create_segment_output_stream(replica::database&);

    friend class segment_manager_impl;

};

}
}