/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <optional>
#include <seastar/core/scheduling.hh>
#include "db/cache_tracker.hh"
#include "readers/mutation_reader.hh"
#include "replica/logstor/compaction.hh"
#include "types.hh"
#include "index.hh"
#include "record_format.hh"
#include "segment_manager.hh"
#include "write_buffer.hh"
#include "cache.hh"
#include "mutation/mutation.hh"
#include "dht/decorated_key.hh"

namespace replica {

class database;

namespace logstor {

extern seastar::logger logstor_logger;

struct logstor_config {
    segment_manager_config segment_manager_cfg;
    seastar::scheduling_group flush_sg;
    size_t max_queued_write_bytes{0};
    size_t write_buffer_ring_size{5};
};

class logstor {

    struct stats {
        uint64_t write_failures{0};
    };

    segment_manager _segment_manager;
    buffered_writer _write_buffer;
    // The buffer every write of this shard encodes its record value through.
    row_value_encoder _encoder;
    cache_tracker _cache_tracker;
    seastar::metrics::metric_groups _metrics;
    stats _stats;
    seastar::gate _async_gate;

    // The part of a write that goes through the shared write buffer: the record waits to be taken
    // into a buffer and for that buffer to reach a segment, and is then put in the index.
    future<> write_through_buffer(log_record_writer, primary_index_key, api::timestamp_type ts, primary_index&,
            write_target, db::timeout_clock::time_point timeout, seastar::gate::holder);

    // A record the direct path could have taken but for the group's buffer being full while the one
    // before it was still being written out. It waits for that write and offers the record again,
    // and only takes the shared write buffer - and with it a second trip to the disk - if the group
    // still cannot have it. The record is owned by then, which is what the shared path needs anyway
    // and what this needs to survive the wait.
    future<> write_direct_after_flush(primary_index_key, bytes value, api::timestamp_type ts, table_id,
            primary_index&, write_target, db::timeout_clock::time_point timeout, seastar::gate::holder);

    // The part of a read the cache could not answer: the record is read from its segment, decoded,
    // and admitted to the cache. What read() holds for the duration of a read is handed over to it,
    // because this is where such a read ends.
    future<std::optional<mutation>> read_from_segment(const schema&, const primary_index&, const dht::decorated_key&,
            index_entry entry_for_read, cache_tracker* cache, seastar::gate::holder,
            utils::phased_barrier::operation, cache_tracker::read_accounter);

public:

    logstor(logstor_config, ::cache_tracker& shared_cache_tracker);

    logstor(const logstor&) = delete;
    logstor& operator=(const logstor&) = delete;

    future<> do_recovery(replica::database&);
    future<> do_recovery_for_test();

    future<> start();
    future<> stop();

    size_t get_memory_usage() const;

    segment_manager& get_segment_manager() noexcept;
    const segment_manager& get_segment_manager() const noexcept;

    compaction_manager& get_compaction_manager() noexcept;
    const compaction_manager& get_compaction_manager() const noexcept;

    std::unique_ptr<primary_index> make_primary_index(schema_ptr schema, bool cache_enabled);

    future<> write(const mutation&, write_target target, db::timeout_clock::time_point timeout);

    future<std::optional<mutation>> read(const schema&, const primary_index&, const dht::decorated_key&, const query::partition_slice&);

    /// Create a mutation reader for a specific key
    mutation_reader make_reader(schema_ptr schema,
                                       const primary_index& index,
                                       reader_permit permit,
                                       const dht::partition_range& pr,
                                       const query::partition_slice& slice,
                                       tracing::trace_state_ptr trace_state = nullptr);

    future<> flush_to_separator();

};

} // namespace logstor
} // namespace replica
