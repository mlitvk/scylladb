/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <chrono>
#include <map>
#include <seastar/core/scheduling.hh>
#include "db/cache_tracker.hh"
#include "readers/mutation_reader.hh"
#include "readers/mutation_reader_fwd.hh"
#include "replica/logstor/compaction.hh"
#include "readers/mutation_source.hh"
#include "types.hh"
#include "index.hh"
#include "segment_manager.hh"
#include "write_buffer_ring.hh"
#include "cache.hh"
#include "mutation/mutation.hh"
#include "dht/decorated_key.hh"

class frozen_mutation;

namespace replica {

class database;

namespace logstor {

extern seastar::logger logstor_logger;

// The number of buffers in the write buffer ring, so at most `ring_size - 1` flushes are in flight.
// With a sync period of 0, records collect in a buffer only while all flushes are in flight, so a
// larger ring writes more and smaller chunks, each padded to a block, and adds no throughput. 4
// flushes in flight are sufficient to overlap the disk write with the index and separator work.
inline constexpr size_t default_write_buffer_ring_size = 5;

// How long the write buffer ring waits after the first record of a buffer that is not full before
// it seals the buffer. A longer period writes fewer and larger chunks at low load, but each write
// then waits up to the period. With 0, records still collect in a buffer while all flushes are in
// flight, so the chunks are not small under load.
inline constexpr std::chrono::milliseconds default_write_buffer_sync_period{0};

struct logstor_config {
    segment_manager_config segment_manager_cfg;
    seastar::scheduling_group flush_sg;
    size_t max_queued_write_bytes{0};
    size_t write_buffer_ring_size{default_write_buffer_ring_size};
    std::chrono::milliseconds write_buffer_sync_period{default_write_buffer_sync_period};
};

class logstor {

    struct stats {
        uint64_t write_failures{0};
    };

    segment_manager _segment_manager;
    write_buffer_ring _write_buffer_ring;
    cache_tracker _cache_tracker;
    seastar::metrics::metric_groups _metrics;
    stats _stats;
    seastar::gate _async_gate;

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

    std::unique_ptr<primary_index> make_primary_index(bool cache_enabled);

    future<> flush_to_separator();

    future<> write(const mutation&, write_target target, db::timeout_clock::time_point timeout) noexcept;

    // Writes a frozen mutation without unfreezing it. s must be the schema version that the
    // mutation was frozen with.
    future<> write(const frozen_mutation&, const schema& s, write_target target, db::timeout_clock::time_point timeout) noexcept;

    // Create a mutation reader for a partition range.
    mutation_reader make_reader(schema_ptr schema,
                                const primary_index& index,
                                reader_permit permit,
                                const dht::partition_range& pr,
                                const query::partition_slice& slice,
                                tracing::trace_state_ptr trace_state = nullptr,
                                streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
                                mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no);

    // Debug introspection for SELECT * FROM MUTATION_FRAGMENTS(): returns one
    // mutation source per place the key's data currently lives in - the
    // in-memory logstor cache (if cached) and the log record - keyed by a name
    // identifying the source. Returns an empty map when the key is not in the
    // index. Neither source reads, populates or otherwise perturbs the cache.
    // The cached mutation is snapshotted here and charged to the given permit,
    // since the cache entry may be evicted before the source is read from.
    std::map<sstring, mutation_source> make_mutation_sources_for_dump(schema_ptr, const primary_index&, const dht::decorated_key&, reader_permit);

private:
    future<> do_write(const schema&, dht::decorated_key, api::timestamp_type, record_value,
            write_target target, db::timeout_clock::time_point timeout);
};

} // namespace logstor
} // namespace replica
