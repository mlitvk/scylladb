/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <filesystem>
#include <memory>

#include <seastar/core/scheduling.hh>

#include "db/cache_tracker.hh"
#include "replica/logstor/cache.hh"
#include "replica/logstor/compaction.hh"
#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/segment_manager.hh"
#include "schema/schema_fwd.hh"
#include "utils/updateable_value.hh"

// What a test needs around a logstor instance to write to it and read from it: the cache it shares
// with the row cache of a database, and a compaction group to write into. A test has neither a
// database nor a table, which is what owns those in a running node.
namespace tests::logstor {

struct shared_logstor_cache {
    ::cache_tracker shared_tracker;
    replica::logstor::cache_tracker logstor_tracker;

    shared_logstor_cache()
        : shared_tracker(utils::updateable_value<double>(1.0), ::cache_tracker::register_metrics::no)
        , logstor_tracker(shared_tracker) {
    }
};

// The defaults are a logstor small enough for a unit test: a disk of four files of 32 segments each.
struct logstor_params {
    size_t segment_size = 128 * 1024;
    size_t file_size = 32 * 128 * 1024;
    size_t disk_size = 4 * 32 * 128 * 1024;
    bool format_on_startup = true;
    bool compaction_enabled = true;
    size_t max_segments_per_compaction = 8;
    bool direct_group_writes = false;
    // The live switch a node's operator has. Tests that change it hold a source of their own.
    utils::updateable_value<bool> direct_writes_enabled{true};
    std::chrono::milliseconds direct_sync_period{10000};
    uint64_t direct_hot_threshold_bytes = 0;
    // The memory the direct write path may hold. Zero means room for eight hot groups at the
    // segment size of the test, which is two buffers each.
    size_t direct_write_memory = 0;
};

inline replica::logstor::logstor_config make_test_logstor_config(const std::filesystem::path& base_dir, logstor_params params = {}) {
    return replica::logstor::logstor_config{
        .segment_manager_cfg = {
            .base_dir = base_dir,
            .segment_size = params.segment_size,
            .file_size = params.file_size,
            .disk_size = params.disk_size,
            .format_on_startup = params.format_on_startup,
            .compaction_enabled = params.compaction_enabled,
            .max_segments_per_compaction = params.max_segments_per_compaction,
            .compaction_sg = seastar::current_scheduling_group(),
            .compaction_static_shares = utils::updateable_value<float>(0.0f),
            .compaction_max_shares = utils::updateable_value<float>(2000.0f),
            .separator_sg = seastar::current_scheduling_group(),
            .split_compaction_sg = seastar::current_scheduling_group(),
            .direct_group_writes = params.direct_group_writes,
            .direct_writes_enabled = params.direct_writes_enabled,
            .direct_sync_period = params.direct_sync_period,
            .direct_hot_threshold_bytes = params.direct_hot_threshold_bytes,
            .direct_write_memory = params.direct_write_memory ? params.direct_write_memory : 8 * 2 * params.segment_size,
        },
        .flush_sg = seastar::current_scheduling_group(),
    };
}

// A compaction group of a table, which is what the separator and compaction work on. Registers
// itself with the compaction manager, so it must be destroyed in a seastar thread.
class test_logstor_group final : public replica::logstor::logstor_group {
    ::table_id _table_id;
    std::unique_ptr<replica::logstor::primary_index> _owned_index;
    replica::logstor::primary_index& _index;
    replica::logstor::compaction_manager& _cm;
public:
    test_logstor_group(schema_ptr schema, replica::logstor::logstor& ls, bool cache_enabled = false)
        // Accounts its segments straight into the shard level of the statistics rollup: a test group
        // stands on its own, with no table level between it and the shard.
        : logstor_group(ls.get_segment_manager().get_segment_size(), &ls.get_compaction_manager().shard_segment_stats())
        , _table_id(schema->id())
        , _owned_index(ls.make_primary_index(schema, cache_enabled))
        , _index(*_owned_index)
        , _cm(ls.get_compaction_manager()) {
        _cm.add(*this);
    }

    // A group of a table that already has an index: the index is per table, and all the groups of
    // a table share it. This is what the groups of a split have.
    test_logstor_group(schema_ptr schema, replica::logstor::logstor& ls, replica::logstor::primary_index& index)
        // Accounts its segments straight into the shard level of the statistics rollup: a test group
        // stands on its own, with no table level between it and the shard.
        : logstor_group(ls.get_segment_manager().get_segment_size(), &ls.get_compaction_manager().shard_segment_stats())
        , _table_id(schema->id())
        , _index(index)
        , _cm(ls.get_compaction_manager()) {
        _cm.add(*this);
    }

    ~test_logstor_group() override {
        _cm.remove(*this).get();
    }

    ::table_id table_id() const noexcept override {
        return _table_id;
    }

    replica::logstor::primary_index& logstor_index() noexcept override {
        return _index;
    }

    const replica::logstor::primary_index& logstor_index() const noexcept override {
        return _index;
    }

protected:
    replica::logstor::compaction_manager& logstor_compaction_manager() noexcept override {
        return _cm;
    }
};

} // namespace tests::logstor
