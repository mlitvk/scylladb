/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the sizes a logstor table reports to the tablet balancer: the live record bytes of the
// segments its groups own, and not the space of those segments.

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "replica/database.hh"
#include "replica/logstor/segment_manager.hh"
#include "service/storage_service.hh"
#include "test/boost/sstable_test.hh"
#include "test/lib/cql_test_env.hh"
#include "utils/disk_space_monitor.hh"

namespace {

constexpr size_t value_size = 20 * 1024;
constexpr int row_count = 20;

cql_test_config make_logstor_env_config(bool sparse_files = true) {
    cql_test_config cfg;
    cfg.db_config->logstor_sparse_files(sparse_files);
    cfg.db_config->experimental_features({db::experimental_features_t::feature::LOGSTOR});
    cfg.db_config->tablets_mode_for_new_keyspaces(db::tablets_mode_t::mode::enabled);
    cfg.db_config->logstor_disk_size_in_mb(16);
    cfg.db_config->logstor_file_size_in_mb(4);
    cfg.initial_tablets = 2;
    return cfg;
}

void create_table(cql_test_env& e) {
    e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v text) WITH storage_engine = 'logstor'").get();
}

// Writes the same rows, with values of the same size, on each call.
void write_rows(cql_test_env& e) {
    for (int pk = 0; pk < row_count; ++pk) {
        e.execute_cql(format("INSERT INTO ks.t (pk, v) VALUES ({}, '{}')", pk, sstring(value_size, 'x'))).get();
    }
}

// Moves the records into the segments of their groups, which is where data_size() counts them.
void flush(cql_test_env& e) {
    replica::database::flush_logstor_separator_on_all_shards(e.db()).get();
}

struct group_sizes {
    uint64_t data_size = 0;
    uint64_t live_disk_space_used = 0;

    group_sizes operator+(const group_sizes& o) const {
        return {data_size + o.data_size, live_disk_space_used + o.live_disk_space_used};
    }
};

// The sum over the storage groups of the table on all shards.
group_sizes table_sizes(cql_test_env& e) {
    return e.db().map_reduce0([] (replica::database& db) {
        group_sizes sizes;
        auto& t = db.find_column_family("ks", "t");
        for (auto& [id, sg] : column_family_test::get_storage_group_manager(t)->storage_groups()) {
            sizes = sizes + group_sizes{sg->data_size(), sg->live_disk_space_used()};
        }
        return sizes;
    }, group_sizes{}, std::plus<group_sizes>()).get();
}

// The live record bytes the index of the table refers to, on all shards.
uint64_t index_live_record_bytes(cql_test_env& e) {
    return e.db().map_reduce0([] (replica::database& db) {
        return db.find_column_family("ks", "t").logstor_live_record_bytes();
    }, uint64_t(0), std::plus<uint64_t>()).get();
}

// The space of the logstor segments of this shard: the space of the files allocated for them, or,
// with sparse files, of the segments that hold records.
uint64_t logstor_space(const replica::database& db) {
    const auto usage = db.find_column_family("ks", "t").get_logstor_segment_manager().get_usage();
    return db.get_config().logstor_sparse_files() ? usage.segment_bytes_in_use : usage.disk_usage;
}

void set_critical_level(cql_test_env& e, sstring level) {
    e.db_config().logstor_critical_utilization_level.set_value_on_all_shards(level, utils::config_file::config_source::Internal).get();
}

// Whether the logstor pool of each shard holding records of the table is critical, and of no other.
// Returns false if no shard holds records.
bool pool_critical_where_records_are(cql_test_env& e) {
    return e.db().map_reduce0([] (replica::database& db) {
        const bool has_records = db.find_column_family("ks", "t").logstor_live_record_bytes() > 0;
        BOOST_REQUIRE_EQUAL(db.logstor_pool_critical(), has_records);
        return has_records;
    }, false, std::logical_or<bool>()).get();
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_data_size_follows_live_records) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_table(e);
        write_rows(e);
        flush(e);

        const auto sizes = table_sizes(e);
        BOOST_REQUIRE_GE(sizes.data_size, row_count * value_size);
        BOOST_REQUIRE_EQUAL(sizes.data_size, index_live_record_bytes(e));
        BOOST_REQUIRE_GT(sizes.live_disk_space_used, sizes.data_size);

        // An overwrite kills the old records in the segments of the group, so the space grows while
        // the data stays the same.
        write_rows(e);
        flush(e);
        const auto sizes_after_overwrite = table_sizes(e);
        BOOST_REQUIRE_EQUAL(sizes_after_overwrite.data_size, sizes.data_size);
        BOOST_REQUIRE_GT(sizes_after_overwrite.live_disk_space_used, sizes.live_disk_space_used);
    }, make_logstor_env_config());
}

SEASTAR_TEST_CASE(test_table_load_stats_reports_data_size) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_table(e);
        write_rows(e);
        write_rows(e);
        flush(e);

        e.db().invoke_on_all([] (replica::database& db) {
            auto& t = db.find_column_family("ks", "t");
            const auto stats = t.table_load_stats();
            const auto& tmap = t.get_effective_replication_map()->get_token_metadata().tablets().get_tablet_map(t.schema()->id());
            const auto& tablet_sizes = stats.tablet_ls.tablet_sizes.at(t.schema()->id());
            const auto& storage_groups = column_family_test::get_storage_group_manager(t)->storage_groups();

            uint64_t data_size = 0;
            uint64_t live_disk_space_used = 0;
            for (auto& [id, sg] : storage_groups) {
                BOOST_REQUIRE_EQUAL(tablet_sizes.at(tmap.get_token_range(locator::tablet_id(id))), sg->data_size());
                data_size += sg->data_size();
                live_disk_space_used += sg->live_disk_space_used();
            }
            BOOST_REQUIRE_EQUAL(tablet_sizes.size(), storage_groups.size());
            BOOST_REQUIRE_EQUAL(stats.table_ls.size_in_bytes, data_size);
            // The rows were overwritten, so the space of the segments is not the size.
            BOOST_REQUIRE_NE(stats.table_ls.size_in_bytes, live_disk_space_used);
        }).get();
    }, make_logstor_env_config());
}

// The tablet sizes and the logstor space not in them add up to the space of the logstor segments.
void check_logstor_space_not_in_tablet_sizes(bool sparse_files) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        create_table(e);
        write_rows(e);
        write_rows(e);
        flush(e);

        e.db().invoke_on_all([] (replica::database& db) {
            const auto& t = db.find_column_family("ks", "t");
            const uint64_t tablet_sizes = t.table_load_stats().table_ls.size_in_bytes;
            BOOST_REQUIRE_GT(tablet_sizes, 0);
            BOOST_REQUIRE_EQUAL(tablet_sizes + db.logstor_space_not_in_tablet_sizes(), logstor_space(db));
        }).get();
    }, make_logstor_env_config(sparse_files)).get();
}

SEASTAR_THREAD_TEST_CASE(test_logstor_space_not_in_tablet_sizes_sparse_files) {
    check_logstor_space_not_in_tablet_sizes(true);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_space_not_in_tablet_sizes_allocated_files) {
    check_logstor_space_not_in_tablet_sizes(false);
}

// With files allocated in full, the files are not in the free space of the disk, so the capacity of
// the node must add them back: all of them, not only the live record bytes the tablets report.
SEASTAR_TEST_CASE(test_effective_capacity_counts_the_pool) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_table(e);
        write_rows(e);
        write_rows(e);
        flush(e);

        const uint64_t logstor_space_on_all_shards = e.db().map_reduce0([] (replica::database& db) {
            return logstor_space(db);
        }, uint64_t(0), std::plus<uint64_t>()).get();
        BOOST_REQUIRE_GT(logstor_space_on_all_shards, 0);

        // The capacity reads the free space the disk space monitor polled last. Read it on both
        // sides of the stats, and retry if a poll came in between.
        for (;;) {
            const uint64_t available = e.disk_space_monitor().space().available;
            const auto stats = e.get_storage_service().local().load_stats_for_tablet_based_tables().get();
            if (e.disk_space_monitor().space().available != available) {
                continue;
            }
            const auto host = e.db().local().get_token_metadata().get_my_id();
            // The logstor table is the only table with tablets, so its tablets and the rest of the
            // logstor files are all the capacity adds to the free space.
            BOOST_REQUIRE_EQUAL(stats.tablet_stats.at(host).effective_capacity, available + logstor_space_on_all_shards);
            break;
        }
    }, make_logstor_env_config(false));
}

SEASTAR_TEST_CASE(test_pool_critical_follows_the_level) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_table(e);
        set_critical_level(e, "0.000001");
        BOOST_REQUIRE(!pool_critical_where_records_are(e));

        write_rows(e);
        BOOST_REQUIRE(pool_critical_where_records_are(e));

        set_critical_level(e, "1");
        e.db().invoke_on_all([] (replica::database& db) {
            BOOST_REQUIRE(!db.logstor_pool_critical());
        }).get();
    }, make_logstor_env_config());
}
