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
#include "test/boost/sstable_test.hh"
#include "test/lib/cql_test_env.hh"

namespace {

constexpr size_t value_size = 20 * 1024;
constexpr int row_count = 20;

cql_test_config make_logstor_env_config() {
    cql_test_config cfg;
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
