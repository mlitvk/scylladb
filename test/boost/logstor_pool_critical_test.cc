/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the pool-critical level of a logstor shard: the live records take more of the pool of
// the shard than logstor_critical_utilization_level.

#include <seastar/testing/test_case.hh>

#include "db/config.hh"
#include "replica/database.hh"
#include "service/storage_service.hh"
#include "test/lib/cql_test_env.hh"

class storage_service_test {
public:
    static future<locator::load_stats> load_stats_for_tablet_based_tables(service::storage_service& ss) {
        return ss.load_stats_for_tablet_based_tables();
    }
};

namespace {

constexpr size_t value_size = 20 * 1024;
constexpr int row_count = 20;

cql_test_config make_logstor_env_config() {
    cql_test_config cfg;
    cfg.db_config->logstor_sparse_files(true);
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

void write_rows(cql_test_env& e) {
    for (int pk = 0; pk < row_count; ++pk) {
        e.execute_cql(format("INSERT INTO ks.t (pk, v) VALUES ({}, '{}')", pk, sstring(value_size, 'x'))).get();
    }
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

SEASTAR_TEST_CASE(test_pool_critical_marks_the_shards) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_table(e);
        write_rows(e);
        auto& ss = e.get_storage_service().local();
        const auto host = e.db().local().get_token_metadata().get_my_id();

        auto stats = storage_service_test::load_stats_for_tablet_based_tables(ss).get();
        BOOST_REQUIRE(stats.logstor_pool_critical.at(host).empty());
        BOOST_REQUIRE(!stats.critical_disk_utilization.at(host));

        set_critical_level(e, "0.000001");
        auto shards_with_records = e.db().map_reduce0([] (replica::database& db) {
            const bool has_records = db.find_column_family("ks", "t").logstor_live_record_bytes() > 0;
            return has_records ? std::vector<shard_id>{this_shard_id()} : std::vector<shard_id>{};
        }, std::vector<shard_id>{}, [] (std::vector<shard_id> a, std::vector<shard_id> b) {
            a.insert(a.end(), b.begin(), b.end());
            return a;
        }).get();
        std::ranges::sort(shards_with_records);
        BOOST_REQUIRE(!shards_with_records.empty());

        stats = storage_service_test::load_stats_for_tablet_based_tables(ss).get();
        BOOST_REQUIRE(stats.logstor_pool_critical.at(host) == shards_with_records);
        // The pool of a shard says nothing about the disk.
        BOOST_REQUIRE(!stats.critical_disk_utilization.at(host));
    }, make_logstor_env_config());
}
