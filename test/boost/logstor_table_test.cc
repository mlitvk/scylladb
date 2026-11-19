/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// combined_tests has the main function.
#undef SEASTAR_TESTING_MAIN

#include <boost/test/unit_test.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include "db/config.hh"
#include "locator/tablets.hh"
#include "replica/compaction_group.hh"
#include "replica/database.hh"
#include "replica/logstor/compaction.hh"
#include "replica/logstor/segment_manager.hh"
#include "test/lib/cql_test_env.hh"
#include "utils/error_injection.hh"

#include <functional>
#include <string_view>
#include <vector>

// The tests of a logstor table through replica::table and its storage groups and compaction groups.
BOOST_AUTO_TEST_SUITE(logstor_table_test)

namespace {

cql_test_config logstor_test_config() {
    auto cfg = make_shared<db::config>();
    cfg->experimental_features({db::experimental_features_t::feature::LOGSTOR});
    cfg->logstor_disk_size_in_mb(20);
    cfg->logstor_file_size_in_mb(1);
    return cql_test_config(std::move(cfg));
}

// Creates the logstor table test_ks.tbl with one tablet, and returns the shard of the tablet.
future<unsigned> create_one_tablet_table(cql_test_env& env) {
    co_await env.execute_cql(
            "CREATE KEYSPACE test_ks "
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
            "AND tablets = {'initial': 1}");
    co_await env.execute_cql("CREATE TABLE test_ks.tbl (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'");

    const auto table = env.local_db().find_schema("test_ks", "tbl")->id();
    const auto& replicas = env.local_token_metadata_ptr()->tablets().get_tablet_map(table)
            .get_tablet_info(locator::tablet_id(0)).replicas;
    BOOST_REQUIRE_EQUAL(replicas.size(), 1u);
    co_return replicas.front().shard;
}

// Checks that `flush` takes a write whose record is not in a separator buffer yet. A write
// completes when its record is in a mixed segment, and the separator fiber moves the record to the
// separator buffer of its group after that. The injection holds the fiber before that move.
future<> check_flush_takes_a_record_that_is_not_in_a_separator_buffer(std::function<future<>(replica::table&)> flush) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return make_ready_future<>();
    }

    return do_with_cql_env([flush = std::move(flush)] (cql_test_env& env) -> future<> {
        constexpr std::string_view allocated = "logstor_separator_buffer_allocated";
        const auto shard = co_await create_one_tablet_table(env);

        co_await env.db().invoke_on(shard, [allocated] (replica::database&) {
            utils::get_local_injector().enable(allocated);
        });
        co_await env.execute_cql("INSERT INTO test_ks.tbl (pk, v) VALUES (1, 1)");

        co_await env.db().invoke_on(shard, [allocated, &flush] (replica::database& db) -> future<> {
            auto& injector = utils::get_local_injector();
            auto disable = seastar::defer([&injector, allocated] noexcept { injector.disable(allocated); });
            auto& table = db.find_column_family("test_ks", "tbl");
            auto& segments = table.get_any_compaction_group()->logstor_segments();
            BOOST_REQUIRE_EQUAL(segments.segment_count(), 0u);

            auto flushed = flush(table);
            BOOST_REQUIRE(!flushed.available());
            injector.receive_message(allocated);
            co_await std::move(flushed);

            BOOST_REQUIRE_EQUAL(segments.segment_count(), 1u);
        });
    }, logstor_test_config());
}

}

SEASTAR_TEST_CASE(test_compaction_group_flush_takes_a_record_that_is_not_in_a_separator_buffer) {
    return check_flush_takes_a_record_that_is_not_in_a_separator_buffer([] (replica::table& table) {
        return table.get_any_compaction_group()->flush();
    });
}

// The snapshot of a token range flushes each storage group of the range.
SEASTAR_TEST_CASE(test_snapshot_takes_a_record_that_is_not_in_a_separator_buffer) {
    return check_flush_takes_a_record_that_is_not_in_a_separator_buffer([] (replica::table& table) {
        return table.take_logstor_snapshot(dht::token_range::make_open_ended_both_sides()).discard_result();
    });
}

SEASTAR_TEST_CASE(test_table_flush_takes_a_record_that_is_not_in_a_separator_buffer) {
    return check_flush_takes_a_record_that_is_not_in_a_separator_buffer([] (replica::table& table) {
        return table.flush();
    });
}

BOOST_AUTO_TEST_SUITE_END()
