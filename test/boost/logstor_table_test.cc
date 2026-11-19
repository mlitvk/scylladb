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
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "locator/tablets.hh"
#include "replica/compaction_group.hh"
#include "replica/database.hh"
#include "replica/logstor/compaction.hh"
#include "replica/logstor/segment_manager.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/eventually.hh"
#include "test/lib/logstor_test_utils.hh"
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
    auto test_cfg = cql_test_config(std::move(cfg));
    // The drop of a table makes queries that need the remote part of storage_proxy.
    test_cfg.need_remote_proxy = true;
    return test_cfg;
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

namespace {

// The injection that fails each attempt of a separator flush while it is enabled.
constexpr std::string_view flush_failure = "fail_flush_separator_buffer";

// Runs `fn` in a thread on `shard`, with the database of the shard.
void run_on_shard(cql_test_env& env, unsigned shard, std::function<void(replica::database&)> fn) {
    env.db().invoke_on(shard, [&fn] (replica::database& db) {
        return seastar::async([&fn, &db] { fn(db); });
    }).get();
}

// Runs `test` with the logstor table test_ks.tbl, which has one tablet and one row whose record is
// in the separator buffer of its group. Each attempt of a separator flush on the shard of the
// tablet fails until the test disables `flush_failure`.
future<> with_failing_separator_flush(std::function<void(cql_test_env&, unsigned shard)> test) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return make_ready_future<>();
    }

    return do_with_cql_env_thread([test = std::move(test)] (cql_test_env& env) {
        const auto shard = create_one_tablet_table(env).get();
        env.execute_cql("INSERT INTO test_ks.tbl (pk, v) VALUES (1, 1)").get();
        run_on_shard(env, shard, [] (replica::database& db) {
            // The flush of the record fails later, so that the write and this wait do not fail.
            db.find_column_family("test_ks", "tbl").get_logstor_segment_manager().await_pending_writes().get();
            utils::get_local_injector().enable(flush_failure);
        });
        auto disable = seastar::defer([&env, shard] noexcept {
            run_on_shard(env, shard, [] (replica::database&) {
                utils::get_local_injector().disable(flush_failure);
            });
        });
        test(env, shard);
    }, logstor_test_config());
}

// Waits until two attempts of a separator flush failed on this shard.
void wait_for_failed_flush_attempts() {
    BOOST_REQUIRE(eventually_true([] {
        return utils::get_local_injector().enter_count(flush_failure) >= 2;
    }));
}

}

// Checks that the snapshot of a token range, which a tablet migration streams, waits for a
// separator flush that keeps failing, and that it then holds the record of the flush.
SEASTAR_TEST_CASE(test_snapshot_waits_for_a_separator_flush_that_fails) {
    return with_failing_separator_flush([] (cql_test_env& env, unsigned shard) {
        run_on_shard(env, shard, [] (replica::database& db) {
            auto& table = db.find_column_family("test_ks", "tbl");
            auto snapshot = table.take_logstor_snapshot(dht::token_range::make_open_ended_both_sides());
            wait_for_failed_flush_attempts();
            BOOST_REQUIRE(!snapshot.available());

            utils::get_local_injector().disable(flush_failure);
            BOOST_REQUIRE_EQUAL(snapshot.get().size(), 1u);
            BOOST_REQUIRE_EQUAL(tests::logstor::read_segment_manager_metric("kept_segments"), 0);
        });
    });
}

// Checks that a tablet cleanup waits for a separator flush that keeps failing. The cleanup stops the
// compaction groups of the tablet, and the stop of a group does not end the retry of its flush.
SEASTAR_TEST_CASE(test_tablet_cleanup_waits_for_a_separator_flush_that_fails) {
    return with_failing_separator_flush([] (cql_test_env& env, unsigned shard) {
        run_on_shard(env, shard, [&env] (replica::database& db) {
            auto& table = db.find_column_family("test_ks", "tbl");
            auto cleanup = table.cleanup_tablet(db, env.get_system_keyspace().local(), locator::tablet_id(0));
            wait_for_failed_flush_attempts();
            BOOST_REQUIRE(!cleanup.available());

            utils::get_local_injector().disable(flush_failure);
            cleanup.get();
            BOOST_REQUIRE_EQUAL(tests::logstor::read_segment_manager_metric("kept_segments"), 0);
        });
    });
}

// Checks that the drop of a table waits for a separator flush that keeps failing. The drop
// truncates the table before it stops the table, and the truncate flushes the separator.
SEASTAR_TEST_CASE(test_table_drop_waits_for_a_separator_flush_that_fails) {
    return with_failing_separator_flush([] (cql_test_env& env, unsigned shard) {
        auto drop = env.execute_cql("DROP TABLE test_ks.tbl");
        run_on_shard(env, shard, [] (replica::database&) {
            wait_for_failed_flush_attempts();
        });
        BOOST_REQUIRE(!drop.available());

        run_on_shard(env, shard, [] (replica::database&) {
            utils::get_local_injector().disable(flush_failure);
        });
        drop.get();
        run_on_shard(env, shard, [] (replica::database&) {
            BOOST_REQUIRE_EQUAL(tests::logstor::read_segment_manager_metric("kept_segments"), 0);
        });
    });
}

// Checks that the stop of a table ends the retry of a separator flush. The stop of the table
// completes while the flush still fails, and the segment of the record is kept.
SEASTAR_TEST_CASE(test_table_stop_ends_the_retry_of_a_separator_flush) {
    return with_failing_separator_flush([] (cql_test_env& env, unsigned shard) {
        run_on_shard(env, shard, [] (replica::database& db) {
            auto& table = db.find_column_family("test_ks", "tbl");
            table.stop().get();
            BOOST_REQUIRE_GE(utils::get_local_injector().enter_count(flush_failure), 1u);
            BOOST_REQUIRE_EQUAL(tests::logstor::read_segment_manager_metric("kept_segments"), 1);
        });
    });
}

BOOST_AUTO_TEST_SUITE_END()
