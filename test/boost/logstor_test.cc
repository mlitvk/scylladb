/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <set>
#include <seastar/util/defer.hh>

#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>

#include "replica/logstor/logstor.hh"
#include "db/cache_tracker.hh"
#include "schema/schema_builder.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/tmpdir.hh"

using namespace replica::logstor;

namespace {

struct shared_logstor_cache {
    ::cache_tracker shared_tracker;
    replica::logstor::cache_tracker logstor_tracker;

    shared_logstor_cache()
        : shared_tracker(utils::updateable_value<double>(1.0), ::cache_tracker::register_metrics::no)
        , logstor_tracker(shared_tracker) {
    }
};

schema_ptr make_kv_schema() {
    return schema_builder(1, "ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();
}

mutation make_kv_mutation(schema_ptr schema, sstring pk, sstring value, api::timestamp_type ts = api::min_timestamp) {
    auto key = partition_key::from_single_value(*schema, serialized(pk));
    auto dk = dht::decorate_key(*schema, key);
    mutation m(schema, dk);
    auto& row = m.partition().clustered_row(*schema, clustering_key::make_empty());
    row.apply(row_marker(ts));
    const auto& v_def = *schema->get_column_definition("v");
    row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, ts, serialized(value)));
    return m;
}

logstor_config make_test_logstor_config(const std::filesystem::path& base_dir) {
    constexpr size_t segment_size = 128 * 1024;
    constexpr size_t file_size = 4 * segment_size;
    return logstor_config{
        .segment_manager_cfg = {
            .base_dir = base_dir,
            .segment_size = segment_size,
            .file_size = file_size,
            .disk_size = 4 * file_size,
            .compaction_enabled = true,
            .max_segments_per_compaction = 8,
            .compaction_sg = seastar::current_scheduling_group(),
            .compaction_static_shares = utils::updateable_value<float>(0.0f),
            .separator_sg = seastar::current_scheduling_group(),
            .separator_delay_limit_ms = 0,
            .max_separator_memory = segment_size,
        },
        .flush_sg = seastar::current_scheduling_group(),
    };
}

class test_compaction_group_handle final : public logstor_group {
    ::table_id _table_id;
    primary_index _index;
    compaction_manager& _cm;
public:
    test_compaction_group_handle(schema_ptr schema, compaction_manager& cm)
        : _table_id(schema->id())
        , _index(std::move(schema))
        , _cm(cm) {
    }

    ::table_id table_id() const noexcept override {
        return _table_id;
    }

    primary_index& logstor_index() noexcept override {
        return _index;
    }

    const primary_index& logstor_index() const noexcept override {
        return _index;
    }

protected:
    compaction_manager& logstor_compaction_manager() noexcept override {
        return _cm;
    }
};

std::set<log_segment_id> snapshot_segment_ids(const utils::chunked_vector<segment_snapshot>& snapshot) {
    std::set<log_segment_id> ids;
    for (const auto& seg : snapshot) {
        ids.insert(seg.segment_id);
    }
    return ids;
}

void write_and_flush_segment(logstor& ls, test_compaction_group_handle& cg, const mutation& m) {
    ls.write(m, write_target(&cg, {}), db::no_timeout).get();
    ls.flush_to_separator().get();
    cg.flush_separator().get();
}

}

SEASTAR_THREAD_TEST_CASE(test_logstor_write_and_separator_flush) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls.get_compaction_manager());

    auto expected = make_kv_mutation(schema, "pk0", "separator-value");
    auto key = expected.decorated_key();

    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
    ls.flush_to_separator().get();

    BOOST_REQUIRE(cg.separator_has_data());
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 1u);
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 0u);

    auto entry_before_flush = cg.logstor_index().get(primary_index_key{key});
    BOOST_REQUIRE(entry_before_flush);

    cg.flush_separator().get();

    BOOST_REQUIRE(!cg.separator_has_data());
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);

    auto snapshot = ls.get_segment_manager().make_snapshot(cg).get();
    BOOST_REQUIRE_EQUAL(snapshot.size(), 1u);

    auto entry_after_flush = cg.logstor_index().get(primary_index_key{key});
    BOOST_REQUIRE(entry_after_flush);
    BOOST_REQUIRE(entry_after_flush->location.segment != entry_before_flush->location.segment);
    BOOST_REQUIRE_EQUAL(entry_after_flush->location.segment.value, snapshot.front().segment_id.value);

    auto actual = ls.read(*schema, cg.logstor_index(), key, schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_group_compaction_rewrites_live_records) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls.get_compaction_manager());

    auto pk0_v0 = make_kv_mutation(schema, "pk0", "v0", api::timestamp_type(1));
    auto pk1_v0 = make_kv_mutation(schema, "pk1", "v1", api::timestamp_type(2));
    auto pk2_v0 = make_kv_mutation(schema, "pk2", "v2", api::timestamp_type(3));
    auto pk0_v1 = make_kv_mutation(schema, "pk0", "v0-new", api::timestamp_type(4));
    auto pk1_v1 = make_kv_mutation(schema, "pk1", "v1-new", api::timestamp_type(5));

    write_and_flush_segment(ls, cg, pk0_v0);
    write_and_flush_segment(ls, cg, pk1_v0);
    write_and_flush_segment(ls, cg, pk2_v0);

    const auto pk0 = primary_index_key{pk0_v1.decorated_key()};
    const auto pk1 = primary_index_key{pk1_v1.decorated_key()};
    const auto pk2 = primary_index_key{pk2_v0.decorated_key()};

    auto stale_pk0_location = cg.logstor_index().get(pk0);
    auto stale_pk1_location = cg.logstor_index().get(pk1);

    BOOST_REQUIRE(stale_pk0_location);
    BOOST_REQUIRE(stale_pk1_location);

    write_and_flush_segment(ls, cg, pk0_v1);
    write_and_flush_segment(ls, cg, pk1_v1);

    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 5u);

    auto live_pk0_before = cg.logstor_index().get(pk0);
    auto live_pk1_before = cg.logstor_index().get(pk1);
    auto live_pk2_before = cg.logstor_index().get(pk2);

    BOOST_REQUIRE(live_pk0_before);
    BOOST_REQUIRE(live_pk1_before);
    BOOST_REQUIRE(live_pk2_before);

    auto old_snapshot = ls.get_segment_manager().make_snapshot(cg).get();
    BOOST_REQUIRE_EQUAL(old_snapshot.size(), 5u);
    const auto old_segment_ids = snapshot_segment_ids(old_snapshot);

    ls.get_compaction_manager().submit(cg);
    auto compaction_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    auto new_snapshot = ls.get_segment_manager().make_snapshot(cg).get();
    BOOST_REQUIRE_EQUAL(new_snapshot.size(), 1u);
    const auto new_segment_ids = snapshot_segment_ids(new_snapshot);

    auto live_pk0_after = cg.logstor_index().get(pk0);
    auto live_pk1_after = cg.logstor_index().get(pk1);
    auto live_pk2_after = cg.logstor_index().get(pk2);

    BOOST_REQUIRE(live_pk0_after);
    BOOST_REQUIRE(live_pk1_after);
    BOOST_REQUIRE(live_pk2_after);

    BOOST_REQUIRE(live_pk0_after->location != live_pk0_before->location);
    BOOST_REQUIRE(live_pk1_after->location != live_pk1_before->location);
    BOOST_REQUIRE(live_pk2_after->location != live_pk2_before->location);

    BOOST_REQUIRE(new_segment_ids.contains(live_pk0_after->location.segment));
    BOOST_REQUIRE(new_segment_ids.contains(live_pk1_after->location.segment));
    BOOST_REQUIRE(new_segment_ids.contains(live_pk2_after->location.segment));

    for (const auto old_segment_id : old_segment_ids) {
        BOOST_REQUIRE(!new_segment_ids.contains(old_segment_id));
    }

    BOOST_REQUIRE(!new_segment_ids.contains(stale_pk0_location->location.segment));
    BOOST_REQUIRE(!new_segment_ids.contains(stale_pk1_location->location.segment));

    auto actual_pk0 = ls.read(*schema, cg.logstor_index(), pk0.dk, schema->full_slice()).get();
    auto actual_pk1 = ls.read(*schema, cg.logstor_index(), pk1.dk, schema->full_slice()).get();
    auto actual_pk2 = ls.read(*schema, cg.logstor_index(), pk2.dk, schema->full_slice()).get();

    BOOST_REQUIRE(actual_pk0);
    BOOST_REQUIRE(actual_pk1);
    BOOST_REQUIRE(actual_pk2);

    assert_that(*actual_pk0).is_equal_to(pk0_v1);
    assert_that(*actual_pk1).is_equal_to(pk1_v1);
    assert_that(*actual_pk2).is_equal_to(pk2_v0);
}
