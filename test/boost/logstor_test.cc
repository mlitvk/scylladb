/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/util/defer.hh>

#include "replica/log_structured/index.hh"
#include "replica/log_structured/types.hh"
#include "test/lib/tmpdir.hh"
#include "replica/log_structured/logstor.hh"
#include "mutation/mutation.hh"
#include "schema/schema_builder.hh"
#include "types/types.hh"
#include "test/lib/mutation_assertions.hh"
#include "serializer_impl.hh"
#include "idl/log_structured.dist.hh"
#include "idl/log_structured.dist.impl.hh"

using namespace replica::log_structured;

namespace replica::log_structured {
inline std::ostream& operator<<(std::ostream& os, const log_segment_id& id) {
    return os << id.value;
}
inline std::ostream& operator<<(std::ostream& os, const log_location& loc) {
    return os << "log_location(segment=" << loc.segment << ", offset=" << loc.offset << ", size=" << loc.size << ")";
}
inline std::ostream& operator<<(std::ostream& os, const index_entry& entry) {
    return os << "index_entry(" << entry.location << ")";
}
}

BOOST_AUTO_TEST_SUITE(logstor_test)

static future<> do_logstor_test(logstor_config cfg, noncopyable_function<future<> (logstor&)> f) {
    auto ls = logstor(cfg);
    co_await ls.start();
    co_await f(ls).finally([&] {
        return ls.stop();
    });
}

static future<> do_logstor_test(noncopyable_function<future<> (logstor&)> f) {
    tmpdir tmp;
    logstor_config cfg = {
        .base_dir = tmp.path()
    };
    return do_logstor_test(cfg, std::move(f)).finally([tmp = std::move(tmp)] {});
}

SEASTAR_TEST_CASE(test_create_logstor) {
    return do_logstor_test([](logstor& ls) -> future<> {
        // Basic test - just start and stop the logstor
        co_return;
    });
}

SEASTAR_TEST_CASE(test_single_write_and_read) {
    return do_logstor_test([](logstor& ls) -> future<> {
        // Create a simple schema and mutation
        auto s = schema_builder("test_ks", "test_cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();

        auto key = partition_key::from_single_value(*s, to_bytes("test_key"));
        auto dk = dht::decorate_key(*s, key);
        mutation m(s, key);
        m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(to_bytes("test_value")), 1);

        // Write the mutation
        co_await ls.write(m);

        // Read it back using the decorated key
        auto read_result = co_await ls.read(*s, dk);

        // Verify we got the mutation back
        BOOST_REQUIRE(read_result.has_value());
        auto read_mutation = read_result->to_mutation(s);

        // Compare the mutations using proper assertion helper
        assert_that(read_mutation).is_equal_to(m);

        co_return;
    });
}

SEASTAR_TEST_CASE(test_write_and_read_multiple_keys) {
    return do_logstor_test([](logstor& ls) -> future<> {
        // Create a simple schema
        auto s = schema_builder("test_ks", "test_cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();

        // Create first mutation
        auto key1 = partition_key::from_single_value(*s, to_bytes("test_key1"));
        auto dk1 = dht::decorate_key(*s, key1);
        mutation m1(s, key1);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(to_bytes("test_value1")), 1);

        // Create second mutation with different key
        auto key2 = partition_key::from_single_value(*s, to_bytes("test_key2"));
        auto dk2 = dht::decorate_key(*s, key2);
        mutation m2(s, key2);
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(to_bytes("test_value2")), 2);

        // Write both mutations
        co_await ls.write(m1);
        co_await ls.write(m2);

        // Read back first mutation
        auto read_result1 = co_await ls.read(*s, dk1);
        BOOST_REQUIRE(read_result1.has_value());
        auto read_mutation1 = read_result1->to_mutation(s);
        assert_that(read_mutation1).is_equal_to(m1);

        // Read back second mutation
        auto read_result2 = co_await ls.read(*s, dk2);
        BOOST_REQUIRE(read_result2.has_value());
        auto read_mutation2 = read_result2->to_mutation(s);
        assert_that(read_mutation2).is_equal_to(m2);

        co_return;
    });
}

SEASTAR_TEST_CASE(test_overwrite_same_key) {
    return do_logstor_test([](logstor& ls) -> future<> {
        // Create a simple schema
        auto s = schema_builder("test_ks", "test_cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();

        auto key = partition_key::from_single_value(*s, to_bytes("test_key"));
        auto dk = dht::decorate_key(*s, key);

        // Create first mutation with initial value
        mutation m1(s, key);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(to_bytes("initial_value")), 1);

        // Write first mutation
        co_await ls.write(m1);

        // Create second mutation with same key but different value and higher timestamp
        mutation m2(s, key);
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(to_bytes("updated_value")), 2);

        // Write second mutation (should overwrite the first)
        co_await ls.write(m2);

        // Read back and verify we get the updated value
        auto read_result = co_await ls.read(*s, dk);
        BOOST_REQUIRE(read_result.has_value());
        auto read_mutation = read_result->to_mutation(s);

        // Should get the second mutation (higher timestamp wins)
        assert_that(read_mutation).is_equal_to(m2);

        co_return;
    });
}

SEASTAR_TEST_CASE(test_same_key_in_different_tables) {
    return do_logstor_test([](logstor& ls) -> future<> {
        // Create two tables with identical schemas
        auto s1 = schema_builder("test_ks", "test_cf1")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();

        auto s2 = schema_builder("test_ks", "test_cf2")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();

        // Use the same key for both tables
        auto key = partition_key::from_single_value(*s1, to_bytes("shared_key"));
        auto dk1 = dht::decorate_key(*s1, key);
        auto dk2 = dht::decorate_key(*s2, key);

        // Create mutation for first table
        mutation m1(s1, key);
        m1.set_clustered_cell(clustering_key::make_empty(), "v",
                            data_value(to_bytes("value_in_table1")), 1);

        // Create mutation for second table with same key, different value
        mutation m2(s2, key);
        m2.set_clustered_cell(clustering_key::make_empty(), "v",
                            data_value(to_bytes("value_in_table2")), 1);

        // Write to both tables
        co_await ls.write(m1);
        co_await ls.write(m2);

        // Read from first table and verify
        auto read_result1 = co_await ls.read(*s1, dk1);
        BOOST_REQUIRE(read_result1.has_value());
        auto read_mutation1 = read_result1->to_mutation(s1);
        assert_that(read_mutation1).is_equal_to(m1);

        // Read from second table and verify
        auto read_result2 = co_await ls.read(*s2, dk2);
        BOOST_REQUIRE(read_result2.has_value());
        auto read_mutation2 = read_result2->to_mutation(s2);
        assert_that(read_mutation2).is_equal_to(m2);
    });
}

static future<> do_segment_manager_test(segment_manager_config cfg, noncopyable_function<future<> (segment_manager&)> f) {
    auto la = segment_manager(cfg);
    co_await la.start();
    co_await f(la).finally([&] {
        return la.stop();
    });
}

SEASTAR_TEST_CASE(test_segment_manager_basic_writes) {
    tmpdir tmp;
    segment_manager_config cfg = {
        .base_dir = tmp.path(),
        .segment_size = 8 * 1024,  // 8KB segments
        .file_size = 16 * 1024     // 16KB files (2 segments per file)
    };

    co_await do_segment_manager_test(cfg, [](segment_manager& sm) -> future<> {
        // Do 2 writes of 4KB each - should fit in one segment
        const size_t data_size = 4 * 1024;
        bytes data(bytes::initialized_later{}, data_size);
        std::fill(data.begin(), data.end(), 'A');

        auto loc1 = co_await sm.write(data);
        BOOST_REQUIRE_EQUAL(loc1.segment, log_segment_id(0));
        BOOST_REQUIRE_EQUAL(loc1.offset, 0);
        BOOST_REQUIRE_EQUAL(loc1.size, data_size);

        auto loc2 = co_await sm.write(data);
        BOOST_REQUIRE_EQUAL(loc2.segment, log_segment_id(0));
        BOOST_REQUIRE_EQUAL(loc2.offset, data_size);
        BOOST_REQUIRE_EQUAL(loc2.size, data_size);

        co_return;
    });

    co_await do_segment_manager_test(cfg, [](segment_manager& sm) -> future<> {
        // Write a 4KB chunk and then a 8KB chunk
        bytes data1(bytes::initialized_later{}, 4 * 1024);
        std::fill(data1.begin(), data1.end(), 'A');

        auto loc1 = co_await sm.write(data1);
        BOOST_REQUIRE_EQUAL(loc1.segment, log_segment_id(0));
        BOOST_REQUIRE_EQUAL(loc1.offset, 0);
        BOOST_REQUIRE_EQUAL(loc1.size, 4 * 1024);

        bytes data2(bytes::initialized_later{}, 8 * 1024);
        std::fill(data2.begin(), data2.end(), 'B');

        // doesn't fit in the current segment, should go in the next segment
        auto loc2 = co_await sm.write(data2);
        BOOST_REQUIRE_EQUAL(loc2.segment, log_segment_id(1));
        BOOST_REQUIRE_EQUAL(loc2.offset, 0);
        BOOST_REQUIRE_EQUAL(loc2.size, 8 * 1024);

        co_return;
    });

    co_await do_segment_manager_test(cfg, [](segment_manager& sm) -> future<> {
        // Do 3 writes of 8KB each
        const size_t data_size = 8 * 1024;
        bytes data(bytes::initialized_later{}, data_size);
        std::fill(data.begin(), data.end(), 'A');

        auto loc1 = co_await sm.write(data);
        BOOST_REQUIRE_EQUAL(loc1.segment, log_segment_id(0));
        BOOST_REQUIRE_EQUAL(loc1.offset, 0);
        BOOST_REQUIRE_EQUAL(loc1.size, data_size);

        // should go to the next segment in the same file
        auto loc2 = co_await sm.write(data);
        BOOST_REQUIRE_EQUAL(loc2.segment, log_segment_id(1));
        BOOST_REQUIRE_EQUAL(loc2.offset, 0);
        BOOST_REQUIRE_EQUAL(loc2.size, data_size);

        // should go to a new file and segment
        auto loc3 = co_await sm.write(data);
        BOOST_REQUIRE_EQUAL(loc3.segment, log_segment_id(2));
        BOOST_REQUIRE_EQUAL(loc3.offset, 0);
        BOOST_REQUIRE_EQUAL(loc3.size, data_size);

        co_return;
    });
}

SEASTAR_TEST_CASE(test_for_each_record) {
    tmpdir tmp;
    segment_manager_config cfg = {
        .base_dir = tmp.path(),
        .segment_size = 8 * 1024,  // 8KB segments
        .file_size = 16 * 1024     // 16KB files (2 segments per file)
    };

    co_await do_segment_manager_test(cfg, [](segment_manager& sm) -> future<> {
        // Create a simple schema for our test records
        auto s = schema_builder("test_ks", "test_cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();

        // Create write buffer
        buffered_writer wb(sm, default_scheduling_group());
        co_await wb.start();

        auto cleanup = defer([&wb] () noexcept {
            (void)wb.stop();
        });

        // Write several records and track their locations
        struct expected_record {
            mutation mut;
            log_location loc;
        };
        std::vector<expected_record> expected_records;
        std::vector<log_segment_id> segments_used;

        for (int i = 0; i < 5; i++) {
            auto key = partition_key::from_single_value(*s, to_bytes(format("key_{}", i)));
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), "v",
                               data_value(to_bytes(format("value_{}", i))), i + 1);

            // Write through write_buffer
            log_structured_segment_record record{.mut = canonical_mutation(m)};
            auto loc = co_await wb.write(std::move(record));

            expected_records.push_back(expected_record{.mut = m, .loc = loc});

            // Track which segments we used
            if (segments_used.empty() || segments_used.back() != loc.segment) {
                segments_used.push_back(loc.segment);
            }
        }

        // Flush write buffer to ensure all data is written
        co_await wb.stop();

        // Now read back all records using for_each_record
        size_t record_index = 0;
        co_await sm.for_each_record(segments_used,
            [&](log_location loc, log_structured_segment_record record) -> future<> {
                BOOST_REQUIRE(record_index < expected_records.size());

                auto& expected = expected_records[record_index];

                // Verify location matches
                BOOST_REQUIRE_EQUAL(loc.segment, expected.loc.segment);
                BOOST_REQUIRE_EQUAL(loc.offset, expected.loc.offset);
                BOOST_REQUIRE_EQUAL(loc.size, expected.loc.size);

                // Verify mutation matches
                auto read_mutation = record.mut.to_mutation(s);
                assert_that(read_mutation).is_equal_to(expected.mut);

                record_index++;
                co_return;
            });

        // Verify we read exactly the expected number of records
        BOOST_REQUIRE_EQUAL(record_index, expected_records.size());

        co_return;
    });
}

SEASTAR_TEST_CASE(test_segment_manager_out_of_space) {
    tmpdir tmp;
    segment_manager_config cfg = {
        .base_dir = tmp.path(),
        .segment_size = 4 * 1024,   // 4KB segments
        .file_size = 4 * 1024,      // 4KB files (1 segment per file)
        .disk_size = 8 * 1024       // 8KB total (2 segments max)
    };

    co_await do_segment_manager_test(cfg, [](segment_manager& sm) -> future<> {
        // Create data exactly 4KB to fit in one segment
        const size_t data_size = 4 * 1024;
        bytes data1(bytes::initialized_later{}, data_size);
        std::fill(data1.begin(), data1.end(), 'A');

        // First write - should use segment 0
        auto loc1 = co_await sm.write(data1);
        BOOST_REQUIRE_EQUAL(loc1.segment, log_segment_id(0));

        // Second write with different data
        bytes data2(bytes::initialized_later{}, data_size);
        std::fill(data2.begin(), data2.end(), 'B');

        // Second write - should use segment 1 (last available segment)
        auto loc2 = co_await sm.write(data2);
        BOOST_REQUIRE_EQUAL(loc2.segment, log_segment_id(1));

        // Mark first write as dead (simulating overwrite)
        sm.free_record(loc1);

        // Free the first write's segment to make space for third write
        co_await sm.free_segment(loc1.segment);

        // Third write - should now be able to reuse freed segment
        bytes data3(bytes::initialized_later{}, data_size);
        std::fill(data3.begin(), data3.end(), 'C');

        auto loc3 = co_await sm.write(data3);
        BOOST_REQUIRE_EQUAL(loc3.segment, loc1.segment);
    });
}

SEASTAR_TEST_CASE(test_compaction_candidates) {
    tmpdir tmp;
    segment_manager_config cfg = {
        .base_dir = tmp.path(),
        .segment_size = 12 * 1024,
        .file_size = 4 * 12 * 1024, // 4 segments
        .disk_size = 4 * 12 * 1024 // single file
    };

    co_await do_segment_manager_test(cfg, [](segment_manager& sm) -> future<> {
        // Write 12 records of 4KB each, 3 records per segment, filling 4 segments
        const size_t data_size = 4 * 1024;
        bytes data1(bytes::initialized_later{}, data_size);
        std::fill(data1.begin(), data1.end(), 'A');

        std::vector<log_location> locations;
        for (int i = 0; i < 12; i++) {
            auto loc = co_await sm.write(data1);
            BOOST_REQUIRE_EQUAL(loc.segment, log_segment_id(i / 3));
            locations.push_back(loc);
        }

        {
            auto candidates = sm.find_segments_for_compaction(0.5, 10);
            BOOST_REQUIRE_EQUAL(candidates.size(), 0);
        }

        // free first write in segment 0 (1/3 free)
        sm.free_record(locations[0]);

        {
            auto candidates = sm.find_segments_for_compaction(0.5, 10);
            BOOST_REQUIRE_EQUAL(candidates.size(), 0);
        }

        {
            auto candidates = sm.find_segments_for_compaction(0.7, 10);
            BOOST_REQUIRE_EQUAL(candidates.size(), 1);
            BOOST_REQUIRE_EQUAL(candidates[0], locations[0].segment);
        }

        // free another write in segment 0 (2/3 free)
        sm.free_record(locations[1]);

        {
            auto candidates = sm.find_segments_for_compaction(0.5, 10);
            BOOST_REQUIRE_EQUAL(candidates.size(), 1);
            BOOST_REQUIRE_EQUAL(candidates[0], locations[0].segment);
        }

        // free a write in segment 1 (1/3 free)
        sm.free_record(locations[3]);

        {
            auto candidates = sm.find_segments_for_compaction(0.5, 10);
            BOOST_REQUIRE_EQUAL(candidates.size(), 1);
            BOOST_REQUIRE_EQUAL(candidates[0], locations[0].segment);
        }

        {
            auto candidates = sm.find_segments_for_compaction(0.7, 10);
            BOOST_REQUIRE_EQUAL(candidates.size(), 2);
            BOOST_REQUIRE_EQUAL(candidates[0], locations[0].segment);
            BOOST_REQUIRE_EQUAL(candidates[1], locations[3].segment);
        }

        // free remaining writes in segment 1
        sm.free_record(locations[4]);
        sm.free_record(locations[5]);

        {
            auto candidates = sm.find_segments_for_compaction(0.5, 10);
            BOOST_REQUIRE_EQUAL(candidates.size(), 2);
            BOOST_REQUIRE_EQUAL(candidates[0], locations[3].segment);
            BOOST_REQUIRE_EQUAL(candidates[1], locations[0].segment);
        }
    });
}

SEASTAR_TEST_CASE(test_index) {
    log_index index;

    auto s = schema_builder("test_ks", "test_cf")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("v", utf8_type)
        .build();

    auto pk1 = dht::decorate_key(*s, partition_key::from_single_value(*s, to_bytes("key1")));
    index_key key1 = logstor::calculate_key(*s, pk1);
    index_entry entry1 = {
        .location = log_location{.segment = log_segment_id(0),
        .offset = 0,
        .size = 100}
    };
    index_entry entry2 = {
        .location = log_location{.segment = log_segment_id(1),
        .offset = 0,
        .size = 200}
    };
    index_entry entry3 = {
        .location = log_location{.segment = log_segment_id(2),
        .offset = 0,
        .size = 300}
    };

    {
        auto old = index.exchange(key1, entry1);
        BOOST_REQUIRE_EQUAL(old.has_value(), false);
    }

    {
        auto result = index.get(key1);
        BOOST_REQUIRE_EQUAL(result.has_value(), true);
        BOOST_REQUIRE_EQUAL(*result, entry1);
    }

    {
        auto old = index.exchange(key1, entry2);
        BOOST_REQUIRE_EQUAL(old.has_value(), true);
        BOOST_REQUIRE_EQUAL(*old, entry1);
    }

    {
        auto result = index.get(key1);
        BOOST_REQUIRE_EQUAL(result.has_value(), true);
        BOOST_REQUIRE_EQUAL(*result, entry2);
    }

    {
        BOOST_REQUIRE_EQUAL(false, index.compare_exchange(key1, entry1, entry3));
        BOOST_REQUIRE_EQUAL(true, index.compare_exchange(key1, entry2, entry3));
        BOOST_REQUIRE_EQUAL(entry3, *index.get(key1));
    }

    co_return;
}

BOOST_AUTO_TEST_SUITE_END()
