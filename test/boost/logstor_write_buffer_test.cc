/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <vector>
#include <fmt/format.h>
#include <seastar/core/abort_source.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/memory-data-source.hh>

#include "replica/exceptions.hh"
#include "replica/logstor/chunk_builder.hh"
#include "replica/logstor/column_mapping_registry.hh"
#include "replica/logstor/index.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/write_buffer.hh"
#include "replica/logstor/write_buffer_pool.hh"
#include "replica/logstor/write_buffer_ring.hh"
#include "schema/schema_builder.hh"
#include "test/lib/logstor_record_utils.hh"
#include "test/lib/logstor_test_utils.hh"
#include "test/lib/mutation_assertions.hh"

using namespace replica::logstor;
using namespace tests::logstor;

namespace {

void assert_log_record_matches(schema_ptr schema, const log_record& actual, const log_record& expected) {
    BOOST_REQUIRE_EQUAL(actual.header.timestamp, expected.header.timestamp);
    BOOST_REQUIRE_EQUAL(actual.header.table, expected.header.table);
    assert_that(to_mutation(actual, schema, test_registry())).is_equal_to(to_mutation(expected, schema, test_registry()));
}

db::timeout_clock::time_point test_timeout() {
    return db::timeout_clock::now() + std::chrono::minutes(1);
}

write_buffer_ring_config make_write_buffer_ring_config(size_t buffer_size, size_t ring_size, size_t max_queued_write_bytes = 0, std::chrono::milliseconds sync_period = std::chrono::milliseconds(0)) {
    return write_buffer_ring_config{
        .buffer_size = buffer_size,
        .ring_size = ring_size,
        .flush_sg = seastar::default_scheduling_group(),
        .max_queued_write_bytes = max_queued_write_bytes,
        .sync_period = sync_period,
    };
}

sstring make_single_buffer_value(schema_ptr schema, size_t buffer_size) {
    sstring value;
    while (true) {
        auto next_value = value;
        next_value += sstring("x");
        auto next_record = make_log_record(schema, "pk0000", next_value, api::timestamp_type(17));
        auto next_writer = make_writer(next_record);
        chunk_builder wb(buffer_size, segment_kind::mixed);

        BOOST_REQUIRE(wb.can_fit(next_writer));
        wb.append(next_writer);
        if (!wb.can_fit(next_writer)) {
            return next_value;
        }

        value = std::move(next_value);
    }
}

log_record make_write_buffer_ring_record(schema_ptr schema, size_t idx, const sstring& value, api::timestamp_type ts) {
    return make_log_record(schema, sstring(fmt::format("pk{:04}", idx)), value, ts);
}

record_location wait_for_persisted(future<record_location>& fut) {
    return fut.get();
}

// A completion that resolves `done` with the location of its record, for the tests of the buffers.
struct location_completion {
    promise<record_location> done;

    void on_written(record_location location) noexcept {
        done.set_value(location);
    }
    void on_failed(std::exception_ptr ex) noexcept {
        done.set_exception(std::move(ex));
    }
};

using test_mixed_buffer = mixed_write_buffer<location_completion>;

// Writes `record` to a test mixed buffer. The future resolves to the location of the record.
future<record_location> buffer_write(test_mixed_buffer& wb, log_record record, write_target target = {}) {
    location_completion completion;
    auto done = completion.done.get_future();
    wb.write(make_writer(std::move(record)), std::move(target), std::move(completion));
    return done;
}

// Writes `record` to the ring with an index_insert into `index`. The future resolves to the location
// that the index then holds for the record, so the records of a test have keys of their own.
future<record_location> ring_write(write_buffer_ring& ring, primary_index& index, log_record record,
        db::timeout_clock::time_point timeout = test_timeout(), write_target target = {}) {
    const auto key = record.header.index_key();
    index_insert completion{
        .index = &index,
        .key = key,
        .timestamp = record.header.timestamp,
    };
    auto done = completion.done.get_future();
    ring.write(make_writer(std::move(record)), std::move(completion), timeout, std::move(target));
    return done.then([&index, key] {
        return index.get(key)->location;
    });
}

// Waits until the ring holds at most `count` queued writes, and fails the test when that takes
// longer than test_timeout(), so that a write that is never taken fails the test, not hangs it.
void wait_for_queued_writes(write_buffer_ring& ring, size_t count) {
    const auto deadline = test_timeout();
    while (ring.queued_write_count() > count) {
        if (db::timeout_clock::now() > deadline) {
            BOOST_FAIL(fmt::format("The ring still holds {} queued writes, more than {}", ring.queued_write_count(), count));
        }
        seastar::thread::yield();
    }
}

struct test_flush_controller {
    struct flushed_buffer {
        temporary_buffer<char> data;
        segment_position base_position;
        size_t record_count;
    };

    bool pause_flushes{false};
    std::optional<size_t> fail_flush_index;
    seastar::semaphore flush_started{0};
    seastar::semaphore flush_release{0};
    std::vector<flushed_buffer> flushed_buffers;
    size_t started_count{0};
    uint32_t next_segment_id{100};
    uint64_t next_sequence{1};

    future<> operator()(user_write_buffer& wb) {
        const auto flush_idx = started_count++;
        flush_started.signal(1);

        if (pause_flushes) {
            co_await flush_release.wait(1);
        }

        if (fail_flush_index && *fail_flush_index == flush_idx) {
            throw std::runtime_error("injected flush failure");
        }

        auto base_position = segment_position{
            .segment = log_segment_id{next_segment_id++},
            .offset = 0,
        };
        flushed_buffers.push_back(flushed_buffer{
            .data = copy_chunk(wb.seal(segment_sequence{next_sequence++})),
            .base_position = base_position,
            .record_count = wb.record_count(),
        });
        co_await wb.complete_writes(base_position);
    }

    void wait_for_flush_starts(size_t target_count) {
        while (started_count < target_count) {
            flush_started.wait().get();
        }
    }

    void release_one_flush() {
        flush_release.signal(1);
    }

    // Lets every flush, paused or to come, go on, so that stop() does not wait for a flush that a
    // failed check left paused.
    void release_all_flushes() {
        pause_flushes = false;
        flush_release.signal(started_count);
    }

    const temporary_buffer<char>& buffer_for_segment(log_segment_id segment_id) const {
        for (const auto& flushed : flushed_buffers) {
            if (flushed.base_position.segment == segment_id) {
                return flushed.data;
            }
        }
        throw std::runtime_error(fmt::format("Missing flushed segment {}", segment_id));
    }

    std::vector<scanned_record> all_records() const {
        std::vector<scanned_record> records;
        for (const auto& flushed : flushed_buffers) {
            auto buffer_records = scan_buffer_records(flushed.data, flushed.base_position.segment);
            records.insert(records.end(), std::make_move_iterator(buffer_records.begin()), std::make_move_iterator(buffer_records.end()));
        }
        return records;
    }
};

void assert_records_in_order(schema_ptr schema, const std::vector<scanned_record>& actual, const std::vector<log_record>& expected) {
    BOOST_REQUIRE_EQUAL(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        assert_log_record_matches(schema, actual[i].record, expected[i]);
    }
}

write_buffer_pool::config make_test_write_buffer_pool_config(size_t capacity, size_t max_cached, size_t preallocate = 0) {
    return write_buffer_pool::config{
        .capacity = capacity,
        .buffer_size = 4 * 1024,
        .preallocate = preallocate,
        .max_cached = max_cached,
    };
}

// A pooled buffer goes back to the pool when its handle is destroyed - here, when this returns.
void return_to_pool(owned_write_buffer buf) {
}

}

// Checks that sealing a full chunk builder writes the expected header fields.
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_record_and_header_serialization) {
    auto schema = make_kv_schema();
    auto expected = make_log_record(schema, "pk0", "v0", api::timestamp_type(7));

    chunk_builder wb(32 * 1024, segment_kind::full);
    auto writer = make_writer(expected);
    auto expected_records_size = size_t(ondisk::record_frame_header_size) + writer.record_size();
    expected_records_size = ((expected_records_size + ondisk::record_alignment - 1) / ondisk::record_alignment) * ondisk::record_alignment;
    wb.append(std::move(writer));

    const auto chunk = wb.seal(segment_sequence{17}, schema->id());
    BOOST_REQUIRE_EQUAL(chunk.size() % ondisk::block_alignment, 0u);

    seastar::simple_memory_input_stream in(reinterpret_cast<const char*>(chunk.data()), chunk.size());
    auto ch = ser::deserialize(in, std::type_identity<ondisk::chunk_header>{});
    auto sh = ser::deserialize(in, std::type_identity<ondisk::segment_header>{});
    BOOST_REQUIRE(ondisk::validate_chunk_headers(ch, sh));
    BOOST_REQUIRE(ch.kind == segment_kind::full);
    BOOST_REQUIRE_EQUAL(ch.segment_seq.value, 17u);
    BOOST_REQUIRE_EQUAL(ch.records_size, expected_records_size);

    BOOST_REQUIRE_EQUAL(sh.table, schema->id());
    BOOST_REQUIRE_EQUAL(sh.first_token, expected.header.key.token());
    BOOST_REQUIRE_EQUAL(sh.last_token, expected.header.key.token());

    // The schema table follows the records and lists the one version the record was written under.
    const auto& mapping = *test_registry().find(schema->version());
    BOOST_REQUIRE_EQUAL(ch.schema_table_size, chunk_layout::single_version_schema_table_size(mapping));
    auto entries = read_schema_table(chunk, ch);
    BOOST_REQUIRE_EQUAL(entries.size(), 1u);
    BOOST_REQUIRE_EQUAL(entries[0].table, schema->id());
    BOOST_REQUIRE_EQUAL(entries[0].schema_version, schema->version());
    BOOST_REQUIRE(entries[0].mapping == mapping.serialized);
}

// Checks that the size finish() returns covers the schema table. The records end at every offset
// of a block, also close enough to its end for the table to push the chunk into the next block.
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_finish_counts_the_schema_table) {
    auto schema = make_kv_schema();
    const auto table_size = chunk_layout::single_version_schema_table_size(test_registry().add(*schema));
    BOOST_REQUIRE_LT(table_size, ondisk::block_alignment);

    for (size_t value_size = 1; value_size <= ondisk::block_alignment; value_size += ondisk::record_alignment) {
        chunk_builder wb(4 * ondisk::block_alignment, segment_kind::mixed);
        wb.append(make_writer(make_log_record(schema, "pk0", sstring(value_size, 'x'), api::timestamp_type(1))));
        const auto size = wb.finish();
        const auto ch = read_chunk_header(copy_chunk(wb.seal(segment_sequence{1})));
        BOOST_REQUIRE_EQUAL(ch.schema_table_size, table_size);
        BOOST_REQUIRE_EQUAL(size, align_up(wb.chunk_headers_size() + ch.records_size + ch.schema_table_size, ondisk::block_alignment));
    }
}

// Checks that a buffer lists each schema version its records were written under once, in the
// order they were first appended, and that a registry built from the table decodes the records.
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_lists_each_schema_version_once) {
    auto schema = make_kv_schema();
    auto altered = schema_builder(schema).with_column("w", int32_type).build();
    BOOST_REQUIRE(altered->version() != schema->version());

    chunk_builder wb(32 * 1024, segment_kind::mixed);
    wb.append(make_writer(make_log_record(schema, "pk0", "v0", api::timestamp_type(1))));
    wb.append(make_writer(make_log_record(schema, "pk1", "v1", api::timestamp_type(2))));
    wb.append(make_writer(make_log_record(altered, "pk2", "v2", api::timestamp_type(3))));
    wb.append(make_writer(make_log_record(schema, "pk3", "v3", api::timestamp_type(4))));
    const auto chunk = wb.seal(segment_sequence{5});

    auto ch = read_chunk_header(copy_chunk(chunk));
    const auto& mapping = *test_registry().find(schema->version());
    const auto& altered_mapping = *test_registry().find(altered->version());
    BOOST_REQUIRE_EQUAL(ch.schema_table_size, ondisk::schema_table_header_size
            + ondisk::schema_table_entry_size(mapping.serialized.size())
            + ondisk::schema_table_entry_size(altered_mapping.serialized.size()));

    auto entries = read_schema_table(chunk, ch);
    BOOST_REQUIRE_EQUAL(entries.size(), 2u);
    BOOST_REQUIRE_EQUAL(entries[0].schema_version, schema->version());
    BOOST_REQUIRE(entries[0].mapping == mapping.serialized);
    BOOST_REQUIRE_EQUAL(entries[1].schema_version, altered->version());
    BOOST_REQUIRE(entries[1].mapping == altered_mapping.serialized);

    // A registry built from the scan decodes the records, also under the other version.
    column_mapping_registry recovered;
    auto serialized = copy_chunk(chunk);
    auto records = scan_buffer_records(serialized, log_segment_id{9}, &recovered);
    BOOST_REQUIRE_EQUAL(recovered.size(), 2u);
    BOOST_REQUIRE(recovered.find(schema->version())->mapping == mapping.mapping);
    BOOST_REQUIRE(recovered.find(altered->version())->mapping == altered_mapping.mapping);
    BOOST_REQUIRE_EQUAL(records.size(), 4u);
    assert_that(to_mutation(records[0].record, altered, recovered)).is_equal_to(make_kv_mutation(altered, "pk0", "v0", api::timestamp_type(1)));
    assert_that(to_mutation(records[2].record, schema, recovered)).is_equal_to(make_kv_mutation(schema, "pk2", "v2", api::timestamp_type(3)));
}

// Checks that a buffer lists a schema version once when its records come with the entries of two
// registries.
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_lists_a_schema_version_once_across_registries) {
    auto schema = make_kv_schema();

    column_mapping_registry first;
    column_mapping_registry second;
    const auto& from_first = first.add(*schema);
    const auto& from_second = second.add(*schema);
    BOOST_REQUIRE(&from_first != &from_second);
    BOOST_REQUIRE_EQUAL(from_first.schema_version, from_second.schema_version);

    chunk_builder wb(32 * 1024, segment_kind::mixed);
    wb.append(log_record_writer(make_log_record(schema, "pk0", "v0", api::timestamp_type(1)), from_first));
    wb.append(log_record_writer(make_log_record(schema, "pk1", "v1", api::timestamp_type(2)), from_second));
    const auto chunk = wb.seal(segment_sequence{7});

    auto ch = read_chunk_header(copy_chunk(chunk));
    BOOST_REQUIRE_EQUAL(ch.schema_table_size, chunk_layout::single_version_schema_table_size(from_first));
    auto entries = read_schema_table(chunk, ch);
    BOOST_REQUIRE_EQUAL(entries.size(), 1u);
    BOOST_REQUIRE_EQUAL(entries[0].schema_version, schema->version());
}

// Checks that the room a record's schema table entry takes is charged to the record that brings
// the version into the buffer: a record that fits as one more of a version the buffer already
// lists does not fit as the first of another version.
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_charges_the_schema_table_entry_to_the_first_record_of_a_version) {
    auto schema = make_kv_schema();
    auto altered = schema_builder(schema).with_column("w", int32_type).build();

    chunk_builder wb(ondisk::block_alignment, segment_kind::mixed);
    wb.append(make_writer(make_log_record(schema, "pk0", "v0", api::timestamp_type(1))));

    // The largest record of the buffer's version that still fits.
    sstring value;
    while (wb.can_fit(make_writer(make_log_record(schema, "pk1", value + "x", api::timestamp_type(2))))) {
        value += "x";
    }
    auto same_version = make_writer(make_log_record(schema, "pk1", value, api::timestamp_type(2)));
    auto other_version = make_writer(make_log_record(altered, "pk1", value, api::timestamp_type(2)));
    BOOST_REQUIRE_EQUAL(same_version.record_size(), other_version.record_size());

    BOOST_REQUIRE(wb.can_fit(same_version));
    BOOST_REQUIRE(!wb.can_fit(other_version));
    BOOST_REQUIRE_THROW(wb.append(other_version), std::runtime_error);

    // The same record fits an empty buffer, whose max_record_size() accounts for its entry.
    chunk_builder empty(ondisk::block_alignment, segment_kind::mixed);
    BOOST_REQUIRE_LE(other_version.record_size(),
            chunk_layout::max_record_size(ondisk::block_alignment, segment_kind::mixed, other_version.mapping()));
    BOOST_REQUIRE(empty.can_fit(other_version));
}

// Checks that a chunk builder can hold and seal a record whose serialized size is exactly max_record_size().
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_accepts_record_at_max_record_size) {
    auto schema = make_kv_schema();

    chunk_builder wb(ondisk::block_alignment, segment_kind::mixed);

    sstring value;
    auto record = make_log_record(schema, "pk", "", api::timestamp_type(27));
    auto writer = make_writer(record);
    auto max_size = chunk_layout::max_record_size(ondisk::block_alignment, segment_kind::mixed, writer.mapping());

    while (writer.record_size() < max_size) {
        value += "x";
        record = make_log_record(schema, "pk", value, api::timestamp_type(27));
        writer = make_writer(record);
    }

    BOOST_REQUIRE_EQUAL(writer.record_size(), max_size);
    BOOST_REQUIRE(wb.can_fit(writer));

    wb.append(writer);

    BOOST_REQUIRE_EQUAL(wb.seal(segment_sequence{29}).size(), ondisk::block_alignment);
}

// Checks that the size finish() returns is the size of the chunk seal() returns, and that the
// chunk carries the sequence number seal() got.
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_finish_returns_the_final_chunk_size) {
    auto schema = make_kv_schema();

    chunk_builder wb(4 * ondisk::block_alignment, segment_kind::mixed);
    wb.append(make_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(1))));
    const auto size = wb.finish();
    BOOST_REQUIRE_EQUAL(size % ondisk::block_alignment, 0u);
    BOOST_REQUIRE_EQUAL(wb.finish(), size);

    auto chunk = copy_chunk(wb.seal(segment_sequence{3}));
    BOOST_REQUIRE_EQUAL(chunk.size(), size);
    BOOST_REQUIRE_EQUAL(read_chunk_header(chunk).segment_seq.value, 3u);
    BOOST_REQUIRE_THROW(wb.seal(segment_sequence{3}), std::runtime_error);
}

// Checks that a builder that is reset after a chunk of one table seals a chunk of another table with
// nothing left from the first: the table, the token range, the schema table and the records.
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_reused_for_another_table_seals_only_its_own_chunk) {
    auto first_schema = make_kv_schema();
    auto second_schema = make_kv_schema();
    BOOST_REQUIRE(first_schema->id() != second_schema->id());

    // Keys whose tokens are in order, so that the first chunk can take the lowest and the highest
    // token and leave the ones between them to the second: a token range the reset does not clear
    // then shows in the segment header of the second chunk.
    std::vector<sstring> keys;
    for (size_t i = 0; i < 7; ++i) {
        keys.push_back(fmt::format("pk{}", i));
    }
    auto token_of = [&] (const sstring& key) {
        return dht::decorate_key(*second_schema, partition_key::from_single_value(*second_schema, serialized(key))).token();
    };
    std::ranges::sort(keys, std::less{}, token_of);

    chunk_builder wb(32 * 1024, segment_kind::full);
    wb.append(make_writer(make_log_record(first_schema, keys.front(), "v", api::timestamp_type(1))));
    wb.append(make_writer(make_log_record(first_schema, keys.back(), "v", api::timestamp_type(2))));
    wb.seal(segment_sequence{1}, first_schema->id());
    wb.reset();

    // Appended so that neither the first nor the last record holds the lowest or the highest token.
    std::vector<log_record> by_token;
    for (size_t i = 1; i < keys.size() - 1; ++i) {
        by_token.push_back(make_log_record(second_schema, keys[i], "v", api::timestamp_type(10 + i)));
    }
    std::vector<log_record> expected;
    for (size_t i : {2, 0, 4, 1, 3}) {
        expected.push_back(by_token[i]);
    }

    size_t expected_records_size = 0;
    for (const auto& record : expected) {
        auto writer = make_writer(record);
        expected_records_size += align_up(ondisk::record_frame_header_size + writer.record_size(), ondisk::record_alignment);
        wb.append(writer);
    }

    const auto chunk = wb.seal(segment_sequence{2}, second_schema->id());
    seastar::simple_memory_input_stream in(reinterpret_cast<const char*>(chunk.data()), chunk.size());
    auto ch = ser::deserialize(in, std::type_identity<ondisk::chunk_header>{});
    auto sh = ser::deserialize(in, std::type_identity<ondisk::segment_header>{});
    BOOST_REQUIRE(ondisk::validate_chunk_headers(ch, sh));
    BOOST_REQUIRE_EQUAL(ch.segment_seq.value, 2u);
    BOOST_REQUIRE_EQUAL(ch.records_size, expected_records_size);
    BOOST_REQUIRE_EQUAL(sh.table, second_schema->id());
    BOOST_REQUIRE_EQUAL(sh.first_token, by_token.front().header.key.token());
    BOOST_REQUIRE_EQUAL(sh.last_token, by_token.back().header.key.token());

    auto entries = read_schema_table(chunk, ch);
    BOOST_REQUIRE_EQUAL(entries.size(), 1u);
    BOOST_REQUIRE_EQUAL(entries[0].table, second_schema->id());
    BOOST_REQUIRE_EQUAL(entries[0].schema_version, second_schema->version());

    assert_records_in_order(second_schema, scan_buffer_records(copy_chunk(chunk), log_segment_id{1}), expected);
}

// Checks that a record that compaction or the separator rewrites from the bytes of its frame gets the
// same frame as the record it was read from.
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_bytes_writer_writes_the_same_frame) {
    auto schema = make_kv_schema();
    auto writer = make_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(5)));

    chunk_builder original(32 * 1024, segment_kind::mixed);
    const auto original_frame = original.append(writer);
    const auto original_bytes = original.frame_bytes(original_frame);

    seastar::simple_memory_input_stream in(reinterpret_cast<const char*>(original_bytes.data()), original_bytes.size());
    auto parsed = ondisk::try_read_record_frame(in);
    BOOST_REQUIRE(parsed);
    BOOST_REQUIRE_EQUAL(in.size(), 0u);

    chunk_builder rewritten(32 * 1024, segment_kind::full);
    const auto bytes_writer = log_record_bytes_writer(parsed->header, parsed->bytes, writer.mapping());
    BOOST_REQUIRE_EQUAL(bytes_writer.record_size(), writer.record_size());
    const auto rewritten_frame = rewritten.append(bytes_writer);

    BOOST_REQUIRE_EQUAL(rewritten_frame.size, original_frame.size);
    BOOST_REQUIRE(rewritten.frame_bytes(rewritten_frame) == original_bytes);
}

// Checks that abort_writes() fails the writes of a buffer whose chunk is not written, and that the
// buffer can then be reset and reused.
SEASTAR_THREAD_TEST_CASE(test_logstor_mixed_write_buffer_abort_writes_fails_its_writes) {
    auto schema = make_kv_schema();

    test_mixed_buffer wb(32 * 1024);
    auto written = buffer_write(wb, make_log_record(schema, "pk0", "v0", api::timestamp_type(1)));
    BOOST_REQUIRE(!written.available());

    wb.abort_writes(std::make_exception_ptr(std::runtime_error("buffer was not flushed")));
    BOOST_REQUIRE_THROW(written.get(), std::runtime_error);

    wb.reset();
    BOOST_REQUIRE(!wb.has_data());
    auto reused = buffer_write(wb, make_log_record(schema, "pk1", "v1", api::timestamp_type(2)));
    wb.seal(segment_sequence{1});
    wb.complete_writes(segment_position{.segment = log_segment_id{5}, .offset = 0}).get();
    BOOST_REQUIRE(reused.get().segment == log_segment_id{5});
}

// A completion that records the order in which the buffer completes the writes.
struct order_completion {
    std::vector<size_t>* order;
    size_t index;

    void on_written(record_location) noexcept {
        order->push_back(index);
    }
    void on_failed(std::exception_ptr) noexcept {}
};

// Checks that complete_writes() completes the writes in the order of the records.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_completes_its_writes_in_record_order) {
    auto schema = make_kv_schema();
    std::vector<size_t> order;
    order.reserve(10);

    full_write_buffer<order_completion> wb(32 * 1024);
    for (size_t i = 0; i < 10; ++i) {
        wb.write(make_writer(make_log_record(schema, fmt::format("pk{}", i), "v", api::timestamp_type(1))),
                order_completion{.order = &order, .index = i});
    }
    wb.seal(segment_sequence{1}, schema->id());
    wb.complete_writes(segment_position{.segment = log_segment_id{5}, .offset = 0}).get();

    BOOST_REQUIRE_EQUAL(order.size(), 10u);
    for (size_t i = 0; i < order.size(); ++i) {
        BOOST_REQUIRE_EQUAL(order[i], i);
    }
}

// Checks that a write that throws, here because the record does not fit, leaves each kind of
// buffer as it was.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_write_that_throws_leaves_the_buffer_unchanged) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    auto small = make_log_record(schema, "pk0", "v0", api::timestamp_type(1));
    auto large = make_log_record(schema, "pk1", sstring(2 * buffer_size, 'x'), api::timestamp_type(1));
    const auto chunk_position = segment_position{.segment = log_segment_id{5}, .offset = 0};

    {
        test_mixed_buffer wb(buffer_size);
        auto written = buffer_write(wb, small);
        location_completion completion;
        auto failed = completion.done.get_future();
        BOOST_REQUIRE_THROW(wb.write(make_writer(large), write_target{}, std::move(completion)), std::runtime_error);
        // The completion of the failed write is left to the caller.
        completion.on_failed(std::make_exception_ptr(std::runtime_error("write failed")));
        BOOST_REQUIRE_THROW(failed.get(), std::runtime_error);
        BOOST_REQUIRE_EQUAL(wb.record_count(), 1u);

        wb.seal(segment_sequence{1});
        const auto records = wb.complete_writes(chunk_position).get();
        BOOST_REQUIRE(written.get().segment == log_segment_id{5});
        BOOST_REQUIRE_EQUAL(records.size(), 1u);
    }

    {
        primary_index index(noop_space_accounting, nullptr, false);
        const auto old_location = record_location{.segment = log_segment_id{1}, .offset = 0, .size = 64};
        index.insert(small.header.index_key(), index_entry{.location = old_location, .timestamp = 1});
        index.insert(large.header.index_key(), index_entry{.location = old_location, .timestamp = 1});
        size_t relocated = 0;

        rewrite_buffer wb(buffer_size);
        wb.write(make_writer(small), index_relocation{.index = &index, .key = small.header.index_key(), .old_location = old_location, .relocated = &relocated});
        BOOST_REQUIRE_THROW(wb.write(make_writer(large), index_relocation{.index = &index, .key = large.header.index_key(), .old_location = old_location, .relocated = &relocated}),
                std::runtime_error);
        BOOST_REQUIRE_EQUAL(wb.record_count(), 1u);

        wb.seal(segment_sequence{1}, schema->id());
        wb.complete_writes(chunk_position).get();
        BOOST_REQUIRE_EQUAL(relocated, 1u);
        BOOST_REQUIRE(index.get(large.header.index_key())->location == old_location);
    }
}

// Checks that an index_relocation points the index at the new location of its record, except for a
// key that a newer write replaced, and that an aborted relocation leaves the index unchanged.
SEASTAR_THREAD_TEST_CASE(test_logstor_index_relocation_moves_the_index_entry) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    size_t relocated = 0;

    auto record0 = make_log_record(schema, "pk0", "v0", api::timestamp_type(1));
    auto record1 = make_log_record(schema, "pk1", "v1", api::timestamp_type(1));
    const auto key0 = record0.header.index_key();
    const auto key1 = record1.header.index_key();
    const auto old_location0 = record_location{.segment = log_segment_id{1}, .offset = 0, .size = 64};
    const auto old_location1 = record_location{.segment = log_segment_id{1}, .offset = 64, .size = 64};
    index.insert(key0, index_entry{.location = old_location0, .timestamp = 1});
    index.insert(key1, index_entry{.location = old_location1, .timestamp = 1});

    rewrite_buffer wb(32 * 1024);
    wb.write(make_writer(record0), index_relocation{.index = &index, .key = key0, .old_location = old_location0, .relocated = &relocated});
    wb.write(make_writer(record1), index_relocation{.index = &index, .key = key1, .old_location = old_location1, .relocated = &relocated});
    wb.seal(segment_sequence{1}, schema->id());

    // A newer write of pk1 replaces the entry before the chunk is written.
    const auto newer_location1 = record_location{.segment = log_segment_id{2}, .offset = 0, .size = 64};
    index.insert(key1, index_entry{.location = newer_location1, .timestamp = 2});

    wb.complete_writes(segment_position{.segment = log_segment_id{5}, .offset = 0}).get();
    BOOST_REQUIRE_EQUAL(relocated, 1u);
    BOOST_REQUIRE(index.get(key0)->location.segment == log_segment_id{5});
    BOOST_REQUIRE(index.get(key1)->location == newer_location1);

    wb.reset();
    const auto location0 = index.get(key0)->location;
    wb.write(make_writer(record0), index_relocation{.index = &index, .key = key0, .old_location = location0, .relocated = &relocated});
    wb.abort_writes(std::make_exception_ptr(std::runtime_error("chunk was not written")));
    BOOST_REQUIRE_EQUAL(relocated, 1u);
    BOOST_REQUIRE(index.get(key0)->location == location0);

    wb.reset();
    BOOST_REQUIRE(!wb.has_data());
}

// Checks that an index_insert inserts its record into the index before it resolves its future, and
// that a failed write fails the future and leaves the index as it was.
SEASTAR_THREAD_TEST_CASE(test_logstor_index_insert_inserts_before_it_resolves) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    auto record = make_log_record(schema, "pk0", "v0", api::timestamp_type(7));
    const auto key = record.header.index_key();

    user_write_buffer wb(32 * 1024);
    index_insert completion{.index = &index, .key = key, .timestamp = 7};
    auto done = completion.done.get_future().then([&index, key] {
        return index.get(key);
    });
    wb.write(make_writer(record), write_target{}, std::move(completion));
    wb.seal(segment_sequence{1});
    const auto records = wb.complete_writes(segment_position{.segment = log_segment_id{5}, .offset = 0}).get();
    // The record is in the index by the time that the buffer gives it to the separator.
    BOOST_REQUIRE(index.get(key));
    const auto entry = done.get();
    BOOST_REQUIRE(entry);
    BOOST_REQUIRE(entry->location.segment == log_segment_id{5});
    BOOST_REQUIRE_EQUAL(entry->timestamp, api::timestamp_type(7));
    BOOST_REQUIRE_EQUAL(records.size(), 1u);

    wb.reset();
    auto other = make_log_record(schema, "pk1", "v1", api::timestamp_type(8));
    index_insert failed{.index = &index, .key = other.header.index_key(), .timestamp = 8};
    auto failed_done = failed.done.get_future();
    wb.write(make_writer(other), write_target{}, std::move(failed));
    wb.abort_writes(std::make_exception_ptr(std::runtime_error("chunk was not written")));
    BOOST_REQUIRE_THROW(failed_done.get(), std::runtime_error);
    BOOST_REQUIRE(!index.get(other.header.index_key()));
}

// Checks that a mixed buffer gives the separator each record with its write target and its location,
// also for a chunk at an offset other than 0, and that an aborted buffer drops its records and the
// holders of their targets.
SEASTAR_THREAD_TEST_CASE(test_logstor_mixed_write_buffer_keeps_its_records_for_the_separator) {
    auto schema = make_kv_schema();
    // Outlives the buffers, whose records hold it.
    seastar::gate target_gate;

    test_mixed_buffer wb(32 * 1024);
    std::vector<future<record_location>> writes;
    for (size_t i = 0; i < 3; ++i) {
        auto record = make_log_record(schema, fmt::format("pk{}", i), "v", api::timestamp_type(1 + i));
        writes.push_back(buffer_write(wb, std::move(record), write_target{.non_empty_holder = target_gate.hold()}));
    }
    BOOST_REQUIRE_EQUAL(target_gate.get_count(), 3u);

    wb.seal(segment_sequence{1});
    const auto chunk_position = segment_position{.segment = log_segment_id{7}, .offset = 3 * ondisk::block_alignment};
    auto records = wb.complete_writes(chunk_position).get();
    std::vector<record_location> locations;
    for (auto& write : writes) {
        locations.push_back(wait_for_persisted(write));
    }

    BOOST_REQUIRE_EQUAL(records.size(), 3u);
    for (size_t i = 0; i < records.size(); ++i) {
        BOOST_REQUIRE(records[i].location(chunk_position) == locations[i]);
        BOOST_REQUIRE_GE(locations[i].offset, chunk_position.offset + ondisk::chunk_header_size);
        BOOST_REQUIRE_EQUAL(records[i].writer.record().header.timestamp, api::timestamp_type(1 + i));
    }
    // The write targets stay with the records until the separator is done with them.
    BOOST_REQUIRE_EQUAL(target_gate.get_count(), 3u);
    records.clear();
    BOOST_REQUIRE_EQUAL(target_gate.get_count(), 0u);

    test_mixed_buffer aborted(32 * 1024);
    auto write = buffer_write(aborted, make_log_record(schema, "pk3", "v", api::timestamp_type(4)),
            write_target{.non_empty_holder = target_gate.hold()});
    BOOST_REQUIRE_EQUAL(target_gate.get_count(), 1u);
    aborted.abort_writes(std::make_exception_ptr(std::runtime_error("chunk was not written")));
    BOOST_REQUIRE_THROW(write.get(), std::runtime_error);
    BOOST_REQUIRE_EQUAL(target_gate.get_count(), 0u);

    target_gate.close().get();
}

// Checks that a pool builds its buffers at the point of use and keeps only max_cached of them once
// they are returned, so that a pool whose capacity is rarely reached does not hold the memory for it.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_pool_builds_buffers_on_demand) {
    abort_source as;
    write_buffer_pool pool(make_test_write_buffer_pool_config(4, 1));
    auto stop_pool = seastar::defer([&pool] noexcept { pool.stop().get(); });

    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 0u);

    auto b0 = pool.allocate(as).get();
    auto b1 = pool.allocate(as).get();
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 2u);
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 2u);
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 2u);
    BOOST_REQUIRE_EQUAL(b0->buffer_size(), 4u * 1024);

    return_to_pool(std::move(b0));
    return_to_pool(std::move(b1));
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 0u);
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 1u);

    // The kept buffer is handed out again, the one that was let go is built anew.
    auto b2 = pool.allocate(as).get();
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 2u);
    auto b3 = pool.allocate(as).get();
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 3u);

    return_to_pool(std::move(b2));
    return_to_pool(std::move(b3));
}

// Checks that a pool configured to build its buffers up front never builds one at the point of use,
// which is what the compaction buffer pool relies on.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_pool_preallocates_its_buffers) {
    abort_source as;
    write_buffer_pool pool(make_test_write_buffer_pool_config(2, 2, 2));
    auto stop_pool = seastar::defer([&pool] noexcept { pool.stop().get(); });

    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 2u);
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 2u);

    auto bufs = pool.allocate_many(2, as).get();
    BOOST_REQUIRE_EQUAL(bufs.size(), 2u);
    BOOST_REQUIRE(bufs[0].get() != bufs[1].get());
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 2u);
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 2u);

    for (auto& buf : bufs) {
        return_to_pool(std::move(buf));
    }
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 0u);
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 2u);
}

// Checks that the capacity bounds how many buffers are out at once, and that raising it lets a
// waiting allocation through.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_pool_capacity_bounds_allocations) {
    abort_source as;
    write_buffer_pool pool(make_test_write_buffer_pool_config(1, 1));
    auto stop_pool = seastar::defer([&pool] noexcept { pool.stop().get(); });

    auto b0 = pool.allocate(as).get();
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 1u);

    auto waiting = pool.allocate(as);
    seastar::thread::yield();
    BOOST_REQUIRE(!waiting.available());
    BOOST_REQUIRE_EQUAL(pool.get_stats().allocation_waits, 1u);

    pool.set_capacity(2);
    auto b1 = waiting.get();
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 2u);

    return_to_pool(std::move(b0));
    return_to_pool(std::move(b1));
}

// Checks that lowering the capacity while buffers are out takes effect as they come back: the first
// return covers the capacity that was taken away and its buffer is freed rather than kept.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_pool_capacity_shrinks_while_buffers_are_out) {
    abort_source as;
    write_buffer_pool pool(make_test_write_buffer_pool_config(2, 2));
    auto stop_pool = seastar::defer([&pool] noexcept { pool.stop().get(); });

    auto b0 = pool.allocate(as).get();
    auto b1 = pool.allocate(as).get();

    pool.set_capacity(1);
    BOOST_REQUIRE_EQUAL(pool.capacity(), 1u);

    return_to_pool(std::move(b0));
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 1u);

    auto waiting = pool.allocate(as);
    seastar::thread::yield();
    BOOST_REQUIRE(!waiting.available());

    return_to_pool(std::move(b1));
    auto b2 = waiting.get();
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 1u);
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 1u);

    return_to_pool(std::move(b2));
}

// Checks that write_buffer_ring forwards records to the flush callback and resolves returned locations.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_basic_flushes_records) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;

    test_flush_controller flush_ctl;
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 3), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected;
    std::vector<future<record_location>> persisted;
    for (size_t i = 0; i < 3; ++i) {
        auto record = make_write_buffer_ring_record(schema, i, sstring(fmt::format("value{}", i)), api::timestamp_type(100 + i));
        expected.push_back(record);
        persisted.push_back(ring_write(ring, index, record));
    }

    std::vector<record_location> locations;
    for (auto& fut : persisted) {
        locations.push_back(wait_for_persisted(fut));
    }

    const auto actual = flush_ctl.all_records();
    assert_records_in_order(schema, actual, expected);

    BOOST_REQUIRE_EQUAL(locations.size(), expected.size());
    for (size_t i = 0; i < locations.size(); ++i) {
        auto read_back = read_record_at_location(flush_ctl.buffer_for_segment(locations[i].segment), locations[i]);
        assert_log_record_matches(schema, read_back, expected[i]);
    }
}

// Checks that flush() seals and writes a partially filled head buffer even when the sync timer has not expired.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_flushes_partial_buffer_with_large_sync_period) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;
    constexpr auto sync_period = std::chrono::hours(1);

    test_flush_controller flush_ctl;
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 3, 0, sync_period), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected;
    std::vector<future<record_location>> persisted;
    for (size_t i = 0; i < 3; ++i) {
        auto record = make_write_buffer_ring_record(schema, i, sstring(fmt::format("value{}", i)), api::timestamp_type(150 + i));
        expected.push_back(record);
        persisted.push_back(ring_write(ring, index, record));
    }

    BOOST_REQUIRE(flush_ctl.flushed_buffers.empty());

    auto flush = ring.flush();
    flush_ctl.wait_for_flush_starts(1);

    std::vector<record_location> locations;
    for (auto& fut : persisted) {
        locations.push_back(wait_for_persisted(fut));
    }

    flush.get();

    BOOST_REQUIRE_EQUAL(flush_ctl.flushed_buffers.size(), 1u);
    BOOST_REQUIRE_EQUAL(flush_ctl.flushed_buffers.front().record_count, expected.size());

    const auto actual = flush_ctl.all_records();
    assert_records_in_order(schema, actual, expected);

    BOOST_REQUIRE_EQUAL(locations.size(), expected.size());
    for (size_t i = 0; i < locations.size(); ++i) {
        auto read_back = read_record_at_location(flush_ctl.buffer_for_segment(locations[i].segment), locations[i]);
        assert_log_record_matches(schema, read_back, expected[i]);
    }
}

// Checks that a paused tail flush can fill the ring, queue later writes, and then drain them without losing order.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_paused_flush_fills_ring_then_drains) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 4; ++i) {
        expected.push_back(make_write_buffer_ring_record(schema, i, large_value, api::timestamp_type(200 + i)));
    }

    auto persisted0 = ring_write(ring, index, expected[0]);
    flush_ctl.wait_for_flush_starts(1);
    BOOST_REQUIRE(!persisted0.available());

    auto persisted1 = ring_write(ring, index, expected[1]);

    auto persisted2 = ring_write(ring, index, expected[2]);
    auto persisted3 = ring_write(ring, index, expected[3]);
    BOOST_REQUIRE(!persisted2.available());
    BOOST_REQUIRE(!persisted3.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 2u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);

    wait_for_queued_writes(ring, 1);
    BOOST_REQUIRE(!persisted3.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 1u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);

    wait_for_queued_writes(ring, 0);
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 0u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    flush_ctl.wait_for_flush_starts(4);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted3);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// A record that finds no room in the ring is queued, and it has to keep the write target it came
// with: the holders of that target are what keep its compaction group from being reported empty to
// a tablet split and from finishing its stop() while the write is still to come.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_queued_write_keeps_its_write_target) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    // Outlive the writer, so that the holders of the queued write are released before the gates
    // they were taken from are destroyed.
    seastar::gate non_empty_gate;
    seastar::gate alive_gate;

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 3; ++i) {
        expected.push_back(make_write_buffer_ring_record(schema, i, large_value, api::timestamp_type(300 + i)));
    }

    // Fill the ring: the first record is being flushed and the second holds the only other buffer.
    auto persisted0 = ring_write(ring, index, expected[0]);
    flush_ctl.wait_for_flush_starts(1);
    auto persisted1 = ring_write(ring, index, expected[1]);

    // The third record has nowhere to go, so it is queued rather than appended.
    auto persisted2 = ring_write(ring, index, expected[2], test_timeout(), write_target{
            .cg = nullptr,
            .non_empty_holder = non_empty_gate.hold(),
            .alive_holder = alive_gate.hold(),
    });
    BOOST_REQUIRE(!persisted2.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 1u);

    // The write is in flight, so its group is still held by both of the target's holders. Checked
    // rather than required, so that a failure here still drains the writer below: a queued write
    // left behind would hang the stop() of this test rather than fail it.
    BOOST_CHECK_EQUAL(non_empty_gate.get_count(), 1u);
    BOOST_CHECK_EQUAL(alive_gate.get_count(), 1u);

    // Let the queued record through and drain everything, which releases the holders.
    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);

    wait_for_queued_writes(ring, 0);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that queued writes are accepted and persisted in FIFO order once capacity becomes available.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_queued_writes_preserve_fifo_order) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 5; ++i) {
        expected.push_back(make_write_buffer_ring_record(schema, i, large_value, api::timestamp_type(300 + i)));
    }

    auto persisted0 = ring_write(ring, index, expected[0]);
    flush_ctl.wait_for_flush_starts(1);

    auto persisted1 = ring_write(ring, index, expected[1]);

    auto persisted2 = ring_write(ring, index, expected[2]);
    auto persisted3 = ring_write(ring, index, expected[3]);
    auto persisted4 = ring_write(ring, index, expected[4]);

    BOOST_REQUIRE(!persisted2.available());
    BOOST_REQUIRE(!persisted3.available());
    BOOST_REQUIRE(!persisted4.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 3u);

    // Each drain step below frees one buffer, which takes one queued write, and the order of the
    // flushed records at the end shows that it is the oldest one.

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);
    wait_for_queued_writes(ring, 2);
    BOOST_REQUIRE(!persisted3.available());
    BOOST_REQUIRE(!persisted4.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    wait_for_queued_writes(ring, 1);
    BOOST_REQUIRE(!persisted4.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    flush_ctl.wait_for_flush_starts(4);
    wait_for_queued_writes(ring, 0);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted3);
    flush_ctl.wait_for_flush_starts(5);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted4);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that queued writes are rejected once max_queued_write_bytes would be exceeded.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_rejects_when_max_queued_write_bytes_exceeded) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    auto record0 = make_write_buffer_ring_record(schema, 0, large_value, api::timestamp_type(400));
    const auto queued_write_budget = make_writer(record0).record_size() * 2;

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2, queued_write_budget), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected{record0};
    for (size_t i = 1; i < 4; ++i) {
        expected.push_back(make_write_buffer_ring_record(schema, i, large_value, api::timestamp_type(400 + i)));
    }

    auto persisted0 = ring_write(ring, index, expected[0]);
    flush_ctl.wait_for_flush_starts(1);

    auto persisted1 = ring_write(ring, index, expected[1]);

    auto persisted2 = ring_write(ring, index, expected[2]);
    auto persisted3 = ring_write(ring, index, expected[3]);
    BOOST_REQUIRE(!persisted2.available());
    BOOST_REQUIRE(!persisted3.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 2u);

    auto rejected = make_write_buffer_ring_record(schema, 4, large_value, api::timestamp_type(404));
    auto rejected_write = ring_write(ring, index, rejected);
    BOOST_REQUIRE_THROW(rejected_write.get(), replica::rate_limit_exception);
    BOOST_REQUIRE_EQUAL(ring.failed_write_count(), 1u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);
    wait_for_queued_writes(ring, 1);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    wait_for_queued_writes(ring, 0);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    flush_ctl.wait_for_flush_starts(4);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted3);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that a queued write that times out fails with timed_out_error, leaves the queue, and leaves
// no failed future that nothing waits for.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_queued_write_times_out) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring, &flush_ctl] noexcept {
        flush_ctl.release_all_flushes();
        ring.stop().get();
    });

    auto persisted0 = ring_write(ring, index, make_write_buffer_ring_record(schema, 0, large_value, api::timestamp_type(500)));
    flush_ctl.wait_for_flush_starts(1);

    auto persisted1 = ring_write(ring, index, make_write_buffer_ring_record(schema, 1, large_value, api::timestamp_type(501)));

    const auto abandoned_before = seastar::engine().abandoned_failed_futures();
    auto queued = ring_write(ring, index, make_write_buffer_ring_record(schema, 2, large_value, api::timestamp_type(502)),
            db::timeout_clock::now() + std::chrono::milliseconds(10));
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 1u);
    BOOST_REQUIRE_THROW(queued.get(), seastar::timed_out_error);
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 0u);
    BOOST_REQUIRE_EQUAL(ring.failed_write_count(), 1u);
    BOOST_REQUIRE_EQUAL(seastar::engine().abandoned_failed_futures(), abandoned_before);

    flush_ctl.release_all_flushes();
    wait_for_persisted(persisted0);
    wait_for_persisted(persisted1);
}

// Checks that a queued write stays on the queue until a buffer is free, and that it completes only
// with the flush of the buffer that takes it.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_queued_write_completes_with_the_flush_of_its_buffer) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    auto record0 = make_write_buffer_ring_record(schema, 0, large_value, api::timestamp_type(500));
    auto record1 = make_write_buffer_ring_record(schema, 1, large_value, api::timestamp_type(501));
    auto record2 = make_write_buffer_ring_record(schema, 2, large_value, api::timestamp_type(502));

    auto persisted0 = ring_write(ring, index, record0);
    flush_ctl.wait_for_flush_starts(1);

    auto persisted1 = ring_write(ring, index, record1);

    auto persisted2 = ring_write(ring, index, record2);
    BOOST_REQUIRE(!persisted2.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 1u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);

    wait_for_queued_writes(ring, 0);
    BOOST_REQUIRE(!persisted2.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    BOOST_REQUIRE(!persisted2.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);

    const auto actual = flush_ctl.all_records();
    const std::vector<log_record> expected{record0, record1, record2};
    assert_records_in_order(schema, actual, expected);
}

// Checks that stop() waits for a blocked in-flight flush instead of dropping it.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_stop_drains_blocked_in_flight_write) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;
    const auto record = make_write_buffer_ring_record(schema, 0, "value", api::timestamp_type(600));

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();

    auto persisted = ring_write(ring, index, record);
    flush_ctl.wait_for_flush_starts(1);

    auto stop_fut = ring.stop();
    BOOST_REQUIRE(!stop_fut.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted);
    stop_fut.get();

    const auto actual = flush_ctl.all_records();
    BOOST_REQUIRE_EQUAL(actual.size(), 1u);
    assert_log_record_matches(schema, actual.front().record, record);
}

// Checks that stop() flushes the writes that wait for room in the ring before it completes, and that
// a write that comes after stop() fails.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_stop_drains_queued_writes) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();

    std::vector<log_record> expected;
    for (size_t i = 0; i < 3; ++i) {
        expected.push_back(make_write_buffer_ring_record(schema, i, large_value, api::timestamp_type(800 + i)));
    }

    auto persisted0 = ring_write(ring, index, expected[0]);
    flush_ctl.wait_for_flush_starts(1);
    auto persisted1 = ring_write(ring, index, expected[1]);
    auto persisted2 = ring_write(ring, index, expected[2]);
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 1u);

    auto stop_fut = ring.stop();

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);
    wait_for_queued_writes(ring, 0);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    stop_fut.get();

    assert_records_in_order(schema, flush_ctl.all_records(), expected);

    auto late = make_write_buffer_ring_record(schema, 3, "value", api::timestamp_type(803));
    auto late_write = ring_write(ring, index, late);
    BOOST_REQUIRE_THROW(late_write.get(), seastar::gate_closed_exception);
    BOOST_REQUIRE_EQUAL(ring.failed_write_count(), 1u);
}

// Checks that a flush failure is propagated to every write that was already accepted into that buffer,
// and that the ring goes on taking and flushing writes after it.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_flush_failure_fails_all_writes_in_buffer) {
    auto schema = make_kv_schema();
    primary_index index(noop_space_accounting, nullptr, false);
    constexpr size_t buffer_size = 4 * 1024;

    auto record0 = make_write_buffer_ring_record(schema, 0, "value0", api::timestamp_type(700));
    auto record1 = make_write_buffer_ring_record(schema, 1, "value1", api::timestamp_type(701));
    chunk_builder scratch(buffer_size, segment_kind::mixed);
    BOOST_REQUIRE(scratch.can_fit(make_writer(record0)));
    scratch.append(make_writer(record0));
    BOOST_REQUIRE(scratch.can_fit(make_writer(record1)));

    test_flush_controller flush_ctl{.fail_flush_index = 0};
    // long sync period so both writes land in the same buffer.
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 3, 0, std::chrono::hours(1)), [&flush_ctl] (user_write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    auto persisted0 = ring_write(ring, index, record0);
    auto persisted1 = ring_write(ring, index, record1);

    auto flush = ring.flush();

    BOOST_REQUIRE_THROW(persisted0.get(), std::runtime_error);
    BOOST_REQUIRE_THROW(persisted1.get(), std::runtime_error);
    BOOST_REQUIRE_EQUAL(ring.failed_write_count(), 2u);

    flush.get();

    BOOST_REQUIRE_EQUAL(flush_ctl.started_count, 1u);
    BOOST_REQUIRE(flush_ctl.flushed_buffers.empty());

    // The ring takes writes again after the failure, and its buffers are reused, including the one
    // whose flush failed: one write and one flush per buffer of the ring.
    std::vector<log_record> expected;
    for (size_t i = 0; i < 3; ++i) {
        expected.push_back(make_write_buffer_ring_record(schema, 2 + i, fmt::format("value{}", 2 + i), api::timestamp_type(702 + i)));
        auto persisted = ring_write(ring, index, expected.back());
        auto flushed = ring.flush();
        wait_for_persisted(persisted);
        flushed.get();
    }

    BOOST_REQUIRE_EQUAL(flush_ctl.started_count, 4u);
    BOOST_REQUIRE_EQUAL(ring.failed_write_count(), 2u);
    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}
