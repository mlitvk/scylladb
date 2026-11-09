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
#include <set>
#include <span>
#include <stdexcept>
#include <vector>

#include <fmt/format.h>

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/memory-data-source.hh>
#include <seastar/testing/thread_test_case.hh>

#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/record_stream.hh"
#include "replica/logstor/record_value.hh"
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/write_buffer.hh"
#include "schema/schema_builder.hh"
#include "serializer.hh"
#include "test/lib/logstor_test_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/tmpdir.hh"
#include "utils/UUID_gen.hh"

using namespace replica::logstor;
using namespace tests::logstor;

namespace {

constexpr size_t segment_size = 128 * 1024;

schema_ptr make_schema() {
    return schema_builder(1, "ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();
}

// A record that a test streams.
struct test_record {
    sstring pk;
    sstring value;
    api::timestamp_type timestamp;
};

mutation make_mutation(schema_ptr schema, const test_record& r) {
    auto key = partition_key::from_single_value(*schema, serialized(r.pk));
    mutation m(schema, dht::decorate_key(*schema, key));
    auto& row = m.partition().clustered_row(*schema, clustering_key::make_empty());
    row.apply(row_marker(r.timestamp));
    const auto& v_def = *schema->get_column_definition("v");
    row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, r.timestamp, serialized(r.value)));
    return m;
}

log_record make_log_record(schema_ptr schema, const test_record& r, table_id table) {
    auto m = make_mutation(schema, r);
    return log_record{
        .header = {
            .key = m.decorated_key(),
            .timestamp = r.timestamp,
            .table = table,
        },
        .value = encode_record_value(m),
    };
}

primary_index_key index_key_of(schema_ptr schema, const test_record& r) {
    return primary_index_key(make_mutation(schema, r).decorated_key());
}

// The record bytes that a record adds to the live record bytes of its group.
size_t frame_size_of(schema_ptr schema, const test_record& r) {
    return ondisk::record_frame_size(make_log_record(schema, r, schema->id()));
}

std::vector<test_record> make_records(size_t count, size_t value_size, size_t first = 0) {
    std::vector<test_record> records;
    for (size_t i = first; i < first + count; ++i) {
        records.push_back(test_record{
            .pk = fmt::format("pk{}", i),
            .value = sstring(value_size, char('a' + i % 26)),
            .timestamp = api::timestamp_type(1000 + i),
        });
    }
    return records;
}

// A chunk as the sender builds it: of a full segment, sealed at record_alignment.
temporary_buffer<char> make_chunk(schema_ptr schema, std::span<const test_record> records, std::optional<table_id> table = {}) {
    const auto chunk_table = table.value_or(schema->id());
    raw_write_buffer buf(segment_size, segment_kind::full);
    for (const auto& r : records) {
        buf.append(log_record_writer(make_log_record(schema, r, chunk_table)));
    }
    buf.seal(segment_sequence{1}, chunk_table, ondisk::record_alignment);
    const auto chunk = buf.chunk();
    return temporary_buffer<char>(reinterpret_cast<const char*>(chunk.data()), chunk.size());
}

// The chunks of the records, `per_chunk` records in each.
std::vector<temporary_buffer<char>> make_chunks(schema_ptr schema, std::span<const test_record> records, size_t per_chunk) {
    std::vector<temporary_buffer<char>> chunks;
    for (size_t i = 0; i < records.size(); i += per_chunk) {
        chunks.push_back(make_chunk(schema, records.subspan(i, std::min(per_chunk, records.size() - i))));
    }
    return chunks;
}

// The bytes of the chunks one after the other, cut into pieces of `piece_size` bytes. A piece size
// that does not divide the chunk sizes makes the chunk headers and bodies cross pieces.
std::vector<temporary_buffer<char>> cut_into_pieces(const std::vector<temporary_buffer<char>>& chunks, size_t piece_size) {
    std::vector<char> bytes;
    for (const auto& chunk : chunks) {
        bytes.insert(bytes.end(), chunk.get(), chunk.get() + chunk.size());
    }
    std::vector<temporary_buffer<char>> pieces;
    for (size_t pos = 0; pos < bytes.size(); pos += piece_size) {
        const size_t len = std::min(piece_size, bytes.size() - pos);
        pieces.emplace_back(bytes.data() + pos, len);
    }
    return pieces;
}

temporary_buffer<char> copy_of(const temporary_buffer<char>& buf) {
    return temporary_buffer<char>(buf.get(), buf.size());
}

void flip_byte(temporary_buffer<char>& buf, size_t offset) {
    buf.get_write()[offset] ^= char(0x1);
}

ondisk::chunk_header read_chunk_header(const temporary_buffer<char>& chunk) {
    seastar::simple_memory_input_stream in(chunk.get(), chunk.size());
    return ser::deserialize(in, std::type_identity<ondisk::chunk_header>{});
}

void write_chunk_header(temporary_buffer<char>& chunk, const ondisk::chunk_header& ch) {
    seastar::simple_memory_output_stream out(chunk.get_write(), ondisk::chunk_header_size);
    ser::serialize<ondisk::chunk_header>(out, ch);
}

// The index of the first record of each segment, when the records are packed in order into full
// segments, as the sink packs them.
std::vector<size_t> segment_starts(schema_ptr schema, std::span<const test_record> records) {
    std::vector<size_t> starts{0};
    raw_write_buffer buf(segment_size, segment_kind::full);
    for (size_t i = 0; i < records.size(); ++i) {
        auto writer = log_record_writer(make_log_record(schema, records[i], schema->id()));
        if (!buf.can_fit(writer)) {
            starts.push_back(i);
            buf.reset();
        }
        buf.append(writer);
    }
    return starts;
}

// A started logstor, stopped at destruction.
struct logstor_env {
    tmpdir dir;
    shared_logstor_cache cache;
    logstor ls;

    logstor_env() : ls(make_test_logstor_config(dir.path()), cache.shared_tracker) {
        test_recovery_catalog no_tables;
        ls.do_recovery(no_tables).get();
        ls.start().get();
    }

    ~logstor_env() {
        ls.stop().get();
    }
};

// The gates a table gives the holders of a write from. A test closes them at the end, so that a
// holder that the sink did not release makes the test hang.
struct target_gates {
    seastar::gate non_empty;
    seastar::gate alive;

    write_target target(logstor_group& group) {
        return write_target{.cg = &group, .non_empty_holder = non_empty.hold(), .alive_holder = alive.hold()};
    }

    void close() {
        non_empty.close().get();
        alive.close().get();
    }
};

void put_pieces(record_stream_sink& sink, std::vector<temporary_buffer<char>>& pieces) {
    for (auto& piece : pieces) {
        sink.put(std::span(&piece, 1)).get();
    }
}

void require_reads_back(logstor& ls, schema_ptr schema, const primary_index& index, const test_record& r) {
    auto expected = make_mutation(schema, r);
    auto actual = read_mutation(ls, schema, index, expected.decorated_key(), schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// Writes the records through the write path and flushes them into one segment of the group.
void write_segment(logstor& ls, test_logstor_group& group, schema_ptr schema, std::span<const test_record> records) {
    for (const auto& r : records) {
        ls.write(make_mutation(schema, r), write_target(&group, {}), db::no_timeout).get();
    }
    ls.flush_to_separator().get();
    group.flush_separator().get();
}

// Reads the source to its end. Only the last buffer is empty.
std::vector<temporary_buffer<char>> read_all(record_stream_source& source) {
    std::vector<temporary_buffer<char>> chunks;
    while (true) {
        auto buf = source.get().get();
        if (buf.empty()) {
            break;
        }
        chunks.push_back(std::move(buf));
    }
    BOOST_REQUIRE(source.get().get().empty());
    return chunks;
}

// The timestamps of the records in the chunks. Every record of a test has a timestamp of its own.
std::multiset<api::timestamp_type> timestamps_in(const std::vector<temporary_buffer<char>>& chunks) {
    std::multiset<api::timestamp_type> timestamps;
    for (const auto& bytes : chunks) {
        auto chunk = parse_chunk(copy_of(bytes)).get();
        BOOST_REQUIRE(chunk);
        for (const auto& record : chunk->records()) {
            timestamps.insert(record.header.timestamp);
        }
    }
    return timestamps;
}

std::multiset<api::timestamp_type> timestamps_of(std::initializer_list<std::span<const test_record>> record_sets) {
    std::multiset<api::timestamp_type> timestamps;
    for (const auto& records : record_sets) {
        for (const auto& r : records) {
            timestamps.insert(r.timestamp);
        }
    }
    return timestamps;
}

// The records with new values and timestamps for the same keys.
std::vector<test_record> overwrite(std::span<const test_record> records, api::timestamp_type first_timestamp) {
    std::vector<test_record> result;
    for (const auto& r : records) {
        result.push_back(test_record{.pk = r.pk, .value = r.value + "-new", .timestamp = first_timestamp++});
    }
    return result;
}

// Gives the chunks of a segment that is in memory, as the segment manager gives the chunks of a
// segment on disk.
record_stream_source::chunk_reader memory_chunk_reader(temporary_buffer<char> segment) {
    return [segment = std::move(segment)] (log_segment_id segment_id, noncopyable_function<future<>(const scanned_chunk&)> fn) -> future<> {
        auto in = seastar::util::as_input_stream(copy_of(segment));
        segment_scanner scanner(in, segment_id, segment.size());
        auto f = co_await coroutine::as_future([&] -> future<> {
            while (auto* chunk = co_await scanner.next_chunk()) {
                co_await fn(*chunk);
            }
        }());
        co_await in.close();
        co_return co_await std::move(f);
    };
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_logstor_parse_chunk_gives_the_records_of_a_chunk) {
    auto schema = make_schema();
    const auto records = make_records(3, 100);

    auto chunk = parse_chunk(make_chunk(schema, records)).get();
    BOOST_REQUIRE(chunk);
    BOOST_REQUIRE(chunk->info().kind == segment_kind::full);
    BOOST_REQUIRE_EQUAL(std::get<segment_info::full>(chunk->info().v).table, schema->id());
    size_t i = 0;
    for (const auto& record : chunk->records()) {
        BOOST_REQUIRE_LT(i, records.size());
        BOOST_REQUIRE(record.header.index_key() == index_key_of(schema, records[i]));
        BOOST_REQUIRE_EQUAL(record.header.timestamp, records[i].timestamp);
        BOOST_REQUIRE_EQUAL(record.header.table, schema->id());
        ++i;
    }
    BOOST_REQUIRE_EQUAL(i, records.size());
}

SEASTAR_THREAD_TEST_CASE(test_logstor_parse_chunk_rejects_a_chunk_that_is_not_whole) {
    auto schema = make_schema();
    auto chunk = make_chunk(schema, make_records(3, 100));

    auto corrupt = copy_of(chunk);
    flip_byte(corrupt, chunk.size() - 1);
    BOOST_REQUIRE(!parse_chunk(std::move(corrupt)).get());

    BOOST_REQUIRE(!parse_chunk(chunk.share(0, chunk.size() - 8)).get());

    temporary_buffer<char> longer(chunk.size() + 8);
    std::fill_n(longer.get_write(), longer.size(), 0);
    std::copy_n(chunk.get(), chunk.size(), longer.get_write());
    BOOST_REQUIRE(!parse_chunk(std::move(longer)).get());
}

SEASTAR_THREAD_TEST_CASE(test_logstor_stream_chunk_assembler_reassembles_chunks) {
    auto schema = make_schema();
    std::vector<temporary_buffer<char>> chunks;
    chunks.push_back(make_chunk(schema, make_records(3, 100)));
    chunks.push_back(make_chunk(schema, make_records(5, 1000, 3)));

    // Pieces that cut the headers, and one piece that holds both chunks.
    for (const size_t piece_size : {size_t(5), size_t(333), size_t(1 << 20)}) {
        stream_chunk_assembler assembler;
        std::vector<temporary_buffer<char>> assembled;
        for (auto& piece : cut_into_pieces(chunks, piece_size)) {
            assembler.push(std::move(piece));
            while (auto chunk = assembler.next_chunk()) {
                assembled.push_back(std::move(*chunk));
            }
        }
        BOOST_REQUIRE(!assembler.has_partial_chunk());
        BOOST_REQUIRE_EQUAL(assembled.size(), chunks.size());
        for (size_t i = 0; i < chunks.size(); ++i) {
            BOOST_REQUIRE(std::ranges::equal(std::span(assembled[i].get(), assembled[i].size()), std::span(chunks[i].get(), chunks[i].size())));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_stream_chunk_assembler_waits_for_a_whole_chunk) {
    auto schema = make_schema();
    auto chunk = make_chunk(schema, make_records(3, 100));

    stream_chunk_assembler assembler;
    assembler.push(chunk.share(0, ondisk::chunk_header_size - 1));
    BOOST_REQUIRE(!assembler.next_chunk());
    assembler.push(chunk.share(ondisk::chunk_header_size - 1, chunk.size() - ondisk::chunk_header_size));
    BOOST_REQUIRE(!assembler.next_chunk());
    BOOST_REQUIRE(assembler.has_partial_chunk());
    assembler.push(chunk.share(chunk.size() - 1, 1));
    BOOST_REQUIRE(assembler.next_chunk());
    BOOST_REQUIRE(!assembler.has_partial_chunk());
}

SEASTAR_THREAD_TEST_CASE(test_logstor_stream_chunk_assembler_rejects_a_bad_chunk_header) {
    auto schema = make_schema();
    auto chunk = make_chunk(schema, make_records(3, 100));

    auto bad_magic = copy_of(chunk);
    flip_byte(bad_magic, 0);
    stream_chunk_assembler a1;
    a1.push(std::move(bad_magic));
    BOOST_REQUIRE_THROW(a1.next_chunk(), record_stream_error);

    auto too_long = copy_of(chunk);
    auto ch = read_chunk_header(too_long);
    ch.records_size = max_stream_chunk_size;
    write_chunk_header(too_long, ch);
    stream_chunk_assembler a2;
    a2.push(std::move(too_long));
    BOOST_REQUIRE_THROW(a2.next_chunk(), record_stream_error);

    raw_write_buffer mixed(segment_size, segment_kind::mixed);
    mixed.append(log_record_writer(make_log_record(schema, make_records(1, 100)[0], schema->id())));
    mixed.seal(segment_sequence{1}, std::nullopt, ondisk::record_alignment);
    stream_chunk_assembler a3;
    a3.push(temporary_buffer<char>(reinterpret_cast<const char*>(mixed.chunk().data()), mixed.chunk().size()));
    BOOST_REQUIRE_THROW(a3.next_chunk(), record_stream_error);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_packs_records_into_segments) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    auto compaction_guard = env.ls.get_compaction_manager().disable_compaction(group).get();
    target_gates gates;

    const auto records = make_records(150, 2000);
    auto pieces = cut_into_pieces(make_chunks(schema, records, 7), 1001);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), [&] (dht::token) {
        return gates.target(group);
    }).get();
    put_pieces(*sink, pieces);
    sink->close().get();
    sink.reset();
    gates.close();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), segment_starts(schema, records).size());
    size_t live_bytes = 0;
    for (const auto& r : records) {
        require_reads_back(env.ls, schema, group.logstor_index(), r);
        live_bytes += frame_size_of(schema, r);
    }
    BOOST_REQUIRE_EQUAL(group.logstor_segments().live_record_bytes(), live_bytes);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_flushes_when_the_group_changes) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group low(schema, env.ls);
    test_logstor_group high(schema, env.ls, low.logstor_index());
    target_gates gates;

    const auto records = make_records(20, 100);
    auto group_of = [&] (dht::token token) -> test_logstor_group& {
        return token < dht::token::from_int64(0) ? low : high;
    };
    auto pieces = cut_into_pieces(make_chunks(schema, records, 20), 4096);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), [&] (dht::token token) {
        return gates.target(group_of(token));
    }).get();
    put_pieces(*sink, pieces);
    sink->close().get();
    sink.reset();
    gates.close();

    size_t low_bytes = 0;
    size_t high_bytes = 0;
    for (const auto& r : records) {
        require_reads_back(env.ls, schema, low.logstor_index(), r);
        auto& group = group_of(index_key_of(schema, r).token());
        (&group == &low ? low_bytes : high_bytes) += frame_size_of(schema, r);
    }
    BOOST_REQUIRE_GT(low_bytes, 0);
    BOOST_REQUIRE_GT(high_bytes, 0);
    BOOST_REQUIRE_EQUAL(low.logstor_segments().live_record_bytes(), low_bytes);
    BOOST_REQUIRE_EQUAL(high.logstor_segments().live_record_bytes(), high_bytes);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_keeps_a_newer_local_record) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    target_gates gates;

    const test_record local{.pk = "pk0", .value = "local", .timestamp = 2000};
    env.ls.write(make_mutation(schema, local), write_target(&group, {}), db::no_timeout).get();
    env.ls.flush_to_separator().get();
    group.flush_separator().get();
    const auto local_entry = group.logstor_index().get(index_key_of(schema, local));
    BOOST_REQUIRE(local_entry);
    const auto live_bytes_before = group.logstor_segments().live_record_bytes();

    const std::vector<test_record> streamed{
        {.pk = "pk0", .value = "streamed", .timestamp = 1000},
        {.pk = "pk1", .value = "streamed", .timestamp = 1000},
    };
    std::vector<temporary_buffer<char>> chunks;
    chunks.push_back(make_chunk(schema, streamed));

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), [&] (dht::token) {
        return gates.target(group);
    }).get();
    sink->put(chunks).get();
    sink->close().get();
    sink.reset();
    gates.close();

    const auto entry = group.logstor_index().get(index_key_of(schema, local));
    BOOST_REQUIRE(entry);
    BOOST_REQUIRE(entry->location == local_entry->location);
    require_reads_back(env.ls, schema, group.logstor_index(), local);
    require_reads_back(env.ls, schema, group.logstor_index(), streamed[1]);
    // The older record is dead space in the streamed segment.
    BOOST_REQUIRE_EQUAL(group.logstor_segments().live_record_bytes(), live_bytes_before + frame_size_of(schema, streamed[1]));
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_rejects_a_corrupt_chunk) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    target_gates gates;

    const auto records = make_records(10, 100);
    auto chunks = make_chunks(schema, records, 5);
    flip_byte(chunks[1], chunks[1].size() - 1);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), [&] (dht::token) {
        return gates.target(group);
    }).get();
    BOOST_REQUIRE_THROW(sink->put(chunks).get(), record_stream_error);
    sink->abort().get();
    sink.reset();
    gates.close();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 0);
    for (const auto& r : records) {
        BOOST_REQUIRE(!group.logstor_index().get(index_key_of(schema, r)));
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_takes_no_data_after_a_failed_put) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    target_gates gates;

    const auto records = make_records(10, 100);
    auto chunks = make_chunks(schema, records, 5);
    flip_byte(chunks[0], chunks[0].size() - 1);
    std::vector<temporary_buffer<char>> corrupt;
    corrupt.push_back(std::move(chunks[0]));
    std::vector<temporary_buffer<char>> valid;
    valid.push_back(std::move(chunks[1]));

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), [&] (dht::token) {
        return gates.target(group);
    }).get();
    BOOST_REQUIRE_THROW(sink->put(corrupt).get(), record_stream_error);
    BOOST_REQUIRE_THROW(sink->put(valid).get(), record_stream_error);
    BOOST_REQUIRE_THROW(sink->close().get(), record_stream_error);
    sink->abort().get();
    sink.reset();
    gates.close();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 0);
    for (const auto& r : records) {
        BOOST_REQUIRE(!group.logstor_index().get(index_key_of(schema, r)));
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_rejects_a_stream_that_ends_inside_a_chunk) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    target_gates gates;

    const auto records = make_records(10, 100);
    auto chunks = make_chunks(schema, records, 5);
    chunks[1].trim(chunks[1].size() - 8);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), [&] (dht::token) {
        return gates.target(group);
    }).get();
    sink->put(chunks).get();
    // A close() that fails releases the sink, so the test does not call abort().
    BOOST_REQUIRE_THROW(sink->close().get(), record_stream_error);
    sink.reset();
    gates.close();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 0);
    for (const auto& r : records) {
        BOOST_REQUIRE(!group.logstor_index().get(index_key_of(schema, r)));
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_rejects_a_record_of_another_table) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    target_gates gates;

    const auto records = make_records(3, 100);
    std::vector<temporary_buffer<char>> chunks;
    chunks.push_back(make_chunk(schema, records, table_id(utils::UUID_gen::get_time_UUID())));

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), [&] (dht::token) {
        return gates.target(group);
    }).get();
    BOOST_REQUIRE_THROW(sink->put(chunks).get(), record_stream_error);
    sink->abort().get();
    sink.reset();
    gates.close();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_abort_keeps_the_written_segments) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    auto compaction_guard = env.ls.get_compaction_manager().disable_compaction(group).get();
    target_gates gates;

    const auto records = make_records(150, 2000);
    const auto starts = segment_starts(schema, records);
    BOOST_REQUIRE_GE(starts.size(), 3);
    // The records of two segments and a part of the third one.
    const size_t streamed = starts[2] + (starts.size() > 3 ? starts[3] - starts[2] : records.size() - starts[2]) / 2;
    auto chunks = make_chunks(schema, std::span(records).first(streamed), 7);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), [&] (dht::token) {
        return gates.target(group);
    }).get();
    sink->put(chunks).get();
    sink->abort().get();
    sink.reset();
    gates.close();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 2);
    for (size_t i = 0; i < streamed; ++i) {
        if (i < starts[2]) {
            require_reads_back(env.ls, schema, group.logstor_index(), records[i]);
        } else {
            BOOST_REQUIRE(!group.logstor_index().get(index_key_of(schema, records[i])));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_failed_segment_write_releases_the_sink) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    target_gates gates;

    write_buffer_pool pool(write_buffer_pool::config{
        .capacity = 1,
        .buffer_size = segment_size,
        .kind = segment_kind::full,
        .preallocate = 0,
        .max_cached = 1,
    });
    seastar::abort_source as;
    seastar::gate segment_manager_gate;

    const auto records = make_records(150, 2000);
    auto chunks = make_chunks(schema, records, 7);

    record_stream_sink sink(schema->id(), [&] (dht::token) { return gates.target(group); }, pool.allocate(as).get(),
            [] (write_buffer&, logstor_group&) -> future<> {
                return make_exception_future<>(std::runtime_error("injected segment write failure"));
            },
            segment_manager_gate.hold());
    BOOST_REQUIRE_THROW(sink.put(chunks).get(), std::runtime_error);
    sink.abort().get();

    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 0);
    segment_manager_gate.close().get();
    gates.close();
    pool.stop().get();
    for (const auto& r : records) {
        BOOST_REQUIRE(!group.logstor_index().get(index_key_of(schema, r)));
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_failed_close_releases_the_sink) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    target_gates gates;

    write_buffer_pool pool(write_buffer_pool::config{
        .capacity = 1,
        .buffer_size = segment_size,
        .kind = segment_kind::full,
        .preallocate = 0,
        .max_cached = 1,
    });
    seastar::abort_source as;
    seastar::gate segment_manager_gate;

    // The records fit in one segment, so put() writes no segment and the write fails in close().
    const auto records = make_records(10, 100);
    auto chunks = make_chunks(schema, records, 7);

    auto sink = std::make_unique<record_stream_sink>(schema->id(), [&] (dht::token) { return gates.target(group); },
            pool.allocate(as).get(),
            [] (write_buffer&, logstor_group&) -> future<> {
                return make_exception_future<>(std::runtime_error("injected segment write failure"));
            },
            segment_manager_gate.hold());
    sink->put(chunks).get();
    BOOST_REQUIRE_THROW(sink->close().get(), std::runtime_error);
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 0);
    // A close() that fails releases the sink, so the sink is destroyed without abort(), and its
    // destructor must not find an open buffer.
    sink.reset();

    segment_manager_gate.close().get();
    gates.close();
    pool.stop().get();
    for (const auto& r : records) {
        BOOST_REQUIRE(!group.logstor_index().get(index_key_of(schema, r)));
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_stop_waits_for_record_stream_sink) {
    auto schema = make_schema();
    logstor_env env;
    auto& sm = env.ls.get_segment_manager();
    // The stream sends no record, so the sink does not resolve a target.
    auto resolve = [] (dht::token) { return write_target{}; };

    auto sink = sm.make_record_stream_sink(schema->id(), resolve).get();

    auto stop_f = env.ls.stop();
    seastar::sleep(std::chrono::milliseconds(100)).get();
    BOOST_REQUIRE(!stop_f.available());

    // A new sink is refused while the store stops.
    BOOST_REQUIRE_THROW(sm.make_record_stream_sink(schema->id(), resolve).get(), seastar::gate_closed_exception);

    sink->abort().get();
    sink.reset();
    stop_f.get();
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_source_sends_only_live_records) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    auto setup_guard = std::make_optional(env.ls.get_compaction_manager().disable_compaction(group).get());

    const auto first = make_records(20, 100);
    const auto second = make_records(20, 100, 20);
    const auto overwritten = overwrite(std::span(first).first(10), 2000);
    write_segment(env.ls, group, schema, first);
    write_segment(env.ls, group, schema, second);
    write_segment(env.ls, group, schema, overwritten);
    setup_guard.reset();

    auto source = env.ls.get_segment_manager().make_record_stream_source(group).get();
    BOOST_REQUIRE(source);
    const auto chunks = read_all(*source);

    BOOST_REQUIRE_EQUAL(chunks.size(), 3);
    BOOST_REQUIRE(timestamps_in(chunks) == timestamps_of({std::span(first).subspan(10), second, overwritten}));
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_source_is_not_made_for_a_group_without_live_records) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    auto& sm = env.ls.get_segment_manager();

    BOOST_REQUIRE(!sm.make_record_stream_source(group).get());

    const auto records = make_records(10, 100);
    write_segment(env.ls, group, schema, records);
    for (const auto& r : records) {
        BOOST_REQUIRE(group.logstor_index().erase(index_key_of(schema, r), group.logstor_index().get(index_key_of(schema, r))->location));
    }
    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 1);
    BOOST_REQUIRE(!sm.make_record_stream_source(group).get());
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_source_does_not_send_records_overwritten_after_it_was_made) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);

    const auto first = make_records(10, 100);
    const auto second = make_records(10, 100, 10);
    write_segment(env.ls, group, schema, first);
    write_segment(env.ls, group, schema, second);

    auto source = env.ls.get_segment_manager().make_record_stream_source(group).get();
    BOOST_REQUIRE(source);
    // The old records are dead, and the new ones are in a segment that is not in the list of the
    // source. The write path sends the new ones to the receiver.
    write_segment(env.ls, group, schema, overwrite(first, 2000));
    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 3);

    const auto chunks = read_all(*source);
    BOOST_REQUIRE_EQUAL(chunks.size(), 1);
    BOOST_REQUIRE(timestamps_in(chunks) == timestamps_of({second}));
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_source_keeps_compaction_disabled) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    auto& cm = env.ls.get_compaction_manager();
    auto setup_guard = std::make_optional(cm.disable_compaction(group).get());

    const auto first = make_records(10, 100);
    write_segment(env.ls, group, schema, first);
    write_segment(env.ls, group, schema, overwrite(first, 2000));
    write_segment(env.ls, group, schema, make_records(10, 100, 10));
    setup_guard.reset();

    auto source = env.ls.get_segment_manager().make_record_stream_source(group).get();
    BOOST_REQUIRE(source);
    cm.submit(group);
    // Waits for a compaction that runs.
    cm.disable_compaction(group).get();
    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 3);

    source.reset();
    cm.submit(group);
    cm.disable_compaction(group).get();
    BOOST_REQUIRE_LT(group.logstor_segments().segment_count(), 3);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_source_fails_on_a_segment_that_is_not_of_the_group) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    const auto records = make_records(3, 100);

    auto make_source = [&] (temporary_buffer<char> segment) {
        utils::chunked_vector<log_segment_id> segments;
        segments.push_back(log_segment_id{0});
        return record_stream_source(schema->id(), group.logstor_index(), compaction_reenabler(), std::move(segments), segment_size,
                memory_chunk_reader(std::move(segment)));
    };
    auto make_segment = [&] (segment_kind kind, table_id table) {
        raw_write_buffer buf(segment_size, kind);
        for (const auto& r : records) {
            buf.append(log_record_writer(make_log_record(schema, r, table)));
        }
        buf.seal(segment_sequence{1}, table, ondisk::block_alignment);
        return temporary_buffer<char>(reinterpret_cast<const char*>(buf.chunk().data()), buf.chunk().size());
    };

    temporary_buffer<char> empty(ondisk::block_alignment);
    std::fill_n(empty.get_write(), empty.size(), 0);
    BOOST_REQUIRE_THROW(make_source(std::move(empty)).get().get(), record_stream_error);

    BOOST_REQUIRE_THROW(make_source(make_segment(segment_kind::mixed, schema->id())).get().get(), record_stream_error);

    BOOST_REQUIRE_THROW(make_source(make_segment(segment_kind::full, table_id(utils::UUID_gen::get_time_UUID()))).get().get(),
            record_stream_error);

    auto corrupt = make_segment(segment_kind::full, schema->id());
    flip_byte(corrupt, ondisk::chunk_header_size + ondisk::segment_header_size + 10);
    BOOST_REQUIRE_THROW(make_source(std::move(corrupt)).get().get(), record_stream_error);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_stream_round_trip) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group sender(schema, env.ls);
    test_logstor_group receiver(schema, env.ls);
    auto setup_guard = std::make_optional(env.ls.get_compaction_manager().disable_compaction(sender).get());
    target_gates gates;

    // Every segment of the sender is half dead.
    const auto records = make_records(150, 2000);
    std::vector<test_record> live;
    for (size_t i = 0; i < records.size(); i += 30) {
        const auto batch = std::span(records).subspan(i, 30);
        write_segment(env.ls, sender, schema, batch);
        const auto newer = overwrite(batch.first(15), 3000 + i);
        write_segment(env.ls, sender, schema, newer);
        live.insert(live.end(), newer.begin(), newer.end());
        live.insert(live.end(), batch.begin() + 15, batch.end());
    }
    setup_guard.reset();

    auto source = env.ls.get_segment_manager().make_record_stream_source(sender).get();
    BOOST_REQUIRE(source);
    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), [&] (dht::token) {
        return gates.target(receiver);
    }).get();
    auto receiver_compaction_guard = env.ls.get_compaction_manager().disable_compaction(receiver).get();
    while (true) {
        auto buf = source->get().get();
        if (buf.empty()) {
            break;
        }
        sink->put(std::span(&buf, 1)).get();
    }
    sink->close().get();
    sink.reset();
    source.reset();
    gates.close();

    BOOST_REQUIRE_EQUAL(receiver.logstor_segments().live_record_bytes(), sender.logstor_segments().live_record_bytes());
    BOOST_REQUIRE_LT(receiver.logstor_segments().segment_count(), sender.logstor_segments().segment_count());
    for (const auto& r : live) {
        require_reads_back(env.ls, schema, receiver.logstor_index(), r);
    }
}
