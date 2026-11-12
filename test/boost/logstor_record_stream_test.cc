/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// combined_tests has the main function.
#undef SEASTAR_TESTING_MAIN

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <functional>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <vector>

#include <fmt/format.h>

#include <seastar/core/abort_source.hh>
#include <seastar/core/align.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/memory-data-source.hh>
#include <seastar/testing/on_internal_error.hh>
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
#include "test/lib/exception_utils.hh"
#include "test/lib/logstor_test_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/tmpdir.hh"
#include "utils/UUID_gen.hh"
#include "utils/phased_barrier.hh"

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

// Builds a stream chunk as the sender does. The chunk is a chunk of a full segment that is sealed
// at record_alignment.
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

// Rewrites the common part of the chunk header at the start of `chunk` with `change` applied and a
// matching CRC.
void rewrite_chunk_header(temporary_buffer<char>& chunk, std::function<void(ondisk::chunk_header_common&)> change) {
    seastar::simple_memory_input_stream in(chunk.get(), ondisk::chunk_header_common_size);
    auto common = ser::deserialize(in, std::type_identity<ondisk::chunk_header_common>{});
    const auto rest = std::span(chunk.get() + ondisk::chunk_header_common_size, ondisk::chunk_header_size(common.kind) - ondisk::chunk_header_common_size);
    change(common);
    common.crc = ondisk::calculate_chunk_header_crc(common, rest);
    seastar::simple_memory_output_stream out(chunk.get_write(), ondisk::chunk_header_common_size);
    ser::serialize(out, common);
}

// Writes a value_size of 0 into the header of the record frame at `offset` of the chunk at the
// start of `chunk`, and computes the body CRC again, so that the CRCs of the chunk match, as in a
// chunk of a writer with a bug.
void break_record_frame(temporary_buffer<char>& chunk, size_t offset) {
    // value_size is the third field of the frame header, after key_size and flags.
    seastar::simple_memory_output_stream out(chunk.get_write() + offset + 2 * sizeof(uint16_t), sizeof(uint32_t));
    ser::serialize(out, uint32_t(0));
    rewrite_chunk_header(chunk, [&] (ondisk::chunk_header_common& common) {
        common.body_crc = ondisk::calculate_body_crc(std::span(chunk.get() + ondisk::chunk_header_size(common.kind), common.records_size));
    });
}

// Returns the offset of the second record frame in a chunk of a full segment whose first record is
// `first`.
size_t second_frame_offset(schema_ptr schema, const test_record& first) {
    return ondisk::chunk_header_size(segment_kind::full) + align_up(frame_size_of(schema, first), ondisk::record_alignment);
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
    test_table_catalog catalog;
    logstor ls;

    // `configure` changes the segment manager config of make_test_logstor_config().
    explicit logstor_env(noncopyable_function<void(segment_manager_config&)> configure = {})
        : ls([&] {
            auto cfg = make_test_logstor_config(dir.path());
            if (configure) {
                configure(cfg.segment_manager_cfg);
            }
            return cfg;
        }(), cache.shared_tracker, catalog) {
        ls.do_recovery().get();
        ls.start().get();
    }

    ~logstor_env() {
        ls.stop().get();
    }
};

// low_range() and high_range() return the token ranges of two groups that divide the tokens at
// token 0, as the two groups of a split do.
dht::token_range low_range() {
    return dht::token_range::make_ending_with({dht::token::from_int64(0), false});
}

dht::token_range high_range() {
    return dht::token_range::make_starting_with({dht::token::from_int64(0), true});
}

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

// Returns true if the token goes to the lower of two groups, in a test whose records go to two
// groups, as the two groups of a split.
bool is_low(dht::token token) {
    return token < dht::token::from_int64(0);
}

// A sink whose segment writes fail, with a pool and a segment manager gate of its own, so that a
// test can check that the sink gives back what it holds.
struct failing_sink {
    write_buffer_pool pool{write_buffer_pool::config{
        .capacity = record_stream_sink_buffers,
        .buffer_size = segment_size,
        .kind = segment_kind::full,
        .preallocate = 0,
        .max_cached = record_stream_sink_buffers,
    }};
    seastar::abort_source as;
    seastar::gate segment_manager_gate;
    std::unique_ptr<record_stream_sink> sink;

    failing_sink(schema_ptr schema, table_catalog& catalog) {
        sink = std::make_unique<record_stream_sink>(schema->id(), catalog,
                pool.allocate_many(record_stream_sink_buffers, as).get(),
                [] (write_buffer&, logstor_group&) -> future<> {
                    return make_exception_future<>(std::runtime_error("injected segment write failure"));
                },
                segment_manager_gate.hold(), utils::phased_barrier::operation());
    }

    // Destroys the sink and checks that it gave back its buffers and its gate holder.
    void require_released() {
        BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 0);
        sink.reset();
        segment_manager_gate.close().get();
        pool.stop().get();
    }
};

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

// A group with one segment that holds records of both sides of a split at token 0, the two groups
// of the split, and a record stream source of the group, which disables compaction of the group.
struct split_during_stream {
    schema_ptr schema = make_schema();
    logstor_env env;
    test_logstor_group src{schema, env.ls};
    test_logstor_group low{schema, env.ls, src.logstor_index()};
    test_logstor_group high{schema, env.ls, src.logstor_index()};
    std::vector<test_record> records = make_records(20, 100);
    std::unique_ptr<record_stream_source> source;

    split_during_stream() {
        write_segment(env.ls, src, schema, records);
        BOOST_REQUIRE_EQUAL(src.logstor_segments().segment_count(), 1);
        source = env.ls.get_segment_manager().make_record_stream_source(src, src).get();
        BOOST_REQUIRE(source);
    }

    // Starts a split compaction of the group at token 0, and checks that it waits.
    future<> start_split() {
        auto split = env.ls.get_compaction_manager().submit_split_compaction(src,
                [] (dht::token token) -> mutation_writer::token_group_id { return is_low(token) ? 0 : 1; },
                [this] (log_segment_id, dht::token first_token, dht::token) -> logstor_group& {
                    return is_low(first_token) ? static_cast<logstor_group&>(low) : high;
                });
        seastar::sleep(std::chrono::milliseconds(100)).get();
        BOOST_REQUIRE(!split.available());
        BOOST_REQUIRE_EQUAL(src.logstor_segments().segment_count(), 1);
        return split;
    }
};

} // anonymous namespace

BOOST_AUTO_TEST_SUITE(logstor_record_stream_test)

// parse_chunk() gives the segment info of a whole chunk and the header of each record in it, in
// order.
SEASTAR_THREAD_TEST_CASE(test_logstor_parse_chunk_gives_the_records_of_a_chunk) {
    auto schema = make_schema();
    const auto records = make_records(3, 100);

    auto chunk = parse_chunk(make_chunk(schema, records)).get();
    BOOST_REQUIRE(chunk);
    BOOST_REQUIRE(chunk->info().kind() == segment_kind::full);
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

// parse_chunk() rejects a chunk with a bad CRC, a chunk that is shorter than its header says, and a
// chunk that is longer.
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

// The chunk assembler gives back the chunks that the sender sent, when the pieces of the stream cut
// the chunk headers and when one piece holds all the chunks.
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

// The chunk assembler gives no chunk until it has the whole chunk header and the whole chunk body.
SEASTAR_THREAD_TEST_CASE(test_logstor_stream_chunk_assembler_waits_for_a_whole_chunk) {
    auto schema = make_schema();
    auto chunk = make_chunk(schema, make_records(3, 100));

    stream_chunk_assembler assembler;
    assembler.push(chunk.share(0, ondisk::chunk_header_common_size - 1));
    BOOST_REQUIRE(!assembler.next_chunk());
    assembler.push(chunk.share(ondisk::chunk_header_common_size - 1, chunk.size() - ondisk::chunk_header_common_size));
    BOOST_REQUIRE(!assembler.next_chunk());
    BOOST_REQUIRE(assembler.has_partial_chunk());
    assembler.push(chunk.share(chunk.size() - 1, 1));
    BOOST_REQUIRE(assembler.next_chunk());
    BOOST_REQUIRE(!assembler.has_partial_chunk());
}

// The chunk assembler fails the stream on a chunk header with a bad magic, on a chunk that is
// longer than `max_stream_chunk_size`, and on a chunk of a mixed segment.
SEASTAR_THREAD_TEST_CASE(test_logstor_stream_chunk_assembler_rejects_a_bad_chunk_header) {
    auto schema = make_schema();
    auto chunk = make_chunk(schema, make_records(3, 100));

    auto bad_magic = copy_of(chunk);
    flip_byte(bad_magic, 0);
    stream_chunk_assembler a1;
    a1.push(std::move(bad_magic));
    BOOST_REQUIRE_THROW(a1.next_chunk(), record_stream_error);

    auto too_long = copy_of(chunk);
    rewrite_chunk_header(too_long, [] (ondisk::chunk_header_common& common) { common.records_size = max_stream_chunk_size; });
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

// A chunk from a release with another format version or with a flag that this release does not
// define fails the stream with an error that says so.
SEASTAR_THREAD_TEST_CASE(test_logstor_stream_chunk_assembler_rejects_a_chunk_of_another_release) {
    auto schema = make_schema();
    auto chunk = make_chunk(schema, make_records(3, 100));

    auto require_rejected = [&] (std::function<void(ondisk::chunk_header_common&)> change, const sstring& expected) {
        auto changed = copy_of(chunk);
        rewrite_chunk_header(changed, std::move(change));
        stream_chunk_assembler assembler;
        assembler.push(std::move(changed));
        BOOST_REQUIRE_EXCEPTION(assembler.next_chunk(), record_stream_error, exception_predicate::message_contains(expected));
    };
    require_rejected([] (ondisk::chunk_header_common& common) { common.version = ondisk::min_read_version - 1; }, "has a chunk of format version");
    require_rejected([] (ondisk::chunk_header_common& common) { common.version = ondisk::current_version + 1; }, "has a chunk of format version");
    require_rejected([] (ondisk::chunk_header_common& common) { common.flags = uint16_t(~ondisk::known_chunk_flags); }, "has a chunk with flags");
}

// The sink packs the streamed records in order into the fewest full segments, also when the pieces
// of the stream cut the chunks, and every record reads back from the index.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_packs_records_into_segments) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    auto compaction_guard = env.ls.get_compaction_manager().disable_compaction(group).get();
    env.catalog.add(group);

    const auto records = make_records(150, 2000);
    auto pieces = cut_into_pieces(make_chunks(schema, records, 7), 1001);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    put_pieces(*sink, pieces);
    sink->close().get();
    sink.reset();
    group.close_gates().get();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), segment_starts(schema, records).size());
    size_t live_bytes = 0;
    for (const auto& r : records) {
        require_reads_back(env.ls, schema, group.logstor_index(), r);
        live_bytes += frame_size_of(schema, r);
    }
    BOOST_REQUIRE_EQUAL(group.logstor_segments().live_record_bytes(), live_bytes);
}

// The sink packs the records of each group into as few segments as the records of that group alone
// fill, also when the records of two groups come mixed.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_packs_each_group_into_its_own_segments) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group low(schema, env.ls);
    test_logstor_group high(schema, env.ls, low.logstor_index());
    // Compaction would change the segment counts that the test checks.
    auto low_guard = env.ls.get_compaction_manager().disable_compaction(low).get();
    auto high_guard = env.ls.get_compaction_manager().disable_compaction(high).get();
    env.catalog.add(low, low_range());
    env.catalog.add(high, high_range());

    // The records of the two groups come mixed, as from a group that did not split yet.
    const auto records = make_records(400, 1000);
    std::vector<test_record> low_records;
    std::vector<test_record> high_records;
    size_t group_changes = 0;
    for (size_t i = 0; i < records.size(); ++i) {
        const bool low_record = is_low(index_key_of(schema, records[i]).token());
        (low_record ? low_records : high_records).push_back(records[i]);
        if (i > 0 && low_record != is_low(index_key_of(schema, records[i - 1]).token())) {
            ++group_changes;
        }
    }
    const auto low_segments = segment_starts(schema, low_records).size();
    const auto high_segments = segment_starts(schema, high_records).size();
    BOOST_REQUIRE_GT(low_segments, 1);
    BOOST_REQUIRE_GT(high_segments, 1);
    BOOST_REQUIRE_GT(group_changes, low_segments + high_segments);

    auto pieces = cut_into_pieces(make_chunks(schema, records, 20), 4096);
    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    put_pieces(*sink, pieces);
    sink->close().get();
    sink.reset();
    low.close_gates().get();
    high.close_gates().get();

    BOOST_REQUIRE_EQUAL(low.logstor_segments().segment_count(), low_segments);
    BOOST_REQUIRE_EQUAL(high.logstor_segments().segment_count(), high_segments);
    size_t low_bytes = 0;
    size_t high_bytes = 0;
    for (const auto& r : records) {
        require_reads_back(env.ls, schema, low.logstor_index(), r);
        (is_low(index_key_of(schema, r).token()) ? low_bytes : high_bytes) += frame_size_of(schema, r);
    }
    BOOST_REQUIRE_EQUAL(low.logstor_segments().live_record_bytes(), low_bytes);
    BOOST_REQUIRE_EQUAL(high.logstor_segments().live_record_bytes(), high_bytes);
}

// The records first go to the main group of a storage group, and then to the two groups of its
// split, as when the storage group enters split mode during the stream.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_writes_out_a_group_that_the_catalog_stops_returning) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group main(schema, env.ls);
    test_logstor_group low(schema, env.ls, main.logstor_index());
    test_logstor_group high(schema, env.ls, main.logstor_index());
    env.catalog.add(main);

    const auto records = make_records(60, 100);
    const auto before_split = std::span(records).first(10);
    const auto after_split = std::span(records).subspan(10);
    BOOST_REQUIRE(std::ranges::any_of(after_split, [&] (const test_record& r) { return is_low(index_key_of(schema, r).token()); }));
    BOOST_REQUIRE(!std::ranges::all_of(after_split, [&] (const test_record& r) { return is_low(index_key_of(schema, r).token()); }));

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    auto first_chunks = make_chunks(schema, before_split, 7);
    sink->put(first_chunks).get();
    BOOST_REQUIRE_GT(main.non_empty_gate().get_count(), 0);

    env.catalog.remove(main);
    env.catalog.add(low, low_range());
    env.catalog.add(high, high_range());
    auto rest_chunks = make_chunks(schema, after_split, 7);
    sink->put(rest_chunks).get();
    // Records of both new groups arrived, so the records of the main group are in a segment of
    // their own, and the sink holds nothing that keeps the main group from being empty.
    BOOST_REQUIRE_EQUAL(main.non_empty_gate().get_count(), 0);
    BOOST_REQUIRE_EQUAL(main.logstor_segments().segment_count(), 1);

    sink->close().get();
    sink.reset();
    main.close_gates().get();
    low.close_gates().get();
    high.close_gates().get();

    BOOST_REQUIRE_EQUAL(low.logstor_segments().segment_count(), 1);
    BOOST_REQUIRE_EQUAL(high.logstor_segments().segment_count(), 1);
    for (const auto& r : records) {
        require_reads_back(env.ls, schema, main.logstor_index(), r);
    }
}

// A streamed record that is older than the local record of the same key does not replace the local
// record in the index.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_keeps_a_newer_local_record) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    env.catalog.add(group);

    const test_record local{.pk = "pk0", .value = "local", .timestamp = 2000};
    env.ls.write(make_mutation(schema, local), write_target(&group, {}), db::no_timeout).get();
    env.ls.flush_to_separator().get();
    group.flush_separator().get();
    const auto local_entry = lookup_location(group.logstor_index(), index_key_of(schema, local), *schema);
    BOOST_REQUIRE(local_entry);
    const auto live_bytes_before = group.logstor_segments().live_record_bytes();

    const std::vector<test_record> streamed{
        {.pk = "pk0", .value = "streamed", .timestamp = 1000},
        {.pk = "pk1", .value = "streamed", .timestamp = 1000},
    };
    std::vector<temporary_buffer<char>> chunks;
    chunks.push_back(make_chunk(schema, streamed));

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    sink->put(chunks).get();
    sink->close().get();
    sink.reset();
    group.close_gates().get();

    const auto entry = lookup_location(group.logstor_index(), index_key_of(schema, local), *schema);
    BOOST_REQUIRE(entry);
    BOOST_REQUIRE(*entry == *local_entry);
    require_reads_back(env.ls, schema, group.logstor_index(), local);
    require_reads_back(env.ls, schema, group.logstor_index(), streamed[1]);
    // The older record is dead space in the streamed segment.
    BOOST_REQUIRE_EQUAL(group.logstor_segments().live_record_bytes(), live_bytes_before + frame_size_of(schema, streamed[1]));
}

// A corrupt chunk fails put(), and after abort() the group has no segment and no record of the
// stream.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_rejects_a_corrupt_chunk) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    env.catalog.add(group);

    const auto records = make_records(10, 100);
    auto chunks = make_chunks(schema, records, 5);
    flip_byte(chunks[1], chunks[1].size() - 1);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    BOOST_REQUIRE_THROW(sink->put(chunks).get(), record_stream_error);
    sink->abort().get();
    sink.reset();
    group.close_gates().get();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 0);
    for (const auto& r : records) {
        BOOST_REQUIRE(!lookup_location(group.logstor_index(), index_key_of(schema, r), *schema));
    }
}

// A chunk whose CRCs match but whose second record frame does not parse fails put(), and after
// abort() the group has no segment and no record of the chunk. The sink must not write the first
// record of the chunk and lose the others without notice.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_rejects_a_chunk_with_a_bad_record_frame) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    env.catalog.add(group);

    const auto records = make_records(3, 100);
    std::vector<temporary_buffer<char>> chunks;
    chunks.push_back(make_chunk(schema, records));
    break_record_frame(chunks[0], second_frame_offset(schema, records[0]));

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    BOOST_REQUIRE_EXCEPTION(sink->put(chunks).get(), std::runtime_error, exception_predicate::message_contains("not a valid record frame"));
    sink->abort().get();
    sink.reset();
    group.close_gates().get();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 0);
    for (const auto& r : records) {
        BOOST_REQUIRE(!lookup_location(group.logstor_index(), index_key_of(schema, r), *schema));
    }
}

// After a put() fails, the sink refuses the next put() and close(), so a valid chunk after a
// corrupt chunk adds no record to the group.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_takes_no_data_after_a_failed_put) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    env.catalog.add(group);

    const auto records = make_records(10, 100);
    auto chunks = make_chunks(schema, records, 5);
    flip_byte(chunks[0], chunks[0].size() - 1);
    std::vector<temporary_buffer<char>> corrupt;
    corrupt.push_back(std::move(chunks[0]));
    std::vector<temporary_buffer<char>> valid;
    valid.push_back(std::move(chunks[1]));

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    BOOST_REQUIRE_THROW(sink->put(corrupt).get(), record_stream_error);
    BOOST_REQUIRE_THROW(sink->put(valid).get(), record_stream_error);
    BOOST_REQUIRE_THROW(sink->close().get(), record_stream_error);
    sink->abort().get();
    sink.reset();
    group.close_gates().get();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 0);
    for (const auto& r : records) {
        BOOST_REQUIRE(!lookup_location(group.logstor_index(), index_key_of(schema, r), *schema));
    }
}

// A put() or a close() after close() is a bug of the caller. The sink reports the bug as an internal
// error and does not use the buffers that it gave back to the pool.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_refuses_a_call_after_close) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    env.catalog.add(group);

    auto chunks = make_chunks(schema, make_records(10, 100), 5);
    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    sink->close().get();

    seastar::testing::scoped_no_abort_on_internal_error no_abort;
    const auto used_after_close = exception_predicate::message_contains("was used after close() or abort()");
    BOOST_REQUIRE_EXCEPTION(sink->put(chunks).get(), std::runtime_error, used_after_close);
    BOOST_REQUIRE_EXCEPTION(sink->close().get(), std::runtime_error, used_after_close);
    sink.reset();
    group.close_gates().get();
}

// A stream that ends inside a chunk fails close(), and the group gets no record of the stream.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_rejects_a_stream_that_ends_inside_a_chunk) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    env.catalog.add(group);

    const auto records = make_records(10, 100);
    auto chunks = make_chunks(schema, records, 5);
    chunks[1].trim(chunks[1].size() - 8);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    sink->put(chunks).get();
    // A close() that fails releases the sink, so the test does not call abort().
    BOOST_REQUIRE_THROW(sink->close().get(), record_stream_error);
    sink.reset();
    group.close_gates().get();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 0);
    for (const auto& r : records) {
        BOOST_REQUIRE(!lookup_location(group.logstor_index(), index_key_of(schema, r), *schema));
    }
}

// A chunk of another table fails put(), and the sink writes no segment.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_rejects_a_record_of_another_table) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    env.catalog.add(group);

    const auto records = make_records(3, 100);
    std::vector<temporary_buffer<char>> chunks;
    chunks.push_back(make_chunk(schema, records, table_id(utils::UUID_gen::get_time_UUID())));

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    BOOST_REQUIRE_THROW(sink->put(chunks).get(), record_stream_error);
    sink->abort().get();
    sink.reset();
    group.close_gates().get();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 0);
}

// A record whose token has no compaction group in the catalog fails put() with an error of the
// stream, and abort() gives back what the sink holds.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_fails_on_a_token_without_a_group) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group low(schema, env.ls);
    env.catalog.add(low, low_range());

    // The records fit in the buffer of the sink, so the sink writes no segment before the failure.
    const auto records = make_records(20, 100);
    BOOST_REQUIRE(std::ranges::any_of(records, [&] (const test_record& r) {
        return !low_range().contains(index_key_of(schema, r).token(), dht::token_comparator());
    }));
    auto chunks = make_chunks(schema, records, 7);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    BOOST_REQUIRE_THROW(sink->put(chunks).get(), record_stream_error);
    sink->abort().get();
    sink.reset();
    low.close_gates().get();

    BOOST_REQUIRE_EQUAL(low.logstor_segments().segment_count(), 0);
    BOOST_REQUIRE_EQUAL(low.logstor_index().get_usage().key_count, 0);
}

// The stop of a compaction group waits for the holders that the sink has for the buffer of the
// group. The next record of the group then fails put(), and after abort() the stop goes on.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_fails_when_its_group_stops) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    env.catalog.add(group);

    // The records fit in the buffer of the sink, so the sink keeps the holders of the group.
    const auto records = make_records(20, 100);
    auto first_chunks = make_chunks(schema, std::span(records).first(10), 7);
    auto rest_chunks = make_chunks(schema, std::span(records).subspan(10), 7);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    sink->put(first_chunks).get();

    auto stop_f = group.close_gates();
    seastar::sleep(std::chrono::milliseconds(100)).get();
    BOOST_REQUIRE(!stop_f.available());

    BOOST_REQUIRE_EXCEPTION(sink->put(rest_chunks).get(), std::runtime_error,
            exception_predicate::message_contains("because its compaction group stops"));
    sink->abort().get();
    sink.reset();
    stop_f.get();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 0);
}

// A record of a table that the catalog does not have fails put(), and abort() gives back what the
// sink holds.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_fails_on_a_table_that_does_not_exist) {
    auto schema = make_schema();
    logstor_env env;

    const auto records = make_records(3, 100);
    auto chunks = make_chunks(schema, records, 7);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    BOOST_REQUIRE_EXCEPTION(sink->put(chunks).get(), std::runtime_error,
            exception_predicate::message_contains("because the table does not exist"));
    sink->abort().get();
}

// After abort(), the group keeps the segments that the sink wrote, and the records in the buffer of
// the sink are not indexed.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_abort_keeps_the_written_segments) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    auto compaction_guard = env.ls.get_compaction_manager().disable_compaction(group).get();
    env.catalog.add(group);

    const auto records = make_records(150, 2000);
    const auto starts = segment_starts(schema, records);
    BOOST_REQUIRE_GE(starts.size(), 3);
    // The records of two segments and a part of the third one.
    const size_t streamed = starts[2] + (starts.size() > 3 ? starts[3] - starts[2] : records.size() - starts[2]) / 2;
    auto chunks = make_chunks(schema, std::span(records).first(streamed), 7);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    sink->put(chunks).get();
    sink->abort().get();
    sink.reset();
    group.close_gates().get();

    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 2);
    for (size_t i = 0; i < streamed; ++i) {
        if (i < starts[2]) {
            require_reads_back(env.ls, schema, group.logstor_index(), records[i]);
        } else {
            BOOST_REQUIRE(!lookup_location(group.logstor_index(), index_key_of(schema, records[i]), *schema));
        }
    }
}

// After abort(), the records in the buffers of both groups are not indexed, and neither group gets
// a segment.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_abort_drops_the_records_of_both_groups) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group low(schema, env.ls);
    test_logstor_group high(schema, env.ls, low.logstor_index());
    env.catalog.add(low, low_range());
    env.catalog.add(high, high_range());

    // The records fit in a buffer of each group, so the sink writes no segment before abort().
    const auto records = make_records(40, 100);
    auto chunks = make_chunks(schema, records, 7);

    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
    sink->put(chunks).get();
    sink->abort().get();
    sink.reset();
    low.close_gates().get();
    high.close_gates().get();

    BOOST_REQUIRE_EQUAL(low.logstor_segments().segment_count(), 0);
    BOOST_REQUIRE_EQUAL(high.logstor_segments().segment_count(), 0);
    for (const auto& r : records) {
        BOOST_REQUIRE(!lookup_location(low.logstor_index(), index_key_of(schema, r), *schema));
    }
}

// When a segment write fails in put(), the sink indexes no record, and abort() gives back the
// buffer and the gate holder that the sink holds.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_failed_segment_write_releases_the_sink) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    env.catalog.add(group);

    const auto records = make_records(150, 2000);
    auto chunks = make_chunks(schema, records, 7);

    failing_sink fs(schema, env.catalog);
    BOOST_REQUIRE_THROW(fs.sink->put(chunks).get(), std::runtime_error);
    fs.sink->abort().get();

    fs.require_released();
    group.close_gates().get();
    for (const auto& r : records) {
        BOOST_REQUIRE(!lookup_location(group.logstor_index(), index_key_of(schema, r), *schema));
    }
}

// When a segment write fails in close(), close() gives back the buffer and the gate holder that the
// sink holds, so the sink needs no abort().
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_failed_close_releases_the_sink) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group low(schema, env.ls);
    test_logstor_group high(schema, env.ls, low.logstor_index());
    env.catalog.add(low, low_range());
    env.catalog.add(high, high_range());

    // The records fit in a buffer of each group, so put() writes no segment, and the write of the
    // first group fails in close() while the buffer of the second group holds records.
    const auto records = make_records(40, 100);
    auto chunks = make_chunks(schema, records, 7);

    failing_sink fs(schema, env.catalog);
    fs.sink->put(chunks).get();
    BOOST_REQUIRE_THROW(fs.sink->close().get(), std::runtime_error);
    // A close() that fails releases the sink, so the sink is destroyed without abort(), and its
    // destructor must not find an open buffer.
    fs.require_released();
    low.close_gates().get();
    high.close_gates().get();
    for (const auto& r : records) {
        BOOST_REQUIRE(!lookup_location(low.logstor_index(), index_key_of(schema, r), *schema));
    }
}

// The stop of logstor waits until an open sink ends, and refuses a new sink while it waits.
SEASTAR_THREAD_TEST_CASE(test_logstor_stop_waits_for_record_stream_sink) {
    auto schema = make_schema();
    logstor_env env;
    auto& sm = env.ls.get_segment_manager();

    auto sink = sm.make_record_stream_sink(schema->id(), {}).get();

    auto stop_f = env.ls.stop();
    seastar::sleep(std::chrono::milliseconds(100)).get();
    BOOST_REQUIRE(!stop_f.available());

    // A new sink is refused while the store stops.
    BOOST_REQUIRE_THROW(sm.make_record_stream_sink(schema->id(), {}).get(), seastar::gate_closed_exception);

    sink->abort().get();
    sink.reset();
    stop_f.get();
}

// When `max_record_stream_sinks` sinks are open, a new sink waits until an open sink closes and
// gives back its buffers.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_sink_waits_for_the_buffers_of_another_sink) {
    auto schema = make_schema();
    logstor_env env([] (segment_manager_config& cfg) {
        cfg.max_record_stream_sinks = 1;
    });
    auto& sm = env.ls.get_segment_manager();

    auto first = sm.make_record_stream_sink(schema->id(), {}).get();
    auto second_f = sm.make_record_stream_sink(schema->id(), {});
    seastar::sleep(std::chrono::milliseconds(100)).get();
    BOOST_REQUIRE(!second_f.available());

    first->close().get();
    first.reset();
    auto second = second_f.get();
    second->close().get();
}

// The source sends only the live records of the group, one chunk for each segment that holds live
// records.
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

    auto source = env.ls.get_segment_manager().make_record_stream_source(group, group).get();
    BOOST_REQUIRE(source);
    const auto chunks = read_all(*source);

    BOOST_REQUIRE_EQUAL(chunks.size(), 3);
    BOOST_REQUIRE(timestamps_in(chunks) == timestamps_of({std::span(first).subspan(10), second, overwritten}));
}

// The segment manager makes no source for a group that has no segment, or whose segments hold no
// live record.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_source_is_not_made_for_a_group_without_live_records) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    auto& sm = env.ls.get_segment_manager();

    BOOST_REQUIRE(!sm.make_record_stream_source(group, group).get());

    const auto records = make_records(10, 100);
    write_segment(env.ls, group, schema, records);
    for (const auto& r : records) {
        BOOST_REQUIRE(group.logstor_index().erase(index_key_of(schema, r), *lookup_location(group.logstor_index(), index_key_of(schema, r), *schema)));
    }
    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 1);
    BOOST_REQUIRE(!sm.make_record_stream_source(group, group).get());
}

// The source sends only the records that are live when it reads them, and it does not read a
// segment that the group gets after the source was made.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_source_does_not_send_records_overwritten_after_it_was_made) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);

    const auto first = make_records(10, 100);
    const auto second = make_records(10, 100, 10);
    write_segment(env.ls, group, schema, first);
    write_segment(env.ls, group, schema, second);

    auto source = env.ls.get_segment_manager().make_record_stream_source(group, group).get();
    BOOST_REQUIRE(source);
    // The old records are dead, and the new ones are in a segment that is not in the list of the
    // source. The write path sends the new ones to the receiver.
    write_segment(env.ls, group, schema, overwrite(first, 2000));
    BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), 3);

    const auto chunks = read_all(*source);
    BOOST_REQUIRE_EQUAL(chunks.size(), 1);
    BOOST_REQUIRE(timestamps_in(chunks) == timestamps_of({second}));
}

// Compaction does not change the segments of the group while a source of the group exists, and
// compaction runs again after the source is destroyed.
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

    auto source = env.ls.get_segment_manager().make_record_stream_source(group, group).get();
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

// A tablet merge can move the segments of a group to its merge target while a source of the group
// exists. Compaction of the merge target does not move the records of these segments before the
// source sends them, and compaction of the merge target runs again after the source is destroyed.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_source_keeps_compaction_of_the_merge_target_disabled) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    test_logstor_group merge_target(schema, env.ls, group.logstor_index());
    auto& cm = env.ls.get_compaction_manager();
    auto setup_guard = std::make_optional(cm.disable_compaction(group).get());

    const auto first = make_records(10, 100);
    const auto overwritten = overwrite(first, 2000);
    const auto second = make_records(10, 100, 10);
    write_segment(env.ls, group, schema, first);
    write_segment(env.ls, group, schema, overwritten);
    write_segment(env.ls, group, schema, second);
    setup_guard.reset();

    auto source = env.ls.get_segment_manager().make_record_stream_source(group, merge_target).get();
    BOOST_REQUIRE(source);
    merge_target.logstor_segments().merge(group.logstor_segments()).get();
    cm.submit(merge_target);
    // Waits for a compaction that runs.
    cm.disable_compaction(merge_target).get();
    BOOST_REQUIRE_EQUAL(merge_target.logstor_segments().segment_count(), 3);

    const auto chunks = read_all(*source);
    BOOST_REQUIRE(timestamps_in(chunks) == timestamps_of({overwritten, second}));

    source.reset();
    cm.submit(merge_target);
    cm.disable_compaction(merge_target).get();
    BOOST_REQUIRE_LT(merge_target.logstor_segments().segment_count(), 3);
}

// The source fails the stream on a segment that is not a full segment of the table: an empty
// segment, a mixed segment, a segment of another table, a corrupt segment, and a segment whose CRCs
// match but whose second record frame does not parse.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_source_fails_on_a_segment_that_is_not_of_the_group) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group group(schema, env.ls);
    const auto records = make_records(3, 100);

    auto make_source = [&] (temporary_buffer<char> segment) {
        utils::chunked_vector<log_segment_id> segments;
        segments.push_back(log_segment_id{0});
        return record_stream_source(schema->id(), group.logstor_index(), compaction_reenabler(), compaction_reenabler(),
                std::move(segments), segment_size, memory_chunk_reader(std::move(segment)));
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
    flip_byte(corrupt, ondisk::chunk_header_size(segment_kind::full) + 10);
    BOOST_REQUIRE_THROW(make_source(std::move(corrupt)).get().get(), record_stream_error);

    auto bad_frame = make_segment(segment_kind::full, schema->id());
    break_record_frame(bad_frame, second_frame_offset(schema, records[0]));
    BOOST_REQUIRE_EXCEPTION(make_source(std::move(bad_frame)).get().get(), std::runtime_error,
            exception_predicate::message_contains("not a valid record frame"));
}

// A source and a sink move the live records of a group whose segments are half dead into fewer
// segments of the receiving group, with the same live record bytes.
SEASTAR_THREAD_TEST_CASE(test_logstor_record_stream_round_trip) {
    auto schema = make_schema();
    logstor_env env;
    test_logstor_group sender(schema, env.ls);
    test_logstor_group receiver(schema, env.ls);
    auto setup_guard = std::make_optional(env.ls.get_compaction_manager().disable_compaction(sender).get());
    env.catalog.add(receiver);

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

    auto source = env.ls.get_segment_manager().make_record_stream_source(sender, sender).get();
    BOOST_REQUIRE(source);
    auto sink = env.ls.get_segment_manager().make_record_stream_sink(schema->id(), {}).get();
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
    receiver.close_gates().get();

    BOOST_REQUIRE_EQUAL(receiver.logstor_segments().live_record_bytes(), sender.logstor_segments().live_record_bytes());
    BOOST_REQUIRE_LT(receiver.logstor_segments().segment_count(), sender.logstor_segments().segment_count());
    for (const auto& r : live) {
        require_reads_back(env.ls, schema, receiver.logstor_index(), r);
    }
}

// A split compaction of a group waits until the record stream of the group ends, and then moves the
// records of the group into the two groups of the split.
SEASTAR_THREAD_TEST_CASE(test_logstor_split_compaction_waits_for_a_record_stream) {
    split_during_stream t;
    auto split = t.start_split();

    // The stream reads its segment while the split waits.
    BOOST_REQUIRE(timestamps_in(read_all(*t.source)) == timestamps_of({t.records}));
    t.source.reset();
    split.get();

    BOOST_REQUIRE_EQUAL(t.src.logstor_segments().segment_count(), 0);
    BOOST_REQUIRE_GT(t.low.logstor_segments().segment_count(), 0);
    BOOST_REQUIRE_GT(t.high.logstor_segments().segment_count(), 0);
    for (const auto& r : t.records) {
        require_reads_back(t.env.ls, t.schema, t.src.logstor_index(), r);
    }
}

BOOST_AUTO_TEST_SUITE_END()
