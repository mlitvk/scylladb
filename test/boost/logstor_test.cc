/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <chrono>
#include <array>
#include <cmath>
#include <deque>
#include <functional>
#include <stdexcept>
#include <map>
#include <span>
#include <vector>
#include <fmt/format.h>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/format.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/memory-data-source.hh>
#include <seastar/util/defer.hh>

#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/record_format.hh"
#include "replica/logstor/write_buffer.hh"
#include <seastar/testing/on_internal_error.hh>
#include <seastar/testing/thread_test_case.hh>

#include "replica/logstor/segment_io.hh"
#include "schema/schema_builder.hh"
#include <seastar/core/simple-stream.hh>
#include "test/lib/logstor_test_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/tmpdir.hh"
#include "utils/disk-error-handler.hh"
#include "utils/error_injection.hh"
#include "utils/exceptions.hh"

using namespace replica::logstor;
using namespace tests::logstor;

namespace {

schema_ptr make_kv_schema() {
    return schema_builder(1, "ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();
}

log_record make_log_record(const mutation& m, api::timestamp_type ts) {
    row_value_encoder encoder;
    return log_record {
        .header = {
            .key = primary_index_key{m.decorated_key()},
            .timestamp = ts,
            .table = m.schema()->id(),
        },
        .value = row_value{bytes(encoder.encode(*m.schema(), m.partition(), ts))},
    };
}

mutation to_mutation(schema_ptr schema, const log_record& record) {
    column_translation_cache translations;
    mutation m(schema, record.header.key.dk);
    read_row_value_into(m.partition(), *schema, record.value.view(), record.header.timestamp, translations);
    return m;
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

log_record make_log_record(schema_ptr schema, sstring pk, sstring value, api::timestamp_type ts = api::min_timestamp) {
    auto m = make_kv_mutation(schema, std::move(pk), std::move(value), ts);
    return make_log_record(m, ts);
}

temporary_buffer<char> make_serialized_buffer_copy(const raw_write_buffer& wb) {
    temporary_buffer<char> buf(wb.serialized_size());
    std::copy_n(wb.data(), wb.serialized_size(), buf.get_write());
    return buf;
}

temporary_buffer<char> make_serialized_buffer_copy(const write_buffer& wb) {
    temporary_buffer<char> buf(wb.serialized_size());
    std::copy_n(wb.data(), wb.serialized_size(), buf.get_write());
    return buf;
}

temporary_buffer<char> concat_serialized_buffers(std::initializer_list<const temporary_buffer<char>*> bufs) {
    size_t total_size = 0;
    for (const auto* buf : bufs) {
        total_size += buf->size();
    }

    temporary_buffer<char> out(total_size);
    size_t offset = 0;
    for (const auto* buf : bufs) {
        std::copy_n(buf->get(), buf->size(), out.get_write() + offset);
        offset += buf->size();
    }
    return out;
}

log_record read_record_at_location(const temporary_buffer<char>& segment, log_location loc) {
    BOOST_REQUIRE_EQUAL(loc.offset + loc.size <= segment.size(), true);

    temporary_buffer<char> buf(loc.size);
    std::copy_n(segment.get() + loc.offset, loc.size, buf.get_write());
    return deserialize_log_record(simple_memory_input_stream(buf.begin(), buf.size()));
}

void flip_byte(temporary_buffer<char>& buf, size_t offset) {
    buf.get_write()[offset] ^= char(0x1);
}

std::optional<segment_header> read_segment_header_from_bytes(const temporary_buffer<char>& buf) {
    temporary_buffer<char> copy(buf.size());
    std::copy_n(buf.get(), buf.size(), copy.get_write());
    auto in = seastar::util::as_input_stream(std::move(copy));
    auto header = read_segment_header(in).get();
    in.close().get();
    return header;
}

temporary_buffer<char> slice_buffer(const temporary_buffer<char>& buf, size_t offset, size_t size) {
    temporary_buffer<char> out(size);
    std::copy_n(buf.get() + offset, size, out.get_write());
    return out;
}

ondisk::buffer_header read_buffer_header(const temporary_buffer<char>& buf) {
    seastar::simple_memory_input_stream in(buf.get(), buf.size());
    return ser::deserialize(in, std::type_identity<ondisk::buffer_header>{});
}

struct rewritten_stream_result {
    temporary_buffer<char> data;
    size_t write_count{0};
};

rewritten_stream_result rewrite_streamed_segment(log_segment_id segment_id, segment_sequence seq_num, std::span<temporary_buffer<char>> chunks) {
    std::vector<char> written;
    size_t write_count = 0;

    streamed_segment_rewriter rewriter(segment_id, seq_num, [&written, &write_count] (bytes_view data) {
        auto* ptr = reinterpret_cast<const char*>(data.data());
        written.insert(written.end(), ptr, ptr + data.size());
        ++write_count;
        return make_ready_future<>();
    });

    rewriter.put(chunks).get();
    rewriter.close().get();

    temporary_buffer<char> out(written.size());
    std::copy(written.begin(), written.end(), out.get_write());
    return rewritten_stream_result{.data = std::move(out), .write_count = write_count};
}

struct scanned_record {
    log_location location;
    log_record record;
};

std::vector<scanned_record> scan_buffer_records(const temporary_buffer<char>& buf, log_segment_id segment_id) {
    temporary_buffer<char> copy(buf.size());
    std::copy_n(buf.get(), buf.size(), copy.get_write());
    auto in = seastar::util::as_input_stream(std::move(copy));
    std::vector<scanned_record> records;

    scan_segment(in, segment_id, buf.size(),
        [] (const segment_header&) {
            return make_ready_future<>();
        },
        [] (log_location, const log_record_header&) {
            return want_data::yes;
        },
        [&records] (log_location loc, log_record rec) {
            records.push_back(scanned_record{.location = loc, .record = std::move(rec)});
            return make_ready_future<>();
        }).get();
    in.close().get();

    return records;
}

void assert_log_record_matches(schema_ptr schema, const log_record& actual, const log_record& expected) {
    BOOST_REQUIRE_EQUAL(actual.header.timestamp, expected.header.timestamp);
    BOOST_REQUIRE_EQUAL(actual.header.table, expected.header.table);
    assert_that(to_mutation(schema, actual)).is_equal_to(to_mutation(schema, expected));
}

db::timeout_clock::time_point test_timeout() {
    return db::timeout_clock::now() + std::chrono::minutes(1);
}

buffered_writer_config make_buffered_writer_config(size_t buffer_size, size_t ring_size, size_t max_queued_write_bytes = 0, std::chrono::milliseconds sync_period = std::chrono::milliseconds(0)) {
    return buffered_writer_config{
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
        auto next_writer = log_record_writer(next_record);
        raw_write_buffer wb(buffer_size, segment_kind::mixed);

        BOOST_REQUIRE(wb.can_fit(next_writer));
        wb.append(next_writer);
        if (!wb.can_fit(next_writer)) {
            return next_value;
        }

        value = std::move(next_value);
    }
}

log_record make_buffered_writer_record(schema_ptr schema, size_t idx, const sstring& value, api::timestamp_type ts) {
    return make_log_record(schema, sstring(fmt::format("pk{:04}", idx)), value, ts);
}

log_location wait_for_persisted(future<log_location_with_holder>& fut) {
    auto [loc, op] = fut.get();
    return loc;
}

struct test_flush_controller {
    struct flushed_buffer {
        temporary_buffer<char> data;
        log_location base_location;
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

    future<> operator()(write_buffer& wb) {
        const auto flush_idx = started_count++;
        flush_started.signal(1);

        if (pause_flushes) {
            co_await flush_release.wait(1);
        }

        if (fail_flush_index && *fail_flush_index == flush_idx) {
            throw std::runtime_error("injected flush failure");
        }

        wb.seal(segment_sequence{next_sequence++}, std::nullopt, ondisk::block_alignment);
        auto base_location = log_location{
            .segment = log_segment_id{next_segment_id++},
            .offset = 0,
            .size = static_cast<uint32_t>(wb.serialized_size()),
        };
        flushed_buffers.push_back(flushed_buffer{
            .data = make_serialized_buffer_copy(wb),
            .base_location = base_location,
            .record_count = wb.record_count(),
        });
        co_await wb.complete_writes(base_location);
    }

    void wait_for_flush_starts(size_t target_count) {
        while (started_count < target_count) {
            flush_started.wait().get();
        }
    }

    void release_one_flush() {
        flush_release.signal(1);
    }

    const temporary_buffer<char>& buffer_for_segment(log_segment_id segment_id) const {
        for (const auto& flushed : flushed_buffers) {
            if (flushed.base_location.segment == segment_id) {
                return flushed.data;
            }
        }
        throw std::runtime_error(fmt::format("Missing flushed segment {}", segment_id));
    }

    std::vector<scanned_record> all_records() const {
        std::vector<scanned_record> records;
        for (const auto& flushed : flushed_buffers) {
            auto buffer_records = scan_buffer_records(flushed.data, flushed.base_location.segment);
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

std::set<log_segment_id> snapshot_segment_ids(const utils::chunked_vector<segment_snapshot>& snapshot) {
    std::set<log_segment_id> ids;
    for (const auto& seg : snapshot) {
        ids.insert(seg.segment_id);
    }
    return ids;
}

// Writes all the mutations and flushes the group's separator once, so that they all end up in a
// single segment of the group.
void write_and_flush_segment(logstor& ls, test_logstor_group& cg, std::span<const mutation> ms) {
    for (const auto& m : ms) {
        ls.write(m, write_target(&cg, {}), db::no_timeout).get();
    }
    ls.flush_to_separator().get();
    cg.flush_separator().get();
}

void write_and_flush_segment(logstor& ls, test_logstor_group& cg, const mutation& m) {
    write_and_flush_segment(ls, cg, std::span(&m, 1));
}

// Which side of a split a key falls on is decided by its token, so a split test needs its keys in
// token order. Returns `count` distinct keys, sorted by token.
std::vector<sstring> make_token_ordered_keys(schema_ptr schema, size_t count) {
    auto token_of = [&] (const sstring& pk) {
        return dht::decorate_key(*schema, partition_key::from_single_value(*schema, serialized(pk))).token();
    };
    std::vector<sstring> keys;
    for (size_t i = 0; i < count; ++i) {
        keys.push_back(seastar::format("pk{}", i));
    }
    std::ranges::sort(keys, std::less<>{}, token_of);
    return keys;
}

// Scans every segment of the snapshot and counts the records it holds by their timestamp, which is
// what the tests give each record version to tell them apart.
std::map<api::timestamp_type, size_t> count_records_by_timestamp(logstor& ls, utils::chunked_vector<segment_snapshot>& snapshot) {
    std::map<api::timestamp_type, size_t> counts;
    const auto segment_size = ls.get_segment_manager().get_segment_size();
    for (auto& snap : snapshot) {
        auto in = snap.source(seastar::file_input_stream_options{
            .buffer_size = std::min<size_t>(segment_size, 128 * 1024),
            .read_ahead = 1,
        }).get();
        scan_segment(in, snap.segment_id, segment_size,
            [] (const segment_header&) { return make_ready_future<>(); },
            [&counts] (log_location, const log_record_header& rh) {
                counts[rh.timestamp]++;
                return want_data::no;
            },
            [] (log_location, log_record) { return make_ready_future<>(); }
        ).get();
        in.close().get();
    }
    return counts;
}

write_buffer_pool::config make_test_write_buffer_pool_config(size_t capacity, size_t max_cached, size_t preallocate = 0) {
    return write_buffer_pool::config{
        .capacity = capacity,
        .buffer_size = 4 * 1024,
        .kind = segment_kind::full,
        .preallocate = preallocate,
        .max_cached = max_cached,
    };
}

// A pooled buffer has to be closed before it can go back to the pool, which happens when the
// handle is destroyed - here, when this returns.
void close_and_return(owned_write_buffer buf) {
    buf->close().get();
}

size_t record_size_with_value(schema_ptr schema, const sstring& pk, size_t value_size) {
    return log_record_writer(make_log_record(schema, pk, sstring(value_size, 'x'))).size();
}

// Builds a mutation whose log record serializes to exactly `record_size` bytes, by sizing its value
// to whatever is left over. The serialized size grows by a byte per value byte, apart from the
// value's length prefix, so this converges in a step or two.
mutation make_kv_mutation_of_record_size(schema_ptr schema, const sstring& pk, size_t record_size) {
    const auto target = static_cast<ssize_t>(record_size);
    auto size_with_value = [&] (ssize_t value_size) {
        return static_cast<ssize_t>(record_size_with_value(schema, pk, std::max<ssize_t>(value_size, 0)));
    };

    ssize_t value_size = target - size_with_value(0);
    for (int i = 0; i < 4 && size_with_value(value_size) != target; ++i) {
        value_size += target - size_with_value(value_size);
    }

    BOOST_REQUIRE_EQUAL(size_with_value(value_size), target);
    return make_kv_mutation(schema, pk, sstring(value_size, 'x'));
}

}

// Checks what a point read does with the buffer it reads from a segment: the record is split
// in two without parsing either half, the fields it needs are taken out of the header at
// their offsets, and the value is decoded straight out of the buffer.
SEASTAR_THREAD_TEST_CASE(test_logstor_view_log_record) {
    auto schema = make_kv_schema();
    const auto ts = api::timestamp_type(4711);
    auto expected = make_kv_mutation(schema, "pk0", "v0", ts);

    raw_write_buffer wb(32 * 1024, segment_kind::mixed);
    auto appended = wb.append(log_record_writer(make_log_record(expected, ts)));
    wb.seal(segment_sequence{3}, std::nullopt, ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    const auto record_bytes = bytes_view(reinterpret_cast<const int8_t*>(serialized.get()) + appended.record_header_offset,
            appended.total_size);
    const auto record = view_log_record(record_bytes);

    BOOST_REQUIRE_EQUAL(ondisk::log_record_header_timestamp(record.header), ts);
    const auto key = linearized(managed_bytes_view(expected.decorated_key().key().representation()));
    BOOST_REQUIRE(ondisk::log_record_header_key(record.header) == bytes_view(key));

    column_translation_cache translations;
    mutation m(schema, expected.decorated_key());
    read_row_value_into(m.partition(), *schema, record.data,
            ondisk::log_record_header_timestamp(record.header), translations);
    assert_that(m).is_equal_to(expected);

    // A record whose sizes do not fit what was read is refused rather than read past.
    BOOST_REQUIRE_THROW(view_log_record(record_bytes.substr(0, record_bytes.size() - 1)), std::runtime_error);
    BOOST_REQUIRE_THROW(view_log_record(record_bytes.substr(0, ondisk::record_header_size)), std::runtime_error);
}

// Checks the compact log_record_header encoding: what it takes, that it round trips, and
// that the fields a point read reads without deserializing it are where it expects them.
SEASTAR_THREAD_TEST_CASE(test_logstor_log_record_header_serialization) {
    auto schema = make_kv_schema();
    auto record = make_log_record(schema, "pk0", "v0", api::timestamp_type(7));
    const auto& header = record.header;
    const auto key = linearized(managed_bytes_view(header.key.dk.key().representation()));

    bytes serialized(bytes::initialized_later(), ondisk::log_record_header_size(header));
    auto out = seastar::simple_memory_output_stream(reinterpret_cast<char*>(serialized.data()), serialized.size());
    ser::serialize(out, header);
    BOOST_REQUIRE_EQUAL(out.size(), 0u);
    BOOST_REQUIRE_EQUAL(serialized.size(), ondisk::log_record_header_fixed_size + key.size());

    // The timestamp is at a fixed offset and the key right after it, so a point read takes
    // the one and memcmps the other without deserializing the header.
    auto timestamp_in = seastar::simple_memory_input_stream(
            reinterpret_cast<const char*>(serialized.data()) + ondisk::log_record_header_timestamp_offset,
            sizeof(int64_t));
    BOOST_REQUIRE_EQUAL(ser::deserialize(timestamp_in, std::type_identity<int64_t>{}), header.timestamp);
    BOOST_REQUIRE(bytes_view(serialized).substr(ondisk::log_record_header_key_offset, key.size()) == bytes_view(key));

    auto in = seastar::simple_memory_input_stream(reinterpret_cast<const char*>(serialized.data()), serialized.size());
    auto read = ser::deserialize(in, std::type_identity<log_record_header>{});
    BOOST_REQUIRE_EQUAL(read.timestamp, header.timestamp);
    BOOST_REQUIRE_EQUAL(read.table, header.table);
    BOOST_REQUIRE(read.key.dk.equal(*schema, header.key.dk));

    auto skipped = seastar::simple_memory_input_stream(reinterpret_cast<const char*>(serialized.data()), serialized.size());
    ser::skip(skipped, std::type_identity<log_record_header>{});
    BOOST_REQUIRE_EQUAL(skipped.size(), 0u);
}

// Checks that sealing a full raw write buffer writes the expected header fields.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_record_and_header_serialization) {
    auto schema = make_kv_schema();
    auto expected = make_log_record(schema, "pk0", "v0", api::timestamp_type(7));

    raw_write_buffer wb(32 * 1024, segment_kind::full);
    auto writer = log_record_writer(expected);
    auto expected_data_size = size_t(ondisk::record_header_size) + writer.size();
    expected_data_size = ((expected_data_size + ondisk::record_alignment - 1) / ondisk::record_alignment) * ondisk::record_alignment;
    wb.append(std::move(writer));
    wb.seal(segment_sequence{17}, schema->id(), ondisk::block_alignment);

    BOOST_REQUIRE_EQUAL(wb.serialized_size() % ondisk::block_alignment, 0u);

    seastar::simple_memory_input_stream in(wb.data(), wb.serialized_size());
    auto bh = ser::deserialize(in, std::type_identity<ondisk::buffer_header>{});
    BOOST_REQUIRE(raw_write_buffer::validate_header(bh));
    BOOST_REQUIRE(bh.kind == segment_kind::full);
    BOOST_REQUIRE_EQUAL(bh.segment_seq.value, 17u);
    BOOST_REQUIRE_EQUAL(bh.data_size, expected_data_size);

    auto sh = ser::deserialize(in, std::type_identity<ondisk::segment_header>{});
    BOOST_REQUIRE_EQUAL(sh.table, schema->id());
    BOOST_REQUIRE_EQUAL(sh.first_token, expected.header.key.dk.token());
    BOOST_REQUIRE_EQUAL(sh.last_token, expected.header.key.dk.token());
}

// Checks that a raw write buffer can hold and seal a record whose serialized size is exactly max_record_size().
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_accepts_record_at_max_record_size) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(ondisk::block_alignment, segment_kind::mixed);
    auto max_size = wb.max_record_size();

    sstring value;
    auto record = make_log_record(schema, "pk", "", api::timestamp_type(27));
    log_record_writer writer(record);

    while (writer.size() < max_size) {
        value += "x";
        record = make_log_record(schema, "pk", value, api::timestamp_type(27));
        writer = log_record_writer(record);
    }

    BOOST_REQUIRE_EQUAL(writer.size(), max_size);
    BOOST_REQUIRE(wb.can_fit(writer));

    wb.append(writer);
    wb.seal(segment_sequence{29}, std::nullopt, ondisk::block_alignment);

    BOOST_REQUIRE_EQUAL(wb.serialized_size(), ondisk::block_alignment);
}

// A record written to a mixed segment is rewritten by the separator into a full segment of its
// compaction group, so it has to fit a buffer of either kind, which do not have the same room for
// records. The write path used to bound a record by the mixed buffer alone, so a record that only
// fits that one was accepted and could then never be separated, spinning in write_to_separator.
SEASTAR_THREAD_TEST_CASE(test_logstor_largest_accepted_record_can_be_separated) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);

    const auto segment_size = ls.get_segment_manager().get_segment_size();
    const auto max_size = raw_write_buffer::max_record_size_any_kind(segment_size);

    auto expected = make_kv_mutation_of_record_size(schema, "pk0", max_size);
    auto key = expected.decorated_key();

    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
    ls.flush_to_separator().get();
    cg.flush_separator().get();

    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);

    auto actual = ls.read(*schema, cg.logstor_index(), key, schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// Checks that a record that does not fit a segment of either kind is rejected by the write path,
// rather than accepted into a mixed segment that the separator would then not be able to split.
SEASTAR_THREAD_TEST_CASE(test_logstor_rejects_record_that_does_not_fit_a_segment_of_either_kind) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);

    const auto segment_size = ls.get_segment_manager().get_segment_size();
    const auto max_size = raw_write_buffer::max_record_size_any_kind(segment_size);

    auto too_big = make_kv_mutation_of_record_size(schema, "pk0", max_size + 1);
    BOOST_REQUIRE_THROW(ls.write(too_big, write_target(&cg, {}), db::no_timeout).get(), std::runtime_error);

    BOOST_REQUIRE(!cg.separator_has_data());
}

// Checks that a buffer holding records that were never flushed can still be closed and reused
// after aborting its writes. A pending write is resolved only by the flush and holds the buffer's
// write gate until then, so close() alone never completes. The compaction buffer pool relies on
// abort_writes() to reclaim a buffer left behind by a compaction that failed before flushing.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_abort_writes_reclaims_unflushed_buffer) {
    auto schema = make_kv_schema();

    write_buffer wb(32 * 1024, segment_kind::full);
    auto written = wb.write(log_record_writer(make_log_record(schema, "pk0", "v0", api::timestamp_type(1))));
    BOOST_REQUIRE(!written.available());

    auto closed = wb.close();
    seastar::thread::yield();
    BOOST_REQUIRE(!closed.available());

    wb.abort_writes(std::make_exception_ptr(std::runtime_error("buffer was not flushed"))).get();
    BOOST_REQUIRE_THROW(written.get(), std::runtime_error);
    closed.get();
    BOOST_REQUIRE(wb.is_closed());

    wb.reset();
    BOOST_REQUIRE(!wb.is_closed());
    BOOST_REQUIRE(!wb.has_data());
}

// A record appended to a buffer that is already bound to a segment knows its final location right
// away, so it is appended without the completion tracking write() sets up. Checks that the two ways
// in produce the same bytes and the same offsets, so that a location computed at append time is the
// location the record ends up at.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_append_synchronously_matches_a_raw_append) {
    auto schema = make_kv_schema();
    const auto records = std::vector<log_record>{
        make_log_record(schema, "pk0", "v0", api::timestamp_type(1)),
        make_log_record(schema, "pk1", "v1", api::timestamp_type(2)),
        make_log_record(schema, "pk2", "v2", api::timestamp_type(3)),
    };

    raw_write_buffer expected_wb(32 * 1024, segment_kind::full);
    write_buffer wb(32 * 1024, segment_kind::full);

    for (const auto& record : records) {
        const auto expected = expected_wb.append(log_record_writer(record));
        const auto actual = wb.append_synchronously(log_record_writer(record));
        BOOST_REQUIRE_EQUAL(actual.record_header_offset, expected.record_header_offset);
        BOOST_REQUIRE_EQUAL(actual.total_size, expected.total_size);
        BOOST_REQUIRE_EQUAL(wb.offset_in_buffer(), expected_wb.offset_in_buffer());
        BOOST_REQUIRE_EQUAL(wb.net_data_size(), expected_wb.net_data_size());
        BOOST_REQUIRE_EQUAL(wb.record_count(), expected_wb.record_count());
    }

    expected_wb.seal(segment_sequence{5}, schema->id(), ondisk::block_alignment);
    wb.seal(segment_sequence{5}, schema->id(), ondisk::block_alignment);

    // The segment header carries the token range of the appended records, which append() is what
    // maintains, so equal bytes here also say the range came out right.
    const auto expected_bytes = make_serialized_buffer_copy(expected_wb);
    const auto actual_bytes = make_serialized_buffer_copy(wb);
    BOOST_REQUIRE_EQUAL(actual_bytes.size(), expected_bytes.size());
    BOOST_REQUIRE(std::equal(actual_bytes.begin(), actual_bytes.end(), expected_bytes.begin()));
}

// The direct write path appends a record it owns nothing of - the key is the mutation's and the
// value is the encoder's - so the bytes it writes have to be the bytes a record that owns them
// writes, or a segment written directly would not be readable the ordinary way. Keys of both sides
// of the inline/external threshold of a managed_bytes, because the two are serialized by walking
// the key's fragments.
SEASTAR_THREAD_TEST_CASE(test_logstor_append_of_a_ref_writer_matches_an_owning_writer) {
    auto schema = make_kv_schema();
    const auto records = std::vector<log_record>{
        make_log_record(schema, "pk0", "v0", api::timestamp_type(1)),
        make_log_record(schema, sstring(200, 'k'), "v1", api::timestamp_type(2)),
        make_log_record(schema, "pk2", sstring(2000, 'x'), api::timestamp_type(3)),
    };

    raw_write_buffer expected_wb(32 * 1024, segment_kind::full);
    raw_write_buffer wb(32 * 1024, segment_kind::full);

    for (const auto& record : records) {
        const auto& header = record.header;
        auto ref_writer = log_record_ref_writer(log_record_header_view{
            .token = header.key.dk.token(),
            .timestamp = header.timestamp,
            .table = header.table,
            .key = managed_bytes_view(header.key.dk.key().representation()),
        }, record.value.view());

        const auto expected = expected_wb.append(log_record_writer(record));
        const auto actual = wb.append(ref_writer);
        BOOST_REQUIRE_EQUAL(actual.record_header_offset, expected.record_header_offset);
        BOOST_REQUIRE_EQUAL(actual.total_size, expected.total_size);
    }

    // Sealing writes the segment header, whose token range the appends are what maintain, so equal
    // bytes here also say the ref writer reported the same token.
    expected_wb.seal(segment_sequence{7}, schema->id(), ondisk::block_alignment);
    wb.seal(segment_sequence{7}, schema->id(), ondisk::block_alignment);

    const auto expected_bytes = make_serialized_buffer_copy(expected_wb);
    const auto actual_bytes = make_serialized_buffer_copy(wb);
    BOOST_REQUIRE_EQUAL(actual_bytes.size(), expected_bytes.size());
    BOOST_REQUIRE(std::equal(actual_bytes.begin(), actual_bytes.end(), expected_bytes.begin()));
}

// A mixed buffer also has to retain a copy of every record for the separator to replay, which an
// append that tracks nothing cannot do, so it is refused rather than silently losing the copy.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_append_synchronously_refuses_a_mixed_buffer) {
    auto schema = make_kv_schema();

    seastar::testing::scoped_no_abort_on_internal_error no_abort;
    write_buffer wb(32 * 1024, segment_kind::mixed);
    BOOST_REQUIRE_THROW(wb.append_synchronously(
            log_record_writer(make_log_record(schema, "pk0", "v0", api::timestamp_type(1)))),
            std::runtime_error);
}

// Nothing subscribes to the flush of a buffer that was only appended to synchronously, so the flush
// has to complete and close it all the same - which is what lets it go back to its pool.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_appended_synchronously_returns_to_its_pool) {
    auto schema = make_kv_schema();
    abort_source as;
    write_buffer_pool pool(make_test_write_buffer_pool_config(1, 1));
    auto stop_pool = seastar::defer([&pool] noexcept { pool.stop().get(); });

    {
        auto buf = pool.allocate(as).get();
        buf->append_synchronously(log_record_writer(make_log_record(schema, "pk0", "v0", api::timestamp_type(1))));
        buf->complete_writes(log_location{.segment = log_segment_id{3}, .offset = 0, .size = 0}).get();
        BOOST_REQUIRE(buf->is_closed());
    }

    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 0u);
    close_and_return(pool.allocate(as).get());
}

// The separator rewrites records into a buffer and owes the index an entry pointing at each copy.
// Checks that the updates are applied when the buffer learns where it was written, at the location
// each record ended up at - and that a flush that failed moves nothing, leaving every entry
// pointing at the record in the segment it was read from.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_applies_separator_index_updates) {
    auto schema = make_kv_schema();
    struct : space_accounting_subscriber {
        void on_add_record(log_location) noexcept override {}
        void on_free_record(log_location) noexcept override {}
    } accounting;

    const auto records = std::vector<log_record>{
        make_log_record(schema, "pk0", "v0", api::timestamp_type(1)),
        make_log_record(schema, "pk1", "v1", api::timestamp_type(2)),
        make_log_record(schema, "pk2", "v2", api::timestamp_type(3)),
    };

    // Where the separator read the records from, and where it wrote the buffer holding their copies.
    const auto source_location = [] (size_t i) {
        return log_location{.segment = log_segment_id{1}, .offset = uint32_t(128 * i), .size = 64};
    };
    const auto buffer_location = log_location{.segment = log_segment_id{7}, .offset = 4096, .size = 0};

    auto append_records = [&] (write_buffer& wb, primary_index& index) {
        std::vector<log_location> rewritten;
        for (size_t i = 0; i < records.size(); ++i) {
            const auto appended = wb.append_synchronously(log_record_writer(records[i]));
            wb.add_index_update(separator_index_update{
                .index = &index,
                .key = records[i].header.key,
                .prev_location = source_location(i),
                .offset_in_buffer = appended.record_header_offset,
                .size = appended.total_size,
            });
            rewritten.push_back(record_location(buffer_location, appended.record_header_offset, appended.total_size));
        }
        return rewritten;
    };

    auto fill_index = [&] (primary_index& index) {
        for (size_t i = 0; i < records.size(); ++i) {
            index.insert(records[i].header.key, index_entry{
                .location = source_location(i),
                .timestamp = records[i].header.timestamp,
            });
        }
    };

    {
        primary_index index(schema, accounting);
        fill_index(index);

        write_buffer wb(32 * 1024, segment_kind::full);
        const auto rewritten = append_records(wb, index);
        wb.complete_writes(buffer_location).get();

        for (size_t i = 0; i < records.size(); ++i) {
            BOOST_REQUIRE(index.is_record_alive(records[i].header.key, rewritten[i]));
            BOOST_REQUIRE(!index.is_record_alive(records[i].header.key, source_location(i)));
        }
    }

    {
        primary_index index(schema, accounting);
        fill_index(index);

        write_buffer wb(32 * 1024, segment_kind::full);
        const auto rewritten = append_records(wb, index);
        wb.abort_writes(std::make_exception_ptr(std::runtime_error("buffer was not flushed"))).get();

        for (size_t i = 0; i < records.size(); ++i) {
            BOOST_REQUIRE(index.is_record_alive(records[i].header.key, source_location(i)));
            BOOST_REQUIRE(!index.is_record_alive(records[i].header.key, rewritten[i]));
        }
    }
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
    BOOST_REQUIRE_EQUAL(b0->get_buffer_size(), 4u * 1024);

    close_and_return(std::move(b0));
    close_and_return(std::move(b1));
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 0u);
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 1u);

    // The kept buffer is handed out again, the one that was let go is built anew.
    auto b2 = pool.allocate(as).get();
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 2u);
    auto b3 = pool.allocate(as).get();
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 3u);

    close_and_return(std::move(b2));
    close_and_return(std::move(b3));
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
        close_and_return(std::move(buf));
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

    close_and_return(std::move(b0));
    close_and_return(std::move(b1));
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

    close_and_return(std::move(b0));
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 1u);

    auto waiting = pool.allocate(as);
    seastar::thread::yield();
    BOOST_REQUIRE(!waiting.available());

    close_and_return(std::move(b1));
    auto b2 = waiting.get();
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 1u);
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 1u);

    close_and_return(std::move(b2));
}

// Checks that try_allocate() hands out a buffer only when the pool has one to spare and never waits,
// which is what lets the direct write path go on without one instead of parking on the pool.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_pool_try_allocate_never_waits) {
    abort_source as;
    write_buffer_pool pool(make_test_write_buffer_pool_config(1, 1));
    auto stop_pool = seastar::defer([&pool] noexcept { pool.stop().get(); });

    auto b0 = pool.try_allocate();
    BOOST_REQUIRE(b0);
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 1u);

    // The capacity is taken, so this refuses rather than waits.
    BOOST_REQUIRE(!pool.try_allocate());
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 1u);
    BOOST_REQUIRE_EQUAL(pool.get_stats().allocation_waits, 0u);

    close_and_return(std::move(*b0));
    auto b1 = pool.try_allocate();
    BOOST_REQUIRE(b1);
    close_and_return(std::move(*b1));

    // A waiting allocation holds the capacity too, so a refusal here does not jump the queue.
    auto b2 = pool.allocate(as).get();
    auto waiting = pool.allocate(as);
    seastar::thread::yield();
    BOOST_REQUIRE(!waiting.available());
    BOOST_REQUIRE(!pool.try_allocate());

    close_and_return(std::move(b2));
    close_and_return(waiting.get());
}

// Checks that a compaction that fails after rewriting records into its buffer returns the buffer to
// the pool, so that later compactions - and shutdown, which waits for the pool to drain - are not
// blocked by it.
SEASTAR_THREAD_TEST_CASE(test_logstor_failed_compaction_returns_its_buffer_to_the_pool) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return;
    }

    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    auto setup_guard = std::make_optional(ls.get_compaction_manager().disable_compaction(cg).get());

    auto pk0_v0 = make_kv_mutation(schema, "pk0", "v0", api::timestamp_type(1));
    auto pk1_v0 = make_kv_mutation(schema, "pk1", "v1", api::timestamp_type(2));
    auto pk0_v1 = make_kv_mutation(schema, "pk0", "v0-new", api::timestamp_type(3));

    write_and_flush_segment(ls, cg, pk0_v0);
    write_and_flush_segment(ls, cg, pk1_v0);
    write_and_flush_segment(ls, cg, pk0_v1);

    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 3u);

    const auto pk0 = primary_index_key{pk0_v1.decorated_key()};
    const auto pk1 = primary_index_key{pk1_v0.decorated_key()};

    auto pk0_before = cg.logstor_index().get(pk0);
    auto pk1_before = cg.logstor_index().get(pk1);
    BOOST_REQUIRE(pk0_before);
    BOOST_REQUIRE(pk1_before);

    // The first compaction fails right after rewriting a record, leaving records in its buffer that
    // will never be flushed.
    utils::get_local_injector().enable("logstor_compaction_fail_after_rewrite", true /* one shot */);

    setup_guard.reset();
    ls.get_compaction_manager().submit(cg);
    setup_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    // Nothing was flushed, so the group and the index are unchanged.
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 3u);
    BOOST_REQUIRE(cg.logstor_index().get(pk0)->location == pk0_before->location);
    BOOST_REQUIRE(cg.logstor_index().get(pk1)->location == pk1_before->location);

    // The buffer of the failed compaction is back in the pool, so this one runs to completion.
    setup_guard.reset();
    ls.get_compaction_manager().submit(cg);
    auto compaction_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);

    auto pk0_after = cg.logstor_index().get(pk0);
    auto pk1_after = cg.logstor_index().get(pk1);
    BOOST_REQUIRE(pk0_after);
    BOOST_REQUIRE(pk1_after);
    BOOST_REQUIRE(pk0_after->location != pk0_before->location);
    BOOST_REQUIRE(pk1_after->location != pk1_before->location);

    assert_that(*ls.read(*schema, cg.logstor_index(), pk0.dk, schema->full_slice()).get()).is_equal_to(pk0_v1);
    assert_that(*ls.read(*schema, cg.logstor_index(), pk1.dk, schema->full_slice()).get()).is_equal_to(pk1_v0);
}

// Checks that a compaction whose index updates fail returns its buffer to the pool. This failure
// surfaces only after the output segment was written, so it lands after flush() handed the pending
// updates to when_all_succeed() - which moves them out of the vector. That is the path where a
// teardown still walking that vector would operate on moved-from futures.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_failing_index_update_returns_its_buffer_to_the_pool) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return;
    }

    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    auto setup_guard = std::make_optional(ls.get_compaction_manager().disable_compaction(cg).get());

    auto pk0_v0 = make_kv_mutation(schema, "pk0", "v0", api::timestamp_type(1));
    auto pk1_v0 = make_kv_mutation(schema, "pk1", "v1", api::timestamp_type(2));
    auto pk0_v1 = make_kv_mutation(schema, "pk0", "v0-new", api::timestamp_type(3));

    write_and_flush_segment(ls, cg, pk0_v0);
    write_and_flush_segment(ls, cg, pk1_v0);
    write_and_flush_segment(ls, cg, pk0_v1);

    const auto pk0 = primary_index_key{pk0_v1.decorated_key()};
    const auto pk1 = primary_index_key{pk1_v0.decorated_key()};

    utils::get_local_injector().enable("logstor_compaction_fail_index_update", true /* one shot */);

    setup_guard.reset();
    ls.get_compaction_manager().submit(cg);
    setup_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    // The records are readable whichever location the index kept for them.
    assert_that(*ls.read(*schema, cg.logstor_index(), pk0.dk, schema->full_slice()).get()).is_equal_to(pk0_v1);
    assert_that(*ls.read(*schema, cg.logstor_index(), pk1.dk, schema->full_slice()).get()).is_equal_to(pk1_v0);

    // The buffer of the failed compaction is back in the pool, so compaction still runs.
    setup_guard.reset();
    ls.get_compaction_manager().submit(cg);
    auto compaction_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    assert_that(*ls.read(*schema, cg.logstor_index(), pk0.dk, schema->full_slice()).get()).is_equal_to(pk0_v1);
    assert_that(*ls.read(*schema, cg.logstor_index(), pk1.dk, schema->full_slice()).get()).is_equal_to(pk1_v0);
}

// Checks that primary_index accounting callbacks track live bytes across
// inserts, overwrites, relocations, erases, range erases, and clear().
SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_space_accounting) {
    auto schema = make_kv_schema();
    struct accounting_subscriber : space_accounting_subscriber {
        ssize_t live_bytes = 0;
        size_t add_calls = 0;
        size_t free_calls = 0;
        std::vector<log_location> added_locations;
        std::vector<log_location> freed_locations;

        bool is_live(log_location loc) const {
            return std::count(added_locations.begin(), added_locations.end(), loc)
                 > std::count(freed_locations.begin(), freed_locations.end(), loc);
        }

        size_t live_location_count() const {
            return std::count_if(added_locations.begin(), added_locations.end(), [&] (log_location loc) {
                return is_live(loc);
            });
        }

        void on_add_record(log_location loc) noexcept override {
            live_bytes += loc.size;
            ++add_calls;
            added_locations.push_back(loc);
        }

        void on_free_record(log_location loc) noexcept override {
            live_bytes -= loc.size;
            ++free_calls;
            BOOST_REQUIRE(is_live(loc));
            freed_locations.push_back(loc);
        }
    } accounting;

    primary_index index(schema, accounting);

    // The index counts the bytes of the records it points at for the table it belongs to, which is
    // the same number the subscriber accumulates, so the two must agree after every operation.
    auto check_live_bytes = [&] (ssize_t expected) {
        BOOST_REQUIRE_EQUAL(accounting.live_bytes, expected);
        BOOST_REQUIRE_EQUAL(index.get_live_record_bytes(), uint64_t(expected));
    };

    const auto pk0 = primary_index_key{make_kv_mutation(schema, "pk0", "v0").decorated_key()};
    const auto pk1 = primary_index_key{make_kv_mutation(schema, "pk1", "v1").decorated_key()};
    const auto pk2 = primary_index_key{make_kv_mutation(schema, "pk2", "v2").decorated_key()};

    const log_location loc0{.segment = log_segment_id{1}, .offset = 0, .size = 11};
    const log_location loc0_old{.segment = log_segment_id{1}, .offset = 16, .size = 7};
    const log_location loc1{.segment = log_segment_id{2}, .offset = 0, .size = 17};
    const log_location loc2{.segment = log_segment_id{3}, .offset = 0, .size = 13};
    const log_location loc3{.segment = log_segment_id{4}, .offset = 0, .size = 19};
    const log_location loc4{.segment = log_segment_id{5}, .offset = 0, .size = 23};
    const log_location loc5{.segment = log_segment_id{6}, .offset = 0, .size = 29};

    // insert(pk0, loc0): new entry, succeeds, no previous entry to free  →  {pk0: loc0}
    auto [inserted0, prev0] = index.insert(pk0, index_entry{.location = loc0, .timestamp = api::timestamp_type(10)});
    BOOST_REQUIRE(inserted0);
    BOOST_REQUIRE(!prev0);
    check_live_bytes(ssize_t(loc0.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc0));

    // insert(pk0, loc0_old): older timestamp, rejected, no accounting change  →  {pk0: loc0}
    auto [inserted_old, prev_old] = index.insert(pk0, index_entry{.location = loc0_old, .timestamp = api::timestamp_type(9)});
    BOOST_REQUIRE(!inserted_old);
    BOOST_REQUIRE(prev_old);
    BOOST_REQUIRE(prev_old->location == loc0);
    BOOST_REQUIRE_EQUAL(prev_old->timestamp, api::timestamp_type(10));
    check_live_bytes(ssize_t(loc0.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc0));

    // insert(pk0, loc1): newer timestamp, replaces loc0, old location freed via accounting  →  {pk0: loc1}
    auto [inserted1, prev1] = index.insert(pk0, index_entry{.location = loc1, .timestamp = api::timestamp_type(11)});
    BOOST_REQUIRE(inserted1);
    BOOST_REQUIRE(prev1);
    BOOST_REQUIRE(prev1->location == loc0);
    BOOST_REQUIRE_EQUAL(prev1->timestamp, api::timestamp_type(10));
    check_live_bytes(ssize_t(loc1.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 2u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc1));
    BOOST_REQUIRE(!accounting.is_live(loc0));
    BOOST_REQUIRE(accounting.added_locations.back() == loc1);
    BOOST_REQUIRE(accounting.freed_locations.back() == loc0);

    // insert(pk1, loc2): new key, succeeds, adds to live bytes  →  {pk0: loc1, pk1: loc2}
    auto [inserted2, prev2] = index.insert(pk1, index_entry{.location = loc2, .timestamp = api::timestamp_type(7)});
    BOOST_REQUIRE(inserted2);
    BOOST_REQUIRE(!prev2);
    check_live_bytes(ssize_t(loc1.size + loc2.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);
    BOOST_REQUIRE(accounting.is_live(loc1));
    BOOST_REQUIRE(accounting.is_live(loc2));

    // erase(pk1, loc1): location mismatch, returns false, no accounting change  →  {pk0: loc1, pk1: loc2}
    BOOST_REQUIRE(!index.erase(pk1, loc1));
    check_live_bytes(ssize_t(loc1.size + loc2.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);

    // update_record_location(pk0, loc1 -> loc3): old location matches, succeeds, frees loc1 adds loc3  →  {pk0: loc3, pk1: loc2}
    BOOST_REQUIRE(index.update_record_location(pk0, loc1, loc3));
    check_live_bytes(ssize_t(loc2.size + loc3.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 4u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 2u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);
    BOOST_REQUIRE(accounting.is_live(loc2));
    BOOST_REQUIRE(accounting.is_live(loc3));
    BOOST_REQUIRE(!accounting.is_live(loc1));
    BOOST_REQUIRE(accounting.added_locations.back() == loc3);
    BOOST_REQUIRE(accounting.freed_locations.back() == loc1);

    // update_record_location(pk0, loc1 -> loc4): old location no longer current, fails, no accounting change  →  {pk0: loc3, pk1: loc2}
    BOOST_REQUIRE(!index.update_record_location(pk0, loc1, loc4));
    check_live_bytes(ssize_t(loc2.size + loc3.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 4u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 2u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);

    // erase(pk1, loc2): location matches, succeeds, frees loc2  →  {pk0: loc3}
    BOOST_REQUIRE(index.erase(pk1, loc2));
    check_live_bytes(ssize_t(loc3.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 4u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc3));
    BOOST_REQUIRE(!accounting.is_live(loc2));
    BOOST_REQUIRE(accounting.freed_locations.back() == loc2);

    // insert(pk1, loc4): new entry for pk1 (previously erased), succeeds  →  {pk0: loc3, pk1: loc4}
    auto [inserted4, prev4] = index.insert(pk1, index_entry{.location = loc4, .timestamp = api::timestamp_type(12)});
    BOOST_REQUIRE(inserted4);
    BOOST_REQUIRE(!prev4);
    // insert(pk2, loc5): new key, succeeds  →  {pk0: loc3, pk1: loc4, pk2: loc5}
    auto [inserted5, prev5] = index.insert(pk2, index_entry{.location = loc5, .timestamp = api::timestamp_type(13)});
    BOOST_REQUIRE(inserted5);
    BOOST_REQUIRE(!prev5);
    check_live_bytes(ssize_t(loc3.size + loc4.size + loc5.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 6u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 3);

    // range erase pk1: removes pk1 entry (loc4), frees via accounting  →  {pk0: loc3, pk2: loc5}
    index.erase(dht::partition_range::make_singular(pk1.dk)).get();
    check_live_bytes(ssize_t(loc3.size + loc5.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 6u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 4u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);
    BOOST_REQUIRE(!accounting.is_live(loc4));
    BOOST_REQUIRE(accounting.is_live(loc3));
    BOOST_REQUIRE(accounting.is_live(loc5));
    BOOST_REQUIRE(accounting.freed_locations.back() == loc4);

    // clear(): removes all remaining entries (pk0->loc3, pk2->loc5), all freed  →  {}
    index.clear().get();
    BOOST_REQUIRE(index.empty());
    check_live_bytes(0);
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 6u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 6u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 0);
    BOOST_REQUIRE_EQUAL(accounting.freed_locations.size(), 6u);
}

// Checks that range erase and clear() account for freed log locations correctly.
SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_range_erase_and_clear_space_accounting) {
    auto schema = make_kv_schema();
    struct accounting_subscriber : space_accounting_subscriber {
        ssize_t live_bytes = 0;
        size_t add_calls = 0;
        size_t free_calls = 0;
        std::vector<log_location> freed_locations;

        void on_add_record(log_location loc) noexcept override {
            live_bytes += loc.size;
            ++add_calls;
        }

        void on_free_record(log_location loc) noexcept override {
            live_bytes -= loc.size;
            ++free_calls;
            freed_locations.push_back(loc);
        }
    } accounting;

    primary_index index(schema, accounting);

    auto check_live_bytes = [&] (ssize_t expected) {
        BOOST_REQUIRE_EQUAL(accounting.live_bytes, expected);
        BOOST_REQUIRE_EQUAL(index.get_live_record_bytes(), uint64_t(expected));
    };

    struct entry {
        primary_index_key key;
        log_location loc;
    };

    std::vector<entry> entries = {
        { primary_index_key{make_kv_mutation(schema, "pk0", "v0").decorated_key()}, {.segment = log_segment_id{11}, .offset = 0, .size = 5} },
        { primary_index_key{make_kv_mutation(schema, "pk1", "v1").decorated_key()}, {.segment = log_segment_id{12}, .offset = 0, .size = 7} },
        { primary_index_key{make_kv_mutation(schema, "pk2", "v2").decorated_key()}, {.segment = log_segment_id{13}, .offset = 0, .size = 11} },
        { primary_index_key{make_kv_mutation(schema, "pk3", "v3").decorated_key()}, {.segment = log_segment_id{14}, .offset = 0, .size = 13} },
        { primary_index_key{make_kv_mutation(schema, "pk4", "v4").decorated_key()}, {.segment = log_segment_id{15}, .offset = 0, .size = 17} },
    };

    std::sort(entries.begin(), entries.end(), [&] (const entry& a, const entry& b) {
        return dht::decorated_key::less_comparator(schema)(a.key.dk, b.key.dk);
    });

    auto insert = [&] (const entry& e, api::timestamp_type ts) {
        auto [inserted, prev] = index.insert(e.key, index_entry{.location = e.loc, .timestamp = ts});
        BOOST_REQUIRE(inserted);
        BOOST_REQUIRE(!prev);
    };

    insert(entries[0], api::timestamp_type(10));
    insert(entries[1], api::timestamp_type(11));
    insert(entries[2], api::timestamp_type(12));
    insert(entries[3], api::timestamp_type(13));
    insert(entries[4], api::timestamp_type(14));

    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    check_live_bytes(ssize_t(entries[0].loc.size + entries[1].loc.size + entries[2].loc.size + entries[3].loc.size + entries[4].loc.size));

    index.erase(dht::partition_range(
            dht::partition_range::bound(entries[1].key.dk, true),
            dht::partition_range::bound(entries[3].key.dk, true))).get();

    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 3u);
    check_live_bytes(ssize_t(entries[0].loc.size + entries[4].loc.size));
    BOOST_REQUIRE(index.find(entries[0].key.dk) != index.end());
    BOOST_REQUIRE(index.find(entries[1].key.dk) == index.end());
    BOOST_REQUIRE(index.find(entries[2].key.dk) == index.end());
    BOOST_REQUIRE(index.find(entries[3].key.dk) == index.end());
    BOOST_REQUIRE(index.find(entries[4].key.dk) != index.end());
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[1].loc), 1);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[2].loc), 1);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[3].loc), 1);

    index.clear().get();

    BOOST_REQUIRE(index.empty());
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 5u);
    check_live_bytes(0);
    BOOST_REQUIRE_EQUAL(accounting.freed_locations.size(), 5u);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[0].loc), 1);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[4].loc), 1);
}

// Checks that scan_segment() returns mixed-buffer log locations that can be used to read back the expected records.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_scan_mixed_buffers_report_readable_log_locations) {
    auto schema = make_kv_schema();

    raw_write_buffer wb0(64 * 1024, segment_kind::mixed);
    raw_write_buffer wb1(64 * 1024, segment_kind::mixed);

    auto expected0 = make_kv_mutation(schema, "pk0", "value0", api::timestamp_type(11));
    auto expected1 = make_kv_mutation(schema, "pk1", "value1-longer", api::timestamp_type(12));
    auto expected2 = make_kv_mutation(schema, "pk2", "v2", api::timestamp_type(13));
    auto expected3 = make_kv_mutation(schema, "pk3", "value-three-is-even-longer-than-before", api::timestamp_type(14));

    wb0.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(11))));
    wb0.append(log_record_writer(make_log_record(schema, "pk1", "value1-longer", api::timestamp_type(12))));
    wb1.append(log_record_writer(make_log_record(schema, "pk2", "v2", api::timestamp_type(13))));
    wb1.append(log_record_writer(make_log_record(schema, "pk3", "value-three-is-even-longer-than-before", api::timestamp_type(14))));

    wb0.seal(segment_sequence{23}, std::nullopt, ondisk::block_alignment);
    wb1.seal(segment_sequence{23}, std::nullopt, ondisk::block_alignment);

    auto serialized0 = make_serialized_buffer_copy(wb0);
    auto serialized1 = make_serialized_buffer_copy(wb1);
    auto segment = concat_serialized_buffers({&serialized0, &serialized1});
    const auto segment_size = segment.size();
    const auto* segment_data = segment.get();
    auto in = seastar::util::as_input_stream(std::move(segment));

    std::vector<segment_header> seen_segment_headers;
    std::vector<log_record_header> seen_record_headers;
    std::vector<log_location> seen_locations;

    scan_segment(in, log_segment_id{3}, segment_size,
        [&seen_segment_headers] (const segment_header& sh) {
            seen_segment_headers.push_back(sh);
            return make_ready_future<>();
        },
        [&seen_record_headers, &seen_locations] (log_location loc, const log_record_header& rh) {
            seen_record_headers.push_back(rh);
            seen_locations.push_back(loc);
            return want_data::yes;
        },
        [] (log_location, log_record) {
            return make_ready_future<>();
         }).get();
    in.close().get();

    temporary_buffer<char> segment_copy(segment_size);
    std::copy_n(segment_data, segment_size, segment_copy.get_write());

    BOOST_REQUIRE_EQUAL(seen_segment_headers.size(), 2u);
    for (const auto& sh : seen_segment_headers) {
        BOOST_REQUIRE(sh.kind == segment_kind::mixed);
        BOOST_REQUIRE_EQUAL(sh.segment_seq.value, 23u);
    }

    BOOST_REQUIRE_EQUAL(seen_record_headers.size(), 4u);
    BOOST_REQUIRE_EQUAL(seen_record_headers[0].timestamp, api::timestamp_type(11));
    BOOST_REQUIRE_EQUAL(seen_record_headers[1].timestamp, api::timestamp_type(12));
    BOOST_REQUIRE_EQUAL(seen_record_headers[2].timestamp, api::timestamp_type(13));
    BOOST_REQUIRE_EQUAL(seen_record_headers[3].timestamp, api::timestamp_type(14));
    BOOST_REQUIRE_EQUAL(seen_record_headers[0].table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_record_headers[1].table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_record_headers[2].table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_record_headers[3].table, schema->id());

    BOOST_REQUIRE_EQUAL(seen_locations.size(), 4u);
    assert_that(to_mutation(schema, read_record_at_location(segment_copy, seen_locations[0]))).is_equal_to(expected0);
    assert_that(to_mutation(schema, read_record_at_location(segment_copy, seen_locations[1]))).is_equal_to(expected1);
    assert_that(to_mutation(schema, read_record_at_location(segment_copy, seen_locations[2]))).is_equal_to(expected2);
    assert_that(to_mutation(schema, read_record_at_location(segment_copy, seen_locations[3]))).is_equal_to(expected3);

    auto maybe_header = read_segment_header_from_bytes(segment_copy);
    BOOST_REQUIRE(maybe_header);
    BOOST_REQUIRE(maybe_header->kind == segment_kind::mixed);
    BOOST_REQUIRE_EQUAL(maybe_header->segment_seq.value, 23u);
    BOOST_REQUIRE(std::holds_alternative<segment_header::mixed>(maybe_header->v));
}

// Checks that scan_segment() only delivers records whose headers were accepted with want_data::yes.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_scan_returns_only_selected_records) {
    auto schema = make_kv_schema();

    raw_write_buffer wb0(64 * 1024, segment_kind::mixed);
    raw_write_buffer wb1(64 * 1024, segment_kind::mixed);

    auto expected1 = make_kv_mutation(schema, "pk1", "value1", api::timestamp_type(72));
    auto expected3 = make_kv_mutation(schema, "pk3", "value3", api::timestamp_type(74));

    wb0.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(71))));
    wb0.append(log_record_writer(make_log_record(schema, "pk1", "value1", api::timestamp_type(72))));
    wb1.append(log_record_writer(make_log_record(schema, "pk2", "value2", api::timestamp_type(73))));
    wb1.append(log_record_writer(make_log_record(schema, "pk3", "value3", api::timestamp_type(74))));

    wb0.seal(segment_sequence{81}, std::nullopt, ondisk::block_alignment);
    wb1.seal(segment_sequence{81}, std::nullopt, ondisk::block_alignment);

    auto serialized0 = make_serialized_buffer_copy(wb0);
    auto serialized1 = make_serialized_buffer_copy(wb1);
    auto segment = concat_serialized_buffers({&serialized0, &serialized1});
    const auto segment_size = segment.size();
    auto in = seastar::util::as_input_stream(std::move(segment));

    std::vector<api::timestamp_type> seen_header_timestamps;
    std::vector<log_record> selected_records;

    scan_segment(in, log_segment_id{4}, segment_size,
        [] (const segment_header&) {
            return make_ready_future<>();
        },
        [&seen_header_timestamps] (log_location, const log_record_header& rh) {
            seen_header_timestamps.push_back(rh.timestamp);
            return (rh.timestamp == api::timestamp_type(72) || rh.timestamp == api::timestamp_type(74))
                    ? want_data::yes : want_data::no;
        },
        [&selected_records] (log_location, log_record rec) {
            selected_records.push_back(std::move(rec));
            return make_ready_future<>();
        }).get();
    in.close().get();

    BOOST_REQUIRE_EQUAL(seen_header_timestamps.size(), 4u);
    BOOST_REQUIRE_EQUAL(seen_header_timestamps[0], api::timestamp_type(71));
    BOOST_REQUIRE_EQUAL(seen_header_timestamps[1], api::timestamp_type(72));
    BOOST_REQUIRE_EQUAL(seen_header_timestamps[2], api::timestamp_type(73));
    BOOST_REQUIRE_EQUAL(seen_header_timestamps[3], api::timestamp_type(74));

    BOOST_REQUIRE_EQUAL(selected_records.size(), 2u);
    BOOST_REQUIRE_EQUAL(selected_records[0].header.timestamp, api::timestamp_type(72));
    BOOST_REQUIRE_EQUAL(selected_records[1].header.timestamp, api::timestamp_type(74));
    assert_that(to_mutation(schema, selected_records[0])).is_equal_to(expected1);
    assert_that(to_mutation(schema, selected_records[1])).is_equal_to(expected3);
}

// Checks that scan_segment() reads all records from a full buffer with varying serialized sizes.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_scan_reads_full_buffer_records_with_varying_lengths) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(64 * 1024, segment_kind::full);
    auto expected0 = make_kv_mutation(schema, "pk-full-0", "x", api::timestamp_type(31));
    auto expected1 = make_kv_mutation(schema, "pk-full-1-with-longer-key", "medium-value", api::timestamp_type(32));
    auto expected2 = make_kv_mutation(schema, "pk-full-2", "value-with-a-significantly-longer-payload-to-exercise-varying-record-sizes", api::timestamp_type(33));
    wb.append(log_record_writer(make_log_record(schema, "pk-full-0", "x", api::timestamp_type(31))));
    wb.append(log_record_writer(make_log_record(schema, "pk-full-1-with-longer-key", "medium-value", api::timestamp_type(32))));
    wb.append(log_record_writer(make_log_record(schema, "pk-full-2", "value-with-a-significantly-longer-payload-to-exercise-varying-record-sizes", api::timestamp_type(33))));
    wb.seal(segment_sequence{41}, schema->id(), ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    auto maybe_header = read_segment_header_from_bytes(serialized);
    auto in = seastar::util::as_input_stream(std::move(serialized));

    std::vector<segment_header> seen_segment_headers;
    std::vector<log_record> seen_records;

    scan_segment(in, log_segment_id{7}, wb.serialized_size(),
        [&seen_segment_headers] (const segment_header& sh) {
            seen_segment_headers.push_back(sh);
            return make_ready_future<>();
        },
        [] (log_location, const log_record_header&) {
            return want_data::yes;
        },
        [&seen_records] (log_location, log_record rec) {
            seen_records.push_back(std::move(rec));
            return make_ready_future<>();
        }).get();
    in.close().get();

    BOOST_REQUIRE_EQUAL(seen_segment_headers.size(), 1u);
    BOOST_REQUIRE(seen_segment_headers.front().kind == segment_kind::full);
    BOOST_REQUIRE_EQUAL(seen_segment_headers.front().segment_seq.value, 41u);
    BOOST_REQUIRE(std::holds_alternative<segment_header::full>(seen_segment_headers.front().v));
    BOOST_REQUIRE_EQUAL(seen_records.size(), 3u);
    BOOST_REQUIRE_EQUAL(seen_records[0].header.table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_records[1].header.table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_records[2].header.table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_records[0].header.timestamp, api::timestamp_type(31));
    BOOST_REQUIRE_EQUAL(seen_records[1].header.timestamp, api::timestamp_type(32));
    BOOST_REQUIRE_EQUAL(seen_records[2].header.timestamp, api::timestamp_type(33));
    assert_that(to_mutation(schema, seen_records[0])).is_equal_to(expected0);
    assert_that(to_mutation(schema, seen_records[1])).is_equal_to(expected1);
    assert_that(to_mutation(schema, seen_records[2])).is_equal_to(expected2);

    BOOST_REQUIRE(maybe_header);
    BOOST_REQUIRE(maybe_header->kind == segment_kind::full);
    BOOST_REQUIRE_EQUAL(maybe_header->segment_seq.value, 41u);
    BOOST_REQUIRE(std::holds_alternative<segment_header::full>(maybe_header->v));
    auto& full = std::get<segment_header::full>(maybe_header->v);
    auto expected_first_token = std::min({
        seen_records[0].header.key.dk.token(),
        seen_records[1].header.key.dk.token(),
        seen_records[2].header.key.dk.token(),
    });
    auto expected_last_token = std::max({
        seen_records[0].header.key.dk.token(),
        seen_records[1].header.key.dk.token(),
        seen_records[2].header.key.dk.token(),
    });
    BOOST_REQUIRE_EQUAL(full.table, schema->id());
    BOOST_REQUIRE_EQUAL(full.first_token, expected_first_token);
    BOOST_REQUIRE_EQUAL(full.last_token, expected_last_token);
}

// Checks that scan_segment() stops before a later mixed buffer whose sequence number is lower.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_scan_stops_on_mixed_buffer_lower_sequence_number) {
    auto schema = make_kv_schema();

    raw_write_buffer wb0(64 * 1024, segment_kind::mixed);
    raw_write_buffer wb1(64 * 1024, segment_kind::mixed);

    auto expected0 = make_kv_mutation(schema, "pk0", "value0", api::timestamp_type(51));
    auto expected1 = make_kv_mutation(schema, "pk1", "value1", api::timestamp_type(52));
    wb0.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(51))));
    wb0.append(log_record_writer(make_log_record(schema, "pk1", "value1", api::timestamp_type(52))));
    wb1.append(log_record_writer(make_log_record(schema, "pk2", "value2", api::timestamp_type(53))));
    wb1.append(log_record_writer(make_log_record(schema, "pk3", "value3", api::timestamp_type(54))));

    wb0.seal(segment_sequence{61}, std::nullopt, ondisk::block_alignment);
    wb1.seal(segment_sequence{60}, std::nullopt, ondisk::block_alignment);

    auto serialized0 = make_serialized_buffer_copy(wb0);
    auto serialized1 = make_serialized_buffer_copy(wb1);
    auto segment = concat_serialized_buffers({&serialized0, &serialized1});
    const auto segment_size = segment.size();
    auto in = seastar::util::as_input_stream(std::move(segment));

    std::vector<segment_header> seen_segment_headers;
    std::vector<log_record_header> seen_record_headers;
    std::vector<mutation> seen_mutations;

    scan_segment(in, log_segment_id{5}, segment_size,
        [&seen_segment_headers] (const segment_header& sh) {
            seen_segment_headers.push_back(sh);
            return make_ready_future<>();
        },
        [&seen_record_headers] (log_location, const log_record_header& rh) {
            seen_record_headers.push_back(rh);
            return want_data::yes;
        },
        [&seen_mutations, schema] (log_location, log_record rec) {
            seen_mutations.push_back(to_mutation(schema, rec));
            return make_ready_future<>();
        }).get();
    in.close().get();

    BOOST_REQUIRE_EQUAL(seen_segment_headers.size(), 1u);
    BOOST_REQUIRE(seen_segment_headers.front().kind == segment_kind::mixed);
    BOOST_REQUIRE_EQUAL(seen_segment_headers.front().segment_seq.value, 61u);

    BOOST_REQUIRE_EQUAL(seen_record_headers.size(), 2u);
    BOOST_REQUIRE_EQUAL(seen_record_headers[0].timestamp, api::timestamp_type(51));
    BOOST_REQUIRE_EQUAL(seen_record_headers[1].timestamp, api::timestamp_type(52));

    BOOST_REQUIRE_EQUAL(seen_mutations.size(), 2u);
    assert_that(seen_mutations[0]).is_equal_to(expected0);
    assert_that(seen_mutations[1]).is_equal_to(expected1);
}

// Checks that scan_segment() stops after a later mixed buffer with a corrupted header crc.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_scan_stops_on_corrupted_later_mixed_buffer_header) {
    auto schema = make_kv_schema();

    raw_write_buffer wb0(64 * 1024, segment_kind::mixed);
    raw_write_buffer wb1(64 * 1024, segment_kind::mixed);

    auto expected0 = make_kv_mutation(schema, "pk0", "value0", api::timestamp_type(91));
    auto expected1 = make_kv_mutation(schema, "pk1", "value1", api::timestamp_type(92));

    wb0.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(91))));
    wb0.append(log_record_writer(make_log_record(schema, "pk1", "value1", api::timestamp_type(92))));
    wb1.append(log_record_writer(make_log_record(schema, "pk2", "value2", api::timestamp_type(93))));
    wb1.append(log_record_writer(make_log_record(schema, "pk3", "value3", api::timestamp_type(94))));

    wb0.seal(segment_sequence{101}, std::nullopt, ondisk::block_alignment);
    wb1.seal(segment_sequence{101}, std::nullopt, ondisk::block_alignment);

    auto serialized0 = make_serialized_buffer_copy(wb0);
    auto serialized1 = make_serialized_buffer_copy(wb1);
    flip_byte(serialized1, ondisk::buffer_header_size - sizeof(uint32_t));

    auto segment = concat_serialized_buffers({&serialized0, &serialized1});
    const auto segment_size = segment.size();
    auto in = seastar::util::as_input_stream(std::move(segment));

    std::vector<segment_header> seen_segment_headers;
    std::vector<log_record_header> seen_record_headers;
    std::vector<mutation> seen_mutations;

    scan_segment(in, log_segment_id{6}, segment_size,
        [&seen_segment_headers] (const segment_header& sh) {
            seen_segment_headers.push_back(sh);
            return make_ready_future<>();
        },
        [&seen_record_headers] (log_location, const log_record_header& rh) {
            seen_record_headers.push_back(rh);
            return want_data::yes;
        },
        [&seen_mutations, schema] (log_location, log_record rec) {
            seen_mutations.push_back(to_mutation(schema, rec));
            return make_ready_future<>();
        }).get();
    in.close().get();

    BOOST_REQUIRE_EQUAL(seen_segment_headers.size(), 1u);
    BOOST_REQUIRE_EQUAL(seen_record_headers.size(), 2u);
    BOOST_REQUIRE_EQUAL(seen_mutations.size(), 2u);
    BOOST_REQUIRE_EQUAL(seen_record_headers[0].timestamp, api::timestamp_type(91));
    BOOST_REQUIRE_EQUAL(seen_record_headers[1].timestamp, api::timestamp_type(92));
    assert_that(seen_mutations[0]).is_equal_to(expected0);
    assert_that(seen_mutations[1]).is_equal_to(expected1);
}

// Checks that the rewriter updates the initial full-buffer header sequence number.
SEASTAR_THREAD_TEST_CASE(test_logstor_streamed_segment_rewriter_rewrites_initial_full_buffer_header) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(64 * 1024, segment_kind::full);
    auto expected0 = make_kv_mutation(schema, "pk0", "value0", api::timestamp_type(201));
    auto expected1 = make_kv_mutation(schema, "pk1-longer-key", "value1", api::timestamp_type(202));
    auto expected2 = make_kv_mutation(schema, "pk2", "value2-with-a-longer-payload", api::timestamp_type(203));
    wb.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(201))));
    wb.append(log_record_writer(make_log_record(schema, "pk1-longer-key", "value1", api::timestamp_type(202))));
    wb.append(log_record_writer(make_log_record(schema, "pk2", "value2-with-a-longer-payload", api::timestamp_type(203))));
    wb.seal(segment_sequence{211}, schema->id(), ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    auto rewritten = rewrite_streamed_segment(log_segment_id{33}, segment_sequence{221}, std::span(&serialized, 1));
    auto bh = read_buffer_header(rewritten.data);

    auto rewritten_size = rewritten.data.size();
    auto in = seastar::util::as_input_stream(rewritten.data.share());
    std::vector<segment_header> seen_segment_headers;
    std::vector<log_record> seen_records;

    scan_segment(in, log_segment_id{33}, rewritten_size,
        [&seen_segment_headers] (const segment_header& sh) {
            seen_segment_headers.push_back(sh);
            return make_ready_future<>();
        },
        [] (log_location, const log_record_header&) {
            return want_data::yes;
        },
        [&seen_records] (log_location, log_record rec) {
            seen_records.push_back(std::move(rec));
            return make_ready_future<>();
        }).get();
    in.close().get();

    BOOST_REQUIRE_EQUAL(rewritten.write_count, 1u);
    BOOST_REQUIRE(ondisk::validate_header(bh));
    BOOST_REQUIRE_EQUAL(bh.segment_seq.value, 221u);
    BOOST_REQUIRE_EQUAL(seen_segment_headers.size(), 1u);
    BOOST_REQUIRE(seen_segment_headers.front().kind == segment_kind::full);
    BOOST_REQUIRE_EQUAL(seen_segment_headers.front().segment_seq.value, 221u);
    BOOST_REQUIRE(std::holds_alternative<segment_header::full>(seen_segment_headers.front().v));
    BOOST_REQUIRE_EQUAL(seen_records.size(), 3u);
    BOOST_REQUIRE_EQUAL(seen_records[0].header.timestamp, api::timestamp_type(201));
    BOOST_REQUIRE_EQUAL(seen_records[1].header.timestamp, api::timestamp_type(202));
    BOOST_REQUIRE_EQUAL(seen_records[2].header.timestamp, api::timestamp_type(203));
    BOOST_REQUIRE_EQUAL(seen_records[0].header.table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_records[1].header.table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_records[2].header.table, schema->id());
    assert_that(to_mutation(schema, seen_records[0])).is_equal_to(expected0);
    assert_that(to_mutation(schema, seen_records[1])).is_equal_to(expected1);
    assert_that(to_mutation(schema, seen_records[2])).is_equal_to(expected2);
}

// Checks that the rewriter can wait for a fragmented initial header before rewriting it.
SEASTAR_THREAD_TEST_CASE(test_logstor_streamed_segment_rewriter_handles_fragmented_initial_header) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(64 * 1024, segment_kind::mixed);
    wb.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(231))));
    wb.seal(segment_sequence{241}, std::nullopt, ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    auto split = ondisk::buffer_header_size - 1;
    std::vector<temporary_buffer<char>> chunks;
    chunks.push_back(slice_buffer(serialized, 0, split));
    chunks.push_back(slice_buffer(serialized, split, serialized.size() - split));

    auto rewritten = rewrite_streamed_segment(log_segment_id{35}, segment_sequence{251}, chunks);
    auto bh = read_buffer_header(rewritten.data);

    BOOST_REQUIRE_EQUAL(rewritten.write_count, 1u);
    BOOST_REQUIRE_EQUAL(bh.segment_seq.value, 251u);
}

// Checks that the rewriter rejects a stream whose initial buffer header is corrupted.
SEASTAR_THREAD_TEST_CASE(test_logstor_streamed_segment_rewriter_rejects_invalid_initial_header) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(64 * 1024, segment_kind::mixed);
    wb.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(291))));
    wb.seal(segment_sequence{301}, std::nullopt, ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    flip_byte(serialized, 0);

    BOOST_REQUIRE_THROW(rewrite_streamed_segment(log_segment_id{39}, segment_sequence{311}, std::span(&serialized, 1)), std::runtime_error);
}

// Checks that the rewriter rejects streams that end before the initial header is complete.
SEASTAR_THREAD_TEST_CASE(test_logstor_streamed_segment_rewriter_rejects_truncated_initial_header) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(64 * 1024, segment_kind::mixed);
    wb.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(321))));
    wb.seal(segment_sequence{331}, std::nullopt, ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    auto truncated = slice_buffer(serialized, 0, ondisk::buffer_header_size - 1);

    BOOST_REQUIRE_THROW(rewrite_streamed_segment(log_segment_id{41}, segment_sequence{341}, std::span(&truncated, 1)), std::runtime_error);
}

// Checks that buffered_writer forwards records to the flush callback and resolves returned locations.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_basic_flushes_records) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;

    test_flush_controller flush_ctl;
    buffered_writer writer(make_buffered_writer_config(buffer_size, 3), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
        writer.stop().get();
    });

    std::vector<log_record> expected;
    std::vector<future<log_location_with_holder>> persisted;
    for (size_t i = 0; i < 3; ++i) {
        auto record = make_buffered_writer_record(schema, i, sstring(fmt::format("value{}", i)), api::timestamp_type(100 + i));
        expected.push_back(record);
        auto accepted = writer.write_to_buffer(log_record_writer(record), test_timeout()).get();
        persisted.push_back(std::move(accepted.persisted));
    }

    std::vector<log_location> locations;
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
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_flushes_partial_buffer_with_large_sync_period) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    constexpr auto sync_period = std::chrono::hours(1);

    test_flush_controller flush_ctl;
    buffered_writer writer(make_buffered_writer_config(buffer_size, 3, 0, sync_period), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
        writer.stop().get();
    });

    std::vector<log_record> expected;
    std::vector<future<log_location_with_holder>> persisted;
    for (size_t i = 0; i < 3; ++i) {
        auto record = make_buffered_writer_record(schema, i, sstring(fmt::format("value{}", i)), api::timestamp_type(150 + i));
        expected.push_back(record);
        auto accepted = writer.write_to_buffer(log_record_writer(record), test_timeout()).get();
        persisted.push_back(std::move(accepted.persisted));
    }

    BOOST_REQUIRE(flush_ctl.flushed_buffers.empty());

    auto flush = writer.flush();
    flush_ctl.wait_for_flush_starts(1);

    std::vector<log_location> locations;
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
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_paused_flush_fills_ring_then_drains) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
        writer.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 4; ++i) {
        expected.push_back(make_buffered_writer_record(schema, i, large_value, api::timestamp_type(200 + i)));
    }

    auto accepted0 = writer.write_to_buffer(log_record_writer(expected[0]), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);
    BOOST_REQUIRE(!persisted0.available());

    auto accepted1 = writer.write_to_buffer(log_record_writer(expected[1]), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = writer.write_to_buffer(log_record_writer(expected[2]), test_timeout());
    auto queued3 = writer.write_to_buffer(log_record_writer(expected[3]), test_timeout());
    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 2u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);

    auto accepted2 = queued2.get();
    auto persisted2 = std::move(accepted2.persisted);
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 1u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);

    auto accepted3 = queued3.get();
    auto persisted3 = std::move(accepted3.persisted);
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 0u);

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
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_queued_write_keeps_its_write_target) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    // Outlive the writer, so that the holders of the queued write are released before the gates
    // they were taken from are destroyed.
    seastar::gate non_empty_gate;
    seastar::gate alive_gate;

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
        writer.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 3; ++i) {
        expected.push_back(make_buffered_writer_record(schema, i, large_value, api::timestamp_type(300 + i)));
    }

    // Fill the ring: the first record is being flushed and the second holds the only other buffer.
    auto accepted0 = writer.write_to_buffer(log_record_writer(expected[0]), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);
    auto accepted1 = writer.write_to_buffer(log_record_writer(expected[1]), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    // The third record has nowhere to go, so it is queued rather than appended.
    auto queued = writer.write_to_buffer(log_record_writer(expected[2]), test_timeout(), write_target{
            .cg = nullptr,
            .non_empty_holder = non_empty_gate.hold(),
            .alive_holder = alive_gate.hold(),
    });
    BOOST_REQUIRE(!queued.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 1u);

    // The write is in flight, so its group is still held by both of the target's holders. Checked
    // rather than required, so that a failure here still drains the writer below: a queued write
    // left behind would hang the stop() of this test rather than fail it.
    BOOST_CHECK_EQUAL(non_empty_gate.get_count(), 1u);
    BOOST_CHECK_EQUAL(alive_gate.get_count(), 1u);

    // Let the queued record through and drain everything, which releases the holders.
    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);

    auto accepted2 = queued.get();
    auto persisted2 = std::move(accepted2.persisted);
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 0u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that queued writes are accepted and persisted in FIFO order once capacity becomes available.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_queued_writes_preserve_fifo_order) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
        writer.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 5; ++i) {
        expected.push_back(make_buffered_writer_record(schema, i, large_value, api::timestamp_type(300 + i)));
    }

    auto accepted0 = writer.write_to_buffer(log_record_writer(expected[0]), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto accepted1 = writer.write_to_buffer(log_record_writer(expected[1]), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = writer.write_to_buffer(log_record_writer(expected[2]), test_timeout());
    auto queued3 = writer.write_to_buffer(log_record_writer(expected[3]), test_timeout());
    auto queued4 = writer.write_to_buffer(log_record_writer(expected[4]), test_timeout());

    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE(!queued4.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 3u);

    // FIFO acceptance is proven by the !available() checks after each drain step below:
    // each step frees exactly one buffer, and only the oldest queued write may be
    // accepted into it while every later one stays pending.

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);
    auto accepted2 = queued2.get();
    auto persisted2 = std::move(accepted2.persisted);
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE(!queued4.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    auto accepted3 = queued3.get();
    auto persisted3 = std::move(accepted3.persisted);
    BOOST_REQUIRE(!queued4.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    flush_ctl.wait_for_flush_starts(4);
    auto accepted4 = queued4.get();
    auto persisted4 = std::move(accepted4.persisted);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted3);
    flush_ctl.wait_for_flush_starts(5);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted4);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that queued writes are rejected once max_queued_write_bytes would be exceeded.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_rejects_when_max_queued_write_bytes_exceeded) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    auto record0 = make_buffered_writer_record(schema, 0, large_value, api::timestamp_type(400));
    const auto queued_write_budget = log_record_writer(record0).size() * 2;

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2, queued_write_budget), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
        writer.stop().get();
    });

    std::vector<log_record> expected{record0};
    for (size_t i = 1; i < 4; ++i) {
        expected.push_back(make_buffered_writer_record(schema, i, large_value, api::timestamp_type(400 + i)));
    }

    auto accepted0 = writer.write_to_buffer(log_record_writer(expected[0]), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto accepted1 = writer.write_to_buffer(log_record_writer(expected[1]), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = writer.write_to_buffer(log_record_writer(expected[2]), test_timeout());
    auto queued3 = writer.write_to_buffer(log_record_writer(expected[3]), test_timeout());
    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 2u);

    auto rejected = make_buffered_writer_record(schema, 4, large_value, api::timestamp_type(404));
    BOOST_REQUIRE_THROW(writer.write_to_buffer(log_record_writer(rejected), test_timeout()).get(), replica::rate_limit_exception);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);
    auto accepted2 = queued2.get();
    auto persisted2 = std::move(accepted2.persisted);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    auto accepted3 = queued3.get();
    auto persisted3 = std::move(accepted3.persisted);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    flush_ctl.wait_for_flush_starts(4);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted3);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that write_to_buffer() stays pending while a request is queued and only persistence remains pending once it is accepted.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_acceptance_stays_blocked_while_write_is_queued) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
        writer.stop().get();
    });

    auto record0 = make_buffered_writer_record(schema, 0, large_value, api::timestamp_type(500));
    auto record1 = make_buffered_writer_record(schema, 1, large_value, api::timestamp_type(501));
    auto record2 = make_buffered_writer_record(schema, 2, large_value, api::timestamp_type(502));

    auto accepted0 = writer.write_to_buffer(log_record_writer(record0), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto accepted1 = writer.write_to_buffer(log_record_writer(record1), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = writer.write_to_buffer(log_record_writer(record2), test_timeout());
    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 1u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);

    auto accepted2 = queued2.get();
    BOOST_REQUIRE(!accepted2.persisted.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    BOOST_REQUIRE(!accepted2.persisted.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(accepted2.persisted);

    const auto actual = flush_ctl.all_records();
    const std::vector<log_record> expected{record0, record1, record2};
    assert_records_in_order(schema, actual, expected);
}

// Checks that stop() waits for a blocked in-flight flush instead of dropping it.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_stop_drains_blocked_in_flight_write) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto record = make_buffered_writer_record(schema, 0, "value", api::timestamp_type(600));

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();

    auto accepted = writer.write_to_buffer(log_record_writer(record), test_timeout()).get();
    auto persisted = std::move(accepted.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto stop_fut = writer.stop();
    BOOST_REQUIRE(!stop_fut.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted);
    stop_fut.get();

    const auto actual = flush_ctl.all_records();
    BOOST_REQUIRE_EQUAL(actual.size(), 1u);
    assert_log_record_matches(schema, actual.front().record, record);
}

// Checks that a flush failure is propagated to every write that was already accepted into that buffer.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_flush_failure_fails_all_writes_in_buffer) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;

    auto record0 = make_buffered_writer_record(schema, 0, "value0", api::timestamp_type(700));
    auto record1 = make_buffered_writer_record(schema, 1, "value1", api::timestamp_type(701));
    raw_write_buffer scratch(buffer_size, segment_kind::mixed);
    BOOST_REQUIRE(scratch.can_fit(log_record_writer(record0)));
    scratch.append(log_record_writer(record0));
    BOOST_REQUIRE(scratch.can_fit(log_record_writer(record1)));

    test_flush_controller flush_ctl{.fail_flush_index = 0};
    // long sync period so both writes land in the same buffer.
    buffered_writer writer(make_buffered_writer_config(buffer_size, 3, 0, std::chrono::hours(1)), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
        writer.stop().get();
    });

    auto accepted0 = writer.write_to_buffer(log_record_writer(record0), test_timeout()).get();
    auto accepted1 = writer.write_to_buffer(log_record_writer(record1), test_timeout()).get();

    auto flush = writer.flush();

    BOOST_REQUIRE_THROW(accepted0.persisted.get(), std::runtime_error);
    BOOST_REQUIRE_THROW(accepted1.persisted.get(), std::runtime_error);

    flush.get();

    BOOST_REQUIRE_EQUAL(flush_ctl.started_count, 1u);
    BOOST_REQUIRE(flush_ctl.flushed_buffers.empty());
}

// A device error reported by the file layer on a segment write must fire the logstor disk error
// signal, on top of failing the write. The signal is what makes the node isolate itself instead of
// staying in the ring with a store it can no longer write to.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_write_io_error_signals_disk_error) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return;
    }

    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);

    unsigned signalled = 0;
    boost::signals2::scoped_connection conn = logstor_error.connect([&signalled] { ++signalled; });

    utils::get_local_injector().enable("logstor_segment_write_io_error", true /* one shot */);
    auto m = make_kv_mutation(schema, "pk0", "io-error-value");
    BOOST_REQUIRE_THROW(ls.write(m, write_target(&cg, {}), db::no_timeout).get(), storage_io_error);
    BOOST_REQUIRE_EQUAL(signalled, 1u);

    // The failed write only retired its segment, so a write to a new segment still succeeds and
    // does not signal again.
    write_and_flush_segment(ls, cg, make_kv_mutation(schema, "pk1", "value-after-io-error"));
    BOOST_REQUIRE_EQUAL(signalled, 1u);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_write_and_separator_flush) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);

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
    // The flush released everything the buffer was holding, which is also what gives it back to the
    // pool: a group with nothing to separate holds no buffer.
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 0u);
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

namespace {

logstor_params direct_write_params(logstor_params params = {}) {
    params.direct_group_writes = true;
    return params;
}

// Waits for something the shard does on its own - the reserve replenisher building segments, the
// direct write controller promoting or demoting a group - rather than for a future of it.
template <typename Predicate>
void await_until(Predicate&& predicate) {
    for (unsigned i = 0; i < 200 && !predicate(); ++i) {
        seastar::sleep(std::chrono::milliseconds(10)).get();
    }
    BOOST_REQUIRE(predicate());
}

// A group is given its buffers by the controller, once it has measured that the group writes fast
// enough. A test of what the direct path does with a record, rather than of when a group is given
// one, asks for them here instead of writing at a rate for a while.
//
// A promotion is refused rather than queued when the reserve has no segment for the group's
// buffers, which right after start() it may not have built yet, so this asks until it is given.
void await_direct_buffers(logstor& ls, test_logstor_group& cg) {
    await_until([&ls, &cg] {
        ls.get_compaction_manager().promote_direct_writes_for_test(cg).get();
        return cg.direct_writes_enabled();
    });
}

}

// The point of the direct path: a record goes into a buffer that is already bound to a segment of
// its group, so its location in that segment is known when it is written and never changes. Checks
// that the record is readable from the moment it is acknowledged, while it is still only in memory,
// that the separator is given nothing, and that writing the buffer out leaves the index alone.
SEASTAR_THREAD_TEST_CASE(test_logstor_direct_write_is_readable_before_it_reaches_the_disk) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), direct_write_params()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    await_direct_buffers(ls, cg);

    auto expected = make_kv_mutation(schema, "pk0", "direct-value");
    auto key = expected.decorated_key();

    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();

    BOOST_REQUIRE(cg.direct_has_data());
    BOOST_REQUIRE(!cg.separator_has_data());
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 0u);
    BOOST_REQUIRE(!cg.empty());

    auto entry = cg.logstor_index().get(primary_index_key{key});
    BOOST_REQUIRE(entry);

    // Read out of the buffer: the segment the index points at holds nothing yet.
    auto before_flush = ls.read(*schema, cg.logstor_index(), key, schema->full_slice()).get();
    BOOST_REQUIRE(before_flush);
    assert_that(*before_flush).is_equal_to(expected);

    cg.flush_direct_writes().get();

    BOOST_REQUIRE(!cg.direct_has_data());
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);
    BOOST_REQUIRE(!cg.empty());

    auto entry_after = cg.logstor_index().get(primary_index_key{key});
    BOOST_REQUIRE(entry_after);
    BOOST_REQUIRE(entry_after->location == entry->location);

    auto snapshot = ls.get_segment_manager().make_snapshot(cg).get();
    BOOST_REQUIRE_EQUAL(snapshot.size(), 1u);
    BOOST_REQUIRE_EQUAL(entry_after->location.segment.value, snapshot.front().segment_id.value);

    // The same read, now from the disk.
    auto after_flush = ls.read(*schema, cg.logstor_index(), key, schema->full_slice()).get();
    BOOST_REQUIRE(after_flush);
    assert_that(*after_flush).is_equal_to(expected);
}

// Checks that a group goes on taking direct writes across the flushes: each one writes the buffer
// out, links its segment into the group and binds a fresh buffer and segment to write into next.
SEASTAR_THREAD_TEST_CASE(test_logstor_direct_writes_continue_across_flushes) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), direct_write_params()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    await_direct_buffers(ls, cg);

    constexpr unsigned record_count = 3;
    std::vector<mutation> expected;
    for (unsigned i = 0; i < record_count; ++i) {
        expected.push_back(make_kv_mutation(schema, format("pk{}", i), format("value{}", i)));
        ls.write(expected.back(), write_target(&cg, {}), db::no_timeout).get();
        cg.flush_direct_writes().get();
        BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), i + 1);
    }

    BOOST_REQUIRE(!cg.separator_has_data());
    for (const auto& m : expected) {
        auto actual = ls.read(*schema, cg.logstor_index(), m.decorated_key(), schema->full_slice()).get();
        BOOST_REQUIRE(actual);
        assert_that(*actual).is_equal_to(m);
    }
}

// Checks that records keep going in once the buffer they were going into is full: the group rotates
// into its spare and writes the full one out behind it. Whatever a rotation cannot take falls back
// to the ordinary path, so what this defends is that every record ends up readable either way.
SEASTAR_THREAD_TEST_CASE(test_logstor_direct_writes_rotate_when_the_buffer_fills) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), direct_write_params()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    await_direct_buffers(ls, cg);

    // Four records of a third of a segment each: the third one does not fit the buffer the first
    // two went into.
    const auto record_size = ls.get_segment_manager().get_segment_size() / 3;
    constexpr unsigned record_count = 4;
    std::vector<mutation> expected;
    for (unsigned i = 0; i < record_count; ++i) {
        expected.push_back(make_kv_mutation_of_record_size(schema, format("pk{}", i), record_size));
        ls.write(expected.back(), write_target(&cg, {}), db::no_timeout).get();
    }

    ls.flush_to_separator().get();
    cg.flush_separator().get();

    BOOST_REQUIRE(!cg.direct_has_data());
    BOOST_REQUIRE(!cg.separator_has_data());
    BOOST_REQUIRE_GE(cg.logstor_segments().segment_count(), 2u);

    for (const auto& m : expected) {
        auto actual = ls.read(*schema, cg.logstor_index(), m.decorated_key(), schema->full_slice()).get();
        BOOST_REQUIRE(actual);
        assert_that(*actual).is_equal_to(m);
    }
}

// A record overwritten while both copies are still in the same buffer is freed at a location in a
// segment that has not been written yet, and its descriptor is only linked into the group once the
// buffer reaches the disk. Checks that the space accounting of the segment survives that ordering.
SEASTAR_THREAD_TEST_CASE(test_logstor_direct_write_overwritten_in_its_buffer_is_accounted_for) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), direct_write_params()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    await_direct_buffers(ls, cg);

    auto first = make_kv_mutation(schema, "pk0", "first", api::timestamp_type(1));
    auto second = make_kv_mutation(schema, "pk0", "second", api::timestamp_type(2));
    auto other = make_kv_mutation(schema, "pk1", "other", api::timestamp_type(1));

    ls.write(first, write_target(&cg, {}), db::no_timeout).get();
    ls.write(second, write_target(&cg, {}), db::no_timeout).get();
    ls.write(other, write_target(&cg, {}), db::no_timeout).get();

    cg.flush_direct_writes().get();
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);

    const auto stats = cg.logstor_segments().stats();
    const auto recomputed = cg.logstor_segments().recompute_stats_for_test();
    BOOST_REQUIRE_EQUAL(recomputed.segment_count, stats.segment_count);
    BOOST_REQUIRE_EQUAL(recomputed.live_bytes, stats.live_bytes);
    BOOST_REQUIRE(recomputed.utilization == stats.utilization);

    auto actual = ls.read(*schema, cg.logstor_index(), second.decorated_key(), schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(second);
}

// A direct write buffer is bound to a segment, and there is not always a segment to be had. What
// the flush of a buffer means has to stay "the records are on the disk and their segment is in the
// group" and not "and the next segment is in hand": everything that drains a group waits for that
// future - a table flush, a split, a snapshot, shutdown - so a flush that waited for disk space
// would hang all of them on a shard whose segments have run out.
//
// Sizes the shard so that promoting the group takes the last two segments the reserve can hand out,
// then fills the group's buffer. The rotation's flush finds nothing to bind its next buffer to, and
// has to complete anyway.
SEASTAR_THREAD_TEST_CASE(test_logstor_direct_write_flush_does_not_wait_for_its_next_segment) {
    auto schema = make_kv_schema();
    tmpdir dir;

    // Eleven segments in one file: one goes to the shared active segment at start, eight are the
    // compaction reserve, and the two that are left are exactly what one hot group takes.
    auto params = direct_write_params();
    params.file_size = 11 * params.segment_size;
    params.disk_size = params.file_size;
    params.compaction_enabled = false;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), params), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    await_direct_buffers(ls, cg);

    // The premise of the test: all that is left is the compaction reserve, which the direct path
    // may not take from, so nothing below can be given a segment.
    BOOST_REQUIRE_EQUAL(ls.get_segment_manager().get_usage().free_segments, max_compaction_parallelism);

    // Three records of a third of a segment each: the third one does not fit the buffer the first
    // two went into, so the group rotates into its spare and writes the full buffer out.
    const auto record_size = ls.get_segment_manager().get_segment_size() / 3;
    std::vector<mutation> expected;
    for (unsigned i = 0; i < 3; ++i) {
        expected.push_back(make_kv_mutation_of_record_size(schema, format("pk{}", i), record_size));
        ls.write(expected.back(), write_target(&cg, {}), db::no_timeout).get();
    }
    BOOST_REQUIRE(cg.direct_has_data());

    // The drain every tablet operation goes through. It has to return even though the shard has no
    // segment to give the group for its next buffer.
    cg.flush_direct_writes().get();

    BOOST_REQUIRE(!cg.direct_has_data());
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 2u);

    for (const auto& m : expected) {
        auto actual = ls.read(*schema, cg.logstor_index(), m.decorated_key(), schema->full_slice()).get();
        BOOST_REQUIRE(actual);
        assert_that(*actual).is_equal_to(m);
    }
}

// The buffer of a flush that failed is kept in memory for good - its records were acknowledged and
// this is the only copy of them left - and its segment is never reclaimed. A device that keeps
// refusing the writes of a hot group would therefore cost the shard a buffer and a segment every
// period, for records that are not reaching the disk anyway. Checks that a few failures in a row
// put the group back on the ordinary path, which reports a failed write to its caller instead.
SEASTAR_THREAD_TEST_CASE(test_logstor_direct_writes_of_a_group_stop_after_repeated_flush_failures) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return;
    }

    auto schema = make_kv_schema();
    tmpdir dir;

    auto params = direct_write_params();
    // Short enough that the sync fiber's pass over the groups - which is what takes the buffers
    // back - comes around during the test, and long enough that its controller does not run in the
    // middle of it and demote the group for a quiet period instead.
    params.direct_sync_period = std::chrono::milliseconds(200);

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), params), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    await_direct_buffers(ls, cg);

    utils::get_local_injector().enable("logstor_fail_segment_write");
    auto disable_injection = seastar::defer([] noexcept {
        utils::get_local_injector().disable("logstor_fail_segment_write");
    });

    std::vector<mutation> expected;
    for (unsigned i = 0; i < direct_flush_failures_before_demotion; ++i) {
        expected.push_back(make_kv_mutation(schema, format("pk{}", i), "never-reaches-the-disk"));
        ls.write(expected.back(), write_target(&cg, {}), db::no_timeout).get();
        // Does not fail: there is nothing a caller of the drain can do about a record whose only
        // copy is the buffer it is already in.
        cg.flush_direct_writes().get();
    }

    await_until([&cg] { return !cg.direct_writes_enabled(); });
    BOOST_REQUIRE(!cg.direct_has_data());
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 0u);

    // The whole point of keeping those buffers: the records the disk refused are still where the
    // index says they are, and reads are served out of memory.
    for (const auto& m : expected) {
        auto actual = ls.read(*schema, cg.logstor_index(), m.decorated_key(), schema->full_slice()).get();
        BOOST_REQUIRE(actual);
        assert_that(*actual).is_equal_to(m);
    }
}

// Truncating a group clears its index and then discards its segments. A record the group took
// directly is in neither: not in a segment, because its buffer has not been written out, and not in
// the index any more. Checks that the buffer is written out here rather than left to be written out
// later into a group that was emptied on purpose - the segment it produces holds only dead records,
// and the discard frees it along with the rest.
SEASTAR_THREAD_TEST_CASE(test_logstor_discarding_a_group_writes_out_what_it_took_directly) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), direct_write_params()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    const auto free_segments_before = ls.get_segment_manager().get_usage().free_segments;
    await_direct_buffers(ls, cg);

    auto m = make_kv_mutation(schema, "pk0", "truncated-away");
    ls.write(m, write_target(&cg, {}), db::no_timeout).get();
    BOOST_REQUIRE(cg.direct_has_data());

    // What table::discard_logstor_segments() does: the index first, then the segments.
    cg.logstor_index().clear().get();
    ls.get_segment_manager().discard_segments(cg).get();

    BOOST_REQUIRE(!cg.direct_writes_enabled());
    BOOST_REQUIRE(!cg.direct_has_data());
    BOOST_REQUIRE(cg.empty());
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 0u);
    // The two segments the group's buffers were bound to are back, having never been written.
    BOOST_REQUIRE_EQUAL(ls.get_segment_manager().get_usage().free_segments, free_segments_before);
}

// A group is removed while it still holds records that are only in memory. Unlike the separator's
// buffers, which hold a second copy of records that are already on the disk, these are the only
// copy there is, so removing the group has to write them out rather than discard them.
SEASTAR_THREAD_TEST_CASE(test_logstor_removed_group_writes_out_what_it_took_directly) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), direct_write_params()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    await_direct_buffers(ls, cg);

    auto expected = make_kv_mutation(schema, "pk0", "unflushed");
    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
    BOOST_REQUIRE(cg.direct_has_data());

    // The group's destructor removes it too; removing twice must be harmless.
    ls.get_compaction_manager().remove(cg).get();

    BOOST_REQUIRE(!cg.direct_writes_enabled());
    BOOST_REQUIRE(!cg.direct_has_data());
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);

    auto actual = ls.read(*schema, cg.logstor_index(), expected.decorated_key(), schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// The same, at shutdown: stopping the store writes out what the groups took directly, and gives
// back the buffers and the segments that were bound to them but never written.
SEASTAR_THREAD_TEST_CASE(test_logstor_stop_writes_out_what_was_taken_directly) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), direct_write_params()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    bool stopped = false;
    auto stop_store = seastar::defer([&ls, &stopped] noexcept { if (!stopped) { ls.stop().get(); } });

    {
        test_logstor_group cg(schema, ls);
        await_direct_buffers(ls, cg);

        auto expected = make_kv_mutation(schema, "pk0", "unflushed");
        ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
        BOOST_REQUIRE(cg.direct_has_data());

        ls.stop().get();
        stopped = true;

        BOOST_REQUIRE(!cg.direct_has_data());
        BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);
    }
}

// Checks that a group discarded while the separator still holds unflushed records for it gives up
// what it was holding: its records are left where they are, and the segments they are in are left
// allocated. The index still points at them there, so freeing one would let it be reallocated over
// live records - free_segment() aborts on the live data it finds rather than allow that, so the
// reference has to be released as a failed flush rather than as a completed one.
SEASTAR_THREAD_TEST_CASE(test_logstor_discarded_group_does_not_free_its_unflushed_source_segments) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    auto& sm = ls.get_segment_manager();
    test_logstor_group cg(schema, ls);
    // Writes for this group fill the shared active segment, so that the segment holding the record
    // below is switched away from and drops the reference it holds itself. Its own records are
    // separated out, which gives back the reference its separator buffer takes.
    test_logstor_group filler(schema, ls);

    auto expected = make_kv_mutation(schema, "pk0", "separator-value");
    auto key = expected.decorated_key();
    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
    ls.flush_to_separator().get();
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 1u);

    auto entry_before_discard = cg.logstor_index().get(primary_index_key{key});
    BOOST_REQUIRE(entry_before_discard);

    // A value that takes most of a segment, so that the second record does not fit next to the first
    // and the active segment has to be switched.
    const auto filler_value = sstring(sm.get_segment_size() * 5 / 8, 'x');
    write_and_flush_segment(ls, filler, make_kv_mutation(schema, "fill0", filler_value));
    write_and_flush_segment(ls, filler, make_kv_mutation(schema, "fill1", filler_value));

    // The separator buffer of the group under test now holds the last reference to the segment its
    // record is in, and nothing has flushed it.
    BOOST_REQUIRE(cg.separator_has_data());
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 1u);

    // Discard the group the way a compaction group stopped while the separator still has records for
    // it does. Removing it rather than destroying it keeps its index readable below.
    ls.get_compaction_manager().remove(cg).get();

    // Nothing of the buffer is left held, which is also what gives it back to the pool.
    BOOST_REQUIRE(!cg.separator_has_data());
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 0u);

    // The records were never written out, so the index still points at them where they were, and the
    // segment that holds them was not freed and reused underneath it.
    auto entry_after_discard = cg.logstor_index().get(primary_index_key{key});
    BOOST_REQUIRE(entry_after_discard);
    BOOST_REQUIRE(entry_after_discard->location == entry_before_discard->location);

    auto actual = ls.read(*schema, cg.logstor_index(), key, schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// Checks that a group whose separator was closed takes no separator writes at all. Nothing can flush
// what such a group buffers, so a record handed to it afterwards - which is what a write that was
// already in flight when the group was closed amounts to - has to be refused rather than buffered.
// The write itself still succeeds and the record stays readable where it is.
SEASTAR_THREAD_TEST_CASE(test_logstor_closed_group_takes_no_separator_writes) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);

    // Close the group's separator the way stopping its compaction group does, and then write to it
    // anyway.
    ls.get_compaction_manager().remove(cg).get();

    auto expected = make_kv_mutation(schema, "pk0", "separator-value");
    auto key = expected.decorated_key();
    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
    ls.flush_to_separator().get();

    // The separator refused the record, so the group holds nothing: no buffer of its own, no reference
    // to the segment the record is in, and no segment of its own that it was written into.
    BOOST_REQUIRE(!cg.separator_has_data());
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 0u);
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 0u);

    // The write itself succeeded and the record is readable where it was written.
    BOOST_REQUIRE(cg.logstor_index().get(primary_index_key{key}));

    auto actual = ls.read(*schema, cg.logstor_index(), key, schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// Checks the two decisions the direct write controller makes: the threshold is a floor a group has
// to reach, and once it has buffers it keeps them through one quiet period, so that a momentary dip
// does not cost it a round trip through the buffer pool.
SEASTAR_THREAD_TEST_CASE(test_logstor_direct_write_promotion_and_demotion) {
    constexpr uint64_t threshold = 64 * 1024;

    BOOST_REQUIRE(!direct_promotion_wanted(0, threshold));
    BOOST_REQUIRE(!direct_promotion_wanted(threshold - 1, threshold));
    BOOST_REQUIRE(direct_promotion_wanted(threshold, threshold));
    BOOST_REQUIRE(direct_promotion_wanted(4 * threshold, threshold));

    // A group that keeps up is never demoted, however many periods it has been going.
    for (unsigned periods = 0; periods < 4; ++periods) {
        BOOST_TEST_CONTEXT("periods=" << periods) {
            BOOST_REQUIRE(!direct_demotion_wanted(threshold, threshold, periods));
        }
    }

    // Exactly the documented number of under-filled periods, not one fewer.
    static_assert(direct_underfilled_periods_before_demotion == 2);
    BOOST_REQUIRE(!direct_demotion_wanted(threshold - 1, threshold, 1));
    BOOST_REQUIRE(direct_demotion_wanted(threshold - 1, threshold, 2));
    BOOST_REQUIRE(direct_demotion_wanted(0, threshold, 3));
}

// Checks the controller end to end: a group that writes fast enough is given buffers of its own,
// and once it stops writing they are taken back - with what it had buffered written out first, and
// the segments they were bound to freed rather than left half full.
SEASTAR_THREAD_TEST_CASE(test_logstor_direct_write_controller_promotes_and_demotes_a_group) {
    auto schema = make_kv_schema();
    tmpdir dir;

    // A short period so the controller runs several times over the test, and a threshold of one
    // record, so that a single write is a group writing fast enough.
    auto params = direct_write_params();
    params.direct_sync_period = std::chrono::milliseconds(40);
    params.direct_hot_threshold_bytes = 1;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), params), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    const auto free_segments_before = ls.get_segment_manager().get_usage().free_segments;

    test_logstor_group cg(schema, ls);
    BOOST_REQUIRE(!cg.direct_writes_enabled());

    auto expected = make_kv_mutation(schema, "pk0", "controlled");
    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();

    // One period to measure the write, one tick to act on it.
    await_until([&cg] { return cg.direct_writes_enabled(); });

    // Write once more so that the record the group holds went in directly, then stop writing.
    auto second = make_kv_mutation(schema, "pk1", "controlled-too");
    ls.write(second, write_target(&cg, {}), db::no_timeout).get();
    BOOST_REQUIRE(cg.direct_has_data());

    // Idle: the deadline writes the buffer out, and two under-filled periods take the buffers back.
    await_until([&cg] { return !cg.direct_writes_enabled(); });

    BOOST_REQUIRE(!cg.direct_has_data());

    ls.flush_to_separator().get();
    cg.flush_separator().get();

    // One segment written directly and one written by the separator, at most: an empty buffer is
    // never sealed into a segment of its own.
    BOOST_REQUIRE_GE(cg.logstor_segments().segment_count(), 1u);
    BOOST_REQUIRE_LE(cg.logstor_segments().segment_count(), 2u);

    // Nothing is held back: the segments the buffers were bound to but never written into went back
    // to the pool along with the buffers, so the only segments the cycle cost are the group's own.
    const auto free_segments_after = ls.get_segment_manager().get_usage().free_segments;
    BOOST_REQUIRE_GE(free_segments_after + cg.logstor_segments().segment_count(), free_segments_before);

    for (const auto& m : {expected, second}) {
        auto actual = ls.read(*schema, cg.logstor_index(), m.decorated_key(), schema->full_slice()).get();
        BOOST_REQUIRE(actual);
        assert_that(*actual).is_equal_to(m);
    }
}

// The memory the direct path is given is what bounds how many groups may be hot at once, since a
// hot group holds two buffers of a segment each. A group that finds no room in the budget keeps
// writing on the ordinary path.
SEASTAR_THREAD_TEST_CASE(test_logstor_direct_write_memory_bounds_the_hot_groups) {
    auto schema = make_kv_schema();
    tmpdir dir;

    auto params = direct_write_params();
    // Room for the two buffers of one group, and not for a second group's.
    params.direct_write_memory = 3 * params.segment_size;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), params), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group hot(schema, ls);
    test_logstor_group cold(schema, ls);

    await_direct_buffers(ls, hot);
    ls.get_compaction_manager().promote_direct_writes_for_test(cold).get();
    BOOST_REQUIRE(!cold.direct_writes_enabled());

    // The group that was left out writes the ordinary way: through the shared active segment, which
    // hands the record to the separator rather than keeping it in a buffer of the group.
    auto expected = make_kv_mutation(schema, "pk0", "on-the-ordinary-path");
    ls.write(expected, write_target(&cold, {}), db::no_timeout).get();
    BOOST_REQUIRE(!cold.direct_has_data());
    // Nothing of a direct write is ever given to the separator, and this write was.
    ls.flush_to_separator().get();
    BOOST_REQUIRE(cold.separator_has_data());

    auto actual = ls.read(*schema, cold.logstor_index(), expected.decorated_key(), schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// A budget with no room for even one group turns the direct path off, which is how the parameter
// disables it.
SEASTAR_THREAD_TEST_CASE(test_logstor_direct_writes_off_when_memory_leaves_no_room) {
    auto schema = make_kv_schema();
    tmpdir dir;

    auto params = direct_write_params();
    params.direct_write_memory = params.segment_size;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path(), params), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    ls.get_compaction_manager().promote_direct_writes_for_test(cg).get();
    BOOST_REQUIRE(!cg.direct_writes_enabled());

    auto expected = make_kv_mutation(schema, "pk0", "on-the-ordinary-path");
    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
    BOOST_REQUIRE(!cg.direct_has_data());

    auto actual = ls.read(*schema, cg.logstor_index(), expected.decorated_key(), schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// Checks that free-segment watermarks are correctly derived from disk size and target fraction,
// with floor/cap enforcement ensuring no configuration prevents compaction from starting or stopping.
SEASTAR_THREAD_TEST_CASE(test_logstor_free_segment_watermarks) {
    // Properties that have to hold whatever the disk size and whatever the live-updatable trigger
    // threshold happens to be set to.
    auto check = [] (uint64_t segment_count, double fraction) {
        const auto w = make_free_segment_watermarks(segment_count, fraction);
        BOOST_TEST_CONTEXT("segment_count=" << segment_count << " fraction=" << fraction) {
            BOOST_REQUIRE_LE(w.low, w.high);
            // `high` has to stay reachable, or automatic compaction could never stop.
            BOOST_REQUIRE_LE(w.high, segment_count);
            // And there has to be a band for the hysteresis to work in, unless the trigger is
            // disabled or the target already covers the whole disk.
            BOOST_REQUIRE(w.low < w.high || w.low == 0 || w.high == segment_count);
        }
        return w;
    };
    for (uint64_t segment_count : {1u, 8u, 32u, 128u, 1024u, 100000u}) {
        for (double fraction : {0.0, 0.001, 0.05, 0.2, 0.9, 1.0, 1.5, -0.05, std::nan(""), std::numeric_limits<double>::infinity()}) {
            check(segment_count, fraction);
        }
    }

    // On a disk large enough for the configured fraction to clear the floor, that is what the
    // target is.
    const auto large = check(100000, 0.05);
    BOOST_REQUIRE_EQUAL(large.low, 5000u);
    const auto smaller = check(10000, 0.05);
    BOOST_REQUIRE_EQUAL(smaller.low, 500u);

    // An explicitly configured fraction is never clamped down by the floor.
    BOOST_REQUIRE_EQUAL(check(100000, 0.20).low, 20000u);

    // Where the fraction rounds down to a segment or two the absolute floor takes over, so that the
    // target still covers what compaction holds while it works.
    const auto floored = check(1024, 0.001);
    BOOST_REQUIRE_GT(floored.low, static_cast<uint64_t>(std::ceil(1024 * 0.001)));
    BOOST_REQUIRE_GE(floored.low, 2 * min_segments_per_compaction);

    // On a disk too small for even that, the floor is itself capped, so it cannot claim an
    // unreasonable share of the disk.
    const auto tiny = check(32, 0.05);
    BOOST_REQUIRE_LE(tiny.low, 32u / 4);

    // Zero disables the trigger, and so does anything out of range: the threshold is live-updatable
    // with no range check at the config layer, and a negative value must not underflow into a target
    // that swallows the disk.
    for (double fraction : {0.0, -0.05, -1.0, std::nan(""), -std::numeric_limits<double>::infinity()}) {
        BOOST_TEST_CONTEXT("fraction=" << fraction) {
            const auto disabled = make_free_segment_watermarks(100000, fraction);
            BOOST_REQUIRE_EQUAL(disabled.low, 0u);
            BOOST_REQUIRE_EQUAL(disabled.high, 0u);
        }
    }

    // A fraction at or above 1 asks for the whole disk, which is as far as it can go.
    const auto over_one = check(100000, 1.5);
    BOOST_REQUIRE_EQUAL(over_one.low, 100000u);
    BOOST_REQUIRE_EQUAL(over_one.high, 100000u);
}

// Checks that compaction limits enforce that concurrent jobs don't exceed the free-segment target,
// preventing deadlock, with batch size shrinking first and parallelism second as target shrinks.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_limits) {
    constexpr size_t max_batch_cap = 32;

    // A compaction job frees its inputs only when it is done, so what all the jobs in flight can
    // hold at once has to fit in the free-segment target. This is the relation the two limits exist
    // to maintain, and it has to survive any combination of disk size, threshold and configured cap.
    for (uint64_t segment_count : {32u, 128u, 256u, 1024u, 4096u, 100000u}) {
        for (double fraction : {0.0, 0.01, 0.05, 0.2, 1.0}) {
            for (size_t cap : {1u, 8u, 32u, 1024u}) {
                const auto watermarks = make_free_segment_watermarks(segment_count, fraction);
                const auto limits = make_compaction_limits(watermarks, cap);
                BOOST_TEST_CONTEXT("segment_count=" << segment_count << " fraction=" << fraction << " cap=" << cap) {
                    BOOST_REQUIRE_GE(limits.auto_parallelism, 1u);
                    BOOST_REQUIRE_LE(limits.auto_parallelism, max_auto_compaction_parallelism);
                    // A smaller batch could not reclaim anything, so it would never be worth running.
                    BOOST_REQUIRE_GE(limits.batch_cap, min_segments_per_compaction);
                    BOOST_REQUIRE_LE(limits.batch_cap, std::max(cap, min_segments_per_compaction));
                    // Except on a disk whose target is below a single batch, where the target has
                    // already been capped by the disk size and there is nothing left to give.
                    if (watermarks.low >= min_segments_per_compaction) {
                        BOOST_REQUIRE_LE(limits.auto_parallelism * limits.batch_cap, watermarks.low);
                    }
                }
            }
        }
    }

    // A disk large enough for the target to cover every job in flight gets the configured batch
    // bound and the full parallelism.
    const auto large = make_compaction_limits(make_free_segment_watermarks(100000, 0.05), max_batch_cap);
    BOOST_REQUIRE_EQUAL(large.auto_parallelism, max_auto_compaction_parallelism);
    BOOST_REQUIRE_EQUAL(large.batch_cap, max_batch_cap);

    // A disabled trigger leaves no target to protect and no automatic compaction to protect it
    // from, but compaction submitted explicitly still needs a batch it can reclaim with.
    const auto no_trigger = make_compaction_limits(make_free_segment_watermarks(100000, 0.0), max_batch_cap);
    BOOST_REQUIRE_EQUAL(no_trigger.auto_parallelism, 1u);
    BOOST_REQUIRE_EQUAL(no_trigger.batch_cap, min_segments_per_compaction);

    // A configured bound below the smallest useful batch would leave every batch unable to reclaim,
    // so it is raised rather than honoured.
    const auto tiny_bound = make_compaction_limits(make_free_segment_watermarks(100000, 0.05), 1);
    BOOST_REQUIRE_EQUAL(tiny_bound.batch_cap, min_segments_per_compaction);
}

// Verifies that compaction shares pressure scales with free space, stays monotone and within [0, 1],
// and places the target at the optimal control point regardless of disk size or degenerate watermarks.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_shares_pressure) {
    for (uint64_t segment_count : {128u, 4000u, 100000u}) {
        for (double fraction : {0.05, 0.2}) {
            const auto watermarks = make_free_segment_watermarks(segment_count, fraction);
            BOOST_TEST_CONTEXT("segment_count=" << segment_count << " fraction=" << fraction) {
                // At and above the watermark where automatic compaction stops there is no space
                // demand at all, so the controller asks for no more than its floor.
                BOOST_REQUIRE_EQUAL(compaction_shares_pressure(segment_count, watermarks), 0.0f);
                BOOST_REQUIRE_EQUAL(compaction_shares_pressure(watermarks.high, watermarks), 0.0f);
                BOOST_REQUIRE_EQUAL(compaction_shares_pressure(0, watermarks), 1.0f);

                // The free-segment target is the intended steady-state operating point, and the
                // relative hysteresis puts it a third of the way up the ramp on any disk size -
                // which is where the shares controller puts its middle control point. Only up to
                // the rounding of the two watermarks to whole segments, hence the tolerance.
                BOOST_REQUIRE_CLOSE(compaction_shares_pressure(watermarks.low, watermarks),
                        compaction_shares_pressure_at_target, 10.0);
            }
        }
    }

    // A disabled trigger has no free-segment target, hence no space-driven demand for shares, even
    // with the disk fully consumed.
    BOOST_REQUIRE_EQUAL(compaction_shares_pressure(0, make_free_segment_watermarks(4000, 0.0)), 0.0f);

    // A one-segment target degenerates to a zero saturation point; pressure must stay in range and
    // still reach 1 rather than dividing by zero.
    const free_segment_watermarks minimal{.low = 1, .high = 2};
    BOOST_REQUIRE_EQUAL(compaction_shares_pressure(2, minimal), 0.0f);
    BOOST_REQUIRE_GT(compaction_shares_pressure(1, minimal), 0.0f);
    BOOST_REQUIRE_LT(compaction_shares_pressure(1, minimal), 1.0f);
    BOOST_REQUIRE_EQUAL(compaction_shares_pressure(0, minimal), 1.0f);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_candidate_score_ranks_by_efficiency) {
    constexpr uint64_t segment_size = 128 * 1024;

    // Rewriting 8 segments into 6 reclaims two segments, but copies 5.28 segments worth of live
    // data to do it - a marginal write amplification of 2.6. Rewriting the emptiest 3 into 2
    // reclaims only one segment, for 1.98 segments copied. The efficiency rule prefers the latter.
    const compaction_candidate_score big{.n_in = 8, .n_out = 6, .live_bytes = 528 * segment_size / 100};
    const compaction_candidate_score cheap{.n_in = 3, .n_out = 2, .live_bytes = 198 * segment_size / 100};
    BOOST_REQUIRE_EQUAL(big.reclaimed(), 2u);
    BOOST_REQUIRE_EQUAL(cheap.reclaimed(), 1u);
    BOOST_REQUIRE_LT(big.efficiency(segment_size), cheap.efficiency(segment_size));
    BOOST_REQUIRE(big < cheap);

    // Efficiency is the reciprocal of the batch's marginal write amplification.
    BOOST_REQUIRE_CLOSE(cheap.efficiency(segment_size), 1.0 / 1.98, 0.1);

    // Equal efficiency is broken by reclaiming more per job.
    const compaction_candidate_score two_in{.n_in = 2, .n_out = 1, .live_bytes = segment_size};
    const compaction_candidate_score four_in{.n_in = 4, .n_out = 2, .live_bytes = 2 * segment_size};
    BOOST_REQUIRE_EQUAL(two_in.efficiency(segment_size), four_in.efficiency(segment_size));
    BOOST_REQUIRE(two_in < four_in);

    // A batch of fully dead segments copies nothing and outranks any batch that copies.
    const compaction_candidate_score all_dead{.n_in = 2, .n_out = 0, .live_bytes = 0};
    BOOST_REQUIRE(cheap < all_dead);
    BOOST_REQUIRE(four_in < all_dead);
}

// The marginal-admission gate of run_auto_compaction(): a job started while another one is already
// running has to keep up with the best batch the shard has to offer, because with unequal groups the
// slots beyond the first are filled by whatever group is not already compacting.
SEASTAR_THREAD_TEST_CASE(test_logstor_marginal_compaction_admission) {
    constexpr uint64_t segment_size = 128 * 1024;

    struct candidate {
        compaction_candidate_score score;
    };

    // What the one tablet holding the shard's garbage offers, a batch at exactly the admission
    // ratio, and what a group holding almost nothing dead offers.
    const compaction_candidate_score rich{.n_in = 8, .n_out = 4, .live_bytes = 4 * segment_size};
    const compaction_candidate_score borderline{.n_in = 8, .n_out = 5, .live_bytes = 4 * segment_size};
    const compaction_candidate_score marginal{.n_in = 8, .n_out = 7, .live_bytes = 7 * segment_size};
    BOOST_REQUIRE_CLOSE(rich.efficiency(segment_size), 1.0, 0.001);
    BOOST_REQUIRE_CLOSE(borderline.efficiency(segment_size), compaction_marginal_admission_ratio, 0.001);

    top_compaction_candidates<candidate> ranking(4);
    for (const auto& score : {marginal, rich, borderline}) {
        ranking.add(candidate{score});
    }
    const auto ranked = std::move(ranking).take();
    // Worst first, so the driver takes the best batch first and the bar is the last of them.
    BOOST_REQUIRE_EQUAL(ranked.size(), 3u);
    BOOST_REQUIRE(ranked.front().score == marginal);
    BOOST_REQUIRE(ranked.back().score == rich);

    const auto bar = marginal_admission_bar(ranked);
    BOOST_REQUIRE(bar);
    BOOST_REQUIRE(*bar == rich);

    // The best candidate clears its own bar, so the first job of a run is never refused, a batch at
    // exactly the ratio is admitted, and a group with nothing worth reclaiming is not.
    BOOST_REQUIRE(rich.efficiency_at_least(*bar, compaction_marginal_admission_ratio));
    BOOST_REQUIRE(borderline.efficiency_at_least(*bar, compaction_marginal_admission_ratio));
    BOOST_REQUIRE(!marginal.efficiency_at_least(*bar, compaction_marginal_admission_ratio));

    // The bar has to be relative to what the disk can offer: above 50% utilization even the best
    // batch reclaims less than a segment per segment copied, so an absolute floor of one would
    // refuse every batch there is - including the one the disk depends on.
    const compaction_candidate_score packed_best{.n_in = 8, .n_out = 6, .live_bytes = 6 * segment_size};
    BOOST_REQUIRE_LT(packed_best.efficiency(segment_size), 1.0);
    BOOST_REQUIRE(packed_best.efficiency_at_least(packed_best, compaction_marginal_admission_ratio));
    BOOST_REQUIRE(!marginal.efficiency_at_least(packed_best, compaction_marginal_admission_ratio));

    // A batch that copies nothing reclaims at infinite efficiency, which nothing that copies is a
    // fraction of, so it ranks first but does not get to set the bar.
    const compaction_candidate_score all_dead{.n_in = 2, .n_out = 0, .live_bytes = 0};
    BOOST_REQUIRE(!rich.efficiency_at_least(all_dead, compaction_marginal_admission_ratio));
    top_compaction_candidates<candidate> with_dead(4);
    for (const auto& score : {marginal, all_dead, rich}) {
        with_dead.add(candidate{score});
    }
    const auto dead_ranked = std::move(with_dead).take();
    BOOST_REQUIRE(dead_ranked.back().score == all_dead);
    const auto dead_bar = marginal_admission_bar(dead_ranked);
    BOOST_REQUIRE(dead_bar);
    BOOST_REQUIRE(*dead_bar == rich);

    // With nothing but free reclamation on offer there is no bar at all, and every candidate runs.
    top_compaction_candidates<candidate> only_dead(4);
    only_dead.add(candidate{all_dead});
    BOOST_REQUIRE(!marginal_admission_bar(std::move(only_dead).take()));
}

// The statistics of a segment set are maintained as segments are linked, freed from and unlinked,
// rather than computed by walking it, so every one of those paths has to keep them in step with the
// segments the set actually holds.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_set_stats) {
    constexpr uint64_t segment_size = 128 * 1024;
    // One record per utilization bucket, so that a segment holding n of them lands in bucket n.
    constexpr size_t record_size = segment_size / utilization_bucket_count;

    // Descriptors are intrusively linked into the sets, so they must keep their addresses, and every
    // set must be destroyed before them.
    std::deque<segment_descriptor> descs;
    segment_set segments{segment_size};

    auto add_segment = [&] (size_t live_records) -> segment_descriptor& {
        auto& desc = descs.emplace_back();
        desc.reset(segment_size);
        desc.on_write(live_records * record_size, live_records);
        segments.add_segment(desc);
        return desc;
    };

    auto check_stats = [&] (const segment_set& set, uint64_t expected_segments, uint64_t expected_live_bytes) {
        const auto& stats = set.stats();
        BOOST_REQUIRE_EQUAL(stats.segment_count, expected_segments);
        BOOST_REQUIRE_EQUAL(stats.live_bytes, expected_live_bytes);
        // The maintained statistics have to say what the segments themselves do, whatever the path
        // that got the set here.
        const auto recomputed = set.recompute_stats_for_test();
        BOOST_REQUIRE_EQUAL(recomputed.segment_count, stats.segment_count);
        BOOST_REQUIRE_EQUAL(recomputed.live_bytes, stats.live_bytes);
        BOOST_REQUIRE(recomputed.utilization == stats.utilization);
        // Every segment of the set is counted, and in exactly one bucket.
        uint64_t counted = 0;
        for (auto count : stats.utilization) {
            counted += count;
        }
        BOOST_REQUIRE_EQUAL(counted, expected_segments);
    };

    auto bucket_of = [&] (const segment_set& set, size_t bucket) {
        return set.stats().utilization[bucket];
    };

    // An empty set has nothing to report.
    check_stats(segments, 0, 0);

    // A segment is counted in the bucket its utilization falls in, and a fully utilized one in the
    // last bucket rather than in one past the end of the histogram.
    auto& sparse = add_segment(1);
    auto& full = add_segment(utilization_bucket_count);
    check_stats(segments, 2, (utilization_bucket_count + 1) * record_size);
    BOOST_REQUIRE_EQUAL(bucket_of(segments, 1), 1);
    BOOST_REQUIRE_EQUAL(bucket_of(segments, utilization_bucket_count - 1), 1);

    // Freeing records moves a segment down the histogram, and takes off exactly the space they
    // gave back.
    constexpr size_t freed_records = utilization_bucket_count / 2;
    full.on_free(freed_records * record_size, freed_records);
    segments.update_segment(full, freed_records * record_size);
    check_stats(segments, 2, (freed_records + 1) * record_size);
    BOOST_REQUIRE_EQUAL(bucket_of(segments, utilization_bucket_count - 1), 0);
    BOOST_REQUIRE_EQUAL(bucket_of(segments, freed_records), 1);

    // Merging hands over the segments together with everything counted about them.
    segment_set other{segment_size};
    other.merge(segments).get();
    check_stats(segments, 0, 0);
    check_stats(other, 2, (freed_records + 1) * record_size);
    BOOST_REQUIRE_EQUAL(bucket_of(other, 1), 1);
    BOOST_REQUIRE_EQUAL(bucket_of(other, freed_records), 1);

    // Removing a segment takes it, and its bytes, out of the set.
    other.remove_segment(sparse);
    check_stats(other, 1, freed_records * record_size);
    BOOST_REQUIRE_EQUAL(bucket_of(other, 1), 0);

    other.clear();
    check_stats(other, 0, 0);
}

// A level of the segment statistics rollup holds the statistics of the levels below it, which every
// path that changes a set of segments has to keep true, since nothing sums the sets on read.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_stats_rollup) {
    constexpr uint64_t segment_size = 128 * 1024;
    // One record per utilization bucket, so that a segment holding n of them lands in bucket n.
    constexpr size_t record_size = segment_size / utilization_bucket_count;

    // Descriptors are intrusively linked into the sets, so they must keep their addresses, and every
    // set must be destroyed before them.
    std::deque<segment_descriptor> descs;
    // A shard and one table on it, the levels above the groups of that table.
    segment_stats_node shard;
    segment_stats_node table{&shard};

    auto add_segment = [&] (segment_set& set, size_t live_records) -> segment_descriptor& {
        auto& desc = descs.emplace_back();
        desc.reset(segment_size);
        desc.on_write(live_records * record_size, live_records);
        set.add_segment(desc);
        return desc;
    };

    auto check_levels = [&] (std::initializer_list<std::reference_wrapper<const segment_set>> sets) {
        segment_stats expected;
        for (const auto& set : sets) {
            expected += set.get().stats();
        }
        for (const auto* level : {&table, &shard}) {
            // The table is the sum of its groups, and the shard the sum of its tables, of which this
            // one is the only one.
            BOOST_REQUIRE_EQUAL(level->stats().group_count, expected.group_count);
            BOOST_REQUIRE_EQUAL(level->stats().segment_count, expected.segment_count);
            BOOST_REQUIRE_EQUAL(level->stats().live_bytes, expected.live_bytes);
            BOOST_REQUIRE(level->stats().utilization == expected.utilization);
        }
    };

    // A level that only aggregates holds nothing of its own, before a group is created under it.
    check_levels({});

    {
        segment_set first{segment_size, &table};
        segment_set second{segment_size, &table};

        // A set of segments counts as one group at every level above it, before it holds a segment.
        check_levels({first, second});
        BOOST_REQUIRE_EQUAL(table.stats().group_count, 2u);

        // Linking a segment reaches every level above the set it was linked into.
        auto& sparse = add_segment(first, 1);
        add_segment(second, utilization_bucket_count);
        check_levels({first, second});
        BOOST_REQUIRE_EQUAL(table.stats().live_bytes, (utilization_bucket_count + 1) * record_size);

        // So does freeing the records of one.
        sparse.on_free(record_size);
        first.update_segment(sparse, record_size);
        check_levels({first, second});
        BOOST_REQUIRE_EQUAL(table.stats().live_bytes, utilization_bucket_count * record_size);

        // Segments moving between the groups of a table leave the statistics of the table where they
        // were: what one group loses the other gains.
        const auto before_merge = table.stats();
        second.merge(first).get();
        check_levels({first, second});
        BOOST_REQUIRE_EQUAL(table.stats().segment_count, before_merge.segment_count);
        BOOST_REQUIRE_EQUAL(table.stats().live_bytes, before_merge.live_bytes);
        BOOST_REQUIRE(table.stats().utilization == before_merge.utilization);

        second.remove_segment(sparse);
        check_levels({first, second});
    }

    // A group takes what it still held away with it, the segment `second` was left holding included.
    check_levels({});
}

// Checks that compaction candidate selection chooses segments in ascending utilization order,
// respects the batch cap, and the returned score accurately describes the selected segments.
SEASTAR_THREAD_TEST_CASE(test_logstor_select_compaction_batch) {
    constexpr uint64_t segment_size = 128 * 1024;
    constexpr size_t record_size = 1024;
    constexpr size_t records_per_segment = segment_size / record_size;
    // About a tenth live, so that a few of them rewrite into a single segment.
    constexpr size_t sparse_records = records_per_segment / 10;
    // A batch of n segments reclaims only when its mean utilization is below 1 - 1/n, so at this
    // utilization no batch that select_compaction_batch() would consider has a net gain.
    constexpr size_t dense_records = records_per_segment * 98 / 100;
    constexpr size_t sparse_count = 3;

    // Descriptors are intrusively linked into the sets, so they must keep their addresses, and every
    // set must be destroyed before them.
    std::deque<segment_descriptor> descs;
    segment_set segments{segment_size};
    auto add_segment = [&] (segment_set& set, size_t live_records) {
        auto& desc = descs.emplace_back();
        desc.reset(segment_size);
        desc.on_write(live_records * record_size, live_records);
        set.add_segment(desc);
    };

    // Added interleaved, so that the batch below can only be right if selection went through the
    // free-space histogram rather than the order the segments were added in.
    for (size_t i = 0; i < sparse_count; ++i) {
        add_segment(segments, dense_records);
        add_segment(segments, sparse_records);
    }

    // What the caller relies on for any batch: the segments come least utilized first, and the score
    // describes exactly the segments returned. The same score is what ranks this group against the
    // others in find_top_compaction_candidates(), so a score describing a different batch would rank
    // the group by work it is not about to do.
    auto check_batch = [&] (const compaction_batch& batch) {
        BOOST_REQUIRE(!batch.segments.empty());
        BOOST_REQUIRE_EQUAL(batch.score.n_in, batch.segments.size());
        BOOST_REQUIRE_GT(batch.score.reclaimed(), 0u);
        uint64_t live_bytes = 0;
        size_t prev_net_data_size = 0;
        for (const auto* desc : batch.segments) {
            BOOST_REQUIRE_GE(desc->net_data_size(segment_size), prev_net_data_size);
            prev_net_data_size = desc->net_data_size(segment_size);
            live_bytes += desc->net_data_size(segment_size);
        }
        BOOST_REQUIRE_EQUAL(batch.score.live_bytes, live_bytes);
    };

    auto batch = select_compaction_batch(segments, segment_size, min_segments_per_compaction);
    BOOST_REQUIRE(batch);
    check_batch(*batch);

    // Only the sparse segments are worth taking: they rewrite into a single segment, while extending
    // into an almost fully live one would cost far more than the segment it reclaims.
    BOOST_REQUIRE_EQUAL(batch->segments.size(), sparse_count);
    for (const auto* desc : batch->segments) {
        BOOST_REQUIRE_EQUAL(desc->net_data_size(segment_size), sparse_records * record_size);
    }

    // The cap bounds the candidate set, not only the prefix chosen out of it.
    auto capped = select_compaction_batch(segments, segment_size, 2);
    BOOST_REQUIRE(capped);
    check_batch(*capped);
    BOOST_REQUIRE_LE(capped->segments.size(), 2u);

    // A group whose segments are all nearly full has no batch with a net gain, which is the answer
    // for the whole group and not only for the prefix that happened to be scored.
    segment_set dense{segment_size};
    for (size_t i = 0; i < min_segments_per_compaction; ++i) {
        add_segment(dense, dense_records);
    }
    BOOST_REQUIRE(!select_compaction_batch(dense, segment_size, min_segments_per_compaction));

    // An empty group has nothing to compact.
    segment_set empty{segment_size};
    BOOST_REQUIRE(!select_compaction_batch(empty, segment_size, min_segments_per_compaction));
}

SEASTAR_THREAD_TEST_CASE(test_logstor_group_compaction_rewrites_live_records) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    auto setup_guard = std::make_optional(ls.get_compaction_manager().disable_compaction(cg).get());

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

    setup_guard.reset();
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

    // Scan all segments after compaction and verify they contain exactly the live records.
    // Live records (pk0_v1 ts=4, pk1_v1 ts=5, pk2_v0 ts=3) must appear exactly once;
    // overwritten records (pk0_v0 ts=1, pk1_v0 ts=2) must not appear at all.
    auto record_counts = count_records_by_timestamp(ls, new_snapshot);

    // Each live record appears exactly once.
    BOOST_REQUIRE_EQUAL(record_counts[api::timestamp_type(3)], 1u); // pk2_v0 - untouched
    BOOST_REQUIRE_EQUAL(record_counts[api::timestamp_type(4)], 1u); // pk0_v1 - latest version
    BOOST_REQUIRE_EQUAL(record_counts[api::timestamp_type(5)], 1u); // pk1_v1 - latest version

    // Overwritten records do not appear.
    BOOST_REQUIRE_EQUAL(record_counts.count(api::timestamp_type(1)), 0u); // pk0_v0 - stale
    BOOST_REQUIRE_EQUAL(record_counts.count(api::timestamp_type(2)), 0u); // pk1_v0 - stale
}

SEASTAR_THREAD_TEST_CASE(test_logstor_disabled_group_does_not_compact_on_submit) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    auto compaction_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    auto pk0_v0 = make_kv_mutation(schema, "pk0", "v0", api::timestamp_type(1));
    auto pk1_v0 = make_kv_mutation(schema, "pk1", "v1", api::timestamp_type(2));
    auto pk2_v0 = make_kv_mutation(schema, "pk2", "v2", api::timestamp_type(3));
    auto pk0_v1 = make_kv_mutation(schema, "pk0", "v0-new", api::timestamp_type(4));
    auto pk1_v1 = make_kv_mutation(schema, "pk1", "v1-new", api::timestamp_type(5));

    write_and_flush_segment(ls, cg, pk0_v0);
    write_and_flush_segment(ls, cg, pk1_v0);
    write_and_flush_segment(ls, cg, pk2_v0);
    write_and_flush_segment(ls, cg, pk0_v1);
    write_and_flush_segment(ls, cg, pk1_v1);

    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 5u);

    const auto pk0 = primary_index_key{pk0_v1.decorated_key()};
    const auto pk1 = primary_index_key{pk1_v1.decorated_key()};
    const auto pk2 = primary_index_key{pk2_v0.decorated_key()};

    auto live_pk0_before = cg.logstor_index().get(pk0);
    auto live_pk1_before = cg.logstor_index().get(pk1);
    auto live_pk2_before = cg.logstor_index().get(pk2);

    BOOST_REQUIRE(live_pk0_before);
    BOOST_REQUIRE(live_pk1_before);
    BOOST_REQUIRE(live_pk2_before);

    auto snapshot_before = ls.get_segment_manager().make_snapshot(cg).get();
    const auto segment_ids_before = snapshot_segment_ids(snapshot_before);

    ls.get_compaction_manager().submit(cg);

    auto snapshot_after = ls.get_segment_manager().make_snapshot(cg).get();
    const auto segment_ids_after = snapshot_segment_ids(snapshot_after);

    BOOST_REQUIRE_EQUAL(snapshot_after.size(), snapshot_before.size());
    BOOST_REQUIRE(segment_ids_after == segment_ids_before);

    auto live_pk0_after = cg.logstor_index().get(pk0);
    auto live_pk1_after = cg.logstor_index().get(pk1);
    auto live_pk2_after = cg.logstor_index().get(pk2);

    BOOST_REQUIRE(live_pk0_after);
    BOOST_REQUIRE(live_pk1_after);
    BOOST_REQUIRE(live_pk2_after);

    BOOST_REQUIRE(live_pk0_after->location == live_pk0_before->location);
    BOOST_REQUIRE(live_pk1_after->location == live_pk1_before->location);
    BOOST_REQUIRE(live_pk2_after->location == live_pk2_before->location);

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

// Split compaction hands every segment of a group to the group its records belong to after the
// split: a segment whose records are all on one side is moved as it is, and one that straddles the
// split is rewritten into a segment per side, skipping the records that are no longer live.
SEASTAR_THREAD_TEST_CASE(test_logstor_split_compaction_splits_segments_between_target_groups) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    // The src and target groups in a split share the logstor index.
    test_logstor_group src(schema, ls);
    test_logstor_group left(schema, ls, src.logstor_index());
    test_logstor_group right(schema, ls, src.logstor_index());
    auto& index = src.logstor_index();
    auto setup_guard = std::make_optional(ls.get_compaction_manager().disable_compaction(src).get());

    // Keys in token order, split in the middle: keys[0..2] go to the left group, keys[3..5] to the
    // right one.
    const auto keys = make_token_ordered_keys(schema, 6);
    const auto boundary = dht::decorate_key(*schema, partition_key::from_single_value(*schema, serialized(keys[2]))).token();
    auto classify = [boundary] (dht::token t) -> mutation_writer::token_group_id {
        return t <= boundary ? 0 : 1;
    };

    auto k0 = make_kv_mutation(schema, keys[0], "v0", api::timestamp_type(1));
    auto k1 = make_kv_mutation(schema, keys[1], "v1", api::timestamp_type(2));
    auto k2_v0 = make_kv_mutation(schema, keys[2], "v2", api::timestamp_type(3));
    auto k3 = make_kv_mutation(schema, keys[3], "v3", api::timestamp_type(4));
    auto k4 = make_kv_mutation(schema, keys[4], "v4", api::timestamp_type(5));
    auto k5 = make_kv_mutation(schema, keys[5], "v5", api::timestamp_type(6));
    auto k2_v1 = make_kv_mutation(schema, keys[2], "v2-new", api::timestamp_type(7));

    // One segment on each side of the split, one that straddles it, and one that overwrites a
    // record of the straddling segment, so that the split also has a dead record to skip.
    write_and_flush_segment(ls, src, k0);
    const std::array right_only = {k4, k5};
    write_and_flush_segment(ls, src, right_only);
    const std::array straddling = {k1, k2_v0, k3};
    write_and_flush_segment(ls, src, straddling);
    write_and_flush_segment(ls, src, k2_v1);

    BOOST_REQUIRE_EQUAL(src.logstor_segments().segment_count(), 4u);

    std::vector<primary_index_key> index_keys;
    for (const auto& m : {k0, k1, k2_v1, k3, k4, k5}) {
        index_keys.push_back(primary_index_key{m.decorated_key()});
    }

    std::vector<log_location> locations_before;
    for (const auto& key : index_keys) {
        auto entry = index.get(key);
        BOOST_REQUIRE(entry);
        locations_before.push_back(entry->location);
    }

    const auto single_sided_segments = std::set<log_segment_id>{
        locations_before[0].segment, // k0
        locations_before[2].segment, // k2_v1
        locations_before[4].segment, // k4, k5
    };
    const auto straddling_segment = locations_before[1].segment; // k1, k2_v0, k3
    BOOST_REQUIRE(!single_sided_segments.contains(straddling_segment));
    BOOST_REQUIRE(locations_before[4].segment == locations_before[5].segment);
    BOOST_REQUIRE(locations_before[3].segment == straddling_segment);

    setup_guard.reset();
    ls.get_compaction_manager().submit_split_compaction(src, classify,
            [&] (log_segment_id, dht::token first_token, dht::token last_token) -> logstor_group& {
                // Split compaction only asks about segments it has decided are on a single side.
                BOOST_REQUIRE_EQUAL(classify(first_token), classify(last_token));
                return classify(first_token) == 0 ? static_cast<logstor_group&>(left) : right;
            }).get();

    // Every segment of the group being split ends up in one of the target groups.
    BOOST_REQUIRE_EQUAL(src.logstor_segments().segment_count(), 0u);

    auto left_snapshot = ls.get_segment_manager().make_snapshot(left).get();
    auto right_snapshot = ls.get_segment_manager().make_snapshot(right).get();
    const auto left_ids = snapshot_segment_ids(left_snapshot);
    const auto right_ids = snapshot_segment_ids(right_snapshot);

    std::vector<log_location> locations_after;
    for (const auto& key : index_keys) {
        auto entry = index.get(key);
        BOOST_REQUIRE(entry);
        locations_after.push_back(entry->location);
    }

    // The single-sided segments were moved as they are: the records they hold did not move, and
    // their segments are now owned by the group of their side.
    BOOST_REQUIRE(locations_after[0] == locations_before[0]);
    BOOST_REQUIRE(locations_after[2] == locations_before[2]);
    BOOST_REQUIRE(locations_after[4] == locations_before[4]);
    BOOST_REQUIRE(locations_after[5] == locations_before[5]);
    BOOST_REQUIRE(left_ids.contains(locations_after[0].segment));
    BOOST_REQUIRE(left_ids.contains(locations_after[2].segment));
    BOOST_REQUIRE(right_ids.contains(locations_after[4].segment));

    // The straddling segment was rewritten into a new segment per side and freed.
    BOOST_REQUIRE(locations_after[1] != locations_before[1]);
    BOOST_REQUIRE(locations_after[3] != locations_before[3]);
    BOOST_REQUIRE(left_ids.contains(locations_after[1].segment));
    BOOST_REQUIRE(right_ids.contains(locations_after[3].segment));
    BOOST_REQUIRE(!left_ids.contains(straddling_segment));
    BOOST_REQUIRE(!right_ids.contains(straddling_segment));

    // All the records are readable, from whichever group they ended up in.
    for (const auto& expected : {k0, k1, k2_v1, k3, k4, k5}) {
        auto actual = ls.read(*schema, index, expected.decorated_key(), schema->full_slice()).get();
        BOOST_REQUIRE(actual);
        assert_that(*actual).is_equal_to(expected);
    }

    // Each live record is held exactly once, by the group of its side, and the record the overwrite
    // of k2 made dead was not rewritten.
    auto left_records = count_records_by_timestamp(ls, left_snapshot);
    auto right_records = count_records_by_timestamp(ls, right_snapshot);

    const auto expected_left = std::map<api::timestamp_type, size_t>{
        {api::timestamp_type(1), 1}, // k0
        {api::timestamp_type(2), 1}, // k1
        {api::timestamp_type(7), 1}, // k2_v1
    };
    const auto expected_right = std::map<api::timestamp_type, size_t>{
        {api::timestamp_type(4), 1}, // k3
        {api::timestamp_type(5), 1}, // k4
        {api::timestamp_type(6), 1}, // k5
    };
    BOOST_REQUIRE(left_records == expected_left);
    BOOST_REQUIRE(right_records == expected_right);
}
