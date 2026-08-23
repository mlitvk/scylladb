/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include <boost/test/unit_test.hpp>

// Defines the test main, so it comes before any seastar testing header.
#include "test/lib/scylla_test_case.hh"

#include <random>

#include <seastar/core/simple-stream.hh>
#include <seastar/testing/thread_test_case.hh>

#include "keys/keys.hh"
#include "mutation/canonical_mutation.hh"
#include "mutation/mutation.hh"
#include "replica/logstor/record_format.hh"
#include "schema/schema_builder.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/random_utils.hh"
#include "types/list.hh"
#include "types/map.hh"

using namespace replica::logstor;

namespace {

// A logstor table: a partition key and regular columns, no clustering and hence no static
// columns.
schema_ptr make_logstor_schema(std::vector<std::pair<sstring, data_type>> columns, size_t pk_components = 1) {
    schema_builder builder(1, "ks", "cf");
    for (size_t i = 0; i < pk_components; ++i) {
        builder.with_column(to_bytes(fmt::format("pk{}", i)), bytes_type, column_kind::partition_key);
    }
    for (auto& [name, type] : columns) {
        builder.with_column(to_bytes(name), std::move(type));
    }
    return builder.build();
}

schema_ptr make_blob_column_schema(size_t column_count) {
    std::vector<std::pair<sstring, data_type>> columns;
    for (size_t i = 0; i < column_count; ++i) {
        columns.emplace_back(fmt::format("column{:02}", i), bytes_type);
    }
    return make_logstor_schema(std::move(columns));
}

mutation make_logstor_mutation(schema_ptr s, sstring pk = "key") {
    return mutation(s, partition_key::from_single_value(*s, to_bytes(pk)));
}

deletable_row& row_of(mutation& m) {
    return m.partition().clustered_row(*m.schema(), clustering_key::make_empty());
}

void set_cell(mutation& m, const sstring& column, atomic_cell cell) {
    const auto& cdef = *m.schema()->get_column_definition(to_bytes(column));
    row_of(m).cells().apply(cdef, std::move(cell));
}

void set_blob_cell(mutation& m, const sstring& column, const bytes& value, api::timestamp_type ts) {
    const auto& cdef = *m.schema()->get_column_definition(to_bytes(column));
    set_cell(m, column, atomic_cell::make_live(*cdef.type, ts, value));
}

// The timestamp a record is written with: the one of its row marker, or of its partition
// tombstone when it has no marker. Mirrors extract_logstor_record_timestamp().
api::timestamp_type base_timestamp_of(const mutation& m) {
    for (const auto& e : m.partition().clustered_rows()) {
        if (!e.dummy() && !e.row().marker().is_missing()) {
            return e.row().marker().timestamp();
        }
    }
    if (const auto t = m.partition().partition_tombstone()) {
        return t.timestamp;
    }
    return api::min_timestamp;
}

bytes encode(const mutation& m, api::timestamp_type base_ts) {
    const auto size = measure_row_value(*m.schema(), m.partition(), base_ts);
    bytes encoded(bytes::initialized_later(), size);
    auto out = seastar::simple_memory_output_stream(reinterpret_cast<char*>(encoded.data()), encoded.size());
    write_row_value(out, *m.schema(), m.partition(), base_ts);
    // A record is written straight into the write buffer, at the offset the measured size
    // reserved for it, so the two must agree exactly.
    BOOST_REQUIRE_EQUAL(out.size(), 0);
    return encoded;
}

mutation decode(schema_ptr s, const dht::decorated_key& dk, bytes_view encoded, api::timestamp_type base_ts) {
    mutation_partition p(*s);
    read_row_value_into(p, *s, encoded, base_ts);
    return mutation(s, dk, std::move(p));
}

// Encodes and decodes the mutation and requires the result to be equal to it. Uses both the
// record's own timestamp as the delta base, which is what logstor writes with, and an
// unrelated one, so that the deltas are exercised as well.
size_t check_round_trip(const mutation& m) {
    size_t encoded_size = 0;
    for (auto base_ts : {base_timestamp_of(m), api::timestamp_type(0)}) {
        auto encoded = encode(m, base_ts);
        assert_that(decode(m.schema(), m.decorated_key(), encoded, base_ts)).is_equal_to(m);
        if (base_ts == base_timestamp_of(m)) {
            encoded_size = encoded.size();
        }
    }
    return encoded_size;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_insert_of_every_column) {
    auto s = make_blob_column_schema(5);
    auto m = make_logstor_mutation(s);
    const auto ts = api::timestamp_type(1700000000000000);

    row_of(m).apply(row_marker(ts));
    for (size_t i = 0; i < 5; ++i) {
        set_blob_cell(m, fmt::format("column{:02}", i), bytes(bytes::initialized_later(), 300), ts);
    }

    const auto size = check_round_trip(m);

    // The shape the format was designed against: 5 columns of 300 bytes each written by one
    // INSERT, so every cell shares the row marker's timestamp. 1500 bytes of value and:
    //    1  flags
    //   16  schema version
    //    1  schema description size
    //   94  description: 1 type count + 1 name size + 41 type name
    //                    + 1 column count + 5 x (1 name size + 8 name + 1 type index)
    //    1  marker timestamp delta
    //   15  5 x (1 cell flags + 2 value size)
    // No column bitmap, because every column of the schema is present, and no cell writes a
    // timestamp, because all of them are the record's. The description is 74% of the
    // overhead and phase 2 of the format moves it to a per-buffer dictionary.
    BOOST_REQUIRE_EQUAL(size, 1628);
    BOOST_REQUIRE_LT(size, canonical_mutation(m).representation().size());
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_insert_of_some_columns) {
    auto s = make_blob_column_schema(5);
    auto m = make_logstor_mutation(s);
    const auto ts = api::timestamp_type(1700000000000000);

    row_of(m).apply(row_marker(ts));
    set_blob_cell(m, "column01", to_bytes("v1"), ts);
    set_blob_cell(m, "column03", to_bytes("v3"), ts + 1);

    check_round_trip(m);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_row_with_no_cell) {
    auto s = make_blob_column_schema(3);
    auto m = make_logstor_mutation(s);
    row_of(m).apply(row_marker(api::timestamp_type(17)));

    check_round_trip(m);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_partition_tombstone) {
    auto s = make_blob_column_schema(3);
    auto m = make_logstor_mutation(s);
    m.partition().apply(tombstone(api::timestamp_type(1700000000000000), gc_clock::now()));

    // A DELETE of the whole partition: no row at all.
    const auto size = check_round_trip(m);
    BOOST_REQUIRE_LT(size, 100);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_partition_tombstone_and_row) {
    auto s = make_blob_column_schema(3);
    auto m = make_logstor_mutation(s);
    const auto ts = api::timestamp_type(1700000000000000);

    m.partition().apply(tombstone(ts - 1, gc_clock::now()));
    row_of(m).apply(row_marker(ts));
    set_blob_cell(m, "column00", to_bytes("v"), ts);

    check_round_trip(m);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_row_tombstone) {
    auto s = make_blob_column_schema(2);
    const auto ts = api::timestamp_type(1700000000000000);
    const auto deletion_time = gc_clock::now();

    {
        auto m = make_logstor_mutation(s);
        row_of(m).apply(tombstone(ts, deletion_time));
        check_round_trip(m);
    }
    {
        // A shadowable tombstone, which is what a view update produces. It is only kept as
        // long as the row has no newer live marker, so the marker is the older one here.
        auto m = make_logstor_mutation(s);
        auto& row = row_of(m);
        row.apply(row_marker(ts - 1));
        row.apply(shadowable_tombstone(ts, deletion_time));
        check_round_trip(m);
    }
    {
        auto m = make_logstor_mutation(s);
        auto& row = row_of(m);
        row.apply(row_marker(ts - 2));
        row.apply(tombstone(ts - 1, deletion_time));
        row.apply(shadowable_tombstone(ts, deletion_time));
        check_round_trip(m);
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_dead_marker_and_dead_cells) {
    auto s = make_blob_column_schema(3);
    auto m = make_logstor_mutation(s);
    const auto ts = api::timestamp_type(1700000000000000);
    const auto deletion_time = gc_clock::now();

    row_of(m).apply(row_marker(tombstone(ts, deletion_time)));
    set_cell(m, "column00", atomic_cell::make_dead(ts, deletion_time));
    set_cell(m, "column01", atomic_cell::make_dead(ts + 5, deletion_time + gc_clock::duration(60)));

    check_round_trip(m);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_expiring_marker_and_cells) {
    auto s = make_blob_column_schema(4);
    auto m = make_logstor_mutation(s);
    const auto ts = api::timestamp_type(1700000000000000);
    const auto ttl = gc_clock::duration(3600);
    const auto expiry = gc_clock::now() + ttl;
    const auto& cdef = *s->get_column_definition(to_bytes("column00"));

    row_of(m).apply(row_marker(ts, ttl, expiry));
    // Shares the marker's ttl and expiry, so neither is written.
    set_cell(m, "column00", atomic_cell::make_live(*cdef.type, ts, to_bytes("v0"), expiry, ttl));
    // Its own ttl.
    set_cell(m, "column01", atomic_cell::make_live(*cdef.type, ts, to_bytes("v1"),
            expiry + gc_clock::duration(60), gc_clock::duration(60)));
    // Not expiring at all.
    set_cell(m, "column02", atomic_cell::make_live(*cdef.type, ts, to_bytes("v2")));

    check_round_trip(m);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_expiring_cell_without_a_marker) {
    auto s = make_blob_column_schema(2);
    auto m = make_logstor_mutation(s);
    const auto ts = api::timestamp_type(1700000000000000);
    const auto& cdef = *s->get_column_definition(to_bytes("column00"));

    m.partition().apply(tombstone(ts - 1, gc_clock::now()));
    row_of(m).cells().apply(cdef, atomic_cell::make_live(*cdef.type, ts, to_bytes("v"),
            gc_clock::now() + gc_clock::duration(30), gc_clock::duration(30)));

    check_round_trip(m);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_empty_values) {
    auto s = make_logstor_schema({{"b", bytes_type}, {"t", utf8_type}, {"i", int32_type}});
    auto m = make_logstor_mutation(s);
    const auto ts = api::timestamp_type(1700000000000000);

    row_of(m).apply(row_marker(ts));
    for (auto column : {"b", "t", "i"}) {
        const auto& cdef = *s->get_column_definition(to_bytes(column));
        set_cell(m, column, atomic_cell::make_live(*cdef.type, ts, bytes()));
    }

    check_round_trip(m);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_fixed_width_types) {
    auto s = make_logstor_schema({
            {"i", int32_type},
            {"l", long_type},
            {"u", uuid_type},
            {"d", double_type},
            {"b", boolean_type},
            {"blob", bytes_type}});
    auto m = make_logstor_mutation(s);
    const auto ts = api::timestamp_type(1700000000000000);

    row_of(m).apply(row_marker(ts));
    set_cell(m, "i", atomic_cell::make_live(*int32_type, ts, int32_type->decompose(int32_t(42))));
    set_cell(m, "l", atomic_cell::make_live(*long_type, ts, long_type->decompose(int64_t(42))));
    set_cell(m, "u", atomic_cell::make_live(*uuid_type, ts, uuid_type->decompose(utils::make_random_uuid())));
    set_cell(m, "d", atomic_cell::make_live(*double_type, ts, double_type->decompose(4.2)));
    set_cell(m, "b", atomic_cell::make_live(*boolean_type, ts, boolean_type->decompose(true)));
    set_cell(m, "blob", atomic_cell::make_live(*bytes_type, ts, to_bytes("blob")));

    check_round_trip(m);

    // A cell of a fixed-width type writes no value size, so it costs its flags byte and
    // its value and nothing else, while a cell of the same size of a variable-length type
    // pays a byte for the size as well.
    auto without_cell = make_logstor_mutation(s);
    row_of(without_cell).apply(row_marker(ts));
    const auto base_size = measure_row_value(*s, without_cell.partition(), ts);

    auto with_int = make_logstor_mutation(s);
    row_of(with_int).apply(row_marker(ts));
    set_cell(with_int, "i", atomic_cell::make_live(*int32_type, ts, int32_type->decompose(int32_t(42))));
    BOOST_REQUIRE_EQUAL(measure_row_value(*s, with_int.partition(), ts) - base_size, 1 + 4);

    auto with_blob = make_logstor_mutation(s);
    row_of(with_blob).apply(row_marker(ts));
    set_cell(with_blob, "blob", atomic_cell::make_live(*bytes_type, ts, to_bytes("beef")));
    BOOST_REQUIRE_EQUAL(measure_row_value(*s, with_blob.partition(), ts) - base_size, 1 + 1 + 4);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_frozen_and_non_frozen_collections) {
    auto frozen_map = map_type_impl::get_instance(utf8_type, utf8_type, false);
    auto live_map = map_type_impl::get_instance(utf8_type, utf8_type, true);
    auto live_list = list_type_impl::get_instance(utf8_type, true);
    auto s = make_logstor_schema({{"frozen", frozen_map}, {"m", live_map}, {"l", live_list}});
    auto m = make_logstor_mutation(s);
    const auto ts = api::timestamp_type(1700000000000000);

    row_of(m).apply(row_marker(ts));
    const auto frozen_value = make_map_value(frozen_map,
            map_type_impl::native_type({{data_value(sstring("a")), data_value(sstring("b"))}})).serialize_nonnull();
    set_cell(m, "frozen", atomic_cell::make_live(*frozen_map, ts, frozen_value));

    // A non-frozen collection is a collection_mutation, which the record stores verbatim.
    {
        collection_mutation_writer writer({});
        const auto key = utf8_type->decompose(sstring("k"));
        const auto cell = atomic_cell::make_live(*utf8_type, ts, utf8_type->decompose(sstring("v")),
                atomic_cell::collection_member::yes);
        writer.push_back(bytes_view(key), atomic_cell_view(cell));
        row_of(m).cells().apply(*s->get_column_definition(to_bytes("m")), std::move(writer).finish());
    }
    // One with a collection-wide tombstone and no cell, as an overwrite of a collection produces.
    {
        collection_mutation_writer writer(tombstone(ts - 1, gc_clock::now()));
        row_of(m).cells().apply(*s->get_column_definition(to_bytes("l")), std::move(writer).finish());
    }

    check_round_trip(m);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_wide_schema) {
    // Over 64 columns, where the presence of a column is a raw bitmap rather than a varint.
    constexpr size_t column_count = 100;
    auto s = make_blob_column_schema(column_count);
    const auto ts = api::timestamp_type(1700000000000000);

    {
        auto m = make_logstor_mutation(s);
        row_of(m).apply(row_marker(ts));
        for (size_t i = 0; i < column_count; ++i) {
            set_blob_cell(m, fmt::format("column{:02}", i), to_bytes("v"), ts);
        }
        // Every column is present, so no bitmap is written at all.
        check_round_trip(m);
    }
    {
        auto m = make_logstor_mutation(s);
        row_of(m).apply(row_marker(ts));
        for (size_t i = 0; i < column_count; i += 7) {
            set_blob_cell(m, fmt::format("column{:02}", i), to_bytes("v"), ts);
        }
        check_round_trip(m);
    }
    {
        // The last column only: the bitmap's high byte.
        auto m = make_logstor_mutation(s);
        row_of(m).apply(row_marker(ts));
        set_blob_cell(m, fmt::format("column{:02}", column_count - 1), to_bytes("v"), ts);
        check_round_trip(m);
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_extreme_timestamps) {
    // A timestamp is an arbitrary int64 and the delta of two of them has to survive
    // whatever the two are.
    auto s = make_blob_column_schema(2);
    const auto& cdef = *s->get_column_definition(to_bytes("column00"));

    for (auto marker_ts : {api::min_timestamp, api::max_timestamp, api::timestamp_type(0)}) {
        for (auto cell_ts : {api::min_timestamp, api::max_timestamp, api::timestamp_type(0)}) {
            auto m = make_logstor_mutation(s);
            row_of(m).apply(row_marker(marker_ts));
            set_cell(m, "column00", atomic_cell::make_live(*cdef.type, cell_ts, to_bytes("v")));
            check_round_trip(m);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_rejects_a_foreign_schema_version) {
    auto s = make_blob_column_schema(2);
    auto m = make_logstor_mutation(s);
    const auto ts = api::timestamp_type(1700000000000000);
    row_of(m).apply(row_marker(ts));
    set_blob_cell(m, "column00", to_bytes("v"), ts);

    auto encoded = encode(m, ts);

    auto other = schema_builder(s).with_column(to_bytes("added"), bytes_type).build();
    BOOST_REQUIRE_NE(other->version(), s->version());

    mutation_partition p(*other);
    BOOST_REQUIRE_THROW(read_row_value_into(p, *other, encoded, ts), std::runtime_error);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_rejects_a_partition_it_cannot_hold) {
    // A partition of a shape no logstor table can produce is refused rather than encoded
    // into something that cannot be read back. Such a partition needs a schema logstor
    // rejects at CREATE TABLE, which is what this builds.
    auto s = schema_builder(1, "ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck0", bytes_type, column_kind::clustering_key)
            .with_column("ck1", bytes_type, column_kind::clustering_key)
            .with_column("st", bytes_type, column_kind::static_column)
            .with_column("v", bytes_type)
            .build();
    const auto ts = api::timestamp_type(1700000000000000);
    auto key = partition_key::from_single_value(*s, to_bytes("key"));

    {
        mutation m(s, key);
        auto ck = [&] (const char* v) {
            return clustering_key::from_exploded(*s, {to_bytes(v), to_bytes(v)});
        };
        m.partition().clustered_row(*s, ck("a")).apply(row_marker(ts));
        m.partition().clustered_row(*s, ck("b")).apply(row_marker(ts));
        BOOST_REQUIRE_THROW(measure_row_value(*s, m.partition(), ts), std::runtime_error);
    }
    {
        mutation m(s, key);
        const auto& cdef = *s->get_column_definition(to_bytes("st"));
        m.partition().static_row().maybe_create().apply(cdef, atomic_cell::make_live(*cdef.type, ts, to_bytes("v")));
        BOOST_REQUIRE_THROW(measure_row_value(*s, m.partition(), ts), std::runtime_error);
    }
    {
        mutation m(s, key);
        m.partition().apply_row_tombstone(*s, clustering_key_prefix::from_single_value(*s, to_bytes("a")),
                tombstone(ts, gc_clock::now()));  // a strict prefix of the two-column key
        BOOST_REQUIRE_THROW(measure_row_value(*s, m.partition(), ts), std::runtime_error);
    }
}

SEASTAR_THREAD_TEST_CASE(test_logstor_record_format_round_trips_random_mutations) {
    const auto seed = tests::random::get_int<uint32_t>();
    BOOST_TEST_MESSAGE(fmt::format("random seed: {}", seed));

    auto spec = tests::make_random_schema_specification(
            "ks",
            std::uniform_int_distribution<size_t>(1, 3),   // partition key columns
            std::uniform_int_distribution<size_t>(0, 0),   // no clustering columns
            std::uniform_int_distribution<size_t>(1, 8),   // regular columns
            std::uniform_int_distribution<size_t>(0, 0));  // no static columns
    auto expiry_gen = [] (std::mt19937& engine, tests::timestamp_destination) -> std::optional<tests::expiry_info> {
        if (engine() % 2) {
            return std::nullopt;
        }
        const auto ttl = gc_clock::duration(1 + engine() % 3600);
        return tests::expiry_info{ttl, gc_clock::now() + ttl};
    };

    for (uint32_t schema_index = 0; schema_index < 8; ++schema_index) {
        tests::random_schema random_schema(seed + schema_index, *spec);
        auto engine = std::mt19937(seed + schema_index);

        for (uint32_t partition = 0; partition < 8; ++partition) {
            auto description = random_schema.new_mutation(partition);
            random_schema.set_partition_tombstone(engine, description, tests::default_timestamp_generator(), expiry_gen);
            // The row of a partition of a table with no clustering columns: the one with the
            // empty clustering key.
            random_schema.add_row(engine, description, tests::data_model::mutation_description::key{},
                    tests::default_timestamp_generator(), expiry_gen);
            auto m = description.build(random_schema.schema());

            if (partition % 2) {
                // Drop some cells, so that the record carries a column bitmap.
                for (auto& e : m.partition().mutable_clustered_rows()) {
                    e.row().cells().remove_if([&partition] (column_id id, atomic_cell_or_collection&) {
                        return (id + partition) % 3 == 0;
                    });
                }
            }

            check_round_trip(m);
        }
    }
}
