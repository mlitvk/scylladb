/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// A micro benchmark of the logstor record format on its own: what encoding a partition into the
// value of a record and decoding it back cost, and what the record takes on disk. It builds no
// logstor, touches no disk and runs in a couple of seconds, so it is what to run while changing
// replica/logstor/record_format.cc; test/perf/perf_logstor measures the same two steps, as its
// `encode` and `decode` tests, against a dataset in real segments and next to the rest of a read
// and of a write.
//
// Everything is reported against canonical_mutation, the form a record value took before the
// format, in what it costs and in what it takes, and for as many row shapes as one run is given:
//
//   taskset -c 2 build/release/test/perf/perf_logstor_record_format --smp 1 \
//       --columns 1,5,30 --value-size 20,300
//
// The number to compare between runs is instructions or allocations per operation: the duration
// depends on whatever else the machine happens to be doing.

#include <charconv>
#include <chrono>
#include <ranges>
#include <string_view>
#include <vector>

#include <seastar/core/align.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/testing/linux_perf_event.hh>

#include "keys/keys.hh"
#include "mutation/canonical_mutation.hh"
#include "mutation/mutation.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/record_format.hh"
#include "replica/logstor/types.hh"
#include "schema/schema_builder.hh"
#include "types/types.hh"
#include "utils/UUID_gen.hh"
#include "vint-serialization.hh"

using namespace replica::logstor;

namespace {

// One operation costs of the order of what the measurement loop itself costs, so a sample
// measures a batch of them.
constexpr size_t batch_size = 32;

// Takes the result of an operation that is otherwise unused, so that it is not optimized away
// and cannot be hoisted out of the loop that repeats it.
volatile size_t sink = 0;

// The row one record holds. A logstor partition is at most one row, so a shape is the columns of
// the table, how many of them the row sets, and how large a value is.
struct row_shape {
    size_t columns;
    size_t columns_set;
    size_t value_size;

    size_t payload_size() const noexcept { return columns_set * value_size; }
};

// Prints the average duration, retired instruction count and allocation count of one call of @op.
template <typename Op>
void time_op(std::string_view name, size_t samples, Op op) {
    using clk = std::chrono::steady_clock;
    clk::duration total{};
    uint64_t allocations = 0;
    uint64_t instructions = 0;
    auto instructions_counter = linux_perf_event::user_instructions_retired();
    instructions_counter.enable();
    for (size_t i = 0; i < samples; ++i) {
        const auto allocations_before = memory::stats().mallocs();
        const auto instructions_before = instructions_counter.read();
        const auto start = clk::now();
        for (size_t j = 0; j < batch_size; ++j) {
            op();
        }
        const auto end = clk::now();
        instructions += instructions_counter.read() - instructions_before;
        total += end - start;
        allocations += memory::stats().mallocs() - allocations_before;
    }
    instructions_counter.disable();
    const auto operations = double(samples) * batch_size;
    fmt::print("  {:<34}{:>8.2f} us {:>10.0f} instr {:>7.1f} allocs\n", name,
            std::chrono::duration<double, std::micro>(total).count() / operations,
            double(instructions) / operations, double(allocations) / operations);
}

// A logstor table of @columns blob columns, and a later version of the same table, which a record
// written under the first is read through a column_translation with. Both are versions of one
// table, since that is the only way a record of another version reaches a read.
std::pair<schema_ptr, schema_ptr> make_schemas(size_t columns) {
    auto make_schema = [id = table_id(utils::UUID_gen::get_time_UUID()), columns] (bool altered) {
        schema_builder builder(this_smp_shard_count(), "ks", "cf", id);
        builder.with_column("pk", bytes_type, column_kind::partition_key);
        for (size_t i = 0; i < columns; ++i) {
            builder.with_column(to_bytes(fmt::format("v{}", i)), bytes_type);
        }
        if (altered) {
            builder.with_column("v_added", bytes_type);
        }
        builder.set_logstor();
        return builder.build();
    };
    return {make_schema(false), make_schema(true)};
}

// The partition of one record: the row with the empty clustering key, with a live marker and a
// live cell in each of the first columns_set columns.
mutation make_row(schema_ptr s, const row_shape& shape, const bytes& value, api::timestamp_type ts) {
    mutation m(s, partition_key::from_single_value(*s, to_bytes("key1")));
    auto& row = m.partition().clustered_row(*s, clustering_key::make_empty());
    row.apply(row_marker(ts));
    for (size_t i = 0; i < shape.columns_set; ++i) {
        const auto& cdef = *s->get_column_definition(to_bytes(fmt::format("v{}", i)));
        row.cells().apply(cdef, atomic_cell::make_live(*cdef.type, ts, value));
    }
    return m;
}

// What a write does to turn the partition of a mutation into the value of its record: encode
// it through the buffer the shard's encoder reuses, and copy the exact bytes out into the
// value the record owns.
bytes encode(row_value_encoder& encoder, const mutation& m, api::timestamp_type base_ts) {
    return bytes(encoder.encode(*m.schema(), m.partition(), base_ts));
}

// What an encoded value is made of. The head and the description of the schema are what a record
// pays for being self-contained; a read under the schema version the record was written with steps
// over the description without parsing it, and phase 2 of the format moves it to a per-buffer
// dictionary, where it amortizes to a few bytes per record.
struct value_layout {
    size_t head; // the flags byte, the schema version, and the size of the description
    size_t description;
    size_t row;
};

value_layout layout_of(bytes_view value) {
    const size_t fixed = sizeof(uint8_t) + 2 * sizeof(uint64_t);
    const auto description = unsigned_vint::deserialize(value.substr(fixed));
    const auto head = fixed + unsigned_vint::serialized_size_from_first_byte(value[fixed]);
    return value_layout{
        .head = head,
        .description = description,
        .row = value.size() - head - description,
    };
}

// What a record whose value takes value_size takes in a segment: the framing of the record, the
// serialized log_record_header, the value, and the padding to the alignment of a record.
size_t record_size(const schema& s, const dht::decorated_key& key, size_t value_size) {
    const log_record_header header{
        .key = primary_index_key{key},
        .timestamp = api::new_timestamp(),
        .table = s.id(),
    };
    return seastar::align_up(ondisk::record_header_size + ondisk::log_record_header_size(header) + value_size,
            ondisk::record_alignment);
}

void print_sizes(const row_shape& shape, const schema& s, const mutation& m, const bytes& encoded,
        const canonical_mutation& cm) {
    const auto payload = shape.payload_size();
    const auto ratio = [payload] (size_t size) { return payload ? double(size) / payload : 0.0; };
    // The value of a record before the format was the canonical_mutation of the partition,
    // serialized as a blob, so it carried a size prefix of its own. The header is the same either way.
    const auto canonical_value_size = cm.representation().size() + sizeof(uint32_t);
    const auto layout = layout_of(encoded);
    const auto record = record_size(s, m.decorated_key(), encoded.size());
    const auto canonical_record = record_size(s, m.decorated_key(), canonical_value_size);

    fmt::print("  {:<34}{:>8} B  {:>5.2f}x    canonical_mutation {} B ({:.2f}x)\n", "value",
            encoded.size(), ratio(encoded.size()), canonical_value_size, ratio(canonical_value_size));
    fmt::print("  {:<34}{:>8} B  {:>5.2f}x    canonical_mutation {} B ({:.2f}x)\n",
            fmt::format("record in a segment, {} B key", m.key().representation().size()),
            record, ratio(record), canonical_record, ratio(canonical_record));
    fmt::print("  {:<34}{:>8} B head + {} B schema description + {} B row\n", "value, by part",
            layout.head, layout.description, layout.row);
}

void run_shape(const row_shape& shape, size_t samples) {
    const auto schemas = make_schemas(shape.columns);
    const schema_ptr s = schemas.first;
    const schema_ptr altered = schemas.second;

    bytes value(bytes::initialized_later(), shape.value_size);
    std::ranges::fill(value, int8_t('v'));
    // The timestamp of the row marker, which is the record's own timestamp and the base every
    // timestamp in its value is a delta from.
    const auto ts = api::new_timestamp();
    const auto m = make_row(s, shape, value, ts);
    row_value_encoder encoder;
    const auto encoded = encode(encoder, m, ts);
    const canonical_mutation cm(m);

    fmt::print("\n{} columns x {} B, {} set, {} B of value:\n", shape.columns, shape.value_size,
            shape.columns_set, shape.payload_size());
    print_sizes(shape, *s, m, encoded, cm);

    time_op("encode", samples, [&] {
        sink += encode(encoder, m, ts).size();
    });
    // What the encode used to spend on sizing the buffer it fills: a second walk of the partition,
    // which the single-pass encoder does not make. Kept to measure what it cost.
    time_op("encode, sizing walk only", samples, [&] {
        sink += measure_row_value(*s, m.partition(), ts);
    });
    time_op("freeze (canonical_mutation)", samples, [&] {
        sink += canonical_mutation(m).representation().size();
    });

    column_translation_cache translations;
    time_op("decode", samples, [&] {
        mutation decoded(s, m.decorated_key());
        read_row_value_into(decoded.partition(), *s, encoded, ts, translations);
        sink += decoded.partition().row_count();
    });
    // The translation of the record's columns onto the schema's is built on the first read of a
    // pair of schema versions and cached, so it is built here rather than inside the measurement.
    column_translation_cache altered_translations;
    mutation_partition warmup(*altered);
    read_row_value_into(warmup, *altered, encoded, ts, altered_translations);
    time_op("decode, other schema version", samples, [&] {
        mutation decoded(altered, m.decorated_key());
        read_row_value_into(decoded.partition(), *altered, encoded, ts, altered_translations);
        sink += decoded.partition().row_count();
    });
    time_op("materialize (canonical_mutation)", samples, [&] {
        sink += cm.to_mutation(s).partition().row_count();
    });
    time_op("materialize, other schema version", samples, [&] {
        sink += cm.to_mutation(altered).partition().row_count();
    });
}

std::vector<size_t> parse_sizes(const std::string& option, const std::string& values) {
    std::vector<size_t> sizes;
    for (const auto& part : std::views::split(std::string_view(values), std::string_view(","))) {
        auto text = std::string_view(part.begin(), part.end());
        size_t size = 0;
        const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), size);
        if (ec != std::errc() || end != text.data() + text.size()) {
            throw std::invalid_argument(fmt::format("--{} takes comma separated numbers, got '{}'", option, text));
        }
        sizes.push_back(size);
    }
    if (sizes.empty()) {
        throw std::invalid_argument(fmt::format("--{} takes at least one number", option));
    }
    return sizes;
}

} // anonymous namespace

int main(int argc, char* argv[]) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("columns", bpo::value<std::string>()->default_value("1,5,30"),
                "comma separated value column counts of the table. The description of the schema a record"
                " carries and the framing of its cells both scale with this")
        ("value-size", bpo::value<std::string>()->default_value("20,300"),
                "comma separated sizes of the value of one column, in bytes")
        ("columns-set", bpo::value<size_t>()->default_value(0),
                "columns of the row that carry a cell, or 0 for all of them. Fewer than all makes the"
                " record carry a bitmap of the columns it holds")
        ("iterations", bpo::value<size_t>()->default_value(200),
                "samples per measurement, each of a batch of operations");
    return app.run_deprecated(argc, argv, [&] {
        const auto& config = app.configuration();
        const auto column_counts = parse_sizes("columns", config["columns"].as<std::string>());
        const auto value_sizes = parse_sizes("value-size", config["value-size"].as<std::string>());
        const auto columns_set = config["columns-set"].as<size_t>();
        const auto samples = config["iterations"].as<size_t>();
        for (auto columns : column_counts) {
            for (auto value_size : value_sizes) {
                run_shape(row_shape{
                    .columns = columns,
                    .columns_set = columns_set ? std::min(columns_set, columns) : columns,
                    .value_size = value_size,
                }, samples);
            }
        }
        engine().exit(0);
    });
}
