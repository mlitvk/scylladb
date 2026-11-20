/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/record_value.hh"

#include <optional>

#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/static_visitor.hpp>

#include "mutation/converting_mutation_partition_applier.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition_serializer.hh"
#include "mutation/mutation_partition_view.hh"
#include "partition_builder.hh"
#include "replica/logstor/single_row_partition.hh"
#include "schema/schema.hh"
#include "serializer_impl.hh"
#include "idl/mutation.dist.hh"
#include "idl/mutation.dist.impl.hh"

namespace replica::logstor {

namespace {

// Writes the partition field of a record value. Both overloads write the same bytes: the first
// encodes a partition from memory, the second copies one that is already encoded.
template <typename Output>
void write_partition(Output& out, const schema& s, const mutation_partition& p) {
    mutation_partition_serializer(s, p).write(ser::writer_of_mutation_partition<Output>(out));
}

template <typename Output>
void write_partition(Output& out, const schema&, const ser::mutation_partition_view& p) {
    ser::serialize(out, p);
}

// The record value is the partition the record holds, encoded as a canonical_mutation stripped
// of what the record_header already carries: the table id and the partition key. What is
// left is the version of the schema the record was written under, the column mapping of that
// schema, and the IDL-serialized mutation_partition, each written by the serializer that
// canonical_mutation uses for it.
//
//   i64  schema_version, most significant half
//   i64  schema_version, least significant half
//        column_mapping       ser::serializer<column_mapping>, from idl/mutation.idl.hh
//        mutation_partition   ser::serializer<mutation_partition>, as mutation_partition_serializer
//                             writes it
template <typename Output, typename Partition>
void write_record_value(Output& out, const schema& s, const Partition& p) {
    ser::serializer<int64_t>::write(out, s.version().uuid().get_most_significant_bits());
    ser::serializer<int64_t>::write(out, s.version().uuid().get_least_significant_bits());
    ser::serialize(out, s.get_column_mapping());
    write_partition(out, s, p);
}

// Reads a record value field by field, in the order above. Construction reads the schema
// version; the caller then either reads or skips the mapping, and reads the partition.
template <typename Input>
class record_value_reader {
    Input& _in;
    table_schema_version _schema_version;

    static table_schema_version read_schema_version(Input& in) {
        const auto msb = ser::serializer<int64_t>::read(in);
        const auto lsb = ser::serializer<int64_t>::read(in);
        return table_schema_version(utils::UUID(msb, lsb));
    }

public:
    explicit record_value_reader(Input& in)
        : _in(in)
        , _schema_version(read_schema_version(in))
    { }

    table_schema_version schema_version() const noexcept { return _schema_version; }

    column_mapping read_mapping() {
        return ser::deserialize(_in, std::type_identity<column_mapping>{});
    }

    void skip_mapping() {
        ser::skip(_in, std::type_identity<column_mapping>{});
    }

    ser::mutation_partition_view read_partition() {
        return ser::deserialize(_in, std::type_identity<ser::mutation_partition_view>{});
    }
};

// Applies a record of another schema version to a partition of the schema `s`, as
// converting_mutation_partition_applier does, and finds the cells that the conversion loses.
// The conversion loses a cell when `s` does not have the column of the cell, or has it with a
// type that cannot read the cell. A cell of a column that `s` dropped at or after the time of the
// cell does not count, because the cell is dead under `s` and under every later schema version.
class record_partition_applier final : public mutation_partition_view_virtual_visitor {
    converting_mutation_partition_applier _applier;
    const column_mapping& _mapping;
    const schema& _schema;
    bool _incomplete = false;

    // Marks the partition incomplete if the conversion loses a cell of `column` whose newest
    // timestamp is `timestamp`.
    void check_cell(const column_mapping_entry& column, api::timestamp_type timestamp) {
        if (const auto* def = _schema.get_column_definition(column.name())) {
            _incomplete |= !def->type->is_value_compatible_with(*column.type());
            return;
        }
        const auto& dropped = _schema.dropped_columns();
        auto it = dropped.find(sstring(to_string_view(column.name())));
        _incomplete |= it == dropped.end() || timestamp > it->second.timestamp;
    }

    void check_cell(const column_mapping_entry& column, const collection_mutation& cell) {
        check_cell(column, collection_mutation_view(cell).last_update(*column.type()));
    }

public:
    record_partition_applier(const column_mapping& mapping, const schema& s, mutation_partition& p)
        : _applier(mapping, s, p)
        , _mapping(mapping)
        , _schema(s)
    { }

    bool incomplete() const noexcept {
        return _incomplete;
    }

    void accept_partition_tombstone(tombstone t) override {
        _applier.accept_partition_tombstone(t);
    }

    void accept_static_cell(column_id id, atomic_cell cell) override {
        check_cell(_mapping.static_column_at(id), cell.timestamp());
        _applier.accept_static_cell(id, std::move(cell));
    }

    void accept_static_cell(column_id id, collection_mutation cell) override {
        check_cell(_mapping.static_column_at(id), cell);
        _applier.accept_static_cell(id, cell);
    }

    stop_iteration accept_row_tombstone(range_tombstone rt) override {
        _applier.accept_row_tombstone(rt);
        return stop_iteration::no;
    }

    stop_iteration accept_row(position_in_partition_view key, row_tombstone deleted_at, row_marker rm, is_dummy dummy,
            is_continuous continuous) override {
        _applier.accept_row(key, deleted_at, rm, dummy, continuous);
        return stop_iteration::no;
    }

    void accept_row_cell(column_id id, atomic_cell cell) override {
        check_cell(_mapping.regular_column_at(id), cell.timestamp());
        _applier.accept_row_cell(id, std::move(cell));
    }

    void accept_row_cell(column_id id, collection_mutation cell) override {
        check_cell(_mapping.regular_column_at(id), cell);
        _applier.accept_row_cell(id, cell);
    }
};

// Returns the serialized partition of a frozen mutation.
ser::mutation_partition_view frozen_partition(const frozen_mutation& m) {
    auto in = ser::as_input_stream(m.representation());
    return ser::deserialize(in, std::type_identity<ser::mutation_view>{}).partition();
}

// Returns the timestamp of a serialized row marker, or nullopt if the row has no marker. Follows
// read_row_marker() in mutation/mutation_partition_view.cc, but reads only the timestamp.
std::optional<api::timestamp_type> marker_timestamp(
        boost::variant<ser::live_marker_view, ser::expiring_marker_view, ser::dead_marker_view,
                ser::no_marker_view, ser::unknown_variant_type> marker) {
    struct visitor : boost::static_visitor<std::optional<api::timestamp_type>> {
        std::optional<api::timestamp_type> operator()(ser::live_marker_view& v) const {
            return v.created_at();
        }
        std::optional<api::timestamp_type> operator()(ser::expiring_marker_view& v) const {
            return v.lm().created_at();
        }
        std::optional<api::timestamp_type> operator()(ser::dead_marker_view& v) const {
            return v.tomb().timestamp();
        }
        std::optional<api::timestamp_type> operator()(ser::no_marker_view&) const {
            return std::nullopt;
        }
        std::optional<api::timestamp_type> operator()(ser::unknown_variant_type&) const {
            throw std::runtime_error("logstor: record has a row marker of an unknown type");
        }
    };
    return boost::apply_visitor(visitor(), marker);
}

} // anonymous namespace

api::timestamp_type record_timestamp(const mutation& m) {
    const auto& partition = m.partition();

    for (const auto& row_entry : partition.clustered_rows()) {
        // mutation_partition_serializer does not write dummy rows, so the frozen overload has no
        // dummy rows to skip.
        if (row_entry.dummy()) {
            continue;
        }
        if (!row_entry.row().marker().is_missing()) {
            return row_entry.row().marker().timestamp();
        }
    }

    if (const auto partition_tombstone = partition.partition_tombstone(); partition_tombstone) {
        return partition_tombstone.timestamp;
    }

    throw std::runtime_error("logstor mutation has no row marker or partition tombstone timestamp");
}

api::timestamp_type record_timestamp(const frozen_mutation& m) {
    auto partition = frozen_partition(m);

    for (auto row : partition.rows()) {
        if (auto ts = marker_timestamp(row.marker())) {
            return *ts;
        }
    }

    if (const auto partition_tombstone = tombstone(partition.tomb()); partition_tombstone) {
        return partition_tombstone.timestamp;
    }

    throw std::runtime_error("logstor mutation has no row marker or partition tombstone timestamp");
}

record_value encode_record_value(const mutation& m) {
    bytes_ostream out;
    write_record_value(out, *m.schema(), m.partition());
    return record_value(std::move(out));
}

record_value encode_record_value(const frozen_mutation& m, const schema& s) {
    bytes_ostream out;
    write_record_value(out, s, frozen_partition(m));
    return record_value(std::move(out));
}

decoded_record_value decode_record_value(const record_value& v, const schema& s, const record_header& h) {
    if (s.id() != h.table) {
        throw std::runtime_error(format("logstor: record of table {} decoded with the schema of table {} ({}.{})",
                h.table, s.id(), s.ks_name(), s.cf_name()));
    }

    auto in = ser::as_input_stream(v.representation());
    record_value_reader reader(in);

    // Decode into a mutation_partition, then move the row out of it.
    mutation_partition p(s);
    bool incomplete = false;
    if (reader.schema_version() == s.version()) {
        reader.skip_mapping();
        auto partition_view = mutation_partition_view::from_view(reader.read_partition());
        partition_builder b(s, p);
        partition_view.accept(s, b);
    } else {
        column_mapping cm = reader.read_mapping();
        record_partition_applier applier(cm, s, p);
        auto partition_view = mutation_partition_view::from_view(reader.read_partition());
        partition_view.accept(cm, applier);
        incomplete = applier.incomplete();
    }
    return decoded_record_value{
        .partition = to_single_row_partition(std::move(p), s, h.key.key()),
        .incomplete = incomplete,
    };
}

} // namespace replica::logstor
