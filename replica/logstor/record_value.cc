/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/record_value.hh"

#include <optional>
#include <stdexcept>
#include <string_view>

#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/static_visitor.hpp>
#include <fmt/format.h>

#include "mutation/converting_mutation_partition_applier.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition_serializer.hh"
#include "mutation/mutation_partition_view.hh"
#include "partition_builder.hh"
#include "replica/logstor/column_mapping_registry.hh"
#include "replica/logstor/single_row_partition.hh"
#include "schema/schema.hh"
#include "serializer_impl.hh"
#include "idl/mutation.dist.hh"
#include "idl/mutation.dist.impl.hh"

namespace replica::logstor {

namespace {

// The record value is the partition the record holds, encoded as a canonical_mutation stripped
// of what the record_header already carries - the table id, the partition key and the
// schema version - and of the column mapping, which the schema table of the chunk holds. What
// is left is the IDL-serialized mutation_partition, as mutation_partition_serializer writes it.
//
// Both overloads write the same bytes: the first encodes a partition from memory, the second
// copies one that is already encoded.
template <typename Output>
void write_record_value(Output& out, const schema& s, const mutation_partition& p) {
    mutation_partition_serializer(s, p).write(ser::writer_of_mutation_partition<Output>(out));
}

template <typename Output>
void write_record_value(Output& out, const ser::mutation_partition_view& p) {
    ser::serialize(out, p);
}

template <typename Input>
ser::mutation_partition_view read_record_value(Input& in) {
    return ser::deserialize(in, std::type_identity<ser::mutation_partition_view>{});
}

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

// The column mapping that converts a record written under another schema version than `s`.
// Throws std::runtime_error if the registry has no mapping of the record's version for its table.
const column_mapping& mapping_for_conversion(const column_mapping_registry& mappings, const record_header& h, const schema& s) {
    auto fail = [&] (std::string_view problem) {
        return std::runtime_error(fmt::format("logstor: record of table {} ({}.{}) written under schema version {} {}",
                h.table, s.ks_name(), s.cf_name(), h.schema_version, problem));
    };
    const auto* entry = mappings.find(h.schema_version);
    if (!entry) [[unlikely]] {
        throw fail("has no column mapping");
    }
    if (entry->table != h.table) [[unlikely]] {
        throw fail(fmt::format("has the column mapping of table {}", entry->table));
    }
    return entry->mapping;
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

    throw std::runtime_error("logstor: mutation has no row marker or partition tombstone timestamp");
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

    throw std::runtime_error("logstor: mutation has no row marker or partition tombstone timestamp");
}

record_value encode_record_value(const mutation& m) {
    bytes_ostream out;
    write_record_value(out, *m.schema(), m.partition());
    return record_value(std::move(out));
}

record_value encode_record_value(const frozen_mutation& m) {
    bytes_ostream out;
    write_record_value(out, frozen_partition(m));
    return record_value(std::move(out));
}

single_row_partition decode_record_value(const record_value& v, const schema& s, const record_header& h, const column_mapping_registry& mappings) {
    if (s.id() != h.table) {
        throw std::runtime_error(fmt::format("logstor: record of table {} decoded with the schema of table {} ({}.{})",
                h.table, s.id(), s.ks_name(), s.cf_name()));
    }

    auto in = ser::as_input_stream(v.representation());
    auto partition_view = mutation_partition_view::from_view(read_record_value(in));

    // Decode into a mutation_partition, then move the row out of it.
    mutation_partition p(s);
    if (h.schema_version == s.version()) {
        partition_builder b(s, p);
        partition_view.accept(s, b);
    } else {
        const auto& mapping = mapping_for_conversion(mappings, h, s);
        converting_mutation_partition_applier applier(mapping, s, p);
        partition_view.accept(mapping, applier);
    }
    return to_single_row_partition(std::move(p), s, h.key.key());
}

} // namespace replica::logstor
