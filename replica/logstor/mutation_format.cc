/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/mutation_format.hh"

#include <stdexcept>
#include <string_view>
#include <vector>

#include <seastar/core/simple-stream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/assert.hh>

#include "bytes_fwd.hh"
#include "dht/i_partitioner.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"
#include "mutation/atomic_cell_or_collection.hh"
#include "mutation/tombstone.hh"
#include "schema/schema.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "sstables/writer.hh"
#include "types/types.hh"
#include "utils/to_string.hh"
#include "vint-serialization.hh"

namespace replica::logstor {

namespace {

/*
 * Serialized logstor mutation format, version 1.
 *
 * All size/count fields use unsigned vint encoding unless stated otherwise.
 * Fixed-width integers are written with the generic serializer helpers.
 *
 * Layout:
 *
 *   u8  version
 *   uuid schema_version
 *   partition_key
 *   u8  mutation_flags
 *   [partition_tombstone]            iff has_partition_tombstone
 *   [row_marker]                     iff has_row && has_row_marker
 *   [row_tombstone]                  iff has_row && has_row_tombstone
 *   cell_count
 *   repeated cell
 *
 * partition_key:
 *   component_count
 *   repeated:
 *     component_size
 *     component_bytes
 *
 * cell:
 *   column_id
 *   column_name_size
 *   column_name_bytes
 *   type_name_size
 *   type_name_bytes
 *   payload_size
 *   payload_bytes
 *
 * payload_bytes stores atomic_cell_view::serialize(). This includes frozen
 * collections and frozen UDTs, which are represented as atomic cells. Multi-cell
 * regular columns are rejected before logstor_mutation construction.
 *
 * Invariants:
 *   - logstor stores either zero rows or one row at clustering_key::make_empty()
 *   - static rows and range tombstones are not representable here
 *   - a partition delete may have has_row == false and cell_count == 0
 */
static constexpr uint8_t logstor_mutation_version = 1;

enum class mutation_flags : uint8_t {
    none = 0,
    has_partition_tombstone = 1 << 0,
    has_row_marker = 1 << 1,
    has_row_tombstone = 1 << 2,
    has_row = 1 << 3,
};

inline mutation_flags operator|(mutation_flags lhs, mutation_flags rhs) {
    return mutation_flags(uint8_t(lhs) | uint8_t(rhs));
}

inline mutation_flags& operator|=(mutation_flags& lhs, mutation_flags rhs) {
    lhs = lhs | rhs;
    return lhs;
}

inline bool has_flag(mutation_flags value, mutation_flags flag) {
    return (uint8_t(value) & uint8_t(flag)) != 0;
}

void write_bytes_with_vint(bytes_ostream& out, bytes_view bytes) {
    sstables::write_vint(out, bytes.size());
    out.write(bytes);
}

template <typename Input>
bytes read_vint_bytes(Input& in) {
    if (in.size() == 0) {
        throw std::runtime_error("truncated logstor mutation bytes length");
    }
    bytes::value_type len_first = 0;
    in.read(reinterpret_cast<char*>(&len_first), 1);
    auto len_size = unsigned_vint::serialized_size_from_first_byte(len_first);
    if (in.size() < len_size - 1) {
        throw std::runtime_error("truncated logstor mutation bytes length");
    }
    bytes len_buf(bytes::initialized_later(), len_size);
    len_buf[0] = len_first;
    if (len_size > 1) {
        in.read(reinterpret_cast<char*>(len_buf.begin() + 1), len_size - 1);
    }
    auto len = unsigned_vint::deserialize(len_buf);
    if (in.size() < len) {
        throw std::runtime_error("truncated logstor mutation bytes payload");
    }
    bytes value(bytes::initialized_later(), len);
    in.read(reinterpret_cast<char*>(value.begin()), len);
    return value;
}

template <typename Input>
managed_bytes read_vint_managed_bytes(Input& in) {
    if (in.size() == 0) {
        throw std::runtime_error("truncated logstor mutation bytes length");
    }
    bytes::value_type len_first = 0;
    in.read(reinterpret_cast<char*>(&len_first), 1);
    auto len_size = unsigned_vint::serialized_size_from_first_byte(len_first);
    if (in.size() < len_size - 1) {
        throw std::runtime_error("truncated logstor mutation bytes length");
    }
    bytes len_buf(bytes::initialized_later(), len_size);
    len_buf[0] = len_first;
    if (len_size > 1) {
        in.read(reinterpret_cast<char*>(len_buf.begin() + 1), len_size - 1);
    }
    auto len = unsigned_vint::deserialize(len_buf);
    if (in.size() < len) {
        throw std::runtime_error("truncated logstor mutation bytes payload");
    }
    auto value = managed_bytes(managed_bytes::initialized_later(), len);
    auto view = managed_bytes_mutable_view(value);
    while (!view.empty()) {
        auto fragment = view.current_fragment();
        in.read(reinterpret_cast<char*>(fragment.data()), fragment.size());
        view.remove_current();
    }
    return value;
}

sstring bytes_to_sstring(bytes_view bytes) {
    return sstring(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

template <typename Input>
uint64_t read_unsigned_vint(Input& in) {
    if (in.size() == 0) {
        throw std::runtime_error("truncated logstor mutation vint");
    }
    bytes::value_type first = 0;
    in.read(reinterpret_cast<char*>(&first), 1);
    auto len = unsigned_vint::serialized_size_from_first_byte(first);
    if (in.size() < len - 1) {
        throw std::runtime_error("truncated logstor mutation vint");
    }
    bytes buf(bytes::initialized_later(), len);
    buf[0] = first;
    if (len > 1) {
        in.read(reinterpret_cast<char*>(buf.begin() + 1), len - 1);
    }
    return unsigned_vint::deserialize(buf);
}

template <typename Input>
void skip_vint_bytes(Input& in) {
    auto len = read_unsigned_vint(in);
    if (in.size() < len) {
        throw std::runtime_error("truncated logstor mutation bytes payload");
    }
    in.skip(len);
}

void write_tombstone(bytes_ostream& out, tombstone t) {
    ser::serialize(out, t.timestamp);
    ser::serialize(out, int64_t(t.deletion_time.time_since_epoch().count()));
}

template <typename Input>
tombstone read_tombstone(Input& in) {
    auto timestamp = ser::deserialize(in, std::type_identity<api::timestamp_type>{});
    auto deletion_time = ser::deserialize(in, std::type_identity<int64_t>{});
    return tombstone(timestamp, gc_clock::time_point(gc_clock::duration(deletion_time)));
}

void write_row_marker(bytes_ostream& out, const row_marker& marker) {
    ser::serialize(out, marker.timestamp());
    auto ttl = marker.is_expiring() ? int64_t(marker.ttl().count()) : int64_t(0);
    auto expiry = marker.is_expiring() ? int64_t(marker.expiry().time_since_epoch().count())
                                       : (marker.is_live() ? int64_t(0) : int64_t(marker.deletion_time().time_since_epoch().count()));
    if (!marker.is_live()) {
        ttl = -1;
    }
    ser::serialize(out, ttl);
    ser::serialize(out, expiry);
}

template <typename Input>
row_marker read_row_marker(Input& in) {
    auto timestamp = ser::deserialize(in, std::type_identity<api::timestamp_type>{});
    auto ttl = ser::deserialize(in, std::type_identity<int64_t>{});
    auto expiry = ser::deserialize(in, std::type_identity<int64_t>{});
    if (ttl == 0) {
        return row_marker(timestamp);
    }
    if (ttl == -1) {
        return row_marker(tombstone(timestamp, gc_clock::time_point(gc_clock::duration(expiry))));
    }
    return row_marker(timestamp, gc_clock::duration(ttl), gc_clock::time_point(gc_clock::duration(expiry)));
}

void write_row_tombstone(bytes_ostream& out, const row_tombstone& tomb) {
    write_tombstone(out, tomb.regular());
    ser::serialize(out, uint8_t(tomb.is_shadowable() == is_shadowable::yes));
    if (tomb.is_shadowable()) {
        write_tombstone(out, tomb.shadowable().tomb());
    }
}

template <typename Input>
row_tombstone read_row_tombstone(Input& in) {
    auto regular = read_tombstone(in);
    auto has_shadowable = ser::deserialize(in, std::type_identity<uint8_t>{}) != 0;
    if (!has_shadowable) {
        return row_tombstone(regular);
    }
    auto shadowable = read_tombstone(in);
    return row_tombstone(regular, shadowable_tombstone(shadowable));
}

void write_partition_key(bytes_ostream& out, partition_key_view key) {
    auto representation = partition_key(key).explode();
    sstables::write_vint(out, representation.size());
    for (bytes_view component : representation) {
        write_bytes_with_vint(out, component);
    }
}

template <typename Input>
partition_key read_partition_key(Input& in) {
    auto component_count = read_unsigned_vint(in);
    std::vector<bytes> components;
    components.reserve(component_count);
    for (size_t i = 0; i < component_count; ++i) {
        components.push_back(read_vint_bytes(in));
    }
    return partition_key::from_exploded(std::move(components));
}

struct row_payload {
    bool has_row = false;
    row_marker marker;
    row_tombstone tomb;
    const row* cells = nullptr;
};

row_payload get_single_logstor_row(const mutation& m) {
    const auto& partition = m.partition();

    if (!partition.static_row().empty()) {
        throw std::runtime_error("logstor mutation format does not support static rows");
    }
    if (!partition.row_tombstones().empty()) {
        throw std::runtime_error("logstor mutation format does not support range tombstones");
    }

    row_payload payload{};
    bool found = false;
    for (const auto& entry : partition.clustered_rows()) {
        if (entry.dummy()) {
            continue;
        }
        if (found) {
            throw std::runtime_error("logstor mutation format supports exactly one clustering row");
        }
        if (entry.key() != clustering_key::make_empty()) {
            throw std::runtime_error("logstor mutation format supports only empty clustering key rows");
        }
        found = true;
        payload.has_row = true;
        payload.marker = entry.row().marker();
        payload.tomb = entry.row().deleted_at();
        payload.cells = &entry.row().cells();
    }

    return payload;
}

void maybe_validate_dropped_column(const schema& s, const bytes& column_name, api::timestamp_type cell_timestamp) {
    auto dropped_it = s.dropped_columns().find(bytes_to_sstring(column_name));
    if (dropped_it != s.dropped_columns().end() && cell_timestamp <= dropped_it->second.timestamp) {
        throw std::runtime_error("cell is for a dropped column");
    }
}

}

logstor_mutation::logstor_mutation(const mutation& m) {
    auto row = get_single_logstor_row(m);

    bytes_ostream out;
    ser::serialize(out, logstor_mutation_version);
    ser::serialize(out, m.schema()->version());
    write_partition_key(out, m.key());

    auto flags = mutation_flags::none;
    if (m.partition().partition_tombstone()) {
        flags |= mutation_flags::has_partition_tombstone;
    }
    if (row.has_row) {
        flags |= mutation_flags::has_row;
    }
    if (row.has_row && !row.marker.is_missing()) {
        flags |= mutation_flags::has_row_marker;
    }
    if (row.has_row && row.tomb) {
        flags |= mutation_flags::has_row_tombstone;
    }
    ser::serialize(out, uint8_t(flags));

    if (has_flag(flags, mutation_flags::has_partition_tombstone)) {
        write_tombstone(out, m.partition().partition_tombstone());
    }
    if (has_flag(flags, mutation_flags::has_row)) {
        if (has_flag(flags, mutation_flags::has_row_marker)) {
            write_row_marker(out, row.marker);
        }
        if (has_flag(flags, mutation_flags::has_row_tombstone)) {
            write_row_tombstone(out, row.tomb);
        }
    }

    auto cell_count = row.has_row ? row.cells->size() : 0;
    sstables::write_vint(out, cell_count);
    if (row.has_row) {
        row.cells->for_each_cell([&schema = *m.schema(), &out] (column_id id, const atomic_cell_or_collection& cell) {
            const auto& cdef = schema.column_at(column_kind::regular_column, id);
            if (!cdef.is_atomic()) {
                throw std::runtime_error("logstor mutation format does not support multi-cell columns");
            }
            sstables::write_vint(out, uint64_t(id));
            write_bytes_with_vint(out, cdef.name());
            write_bytes_with_vint(out, to_bytes_view(cdef.type->name()));
            auto serialized = cell.as_atomic_cell(cdef).serialize();
            sstables::write_vint(out, serialized.size());
            out.write(serialized);
        });
    }

    _data = std::move(out);
}

mutation logstor_mutation::to_mutation(schema_ptr schema) const {
    auto in = ser::as_input_stream(_data);

    auto version = ser::deserialize(in, std::type_identity<uint8_t>{});
    if (version != logstor_mutation_version) {
        throw std::runtime_error(format("unsupported logstor mutation format version {}", version));
    }

    auto stored_schema_version = ser::deserialize(in, std::type_identity<table_schema_version>{});
    auto key = read_partition_key(in);
    auto dk = dht::decorate_key(*schema, key);
    mutation m(schema, std::move(dk));

    auto flags = mutation_flags(ser::deserialize(in, std::type_identity<uint8_t>{}));
    if (has_flag(flags, mutation_flags::has_partition_tombstone)) {
        m.partition().apply(read_tombstone(in));
    }

    deletable_row* row = nullptr;
    if (has_flag(flags, mutation_flags::has_row)) {
        row = &m.partition().clustered_row(*schema, clustering_key::make_empty());
        if (has_flag(flags, mutation_flags::has_row_marker)) {
            row->apply(read_row_marker(in));
        }
        if (has_flag(flags, mutation_flags::has_row_tombstone)) {
            row->apply(read_row_tombstone(in));
        }
    }

    auto cell_count = read_unsigned_vint(in);
    auto same_schema_version = stored_schema_version == schema->version();
    for (size_t i = 0; i < cell_count; ++i) {
        auto stored_column_id = column_id(read_unsigned_vint(in));
        const column_definition* cdef = nullptr;
        if (same_schema_version) {
            cdef = &schema->column_at(column_kind::regular_column, stored_column_id);
            skip_vint_bytes(in); // column name, needed only for schema upgrades
            skip_vint_bytes(in); // type name, needed only for schema upgrades
        } else {
            auto column_name = read_vint_bytes(in);
            auto type_name_bytes = read_vint_bytes(in);
            cdef = schema->get_column_definition(column_name);
            if (cdef && cdef->is_regular()) {
                auto stored_type_name = bytes_to_sstring(type_name_bytes);
                if (cdef->type->name() != stored_type_name) {
                    cdef = nullptr;
                }
            }
        }
        if (!cdef || !cdef->is_regular()) {
            skip_vint_bytes(in);
            continue;
        }

        auto payload = read_vint_managed_bytes(in);
        if (!cdef->is_atomic()) {
            continue;
        }
        auto ac = atomic_cell::from_serialized(std::move(payload));
        maybe_validate_dropped_column(*schema, cdef->name(), ac.timestamp());

        if (!row) {
            row = &m.partition().clustered_row(*schema, clustering_key::make_empty());
        }
        row->cells().apply(*cdef, std::move(ac));
    }

    return m;
}

size_t log_record_data::serialized_size() const noexcept {
    return mut.representation().size();
}

void log_record_data::write(seastar::simple_output_stream& out) const {
    for (bytes_view fragment : mut.representation().fragments()) {
        out.write(reinterpret_cast<const char*>(fragment.data()), fragment.size());
    }
}

log_record_data deserialize_log_record_data(seastar::simple_memory_input_stream in) {
    bytes payload(bytes::initialized_later(), in.size());
    in.read(reinterpret_cast<char*>(payload.begin()), payload.size());
    bytes_ostream out;
    out.write(payload);
    return log_record_data{.mut = logstor_mutation(std::move(out))};
}

}
