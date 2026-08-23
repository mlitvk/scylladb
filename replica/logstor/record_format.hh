/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <memory>
#include <optional>
#include <vector>

#include <seastar/core/simple-stream.hh>

#include "bytes_fwd.hh"
#include "mutation/timestamp.hh"
#include "schema/schema_fwd.hh"
#include "types/types.hh"

class column_definition;
class mutation_partition;
class schema;

namespace replica::logstor {

// The value of a logstor record: the encoding of the partition the record holds.
//
// A logstor table has no clustering columns, so a partition is at most one row - the one
// with the empty clustering key - plus a partition tombstone, and has neither static
// columns nor range tombstones. The encoding is built for that shape alone and for the
// point read that decodes it, which is why logstor does not store a canonical_mutation:
// that form carries a full column_mapping and four bytes of framing per field, and costs
// about half of the space of a record and two copies of every cell value to decode.
//
// Every timestamp is written as a delta from the record's own timestamp
// (log_record_header::timestamp, the timestamp of the row marker or of the partition
// tombstone), and every deletion time as a delta from that timestamp's second. Both
// deltas are almost always zero, so they cost a byte. Everything is varint-encoded and
// every optional field is described by a flag bit rather than by a frame.
//
// The record is self-contained: it carries the schema version it was written under and a
// description of the columns of that schema, so it can be decoded without any state
// outside of it.
//
//     u8    flags
//     u128  schema_version
//     uvint schema_description_size          -- lets a same-version read skip the
//     ---- schema description ----              description without parsing it
//     uvint n_types ; n_types x (uvint size ; type_name)         -- interned type names
//     uvint n_columns
//     n_columns x (uvint size ; column_name ; uvint type_index)  -- in column id order
//     ---- partition and row metadata, each present only per flags ----
//     partition_tombstone   : svint ts_delta ; svint deletion_time_delta
//     marker live           : svint ts_delta
//            expiring       : svint ts_delta ; uvint ttl ; svint expiry_delta
//            dead           : svint ts_delta ; svint deletion_time_delta
//     deleted_at            : svint ts_delta ; svint deletion_time_delta
//     shadowable_deleted_at : svint ts_delta ; svint deletion_time_delta
//     ---- cells ----
//     [column bitmap]       -- only if has_column_bitmap, otherwise every column of the
//                              schema is present. uvint for up to 64 columns, else a raw
//                              ceil(n_columns / 8) byte bitmap
//     for each present column, in column id order:
//         u8    cell_flags
//         [svint ts_delta]              unless use_base_timestamp
//         [svint deletion_time_delta]   if is_deleted
//         [uvint ttl ; svint expiry_delta] if is_expiring and not use_marker_ttl
//         [uvint value_size]            if has_value_length
//         value
//
// A cell of a fixed-width type writes no value size: it is taken from the type. A
// non-frozen collection is stored as its collection_mutation blob verbatim, which is
// already a compact form.

// The flags byte at the head of a record value.
namespace row_value_flags {

inline constexpr uint8_t has_partition_tombstone = 0x01;
inline constexpr uint8_t has_row = 0x02;
inline constexpr uint8_t has_deleted_at = 0x04;
inline constexpr uint8_t has_shadowable_deleted_at = 0x08;
inline constexpr uint8_t marker_kind_mask = 0x30;
inline constexpr uint8_t marker_kind_shift = 4;
inline constexpr uint8_t has_column_bitmap = 0x40;
// Set when another flags byte follows. Unused, for a future extension of the format.
inline constexpr uint8_t extension = 0x80;

} // namespace row_value_flags

enum class marker_kind : uint8_t {
    none = 0,
    live = 1,
    expiring = 2,
    dead = 3,
};

// The flags byte at the head of every cell.
namespace cell_flags {

inline constexpr uint8_t is_deleted = 0x01;
inline constexpr uint8_t is_expiring = 0x02;
inline constexpr uint8_t has_empty_value = 0x04;
// The cell's timestamp is the record's timestamp, so no delta is written.
inline constexpr uint8_t use_base_timestamp = 0x08;
// The cell's ttl and expiry are the row marker's, so neither is written.
inline constexpr uint8_t use_marker_ttl = 0x10;
inline constexpr uint8_t is_collection = 0x20;
inline constexpr uint8_t has_value_length = 0x40;

} // namespace cell_flags

// Maps the columns of a record written under one schema version onto the columns of the
// schema it is read under. Building one parses the record's schema description, a type parse
// per column, so it is built once per pair of schema versions rather than once per record -
// which is what column_translation_cache is for.
//
// A column the schema no longer has, or one whose type can no longer hold the values the
// record holds for it, is dropped from the decoded partition rather than reported. That is
// what a logstor read does today, through canonical_mutation and
// converting_mutation_partition_applier, and the format keeps it. Note that this is more
// lenient than the sstable reader, which refuses such a record.
class column_translation {
public:
    struct column {
        // The type the record was written with. It, and not the schema's type, is what the
        // record is decoded with: the two can differ by a value-compatible ALTER TYPE, and
        // it is the record that says how many bytes its values take.
        data_type type;
        std::optional<uint32_t> value_length;
        // Null when the schema being read has no column of that name, or has one the
        // record's values do not fit. Either way the cell is parsed and dropped.
        const column_definition* def = nullptr;
    };

private:
    // Holds the schema the column definitions belong to.
    schema_ptr _schema;
    table_schema_version _record_version;
    std::vector<column> _columns;

public:
    // description is the schema description of a record written under record_version.
    column_translation(const schema& s, table_schema_version record_version, bytes_view description);

    table_schema_version record_version() const noexcept { return _record_version; }
    const std::vector<column>& columns() const noexcept { return _columns; }
};

// The translations a table needs, cached. Held per table, and passed to every read: a read
// of a record written under the schema version it is read under does not touch it.
class column_translation_cache {
    // A table's records span more schema versions than this only while several ALTERs are
    // in flight, and a translation is cheap to rebuild, so the cache is simply emptied
    // rather than grown or ordered by use.
    static constexpr size_t max_entries = 8;

    // The schema version the entries translate to. A schema change makes all of them stale.
    table_schema_version _schema_version;
    // unique_ptr so that a translation handed out stays put when the cache grows.
    std::vector<std::unique_ptr<const column_translation>> _translations;

public:
    // The translation of a record written under record_version to the schema s. Parses the
    // record's schema description only for a pair of versions not seen yet.
    const column_translation& get(const schema& s, table_schema_version record_version, bytes_view description);

    size_t size() const noexcept { return _translations.size(); }
};

// The size the partition takes encoded. Walks the partition without allocating or
// copying, so that the record can be written straight into the write buffer.
size_t measure_row_value(const schema& s, const mutation_partition& p, api::timestamp_type base_ts);

// Encodes the partition into out, which must have room for measure_row_value() bytes.
void write_row_value(seastar::simple_memory_output_stream& out, const schema& s, const mutation_partition& p,
        api::timestamp_type base_ts);

// Decodes an encoded partition into p, which must be empty. The cells are read straight
// out of value, so it does not have to outlive the call. translations is consulted only for
// a record written under a schema version other than the one it is read under.
void read_row_value_into(mutation_partition& p, const schema& s, bytes_view value, api::timestamp_type base_ts,
        column_translation_cache& translations);

} // namespace replica::logstor
