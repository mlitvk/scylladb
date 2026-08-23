/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/simple-stream.hh>

#include "bytes_fwd.hh"
#include "mutation/timestamp.hh"

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

// The size the partition takes encoded. Walks the partition without allocating or
// copying, so that the record can be written straight into the write buffer.
size_t measure_row_value(const schema& s, const mutation_partition& p, api::timestamp_type base_ts);

// Encodes the partition into out, which must have room for measure_row_value() bytes.
void write_row_value(seastar::simple_memory_output_stream& out, const schema& s, const mutation_partition& p,
        api::timestamp_type base_ts);

// Decodes an encoded partition into p, which must be empty. The cells are read straight
// out of value, so it does not have to outlive the call.
void read_row_value_into(mutation_partition& p, const schema& s, bytes_view value, api::timestamp_type base_ts);

} // namespace replica::logstor
