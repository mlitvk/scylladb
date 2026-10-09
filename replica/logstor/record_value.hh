/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/types.hh"
#include "schema/schema_fwd.hh"

class frozen_mutation;
class mutation;

namespace replica::logstor {

class column_mapping_registry;
struct single_row_partition;

// The functions that know what the bytes of a record_value mean. Everything else in logstor
// treats a value as bytes, so changing the encoding is a change to this file alone.
//
// The value holds the partition of the mutation and nothing the record_header already
// carries, which is why decoding takes the header: the key, the table and the schema version of
// the record come from it. The layout of the value is described in replica/logstor/record_value.cc,
// next to the encoder.

// Returns the timestamp of the record that stores m: the timestamp of the row marker, or of the
// partition tombstone if there is no marker. Throws if m has neither. The two overloads must give
// the same result.
api::timestamp_type record_timestamp(const mutation& m);
api::timestamp_type record_timestamp(const frozen_mutation& m);

// Encodes the partition of m as the value of a record whose header is built from m. The two
// overloads give the same bytes.
record_value encode_record_value(const mutation& m);
record_value encode_record_value(const frozen_mutation& m);

// Decodes the value of a record with header h as a partition of the schema s. If the record has
// another schema version than s, `mappings` gives the column mapping of the record's version.
single_row_partition decode_record_value(const record_value& v, const schema& s, const record_header& h,
        const column_mapping_registry& mappings);

} // namespace replica::logstor
