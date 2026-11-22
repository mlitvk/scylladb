/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/single_row_partition.hh"
#include "replica/logstor/types.hh"
#include "schema/schema_fwd.hh"

class frozen_mutation;
class mutation;

namespace replica::logstor {

// The functions that know what the bytes of a record_value mean. Everything else in logstor
// treats a value as bytes, so changing the encoding is a change to this file alone.
//
// The value holds the partition of the mutation and nothing the record_header already
// carries, which is why decoding takes the header: the key and the table of the record come
// from it. The layout of the value is described in replica/logstor/record_value.cc, next to
// the encoder.

// Returns the timestamp of the record that stores m: the timestamp of the row marker, or of the
// partition tombstone if there is no marker. Throws if m has neither. The two overloads must give
// the same result.
api::timestamp_type record_timestamp(const mutation& m);
api::timestamp_type record_timestamp(const frozen_mutation& m);

// Encodes the partition of m as the value of a record whose header is built from m.
record_value encode_record_value(const mutation& m);

// Encodes the partition of m as the value of a record whose header is built from m. s is the
// schema that m was frozen with.
record_value encode_record_value(const frozen_mutation& m, const schema& s);

// The partition that decode_record_value() returns.
struct decoded_record_value {
    single_row_partition partition;
    // The flag is true if the decode loses a cell of the record that a later schema version can
    // read, as conversion_loses_readable_cell() defines it.
    // An incomplete partition is right for a read under the schema of the decode. It must not go
    // into the cache, because a later schema version of the table can read the lost cell.
    bool incomplete = false;
};

// Decodes the value of a record with header h as a partition of the schema s.
decoded_record_value decode_record_value(const record_value& v, const schema& s, const record_header& h);

} // namespace replica::logstor
