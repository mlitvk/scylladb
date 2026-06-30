#pragma once

/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "bytes_ostream.hh"
#include "keys/keys.hh"
#include "mutation/mutation.hh"
#include "schema/schema_fwd.hh"

namespace replica::logstor {

/// Compact self-contained serialization of a logstor mutation payload.
///
/// The format is specialized for logstor tables:
///  - no clustering columns are supported,
///  - at most one logical row is stored,
///  - regular columns must be atomic, including frozen collections/UDTs,
///  - range tombstones and static rows are rejected,
///  - partition tombstone only mutations are supported.
///
/// The serialized payload stores the full partition key, schema version, row
/// metadata, and per-cell column descriptors so it can be read after schema
/// upgrades without relying on the original in-memory mutation shape.
class logstor_mutation {
    bytes_ostream _data;

public:
    logstor_mutation() = default;
    explicit logstor_mutation(bytes_ostream data) noexcept
        : _data(std::move(data)) {
    }
    explicit logstor_mutation(const mutation& m);

    const bytes_ostream& representation() const noexcept {
        return _data;
    }

    mutation to_mutation(schema_ptr schema) const;
};

struct log_record_data {
    logstor_mutation mut;

    size_t serialized_size() const noexcept;
    void write(seastar::simple_output_stream& out) const;
};

log_record_data deserialize_log_record_data(seastar::simple_memory_input_stream in);

}
