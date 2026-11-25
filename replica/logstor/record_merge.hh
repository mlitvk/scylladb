/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <optional>
#include "dht/decorated_key.hh"
#include "mutation/mutation.hh"
#include "replica/logstor/single_row_partition.hh"
#include "schema/schema_fwd.hh"

namespace replica::logstor {

// What the merge of two records of one key with equal timestamps gives.
enum class merge_result {
    // The stored record already has all the content of the incoming record.
    stored_contains_incoming,
    // The incoming record has all the content of the stored record, and more.
    incoming_contains_stored,
    // Each record has content that the other record does not have.
    merged,
};

struct merge_outcome {
    merge_result result;
    // The merge of the two records. It is set only when `result` is merge_result::merged.
    std::optional<mutation> merged;
};

// Merges two records of the key `dk` that have equal timestamps, by the rules of mutation::apply().
// The cells, the row marker and the tombstones reconcile as they do when a read merges the answers
// of two replicas, so the merge does not depend on the order of the two records.
// A record contains the other one when the merge is equal to it. When the two records are equal,
// the result is merge_result::stored_contains_incoming.
// `stored` and `incoming` are partitions of the schema `s`. The caller must give two records with
// equal timestamps, so that the timestamp of the merge is the same timestamp.
merge_outcome merge_tied_records(schema_ptr s, const dht::decorated_key& dk, single_row_partition stored, single_row_partition incoming);

} // namespace replica::logstor
