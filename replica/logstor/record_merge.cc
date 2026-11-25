/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/record_merge.hh"
#include <fmt/format.h>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include "replica/logstor/record_value.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

merge_outcome merge_tied_records(schema_ptr s, const dht::decorated_key& dk, single_row_partition stored, single_row_partition incoming) {
    const auto stored_mutation = to_mutation(s, dk, std::move(stored));
    const auto incoming_mutation = to_mutation(std::move(s), dk, std::move(incoming));

    auto merged = stored_mutation;
    merged.apply(incoming_mutation);

    if (merged == stored_mutation) {
        return {.result = merge_result::stored_contains_incoming};
    }
    if (merged == incoming_mutation) {
        return {.result = merge_result::incoming_contains_stored};
    }
    // The index keeps the timestamp of the stored record for the merge, and recovery reads the
    // timestamp of the merge from its record, so the two must be equal.
    const auto merged_timestamp = record_timestamp(merged);
    if (const auto stored_timestamp = record_timestamp(stored_mutation); merged_timestamp != stored_timestamp) {
        on_internal_error(logstor_logger, fmt::format("The merge of two records of key {} has timestamp {}, and the stored record has timestamp {}",
                dk, merged_timestamp, stored_timestamp));
    }
    return {.result = merge_result::merged, .merged = std::move(merged)};
}

} // namespace replica::logstor
