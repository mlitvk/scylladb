/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/update.hh"

#include "mutation/mutation.hh"
#include "schema/schema.hh"

namespace replica::logstor {

namespace {

row_marker marker_for_created_row(const deletable_row& row, const schema& s, api::timestamp_type ts, gc_clock::time_point now) {
    if (!row.is_live(s, column_kind::regular_column, tombstone(), now)) {
        return row_marker(tombstone(ts, now));
    }
    bool has_non_expiring_cell = false;
    std::optional<row_marker> longest;
    row.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        const auto& def = s.column_at(column_kind::regular_column, id);
        if (!def.is_atomic()) {
            // The elements of a collection expire one by one, so the collection is taken as live
            // without end.
            has_non_expiring_cell = true;
            return;
        }
        auto ac = cell.as_atomic_cell(def);
        if (!ac.is_live(tombstone(), now, false)) {
            return;
        }
        if (!ac.is_live_and_has_ttl()) {
            has_non_expiring_cell = true;
            return;
        }
        if (!longest || ac.expiry() > longest->expiry()) {
            longest.emplace(ts, ac.ttl(), ac.expiry());
        }
    });
    return has_non_expiring_cell ? row_marker(ts) : *longest;
}

} // anonymous namespace

mutation apply_update(std::optional<mutation> partition, mutation update, api::timestamp_type ts, gc_clock::time_point now) {
    if (update.partition().partition_tombstone()) {
        return update;
    }
    const auto& s = *update.schema();
    mutation out = std::move(update);
    if (partition) {
        partition->apply(std::move(out));
        out = std::move(*partition);
    }
    auto& row = out.partition().clustered_row(s, clustering_key::make_empty());
    const auto& marker = row.marker();
    if (marker.is_live(tombstone(), now)) {
        row.apply(marker.is_expiring() ? row_marker(ts, marker.ttl(), marker.expiry()) : row_marker(ts));
    } else {
        row.apply(marker_for_created_row(row, s, ts, now));
    }
    return out;
}

} // namespace replica::logstor
