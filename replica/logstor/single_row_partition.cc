/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/single_row_partition.hh"
#include <algorithm>
#include <type_traits>
#include <seastar/core/format.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include "keys/keys.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition.hh"
#include "query/query-request.hh"
#include "schema/schema.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

void check_partition_shape(const mutation_partition& p, const schema& s, const partition_key& key) {
    if (!p.row_tombstones().empty()) [[unlikely]] {
        on_internal_error(logstor_logger, format("logstor: partition of {} has range tombstones", key.with_schema(s)));
    }
    if (!p.static_row().empty()) [[unlikely]] {
        on_internal_error(logstor_logger, format("logstor: partition of {} has a static row", key.with_schema(s)));
    }
    const auto& rows = p.clustered_rows();
    if (!rows.empty()) {
        auto it = rows.begin();
        if (it->dummy()) [[unlikely]] {
            on_internal_error(logstor_logger, format("logstor: partition of {} has a dummy row", key.with_schema(s)));
        }
        if (std::next(it) != rows.end()) [[unlikely]] {
            on_internal_error(logstor_logger, format("logstor: partition of {} has more than one row", key.with_schema(s)));
        }
    }
}

single_row_partition to_single_row_partition(mutation_partition&& p, const schema& s, const partition_key& key) {
    check_partition_shape(p, s, key);
    single_row_partition partition{.partition_tombstone = p.partition_tombstone()};
    auto& rows = p.mutable_clustered_rows();
    if (!rows.empty()) {
        // Moved out of its entry, which stays in the tree with the key and goes with the partition:
        // what mutation::consume() does with a row.
        partition.row.emplace(std::move(rows.begin()->row()));
    }
    return partition;
}

static_assert(std::is_same_v<column_filter::column_ids, query::column_id_vector>);

column_filter::column_filter(const schema& s, const query::partition_slice& slice) {
    const auto& requested = slice.regular_columns;
    // A slice with as many ids as the table has columns asks for all of them, or names a column
    // twice. The ids are not checked: to keep all cells is correct in both cases.
    if (requested.size() >= s.regular_columns_count()) {
        return;
    }
    column_ids ids(requested.begin(), requested.end());
    std::ranges::sort(ids);
    ids.erase(std::ranges::unique(ids).begin(), ids.end());
    _ids = std::move(ids);
}

bool single_row_partition::can_narrow(const column_filter& columns) const {
    if (!row || columns.selects_all()) {
        return false;
    }
    auto t = row->deleted_at().tomb();
    t.apply(partition_tombstone);
    // An expiring marker is not live at time_point::max(), so the row keeps all cells: the querier
    // decides liveness at the time of the query, which is not known here.
    return row->marker().is_live(t, gc_clock::time_point::max());
}

single_row_partition single_row_partition::copy(const schema& s, const column_filter& columns) const {
    if (!can_narrow(columns)) {
        return copy(s);
    }
    ::row cells;
    for (const column_id id : columns.ids()) {
        if (const auto* cell = row->cells().find_cell(id)) {
            cells.append_cell(id, cell->copy(*s.regular_column_at(id).type));
        }
    }
    single_row_partition partition{.partition_tombstone = partition_tombstone};
    partition.row.emplace(row_tombstone(row->deleted_at()), row_marker(row->marker()), std::move(cells));
    return partition;
}

void single_row_partition::narrow(const column_filter& columns) {
    if (!can_narrow(columns)) {
        return;
    }
    row->cells().remove_if([&columns] (column_id id, const atomic_cell_or_collection&) {
        return !columns.selects(id);
    });
}

mutation to_mutation(schema_ptr s, dht::decorated_key dk, single_row_partition&& partition) {
    mutation m(std::move(s), std::move(dk));
    m.partition().apply(partition.partition_tombstone);
    if (partition.row) {
        auto& dr = m.partition().clustered_row(*m.schema(), clustering_key::make_empty());
        dr.apply(*m.schema(), std::move(*partition.row));
    }
    return m;
}

} // namespace replica::logstor
