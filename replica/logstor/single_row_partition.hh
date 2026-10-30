/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <optional>
#include <span>
#include "dht/decorated_key.hh"
#include "mutation/mutation_partition.hh"
#include "mutation/tombstone.hh"
#include "schema/schema_fwd.hh"

class mutation;

namespace query {
class partition_slice;
}

namespace replica::logstor {

// The regular columns that a read asks for, made once per read from its slice. The filter refers to
// the ids of the slice, so the slice must not move or be destroyed while the filter is in use.
class column_filter {
    // The ids in the order of the slice, which can name a column twice, or nullopt when the read
    // keeps all cells.
    std::optional<std::span<const column_id>> _ids;
public:
    // All columns.
    column_filter() = default;
    column_filter(const schema& s, const query::partition_slice& slice);

    bool selects_all() const noexcept {
        return !_ids;
    }
    // Only when !selects_all().
    std::span<const column_id> ids() const noexcept {
        return *_ids;
    }
};

// A logstor partition as the cache holds it, and as a reader pushes it into its buffer as fragments
// out of the cache or the record decoder. A logstor table has no clustering columns, so its
// partition is at most one row, with no range tombstones and no static row: the row is absent when
// the partition is only a tombstone, and there is never more than one. The row is held without its
// clustering key, which is always the empty one; a reader adds it when it pushes the row. The
// partition key is not in it either; whoever holds a single_row_partition holds the key beside it.
struct single_row_partition {
    tombstone partition_tombstone;
    std::optional<deletable_row> row;

    // A copy, in the current allocator. The row needs the schema `s` of the partition to copy its
    // cells.
    single_row_partition copy(const schema& s) const {
        single_row_partition partition{.partition_tombstone = partition_tombstone};
        if (row) {
            partition.row.emplace(s, *row);
        }
        return partition;
    }

    // A copy with only the cells of the columns that `columns` selects, when the row marker alone
    // keeps the row live. Otherwise a cell that `columns` does not select can be what keeps the row
    // live, so the copy gets all cells. A cache hit reads with this copy. A read from the log keeps
    // all cells of the decoded row: to remove cells costs more than the querier saves on them.
    single_row_partition copy(const schema& s, const column_filter& columns) const;

private:
    bool can_narrow(const column_filter& columns) const;
};

// A cached partition together with its key, as a read of the cache hands it out. The index entry
// has only the hash of the key, so a point read compares this key with its own.
struct keyed_single_row_partition {
    dht::decorated_key key;
    single_row_partition partition;
};

// What a logstor schema guarantees of a partition, checked rather than fallen back from, since a
// reader built on single_row_partition has nothing to fall back to: no range tombstones, no static
// row, and at most one row, which the record decoder never makes a dummy of. An internal error
// otherwise, naming the partition by `key`.
void check_partition_shape(const mutation_partition&, const schema&, const partition_key& key);

// The partition of a consumed mutation_partition of the schema `s`, whose key is `key`: the row is
// moved out of it, no cell is copied.
single_row_partition to_single_row_partition(mutation_partition&&, const schema& s, const partition_key& key);

// The mutation that a single_row_partition describes.
mutation to_mutation(schema_ptr, dht::decorated_key, single_row_partition&&);

} // namespace replica::logstor
