/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <optional>
#include "gc_clock.hh"
#include "mutation/timestamp.hh"

class mutation;

namespace replica::logstor {

// The partition that a write stores when `update` changes a stored partition in place, as a
// conditional statement does: `partition` is what the coordinator read, nullopt when no replica
// has the key, and `ts` is the timestamp of the proposal and so of the record. The result is
// `partition` with `update` applied, under a row marker at `ts`: the marker keeps its TTL when it
// has one. A partition tombstone in `update` is stored alone, since it shadows the row. A row that
// `update` creates gets a marker that expires with its longest-living cell, or a dead marker when
// no cell is live, so the row is visible for as long as it is on an sstable table.
mutation apply_update(std::optional<mutation> partition, mutation update, api::timestamp_type ts,
        gc_clock::time_point now = gc_clock::now());

} // namespace replica::logstor
