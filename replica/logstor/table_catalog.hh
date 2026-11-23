/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/sstring.hh>

#include "dht/token.hh"
#include "schema/schema_fwd.hh"

namespace replica::logstor {

class primary_index;
class logstor_group;

// A table_catalog gives the index of a logstor table, and the compaction group of this shard
// that owns a token or a token range of the table.
//
// The segment manager receives the catalog at construction and uses it until stop() returns.
// The catalog does not keep a table or a group alive, so the pointers that index() and find()
// return are valid until the caller suspends.
// A caller can keep a pointer over a suspension only when no group can stop in that time, which
// is true during recovery and start().
class table_catalog {
public:
    virtual ~table_catalog() = default;

    // The index of a table and one of its compaction groups, found with one lookup of the table.
    struct target {
        primary_index* index = nullptr;
        logstor_group* group = nullptr;
    };

    // The index of the table, or null when the table does not exist or does not use logstor.
    virtual primary_index* index(table_id) = 0;

    // The index of the table, and the compaction group that owns the token on this shard or null.
    virtual target find(table_id, dht::token) = 0;

    // The index of the table, and the compaction group that owns the whole token range on this
    // shard or null when no single group of this shard owns it.
    virtual target find(table_id, dht::token first, dht::token last) = 0;

    virtual sstring table_name(table_id) = 0;
};

} // namespace replica::logstor
