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

// The interface that recovery and the adoption of a streamed segment use to find the index of each
// logstor table and the compaction groups that own its tokens on this shard. The database
// implements it for a node, and a test implements it over its own indexes and groups. The lookups
// do not yield.
class recovery_catalog {
public:
    virtual ~recovery_catalog() = default;

    // The index of a table and one of its compaction groups, found with one lookup of the table.
    struct target {
        primary_index* index = nullptr;
        logstor_group* group = nullptr;
    };

    // Returns the index of the table, or nullptr when the table does not exist or does not use
    // logstor.
    virtual primary_index* index(table_id) = 0;

    // Returns the index of the table and the compaction group of this shard that owns the token.
    // The group is nullptr when no group of this shard owns the token.
    virtual target find(table_id, dht::token) = 0;

    // Returns the index of the table and the compaction group of this shard that owns the whole
    // token range. The group is nullptr when no single group of this shard owns the whole range.
    virtual target find(table_id, dht::token first, dht::token last) = 0;

    // Returns the name of the table for log messages.
    virtual sstring table_name(table_id) = 0;
};

} // namespace replica::logstor
