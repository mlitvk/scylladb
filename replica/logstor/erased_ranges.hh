/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

#include <seastar/core/future.hh>

#include "dht/i_partitioner_fwd.hh"
#include "replica/logstor/fixed_serializer.hh"
#include "replica/logstor/types.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace replica::logstor {

// Describes one erase. A record of `table` is dead when its token is in [`first_token`,
// `last_token`] and its chunk has a segment sequence of at most `seq`. The tokens are raw token
// values, and both bounds are inclusive, which is how the index orders records too.
//
// The erased range file holds each erased range in this layout, little-endian:
//
//   0                    16               24              32        40
//   +--------------------+----------------+---------------+---------+
//   |       table        |  first_token   |  last_token   |   seq   |
//   +--------------------+----------------+---------------+---------+
//
// fixed_serializer writes the members in declaration order, so the order of the members is the
// on-disk layout.
struct erased_range : ondisk::fixed_layout {
    table_id table;
    int64_t first_token;
    int64_t last_token;
    segment_sequence seq;

    bool operator==(const erased_range&) const noexcept = default;
};

// Holds the erased ranges of one shard, and reads and writes the file that keeps them across
// restarts. Freeing a segment leaves its records on disk, and recovery scans every segment, so
// without this file an erase does not survive a restart. Recovery leaves out every record that an
// erased range covers.
class erased_ranges {
public:
    // Returns the erased range of an erase of `range` at `seq`. The caller must pass a range that
    // holds a token, see raw_token_bounds::of().
    static erased_range make(table_id, const dht::token_range&, segment_sequence);

    static std::filesystem::path file_path(const std::filesystem::path& base_dir, unsigned shard);

    // Reads the file of `shard` in `base_dir`. It returns an empty set when there is no file, and
    // throws when the file does not parse.
    static future<erased_ranges> load(const std::filesystem::path& base_dir, unsigned shard);

    // Replaces the file of `shard` in `base_dir` with the current set, atomically and durably.
    future<> save(const std::filesystem::path& base_dir, unsigned shard) const;

    // Adds `r` to the set in memory. When it throws, the set does not change.
    erased_ranges& insert(erased_range r);

    // Saves the set with `r` added to the file of `shard` in `base_dir`, and then adds `r` to the
    // set in memory, so the set in memory never holds an erased range that the file does not. When
    // the save fails, the set does not change. The caller must not change the set until the
    // returned future resolves.
    future<> insert_and_save(erased_range r, const std::filesystem::path& base_dir, unsigned shard);

    // Returns the erased ranges, in the order of their table and then of their first token.
    std::vector<erased_range> all() const;

    bool empty() const noexcept {
        return _size == 0;
    }

    size_t size() const noexcept {
        return _size;
    }

    // Returns whether an erased range covers a record of `table` at `token` that is in a chunk
    // written at `seq`, which means that the record is erased.
    bool covers(table_id table, dht::token token, segment_sequence seq) const noexcept;

    // Returns the sequence floor, which is the highest sequence of any erased range that this set
    // held, in memory or in its file, or 0 when the shard never had an erased range.
    // drop_obsolete() does not lower it, so the recovered sequence counter stays above every erase
    // even after the erased ranges are dropped.
    segment_sequence seq_floor() const noexcept {
        return _seq_floor;
    }

    // Drops the obsolete erased ranges, and returns whether it dropped one. An erased range is
    // obsolete in these cases:
    // - When `min_seq_present` is empty, because the shard holds no chunk.
    // - When its sequence is below `min_seq_present`, the lowest sequence of a chunk on the shard.
    // - When `table_exists` returns false for its table.
    // The sequence floor does not change.
    bool drop_obsolete(std::optional<segment_sequence> min_seq_present, const std::function<bool(table_id)>& table_exists);

private:
    // An erased range without its table, which is the key of `_tables`.
    struct range_in_table {
        int64_t first_token;
        int64_t last_token;
        segment_sequence seq;
    };

    // A step of the step function of a table, see table_state::steps.
    struct step {
        int64_t from;
        segment_sequence seq;
    };

    // The erased ranges of one table, and the step function that gives, for each raw token, the
    // highest sequence of the erased ranges that cover the token. A step applies from its token up
    // to the token of the next step, and a step at sequence 0 covers nothing, because sequences
    // start at 1. Recovery looks the steps up once per record.
    struct table_state {
        // Sorted by the first token.
        std::vector<range_in_table> ranges;
        std::vector<step> steps;
    };

    // The table whose state a save writes in place of the state in `_tables`.
    struct replacement {
        table_id table;
        const table_state& state;
    };

    std::unordered_map<table_id, table_state> _tables;
    // The number of erased ranges of all tables.
    size_t _size = 0;
    segment_sequence _seq_floor{0};

    // Returns the state of a table that has the erased ranges `ranges`: sorts them and computes the
    // steps.
    static table_state build(std::vector<range_in_table> ranges);

    // Returns the state of the table of `r` with `r` added. The set does not change.
    table_state with_range(const erased_range& r) const;

    // Puts `state` in place of the state of `table`, and raises the sequence floor to `seq`. The
    // caller must have added an entry of `table` to `_tables`.
    void replace(table_id table, table_state&& state, segment_sequence seq) noexcept;

    // Writes the set, with `replaced` in place of the state of its table when it is given, to the
    // file of `shard` in `base_dir`, atomically and durably.
    future<> write_file(const std::filesystem::path& base_dir, unsigned shard, const replacement* replaced, segment_sequence seq_floor) const;
};

} // namespace replica::logstor
