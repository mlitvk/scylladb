/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <cstdint>
#include <filesystem>
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
//
// The set keeps the erased ranges in the order of the erases, and the file holds them in the same
// order. The set and the file grow by one erased range with each erase, and covers() checks every
// erased range.
class erased_ranges {
public:
    // Returns the erased range of an erase of `range` at `seq`. The caller must pass a range that
    // holds a token, see raw_token_bounds::of().
    static erased_range make(table_id, const dht::token_range&, segment_sequence);

    static std::filesystem::path file_path(const std::filesystem::path& base_dir, unsigned shard);

    // Reads the file of `shard` in `base_dir`. It returns an empty set when there is no file.
    // Throws when the file does not parse.
    static future<erased_ranges> load(const std::filesystem::path& base_dir, unsigned shard);

    // Replaces the file of `shard` in `base_dir` with the current set, atomically and durably.
    future<> save(const std::filesystem::path& base_dir, unsigned shard) const;

    // Saves the set with `r` added to the file of `shard` in `base_dir`, and then adds `r` to the
    // set in memory, so the set in memory never holds an erased range that the file does not. When
    // the save fails, the set does not change.
    // The caller must not change the set until the returned future resolves.
    future<> insert_and_save(erased_range r, const std::filesystem::path& base_dir, unsigned shard);

    // Returns the erased ranges, in the order of the erases.
    std::vector<erased_range> all() const;

    bool empty() const noexcept {
        return _ranges.empty();
    }

    size_t size() const noexcept {
        return _ranges.size();
    }

    // Returns whether an erased range covers a record of `table` at `token` that is in a chunk
    // written at `seq`, which means that the record is erased.
    bool covers(table_id table, dht::token token, segment_sequence seq) const noexcept;

    // Returns the sequence floor, which is the highest sequence of any erased range that this set
    // held, in memory or in its file, or 0 when the shard never had an erased range.
    segment_sequence seq_floor() const noexcept {
        return _seq_floor;
    }

private:
    std::vector<erased_range> _ranges;
    segment_sequence _seq_floor{0};

    // Writes `ranges` and `seq_floor` to the file of `shard` in `base_dir`, atomically and durably.
    static future<> write_file(const std::filesystem::path& base_dir, unsigned shard, const std::vector<erased_range>& ranges,
            segment_sequence seq_floor);
};

} // namespace replica::logstor
