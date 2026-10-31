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
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <seastar/core/future.hh>

#include "dht/i_partitioner_fwd.hh"
#include "replica/logstor/types.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace replica::logstor {

// One erase: every record of `table` whose token is in [first_token, last_token] and which sits
// in a chunk written at a segment sequence of at most `seq` is dead. The tokens are raw token
// values, both inclusive, which is how the index orders records too.
struct erase_watermark {
    table_id table;
    int64_t first_token;
    int64_t last_token;
    segment_sequence seq;

    bool operator==(const erase_watermark&) const noexcept = default;
};

// The erase watermarks of one shard, and the file that keeps them across restarts. This is the
// durable half of an erase - tablet cleanup, truncate or drop: freeing a segment leaves its records
// on disk, so recovery, which scans every segment, would bring erased records back. A watermark
// tells it which records an erase covered, and it filters them out before they reach the index.
//
// The file is rewritten whole on every erase, through a temporary file that replaces it. A missing
// file means no watermarks: that is what a fresh store looks like. A corrupt one is an error.
class erase_watermarks {
    // Only the watermarks that decide a step below. One whose range later erases cover again, at
    // sequences at least its own, changes no lookup and goes, so the set is bounded by the ranges
    // that still show through and not by the number of erases.
    std::vector<erase_watermark> _watermarks;
    // The highest sequence of any watermark this set held, in memory or in its file. gc() does not
    // lower it, so the recovered sequence counter stays above every erase even after the
    // watermarks are dropped.
    segment_sequence _seq_floor{0};

    // Per table, the highest watermark covering each token, as a step function over the raw token
    // space: a step applies from its token up to the next step's, and a step at sequence 0 covers
    // nothing, since sequences start at 1. Rebuilt whenever the watermarks change, which is rare;
    // recovery looks it up once per record.
    struct step {
        int64_t from;
        segment_sequence seq;
    };
    std::unordered_map<table_id, std::vector<step>> _steps;

    // Rebuilds the steps and drops the watermarks that decide none of them.
    void rebuild_steps();

public:
    // The watermark of an erase of `range` at `seq`, or nothing when the range is empty.
    static std::optional<erase_watermark> make(table_id, const dht::token_range&, segment_sequence);

    static std::filesystem::path file_path(const std::filesystem::path& base_dir);

    // Reads the shard's file. No file is an empty set; a file that does not parse throws.
    static future<erase_watermarks> load(const std::filesystem::path& base_dir);

    // Replaces the shard's file with the current set, atomically and durably.
    future<> save(const std::filesystem::path& base_dir) const;

    // The set with `w` added and the watermarks it covers dropped. The set it is called on does not
    // change, so that an erase can make the new set live and go back to the old one by swaps, which
    // cannot fail.
    erase_watermarks with(erase_watermark w) const;

    const std::vector<erase_watermark>& all() const noexcept {
        return _watermarks;
    }

    bool empty() const noexcept {
        return _watermarks.empty();
    }

    size_t size() const noexcept {
        return _watermarks.size();
    }

    // Whether a record of `table` at `token`, found in a chunk written at `seq`, is erased.
    bool erased(table_id table, dht::token token, segment_sequence seq) const noexcept;

    // The sequence floor, which the recovered sequence counter must stay above. Zero when the
    // shard never had a watermark.
    segment_sequence seq_floor() const noexcept {
        return _seq_floor;
    }

    // Drops the watermarks that can no longer match anything: those below the lowest sequence of
    // any chunk on the shard - none when the shard holds no chunk at all - and those of tables
    // that no longer exist. The sequence floor stays. Returns whether anything was dropped.
    bool gc(std::optional<segment_sequence> min_seq_present, const std::function<bool(table_id)>& table_exists);
};

static_assert(std::is_nothrow_swappable_v<erase_watermarks>);

} // namespace replica::logstor
