/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "db/cache_tracker.hh"
#include "mutation/mutation.hh"
#include "utils/entangled.hh"
#include "utils/lru.hh"
#include "utils/logalloc.hh"
#include <optional>
#include <utility>

namespace replica::logstor {

class cache_tracker;
class primary_index;
class primary_index_entry;
class cached_entry_slot;

// A logstor table has no clustering columns, so a partition holds at most one row - the one with
// the empty clustering key - plus a partition tombstone. The rows a partition accounts for in the
// shared cache tracker are therefore derived from it rather than stored alongside it.
inline size_t partition_row_count(const mutation_partition& p) noexcept {
    return !p.clustered_rows().empty();
}

// An evictable node holding the deserialized mutation for one primary_index_entry.
//
// Lives inside the shared cache tracker's LSA region. When the shared LSA
// reclaimer needs memory it calls on_evicted(), which:
//   1. Nulls out the back-pointer in the owning primary_index_entry.
//   2. Destroys and frees this object via the LSA allocator.
//
// The back-pointer (_slot_link) is an entangled paired with the
// primary_index_entry::_cached_entry._entry_link, which lives in the
// standard-heap B+tree and is therefore stable across LSA compaction.
class cached_mutation_entry final : public evictable {
    schema_ptr _schema;
    mutation_partition _partition;
    entangled _slot_link; // Paired with the primary_index_entry's cached_entry_slot::_entry_link

public:
    // Note: constructed inside the LSA region.
    cached_mutation_entry(schema_ptr schema, const mutation_partition& partition, cached_entry_slot& slot);

    // LSA compaction moves cached entries between addresses, so the owning
    // primary_index_entry::_cached_entry pointer must be rebound to the new object.
    cached_mutation_entry(cached_mutation_entry&& other) noexcept
            : evictable(std::move(other))
            , _schema(std::move(other._schema))
            , _partition(std::move(other._partition))
            , _slot_link(std::move(other._slot_link))
    {
    }

    // Not copyable.
    cached_mutation_entry(const cached_mutation_entry&) = delete;
    cached_mutation_entry& operator=(const cached_mutation_entry&) = delete;
    cached_mutation_entry& operator=(cached_mutation_entry&&) = delete;

    const schema_ptr& schema() const noexcept {
        return _schema;
    }

    const mutation_partition& partition() const noexcept {
        return _partition;
    }

    mutation_partition& partition() noexcept {
        return _partition;
    }

    // Derived, not stored: upgrade() rebuilds the partition but can neither add nor remove its
    // row, because the converting applier creates the row before applying any cell to it.
    size_t row_count() const noexcept {
        return partition_row_count(_partition);
    }

    void upgrade(schema_ptr schema) {
        if (_schema != schema) {
            _partition.upgrade(*_schema, *schema);
            _schema = std::move(schema);
        }
    }

    // Called by lru under memory pressure.
    void on_evicted() noexcept override;

    friend class cached_entry_slot;
};

class cached_entry_slot {
    mutable entangled _entry_link;

public:
    cached_entry_slot() = default;

    cached_entry_slot(cached_entry_slot&&) noexcept = default;
    cached_entry_slot& operator=(cached_entry_slot&&) noexcept = default;
    cached_entry_slot(const cached_entry_slot&) = delete;
    cached_entry_slot& operator=(const cached_entry_slot&) = delete;

    cached_mutation_entry* get() const noexcept {
        return _entry_link.get(&cached_mutation_entry::_slot_link);
    }

    explicit operator bool() const noexcept { return bool(_entry_link); }
    cached_mutation_entry& operator*() const noexcept { return *get(); }
    cached_mutation_entry* operator->() const noexcept { return get(); }

    friend class cached_mutation_entry;
};

// Uses the shared row-cache LSA region and LRU list for the logstor cache.
class cache_tracker {
public:
    friend class cached_mutation_entry;

    // Counts a read against the shared tracker for as long as it is in scope, the way the row
    // cache's read_context does: started when the read begins and done when it ends, whether it
    // returns or throws. A null tracker accounts nothing, which is the bypass_cache case and the
    // case of a table whose cache is disabled.
    class read_accounter {
        cache_tracker* _tracker;
        bool _missed = false;

    public:
        explicit read_accounter(cache_tracker* tracker) noexcept
                : _tracker(tracker) {
            if (_tracker) {
                _tracker->_shared_tracker.on_read_started();
            }
        }

        read_accounter(const read_accounter&) = delete;
        read_accounter& operator=(const read_accounter&) = delete;

        // Moved from the part of a read that the cache could answer into the part that goes to the
        // disk, which is where such a read ends.
        read_accounter(read_accounter&& o) noexcept
                : _tracker(std::exchange(o._tracker, nullptr))
                , _missed(o._missed)
        { }

        ~read_accounter() {
            if (_tracker) {
                _tracker->_shared_tracker.on_read_done(_missed);
            }
        }

        // The read did not find its partition in the cache and went to the disk for it.
        void on_miss() noexcept { _missed = true; }
    };

private:
    ::cache_tracker& _shared_tracker;
    // Used by the read path to lock the LSA region without changing the current allocator
    logalloc::allocating_section _read_section;
    // Used by cache admission to reserve/retry LSA allocations under pressure.
    logalloc::allocating_section _populate_section;

public:
    explicit cache_tracker(::cache_tracker& shared_tracker);

    void evict(const primary_index_entry&);

    // The schema is taken by reference rather than as a schema_ptr: a hit needs one reference of
    // its own for the mutation it returns, and a miss needs none at all.
    std::optional<mutation> lookup(const primary_index_entry&, const schema& target_schema);

    void populate(const primary_index_entry&, const mutation&);

    // A read the index answered by itself: the index is the authority on which keys exist, so a key
    // it does not hold is a negative answer served from memory, with no disk read.
    void on_negative_hit() noexcept {
        _shared_tracker.on_partition_hit();
    }

    // The rows a read had to fetch from the disk because the cache did not hold them.
    void on_rows_missed(const mutation_partition& p) noexcept {
        _shared_tracker.on_rows_missed(partition_row_count(p));
    }

    // A read which fetched a record and then found it stale, so it left the cache alone.
    void on_mispopulate() noexcept {
        _shared_tracker.on_mispopulate();
    }

    logalloc::region& region() noexcept {
        return _shared_tracker.region();
    }

    const logalloc::region& region() const noexcept {
        return _shared_tracker.region();
    }

    allocation_strategy& allocator() noexcept {
        return _shared_tracker.allocator();
    }

    lru& get_lru() noexcept {
        return _shared_tracker.get_lru();
    }
};

} // namespace replica::logstor
