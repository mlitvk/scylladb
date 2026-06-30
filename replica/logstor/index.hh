/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "dht/decorated_key.hh"
#include "dht/ring_position.hh"
#include <absl/container/flat_hash_map.h>
#include <absl/container/btree_set.h>
#include <algorithm>
#include <optional>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/shared_ptr.hh>
#include "mutation/mutation_partition.hh"
#include "types.hh"
#include "utils/bptree.hh"
#include "utils/double-decker.hh"
#include "utils/on_internal_error.hh"
#include "utils/phased_barrier.hh"
#include <utility>
#include "replica/logstor/cache.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

// One entry in the primary index B+tree.
//
// In addition to the on-disk location (index_entry), each entry may hold a
// pointer to a cached_mutation_entry that lives in the logstor_cache_tracker's
// LSA region.  When the cache evicts the entry under memory pressure it zeroes
// _cached_entry via the back-pointer stored inside cached_mutation_entry.
class primary_index_entry {
    dht::decorated_key _key;
    index_entry _e;
    // Non-owning slot pointing into the shared cache region.
    // Empty when no cached mutation exists for this key.
    mutable cached_entry_slot _cached_entry;
    struct {
        bool _head : 1;
        bool _tail : 1;
        bool _train : 1;
    } _flags{};
public:
    friend class cache_tracker;

    primary_index_entry(dht::decorated_key key, index_entry e)
        : _key(std::move(key))
        , _e(std::move(e))
    { }

    ~primary_index_entry() {
        if (_cached_entry) {
            on_internal_error(logstor_logger, "primary_index_entry destroyed while having a cache entry");
        }
    }

    primary_index_entry(primary_index_entry&& other) noexcept
        : _key(std::move(other._key))
        , _e(std::move(other._e))
        , _cached_entry(std::move(other._cached_entry))
        , _flags(other._flags)
    {}

    primary_index_entry& operator=(primary_index_entry&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        if (_cached_entry) {
            on_internal_error(logstor_logger, "primary_index_entry move-assignment overwrote a live cached entry");
        }
        _key = std::move(other._key);
        _e = std::move(other._e);
        _cached_entry = std::move(other._cached_entry);
        _flags = other._flags;
        return *this;
    }

    size_t memory_usage() const noexcept {
        return sizeof(primary_index_entry) + _key.external_memory_usage();
    }

    bool is_head() const noexcept { return _flags._head; }
    void set_head(bool v) noexcept { _flags._head = v; }
    bool is_tail() const noexcept { return _flags._tail; }
    void set_tail(bool v) noexcept { _flags._tail = v; }
    bool with_train() const noexcept { return _flags._train; }
    void set_train(bool v) noexcept { _flags._train = v; }

    const dht::decorated_key& key() const noexcept { return _key; }
    const index_entry& entry() const noexcept { return _e; }

    friend class primary_index;

    friend dht::ring_position_view ring_position_view_to_compare(const primary_index_entry& e) { return e._key; }
};

using pending_generation = uint64_t;

class primary_index final {
public:
    using partitions_type = double_decker<int64_t, primary_index_entry,
                            dht::raw_token_less_comparator, dht::ring_position_comparator,
                            16, bplus::key_search::linear>;
    struct record_accounting_ops {
        seastar::noncopyable_function<void(log_location) noexcept> add_record;
        seastar::noncopyable_function<void(log_location) noexcept> free_record;
    };
private:
    struct pending_key_hash {
        size_t operator()(const dht::decorated_key& k) const noexcept {
            return std::hash<dht::decorated_key>()(k);
        }
    };

    struct pending_key_equal {
        const schema_ptr* schema = nullptr;

        bool operator()(const dht::decorated_key& a, const dht::decorated_key& b) const {
            return a.equal(**schema, b);
        }
    };

    struct pending_key_less {
        const schema_ptr* schema = nullptr;
        using is_transparent = void;

        bool operator()(const dht::decorated_key& a, const dht::decorated_key& b) const {
            return dht::ring_position_comparator(**schema)(a, b) < 0;
        }

        bool operator()(const dht::decorated_key& a, dht::ring_position_view b) const {
            return dht::ring_position_comparator(**schema)(a, b) < 0;
        }

        bool operator()(dht::ring_position_view a, const dht::decorated_key& b) const {
            return dht::ring_position_comparator(**schema)(a, b) < 0;
        }
    };

    using pending_entries_by_key_type = absl::flat_hash_map<dht::decorated_key, pending_entry, pending_key_hash, pending_key_equal>;
    using pending_entries_by_order_type = absl::btree_set<dht::decorated_key, pending_key_less>;

    static constexpr size_t pending_entries_initial_reserve = 4096;

    static bool is_after_pending_range_end(const dht::partition_range& pr, dht::ring_position_comparator cmp, pending_entries_by_order_type::const_iterator it, pending_entries_by_order_type::const_iterator end) {
        if (it == end || !pr.end()) {
            return false;
        }
        auto c = cmp(*it, pr.end()->value());
        return pr.end()->is_inclusive() ? c > 0 : c >= 0;
    }

    partitions_type _partitions;
    schema_ptr _schema;
    pending_entries_by_key_type _pending_entries_by_key;
    pending_entries_by_order_type _pending_entries_by_order;
    pending_generation _next_pending_generation = 1;
    size_t _key_count = 0;
    size_t _memory_usage = 0;

    mutable utils::phased_barrier _reads_phaser{"logstor_primary_index"};

    // Non-owning pointer to the cache tracker; null when the cache is not set up.
    // Mutable so that logically-const methods (lookup_cache, populate_cache) can
    // call non-const methods on the tracker (touch, insert) for LRU accounting.
    mutable cache_tracker* _cache_tracker = nullptr;

    void on_entry_added(const primary_index_entry& e) noexcept {
        _memory_usage += e.memory_usage();
        ++_key_count;
    }

    void on_entry_removed(const primary_index_entry& e) noexcept {
        _memory_usage -= e.memory_usage();
        --_key_count;
    }

    auto make_entry_disposer(record_accounting_ops* accounting) noexcept {
        return [this, accounting] (primary_index_entry* e) noexcept {
            if (_cache_tracker) {
                _cache_tracker->evict(*e);
            }
            if (accounting) {
                accounting->free_record(e->_e.location);
            }
            on_entry_removed(*e);
        };
    }

    void erase_entry(partitions_type::iterator& it, record_accounting_ops* accounting) noexcept {
        it.erase_and_dispose(dht::raw_token_less_comparator{}, make_entry_disposer(accounting));
    }

    future<> erase_range_gently(partitions_type::iterator begin, const partitions_type::iterator end, record_accounting_ops* accounting) {
        static constexpr size_t chunk_size = 1024;
        auto dispose = make_entry_disposer(accounting);
        while (begin != end) {
            auto chunk_end = begin;
            for (size_t i = 0; i < chunk_size && chunk_end != end; ++i, ++chunk_end);
            begin = _partitions.erase_and_dispose(begin, chunk_end, dispose);
            co_await coroutine::maybe_yield();
        }
    }

public:
    explicit primary_index(schema_ptr schema)
        : _partitions(dht::raw_token_less_comparator{})
        , _schema(std::move(schema))
        , _pending_entries_by_key(0, pending_key_hash{}, pending_key_equal{&_schema})
        , _pending_entries_by_order(pending_key_less{&_schema}) {
        _pending_entries_by_key.reserve(pending_entries_initial_reserve);
    }

    void set_schema(schema_ptr s) {
        _schema = std::move(s);
    }

    void set_cache_tracker(cache_tracker* ct) noexcept {
        _cache_tracker = ct;
    }

    cache_tracker* cache_tracker() const noexcept {
        return _cache_tracker;
    }

    future<> drain_cache() {
        if (_cache_tracker) {
            for (auto& pie : _partitions) {
                _cache_tracker->evict(pie);
                co_await coroutine::maybe_yield();
            }
        }
    }

    utils::phased_barrier::operation start_read() const {
        return _reads_phaser.start();
    }

    future<> await_pending_reads() {
        return _reads_phaser.advance_and_await();
    }

    std::optional<index_entry> get(const primary_index_key& key) const {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end()) {
            return it->_e;
        }
        return std::nullopt;
    }

    const pending_entry* get_pending(const dht::decorated_key& key) const {
        auto it = _pending_entries_by_key.find(key);
        if (it != _pending_entries_by_key.end()) {
            return &it->second;
        }
        return nullptr;
    }

    bool is_record_alive(const primary_index_key& key, log_location location) {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end()) {
            return it->_e.location == location;
        } else {
            return false;
        }
    }

    bool update_record_location(const primary_index_key& key, log_location old_location, log_location new_location, record_accounting_ops* accounting) {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end()) {
            if (it->_e.location == old_location) {
                it->_e.location = new_location;
                // The cached mutation is still valid (same data, new location on
                // disk after compaction moved it) — do not evict the cache here.
                if (accounting) {
                    accounting->free_record(old_location);
                    accounting->add_record(new_location);
                }
                return true;
            }
        }
        return false;
    }

    using entry_cmp_fn = std::function<std::strong_ordering(const index_entry&, const index_entry&)>;

    static std::strong_ordering default_entry_cmp(const index_entry& a, const index_entry& b) noexcept {
        return a.timestamp <=> b.timestamp;
    }

    std::optional<pending_generation> insert_pending(const primary_index_key& key, api::timestamp_type ts, shared_log_record_writer writer) {
        auto durable_it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        auto pending_it = _pending_entries_by_key.find(key.dk);

        std::optional<api::timestamp_type> max_ts;
        if (durable_it != _partitions.end()) {
            max_ts = durable_it->_e.timestamp;
        }
        if (pending_it != _pending_entries_by_key.end()) {
            max_ts = max_ts ? std::max(*max_ts, pending_it->second.timestamp) : pending_it->second.timestamp;
        }

        if (max_ts && *max_ts > ts) {
            return std::nullopt;
        }

        if (durable_it != _partitions.end() && _cache_tracker) {
            _cache_tracker->evict(*durable_it);
        }

        auto generation = _next_pending_generation++;

        if (pending_it != _pending_entries_by_key.end()) {
            pending_it->second = pending_entry{
                .generation = generation,
                .timestamp = ts,
                .writer = std::move(writer),
            };
        } else {
            _pending_entries_by_key.emplace(key.dk, pending_entry{
                .generation = generation,
                .timestamp = ts,
                .writer = std::move(writer),
            });
            _pending_entries_by_order.emplace(key.dk);
        }

        return generation;
    }

    struct complete_pending_write_result {
        bool accepted = false;
        bool pending_cleared = false;
        std::optional<log_location> old_durable_location;
    };

    complete_pending_write_result complete_pending_write(const primary_index_key& key, pending_generation generation, api::timestamp_type ts, log_location new_location, record_accounting_ops* accounting) {
        complete_pending_write_result result;

        partitions_type::bound_hint hint;
        auto it = _partitions.lower_bound(key.dk, dht::ring_position_comparator(*_schema), hint);
        if (hint.match) {
            if (ts >= it->_e.timestamp) {
                result.accepted = true;
                result.old_durable_location = it->_e.location;
                it->_e.location = new_location;
                it->_e.timestamp = ts;
                if (accounting) {
                    accounting->free_record(*result.old_durable_location);
                    accounting->add_record(new_location);
                }
            }
        } else {
            index_entry new_entry{
                .location = new_location,
                .timestamp = ts,
            };
            auto inserted = _partitions.emplace_before(it, key.dk.token().raw(), hint, key.dk, std::move(new_entry));
            on_entry_added(*inserted);
            result.accepted = true;
            if (accounting) {
                accounting->add_record(new_location);
            }
        }

        auto pending_it = _pending_entries_by_key.find(key.dk);
        if (pending_it != _pending_entries_by_key.end() && pending_it->second.generation == generation) {
            _pending_entries_by_key.erase(pending_it);
            _pending_entries_by_order.erase(key.dk);
            result.pending_cleared = true;
        }

        return result;
    }

    bool erase_pending_if_current(const primary_index_key& key, pending_generation generation) {
        auto it = _pending_entries_by_key.find(key.dk);
        if (it != _pending_entries_by_key.end() && it->second.generation == generation) {
            _pending_entries_by_key.erase(it);
            _pending_entries_by_order.erase(key.dk);
            return true;
        }
        return false;
    }

    pending_entries_by_order_type::const_iterator pending_end() const {
        return _pending_entries_by_order.end();
    }

    pending_entries_by_order_type::const_iterator lower_bound_pending(dht::ring_position_view pos) const {
        return _pending_entries_by_order.lower_bound(pos);
    }

    pending_entries_by_order_type::const_iterator upper_bound_pending(const dht::decorated_key& key) const {
        return _pending_entries_by_order.upper_bound(key);
    }

    pending_entries_by_order_type::const_iterator first_pending_in_range(const dht::partition_range& pr) const {
        auto it = pr.start()
            ? _pending_entries_by_order.lower_bound(pr.start()->value())
            : _pending_entries_by_order.begin();
        auto cmp = dht::ring_position_comparator(*_schema);
        if (pr.start() && !pr.start()->is_inclusive() && it != _pending_entries_by_order.end() && cmp(*it, pr.start()->value()) == 0) {
            ++it;
        }
        if (is_after_pending_range_end(pr, cmp, it, _pending_entries_by_order.end())) {
            return _pending_entries_by_order.end();
        }
        return it;
    }

    pending_entries_by_order_type::const_iterator next_pending_in_range(const dht::partition_range& pr, const dht::decorated_key& last_key) const {
        auto it = _pending_entries_by_order.upper_bound(last_key);
        auto cmp = dht::ring_position_comparator(*_schema);
        if (is_after_pending_range_end(pr, cmp, it, _pending_entries_by_order.end())) {
            return _pending_entries_by_order.end();
        }
        return it;
    }

    std::pair<bool, std::optional<index_entry>> insert(const primary_index_key& key, index_entry new_entry, record_accounting_ops* accounting , entry_cmp_fn cmp = default_entry_cmp) {
        partitions_type::bound_hint hint;
        auto i = _partitions.lower_bound(key.dk, dht::ring_position_comparator(*_schema), hint);
        if (hint.match) {
            if (cmp(i->_e, new_entry) <= 0) {
                // Overwriting with newer data: evict stale cached mutation.
                if (_cache_tracker) {
                    _cache_tracker->evict(*i);
                }
                auto old_entry = i->_e;
                i->_e = std::move(new_entry);
                if (accounting) {
                    accounting->free_record(old_entry.location);
                    accounting->add_record(i->_e.location);
                }
                return {true, std::make_optional(old_entry)};
            } else {
                return {false, std::make_optional(i->_e)};
            }
        } else {
            auto it = _partitions.emplace_before(i, key.dk.token().raw(), hint, key.dk, std::move(new_entry));
            on_entry_added(*it);
            if (accounting) {
                accounting->add_record(it->_e.location);
            }
            return {true, std::nullopt};
        }
    }

    bool erase(const primary_index_key& key, log_location loc, record_accounting_ops* accounting) {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end() && it->_e.location == loc) {
            erase_entry(it, accounting);
            return true;
        }
        return false;
    }

    future<> erase(const dht::partition_range& pr, record_accounting_ops* accounting) {
        dht::ring_position_comparator cmp(*_schema);
        auto begin = _partitions.lower_bound(dht::ring_position_view::for_range_start(pr), cmp);
        auto end = _partitions.lower_bound(dht::ring_position_view::for_range_end(pr), cmp);
        co_await erase_range_gently(begin, end, accounting);

        auto pending_begin = first_pending_in_range(pr);
        if (pending_begin != _pending_entries_by_order.end()) {
            auto pending_end = pr.end()
                ? _pending_entries_by_order.lower_bound(dht::ring_position_view::for_range_end(pr))
                : _pending_entries_by_order.end();
            for (auto it = pending_begin; it != pending_end; ++it) {
                _pending_entries_by_key.erase(*it);
            }
            _pending_entries_by_order.erase(pending_begin, pending_end);
        }
    }

    future<> clear(record_accounting_ops* accounting) {
        co_await erase_range_gently(_partitions.begin(), _partitions.end(), accounting);
        _pending_entries_by_key.clear();
        _pending_entries_by_order.clear();

        if (_key_count != 0 || _memory_usage != 0) {
            on_internal_error(logstor_logger, format("primary_index::clear ended with key_count {} and memory_usage {}", _key_count, _memory_usage));
        }
    }

    auto begin() const noexcept { return _partitions.begin(); }
    auto end() const noexcept { return _partitions.end(); }

    bool empty() const noexcept { return _partitions.empty(); }
    size_t get_key_count() const noexcept { return _key_count; }
    size_t get_memory_usage() const noexcept { return _memory_usage; }

    partitions_type::const_iterator find(const dht::decorated_key& key) const {
        return _partitions.find(key, dht::ring_position_comparator(*_schema));
    }

    // First entry with key >= pos (for positioning at range start)
    partitions_type::const_iterator lower_bound(const dht::ring_position_view& pos) const {
        return _partitions.lower_bound(pos, dht::ring_position_comparator(*_schema));
    }

    // First entry with key strictly > key (for advancing past a key after a yield)
    partitions_type::const_iterator upper_bound(const dht::decorated_key& key) const {
        return _partitions.upper_bound(key, dht::ring_position_comparator(*_schema));
    }

};

}
