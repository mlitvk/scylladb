/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "dht/decorated_key.hh"
#include "dht/ring_position.hh"
#include "types.hh"
#include "utils/bptree.hh"
#include "utils/double-decker.hh"
#include "utils/phased_barrier.hh"

namespace replica::logstor {

class primary_index_entry {
    dht::decorated_key _key;
    index_entry _e;
    struct {
        bool _head : 1;
        bool _tail : 1;
        bool _train : 1;
    } _flags{};
public:
    primary_index_entry(dht::decorated_key key, index_entry e)
        : _key(std::move(key))
        , _e(std::move(e))
    { }

    primary_index_entry(primary_index_entry&&) noexcept = default;

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

class primary_index final {
public:
    using partitions_type = double_decker<int64_t, primary_index_entry,
                            dht::raw_token_less_comparator, dht::ring_position_comparator,
                            16, bplus::key_search::linear>;
private:
    partitions_type _partitions;
    schema_ptr _schema;
    size_t _key_count = 0;

public:
    explicit primary_index(schema_ptr schema)
        : _partitions(dht::raw_token_less_comparator{})
        , _schema(std::move(schema))
        {}

    void set_schema(schema_ptr s) {
        _schema = std::move(s);
    }

    std::optional<index_entry> get(const primary_index_key& key) const {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end()) {
            return it->_e;
        }
        return std::nullopt;
    }

    std::optional<index_entry> exchange(const primary_index_key& key, index_entry new_entry) {
        partitions_type::bound_hint hint;
        auto i = _partitions.lower_bound(key.dk, dht::ring_position_comparator(*_schema), hint);
        if (hint.match) {
            auto old_entry = i->_e;
            i->_e = std::move(new_entry);
            return old_entry;
        } else {
            _partitions.emplace_before(i, key.dk.token().raw(), hint, key.dk, std::move(new_entry));
            ++_key_count;
            return std::nullopt;
        }
    }

    bool update_record_location(const primary_index_key& key, log_location old_location, log_location new_location) {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end()) {
            if (it->_e.location == old_location) {
                it->_e.location = new_location;
                return true;
            }
        }
        return false;
    }

    std::pair<bool, std::optional<index_entry>> insert_if_newer(const primary_index_key& key, index_entry new_entry) {
        partitions_type::bound_hint hint;
        auto i = _partitions.lower_bound(key.dk, dht::ring_position_comparator(*_schema), hint);
        if (hint.match) {
            if (i->_e.generation < new_entry.generation) {
                auto old_entry = i->_e;
                i->_e = std::move(new_entry);
                return {true, std::make_optional(old_entry)};
            } else {
                return {false, std::make_optional(i->_e)};
            }
        } else {
            _partitions.emplace_before(i, key.dk.token().raw(), hint, key.dk, std::move(new_entry));
            ++_key_count;
            return {true, std::nullopt};
        }
    }

    bool erase(const primary_index_key& key, log_location loc) {
        auto it = _partitions.find(key.dk, dht::ring_position_comparator(*_schema));
        if (it != _partitions.end() && it->_e.location == loc) {
            it.erase(dht::raw_token_less_comparator{});
            --_key_count;
            return true;
        }
        return false;
    }

    auto begin() const noexcept { return _partitions.begin(); }
    auto end() const noexcept { return _partitions.end(); }

    bool empty() const noexcept { return _partitions.empty(); }

    size_t get_key_count() const noexcept { return _key_count; }

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
