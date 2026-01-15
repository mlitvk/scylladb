/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <seastar/core/rwlock.hh>
#include <seastar/core/timer.hh>
#include "types.hh"
#include "utils/bptree.hh"

namespace replica::log_structured {

struct index_key_less {
    bool operator()(const index_key& a, const index_key& b) const noexcept {
        return a < b;
    }
};

class log_index {
    using index_tree = bplus::tree<index_key, index_entry, index_key_less, 16>;

    index_tree _index;

public:
    log_index() : _index(index_key_less{}) {}

    std::optional<index_entry> get(index_key key) const {
        auto it = _index.find(key);
        return it != _index.end() ? std::make_optional(*it) : std::nullopt;
    }

    std::optional<index_entry> exchange(index_key key, index_entry new_entry) {
        auto it = _index.find(key);
        std::optional<index_entry> old_entry;

        if (it != _index.end()) {
            old_entry = *it;
            *it = std::move(new_entry);
        } else {
            _index.emplace(std::move(key), std::move(new_entry));
        }

        return old_entry;
    }

    bool compare_exchange(index_key key, const index_entry& old_value, index_entry new_value) {
        auto it = _index.find(key);
        if (it == _index.end() || *it != old_value) {
            return false;
        }

        *it = std::move(new_value);
        return true;
    }
};

}
