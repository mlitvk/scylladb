/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <concepts>
#include <unordered_map>

#include "bytes.hh"
#include "schema/schema.hh"

namespace replica::logstor {

// The registry entry of one schema version. `serialized` is the column mapping as a schema table
// entry holds it, and `mapping` is the same column mapping, parsed.
struct schema_mapping {
    table_id table;
    table_schema_version schema_version;
    bytes serialized;
    column_mapping mapping;
};

// The column mappings of schema versions, by version. An entry does not change after it is added,
// and a reference to it stays valid until erase_if() erases it.
class column_mapping_registry {
    std::unordered_map<table_schema_version, schema_mapping> _mappings;

public:
    // Returns the entry of the schema's version, and adds it if there is none. Throws
    // std::runtime_error if the version is registered under another table.
    const schema_mapping& add(const schema&);

    // The same, from a serialized mapping, which is parsed only if the version is not registered.
    // Throws std::runtime_error if the mapping does not parse.
    const schema_mapping& add(table_id, table_schema_version, bytes_view serialized_mapping);

    const schema_mapping* find(table_schema_version schema_version) const noexcept {
        auto it = _mappings.find(schema_version);
        return it == _mappings.end() ? nullptr : &it->second;
    }

    size_t size() const noexcept { return _mappings.size(); }

    // Erases the entries that match, and returns how many it erased. The caller must make sure that
    // no reference to an erased entry is left.
    template <std::predicate<const schema_mapping&> Pred>
    size_t erase_if(Pred pred) {
        return std::erase_if(_mappings, [&pred] (const auto& entry) { return pred(entry.second); });
    }
};

} // namespace replica::logstor
