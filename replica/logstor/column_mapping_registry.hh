/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <concepts>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "bytes.hh"
#include "schema/schema.hh"

namespace replica::logstor {

struct schema_mapping {
    table_id table;
    table_schema_version version;
    bytes serialized;
    // Empty if `serialized` does not parse.
    std::optional<column_mapping> mapping;
};

// The column mappings of schema versions, by version. A copy of a mapping that parses replaces an
// entry whose mapping does not parse. No other entry is replaced. A replaced entry is kept, because
// a write buffer that lists it has reserved room for its bytes. So a reference to an entry stays
// valid until erase_if() erases it.
class column_mapping_registry {
    using map_type = std::unordered_map<table_schema_version, std::unique_ptr<schema_mapping>>;
    map_type _mappings;
    std::vector<std::unique_ptr<schema_mapping>> _replaced;

    const schema_mapping& add_or_replace(map_type::iterator it, schema_mapping entry);

public:
    // Returns the entry of the schema's version, and adds it if there is none or if its mapping
    // does not parse. Throws std::runtime_error if the version is registered under another table.
    const schema_mapping& add(const schema&);

    // The same, from a serialized mapping. If the mapping does not parse, logs an error, and adds
    // the entry without the deserialized mapping if there is none.
    const schema_mapping& add(table_id, table_schema_version, bytes_view serialized_mapping);

    const schema_mapping* find(table_schema_version version) const noexcept {
        auto it = _mappings.find(version);
        return it == _mappings.end() ? nullptr : it->second.get();
    }

    size_t size() const noexcept { return _mappings.size(); }

    // Erases the entries that match, replaced ones included. Returns how many entries it erased,
    // not counting the replaced ones. The caller makes sure that no reference to an erased entry is
    // left.
    template <std::predicate<const schema_mapping&> Pred>
    size_t erase_if(Pred pred) {
        std::erase_if(_replaced, [&pred] (const auto& entry) { return pred(*entry); });
        return std::erase_if(_mappings, [&pred] (const auto& entry) { return pred(*entry.second); });
    }
};

} // namespace replica::logstor
