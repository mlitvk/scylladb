/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/column_mapping_registry.hh"

#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

#include <fmt/format.h>
#include <seastar/util/log.hh>

#include "serializer_impl.hh"
#include "idl/mutation.dist.hh"
#include "idl/mutation.dist.impl.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

namespace {

column_mapping parse_column_mapping(bytes_view serialized) {
    auto in = ser::as_input_stream(serialized);
    auto mapping = ser::deserialize(in, std::type_identity<column_mapping>{});
    if (in.size() != 0) {
        throw std::runtime_error(fmt::format("{} bytes are left after the mapping", in.size()));
    }
    return mapping;
}

const schema_mapping& check_table(const schema_mapping& entry, table_id table) {
    if (entry.table != table) {
        throw std::runtime_error(fmt::format("Column mapping of schema version {} of table {}: the version is registered for table {}",
                entry.version, table, entry.table));
    }
    return entry;
}

} // anonymous namespace

const schema_mapping& column_mapping_registry::add_or_replace(map_type::iterator it, schema_mapping entry) {
    auto added = std::make_unique<schema_mapping>(std::move(entry));
    if (it == _mappings.end()) {
        return *_mappings.emplace(added->version, std::move(added)).first->second;
    }
    logstor_logger.warn("Column mapping of schema version {} of table {}: a copy that parses replaces the copy that does not parse",
            added->version, added->table);
    // The slot is added first, so that if push_back() throws, the registry is unchanged.
    _replaced.push_back(nullptr);
    _replaced.back() = std::exchange(it->second, std::move(added));
    return *it->second;
}

const schema_mapping& column_mapping_registry::add(const schema& s) {
    auto it = _mappings.find(s.version());
    if (it != _mappings.end()) {
        const auto& entry = check_table(*it->second, s.id());
        if (entry.mapping) {
            return entry;
        }
    }
    const auto& mapping = s.get_column_mapping();
    return add_or_replace(it, schema_mapping{
        .table = s.id(),
        .version = s.version(),
        .serialized = ser::serialize_to_buffer<bytes>(mapping, 0),
        .mapping = mapping,
    });
}

const schema_mapping& column_mapping_registry::add(table_id table, table_schema_version version, bytes_view serialized_mapping) {
    auto it = _mappings.find(version);
    if (it != _mappings.end()) {
        const auto& entry = check_table(*it->second, table);
        // Bytes equal to those of the entry would fail to parse again.
        if (entry.mapping || bytes_view(entry.serialized) == serialized_mapping) {
            return entry;
        }
    }
    std::optional<column_mapping> mapping;
    try {
        mapping = parse_column_mapping(serialized_mapping);
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        // Parsing throws more than std::runtime_error, for example std::out_of_range and
        // marshal_exception.
        logstor_logger.error("Column mapping of schema version {} of table {} does not parse. Records of the version can be rewritten, "
                "but not read under another schema version: {}",
                version, table, std::current_exception());
    }
    if (it != _mappings.end() && !mapping) {
        return *it->second;
    }
    return add_or_replace(it, schema_mapping{
        .table = table,
        .version = version,
        .serialized = bytes(serialized_mapping.begin(), serialized_mapping.end()),
        .mapping = std::move(mapping),
    });
}

} // namespace replica::logstor
