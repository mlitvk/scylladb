/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/column_mapping_registry.hh"

#include <new>
#include <stdexcept>
#include <utility>

#include <fmt/format.h>
#include <seastar/util/log.hh>

#include "serializer_impl.hh"
#include "idl/mutation.dist.hh"
#include "idl/mutation.dist.impl.hh"

namespace replica::logstor {

namespace {

column_mapping parse_column_mapping(table_id table, table_schema_version schema_version, bytes_view serialized) {
    try {
        auto in = ser::as_input_stream(serialized);
        auto mapping = ser::deserialize(in, std::type_identity<column_mapping>{});
        if (in.size() != 0) {
            throw std::runtime_error(fmt::format("{} bytes are left after the mapping", in.size()));
        }
        return mapping;
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        // Parsing throws more than std::runtime_error, for example std::out_of_range and
        // marshal_exception.
        throw std::runtime_error(fmt::format("Column mapping of schema version {} of table {} does not parse: {}",
                schema_version, table, std::current_exception()));
    }
}

const schema_mapping& check_table(const schema_mapping& entry, table_id table) {
    if (entry.table != table) {
        throw std::runtime_error(fmt::format("Column mapping of schema version {} of table {}: the version is registered for table {}",
                entry.schema_version, table, entry.table));
    }
    return entry;
}

} // anonymous namespace

const schema_mapping& column_mapping_registry::add(const schema& s) {
    if (const auto* entry = find(s.version())) {
        return check_table(*entry, s.id());
    }
    const auto& mapping = s.get_column_mapping();
    return _mappings.emplace(s.version(), schema_mapping{
        .table = s.id(),
        .schema_version = s.version(),
        .serialized = ser::serialize_to_buffer<bytes>(mapping, 0),
        .mapping = mapping,
    }).first->second;
}

const schema_mapping& column_mapping_registry::add(table_id table, table_schema_version schema_version, bytes_view serialized_mapping) {
    if (const auto* entry = find(schema_version)) {
        return check_table(*entry, table);
    }
    return _mappings.emplace(schema_version, schema_mapping{
        .table = table,
        .schema_version = schema_version,
        .serialized = bytes(serialized_mapping.begin(), serialized_mapping.end()),
        .mapping = parse_column_mapping(table, schema_version, serialized_mapping),
    }).first->second;
}

} // namespace replica::logstor
