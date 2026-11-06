/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <concepts>

#include <seastar/core/simple-stream.hh>

#include "replica/logstor/column_mapping_registry.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/types.hh"

namespace replica {

namespace logstor {

// Writer for log records that handles serialization and size computation.
//
// The writer also holds the registry entry of the record's schema version, for the schema table of
// the chunk that the buffer builds. The registry never erases an entry that a writer holds.
class log_record_writer {
    using ostream = seastar::simple_memory_output_stream;

    log_record _record;
    const schema_mapping* _mapping;

public:
    log_record_writer(log_record record, const schema_mapping& mapping)
        : _record(std::move(record))
        , _mapping(&mapping)
    {}

    const log_record& record() const { return _record; }
    const record_header& header() const { return _record.header; }
    const schema_mapping& mapping() const noexcept { return *_mapping; }

    size_t header_size() const { return ondisk::record_header_size(_record.header); }
    size_t value_size() const { return _record.value.size(); }
    // The record without its frame header: the record header and the value.
    size_t record_size() const { return header_size() + value_size(); }

    // Write the record's frame - frame header, record header, value - to an output stream.
    void write_frame(ostream& out) const {
        ondisk::write_record_frame(out, _record.header, _record.value);
    }
};

// Writer for log records that stores pre-serialized bytes.
// Used in compaction and separator rewriting to avoid deserialization.
class log_record_bytes_writer {
    using ostream = seastar::simple_memory_output_stream;

    record_header _header;
    bytes_view _header_bytes;
    bytes_view _value_bytes;
    const schema_mapping* _mapping;

public:
    log_record_bytes_writer(record_header header, log_record_bytes_view record_view, const schema_mapping& mapping)
        : _header(std::move(header))
        , _header_bytes(record_view.header)
        , _value_bytes(record_view.value)
        , _mapping(&mapping)
    {}

    const record_header& header() const { return _header; }
    const schema_mapping& mapping() const noexcept { return *_mapping; }

    size_t header_size() const { return _header_bytes.size(); }
    size_t value_size() const { return _value_bytes.size(); }
    size_t record_size() const { return header_size() + value_size(); }

    void write_frame(ostream& out) const {
        ondisk::write_record_frame(out, log_record_bytes_view{_header_bytes, _value_bytes});
    }
};

template <typename T>
concept log_record_writer_concept = requires(const T& w, seastar::simple_memory_output_stream& out) {
    { w.header() } -> std::convertible_to<const record_header&>;
    { w.mapping() } -> std::convertible_to<const schema_mapping&>;
    { w.header_size() } -> std::convertible_to<size_t>;
    { w.value_size() } -> std::convertible_to<size_t>;
    { w.record_size() } -> std::convertible_to<size_t>;
    { w.write_frame(out) };
};

}
}
