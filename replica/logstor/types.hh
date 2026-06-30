/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <fmt/format.h>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/shared_ptr.hh>
#include "dht/decorated_key.hh"
#include "mutation/timestamp.hh"
#include "replica/logstor/mutation_format.hh"

namespace replica::logstor {

struct log_segment_id {
    uint32_t value;

    bool operator==(const log_segment_id& other) const noexcept = default;
    auto operator<=>(const log_segment_id& other) const noexcept = default;
};

struct log_location {
    log_segment_id segment;
    uint32_t offset;
    uint32_t size;

    bool operator==(const log_location& other) const noexcept = default;
};

struct primary_index_key {
    dht::decorated_key dk;
};

struct index_entry {
    log_location location;
    api::timestamp_type timestamp;

    bool operator==(const index_entry& other) const noexcept = default;
};

struct log_record_header {
    primary_index_key key;
    api::timestamp_type timestamp;
    table_id table;
};

struct log_record {
    log_record_header header;
    log_record_data data;
};

struct log_record_bytes_view {
    bytes_view header;
    bytes_view data;
};

// Writer for log records that handles serialization and size computation.
class log_record_writer {

    using ostream = seastar::simple_memory_output_stream;

    log_record _record;
    mutable std::optional<size_t> _serialized_header_size;
    mutable std::optional<size_t> _serialized_data_size;

    void compute_sizes() const;

public:
    explicit log_record_writer(log_record record)
        : _record(std::move(record))
    {}

    size_t serialized_header_size() const {
        if (!_serialized_header_size) {
            compute_sizes();
        }
        return *_serialized_header_size;
    }

    size_t serialized_data_size() const {
        if (!_serialized_data_size) {
            compute_sizes();
        }
        return *_serialized_data_size;
    }

    size_t serialized_size() const {
        return serialized_header_size() + serialized_data_size();
    }

    size_t header_size() const { return serialized_header_size(); }
    size_t data_size() const { return serialized_data_size(); }
    size_t size() const { return serialized_size(); }

    void write(ostream& out) const;

    const log_record& record() const {
        return _record;
    }
};

using shared_log_record_writer = lw_shared_ptr<log_record_writer>;

struct pending_entry {
    uint64_t generation;
    api::timestamp_type timestamp;
    shared_log_record_writer writer;
};

struct segment_sequence {
    uint64_t value;

    bool operator==(const segment_sequence& other) const noexcept = default;
    auto operator<=>(const segment_sequence& other) const noexcept = default;

    segment_sequence& operator++() noexcept {
        ++value;
        return *this;
    }

    segment_sequence operator++(int) noexcept {
        segment_sequence tmp = *this;
        ++value;
        return tmp;
    }

    segment_sequence operator+(uint64_t increment) const noexcept {
        return segment_sequence{value + increment};
    }
};

enum class segment_kind : uint8_t {
    mixed = 0,
    full = 1,
};

}

// Format specialization declarations and implementations
template <>
struct fmt::formatter<replica::logstor::log_segment_id> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::log_segment_id& id, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "segment({})", id.value);
    }
};

template <>
struct fmt::formatter<replica::logstor::log_location> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::log_location& loc, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{segment:{}, offset:{}, size:{}}}",
                             loc.segment, loc.offset, loc.size);
    }
};

template <>
struct fmt::formatter<replica::logstor::primary_index_key> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::primary_index_key& key, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", key.dk);
    }
};

template <>
struct fmt::formatter<replica::logstor::segment_sequence> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const replica::logstor::segment_sequence& seq, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "sseq({})", seq.value);
    }
};
