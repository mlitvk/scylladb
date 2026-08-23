/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <limits>

#include <seastar/core/byteorder.hh>

#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"
#include "dht/token.hh"
#include "keys/keys.hh"
#include "replica/logstor/types.hh"
#include "serializer.hh"
#include "utils/fragment_range.hh"
#include "utils/managed_bytes.hh"

namespace replica::logstor {

namespace ondisk {

static constexpr size_t block_alignment = 4096;
static constexpr size_t record_alignment = 8;
static constexpr uint8_t current_version = 1;
static constexpr uint32_t buffer_header_magic = 0x4c475342;

struct buffer_header {
    uint32_t magic;
    segment_kind kind;
    uint8_t version;
    uint16_t reserved;
    segment_sequence segment_seq;
    uint32_t data_size; // size of all records data following the header(s)
    uint32_t crc;

    uint32_t calculate_crc() const;
};

static constexpr size_t buffer_header_size =
    sizeof(uint32_t)
    + sizeof(std::underlying_type_t<segment_kind>)
    + sizeof(uint8_t)
    + sizeof(uint16_t)
    + sizeof(segment_sequence)
    + sizeof(uint32_t)
    + sizeof(uint32_t);
static_assert(buffer_header_size % record_alignment == 0, "Buffer header size must be aligned by record_alignment");

struct segment_header {
    table_id table;
    dht::token first_token;
    dht::token last_token;
};

static constexpr size_t segment_header_size =
    sizeof(table_id)
    + 2 * sizeof(int64_t);
static_assert(segment_header_size % record_alignment == 0, "Segment header size must be aligned by record_alignment");

struct record_header {
    uint32_t header_size; // size of the serialized log_record_header
    uint32_t data_size;   // size of the serialized record value
};

static constexpr size_t record_header_size = 2 * sizeof(uint32_t);

// Offsets of the fields of a serialized log_record_header. The fixed-width fields come
// first, so that a point read takes the record's timestamp - the base every timestamp in
// the record value is a delta from - at a fixed offset, and compares the record's key
// against the one it is looking for with a memcmp, without deserializing the header at all.
static constexpr size_t log_record_header_token_offset = 0;
static constexpr size_t log_record_header_timestamp_offset = log_record_header_token_offset + sizeof(int64_t);
static constexpr size_t log_record_header_table_offset = log_record_header_timestamp_offset + sizeof(int64_t);
static constexpr size_t log_record_header_key_size_offset = log_record_header_table_offset + 2 * sizeof(int64_t);
static constexpr size_t log_record_header_key_offset = log_record_header_key_size_offset + sizeof(uint16_t);
static constexpr size_t log_record_header_fixed_size = log_record_header_key_offset;

// A partition key is at most 64K, which is what its size field holds.
static constexpr size_t max_partition_key_size = std::numeric_limits<uint16_t>::max();

// The size a log_record_header takes serialized.
inline size_t log_record_header_size(const log_record_header& h) noexcept {
    return log_record_header_fixed_size + h.key.dk.key().representation().size();
}

// The fields of a serialized log_record_header a point read needs, taken at their offsets
// instead of by deserializing the header, which would allocate for a key the read already
// has. header must hold at least log_record_header_fixed_size bytes.
inline api::timestamp_type log_record_header_timestamp(bytes_view header) noexcept {
    return seastar::read_le<int64_t>(reinterpret_cast<const char*>(header.data()) + log_record_header_timestamp_offset);
}

inline bytes_view log_record_header_key(bytes_view header) noexcept {
    const auto size = seastar::read_le<uint16_t>(
            reinterpret_cast<const char*>(header.data()) + log_record_header_key_size_offset);
    return header.substr(log_record_header_key_offset, size);
}

bool validate_header(const buffer_header& bh);
bool validate_record_header(const record_header& rh);

} // namespace ondisk
} // namespace replica::logstor

namespace ser {

template <>
struct serializer<replica::logstor::ondisk::buffer_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::ondisk::buffer_header& h) {
        serializer<uint32_t>::write(out, h.magic);
        serializer<uint8_t>::write(out, static_cast<uint8_t>(h.kind));
        serializer<uint8_t>::write(out, h.version);
        serializer<uint16_t>::write(out, h.reserved);
        serializer<uint64_t>::write(out, h.segment_seq.value);
        serializer<uint32_t>::write(out, h.data_size);
        serializer<uint32_t>::write(out, h.crc);
    }

    template <typename Input>
    static replica::logstor::ondisk::buffer_header read(Input& in) {
        replica::logstor::ondisk::buffer_header h;
        h.magic = serializer<uint32_t>::read(in);
        h.kind = static_cast<replica::logstor::segment_kind>(serializer<uint8_t>::read(in));
        h.version = serializer<uint8_t>::read(in);
        h.reserved = serializer<uint16_t>::read(in);
        h.segment_seq = replica::logstor::segment_sequence{serializer<uint64_t>::read(in)};
        h.data_size = serializer<uint32_t>::read(in);
        h.crc = serializer<uint32_t>::read(in);
        return h;
    }

    template <typename Input>
    static void skip(Input& in) {
        serializer<uint32_t>::skip(in);
        serializer<uint8_t>::skip(in);
        serializer<uint8_t>::skip(in);
        serializer<uint16_t>::skip(in);
        serializer<uint64_t>::skip(in);
        serializer<uint32_t>::skip(in);
        serializer<uint32_t>::skip(in);
    }
};

template <>
struct serializer<replica::logstor::ondisk::segment_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::ondisk::segment_header& h) {
        serializer<table_id>::write(out, h.table);
        serializer<int64_t>::write(out, h.first_token.raw());
        serializer<int64_t>::write(out, h.last_token.raw());
    }

    template <typename Input>
    static replica::logstor::ondisk::segment_header read(Input& in) {
        replica::logstor::ondisk::segment_header h;
        h.table = serializer<table_id>::read(in);
        h.first_token = dht::token::from_int64(serializer<int64_t>::read(in));
        h.last_token = dht::token::from_int64(serializer<int64_t>::read(in));
        return h;
    }

    template <typename Input>
    static void skip(Input& in) {
        serializer<table_id>::skip(in);
        serializer<int64_t>::skip(in);
        serializer<int64_t>::skip(in);
    }
};

template <>
struct serializer<replica::logstor::ondisk::record_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::ondisk::record_header& h) {
        serializer<uint32_t>::write(out, h.header_size);
        serializer<uint32_t>::write(out, h.data_size);
    }

    template <typename Input>
    static replica::logstor::ondisk::record_header read(Input& in) {
        replica::logstor::ondisk::record_header h;
        h.header_size = serializer<uint32_t>::read(in);
        h.data_size = serializer<uint32_t>::read(in);
        return h;
    }

    template <typename Input>
    static void skip(Input& in) {
        serializer<uint32_t>::skip(in);
        serializer<uint32_t>::skip(in);
    }
};

// Written by hand rather than generated from an IDL definition, which would frame every
// field, serialize the token through a bytes and the key through a vector of its exploded
// components, and cost several allocations on both the read and the write path.
//
//     i64  token
//     i64  timestamp
//     u128 table_id
//     u16  key size
//          key: the partition_key representation, verbatim
template <>
struct serializer<replica::logstor::log_record_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::log_record_header& h) {
        const auto& key = h.key.dk.key().representation();
        if (key.size() > replica::logstor::ondisk::max_partition_key_size) [[unlikely]] {
            throw std::runtime_error(fmt::format("logstor partition key of {} bytes exceeds the maximum of {}",
                    key.size(), replica::logstor::ondisk::max_partition_key_size));
        }
        serializer<int64_t>::write(out, h.key.dk.token().raw());
        serializer<int64_t>::write(out, h.timestamp);
        serializer<table_id>::write(out, h.table);
        serializer<uint16_t>::write(out, static_cast<uint16_t>(key.size()));
        for (bytes_view fragment : fragment_range(managed_bytes_view(key))) {
            out.write(reinterpret_cast<const char*>(fragment.data()), fragment.size());
        }
    }

    template <typename Input>
    static replica::logstor::log_record_header read(Input& in) {
        auto token = dht::token::from_int64(serializer<int64_t>::read(in));
        const auto timestamp = serializer<int64_t>::read(in);
        const auto table = serializer<table_id>::read(in);

        const auto key_size = serializer<uint16_t>::read(in);
        managed_bytes key(managed_bytes::initialized_later(), key_size);
        for (auto view = managed_bytes_mutable_view(key); view.size_bytes(); view.remove_current()) {
            auto fragment = view.current_fragment();
            in.read(reinterpret_cast<char*>(fragment.data()), fragment.size());
        }

        return replica::logstor::log_record_header {
            .key = replica::logstor::primary_index_key{
                dht::decorated_key(std::move(token), partition_key::from_bytes(std::move(key)))},
            .timestamp = timestamp,
            .table = table,
        };
    }

    template <typename Input>
    static void skip(Input& in) {
        in.skip(replica::logstor::ondisk::log_record_header_key_size_offset);
        const auto key_size = serializer<uint16_t>::read(in);
        in.skip(key_size);
    }
};

} // namespace ser
