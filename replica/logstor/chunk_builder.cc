/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/chunk_builder.hh"

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/simple-stream.hh>

#include "dht/token.hh"
#include "replica/logstor/logstor.hh"

namespace replica::logstor {

size_t chunk_schema_table::growth(const schema_mapping& mapping) const noexcept {
    // Matched by version and not by entry, because a chunk lists a version once, whichever registry
    // the entry of the record comes from. The last version added is the likely one, so it is
    // checked first.
    for (auto it = _entries.rbegin(); it != _entries.rend(); ++it) {
        if ((*it)->schema_version == mapping.schema_version) {
            return 0;
        }
    }
    return ondisk::schema_table_entry_size(mapping.serialized.size());
}

void chunk_schema_table::add(const schema_mapping& mapping, size_t growth) {
    if (growth > 0) {
        _entries.push_back(&mapping);
        _size += growth;
    }
}

void chunk_schema_table::clear() noexcept {
    _entries.clear();
    _size = ondisk::schema_table_header_size;
}

void chunk_schema_table::write(seastar::simple_memory_output_stream& out) const {
    ser::serializer<uint32_t>::write(out, static_cast<uint32_t>(_entries.size()));
    for (const auto* mapping : _entries) {
        ondisk::write_schema_table_entry(out, ondisk::schema_table_entry{
            .table = mapping->table,
            .schema_version = mapping->schema_version,
            .mapping = bytes_view(mapping->serialized),
        });
    }
}

chunk_builder::chunk_builder(size_t buffer_size, segment_kind kind)
        : _buffer_size(buffer_size)
        , _buffer(seastar::allocate_aligned_buffer<char>(buffer_size, 4096))
        , _segment_kind(kind)
{
    reset();
}

void chunk_builder::reset() {
    _stream = seastar::simple_memory_output_stream(_buffer.get(), _buffer_size);
    _stream.skip(chunk_headers_size());
    _record_bytes = 0;
    _record_count = 0;
    _min_token = std::nullopt;
    _max_token = std::nullopt;
    _schema_table.clear();
    _finished_body = std::nullopt;
    _sealed = false;
}

bool chunk_builder::can_fit(size_t record_size, size_t schema_table_growth) const noexcept {
    // The padded record frame, and the padded schema table that follows the last record.
    const auto padded_frame_size = align_up(ondisk::record_frame_header_size + record_size, ondisk::record_alignment);
    const auto table_size = align_up(_schema_table.size() + schema_table_growth, ondisk::record_alignment);
    return padded_frame_size + table_size <= _stream.size();
}

bool chunk_builder::has_data() const noexcept {
    return serialized_size() > chunk_headers_size();
}

template <log_record_writer_concept Writer>
frame_extent chunk_builder::append(const Writer& writer) {
    const auto& header = writer.header();
    const size_t header_size = writer.header_size();
    const size_t value_size = writer.value_size();

    if (header_size < ondisk::record_header_fixed_size) {
        on_internal_error(logstor_logger, fmt::format("Record header size {} is below its fixed size {}", header_size, ondisk::record_header_fixed_size));
    }
    if (const size_t key_size = header_size - ondisk::record_header_fixed_size; key_size > ondisk::max_key_size) {
        on_internal_error(logstor_logger, fmt::format("Partition key size {} exceeds the maximum {}", key_size, ondisk::max_key_size));
    }

    if (_finished_body) {
        on_internal_error(logstor_logger, "Cannot append a record to a finished write buffer");
    }

    const auto& mapping = writer.mapping();
    if (mapping.schema_version != header.schema_version || mapping.table != header.table) {
        on_internal_error(logstor_logger, fmt::format("Record of table {} schema version {} appended with the column mapping of table {} schema version {}",
                header.table, header.schema_version, mapping.table, mapping.schema_version));
    }

    const auto record_size = header_size + value_size;
    const auto schema_table_growth = _schema_table.growth(mapping);
    if (!can_fit(record_size, schema_table_growth)) {
        throw std::runtime_error(fmt::format("Record of size {} does not fit the {} bytes left in the buffer, of which the schema table takes {} ({} for schema version {})",
                record_size, _stream.size(), _schema_table.size() + schema_table_growth, schema_table_growth, header.schema_version));
    }

    // The version goes into the table before the frame is written. If add() throws, the buffer is
    // unchanged, and no frame is left whose version the table does not list.
    _schema_table.add(mapping, schema_table_growth);

    const size_t frame_offset = serialized_size();
    const size_t frame_size = ondisk::record_frame_size(header_size, value_size);

    auto frame_out = _stream.write_substream(frame_size);
    writer.write_frame(frame_out);
    if (frame_out.size() != 0) {
        on_internal_error(logstor_logger, fmt::format("Record frame of {} bytes left {} unwritten", frame_size, frame_out.size()));
    }

    pad_to_alignment(ondisk::record_alignment);

    _record_bytes += frame_size;
    _record_count++;
    _min_token = _min_token ? std::min(*_min_token, header.key.token()) : header.key.token();
    _max_token = _max_token ? std::max(*_max_token, header.key.token()) : header.key.token();

    return frame_extent {
        .offset = static_cast<uint32_t>(frame_offset),
        .size = static_cast<uint32_t>(frame_size),
    };
}

void chunk_builder::pad_to_alignment(size_t alignment) {
    auto current_pos = serialized_size();
    auto next_pos = align_up(current_pos, alignment);
    auto padding = next_pos - current_pos;
    if (padding > 0) {
        _stream.fill('\0', padding);
    }
}

size_t chunk_builder::finish() {
    if (_finished_body) {
        return serialized_size();
    }
    const size_t records_size = serialized_size() - chunk_headers_size();
    // The room for the table was reserved by can_fit() as the records were appended.
    auto table_out = _stream.write_substream(_schema_table.size());
    _schema_table.write(table_out);
    if (table_out.size() != 0) {
        on_internal_error(logstor_logger, fmt::format("Schema table of {} bytes left {} unwritten", _schema_table.size(), table_out.size()));
    }
    const auto body_crc = ondisk::calculate_body_crc(std::span(_buffer.get() + chunk_headers_size(),
            records_size + _schema_table.size()));
    pad_to_alignment(ondisk::block_alignment);
    _finished_body = finished_body{
        .records_size = static_cast<uint32_t>(records_size),
        .schema_table_size = static_cast<uint32_t>(_schema_table.size()),
        .body_crc = body_crc,
    };
    return serialized_size();
}

bytes_view chunk_builder::seal(segment_sequence segment_seq, std::optional<table_id> table) {
    if (_sealed) {
        throw std::runtime_error("Cannot seal write buffer more than once");
    }
    finish();

    std::optional<ondisk::segment_header> seg_hdr;
    if (with_segment_header()) {
        seg_hdr = ondisk::segment_header {
            .table = table.value(),
            .first_token = _min_token.value_or(dht::minimum_token()),
            .last_token = _max_token.value_or(dht::minimum_token()),
        };
    }
    ondisk::chunk_header chunk_hdr {
        .magic = ondisk::chunk_header_magic,
        .version = ondisk::current_version,
        .kind = _segment_kind,
        .flags = 0,
        .segment_seq = segment_seq,
        .records_size = _finished_body->records_size,
        .body_crc = _finished_body->body_crc,
        .schema_table_size = _finished_body->schema_table_size,
        .reserved = {},
    };
    // The CRC of the chunk header covers the segment header too.
    chunk_hdr.crc = chunk_hdr.calculate_crc(seg_hdr);

    // The headers go into the room that reset() left ahead of the records, so they do not change
    // the size that finish() returned.
    ostream out(_buffer.get(), chunk_headers_size());
    ser::serialize<ondisk::chunk_header>(out, chunk_hdr);
    if (seg_hdr) {
        ser::serialize<ondisk::segment_header>(out, *seg_hdr);
    }
    _sealed = true;
    return bytes_view(reinterpret_cast<const int8_t*>(_buffer.get()), serialized_size());
}

size_t chunk_builder::estimate_required_segments(size_t record_bytes, size_t record_count, size_t segment_size,
        segment_kind kind, size_t schema_table_size) {
    if (record_count == 0 || record_bytes == 0) {
        return 0;
    }

    const size_t fixed_overhead = chunk_headers_size(kind) + align_up(schema_table_size, ondisk::record_alignment);

    if (segment_size <= fixed_overhead) {
        return 1;
    }

    const auto usable_bytes = segment_size - fixed_overhead;
    auto records_per_segment = (usable_bytes * record_count) / record_bytes;
    if (records_per_segment == 0) {
        records_per_segment = 1;
    }
    return (record_count + records_per_segment - 1) / records_per_segment;
}

template frame_extent chunk_builder::append<log_record_writer>(const log_record_writer&);
template frame_extent chunk_builder::append<log_record_bytes_writer>(const log_record_bytes_writer&);

}
