/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <algorithm>
#include <memory>
#include <optional>

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/simple-stream.hh>

#include "replica/logstor/column_mapping_registry.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/record_writer.hh"
#include "utils/small_vector.hh"

namespace replica {

namespace logstor {

// The offset of a record frame in a chunk, and its size without its padding, as in record_location.
struct frame_extent {
    uint32_t offset;
    uint32_t size;
};

// Returns the location of the record whose frame is at `frame` in a chunk that is written at
// `chunk_position`.
inline record_location locate_record(segment_position chunk_position, frame_extent frame) noexcept {
    return record_location {
        .segment = chunk_position.segment,
        .offset = chunk_position.offset + frame.offset,
        .size = frame.size,
    };
}

// The schema table of a chunk that a buffer builds: the registry entry of each schema version of
// the chunk's records, once, in the order the versions were first added, and the size the table
// takes on disk.
class chunk_schema_table {
    utils::small_vector<const schema_mapping*, 2> _entries;
    size_t _size{ondisk::schema_table_header_size};

public:
    // The bytes the table grows by for a record of the version of `mapping`: its entry if the
    // version is new to the table, nothing otherwise.
    size_t growth(const schema_mapping& mapping) const noexcept;

    // Adds the version of `mapping` if it is new. `growth` is what growth() returned for it.
    void add(const schema_mapping& mapping, size_t growth);

    size_t size() const noexcept { return _size; }

    void clear() noexcept;

    // Writes the table. `out` has exactly size() bytes.
    void write(seastar::simple_memory_output_stream& out) const;
};

// Builds one chunk in memory, in a buffer of buffer_size() bytes:
//
//   0                     +------------------+
//                         |   chunk_header   |  seal()
//                         |  segment_header  |  seal(), full segments only
//   chunk_headers_size()  +------------------+
//                         |   record frame   |  append(), one frame per record,
//                         |   record frame   |  each padded to record_alignment
//                         |       ...        |
//   + records_size        +------------------+
//                         |   schema_table   |  finish()
//   + schema_table_size   +------------------+
//                         |   zero padding   |  finish(), to block_alignment
//   serialized_size()     +------------------+
//                         |      unused      |
//   buffer_size()         +------------------+
//
// reset() reserves the space for the headers, so the records start at
// chunk_headers_size(). The segment_header holds the table and the token range of the
// records. The schema_table lists the column mapping of each schema version of the
// records once. The record frames and the schema table are the body of the chunk,
// which the body CRC covers.
//
// append() also reserves the room for the schema table entry of a new schema version,
// so the table always fits. finish() writes the table, computes the body CRC and pads
// the chunk, and the size it returns does not change after this. seal() then writes
// the headers, which hold the sequence number of the segment that receives the chunk. So a
// caller can reserve room for the chunk in a segment before it knows that number.
//
// This type only serializes a chunk. Tests and write_buffer use it directly.
class chunk_builder {
public:

    using ostream = seastar::simple_memory_output_stream;

private:

    using aligned_buffer_type = std::unique_ptr<char[], free_deleter>;

    size_t _buffer_size;
    aligned_buffer_type _buffer;
    segment_kind _segment_kind;
    ostream _stream;

    // The frame sizes of the appended records summed, their padding excluded.
    size_t _record_bytes{0};
    size_t _record_count{0};
    std::optional<dht::token> _min_token;
    std::optional<dht::token> _max_token;

    chunk_schema_table _schema_table;

    // What finish() computes about the chunk body, for the chunk header.
    struct finished_body {
        uint32_t records_size;
        uint32_t schema_table_size;
        uint32_t body_crc;
    };
    // Set by finish().
    std::optional<finished_body> _finished_body;
    bool _sealed{false};

public:

    chunk_builder(size_t buffer_size, segment_kind kind);

    void reset();

    chunk_builder(const chunk_builder&) = delete;
    chunk_builder& operator=(const chunk_builder&) = delete;

    chunk_builder(chunk_builder&&) noexcept = default;
    chunk_builder& operator=(chunk_builder&&) noexcept = default;

    const char* data() const noexcept { return _buffer.get(); }

    // The bytes written to the buffer so far, which after finish() is the whole chunk.
    size_t serialized_size() const noexcept { return _buffer_size - _stream.size(); }

    size_t buffer_size() const noexcept { return _buffer_size; }

    // Whether the writer's record fits, along with the schema table entry it adds if its schema
    // version is not in the buffer yet.
    template <log_record_writer_concept Writer>
    bool can_fit(const Writer& writer) const noexcept {
        return can_fit(writer.record_size(), _schema_table.growth(writer.mapping()));
    }

    bool has_data() const noexcept;

    // The size of the schema table of a chunk whose records are all of the version of `mapping`.
    static size_t schema_table_size(const schema_mapping& mapping) noexcept {
        return ondisk::schema_table_header_size + ondisk::schema_table_entry_size(mapping.serialized.size());
    }

    // The largest record - record header and value, the frame header aside - of the schema
    // version of `mapping` that fits an empty buffer of this size and kind. The kinds differ in
    // what they carry ahead of their records, so a record can fit a buffer of one kind and not of
    // the other.
    static size_t max_record_size(size_t buffer_size, segment_kind kind, const schema_mapping& mapping) noexcept {
        const size_t overhead = chunk_headers_size(kind) + ondisk::record_frame_header_size
                + align_up(schema_table_size(mapping), ondisk::record_alignment);
        return buffer_size > overhead ? buffer_size - overhead : 0;
    }

    // The largest record of the schema version of `mapping` that fits an empty buffer of this
    // size whatever its kind. A record is written to a segment of one kind and can be rewritten
    // into a segment of the other - the separator rewrites the records of a mixed segment into
    // full segments of their compaction group - so a record that logstor accepts at all has to
    // fit both.
    static size_t max_record_size_any_kind(size_t buffer_size, const schema_mapping& mapping) noexcept {
        return std::min(max_record_size(buffer_size, segment_kind::mixed, mapping),
                        max_record_size(buffer_size, segment_kind::full, mapping));
    }

    size_t max_record_size(const schema_mapping& mapping) const noexcept {
        return max_record_size(_buffer_size, _segment_kind, mapping);
    }

    size_t record_bytes() const noexcept { return _record_bytes; }
    size_t record_count() const noexcept { return _record_count; }
    segment_kind kind() const noexcept { return _segment_kind; }

    template <log_record_writer_concept Writer>
    frame_extent append(const Writer& writer);

    // How many segments of this kind `record_count` records of `record_bytes` bytes take.
    // `schema_table_size` is the estimated size of the schema table of one segment.
    static size_t estimate_required_segments(size_t record_bytes, size_t record_count, size_t segment_size,
            segment_kind, size_t schema_table_size);

    // What a chunk of this kind carries ahead of its records: the chunk header, and the
    // segment header too for a full segment.
    static constexpr size_t chunk_headers_size(segment_kind kind) noexcept {
        size_t s = ondisk::chunk_header_size;
        if (kind == segment_kind::full) {
            s += ondisk::segment_header_size;
        }
        return s;
    }

    size_t chunk_headers_size() const noexcept {
        return chunk_headers_size(_segment_kind);
    }

    // Ends the appends: writes the schema table, pads the chunk to block_alignment and computes
    // the body CRC. Returns the size of the chunk, which does not change after this, so a caller
    // can reserve room for the chunk in a segment before it seals the buffer.
    size_t finish();

    // Completes the chunk: finishes the buffer if finish() was not called, and writes the
    // headers. Returns the chunk, which is what is written to a segment. `table` is set for
    // segment_kind::full.
    bytes_view seal(segment_sequence segment_seq, std::optional<table_id> table);

    // The size of the schema table of the versions appended so far.
    size_t schema_table_size() const noexcept { return _schema_table.size(); }

private:

    bool with_segment_header() const noexcept {
        return _segment_kind == segment_kind::full;
    }

    bool can_fit(size_t record_size, size_t schema_table_growth) const noexcept;

    void pad_to_alignment(size_t alignment);

    friend class write_buffer;
};

extern template frame_extent chunk_builder::append<log_record_writer>(const log_record_writer&);
extern template frame_extent chunk_builder::append<log_record_bytes_writer>(const log_record_bytes_writer&);

}
}
