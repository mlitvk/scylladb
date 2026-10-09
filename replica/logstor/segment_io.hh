/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <concepts>
#include <functional>
#include <optional>
#include <span>
#include <vector>

#include <seastar/core/fstream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "replica/logstor/ondisk.hh"

namespace replica::logstor {

struct segment_info {
    segment_kind kind;
    segment_sequence segment_seq;

    struct mixed {};
    struct full {
        table_id table;
        dht::token first_token;
        dht::token last_token;
    };

    std::variant<mixed, full> v;
};

using want_data = seastar::bool_class<class want_data_tag>;

using record_header_consumer = std::function<want_data(record_location, const record_header&)>;
// Called for each record the scan was asked for, with the record decoded.
using log_record_consumer = std::function<future<>(record_location, log_record)>;
// The same, with the record's header and value as views into the buffer's bytes, for a consumer
// that copies them rather than decoding them.
using log_record_bytes_consumer = std::function<future<>(record_location, const record_header&, log_record_bytes_view)>;
using segment_info_consumer = std::function<future<>(const segment_info&)>;
// Called for each entry of the schema table of a chunk, before any of the chunk's records. The
// entry's mapping view is only valid during the call. A consumer that throws std::runtime_error
// rejects the entry, and the scan skips the records of the entry's version.
using schema_mapping_consumer = std::function<void(const ondisk::schema_table_entry&)>;
using streamed_buffer_consumer = std::function<future<>(bytes_view)>;

template <typename Consumer>
concept log_record_consumer_like =
    (std::invocable<Consumer&, record_location, log_record> &&
     std::same_as<std::invoke_result_t<Consumer&, record_location, log_record>, future<>>) ||
    (std::invocable<Consumer&, record_location, const record_header&, log_record_bytes_view> &&
     std::same_as<std::invoke_result_t<Consumer&, record_location, const record_header&, log_record_bytes_view>, future<>>);

// The segment info of the segment's first chunk, or nothing if its headers are not valid. The
// chunk's body is not read, so a torn chunk is told apart only by scan_segment().
future<std::optional<segment_info>> read_segment_info(seastar::input_stream<char>& in);

// Scans the chunks of a segment in order, and delivers their records.
//
// For each chunk with valid headers, the scan calls on_segment_info once, before it reads the body.
// When the body passes its CRC check, the scan calls on_schema_mapping for each entry of the
// chunk's schema table, before it delivers any record of the chunk. Then it calls on_record_header
// for each record frame, and on_record for each record that on_record_header asks for. The views
// that on_record gets point into the chunk body. They are valid until the future of on_record
// resolves.
//
// The scan stops at the first chunk whose headers are invalid, whose headers or body are cut short,
// or whose segment_seq is not the one of the first chunk. The body is checked against its body CRC
// before the scan delivers a record of the chunk. A chunk whose body fails gives its segment info
// and no records, and the scan continues at the next chunk. The scan also stops after the only
// chunk of a full segment. A record frame that does not parse ends the scan of its chunk only. A
// record whose schema version the schema table does not list for the record's table is skipped,
// and so is a record of a version whose entry on_schema_mapping rejects. A schema table that does
// not parse lists only the versions before the error.
future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_info_consumer on_segment_info,
        record_header_consumer on_record_header,
        log_record_bytes_consumer on_record,
        schema_mapping_consumer on_schema_mapping = {});

// The same, with each record given to on_record as a log_record.
future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_info_consumer on_segment_info,
        record_header_consumer on_record_header,
        log_record_consumer on_record,
        schema_mapping_consumer on_schema_mapping = {});

// Rewrites the initial streamed logstor chunk header to the local segment sequence and
// forwards subsequent bytes unchanged. This preserves the current branch's streaming
// behavior, which only rewrites the first streamed chunk header.
class streamed_segment_rewriter {
    log_segment_id _target_segment;
    segment_sequence _target_seq;
    streamed_buffer_consumer _on_buffer;
    std::vector<char> _pending_data;
    std::optional<size_t> _initial_header_size;
    bool _header_rewritten = false;

    ondisk::chunk_header read_chunk_header() const;
    void maybe_parse_initial_header();
    void rewrite_chunk_header();
    future<> flush_pending_data();

public:
    streamed_segment_rewriter(log_segment_id target_segment, segment_sequence target_seq, streamed_buffer_consumer on_buffer);

    future<> put(std::span<temporary_buffer<char>> data);
    future<> close();

    size_t buffer_size() const noexcept;
};

}
