/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/record_stream.hh"

#include <algorithm>
#include <array>

#include <seastar/core/on_internal_error.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/exception.hh>

#include <fmt/format.h>

#include "replica/logstor/index.hh"
#include "serializer.hh"

namespace replica::logstor {

record_stream_source::record_stream_source(table_id table, const primary_index& index, compaction_reenabler compaction_guard,
        utils::chunked_vector<log_segment_id> segments, size_t segment_size, chunk_reader read_chunks)
    : _table(table)
    , _index(index)
    , _compaction_guard(std::move(compaction_guard))
    , _segments(std::move(segments))
    , _read_chunks(std::move(read_chunks))
    , _chunk(segment_size, segment_kind::full)
{}

future<temporary_buffer<char>> record_stream_source::get() {
    // An empty buffer ends the stream, so a segment without live records gives no buffer.
    while (_next_segment < _segments.size()) {
        co_await collect_live_records(_segments[_next_segment++]);
        if (_chunk.has_data()) {
            co_return take_chunk();
        }
    }
    co_return temporary_buffer<char>();
}

future<> record_stream_source::collect_live_records(log_segment_id segment_id) {
    bool found_chunk = false;
    co_await _read_chunks(segment_id, [this, segment_id, &found_chunk] (const scanned_chunk& chunk) {
        found_chunk = true;
        const auto& info = chunk.info();
        if (!chunk.body_valid() || info.kind != segment_kind::full || std::get<segment_info::full>(info.v).table != _table) {
            throw record_stream_error(fmt::format("Record stream of table {} cannot send {}: it is not a valid full segment of the table",
                    _table, segment_id));
        }
        for (const auto& record : chunk.records()) {
            if (!_index.is_record_alive(record.header.index_key(), record.location)) {
                continue;
            }
            // The records of a segment fit a chunk of the segment size.
            auto writer = log_record_bytes_writer(record.header, record.bytes);
            if (!_chunk.can_fit(writer)) {
                on_internal_error(logstor_logger, fmt::format("The live records of {} do not fit a chunk of {} bytes",
                        segment_id, _chunk.buffer_size()));
            }
            _chunk.append(writer);
        }
        return make_ready_future<>();
    });
    if (!found_chunk) {
        co_await coroutine::return_exception(record_stream_error(fmt::format(
                "Record stream of table {} cannot send {}: it has no valid chunk", _table, segment_id)));
    }
}

temporary_buffer<char> record_stream_source::take_chunk() {
    // The receiver does not read the segment sequence.
    _chunk.seal(segment_sequence{0}, _table, ondisk::record_alignment);
    const auto chunk = _chunk.chunk();
    temporary_buffer<char> buf(reinterpret_cast<const char*>(chunk.data()), chunk.size());
    _chunk.reset();
    return buf;
}

void stream_chunk_assembler::push(temporary_buffer<char> buf) {
    if (buf.empty()) {
        return;
    }
    _size += buf.size();
    _buffers.push_back(std::move(buf));
}

std::optional<temporary_buffer<char>> stream_chunk_assembler::next_chunk() {
    if (!_chunk_size && _size >= ondisk::chunk_header_size) {
        _chunk_size = read_chunk_size();
    }
    if (!_chunk_size || _size < *_chunk_size) {
        return std::nullopt;
    }
    auto chunk = take_front(*_chunk_size);
    _chunk_size.reset();
    return chunk;
}

size_t stream_chunk_assembler::read_chunk_size() const {
    std::array<char, ondisk::chunk_header_size> bytes;
    copy_front(bytes.data(), bytes.size());
    seastar::simple_memory_input_stream in(bytes.data(), bytes.size());
    const auto ch = ser::deserialize(in, std::type_identity<ondisk::chunk_header>{});
    if (!ondisk::validate_chunk_header_fields(ch) || ch.kind != segment_kind::full) {
        throw record_stream_error("Record stream has a chunk header that is not valid");
    }
    const size_t chunk_size = raw_write_buffer::chunk_headers_size(segment_kind::full) + ch.records_size;
    if (chunk_size > max_stream_chunk_size) {
        throw record_stream_error(fmt::format("Record stream has a chunk of {} bytes, more than the limit of {} bytes",
                chunk_size, max_stream_chunk_size));
    }
    return chunk_size;
}

void stream_chunk_assembler::copy_front(char* out, size_t n) const {
    for (auto it = _buffers.begin(); n > 0; ++it) {
        const size_t len = std::min(n, it->size());
        out = std::copy_n(it->get(), len, out);
        n -= len;
    }
}

temporary_buffer<char> stream_chunk_assembler::take_front(size_t n) {
    _size -= n;
    auto& front = _buffers.front();
    // A chunk that is in one buffer is shared, not copied.
    if (front.size() >= n) {
        auto chunk = front.share(0, n);
        front.trim_front(n);
        if (front.empty()) {
            _buffers.pop_front();
        }
        return chunk;
    }
    temporary_buffer<char> chunk(n);
    copy_front(chunk.get_write(), n);
    while (n > 0) {
        auto& buf = _buffers.front();
        const size_t len = std::min(n, buf.size());
        buf.trim_front(len);
        n -= len;
        if (buf.empty()) {
            _buffers.pop_front();
        }
    }
    return chunk;
}

record_stream_sink::record_stream_sink(table_id table, target_resolver resolve_target, owned_write_buffer buffer,
        segment_writer write_segment, seastar::gate::holder segment_manager_holder)
    : _table(table)
    , _resolve_target(std::move(resolve_target))
    , _buffer(std::move(buffer))
    , _write_segment(std::move(write_segment))
    , _segment_manager_holder(std::move(segment_manager_holder))
{}

record_stream_sink::~record_stream_sink() {
    // A sink that failed has index updates that wait for a segment that is not written. abort()
    // fails them. Consume them here, so that they are not reported as ignored.
    for (auto& update : _index_updates) {
        (void)std::move(update).handle_exception([] (std::exception_ptr) {});
    }
}

future<> record_stream_sink::put(std::span<temporary_buffer<char>> data) {
    for (auto& buf : data) {
        _assembler.push(std::move(buf));
        while (auto chunk = _assembler.next_chunk()) {
            co_await write_chunk(std::move(*chunk));
        }
    }
}

future<> record_stream_sink::close() {
    if (_assembler.has_partial_chunk()) {
        co_await coroutine::return_exception(record_stream_error(fmt::format(
                "Record stream of table {} ended inside a chunk", _table)));
    }
    co_await flush();
    co_await release_buffer();
}

future<> record_stream_sink::abort() {
    if (_buffer) {
        co_await _buffer->abort_writes(std::make_exception_ptr(record_stream_error(fmt::format(
                "Record stream of table {} was aborted before its records were written", _table))));
    }
    for (auto& update : std::exchange(_index_updates, {})) {
        co_await std::move(update).handle_exception([] (std::exception_ptr) {});
    }
    co_await release_buffer();
}

future<> record_stream_sink::write_chunk(temporary_buffer<char> bytes) {
    auto chunk = co_await parse_chunk(std::move(bytes));
    if (!chunk) {
        co_await coroutine::return_exception(record_stream_error(fmt::format(
                "Record stream of table {} has a chunk that is not valid", _table)));
    }
    for (const auto& record : chunk->records()) {
        co_await write_record(record);
    }
}

future<> record_stream_sink::write_record(const chunk_record& record) {
    const auto& header = record.header;
    if (header.table != _table) {
        co_await coroutine::return_exception(record_stream_error(fmt::format(
                "Record stream of table {} has a record of table {}", _table, header.table)));
    }
    auto writer = log_record_bytes_writer(header, record.bytes);
    if (writer.record_size() > _buffer->max_record_size()) {
        co_await coroutine::return_exception(record_stream_error(fmt::format(
                "Record stream of table {} has a record of {} bytes, more than the limit of {} bytes",
                _table, writer.record_size(), _buffer->max_record_size())));
    }

    auto key = header.index_key();
    auto target = _resolve_target(key.token());
    if (_buffer->has_data() && (target.cg != _target.cg || !_buffer->can_fit(writer))) {
        co_await flush();
    }
    if (!_target.cg) {
        _target = std::move(target);
    }

    auto* index = &_target.cg->logstor_index();
    _index_updates.push_back(_buffer->write(std::move(writer)).then_unpack(
            [index, key = std::move(key), timestamp = header.timestamp] (record_location location, seastar::gate::holder) {
        index->insert(key, index_entry{.location = location, .timestamp = timestamp});
    }));
}

future<> record_stream_sink::flush() {
    if (_buffer->has_data()) {
        co_await _write_segment(*_buffer, *_target.cg);
    }
    auto updates = std::exchange(_index_updates, {});
    co_await when_all_succeed(updates.begin(), updates.end());
    co_await _buffer->close();
    _buffer->reset();
    _target = {};
}

future<> record_stream_sink::release_buffer() noexcept {
    if (_buffer) {
        // The pool takes back only a closed buffer.
        co_await _buffer->close();
        _buffer.reset();
    }
    _target = {};
    _segment_manager_holder.release();
}

}
