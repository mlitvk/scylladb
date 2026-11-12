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
#include <seastar/coroutine/as_future.hh>

#include <fmt/format.h>

#include "replica/logstor/index.hh"
#include "serializer.hh"
#include "utils/error_injection.hh"

namespace replica::logstor {

record_stream_source::record_stream_source(table_id table, const primary_index& index, compaction_reenabler compaction_guard,
        utils::chunked_vector<log_segment_id> segments, size_t segment_size, chunk_reader read_chunks,
        record_stream_stats& stats)
    : _table(table)
    , _index(index)
    , _compaction_guard(std::move(compaction_guard))
    , _segments(std::move(segments))
    , _read_chunks(std::move(read_chunks))
    , _stats(stats)
    , _buffer(segment_size, segment_kind::full)
{}

future<temporary_buffer<char>> record_stream_source::get() {
    co_await utils::get_local_injector().inject("logstor_record_source_wait", utils::wait_for_message(std::chrono::minutes(5)));
    // An empty buffer ends the stream, so a segment without live records gives no buffer.
    while (_next_segment < _segments.size()) {
        co_await collect_live_records(_segments[_next_segment++]);
        if (_buffer.has_data()) {
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
            if (!_buffer.can_fit(writer)) {
                on_internal_error(logstor_logger, fmt::format("The live records of {} do not fit a chunk of {} bytes",
                        segment_id, _buffer.buffer_size()));
            }
            _buffer.append(writer);
        }
        return make_ready_future<>();
    });
    if (!found_chunk) {
        throw record_stream_error(fmt::format("Record stream of table {} cannot send {}: it has no valid chunk", _table, segment_id));
    }
}

temporary_buffer<char> record_stream_source::take_chunk() {
    // The receiver does not read the segment sequence.
    _buffer.seal(segment_sequence{0}, _table, ondisk::record_alignment);
    _stats.records_sent += _buffer.record_count();
    const auto chunk = _buffer.chunk();
    temporary_buffer<char> buf(reinterpret_cast<const char*>(chunk.data()), chunk.size());
    _buffer.reset();
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

record_stream_sink::record_stream_sink(table_id table, target_resolver resolve_target, std::vector<owned_write_buffer> buffers,
        segment_writer write_segment, seastar::gate::holder segment_manager_holder, record_stream_stats& stats)
    : _table(table)
    , _resolve_target(std::move(resolve_target))
    , _write_segment(std::move(write_segment))
    , _segment_manager_holder(std::move(segment_manager_holder))
    , _stats(stats)
{
    if (buffers.size() != _buffers.size()) {
        on_internal_error(logstor_logger, fmt::format("A record stream sink takes {} buffers, not {}", _buffers.size(), buffers.size()));
    }
    for (size_t i = 0; i < _buffers.size(); ++i) {
        _buffers[i].buffer = std::move(buffers[i]);
    }
}

record_stream_sink::~record_stream_sink() {
    // A sink that failed has index updates that wait for a segment that is not written. abort()
    // fails them. Consume them here, so that they are not reported as ignored.
    for (auto& gb : _buffers) {
        for (auto& update : gb.index_updates) {
            (void)std::move(update).handle_exception([] (std::exception_ptr) {});
        }
    }
}

future<> record_stream_sink::put(std::span<temporary_buffer<char>> data) {
    throw_if_failed();
    _failed = true;
    // The caller can release the storage of `data` when put() returns, which is before the chunks
    // are written, so put() moves all the buffers before the first suspension.
    for (auto& buf : data) {
        _assembler.push(std::move(buf));
    }
    while (auto chunk = _assembler.next_chunk()) {
        co_await write_chunk(std::move(*chunk));
    }
    _failed = false;
}

future<> record_stream_sink::close() {
    auto f = co_await coroutine::as_future(write_buffered_records());
    if (f.failed()) {
        co_await abort();
        co_await std::move(f);
    }
    co_await release();
}

future<> record_stream_sink::abort() {
    for (auto& gb : _buffers) {
        if (gb.buffer) {
            co_await gb.buffer->abort_writes(std::make_exception_ptr(record_stream_error(fmt::format(
                    "Record stream of table {} was aborted before its records were written", _table))));
        }
        for (auto& update : std::exchange(gb.index_updates, {})) {
            co_await std::move(update).handle_exception([] (std::exception_ptr) {});
        }
    }
    co_await release();
}

void record_stream_sink::throw_if_failed() const {
    if (_failed) {
        throw record_stream_error(fmt::format("Record stream of table {} takes no more data after a failure", _table));
    }
}

future<> record_stream_sink::write_chunk(temporary_buffer<char> bytes) {
    auto chunk = co_await parse_chunk(std::move(bytes));
    if (!chunk) {
        throw record_stream_error(fmt::format("Record stream of table {} has a chunk that is not valid", _table));
    }
    for (const auto& record : chunk->records()) {
        co_await write_record(record);
    }
}

future<> record_stream_sink::write_record(const chunk_record& record) {
    const auto& header = record.header;
    if (header.table != _table) {
        throw record_stream_error(fmt::format("Record stream of table {} has a record of table {}", _table, header.table));
    }
    auto writer = log_record_bytes_writer(header, record.bytes);
    // The buffers of a sink have the same size.
    const auto max_record_size = _buffers[0].buffer->max_record_size();
    if (writer.record_size() > max_record_size) {
        throw record_stream_error(fmt::format("Record stream of table {} has a record of {} bytes, more than the limit of {} bytes",
                _table, writer.record_size(), max_record_size));
    }

    auto key = header.index_key();
    auto* gb = co_await buffer_for(_resolve_target(key.token()));
    if (!gb->buffer->can_fit(writer)) {
        co_await flush(*gb);
    }

    auto* index = &gb->target.cg->logstor_index();
    gb->index_updates.push_back(gb->buffer->write(std::move(writer)).then_unpack(
            [stats = &_stats, index, key = std::move(key), timestamp = header.timestamp] (record_location location, seastar::gate::holder) {
        ++stats->records_received;
        if (!index->insert(key, index_entry{.location = location, .timestamp = timestamp}).inserted()) {
            ++stats->records_superseded;
        }
    }));
}

// Returns the buffer of the group of `target`. A group without a buffer takes a buffer without a
// group, or else the buffer that the last record did not use, after its records are written. So
// when a storage group enters split mode during the stream, the buffer of its main group goes to a
// new group as soon as records of both new groups arrive, and the holders of the main group do not
// keep the storage group from becoming ready for the split.
future<record_stream_sink::group_buffer*> record_stream_sink::buffer_for(write_target target) {
    static_assert(record_stream_sink_buffers == 2, "the choice of the buffer to write out assumes two buffers");
    for (size_t i = 0; i < _buffers.size(); ++i) {
        if (_buffers[i].target.cg == target.cg) {
            _last_used = i;
            co_return &_buffers[i];
        }
    }
    auto unused = std::ranges::find_if(_buffers, [] (const group_buffer& gb) { return !gb.target.cg; });
    const size_t i = unused != _buffers.end() ? size_t(unused - _buffers.begin()) : 1 - _last_used;
    auto& gb = _buffers[i];
    if (gb.target.cg) {
        co_await flush(gb);
    }
    gb.target = std::move(target);
    _last_used = i;
    co_return &gb;
}

future<> record_stream_sink::write_buffered_records() {
    throw_if_failed();
    if (_assembler.has_partial_chunk()) {
        throw record_stream_error(fmt::format("Record stream of table {} ended inside a chunk", _table));
    }
    for (auto& gb : _buffers) {
        co_await flush(gb);
    }
}

// Writes the records of the buffer to a segment of its group and waits for their index updates.
// The buffer keeps its group.
future<> record_stream_sink::flush(group_buffer& gb) {
    if (gb.buffer->has_data()) {
        co_await _write_segment(*gb.buffer, *gb.target.cg);
        utils::get_local_injector().inject("logstor_stream_sink_after_flush", [] {
            throw std::runtime_error("logstor_stream_sink_after_flush injection");
        });
    }
    auto updates = std::exchange(gb.index_updates, {});
    co_await when_all_succeed(updates.begin(), updates.end());
    co_await gb.buffer->close();
    gb.buffer->reset();
}

future<> record_stream_sink::release() noexcept {
    for (auto& gb : _buffers) {
        if (gb.buffer) {
            // The pool takes back only a closed buffer.
            co_await gb.buffer->close();
            gb.buffer.reset();
        }
        gb.target = {};
    }
    _resolve_target = {};
    _segment_manager_holder.release();
}

}
