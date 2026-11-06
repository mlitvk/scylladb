/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/write_buffer.hh"

namespace replica::logstor {

write_buffer::write_buffer(size_t buffer_size, segment_kind kind)
        : _builder(buffer_size, kind)
{}

future<> write_buffer::complete_writes(segment_position chunk_position) {
    _chunk_position.set_value(chunk_position);
    co_await close();
}

future<> write_buffer::close() {
    if (!_write_gate.is_closed()) {
        co_await _write_gate.close();
    }
}

bool write_buffer::is_closed() const noexcept {
    return _write_gate.is_closed();
}

future<written_record> write_buffer::track(frame_extent frame) {
    auto buffer_holder = _write_gate.hold();

    return _chunk_position.get_shared_future().then(
            [frame, buffer_holder = std::move(buffer_holder)] (segment_position chunk_position) mutable {
        return written_record{locate_record(chunk_position, frame), std::move(buffer_holder)};
    });
}

future<> write_buffer::abort_writes(std::exception_ptr ex) {
    if (!_chunk_position.available()) {
        _chunk_position.set_exception(std::move(ex));
    }
    co_await close();
}

void write_buffer::reset() {
    _builder.reset();
    _chunk_position = {};
    _write_gate = {};
}

mixed_write_buffer::mixed_write_buffer(size_t buffer_size)
        : write_buffer(buffer_size, segment_kind::mixed)
{
    _separator_records.reserve(buffer_size / 100);
}

future<written_record> mixed_write_buffer::write(log_record_writer writer, write_target target) {
    const auto frame = builder().append(writer);
    _separator_records.push_back(separator_record {
        .writer = std::move(writer),
        .frame = frame,
        .target = std::move(target)
    });
    return track(frame);
}

std::vector<separator_record> mixed_write_buffer::take_separator_records() {
    return std::move(_separator_records);
}

future<> mixed_write_buffer::abort_writes(std::exception_ptr ex) {
    // A failed flush has no separator pass to consume the records, and they were never written
    // anywhere, so drop them, along with the holders of their write targets.
    _separator_records.clear();
    return write_buffer::abort_writes(std::move(ex));
}

void mixed_write_buffer::reset() {
    write_buffer::reset();
    _separator_records.clear();
}

}
