/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/write_buffer.hh"

#include <seastar/core/on_internal_error.hh>

#include "replica/logstor/logstor.hh"

namespace replica::logstor {

write_buffer::write_buffer(size_t buffer_size, segment_kind kind)
        : _builder(buffer_size, kind)
{
    if (keeps_separator_records()) {
        _separator_records.reserve(_builder.buffer_size() / 100);
    }
}

void write_buffer::reset() {
    _builder.reset();
    _chunk_position = {};
    _separator_records.clear();
    _write_gate = {};
}

future<> write_buffer::close() {
    if (!_write_gate.is_closed()) {
        co_await _write_gate.close();
    }
}

bool write_buffer::is_closed() const noexcept {
    return _write_gate.is_closed();
}

template <log_record_writer_concept Writer>
future<written_record> write_buffer::write(Writer writer, write_target target) {
    const auto frame = _builder.append(writer);

    if (keeps_separator_records()) {
        if constexpr (std::same_as<Writer, log_record_writer>) {
            _separator_records.push_back(separator_record {
                .writer = std::move(writer),
                .frame = frame,
                .target = std::move(target)
            });
        } else {
            on_internal_error(logstor_logger, "Record byte writes are not supported for mixed write buffers");
        }
    }

    // hold the write buffer until the write is complete, and pass the holder to the
    // caller for follow-up operations that should continue holding the buffer, such
    // as index updates.
    auto buffer_holder = _write_gate.hold();

    return _chunk_position.get_shared_future().then(
            [frame, buffer_holder = std::move(buffer_holder)] (segment_position chunk_position) mutable {
        return written_record{locate_record(chunk_position, frame), std::move(buffer_holder)};
    });
}

future<> write_buffer::complete_writes(segment_position chunk_position) {
    _chunk_position.set_value(chunk_position);
    co_await close();
}

future<> write_buffer::abort_writes(std::exception_ptr ex) {
    if (!_chunk_position.available()) {
        _chunk_position.set_exception(std::move(ex));
    }

    // Mixed buffers keep copies of their records for separator rewriting. A failed flush has no
    // separator pass to consume them, and they were never written anywhere, so drop them.
    _separator_records.clear();

    co_await close();
}

std::vector<separator_record> write_buffer::take_separator_records() {
    return std::move(_separator_records);
}

template future<written_record> write_buffer::write<log_record_writer>(log_record_writer, write_target);
template future<written_record> write_buffer::write<log_record_bytes_writer>(log_record_bytes_writer, write_target);

}
