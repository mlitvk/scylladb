/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "write_buffer.hh"
#include "segment_manager.hh"
#include "bytes_fwd.hh"
#include "logstor.hh"
#include "replica/log_structured/types.hh"
#include <seastar/core/simple-stream.hh>
#include <seastar/core/with_scheduling_group.hh>
#include "serializer_impl.hh"
#include "idl/log_structured.dist.hh"
#include "idl/log_structured.dist.impl.hh"
#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>

namespace replica::log_structured {

write_buffer::write_buffer(size_t buffer_size)
        : _buffer_size(buffer_size)
        , _buffer(allocate_aligned_buffer<char>(buffer_size, 4096)) {
    reset();
}

void write_buffer::reset() noexcept {
    _buffered_writes.clear();
    total_data_size = 0;

    _stream = seastar::simple_memory_output_stream(_buffer.get(), _buffer_size);
    _buffer_header = _stream.write_substream(buffer_header_size);
}

size_t write_buffer::get_max_write_size() const noexcept {
    return _buffer_size - buffer_header_size - record_header_size;
}

bool write_buffer::has_space(size_t data_size) const noexcept {
    // Calculate total space needed including header, data, and alignment padding
    auto total_size = record_header_size + data_size;
    auto aligned_size = align_up(total_size, segment_manager::record_alignment);

    return aligned_size <= _stream.size();
}

future<log_location> write_buffer::write(std::function<void(ostream&)> writer, uint32_t data_size) {
    // Write header
    auto header_out = _stream.write_substream(record_header_size);
    ser::serialize(header_out, data_size);

    // Write actual data
    size_t data_offset_in_buffer = offset_in_buffer();
    auto data_out = _stream.write_substream(data_size);
    writer(data_out);
    total_data_size += data_size;

    // Add padding to align record
    pad_to_alignment(segment_manager::record_alignment);

    buffered_write bw {
        .offset = data_offset_in_buffer,
        .size = data_size,
        .pr = promise<log_location>()
    };
    auto fut = bw.pr.get_future();
    _buffered_writes.push_back(std::move(bw));
    return fut;
}

void write_buffer::finalize() {
    auto data_size = offset_in_buffer() - buffer_header_size;
    ser::serialize(_buffer_header, static_cast<uint32_t>(data_size));
}

void write_buffer::pad_to_alignment(size_t alignment) {
    auto current_pos = offset_in_buffer();
    auto next_pos = align_up(current_pos, alignment);
    auto padding = next_pos - current_pos;
    if (padding > 0) {
        _stream.fill('\0', padding);
    }
}

void write_buffer::complete_writes(log_location base_location) {
    for (auto& bw : _buffered_writes) {
        bw.pr.set_value(log_location {
            .segment = base_location.segment,
            .offset = base_location.offset + bw.offset,
            .size = static_cast<uint32_t>(bw.size)
        });
    }
}

bool write_buffer::has_data() const noexcept {
    return offset_in_buffer() > buffer_header_size;
}

buffered_writer::buffered_writer(segment_manager& sm, seastar::scheduling_group flush_sg)
        : _sm(sm)
        , _active_buffer({
            .buf = write_buffer(_sm.get_segment_size()),
            .flush_requested = false,
        })
        , _available_buffers(num_buffers - 1)
        , _flush_sg(flush_sg) {
    for (size_t i = 0; i < num_buffers - 1; ++i) {
        _available_buffers.push(write_buffer(_sm.get_segment_size()));
    }
}

future<> buffered_writer::start() {
    logstor_logger.info("Starting write buffer");
    co_return;
}

future<> buffered_writer::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping write buffer");

    co_await _async_gate.close();
    logstor_logger.info("Write buffer stopped");
}

future<log_location> buffered_writer::write(log_structured_segment_record record) {
    auto holder = _async_gate.hold();

    seastar::measuring_output_stream ms;
    ser::serialize(ms, record);
    size_t write_size = ms.size();

    logstor_logger.debug("Buffering mutation with key {} ({} bytes)", record.mut.key(), write_size);

    if (write_size > _active_buffer.buf.get_max_write_size()) {
        throw std::runtime_error(fmt::format("TODO Write size {} exceeds buffer size {}", write_size, _active_buffer.buf.get_max_write_size()));
    }

    // Check if write fits in current buffer
    while (!_active_buffer.buf.has_space(write_size)) {
        co_await _space_available.wait();
    }

    auto& buf = _active_buffer.buf;

    // Write to buffer at current position
    auto fut = buf.write([&] (auto& out) {
        ser::serialize(out, record);
    }, write_size);

    logstor_logger.trace("Buffered {} writes, buffer used {}/{} bytes",
                        buf.num_writes(), buf.offset_in_buffer(), buf.max_size());

    // Trigger flush for the active buffer if not in progress
    if (!std::exchange(_active_buffer.flush_requested, true)) {
        (void)with_gate(_async_gate, [this] {
            return switch_buffer().then([this] (write_buffer old_buf) mutable {
                return seastar::with_scheduling_group(_flush_sg, [this, buf = std::move(old_buf)] () mutable {
                    return flush(std::move(buf));
                });
            });
        });
    }

    co_return co_await std::move(fut);
}

future<write_buffer> buffered_writer::switch_buffer() {
    // Wait for and get the next available buffer
    auto new_buf = co_await _available_buffers.pop_eventually();
    auto next_active_buffer = active_buffer {
        .buf = std::move(new_buf),
        .flush_requested = false,
    };

    auto old_active_buffer = std::exchange(_active_buffer, std::move(next_active_buffer));
    logstor_logger.trace("Switched active write buffer");

    // Notify writers that the active buffer now has space available
    _space_available.signal();

    co_return std::move(old_active_buffer.buf);
}

future<> buffered_writer::flush(write_buffer buf) {
    logstor_logger.debug("Flushing write buffer ({} bytes, {} writes)",
                        buf.offset_in_buffer(), buf.num_writes());

    // Fill buffer header with total data size (excluding header itself)
    buf.finalize();
    buf.pad_to_alignment(_sm.get_write_alignment());

    // Write the aligned buffer to segment manager
    bytes_view data(reinterpret_cast<const int8_t*>(buf.data()), buf.offset_in_buffer());
    auto base_location = co_await _sm.write(data, buf.total_data_size);

    logstor_logger.debug("Write buffer flushed to segment {} at offset {} (data size: {} aligned size: {})",
                        base_location.segment, base_location.offset, buf.total_data_size, buf.offset_in_buffer());

    // Complete all buffered write promises with their individual locations
    buf.complete_writes(base_location);
    buf.reset();

    // Return the flushed buffer to the available queue
    _available_buffers.push(std::move(buf));
}

}
