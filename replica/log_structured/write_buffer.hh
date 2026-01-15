/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/simple-stream.hh>
#include "types.hh"
#include "utils/chunked_vector.hh"

namespace replica::log_structured {

class segment_manager;

/// Manages a single aligned buffer for accumulating writes
struct write_buffer {
    static constexpr size_t buffer_header_size = sizeof(uint32_t);
    static constexpr size_t record_header_size = sizeof(uint32_t);

    using ostream = seastar::simple_memory_output_stream;

    // Buffer memory with proper alignment for DMA
    using aligned_buffer_type = std::unique_ptr<char[], free_deleter>;

    size_t _buffer_size;
    aligned_buffer_type _buffer;
    seastar::simple_memory_output_stream _buffer_header;
    seastar::simple_memory_output_stream _stream;

    size_t total_data_size{0};

    // Track individual writes within the buffer
    struct buffered_write {
        size_t offset;  // Offset within _buffer where this write starts
        size_t size;    // Size of this write in bytes
        promise<log_location> pr;  // Promise to complete when flushed
    };
    utils::chunked_vector<buffered_write> _buffered_writes;

    write_buffer(size_t buffer_size);

    write_buffer(const write_buffer&) = delete;
    write_buffer(write_buffer&&) = default;
    write_buffer& operator=(const write_buffer&) = delete;
    write_buffer& operator=(write_buffer&&) = default;

    /// Reset buffer to empty state
    void reset() noexcept;

    /// Get pointer to start of buffer
    const char* data() const noexcept { return _buffer.get(); }

    size_t offset_in_buffer() const noexcept { return _buffer_size - _stream.size(); }

    /// Check if buffer can fit a write with given data size (including header and alignment)
    bool has_space(size_t data_size) const noexcept;

    /// Check if buffer has any data
    bool has_data() const noexcept;

    size_t get_max_write_size() const noexcept;

    /// Get maximum buffer size
    size_t max_size() const noexcept { return _buffer_size; }

    /// Write data to buffer.
    /// Returns a future that will be resolved with the log location once flushed.
    future<log_location> write(std::function<void(ostream&)> writer, uint32_t data_size);

    void finalize();

    void pad_to_alignment(size_t alignment);

    /// Get number of tracked writes
    size_t num_writes() const noexcept { return _buffered_writes.size(); }

    /// Complete all tracked writes with their locations
    void complete_writes(log_location base_location);

};

/// Write buffer that accumulates mutations before flushing to the segment manager.
///
/// Manages multiple buffers to allow concurrent writes and flushes:
/// - One buffer is active and accumulates incoming writes
/// - Other buffers may be flushing to disk in the background
/// When a flush is triggered, the active buffer is switched, allowing new writes
/// to continue accumulating while the previous buffer completes its flush.
class buffered_writer {
    static constexpr size_t num_buffers = 5;

    segment_manager& _sm;

    struct active_buffer {
        write_buffer buf;
        bool flush_requested{false};
    } _active_buffer;

    seastar::queue<write_buffer> _available_buffers;

    seastar::gate _async_gate;
    seastar::condition_variable _space_available;

    seastar::scheduling_group _flush_sg;

public:
    explicit buffered_writer(segment_manager& sm, seastar::scheduling_group flush_sg);

    buffered_writer(const buffered_writer&) = delete;
    buffered_writer& operator=(const buffered_writer&) = delete;

    future<> start();
    future<> stop();

    /// Add a mutation to the write buffer (flushes if it doesn't fit)
    future<log_location> write(log_structured_segment_record);

    future<write_buffer> switch_buffer();

    /// Flush buffered data to the segment manager and complete all pending writes
    future<> flush(write_buffer);

};

}
