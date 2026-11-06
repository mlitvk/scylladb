/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <concepts>
#include <exception>
#include <type_traits>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/log.hh>

#include "replica/logstor/chunk_builder.hh"
#include "replica/logstor/record_writer.hh"

namespace replica {

namespace logstor {

extern seastar::logger logstor_logger;

class logstor_group;

struct write_target {
    logstor_group* cg = nullptr;
    // Keeps the group from being reported as empty while the write is in flight: until the record
    // is in a segment and in the index, the group's own state does not account for it, and a tablet
    // split ACKs a pre-split group once it looks empty.
    seastar::gate::holder non_empty_holder;
    // Keeps the group from finishing its stop() while the write is in flight. It is released only
    // when the write target is dropped, which for a separator-eligible record is after the record
    // was handed to the group's separator buffer, so a group that stopped has nothing left to come.
    seastar::gate::holder alive_holder;
};

// A record of a mixed buffer that the separator rewrites once the buffer is written.
struct separator_record {
    log_record_writer writer;
    frame_extent frame;
    write_target target;

    record_location location(segment_position chunk_position) const noexcept {
        return locate_record(chunk_position, frame);
    }
};

// The work for one record of a write buffer. The buffer calls on_written() with the location of the
// record once the chunk of the record is written to a segment, or on_failed() when the chunk is not
// written. It calls one of them once for each record, in the order of the records.
template <typename Completion>
concept write_completion = std::is_nothrow_move_constructible_v<Completion>
        && std::is_nothrow_move_assignable_v<Completion>
        && requires (Completion& c, record_location location, std::exception_ptr ex) {
    { c.on_written(location) } noexcept;
    { c.on_failed(ex) } noexcept;
};

// A chunk builder that keeps a completion for each record that it takes, and completes them once
// its chunk is written to a segment, or fails them when the chunk is not written.
//
// mixed_write_buffer and full_write_buffer take different records and seal different headers, and
// this class holds what they share.
template <write_completion Completion>
class write_buffer {
    struct entry {
        Completion completion;
        frame_extent frame{};

        explicit entry(Completion&& c) noexcept
            : completion(std::move(c))
        {}
    };

    chunk_builder _builder;
    // The buffer clears the vector and does not free it, so that a reused buffer allocates nothing.
    std::vector<entry> _entries;

public:
    write_buffer(const write_buffer&) = delete;
    write_buffer& operator=(const write_buffer&) = delete;

    // Ends the appends, see chunk_builder::finish().
    size_t finish() { return _builder.finish(); }

    template <log_record_writer_concept Writer>
    bool can_fit(const Writer& writer) const noexcept { return _builder.can_fit(writer); }
    bool has_data() const noexcept { return _builder.has_data(); }

    size_t serialized_size() const noexcept { return _builder.serialized_size(); }
    size_t buffer_size() const noexcept { return _builder.buffer_size(); }
    size_t record_bytes() const noexcept { return _builder.record_bytes(); }
    size_t record_count() const noexcept { return _builder.record_count(); }

protected:
    write_buffer(size_t buffer_size, segment_kind kind)
        : _builder(buffer_size, kind)
    {}

    // A completion that is destroyed and not called leaves its write without an outcome.
    ~write_buffer() {
        if (!_entries.empty()) {
            on_internal_error_noexcept(logstor_logger, fmt::format("Destroying a write buffer with {} writes that are not complete", _entries.size()));
        }
    }

    write_buffer(write_buffer&&) noexcept = default;
    write_buffer& operator=(write_buffer&&) noexcept = default;

    // Appends the record, and keeps `completion` for complete_writes(). The completion goes into
    // the buffer before the record, so that nothing can fail once the record is in the chunk. When
    // the append throws, `completion` is left to the caller.
    template <log_record_writer_concept Writer>
    frame_extent append(const Writer& writer, Completion&& completion) {
        // emplace_back() allocates before it moves `completion`, so a failed allocation also leaves
        // `completion` to the caller.
        auto& e = _entries.emplace_back(std::move(completion));
        try {
            e.frame = _builder.append(writer);
        } catch (...) {
            completion = std::move(e.completion);
            _entries.pop_back();
            throw;
        }
        return e.frame;
    }

    // Calls on_written() of each completion, once the chunk of the buffer is written at
    // `chunk_position`.
    future<> complete_writes(segment_position chunk_position) {
        for (auto& e : _entries) {
            e.completion.on_written(locate_record(chunk_position, e.frame));
            co_await coroutine::maybe_yield();
        }
        _entries.clear();
    }

    // Calls on_failed() of each completion with `ex`, for a chunk that is not written.
    void abort_writes(std::exception_ptr ex) noexcept {
        for (auto& e : _entries) {
            e.completion.on_failed(ex);
        }
        _entries.clear();
    }

    // Empties the buffer for a new chunk. The writes of the last chunk must be complete or aborted.
    void reset() {
        if (!_entries.empty()) {
            on_internal_error(logstor_logger, fmt::format("Cannot reset a write buffer with {} writes that are not complete", _entries.size()));
        }
        _builder.reset();
    }

    chunk_builder& builder() noexcept { return _builder; }
    const chunk_builder& builder() const noexcept { return _builder; }
};

// The write buffer of a mixed segment, which takes the user writes. It keeps the records it took
// until its chunk is written, for the separator to rewrite them into full segments.
template <write_completion Completion>
class mixed_write_buffer final : public write_buffer<Completion> {
    using base = write_buffer<Completion>;

    // The separator takes the vector with each chunk, so write() reserves the record count of the
    // last chunk for the next one.
    std::vector<separator_record> _separator_records;
    size_t _last_record_count = 0;

public:
    explicit mixed_write_buffer(size_t buffer_size)
        : base(buffer_size, segment_kind::mixed)
    {}

    // Appends the record, see write_buffer::append().
    void write(log_record_writer writer, write_target target, Completion&& completion) {
        if (_separator_records.capacity() == 0) {
            _separator_records.reserve(_last_record_count);
        }
        auto& record = _separator_records.emplace_back(separator_record{
            .writer = std::move(writer),
            .target = std::move(target),
        });
        try {
            record.frame = base::append(record.writer, std::move(completion));
        } catch (...) {
            _separator_records.pop_back();
            throw;
        }
    }

    bytes_view seal(segment_sequence segment_seq) {
        return base::builder().seal(segment_seq);
    }

    // Completes the writes, and only then returns the records of the chunk for the separator, so
    // that the separator finds each record in the index at the location that it moves it from.
    future<std::vector<separator_record>> complete_writes(segment_position chunk_position) {
        co_await base::complete_writes(chunk_position);
        _last_record_count = _separator_records.size();
        co_return std::move(_separator_records);
    }

    void abort_writes(std::exception_ptr ex) noexcept {
        // The separator never gets these records, so drop them and the holders of their targets.
        _separator_records.clear();
        base::abort_writes(std::move(ex));
    }

    void reset() {
        base::reset();
        _separator_records.clear();
    }
};

// The write buffer of a full segment, which compaction and the separator write.
template <write_completion Completion>
class full_write_buffer final : public write_buffer<Completion> {
    using base = write_buffer<Completion>;

public:
    explicit full_write_buffer(size_t buffer_size)
        : base(buffer_size, segment_kind::full)
    {}

    // Appends the record, see write_buffer::append().
    template <log_record_writer_concept Writer>
    void write(const Writer& writer, Completion&& completion) {
        base::append(writer, std::move(completion));
    }

    bytes_view seal(segment_sequence segment_seq, table_id table) {
        return base::builder().seal(segment_seq, table);
    }

    using base::complete_writes;
    using base::abort_writes;
    using base::reset;

    size_t schema_table_size() const noexcept { return base::builder().schema_table_size(); }
};

}
}
