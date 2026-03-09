/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "types.hh"
#include "utils/chunked_vector.hh"
#include "write_buffer.hh"
#include "utils/log_heap.hh"

namespace replica::logstor {

constexpr log_heap_options segment_descriptor_hist_options(4 * 1024, 3, 128 * 1024);

struct segment_set;

struct segment_descriptor : public log_heap_hook<segment_descriptor_hist_options> {
    // free_space = segment_size - net_data_size
    // initially set to segment_size
    // when writing records, decrease by total net data size
    // when freeing a record, increase by the record's net data size
    size_t free_space{0};
    size_t record_count{0};
    segment_generation seg_gen{1};
    segment_set* owner{nullptr}; // non-owning, set when added to a segment_set

    void reset(size_t segment_size) noexcept {
        free_space = segment_size;
        record_count = 0;
    }

    size_t net_data_size(size_t segment_size) const noexcept {
        return segment_size - free_space;
    }

    void on_free_segment() noexcept {
        ++seg_gen;
    }

    void on_write(size_t net_data_size, size_t cnt = 1) noexcept {
        free_space -= net_data_size;
        record_count += cnt;
    }

    void on_write(log_location loc) noexcept {
        on_write(loc.size);
    }

    void on_free(size_t net_data_size, size_t cnt = 1) noexcept {
        free_space += net_data_size;
        record_count -= cnt;
    }

    void on_free(log_location loc) noexcept {
        on_free(loc.size);
    }
};

using segment_descriptor_hist = log_heap<segment_descriptor, segment_descriptor_hist_options>;

struct segment_set {
    segment_descriptor_hist _segments;

    void add_segment(segment_descriptor& desc) {
        desc.owner = this;
        _segments.push(desc);
    }

    void update_segment(segment_descriptor& desc) {
        _segments.adjust_up(desc);
    }

    void remove_segment(segment_descriptor& desc) {
        _segments.erase(desc);
        desc.owner = nullptr;
    }
};

class segment_ref {
    struct state {
        log_segment_id id;
        std::function<void()> on_last_release;
        std::function<void()> on_failure;
        bool flush_failure{false};
        ~state() {
            if (!flush_failure) {
                if (on_last_release) on_last_release();
            } else {
                if (on_failure) on_failure();
            }
        }
    };
    lw_shared_ptr<state> _state;
public:
    segment_ref() = default;

    // Copyable: copying increments the shared ref count
    segment_ref(const segment_ref&) = default;
    segment_ref& operator=(const segment_ref&) = default;
    segment_ref(segment_ref&&) noexcept = default;
    segment_ref& operator=(segment_ref&&) noexcept = default;

    log_segment_id id() const noexcept { return _state->id; }
    bool empty() const noexcept { return !_state; }

    void set_flush_failure() noexcept { if (_state) _state->flush_failure = true; }

private:
    friend class segment_manager_impl;
    explicit segment_ref(log_segment_id id, std::function<void()> on_last_release, std::function<void()> on_failure)
        : _state(make_lw_shared<state>(id, std::move(on_last_release), std::move(on_failure)))
    {}
};

struct separator_buffer {
    write_buffer* buf;
    utils::chunked_vector<future<>> pending_updates;
    utils::chunked_vector<segment_ref> held_segments;
    size_t debt{0};
    bool flushed{false};

    separator_buffer(write_buffer* wb)
        : buf(wb)
    {}

    ~separator_buffer() {
        if (!flushed && buf && buf->has_data()) {
            for (auto& seg_ref : held_segments) {
                seg_ref.set_flush_failure();
            }
        }
    }

    separator_buffer(const separator_buffer&) = delete;
    separator_buffer& operator=(const separator_buffer&) = delete;

    separator_buffer(separator_buffer&&) noexcept = default;
    separator_buffer& operator=(separator_buffer&&) noexcept = default;

    future<log_location_with_holder> write(log_record_writer writer) {
        debt += writer.size();
        return buf->write(std::move(writer));
    }

    bool can_fit(const log_record_writer& writer) const noexcept {
        return buf->can_fit(writer);
    }

    bool can_fit(size_t write_size) const noexcept {
        return buf->can_fit(write_size);
    }
};

class compaction_manager {
public:
    virtual ~compaction_manager() = default;

    virtual separator_buffer allocate_separator_buffer() = 0;

    virtual future<> flush_separator_buffer(separator_buffer, replica::compaction_group&) = 0;

    virtual void submit(replica::compaction_group&) = 0;

    virtual future<> stop_ongoing_compactions(replica::compaction_group&) = 0;
};

}
