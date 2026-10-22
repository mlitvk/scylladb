/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <functional>

#include <fmt/format.h>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include "replica/logstor/segment_types.hh"
#include "seastarx.hh"
#include "utils/on_internal_error.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

// segment_ref refers to a segment. The copies share one state, and the last copy that goes frees
// the segment, unless a copy called keep_on_release(). Then the last copy does not free the
// segment, because records in the segment can still be needed, for example because the index still
// points into the segment.
class segment_ref {
    struct state {
        log_segment_id id;
        std::function<void()> on_last_release;
        std::function<void()> on_kept;
        bool keep{false};
        ~state() {
            if (!keep) {
                if (on_last_release) on_last_release();
            } else {
                if (on_kept) on_kept();
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

    void keep_on_release() noexcept { if (_state) _state->keep = true; }

private:
    friend class segment_manager_impl;
    explicit segment_ref(log_segment_id id, std::function<void()> on_last_release, std::function<void()> on_kept)
        : _state(make_lw_shared<state>(id, std::move(on_last_release), std::move(on_kept)))
    {}
};

// Holds a reference to a segment until a step for the records in the segment is complete, for
// example the write of the records to the separator. Unless the owner calls release(), the
// destructor calls keep_on_release(), so that the store does not free a segment whose records the
// index can still point at.
class keep_on_release_guard {
    segment_ref _ref;
public:
    keep_on_release_guard() = default;
    explicit keep_on_release_guard(segment_ref ref) noexcept : _ref(std::move(ref)) {}

    keep_on_release_guard(keep_on_release_guard&&) noexcept = default;
    // The target must be empty. A guard that holds a segment ends with release() or with its
    // destructor, not with an assignment that would keep its segment silently.
    keep_on_release_guard& operator=(keep_on_release_guard&& o) noexcept {
        if (!_ref.empty()) [[unlikely]] {
            on_fatal_internal_error(logstor_logger, fmt::format("Move-assigning to a keep-on-release guard that holds {}", _ref.id()));
        }
        _ref = std::move(o._ref);
        return *this;
    }

    ~keep_on_release_guard() {
        _ref.keep_on_release();
    }

    const segment_ref& ref() const noexcept { return _ref; }

    // Drops the reference without keep_on_release(). Call this when the step completed.
    void release() noexcept { _ref = segment_ref(); }
};

}
