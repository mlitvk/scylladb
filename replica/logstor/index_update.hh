/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

// The completions that logstor gives its write buffers. They update the index once the records are
// in a segment.

#include <cstddef>
#include <exception>

#include <seastar/core/future.hh>

#include "mutation/timestamp.hh"
#include "replica/logstor/segment_types.hh"
#include "replica/logstor/types.hh"
#include "replica/logstor/write_buffer.hh"

namespace replica::logstor {

class primary_index;

// Inserts a user write into the index once its record is in a segment, and then resolves `done`.
// A write whose chunk is not written, or whose insert fails, fails `done`.
struct index_insert {
    primary_index* index;
    primary_index_key key;
    api::timestamp_type timestamp;
    promise<> done;

    void on_written(record_location location) noexcept;
    void on_failed(std::exception_ptr ex) noexcept;
};

// Points the index entry of a rewritten record at its new location, unless a newer write replaced
// the entry first. `relocated` counts the entries that move, when it is set.
struct index_relocation {
    primary_index* index;
    primary_index_key key;
    record_location old_location;
    size_t* relocated = nullptr;

    void on_written(record_location new_location) noexcept;
    // The entry keeps the old location, where the record still is.
    void on_failed(std::exception_ptr) noexcept {}
};

using user_write_buffer = mixed_write_buffer<index_insert>;

using rewrite_buffer = full_write_buffer<index_relocation>;

}
