/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <memory>
#include <vector>

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include "replica/logstor/index_update.hh"

namespace replica {

namespace logstor {

class write_buffer_pool;

// Gives a borrowed rewrite_buffer back to its pool, along with the pool unit acquired for it.
class write_buffer_returner {
    write_buffer_pool* _pool = nullptr;
    seastar::semaphore_units<> _units;

public:
    write_buffer_returner() = default;
    write_buffer_returner(write_buffer_pool& pool, seastar::semaphore_units<> units) noexcept
        : _pool(&pool), _units(std::move(units)) {}

    void operator()(rewrite_buffer* wb) noexcept;
};

// A rewrite_buffer borrowed from a write_buffer_pool. reset() and destruction give it back, so the
// accounting of the pool holds on every path, also when the owner throws.
using owned_write_buffer = std::unique_ptr<rewrite_buffer, write_buffer_returner>;

// A pool of rewrite_buffers. It hands out at most `capacity` buffers at once, builds them on demand,
// and keeps up to `max_cached` of them for reuse. The owner must call stop() after all users stop.
class write_buffer_pool {
public:
    struct config {
        // The most buffers the pool may have out at once.
        size_t capacity{0};
        size_t buffer_size{0};
        // Buffers built at construction rather than at the point of use. Must not exceed max_cached,
        // or they would not all be kept.
        size_t preallocate{0};
        // The most returned buffers kept for reuse. Must not exceed capacity.
        size_t max_cached{0};
    };

    struct stats {
        uint64_t allocation_waits{0};
        uint64_t buffers_dropped{0};
        uint64_t buffers_created{0};
    };

private:
    size_t _buffer_size;
    size_t _max_cached;
    size_t _capacity;

    // Buffers ready to be handed out. Never longer than _max_cached, and reserved for it up front,
    // so that returning a buffer to it never allocates - it has to be infallible, being the last
    // step of an owner's destructor.
    std::vector<std::unique_ptr<rewrite_buffer>> _free;
    // Buffers that are currently out with an owner.
    size_t _in_use{0};

    seastar::semaphore _available_sem;
    bool _stopped{false};
    stats _stats;

public:

    explicit write_buffer_pool(config);

    future<> stop();

    future<owned_write_buffer> allocate(abort_source& as);
    future<std::vector<owned_write_buffer>> allocate_many(size_t count, abort_source& as);

    void return_buffer(rewrite_buffer* wb, seastar::semaphore_units<> pool_units) noexcept;

    // Changes the number of buffers the pool may have out at once. Lowering it does not take back
    // any of the buffers that are already out - it only makes the pool hand out that many fewer
    // before the returns catch up - but it does free the ones it no longer needs to keep.
    void set_capacity(size_t capacity);

    size_t capacity() const noexcept { return _capacity; }

    size_t memory_usage() const noexcept { return allocated_buffer_count() * _buffer_size; }

    // Buffers that are currently out with an owner.
    size_t used_buffer_count() const noexcept { return _in_use; }

    // Buffers that exist, and therefore the memory the pool holds.
    size_t allocated_buffer_count() const noexcept {
        return _in_use + _free.size();
    }

    const stats& get_stats() const noexcept {
        return _stats;
    }

private:
    bool would_wait(size_t count) const noexcept {
        return _available_sem.available_units() < static_cast<ssize_t>(count) || _available_sem.waiters() > 0;
    }
};

}
}
