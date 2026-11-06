/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/write_buffer_pool.hh"

#include <seastar/core/on_internal_error.hh>

#include "replica/logstor/logstor.hh"

namespace replica::logstor {

void write_buffer_returner::operator()(rewrite_buffer* wb) noexcept {
    _pool->return_buffer(wb, std::move(_units));
}

write_buffer_pool::write_buffer_pool(config cfg)
    : _buffer_size(cfg.buffer_size)
    , _max_cached(cfg.max_cached)
    , _capacity(cfg.capacity)
    , _available_sem(cfg.capacity)
{
    if (cfg.preallocate > cfg.max_cached || cfg.max_cached > cfg.capacity) {
        on_internal_error(logstor_logger, fmt::format(
                "Invalid logstor write buffer pool configuration: capacity {}, preallocate {}, max cached {}",
                cfg.capacity, cfg.preallocate, cfg.max_cached));
    }
    _free.reserve(_max_cached);
    for (size_t i = 0; i < cfg.preallocate; ++i) {
        _free.push_back(std::make_unique<rewrite_buffer>(_buffer_size));
        ++_stats.buffers_created;
    }
}

future<owned_write_buffer> write_buffer_pool::allocate(abort_source& as) {
    auto buffers = co_await allocate_many(1, as);
    co_return std::move(buffers[0]);
}

future<std::vector<owned_write_buffer>> write_buffer_pool::allocate_many(size_t count, abort_source& as) {
    if (count > _capacity) {
        on_internal_error(logstor_logger, fmt::format("Cannot allocate {} logstor write buffers from a pool of capacity {}", count, _capacity));
    }

    if (would_wait(count)) {
        ++_stats.allocation_waits;
    }
    auto units = co_await get_units(_available_sem, count, as);

    // Take the cached buffers and build the ones that are missing before handing any of them out,
    // so that failing to build one leaves the pool as it was and signals the units back.
    std::vector<std::unique_ptr<rewrite_buffer>> bufs;
    bufs.reserve(count);
    while (bufs.size() < count) {
        if (!_free.empty()) {
            bufs.push_back(std::move(_free.back()));
            _free.pop_back();
        } else {
            bufs.push_back(std::make_unique<rewrite_buffer>(_buffer_size));
            ++_stats.buffers_created;
        }
    }

    std::vector<owned_write_buffer> buffers;
    buffers.reserve(count);
    for (auto& buf : bufs) {
        buffers.emplace_back(buf.release(), write_buffer_returner(*this, units.split(1)));
        ++_in_use;
    }
    co_return buffers;
}

void write_buffer_pool::return_buffer(rewrite_buffer* wb, seastar::semaphore_units<> pool_units) noexcept {
    // Take the buffer back, so that it is destroyed when this returns unless it is kept for reuse.
    auto buf = std::unique_ptr<rewrite_buffer>(wb);
    --_in_use;

    // Hold on to the buffer only as long as the pool is meant to keep one - the units are signalled
    // back either way, so a buffer that is let go here is simply built again when it is needed.
    if (_stopped || _free.size() >= _max_cached || _in_use + _free.size() >= _capacity) {
        return;
    }

    try {
        buf->reset();
        _free.push_back(std::move(buf));
    } catch (...) {
        // The buffer is destroyed when this returns, and the units go back with it, so the pool
        // builds a new buffer when it needs one.
        ++_stats.buffers_dropped;
        logstor_logger.error("Failed to return a write buffer, dropping it from the pool: {:t}", std::current_exception());
    }
}

void write_buffer_pool::set_capacity(size_t capacity) {
    if (capacity == _capacity) {
        return;
    }

    if (capacity > _capacity) {
        _available_sem.signal(capacity - _capacity);
    } else {
        // consume() does not wait: it drives the available units negative when the capacity is
        // lowered while the buffers it stood for are still out, and the returns absorb it.
        _available_sem.consume(_capacity - capacity);
        // Give back the memory the pool no longer needs to hold.
        while (!_free.empty() && _in_use + _free.size() > capacity) {
            _free.pop_back();
        }
    }

    _capacity = capacity;
}

future<> write_buffer_pool::stop() {
    if (_stopped) {
        co_return;
    }
    _stopped = true;
    // Fail pending and future allocations, so that nothing can start using a buffer of a pool that
    // is being stopped.
    _available_sem.broken();
    // A buffer that is still out here is one that its owner never gave back.
    if (used_buffer_count() != 0) {
        on_internal_error_noexcept(logstor_logger, fmt::format(
                "Stopping logstor write buffer pool with {} of {} buffers still in use",
                used_buffer_count(), _capacity));
    }
    _free.clear();
}

}
