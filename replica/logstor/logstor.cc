/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/logstor.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/log.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/future.hh>
#include "query/query-request.hh"
#include "readers/from_mutations.hh"
#include "keys/keys.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/types.hh"
#include "utils/managed_bytes.hh"
#include <openssl/ripemd.h>
#include <openssl/evp.h>

namespace replica::logstor {

seastar::logger logstor_logger("logstor");

static api::timestamp_type extract_logstor_record_timestamp(const mutation& m) {
    const auto& partition = m.partition();

    for (const auto& row_entry : partition.clustered_rows()) {
        if (row_entry.dummy()) {
            continue;
        }
        if (!row_entry.row().marker().is_missing()) {
            return row_entry.row().marker().timestamp();
        }
    }

    if (const auto partition_tombstone = partition.partition_tombstone(); partition_tombstone) {
        return partition_tombstone.timestamp;
    }

    throw std::runtime_error("logstor mutation has no row marker or partition tombstone timestamp");
}

logstor::logstor(logstor_config config, ::cache_tracker& shared_cache_tracker)
    : _segment_manager(config.segment_manager_cfg)
    , _write_buffer(_segment_manager, config.flush_sg, config.mode == logstor_sync_mode::periodic ? config.sync_period : std::chrono::milliseconds(0), config.max_queued_write_bytes)
    , _cache_tracker(shared_cache_tracker)
    , _mode(config.mode) {

    namespace sm = seastar::metrics;

    _metrics.add_group("logstor", {
        sm::make_gauge("queued_write_count", [this] { return _write_buffer.queued_write_count(); },
                       sm::description("Number of writes currently queued in the write buffer.")),
        sm::make_counter("write_failures", [this] { return _stats.write_failures; },
                       sm::description("Number of writes that failed to be persisted.")),
    });
}

future<> logstor::do_recovery(replica::database& db) {
    co_await _segment_manager.do_recovery(db);
}

future<> logstor::do_recovery_for_test() {
    co_await _segment_manager.do_recovery_for_test();
}

future<> logstor::start() {
    logstor_logger.info("Starting logstor");

    co_await _segment_manager.start();
    co_await _write_buffer.start();

    logstor_logger.info("logstor started");
}

future<> logstor::stop() {
    logstor_logger.info("Stopping logstor");

    co_await _write_buffer.stop();
    co_await _async_gate.close();
    co_await _segment_manager.stop();

    logstor_logger.info("logstor stopped");
}

size_t logstor::get_memory_usage() const {
    return _segment_manager.get_memory_usage();
}

segment_manager& logstor::get_segment_manager() noexcept {
    return _segment_manager;
}

const segment_manager& logstor::get_segment_manager() const noexcept {
    return _segment_manager;
}

compaction_manager& logstor::get_compaction_manager() noexcept {
    return _segment_manager.get_compaction_manager();
}

const compaction_manager& logstor::get_compaction_manager() const noexcept {
    return _segment_manager.get_compaction_manager();
}

future<> logstor::write(const mutation& m, write_target target, db::timeout_clock::time_point timeout) {
    auto& cg = *target.cg;
    primary_index_key key(m.decorated_key());
    table_id table = m.schema()->id();
    auto& index = cg.logstor_index();

    const auto ts = extract_logstor_record_timestamp(m);

    log_record record {
        .header = {
            .key = key,
            .timestamp = ts,
            .table = table,
        },
        .mut = canonical_mutation(m)
    };

    log_record_writer writer(std::move(record));

    if (_mode == logstor_sync_mode::periodic) {
        auto pending = index.insert_pending(key, ts, writer.record().mut);
        if (!pending) {
            co_return;
        }

        auto accounting = _segment_manager.segment_accounting_updater();

        auto holder = _async_gate.hold();
        (void)_write_buffer.write(std::move(writer), timeout, std::move(target))
            .then_unpack([index_ptr = &index, key, generation = *pending, ts, accounting = std::move(accounting)] (log_location location, seastar::gate::holder op) mutable {
                index_ptr->complete_pending_write(key, generation, ts, location, &accounting);
            }).handle_exception([this, index_ptr = &index, key, pending] (std::exception_ptr ep) {
                index_ptr->erase_pending_if_current(key, *pending);
                _stats.write_failures++;
            }).finally([holder = std::move(holder)] {});
        co_return;
    }

    auto accounting = _segment_manager.segment_accounting_updater();

    co_await _write_buffer.write(std::move(writer), timeout, std::move(target)).then_unpack([index_ptr = &index, ts, key = std::move(key), accounting = std::move(accounting)] (log_location location, seastar::gate::holder op) mutable {
        index_entry new_entry {
            .location = location,
            .timestamp = ts,
        };
        index_ptr->insert(key, std::move(new_entry), &accounting);
    }).handle_exception([this] (std::exception_ptr ep) {
        _stats.write_failures++;
        return make_exception_future<>(ep);
    });
}

future<std::optional<mutation>> logstor::read(const schema& s, const primary_index& index, const dht::decorated_key& dk, const query::partition_slice& slice) {
    auto op = index.start_read();

    const auto bypass_cache = slice.options.contains(query::partition_slice::option::bypass_cache);
    auto* cache = bypass_cache ? nullptr : index.cache_tracker();

    auto it = index.find(dk);
    auto pending = index.get_pending(dk);

    if (it == index.end()) {
        if (!pending) {
            co_return std::nullopt;
        }
        co_return pending->mutation.to_mutation(s.shared_from_this());
    }

    if (pending && pending->timestamp >= it->entry().timestamp) {
        co_return pending->mutation.to_mutation(s.shared_from_this());
    }

    // lookup in cache
    if (cache) {
        auto cached_mut = cache->lookup(*it, s.shared_from_this());
        if (cached_mut) {
            co_return std::move(*cached_mut);
        }
    }

    // Cache miss (or bypass): read from disk using the entry we already have.
    // copy the entry. we want to remember the original entry that we use for the read. the entry may change while we read.
    const index_entry entry_for_read = it->entry();
    auto record = co_await _segment_manager.read(entry_for_read.location);

    if (record.mut.key() != dk.key()) [[unlikely]] {
        on_internal_error(logstor_logger, format("Key mismatch reading log entry: expected {}, got {}", dk.key(), record.mut.key()));
    }

    mutation m = record.mut.to_mutation(s.shared_from_this());

    // Populate the cache with the freshly deserialized mutation.
    // Skipped when bypass_cache is set.
    // We must re-find the entry because the iterator may have been invalidated
    // across the co_await above.
    if (cache) {
        auto it = index.find(dk);
        auto pending = index.get_pending(dk);
        if (it != index.end() && it->entry().location == entry_for_read.location && (!pending || pending->timestamp < entry_for_read.timestamp)) {
            cache->populate(*it, m);
        }
    }

    co_return std::move(m);
}

mutation_reader logstor::make_reader(schema_ptr schema, const primary_index& index, reader_permit permit, const dht::partition_range& pr,
        const query::partition_slice& slice, tracing::trace_state_ptr trace_state) {

    class logstor_range_reader : public mutation_reader::impl {
        logstor* _logstor;
        const primary_index& _index;
        dht::partition_range _pr;
        query::partition_slice _slice;
        tracing::trace_state_ptr _trace_state;
        std::optional<dht::decorated_key> _last_key; // owns the key, safe across yields
        mutation_reader_opt _current_partition_reader;
        dht::ring_position_comparator _cmp;

        // Finds the next durable iterator to process, safe to call after any co_await.
        primary_index::partitions_type::const_iterator find_next_durable() const {
            return _last_key
                ? _index.upper_bound(*_last_key)
                : position_at_range_start();
        }

        primary_index::partitions_type::const_iterator position_at_range_start() const {
            if (!_pr.start()) {
                return _index.begin();
            }
            auto it = _index.lower_bound(_pr.start()->value());
            if (!_pr.start()->is_inclusive() && it != _index.end()) {
                if (_cmp(it->key(), _pr.start()->value()) == 0) {
                    ++it;
                }
            }
            return it;
        }

        bool exceeds_range_end(const primary_index_entry& e) const {
            if (!_pr.end()) return false;
            auto c = _cmp(e.key(), _pr.end()->value());
            return _pr.end()->is_inclusive() ? c > 0 : c >= 0;
        }

        std::optional<dht::decorated_key> find_next_pending() const {
            auto it = _last_key
                ? _index.next_pending_in_range(_pr, *_last_key)
                : _index.first_pending_in_range(_pr);
            if (it == _index.pending_end()) {
                return std::nullopt;
            }
            return *it;
        }

        std::optional<dht::decorated_key> find_next_key() const {
            std::optional<dht::decorated_key> durable_key;
            auto durable_it = find_next_durable();
            if (durable_it != _index.end() && !exceeds_range_end(*durable_it)) {
                durable_key = durable_it->key();
            }

            auto pending_key = find_next_pending();
            if (!durable_key) {
                return pending_key;
            }
            if (!pending_key) {
                return durable_key;
            }
            return _cmp(*pending_key, *durable_key) < 0 ? std::move(pending_key) : std::move(durable_key);
        }

    public:
        logstor_range_reader(schema_ptr s, const primary_index& idx, reader_permit p,
                    logstor* ls, dht::partition_range pr,
                    query::partition_slice slice, tracing::trace_state_ptr ts)
            : impl(std::move(s), std::move(p))
            , _logstor(ls), _index(idx), _pr(std::move(pr))
            , _slice(std::move(slice)), _trace_state(std::move(ts))
            , _cmp(*_schema)
        {}

        virtual future<> fill_buffer() override {
            while (!is_buffer_full() && !_end_of_stream) {
                // Drain current partition's reader first
                if (_current_partition_reader) {
                    co_await _current_partition_reader->fill_buffer();
                    _current_partition_reader->move_buffer_content_to(*this);
                    if (!_current_partition_reader->is_end_of_stream()) {
                        continue;
                    }
                    co_await _current_partition_reader->close();
                    _current_partition_reader = std::nullopt;
                    // _last_key was already set when we opened the reader
                }

                // Find next key in range (safe after co_await since we use _last_key)
                auto next_key = find_next_key();
                if (!next_key) {
                    _end_of_stream = true;
                    break;
                }

                // Snapshot the key before yielding
                auto current_key = std::move(*next_key);

                auto guard = reader_permit::awaits_guard(_permit);
                auto mut = co_await _logstor->read(*_schema, _index, current_key, _slice);

                _last_key = current_key; // mark as visited even if not found (tombstoned)

                if (!mut) {
                    continue; // key was removed between index lookup and read
                }

                tracing::trace(_trace_state, "logstor_range_reader: fetched key {}", current_key);

                _current_partition_reader = make_mutation_reader_from_mutations(
                    _schema, _permit, std::move(*mut),
                    _slice, streamed_mutation::forwarding::no
                );
            }
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (!is_buffer_empty()) return make_ready_future<>();
            _end_of_stream = false;
            if (_current_partition_reader) {
                auto fut = _current_partition_reader->close();
                _current_partition_reader = std::nullopt;
                return fut;
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            _pr = pr;
            _last_key = std::nullopt;      // re-position from new range start
            if (_current_partition_reader) {
                auto fut = _current_partition_reader->close();
                _current_partition_reader = std::nullopt;
                return fut;
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(position_range pr) override {
            if (_current_partition_reader) {
                clear_buffer();
                return _current_partition_reader->fast_forward_to(std::move(pr));
            }
            return make_ready_future<>();
        }

        virtual future<> close() noexcept override {
            if (_current_partition_reader) {
                return _current_partition_reader->close();
            }
            return make_ready_future<>();
        }
    };

    return make_mutation_reader<logstor_range_reader>(
        std::move(schema), index, std::move(permit), this, pr, slice, std::move(trace_state)
    );
}

future<> logstor::flush_to_separator() {
    co_await _write_buffer.flush();
    co_await _segment_manager.await_pending_writes();
}

}
