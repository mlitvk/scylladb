/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/logstor.hh"

#include <cstring>

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include "query/query-request.hh"
#include "readers/from_mutations.hh"
#include "keys/keys.hh"
#include "replica/logstor/record_format.hh"
#include "replica/logstor/segment_io.hh"
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
    , _write_buffer(buffered_writer_config{
            .buffer_size = _segment_manager.get_segment_size(),
            .ring_size = config.write_buffer_ring_size,
            .flush_sg = config.flush_sg,
            .max_queued_write_bytes = config.max_queued_write_bytes,
        }, [&sm = _segment_manager] (write_buffer& buf) { return sm.write(buf); })
    , _cache_tracker(shared_cache_tracker) {

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
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping logstor");

    co_await _async_gate.close();
    co_await _write_buffer.stop();
    co_await _segment_manager.stop();

    logstor_logger.info("logstor stopped");
}

size_t logstor::get_memory_usage() const {
    return _segment_manager.get_usage().memory_usage;
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

std::unique_ptr<primary_index> logstor::make_primary_index(schema_ptr schema, bool cache_enabled) {
    auto index = std::make_unique<primary_index>(schema, _segment_manager);
    if (cache_enabled) {
        index->set_cache_tracker(&_cache_tracker);
    }
    return index;
}

// A write the direct path takes is done when it returns - the record is in the group's buffer and in
// the index, with nothing awaited in between - so this is not a coroutine. A write that goes through
// the shared buffer waits twice, in write_through_buffer().
future<> logstor::write(const mutation& m, write_target target, db::timeout_clock::time_point timeout) {
    auto gate_holder = _async_gate.hold();

    auto& cg = *target.cg;
    const auto& dk = m.decorated_key();
    table_id table = m.schema()->id();
    auto& index = cg.logstor_index();

    const auto ts = extract_logstor_record_timestamp(m);

    // The partition is encoded into the buffer the encoder reuses - one walk of the partition,
    // rather than a walk to size an allocation and another to fill it. Nothing is awaited between
    // here and the append below, so no other write can take that buffer.
    const auto encoded = _encoder.encode(*m.schema(), m.partition(), ts);

    // A group that writes fast enough has a buffer of its own, already bound to one of its
    // segments, so the record goes straight in and its final location is known here. The write is
    // acknowledged once the record is in that buffer and in the index - it reaches the disk when
    // the buffer is written out, which is what the periodic sync mode promises. Nothing is awaited
    // between the two, so the record is visible to a reader and to a drain from the moment the
    // buffer takes it, and the gate holders of the write target are released by returning.
    //
    // Because the record is serialized before this returns and is never retained, nothing of it has
    // to be owned: the key stays the mutation's and the value the encoder's, so a direct write
    // copies neither.
    const auto header = log_record_header_view {
        .token = dk.token(),
        .timestamp = ts,
        .table = table,
        .key = managed_bytes_view(dk.key().representation()),
    };

    const auto direct = _segment_manager.try_write_direct(cg, header, encoded);
    if (direct.location) {
        index.insert(primary_index_key(dk), index_entry{.location = *direct.location, .timestamp = ts});
        return make_ready_future<>();
    }

    // A record that is not taken here has to own itself, whichever of the two ways below it goes:
    // the shared buffer needs it because the separator replays a retained copy after the buffer it
    // was appended to has been reset, and a record that waits for the group's flush needs it
    // because neither the encoder's buffer nor the mutation's key will still be its own by then.
    // The exact bytes are copied out of the encoder's buffer, which is still the one holding this
    // partition.
    bytes value(bytes::initialized_later(), encoded.size());
    std::memcpy(value.data(), encoded.data(), encoded.size());
    primary_index_key key(dk);

    if (direct.retry_after_flush) {
        return write_direct_after_flush(std::move(key), std::move(value), ts, table, index,
                std::move(target), timeout, std::move(gate_holder));
    }

    log_record record {
        .header = {
            .key = key,
            .timestamp = ts,
            .table = table,
        },
        .value = row_value{std::move(value)},
    };

    // The key is moved rather than copied: what the record carries is a copy of its own, made above.
    return write_through_buffer(log_record_writer(std::move(record)), std::move(key), ts, index,
            std::move(target), timeout, std::move(gate_holder));
}

future<> logstor::write_direct_after_flush(primary_index_key key, bytes value, api::timestamp_type ts,
        table_id table, primary_index& index, write_target target,
        db::timeout_clock::time_point timeout, seastar::gate::holder gate_holder) {
    auto& cg = *target.cg;

    // The write target is held across this, so the group is still there to be written into when the
    // wait is over. A failure here is the timeout running out; the ordinary path below is given the
    // same timeout and is what reports it.
    auto waited = co_await coroutine::as_future(cg.await_direct_flush(timeout));
    if (!waited.failed()) {
        const auto header = log_record_header_view {
            .token = key.dk.token(),
            .timestamp = ts,
            .table = table,
            .key = managed_bytes_view(key.dk.key().representation()),
        };
        // Offered once more, and not asked to wait again: the group may have rotated for another
        // write while this one waited, or lost its buffers altogether.
        const auto direct = _segment_manager.try_write_direct(cg, header, value,
                direct_write_attempt::after_flush);
        if (direct.location) {
            index.insert(std::move(key), index_entry{.location = *direct.location, .timestamp = ts});
            co_return;
        }
    } else {
        waited.ignore_ready_future();
    }

    log_record record {
        .header = {
            .key = key,
            .timestamp = ts,
            .table = table,
        },
        .value = row_value{std::move(value)},
    };

    co_await write_through_buffer(log_record_writer(std::move(record)), std::move(key), ts, index,
            std::move(target), timeout, std::move(gate_holder));
}

future<> logstor::write_through_buffer(log_record_writer writer, primary_index_key key, api::timestamp_type ts,
        primary_index& index, write_target target, db::timeout_clock::time_point timeout,
        seastar::gate::holder gate_holder) {
    // Two waits, not one: first for the record to be taken into a buffer, then for that buffer to
    // reach a segment. They are awaited here rather than behind a call of their own, which would
    // cost a coroutine frame per write to do nothing else.
    auto accepted_f = co_await coroutine::as_future(_write_buffer.write_to_buffer(std::move(writer), timeout, std::move(target)));
    if (accepted_f.failed()) {
        _stats.write_failures++;
        co_await coroutine::return_exception_ptr(accepted_f.get_exception());
    }
    auto persisted_f = co_await coroutine::as_future(std::move(accepted_f.get().persisted));
    if (persisted_f.failed()) {
        _stats.write_failures++;
        co_await coroutine::return_exception_ptr(persisted_f.get_exception());
    }
    auto [location, op] = persisted_f.get();
    index_entry new_entry {
        .location = location,
        .timestamp = ts,
    };
    index.insert(key, std::move(new_entry));
}

// A read that the index and the cache can answer between them - a key the index does not hold, or a
// partition the cache holds - waits for nothing, so it is answered here rather than from a
// coroutine, which would allocate a frame per read to suspend in nowhere. Only a read that goes to
// a segment pays for one, in read_from_segment().
future<std::optional<mutation>> logstor::read(const schema& s, const primary_index& index, const dht::decorated_key& dk, const query::partition_slice& slice) {
    auto gate_holder = _async_gate.hold();

    auto op = index.start_read();

    const auto bypass_cache = slice.options.contains(query::partition_slice::option::bypass_cache);
    auto* cache = bypass_cache ? nullptr : index.cache_tracker();
    cache_tracker::read_accounter read_acc(cache);

    auto it = index.find(dk);
    if (it == index.end()) {
        if (cache) {
            cache->on_negative_hit();
        }
        return make_ready_future<std::optional<mutation>>(std::nullopt);
    }

    // lookup in cache
    if (cache) {
        auto cached_mut = cache->lookup(*it, s);
        if (cached_mut) {
            return make_ready_future<std::optional<mutation>>(std::move(cached_mut));
        }
        read_acc.on_miss();
    }

    // Cache miss (or bypass): read from disk using the entry we already have. The entry is copied
    // because it may change while the record is read.
    return read_from_segment(s, index, dk, it->entry(), cache, std::move(gate_holder), std::move(op), std::move(read_acc));
}

future<std::optional<mutation>> logstor::read_from_segment(const schema& s, const primary_index& index,
        const dht::decorated_key& dk, const index_entry entry_for_read, cache_tracker* cache,
        seastar::gate::holder gate_holder, utils::phased_barrier::operation op,
        cache_tracker::read_accounter read_acc) {
    auto buf = co_await _segment_manager.read_record_bytes(entry_for_read.location);
    const auto record = view_log_record(bytes_view(reinterpret_cast<const int8_t*>(buf.get()), buf.size()));

    // The header holds the key of the record, which is the key of this read, so it is compared
    // rather than deserialized: both sides are the compound blob the key already is.
    const auto record_key = ondisk::log_record_header_key(record.header);
    if (managed_bytes_view(record_key) != managed_bytes_view(dk.key().representation())) [[unlikely]] {
        on_internal_error(logstor_logger, format("Key mismatch reading log entry: expected {}, got {}",
                dk.key(), partition_key::from_bytes(record_key)));
    }

    // The timestamp of the record is the base its value's timestamps are deltas from, and it is
    // the only other field of the header a read needs.
    mutation m(s.shared_from_this(), dk);
    read_row_value_into(m.partition(), s, record.data, ondisk::log_record_header_timestamp(record.header),
            index.translations());

    // Populate the cache with the freshly deserialized mutation.
    // Skipped when bypass_cache is set.
    // We must re-find the entry because the iterator may have been invalidated
    // across the co_await above.
    if (cache) {
        // Counted before the populate below, so that a read which ends up not populating the cache
        // still counts the rows it had to fetch from the disk.
        cache->on_rows_missed(m.partition());
        auto it = index.find(dk);
        if (it != index.end() && it->entry().location == entry_for_read.location) {
            cache->populate(*it, m);
        } else {
            // The entry changed or went away across the co_await, which makes what was read stale.
            cache->on_mispopulate();
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

        // Finds the next iterator to process, safe to call after any co_await
        primary_index::partitions_type::const_iterator find_next() const {
            auto it = _last_key
                ? _index.upper_bound(*_last_key)                        // strictly after last key
                : position_at_range_start();                            // initial positioning
            // If start was exclusive and we haven't yet seen a key
            return it;
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
                auto it = find_next();
                if (it == _index.end() || exceeds_range_end(*it)) {
                    _end_of_stream = true;
                    break;
                }

                // Snapshot the key before yielding
                auto current_key = it->key();

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
    auto gate_holder = _async_gate.hold();
    co_await _write_buffer.flush();
    co_await _segment_manager.await_pending_writes();
}

}
