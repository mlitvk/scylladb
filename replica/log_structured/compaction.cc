/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "segment_manager.hh"
#include "compaction.hh"
#include "index.hh"
#include "logstor.hh"
#include "replica/log_structured/write_buffer.hh"
#include <seastar/core/when_all.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/sleep.hh>
#include "serializer_impl.hh"
#include "idl/log_structured.dist.hh"
#include "idl/log_structured.dist.impl.hh"

namespace replica::log_structured {

compaction_manager::compaction_manager(segment_manager& seg_mgr, log_index& index,
                             compaction_config config)
    : _sm(seg_mgr)
    , _index(index)
    , _config(std::move(config))
    , _compaction_action([this] {
        return compact();
    }) {

    namespace sm = seastar::metrics;

    _metrics.add_group("logstor_compaction", {
        sm::make_counter("bytes_written", _stats.bytes_written,
                       sm::description("Counts number of bytes written to the disk.")),
        sm::make_counter("compaction_started", _stats.compaction_started,
                       sm::description("Counts number of compaction operations started.")),
        sm::make_counter("segments_compacted", _stats.segments_compacted,
                       sm::description("Counts number of segments compacted.")),
        sm::make_counter("records_rewritten", _stats.records_rewritten,
                       sm::description("Counts number of records rewritten during compaction.")),
        sm::make_counter("records_skipped", _stats.records_skipped,
                       sm::description("Counts number of records skipped during compaction.")),
    });
}

future<> compaction_manager::start() {
    logstor_logger.info("Starting compaction manager (min_live_ratio={}, interval={}ms)",
                       _config.min_live_ratio, _config.compaction_interval.count());
    _running = true;

    _sm.set_compaction_trigger([this] {
        logstor_logger.debug("Compaction triggered by segment manager");
        return _compaction_action.trigger();
    });

    co_return;
}

future<> compaction_manager::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }

    logstor_logger.info("Stopping compaction manager");
    _running = false;

    co_await _async_gate.close();
    co_await _compaction_action.join();

    logstor_logger.info("compaction manager stopped");
}

void compaction_manager::enable_auto_compaction() {
    logstor_logger.info("Enabling automatic compaction");
    _config.compaction_enabled = true;
}

future<> compaction_manager::disable_auto_compaction() {
    logstor_logger.info("Disabling automatic compaction");
    _config.compaction_enabled = false;
    co_await _compaction_action.join();
}

future<> compaction_manager::trigger_compaction() {
    logstor_logger.debug("Manual compaction triggered");
    return _compaction_action.trigger();
}

future<> compaction_manager::compact() {
    if (!_config.compaction_enabled) {
        logstor_logger.debug("Compaction is disabled, skipping");
        co_return;
    }

    auto candidates = _sm.find_segments_for_compaction(_config.min_live_ratio, _config.max_segments_per_compaction);
    if (candidates.size() < 2) {
        logstor_logger.debug("Not enough segments for compaction");
        co_return;
    }

    auto holder = _async_gate.hold();

    logstor_logger.info("Starting compaction of {} segments", candidates.size());
    co_await with_scheduling_group(_config.compaction_sg, [this, candidates = std::move(candidates)] mutable {
        return compact_segments(std::move(candidates));
    });

    (void)_compaction_action.trigger_later();
}

future<> compaction_manager::compact_segments(std::vector<log_segment_id> segments) {
    _stats.compaction_started++;

    size_t records_rewritten = 0;
    size_t records_skipped = 0;

    // Store all pending write futures to wait for after flush
    std::vector<future<>> pending_writes;

    write_buffer compaction_buffer(_sm.get_segment_size());

    co_await _sm.for_each_record(segments,
        [this, &records_rewritten, &records_skipped, &pending_writes, &compaction_buffer]
        (log_location read_location, log_structured_segment_record record) -> future<> {

        // Check if this record is still current in the index
        auto index_entry_opt = _index.get(record.key);

        if (index_entry_opt) {
            logstor_logger.trace("Compacting record with key {} at location {} last location {}",
                                record.key, read_location, index_entry_opt->location);
        } else {
            logstor_logger.trace("Compacting record with key {} at location {} not found in index",
                                record.key, read_location);
        }

        bool is_live = index_entry_opt.has_value() &&
                       index_entry_opt->location == read_location;

        if (!is_live) {
            records_skipped++;
            _stats.records_skipped++;
            co_return;
        }

        // Serialize the record
        seastar::measuring_output_stream ms;
        ser::serialize(ms, record);
        size_t record_size = ms.size();

        // If record doesn't fit in buffer, flush first and wait for pending writes
        if (!compaction_buffer.has_space(record_size)) {
            co_await flush_compaction_buffer(compaction_buffer);
            co_await when_all_succeed(pending_writes.begin(), pending_writes.end());
            pending_writes.clear();
        }

        // Write to compaction buffer
        auto write_future = compaction_buffer.write([&] (auto& out) {
            ser::serialize(out, record);
        }, record_size);

        // Atomically update index only if it still points to the old location
        // We'll resolve the location after flushing the buffer
        auto old_entry = *index_entry_opt;

        // Create a continuation that will be called after flush
        auto write_and_update_index = write_future.then([this, key = record.key, old_entry, &records_rewritten, &records_skipped]
                                        (log_location new_location) {

            auto new_entry = old_entry;
            new_entry.location = new_location;

            bool exchanged = _index.compare_exchange(key, old_entry, std::move(new_entry));

            if (!exchanged) {
                // Lost the race - another write updated this key
                _sm.free_record(new_location);
                records_skipped++;
                _stats.records_skipped++;
            } else {
                logstor_logger.trace("Moved record with key {} from {} to {}",
                                    key, old_entry.location, new_location);
                records_rewritten++;
                _stats.records_rewritten++;
            }
        });

        // Store the future to wait for later
        pending_writes.push_back(std::move(write_and_update_index));
    });

    // Flush any remaining buffered records
    if (compaction_buffer.has_data()) {
        co_await flush_compaction_buffer(compaction_buffer);
    }

    // Wait for all pending index updates to complete
    co_await when_all_succeed(pending_writes.begin(), pending_writes.end());

    logstor_logger.info("Compaction complete: {} records rewritten, {} skipped from {} segments",
                       records_rewritten, records_skipped, segments.size());

    // Free the compacted segments
    for (auto segment_id : segments) {
        co_await _sm.free_segment(segment_id);
    }
    _stats.segments_compacted += segments.size();
}

future<> compaction_manager::flush_compaction_buffer(write_buffer& compaction_buffer) {
    if (!compaction_buffer.has_data()) {
        co_return;
    }

    logstor_logger.debug("Flushing compaction buffer ({} bytes, {} writes)",
                        compaction_buffer.offset_in_buffer(), compaction_buffer.num_writes());

    // Finalize buffer header
    compaction_buffer.finalize();
    compaction_buffer.pad_to_alignment(_sm.get_write_alignment());

    // Write to segment manager
    const auto write_size = compaction_buffer.offset_in_buffer();
    bytes_view data(reinterpret_cast<const int8_t*>(compaction_buffer.data()), write_size);
    auto base_location = co_await _sm.write(data, compaction_buffer.total_data_size);
    _stats.bytes_written += write_size;

    logstor_logger.debug("Compaction buffer flushed to segment {} at offset {}",
                        base_location.segment, base_location.offset);

    // Complete all buffered write promises with their individual locations
    compaction_buffer.complete_writes(base_location);
}

}
