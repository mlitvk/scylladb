/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "replica/log_structured/logstor.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include <seastar/core/future.hh>
#include "readers/from_mutations.hh"

namespace replica {
namespace log_structured {

seastar::logger logstor_logger("logstor");

logstor::logstor(logstor_config config)
    : _config(std::move(config))
    , _index()
    , _segment_manager(segment_manager_config{
        .base_dir = _config.base_dir,
        .sched_group = _config.flush_sg,
    })
    , _write_buffer(_segment_manager, _config.flush_sg)
    , _compaction_manager(_segment_manager, _index, _config.compaction_cfg) {
}

future<> logstor::start() {
    logstor_logger.info("Starting log structured storage at {}", _config.base_dir.string());

    co_await _segment_manager.start();
    co_await _write_buffer.start();
    co_await _compaction_manager.start();

    logstor_logger.info("Log structured storage started");
}

future<> logstor::stop() {
    logstor_logger.info("Stopping log structured storage");

    co_await _compaction_manager.stop();
    co_await _write_buffer.stop();
    co_await _segment_manager.stop();

    logstor_logger.info("Log structured storage stopped");
}

void logstor::enable_auto_compaction() {
    _compaction_manager.enable_auto_compaction();
}

future<> logstor::disable_auto_compaction() {
    return _compaction_manager.disable_auto_compaction();
}

future<> logstor::write(const mutation& m) {
    logstor_logger.trace("Writing mutation with key {}", m.decorated_key());
    auto key = calculate_key(*m.schema(), m.decorated_key());

    log_structured_segment_record record {
        .key = key,
        .mut = canonical_mutation(m)
    };

    return _write_buffer.write(std::move(record)).then([this, key = std::move(key)] (log_location location) {
        logstor_logger.trace("Mutation with key {} written to {}", key, location);

        // Update index
        index_entry new_entry {
            .location = location,
        };
        auto old_entry = _index.exchange(key, std::move(new_entry));

        // If overwriting, free old record
        if (old_entry) {
            _segment_manager.free_record(old_entry->location);
        }
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.error("Error writing log structured mutation: {}", ep);
        return make_exception_future<>(ep);
    });
}

future<std::optional<log_structured_segment_record>> logstor::read(index_key key) {
    // First, look up the key in the index
    logstor_logger.trace("Reading mutation with key {}", key);
    auto entry_opt = _index.get(key);
    if (!entry_opt.has_value()) {
        logstor_logger.trace("Key {} not found in log index", key);
        return make_ready_future<std::optional<log_structured_segment_record>>(std::nullopt);
    }

    const auto& entry = *entry_opt;
    logstor_logger.trace("Found log entry for key {} at {}", key, entry.location);

    // Read the full entry from the log
    return _segment_manager.read(entry.location).then([] (log_structured_segment_record record) {
        return std::optional<log_structured_segment_record>(std::move(record));
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.error("Error reading log structured record: {}", ep);
        return make_exception_future<std::optional<log_structured_segment_record>>(ep);
    });
}

future<std::optional<canonical_mutation>> logstor::read(const schema& s, const dht::decorated_key& key) {
    return read(calculate_key(s, key)).then([&key] (std::optional<log_structured_segment_record> record_opt) -> std::optional<canonical_mutation> {
        if (!record_opt.has_value()) {
            return std::nullopt;
        }

        const auto& record = *record_opt;

        if (record.mut.key() != key.key()) {
            throw std::runtime_error(fmt::format(
                "Key mismatch reading log entry: expected {}, got {}",
                key.key(), record.mut.key()
            ));
        }

        return std::optional<canonical_mutation>(std::move(record.mut));
    });
}

index_key logstor::calculate_key(const schema& s, const dht::decorated_key& key) {
    // hash of (ks name, table name, partition key)
    return index_key {
        utils::hash_combine(
            dht::token::to_int64(key.token()),
            utils::hash_combine(
                std::hash<sstring>()(s.ks_name()),
                std::hash<sstring>()(s.cf_name())
            )
        )
    };
}

}
}