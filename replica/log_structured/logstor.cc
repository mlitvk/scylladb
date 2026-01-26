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
#include "keys/keys.hh"
#include "rmd160.hh"

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

mutation_reader logstor::make_reader_for_key(schema_ptr schema,
                                            reader_permit permit,
                                            const dht::decorated_key& key,
                                            const query::partition_slice& slice,
                                            tracing::trace_state_ptr trace_state) {

    // Create a simple reader that reads the mutation and delegates to the existing infrastructure
    class single_key_reader : public mutation_reader::impl {
    private:
        logstor* _logstor;
        dht::decorated_key _key;
        query::partition_slice _slice;
        tracing::trace_state_ptr _trace_state;
        mutation_reader_opt _delegate_reader;

    public:
        single_key_reader(schema_ptr schema,
                         reader_permit permit,
                         logstor* logstor,
                         dht::decorated_key key,
                         query::partition_slice slice,
                         tracing::trace_state_ptr trace_state)
            : impl(std::move(schema), std::move(permit))
            , _logstor(logstor)
            , _key(std::move(key))
            , _slice(std::move(slice))
            , _trace_state(std::move(trace_state)) {
        }

        virtual future<> fill_buffer() override {
            if (_end_of_stream) {
                co_return;
            }

            // Create delegate reader if not already created
            if (!_delegate_reader) {
                // Read the mutation from log-structured storage
                auto cmut = co_await _logstor->read(*_schema, _key);

                if (!cmut.has_value()) {
                    // Key not found - end of stream
                    _end_of_stream = true;
                    co_return;
                }

                tracing::trace(_trace_state, "Retrieved log-structured mutation for key {}", _key);

                // Create a reader from the mutation using existing infrastructure
                _delegate_reader = make_mutation_reader_from_mutations(
                    _schema,
                    _permit,
                    cmut->to_mutation(_schema),
                    _slice,
                    streamed_mutation::forwarding::no
                );
            }

            // Delegate to the mutation reader
            co_await _delegate_reader->fill_buffer();
            _delegate_reader->move_buffer_content_to(*this);
            _end_of_stream = _delegate_reader->is_end_of_stream();
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            _end_of_stream = true;
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            // Single key reader doesn't support range forwarding beyond the current key
            if (!pr.contains(_key, dht::ring_position_comparator(*_schema))) {
                _end_of_stream = true;
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(position_range pr) override {
            // Delegate position forwarding to the underlying reader if it exists
            if (_delegate_reader) {
                clear_buffer();
                return _delegate_reader->fast_forward_to(std::move(pr));
            }
            return make_ready_future<>();
        }

        virtual future<> close() noexcept override {
            if (_delegate_reader) {
                return _delegate_reader->close();
            }
            return make_ready_future<>();
        }
    };

    return make_mutation_reader<single_key_reader>(
        schema,
        std::move(permit),
        this,
        key,
        slice,
        std::move(trace_state)
    );
}

index_key logstor::calculate_key(const schema& s, const dht::decorated_key& key) {
    // hash of (ks name, table name, partition key)

    dword MDbuf[5];
    MDinit(MDbuf);

    dword X[16];

    auto hash_bytes_view = [&](bytes_view bv) {
        const byte* data = reinterpret_cast<const byte*>(bv.data());
        size_t len = bv.size();

        while (len >= 64) {
            for (unsigned i = 0; i < 16; i++) {
                X[i] = BYTES_TO_DWORD(data + 4 * i);
            }
            compress(MDbuf, X);
            data += 64;
            len -= 64;
        }
        return std::pair{data, len};
    };

    // Hash keyspace name
    auto ks_bytes = to_bytes(s.ks_name());
    auto [ks_data, ks_len] = hash_bytes_view(ks_bytes);

    // Hash table name
    auto cf_bytes = to_bytes(s.cf_name());
    auto [cf_data, cf_len] = hash_bytes_view(cf_bytes);

    // Hash partition key
    std::pair<const byte*, size_t> key_remaining;
    with_linearized(managed_bytes_view(key.key()), [&](bytes_view bv) {
        key_remaining = hash_bytes_view(bv);
    });

    // Combine remaining bytes and finalize
    size_t total_remaining = ks_len + cf_len + key_remaining.second;
    std::vector<byte> remaining;
    remaining.reserve(total_remaining);
    remaining.insert(remaining.end(), ks_data, ks_data + ks_len);
    remaining.insert(remaining.end(), cf_data, cf_data + cf_len);
    remaining.insert(remaining.end(), key_remaining.first, key_remaining.first + key_remaining.second);

    MDfinish(MDbuf, remaining.data(), total_remaining, 0);

    // Extract digest
    index_key result;
    for (size_t i = 0; i < 5; i++) {
        result.digest[4*i + 0] = static_cast<uint8_t>(MDbuf[i]);
        result.digest[4*i + 1] = static_cast<uint8_t>(MDbuf[i] >> 8);
        result.digest[4*i + 2] = static_cast<uint8_t>(MDbuf[i] >> 16);
        result.digest[4*i + 3] = static_cast<uint8_t>(MDbuf[i] >> 24);
    }

    return result;
}

}
}