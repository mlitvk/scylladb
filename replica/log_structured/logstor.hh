/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <optional>
#include <filesystem>
#include "readers/mutation_reader.hh"
#include <seastar/core/scheduling.hh>
#include "types.hh"
#include "index.hh"
#include "segment_manager.hh"
#include "write_buffer.hh"
#include "compaction.hh"
#include "mutation/mutation.hh"
#include "dht/decorated_key.hh"
#include "tracing/tracing.hh"

namespace replica {
namespace log_structured {

extern seastar::logger logstor_logger;

/// Configuration for the log structured storage engine
struct logstor_config {
    std::filesystem::path base_dir;
    compaction_config compaction_cfg;
    seastar::scheduling_group flush_sg;
};

/// Log structured storage engine
class logstor {
private:
    logstor_config _config;

    log_index _index;
    segment_manager _segment_manager;
    buffered_writer _write_buffer;
    compaction_manager _compaction_manager;

public:
    /// Constructor that takes configuration parameters
    explicit logstor(logstor_config config);

    logstor(const logstor&) = delete;
    logstor& operator=(const logstor&) = delete;

    future<> start();
    future<> stop();

    void enable_auto_compaction();
    future<> disable_auto_compaction();

    static index_key calculate_key(const schema&, const dht::decorated_key&);

    /// Write a mutation to the log structured storage
    future<> write(const mutation& m);

    /// Read a record from the log structured storage
    future<std::optional<log_structured_segment_record>> read(index_key key);

    /// Read a mutation from the log structured storage
    future<std::optional<canonical_mutation>> read(const schema&, const dht::decorated_key& key);

};

} // namespace log_structured
} // namespace replica
