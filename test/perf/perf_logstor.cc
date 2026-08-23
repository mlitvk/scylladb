/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// A CPU oriented micro benchmark of the logstor hot path. It drives a logstor instance directly,
// without the CQL, coordinator and replica layers that perf-simple-query also measures, and it
// measures the steps of a read and of a write both together and one at a time, so that the cost of
// a change can be attributed to the step it changed.
//
// The number to compare between runs is instructions or cycles per operation: the throughput of
// anything that touches the disk is bounded by the disk, and of the pure CPU tests by whatever else
// the machine is doing. Run it on a single shard pinned to a core, with the data directory on the
// kind of disk the numbers are meant to describe:
//
//   TMPDIR=/nvme taskset -c 2 build/release/test/perf/perf_logstor --smp 1 --test all

#include <filesystem>
#include <ranges>
#include <vector>

#include <fmt/ranges.h>
#include <json/json.h>

#include <seastar/core/align.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_runner.hh>

#include "dht/i_partitioner.hh"
#include "keys/keys.hh"
#include "mutation/canonical_mutation.hh"
#include "mutation/mutation.hh"
#include "partition_slice_builder.hh"
#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/write_buffer.hh"
#include "schema/schema_builder.hh"
#include "test/lib/logstor_test_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/tmpdir.hh"
#include "test/perf/perf.hh"
#include "types/types.hh"

// The record header is serialized through the same generated code the write path uses, so that what
// the size of a record costs to compute here is what it costs there.
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "serializer_impl.hh"

using namespace replica::logstor;

namespace {

// What one operation of a test does. The first tests are the steps of a read and of a write with no
// IO in them at all, which is where a CPU regression shows up with the least noise; the ones after
// them add the index, the disk and the caches back, up to a whole read and a whole write.
enum class test_kind {
    index_lookup,
    index_insert,
    deserialize,
    materialize,
    build_mutation,
    freeze,
    record_header,
    record_sizes,
    append,
    serialize,
    cache_lookup,
    cache_populate,
    raw_read,
    read_cached,
    read_disk,
    segment_read,
    write,
};

const std::vector<std::pair<std::string_view, test_kind>> test_kinds = {
    {"index-lookup", test_kind::index_lookup},
    {"index-insert", test_kind::index_insert},
    {"deserialize", test_kind::deserialize},
    {"materialize", test_kind::materialize},
    {"build-mutation", test_kind::build_mutation},
    {"freeze", test_kind::freeze},
    {"record-header", test_kind::record_header},
    {"record-sizes", test_kind::record_sizes},
    {"append", test_kind::append},
    {"serialize", test_kind::serialize},
    {"cache-lookup", test_kind::cache_lookup},
    {"cache-populate", test_kind::cache_populate},
    {"raw-read", test_kind::raw_read},
    {"read-cached", test_kind::read_cached},
    {"read-disk", test_kind::read_disk},
    {"segment-read", test_kind::segment_read},
    {"write", test_kind::write},
};

std::string_view name_of(test_kind kind) {
    for (const auto& [name, k] : test_kinds) {
        if (k == kind) {
            return name;
        }
    }
    on_internal_error(logstor_logger, "unknown test kind");
}

// A pure CPU operation costs a few thousand instructions, of the order of what the measurement loop
// itself costs per invocation, so those tests do a batch of them per invocation.
constexpr unsigned cpu_test_batch = 32;

struct test_config {
    unsigned partitions;
    unsigned columns;
    size_t value_size;
    unsigned concurrency;
    unsigned duration_in_seconds;
    unsigned operations_per_shard;
    size_t segment_size;
    size_t file_size;
    size_t disk_size;
    bool compaction;
    bool stop_on_error;
};

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    return os << "{partitions=" << cfg.partitions
           << ", columns=" << cfg.columns
           << ", value_size=" << cfg.value_size
           << ", concurrency=" << cfg.concurrency
           << ", segment_size=" << cfg.segment_size
           << ", disk_size=" << cfg.disk_size
           << ", compaction=" << (cfg.compaction ? "yes" : "no")
           << "}";
}

// A logstor table: a partition key and the value columns of the row it holds. The number of them is
// what the description of the schema a record carries scales with, and what the framing of the
// cells scales with, so it is a parameter of the run rather than one column.
schema_ptr make_kv_schema(unsigned columns) {
    auto sb = schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key);
    for (unsigned i = 0; i < columns; ++i) {
        sb.with_column(to_bytes(fmt::format("v{}", i)), bytes_type);
    }
    return sb.set_logstor().build();
}

// One logstor of one shard, with a dataset written to it, which is what a shard of a node has: the
// segments of a shard are its own, and so is its index.
class logstor_bench {
    test_config _cfg;
    schema_ptr _schema;
    // Outlives _logstor, which holds a reference to it.
    tests::logstor::shared_logstor_cache _cache;
    std::unique_ptr<logstor> _logstor;
    std::unique_ptr<tests::logstor::test_logstor_group> _group;
    std::vector<dht::decorated_key> _keys;
    bytes _value;
    query::partition_slice _slice;
    query::partition_slice _slice_bypassing_cache;
    // One record of the dataset in the forms the steps of the read and the write path work on, for
    // the tests that measure a single step.
    std::unique_ptr<raw_write_buffer> _serialization_buffer;
    temporary_buffer<char> _serialized_record;
    canonical_mutation _canonical_mutation;
    std::optional<mutation> _mutation;
    // A record whose sizes have already been computed, for the append test, which is about what the
    // copy into the buffer costs and not about what computing the sizes of a record costs.
    std::optional<log_record_writer> _record_writer;
    // Takes the result of a step whose result is otherwise unused, so that the step is not optimized
    // away and cannot be hoisted out of the loop of the test that repeats it. Volatile because that
    // is what makes the store to it something the compiler has to keep; it costs the same load and
    // store in every test that uses it, so it cancels out of a difference between two of them.
    volatile uint64_t _sink = 0;
    // The first data file of the shard, opened for the raw read test.
    seastar::file _data_file;
    uint64_t _data_file_size = 0;
    std::filesystem::path _dir;

public:
    logstor_bench(test_config cfg, std::filesystem::path dir)
        : _cfg(cfg)
        , _schema(make_kv_schema(cfg.columns))
        , _value(bytes::initialized_later(), cfg.value_size)
        , _slice(partition_slice_builder(*_schema).build())
        , _slice_bypassing_cache(partition_slice_builder(*_schema)
                .with_option<query::partition_slice::option::bypass_cache>()
                .build())
        , _serialization_buffer(std::make_unique<raw_write_buffer>(cfg.segment_size, segment_kind::mixed))
        , _dir(dir) {
        std::ranges::fill(_value, int8_t('v'));
        auto logstor_cfg = tests::logstor::make_test_logstor_config(dir, {
            .segment_size = cfg.segment_size,
            .file_size = cfg.file_size,
            .disk_size = cfg.disk_size,
            .compaction_enabled = cfg.compaction,
        });
        // The same share of memory a node gives the writes it has taken but not yet flushed, which
        // is what bounds how far ahead of the disk the write path may run.
        logstor_cfg.max_queued_write_bytes = memory::stats().total_memory() / 100;
        _logstor = std::make_unique<logstor>(std::move(logstor_cfg), _cache.shared_tracker);
    }

    future<> start() {
        co_await _logstor->do_recovery_for_test();
        co_await _logstor->start();
        // The cache is enabled, and the tests that are not about it bypass it per read instead, so
        // that one dataset serves them all.
        _group = std::make_unique<tests::logstor::test_logstor_group>(_schema, *_logstor, true /* cache_enabled */);
        co_await populate();
        prepare_single_step_inputs();
        co_await open_data_file();
    }

    future<> stop() {
        if (_data_file) {
            co_await _data_file.close();
            _data_file = seastar::file();
        }
        if (_group) {
            // The index evicts what it has in the cache before it goes, since a cached mutation
            // lives in the shared cache region and the index entry only points at it. This is what
            // a table does when it stops.
            co_await index().drain_cache();
            // The group deregisters itself from the compaction manager in its destructor, which
            // waits for an ongoing compaction of the group and therefore needs a seastar thread. It
            // has to be gone before the logstor it belongs to is stopped.
            co_await seastar::async([this] {
                _group.reset();
            });
        }
        co_await _logstor->stop();
    }

    future<> do_write() {
        auto m = make_mutation(random_key());
        co_await _logstor->write(m, write_target{.cg = _group.get()}, db::no_timeout);
    }

    future<> do_read(const query::partition_slice& slice) {
        auto mut = co_await _logstor->read(*_schema, index(), random_key(), slice);
        if (!mut) [[unlikely]] {
            on_internal_error(logstor_logger, "key of the dataset is missing from the index");
        }
    }

    future<> do_read_cached() {
        return do_read(_slice);
    }

    future<> do_read_bypassing_cache() {
        return do_read(_slice_bypassing_cache);
    }

    // The read of the record from its segment and its deserialization, without the index lookup and
    // without materializing the mutation the read returns.
    future<> do_segment_read() {
        const auto& key = random_key();
        auto entry = index().get(primary_index_key{key});
        if (!entry) [[unlikely]] {
            on_internal_error(logstor_logger, "key of the dataset is missing from the index");
        }
        co_await _logstor->get_segment_manager().read(entry->location);
    }

    // One DMA read of the size of a record, issued straight to the data file: what a read of a
    // record costs before logstor puts anything of its own on top of it. What segment-read costs
    // beyond this is the index lookup of the location, the file handle lookup, the segment object
    // built per read, and the deserialization of the record.
    future<> do_raw_read() {
        const auto size = _serialized_record.size();
        auto offset = seastar::align_down<uint64_t>(tests::random::get_int<uint64_t>(uint64_t(_data_file_size - size)), 512);
        auto buf = co_await _data_file.dma_read_exactly<char>(offset, size);
        if (buf.size() < size) [[unlikely]] {
            on_internal_error(logstor_logger, "short read of the data file");
        }
    }

    void do_index_lookup(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            if (!index().get(primary_index_key{random_key()})) [[unlikely]] {
                on_internal_error(logstor_logger, "key of the dataset is missing from the index");
            }
        }
    }

    // What a write pays once its record is in a segment: the index is pointed at the new record.
    // The record is left where it is - the entry is replaced by one with the same location and a
    // newer timestamp - so that the space accounting of the segment manager sees a record freed and
    // the same record added, and the dataset the other tests read stays as it was. It includes the
    // lookup of the location to reinsert, which is what index-lookup measures on its own.
    void do_index_insert(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            primary_index_key key{random_key()};
            auto entry = index().get(key);
            if (!entry) [[unlikely]] {
                on_internal_error(logstor_logger, "key of the dataset is missing from the index");
            }
            index().insert(key, index_entry{
                .location = entry->location,
                .timestamp = api::new_timestamp(),
            });
        }
    }

    // What a write pays before its record reaches a buffer of the writer: the mutation is frozen
    // into a canonical_mutation and the record is serialized into the buffer.
    void do_serialize(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            _serialization_buffer->reset();
            _serialization_buffer->append(log_record_writer(log_record{
                .header = make_record_header(_mutation->decorated_key()),
                .mut = canonical_mutation(*_mutation),
            }));
        }
    }

    // Not a step of a write: the mutation a write is given, which the test has to build itself and
    // a node is handed by the layer above it. Measured so that it can be taken off what the write
    // test costs, which is the only test that builds one per operation.
    void do_build_mutation(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            auto m = make_mutation(random_key());
            _sink += m.partition().row_count();
        }
    }

    // The first half of what do_serialize() measures: the mutation is frozen into the
    // canonical_mutation that the record of a write carries.
    void do_freeze(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            auto frozen = canonical_mutation(*_mutation);
            (void)frozen;
        }
    }

    // The header of the record a write builds around its frozen mutation. It carries a copy of the
    // decorated key of the partition, which is what makes building it cost anything at all.
    void do_record_header(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            auto header = make_record_header(random_key());
            _sink += header.timestamp;
        }
    }

    // What log_record_writer::compute_sizes() does: the size of the header is arithmetic on its
    // key, and the value is serialized once into a stream that only counts the bytes, so that the
    // writer knows how much room to ask the buffer for. Measured over the header building of
    // do_record_header(), since the size of a header can only be measured on a header, and against
    // a key that changes per operation, so that the measuring cannot be hoisted out of the loop.
    void do_record_sizes(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            auto header = make_record_header(random_key());
            seastar::measuring_output_stream data_size;
            ser::serialize(data_size, _canonical_mutation);
            _sink += header.timestamp + ondisk::log_record_header_size(header) + data_size.size();
        }
    }

    // And the last: the record, whose sizes are already known, is copied into the buffer of the
    // writer. What do_serialize() costs beyond these two is the computation of those sizes, which
    // serializes the record once more only to measure it.
    void do_append(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            _serialization_buffer->reset();
            _serialization_buffer->append(*_record_writer);
        }
    }

    // What a read that the cache serves pays beyond the lookup of its index entry: the partition of
    // the cached mutation is copied out of the cache region.
    void do_cache_lookup(unsigned count) {
        auto* cache = index().cache_tracker();
        for (unsigned i = 0; i < count; ++i) {
            const auto& key = random_key();
            auto it = index().find(key);
            if (it == index().end()) [[unlikely]] {
                on_internal_error(logstor_logger, "key of the dataset is missing from the index");
            }
            if (!cache->lookup(*it, _schema)) [[unlikely]] {
                on_internal_error(logstor_logger, "key of the dataset is missing from the cache");
            }
        }
    }

    // What a read that went to a segment pays to leave its mutation in the cache. The entry is
    // evicted first, since a populate of an entry that is already cached does nothing, and what is
    // measured is therefore the eviction of a cached partition together with the admission of one.
    // What it admits is the partition prepared for the single step tests, so that the cost of
    // building a mutation is not part of the measurement; the entry is left holding a partition of
    // the same shape as its own, which is all a test that runs after this one reads out of it.
    void do_cache_populate(unsigned count) {
        auto* cache = index().cache_tracker();
        for (unsigned i = 0; i < count; ++i) {
            const auto& key = random_key();
            auto it = index().find(key);
            if (it == index().end()) [[unlikely]] {
                on_internal_error(logstor_logger, "key of the dataset is missing from the index");
            }
            cache->evict(*it);
            cache->populate(*it, *_mutation);
        }
    }

    // What a read pays once the record is in memory: the record is deserialized, which copies the
    // bytes of its canonical_mutation out of the buffer read from the segment.
    void do_deserialize(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            deserialize_log_record(simple_memory_input_stream(_serialized_record.begin(), _serialized_record.size()));
        }
    }

    // And what it pays after that: the canonical_mutation is turned into the mutation the read
    // returns.
    void do_materialize(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            _canonical_mutation.to_mutation(_schema);
        }
    }

    // Reads every key once, so that the reads that follow find them in the cache.
    future<> warm_cache() {
        return max_concurrent_for_each(_keys, _cfg.concurrency, [this] (const dht::decorated_key& key) {
            return _logstor->read(*_schema, index(), key, _slice).discard_result();
        });
    }

    segment_manager_usage usage() const noexcept {
        return _logstor->get_segment_manager().get_usage();
    }

    // What one record of the dataset takes in a segment, against the bytes of value it carries. The
    // difference between the two is what the format of a record costs, which is paid on every write,
    // on every disk byte and on every read that goes to a segment.
    size_t record_size() const noexcept { return _serialized_record.size(); }
    size_t payload_size() const noexcept { return size_t(_cfg.columns) * _cfg.value_size; }

private:
    primary_index& index() noexcept {
        return _group->logstor_index();
    }

    const dht::decorated_key& random_key() const noexcept {
        return _keys[tests::random::get_int<size_t>(_keys.size() - 1)];
    }

    log_record_header make_record_header(const dht::decorated_key& key) const {
        return log_record_header{
            .key = primary_index_key{key},
            .timestamp = api::new_timestamp(),
            .table = _schema->id(),
        };
    }

    mutation make_mutation(const dht::decorated_key& key) const {
        const auto ts = api::new_timestamp();
        mutation m(_schema, key);
        auto& row = m.partition().clustered_row(*_schema, clustering_key::make_empty());
        row.apply(row_marker(ts));
        for (const auto& value_def : _schema->regular_columns()) {
            row.cells().apply(value_def, atomic_cell::make_live(*value_def.type, ts, _value));
        }
        return m;
    }

    // The data files of a shard are named ls_{shard}-{file}-Data.db, and the first one is as good
    // as any for a read that is only about what the read costs.
    future<> open_data_file() {
        auto path = _dir / fmt::format("ls_{}-0-Data.db", this_shard_id());
        _data_file = co_await seastar::open_file_dma(path.string(), seastar::open_flags::ro);
        _data_file_size = co_await _data_file.size();
    }

    future<> populate() {
        _keys.reserve(_cfg.partitions);
        for (unsigned i = 0; i < _cfg.partitions; ++i) {
            auto pk = partition_key::from_single_value(*_schema, serialized(int64_t(i)));
            _keys.push_back(dht::decorate_key(*_schema, std::move(pk)));
        }
        // A write completes only once its record has been flushed to a segment, so writing the
        // dataset one record at a time would wait for the disk for the whole populate phase.
        co_await max_concurrent_for_each(_keys, _cfg.concurrency, [this] (const dht::decorated_key& key) -> future<> {
            auto m = make_mutation(key);
            co_await _logstor->write(m, write_target{.cg = _group.get()}, db::no_timeout);
        });
        // Seal the records into segments of the group, which is where the steady state of a node
        // has them and where a read finds them.
        co_await _logstor->flush_to_separator();
        co_await _group->flush_separator();
    }

    void prepare_single_step_inputs() {
        _mutation = make_mutation(_keys[0]);
        _canonical_mutation = canonical_mutation(*_mutation);
        _record_writer.emplace(log_record{
            .header = make_record_header(_mutation->decorated_key()),
            .mut = _canonical_mutation,
        });
        _serialization_buffer->reset();
        auto appended = _serialization_buffer->append(*_record_writer);
        _serialized_record = temporary_buffer<char>(_serialization_buffer->data() + appended.record_header_offset,
                appended.total_size);
    }
};

std::vector<perf_result_with_io> run_test(sharded<logstor_bench>& bench, test_kind kind, const test_config& cfg) {
    // An operation of the tests that only use the CPU does not wait for anything, so running more
    // than one of them at a time only adds the cost of switching between them.
    const auto cpu_test = [&bench, &cfg] (void (logstor_bench::*op)(unsigned)) {
        return time_parallel_ex<perf_result_with_io>([&bench, op] {
            (bench.local().*op)(cpu_test_batch);
            return make_ready_future<>();
        }, 1, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error, io_counters_updater(), cpu_test_batch);
    };
    const auto io_test = [&bench, &cfg] (future<> (logstor_bench::*op)()) {
        return time_parallel_ex<perf_result_with_io>([&bench, op] {
            return (bench.local().*op)();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error, io_counters_updater());
    };

    switch (kind) {
    case test_kind::index_lookup:
        return cpu_test(&logstor_bench::do_index_lookup);
    case test_kind::index_insert:
        return cpu_test(&logstor_bench::do_index_insert);
    case test_kind::deserialize:
        return cpu_test(&logstor_bench::do_deserialize);
    case test_kind::materialize:
        return cpu_test(&logstor_bench::do_materialize);
    case test_kind::build_mutation:
        return cpu_test(&logstor_bench::do_build_mutation);
    case test_kind::freeze:
        return cpu_test(&logstor_bench::do_freeze);
    case test_kind::record_header:
        return cpu_test(&logstor_bench::do_record_header);
    case test_kind::record_sizes:
        return cpu_test(&logstor_bench::do_record_sizes);
    case test_kind::append:
        return cpu_test(&logstor_bench::do_append);
    case test_kind::serialize:
        return cpu_test(&logstor_bench::do_serialize);
    case test_kind::cache_lookup:
        // Every key has to be in the cache, since what is measured is what a hit costs.
        bench.invoke_on_all(&logstor_bench::warm_cache).get();
        return cpu_test(&logstor_bench::do_cache_lookup);
    case test_kind::cache_populate:
        return cpu_test(&logstor_bench::do_cache_populate);
    case test_kind::raw_read:
        return io_test(&logstor_bench::do_raw_read);
    case test_kind::read_cached:
        bench.invoke_on_all(&logstor_bench::warm_cache).get();
        return io_test(&logstor_bench::do_read_cached);
    case test_kind::read_disk:
        return io_test(&logstor_bench::do_read_bypassing_cache);
    case test_kind::segment_read:
        return io_test(&logstor_bench::do_segment_read);
    case test_kind::write:
        return io_test(&logstor_bench::do_write);
    }
    on_internal_error(logstor_logger, "unknown test kind");
}

void write_json_result(const std::string& file, const test_config& cfg, test_kind kind, const aggregated_perf_results& agg,
        const perf_result_with_io& median, size_t record_bytes, size_t payload_bytes) {
    Json::Value params;
    params["partitions"] = cfg.partitions;
    params["columns"] = cfg.columns;
    params["value_size"] = cfg.value_size;
    params["concurrency"] = cfg.concurrency;
    params["cpus"] = this_smp_shard_count();
    params["duration"] = cfg.duration_in_seconds;
    params["segment_size"] = Json::UInt64(cfg.segment_size);
    params["disk_size"] = Json::UInt64(cfg.disk_size);
    params["compaction"] = cfg.compaction;

    Json::Value extra_stats;
    extra_stats["reads_per_op"] = median.reads;
    extra_stats["read_bytes_per_op"] = median.read_bytes;
    extra_stats["writes_per_op"] = median.writes;
    extra_stats["write_bytes_per_op"] = median.write_bytes;
    extra_stats["record_bytes"] = Json::UInt64(record_bytes);
    extra_stats["payload_bytes"] = Json::UInt64(payload_bytes);

    perf::write_json_result(file, agg, params, fmt::format("logstor_{}", name_of(kind)), extra_stats);
}

// Everything a run takes from the command line. The body of the run is a coroutine, and the lambda
// that holds the reference to the application does not outlive it, so the configuration is read out
// once, up front, rather than looked up as the run goes.
struct run_config {
    test_config test;
    std::vector<test_kind> tests;
    std::string dir;
    std::string json_result;
    unsigned seed;
};

std::vector<test_kind> parse_tests(const std::string& names) {
    if (names == "all") {
        return test_kinds | std::views::values | std::ranges::to<std::vector<test_kind>>();
    }
    std::vector<test_kind> kinds;
    for (const auto& name : std::views::split(std::string_view(names), std::string_view(","))) {
        auto sv = std::string_view(name.begin(), name.end());
        auto found = std::ranges::find(test_kinds, sv, &std::pair<std::string_view, test_kind>::first);
        if (found == test_kinds.end()) {
            throw std::invalid_argument(fmt::format("unknown test '{}', expected one of {} or 'all'",
                    sv, fmt::join(test_kinds | std::views::keys, ", ")));
        }
        kinds.push_back(found->second);
    }
    return kinds;
}

run_config make_run_config(const boost::program_options::variables_map& config) {
    auto seed = config["random-seed"];
    run_config run{
        .test = test_config{
            .partitions = config["partitions"].as<unsigned>(),
            .columns = config["columns"].as<unsigned>(),
            .value_size = config["value-size"].as<unsigned>(),
            .concurrency = config["concurrency"].as<unsigned>(),
            .duration_in_seconds = config["duration"].as<unsigned>(),
            .operations_per_shard = 0,
            .segment_size = config["segment-size-in-kb"].as<unsigned>() * 1024ull,
            .file_size = config["file-size-in-mb"].as<unsigned>() * 1024ull * 1024ull,
            .disk_size = config["disk-size-in-mb"].as<unsigned>() * 1024ull * 1024ull,
            .compaction = config["compaction"].as<bool>(),
            .stop_on_error = config["stop-on-error"].as<bool>(),
        },
        .tests = parse_tests(config["test"].as<std::string>()),
        .dir = config.contains("dir") ? config["dir"].as<std::string>() : std::string(),
        .json_result = config.contains("json-result") ? config["json-result"].as<std::string>() : std::string(),
        .seed = seed.empty() ? std::random_device()() : seed.as<unsigned>(),
    };
    if (config.contains("operations-per-shard")) {
        run.test.operations_per_shard = config["operations-per-shard"].as<unsigned>();
    }
    if (run.test.partitions == 0) {
        throw std::invalid_argument("--partitions must be at least one: every test reads or overwrites the dataset");
    }
    return run;
}

} // namespace

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("random-seed", bpo::value<unsigned>(), "random number generator seed")
        ("test", bpo::value<std::string>()->default_value("all"), "comma separated tests to run, or 'all'")
        ("partitions", bpo::value<unsigned>()->default_value(100000), "number of partitions written per shard")
        ("columns", bpo::value<unsigned>()->default_value(1), "number of value columns of the table, each holding a value of --value-size bytes")
        ("value-size", bpo::value<unsigned>()->default_value(200), "size of the value of a column in bytes")
        ("concurrency", bpo::value<unsigned>()->default_value(50), "operations in flight per shard, for the tests that wait for the disk")
        ("duration", bpo::value<unsigned>()->default_value(5), "number of one second iterations per test")
        ("operations-per-shard", bpo::value<unsigned>(), "run this many operations per shard (overrides duration)")
        ("segment-size-in-kb", bpo::value<unsigned>()->default_value(128), "size of a logstor segment")
        ("file-size-in-mb", bpo::value<unsigned>()->default_value(32), "size of a logstor data file")
        ("disk-size-in-mb", bpo::value<unsigned>()->default_value(512), "size of the logstor segment pool of a shard")
        ("compaction", bpo::value<bool>()->default_value(true), "let compaction run, which is what gives free segments back")
        ("dir", bpo::value<std::string>(), "directory for the logstor files (default: a temporary directory)")
        ("stop-on-error", bpo::value<bool>()->default_value(true), "stop after encountering the first error")
        ("json-result", bpo::value<std::string>(), "name of the json result file, suffixed with the name of the test")
        ;

    set_abort_on_internal_error(true);

    return app.run(argc, argv, [&app] () -> future<> {
        auto run = make_run_config(app.configuration());
        const auto& cfg = run.test;
        fmt::print("random-seed={}\n", run.seed);
        co_await smp::invoke_on_all([seed = run.seed] {
            seastar::testing::local_random_engine.seed(seed + this_shard_id());
        });

        // Overwriting the dataset needs room for the dead records the overwrites leave behind, which
        // compaction only reclaims once the disk is short of free segments.
        const auto dataset_size = size_t(cfg.partitions) * (cfg.columns * cfg.value_size + 100);
        if (dataset_size * 2 > cfg.disk_size) {
            fmt::print("WARNING: the dataset of about {} MB does not leave room in a segment pool of {} MB;"
                    " writes will stall on compaction\n", dataset_size / 1024 / 1024, cfg.disk_size / 1024 / 1024);
        }

        tmpdir tmp;
        auto dir = run.dir.empty() ? tmp.path() : std::filesystem::path(run.dir);
        co_await recursive_touch_directory(dir.string());
        std::cout << "Running logstor tests with config: " << cfg << ", dir=" << dir << std::endl;

        sharded<logstor_bench> bench;
        co_await bench.start(cfg, dir);
        std::exception_ptr ex;
        try {
            co_await bench.invoke_on_all(&logstor_bench::start);
            co_await seastar::async([&] {
                auto usage = bench.local().usage();
                const auto record_bytes = bench.local().record_size();
                const auto payload_bytes = bench.local().payload_size();
                fmt::print("dataset written: {} of {} segments of shard 0 are free\n", usage.free_segments, usage.total_segments);
                fmt::print("record: {} bytes in a segment for {} bytes of value ({:.2f}x)\n",
                        record_bytes, payload_bytes, payload_bytes ? double(record_bytes) / payload_bytes : 0.0);
                for (auto kind : run.tests) {
                    fmt::print("\n{}:\n", name_of(kind));
                    auto results = run_test(bench, kind, cfg);
                    std::vector<perf_result> throughput_results(results.begin(), results.end());
                    aggregated_perf_results agg(throughput_results);
                    std::cout << agg << std::endl;
                    // The same median as the aggregation reports, with the IO counters of that
                    // iteration, which are not part of it.
                    std::ranges::sort(results, std::less<>{}, &perf_result::throughput);
                    const auto& median = results[results.size() / 2];
                    fmt::print("median: {}\n", median);
                    if (!run.json_result.empty()) {
                        write_json_result(fmt::format("{}.{}", run.json_result, name_of(kind)), cfg, kind, agg, median,
                                record_bytes, payload_bytes);
                    }
                }
            });
        } catch (...) {
            ex = std::current_exception();
        }

        // Stops every instance before destroying it.
        co_await bench.stop();
        if (ex) {
            std::rethrow_exception(ex);
        }
    });
}
