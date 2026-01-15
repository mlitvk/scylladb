/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "replica/log_structured/segment_manager.hh"
#include "replica/log_structured/logstor.hh"
#include "replica/log_structured/types.hh"
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/metrics.hh>
#include "serializer_impl.hh"
#include "idl/log_structured.dist.hh"
#include "idl/log_structured.dist.impl.hh"
#include "utils/dynamic_bitset.hh"
#include "utils/log_heap.hh"

namespace replica::log_structured {

class segment {
protected:
    log_segment_id _id;
    seastar::file _file;
    uint64_t _file_offset; // Offset within the shared file where this segment starts
    uint64_t _current_offset = 0; // Relative offset within this segment
    size_t _max_size;

    // Track how much data is actually live in this segment
    size_t _live_size = 0;

public:
    segment(log_segment_id id, seastar::file file, uint64_t file_offset, size_t max_size);

    virtual ~segment() = default;

    future<> load();
    future<> close();

    future<log_structured_segment_record> read(uint64_t offset, uint32_t size);

    // Properties
    uint64_t current_offset() const noexcept { return _current_offset; }
    log_segment_id id() const noexcept { return _id; }

    seastar::file& get_file() noexcept { return _file; }

    // Used size tracking
    size_t live_size() const noexcept { return _live_size; }
    void update_live_size(size_t size) noexcept { _live_size += size; }

    size_t bytes_remaining() const noexcept {
        return _max_size - _current_offset;
    }

protected:
    uint64_t absolute_offset(uint64_t relative_offset) const noexcept {
        return _file_offset + relative_offset;
    }
};

class writeable_segment : public segment {
    bool _sealed = false;
    seastar::gate _write_gate;

public:
    using segment::segment;

    future<> start();
    future<> stop();

    // Write a serialized sequence of records
    log_location allocate(size_t data_size);
    future<> write(log_location loc, bytes_view data);

    // Lifecycle management
    future<> seal();
    future<> sync();

    // Properties
    bool is_sealed() const noexcept { return _sealed; }
    bool can_fit(size_t data_size) const noexcept;
};

segment::segment(log_segment_id id, seastar::file file, uint64_t file_offset, size_t max_size)
    : _id(id)
    , _file(std::move(file))
    , _file_offset(file_offset)
    , _max_size(max_size) {
}

future<> segment::load() {
    _current_offset = _max_size;
    co_return;
}

future<> segment::close() {
    if (_file) {
        co_await _file.close();
    }
}

future<log_structured_segment_record> segment::read(uint64_t offset, uint32_t size) {
    if (offset + size > _current_offset) {
        throw std::runtime_error(fmt::format("Read beyond end of segment {}: offset {} + size {} > current_offset {}",
                                             _id, offset, size, _current_offset));
    }

    // Read the serialized record
    return _file.dma_read<char>(absolute_offset(offset), size).then([] (seastar::temporary_buffer<char> buf) {
        seastar::simple_input_stream in(buf.begin(), buf.size());
        auto record = ser::deserialize(in, std::type_identity<log_structured_segment_record>{});
        return record;
    });
}

future<> writeable_segment::start() {
    logstor_logger.debug("Starting writeable log segment {}", _id);
    co_return;
}

future<> writeable_segment::stop() {
    if (_write_gate.is_closed()) {
        co_return;
    }
    logstor_logger.debug("Stopping writeable log segment {}", _id);
    co_await _write_gate.close();
    if (!_sealed) {
        co_await seal();
    }
    logstor_logger.debug("Writeable log segment {} stopped", _id);
}

log_location writeable_segment::allocate(size_t data_size) {
    if (!can_fit(data_size)) {
        throw std::runtime_error("Entry too large for remaining segment space");
    }

    auto current_pos = _current_offset;
    _current_offset += data_size;

    return log_location{
        .segment = _id,
        .offset = current_pos,
        .size = static_cast<uint32_t>(data_size)
    };
}

future<> writeable_segment::write(log_location loc, bytes_view data) {
    if (_sealed) {
        throw std::runtime_error("Cannot append to sealed segment");
    }

    auto holder = _write_gate.hold();

    // Write serialized bytes directly
    co_await _file.dma_write(absolute_offset(loc.offset), data.data(), data.size());

    // Track used size
    update_live_size(data.size());
}

future<> writeable_segment::seal() {
    if (_sealed) {
        co_return;
    }

    if (!_write_gate.is_closed()) {
        co_await _write_gate.close();
    }
    co_await sync();
    _sealed = true;
}

future<> writeable_segment::sync() {
    return _file.flush();
}

bool writeable_segment::can_fit(size_t data_size) const noexcept {
    return _current_offset + data_size <= _max_size;
}

constexpr log_heap_options segment_descriptor_hist_options(4 * 1024, 3, 128 * 1024);

struct segment_descriptor : public log_heap_hook<segment_descriptor_hist_options> {
    size_t _free_space{0};

    void reset() noexcept {
        _free_space = 0;
    }

    size_t free_space() const noexcept {
        return _free_space;
    }
};

using segment_descriptor_hist = log_heap<segment_descriptor, segment_descriptor_hist_options>;

class segment_manager_impl {

    using sseg_ptr = lw_shared_ptr<writeable_segment>;

    constexpr static size_t reserve_segments_target = 2;

    struct file_stats {
        utils::dynamic_bitset free_segments{0}; // set = free, clear = used

        file_stats() = default;
        explicit file_stats(size_t segments_per_file) : free_segments(segments_per_file) {
            // Initialize all segments as free
            for (size_t i = 0; i < segments_per_file; ++i) {
                free_segments.set(i);
            }
        }
    };

    struct segment_location {
        uint64_t file_id;
        uint64_t file_offset;
    };

    struct stats {
        uint64_t segments_in_use{0};
        uint64_t bytes_written{0};
        uint64_t data_bytes_written{0};
        uint64_t bytes_read{0};
        uint64_t bytes_released{0};
        uint64_t segments_allocated{0};
        uint64_t segments_freed{0};
        uint64_t files_created{0};
        uint64_t bytes_unused{0};
    };

    std::filesystem::path _base_dir;
    size_t _segment_size;
    size_t _file_size;
    size_t _segments_per_file;
    size_t _max_files;

    seastar::scheduling_group _sched_group;

    stats _stats;
    seastar::metrics::metric_groups _metrics;

    size_t _next_file_id{0};

    sseg_ptr _active_segment;
    seastar::queue<sseg_ptr> _reserve_segments{reserve_segments_target};
    std::optional<shared_future<>> _next_segment;

    seastar::gate _async_gate;
    future<> _reserve_replenisher{make_ready_future<>()};
    seastar::condition_variable _segment_freed_cv;

    utils::chunked_vector<segment_descriptor> _segment_descs;
    segment_descriptor_hist _segment_hist;
    utils::chunked_vector<file_stats> _file_stats;

    static constexpr size_t max_cached_read_files = 4;
    std::list<std::pair<uint64_t, seastar::file>> _read_file_cache_lru;
    std::unordered_map<uint64_t, decltype(_read_file_cache_lru)::iterator> _open_read_files;

    std::optional<segment_manager::compaction_trigger_fn> _compaction_trigger;

public:
    static constexpr size_t record_alignment = 8;
    static constexpr size_t disk_alignment = 4096;

    explicit segment_manager_impl(segment_manager_config config);

    ~segment_manager_impl();

    segment_manager_impl(const segment_manager_impl&) = delete;
    segment_manager_impl& operator=(const segment_manager_impl&) = delete;

    future<> start();
    future<> stop();

    // Write a serialized sequence of records
    future<log_location> write(bytes_view data);
    future<log_location> write(bytes_view data, int64_t live_size);

    future<log_structured_segment_record> read(log_location location);

    void free_record(log_location location);

    /// Find segments suitable for compaction based on live ratio
    /// Returns up to max_segments with live ratio below threshold
    std::vector<log_segment_id> find_segments_for_compaction(double min_live_ratio, size_t max_segments);

    /// Free an entire segment (used after compaction)
    future<> free_segment(log_segment_id segment_id);

    /// Read all records from the specified segments
    /// Callback is invoked for each record found
    future<> for_each_record(std::vector<log_segment_id> segments,
                            std::function<future<>(log_location, log_structured_segment_record)> callback);

    void set_compaction_trigger(segment_manager::compaction_trigger_fn fn) {
        _compaction_trigger.emplace(std::move(fn));
    }

    void trigger_compaction() {
        if (_compaction_trigger) {
            (*_compaction_trigger)();
        }
    }

    size_t get_segment_size() const noexcept {
        return _segment_size;
    }

private:
    future<> new_segment();
    future<> replenish_reserve();

    future<lw_shared_ptr<writeable_segment>> create_new_segment();
    std::optional<segment_manager_impl::segment_location> find_free_segment();
    future<segment_location> allocate_segment();

    // File management helpers
    future<seastar::file> get_file_for_write(uint64_t file_id);
    future<seastar::file> get_file_for_read(uint64_t file_id);
    std::string get_file_path(uint64_t file_id) const;

    future<> format_file_region(seastar::file file, uint64_t offset, size_t size);

    segment_location segment_id_to_file_location(log_segment_id segment_id) const noexcept;
    log_segment_id file_location_to_segment_id(segment_location) const noexcept;
    size_t file_offset_to_segment_index(uint64_t file_offset) const noexcept;
    uint64_t segment_index_to_file_offset(size_t segment_index) const noexcept;

    segment_descriptor& get_segment_descriptor(log_segment_id segment_id) {
        return _segment_descs[segment_id.value];
    }

    log_segment_id desc_to_segment_id(const segment_descriptor& desc) const noexcept {
        size_t index = &desc - &_segment_descs[0];
        return log_segment_id(static_cast<uint32_t>(index));
    }
};

segment_manager_impl::segment_manager_impl(segment_manager_config config)
    : _base_dir(std::move(config.base_dir))
    , _segment_size(config.segment_size)
    , _file_size(config.file_size)
    , _segments_per_file(_file_size / _segment_size)
    , _max_files(config.disk_size / config.file_size)
    , _sched_group(config.sched_group)
    , _segment_descs(_max_files * _segments_per_file)
    , _file_stats(_max_files) {

    namespace sm = seastar::metrics;

    _metrics.add_group("logstor_sm", {
        sm::make_gauge("segments_in_use", _stats.segments_in_use,
                       sm::description("Counts number of segments currently in use.")),
        sm::make_counter("bytes_written", _stats.bytes_written,
                       sm::description("Counts number of bytes written to the disk.")),
        sm::make_counter("data_bytes_written", _stats.data_bytes_written,
                       sm::description("Counts number of data bytes written to the disk.")),
        sm::make_counter("bytes_read", _stats.bytes_read,
                       sm::description("Counts number of bytes read from the disk.")),
        sm::make_counter("bytes_released", _stats.bytes_released,
                       sm::description("Counts number of data bytes released.")),
        sm::make_counter("segments_allocated", _stats.segments_allocated,
                       sm::description("Counts number of segments allocated.")),
        sm::make_counter("segments_freed", _stats.segments_freed,
                       sm::description("Counts number of segments freed.")),
        sm::make_counter("files_created", _stats.files_created,
                       sm::description("Counts number of files created.")),
        sm::make_counter("bytes_unused", _stats.bytes_unused,
                       sm::description("Counts number of unused bytes in closed segments.")),
        sm::make_gauge("disk_usage", [this]() { return _next_file_id * _file_size; },
                       sm::description("Total disk usage.")),
    });
}

segment_manager_impl::~segment_manager_impl() {
    // unlink all segment descriptors before destroying
    while (!_segment_hist.empty()) {
        _segment_hist.pop_one_of_largest();
    }
}

future<> segment_manager_impl::start() {
    co_await seastar::recursive_touch_directory(_base_dir.string());

    // Start background replenisher before creating initial segment
    _reserve_replenisher = with_scheduling_group(_sched_group, [this] {
        return replenish_reserve();
    });

    co_await new_segment();

    logstor_logger.info("segment manager started with base directory {}", _base_dir.string());
}

future<> segment_manager_impl::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping segment manager");

    _segment_freed_cv.broken();

    // Close gate first to stop new operations
    co_await _async_gate.close();

    // Abort reserve queue to wake up replenisher
    _reserve_segments.abort(std::make_exception_ptr(std::runtime_error("shutting down")));

    // Wait for replenisher to finish
    co_await std::move(_reserve_replenisher);

    if (auto seg = std::exchange(_active_segment, nullptr)) {
        co_await seg->stop();
    }

    _open_read_files.clear();
    _read_file_cache_lru.clear();

    logstor_logger.info("segment manager stopped");
}

future<log_location> segment_manager_impl::write(bytes_view data, int64_t live_size) {
    auto holder = _async_gate.hold();

    if (data.size() > _segment_size) {
        throw std::runtime_error(fmt::format( "Write size {} exceeds segment size {}", data.size(), _segment_size));
    }

    if (data.size() % disk_alignment != 0) {
        on_internal_error(logstor_logger, fmt::format("Unaligned write of size {} bytes", data.size()));
    }

    while (!_active_segment || !_active_segment->can_fit(data.size())) {
        logstor_logger.debug("Active segment {} full, creating new segment", _active_segment->id());
        if (!_next_segment) {
            auto f = new_segment();
            if (f.available()) {
                f.get();
                continue;
            }
            _next_segment.emplace(f.then([this] {
                _next_segment.reset();
            }));
        }
        co_await _next_segment->get_future();
    }

    auto seg = _active_segment;

    auto loc = seg->allocate(data.size());
    co_await seg->write(loc, data);

    auto& desc = get_segment_descriptor(loc.segment);
    desc._free_space += data.size() - live_size;

    _stats.bytes_written += data.size();
    _stats.data_bytes_written += live_size;

    co_return loc;
}

void segment_manager_impl::free_record(log_location location) {
    _stats.bytes_released += location.size;

    auto& desc = get_segment_descriptor(location.segment);
    desc._free_space += location.size;
    if (desc.is_linked()) {
        _segment_hist.adjust_up(desc);
    }
}

future<log_structured_segment_record> segment_manager_impl::read(log_location location) {
    auto holder = _async_gate.hold();

    auto [file_id, file_offset] = segment_id_to_file_location(location.segment);
    auto file = co_await get_file_for_read(file_id);

    logstor_logger.trace("Loading segment {} from file {} at offset {}", location.segment, file_id, file_offset);

    segment segment(location.segment, std::move(file), file_offset, _segment_size);
    co_await segment.load();
    auto record = co_await segment.read(location.offset, location.size);

    _stats.bytes_read += location.size;

    co_return std::move(record);
}

future<> segment_manager_impl::new_segment() {
    auto holder = _async_gate.hold();

    auto new_seg = co_await _reserve_segments.pop_eventually();
    auto old_seg = std::exchange(_active_segment, new_seg);
    logstor_logger.trace("Switched active segment to {}", _active_segment->id());
    _stats.segments_in_use++;

    if (old_seg) {
        // Close old segment in background, don't wait
        (void)with_gate(_async_gate, [this, old_seg]() {
            return old_seg->stop().then([this, old_seg] {
                auto old_seg_id = old_seg->id();
                auto& desc = get_segment_descriptor(old_seg_id);
                desc._free_space += old_seg->bytes_remaining();
                _segment_hist.push(desc);

                _stats.bytes_unused += old_seg->bytes_remaining();

                logstor_logger.debug("Closed segment {} with free space {}", old_seg_id, desc.free_space());
            });
        });
    }

    auto active_file_id = segment_id_to_file_location(_active_segment->id()).file_id;
    _read_file_cache_lru.emplace_front(active_file_id, _active_segment->get_file());
    if (_open_read_files.size() > max_cached_read_files) {
        auto& lru_entry = _read_file_cache_lru.back();
        _open_read_files.erase(lru_entry.first);
        _read_file_cache_lru.pop_back();
    }
    _open_read_files.emplace(active_file_id, _read_file_cache_lru.begin());
}

future<> segment_manager_impl::replenish_reserve() {
    while (true) {
        bool retry = false;
        try {
            // Wait until there's space in the reserve queue
            co_await _reserve_segments.not_full();

            // Check if we're shutting down (queue was aborted)
            if (_async_gate.is_closed()) {
                break;
            }

            gate::holder holder(_async_gate);

            // create and push a new segment to reserve queue
            auto seg = co_await create_new_segment();
            co_await _reserve_segments.push_eventually(std::move(seg));
        } catch (...) {
            // If gate is closed or we got shutdown exception, break out
            if (_async_gate.is_closed()) {
                logstor_logger.debug("Reserve replenisher stopping due to gate close");
                break;
            }

            retry = true;
            logstor_logger.warn("Exception in reserve replenisher: {}, will retry", std::current_exception());
        }

        if (retry) {
            co_await seastar::sleep(std::chrono::seconds(1));
        }
    }

    logstor_logger.debug("Reserve replenisher stopped");
}

future<lw_shared_ptr<writeable_segment>> segment_manager_impl::create_new_segment() {
    auto seg_loc = co_await allocate_segment();
    auto segment_id = file_location_to_segment_id(seg_loc);
    auto file = co_await get_file_for_write(seg_loc.file_id);

    logstor_logger.debug("Creating new log segment {} in file {} at offset {}", segment_id, seg_loc.file_id, seg_loc.file_offset);

    auto segment = make_lw_shared<writeable_segment>(segment_id, std::move(file), seg_loc.file_offset, _segment_size);
    co_await segment->start();

    // Mark segment as allocated in file stats
    size_t seg_idx = file_offset_to_segment_index(seg_loc.file_offset);
    _file_stats[seg_loc.file_id].free_segments.clear(seg_idx);

    _stats.segments_allocated++;

    co_return segment;
}

std::optional<segment_manager_impl::segment_location> segment_manager_impl::find_free_segment() {
    // TODO improve search
    for (size_t file_id = 0; file_id < _next_file_id; ++file_id) {
        size_t free_segment_idx = _file_stats[file_id].free_segments.find_first_set();
        if (free_segment_idx != utils::dynamic_bitset::npos) {
            uint64_t offset = segment_index_to_file_offset(free_segment_idx);
            logstor_logger.trace("Found free segment at index {} in file {} (offset {})",
                               free_segment_idx, file_id, offset);
            return segment_location{file_id, offset};
        }
    }
    return std::nullopt;
}

future<segment_manager_impl::segment_location> segment_manager_impl::allocate_segment() {
    // Try to find a free segment in any existing file
    if (auto seg_loc = find_free_segment()) {
        co_return *seg_loc;
    }

    // All existing files are full, create new file.
    // if out of disk space, wait for segment in an existing file to be freed.

    if (_next_file_id >= _max_files) {
        logstor_logger.warn("Maximum number of files {} reached, waiting for free segments", _max_files);

        while (true) {
            // TODO: compaction might fail to free segments now, but can succeed later as data is freed.
            trigger_compaction();

            co_await _segment_freed_cv.wait();

            if (auto seg_loc = find_free_segment()) {
                co_return *seg_loc;
            }
        }
    }

    auto file_id = _next_file_id++;
    _file_stats[file_id] = file_stats(_segments_per_file);

    logstor_logger.debug("Creating new file {} with {} free segments", file_id, _segments_per_file);

    co_return segment_location{file_id, 0UL};
}

future<seastar::file> segment_manager_impl::get_file_for_write(uint64_t file_id) {
    auto file_path = get_file_path(file_id);
    bool file_exists = co_await seastar::file_exists(file_path);

    auto file = co_await seastar::open_file_dma(file_path,
            seastar::open_flags::rw | seastar::open_flags::create);

    if (!file_exists) {
        logstor_logger.info("Creating and formatting new file {} ({} MB)", file_path, _file_size/(1024*1024));
        co_await file.allocate(0, _file_size);
        co_await format_file_region(file, 0, _file_size);
        _stats.files_created++;
    }

    co_return file;
}

future<seastar::file> segment_manager_impl::get_file_for_read(uint64_t file_id) {
    if (auto it = _open_read_files.find(file_id); it != _open_read_files.end()) {
        co_return it->second->second;
    }

    auto file = co_await seastar::open_file_dma(
        get_file_path(file_id),
        seastar::open_flags::ro
    );

    co_return std::move(file);
}

future<> segment_manager_impl::format_file_region(seastar::file file, uint64_t offset, size_t size) {
    // Allocate aligned buffer for zeroing
    const auto write_alignment = file.disk_write_dma_alignment();
    size_t buf_size = align_up<size_t>(128 * 1024, size_t(write_alignment));
    auto zero_buf = allocate_aligned_buffer<char>(buf_size, write_alignment);
    std::memset(zero_buf.get(), 0, buf_size);

    // Write zeros to entire region
    size_t remaining = size;
    uint64_t current_offset = offset;

    while (remaining > 0) {
        auto write_size = std::min(remaining, buf_size);
        auto written = co_await file.dma_write(current_offset, zero_buf.get(), write_size);

        if (written == 0) [[unlikely]] {
            on_internal_error(logstor_logger,
                format("dma_write returned 0 while formatting: offset={} remaining={}",
                       current_offset, remaining));
        }

        current_offset += written;
        remaining -= written;
    }

    co_await file.flush();
}

std::string segment_manager_impl::get_file_path(uint64_t file_id) const {
    return fmt::format("{}/ls_{}-{}-Data.db", _base_dir.string(), this_shard_id(), file_id);
}

segment_manager_impl::segment_location segment_manager_impl::segment_id_to_file_location(log_segment_id segment_id) const noexcept {
    uint64_t segment_id_val = segment_id.value;
    uint64_t file_id = segment_id_val / _segments_per_file;
    uint64_t segment_in_file = segment_id_val % _segments_per_file;
    uint64_t file_offset = segment_index_to_file_offset(segment_in_file);
    return segment_location{file_id, file_offset};
}

log_segment_id segment_manager_impl::file_location_to_segment_id(segment_location seg_loc) const noexcept {
    uint64_t segment_in_file = file_offset_to_segment_index(seg_loc.file_offset);
    return log_segment_id(seg_loc.file_id * _segments_per_file + segment_in_file);
}

size_t segment_manager_impl::file_offset_to_segment_index(uint64_t file_offset) const noexcept {
    return file_offset / _segment_size;
}

uint64_t segment_manager_impl::segment_index_to_file_offset(size_t segment_index) const noexcept {
    return segment_index * _segment_size;
}

std::vector<log_segment_id> segment_manager_impl::find_segments_for_compaction(double min_live_ratio, size_t max_segments) {
    std::vector<log_segment_id> result;
    result.reserve(max_segments);
    for (auto& desc : _segment_hist) {
        auto seg_id = desc_to_segment_id(desc);
        double live_ratio = 1.0 - static_cast<double>(desc.free_space()) / _segment_size;
        if (live_ratio < min_live_ratio) {
            result.push_back(seg_id);
            if (result.size() >= max_segments) {
                break;
            }
        } else {
            break;
        }
    }

    return result;
}

future<> segment_manager_impl::free_segment(log_segment_id segment_id) {
    auto holder = _async_gate.hold();

    logstor_logger.debug("Freeing segment {}", segment_id);

    // Update stats
    auto& desc = get_segment_descriptor(segment_id);
    _segment_hist.erase(desc);
    desc.reset();

    // Get file location for this segment
    auto [file_id, file_offset] = segment_id_to_file_location(segment_id);
    auto segment_index = file_offset_to_segment_index(file_offset);

    // Open file and zero out the segment
    auto file = co_await get_file_for_write(file_id);
    co_await format_file_region(file, file_offset, _segment_size);

    logstor_logger.debug("Zeroed segment {} ({} bytes) in file {} at offset {}",
                        segment_id, _segment_size, file_id, file_offset);

    _file_stats[file_id].free_segments.set(segment_index);
    logstor_logger.debug("Marked segment {} free in file {} at index {}",
                        segment_id, file_id, segment_index);

    _segment_freed_cv.signal();

    _stats.segments_freed++;
    _stats.segments_in_use--;
}

future<> segment_manager_impl::for_each_record(std::vector<log_segment_id> segments,
                                        std::function<future<>(log_location, log_structured_segment_record)> callback) {
    auto holder = _async_gate.hold();

    for (auto segment_id : segments) {
        auto [file_id, file_offset] = segment_id_to_file_location(segment_id);
        auto file = co_await get_file_for_read(file_id);
        auto fin = make_file_input_stream(std::move(file), file_offset, _segment_size,
            file_input_stream_options {
                .buffer_size = std::min<size_t>(_segment_size, 128 * 1024),
                .read_ahead = 0,
        });
        size_t current_position = 0;

        logstor_logger.debug("Reading records from segment {} at file {} offset {}",
                            segment_id, file_id, file_offset);

        constexpr size_t record_header_size = sizeof(uint32_t);
        constexpr size_t buffer_header_size = sizeof(uint32_t);

        while (current_position < _segment_size) {
            // Align to disk boundary
            auto skip_bytes = align_up(current_position, disk_alignment) - current_position;
            if (skip_bytes > 0) {
                co_await fin.skip(skip_bytes);
                current_position += skip_bytes;
            }

            if (current_position >= _segment_size) {
                break;
            }

            // Read buffer header (total data size in this buffer)
            auto buffer_header_buf = co_await fin.read_exactly(buffer_header_size);
            current_position += buffer_header_size;
            if (buffer_header_buf.size() < buffer_header_size) {
                break;
            }
            seastar::simple_memory_input_stream header_stream(buffer_header_buf.get(), buffer_header_buf.size());
            auto buffer_data_size = ser::deserialize(header_stream, std::type_identity<uint32_t>{});

            logstor_logger.trace("Reading buffer of size {} bytes in segment {}",
                                buffer_data_size, segment_id);

            if (buffer_data_size == 0) {
                // Empty buffer, skip to next aligned block
                continue;
            }

            // Now read record headers and data within this buffer
            size_t buffer_bytes_read = 0;
            while (buffer_bytes_read < buffer_data_size) {
                // Read size header
                auto size_buf = co_await fin.read_exactly(record_header_size);
                current_position += record_header_size;
                if (size_buf.size() < record_header_size) {
                    break;
                }
                seastar::simple_memory_input_stream size_stream(size_buf.get(), size_buf.size());
                auto data_size = ser::deserialize(size_stream, std::type_identity<uint32_t>{});
                buffer_bytes_read += record_header_size;

                logstor_logger.trace("Found record of size {} bytes in segment {}",
                                    data_size, segment_id);

                if (data_size == 0) {
                    // End of records in this buffer
                    break;
                }

                auto record_offset = current_position;
                auto record_buf = co_await fin.read_exactly(data_size);
                current_position += data_size;
                if (record_buf.size() < data_size) {
                    break;
                }
                buffer_bytes_read += data_size;

                seastar::simple_memory_input_stream record_stream(record_buf.get(), record_buf.size());
                auto record = ser::deserialize(record_stream, std::type_identity<log_structured_segment_record>{});

                log_location loc {
                    .segment = segment_id,
                    .offset = record_offset,
                    .size = data_size
                };

                // Invoke callback with location and record
                co_await callback(loc, std::move(record));

                // align up to next record
                auto padding = align_up(current_position, record_alignment) - current_position;
                if (padding > 0) {
                    co_await fin.skip(padding);
                    current_position += padding;
                    buffer_bytes_read += padding;
                }
            }

            // Skip any remaining padding in this buffer to reach next 4K boundary
            if (buffer_bytes_read < buffer_data_size) {
                auto remaining = buffer_data_size - buffer_bytes_read;
                co_await fin.skip(remaining);
                current_position += remaining;
            }
        }

        co_await fin.close();
    }
}

// segment_manager wrapper

segment_manager::segment_manager(segment_manager_config config)
    : _impl(std::make_unique<segment_manager_impl>(std::move(config)))
{ }

segment_manager::~segment_manager() = default;

segment_manager_impl& segment_manager::get_impl() noexcept {
    return *_impl;
}

const segment_manager_impl& segment_manager::get_impl() const noexcept {
    return *_impl;
}

future<> segment_manager::start() {
    return _impl->start();
}

future<> segment_manager::stop() {
    return _impl->stop();
}

future<log_location> segment_manager::write(bytes_view data, int64_t live_size) {
    return _impl->write(data, live_size);
}

future<log_structured_segment_record> segment_manager::read(log_location location) {
    return _impl->read(location);
}

void segment_manager::free_record(log_location location) {
    _impl->free_record(location);
}

std::vector<log_segment_id> segment_manager::find_segments_for_compaction(double min_live_ratio, size_t max_segments) {
    return _impl->find_segments_for_compaction(min_live_ratio, max_segments);
}

future<> segment_manager::free_segment(log_segment_id segment_id) {
    return _impl->free_segment(segment_id);
}

future<> segment_manager::for_each_record(std::vector<log_segment_id> segments,
                                        std::function<future<>(log_location, log_structured_segment_record)> callback) {
    return _impl->for_each_record(std::move(segments), std::move(callback));
}

void segment_manager::set_compaction_trigger(compaction_trigger_fn fn) {
    _impl->set_compaction_trigger(std::move(fn));
}

size_t segment_manager::get_segment_size() const noexcept {
    return _impl->get_segment_size();
}

}

template<>
size_t hist_key<replica::log_structured::segment_descriptor>(const replica::log_structured::segment_descriptor& desc) {
    return desc.free_space();
}
