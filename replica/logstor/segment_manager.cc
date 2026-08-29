/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/segment_io.hh"
#include "exceptions/exceptions.hh"
#include "replica/compaction_group.hh"
#include "backlog_controller.hh"
#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/types.hh"
#include "replica/logstor/compaction.hh"
#include <absl/container/flat_hash_map.h>
#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <system_error>
#include <linux/if_link.h>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/memory-data-source.hh>
#include <seastar/util/closeable.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include "replica/logstor/write_buffer.hh"
#include "utils/checked-file-impl.hh"
#include "utils/dynamic_bitset.hh"
#include "utils/serialized_action.hh"
#include "utils/lister.hh"
#include "replica/database.hh"

namespace replica::logstor {

using file_id_t = uint64_t;

class segment {
protected:
    log_segment_id _id;
    seastar::file _file;
    uint64_t _file_offset; // Offset within the shared file where this segment starts
    uint64_t _max_size;

public:
    segment(log_segment_id id, seastar::file file, uint64_t file_offset, uint64_t max_size);

    virtual ~segment() = default;

    log_segment_id id() const noexcept { return _id; }
    seastar::file& get_file() noexcept { return _file; }

protected:
    uint64_t absolute_offset(uint64_t relative_offset) const noexcept {
        return _file_offset + relative_offset;
    }
};

class writeable_segment : public segment {
    seastar::gate _write_gate;
    seastar::semaphore _append_sem{1};
    segment_ref _seg_ref;
    segment_sequence _seq_num;

    uint32_t _current_offset = 0; // next offset for write

    // Set when a write to this segment failed. The segment is retired: it accepts no
    // further appends, and reserve() returns nullopt so that the caller switches to a
    // new segment.
    bool _failed = false;

    future<> do_write(log_location , bytes_view data);

    bool can_fit(size_t data_size) const noexcept {
        return _current_offset + data_size <= _max_size;
    }

public:
    using segment::segment;

    void start(segment_ref, segment_sequence);

    future<> stop();

    struct append_reservation {
        log_location loc;
        seastar::semaphore_units<> units;
    };

    // Reserve the next append offset for this segment and keep the permit held
    // until the DMA write completes.
    // Returns nullopt if the data doesn't fit in the segment, or if the segment was
    // retired by a failed write. In both cases nothing was written and the caller is
    // expected to move on to another segment.
    future<std::optional<append_reservation>> reserve(size_t data_size);

    future<log_location> write_reserved(append_reservation reservation, bytes_view data);

    // Convenience helper for callers that do not need to split reservation and write.
    // Fails if the segment cannot fit the write.
    future<log_location> append(bytes_view data);

    size_t bytes_remaining() const noexcept {
        return _max_size - _current_offset;
    }

    seastar::gate::holder hold() {
        return _write_gate.hold();
    }

    segment_ref ref() {
        return _seg_ref;
    }

    segment_sequence seq_num() const noexcept {
        return _seq_num;
    }

    // Gives the segment a sequence number later than the one it was started with. Recovery
    // tie-breaks records of equal timestamp by the sequence number of the segment they are in, so a
    // segment that is handed out long before it is written has to be renumbered when it is sealed,
    // or it would lose the tie to every segment allocated in between.
    void set_seq_num(segment_sequence seq_num) noexcept {
        _seq_num = seq_num;
    }
};

segment::segment(log_segment_id id, seastar::file file, uint64_t file_offset, uint64_t max_size)
    : _id(id)
    , _file(std::move(file))
    , _file_offset(file_offset)
    , _max_size(max_size) {
}

void writeable_segment::start(segment_ref seg_ref, segment_sequence seq_num) {
    _seg_ref = std::move(seg_ref);
    _seq_num = seq_num;
}

future<> writeable_segment::stop() {
    if (_write_gate.is_closed()) {
        co_return;
    }
    co_await _write_gate.close();
}

future<std::optional<writeable_segment::append_reservation>> writeable_segment::reserve(size_t data_size) {
    if (_failed) [[unlikely]] {
        co_return std::nullopt;
    }

    std::optional<seastar::semaphore_units<>> units;
    try {
        units = co_await get_units(_append_sem, 1);
    } catch (...) {
        // The only way _append_sem breaks is a failed write retiring the segment, which
        // may have happened while we were waiting for the permit. Nothing was reserved,
        // so the caller can retry on another segment.
        co_return std::nullopt;
    }

    if (!can_fit(data_size)) {
        co_return std::nullopt;
    }

    append_reservation reservation {
        .loc = log_location {
            .segment = _id,
            .offset = _current_offset,
            .size = static_cast<uint32_t>(data_size)
        },
        .units = std::move(*units),
    };

    _current_offset += data_size;

    co_return std::move(reservation);
}

future<log_location> writeable_segment::write_reserved(append_reservation reservation, bytes_view data) {
    auto write_result = co_await coroutine::as_future(do_write(reservation.loc, data));
    if (write_result.failed()) {
        auto ex = write_result.get_exception();

        // if a write fails, fail also all later writes to the segment, since we assume no holes in a segment.
        // the segment is retired, so that callers switch to a new segment instead of failing forever.
        _failed = true;
        _append_sem.broken(ex);
        logstor_logger.warn("Write to segment {} seq {} failed: {}, retiring segment", _id, _seq_num, ex);

        co_await coroutine::return_exception_ptr(std::move(ex));
    }
    co_return reservation.loc;
}

future<log_location> writeable_segment::append(bytes_view data) {
    auto reservation = co_await reserve(data.size());
    if (!reservation) {
        co_return coroutine::return_exception(std::runtime_error(_failed
                ? fmt::format("Can't append write of size {} to segment {} retired by a failed write", data.size(), _id)
                : fmt::format("Can't append write of size {} to segment with {} bytes remaining", data.size(), bytes_remaining())));
    }
    co_return co_await write_reserved(std::move(*reservation), data);
}

future<> writeable_segment::do_write(log_location loc, bytes_view data) {
    utils::get_local_injector().inject("logstor_fail_segment_write", [] {
        throw std::runtime_error("segment write failed by injection");
    });

    utils::get_local_injector().inject("logstor_segment_write_io_error", [] {
        // Simulates a device error reported by the file layer: the handler fires the logstor
        // disk error signal, which isolates the node, and turns it into a storage_io_error.
        logstor_error_handler(std::make_exception_ptr(std::system_error(EIO, std::system_category(),
                "segment write I/O error by injection")));
    });

    const auto alignment = _file.disk_write_dma_alignment();
    const uint64_t total = data.size();
    auto base_offset = absolute_offset(loc.offset);
    uint64_t written = 0;

    while (written < total) {
        auto new_written = co_await _file.dma_write(
                base_offset + written, data.data() + written, total - written);

        written += new_written;
        if (written == total) {
            break;
        }

        written = align_down(written, alignment);
    }
}

using seg_ptr = lw_shared_ptr<writeable_segment>;

class file_manager {
    uint64_t _segments_per_file;

    // configured: file count allowed by the current configuration; new file ids
    // are allocated only below this limit.
    // actual: file count currently present on disk. Recovery may raise this
    // above configured if it finds existing files beyond the configured limit.
    // Such files remain readable and their live segments remain usable until
    // freed, but new files are never allocated beyond configured. Fully retired
    // files may be deleted from the end.
    struct {
        uint64_t configured;
        uint64_t actual;
    } _max_files;

    uint64_t _file_size;
    std::filesystem::path _base_dir;
    seastar::scheduling_group _sched_group;
    bool _format_on_startup;

    file_id_t _next_file_id{0};

    seastar::gate _async_gate;
    shared_future<> _next_file_formatter{make_ready_future<>()};

    std::vector<seastar::file> _open_read_files;

    std::unique_ptr<char[], seastar::free_deleter> _zero_buf;
    size_t _zero_buf_size{0};

public:
    file_manager(segment_manager_config cfg)
        : _segments_per_file(cfg.file_size / cfg.segment_size)
        , _max_files{cfg.disk_size / cfg.file_size, cfg.disk_size / cfg.file_size}
        , _file_size(cfg.file_size)
        , _base_dir(cfg.base_dir)
        , _sched_group(cfg.compaction_sg)
        , _format_on_startup(cfg.format_on_startup)
        , _open_read_files(static_cast<size_t>(_max_files.actual))
    {}

    future<> start();
    future<> stop();

    future<seastar::file> get_file_for_write(file_id_t);
    future<seastar::file> get_file_for_read(file_id_t);

    // The file of a read, if it is already open. Every read of a file after the first one finds it
    // here, and a read that does pays for no coroutine of its own to get at it. Returns an unset
    // file when it is not open yet, which is when get_file_for_read() has to open it.
    seastar::file opened_file_for_read(file_id_t file_id) const {
        if (file_id >= _open_read_files.size()) [[unlikely]] {
            on_internal_error(logstor_logger, "Attempted to access file beyond actual disk capacity");
        }
        return _open_read_files[file_id];
    }

    future<> format_file_region(seastar::file file, uint64_t offset, uint64_t size);
    future<> format_file(file_id_t);
    future<> recover_next_file(file_id_t);
    future<> remove_file(file_id_t);
    void set_actual_max_files(uint64_t actual_max_files);

    uint64_t allocated_file_count() const noexcept { return _next_file_id; }

    uint64_t segments_per_file() const noexcept { return _segments_per_file; }
    uint64_t configured_max_files() const noexcept { return _max_files.configured; }
    uint64_t actual_max_files() const noexcept { return _max_files.actual; }

    // the file names are ls_{shard_id}-{file_id}-Data.db
    static const sstring get_file_name_prefix() {
        return fmt::format("ls_{}-", this_shard_id());
    }

    std::filesystem::path get_file_path(file_id_t file_id) const {
        auto fname = fmt::format("{}{}-Data.db", get_file_name_prefix(), file_id);
        return _base_dir / fname;
    }

    std::optional<file_id_t> file_name_to_file_id(const std::string& fname) const;
};

future<> file_manager::start() {
    co_await seastar::recursive_touch_directory(_base_dir.string());
    _zero_buf_size = 128 * 1024;
    _zero_buf = allocate_aligned_buffer<char>(_zero_buf_size, 4096);
    std::memset(_zero_buf.get(), 0, _zero_buf_size);
}

future<> file_manager::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    co_await _async_gate.close();
}

// Formatting a new file grows the store, so its I/O deliberately does not go through
// logstor_error_handler: failing to grow (e.g. ENOSPC) only stops further allocation and
// must not isolate the node, while writes to already formatted files keep working.
future<> file_manager::format_file(file_id_t file_id) {
    auto file_path = get_file_path(file_id).string();
    bool file_exists = co_await seastar::file_exists(file_path);
    if (!file_exists) {
        // Create and format a temporary file, then move it to the final location
        auto tmp_path = file_path + ".tmp";
        auto tmp_file = co_await seastar::open_file_dma(tmp_path,
                seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate | seastar::open_flags::dsync);
        co_await tmp_file.allocate(0, _file_size);
        co_await format_file_region(tmp_file, 0, _file_size);
        co_await tmp_file.close();

        // move the temp file to the final location
        co_await seastar::rename_file(tmp_path, file_path);
    }
}

future<> file_manager::recover_next_file(file_id_t next_file_id) {
    _next_file_id = next_file_id;

    if (_format_on_startup) {
        auto holder = _async_gate.hold();
        file_id_t next_file_id = _next_file_id;
        co_await with_scheduling_group(_sched_group, [this, next_file_id] -> future<> {
            logstor_logger.info("Formatting {} logstor files", _max_files.configured);
            for (file_id_t file_id = next_file_id; file_id < _max_files.configured; ++file_id) {
                co_await format_file(file_id);
                _next_file_id = file_id + 1;
            }
        });
        _next_file_formatter = make_ready_future<>();
        co_return;
    }

    if (_next_file_id < _max_files.configured) {
        _next_file_formatter = with_gate(_async_gate, [this] {
            return with_scheduling_group(_sched_group, [this] {
                return format_file(_next_file_id);
            });
        });
    }
}

future<> file_manager::remove_file(file_id_t file_id) {
    if (file_id != _max_files.actual - 1) {
        on_internal_error(logstor_logger, fmt::format("Attempted to remove file {} that is not the last allocated file", file_id));
    }
    if (_max_files.actual <= _max_files.configured) {
        on_internal_error(logstor_logger, fmt::format("Attempted to remove file {} while actual max files {} is not above configured max files {}", file_id, _max_files.actual, _max_files.configured));
    }
    if (file_id < _open_read_files.size()) {
        if (file_id + 1 != _open_read_files.size()) {
            on_internal_error(logstor_logger, fmt::format("Attempted to remove file {} while higher file {} is still allocated", file_id, file_id + 1));
        }
        if (_open_read_files[file_id]) {
            co_await _open_read_files[file_id].close();
        }
        _open_read_files.pop_back();
    }
    co_await do_io_check(logstor_error_handler, [this, file_id] {
        return seastar::remove_file(get_file_path(file_id).string());
    });
    _max_files.actual--;
}

void file_manager::set_actual_max_files(uint64_t actual_max_files) {
    if (actual_max_files < _max_files.configured) {
        on_internal_error(logstor_logger, fmt::format("Attempted to set actual max files {} below configured max files {}", actual_max_files, _max_files.configured));
    }
    if (actual_max_files < _max_files.actual) {
        on_internal_error(logstor_logger, fmt::format("Attempted to reduce actual max files from {} to {}", _max_files.actual, actual_max_files));
    }
    _max_files.actual = actual_max_files;
    _open_read_files.resize(static_cast<size_t>(_max_files.actual));
}

future<seastar::file> file_manager::get_file_for_write(file_id_t file_id) {
    if (file_id >= _max_files.actual) {
        on_internal_error(logstor_logger, "Attempted to access file beyond actual disk capacity");
    }

    if (file_id == _next_file_id && file_id < _max_files.configured) {
        // allocate file_id and wait for it to be formatted, and start formatting
        // the next file in background
        co_await _next_file_formatter.get_future();

        if (file_id == _next_file_id) {
            _next_file_id++;

            if (_next_file_id < _max_files.configured) {
                _next_file_formatter = with_gate(_async_gate, [this] {
                    return with_scheduling_group(_sched_group, [this] {
                        return format_file(_next_file_id);
                    });
                });
            }
        }
    } else if (file_id >= _max_files.configured && file_id >= _next_file_id) {
        on_internal_error(logstor_logger, "Disk size limit reached, cannot allocate more files");
    } else if (file_id > _next_file_id) {
        on_internal_error(logstor_logger, "files must be allocated in sequential order");
    }

    auto file_path = get_file_path(file_id).string();
    auto file = co_await open_checked_file_dma(logstor_error_handler, file_path,
            seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::dsync);

    if (!_open_read_files[file_id]) {
        _open_read_files[file_id] = file;
    }

    co_return file;
}

future<seastar::file> file_manager::get_file_for_read(file_id_t file_id) {
    if (file_id >= _open_read_files.size()) {
        on_internal_error(logstor_logger, "Attempted to access file beyond actual disk capacity");
    }

    auto& cached_file = _open_read_files[file_id];
    if (cached_file) {
        co_return cached_file;
    }

    auto file = co_await open_checked_file_dma(logstor_error_handler,
        get_file_path(file_id).string(),
        seastar::open_flags::ro
    );

    _open_read_files[file_id] = file;

    co_return std::move(file);
}

future<> file_manager::format_file_region(seastar::file file, uint64_t offset, uint64_t size) {
    // Write zeros to entire region using the pre-allocated zero buffer
    uint64_t remaining = size;
    uint64_t current_offset = offset;

    while (remaining > 0) {
        auto write_size = std::min<uint64_t>(remaining, _zero_buf_size);
        auto written = co_await file.dma_write(current_offset, _zero_buf.get(), write_size);

        current_offset += written;
        remaining -= written;
    }
}

std::optional<file_id_t> file_manager::file_name_to_file_id(const std::string& fname) const {
    std::string prefix = get_file_name_prefix();
    std::string suffix = "-Data.db";
    if (fname.starts_with(prefix) && fname.ends_with(suffix)) {
        // Extract file_id between prefix and suffix
        size_t start = prefix.size();
        size_t end = fname.size() - suffix.size();
        std::string file_id_str = fname.substr(start, end - start);
        try {
            return static_cast<file_id_t>(std::stoull(file_id_str));
        } catch (...) {
            return std::nullopt;
        }
    }
    return std::nullopt;
}

// Maps compaction_shares_pressure() to CPU shares. The backlog is the pressure itself, so control
// point inputs are in [0, 1]. The maximum output is logstor_compaction_max_shares.
//
// Each control point sits on an anchor of the pressure ramp:
//
//   - pressure 0, at or above automatic compaction's stop watermark: nothing to reclaim, so only
//     explicitly submitted compaction runs, at the same floor the sstable controller uses;
//   - pressure compaction_shares_pressure_at_target, at the free-segment target: the intended
//     steady-state operating point;
//   - pressure 1, once half the target has been consumed: compaction is losing against the write
//     rate and gets the maximum.
//
// Shares therefore grow gently across the trigger's hysteresis band, where the target has already
// been met, and six times faster below the target. The exact output at the target is not critical:
// the loop settles at whatever shares match the write rate, and this constant only decides how far
// below the target it settles.
class logstor_compaction_controller : public backlog_controller {
public:
    static constexpr float idle_shares = 50.0f;
    static constexpr float target_shares = 200.0f;
    // Below this the scheduling group's shares stop being meaningful, so a cap this low is treated
    // as a misconfiguration rather than as a choice to honour.
    static constexpr float min_max_shares = 1.0f;

    logstor_compaction_controller(scheduling_group sg, float static_shares, float max_shares,
            std::chrono::milliseconds interval, std::function<float()> current_backlog)
        : backlog_controller(std::move(sg), interval, make_control_points(max_shares),
                std::move(current_backlog), static_shares) {
    }

    void set_max_shares(float max_shares) {
        _control_points = make_control_points(max_shares);
    }

private:
    // The ramp for a given cap, which is the only input: the ramp has no default of its own, so a
    // controller always follows logstor_compaction_max_shares.
    //
    // The lower two points keep their constants, since the cap is meant to bound the peak rather
    // than move the steady-state operating point. They only follow the cap down when keeping them
    // would make the ramp non-monotone: scaling them is better than raising the configured cap back
    // up, because the ramp then still responds to pressure instead of flattening into a fixed share
    // count. The fractions are chosen to reproduce the constants exactly at any cap of 300 shares or
    // more, so the coupling is invisible above the point where it is needed.
    static std::vector<control_point> make_control_points(float max_shares) {
        if (max_shares < min_max_shares) {
            logstor_logger.warn("logstor_compaction_max_shares of {} is too low, using {}", max_shares, min_max_shares);
            max_shares = min_max_shares;
        }
        const float target = std::min(target_shares, max_shares * 2.0f / 3.0f);
        return {
                {0.0f, std::min(idle_shares, target / 4.0f)},
                {compaction_shares_pressure_at_target, target},
                {1.0f, max_shares},
        };
    }
};

class compaction_manager_impl : public compaction_manager {
public:

    struct compaction_config {
        bool compaction_enabled;
        // Upper bound on the batch cap; the cap actually used is derived from the free-segment
        // target by make_compaction_limits().
        size_t max_segments_per_compaction;
        seastar::scheduling_group compaction_sg;
        utils::updateable_value<float> compaction_static_shares;
        utils::updateable_value<float> compaction_max_shares;
        seastar::scheduling_group separator_sg;
        seastar::scheduling_group split_compaction_sg;
    };

private:
    segment_manager_impl& _sm;
    compaction_config _cfg;

    bool _admission_closed{false};

    bool can_submit_compaction() const noexcept {
        return !_admission_closed && _cfg.compaction_enabled;
    }

    struct stats {
        uint64_t compaction_segments_in{0};
        uint64_t compaction_segments_out{0};
        uint64_t compaction_records_skipped{0};
        uint64_t compaction_records_rewritten{0};
        uint64_t compaction_bytes_read{0};
        uint64_t compaction_failures{0};
        uint64_t compaction_batches_refused{0};
        uint64_t separator_buffer_flushed{0};
        uint64_t separator_segments_freed{0};
        uint64_t separator_flush_failures{0};
    } _stats;

    logstor_compaction_controller _shares_controller;
    utils::observer<float> _compaction_static_shares_observer;
    utils::observer<float> _compaction_max_shares_observer;

    struct group_compaction_state {
        shared_future<> completion{make_ready_future<>()};
        abort_source as;
        int compaction_disabled_counter{0};

        bool running() const noexcept {
            return !completion.available();
        }
    };

    struct compaction_candidate {
        logstor_group* group;
        std::vector<log_segment_id> segments;
        compaction_candidate_score score;
    };

    using group_compaction_state_map = absl::flat_hash_map<logstor_group*, std::unique_ptr<group_compaction_state>>;
    group_compaction_state_map _groups;

    // The top of the segment statistics rollup of this shard: the tables account into it, so it holds
    // the statistics of the segments owned by every group of every table on the shard, registered here
    // or not. It outlives the tables, and therefore the groups accounting into it, since logstor is
    // created before them and destroyed after them.
    segment_stats_node _segment_stats;

    shared_future<> _auto_compaction_completion{make_ready_future<>()};
    // Sized for the static maximum; a run holds back the difference between it and the current
    // limit for its duration, see run_auto_compaction().
    seastar::semaphore _auto_compaction_sem{max_auto_compaction_parallelism};

    bool auto_compaction_active() const noexcept {
        return !_auto_compaction_completion.available();
    }

public:
    compaction_manager_impl(segment_manager_impl& sm, compaction_config cfg)
        : _sm(sm)
        , _cfg(std::move(cfg))
        , _shares_controller(
                _cfg.compaction_sg,
                _cfg.compaction_static_shares.get(),
                _cfg.compaction_max_shares.get(),
                std::chrono::milliseconds(250),
                [this] {
                    return compaction_pressure();
                }
            )
        , _compaction_static_shares_observer(_cfg.compaction_static_shares.observe([this] (float new_shares) {
            return _shares_controller.update_static_shares(new_shares);
        }))
        , _compaction_max_shares_observer(_cfg.compaction_max_shares.observe([this] (float new_max_shares) {
            logstor_logger.info("Updating logstor compaction max shares to {}", new_max_shares);
            _shares_controller.set_max_shares(new_max_shares);
        }))
    {}

    future<> start();
    future<> stop();

    const stats& get_stats() const noexcept { return _stats; }
    float compaction_pressure() const noexcept;

    future<> flush_separator_buffer(separator_buffer&, logstor_group&) override;
    future<owned_write_buffer> allocate_separator_buffer() override;
    future<> flush_all_separator_buffers(std::optional<segment_sequence>);

    void rotate_direct_buffer(logstor_group&) override;
    future<> release_direct_buffers(logstor_group&) override;
    future<> drain_all_direct_buffers();
    future<> promote_direct_writes_for_test(logstor_group&) override;
    // One deadline pass over every group of the shard, and one controller pass. They are separate
    // because the controller waits and the deadlines must not.
    void tick_direct_deadlines(seastar::lowres_clock::time_point now);
    future<> run_direct_controller();

    // How many of this shard's groups are taking direct writes. Counted rather than tracked: it is
    // read when a group is promoted, which is rare, and the groups of a shard are few.
    size_t direct_hot_group_count() const noexcept {
        return std::ranges::count_if(_groups, [] (const auto& entry) {
            return entry.first->direct_writes_enabled();
        });
    }

    void add(logstor_group&) override;

    bool contains(logstor_group& cg) const noexcept override {
        return _groups.contains(&cg);
    }

    segment_stats_node& shard_segment_stats() noexcept override {
        return _segment_stats;
    }

    void submit(logstor_group&) override;
    future<> stop_ongoing_compactions(logstor_group&) override;
    future<> remove(logstor_group&) override;
    future<compaction_reenabler> disable_compaction(logstor_group&) override;
    compaction_reenabler disable_compaction_no_wait(logstor_group&) override;
    future<> submit_normal_compaction(logstor_group&);
    future<> submit_split_compaction(logstor_group&, mutation_writer::classify_by_token_group, split_target_group) override;

    void schedule_auto_compaction();

    // Refreshes the cached free segment watermarks and the compaction limits derived from them.
    // Called from start() and whenever trigger_compaction_threshold changes; the segment count and
    // the configured batch bound are fixed for the lifetime of the segment manager, so the
    // threshold is the only other input.
    void refresh_free_segment_watermarks() noexcept;

private:

    // Cached result of make_free_segment_watermarks(). should_run_auto_compaction() calls
    // get_free_segment_watermarks() on every write, so caching avoids repeating the same
    // computation on every hot-path call.
    free_segment_watermarks _free_segment_watermarks{};
    compaction_limits _compaction_limits{};

    const free_segment_watermarks& get_free_segment_watermarks() const noexcept {
        return _free_segment_watermarks;
    }

    bool should_run_auto_compaction() noexcept;
    future<> run_auto_compaction();

    std::optional<compaction_candidate> select_segments_for_compaction(logstor_group&);
    future<std::vector<compaction_candidate>> find_top_compaction_candidates(size_t);

    future<> submit_group_compaction(logstor_group&, std::function<future<>(group_compaction_state&)>, scheduling_group);
    future<> do_compaction(logstor_group&, abort_source&);
    future<> do_split_compaction(logstor_group&, mutation_writer::classify_by_token_group, split_target_group, abort_source&);

    group_compaction_state& get_group_state(logstor_group&);
    group_compaction_state* find_group_state(logstor_group&) noexcept;

};

future<> compaction_manager_impl::start() {
    refresh_free_segment_watermarks();
    co_return;
}

future<> compaction_manager_impl::stop() {
    if (_admission_closed) {
        co_return;
    }
    _admission_closed = true;

    for (auto& [cg, state] : _groups) {
        state->as.request_abort();
    }
    co_await coroutine::parallel_for_each(_groups, [] (auto& entry) -> future<> {
        return entry.second->completion.get_future().handle_exception([] (std::exception_ptr) {});
    });

    auto f = co_await coroutine::as_future(_auto_compaction_completion.get_future());
    f.ignore_ready_future();

    co_await _shares_controller.shutdown();

    _groups.clear();
}

enum class write_source {
    normal_write,
    compaction,
    separator,
    streaming,
    direct_write,
};

static constexpr size_t write_source_count = 5;

// Why a write of a group that is taking direct writes could not be taken by the direct path. They
// are counted apart because they call for different answers: no buffer means the shard is short of
// segments or of write buffer memory, a flush in flight means the group outruns what two buffers of
// a segment each can carry between flushes, and a record too large is neither.
enum class direct_fallback_reason {
    no_buffer,
    flush_in_flight,
    record_too_large,
};

static constexpr size_t direct_fallback_reason_count = 3;

// Labels the direct_fallbacks counter, so that a dashboard can sum the reasons or tell them apart.
static const seastar::metrics::label direct_fallback_reason_label("reason");

static sstring direct_fallback_reason_to_string(direct_fallback_reason reason) {
    switch (reason) {
        case direct_fallback_reason::no_buffer: return "no_buffer";
        case direct_fallback_reason::flush_in_flight: return "flush_in_flight";
        case direct_fallback_reason::record_too_large: return "record_too_large";
    }
    return "unknown";
}

static sstring write_source_to_string(write_source src) {
    switch (src) {
        case write_source::normal_write: return "normal_write";
        case write_source::compaction: return "compaction";
        case write_source::separator: return "separator";
        case write_source::streaming: return "streaming";
        case write_source::direct_write: return "direct_write";
    }
    return "unknown";
}

class segment_pool {

    seastar::queue<seg_ptr> _segments;
    size_t _reserved_for_compaction;
    seastar::condition_variable _segment_available;

public:

    struct stats {
        uint64_t segments_put{0};
        std::array<uint64_t, write_source_count> segments_get{0};
        uint64_t normal_segments_wait{0};
    } _stats;

    segment_pool(size_t pool_size, size_t reserved_for_compaction)
        : _segments(pool_size)
        , _reserved_for_compaction(reserved_for_compaction)
    {}

    future<> start() {
        co_return;
    }

    future<> stop() {
        _segments.abort(std::make_exception_ptr(abort_requested_exception()));
        _segment_available.broken();
        co_return;
    }

    future<> put(seg_ptr seg) {
        co_await _segments.push_eventually(std::move(seg));
        _segment_available.broadcast();
        _stats.segments_put++;
    }

    future<seg_ptr> get_segment(write_source src) {
        seg_ptr seg;
        if (src == write_source::compaction) {
            _stats.segments_get[static_cast<size_t>(src)]++;
            co_return co_await _segments.pop_eventually();
        }
        if (_segments.size() <= _reserved_for_compaction) {
            if (src == write_source::normal_write) {
                _stats.normal_segments_wait++;
            }
            while (_segments.size() <= _reserved_for_compaction) {
                co_await _segment_available.wait([this] {
                    return _segments.size() > _reserved_for_compaction;
                });
            }
        }
        _stats.segments_get[static_cast<size_t>(src)]++;
        co_return _segments.pop();
    }

    // Hands out a segment only if one can be taken without waiting, for a caller that has something
    // to do other than wait for the disk to give space back - the direct write path, which goes on
    // writing through the shared active segment until a segment of its own can be had.
    std::optional<seg_ptr> try_get_segment(write_source src) {
        if (available_segment_count(src) == 0) {
            return std::nullopt;
        }
        _stats.segments_get[static_cast<size_t>(src)]++;
        return _segments.pop();
    }

    size_t available_segment_count(write_source src) const noexcept {
        switch (src) {
            case write_source::compaction:
                return _segments.size();
            case write_source::normal_write:
            case write_source::separator:
            case write_source::streaming:
            case write_source::direct_write:
                return _segments.size() > _reserved_for_compaction ? _segments.size() - _reserved_for_compaction : 0;
        }
    }

    size_t size() const noexcept {
        return _segments.size();
    }

    const stats& get_stats() const noexcept {
        return _stats;
    }
};

struct separator_task {
    std::vector<write_buffer::record_in_buffer> records;
    // Where the buffer holding the records was written, which is what the records' own locations are
    // relative to.
    log_location buffer_location{};
    segment_ref seg_ref;
    segment_sequence seq_num{};
    utils::phased_barrier::operation write_op;
};

class segment_manager_impl {

    struct stats {
        std::array<uint64_t, write_source_count> bytes_written{0};
        std::array<uint64_t, write_source_count> data_bytes_written{0};
        uint64_t write_failures{0};
        uint64_t bytes_read{0};
        uint64_t bytes_freed{0};
        uint64_t segments_allocated{0};
        uint64_t segments_freed{0};
        uint64_t compaction_bytes_written{0};
        uint64_t compaction_data_bytes_written{0};
        uint64_t separator_bytes_written{0};
        uint64_t separator_data_bytes_written{0};
        uint64_t live_record_bytes{0};
        uint64_t live_record_count{0};
        uint64_t separator_task_failures{0};
        uint64_t segment_free_failures{0};
        uint64_t direct_records_written{0};
        std::array<uint64_t, direct_fallback_reason_count> direct_fallbacks{0};
        uint64_t direct_read_hits{0};
        uint64_t direct_flush_failures{0};
        // The memory of the buffers whose write failed, which the shard keeps so that the records
        // they hold stay readable and therefore never gets back.
        uint64_t direct_failed_buffer_bytes{0};
        // Which of the two reasons wrote a direct buffer out. A shard whose buffers mostly go out
        // on the deadline is one whose groups do not fill a segment per period, so the segments it
        // writes are partly empty and its groups belong on the ordinary path.
        uint64_t direct_full_flushes{0};
        uint64_t direct_deadline_flushes{0};
        // What the direct buffers of the shard hold that is not on the disk yet, which is what a
        // crash would lose. Maintained here rather than summed over the groups on demand, because a
        // metric callback cannot walk them.
        uint64_t direct_unflushed_bytes{0};
    };

    file_manager _file_mgr;
    compaction_manager_impl _compaction_mgr;

    segment_manager_config _cfg;
    uint64_t _segments_per_file;

    // The shifts segment_id_to_file_location() locates a segment with, set when the segments per
    // file and the segment size are both powers of two.
    struct pow2_segment_layout {
        unsigned segments_per_file_log2;
        unsigned segment_size_log2;
    };
    std::optional<pow2_segment_layout> _pow2_layout;

    static std::optional<pow2_segment_layout> make_pow2_layout(uint64_t segments_per_file, uint64_t segment_size) noexcept {
        if (!std::has_single_bit(segments_per_file) || !std::has_single_bit(segment_size)) {
            return std::nullopt;
        }
        return pow2_segment_layout{
            .segments_per_file_log2 = static_cast<unsigned>(std::countr_zero(segments_per_file)),
            .segment_size_log2 = static_cast<unsigned>(std::countr_zero(segment_size)),
        };
    }

    // configured: segment count allowed by the current configuration; new
    // segment ids are allocated only below this limit.
    // actual: segment slots currently present on disk. Recovery may raise this
    // above configured if it finds existing segments beyond the configured
    // limit. Those segments remain usable until freed; once freed, beyond-
    // configured segments become retired and are not allocated again.
    struct {
        uint64_t configured;
        uint64_t actual;
    } _max_segments;

    uint64_t _next_new_segment_id{0};

    stats _stats;
    seastar::metrics::metric_groups _metrics;

    static constexpr size_t segment_pool_size = 128;

    std::vector<segment_descriptor> _segment_descs;
    seastar::circular_buffer<log_segment_id> _free_segments;

    seg_ptr _active_segment;
    segment_pool _segment_pool;
    std::optional<shared_future<>> _switch_segment_fut;
    segment_sequence _next_segment_seq{1};

    seastar::gate _async_gate;
    future<> _reserve_replenisher{make_ready_future<>()};
    seastar::condition_variable _segment_freed_cv;

    write_buffer_pool _compaction_buffer_pool;
    write_buffer_pool _separator_buffer_pool;
    abort_source _separator_buffer_abort;

    // How many groups the memory budget of the direct path has room for, which is fixed for the
    // lifetime of the shard: the pool below is sized for exactly this many.
    size_t _max_hot_groups;
    write_buffer_pool _direct_buffer_pool;
    // The buffers of the groups that are taking direct writes, by the id of the segment each one is
    // bound to. A read of a record that is still only in memory is served out of these, which is
    // what makes a direct write visible as soon as it is acknowledged.
    absl::flat_hash_map<uint32_t, const write_buffer*> _direct_readable;
    future<> _direct_sync_fiber{make_ready_future<>()};
    abort_source _direct_abort;
    // Buffers whose write to the disk failed. Their records were acknowledged and this memory is
    // the only copy left of them, so the buffers stay registered above and are not given back to
    // the pool - the one place the direct path deliberately holds on to a buffer for good.
    std::vector<direct_write_buffer> _direct_failed_buffers;
    // Set once those buffers hold the whole memory budget of the path, which turns it off for the
    // rest of the life of the shard, see note_direct_flush_failure().
    bool _direct_writes_stopped{false};

    static constexpr size_t separator_queue_depth = 16;

    seastar::queue<separator_task> _separator_task_queue;
    seastar::semaphore _separator_enqueue_sem{1};
    future<> _separator_fiber{make_ready_future<>()};

    utils::phased_barrier _writes_phaser{"logstor_sm_writes"};

    utils::observer<double> _trigger_threshold_observer;

public:
    static constexpr size_t block_alignment = ondisk::block_alignment;

    explicit segment_manager_impl(segment_manager_config);

    segment_manager_impl(const segment_manager_impl&) = delete;
    segment_manager_impl& operator=(const segment_manager_impl&) = delete;

    future<> do_recovery(replica::database&);
    future<> do_recovery_for_test();

    future<> start();
    future<> stop();

    future<> write(write_buffer&);
    future<> write_full_segment(write_buffer&, logstor_group&, write_source);
    // Writes a buffer out into a segment that has already been allocated, and links the segment
    // into the group. Split out of write_full_segment() for a caller that holds the segment from
    // before the buffer was filled, rather than taking one to write the buffer it has.
    future<> write_full_segment_tail(seg_ptr, write_buffer&, logstor_group&, write_source);

    // The direct write path, see direct_write_buffer.
    std::optional<log_location> try_write_direct(logstor_group&, const log_record_header_view&, bytes_view value);
    void rotate_direct_buffer(logstor_group&);
    future<> flush_direct_buffer(logstor_group&, direct_write_buffer full);
    // Gives a slot a buffer and a segment of the group to put it in, if both can be had without
    // waiting. A group whose slot stays unbound writes through the shared active segment instead,
    // which is what a caller does when this says no; the sync fiber retries. Synchronous, so a
    // flush never has to wait for the next segment before it can report the records durable.
    bool try_bind_direct_slot(logstor_group&, direct_write_buffer& slot);
    future<> release_direct_slot(direct_write_buffer& slot);
    future<> release_direct_buffers(logstor_group&);
    // Gives the group two buffers to write into and counts it as hot, unless the shard has no room
    // for another hot group. Says whether it did.
    bool promote_direct_group(logstor_group&);
    // Binds what the group is missing and writes out a buffer whose deadline has come. Synchronous
    // and never fails, so one group's disk cannot delay another group's deadline - which is what
    // bounds how much a crash of this shard would lose.
    void tick_direct_deadlines(logstor_group&, seastar::lowres_clock::time_point now);
    // Whether the group still writes fast enough to keep its buffers, or fast enough to be given
    // them, over the period that just ended. Waits for a demotion to be drained, so it runs behind
    // the deadlines rather than in front of them.
    future<> run_direct_controller(logstor_group&);
    future<> run_direct_sync_fiber();
    uint64_t direct_hot_threshold_bytes() const noexcept {
        return _cfg.direct_hot_threshold_bytes ? _cfg.direct_hot_threshold_bytes : _cfg.segment_size / 2;
    }
    // The most groups that may be hot at once: what the memory budget of the direct path holds,
    // given the two buffers of a segment each that a hot group takes.
    static size_t max_hot_groups(const segment_manager_config& cfg) noexcept {
        return cfg.direct_group_writes ? cfg.direct_write_memory / (2 * cfg.segment_size) : 0;
    }
    // Whether the direct write path is on: the durability mode has to allow it, the memory budget
    // has to have room for at least one hot group, and the disk has to have been taking the writes.
    bool direct_writes_enabled() const noexcept {
        return _max_hot_groups > 0 && !_direct_writes_stopped;
    }

    // Takes a buffer whose write failed into the memory the path keeps for good, and turns the path
    // off once those buffers hold as much of it as the path was budgeted to write with.
    void note_direct_flush_failure(direct_write_buffer failed);

    future<temporary_buffer<char>> read_record_bytes(log_location);

    void on_add_record(log_location) noexcept;
    void on_free_record(log_location) noexcept;

    template <record_consumer_like RecordConsumer>
    future<> for_each_record(log_segment_id segment_id,
                            std::function<want_data(log_location, const log_record_header&)> on_header,
                            RecordConsumer on_record)
    {
        return scan_segment(segment_id,
            [] (const segment_header&) { return make_ready_future<>(); },
            std::move(on_header), std::move(on_record));
    }

    template <std::ranges::input_range Segments, record_consumer_like RecordConsumer>
        requires std::same_as<std::ranges::range_value_t<Segments>, log_segment_id>
    future<> for_each_record(Segments&& segments,
                            std::function<want_data(log_location, const log_record_header&)> on_header,
                            RecordConsumer on_record)
    {
        std::vector<log_segment_id> sorted_segments(std::ranges::begin(segments), std::ranges::end(segments));
        std::ranges::sort(sorted_segments);

        for (auto segment_id : sorted_segments) {
            co_await for_each_record(segment_id, on_header, on_record);
        }
    }

    future<> load_segment(replica::database&, log_segment_id);
    future<> recover_segment(replica::database&, log_segment_id, primary_index::entry_cmp_fn cmp, std::function<void(const segment_header&)> on_header);
    future<> add_segment_to_compaction_group(replica::database&, segment_descriptor&);

    compaction_manager& get_compaction_manager() noexcept {
        return _compaction_mgr;
    }

    const compaction_manager& get_compaction_manager() const noexcept {
        return _compaction_mgr;
    }

    uint64_t get_segment_size() const noexcept {
        return _cfg.segment_size;
    }

    future<> discard_segments(logstor_group&);

    size_t get_memory_usage() const noexcept {
        return sizeof(_segment_descs);
    }

    uint64_t get_disk_usage() const noexcept {
        return _file_mgr.allocated_file_count() * _cfg.file_size;
    }

    uint64_t get_segment_pool_size() const noexcept {
        return _max_segments.configured * _cfg.segment_size;
    }

    future<owned_write_buffer> allocate_separator_buffer() {
        return _separator_buffer_pool.allocate(_separator_buffer_abort);
    }

    void update_group_count(size_t group_count) {
        _separator_buffer_pool.set_capacity(group_count + separator_flush_reserve);
    }

    future<> await_pending_writes() {
        return _writes_phaser.advance_and_await();
    }

    future<utils::chunked_vector<segment_snapshot>> make_snapshot(logstor_group&);

    future<seastar::input_stream<char>> create_segment_input_stream(log_segment_id segment_id, const seastar::file_input_stream_options& opts);
    future<std::unique_ptr<segment_stream_sink>> create_segment_output_stream(replica::database&);

    uint64_t allocatable_new_segment_count() const noexcept {
        return _next_new_segment_id < _max_segments.configured ? _max_segments.configured - _next_new_segment_id : 0;
    }

    uint64_t available_segment_count() const noexcept {
        return static_cast<uint64_t>(_free_segments.size()) + static_cast<uint64_t>(_segment_pool.size()) + allocatable_new_segment_count();
    }

    uint64_t available_segment_count(write_source src) const noexcept {
        return static_cast<uint64_t>(_free_segments.size()) + static_cast<uint64_t>(_segment_pool.available_segment_count(src)) + allocatable_new_segment_count();
    }

    uint64_t get_total_segment_count() const noexcept {
        return _max_segments.actual;
    }

    void set_actual_max_segments(uint64_t actual_max_segments) {
        _max_segments.actual = actual_max_segments;
        _segment_descs.resize(static_cast<size_t>(_max_segments.actual));
        _free_segments.reserve(static_cast<size_t>(_max_segments.actual));
    }

private:

    future<> replenish_reserve();
    future<seg_ptr> allocate_segment();

    future<> request_segment_switch();
    future<> switch_active_segment();
    segment_sequence allocate_segment_seq() noexcept {
        return _next_segment_seq++;
    }
    future<> run_separator_fiber();

    future<> write_to_separator(std::vector<write_buffer::record_in_buffer>&, log_location buffer_location, segment_ref, segment_sequence);

    future<std::optional<segment_header>> read_segment_header(log_segment_id);

    // Sequentially scans one segment and invokes callbacks for the decoded
    // contents.
    //
    // `segment_id` selects the on-disk segment to read.
    // `header_callback` is invoked once per buffer with the decoded segment header.
    // `on_header` is called for each record header and returns whether the record
    // payload should be read and passed to `on_record`, or skipped.
    // `on_record` is invoked only for records whose payload was requested.
    template <record_consumer_like RecordConsumer>
    future<> scan_segment(log_segment_id segment_id,
                          std::function<future<>(const segment_header&)> header_callback,
                          std::function<want_data(log_location, const log_record_header&)> on_header,
                          RecordConsumer on_record);

    segment_ref make_segment_ref(log_segment_id seg_id) {
        auto& desc = get_segment_descriptor(seg_id);
        ++desc.ref_count;

        return segment_ref(seg_id,
            [this, seg_id] {
                auto& desc = get_segment_descriptor(seg_id);
                if (--desc.ref_count == 0) {
                    return free_segment(seg_id);
                }
            },
            [this, seg_id] {
                ++_stats.segment_free_failures;
                logstor_logger.warn("Segment {} can't be freed", seg_id);
            }
        );
    }

    future<seg_ptr> get_segment(write_source src) {
        seg_ptr seg = co_await _segment_pool.get_segment(src);
        seg->start(make_segment_ref(seg->id()), allocate_segment_seq());
        co_return seg;
    }

    // See segment_pool::try_get_segment(). Synchronous, so a caller can decide against a segment
    // without a yield between asking and going on without one.
    std::optional<seg_ptr> try_get_segment(write_source src) {
        auto seg = _segment_pool.try_get_segment(src);
        if (seg) {
            (*seg)->start(make_segment_ref((*seg)->id()), allocate_segment_seq());
        }
        return seg;
    }

    void free_segment(log_segment_id) noexcept;

    segment_descriptor& get_segment_descriptor(log_segment_id segment_id) noexcept {
        return _segment_descs[segment_id.value];
    }

    segment_descriptor& get_segment_descriptor(log_location loc) noexcept {
        return _segment_descs[loc.segment.value];
    }

    log_segment_id desc_to_segment_id(const segment_descriptor& desc) const noexcept {
        uint64_t index = &desc - &_segment_descs[0];
        return log_segment_id(index);
    }

    struct segment_location {
        file_id_t file_id;
        uint64_t file_offset;
    };

    uint64_t file_offset_to_segment_index(uint64_t file_offset) const noexcept {
        return file_offset / _cfg.segment_size;
    }

    uint64_t segment_index_to_file_offset(uint64_t segment_index) const noexcept {
        return segment_index * _cfg.segment_size;
    }

    // Every read of a record starts here, so the division by the segments per file and the
    // multiplication by the segment size are a shift and a mask when both are powers of two, which
    // is what a configuration uses. The general form runs when they are not.
    segment_location segment_id_to_file_location(log_segment_id segment_id) const noexcept {
        if (_pow2_layout) [[likely]] {
            const auto file_id = segment_id.value >> _pow2_layout->segments_per_file_log2;
            const auto segment_index = segment_id.value & ((uint64_t(1) << _pow2_layout->segments_per_file_log2) - 1);
            return segment_location{static_cast<file_id_t>(file_id), segment_index << _pow2_layout->segment_size_log2};
        }
        file_id_t file_id = segment_id.value / _segments_per_file;
        uint64_t file_offset = (segment_id.value % _segments_per_file) * _cfg.segment_size;
        return segment_location{file_id, file_offset};
    }

    auto segments_in_file(file_id_t file_id) const noexcept {
        return std::views::iota(file_id * _segments_per_file, (file_id + 1) * _segments_per_file)
            | std::views::transform([] (uint64_t i) {
                return log_segment_id(i);
            });
    }

    friend class compaction_manager_impl;
    friend struct compaction_buffer;
    friend class segment_stream_sink_impl;
};

float compaction_manager_impl::compaction_pressure() const noexcept {
    return compaction_shares_pressure(_sm.available_segment_count(write_source::normal_write), get_free_segment_watermarks());
}

void compaction_manager_impl::refresh_free_segment_watermarks() noexcept {
    _free_segment_watermarks = make_free_segment_watermarks(_sm._max_segments.configured, _sm._cfg.trigger_compaction_threshold());
    _compaction_limits = make_compaction_limits(_free_segment_watermarks, _cfg.max_segments_per_compaction);

    logstor_logger.debug("Free segment target {} (stop at {}), compaction parallelism {}, up to {} segments per compaction",
            _free_segment_watermarks.low, _free_segment_watermarks.high,
            _compaction_limits.auto_parallelism, _compaction_limits.batch_cap);
}

compaction_manager_impl::group_compaction_state& compaction_manager_impl::get_group_state(logstor_group& cg) {
    auto it = _groups.find(&cg);
    if (it == _groups.end()) {
        on_internal_error(logstor_logger, format("compaction_state for logstor group {} [{}] does not exist", cg.table_id(), fmt::ptr(&cg)));
    }
    return *it->second;
}

compaction_manager_impl::group_compaction_state* compaction_manager_impl::find_group_state(logstor_group& cg) noexcept {
    auto it = _groups.find(&cg);
    return it != _groups.end() ? it->second.get() : nullptr;
}

void compaction_manager_impl::add(logstor_group& cg) {
    auto [it, inserted] = _groups.try_emplace(&cg, std::make_unique<group_compaction_state>());
    if (!inserted) {
        on_internal_error(logstor_logger, format("compaction_state for logstor group {} [{}] already exists", cg.table_id(), fmt::ptr(&cg)));
    }
    // A group that is not registered here can neither be flushed by flush_all_separator_buffers()
    // nor accounted for in the separator buffer pool, so registering it is what makes it a group
    // the separator may write to.
    cg.enable_separator_writes();
    _sm.update_group_count(_groups.size());
}

future<owned_write_buffer> compaction_manager_impl::allocate_separator_buffer() {
    return _sm.allocate_separator_buffer();
}

segment_manager_impl::segment_manager_impl(segment_manager_config config)
    : _file_mgr(config)
    , _compaction_mgr(*this, compaction_manager_impl::compaction_config{
            .compaction_enabled = config.compaction_enabled,
            .max_segments_per_compaction = config.max_segments_per_compaction,
            .compaction_sg = config.compaction_sg,
            .compaction_static_shares = config.compaction_static_shares,
            .compaction_max_shares = config.compaction_max_shares,
            .separator_sg = config.separator_sg,
            .split_compaction_sg = config.split_compaction_sg
        })
    , _cfg(config)
    , _segments_per_file(config.file_size / config.segment_size)
    , _pow2_layout(make_pow2_layout(_segments_per_file, config.segment_size))
    , _max_segments{(config.disk_size / config.file_size) * _segments_per_file, (config.disk_size / config.file_size) * _segments_per_file}
    , _segment_descs(static_cast<size_t>(_max_segments.actual))
    // A compaction job takes one segment from the pool per flush, so the reserve that keeps normal
    // writes from draining the pool is one segment per concurrent job.
    , _segment_pool(segment_pool_size, max_compaction_parallelism)
    // Compaction concurrency is limited by buffer availability. Normal compaction
    // uses one buffer; split compaction allocates two buffers. The buffers are built up front and
    // all of them are kept, so that a compaction never waits for memory to build one.
    , _compaction_buffer_pool(write_buffer_pool::config{
            .capacity = max_compaction_parallelism,
            .buffer_size = config.segment_size,
            .kind = segment_kind::full,
            .preallocate = max_compaction_parallelism,
            .max_cached = max_compaction_parallelism,
      })
    // One buffer per compaction group, plus a reserve for the flushes in flight - see
    // separator_flush_reserve. The capacity follows the group count, and the buffers are built at
    // the point of use, so a group that is not being separated into holds no memory at all.
    , _separator_buffer_pool(write_buffer_pool::config{
            .capacity = separator_flush_reserve,
            .buffer_size = config.segment_size,
            .kind = segment_kind::full,
            .preallocate = 0,
            .max_cached = separator_flush_reserve,
      })
    // Two buffers per hot group: the one it writes into and the one it rotates to. A group holds no
    // more than that at any moment - the buffer being written out is the one the group gave up - so
    // this is both the pool's capacity and the memory the direct path can take, which is what
    // direct_write_memory was given as.
    , _max_hot_groups(max_hot_groups(config))
    , _direct_buffer_pool(write_buffer_pool::config{
            .capacity = 2 * _max_hot_groups,
            .buffer_size = config.segment_size,
            .kind = segment_kind::full,
            .preallocate = 0,
            .max_cached = 2 * _max_hot_groups,
      })
    , _separator_task_queue(separator_queue_depth)
    , _trigger_threshold_observer(_cfg.trigger_compaction_threshold.observe([this] (double new_threshold) {
            logstor_logger.debug("Compaction trigger threshold changed to {}", new_threshold);
            _compaction_mgr.refresh_free_segment_watermarks();
            _compaction_mgr.schedule_auto_compaction();
      }))
    {

    if (_segments_per_file == 0) {
        throw exceptions::configuration_exception(fmt::format("Segment size {} must be less than or equal to file size {}", config.segment_size, config.file_size));
    }

    // The free space of a segment is bucketed by segment_descriptor_hist, which indexes a
    // fixed-size bucket array without checking its bounds. A segment larger than the histogram's
    // maximum would compute an out of range bucket for a fully free segment.
    if (config.segment_size > segment_descriptor_hist_options.max_size) {
        throw exceptions::configuration_exception(fmt::format("Segment size {} must be less than or equal to {}",
                config.segment_size, segment_descriptor_hist_options.max_size));
    }

    if (_max_segments.configured == 0) {
        throw exceptions::configuration_exception(fmt::format("Disk size {} must be greater than or equal to file size {}", config.disk_size, config.file_size));
    }

    // A budget of zero is how the direct path is turned off, but one that is set and still too small
    // for a single group is a configuration that does not do what it was meant to.
    if (_cfg.direct_group_writes && _cfg.direct_write_memory && !_max_hot_groups) {
        logstor_logger.warn("Direct write memory of {} bytes has no room for the two {} byte buffers a group takes, direct writes are off",
                _cfg.direct_write_memory, _cfg.segment_size);
    }

    _free_segments.reserve(static_cast<size_t>(_max_segments.actual));

    namespace sm = seastar::metrics;

    _metrics.add_group("logstor_sm", {
        sm::make_gauge("segments_in_use", [this] { return _max_segments.actual - available_segment_count(); },
                       sm::description("Counts number of segments currently in use.")),
        sm::make_gauge("free_segments", [this] { return available_segment_count(); },
                       sm::description("Counts number of free segments currently available.")),
        sm::make_histogram("segment_utilization", [this] { return to_metrics_histogram(_compaction_mgr.shard_segment_stats().stats(), _cfg.segment_size); },
                       sm::description("Distribution of the segments owned by the compaction groups of this shard by utilization, the fraction of a segment held by live records. A snapshot of the distribution and not a count of events, so the buckets fall as well as rise.")).aggregate({sm::shard_label}),
        sm::make_gauge("live_record_bytes", [this] { return _stats.live_record_bytes; },
                       sm::description("Counts the durable live record bytes currently referenced by the primary index.")),
        sm::make_gauge("live_record_count", [this] { return _stats.live_record_count; },
                   sm::description("Counts the durable live records currently referenced by the primary index.")),
        sm::make_gauge("segment_pool_size", [this] { return _segment_pool.size(); },
                       sm::description("Counts number of segments in the segment pool.")),
        sm::make_counter("segment_pool_segments_put", _segment_pool.get_stats().segments_put,
                       sm::description("Counts number of segments returned to the segment pool.")),
        sm::make_counter("segment_pool_normal_segments_get", _segment_pool.get_stats().segments_get[static_cast<size_t>(write_source::normal_write)],
                       sm::description("Counts number of segments taken from the segment pool for normal writes.")),
        sm::make_counter("segment_pool_compaction_segments_get", _segment_pool.get_stats().segments_get[static_cast<size_t>(write_source::compaction)],
                       sm::description("Counts number of segments taken from the segment pool for compaction.")),
        sm::make_counter("segment_pool_separator_segments_get", _segment_pool.get_stats().segments_get[static_cast<size_t>(write_source::separator)],
                       sm::description("Counts number of segments taken from the segment pool for separator writes.")),
        sm::make_counter("segment_pool_normal_segments_wait", _segment_pool.get_stats().normal_segments_wait,
                       sm::description("Counts number of times normal writes had to wait for a segment to become available in the segment pool.")),
        sm::make_counter("bytes_written", _stats.bytes_written[static_cast<size_t>(write_source::normal_write)],
                       sm::description("Counts number of bytes written to the disk.")),
        sm::make_counter("data_bytes_written", _stats.data_bytes_written[static_cast<size_t>(write_source::normal_write)],
                       sm::description("Counts number of data bytes written to the disk.")),
        sm::make_counter("write_failures", _stats.write_failures,
                       sm::description("Counts number of write failures when appending to a logstor segment.")),
        sm::make_counter("bytes_read", _stats.bytes_read,
                       sm::description("Counts number of bytes read from the disk.")),
        sm::make_counter("bytes_freed", _stats.bytes_freed,
                       sm::description("Counts number of data bytes freed.")),
        sm::make_counter("segments_allocated", _stats.segments_allocated,
                       sm::description("Counts number of segments allocated.")),
        sm::make_counter("segments_freed", _stats.segments_freed,
                       sm::description("Counts number of segments freed.")),
        sm::make_gauge("disk_usage", [this] { return get_disk_usage(); },
                       sm::description("Total disk usage.")),
        sm::make_counter("compaction_bytes_written", _stats.bytes_written[static_cast<size_t>(write_source::compaction)],
                       sm::description("Counts number of bytes written to the disk by compaction.")),
        sm::make_counter("compaction_data_bytes_written", _stats.data_bytes_written[static_cast<size_t>(write_source::compaction)],
                       sm::description("Counts number of data bytes written to the disk by compaction.")),
        sm::make_counter("compaction_segments_in", _compaction_mgr.get_stats().compaction_segments_in,
                       sm::description("Counts number of input segments selected for compaction.")),
        sm::make_counter("compaction_segments_out", _compaction_mgr.get_stats().compaction_segments_out,
                       sm::description("Counts number of output segments written by compaction.")),
        sm::make_counter("compaction_records_skipped", _compaction_mgr.get_stats().compaction_records_skipped,
                       sm::description("Counts number of records skipped during compaction.")),
        sm::make_counter("compaction_records_rewritten", _compaction_mgr.get_stats().compaction_records_rewritten,
                       sm::description("Counts number of records rewritten during compaction.")),
        sm::make_gauge("compaction_buffers_in_use", [this] { return _compaction_buffer_pool.used_buffer_count(); },
                       sm::description("Counts number of write buffers currently held by compaction. The pool size is the cap on concurrent compactions.")),
        sm::make_counter("compaction_buffer_allocation_waits", _compaction_buffer_pool.get_stats().allocation_waits,
                       sm::description("Counts number of times a compaction had to wait for a write buffer to become available.")),
        sm::make_counter("compaction_buffers_dropped", _compaction_buffer_pool.get_stats().buffers_dropped,
                       sm::description("Counts number of write buffers permanently removed from the pool after they could not be reclaimed.")),
        sm::make_counter("compaction_bytes_read", _compaction_mgr.get_stats().compaction_bytes_read,
                        sm::description("Counts number of bytes read by compaction.")),
        sm::make_counter("compaction_failures", _compaction_mgr.get_stats().compaction_failures,
                       sm::description("Counts number of logstor compaction failures.")),
        sm::make_counter("compaction_batches_refused", _compaction_mgr.get_stats().compaction_batches_refused,
                       sm::description("Counts number of compaction batches that were not started alongside a running compaction because they reclaimed far less per byte copied than the best batch on the shard.")),
        sm::make_counter("separator_bytes_written", _stats.bytes_written[static_cast<size_t>(write_source::separator)],
                       sm::description("Counts number of bytes written to the separator.")),
        sm::make_counter("separator_data_bytes_written", _stats.data_bytes_written[static_cast<size_t>(write_source::separator)],
                       sm::description("Counts number of data bytes written to the separator.")),
        sm::make_counter("separator_buffer_flushed", _compaction_mgr.get_stats().separator_buffer_flushed,
                       sm::description("Counts number of times the separator buffer has been flushed.")),
        sm::make_counter("separator_segments_freed", _compaction_mgr.get_stats().separator_segments_freed,
                       sm::description("Counts number of segments freed by the separator.")),
        sm::make_counter("separator_flush_failures", _compaction_mgr.get_stats().separator_flush_failures,
                       sm::description("Counts number of logstor separator flush failures.")),
        sm::make_counter("separator_task_failures", _stats.separator_task_failures,
                       sm::description("Counts number of logstor separator task failures.")),
        sm::make_gauge("separator_buffers_in_use", [this] { return _separator_buffer_pool.used_buffer_count(); },
                       sm::description("Counts number of write buffers currently held by the separator, one per compaction group that has records to separate plus one per flush in flight.")),
        sm::make_gauge("separator_buffers_allocated", [this] { return _separator_buffer_pool.allocated_buffer_count(); },
                       sm::description("Counts number of write buffers the separator buffer pool holds the memory of, including the ones it keeps for reuse.")),
        sm::make_gauge("separator_buffer_pool_capacity", [this] { return _separator_buffer_pool.capacity(); },
                       sm::description("Number of write buffers the separator can hold at once, which follows the number of compaction groups.")),
        sm::make_counter("separator_buffer_allocation_waits", _separator_buffer_pool.get_stats().allocation_waits,
                       sm::description("Counts number of times the separator had to wait for a write buffer to become available.")),
        sm::make_counter("separator_buffers_dropped", _separator_buffer_pool.get_stats().buffers_dropped,
                       sm::description("Counts number of separator write buffers permanently removed from the pool after they could not be reclaimed.")),
        sm::make_counter("segment_free_failures", _stats.segment_free_failures,
                       sm::description("Counts number of logstor segment references that failed to free the segment.")),
        sm::make_gauge("separator_task_queue_size", [this]() { return _separator_task_queue.size(); },
                       sm::description("Current number of pending tasks in the separator task queue.")),
        sm::make_counter("direct_bytes_written", _stats.bytes_written[static_cast<size_t>(write_source::direct_write)],
                       sm::description("Counts number of bytes written to segments by the direct write path, which writes the records of a group straight into a segment of that group instead of through the shared active segment and the separator.")),
        sm::make_counter("direct_data_bytes_written", _stats.data_bytes_written[static_cast<size_t>(write_source::direct_write)],
                       sm::description("Counts number of data bytes written to segments by the direct write path.")),
        sm::make_counter("direct_records_written", _stats.direct_records_written,
                       sm::description("Counts number of records taken by the direct write path, and therefore acknowledged before they were on the disk.")),
        sm::make_counter("direct_fallbacks", _stats.direct_fallbacks[static_cast<size_t>(direct_fallback_reason::no_buffer)],
                       sm::description("Counts number of writes of a group that takes direct writes that the direct path could not take, and that went the ordinary way instead. The reason label says which of them the shard can do something about: no_buffer means the group has no buffer to write into, because the shard had no write buffer or no segment to spare when its last flush finished; flush_in_flight means the group filled a buffer while the previous one was still being written out, which two buffers of a segment each cannot carry; record_too_large means the record does not fit a segment at all."),
                       {direct_fallback_reason_label(direct_fallback_reason_to_string(direct_fallback_reason::no_buffer))}),
        sm::make_counter("direct_fallbacks", _stats.direct_fallbacks[static_cast<size_t>(direct_fallback_reason::flush_in_flight)],
                       sm::description("See the no_buffer reason of this counter."),
                       {direct_fallback_reason_label(direct_fallback_reason_to_string(direct_fallback_reason::flush_in_flight))}),
        sm::make_counter("direct_fallbacks", _stats.direct_fallbacks[static_cast<size_t>(direct_fallback_reason::record_too_large)],
                       sm::description("See the no_buffer reason of this counter."),
                       {direct_fallback_reason_label(direct_fallback_reason_to_string(direct_fallback_reason::record_too_large))}),
        sm::make_counter("direct_read_hits", _stats.direct_read_hits,
                       sm::description("Counts number of reads served out of a direct write buffer, of a record that had been acknowledged but was not on the disk yet.")),
        sm::make_counter("direct_full_flushes", _stats.direct_full_flushes,
                       sm::description("Counts number of direct write buffers written out because they were full, which is the direct path working as intended.")),
        sm::make_counter("direct_deadline_flushes", _stats.direct_deadline_flushes,
                       sm::description("Counts number of direct write buffers written out because their sync period expired. A shard where these dominate is writing partly empty segments, and its groups belong on the ordinary path.")),
        sm::make_counter("direct_flush_failures", _stats.direct_flush_failures,
                       sm::description("Counts number of direct write buffers whose write to the disk failed. Their records stay readable from memory, at the cost of the buffer and the segment never being reclaimed.")),
        sm::make_gauge("direct_failed_buffer_bytes", [this] { return _stats.direct_failed_buffer_bytes; },
                       sm::description("Bytes of memory held by direct write buffers whose write to the disk failed. The shard keeps them so that the records they hold stay readable and never gets that memory back; once they hold the whole memory budget of the direct write path, the path is turned off.")),
        sm::make_gauge("direct_unflushed_bytes", [this] { return _stats.direct_unflushed_bytes; },
                       sm::description("Bytes of records that have been acknowledged but are only in a direct write buffer, which is what a crash of this shard would lose.")),
        sm::make_gauge("direct_hot_groups", [this] { return _compaction_mgr.direct_hot_group_count(); },
                       sm::description("Number of compaction groups currently writing into buffers of their own.")),
        sm::make_gauge("direct_buffers_in_use", [this] { return _direct_buffer_pool.used_buffer_count(); },
                       sm::description("Counts number of write buffers currently held by the direct write path, two per hot compaction group.")),
    });
}

future<> segment_manager_impl::start() {
    // Start background replenisher before creating initial segment
    _reserve_replenisher = with_scheduling_group(_cfg.compaction_sg, [this] {
        return replenish_reserve();
    });

    co_await _compaction_mgr.start();

    _separator_fiber = with_scheduling_group(_cfg.separator_sg, [this] {
        return run_separator_fiber();
    });

    if (direct_writes_enabled()) {
        // In the separator's group: it does the same work for the same reason, taking the records
        // of a group to a segment of that group, only without the second write.
        _direct_sync_fiber = with_scheduling_group(_cfg.separator_sg, [this] {
            return run_direct_sync_fiber();
        });
    }

    co_await switch_active_segment();

    logstor_logger.info("Segment manager started with base directory {}", _cfg.base_dir.string());
}

future<> segment_manager_impl::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping segment manager");

    // stop admitting new writes and external operations
    co_await _async_gate.close();

    // let accepted writes finish and be written to separator buffer
    co_await _writes_phaser.advance_and_await();

    // abort separator task queue and fiber. it should be drained at this point since all tasks hold the write phaser.
    logstor_logger.debug("Stopping separator fiber and flushing separator buffers");
    _separator_task_queue.abort(std::make_exception_ptr(seastar::abort_requested_exception()));
    // Only the flushes below can hand a buffer back now, and they need none, so a separator write
    // that is waiting for one would hold up the fiber until a flush it cannot trigger completes.
    _separator_buffer_abort.request_abort();
    co_await std::move(_separator_fiber).handle_exception([] (std::exception_ptr) {});

    // Before the separator buffers, and before anything is torn down: a record taken directly is
    // acknowledged and in the index, but only in memory, so it has to reach the disk here.
    logstor_logger.debug("Stopping the direct write sync fiber and flushing the direct write buffers");
    _direct_abort.request_abort();
    co_await std::move(_direct_sync_fiber).handle_exception([] (std::exception_ptr) {});
    co_await _compaction_mgr.drain_all_direct_buffers().handle_exception([] (std::exception_ptr ep) {
        logstor_logger.warn("Failed to drain the logstor direct write buffers: {}", ep);
    });

    co_await _compaction_mgr.flush_all_separator_buffers(std::nullopt);

    if (_active_segment) {
        co_await _active_segment->stop();
    }

    if (_switch_segment_fut) {
        auto f = co_await coroutine::as_future(_switch_segment_fut->get_future());
        f.ignore_ready_future();
    }

    logstor_logger.debug("Stopping compaction manager and buffer pools");
    co_await _compaction_mgr.stop();
    co_await _compaction_buffer_pool.stop();
    co_await _separator_buffer_pool.stop();
    // Nothing reads out of these any more, so the records they hold - which the disk never took -
    // are finally let go. Their segments are marked as failed, so releasing the last reference to
    // one leaves it allocated rather than freeing a segment the index still points into.
    for (auto& failed : _direct_failed_buffers) {
        if (failed.seg) {
            _direct_readable.erase(failed.seg->id().value);
        }
    }
    _direct_failed_buffers.clear();
    co_await _direct_buffer_pool.stop();

    logstor_logger.debug("Stopping segment pool");
    co_await _segment_pool.stop();
    _segment_freed_cv.broken();
    co_await std::move(_reserve_replenisher);

    co_await _file_mgr.stop();

    logstor_logger.info("Segment manager stopped");
}

future<> segment_manager_impl::run_separator_fiber() {
    while (true) {
        separator_task task;
        try {
            task = co_await _separator_task_queue.pop_eventually();
        } catch (seastar::abort_requested_exception&) {
            co_return;
        }

        auto write_to_separator_failed = defer([seg_ref = task.seg_ref] mutable noexcept {
            seg_ref.set_flush_failure();
        });

        try {
            co_await write_to_separator(task.records, task.buffer_location, std::move(task.seg_ref), task.seq_num);
            write_to_separator_failed.cancel();
        } catch (...) {
            ++_stats.separator_task_failures;
            logstor_logger.debug("logstor separator task failure in separator fiber: {}", std::current_exception());
        }
    }
}

future<> segment_manager_impl::write(write_buffer& wb) {
    write_source source = write_source::normal_write;
    auto holder = _async_gate.hold();
    auto write_op = _writes_phaser.start();
    const auto sealed_size = wb.sealed_size(block_alignment);

    if (sealed_size > _cfg.segment_size) {
        throw std::runtime_error(fmt::format( "Write size {} exceeds segment size {}", sealed_size, _cfg.segment_size));
    }

    while (true) {
        if (!_active_segment) {
            co_await request_segment_switch();
            continue;
        }

        seg_ptr seg = _active_segment;
        auto seg_holder = seg->hold();
        auto seg_ref = seg->ref();

        auto reservation = co_await seg->reserve(sealed_size);
        if (!reservation) {
            if (_active_segment == seg) {
                co_await request_segment_switch();
            }
            continue;
        }

        auto seq_num = seg->seq_num();

        wb.seal(seq_num, std::nullopt, block_alignment);

        // if we wrote a record to the segment but failed to write it to the separator, the segment should not be freed.
        auto write_to_separator_failed = defer([seg_ref] mutable noexcept {
            seg_ref.set_flush_failure();
        });

        logstor_logger.trace("Write active segment {} seq {}", seg->id(), seq_num);

        bytes_view data(reinterpret_cast<const int8_t*>(wb.data()), wb.serialized_size());
        auto append_result = co_await coroutine::as_future(seg->write_reserved(std::move(*reservation), data));
        if (append_result.failed()) {
            auto ex = append_result.get_exception();
            ++_stats.write_failures;
            co_await wb.abort_writes(ex);
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
        auto loc = append_result.get();

        _stats.bytes_written[static_cast<size_t>(source)] += data.size();
        _stats.data_bytes_written[static_cast<size_t>(source)] += wb.net_data_size();

        // complete all buffered writes with their individual locations and wait
        // for them to be updated in the index.
        co_await wb.complete_writes(loc);

        auto records = wb.take_separator_records();
        if (!records.empty()) {
            co_await with_semaphore(_separator_enqueue_sem, 1, [&] {
                return _separator_task_queue.push_eventually(separator_task{
                    .records = std::move(records),
                    .buffer_location = loc,
                    .seg_ref = seg_ref,
                    .seq_num = seq_num,
                    .write_op = std::move(write_op),
                });
            });
        }
        write_to_separator_failed.cancel();
        break;
    }
}

future<> segment_manager_impl::write_full_segment(write_buffer& wb, logstor_group& cg, write_source source) {
    const auto sealed_size = wb.sealed_size(block_alignment);

    if (sealed_size > _cfg.segment_size) {
        throw std::runtime_error(fmt::format("Write size {} exceeds segment size {}", sealed_size, _cfg.segment_size));
    }

    auto seg = co_await get_segment(source);
    co_await write_full_segment_tail(std::move(seg), wb, cg, source);
}

future<> segment_manager_impl::write_full_segment_tail(seg_ptr seg, write_buffer& wb, logstor_group& cg, write_source source) {
    logstor_logger.trace("Write full segment {} seq {} from {}", seg->id(), seg->seq_num(), write_source_to_string(source));

    wb.seal(seg->seq_num(), cg.table_id(), block_alignment);
    bytes_view data(reinterpret_cast<const int8_t*>(wb.data()), wb.serialized_size());

    auto append_result = co_await coroutine::as_future(seg->append(data));
    if (append_result.failed()) {
        auto ex = append_result.get_exception();
        ++_stats.write_failures;
        co_await wb.abort_writes(ex);
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
    auto loc = append_result.get();

    _stats.bytes_written[static_cast<size_t>(source)] += data.size();
    _stats.data_bytes_written[static_cast<size_t>(source)] += wb.net_data_size();

    // The records are on disk and the index already points into this segment, so if adding it to
    // the compaction group fails, the segment must not be freed when the reference below is
    // released. Marking the reference as failed leaves it allocated instead, so the records stay
    // readable, at the cost of the segment never being reclaimed.
    auto seg_ref = seg->ref();
    auto add_to_group_failed = defer([seg_ref] mutable noexcept {
        seg_ref.set_flush_failure();
        logstor_logger.warn("Failed to add segment {} to a compaction group, it will not be reclaimed", seg_ref.id());
    });

    co_await wb.complete_writes(loc);
    co_await seg->stop();

    // add the segment after all index updates are completed.
    auto& desc = get_segment_descriptor(seg->id());
    cg.add_logstor_segment(desc);
    add_to_group_failed.cancel();
}

// The direct write path. See direct_write_buffer in compaction.hh for what it is and why the
// segment immutability the rest of logstor rests on survives it.

std::optional<log_location> segment_manager_impl::try_write_direct(logstor_group& cg,
        const log_record_header_view& header, bytes_view value) {
    if (!direct_writes_enabled()) {
        return std::nullopt;
    }

    const auto writer = log_record_ref_writer(header, value);

    // Counted for every write of the group, hot or cold: this is the write rate the controller
    // measures, and a cold group is promoted on it.
    cg._direct_bytes_this_period += writer.size();

    if (!cg._direct_enabled) {
        return std::nullopt;
    }

    auto& active = cg._direct_active;
    if (!active.bound()) {
        // The group is hot but its last flush could not be given a buffer or a segment for what it
        // gave back. Counted, because a shard whose reserve is short takes this shape.
        ++_stats.direct_fallbacks[static_cast<size_t>(direct_fallback_reason::no_buffer)];
        return std::nullopt;
    }

    if (!active.buf->can_fit(writer)) {
        if (!cg._direct_flush.available()) {
            // The previous rotation is still being written out, so there is no spare to rotate
            // into. The record takes the ordinary path, and with it that path's back-pressure.
            ++_stats.direct_fallbacks[static_cast<size_t>(direct_fallback_reason::flush_in_flight)];
            return std::nullopt;
        }
        ++_stats.direct_full_flushes;
        rotate_direct_buffer(cg);
        if (!active.bound()) {
            // The rotation had no spare to give.
            ++_stats.direct_fallbacks[static_cast<size_t>(direct_fallback_reason::no_buffer)];
            return std::nullopt;
        }
        if (!active.buf->can_fit(writer)) {
            // The record does not fit an empty buffer at all - the ordinary path bounds it by
            // max_record_size_any_kind and rejects it there.
            ++_stats.direct_fallbacks[static_cast<size_t>(direct_fallback_reason::record_too_large)];
            return std::nullopt;
        }
    }

    if (!active.buf->has_data()) {
        active.first_append = seastar::lowres_clock::now();
    }

    const auto appended = active.buf->append_synchronously(writer);
    ++_stats.direct_records_written;
    _stats.direct_unflushed_bytes += appended.total_size;

    // A full segment holds exactly one buffer, at offset zero, so a record's offset in the buffer
    // is its offset in the segment. try_bind_direct_slot() checks that the segment is untouched.
    return record_location(log_location{.segment = active.seg->id(), .offset = 0, .size = 0},
            appended.record_header_offset, appended.total_size);
}

void segment_manager_impl::rotate_direct_buffer(logstor_group& cg) {
    if (!cg._direct_flush.available()) {
        on_internal_error(logstor_logger, "Attempted to rotate a direct write buffer while a flush is in progress");
    }

    auto full = std::exchange(cg._direct_active, std::move(cg._direct_spare));
    cg._direct_spare = {};
    cg._direct_flush = shared_future<>(flush_direct_buffer(cg, std::move(full)));
}

future<> segment_manager_impl::flush_direct_buffer(logstor_group& cg, direct_write_buffer full) {
    // No hold on _async_gate: stopping closes that gate before it drains the direct buffers, the
    // same way it drains the separator buffers. The phaser is what await_pending_writes() and
    // stop() see this by.
    auto write_op = _writes_phaser.start();

    if (full.bound() && !full.empty()) {
        const auto seg_id = full.seg->id();
        const auto unflushed_bytes = full.buf->net_data_size();

        // The sequence number is taken here rather than when the segment was handed out: recovery
        // tie-breaks records of equal timestamp by it, and every segment written while this buffer
        // was filling has a number of its own. Numbering it at seal time keeps that order. The
        // numbers the skipped allocations leave behind are harmless - recovery takes the maximum.
        full.seg->set_seq_num(allocate_segment_seq());

        auto written = co_await coroutine::as_future(
                write_full_segment_tail(full.seg, *full.buf, cg, write_source::direct_write));
        // Either way this buffer is no longer what a crash of the shard would lose: its records are
        // on the disk, or they are already gone and counted as direct_failed_buffer_bytes.
        _stats.direct_unflushed_bytes -= unflushed_bytes;
        if (written.failed()) {
            ++_stats.direct_flush_failures;
            ++cg._direct_flush_failures;
            // The index points into this segment, so it must never be freed and handed out again
            // over the records it holds. Marking it as failed leaks it instead.
            // write_full_segment_tail() only does that once the bytes have reached the disk,
            // because for the separator and for compaction a target segment that took nothing is
            // simply given back; the direct path is the one whose index already points into a
            // segment that was never written. The buffer is kept for the same reason, registered
            // for reads: these records were acknowledged and this memory is the only copy of them
            // left. The one path that deliberately keeps a buffer out of its pool.
            full.seg->ref().set_flush_failure();
            logstor_logger.error("Failed to write logstor segment {} of table {} directly: {}."
                    " Its buffer is kept in memory so that its records stay readable, and neither it"
                    " nor the segment will be reclaimed", seg_id, cg.table_id(), written.get_exception());
            note_direct_flush_failure(std::move(full));
        } else {
            cg._direct_flush_failures = 0;
            // Only now that the records are on the disk and their segment is in the group: until
            // here a read of one of them had to come out of this buffer.
            _direct_readable.erase(seg_id.value);
            // Given back before another is asked for below, so a group never holds three at once.
            full.buf.reset();
            full.seg = {};
        }
    } else if (full.bound()) {
        // Nothing went into it, so there is nothing to seal. Keep it rather than giving its segment
        // back only to take another one.
        if (cg._direct_spare.bound()) {
            co_await release_direct_slot(full);
        } else {
            cg._direct_spare = std::move(full);
        }
    }

    // Give the group back what it rotated out of, so its next rotation has somewhere to go. Best
    // effort and synchronous: what this future means is that the records are on the disk and their
    // segment is in the group, and making it also mean "and the next segment is in hand" is what
    // would put an unbounded wait for disk space on every drain that waits for it. A slot left
    // unbound here is bound by the sync fiber once the shard has room for it again.
    try_bind_direct_slot(cg, cg._direct_active);
    try_bind_direct_slot(cg, cg._direct_spare);
}

void segment_manager_impl::note_direct_flush_failure(direct_write_buffer failed) {
    _direct_failed_buffers.push_back(std::move(failed));
    _stats.direct_failed_buffer_bytes = _direct_failed_buffers.size() * _cfg.segment_size;

    // These buffers hold memory the path was given to write with, and they never give it back.
    // Once they hold all of it, writing directly could only add to them, so the path goes off for
    // good and every group falls back to the ordinary one - which reports a failed write to its
    // caller instead of keeping the record in memory and calling it acknowledged.
    if (!_direct_writes_stopped && _stats.direct_failed_buffer_bytes >= _cfg.direct_write_memory) {
        _direct_writes_stopped = true;
        logstor_logger.error("Logstor direct writes are off for the rest of the life of this shard:"
                " {} buffers holding {} bytes could not be written and are kept in memory, which is"
                " the whole memory budget of the path", _direct_failed_buffers.size(),
                _stats.direct_failed_buffer_bytes);
    }
}

bool segment_manager_impl::try_bind_direct_slot(logstor_group& cg, direct_write_buffer& slot) {
    if (slot.bound()) {
        return true;
    }
    if (!cg._direct_enabled || _async_gate.is_closed()) {
        return false;
    }
    // The segments the reserve holds ready, not the free slots on the disk: taking one has to be
    // synchronous, so only a segment the replenisher has already built is one this can have.
    // Checked before the buffer is taken, and nothing below yields, so the segment cannot go away
    // between the two and leave a buffer with nowhere to put it.
    if (_segment_pool.available_segment_count(write_source::direct_write) == 0) {
        return false;
    }

    auto buf = _direct_buffer_pool.try_allocate();
    if (!buf) {
        return false;
    }
    auto seg = try_get_segment(write_source::direct_write);
    if (!seg) [[unlikely]] {
        on_internal_error(logstor_logger, "A logstor segment for a direct write buffer went away after it was counted");
    }

    if ((*seg)->bytes_remaining() != _cfg.segment_size) {
        on_internal_error(logstor_logger, format("Segment {} bound to a direct write buffer is not empty", (*seg)->id()));
    }

    slot.buf = std::move(*buf);
    slot.seg = std::move(*seg);
    slot.first_append = {};
    _direct_readable.emplace(slot.seg->id().value, slot.buf.get());
    return true;
}

future<> segment_manager_impl::release_direct_slot(direct_write_buffer& slot) {
    if (slot.seg) {
        _direct_readable.erase(slot.seg->id().value);
    }
    if (slot.buf) {
        if (slot.buf->has_data()) {
            on_internal_error(logstor_logger, "Releasing a logstor direct write buffer that was not written out");
        }
        // The pool takes a buffer back only once it is closed, and one that was only appended to
        // directly holds nothing, so this completes right away.
        co_await slot.buf->close();
        slot.buf.reset();
    }
    if (slot.seg) {
        // The segment was never written, so it holds no data and is in no segment set: dropping the
        // last reference to it below simply gives the slot back to the pool.
        co_await slot.seg->stop();
        slot.seg = {};
    }
    slot.first_append = {};
}

future<> segment_manager_impl::release_direct_buffers(logstor_group& cg) {
    cg._direct_enabled = false;
    co_await release_direct_slot(cg._direct_active);
    co_await release_direct_slot(cg._direct_spare);
    cg._direct_underfilled_periods = 0;
    cg._direct_flush_failures = 0;
}

bool segment_manager_impl::promote_direct_group(logstor_group& cg) {
    if (!direct_writes_enabled() || cg._direct_enabled || _async_gate.is_closed()) {
        return false;
    }
    // Room for another hot group, and for the two segments its buffers are going to be bound to.
    // The room is counted in buffers that are out rather than in groups that are enabled: a group
    // being demoted clears the flag before it gives its buffers back, and counting groups would let
    // a promotion in that window take buffers the pool does not have.
    if (_direct_buffer_pool.used_buffer_count() + 2 > _direct_buffer_pool.capacity()
            || available_segment_count(write_source::direct_write) < 2) {
        return false;
    }
    if (!cg._direct_flush.available()) {
        return false;
    }

    cg._direct_enabled = true;
    cg._direct_underfilled_periods = 0;
    if (!try_bind_direct_slot(cg, cg._direct_active)) {
        cg._direct_enabled = false;
        return false;
    }
    try_bind_direct_slot(cg, cg._direct_spare);
    return true;
}

void segment_manager_impl::tick_direct_deadlines(logstor_group& cg, seastar::lowres_clock::time_point now) {
    if (!cg._direct_enabled) {
        return;
    }

    // A slot can be left unbound by a flush that finished while the shard had no buffer or no
    // segment to spare, and by one whose write failed. Nothing else comes back to it, so without
    // this the group would go on writing through the shared active segment for good while still
    // counting as hot. Not while a flush is in flight: the buffer it is writing is the group's
    // second one, and binding a third here would take more of the pool than a group is allowed.
    if (cg._direct_flush.available()) {
        try_bind_direct_slot(cg, cg._direct_active);
        try_bind_direct_slot(cg, cg._direct_spare);
    }

    // The deadline: a buffer that has been holding records for a whole sync period is written out
    // even though it is not full, which is what bounds how much a crash can lose.
    if (!cg._direct_active.empty() && cg._direct_flush.available()
            && now - cg._direct_active.first_append >= _cfg.direct_sync_period) {
        ++_stats.direct_deadline_flushes;
        rotate_direct_buffer(cg);
    }
}

future<> segment_manager_impl::run_direct_controller(logstor_group& cg) {
    // A group whose writes the disk keeps refusing goes back to the ordinary path, and so does
    // every group once the path is off shard wide. Both are decided here rather than at the flush,
    // which cannot wait for a group to be closed.
    if (cg._direct_enabled && (!direct_writes_enabled()
            || cg._direct_flush_failures >= direct_flush_failures_before_demotion)) {
        logstor_logger.warn("Taking the direct write buffers of a logstor group of table {} back after"
                " {} failed flushes", cg.table_id(), cg._direct_flush_failures);
        co_await cg.close_direct_writes();
        co_return;
    }

    // What the group wrote over the period just ended, direct or not, which is the rate the
    // decisions below are made on.
    const auto bytes = std::exchange(cg._direct_bytes_this_period, 0);
    const auto threshold = direct_hot_threshold_bytes();

    if (!cg._direct_enabled) {
        if (direct_promotion_wanted(bytes, threshold)) {
            promote_direct_group(cg);
        }
        co_return;
    }

    if (bytes >= threshold) {
        cg._direct_underfilled_periods = 0;
        co_return;
    }

    ++cg._direct_underfilled_periods;
    if (direct_demotion_wanted(bytes, threshold, cg._direct_underfilled_periods)) {
        // Writes out what is buffered and gives the buffers and their segments back, so that a
        // group that is actually writing can have them.
        co_await cg.close_direct_writes();
    }
}

future<> segment_manager_impl::run_direct_sync_fiber() {
    // Several ticks per period: a buffer's deadline runs from its first record rather than from the
    // start of a period, so it has to be looked at more often than once a period to be honoured
    // anywhere near it. The controller itself only decides once per whole period, which is the
    // window the rate it measures is over.
    const auto tick = std::clamp(_cfg.direct_sync_period / 4,
            std::chrono::milliseconds(10), std::chrono::milliseconds(1000));
    auto period_start = seastar::lowres_clock::now();
    // The controller runs beside the ticks rather than in one: a demotion waits for the group it
    // demotes to be drained, and a deadline that waited behind that would not be a deadline.
    future<> controller = make_ready_future<>();

    while (true) {
        try {
            co_await seastar::sleep_abortable(tick, _direct_abort);
        } catch (const seastar::sleep_aborted&) {
            break;
        }

        const auto now = seastar::lowres_clock::now();

        try {
            _compaction_mgr.tick_direct_deadlines(now);
        } catch (...) {
            logstor_logger.warn("logstor direct write sync failed: {}. Ignored", std::current_exception());
        }

        if (now - period_start < _cfg.direct_sync_period) {
            continue;
        }
        // Whole periods, so a pass that took longer than one does not leave the next one measuring
        // a write rate over part of a period and demoting a group that was writing all along.
        while (now - period_start >= _cfg.direct_sync_period) {
            period_start += _cfg.direct_sync_period;
        }
        // A controller pass that is still running keeps the period it started in; skipping is what
        // makes it fall behind the deadlines rather than queue in front of them.
        if (controller.available()) {
            controller = _compaction_mgr.run_direct_controller().handle_exception([] (std::exception_ptr ep) {
                logstor_logger.warn("logstor direct write controller failed: {}. Ignored", ep);
            });
        }
    }

    co_await std::move(controller);
    logstor_logger.debug("Direct write sync fiber stopped");
}

void segment_manager_impl::on_add_record(log_location location) noexcept {
    auto& desc = get_segment_descriptor(location);
    desc.on_write(location);
    _stats.live_record_bytes += location.size;
    _stats.live_record_count++;
}

void segment_manager_impl::on_free_record(log_location location) noexcept {
    auto& desc = get_segment_descriptor(location);
    desc.on_free(location);
    _stats.live_record_bytes -= location.size;
    _stats.live_record_count--;
    if (desc.owner) {
        desc.owner->update_segment(desc, location.size);
    }
    _stats.bytes_freed += location.size;
}

future<temporary_buffer<char>> segment_manager_impl::read_record_bytes(log_location location) {
    auto holder = _async_gate.hold();

    if (location.offset + location.size > _cfg.segment_size) [[unlikely]] {
        co_return coroutine::exception(std::make_exception_ptr(std::runtime_error(fmt::format(
            "Read beyond end of segment {}: offset {} + size {} > segment size {}",
            location.segment, location.offset, location.size, _cfg.segment_size))));
    }

    // A record taken by the direct path is in the index, and readable, before its segment has been
    // written, so a location may point into a buffer rather than at the disk. The copy below is
    // synchronous, so nothing can give the buffer back to its pool between finding it and reading
    // it, and the bytes of a record never change once it has been appended - sealing only fills in
    // the header area the buffer reserved and pads its tail.
    //
    // A segment that belongs to a compaction group has been written, and one a buffer is bound to
    // belongs to none until then, so the descriptor rules most reads out with one load. Checking
    // that the map is non-empty would not: a group that is taking direct writes has its buffers in
    // it from the moment they are bound, so it is never empty while the path is doing anything.
    if (!get_segment_descriptor(location.segment).owner) [[unlikely]] {
        if (auto it = _direct_readable.find(location.segment.value); it != _direct_readable.end()) {
            ++_stats.direct_read_hits;
            _stats.bytes_read += location.size;
            co_return temporary_buffer<char>(it->second->data() + location.offset, location.size);
        }
    }

    auto [file_id, file_offset] = segment_id_to_file_location(location.segment);
    // The file it reads from outlives the read: it is held here, on the frame of this coroutine,
    // because the read is issued on it and seastar keeps reading from it after the first suspension.
    auto file = _file_mgr.opened_file_for_read(file_id);
    if (!file) [[unlikely]] {
        file = co_await _file_mgr.get_file_for_read(file_id);
    }

    // The bulk read is what dma_read_exactly() does underneath, over two coroutine frames of its
    // own: one to trim the buffer the disk gave back to the size that was asked for, and one to
    // reject a short read. This coroutine is already here to do both.
    auto buf = co_await file.dma_read_bulk<char>(file_offset + location.offset, location.size);
    if (buf.size() < location.size) [[unlikely]] {
        co_return coroutine::exception(std::make_exception_ptr(std::runtime_error(fmt::format(
            "Short read of segment {}: got {} bytes of the {} asked for at offset {}",
            location.segment, buf.size(), location.size, location.offset))));
    }
    buf.trim(location.size);
    _stats.bytes_read += location.size;
    co_return std::move(buf);
}

future<> segment_manager_impl::request_segment_switch() {
    if (!_switch_segment_fut) {
        auto f = switch_active_segment();
        if (f.available()) {
            f.get();
            co_return;
        }
        _switch_segment_fut.emplace(f.discard_result().finally([this] {
            _switch_segment_fut.reset();
        }));
    }
    co_await _switch_segment_fut->get_future();
}

future<> segment_manager_impl::switch_active_segment() {
    auto new_seg = co_await get_segment(write_source::normal_write);

    auto old_seg = std::exchange(_active_segment, std::move(new_seg));

    if (old_seg) {
        // close old segment in background
        (void)old_seg->stop().finally([old_seg] {}).handle_exception([] (std::exception_ptr) {});
    }

    // trigger separator flush for separator buffers that hold old segments
    auto u = std::max<size_t>(1, _max_segments.configured / 100);
    if (_next_segment_seq.value % u == 0 && _next_segment_seq.value > 5*u) {
        (void)_compaction_mgr.flush_all_separator_buffers(segment_sequence(_next_segment_seq.value - 5*u)).handle_exception([] (std::exception_ptr) {});
    }

    logstor_logger.trace("Switched active segment to {} seq {}", _active_segment->id(), _active_segment->seq_num());
}

future<> segment_manager_impl::replenish_reserve() {
    while (true) {
        bool retry = false;
        try {
            auto seg = co_await allocate_segment();
            co_await _segment_pool.put(std::move(seg));
        } catch (abort_requested_exception&) {
            logstor_logger.debug("Reserve replenisher stopping due to abort");
            break;
        } catch (...) {
            retry = true;
            logstor_logger.warn("Exception in reserve replenisher: {}, will retry", std::current_exception());
        }

        if (retry) {
            co_await seastar::sleep(std::chrono::seconds(1));
        }
    }

    logstor_logger.debug("Reserve replenisher stopped");
}

future<seg_ptr> segment_manager_impl::allocate_segment() {
    auto make_segment = [this] (log_segment_id seg_id) -> future<seg_ptr> {
        try {
            auto seg_loc = segment_id_to_file_location(seg_id);
            auto file = co_await _file_mgr.get_file_for_write(seg_loc.file_id);
            auto seg = make_lw_shared<writeable_segment>(seg_id, std::move(file), seg_loc.file_offset, _cfg.segment_size);
            get_segment_descriptor(seg_id).reset(_cfg.segment_size);
            _stats.segments_allocated++;
            co_return std::move(seg);
        } catch (...) {
            _free_segments.push_back(seg_id);
            _segment_freed_cv.signal();
            throw;
        }
    };

    while (true) {
        // first, allocate all new segments sequentially
        if (_next_new_segment_id < _max_segments.configured) {
            auto seg_id = log_segment_id(_next_new_segment_id++);
            co_return co_await make_segment(seg_id);
        }

        _compaction_mgr.schedule_auto_compaction();

        // reuse freed segments
        if (!_free_segments.empty()) {
            auto seg_id = _free_segments.front();
            _free_segments.pop_front();
            co_return co_await make_segment(seg_id);
        }

        // no free segments - wait for a segment to be freed.
        // compaction might fail to free segments now, but can succeed later as data is freed.
        // for now let's solve it by waiting with a timeout to re-trigger compaction periodically.
        try {
            co_await _segment_freed_cv.wait(std::chrono::seconds(5));
        } catch (seastar::broken_condition_variable&) {
            co_return coroutine::return_exception(abort_requested_exception());
        }
    }
}

void segment_manager_impl::free_segment(log_segment_id segment_id) noexcept {
    // Before freeing a segment, ensure there are no ongoing operations that use
    // locations in this segment. See for example `await_pending_reads`.
    logstor_logger.trace("Free segment {}", segment_id);

    // This runs on the release path of segment_ref (including from its destructor), so it must
    // not throw: any of the checks below firing is a correctness bug (freeing a segment that is
    // still live or still referenced would let it be reallocated and clobber live data), so we
    // abort unconditionally rather than risk continuing, or a throw escaping a destructor.
    auto& desc = get_segment_descriptor(segment_id);
    if (desc.net_data_size(_cfg.segment_size) != 0) {
        on_fatal_internal_error(logstor_logger, format("Freeing segment {} that has data", segment_id));
    }
    if (desc.ref_count != 0) {
        on_fatal_internal_error(logstor_logger, format("Freeing segment {} with non-zero reference count", segment_id));
    }
    if (segment_id.value < _max_segments.configured) {
        // _free_segments is pre-reserved to fit every configured segment (see the constructor and
        // set_actual_max_segments()), so this should never grow the vector.
        _free_segments.push_back(segment_id);
        _segment_freed_cv.signal();
    }

    _stats.segments_freed++;
}

future<> segment_manager_impl::discard_segments(logstor_group& cg) {
    auto compaction_disable_guard = co_await _compaction_mgr.disable_compaction(cg);

    auto holder = _async_gate.hold();

    // Records the group took directly are not in any of its segments yet, so the loop below would
    // not see them, and the flush that came later would put a segment holding them into a group
    // that was just emptied. They are written out here instead of dropped, exactly like the records
    // the separator is still holding for the group: the caller clears the index before it discards,
    // so what comes out of the buffers is a segment of dead records that the loop below then frees.
    // A record that was written after that clear is the one case where this matters - dropping it
    // would free a segment the index still points into, which free_segment() treats as fatal, while
    // writing it out leaves the loop below to report it the way it reports a late separator write.
    co_await cg.close_direct_writes();

    auto& ss = cg.logstor_segments();

    // Collect and validate the segments to discard before removing any of them, so that this
    // cannot fail with a segment already out of the group but not yet freed, which would leak it
    // and leave its data on disk for recovery to find.
    // Compaction is the only thing that removes segments from a group and it is disabled here, so
    // the positions of the first `segment_count` segments stay valid. Segments appended while this runs
    // are past `segment_count` and are left in the group.
    const auto segment_count = ss.segment_count();
    utils::chunked_vector<log_segment_id> segments;
    segments.reserve(segment_count);
    for (size_t i = 0; i < segment_count; ++i) {
        auto& desc = *ss._segment_list[i];
        auto seg_id = desc_to_segment_id(desc);

        // Belonging to the group is the only reference the segment may have left.
        if (desc.ref_count != 1) {
            on_internal_error(logstor_logger, format("Discarding segment {} with non-zero reference count", seg_id));
        }

        // the index should be cleared before discarding segments, so no data should be reachable
        if (desc.net_data_size(_cfg.segment_size) != 0) {
            on_internal_error(logstor_logger, format("Discarding segment {} that has data", seg_id));
        }

        segments.push_back(seg_id);
        co_await coroutine::maybe_yield();
    }

    // Removing the segments cannot fail, so every segment taken out of the group below is also
    // freed further down.
    for (auto seg_id : segments) {
        ss.remove_segment(get_segment_descriptor(seg_id));
        co_await coroutine::maybe_yield();
    }

    logstor_logger.debug("Discarding {} segments", segments.size());

    // Invalidate the first header block so recovery treats the slot as empty.
    co_await max_concurrent_for_each(segments, 32, [this] (log_segment_id seg_id) -> future<> {
        logstor_logger.trace("Discard segment {}", seg_id);
        auto [file_id, file_offset] = segment_id_to_file_location(seg_id);
        auto file = co_await _file_mgr.get_file_for_write(file_id);
        co_await _file_mgr.format_file_region(file, file_offset, block_alignment);
        free_segment(seg_id);
    });
}

future<std::optional<segment_header>> segment_manager_impl::read_segment_header(log_segment_id segment_id) {
    auto in = co_await create_segment_input_stream(segment_id, file_input_stream_options {
        .buffer_size = block_alignment,
        .read_ahead = 0,
    });
    auto result = co_await coroutine::as_future(::replica::logstor::read_segment_header(in));
    co_await in.close();
    if (result.failed()) {
        co_return coroutine::exception(result.get_exception());
    }
    co_return result.get();
}

template <record_consumer_like RecordConsumer>
future<> segment_manager_impl::scan_segment(log_segment_id segment_id,
                                std::function<future<>(const segment_header&)> header_callback,
                                std::function<want_data(log_location, const log_record_header&)> on_header,
                                RecordConsumer on_record) {
    auto in = co_await create_segment_input_stream(segment_id, seastar::file_input_stream_options {
        .buffer_size = std::max<size_t>(_cfg.segment_size, 128 * 1024),
        .read_ahead = 0,
    });
    auto scan_result = co_await coroutine::as_future(::replica::logstor::scan_segment(in, segment_id, _cfg.segment_size,
            std::move(header_callback), std::move(on_header), std::move(on_record)));
    co_await in.close();
    if (scan_result.failed()) {
        co_await coroutine::return_exception_ptr(scan_result.get_exception());
    }
}

future<> compaction_manager_impl::submit_group_compaction(logstor_group& cg, std::function<future<>(group_compaction_state&)> op, scheduling_group sg) {
    while (can_submit_compaction()) {
        auto* state = find_group_state(cg);
        if (!state) {
            co_await coroutine::return_exception(std::runtime_error("logstor group submitted for compaction but it is not registered"));
        }

        auto completion = state->completion;
        if (!completion.available()) {
            auto f = co_await coroutine::as_future(completion.get_future());
            f.ignore_ready_future();
            continue;
        }

        if (state->compaction_disabled_counter > 0) {
            co_await coroutine::return_exception(std::runtime_error("logstor group submitted for compaction but compaction is disabled"));
        }

        state->as = {};
        state->completion = shared_future(with_scheduling_group(sg, [state, op = std::move(op)] {
            return op(*state);
        }).handle_exception([this] (std::exception_ptr ep) {
            ++_stats.compaction_failures;
            logstor_logger.warn("logstor compaction failed: {}", ep);
            return make_exception_future<>(std::move(ep));
        }));
        co_return co_await state->completion.get_future();
    }

    co_await coroutine::return_exception(std::runtime_error("logstor compaction submission is closed"));
}

future<> compaction_manager_impl::submit_normal_compaction(logstor_group& cg) {
    co_await submit_group_compaction(cg, [this, &cg] (group_compaction_state& state) {
        return do_compaction(cg, state.as);
    }, _cfg.compaction_sg);
}

future<> compaction_manager_impl::submit_split_compaction(logstor_group& src, mutation_writer::classify_by_token_group classifier, split_target_group target_group) {
    co_await submit_group_compaction(src, [this, &src, classifier = std::move(classifier), target_group = std::move(target_group)] (group_compaction_state& state) mutable {
        return do_split_compaction(src, std::move(classifier), std::move(target_group), state.as);
    }, _cfg.split_compaction_sg);
}

void compaction_manager_impl::submit(logstor_group& cg) {
    (void)submit_normal_compaction(cg).handle_exception([table_id = cg.table_id()] (std::exception_ptr ep) {
        logstor_logger.debug("logstor compaction for table {} failed: {}. Ignored", table_id, ep);
    });
}

future<> compaction_manager_impl::stop_ongoing_compactions(logstor_group& cg) {
    auto reenabler = co_await disable_compaction(cg);
}

future<> compaction_manager_impl::remove(logstor_group& cg) {
    auto it = _groups.find(&cg);
    if (it == _groups.end()) {
        co_return;
    }

    auto state = std::move(it->second);
    _groups.erase(it);

    auto direct_result = co_await coroutine::as_future(cg.close_direct_writes());
    if (direct_result.failed()) {
        logstor_logger.warn("Failed to close the direct writes of a logstor compaction group: {}",
                direct_result.get_exception());
    }

    auto close_result = co_await coroutine::as_future(cg.close_separator());
    if (close_result.failed()) {
        logstor_logger.warn("Failed to close the separator of a logstor compaction group: {}",
                close_result.get_exception());
    }

    _sm.update_group_count(_groups.size());

    state->as.request_abort();
    auto f = co_await coroutine::as_future(state->completion.get_future());
    f.ignore_ready_future();
}

future<compaction_reenabler> compaction_manager_impl::disable_compaction(logstor_group& cg) {
    auto* state = find_group_state(cg);
    if (!state) {
        // The group was already removed, so no compaction can run on it anymore and there
        // is nothing to disable. This happens when a group is discarded after it was stopped.
        co_return compaction_reenabler();
    }

    ++state->compaction_disabled_counter;

    // Wait for any ongoing compaction to finish before disabling
    auto f = co_await coroutine::as_future(state->completion.get_future());
    f.ignore_ready_future();

    co_return compaction_reenabler([this, &cg] {
        auto it = _groups.find(&cg);
        if (it != _groups.end()) {
            --it->second->compaction_disabled_counter;
        }
    });
}

compaction_reenabler compaction_manager_impl::disable_compaction_no_wait(logstor_group& cg) {
    auto& state = get_group_state(cg);

    ++state.compaction_disabled_counter;

    return compaction_reenabler([this, &cg] {
        auto it = _groups.find(&cg);
        if (it != _groups.end()) {
            --it->second->compaction_disabled_counter;
        }
    });
}

std::optional<compaction_manager_impl::compaction_candidate>
compaction_manager_impl::select_segments_for_compaction(logstor_group& cg) {
    const auto segment_size = _sm.get_segment_size();
    auto batch = select_compaction_batch(cg.logstor_segments(), segment_size, _compaction_limits.batch_cap);
    if (!batch) {
        return std::nullopt;
    }

    auto candidate = compaction_candidate{
        .group = &cg,
        .segments = batch->segments
            | std::views::transform([this] (const segment_descriptor* desc) { return _sm.desc_to_segment_id(*desc); })
            | std::ranges::to<std::vector<log_segment_id>>(),
        .score = batch->score,
    };

    logstor_logger.trace("Selected segments {} for compaction with n_in {} n_out {} reclaimed {} efficiency {}",
            candidate.segments, candidate.score.n_in, candidate.score.n_out,
            candidate.score.reclaimed(), candidate.score.efficiency(segment_size));

    return candidate;
}

future<std::vector<compaction_manager_impl::compaction_candidate>>
compaction_manager_impl::find_top_compaction_candidates(size_t max_candidates) {
    top_compaction_candidates<compaction_candidate> best_candidates(max_candidates);

    std::vector<logstor_group*> group_snapshot;
    group_snapshot.reserve(_groups.size());
    for (const auto& [cg, state] : _groups) {
        group_snapshot.push_back(cg);
    }

    for (auto* cg : group_snapshot) {
        co_await coroutine::maybe_yield();

        auto it = _groups.find(cg);
        if (it == _groups.end()) {
            continue;
        }

        auto& state = *it->second;
        if (state.running() || state.compaction_disabled_counter > 0) {
            continue;
        }

        auto candidate = select_segments_for_compaction(*it->first);
        if (candidate) {
            best_candidates.add(std::move(*candidate));
        }
    }

    co_return std::move(best_candidates).take();
}

bool compaction_manager_impl::should_run_auto_compaction() noexcept {
    const auto running = auto_compaction_active();

    if (_admission_closed) {
        if (running) {
            logstor_logger.debug("Stopping auto compaction: admission closed");
        }
        return false;
    }

    const auto available_segments = _sm.available_segment_count(write_source::normal_write);
    const auto watermarks = get_free_segment_watermarks();

    if (!auto_compaction_wanted(running, available_segments, watermarks)) {
        if (running) {
            logstor_logger.debug("Stopping auto compaction: available segments {} is above high watermark {}",
                    available_segments, watermarks.high);
        }
        return false;
    }

    if (!running) {
        logstor_logger.debug("Starting auto compaction: available segments {} is below the free segment target {}",
                available_segments, watermarks.low);
    }
    return true;
}

void compaction_manager_impl::schedule_auto_compaction() {
    if (can_submit_compaction() && should_run_auto_compaction() && !auto_compaction_active()) {
        _auto_compaction_completion = shared_future(run_auto_compaction().handle_exception([] (std::exception_ptr ep) {
            logstor_logger.warn("Automatic logstor compaction failed: {}. Ignored", ep);
        }));
    }
}

future<> compaction_manager_impl::run_auto_compaction() {
    // The parallelism is snapshotted for the whole run. It only changes with the free-segment
    // target, which is a rarely moved live-updatable setting, and holding back the difference to
    // the semaphore's static size for the duration of the run is what lets the drain below wait
    // for exactly the jobs this run submitted.
    const auto parallelism = _compaction_limits.auto_parallelism;
    auto held_back = co_await get_units(_auto_compaction_sem, max_auto_compaction_parallelism - parallelism);

    std::vector<compaction_candidate> pending;
    // What the marginal-admission gate below measures a candidate against, from the ranking that
    // produced `pending`.
    std::optional<compaction_candidate_score> admission_bar;

    while (can_submit_compaction() && should_run_auto_compaction()) {
        auto units = co_await get_units(_auto_compaction_sem, 1);

        if (!can_submit_compaction() || !should_run_auto_compaction()) {
            break;
        }

        if (pending.empty()) {
            pending = co_await find_top_compaction_candidates(parallelism);
            if (pending.empty()) {
                co_await seastar::sleep(std::chrono::milliseconds(100));
                continue;
            }
            admission_bar = marginal_admission_bar(pending);
        }

        auto candidate = std::move(pending.back());
        pending.pop_back();

        auto it = _groups.find(candidate.group);
        if (it == _groups.end() || it->second->running() || it->second->compaction_disabled_counter > 0) {
            continue;
        }

        // Whatever the fiber does not hold of the snapshotted parallelism is held by a job in
        // flight, so a candidate that reaches here while the semaphore is short of units would run
        // as a marginal job - one that buys throughput at the marginal write amplification of its
        // own batch. With unequal groups that is arbitrarily bad: a job slot can only be filled by a
        // group that is not already compacting, so a shard whose garbage sits in one tablet spends
        // the rest of its parallelism rewriting groups that hold nothing worth reclaiming. A
        // marginal job therefore has to keep up with the best batch the shard has to offer.
        if (admission_bar && std::cmp_less(_auto_compaction_sem.available_units(), parallelism - 1)
                && !candidate.score.efficiency_at_least(*admission_bar, compaction_marginal_admission_ratio)) {
            ++_stats.compaction_batches_refused;
            logstor_logger.debug("Refused a marginal compaction of table {}: efficiency {} against the best candidate's {}",
                    candidate.group->table_id(), candidate.score.efficiency(_sm.get_segment_size()),
                    admission_bar->efficiency(_sm.get_segment_size()));
            // The ranking is best first, so no later candidate clears the bar either, and nothing
            // the bar is compared with changes until a job finishes and gives its inputs back.
            // Taking the rest of the parallelism is what waits for the jobs in flight; re-ranking
            // every group until then would only find the same answer.
            pending.clear();
            admission_bar.reset();
            auto drained = co_await get_units(_auto_compaction_sem, parallelism - 1);
            continue;
        }

        // The slot is released only once the job is done, which is what wakes this fiber up to
        // submit the next candidate.
        (void)submit_normal_compaction(*candidate.group).handle_exception([] (std::exception_ptr ep) {
            logstor_logger.warn("Automatic logstor compaction failed: {}. Ignored", ep);
        }).finally([units = std::move(units)] {});
    }

    // Wait for submitted jobs to complete.
    co_await get_units(_auto_compaction_sem, parallelism);
}

// A single buffer used by compaction for rewriting records into new segments in a single compaction group.
// `rewrite_record` append a record to the buffer and given it's current location, and
// when the buffer is flushed it updates the index with the new location.
// the buffer is flushed when the next record doesn't fit and on close().
struct compaction_buffer_stats {
    size_t flush_count{0};
    size_t records_rewritten{0};
    size_t records_skipped{0};

    compaction_buffer_stats& operator+=(const compaction_buffer_stats& o) noexcept {
        flush_count += o.flush_count;
        records_rewritten += o.records_rewritten;
        records_skipped += o.records_skipped;
        return *this;
    }

    compaction_buffer_stats operator+(const compaction_buffer_stats& o) const noexcept {
        auto result = *this;
        result += o;
        return result;
    }
};

struct compaction_buffer {
    segment_manager_impl& sm;
    owned_write_buffer buf;
    logstor_group& cg;
    std::vector<future<>> pending_updates;

    compaction_buffer_stats stats;

    compaction_buffer(segment_manager_impl& sm, owned_write_buffer buf, logstor_group& cg)
        : sm(sm)
        , buf(std::move(buf))
        , cg(cg)
    {}

    compaction_buffer(compaction_buffer&& o) noexcept
        : sm(o.sm), buf(std::move(o.buf)), cg(o.cg)
        , pending_updates(std::move(o.pending_updates)), stats(o.stats) {}

    ~compaction_buffer() {
        // A compaction that ends without a final flush - because it failed or was aborted - leaves
        // index updates waiting for a flush that will never happen. close() fails them, so consume
        // them here to keep them from being reported as ignored.
        for (auto& update : pending_updates) {
            (void)std::move(update).handle_exception([] (std::exception_ptr) {});
        }
    }

    future<> flush() {
        if (buf->has_data()) {
            co_await sm.write_full_segment(*buf, cg, write_source::compaction);
            stats.flush_count++;
            logstor_logger.trace("Compaction buffer flushed with {} bytes", buf->net_data_size());
        }
        auto updates = std::move(pending_updates);
        pending_updates.clear();
        co_await when_all_succeed(updates.begin(), updates.end());
        co_await buf->close();
        buf->reset();
    }

    // Closes the buffer and hands it back to the pool. Run from with_closeable(), so it also runs
    // for a compaction that failed or was aborted, where the buffer can still hold records that
    // were never flushed.
    future<> close() noexcept {
        co_await buf->abort_writes(std::make_exception_ptr(
                std::runtime_error("logstor compaction buffer was closed before its records were flushed")));
        buf.reset();
    }

    // Rewrite a single live record into this buffer, updating the index atomically.
    // Returns immediately after queuing the write; caller must co_await close()/flush()
    // to ensure all pending updates complete.
    future<> rewrite_record(primary_index& index, log_location read_location, const log_record_header& record_header, log_record_bytes_view record_bytes) {
        auto* index_ptr = &index;
        auto key = record_header.key;

        auto writer = log_record_bytes_writer(record_header, record_bytes);

        if (!buf->can_fit(writer)) {
            co_await flush();
        }

        auto write_and_update_index = buf->write(std::move(writer)).then_unpack(
                [this, index_ptr, key = std::move(key), read_location]
                (log_location new_location, seastar::gate::holder op) {
            utils::get_local_injector().inject("logstor_compaction_fail_index_update", [] {
                throw std::runtime_error("compaction index update failed by injection");
            });
            if (index_ptr->update_record_location(key, read_location, new_location)) {
                stats.records_rewritten++;
            } else {
                // another write updated this key
                stats.records_skipped++;
            }
        });

        pending_updates.push_back(std::move(write_and_update_index));

        utils::get_local_injector().inject("logstor_compaction_fail_after_rewrite", [] {
            throw std::runtime_error("compaction failed after rewriting a record by injection");
        });
    }
};

// The two compaction buffers a split compaction rewrites into, one per target group, closed as a
// unit so that a failure to close one still closes the other.
struct split_buffer_pair {
    std::array<compaction_buffer, 2> bufs;

    future<> close() noexcept {
        auto first = co_await coroutine::as_future(bufs[0].close());
        co_await bufs[1].close();
        if (first.failed()) {
            logstor_logger.error("Failed to close a split compaction buffer: {}", first.get_exception());
        }
    }
};

future<> compaction_manager_impl::do_compaction(logstor_group& cg, abort_source& as) {
    auto candidate = select_segments_for_compaction(cg);
    if (!candidate) {
        co_return;
    }
    auto segments = std::move(candidate->segments);

    logstor_logger.trace("Starting compaction of segments {} in compaction group {}", segments, cg.table_id());

    auto& index = cg.logstor_index();

    auto nonempty_segments = segments
            | std::views::filter([this] (log_segment_id seg_id) {
                auto& desc = _sm.get_segment_descriptor(seg_id);
                return desc.net_data_size(_sm.get_segment_size()) > 0;
            })
            | std::ranges::to<std::vector<log_segment_id>>();

    // with_closeable() returns the buffer to the pool on every way out, including the ones that
    // throw - a compaction that fails part way through simply discards what it had buffered.
    auto cb_stats = co_await with_closeable(
            compaction_buffer(_sm, co_await _sm._compaction_buffer_pool.allocate(as), cg),
            [this, &index, &nonempty_segments] (compaction_buffer& cb) -> future<compaction_buffer_stats> {
        co_await _sm.for_each_record(nonempty_segments,
            [&index, &cb] (log_location read_location, const log_record_header& record_header) -> want_data {
                if (!index.is_record_alive(record_header.key, read_location)) {
                    cb.stats.records_skipped++;
                    return want_data::no;
                }
                return want_data::yes;
            },
            [&index, &cb] (log_location read_location, const log_record_header& record_header, log_record_bytes_view record_bytes) -> future<> {
                co_await cb.rewrite_record(index, read_location, record_header, record_bytes);
            }
        );
        co_await cb.flush();
        co_return cb.stats;
    });

    const size_t compaction_segments_in = segments.size();
    const size_t compaction_segments_out = cb_stats.flush_count;

    logstor_logger.debug("Compaction complete: in {} out {}", compaction_segments_in, compaction_segments_out);

    // wait for read operations that use the old locations
    co_await index.await_pending_reads();
    co_await utils::get_local_injector().inject("logstor_compaction_wait_before_remove_segments", utils::wait_for_message(std::chrono::seconds{60}));

    // Free the compacted segments
    auto& ss = cg.logstor_segments();
    for (auto seg_id : segments) {
        logstor_logger.trace("Free segment {} by compaction", seg_id);
        auto& desc = _sm.get_segment_descriptor(seg_id);
        ss.remove_segment(desc);
        if (desc.ref_count == 0) {
            _sm.free_segment(seg_id);
        }
    }

    _stats.compaction_segments_in += compaction_segments_in;
    _stats.compaction_segments_out += compaction_segments_out;
    _stats.compaction_records_rewritten += cb_stats.records_rewritten;
    _stats.compaction_records_skipped += cb_stats.records_skipped;
    _stats.compaction_bytes_read += nonempty_segments.size() * _sm.get_segment_size();
}

future<> compaction_manager_impl::do_split_compaction(logstor_group& src, mutation_writer::classify_by_token_group classifier, split_target_group target_group, abort_source& as) {
    static constexpr size_t batch_size = 32;

    auto& src_segments = src.logstor_segments();

    // the src and target groups both share the table index
    auto& index = src.logstor_index();

    while (!src_segments._segments.empty() && !as.abort_requested()) {
        // Collect candidate IDs without yielding to avoid iterator invalidation.
        std::vector<log_segment_id> candidates;
        candidates.reserve(batch_size);
        for (const auto& cand_desc : src_segments._segments) {
            candidates.push_back(_sm.desc_to_segment_id(cand_desc));
            if (candidates.size() == batch_size) {
                break;
            }
        }

        // For each candidate, check whether it already belongs to a single group (fast path)
        // or straddles the split boundary (slow path).
        // Fast-path segments are moved to the correct child group immediately.
        // Slow-path segments are collected into a batch for rewriting.
        std::vector<log_segment_id> batch;
        batch.reserve(candidates.size());
        for (auto cand_seg_id : candidates) {
            auto cand_hdr = co_await _sm.read_segment_header(cand_seg_id);
            if (!cand_hdr || !std::holds_alternative<segment_header::full>(cand_hdr->v)) {
                on_internal_error(logstor_logger, format("Invalid segment header for segment {} during split compaction", cand_seg_id));
            }
            auto& cand_seg_hdr = std::get<segment_header::full>(cand_hdr->v);
            if (classifier(cand_seg_hdr.first_token) == classifier(cand_seg_hdr.last_token)) {
                // Fast path: segment already belongs to a single group.
                // Remove from src and add to the correct child group.
                logstor_logger.trace("Fast path split segment {} with token range [{}, {}]", cand_seg_id, cand_seg_hdr.first_token, cand_seg_hdr.last_token);
                auto& target = target_group(cand_seg_id, cand_seg_hdr.first_token, cand_seg_hdr.last_token);
                // A target that is the group being split would take the segment straight back, and
                // the loop would keep handing it the same segment forever.
                if (&target == &src) {
                    on_internal_error(logstor_logger, format("Split compaction of segment {} targets the group it is split out of", cand_seg_id));
                }
                auto& cand_desc = _sm.get_segment_descriptor(cand_seg_id);
                src_segments.remove_segment(cand_desc);
                target.add_logstor_segment(cand_desc);
            } else {
                batch.push_back(cand_seg_id);
            }
        }

        if (batch.empty()) {
            continue;
        }

        // Slow path: rewrite live records from straddling segments into two compaction_buffers
        // (one per target group). Both buffers write back into src; the next outer loop
        // iteration will fast-path the resulting single-group segments to the correct child group.

        auto nonempty_segments = batch
                | std::views::filter([this] (log_segment_id seg_id) {
                    auto& desc = _sm.get_segment_descriptor(seg_id);
                    return desc.net_data_size(_sm.get_segment_size()) > 0;
                })
                | std::ranges::to<std::vector<log_segment_id>>();

        auto split_buffers = co_await _sm._compaction_buffer_pool.allocate_many(2, as);
        auto batch_stats = co_await with_closeable(
                split_buffer_pair{
                    compaction_buffer(_sm, std::move(split_buffers[0]), src),
                    compaction_buffer(_sm, std::move(split_buffers[1]), src),
                },
                [this, &index, &classifier, &nonempty_segments] (split_buffer_pair& bufs) -> future<compaction_buffer_stats> {
            co_await _sm.for_each_record(nonempty_segments,
                [&index] (log_location read_location, const log_record_header& record_header) -> want_data {
                    if (!index.is_record_alive(record_header.key, read_location)) {
                        return want_data::no;
                    }
                    return want_data::yes;
                },
                [&index, &bufs, &classifier] (log_location read_location, const log_record_header& record_header, log_record_bytes_view record_bytes) -> future<> {
                    auto& cb = bufs.bufs[classifier(record_header.key.dk.token())];
                    co_await cb.rewrite_record(index, read_location, record_header, record_bytes);
                }
            );
            co_await coroutine::parallel_for_each(bufs.bufs, &compaction_buffer::flush);
            co_return bufs.bufs[0].stats + bufs.bufs[1].stats;
        });

        logstor_logger.debug("Split compaction: flushed {} times from {} segments",
                             batch_stats.flush_count, batch.size());

        _stats.compaction_bytes_read += nonempty_segments.size() * _sm.get_segment_size();

        // All records are safely written to new segments in src.
        // Await pending reads before freeing the source segments.
        co_await index.await_pending_reads();

        // Remove and free the source segments of this batch.
        for (auto seg_id : batch) {
            logstor_logger.trace("Free segment {} by split", seg_id);
            auto& desc = _sm.get_segment_descriptor(seg_id);
            src_segments.remove_segment(desc);
            if (desc.ref_count == 0) {
                _sm.free_segment(seg_id);
            }
        }
    }
}

future<> compaction_manager_impl::flush_all_separator_buffers(std::optional<segment_sequence> seq) {
    co_await coroutine::parallel_for_each(_groups, [seq] (auto& entry) {
        return entry.first->flush_separator(seq);
    });
}

void compaction_manager_impl::rotate_direct_buffer(logstor_group& cg) {
    _sm.rotate_direct_buffer(cg);
}

future<> compaction_manager_impl::release_direct_buffers(logstor_group& cg) {
    return _sm.release_direct_buffers(cg);
}

future<> compaction_manager_impl::drain_all_direct_buffers() {
    co_await coroutine::parallel_for_each(_groups, [] (auto& entry) {
        return entry.first->close_direct_writes();
    });
}

future<> compaction_manager_impl::promote_direct_writes_for_test(logstor_group& cg) {
    // One attempt, and it never waits - a promotion needs two segments the reserve has already
    // built, and a caller that wants the answer a running shard would give has to come back for it,
    // the way the sync fiber does.
    _sm.promote_direct_group(cg);
    co_await cg.flush_direct_writes();
}

void compaction_manager_impl::tick_direct_deadlines(seastar::lowres_clock::time_point now) {
    // Nothing here yields, so the groups cannot change under it and it needs neither a snapshot of
    // them nor a re-check after each one.
    for (const auto& [cg, state] : _groups) {
        _sm.tick_direct_deadlines(*cg, now);
    }
}

future<> compaction_manager_impl::run_direct_controller() {
    // The groups are re-checked after every yield: one of them can be removed while this walks them.
    std::vector<logstor_group*> group_snapshot;
    group_snapshot.reserve(_groups.size());
    for (const auto& [cg, state] : _groups) {
        group_snapshot.push_back(cg);
    }

    for (auto* cg : group_snapshot) {
        co_await coroutine::maybe_yield();
        if (!_groups.contains(cg)) {
            continue;
        }
        co_await _sm.run_direct_controller(*cg);
    }
}

void separator_index_update::apply(log_location buffer_location) const {
    index->update_record_location(key, prev_location, record_location(buffer_location, offset_in_buffer, size));
}

future<> segment_manager_impl::write_to_separator(std::vector<write_buffer::record_in_buffer>& records, log_location buffer_location,
        segment_ref seg_ref, segment_sequence segment_seq_num) {
    static constexpr size_t separator_group_write_concurrency = 4;

    struct separator_group_records {
        logstor_group* cg;
        std::vector<write_buffer::record_in_buffer*> records;
    };

    std::vector<separator_group_records> groups;
    groups.reserve(records.size());
    absl::flat_hash_map<logstor_group*, size_t> group_index;
    group_index.reserve(records.size());

    for (auto& record : records) {
        auto [it, inserted] = group_index.emplace(record.target.cg, groups.size());
        if (inserted) {
            groups.push_back(separator_group_records{
                .cg = record.target.cg,
            });
        }
        groups[it->second].records.push_back(&record);
        co_await coroutine::maybe_yield();
    }

    co_await seastar::max_concurrent_for_each(groups, separator_group_write_concurrency,
            [buffer_location, seg_ref, segment_seq_num] (separator_group_records& group) -> future<> {
        for (auto* record : group.records) {
            co_await group.cg->write_to_separator(std::move(record->writer), seg_ref, segment_seq_num,
                    record->location(buffer_location));
        }
    });
}

future<> compaction_manager_impl::flush_separator_buffer(separator_buffer& buf, logstor_group& cg) {
    logstor_logger.trace("Flushing separator buffer with {} bytes", buf.offset_in_buffer());

    auto flush_result = co_await coroutine::as_future([this, &buf, &cg] () -> future<> {
        utils::get_local_injector().inject("fail_flush_separator_buffer", []() {
            throw std::runtime_error("flush_separator_buffer failed by injection");
        });

        if (!buf.empty()) {
            co_await with_scheduling_group(_cfg.separator_sg, [&] {
                return _sm.write_full_segment(*buf.buf, cg, write_source::separator);
            });
            _stats.separator_buffer_flushed++;
        }

        // The index updates the records of the buffer owed were applied by the write above, before
        // the segment it wrote joined the compaction group - see write_buffer::complete_writes().
        co_await buf.close();

        // wait for read operations that use the old locations before freeing the old segments
        co_await cg.logstor_index().await_pending_reads();
    }());

    if (flush_result.failed()) {
        auto ep = flush_result.get_exception();
        ++_stats.separator_flush_failures;
        logstor_logger.debug("logstor separator flush failure: {}", ep);

        auto abort_result = co_await coroutine::as_future(buf.abort(ep));
        if (abort_result.failed()) {
            logstor_logger.warn("logstor separator buffer abort failed after flush failure: {}", abort_result.get_exception());
        }

        co_await buf.release();

        co_await coroutine::return_exception_ptr(std::move(ep));
    }

    // give the buffer back to the pool and release all held segments
    co_await buf.release();
}

future<> segment_manager_impl::do_recovery(replica::database& db) {
    logstor_logger.info("Starting recovery for shard {} in directory {}", this_shard_id(), _cfg.base_dir.string());

    co_await _file_mgr.start();

    // Scan the base directory for all files belonging to this shard.
    std::set<file_id_t> found_file_ids;
    std::vector<sstring> files_for_removal;
    std::string file_prefix = _file_mgr.get_file_name_prefix();
    co_await lister::scan_dir(_cfg.base_dir, lister::dir_entry_types::of<directory_entry_type::regular>(),
            [this, &found_file_ids, &files_for_removal, &file_prefix] (fs::path dir, directory_entry de) {
        if (!de.name.starts_with(file_prefix)) {
            // not our file
            return make_ready_future<>();
        }
        auto file_id_opt = _file_mgr.file_name_to_file_id(de.name);
        if (file_id_opt) {
            found_file_ids.insert(*file_id_opt);
        } else if (de.name.ends_with(".tmp")) {
            files_for_removal.push_back((dir / de.name).string());
        }
        return make_ready_future<>();
    });
    logstor_logger.info("Recovery: found {} files for shard {} in {}", found_file_ids.size(), this_shard_id(), _cfg.base_dir.string());

    // Remove any leftover temp files
    co_await coroutine::parallel_for_each(files_for_removal.begin(), files_for_removal.end(),
        [] (const sstring& file_path) {
            logstor_logger.info("Recovery: removing leftover temp file {}", file_path);
            return seastar::remove_file(file_path);
        }
    );

    // Verify all files are present
    file_id_t next_file_id = 0;
    for (auto file_id : found_file_ids) {
        if (file_id != next_file_id) {
            throw std::runtime_error(fmt::format("Missing log segment file(s) detected during recovery: file {} missing", _file_mgr.get_file_path(next_file_id).string()));
        }
        next_file_id++;
    }

    uint64_t allocated_segment_count = next_file_id * _segments_per_file;

    _file_mgr.set_actual_max_files(std::max(_file_mgr.configured_max_files(), next_file_id));
    set_actual_max_segments(std::max(_max_segments.configured, allocated_segment_count));

    std::vector<segment_sequence> segment_seqs(static_cast<size_t>(allocated_segment_count), segment_sequence(0));
    segment_sequence max_segment_seq = segment_sequence(0);

    // Populate the index from all segments. Keep the latest record for each key.
    // For equal records, keep the one from the segment with the highest sequence number.
    auto cmp_with_seq = [&segment_seqs] (const index_entry& old_entry, const index_entry& candidate) -> std::strong_ordering {
        if (auto c = primary_index::default_entry_cmp{}(old_entry, candidate); c != 0) {
            return c;
        }
        const auto old_seq = segment_seqs[old_entry.location.segment.value];
        const auto new_seq = segment_seqs[candidate.location.segment.value];
        if (auto c = old_seq <=> new_seq; c != 0) {
            return c;
        }
        return old_entry.location.offset <=> candidate.location.offset;
    };

    for (auto file_id : found_file_ids) {
        logstor_logger.info("Recovering segments from file {}: {}%", _file_mgr.get_file_path(file_id).string(), (file_id + 1) * 100 / found_file_ids.size());
        co_await max_concurrent_for_each(segments_in_file(file_id), 32,
            [this, &db, &cmp_with_seq, &segment_seqs, &max_segment_seq] (log_segment_id seg_id) {
                return recover_segment(db, seg_id, cmp_with_seq,
                    [seg_id, &segment_seqs, &max_segment_seq] (const segment_header& seg_hdr) {
                        segment_seqs[seg_id.value] = seg_hdr.segment_seq;
                        max_segment_seq = std::max(max_segment_seq, seg_hdr.segment_seq);
                    });
            }
        );
    }

    // go over the index and mark all segments that have live data as used.
    utils::dynamic_bitset used_segments(static_cast<size_t>(allocated_segment_count));

    co_await db.get_tables_metadata().for_each_table_gently([&] (table_id tid, lw_shared_ptr<table> tp) -> future<> {
        if (!tp->uses_logstor()) {
            co_return;
        }
        logstor_logger.info("Table {}.{} has {} entries in logstor index", tp->schema()->ks_name(), tp->schema()->cf_name(), tp->logstor_index().get_key_count());
        for (const auto& entry : tp->logstor_index()) {
            used_segments.set(entry.entry().location.segment.value);
            co_await coroutine::maybe_yield();
        }
    });

    // put used segments in compaction groups, and put the rest in the free list.
    uint64_t free_segment_count = 0;
    uint64_t used_segment_count = 0;
    uint64_t max_used_seg_idx = 0;
    for (uint64_t seg_idx = 0; seg_idx < allocated_segment_count; ++seg_idx) {
        co_await coroutine::maybe_yield();
        log_segment_id seg_id(seg_idx);
        if (!used_segments.test(seg_idx)) {
            if (seg_idx < _max_segments.configured) {
                _free_segments.push_back(seg_id);
                free_segment_count++;
            }
        } else {
            used_segment_count++;
            max_used_seg_idx = seg_idx;
        }
    }
    logstor_logger.info("Found {} used segments and {} free segments", used_segment_count, free_segment_count);

    auto target_file_count = std::max(max_used_seg_idx / _segments_per_file + 1, _file_mgr.configured_max_files());
    for (; next_file_id > target_file_count; next_file_id--) {
        auto file_id = next_file_id - 1;
        logstor_logger.info("Recovery: removing retired file {}", _file_mgr.get_file_path(file_id).string());
        co_await _file_mgr.remove_file(file_id);
    }

    allocated_segment_count = next_file_id * _segments_per_file;
    set_actual_max_segments(std::max(_max_segments.configured, allocated_segment_count));

    uint64_t recovered_used_segment_count = 0;
    if (used_segments.size() > 0) {
        for (auto seg_idx = used_segments.find_first_set(); seg_idx != utils::dynamic_bitset::npos; seg_idx = used_segments.find_next_set(seg_idx)) {
            co_await coroutine::maybe_yield();
            log_segment_id seg_id(seg_idx);
            auto& desc = get_segment_descriptor(seg_id);
            logstor_logger.trace("Recovering used segment {}", seg_id);
            if (recovered_used_segment_count % 1000 == 0) {
                logstor_logger.info("Recovering used segments: {}%", 100 * recovered_used_segment_count / used_segment_count);
            }
            co_await add_segment_to_compaction_group(db, desc);
            recovered_used_segment_count++;
        }
    }

    _next_new_segment_id = allocated_segment_count;
    _next_segment_seq = max_segment_seq + 1;

    co_await _file_mgr.recover_next_file(next_file_id);

    logstor_logger.info("Recovery complete");
}

future<> segment_manager_impl::do_recovery_for_test() {
    co_await _file_mgr.start();
    co_await _file_mgr.recover_next_file(0);
}

future<> segment_manager_impl::recover_segment(replica::database& db, log_segment_id segment_id,
        primary_index::entry_cmp_fn cmp, std::function<void(const segment_header&)> on_header) {
    auto& desc = get_segment_descriptor(segment_id);
    desc.reset(_cfg.segment_size);

    co_await scan_segment(segment_id,
        [segment_id, on_header = std::move(on_header)] (const segment_header& seg_hdr) mutable {
            logstor_logger.trace("Recovering segment {} with sequence {}", segment_id, seg_hdr.segment_seq);
            on_header(seg_hdr);
            return make_ready_future<>();
        },
        [&db, &cmp] (log_location loc, const log_record_header& header) -> want_data {
            logstor_logger.trace("Recovery: read record at {} key {} ts {}", loc, header.key, header.timestamp);

            index_entry new_entry {
                .location = loc,
                .timestamp = header.timestamp
            };

            try {
                auto& t = db.find_column_family(header.table);
                if (!t.uses_logstor()) {
                    return want_data::no;
                }
                t.logstor_index().insert(header.key, new_entry, cmp);
            } catch (const replica::no_such_column_family&) {
                // ignore record
            }

            return want_data::no;
        },
        [] (log_location, log_record) {
            // we don't read record data, only headers.
            return make_ready_future<>();
        });
}

void segment_manager::on_add_record(log_location location) noexcept {
    get_impl().on_add_record(location);
}

void segment_manager::on_free_record(log_location location) noexcept {
    get_impl().on_free_record(location);
}

future<> segment_manager_impl::add_segment_to_compaction_group(replica::database& db, segment_descriptor& desc) {
    auto seg_id = desc_to_segment_id(desc);
    auto maybe_header = co_await read_segment_header(seg_id);
    if (!maybe_header) {
        co_return;
    }
    auto& header = *maybe_header;

    bool need_separator = false;

    switch (header.kind) {
    case segment_kind::mixed:
        logstor_logger.debug("Recovering mixed segment {} using separator", seg_id);
        need_separator = true;
        break;
    case segment_kind::full:
        auto& seg_header = std::get<segment_header::full>(header.v);
        try {
            auto& t = db.find_column_family(seg_header.table);
            t.get_logstor_group(seg_id, seg_header.first_token, seg_header.last_token).add_logstor_segment(desc);
            logstor_logger.debug("Added segment {} with tokens [{},{}] to compaction group of table {}.{}", seg_id, seg_header.first_token, seg_header.last_token, t.schema()->ks_name(), t.schema()->cf_name());
        } catch (const replica::no_such_column_family&) {
            co_return;
        }
        break;
    }

    if (need_separator) {
        auto seg_ref = make_segment_ref(seg_id);
        auto write_to_separator_failed = defer([seg_ref] mutable noexcept {
            seg_ref.set_flush_failure();
        });
        co_await for_each_record(seg_id,
            [&db] (log_location prev_loc, const log_record_header& record_header) -> want_data {
                try {
                    auto& t = db.find_column_family(record_header.table);
                    return t.uses_logstor() && t.logstor_index().is_record_alive(record_header.key, prev_loc)
                            ? want_data::yes : want_data::no;
                } catch (const replica::no_such_column_family&) {
                    return want_data::no;
                }
            },
            [seg_ref, &db] (log_location prev_loc, const log_record_header& record_header, log_record_bytes_view record_bytes) -> future<> {
                try {
                    auto& t = db.find_column_family(record_header.table);
                    auto& cg = t.get_logstor_group(record_header.key.dk.token());
                    auto writer = log_record_bytes_writer(record_header, record_bytes);

                    co_await cg.write_to_separator(std::move(writer), seg_ref, std::nullopt, prev_loc);
                } catch (const replica::no_such_column_family&) {
                    // ignore
                }
            });
        write_to_separator_failed.cancel();
    }
}

future<utils::chunked_vector<segment_snapshot>> segment_manager_impl::make_snapshot(logstor_group& cg) {
    auto compaction_disable_guard = co_await _compaction_mgr.disable_compaction(cg);

    auto& segments = cg.logstor_segments();

    // iterate the segment list by index - safe to do with yields.
    // segments may be added to the end of the list, but not removed since compaction is disabled.
    const auto segment_count = segments.segment_count();
    utils::chunked_vector<segment_snapshot> snp;
    snp.reserve(segment_count);
    for (size_t i = 0; i < segment_count; ++i) {
        auto seg_id = desc_to_segment_id(*segments._segment_list[i]);
        snp.push_back(segment_snapshot{
            .segment_id = seg_id,
            .seg_ref = make_segment_ref(seg_id),
            .source = [this, seg_id] (const file_input_stream_options& opts) {
                return create_segment_input_stream(seg_id, opts);
            }
        });
        co_await coroutine::maybe_yield();
    }

    co_return std::move(snp);
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

future<> segment_manager::do_recovery(replica::database& db) {
    return _impl->do_recovery(db);
}

future<> segment_manager::do_recovery_for_test() {
    return _impl->do_recovery_for_test();
}

future<> segment_manager::start() {
    return _impl->start();
}

future<> segment_manager::stop() {
    return _impl->stop();
}

future<> segment_manager::write(write_buffer& wb) {
    return _impl->write(wb);
}

std::optional<log_location> segment_manager::try_write_direct(logstor_group& cg,
        const log_record_header_view& header, bytes_view value) {
    return _impl->try_write_direct(cg, header, value);
}

future<temporary_buffer<char>> segment_manager::read_record_bytes(log_location location) {
    return _impl->read_record_bytes(location);
}

compaction_manager& segment_manager::get_compaction_manager() noexcept {
    return _impl->get_compaction_manager();
}

const compaction_manager& segment_manager::get_compaction_manager() const noexcept {
    return _impl->get_compaction_manager();
}

uint64_t segment_manager::get_segment_size() const noexcept {
    return _impl->get_segment_size();
}

future<> segment_manager::discard_segments(logstor_group& cg) {
    return _impl->discard_segments(cg);
}

segment_manager_usage segment_manager::get_usage() const noexcept {
    return segment_manager_usage{
        .free_segments = _impl->available_segment_count(),
        .total_segments = _impl->get_total_segment_count(),
        .segment_pool_size = _impl->get_segment_pool_size(),
        .disk_usage = _impl->get_disk_usage(),
        .memory_usage = _impl->get_memory_usage(),
    };
}

future<> segment_manager::await_pending_writes() {
    return _impl->await_pending_writes();
}

class segment_data_sink_impl : public data_sink_impl {
    seg_ptr _segment;
    streamed_segment_rewriter _rewriter;
public:
    segment_data_sink_impl(seg_ptr segment)
        : _segment(std::move(segment))
        , _rewriter(_segment->id(), _segment->seq_num(), [segment = _segment] (bytes_view data) {
            return segment->append(data).discard_result();
        })
    {}

    virtual future<> put(std::span<temporary_buffer<char>> data) override {
        co_await _rewriter.put(data);
    }

    virtual future<> close() override {
        co_await _rewriter.close();
    }

    virtual size_t buffer_size() const noexcept override {
        return _rewriter.buffer_size();
    }
};

future<seastar::input_stream<char>> segment_manager_impl::create_segment_input_stream(log_segment_id segment_id, const seastar::file_input_stream_options& opts) {
    auto [file_id, file_offset] = segment_id_to_file_location(segment_id);
    auto file = co_await _file_mgr.get_file_for_read(file_id);
    auto stream = make_file_input_stream(std::move(file), file_offset, _cfg.segment_size, opts);
    co_return std::move(stream);
}

class segment_stream_sink_impl : public segment_stream_sink {
    segment_manager_impl& _sm;
    replica::database& _db;
    seg_ptr _seg;
public:
    segment_stream_sink_impl(segment_manager_impl& sm, replica::database& db, seg_ptr seg)
        : _sm(sm), _db(db), _seg(std::move(seg))
    {}
public:
    log_segment_id segment_id() const noexcept override {
        return _seg->id();
    }
    future<output_stream<char>> output() override {
        auto sink = std::make_unique<segment_data_sink_impl>(_seg);
        auto stream = output_stream<char>(data_sink(std::move(sink)));
        co_return std::move(stream);
    }
    future<> close() override {
        co_await _seg->stop();
        co_await _sm.load_segment(_db, _seg->id());
    }
    future<> abort() override {
        co_await _seg->stop();
    }
};

future<> segment_manager_impl::load_segment(replica::database& db, log_segment_id seg_id) {
    // read the segment and populate the index
    co_await recover_segment(db, seg_id, primary_index::default_entry_cmp{}, [] (const segment_header&) {});

    auto& desc = get_segment_descriptor(seg_id);
    co_await add_segment_to_compaction_group(db, desc);
}

future<std::unique_ptr<segment_stream_sink>> segment_manager_impl::create_segment_output_stream(replica::database& db) {
    auto seg = co_await get_segment(write_source::streaming);
    co_return std::make_unique<segment_stream_sink_impl>(*this, db, std::move(seg));
}

future<utils::chunked_vector<segment_snapshot>> segment_manager::make_snapshot(logstor_group& cg) {
    return _impl->make_snapshot(cg);
}

future<std::unique_ptr<segment_stream_sink>> segment_manager::create_segment_output_stream(replica::database& db) {
    return _impl->create_segment_output_stream(db);
}

separator_buffer::~separator_buffer() {
    if (held_segments.empty()) {
        return;
    }
    logstor_logger.error("Destroying a logstor separator buffer that still holds {} segments,"
            " they will not be reclaimed", held_segments.size());
    for (auto& seg_ref : held_segments) {
        seg_ref.set_flush_failure();
    }
}

future<> separator_buffer::abort(std::exception_ptr ep) {
    for (auto& seg_ref : held_segments) {
        seg_ref.set_flush_failure();
    }
    if (!empty()) {
        co_await buf->abort_writes(std::move(ep));
    }
}

future<> separator_buffer::release() {
    if (buf && !buf->is_closed()) {
        // The pool takes back only closed buffers, and closing waits for the writes the buffer
        // holds, which only a flush or an abort resolves. A buffer that reaches this without either
        // is one whose flush failed before it had anything to abort, so there is nothing to lose by
        // failing its writes here - and a record that was never flushed is still on disk at the
        // location the index points to.
        co_await buf->abort_writes(std::make_exception_ptr(
                std::runtime_error("logstor separator buffer was released before its records were flushed")));
    }
    buf.reset();
    held_segments.clear();
    min_seq_num.reset();
}

// Defined here, where writeable_segment is a complete type.
direct_write_buffer::direct_write_buffer() noexcept = default;
direct_write_buffer::direct_write_buffer(direct_write_buffer&&) noexcept = default;
direct_write_buffer& direct_write_buffer::operator=(direct_write_buffer&&) noexcept = default;
direct_write_buffer::~direct_write_buffer() = default;

// logstor_group

future<> logstor_group::await_direct_settled() {
    while (!_direct_flush.available()) {
        auto f = co_await coroutine::as_future(_direct_flush.get_future());
        // Like the separator waits: a flush that failed has already said so and left the records
        // readable, and there is nothing a waiter here can do about it.
        f.ignore_ready_future();
    }
}

future<> logstor_group::flush_direct_writes() {
    // At most two waits: rotating needs the previous flush to have finished, so the first wait may
    // only see that one out, and the second waits for the flush of what is in the buffer now.
    co_await await_direct_settled();
    if (!_direct_active.empty()) {
        logstor_compaction_manager().rotate_direct_buffer(*this);
        co_await await_direct_settled();
    }
}

future<> logstor_group::close_direct_writes() {
    // Before anything that can wait, so that a write which resumes into the direct path finds the
    // group closed rather than appending into a buffer that is on its way back to the pool.
    _direct_enabled = false;

    co_await flush_direct_writes();
    co_await logstor_compaction_manager().release_direct_buffers(*this);
}


void logstor_group::switch_active_separator_buffer() {
    if (!_separator_flush.available()) {
        on_internal_error(logstor_logger, "Attempted to switch active separator buffer while flush is in progress");
    }

    std::swap(_active_buffer, _flushing_buffer);
    ++_separator_generation;

    _separator_flush = shared_future(logstor_compaction_manager().flush_separator_buffer(_flushing_buffer, *this));
}

future<> logstor_group::allocate_active_separator_buffer() {
    auto buf = co_await logstor_compaction_manager().allocate_separator_buffer();
    if (_separator_enabled && !_active_buffer.allocated()) {
        _active_buffer.buf = std::move(buf);
    }
}

// A record the active buffer can take is buffered by returning, with nothing awaited on the way, so
// this is not a coroutine: one that suspends nowhere would allocate a frame per rewritten record to
// do nothing with it. The waiting - for a buffer, or for the flush of a full one - is what
// wait_and_write_to_separator() is for.
template <log_record_writer_concept Writer>
future<> logstor_group::write_to_separator(Writer writer, segment_ref seg_ref, std::optional<segment_sequence> segment_seq_num, log_location prev_location) {
    if (!_separator_enabled || !_active_buffer.can_fit(writer)) [[unlikely]] {
        return wait_and_write_to_separator(std::move(writer), std::move(seg_ref), segment_seq_num, prev_location);
    }

    _active_buffer.write(std::move(seg_ref), segment_seq_num, std::move(writer), logstor_index(), prev_location);
    return make_ready_future<>();
}

template <log_record_writer_concept Writer>
future<> logstor_group::wait_and_write_to_separator(Writer writer, segment_ref seg_ref, std::optional<segment_sequence> segment_seq_num, log_location prev_location) {
    while (!_active_buffer.can_fit(writer)) {
        if (!_separator_enabled) {
            break;
        }

        if (!_active_buffer.allocated()) {
            co_await allocate_active_separator_buffer();
            continue;
        }

        if (_active_buffer.empty()) {
            // The record does not fit a buffer that has nothing in it, so no flush can make room
            // for it and the loop would never end. The write path should bound a record by what a segment
            // of either kind takes.
            on_internal_error(logstor_logger, fmt::format(
                    "logstor separator record of size {} does not fit a separator buffer of {} bytes",
                    writer.size(), _active_buffer.buf->get_buffer_size()));
        }

        if (!_separator_flush.available()) {
            auto f = co_await coroutine::as_future(_separator_flush.get_future());
            f.ignore_ready_future();
            continue;
        }

        switch_active_separator_buffer();
    }

    // Checked after the loop, so that it also covers the group being closed while this was waiting for
    // a buffer or for a flush. Nothing flushes what a closed group buffers, so fail the write rather
    // than take a record that would never be written out: it stays where it already is, and the caller
    // keeps the segment holding it from being freed - see run_separator_fiber().
    if (!_separator_enabled) {
        throw std::runtime_error(fmt::format(
                "logstor separator write to a closed compaction group of table {}", table_id()));
    }

    _active_buffer.write(std::move(seg_ref), segment_seq_num, std::move(writer), logstor_index(), prev_location);
}

template future<> logstor_group::write_to_separator(log_record_writer, segment_ref, std::optional<segment_sequence>, log_location);
template future<> logstor_group::write_to_separator(log_record_bytes_writer, segment_ref, std::optional<segment_sequence>, log_location);

future<> logstor_group::flush_separator(std::optional<segment_sequence> seq_num) {
    // Only for a drain of everything the group holds. A sequence number means the caller is after
    // the buffers still pinning segments older than it - the periodic nudge from
    // switch_active_segment() - which is no reason to force a partly filled direct buffer out.
    // Every real drain - table flush, split, snapshot, group stop, truncate - passes none.
    if (!seq_num) {
        co_await flush_direct_writes();
    }

    auto should_flush = [seq_num] (separator_buffer& buf) {
        return !buf.empty() && (!seq_num || (buf.min_seq_num && buf.min_seq_num < *seq_num));
    };

    auto target_generation = _separator_generation;
    if (!should_flush(_active_buffer)) {
        if (_separator_flush.available()) {
            co_return;
        }
        target_generation--;
    }

    while (target_generation >= _separator_generation) {
        if (!_separator_flush.available()) {
            auto f = co_await coroutine::as_future(_separator_flush.get_future());
            f.ignore_ready_future();
            continue;
        }

        switch_active_separator_buffer();
    }

    if (target_generation == _separator_generation - 1 && !_separator_flush.available()) {
        auto f = co_await coroutine::as_future(_separator_flush.get_future());
        f.ignore_ready_future();
    }
}

future<> logstor_group::close_separator() {
    // Before anything that can wait: a separator write that is parked on the pool or on the flush
    // below resumes into write_to_separator()'s check and fails there, rather than buffering a record
    // into a group whose buffers are on their way back to the pool.
    _separator_enabled = false;

    // A flush in progress owns the buffer it is writing out, and releases it when it is done, so let
    // it finish rather than pulling the buffer out from under it.
    if (!_separator_flush.available()) {
        auto f = co_await coroutine::as_future(_separator_flush.get_future());
        f.ignore_ready_future();
    }

    auto discard = [this] (separator_buffer& buf) -> future<> {
        if (!buf.allocated()) {
            co_return;
        }
        if (!buf.empty()) {
            logstor_logger.warn("Discarding a logstor separator buffer of table {} with {} bytes from {} segments,"
                    " the segments will not be reclaimed", table_id(), buf.offset_in_buffer(), buf.held_segments.size());
        }
        // The same teardown a failed flush takes: abort() fails the writes the buffer holds, which
        // is what lets it be closed, and marks the segments those writes came from. Failing partway
        // leaves the rest to the group's destruction, which is why remove() only logs what comes out
        // of here.
        co_await buf.abort(std::make_exception_ptr(
                std::runtime_error("logstor separator buffer was discarded with its compaction group")));
        co_await buf.release();
    };
    co_await discard(_active_buffer);
    co_await discard(_flushing_buffer);
}

}

template<>
size_t hist_key<replica::logstor::segment_descriptor>(const replica::logstor::segment_descriptor& desc) {
    return desc.free_space;
}
