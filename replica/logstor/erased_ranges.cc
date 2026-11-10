/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/erased_ranges.hh"

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>
#include <queue>
#include <stdexcept>
#include <tuple>
#include <utility>

#include <fmt/format.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/as_future.hh>

#include "dht/token.hh"
#include "replica/logstor/index.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/segment_file.hh"
#include "serializer.hh"
#include "utils/crc.hh"
#include "utils/error_injection.hh"

namespace replica::logstor {

namespace {

// The value spells "LGSE" in the order of its bytes from the most significant, as
// ondisk::chunk_header_magic spells "LGSB". The file holds it little-endian, so the bytes on disk
// read "ESGL".
constexpr uint32_t file_magic = 0x4c475345;
// The load refuses a file of another version.
constexpr uint8_t file_version = 1;

// The erased range file holds this header, then `count` erased ranges (see erased_range), and then
// a CRC32 of all the bytes before the CRC. The header, little-endian:
//
//   0         4     5            8         12                 20
//   +---------+-----+------------+---------+------------------+
//   |  magic  | ver |  reserved  |  count  |    seq_floor     |
//   +---------+-----+------------+---------+------------------+
//
// fixed_serializer writes the members in declaration order, so the order of the members is the
// on-disk layout. A test checks the exact bytes of the file.
struct file_header : ondisk::fixed_layout {
    uint32_t magic;
    uint8_t version;
    // The writer sets these bytes to zero.
    std::array<char, 3> reserved;
    uint32_t count;
    // The sequence floor of the set, see erased_ranges::seq_floor().
    segment_sequence seq_floor;
};

constexpr size_t header_size = ondisk::fixed_size<file_header>;
constexpr size_t erased_range_size = ondisk::fixed_size<erased_range>;
constexpr size_t crc_size = ondisk::field<uint32_t>::size;

static_assert(header_size == 20, "The header must keep the layout of the diagram");
static_assert(erased_range_size == 40, "An erased range must keep the layout of the diagram at erased_range");

constexpr int64_t min_raw_token = std::numeric_limits<int64_t>::min();
constexpr int64_t max_raw_token = std::numeric_limits<int64_t>::max();

size_t file_size_for(size_t count) {
    return header_size + count * erased_range_size + crc_size;
}

// The buffer size of the streams that read and write the file. The streams keep the memory of a
// load or a save to a few buffers, whatever the number of erased ranges.
constexpr size_t stream_buffer_size = 128 * 1024;
// The number of erased ranges that a load or a save reads or writes in one step, which keeps a step
// below a block.
constexpr size_t ranges_per_batch = 100;

void add_to_crc(utils::crc32& crc, const char* data, size_t size) {
    crc.process(reinterpret_cast<const uint8_t*>(data), size);
}

} // anonymous namespace

erased_range erased_ranges::make(table_id table, const dht::token_range& range, segment_sequence seq) {
    const auto bounds = raw_token_bounds::of(range);
    if (!bounds) {
        on_internal_error(logstor_logger, fmt::format("Making an erased range of table {} for the empty range {}", table, range));
    }
    return erased_range{
        .table = table,
        .first_token = bounds->first,
        .last_token = bounds->last,
        .seq = seq,
    };
}

std::filesystem::path erased_ranges::file_path(const std::filesystem::path& base_dir, unsigned shard) {
    return file_name::erase(shard).in(base_dir);
}

future<erased_ranges> erased_ranges::load(const std::filesystem::path& base_dir, unsigned shard) {
    const auto path = file_path(base_dir, shard).string();
    erased_ranges result;
    if (!co_await seastar::file_exists(path)) {
        co_return result;
    }

    std::unordered_map<table_id, std::vector<range_in_table>> by_table;
    co_await seastar::with_file(seastar::open_file_dma(path, seastar::open_flags::ro), [&] (seastar::file& f) -> future<> {
        // The load checks only that the header fits here, and checks the size after the version.
        const auto size = co_await f.size();
        if (size < header_size) {
            throw std::runtime_error(fmt::format("Corrupt logstor erased range file {}: size {}", path, size));
        }
        auto in = seastar::make_file_input_stream(f, 0, size, seastar::file_input_stream_options{.buffer_size = stream_buffer_size});
        auto read = co_await coroutine::as_future([&] () -> future<> {
            utils::crc32 crc;
            const auto header_bytes = co_await in.read_exactly(header_size);
            add_to_crc(crc, header_bytes.get(), header_bytes.size());
            auto header_in = seastar::simple_memory_input_stream(header_bytes.get(), header_bytes.size());
            const auto header = ser::serializer<file_header>::read(header_in);
            if (header.magic != file_magic) {
                throw std::runtime_error(fmt::format("Corrupt logstor erased range file {}: magic {:#x}", path, header.magic));
            }
            // The load checks the version before the size and the CRC, because a file of a later
            // version can have another layout and another size.
            if (header.version != file_version) {
                throw std::runtime_error(fmt::format("Logstor erased range file {} has format version {}, and this release reads version {}",
                        path, header.version, file_version));
            }
            if (file_size_for(header.count) != size) {
                throw std::runtime_error(fmt::format("Corrupt logstor erased range file {}: count {} size {}", path, header.count, size));
            }

            result._seq_floor = header.seq_floor;
            for (size_t done = 0; done < header.count;) {
                const auto batch = std::min<size_t>(ranges_per_batch, header.count - done);
                const auto bytes = co_await in.read_exactly(batch * erased_range_size);
                if (bytes.size() != batch * erased_range_size) {
                    throw std::runtime_error(fmt::format("Corrupt logstor erased range file {}: short read at erased range {}", path, done));
                }
                add_to_crc(crc, bytes.get(), bytes.size());
                auto ranges_in = seastar::simple_memory_input_stream(bytes.get(), bytes.size());
                for (size_t i = 0; i < batch; ++i) {
                    const auto r = ser::serializer<erased_range>::read(ranges_in);
                    by_table[r.table].push_back(range_in_table{.first_token = r.first_token, .last_token = r.last_token, .seq = r.seq});
                    // save() never writes a floor below an erased range, but the counter must not
                    // depend on that.
                    result._seq_floor = std::max(result._seq_floor, r.seq);
                }
                done += batch;
            }

            const auto crc_bytes = co_await in.read_exactly(crc_size);
            auto crc_in = seastar::simple_memory_input_stream(crc_bytes.get(), crc_bytes.size());
            if (const auto crc_on_disk = ser::serializer<uint32_t>::read(crc_in); crc_on_disk != crc.get()) {
                throw std::runtime_error(fmt::format("Corrupt logstor erased range file {}: crc {:#x}, expected {:#x}", path, crc_on_disk, crc.get()));
            }
        }());
        co_await in.close();
        co_await std::move(read);
    });

    for (auto& [table, ranges] : by_table) {
        auto state = build(std::move(ranges));
        result._size += state.ranges.size();
        result._tables.emplace(table, std::move(state));
    }
    co_return result;
}

future<> erased_ranges::save(const std::filesystem::path& base_dir, unsigned shard) const {
    return write_file(base_dir, shard, nullptr, _seq_floor);
}

erased_ranges& erased_ranges::insert(erased_range r) {
    auto state = with_range(r);
    _tables.try_emplace(r.table);
    replace(r.table, std::move(state), r.seq);
    return *this;
}

future<> erased_ranges::insert_and_save(erased_range r, const std::filesystem::path& base_dir, unsigned shard) {
    auto state = with_range(r);
    // The entry of the table exists before the save, so replace() cannot fail after the file holds
    // `r`. A new entry has no steps and covers nothing, so the lookups during the save see the set
    // as it was.
    const bool inserted = _tables.try_emplace(r.table).second;
    const replacement replaced{.table = r.table, .state = state};
    auto saved = co_await coroutine::as_future(write_file(base_dir, shard, &replaced, std::max(_seq_floor, r.seq)));
    if (saved.failed()) {
        if (inserted) {
            _tables.erase(r.table);
        }
        co_await std::move(saved);
    }
    replace(r.table, std::move(state), r.seq);
}

std::vector<erased_range> erased_ranges::all() const {
    std::vector<erased_range> result;
    result.reserve(_size);
    for (const auto& [table, state] : _tables) {
        for (const auto& r : state.ranges) {
            result.push_back(erased_range{.table = table, .first_token = r.first_token, .last_token = r.last_token, .seq = r.seq});
        }
    }
    // The ranges of a table are already in the order of their first token.
    std::ranges::stable_sort(result, {}, &erased_range::table);
    return result;
}

bool erased_ranges::covers(table_id table, dht::token token, segment_sequence seq) const noexcept {
    const auto it = _tables.find(table);
    if (it == _tables.end() || it->second.steps.empty()) {
        return false;
    }
    const auto& steps = it->second.steps;
    // The first step starts at the lowest raw token, so there is always one at or before the token.
    const auto after = std::ranges::upper_bound(steps, token.raw(), std::less<>{}, &step::from);
    return seq <= std::prev(after)->seq;
}

bool erased_ranges::drop_obsolete(std::optional<segment_sequence> min_seq_present, const std::function<bool(table_id)>& table_exists) {
    bool dropped = false;
    for (auto it = _tables.begin(); it != _tables.end();) {
        auto& state = it->second;
        const auto size_before = state.ranges.size();
        if (!table_exists(it->first)) {
            state.ranges.clear();
        } else {
            std::erase_if(state.ranges, [&] (const range_in_table& r) {
                return !min_seq_present || r.seq < *min_seq_present;
            });
        }
        if (state.ranges.size() == size_before) {
            ++it;
            continue;
        }
        dropped = true;
        _size -= size_before;
        if (state.ranges.empty()) {
            it = _tables.erase(it);
            continue;
        }
        state = build(std::move(state.ranges));
        _size += state.ranges.size();
        ++it;
    }
    return dropped;
}

erased_ranges::table_state erased_ranges::build(std::vector<range_in_table> ranges) {
    std::ranges::sort(ranges, {}, [] (const range_in_table& r) { return std::tie(r.first_token, r.last_token, r.seq); });

    // The sweep visits the raw token space in O(k log k) for k erased ranges. The step function can
    // change only where a range starts or just after a range ends, so the sweep visits only those
    // points, in order.
    //
    // When the sweep reaches the first token of a range, the sweep pushes the range on a heap that
    // is ordered by sequence. At each point, the top range of the heap that has not ended gives the
    // value.
    std::vector<int64_t> bounds;
    bounds.reserve(2 * ranges.size() + 1);
    bounds.push_back(min_raw_token);
    for (const auto& r : ranges) {
        bounds.push_back(r.first_token);
        if (r.last_token < max_raw_token) {
            bounds.push_back(r.last_token + 1);
        }
    }
    std::ranges::sort(bounds);
    const auto [duplicates_begin, duplicates_end] = std::ranges::unique(bounds);
    bounds.erase(duplicates_begin, duplicates_end);

    struct covering {
        segment_sequence seq;
        int64_t last_token;
        auto operator<=>(const covering&) const noexcept = default;
    };
    std::priority_queue<covering> started;
    std::vector<step> steps;
    steps.reserve(bounds.size());
    size_t next = 0;
    for (const auto from : bounds) {
        for (; next < ranges.size() && ranges[next].first_token <= from; ++next) {
            started.push(covering{.seq = ranges[next].seq, .last_token = ranges[next].last_token});
        }
        // The sweep pops an ended range only when the range is at the top, because only the top
        // range gives the value.
        while (!started.empty() && started.top().last_token < from) {
            started.pop();
        }
        const auto seq = started.empty() ? segment_sequence{0} : started.top().seq;
        if (steps.empty() || steps.back().seq != seq) {
            steps.push_back(step{.from = from, .seq = seq});
        }
    }

    // The state keeps no spare capacity, because it lives until the next erase of its table.
    table_state state;
    state.steps.assign(steps.begin(), steps.end());
    state.ranges.assign(ranges.begin(), ranges.end());
    return state;
}

erased_ranges::table_state erased_ranges::with_range(const erased_range& r) const {
    std::vector<range_in_table> ranges;
    if (const auto it = _tables.find(r.table); it != _tables.end()) {
        ranges.reserve(it->second.ranges.size() + 1);
        ranges.assign(it->second.ranges.begin(), it->second.ranges.end());
    }
    ranges.push_back(range_in_table{.first_token = r.first_token, .last_token = r.last_token, .seq = r.seq});
    return build(std::move(ranges));
}

void erased_ranges::replace(table_id table, table_state&& state, segment_sequence seq) noexcept {
    auto& current = _tables.find(table)->second;
    _size = _size - current.ranges.size() + state.ranges.size();
    current = std::move(state);
    _seq_floor = std::max(_seq_floor, seq);
}

future<> erased_ranges::write_file(const std::filesystem::path& base_dir, unsigned shard, const replacement* replaced, segment_sequence seq_floor) const {
    utils::get_local_injector().inject("logstor_erased_ranges_save_fail", [] {
        throw std::runtime_error("erased range save failed by injection");
    });
    const auto name = file_name::erase(shard);
    const auto path = name.in(base_dir).string();
    const auto tmp_path = name.tmp().in(base_dir).string();

    // The save writes the tables in the order of their ids, so the bytes of the file do not depend
    // on the order of the hash map.
    std::vector<std::pair<table_id, const table_state*>> tables;
    tables.reserve(_tables.size() + 1);
    size_t count = 0;
    for (const auto& [table, state] : _tables) {
        if (!replaced || table != replaced->table) {
            tables.emplace_back(table, &state);
            count += state.ranges.size();
        }
    }
    if (replaced) {
        tables.emplace_back(replaced->table, &replaced->state);
        count += replaced->state.ranges.size();
    }
    std::ranges::sort(tables, {}, &std::pair<table_id, const table_state*>::first);

    // The save writes the temporary file whole and flushes the file before the rename to the final
    // name, so a crash at any point leaves either the previous file or this one. Nothing reads a
    // leftover temporary file, and the next save truncates it. The output stream closes the file.
    auto out = co_await seastar::make_file_output_stream(
            co_await seastar::open_file_dma(tmp_path, seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate),
            seastar::file_output_stream_options{.buffer_size = stream_buffer_size});
    auto written = co_await coroutine::as_future([&] () -> future<> {
        utils::crc32 crc;
        std::array<char, header_size> header_bytes;
        auto header_out = seastar::simple_memory_output_stream(header_bytes.data(), header_bytes.size());
        ser::serializer<file_header>::write(header_out, file_header{
            .magic = file_magic,
            .version = file_version,
            .reserved = {},
            .count = static_cast<uint32_t>(count),
            .seq_floor = seq_floor,
        });
        add_to_crc(crc, header_bytes.data(), header_bytes.size());
        co_await out.write(header_bytes.data(), header_bytes.size());

        auto batch = seastar::temporary_buffer<char>(ranges_per_batch * erased_range_size);
        size_t batched = 0;
        auto write_batch = [&] {
            const auto size = batched * erased_range_size;
            add_to_crc(crc, batch.get(), size);
            batched = 0;
            return out.write(batch.get(), size);
        };
        for (const auto& [table, state] : tables) {
            for (const auto& r : state->ranges) {
                auto range_out = seastar::simple_memory_output_stream(batch.get_write() + batched * erased_range_size, erased_range_size);
                ser::serializer<erased_range>::write(range_out, erased_range{.table = table, .first_token = r.first_token, .last_token = r.last_token, .seq = r.seq});
                if (++batched == ranges_per_batch) {
                    co_await write_batch();
                }
            }
        }
        co_await write_batch();

        std::array<char, crc_size> crc_bytes;
        auto crc_out = seastar::simple_memory_output_stream(crc_bytes.data(), crc_bytes.size());
        ser::serializer<uint32_t>::write(crc_out, crc.get());
        co_await out.write(crc_bytes.data(), crc_bytes.size());
        co_await out.flush();
    }());
    auto closed = co_await coroutine::as_future(out.close());
    co_await std::move(written);
    co_await std::move(closed);

    co_await seastar::rename_file(tmp_path, path);
    co_await seastar::sync_directory(base_dir.string());
    logstor_logger.debug("Saved {} erased ranges to {}", count, path);
}

} // namespace replica::logstor
