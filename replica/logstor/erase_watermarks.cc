/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/erase_watermarks.hh"

#include <algorithm>
#include <iterator>
#include <limits>
#include <queue>
#include <stdexcept>

#include <fmt/format.h>
#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "dht/token.hh"
#include "idl/uuid.dist.hh"
#include "replica/logstor/compaction.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/segment_file.hh"
#include "serializer.hh"
#include "utils/crc.hh"

namespace replica::logstor {

namespace {

// The file: a header, the watermarks, and a CRC of everything before it.
//
// | Offset | Size | Field       |
// |--------|------|-------------|
// | 0      | 4    | magic       |
// | 4      | 1    | version     |
// | 5      | 3    | reserved    |
// | 8      | 4    | count       |
// | 12     | 8    | seq_floor   |
// | 20     | 40*n | watermarks: table (16), first_token (8), last_token (8), seq (8) |
// |        | 4    | crc         |
//
// All integers little-endian, the table id as its two 64-bit halves like a segment header's.
constexpr uint32_t file_magic = 0x4c475345; // "LGSE"
constexpr uint8_t file_version = 1;
constexpr size_t header_size = 20;
constexpr size_t watermark_size = 2 * sizeof(int64_t) + 2 * sizeof(int64_t) + sizeof(uint64_t);
constexpr size_t crc_size = sizeof(uint32_t);

constexpr int64_t min_raw_token = std::numeric_limits<int64_t>::min();
constexpr int64_t max_raw_token = std::numeric_limits<int64_t>::max();

size_t file_size_for(size_t count) {
    return header_size + count * watermark_size + crc_size;
}

uint32_t crc_of(const char* data, size_t size) {
    utils::crc32 c;
    c.process(reinterpret_cast<const uint8_t*>(data), size);
    return c.get();
}

} // anonymous namespace

std::optional<erase_watermark> erase_watermarks::make(table_id table, const dht::token_range& range, segment_sequence seq) {
    // The same bounds the index applies to a range, see primary_index::position_at_range_start().
    int64_t first = min_raw_token;
    if (range.start()) {
        const auto raw = range.start()->value().raw();
        if (range.start()->is_inclusive()) {
            first = raw;
        } else if (raw == max_raw_token) {
            return std::nullopt;
        } else {
            first = raw + 1;
        }
    }
    int64_t last = max_raw_token;
    if (range.end()) {
        const auto raw = range.end()->value().raw();
        if (range.end()->is_inclusive()) {
            last = raw;
        } else if (raw == min_raw_token) {
            return std::nullopt;
        } else {
            last = raw - 1;
        }
    }
    if (first > last) {
        return std::nullopt;
    }
    return erase_watermark{
        .table = table,
        .first_token = first,
        .last_token = last,
        .seq = seq,
    };
}

std::filesystem::path erase_watermarks::file_path(const std::filesystem::path& base_dir) {
    return file_name::erase().in(base_dir);
}

future<erase_watermarks> erase_watermarks::load(const std::filesystem::path& base_dir) {
    const auto path = file_path(base_dir).string();
    erase_watermarks result;
    if (!co_await seastar::file_exists(path)) {
        co_return result;
    }

    auto buf = co_await seastar::with_file(seastar::open_file_dma(path, seastar::open_flags::ro),
            [&path] (seastar::file& f) -> future<seastar::temporary_buffer<char>> {
        const auto size = co_await f.size();
        if (size < file_size_for(0) || (size - file_size_for(0)) % watermark_size != 0) {
            throw std::runtime_error(fmt::format("Corrupt logstor erase watermark file {}: size {}", path, size));
        }
        auto buf = co_await f.dma_read_exactly<char>(0, size);
        if (buf.size() != size) {
            throw std::runtime_error(fmt::format("Corrupt logstor erase watermark file {}: short read of {} bytes out of {}", path, buf.size(), size));
        }
        co_return buf;
    });
    const auto size = buf.size();

    auto in = seastar::simple_memory_input_stream(buf.get(), buf.size());
    const auto magic = ser::serializer<uint32_t>::read(in);
    const auto version = ser::serializer<uint8_t>::read(in);
    ser::serializer<uint8_t>::skip(in);
    ser::serializer<uint16_t>::skip(in);
    const auto count = ser::serializer<uint32_t>::read(in);
    const auto seq_floor = segment_sequence{ser::serializer<uint64_t>::read(in)};
    if (magic != file_magic || version != file_version || file_size_for(count) != size) {
        throw std::runtime_error(fmt::format("Corrupt logstor erase watermark file {}: magic {:#x} version {} count {} size {}",
                path, magic, version, count, size));
    }
    const auto expected_crc = crc_of(buf.get(), size - crc_size);
    auto crc_in = seastar::simple_memory_input_stream(buf.get() + size - crc_size, crc_size);
    if (const auto crc = ser::serializer<uint32_t>::read(crc_in); crc != expected_crc) {
        throw std::runtime_error(fmt::format("Corrupt logstor erase watermark file {}: crc {:#x}, expected {:#x}", path, crc, expected_crc));
    }

    result._seq_floor = seq_floor;
    result._watermarks.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        erase_watermark w;
        w.table = ser::serializer<table_id>::read(in);
        w.first_token = ser::serializer<int64_t>::read(in);
        w.last_token = ser::serializer<int64_t>::read(in);
        w.seq = segment_sequence{ser::serializer<uint64_t>::read(in)};
        result._watermarks.push_back(w);
        // save() never writes a floor below a watermark, but the counter must not depend on that.
        result._seq_floor = std::max(result._seq_floor, w.seq);
    }
    result.rebuild_steps();
    co_return result;
}

future<> erase_watermarks::save(const std::filesystem::path& base_dir) const {
    const auto path = file_path(base_dir).string();
    const auto tmp_path = file_name::erase().tmp().in(base_dir).string();
    const auto size = file_size_for(_watermarks.size());

    auto buf = seastar::temporary_buffer<char>::aligned(ondisk::block_alignment, seastar::align_up(size, ondisk::block_alignment));
    std::fill(buf.get_write(), buf.get_write() + buf.size(), 0);
    auto out = seastar::simple_memory_output_stream(buf.get_write(), size);
    ser::serializer<uint32_t>::write(out, file_magic);
    ser::serializer<uint8_t>::write(out, file_version);
    ser::serializer<uint8_t>::write(out, uint8_t(0));
    ser::serializer<uint16_t>::write(out, uint16_t(0));
    ser::serializer<uint32_t>::write(out, static_cast<uint32_t>(_watermarks.size()));
    ser::serializer<uint64_t>::write(out, _seq_floor.value);
    for (const auto& w : _watermarks) {
        ser::serializer<table_id>::write(out, w.table);
        ser::serializer<int64_t>::write(out, w.first_token);
        ser::serializer<int64_t>::write(out, w.last_token);
        ser::serializer<uint64_t>::write(out, w.seq.value);
    }
    ser::serializer<uint32_t>::write(out, crc_of(buf.get(), size - crc_size));

    // Written whole, made durable, then published under the final name, so that a crash at any point
    // leaves either the previous file or this one. A leftover temporary file is not read, and the
    // next save truncates it.
    co_await seastar::with_file(seastar::open_file_dma(tmp_path, seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate),
            [&] (seastar::file& f) -> future<> {
        const auto written = co_await f.dma_write(0, buf.get(), buf.size());
        if (written != buf.size()) {
            throw std::runtime_error(fmt::format("Short write of logstor erase watermark file {}: {} bytes out of {}", tmp_path, written, buf.size()));
        }
        co_await f.truncate(size);
        co_await f.flush();
    });
    co_await seastar::rename_file(tmp_path, path);
    co_await seastar::sync_directory(base_dir.string());
    logstor_logger.debug("Saved {} erase watermarks to {}", _watermarks.size(), path);
}

erase_watermarks erase_watermarks::with(erase_watermark w) const {
    erase_watermarks result;
    result._watermarks.reserve(_watermarks.size() + 1);
    result._watermarks.assign(_watermarks.begin(), _watermarks.end());
    result._watermarks.push_back(w);
    result._seq_floor = std::max(_seq_floor, w.seq);
    result.rebuild_steps();
    return result;
}

void erase_watermarks::rebuild_steps() {
    _steps.clear();
    std::unordered_map<table_id, std::vector<const erase_watermark*>> by_table;
    for (const auto& w : _watermarks) {
        by_table[w.table].push_back(&w);
    }
    for (auto& [table, watermarks] : by_table) {
        // A sweep over the raw token space, O(k log k) for k watermarks. The function can only
        // change where a range starts or just past where one ends, so those are the points visited,
        // in order. The ranges are sorted by start and enter a heap keyed by sequence as the sweep
        // reaches them; the top of the heap that has not ended yet is the value at the point.
        std::vector<int64_t> bounds;
        bounds.reserve(2 * watermarks.size() + 1);
        bounds.push_back(min_raw_token);
        for (const auto* w : watermarks) {
            bounds.push_back(w->first_token);
            if (w->last_token < max_raw_token) {
                bounds.push_back(w->last_token + 1);
            }
        }
        std::ranges::sort(bounds);
        const auto [duplicates_begin, duplicates_end] = std::ranges::unique(bounds);
        bounds.erase(duplicates_begin, duplicates_end);
        std::ranges::sort(watermarks, {}, [] (const erase_watermark* w) { return w->first_token; });

        struct covering {
            segment_sequence seq;
            int64_t last_token;
            bool operator<(const covering& o) const noexcept {
                return seq < o.seq;
            }
        };
        std::priority_queue<covering> started;
        auto next = watermarks.begin();
        std::vector<step> steps;
        for (const auto from : bounds) {
            for (; next != watermarks.end() && (*next)->first_token <= from; ++next) {
                started.push(covering{.seq = (*next)->seq, .last_token = (*next)->last_token});
            }
            // A range that has ended is dropped only once it reaches the top, which is the only
            // place it would be read: below the top it decides nothing.
            while (!started.empty() && started.top().last_token < from) {
                started.pop();
            }
            const auto seq = started.empty() ? segment_sequence{0} : started.top().seq;
            if (steps.empty() || steps.back().seq != seq) {
                steps.push_back(step{.from = from, .seq = seq});
            }
        }
        _steps.emplace(table, std::move(steps));
    }
}

bool erase_watermarks::erased(table_id table, dht::token token, segment_sequence seq) const noexcept {
    if (_steps.empty()) {
        return false;
    }
    const auto it = _steps.find(table);
    if (it == _steps.end()) {
        return false;
    }
    const auto& steps = it->second;
    // The first step starts at the lowest raw token, so there is always one at or before the token.
    const auto after = std::ranges::upper_bound(steps, token.raw(), std::less<>{}, &step::from);
    return seq <= std::prev(after)->seq;
}

bool erase_watermarks::gc(std::optional<segment_sequence> min_seq_present, const std::function<bool(table_id)>& table_exists) {
    const auto dropped = std::erase_if(_watermarks, [&] (const erase_watermark& w) {
        return !min_seq_present || w.seq < *min_seq_present || !table_exists(w.table);
    });
    if (dropped) {
        rebuild_steps();
    }
    return dropped != 0;
}

} // namespace replica::logstor
