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
#include <stdexcept>
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

    co_await seastar::with_file(seastar::open_file_dma(path, seastar::open_flags::ro), [&] (seastar::file& f) -> future<> {
        // The load checks only that the header fits here, and checks the size after the version.
        const auto size = co_await f.size();
        if (size < header_size) {
            throw std::runtime_error(fmt::format("Corrupt logstor erased range file {}: size {}", path, size));
        }
        auto in = seastar::make_file_input_stream(f);
        auto read = co_await coroutine::as_future(in.read_exactly(size));
        co_await in.close();
        const auto contents = co_await std::move(read);
        if (contents.size() != size) {
            throw std::runtime_error(fmt::format("Corrupt logstor erased range file {}: short read of {} bytes", path, contents.size()));
        }

        auto contents_in = seastar::simple_memory_input_stream(contents.get(), contents.size());
        const auto header = ser::serializer<file_header>::read(contents_in);
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

        // The load checks the CRC before the order of the erased ranges, so that it reports damage to
        // the file as a CRC mismatch.
        utils::crc32 crc;
        add_to_crc(crc, contents.get(), size - crc_size);
        auto crc_in = seastar::simple_memory_input_stream(contents.get() + size - crc_size, crc_size);
        if (const auto crc_on_disk = ser::serializer<uint32_t>::read(crc_in); crc_on_disk != crc.get()) {
            throw std::runtime_error(fmt::format("Corrupt logstor erased range file {}: crc {:#x}, expected {:#x}", path, crc_on_disk, crc.get()));
        }

        result._seq_floor = header.seq_floor;
        // The save writes the tables in the order of their ids, and the erased ranges of a table in
        // the order of their tokens, with no overlap, so the load appends each erased range to the
        // steps of its table.
        table_state* state = nullptr;
        erased_range last;
        for (size_t i = 0; i < header.count; ++i) {
            const auto r = ser::serializer<erased_range>::read(contents_in);
            const bool same_table = state && r.table == last.table;
            if (r.first_token > r.last_token || (state && r.table < last.table) || (same_table && r.first_token <= last.last_token)) {
                throw std::runtime_error(fmt::format("Corrupt logstor erased range file {}: erased range {} of table {} [{}, {}] is out of order",
                        path, i, r.table, r.first_token, r.last_token));
            }
            if (!same_table) {
                state = &result._tables[r.table];
                append_step(*state, min_raw_token, segment_sequence{0});
            }
            append_step(*state, r.first_token, r.seq);
            if (r.last_token < max_raw_token) {
                append_step(*state, r.last_token + 1, segment_sequence{0});
            }
            last = r;
            // save() never writes a floor below an erased range, but the counter must not depend on
            // that.
            result._seq_floor = std::max(result._seq_floor, r.seq);
        }
    });

    for (const auto& [table, state] : result._tables) {
        result._size += state.range_count;
    }
    co_return result;
}

future<> erased_ranges::save(const std::filesystem::path& base_dir, unsigned shard) const {
    return write_file(base_dir, shard, nullptr, _seq_floor);
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
        for (auto s = state.steps.begin(); s != state.steps.end(); ++s) {
            if (s->seq != segment_sequence{0}) {
                const auto next = std::next(s);
                result.push_back(erased_range{.table = table, .first_token = s->from,
                        .last_token = next == state.steps.end() ? max_raw_token : next->from - 1, .seq = s->seq});
            }
        }
    }
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

void erased_ranges::append_step(table_state& state, int64_t from, segment_sequence seq) {
    if (!state.steps.empty() && state.steps.back().from == from) {
        state.range_count -= state.steps.back().seq != segment_sequence{0};
        state.steps.pop_back();
    }
    if (state.steps.empty() || state.steps.back().seq != seq) {
        state.steps.push_back(step{.from = from, .seq = seq});
        state.range_count += seq != segment_sequence{0};
    }
}

erased_ranges::table_state erased_ranges::with_range(const erased_range& r) const {
    // A table with no erased range has one step at sequence 0.
    std::vector<step> no_steps;
    const auto it = _tables.find(r.table);
    const auto& steps = it != _tables.end() && !it->second.steps.empty() ? it->second.steps : no_steps;
    if (steps.empty()) {
        no_steps.push_back(step{.from = min_raw_token, .seq = segment_sequence{0}});
    }

    // Each token of `r` gets the higher of its sequence and the sequence of `r`, and the other tokens
    // keep their sequence. `current` is the sequence of the last step that the loops passed.
    table_state state;
    auto s = steps.begin();
    auto current = segment_sequence{0};
    for (; s != steps.end() && s->from < r.first_token; ++s) {
        current = s->seq;
        append_step(state, s->from, s->seq);
    }
    if (s != steps.end() && s->from == r.first_token) {
        current = s->seq;
        ++s;
    }
    append_step(state, r.first_token, std::max(current, r.seq));
    for (; s != steps.end() && s->from <= r.last_token; ++s) {
        current = s->seq;
        append_step(state, s->from, std::max(s->seq, r.seq));
    }
    if (r.last_token < max_raw_token) {
        // The token after `r` gets back the sequence that it had, unless a step starts there.
        if (s == steps.end() || s->from != r.last_token + 1) {
            append_step(state, r.last_token + 1, current);
        }
        for (; s != steps.end(); ++s) {
            append_step(state, s->from, s->seq);
        }
    }
    return state;
}

void erased_ranges::replace(table_id table, table_state&& state, segment_sequence seq) noexcept {
    auto& current = _tables.find(table)->second;
    _size = _size - current.range_count + state.range_count;
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

    // The map holds an entry of the replaced table, see insert_and_save().
    auto count = _size;
    if (replaced) {
        count = count - _tables.at(replaced->table).range_count + replaced->state.range_count;
    }

    auto contents = seastar::temporary_buffer<char>(file_size_for(count));
    auto contents_out = seastar::simple_memory_output_stream(contents.get_write(), contents.size());
    ser::serializer<file_header>::write(contents_out, file_header{
        .magic = file_magic,
        .version = file_version,
        .reserved = {},
        .count = static_cast<uint32_t>(count),
        .seq_floor = seq_floor,
    });
    for (const auto& [table, current] : _tables) {
        const auto& state = replaced && table == replaced->table ? replaced->state : current;
        for (auto s = state.steps.begin(); s != state.steps.end(); ++s) {
            if (s->seq == segment_sequence{0}) {
                continue;
            }
            const auto next = std::next(s);
            ser::serializer<erased_range>::write(contents_out, erased_range{.table = table, .first_token = s->from,
                    .last_token = next == state.steps.end() ? max_raw_token : next->from - 1, .seq = s->seq});
        }
    }
    utils::crc32 crc;
    add_to_crc(crc, contents.get(), contents.size() - crc_size);
    ser::serializer<uint32_t>::write(contents_out, crc.get());

    // The save writes the temporary file whole and flushes the file before the rename to the final
    // name, so a crash at any point leaves either the previous file or this one. Nothing reads a
    // leftover temporary file, and the next save truncates it. The output stream closes the file.
    auto out = co_await seastar::make_file_output_stream(
            co_await seastar::open_file_dma(tmp_path, seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate));
    auto written = co_await coroutine::as_future(out.write(contents.get(), contents.size()).then([&out] {
        return out.flush();
    }));
    auto closed = co_await coroutine::as_future(out.close());
    co_await std::move(written);
    co_await std::move(closed);

    co_await seastar::rename_file(tmp_path, path);
    co_await seastar::sync_directory(base_dir.string());
    logstor_logger.debug("Saved {} erased ranges to {}", count, path);
}

} // namespace replica::logstor
