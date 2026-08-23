/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/record_format.hh"

#include <seastar/core/byteorder.hh>

#include "bytes.hh"
#include "keys/keys.hh"
#include "mutation/atomic_cell.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/mutation_partition.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/fragment_range.hh"
#include "utils/small_vector.hh"
#include "vint-serialization.hh"

namespace replica::logstor {

namespace {

// Deltas are taken in unsigned arithmetic and converted back the same way: a timestamp is
// an arbitrary int64 and the difference of two of them can overflow, while the wrapping
// difference reconstructs the value exactly whatever the two are, and still takes a single
// byte for the small deltas the format is built around.
int64_t delta_of(int64_t value, int64_t base) noexcept {
    return static_cast<int64_t>(static_cast<uint64_t>(value) - static_cast<uint64_t>(base));
}

int64_t value_of_delta(int64_t delta, int64_t base) noexcept {
    return static_cast<int64_t>(static_cast<uint64_t>(base) + static_cast<uint64_t>(delta));
}

// A deletion time is seconds since the epoch while a record timestamp is microseconds
// since the epoch, so a deletion time is written as a delta from the record timestamp's
// second.
int64_t deletion_time_base(api::timestamp_type base_ts) noexcept {
    return base_ts / 1000000;
}

int64_t deletion_time_delta(gc_clock::time_point t, int64_t base) noexcept {
    return delta_of(t.time_since_epoch().count(), base);
}

gc_clock::time_point deletion_time_from_delta(int64_t delta, int64_t base) noexcept {
    return gc_clock::time_point(gc_clock::duration(value_of_delta(delta, base)));
}

// Counts the bytes the encoding takes without touching memory, so that the size of a
// record is known before a buffer is reserved for it. The encoder runs against this and
// against writing_sink, which is what keeps the two in step.
struct measuring_sink {
    size_t size = 0;

    void u8(uint8_t) noexcept { size += sizeof(uint8_t); }
    void u64(uint64_t) noexcept { size += sizeof(uint64_t); }
    void uvint(uint64_t v) noexcept { size += unsigned_vint::serialized_size(v); }
    void svint(int64_t v) noexcept { size += signed_vint::serialized_size(v); }
    void blob(bytes_view v) noexcept { size += v.size(); }
    void blob(managed_bytes_view v) noexcept { size += v.size_bytes(); }
};

class writing_sink {
    seastar::simple_memory_output_stream& _out;

    // The vint codec writes through a raw pointer, so the room for it is checked here.
    char* reserve(size_t size) {
        if (size > _out.size()) {
            throw std::out_of_range("logstor record value buffer overflow");
        }
        return _out.begin();
    }

public:
    explicit writing_sink(seastar::simple_memory_output_stream& out) noexcept : _out(out) { }

    void u8(uint8_t v) {
        *reserve(sizeof(v)) = static_cast<char>(v);
        _out.skip(sizeof(v));
    }
    void u64(uint64_t v) {
        seastar::write_le<uint64_t>(reserve(sizeof(v)), v);
        _out.skip(sizeof(v));
    }
    void uvint(uint64_t v) {
        const auto size = unsigned_vint::serialized_size(v);
        unsigned_vint::serialize(v, reinterpret_cast<bytes::iterator>(reserve(size)));
        _out.skip(size);
    }
    void svint(int64_t v) {
        const auto size = signed_vint::serialized_size(v);
        signed_vint::serialize(v, reinterpret_cast<bytes::iterator>(reserve(size)));
        _out.skip(size);
    }
    void blob(bytes_view v) {
        _out.write(reinterpret_cast<const char*>(v.data()), v.size());
    }
    void blob(managed_bytes_view v) {
        for (bytes_view fragment : fragment_range(v)) {
            blob(fragment);
        }
    }
};

class value_reader {
    bytes_view _v;

    size_t vint_size() const {
        if (_v.empty()) {
            throw std::out_of_range("truncated logstor record value");
        }
        const auto size = unsigned_vint::serialized_size_from_first_byte(_v[0]);
        if (size > _v.size()) {
            throw std::out_of_range("truncated logstor record value");
        }
        return size;
    }

public:
    explicit value_reader(bytes_view v) noexcept : _v(v) { }

    bytes_view read(size_t size) {
        if (size > _v.size()) {
            throw std::out_of_range("truncated logstor record value");
        }
        auto read = _v.substr(0, size);
        _v.remove_prefix(size);
        return read;
    }
    void skip(size_t size) {
        read(size);
    }
    uint8_t u8() {
        return static_cast<uint8_t>(read(sizeof(uint8_t))[0]);
    }
    uint64_t u64() {
        return seastar::read_le<uint64_t>(reinterpret_cast<const char*>(read(sizeof(uint64_t)).data()));
    }
    uint64_t uvint() {
        const auto size = vint_size();
        const auto v = unsigned_vint::deserialize(_v);
        _v.remove_prefix(size);
        return v;
    }
    int64_t svint() {
        const auto size = vint_size();
        const auto v = signed_vint::deserialize(_v);
        _v.remove_prefix(size);
        return v;
    }
    size_t size() const noexcept {
        return _v.size();
    }
};

// The type names of a schema's regular columns, interned: the columns of a logstor table
// tend to share a type and a type name runs 40 characters or more. In phase 2 the schema
// description moves to a per-buffer dictionary and this goes away.
class type_table {
    utils::small_vector<const sstring*, 8> _names;

public:
    explicit type_table(const schema& s) {
        for (const auto& cdef : s.regular_columns()) {
            intern(cdef.type->name());
        }
    }

    size_t size() const noexcept { return _names.size(); }
    const sstring& operator[](size_t i) const noexcept { return *_names[i]; }

    size_t index_of(const sstring& name) const {
        for (size_t i = 0; i < _names.size(); ++i) {
            if (*_names[i] == name) {
                return i;
            }
        }
        throw std::runtime_error(fmt::format("logstor record type {} is not in the record's type table", name));
    }

private:
    void intern(const sstring& name) {
        for (const auto* interned : _names) {
            if (*interned == name) {
                return;
            }
        }
        _names.push_back(&name);
    }
};

marker_kind kind_of(const row_marker& marker) noexcept {
    if (marker.is_missing()) {
        return marker_kind::none;
    }
    if (!marker.is_live()) {
        return marker_kind::dead;
    }
    return marker.is_expiring() ? marker_kind::expiring : marker_kind::live;
}

// The single row a logstor partition holds, or nullptr if it holds none. Throws if the
// partition is not of the shape a logstor table can produce.
const deletable_row* single_row(const mutation_partition& p) {
    if (!p.static_row().empty()) {
        throw std::runtime_error("logstor record cannot hold static columns");
    }
    if (!p.row_tombstones().empty()) {
        throw std::runtime_error("logstor record cannot hold range tombstones");
    }

    const deletable_row* row = nullptr;
    for (const auto& e : p.clustered_rows()) {
        if (e.dummy()) {
            continue;
        }
        if (row) {
            throw std::runtime_error("logstor record cannot hold more than one row");
        }
        row = &e.row();
    }
    return row;
}

template <typename Sink>
void encode_schema_description(Sink& sink, const schema& s, const type_table& types) {
    sink.uvint(types.size());
    for (size_t i = 0; i < types.size(); ++i) {
        const auto& name = types[i];
        sink.uvint(name.size());
        sink.blob(to_bytes_view(name));
    }

    sink.uvint(s.regular_columns_count());
    for (const auto& cdef : s.regular_columns()) {
        sink.uvint(cdef.name().size());
        sink.blob(bytes_view(cdef.name()));
        sink.uvint(types.index_of(cdef.type->name()));
    }
}

template <typename Sink>
void encode_column_bitmap(Sink& sink, const row& cells, column_count_type n_columns) {
    if (n_columns <= 64) {
        uint64_t bitmap = 0;
        cells.for_each_cell([&bitmap] (column_id id, const atomic_cell_or_collection&) {
            bitmap |= uint64_t(1) << id;
        });
        sink.uvint(bitmap);
        return;
    }

    // A wide schema gets a raw bitmap: a varint would be no smaller than one.
    utils::small_vector<uint8_t, 32> bitmap;
    bitmap.resize((n_columns + 7) / 8, 0);
    cells.for_each_cell([&bitmap] (column_id id, const atomic_cell_or_collection&) {
        bitmap[id / 8] |= uint8_t(1) << (id % 8);
    });
    for (auto byte : bitmap) {
        sink.u8(byte);
    }
}

// Which columns a record carries a cell for.
class column_presence {
    uint64_t _small = 0;
    bytes_view _large;
    bool _all = false;

public:
    static column_presence all() noexcept {
        column_presence p;
        p._all = true;
        return p;
    }
    static column_presence read(value_reader& r, column_count_type n_columns) {
        column_presence p;
        if (n_columns <= 64) {
            p._small = r.uvint();
        } else {
            p._large = r.read((n_columns + 7) / 8);
        }
        return p;
    }

    bool contains(column_id id) const noexcept {
        if (_all) {
            return true;
        }
        if (_large.empty()) {
            return _small & (uint64_t(1) << id);
        }
        return static_cast<uint8_t>(_large[id / 8]) & (uint8_t(1) << (id % 8));
    }
};

template <typename Sink>
void encode_cell(Sink& sink, const column_definition& cdef, const atomic_cell_or_collection& cell,
        api::timestamp_type base_ts, int64_t dt_base, const row_marker* expiring_marker) {
    if (cdef.is_counter()) {
        throw std::runtime_error("logstor record cannot hold a counter cell");
    }

    if (cdef.is_multi_cell()) {
        // A collection_mutation is already a compact form, so it is stored verbatim.
        const auto data = cell.as_collection_mutation().data;
        sink.u8(cell_flags::is_collection | cell_flags::has_value_length);
        sink.uvint(data.size_bytes());
        sink.blob(data);
        return;
    }

    const auto ac = cell.as_atomic_cell(cdef);
    const auto ts_delta = delta_of(ac.timestamp(), base_ts);
    const bool live = ac.is_live();

    uint8_t flags = 0;
    if (ts_delta == 0) {
        flags |= cell_flags::use_base_timestamp;
    }
    if (!live) {
        flags |= cell_flags::is_deleted;
    } else if (ac.is_live_and_has_ttl()) {
        flags |= cell_flags::is_expiring;
        if (expiring_marker && ac.ttl() == expiring_marker->ttl() && ac.expiry() == expiring_marker->expiry()) {
            flags |= cell_flags::use_marker_ttl;
        }
    }

    size_t value_size = 0;
    if (live) {
        value_size = ac.value().size_bytes();
        if (value_size == 0) {
            flags |= cell_flags::has_empty_value;
        } else {
            // A cell of a fixed-width type takes its size from the type.
            const auto fixed_size = cdef.type->value_length_if_fixed();
            if (!fixed_size || *fixed_size != value_size) {
                flags |= cell_flags::has_value_length;
            }
        }
    }

    sink.u8(flags);
    if (!(flags & cell_flags::use_base_timestamp)) {
        sink.svint(ts_delta);
    }
    if (flags & cell_flags::is_deleted) {
        sink.svint(deletion_time_delta(ac.deletion_time(), dt_base));
    }
    if ((flags & cell_flags::is_expiring) && !(flags & cell_flags::use_marker_ttl)) {
        sink.uvint(ac.ttl().count());
        sink.svint(deletion_time_delta(ac.expiry(), dt_base));
    }
    if (flags & cell_flags::has_value_length) {
        sink.uvint(value_size);
    }
    if (value_size) {
        sink.blob(ac.value());
    }
}

template <typename Sink>
void encode_row_value(Sink& sink, const schema& s, const mutation_partition& p, api::timestamp_type base_ts) {
    const auto* row = single_row(p);
    const auto partition_tombstone = p.partition_tombstone();
    const auto kind = row ? kind_of(row->marker()) : marker_kind::none;
    const auto n_columns = s.regular_columns_count();

    uint8_t flags = static_cast<uint8_t>(kind) << row_value_flags::marker_kind_shift;
    if (partition_tombstone) {
        flags |= row_value_flags::has_partition_tombstone;
    }
    if (row) {
        flags |= row_value_flags::has_row;
        if (row->deleted_at().regular()) {
            flags |= row_value_flags::has_deleted_at;
        }
        if (row->deleted_at().is_shadowable()) {
            flags |= row_value_flags::has_shadowable_deleted_at;
        }
        if (row->cells().size() != n_columns) {
            flags |= row_value_flags::has_column_bitmap;
        }
    }
    sink.u8(flags);

    const auto version = s.version().uuid();
    sink.u64(static_cast<uint64_t>(version.get_most_significant_bits()));
    sink.u64(static_cast<uint64_t>(version.get_least_significant_bits()));

    const type_table types(s);
    measuring_sink description_size;
    encode_schema_description(description_size, s, types);
    sink.uvint(description_size.size);
    encode_schema_description(sink, s, types);

    const auto dt_base = deletion_time_base(base_ts);
    auto encode_tombstone = [&sink, base_ts, dt_base] (const tombstone& t) {
        sink.svint(delta_of(t.timestamp, base_ts));
        sink.svint(deletion_time_delta(t.deletion_time, dt_base));
    };

    if (flags & row_value_flags::has_partition_tombstone) {
        encode_tombstone(partition_tombstone);
    }
    if (!row) {
        return;
    }

    const auto& marker = row->marker();
    switch (kind) {
    case marker_kind::none:
        break;
    case marker_kind::live:
        sink.svint(delta_of(marker.timestamp(), base_ts));
        break;
    case marker_kind::expiring:
        sink.svint(delta_of(marker.timestamp(), base_ts));
        sink.uvint(marker.ttl().count());
        sink.svint(deletion_time_delta(marker.expiry(), dt_base));
        break;
    case marker_kind::dead:
        sink.svint(delta_of(marker.timestamp(), base_ts));
        sink.svint(deletion_time_delta(marker.deletion_time(), dt_base));
        break;
    }

    if (flags & row_value_flags::has_deleted_at) {
        encode_tombstone(row->deleted_at().regular());
    }
    if (flags & row_value_flags::has_shadowable_deleted_at) {
        encode_tombstone(row->deleted_at().shadowable().tomb());
    }

    if (flags & row_value_flags::has_column_bitmap) {
        encode_column_bitmap(sink, row->cells(), n_columns);
    }
    const auto* expiring_marker = kind == marker_kind::expiring ? &marker : nullptr;
    row->cells().for_each_cell([&sink, &s, base_ts, dt_base, expiring_marker] (column_id id, const atomic_cell_or_collection& cell) {
        encode_cell(sink, s.regular_column_at(id), cell, base_ts, dt_base, expiring_marker);
    });
}

} // anonymous namespace

size_t measure_row_value(const schema& s, const mutation_partition& p, api::timestamp_type base_ts) {
    measuring_sink sink;
    encode_row_value(sink, s, p, base_ts);
    return sink.size;
}

void write_row_value(seastar::simple_memory_output_stream& out, const schema& s, const mutation_partition& p,
        api::timestamp_type base_ts) {
    writing_sink sink(out);
    encode_row_value(sink, s, p, base_ts);
}

void read_row_value_into(mutation_partition& p, const schema& s, bytes_view value, api::timestamp_type base_ts) {
    value_reader r(value);

    const auto flags = r.u8();
    if (flags & row_value_flags::extension) {
        throw std::runtime_error("logstor record value has an unsupported extension");
    }

    const auto version_msb = static_cast<int64_t>(r.u64());
    const auto version_lsb = static_cast<int64_t>(r.u64());
    const auto version = table_schema_version(utils::UUID(version_msb, version_lsb));
    const auto description_size = r.uvint();
    if (version != s.version()) {
        throw std::runtime_error(fmt::format("logstor record was written under schema version {}, read under {}",
                version, s.version()));
    }
    // Same schema version: position i in the encoding is column id i, so the description
    // is skipped without being parsed.
    r.skip(description_size);

    const auto dt_base = deletion_time_base(base_ts);
    auto read_tombstone = [&r, base_ts, dt_base] {
        const auto timestamp = value_of_delta(r.svint(), base_ts);
        const auto deletion_time = deletion_time_from_delta(r.svint(), dt_base);
        return tombstone(timestamp, deletion_time);
    };

    if (flags & row_value_flags::has_partition_tombstone) {
        p.apply(read_tombstone());
    }
    if (!(flags & row_value_flags::has_row)) {
        return;
    }

    auto& dr = p.clustered_row(s, clustering_key::make_empty());

    row_marker marker;
    switch (static_cast<marker_kind>((flags & row_value_flags::marker_kind_mask) >> row_value_flags::marker_kind_shift)) {
    case marker_kind::none:
        break;
    case marker_kind::live:
        marker = row_marker(value_of_delta(r.svint(), base_ts));
        break;
    case marker_kind::expiring: {
        const auto timestamp = value_of_delta(r.svint(), base_ts);
        const auto ttl = gc_clock::duration(static_cast<gc_clock::rep>(r.uvint()));
        const auto expiry = deletion_time_from_delta(r.svint(), dt_base);
        marker = row_marker(timestamp, ttl, expiry);
        break;
    }
    case marker_kind::dead:
        marker = row_marker(read_tombstone());
        break;
    }
    dr.apply(marker);

    tombstone deleted_at;
    if (flags & row_value_flags::has_deleted_at) {
        deleted_at = read_tombstone();
    }
    shadowable_tombstone shadowable_deleted_at(deleted_at);
    if (flags & row_value_flags::has_shadowable_deleted_at) {
        shadowable_deleted_at = shadowable_tombstone(read_tombstone());
    }
    if (deleted_at || shadowable_deleted_at) {
        dr.apply(row_tombstone(deleted_at, shadowable_deleted_at));
    }

    const auto n_columns = s.regular_columns_count();
    const auto presence = flags & row_value_flags::has_column_bitmap
            ? column_presence::read(r, n_columns)
            : column_presence::all();

    auto& cells = dr.cells();
    for (column_id id = 0; id < n_columns; ++id) {
        if (!presence.contains(id)) {
            continue;
        }
        const auto& cdef = s.regular_column_at(id);
        const auto cell_flags_byte = r.u8();

        if (cell_flags_byte & cell_flags::is_collection) {
            if (!cdef.is_multi_cell()) {
                throw std::runtime_error(fmt::format("logstor record holds a collection for the atomic column {}",
                        cdef.name_as_text()));
            }
            const auto data = r.read(r.uvint());
            cells.append_cell(id, collection_mutation(managed_bytes(data)));
            continue;
        }
        if (cdef.is_multi_cell()) {
            throw std::runtime_error(fmt::format("logstor record holds an atomic cell for the collection column {}",
                    cdef.name_as_text()));
        }

        const auto timestamp = cell_flags_byte & cell_flags::use_base_timestamp
                ? base_ts
                : value_of_delta(r.svint(), base_ts);

        if (cell_flags_byte & cell_flags::is_deleted) {
            const auto deletion_time = deletion_time_from_delta(r.svint(), dt_base);
            cells.append_cell(id, atomic_cell::make_dead(timestamp, deletion_time));
            continue;
        }

        std::optional<gc_clock::duration> ttl;
        std::optional<gc_clock::time_point> expiry;
        if (cell_flags_byte & cell_flags::is_expiring) {
            if (cell_flags_byte & cell_flags::use_marker_ttl) {
                if (!marker.is_live() || !marker.is_expiring()) {
                    throw std::runtime_error("logstor record cell takes its ttl from a row marker that has none");
                }
                ttl = marker.ttl();
                expiry = marker.expiry();
            } else {
                ttl = gc_clock::duration(static_cast<gc_clock::rep>(r.uvint()));
                expiry = deletion_time_from_delta(r.svint(), dt_base);
            }
        }

        bytes_view value_bytes;
        if (!(cell_flags_byte & cell_flags::has_empty_value)) {
            if (cell_flags_byte & cell_flags::has_value_length) {
                value_bytes = r.read(r.uvint());
            } else {
                const auto fixed_size = cdef.type->value_length_if_fixed();
                if (!fixed_size) {
                    throw std::runtime_error(fmt::format("logstor record holds a cell of no length for the "
                            "variable-length column {}", cdef.name_as_text()));
                }
                value_bytes = r.read(*fixed_size);
            }
        }

        cells.append_cell(id, expiry
                ? atomic_cell::make_live(*cdef.type, timestamp, value_bytes, *expiry, *ttl)
                : atomic_cell::make_live(*cdef.type, timestamp, value_bytes));
    }

    if (r.size()) {
        throw std::runtime_error(fmt::format("logstor record value has {} trailing bytes", r.size()));
    }
}

} // namespace replica::logstor
