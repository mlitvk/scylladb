<!--
Copyright (C) 2026-present ScyllaDB
SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-->

# Logstor Repair Design

## Purpose and status

This document describes how table repair currently works for normal ScyllaDB tables,
why that implementation does not directly support logstor tables, and the design
options for adding repair support to logstor.

The goal of this document is to serve as a working design reference. It should be
updated as implementation decisions change.

At the time of writing, repair is implemented for the normal memtable + SSTable
storage path. Logstor tables use a different read and write path, so they cannot
reuse the current repair implementation without additional work.

Related documents:

- `docs/dev/logstor.md`
- `docs/dev/row_level_repair.md`
- `docs/dev/repair_based_node_ops.md`

## Current repair implementation for non-logstor tables

### High-level flow

Repair is implemented as row-level repair over token ranges. A repair master picks
 a local token range and a set of peer replicas, then synchronizes that range in
 rounds. Each round reads a bounded amount of data from disk, computes row hashes,
 detects mismatches, fetches missing rows from peers, and pushes missing rows to
 peers.

The main entry point for a table/range pair is:

- `repair::shard_repair_task_impl::repair_range()` in `repair/repair.cc`
- `repair_cf_range_row_level()` in `repair/row_level.cc`

The main state machine for the row-level algorithm is implemented in:

- `row_level_repair::run()` in `repair/row_level.cc`

The wire verbs and wire structs used by repair are defined in:

- `idl/repair.idl.hh`

### Step 1: choose a token range and peers

Repair scheduling and range selection happen above the row-level algorithm.
`repair::shard_repair_task_impl::repair_range()` receives a specific local token
range, determines the peer replicas for that range, validates liveness, syncs
schema with the peers, and then invokes row-level repair for the table and range.

Relevant code:

- `repair/repair.cc`: `repair::shard_repair_task_impl::repair_range()`
- `repair/repair.cc`: `repair::shard_repair_task_impl::get_repair_neighbors()`

At this stage repair is already working on a single logical token range. The
row-level repair code does not decide cluster-wide ownership; it consumes the
range selected by the higher layer.

### Step 2: initialize row-level repair for the range

`repair_cf_range_row_level()` constructs a `row_level_repair` object and calls
`run()`. The repair master and followers then create per-range `repair_meta`
objects that keep the state for the current repair session.

Relevant code:

- `repair/row_level.cc`: `repair_cf_range_row_level()`
- `repair/row_level.cc`: `row_level_repair::run()`
- `repair/row_level.cc`: repair RPC start/stop handlers and `repair_meta`

Each `repair_meta` tracks:

- the schema and token range
- the current repair sync boundaries
- the repair reader
- buffered rows and their hashes
- peer row hash sets
- the repair writer used to apply data

### Step 3: read a bounded window of rows from the table

Repair does not process the whole range at once. It advances through the token
range in small windows, controlled by a byte budget.

The read side is implemented by `repair_reader`, which wraps a `mutation_reader`.
The reader strategies are:

- `local`
- `multishard_split`
- `multishard_filter`
- `incremental_repair`

Relevant code:

- `repair/reader.hh`
- `repair/row_level.cc`: `repair_reader::make_reader()`
- `repair/row_level.cc`: `repair_reader::repair_reader()`
- `repair/row_level.cc`: `repair_meta::read_rows_from_disk()`
- `repair/row_level.cc`: `repair_meta::get_sync_boundary()`

For normal tables, these strategies ultimately read through
`replica::table::make_streaming_reader(...)`.

Relevant code:

- `replica/table.cc`: `table::make_streaming_reader(schema_ptr, reader_permit, const dht::partition_range&, ...)`
- `replica/table.cc`: `table::make_streaming_reader(schema_ptr, reader_permit, const dht::partition_range_vector&, ...)`
- `replica/table.cc`: `table::make_streaming_reader(schema_ptr, reader_permit, const dht::partition_range&, lw_shared_ptr<sstables::sstable_set>, ...)`

For non-logstor tables, `make_streaming_reader()` reads from memtables and
SSTables, and can wrap the read in compaction logic for streaming and repair.

Important properties of the current repair reader path:

- it returns mutation fragments in the standard mutation-reader format
- it supports reading a live table range or an SSTable snapshot set
- it matches the rest of the LSM storage stack, including incremental repair

### Step 4: compute row hashes and combined digests

Rows read from the mutation reader are converted to `repair_row` objects.

Relevant code:

- `repair/row_level.cc`: `handle_mutation_fragment()`
- `repair/row.hh`

Repair hashes are computed per row. The per-row hash is based on:

- a hash of the partition key, stored in `decorated_key_with_hash`
- the mutation fragment contents

Relevant code:

- `repair/decorated_key_with_hash.hh`
- `repair/hash.hh`
- `repair/row_level.cc`: `repair_hasher::do_hash_for_mf()`

The combined digest for a working buffer is an XOR of the row hashes. The current
repair window is described by a `repair_sync_boundary`, which contains:

- decorated partition key
- position in partition

Relevant code:

- `repair/sync_boundary.hh`
- `repair/row_level.cc`: `repair_meta::get_sync_boundary()`
- `repair/row_level.cc`: `repair_meta::request_row_hashes()`

The algorithm proceeds roughly as follows:

1. Each replica reads rows until its local byte budget is reached.
2. Each replica reports the boundary of the last row it read and the combined
   hash of what it buffered.
3. The master chooses a common boundary.
4. If combined hashes match, the current window is already synchronized.
5. Otherwise the master asks for full row hashes and then the actual missing rows.

This is described conceptually in `docs/dev/row_level_repair.md`, and the actual
implementation lives in `repair/row_level.cc`.

### Step 5: move row data between replicas

Repair uses dedicated repair RPC verbs. The important ones are:

- `repair_get_sync_boundary`
- `repair_get_combined_row_hash`
- `repair_get_full_row_hashes`
- `repair_get_row_diff`
- `repair_put_row_diff`
- `repair_row_level_start`
- `repair_row_level_stop`

Relevant code:

- `idl/repair.idl.hh`
- `repair/repair.hh`
- `message/messaging_service.hh`
- `message/messaging_service.cc`

The wire format for repair data is row-oriented. The main container is
`repair_rows_on_wire`, which is a list of partition-key plus
frozen-mutation-fragment groups.

Relevant code:

- `repair/repair.hh`: `partition_key_and_mutation_fragments`
- `idl/repair.idl.hh`: `partition_key_and_mutation_fragments`
- `repair/row_level.cc`: `to_repair_rows_on_wire()`
- `repair/row_level.cc`: `to_repair_rows_list()`

The protocol has two variants:

- ordinary request/response RPCs
- RPC stream variants for larger hash and row transfers

Relevant code:

- `repair/row_level.cc`: `get_full_row_hashes_with_rpc_stream(...)`
- `repair/row_level.cc`: `repair_get_row_diff_with_rpc_stream_process_op_slow_path(...)`
- `repair/row_level.cc`: `repair_put_row_diff_with_rpc_stream_process_op(...)`

The important point is that repair moves logical row data, not SSTables or raw
storage files.

### Step 6: apply incoming repair data

When a follower receives repair rows, it applies them through the repair writer.

Relevant code:

- `repair/row_level.cc`: `put_row_diff_handler()`
- `repair/row_level.cc`: `apply_rows_on_follower()`
- `repair/row_level.cc`: `do_apply_rows()`
- `repair/writer.hh`
- `repair/row_level.cc`: `repair_writer_impl`

Incoming wire rows are first converted back into `repair_row`s by
`to_repair_rows_list()`. On followers, rows with the same partition and position
may be merged before writing, because the SSTable writer expects the stream in the
normal mutation-fragment form.

The actual apply path is SSTable-based:

1. `repair_writer` creates a mutation-fragment queue.
2. `repair_writer_impl::create_writer()` feeds that queue into
   `mutation_writer::distribute_reader_and_consume_on_shards(...)`.
3. The consumer is `streaming::make_streaming_consumer(...)`.
4. The streaming consumer writes streaming SSTables and adds them to the table.

Relevant code:

- `repair/row_level.cc`: `repair_writer_impl::create_writer()`
- `streaming/consumer.cc`: `make_streaming_consumer(...)`
- `replica/table.cc`: `table::make_streaming_sstable_for_write()`
- `replica/table.cc`: `table::make_streaming_staging_sstable()`

So although repair moves logical rows over the network, the current apply path is
still fundamentally tied to the SSTable storage engine.

### Step 7: incremental repair specifics

Incremental repair is implemented in terms of SSTable snapshots and repaired
metadata.

Note that user-triggered repair currently rejects the `incremental=true` option in
`repair/repair.cc`. The internal incremental-repair machinery described here still
exists in the row-level repair path and is relevant when considering how logstor
would support an eventual incremental-repair design.

Relevant code:

- `repair/incremental.hh`
- `repair/incremental.cc`
- `repair/row_level.cc`: `prepare_sstables_for_incremental_repair()`
- `repair/row_level.cc`: `mark_sstable_as_repaired()`

The current logic:

- takes a storage snapshot as a set of SSTables
- filters or marks SSTables based on repaired state
- reads from a snapshot SSTable set for the repair session
- marks resulting SSTables as repaired afterward

This is tightly coupled to the normal LSM storage layout.

## What does not work for logstor today

### The current repair reader path is not logstor-aware

Normal query reads for logstor tables already have a separate path:

- `replica::table::make_mutation_reader(...)` checks `_logstor`
- if the table uses logstor, it calls `_logstor->make_reader(...)`

Relevant code:

- `replica/table.cc`: `table::make_mutation_reader(...)`
- `replica/logstor/logstor.hh`
- `replica/logstor/logstor.cc`: `logstor::make_reader(...)`

However, repair does not use `make_mutation_reader(...)`. It uses
`make_streaming_reader(...)`, and `make_streaming_reader(...)` currently has only
the normal memtable + SSTable implementation.

Relevant code:

- `repair/row_level.cc`: `repair_reader::make_reader()`
- `replica/table.cc`: `table::make_streaming_reader(...)`

This means the current repair read path bypasses the existing logstor-aware range
reader.

### The current repair apply path is not logstor-aware

Normal table writes during repair produce streaming SSTables. Logstor writes do
not go through the memtable/SSTable write path.

Relevant code:

- `repair/row_level.cc`: `repair_writer_impl::create_writer()`
- `streaming/consumer.cc`: `make_streaming_consumer(...)`
- `replica/table.cc`: `table::apply(const mutation&, ...)`
- `replica/table.cc`: `table::apply(const frozen_mutation&, ...)`

For logstor tables, `table::apply(...)` calls `_logstor->write(...)` directly.
That means a repair apply path that only knows how to build SSTables cannot work
for logstor.

Relevant code:

- `replica/table.cc`: `if (_logstor) return _logstor->write(...)`

### Incremental repair is SSTable-based

Incremental repair tracks SSTable repaired state and reads from SSTable snapshots.
Logstor does not have an equivalent repaired/unrepaired segment model at this
layer today.

Relevant code:

- `repair/incremental.hh`
- `repair/row_level.cc`: `prepare_sstables_for_incremental_repair()`
- `repair/row_level.cc`: `mark_sstable_as_repaired()`

### Current repair assumptions are LSM-specific in several places

The current implementation assumes that the underlying storage can provide:

- a streaming reader compatible with the mutation-reader contract
- a repair writer that materializes SSTables
- repaired-state bookkeeping on SSTables

Logstor currently provides a different set of primitives:

- token-range scans over the primary index
- direct log-record reads from segment locations
- direct writes to log segments
- snapshots and network streaming of raw logstor segments

Relevant code:

- `replica/logstor/logstor.cc`
- `replica/logstor/index.hh`
- `replica/logstor/segment_manager.cc`
- `replica/table.cc`: `take_logstor_snapshot(...)`
- `streaming/stream_blob.cc`: `stream_logstor_segments`

These primitives do not line up with the current repair interfaces out of the box.

## Logstor storage and read/write model relevant to repair

### Logstor table model

Logstor is currently for partition-key-only tables, with no clustering columns.
Each log record stores a full partition value as a `canonical_mutation`.

Relevant documentation:

- `docs/dev/logstor.md`

Important consequences for repair:

- the partition key space is still token ordered, so range repair still makes
  sense
- there are no user-defined clustering columns in the current logstor table model
- live data is still represented internally as one `clustering_row` with
  `clustering_key::make_empty()`
- the storage format stores full partition values rather than SSTable-style
  partition streams

### Logstor read path

For singular-key reads, logstor performs an index lookup and reads the referenced
record from disk.

For range reads, logstor:

1. converts the partition range to a token range
2. scans the in-memory primary index by token
3. batches log locations
4. reads the corresponding log records from disk
5. sorts same-token runs by decorated-key ring order
6. filters to the exact partition range
7. exposes the results through a mutation reader

Relevant code:

- `replica/logstor/logstor.cc`: `logstor_range_reader`
- `replica/logstor/index.hh`: token-range scan support

This is the closest existing primitive to what repair needs on the read side.

### Logstor write path

Logstor writes do not go through memtables. They are appended to the log and the
primary index is updated to point to the latest record.

Relevant code:

- `replica/table.cc`: logstor branch in `table::apply(...)`
- `replica/logstor/logstor.hh`
- `replica/logstor/logstor.cc`

This is the closest existing primitive to what repair needs on the apply side.

### Mutation format and mutation fragments

Repair is written in terms of logical mutation fragments, while logstor persists a
full partition value per key. To design logstor repair correctly we need to be
explicit about both representations and how they map to each other.

#### Canonical mutation stored by logstor

Each logstor record stores a `canonical_mutation` inside the log record payload.
`canonical_mutation` is the schema-version-tolerant serialized form of a logical
Scylla mutation.

Relevant code:

- `idl/mutation.idl.hh`: `canonical_mutation`
- `mutation/canonical_mutation.cc`: `canonical_mutation::canonical_mutation(const mutation&)`
- `mutation/canonical_mutation.cc`: `canonical_mutation::to_mutation(schema_ptr)`

The serialized `canonical_mutation` contains:

- table id
- schema version
- partition key
- column mapping
- serialized `mutation_partition`

Relevant code:

- `idl/mutation.idl.hh`: `canonical_mutation`
- `idl/mutation.idl.hh`: `mutation_partition`

`canonical_mutation::to_mutation()` reconstructs a normal in-memory `mutation`. If
the stored schema version matches the current schema version, the serialized
partition view is applied directly. Otherwise a schema-converting applier is used.

This is important for repair because it means logstor does not expose a special
repair-only format. The read path reconstructs a normal Scylla `mutation`, and the
repair code then sees the standard mutation-fragment stream derived from that
mutation.

#### Shape of `mutation` and `mutation_partition`

The generic in-memory `mutation_partition` contains:

- partition tombstone
- static row
- range tombstones
- clustered rows

Relevant code:

- `idl/mutation.idl.hh`: `mutation_partition`

Each serialized clustered row (`deletable_row`) contains:

- clustering key
- row marker
- regular row tombstone
- row cells
- shadowable row tombstone

Relevant code:

- `idl/mutation.idl.hh`: `deletable_row`

The serializer writes only non-dummy rows.

Relevant code:

- `mutation/mutation_partition_serializer.cc`: `mutation_partition_serializer::write_serialized(...)`

When the serialized partition is read back, rows are exposed to the partition view
visitor as normal non-dummy rows.

Relevant code:

- `mutation/mutation_partition_view.cc`: `mutation_partition_view::do_accept(...)`

This matters because the persisted logstor mutation is already a clean logical
partition representation. Repair does not need to understand internal cache-only or
dummy-row artifacts.

#### What a live logstor mutation looks like

Although logstor tables have no user-defined clustering columns, a live logstor row
is still represented internally using the generic clustered-row model.

For current logstor tables, a normal live partition is effectively:

- one partition key
- one clustered row at `clustering_key::make_empty()`
- one row marker carrying the row timestamp/liveness
- regular cells carrying the column values

Relevant code:

- `test/boost/logstor_test.cc`: `make_kv_mutation(...)`
- `mutation/mutation_partition.hh`: `row_marker`

The logstor write path derives the persisted log-record timestamp from the first
non-dummy clustered row marker. If no such row marker exists, it falls back to the
partition tombstone timestamp. If neither exists, the write is rejected.

Relevant code:

- `replica/logstor/logstor.cc`: `extract_logstor_record_timestamp(...)`

This strongly constrains the supported logical mutation shape for persisted logstor
records:

- live data must have a row marker on the logical row
- deletes are represented by a partition tombstone

#### What a deleted logstor mutation looks like

For the current logstor table model, deleting the single logical row of the
partition is expected to be represented as a partition tombstone. In the logstor
write path this is the delete representation that is directly supported for record
timestamp extraction.

Relevant code:

- `replica/logstor/logstor.cc`: `extract_logstor_record_timestamp(...)`

This is the delete shape that logstor repair should assume and preserve.

#### What mutation fragments are

Repair operates on mutation fragments produced by a mutation reader. The generic
fragment stream shape is:

1. `partition_start`
2. optional `static_row`
3. zero or more clustered fragments:
   - `clustering_row`
   - `range_tombstone_change`
4. `partition_end`

Relevant code:

- `readers/mutation_reader.hh`

Fragments are ordered by:

- ring position of the partition key
- then `position_in_partition` inside the partition

Relevant code:

- `readers/mutation_reader.hh`

Fragments of the same kind and position may be merged with `mutation_fragment::apply()`.
`range_tombstone` fragments are not mergeable by this helper.

Relevant code:

- `mutation/mutation_fragment.hh`: `mergeable_with(...)`

#### What fragments logstor reads produce

Logstor does not invent a custom fragment layout. It reads a `canonical_mutation`,
converts it to a normal `mutation`, and then uses `make_mutation_reader_from_mutations(...)`
to expose the standard mutation-fragment stream.

Relevant code:

- `replica/logstor/logstor.cc`: `logstor::read(...)`
- `replica/logstor/logstor.cc`: `logstor::make_reader(...)`
- `readers/mutation_readers.cc`: reader-from-mutation adapter

For a live logstor partition, the expected fragment stream is:

1. `partition_start` with no partition tombstone
2. no `static_row`
3. one `clustering_row` at `clustering_key::make_empty()` containing:
   - the row marker
   - the row cells
4. `partition_end`

For a deleted logstor partition, the expected fragment stream is:

1. `partition_start` carrying the partition tombstone
2. no `static_row`
3. no `clustering_row`
4. `partition_end`

For current logstor tables we do not expect user-data `range_tombstone_change`
fragments, because there are no user clustering ranges and updates replace the full
logical row/partition.

#### Implications for logstor repair

The important conclusion is that logstor repair does not need a new fragment
protocol. It needs to preserve the normal mutation-fragment semantics while being
aware that:

- the live logical value is a single clustered row at empty clustering key
- deletes are partition tombstones
- the persisted representation is a full `canonical_mutation`

This also gives us a likely implementation tool for the apply side: when repair
receives fragments, it can reconstruct a full logical `mutation` per partition
using the existing mutation rebuild helpers and then write that mutation through
the logstor write path.

Relevant code:

- `mutation/mutation_rebuilder.hh`

### Logstor file snapshot and file streaming path

Separate from repair, logstor already supports snapshotting and streaming raw
segments for tablet file streaming.

Relevant code:

- `replica/table.cc`: `take_logstor_snapshot(...)`
- `replica/table.cc`: `create_logstor_segment_sink(...)`
- `replica/logstor/segment_manager.cc`: `create_segment_output_stream(...)`
- `replica/logstor/segment_manager.cc`: `load_segment(...)`
- `streaming/stream_blob.hh`: `file_ops::stream_logstor_segments`
- `streaming/stream_blob.cc`

This is important because it shows that logstor already has a storage-native file
movement path, even though repair itself is currently row-oriented.

## Design issues and solution space

### Row reading and hash calculation

#### Problem statement

Repair needs a mutation-reader-compatible stream over a token range so that it can:

- stop after a bounded amount of data
- calculate per-row hashes and combined digests
- advance via sync boundaries
- reuse the existing repair algorithm with minimal changes

Today the repair reader uses `table::make_streaming_reader(...)`, but that path is
not logstor-aware.

#### Constraints

- The repair algorithm is already implemented in terms of mutation fragments and
  sync boundaries.
- We want to preserve the existing row-level repair algorithm if possible.
- Logstor range reads already know how to scan the primary index and produce
  mutations in ring order.
- Repair may need multi-shard reading and filtering behavior similar to the
  existing LSM path.

#### Possible solution A: make `table::make_streaming_reader(...)` logstor-aware

Add a logstor branch to `table::make_streaming_reader(...)` so that repair can keep
using the same higher-level interface.

Advantages:

- smallest change to repair call sites
- preserves the existing `repair_reader` design
- keeps repair logic generic at the `replica::table` interface level

Disadvantages:

- `make_streaming_reader(...)` currently has LSM-oriented semantics, including
  compaction wrapping and SSTable-set overloads
- the name suggests streaming/repair over the normal storage stack, so overloading
  it for logstor may blur responsibilities

Likely shape:

- for live repair reads, a logstor table would delegate to `_logstor->make_reader(...)`
- for the SSTable-set overload used by incremental repair, either reject logstor or
  add a separate logstor-specific snapshot reader later

#### Possible solution B: add a dedicated repair-reader entry point for tables

Introduce a new `table` API specifically for repair, such as a repair reader or
streaming-for-repair reader, and have it dispatch to the correct backend.

Advantages:

- makes the backend split explicit
- avoids overloading LSM-specific APIs with logstor behavior
- gives room for logstor-specific semantics later

Disadvantages:

- broader interface change
- duplicates some of the current `make_streaming_reader(...)` role

#### Hash calculation considerations

If the repair read side uses the existing mutation-reader contract, the current
row-level hashing code can likely remain unchanged. The repair code hashes
mutation fragments after they are materialized, not raw SSTable rows.

Relevant code:

- `repair/decorated_key_with_hash.hh`
- `repair/hash.hh`
- `repair/row_level.cc`: `repair_hasher::do_hash_for_mf()`

This strongly suggests that the preferred path is to make logstor expose a repair
reader that produces the same logical mutation-fragment stream as normal tables.

#### Additional questions to resolve

- Is the mutation stream produced by `logstor::make_reader(...)` fully compatible
  with all repair assumptions?
- Do deletes and tombstones appear in a form that hashes and compares correctly?
- Does logstor need any flush or separator barrier before a repair scan starts?
- Can the current multishard reader logic be reused as-is for logstor tables?

#### Adapting the repair-reader API to use the logstor range reader

The baseline logstor repair design should start by adapting the existing repair
reader API so that repair reads for logstor tables are served by the current
logstor range reader.

Relevant code:

- `repair/reader.hh`: `repair_reader`
- `repair/row_level.cc`: `repair_reader::make_reader(...)`
- `replica/table.cc`: `table::make_mutation_reader(...)`
- `replica/table.cc`: `table::make_streaming_reader(...)`
- `replica/database.hh`: `make_multishard_streaming_reader(...)`
- `replica/database.cc`: `streaming_reader_lifecycle_policy::create_reader(...)`

Today the API split is:

- normal query path uses `table::make_mutation_reader(...)`
- logstor support already exists in `make_mutation_reader(...)`
- repair local and multishard reads use `table::make_streaming_reader(...)`
- `make_multishard_streaming_reader(...)` also depends on
  `table::make_streaming_reader(...)` for each shard reader

This means that routing repair through the existing logstor range reader is mostly
an API-dispatch problem. The logstor-compatible reader already exists, but repair
does not call through the API that reaches it.

##### Current read call chain for repair

For local reads:

1. `repair_reader::make_reader(...)` chooses `read_strategy::local`
2. it creates a `mutation_source`
3. that source calls `cf.make_streaming_reader(...)`

Relevant code:

- `repair/row_level.cc`: `repair_reader::make_reader(...)`

For multishard reads:

1. `repair_reader::make_reader(...)` chooses `multishard_split` or
   `multishard_filter`
2. it calls `make_multishard_streaming_reader(...)`
3. `make_multishard_streaming_reader(...)` creates per-shard readers through
   `streaming_reader_lifecycle_policy::create_reader(...)`
4. `streaming_reader_lifecycle_policy::create_reader(...)` calls
   `cf.make_streaming_reader(...)`

Relevant code:

- `repair/row_level.cc`: `repair_reader::make_reader(...)`
- `replica/database.hh`: `streaming_reader_lifecycle_policy`
- `replica/database.cc`: `make_multishard_streaming_reader(...)`

So any backend-aware solution for repair reads must account for both:

- direct local repair readers
- shard-local readers created inside the multishard reader

##### Option A: make `table::make_streaming_reader(...)` backend-aware for live reads

This is the least disruptive baseline option.

The idea is:

- if the table uses logstor, the live overloads of
  `table::make_streaming_reader(...)` delegate to `_logstor->make_reader(...)`
- keep the existing `make_streaming_reader(...)` call sites in repair unchanged
- keep the multishard reader infrastructure unchanged, since it already obtains its
  shard-local readers through `make_streaming_reader(...)`

Concretely, this applies to the live-read overloads:

- `table::make_streaming_reader(schema_ptr, reader_permit, const dht::partition_range&, const query::partition_slice&, mutation_reader::forwarding, gc_clock::time_point)`
- `table::make_streaming_reader(schema_ptr, reader_permit, const dht::partition_range_vector&, gc_clock::time_point)`

For logstor tables, the likely behavior is:

- single-range live read: call `_logstor->make_reader(...)`
- multi-range live read: build a multi-range reader over a mutation source that in
  turn calls `_logstor->make_reader(...)` for each range

Advantages:

- smallest change to the repair code
- preserves `repair_reader` structure and `read_strategy` logic
- preserves the existing multishard reader stack without adding a second
  logstor-specific multishard implementation
- keeps baseline logstor repair as close as possible to the non-logstor path

Disadvantages:

- `make_streaming_reader(...)` remains a somewhat misleading name for logstor,
  since for logstor it would no longer imply memtable + SSTable streaming
- the overload used for SSTable-set reads is still inherently LSM-specific

##### Option B: add a dedicated backend-aware repair read API on `table`

Another option is to introduce a separate table-level API specifically for repair
reads, for example a repair reader or streaming-for-repair reader. The repair code
and multishard reader lifecycle policy would call that new API instead of
`make_streaming_reader(...)`.

Advantages:

- makes the backend split explicit
- avoids broadening the meaning of `make_streaming_reader(...)`
- gives future logstor-specific repair behavior a clearer home

Disadvantages:

- requires more call-site changes in both repair and multishard reader code
- duplicates much of the role already played by `make_streaming_reader(...)`
- adds another reader API surface that must stay aligned with the existing one

##### Recommended baseline approach

For the initial implementation, Option A is preferred.

Reasons:

- it automatically fixes both local repair reads and multishard repair reads,
  because both already depend on `table::make_streaming_reader(...)`
- it keeps the change focused on backend dispatch instead of introducing a wider
  API redesign
- it reduces the risk of diverging repair-reader behavior between normal tables
  and logstor tables

The recommendation is therefore:

1. keep `repair_reader::read_strategy` unchanged for the initial implementation
2. make the live-read `table::make_streaming_reader(...)` overloads dispatch to
   logstor when `uses_logstor()` is true
3. leave the snapshot/SSTable-set `make_streaming_reader(...)` overload LSM-only
4. explicitly reject incremental repair for logstor rather than attempting to
   force the SSTable-set overload to support it

##### Expected behavior by repair read strategy

For `read_strategy::local`:

- repair continues to call `cf.make_streaming_reader(...)`
- for normal tables this keeps the current memtable + SSTable path
- for logstor tables this would route to `_logstor->make_reader(...)`

For `read_strategy::multishard_split` and `read_strategy::multishard_filter`:

- repair continues to call `make_multishard_streaming_reader(...)`
- the multishard machinery continues to create shard readers through
  `streaming_reader_lifecycle_policy::create_reader(...)`
- because that method already calls `cf.make_streaming_reader(...)`, the logstor
  dispatch automatically applies on each shard

For `read_strategy::incremental_repair`:

- keep the current SSTable-set-based implementation for normal tables
- reject or disable this strategy for logstor tables in the initial version

##### Behavioral checks needed before implementation is considered complete

Even if the API adaptation is small, the baseline design still needs validation of
the resulting reader behavior.

The following properties should be explicitly checked:

- same-token partitions are emitted in decorated-key ring order
- the mutation-fragment stream shape matches what repair expects for both live rows
  and partition tombstones
- `next_partition()` and `fast_forward_to(...)` remain correct when the reader is
  used through the multishard combining reader
- logstor read barriers and phasing are sufficient for repair scans, or any
  required extra barrier/flush step is identified and documented

##### Expected code changes for the baseline reader-first implementation

At a high level, the smallest implementation path is expected to touch:

- `replica/table.cc`
  - add a logstor branch in the live `make_streaming_reader(...)` overloads
- possibly `replica/database.hh` / `replica/database.cc`
  - only if the current comments or lifecycle semantics need adjustment to account
    for logstor-backed streaming readers
- `repair/row_level.cc`
  - likely no structural change for the first step, beyond any explicit rejection
    of incremental repair on logstor

This is the main reason the design prefers starting from the logstor range reader:
the existing repair-reader API is already close enough that the baseline adaptation
can be made mostly as a backend-dispatch change.

### Data movement and wire format

#### Problem statement

Repair currently moves logical rows and hashes, not storage files. Logstor already
has a storage-file streaming path for raw segments. We need to decide whether
logstor repair should:

- reuse the existing row-level wire format, or
- introduce a logstor-specific file/segment movement repair path

#### Existing repair wire format

Current repair sends:

- combined hashes for a sync window
- full row hash sets when needed
- row diffs as partition key + frozen mutation fragments

Relevant code:

- `idl/repair.idl.hh`
- `repair/repair.hh`
- `repair/row_level.cc`: `to_repair_rows_on_wire()`

#### Possible solution A: keep the existing row-level repair wire format

This means logstor participates in repair just like normal tables:

- read logical rows from logstor
- compute row hashes the same way
- send and receive `repair_rows_on_wire`

Advantages:

- minimal protocol change
- full compatibility with current repair master/follower algorithm
- keeps repair semantics backend-independent at the wire level
- naturally supports "only send the delta"

Disadvantages:

- may not exploit any storage-specific efficiencies of logstor segment layout
- requires a separate logstor-aware apply path on the receiver

#### Possible solution B: design a segment-oriented repair path

Repair could potentially stream raw logstor segments or segment subsets instead of
logical rows.

Advantages:

- could reuse existing segment streaming/import machinery
- may be efficient for some full-copy scenarios

Disadvantages:

- does not fit the current row-level mismatch detection protocol
- difficult to reconcile "latest value wins" semantics at row granularity
- hard to send only the exact delta when only a few keys differ
- likely requires a second, logstor-specific repair protocol instead of extending
  the existing one

#### Recommendation

The initial logstor repair implementation should keep the existing row-level wire
format. The segment-streaming path is useful evidence that logstor has a storage
native import/export capability, but it is a better fit for tablet file streaming
and bulk-copy operations than for row-level repair.

#### Possible future optimization: index-stored repair hashes

One possible later optimization for logstor repair is to avoid reading log records
from disk for the initial hash-comparison phases. The idea is:

1. pre-calculate a stable per-key repair fingerprint when writing a logstor record
2. store that fingerprint in the in-memory primary index entry
3. satisfy repair hash-range scans directly from the index instead of doing random
   reads through the logstor range reader

For current logstor tables this idea is attractive because each key effectively
contributes a single logical repair item:

- a live partition is represented as one logical row at
  `clustering_key::make_empty()`
- a deleted partition is represented as a partition tombstone

That means a per-key repair fingerprint is conceptually natural for the current
logstor table model.

Potential advantages:

- `get_combined_row_hash` could avoid random disk reads for already-synchronized
  ranges
- `get_full_row_hashes` could be served from the index very efficiently
- in the common case where ranges are already synchronized, repair may avoid most
  of the expensive log-record reads
- range scans over the index are already a core logstor primitive and are much
  cheaper than deserializing the corresponding records from disk

However, this idea does not fit cleanly into the current repair design as a
drop-in replacement for the baseline logstor repair reader.

##### Why this is not the initial design

The first logstor repair implementation should still use the normal logstor range
reader and expose the standard mutation-fragment stream. This keeps the repair
algorithm as close as possible to the existing non-logstor implementation.

The index-hash idea is intentionally deferred because it introduces several extra
design problems at once.

##### Problem 1: current repair hashes are not precomputable

The current repair hash is not a stable storage fingerprint. It is computed from:

- a per-repair-session random seed
- the mutation fragment contents
- a seeded partition-key hash

Relevant code:

- `repair/row_level.cc`: `get_random_seed()`
- `repair/decorated_key_with_hash.hh`
- `repair/hash.hh`
- `repair/row_level.cc`: `repair_hasher::do_hash_for_mf(...)`

Because the current repair hash depends on a random session seed, it cannot simply
be precomputed once at write time and reused forever.

To make the index-hash optimization work, logstor repair would need a second hash
definition: a stable stored fingerprint suitable for index storage. That is a
protocol and design change, not just an implementation optimization.

##### Problem 2: current repair ordering and sync boundaries are key-based, not index-key-based

The current repair algorithm negotiates sync windows using `repair_sync_boundary`,
which contains:

- a full decorated key
- a position in partition

Relevant code:

- `idl/repair.idl.hh`: `repair_sync_boundary`
- `repair/row_level.cc`: `get_common_sync_boundary(...)`

The logstor primary index does not store the full partition key. It stores a
compact `primary_index_key` derived from token and key hash, plus the record
location and timestamp.

Relevant code:

- `replica/logstor/types.hh`: `primary_index_key`
- `replica/logstor/types.hh`: `index_entry`
- `replica/logstor/index.hh`

This causes two mismatches:

- the current repair boundary type cannot be produced from index-only data
- same-token ordering in the index is by key hash, while the generic repair read
  order is by full decorated key

The existing logstor range reader solves this by reading records and re-sorting
same-token runs by decorated key.

Relevant code:

- `replica/logstor/logstor.cc`: `sort_and_filter_mutations_for_range(...)`

An index-hash optimization would therefore need either:

- a logstor-specific repair boundary/order definition, or
- an auxiliary way to recover decorated-key ordering without full record reads

##### Problem 3: current repair intentionally reads once and reuses buffered rows

The current row-level repair path reads data once into `_row_buf` /
`_working_row_buf`, computes hashes, and then reuses those buffered rows if a
mismatch is detected.

Relevant code:

- `repair/row_level.cc`: `get_sync_boundary(...)`
- `repair/row_level.cc`: `request_row_hashes(...)`
- `repair/row_level.cc`: `get_full_row_hashes_handler()`
- `repair/row_level.cc`: `get_row_diff(...)`

This means the current design pays the I/O cost up front, but avoids rereading the
same rows when it needs to send actual diffs.

The index-hash optimization changes that tradeoff:

- fast path becomes much cheaper for synchronized ranges
- slow path still needs to read the actual records from disk later

That may still be a good tradeoff, but only if the synchronized case is common
enough to justify the extra index metadata and protocol complexity.

##### Problem 4: index memory overhead

The logstor primary index is intentionally compact and entirely in memory.

Relevant documentation and code:

- `docs/dev/logstor.md`: primary index description
- `replica/logstor/index.hh`: `primary_index_entry`

Adding a stored repair fingerprint to each index entry increases the memory cost of
the index for every key, even when repair is not running.

At minimum the design would likely need to add:

- a stable stored repair hash
- possibly a small amount of metadata describing what the hash represents

Before implementing this optimization, the memory overhead should be evaluated
against the expected repair performance benefit.

##### Problem 5: recovery and persistence questions

If the repair fingerprint exists only in memory, then recovery must recompute it
for every recovered log record at startup.

If startup/recovery cost is a concern, another option would be to persist the
fingerprint in the log record format itself. That would broaden the change from an
in-memory optimization into an on-disk format extension.

Relevant code:

- `replica/logstor/segment_manager.cc`: recovery path
- `replica/logstor/ondisk.hh`

##### What would need to be defined to make this optimization work

The deferred optimization would need explicit answers for at least these questions:

1. What is the stable stored repair fingerprint?
2. Does it represent the full logical mutation, the single live row fragment, the
   partition tombstone fragment, or something else?
3. How are repair sync boundaries defined without reading full decorated keys from
   disk?
4. How does repair fall back from index-only hashing to actual row reads on
   mismatch?
5. Is the fingerprint only in-memory, or persisted in the log record format?
6. What memory overhead per key is acceptable for the primary index?

##### Recommended role of this idea in the design

This should be treated as a possible future logstor-specific optimization, not as
the baseline repair design.

The recommended staged approach is:

1. First implement logstor repair using the normal logstor range reader and the
   standard mutation-fragment-based repair pipeline.
2. Validate correctness and baseline performance.
3. Only afterward evaluate whether an index-stored repair-fingerprint fast path is
   worth the added complexity.

The baseline reader-first design is simpler because it is already mostly aligned
with the current repair algorithm, while the index-hash optimization would require
new stored-hash semantics and likely a logstor-specific notion of repair window
ordering.

### Apply of repair data

#### Problem statement

The current repair apply path is built around writing SSTables. Logstor needs a
different apply path that writes repaired data through the logstor write engine.

To make a sound decision for logstor, it is important to understand that the
normal repair path writes to streaming SSTables for a mix of correctness reasons
and LSM-specific implementation choices.

#### Why normal repair writes to streaming SSTables first

The current non-logstor repair apply path does not call the normal table write
path. Instead it:

1. turns incoming repair rows into a mutation-fragment stream
2. feeds that stream into `repair_writer`
3. uses `streaming::make_streaming_consumer(...)`
4. writes streaming SSTables and publishes them into the table

Relevant code:

- `repair/row_level.cc`: `do_apply_rows(...)`
- `repair/row_level.cc`: `repair_writer_impl::create_writer(...)`
- `streaming/consumer.cc`: `make_streaming_consumer(...)`

This should not be interpreted as "repair must always stage through a special
storage object". In the current system, writing to streaming SSTables is how
repair integrates with the normal-table storage model and with existing view and
incremental-repair hooks.

The main reasons are:

##### Reason 1: view-update handling for normal tables

The normal write path performs write-side view handling through
`push_view_replica_updates(...)` before the base write is applied.

Relevant code:

- `replica/database.cc`: `database::do_apply(...)`
- `replica/table.cc`: `push_view_replica_updates(...)`

Repair does not use that path. Instead, the SSTable streaming path can route data
through staging SSTables and the view-update generator/view-building path.

Relevant code:

- `db/view/view.cc`: `check_needs_view_update_path(...)`
- `streaming/consumer.cc`: staging versus normal SSTable decision
- `db/view/view_update_generator.cc`: generate updates from staging SSTables and
  move them into the base directory afterward
- `replica/table.cc`: exclude staging SSTables from normal reads until view work is
  complete

So for normal tables, the staging path is part of how repair remains compatible
with materialized-view maintenance.

##### Reason 2: incremental repair state is modeled on SSTables

The current incremental repair machinery tracks repaired state using SSTable
metadata such as:

- `repaired_at`
- `being_repaired`
- repaired / repairing / unrepaired classification

Relevant code:

- `repair/incremental.cc`
- `repair/row_level.cc`: `mark_sstable_as_repaired(...)`
- `replica/table.cc`: repair SSTable classifier logic

This is another reason the current repair path is SSTable-oriented. The repair
state machine for normal tables is attached to SSTable ingestion and SSTable
metadata, not to the ordinary mutation-apply path.

##### Reason 3: the rest is mostly LSM-specific ingestion policy

Once repair uses SSTables, it can also reuse LSM-specific behavior such as:

- offstrategy / maintenance-set placement
- later reshape by compaction
- repair-origin SSTable policies

Relevant code:

- `repair/row_level.cc`: offstrategy handling in `repair_writer_impl::create_writer(...)`
- `streaming/consumer.cc`
- `replica/table.cc`: `add_new_sstable_and_update_cache(...)`

These are important for normal-table performance and layout, but they are not by
themselves a proof that logstor must also stage repair data before making it
visible.

#### Constraints

- incoming repair data arrives as a mutation-fragment stream grouped by partition
- logstor writes full partition values as `canonical_mutation`
- current logstor tables have partition key only, with no clustering columns
- for the initial logstor repair implementation, we assume logstor base tables do
  not need materialized-view support in the repair path
- incremental repair is out of scope for the initial logstor implementation

That last property is important: it should make it possible to reconstruct one
full-partition mutation per repaired partition, then write it through logstor.

Those constraints lead to an important design conclusion: the main correctness
reasons that force normal repair through streaming SSTables do not apply to the
initial logstor repair scope.

- We are not trying to reuse SSTable-based incremental repair bookkeeping.
- We are explicitly assuming no materialized-view support for logstor repair in the
  first version.

Under those assumptions, direct apply through the logstor table write path is the
preferred design.

#### Possible solution A: build a logstor-specific repair writer

Add a repair writer implementation specialized for logstor tables.

Relevant code:

- `repair/writer.hh`: `repair_writer`
- `repair/row_level.cc`: existing `repair_writer_impl`
- `mutation/mutation_rebuilder.hh`
- `replica/table.cc`: `table::apply(const mutation&, ...)`

Conceptually:

1. receive repair rows on wire
2. convert them to mutation fragments as today
3. reassemble a full partition mutation for each partition
4. write it through `table::apply(...)`

This is the chosen direction for the initial logstor repair implementation.

Advantages:

- fits the existing repair protocol
- uses logstor's natural write path
- keeps the backend-specific logic on the apply side, where it belongs
- avoids forcing logstor to invent an SSTable-like staging layer just for repair
- keeps the implementation aligned with the actual logstor storage model, which
  stores full logical partition values rather than SSTables

Disadvantages:

- requires partition reconstruction rather than direct fragment streaming to an
  SSTable writer
- needs careful handling of deletes/tombstones and timestamps
- does not provide materialized-view support in the initial version
- does not provide an incremental-repair state model in the initial version

#### Possible solution B: materialize temporary SSTables then import into logstor

This would preserve the current repair writer and add an extra conversion step.

Advantages:

- may reuse more of the current repair writer

Disadvantages:

- unnatural for logstor
- adds a costly and conceptually awkward storage conversion step
- likely more complex than writing directly to logstor
- would still not naturally solve how logstor should represent incremental repair
  state or any future view-related behavior

#### Partition reconstruction considerations

Because logstor stores partition-key-only tables with full partition values, the
repair apply path should likely accumulate all mutation fragments for one
partition, materialize a final `mutation`, and then write that mutation through
the normal logstor write path.

This is now the intended baseline design.

The existing `mutation_rebuilder` / `mutation_rebuilder_v2` utilities are the
natural building blocks for this step. They already know how to reconstruct a
logical `mutation` from the standard mutation-fragment stream.

Relevant code:

- `mutation/mutation_rebuilder.hh`

Questions to resolve during implementation:

- what is the simplest and safest way to rebuild a `mutation` from the repair
  fragments already available?
- should writes be performed partition-by-partition or batched?
- what barriers or gating are required relative to compaction/separator activity?

#### Logstor repair writer API design

The initial implementation should preserve the existing high-level repair-writer
abstraction and introduce a second backend-specific implementation for logstor.

The planned split is:

- normal tables:
  - existing `repair_writer_impl`
  - fragment queue -> streaming consumer -> streaming SSTables
- logstor tables:
  - new logstor repair writer implementation
  - fragment stream -> `mutation_rebuilder` -> `table::apply(...)`

This keeps the row-level repair protocol and the outer repair writer interface
stable, while changing only the sink implementation.

The expected high-level API shape is:

1. `make_repair_writer(...)` inspects the base table backend
2. if the table is normal, return the existing SSTable-based writer
3. if the table uses logstor, return a logstor-specific repair writer

The logstor writer should:

1. accept the repaired mutation-fragment stream from the existing repair code
2. preserve the partition ordering guaranteed by repair
3. reconstruct one logical `mutation` per partition
4. route the rebuilt partition mutation to the correct local owning shard before
   applying it
5. on the owning shard, write that mutation through `table::apply(...)`
6. clear local reconstruction state and continue with the next partition

This is intentionally different from the normal-table writer, which preserves the
fragment stream and lets SSTable writing materialize storage. For logstor, the
writer itself should materialize the full logical partition because logstor stores
and writes full partition values.

#### Required local shard routing for the logstor repair writer

This is a required part of the design, not an optional optimization.

The current repair write path for normal tables redistributes incoming repaired data
across local shards before ingesting it. The direct logstor writer must preserve
the same property.

Relevant code:

- `repair/row_level.cc`: `repair_writer_impl::create_writer(...)`
- `repair/row_level.cc`: `get_sharder_helper(...)`
- `replica/table.cc`: `table::apply(const mutation&, ...)`

`table::apply(...)` does not itself cross-dispatch to another shard. It applies the
mutation on the shard where it is called, selecting a compaction group only within
that shard.

Therefore, the logstor repair writer must not simply rebuild a partition and call
`table::apply(...)` on the shard where the repair RPC happened to land. Instead it
must:

1. determine the correct owning local shard for the partition mutation
2. dispatch the apply step to that shard
3. call `table::apply(...)` there

The most likely implementation shape is to reuse the same sharder information that
the current repair writer already uses for the normal-table multishard write path,
but apply it at partition granularity instead of fragment-stream distribution.

This requirement should be treated as a core correctness item for the direct-apply
writer.

#### Concrete shard-routing plan for the logstor repair writer

The recommended routing plan is to reuse the current repair writer's sharder model,
but to apply it partition by partition.

Relevant code:

- `repair/row_level.cc`: `get_sharder_helper(...)`
- `repair/row_level.cc`: current use in `repair_writer_impl::create_writer(...)`
- `mutation_writer/multishard_writer.hh`
- `replica/database.hh`: `shard_for_writes(...)`

The current normal-table repair writer already obtains a topology-aware sharder via
`get_sharder_helper(...)`.

That helper handles two important cases:

- no topology guard:
  - keep `effective_replication_map_ptr` alive
  - use `erm->get_sharder(schema)`
- repair under a topology guard / tablet-aware case:
  - use `dht::auto_refreshing_sharder`
  - keep the topology guard alive together with the sharder

The logstor writer should reuse the same lifetime model.

The routing algorithm for each rebuilt partition mutation should be:

1. obtain the partition token from the rebuilt `mutation`
2. call `sharder.shard_for_writes(token)`
3. if the returned shard set is empty, fail the repair operation
4. freeze or otherwise preserve the rebuilt mutation for cross-shard dispatch
5. dispatch the apply step to every returned shard
6. on each destination shard, call `table::apply(...)`
7. wait for all destination-shard apply operations to finish before advancing

Using `shard_for_writes()` is important. The logstor writer must not use
`shard_for_reads()`, because write routing may differ from read routing, and during
tablet transitions writes may need to be sent to more than one local shard.

This means that the direct-apply logstor writer must be prepared for
`shard_for_writes()` to return more than one local shard and to apply the same
reconciled partition mutation to all returned shards.

#### Recommended implementation shape for shard routing

The most likely implementation shape is:

1. when the logstor repair writer is created, build and retain a `sharder_helper`
   using the base table, schema, and repair topology guard
2. while consuming the fragment stream, rebuild one full partition mutation at a
   time
3. when a partition is complete, hand the rebuilt mutation to a helper such as
   `apply_rebuilt_partition(...)`
4. inside that helper:
   - compute the destination shards via `shard_for_writes(token)`
   - dispatch to each destination shard using `db.invoke_on(...)`
   - on the target shard, look up the local table and call the future
     `table::apply(...)` overload
   - wait for all destination-shard operations to succeed

This keeps the repair writer logic simple:

- partition reconstruction stays local to the writer
- shard routing happens only once per completed partition
- actual persistence still goes through the normal table-level logstor write entry
  point on the correct shard

#### Important pitfalls to avoid

The shard-routing design should explicitly avoid these mistakes:

1. rebuilding a partition and calling `table::apply(...)` on the repair RPC handler
   shard without routing first
2. using `shard_for_reads()` instead of `shard_for_writes()`
3. using a static sharder source that is not correct for tablet-aware repair
4. using the inline non-future `table::apply(...)` helper overloads instead of the
   future write path implemented in `replica/table.cc`

If the routing plan follows the current `get_sharder_helper(...)` model and uses
`shard_for_writes()` plus `db.invoke_on(...)`, it will remain consistent with the
way the normal repair writer already distributes repaired data across local shards.

#### Why `table::apply(...)` is the chosen write target

For the initial implementation, the logstor repair writer should target
`table::apply(const mutation&, db::rp_handle&&, db::timeout_clock::time_point)`.

Relevant code:

- `replica/table.cc`: `table::apply(const mutation&, ...)`
- `replica/logstor/logstor.hh`: `logstor::write(...)`

This is preferred over calling `logstor::write(...)` directly because:

- `table::apply(...)` is the normal table-level entry point for writes
- the existing logstor branch in `table::apply(...)` already routes the mutation
  to `_logstor->write(...)`
- it preserves table-level compaction-group routing and the current async-gate
  structure around writes
- it keeps the repair writer coupled to the table abstraction rather than to the
  lower-level logstor internals

With this choice, the logstor repair writer remains a backend-aware repair sink,
not a second write path that has to reimplement table-level routing rules.

This does not remove the need for explicit local shard routing in the repair writer.
It only means that once the writer dispatches to the correct shard, it should use
the normal table-level write entry point there.

#### Required operation-lifetime tracking for the direct-apply writer

Another required design item is preserving the pending-operation phaser semantics
that the current repair/streaming path gets from `stream_in_progress()`.

Relevant code:

- `repair/row_level.cc`: current repair writer uses `t.stream_in_progress()`
- `replica/database.cc`: normal writes use `write_in_progress()`
- `replica/table.cc`: `table::apply(...)` does not acquire the write or stream
  phasers on its own

The direct logstor writer must therefore hold an explicit repair-operation guard
while it is applying rebuilt partitions. For the initial implementation, the repair
should be treated as a streaming-style maintenance operation, so the preferred
design is to keep using `stream_in_progress()` around the direct-apply writer.

This preserves the table-level pending-operation accounting that other subsystems
already rely on when coordinating with repair/streaming activity.

#### Decision for the initial logstor repair implementation

The repair apply path for logstor will use direct logical apply, not SSTable
staging.

Concretely:

1. repair receives mutation fragments using the existing row-level repair protocol
2. the logstor repair writer reconstructs one logical `mutation` per partition,
   using the standard mutation-fragment representation
3. the writer routes the rebuilt mutation to the correct local owning shard
4. on that shard, the resulting reconciled mutation is applied through the logstor
   table write path via `table::apply(...)`
5. the writer holds the appropriate repair-operation phaser guard while these
   direct applies are in progress

This decision is based on the current scope assumptions:

- full repair only
- no materialized-view support for logstor repair in the first version
- no attempt to reuse SSTable-based incremental repair metadata

This keeps the logstor repair apply path aligned with the logstor storage model and
avoids introducing an unnecessary LSM-style staging layer.

#### Limitations of the direct-apply decision

The direct-apply design is intentionally scoped and should be documented with its
limitations.

For the initial version:

- logstor repair does not support the normal-table staging-SSTable view-update path
- logstor repair assumes no materialized views on repaired logstor base tables
- logstor repair does not support incremental repair

This means that unsupported MV-related repair behavior for logstor must be rejected
explicitly before the direct-apply writer is used. It should not be left as an
implicit limitation.

If any of those assumptions change in the future, the apply-path design will need
to be revisited. In particular, future MV support for logstor repair may require a
new logstor-specific deferred-view-update mechanism, and future incremental repair
may require a non-SSTable repaired-state model.

### Incremental repair and full repair

#### Current state

The current incremental repair implementation is explicitly SSTable-based.

Relevant code:

- `repair/incremental.hh`
- `repair/row_level.cc`: `prepare_sstables_for_incremental_repair()`
- `repair/row_level.cc`: `mark_sstable_as_repaired()`

It depends on:

- storage snapshots as SSTable sets
- repaired-at metadata on SSTables
- compaction behavior that understands repaired state

#### Full repair for logstor

Full repair is the best initial target for logstor support.

Reasons:

- it can operate on the live logical data stream exposed by a logstor repair
  reader
- it does not need repaired/unrepaired storage bookkeeping
- it minimizes new storage metadata requirements
- it matches the direct-apply decision for the logstor repair writer

#### Incremental repair options for logstor

Possible approaches:

1. Do not support incremental repair initially.
2. Support a logstor-specific notion of repaired state later.
3. Emulate incremental repair by taking logical snapshots and tracking separate
   metadata outside the current SSTable-based scheme.

Option 1 is strongly preferred for the first implementation.

Reasons:

- avoids designing segment-level repaired-state metadata prematurely
- allows the team to first validate full repair correctness and performance
- keeps the initial scope bounded

If incremental repair is added later, the design will need answers for:

- what is the repaired unit: segment, record, range, or logical snapshot?
- how repaired state interacts with compaction and separator rewrites
- how repaired state survives restart and recovery
- how to prevent rewritten or moved log records from losing repaired metadata

### Convergence semantics and equal-timestamp conflicts

#### Problem statement

Repair is often summarized as "newer timestamp wins", but that is only the first
level of the reconciliation semantics. We also need deterministic convergence when
replicas have different contents with the same timestamp.

For logstor this question matters in two places:

- when repair receives multiple fragment variants and must reconcile them
- when the final repaired mutation is written back through the logstor backend

#### Current repair behavior

The current repair code does not appear to implement a repair-specific value
tie-breaker. Instead, it:

- detects mismatching fragments by hash
- transfers the differing fragments
- merges same-position fragments before writing
- relies on the generic mutation reconciliation rules to choose the winner

Relevant code:

- `repair/row_level.cc`: follower-side merge in `to_repair_rows_list(...)`
- `repair/row_level.cc`: master-side merge in `flush_rows(...)`
- `mutation/mutation_fragment.hh`: `mergeable_with(...)`

In other words, repair mostly orchestrates data exchange. The semantic winner is
chosen by generic mutation/row/cell merge logic.

#### Generic reconciliation rules already used by Scylla

##### Tombstones

Partition tombstones are merged by ordinary tombstone ordering. The larger
tombstone wins according to the tuple ordering of:

- timestamp
- then deletion time

Relevant code:

- `mutation/tombstone.hh`: `tombstone::apply(...)`

##### Row markers

Row markers use `compare_row_marker_for_merge(...)`.

Relevant code:

- `mutation/mutation_partition.cc`: `compare_row_marker_for_merge(...)`
- `mutation/mutation_partition.hh`: `row_marker::apply(...)`

At equal timestamp, the deterministic rules are:

- dead beats live
- expiring beats non-expiring
- if both are expiring, later expiry wins
- if expiry is equal, smaller TTL wins because that implies a later derived write
  time
- if both are dead, later deletion time wins

If all of the above are equal, the row markers are equal. Row markers do not have
an additional value-based tie-breaker.

##### Atomic cells

Atomic cells use `compare_atomic_cell_for_merge(...)`.

Relevant code:

- `mutation/atomic_cell.cc`: `compare_atomic_cell_for_merge(...)`

At equal timestamp, the deterministic rules are:

- tombstone beats live
- expiring beats non-expiring
- if both are expiring, later expiry wins
- if expiry is equal, smaller TTL wins
- if both are live and all other attributes are equal, the lexicographically
  larger unsigned cell value wins
- if both are deleted, later deletion time wins

This is the most important existing rule for "same timestamp but different live
value" conflicts.

##### Row-level application order

When two versions of the same logical row are applied, Scylla merges:

1. cells
2. row marker
3. row tombstone / shadowable tombstone interaction

Relevant code:

- `mutation/mutation_partition.cc`: `deletable_row::apply_monotonically(...)`
- `mutation/mutation_partition.hh`: `shadowable_tombstone`

So the final logical winner is not chosen by a single "repair rule". It is the
composition of the generic row marker, cell, and tombstone reconciliation rules.

#### Implications for logstor repair

For current logstor tables, a live partition is effectively one logical row with:

- one row marker
- one or more regular cells

Therefore, equal-timestamp conflicts for logstor should be resolved by preserving
the generic mutation semantics above, not by inventing a logstor-specific repair
winner.

The preferred logstor-repair semantics are:

1. reconstruct a normal logical `mutation` from the incoming fragments
2. let generic mutation reconciliation rules determine the winning contents
3. write the single reconciled mutation through `table::apply(...)`, which then
   routes it to the logstor backend

This preserves the same semantics as normal-table repair.

#### Important distinction: repair semantics versus current logstor index replacement

There is a related but different logstor behavior at the storage/index layer.
Logstor's primary index currently decides whether a new record replaces an old one
using only the record timestamp.

Relevant code:

- `replica/logstor/index.hh`: `default_entry_cmp(...)`
- `replica/logstor/index.hh`: `insert(...)`
- `replica/logstor/logstor.cc`: `logstor::write(...)`

`default_entry_cmp(...)` compares only `index_entry.timestamp`. This means that if
two unreconciled full-partition logstor records for the same key have the same
timestamp, the later inserted record wins at the primary-index level, regardless of
the deeper cell-level tie-breakers described above.

This is not the same thing as repair semantics. For repair, we should not rely on
storage-layer equal-timestamp replacement to perform reconciliation. Instead, the
repair path should first compute the reconciled logical mutation and then write that
single final result.

#### Possible solutions for logstor repair

##### Solution A: preserve generic mutation reconciliation and write one final mutation

This is the preferred approach.

Conceptually:

1. receive repair fragments
2. rebuild one logical `mutation` per partition
3. merge conflicting contents using the normal mutation reconciliation rules
4. write the final reconciled mutation through `table::apply(...)`

Advantages:

- matches existing Scylla repair semantics
- avoids inventing a logstor-specific repair tie-breaker
- keeps repair behavior aligned with normal tables
- matches the direct-apply repair-writer design chosen for logstor
- keeps the writer aligned with the normal table abstraction while still reaching
  the logstor backend

Disadvantages:

- requires reconstruction and reconciliation before the logstor write

##### Solution B: define a logstor-specific tie-break at the storage record level

An alternative would be to extend logstor's storage/index replacement rules to look
deeper than the top-level record timestamp for equal-timestamp records.

For example, one could imagine tie-breaking on:

- reconciled logical mutation ordering
- canonical serialized bytes
- a stable digest of the canonical mutation

This is not recommended for the initial repair implementation.

Reasons:

- it would broaden the scope from repair into core logstor write semantics
- it would need to be shown equivalent to the generic mutation reconciliation
  rules, which is non-trivial
- it is unnecessary if repair already writes a single reconciled final mutation

#### Recommendation

The logstor repair design should define convergence in terms of the generic Scylla
mutation reconciliation semantics. The repair path should produce one reconciled
logical mutation per partition and then write that result through `table::apply()`.

The design document should also explicitly note that current logstor primary-index
replacement is timestamp-only, and that this is a separate storage-layer concern
from repair convergence semantics.

### Other relevant concerns

#### Ordering guarantees

Repair assumes a stable order over the logical data stream. Logstor's primary
index is ordered by token and key hash, not full decorated key. The existing
logstor range reader compensates by sorting same-token runs after reading the log
records.

Relevant code:

- `replica/logstor/logstor.cc`: `sort_and_filter_mutations_for_range(...)`

Any repair reader built on top of logstor must preserve this ordering behavior.

#### Cache interaction

The current logstor range reader bypasses cache because the cache entries do not
store the full partition key.

Relevant code:

- `replica/logstor/logstor.cc`: comments in `read_mutations_for_batch(...)`
- `replica/logstor/cache.hh`

This is acceptable for correctness, but it may affect repair performance.

#### Snapshot and consistency boundaries

The current logstor snapshot/file-stream path explicitly flushes separator buffers
before taking a logstor snapshot.

Relevant code:

- `replica/table.cc`: `table::take_logstor_snapshot(...)`

It is not yet clear whether full row-level repair needs a similar barrier before a
live repair scan, or whether the normal logstor index/read phasing is sufficient.

This needs explicit validation.

#### Tablet and storage-group interactions

Repair, tablet migration, and logstor segment ownership all interact with storage
groups and compaction groups. Logstor segment routing already has logic to map
loaded segments into the right groups.

Relevant code:

- `replica/table.cc`
- `replica/logstor/segment_manager.cc`

The first full-repair implementation should avoid overcomplicating this. It should
read logical rows from the current table view and write logical rows back through
the normal table write path, letting the existing logstor storage-group logic make
placement decisions.

## Preferred design direction

### Summary

The preferred first implementation is:

1. keep the existing row-level repair protocol and wire format
2. add a logstor-aware repair reader
3. add a logstor-aware repair apply path
4. support full repair only
5. explicitly reject incremental repair for logstor tables initially

This keeps the repair algorithm storage-backend-agnostic at the protocol level,
while allowing the read and write endpoints to dispatch to the correct backend.

### Why this is preferred

- It minimizes changes to the core repair algorithm.
- It reuses the current row-level hash/diff protocol.
- It naturally supports delta repair instead of bulk file transfer.
- It matches logstor's logical data model more closely than SSTable-based apply.
- It avoids prematurely designing repaired-state metadata for logstor.

## Design and implementation plan

### Phase 1: make unsupported cases explicit and prepare the repair entry points

Objective:

- ensure the code enters only the subset of behavior we are intentionally
  designing for:
  - full repair
  - no materialized-view support for logstor repair
  - no incremental repair for logstor

Files likely to touch:

- `repair/repair.cc`
- `repair/row_level.cc`
- `repair/writer.hh`
- `docs/dev/logstor_repair.md`

Concrete tasks:

1. Add explicit rejection for logstor incremental repair before the SSTable-set
   repair path is reached.
2. Add comments in the repair read and write paths documenting that logstor uses a
   different implementation strategy.
3. Add an explicit guard that rejects unsupported MV-related repair behavior for
   logstor tables.
4. Make sure the design document remains synchronized with the implementation
   scope.

Exit criteria:

- the code clearly rejects unsupported logstor repair modes instead of failing
  later in SSTable-specific code
- the intended scope of the first implementation is visible in both code and docs

### Phase 2: route repair reads through the existing logstor range reader

Objective:

- make the current row-level repair reader use logstor's live range reader while
  preserving the existing repair algorithm and multishard reader structure

Files likely to touch:

- `replica/table.cc`
- possibly `replica/database.hh`
- possibly `replica/database.cc`
- `repair/row_level.cc`
- `docs/dev/logstor_repair.md`

Concrete tasks:

1. Extend the live-read overloads of `table::make_streaming_reader(...)` so that,
   when `uses_logstor()` is true, they delegate to `_logstor->make_reader(...)`
   instead of the memtable + SSTable read path.
2. Ensure the multi-range live-read overload also dispatches to logstor-backed
   per-range readers for logstor tables.
3. Keep the SSTable-set overload of `make_streaming_reader(...)` unchanged and
   LSM-only.
4. Leave `repair_reader::read_strategy` unchanged for the initial version.
5. Verify that `make_multishard_streaming_reader(...)` automatically benefits from
   the new backend-aware `make_streaming_reader(...)` behavior, since it creates
   shard readers through that API.

Behavior to validate:

1. Repair local reads on logstor tables produce a valid mutation-fragment stream.
2. Repair multishard reads on logstor tables still work through the existing
   multishard combining reader.
3. Same-token partitions are emitted in decorated-key ring order.
4. Live rows and partition tombstones are emitted in the fragment shapes described
   earlier in this document.
5. `next_partition()` and `fast_forward_to(...)` remain correct under the repair
   reader and multishard reader usage patterns.

Exit criteria:

- full row-level repair can read logstor table data using the normal repair reader
  pipeline without entering SSTable-specific live-read code

### Phase 3: add a logstor-specific repair writer that rebuilds partitions and uses `table::apply(...)`

Objective:

- keep the existing repair protocol and reconciliation behavior, but replace the
  SSTable sink with a logstor sink that rebuilds and directly applies full logical
  mutations

Files likely to touch:

- `repair/writer.hh`
- `repair/row_level.cc`
- possibly a new helper in `repair/` for the logstor writer implementation
- `replica/table.cc` only if small writer-facing helpers are needed
- `docs/dev/logstor_repair.md`

Concrete tasks:

1. Extend `make_repair_writer(...)` so it dispatches based on the base table
   backend.
2. Keep the existing `repair_writer_impl` unchanged for normal tables.
3. Add a new logstor-specific `repair_writer::impl`.
4. In the logstor writer implementation, consume the repaired mutation-fragment
   stream partition by partition.
5. Rebuild one logical `mutation` per partition using
   `mutation_rebuilder` or `mutation_rebuilder_v2`.
6. Reuse the current `get_sharder_helper(...)` pattern to obtain a repair-safe
   write sharder for the table.
7. Determine the correct local owning shard set for each rebuilt partition mutation
   via `shard_for_writes(token)`.
8. Dispatch the apply step to every returned destination shard.
8. Hold `stream_in_progress()` or an equivalent repair-operation guard for the
   direct-apply writer.
9. On each owning shard, write the rebuilt mutation through
   `table::apply(const mutation&, db::rp_handle&&, db::timeout_clock::time_point)`.
10. Clear reconstruction state and continue with the next partition.
11. Keep the implementation explicitly scoped to:
   - no MV support for logstor repair
   - no incremental repair support for logstor

Behavior to validate:

1. Incoming repair fragments reconstruct the expected full partition mutation.
2. Deletes reconstruct to the expected partition-tombstone mutation shape.
3. Rebuilt partitions are applied on the correct local owning shard set.
4. Multi-shard write routing behaves correctly if `shard_for_writes()` returns more
   than one shard.
5. Writes are applied in partition order and remain idempotent under repeated
   repair.
6. The chosen timeout and gating policy around `table::apply(...)` is correct and
   documented.
7. The direct-apply writer preserves the required pending-operation phaser
   semantics.
8. The logstor writer does not need a hidden staging object in the current scoped
   design.

Exit criteria:

- a follower can receive repaired rows for a logstor table and persist them via
  direct logical apply through `table::apply(...)`

### Phase 4: validate convergence and conflict resolution end to end

Objective:

- prove that logstor repair converges to the same logical result as the normal
  repair semantics, including same-timestamp conflicts

Files likely to touch:

- `test/cluster/test_logstor.py`
- `test/cluster/test_repair.py`
- `test/boost/logstor_test.cc`

Concrete validation scenarios:

1. Missing partition on one replica.
2. Different live values on replicas with different timestamps.
3. Delete versus live row conflict.
4. Equal-timestamp conflicting live values, verifying that repair reconciliation
   produces one final logical mutation and that the final apply converges to that
   result.
5. Repeated repair of already-repaired data.
6. Repair over a bounded token range.
7. A same-token multi-key case, to validate decorated-key ordering through the
   logstor range reader.

Key success criteria:

- repaired value wins according to the generic mutation reconciliation rules
- equal-timestamp conflicting values converge according to the documented generic
  rules before the final write
- the final `table::apply(...)` write makes the reconciled result authoritative in
  logstor
- direct apply preserves correct local shard placement
- direct apply preserves the required repair-operation lifetime tracking
- direct apply behaves correctly in the no-MV, full-repair-only scope

### Phase 5: performance and robustness follow-up for the baseline design

Objective:

- stabilize the baseline reader-first, direct-apply implementation before any
  larger optimization work

Concrete follow-up topics:

1. Measure repair throughput and read amplification with the logstor range reader.
2. Evaluate memory behavior of partition reconstruction in the logstor repair
   writer.
3. Validate that no extra barrier beyond the existing logstor reader/index phasing
   is required for correctness, or document and implement one if needed.
4. Revisit timeout policy around repeated `table::apply(...)` calls in the writer.
5. Revisit whether the direct-apply writer should keep using `stream_in_progress()`
   as-is or whether a narrower repair-specific lifetime guard is justified.
6. Revisit batching only if profiling shows that per-partition direct apply is too
   expensive.

This phase should happen before introducing more ambitious optimizations.

### Phase 6: deferred design work after the baseline is stable

Only after the baseline full-repair implementation is correct and tested should we
consider broader extensions such as:

1. index-stored repair fingerprint optimization
2. MV support for logstor repair
3. incremental repair for logstor

Questions that remain deferred until then:

- what storage object should carry repaired state?
- how does repaired state survive segment rewrite during compaction or separator
  movement?
- does logstor need segment-level metadata, per-record metadata, or external range
  metadata?
- how should repaired state interact with recovery and snapshotting?
- what logstor-specific deferred-view-update mechanism would be needed if repair on
  MV-bearing logstor tables is ever supported?

No incremental-repair or MV-support implementation for logstor should start before
these questions are answered.

## Open questions

1. Should the table-level API for repair reads remain `make_streaming_reader(...)`,
   or should repair get a dedicated backend-dispatching read API?
2. Is `logstor::make_reader(...)` already sufficient as the repair read primitive,
   or does repair need stronger consistency or ordering guarantees?
3. What is the cleanest way to reconstruct a full partition `mutation` from the
   incoming repair fragments?
4. Does full repair on logstor require any extra flush/barrier step before scans?
5. What timeout/gating policy should the logstor repair writer use around
   `table::apply(...)` calls?
6. Should the direct-apply writer hold `stream_in_progress()` exactly as the
   current streaming repair path does, or is a narrower equivalent guard needed?
7. Should logstor's storage-layer equal-timestamp replacement semantics remain
   timestamp-only, or is a broader storage-engine change needed outside repair?
8. Is there any need for a future segment-based repair optimization after row-level
   support exists, or would that only add complexity?

## Initial recommendations

- Implement full repair first.
- Keep the current row-level repair wire protocol.
- Make the repair reader backend-aware.
- Make the repair writer backend-aware.
- Reject incremental repair for logstor in the initial version.
- Add focused cluster tests before attempting any broader optimization work.
