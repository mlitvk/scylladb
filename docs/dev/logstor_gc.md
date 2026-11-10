# Logstor Tombstone GC and TTL Expiry

## Status

> **Note, Nov 10 2026.** The branch names and the terms of the erase changed after this
> document was written. The body keeps the old names, and the
> [addendum](#addendum-branches-and-terms-since-this-document-was-written) at the end maps them to the current ones.

Design document, reviewed against the code on 2026-10-01: branch `logstor_gc`
(master at 1fc937a62f) and the parallel branch `logstor_recovery1`, whose
discard/recovery work this design now builds on (see
[Interaction with the discard work](#interaction-with-the-discard-work-logstor_recovery1)).
The GC branch is to be rebased onto `logstor_recovery1` before implementation.

Gate 2 (local no-resurrection) has **three candidate mechanisms**:

- the **sequence reclaim-frontier** plus monotone **horizons** — the
  recommended base, detailed in [Gate 2](#gate-2--local-no-resurrection).
  This revision replaces its global external horizon with a per-entry
  re-clamp and closes the in-flight-copy hole by the publish-before-free rule
  the discard work wrote down;
- the **purge journal** — a per-key durable purge record filtered at
  recovery, the per-key form of `logstor_recovery1`'s erase watermark. It
  decouples drop latency from frontier progress and is the recommended
  extension once the base works (see [Purge journal](#gate-2-candidate-2--purge-journal));
- **mark-and-sweep verification** — a read-only scan of all scannable
  segments plus a write barrier. Kept for the record (see
  [Mark-and-sweep](#gate-2-candidate-3--mark-and-sweep-verification)); no
  longer recommended, since the purge journal removes forced rewriting at a
  lower cost.

Everything outside Gate 2 — GC metadata (Stage A), Gate 1, the two-gate rule,
TTL handling — is common to all three. In the base nothing new is persisted
and GC state is recomputed at recovery; the journal is the one candidate that
persists per-key state.

What this review settled and what stays open is listed at the
[end](#settled-in-the-review-of-2026-10-01).

## Problem

Logstor never physically removes a deleted or expired partition.

- A delete is written as a normal record whose value is a pure
  partition tombstone. It overwrites the previous record in the primary index
  and the old record is freed, but the tombstone record itself becomes the sole
  live index entry and is rewritten forever by compaction — its space is never
  reclaimed on disk or in the in-memory index.
- A row that expires by TTL behaves the same way: reads hide it, but the record
  stays alive in the index and on disk indefinitely.

The goal is to reclaim the disk and memory occupied by tombstones and expired
rows, safely — without resurrecting older values on this node or across the
cluster.

## Background

### How logstor stores data

Logstor is a **whole-partition, single-version-per-key** store (key-value
tables, no clustering columns). Each write serializes the entire partition as
one `log_record` (a `record_value`, the encoded partition without its key
and table, plus a `record_header{key, timestamp, table}`). The in-memory `primary_index`
holds **exactly one live `index_entry{location, timestamp}` per key**.

Relevant behaviors (see `replica/logstor/`; the facts below were checked
against the code, and the ones marked *recovery1* describe the
`logstor_recovery1` branch):

- **A delete is just a write.** `logstor::write` builds the record, waits for
  the write buffer to report it durable, then calls `primary_index::insert`;
  on overwrite the index itself frees the previous record through
  `space_accounting_subscriber::on_free_record`. The record timestamp is the
  live row-marker timestamp or, for a delete, the partition tombstone
  timestamp (`extract_logstor_record_timestamp`). The on-disk header
  (`ondisk.hh`: token, timestamp, table id, key — hand-encoded, not IDL) has
  nothing that distinguishes a tombstone from a live record.
- **Conflict resolution is record-level LWW.** `primary_index::insert` keeps
  the entry with the higher timestamp; on a tie the *new* entry wins, whereas
  recovery prefers the higher segment sequence — the equal-timestamp gap
  documented in `logstor_discard.md`. A write that loses (a late or backdated
  write) is still written physically and is then **never accounted**:
  `logstor::write` ignores the insert outcome, the bytes stay in the segment
  counted as free space, and no `on_free_record` runs. The same holds for a
  relocation copy whose `update_record_location` fails. Such records are
  *stale copies* exactly like freed ones, but the per-segment accounting does
  not know about them.
- **`on_free_record` frees accounting, not bytes.** It increases
  `segment_descriptor.free_space`; the record bytes physically remain in the
  segment until the segment is rewritten or reused.
- **`free_segment` frees the slot, not the disk.** Its precondition is
  `record_bytes == 0 && ref_count == 0`. The slot goes back to
  `_free_segments` with its on-disk content intact and fully scannable; new
  slots are allocated first and the free list is FIFO, so a freed slot can
  stay scannable for a long time. Only the erase path
  (*recovery1*: `discard_group_segments`, formerly `discard_segments`)
  zeroes a segment's first block. *recovery1* writes the two rules that make
  freeing safe at `free_segment`: **P3, publish before free** — whoever frees
  a segment because it copied its records elsewhere has made the copies
  index-visible and drained readers first; **P2, erase implies a watermark**.
- **Compaction keeps everything the index points to.** `do_compaction`
  rewrites every record for which `index.is_record_alive()` is true at
  header time and skips the rest. A tombstone/expired record is the live
  entry, so it is rewritten forever. Segment selection is utilization-based;
  tombstones count as live data. The copier holds a `segment_ref` on every
  source segment until each copy's `update_record_location` has run and
  pending reads have drained (P3).
- **The separator copies every record of a mixed buffer, alive or not.**
  User writes land in the shared mixed active segment; the separator task
  (queued under `_writes_phaser`, holding the segment's ref) buffers a copy
  of each record in its group's separator buffer, and the buffer's flush
  writes them into a full segment of the group and calls
  `update_record_location` for each — whose result it ignores. A copy of a
  record that was overwritten meanwhile is therefore a stale copy in a
  fresh, high-seq segment. Buffers flush when full, when the segment switch
  finds them older than 5% of the disk in sequences
  (`flush_all_separator_buffers(seq)`), and on erase/stop; a cold group's
  buffer can hold a record for a long time.
- **Recovery rebuilds the index by scanning all slots of all files**,
  keeping the max `(timestamp, segment_seq, offset)` per key
  (`cmp_with_seq`). Stale copies are re-scanned and lose only because a
  newer record exists. *recovery1*: `rebuild_index(index_resolver,
  erase_watermarks)` drops, at scan time and before insertion, records
  covered by an erase watermark and records whose token has no group on the
  shard; both are counted as tripwire metrics.
- **`segment_seq`** is a monotonic per-shard sequence
  (`_next_segment_seq`), assigned to a segment when it is taken out of the
  segment pool (`get_segment`), so every seq issued so far is
  `<= _next_segment_seq − 1` and anything written from now on gets a higher
  one. It lives only in `writeable_segment` and in recovery's local vector;
  the descriptor does not keep it. All chunks of a mixed segment carry the
  segment's seq; `scan_segment` stops at the first invalid header and at
  the first chunk whose seq differs from the first chunk's, so a *reused*
  slot's stale tail is unreachable. Streamed-in segments get a fresh local
  seq (`streamed_segment_rewriter`); *recovery1* writes their first block
  last, so an aborted transfer leaves no valid header.
- **Files are opened with `O_DSYNC`** — a completed DMA write is durable.
- *recovery1*: **every chunk header carries a CRC of its body**
  (`body_crc`; the header grew to 32 bytes, format version stays 1: logstor
  is in development and old stores are simply unreadable). `scan_segment`
  reads a chunk's body whole and checks it before delivering a record, so a torn
  chunk — a crash mid-write, or a scan racing an append — is rejected
  rather than parsed.
- **Bytes land before the index call, on every path.** The write buffer
  reports a record durable before `logstor::write` inserts it (the buffer's
  gate is held until every insert of the buffer ran, so `await_pending_writes()`
  drains the inserts too); the copiers write the copy before
  `update_record_location`; `load_segment` writes the segment before
  `scan_slot` inserts each record.

### How traditional (LSM) storage does it

In the LSM engine, physical removal of tombstones and expired data happens
only when compaction rewrites sstables. Every fragment that flows through a
compacting reader is processed by the **mutation compactor**
(`compact_mutation_state` in `mutation/mutation_compactor.hh:168`,
instantiated as `compact_for_compaction` by `compaction/compaction.cc`).
Each tombstone it encounters — partition tombstone, range tombstone, row
tombstone, dead cell/marker — is tested by `can_purge_tombstone()`
(`mutation_compactor.hh:280`), which is the conjunction of the two gates
below. The same compactor class also runs at query time
(`compact_for_query`), where `can_gc` is just `is_gc_enabled()`: tombstones
past `gc_before` are *hidden* from results with no Gate 2 check, and nothing
is purged. The row cache has a read-time purge path of its own
(`db/cache_mutation_reader.hh:202`, with its own `gc_before`/`can_gc` fed
by `table::get_max_purgeable_fn_for_cache_underlying_reader`); physical
purge otherwise happens in `compact_for_sstables::yes` mode only.

#### Gate 1 — grace (`gc_before`)

The grace gate is `deletion_time < gc_before` (`satisfy_grace_period`,
`mutation_compactor.hh:276`). `gc_before` is computed per partition key by
`tombstone_gc_state::get_gc_before_for_key(schema, dk, query_time)`
(`tombstone_gc.cc:155`), according to the table's `tombstone_gc` mode
(`tombstone_gc_options.hh`):

- **`timeout`** — `gc_before = query_time − gc_grace_seconds` (the classic
  Cassandra rule). Purely heuristic cross-node safety: it *assumes* the
  tombstone reached all replicas within the grace window (via write
  availability, hints, read repair, or an operator-run repair). A replica that
  was down longer than `gc_grace_seconds` can resurrect data.
- **`repair`** — the safe mode, and the default for repairable keyspaces
  (`get_default_tombstone_gc_mode`, `tombstone_gc.cc:325`). `gc_before =
  last_repair_time(range) − propagation_delay` (delay defaults to 1 hour,
  `tombstone_gc_options.hh:19`). Repair time is looked up per token in a
  per-table **repair history map** — an interval map over token ranges,
  updated copy-on-write. If the key's range was never repaired, `gc_before =
  time_point::min()`, i.e. **never purge** — the safe default. Special cases:
  RF=1 tables behave like `immediate` (nothing to repair,
  `tombstone_gc.cc:188`), and group0 tables use the group0 state-id refresh
  time (`get_gc_before_for_group0`).
- **`immediate`** — `gc_before = query_time`; for TWCS-style workloads with no
  overwrites (purging a tombstone the moment it appears in compaction).
- **`disabled`** — `gc_before = time_point::min()`; never purge.

The repair history is populated at the end of a repair
(`repair_update_system_table_handler` → `update_repair_time`,
`repair/row_level.cc:2659`) and reloaded at boot in a batch
(`repair_service::load_history` → `batch_update_repair_time`,
`row_level.cc:3930`); tablet repairs go through
`insert_pending_repair_time_update` (`replica/tablets.cc:879`) and
`flush_pending_repair_time_update` (`tombstone_gc.cc:273`), which flushes
together with a commitlog cleanup record. It is persisted in
`system.repair_history`.

**The commitlog clamp.** Every mode's result except the trivially minimal
ones (`disabled`, and `no_gc`) passes through `check_min`
(`tombstone_gc.cc:148`), which caps `gc_before` at the commitlog's
`min_gc_time` for the table (wired in `main.cc:2122`). This prevents purging
a tombstone while writes it shadows could still be **replayed from the
commitlog** after a crash — replay would resurrect them once the tombstone is
gone. This is the LSM engine's own local no-resurrection-on-recovery guard,
directly analogous to logstor's Gate 2 (logstor recovery replays *everything*,
which is why its Gate 2 is a bigger deal).

For per-sstable decisions there is a range variant,
`get_gc_before_for_range(schema, token_range, query_time)`
(`tombstone_gc.cc:61`), returning `{min_gc_before, max_gc_before,
knows_entire_range}` — used for estimating droppable tombstones and
whole-sstable expiry (below). `cheap_to_get_gc_before()`
(`tombstone_gc.cc:141`) tells the compactor whether Gate 1 is cheap for this
table (everything except `repair` mode); the compactor orders the two gates
accordingly to avoid needless bloom-filter checks
(`mutation_compactor.hh:284`).

#### Gate 2 — no-resurrection (`max_purgeable`)

Dropping a tombstone must not un-hide older data in sstables/memtables that
are **not** part of this compaction. For each partition key, compaction
computes a `max_purgeable` bound (`get_max_purgeable_timestamp`,
`compaction/compaction.cc:197`): the minimum **live-data timestamp** among
data sources outside the compacting set that may contain the key:

- **memtables**: `min_memtable_live_timestamp()`, consulted only if the
  memtable actually contains the key and holds data no newer than the
  compacting sstables' max timestamp (`compaction.cc:240`), and unless
  `skip_memtable_for_tombstone_gc()` says otherwise;
- **overlapping non-compacting sstables**: selected by token overlap
  (`sstable_set::incremental_selector`), filtered by a **bloom-filter** key
  check; each contributes its `min_live_timestamp` from the sstable's
  extended timestamp stats (falling back to `min_timestamp`);
- **compacted-undeleted sstables** (sstables whose deletion failed after
  compaction) are included too.

A tombstone is then purgeable iff `tombstone.timestamp <
max_purgeable.timestamp` — no possibly-shadowed data older than the tombstone
exists elsewhere (`max_purgeable::can_purge`,
`compaction/compaction_garbage_collector.hh:35`, `compaction.cc:95`). Two
refinements:

- **Shadowable tombstones** (from materialized-view updates) compare against
  the minimum live *row-marker* timestamp instead of the minimum live cell
  timestamp (`compaction.cc:217`; issue #20424). Only live data counts:
  purgeable tombstones that shadow *dead* data outside the compacting set may
  still be purged (issue #20423 explains why dead memtable data does not
  count).
- An **expiry-threshold elision** exists in `max_purgeable`
  (`compaction_garbage_collector.hh:82`): if the tombstone was already expired
  when the outside data sources were created, the timestamp check can be
  skipped. Compaction never sets it; only the row cache's read-time path does
  (`memtable_list::get_max_purgeable`, `replica/database.cc:2129`).

The per-key result is cached per partition in the compactor
(`_max_purgeable_regular/_shadowable`, `mutation_compactor.hh:172`). Purge
failures are attributed in `tombstone_purge_stats` (blocked by memtable vs.
by other sstables — `mutation_compactor.hh:308`), exported as metrics. A
major compaction can restrict the check to the compacting set
(`compaction_descriptor::gc_check_only_compacting_sstables`, set from the
REST parameter `consider_only_existing_data`; it also disables the commitlog
clamp) for setups that guarantee non-overlap.

#### TTL expiry

TTL is entirely **lazy** — nothing is scheduled when data is written:

- A live cell optionally carries `(expiry, ttl)` in its on-disk/in-memory
  representation (`mutation/atomic_cell.hh:70`); a `row_marker` carries the
  same (`mutation/mutation_partition.hh:550`). The logical **deletion time of
  an expired cell is `expiry − ttl`** — i.e. as if it was deleted when
  written (`atomic_cell.hh:271`, `mutation_partition.hh:604`), so the grace
  period effectively counts from the write.
- **Reads hide expired data on the fly**: `cell.has_expired(query_time)`
  (`expiry <= now`) makes the cell behave as dead during query-time
  compaction; no storage is touched.
- **Compaction converts and purges** in `row::compact_and_expire`
  (`mutation/mutation_partition.cc:1621`): an expired cell is either purged
  immediately (if both gates pass for `tombstone{timestamp, expiry − ttl}`) or
  **converted in place to a dead cell** — `atomic_cell::make_dead(timestamp,
  deletion_time)` — dropping the value payload (`mutation_partition.cc:1654`).
  The row marker does the same in `row_marker::compact_and_expire`
  (`mutation_partition.cc:1801`): on expiry it becomes `_expiry -= _ttl; _ttl
  = dead`, then is purged once `_expiry < gc_before && can_gc(...)`. Logstor's
  "expired record ⇒ tombstone with `deletion_time = expiry − ttl`, drop or
  slim-rewrite" rule mirrors exactly this.

#### GC-driven compaction triggering

Purging only happens if compaction visits the data, so the engine has
dedicated triggers beyond size/tiering heuristics:

- **Per-sstable statistics** written at sstable creation: `min_timestamp` /
  `max_timestamp`, `max_local_deletion_time` (max deletion/expiry time of any
  tombstone or cell), and `estimated_tombstone_drop_time` — a histogram of
  tombstone deletion times (`sstables/types.hh:317`).
- **Droppable-ratio trigger**: `sstable::estimate_droppable_tombstone_ratio`
  (`sstables/sstables.cc:1569`) integrates that histogram up to the
  optimistic `max_gc_before` of the sstable's token range, over the estimated
  cell count. Strategies (`worth_dropping_tombstones`,
  `compaction/compaction_strategy.cc:61`) submit a single-sstable "tombstone
  compaction" when the ratio reaches `tombstone_threshold` (default 0.2) —
  but only if the sstable is older than `tombstone_compaction_interval`
  (default 1 day) to avoid repeatedly rewriting an sstable whose tombstones
  cannot be purged yet. STCS, TWCS and LCS use this as a fallback when no
  regular compaction is pending (`size_tiered_compaction_strategy.cc:269`,
  `time_window_compaction_strategy.cc:412`,
  `leveled_compaction_strategy.cc:54`); ICS has a per-run variant of its own
  (`incremental_compaction_strategy.cc:272`).
- **Whole-sstable drop**: `get_fully_expired_sstables`
  (`compaction/compaction.cc:2300`) finds compacting sstables with
  `max_local_deletion_time < gc_before` — everything in them is expired
  tombstones/TTL data past grace — and whose timestamp range cannot shadow
  older data in overlapping sstables. These are **deleted outright without
  rewriting** (TWCS relies on this heavily for time-series workloads).

The equivalents in this design: logstor's per-segment `gc_bytes` /
`min_gc_time` play the role of the sstable stats metadata; the sweep plus the
expiry-driven candidate boost play the role of the droppable-ratio trigger;
and compaction of a segment whose records are all droppable naturally frees it
without rewriting anything, which is the segment-level analog of the
whole-sstable drop.

## Gap analysis

| Capability | Traditional (LSM) | Logstor today |
|---|---|---|
| Identify a record as tombstone/expiring cheaply | per-cell flags + sstable metadata | none — header has no kind/deletion_time; must deserialize |
| Grace gate (`gc_before`, repair-aware) | `tombstone_gc_state` | not wired in |
| No-resurrection gate | `max_purgeable` overlap check | N/A locally (one version per key), but stale physical copies + recovery-by-scan create a different resurrection risk |
| Durable record that data is dead | sstable deletion; commitlog cleanup records | erase watermarks per token range (*recovery1*); nothing per key |
| Physically drop a tombstone | compaction purge | impossible — always rewritten |
| TTL expiry -> reclaim | lazy convert + purge | expired rows hidden at read but kept forever |
| GC-driven compaction trigger | droppable-ratio + interval | utilization-based only |

## Design overview

- **GC metadata** (record kind, deletion/expiry times) is computed on the write
  path and stored in the on-disk record header and (compactly) in the index
  entry, so GC decisions never decode the record value.
- A tombstone may be physically dropped only when **two gates** pass:
  **Gate 1 (grace)** — cross-node safety, same rule as traditional storage; and
  **Gate 2 (local no-resurrection)** — recovery-by-scan cannot re-insert an
  older physical copy of the key.
- Gate 2 is implemented with a **reclaim frontier** `F` over segment sequences
  plus monotone **horizons** that bound where stale copies can physically
  live; the [purge journal](#gate-2-candidate-2--purge-journal) can later
  decouple drops from frontier progress.
- Dropping is done by two cooperating mechanisms: a background **GC sweep**
  over the in-memory index (reclaims index memory promptly and converts the
  record into dead bytes for normal compaction), and an opportunistic **drop in
  compaction** (skip-instead-of-rewrite).
- **TTL** unifies with tombstone GC: an expired record is treated as a
  tombstone with `deletion_time = expiry − ttl`; compaction either drops it
  outright (if both gates already pass) or rewrites it as a **slim tombstone**.
- **Frontier liveness** requires compacting the oldest-seq segments at a
  bounded rate, independent of utilization.

## Design

### Record and index GC metadata

At write time (`logstor::write` holds the full `mutation`, so this is cheap and
allocation-free) classify the record:

- `kind`: `live`, `expiring`, or `deletion`;
- for `deletion`: `deletion_time` (the partition tombstone's deletion time);
- for `expiring`: `expiry` and `deletion_time` (see [TTL](#ttl-expiry) for the
  exact rules when components carry different TTLs).

**Local-time clamp.** For GC-gating purposes the stored time is clamped to
the node-local wall clock at insert:
`gc_time = max(deletion_time_or_expiry, gc_clock::now())`. This makes the
grace period count from when *this node learned* of the delete, which is
what Gate 2 needs under client-backdated timestamps (`USING TIMESTAMP`) — see
the threat analysis below. It only ever delays GC, never advances it. The
clamped `gc_time` is what goes to disk and what recovery rebuilds the index
from: re-clamping at boot would restart the grace period after every
restart. The unclamped `deletion_time` is stored too (it is what a converted
tombstone carries semantically), and a re-clamp *during* the tombstone's life
is part of the Gate 2 mechanism (below).

**On-disk.** `record_header` is logstor's own encoding in `ondisk.hh`
(`write_record_header` / `read_record_header`, a 32-byte fixed part
followed by the key); there is no IDL for it. Append a 16-byte fixed block:
`kind` (1 byte, 3 padding), `gc_time`, `deletion_time`, `expiry` (32-bit
seconds each, `gc_clock::as_int32`, as the sstable format stores deletion
times; zero for `live` records).
Per the decision taken in `logstor_recovery1`, the format version stays 1
while logstor is in development: a store written before the change is not
readable after it, and nothing streams between the two. On a ~1.3 KB record
this is +1.2%; if that matters, `kind` fits in the upper 16 bits of
`record_frame_header::key_size` (keys are at most 65535 bytes) and the two semantic
times are only needed for `expiring` records, which brings it to 8 bytes at
the price of a kind-dependent layout.

**In-memory.** The layouts today are `index_entry` = `record_location` (12
bytes) + `timestamp` (8) = 24 bytes with 4 bytes of padding, and
`primary_index_entry` = key (a 16-byte `dht::token` plus the 16-byte hash)
+ `index_entry` (24) + cache slot (8) + a flags byte = 65, padded to 72. So:

- `gc_time` (32-bit, `gc_clock::as_int32`) goes into `index_entry`'s
  padding — 24 bytes stay 24;
- `kind` (2 bits) goes into `primary_index_entry`'s flags byte (5 spare
  bits) — 72 bytes stay 72.

`insert` and `update_record_location` take the GC info as a small struct
next to the `index_entry`, and a `static_assert` pins both sizes. Per-key
memory growth: zero.

One time field suffices for both kinds because using `expiry` in the Gate 1
comparison is *conservative* for expiring records: `deletion_time = expiry −
ttl <= expiry`, so `expiry < gc_before ⟹ deletion_time < gc_before`. The
sweep therefore waits "grace after expiry" instead of "grace after logical
deletion" — safe, and the exact `deletion_time` remains available from the
on-disk header when compaction converts the record.

**Per-segment accounting** (`segment_descriptor` in `compaction.hh`): add

- `seq` — the segment's sequence, set at `get_segment` and by recovery from
  the scanned headers. Needed by the frontier, and also by the
  equal-timestamp fix planned in `logstor_discard.md`, so it lands first as
  shared groundwork;
- `gc_bytes` — bytes of live `deletion` + `expiring` records in the segment;
- `min_gc_time` — minimum `gc_time` among records written to the segment.
  Not adjusted on free (a conservative lower bound is fine — it is only a
  triggering heuristic, never a correctness input);
- `has_stale` — whether the segment holds any stale copy: set by
  `on_free_record` and by the three **loser sites** (a superseded `insert`,
  a failed `update_record_location` in the separator and in compaction).
  Today a loser is never accounted at all, so `free_space` alone cannot tell
  a fully-live segment; this flag is what the effective-frontier refinement
  and the skip-fully-live scan reduction need.

The accounting hooks need the record kind on the free path:
`space_accounting_subscriber::on_add_record/on_free_record` gain the kind
(the index has the entry at every call site — the disposer, the overwrite
branch of `insert`, `update_record_location`).

### The two-gate purge rule

A `deletion` record (including an expired record — see TTL below) may be
physically dropped (erased from the index and freed) only if **both** gates
pass:

- **Gate 1 — Grace (cross-node safety).** `gc_time < gc_before`.
  - Stage 1: `timeout` mode only, `gc_before = now − gc_grace_seconds` from the
    table schema.
  - Later stage: full parity via the table's `tombstone_gc_state`
    (`get_gc_before_for_key`), supporting `repair`/`immediate`/`disabled` and
    per-token-range `gc_before`.
- **Gate 2 — Local no-resurrection.** Guarantees that dropping the tombstone
  cannot cause recovery to resurrect an older physical copy of the same key.

After both gates pass, the drop itself is `index.erase(key, loc)`: the
entry's disposer already calls `on_free_record` (so the segment's
`record_bytes` accounting moves towards zero for `free_segment`) and evicts
any cached mutation. It is synchronous — no yield between the gate check and
the erase. The freed bytes are then reclaimed by normal compaction.

If, after a purge, a write for the key arrives with an older timestamp, it
wins (there is nothing to shadow it). This matches traditional post-purge LWW
semantics and is inherent to tombstone GC, not a defect of this design.

### Gate 2 — local no-resurrection

#### Threat model

Recovery rebuilds the index by scanning **all scannable segments** and keeping
the max-timestamp record per key. If tombstone `R` of key `K` is physically
dropped while an older *stale copy* `R'` of `K` (freed, or never accounted)
is still *scannable*, recovery re-inserts `R'` — a resurrection. "Scannable"
includes segments freed by compaction but not yet reused (their content is
intact) — only reuse (seq-mismatch stops the scan) or explicit header
invalidation makes a segment unscannable. (*recovery1*'s erase watermarks
make some scannable records unrecoverable; this design conservatively keeps
reasoning about scannability.)

Every path that writes a record and then loses a liveness check produces
stale copies, and they can land in **arbitrarily high-seq segments**:

1. **Normal overwrite** — the previous record is freed in place. Its segment
   may have a *higher* seq than the overwriting record's segment (the separator
   may have just moved it into a fresh full segment).
2. **Separator** — copies *every* record of a mixed buffer into a fresh full
   segment, including records overwritten since; the copy of an overwritten
   record is stale on arrival (`update_record_location` fails).
3. **Compaction / split compaction** — write-then-check; losers left
   unaccounted in a fresh output segment. The recovery path that separates
   a mixed segment's live records is the same window.
4. **Late/backdated direct writes** — a write that loses LWW to the live entry
   (hinted-handoff replay, retries, client `USING TIMESTAMP`) is written
   physically into the active segment — at **any** time, including long after
   the key became a tombstone and passed grace — and the separator then
   copies it once more (source 2).
5. **Streaming / restore ingest** — a peer's segment is loaded
   (`load_segment`); records that lose to the local index become
   stale copies in a fresh-seq local segment, at any time.
6. **In-flight copies** — a separator/compaction copy of a pre-delete value
   may *land* (complete its write) after the delete was applied to the index,
   and — with backdated deletes — even after the delete passes Gate 1.

Sources 4–6 show that the simple rule "all stale copies of `K` predate the
moment the tombstone passed grace" is **not** sufficient on its own. The
design below closes each hole explicitly.

#### Mechanism

Per-shard state (all in `segment_manager`; segment seqs are per-shard):

- **Scannable set / frontier `F`.** Track the seqs of all scannable segments
  (an ordered structure over `segment_descriptor::seq`).
  `F = min(scannable seqs) − 1` (`F = max allocated seq` when empty).
  To make freed segments leave the set, **invalidate the on-disk header when a
  segment is freed** — the 4K-zero write `discard_group_segments` already
  does, files are `O_DSYNC` so completion ⇒ durable. `free_segment` is
  synchronous and `noexcept` (it runs from `segment_ref`'s release path), so
  it hands the slot to an **invalidation queue** instead of to
  `_free_segments`; a background fiber zeroes the first block, then removes
  the seq from the scannable set and pushes the slot onto the free list.
  Invalidating *before* the slot is reusable keeps the invalidation write
  from racing a new incarnation's header, and gives the simplest invariant:
  a slot is scannable iff it is allocated with a valid header or queued for
  invalidation. Slots recovery puts on the free list and that still carry a
  valid header are invalidated the same way at boot, in the background and
  rate-limited; until then their seqs stay in the set. The erase path's
  zeroing merges into this queue.
- **Snapshot ring.** Periodically (config: `gc_snapshot_period`, default
  ~1 min) append `(t = gc_clock::now(), H_t = max allocated seq)`. The ring is
  bounded (e.g. 64 entries) by merging the oldest entries. After recovery the
  ring starts empty; the first snapshot's `H` covers everything on disk at
  boot. In `repair` mode a snapshot also keeps a reference to the table's
  repair-history map as of `t` (it is copy-on-write), so that `gc_before(t)`
  can be evaluated later.
- **Re-clamp on a loser** (replaces the *external horizon* `H_ext` of the
  previous revision). Whenever an index call for `K` loses to a live entry
  whose `kind == deletion` — a superseded `insert` (user write, or a record
  of a streamed/restored segment inserted by `load_segment`), or a failed
  `update_record_location` (separator, compaction, split, recovery
  separation) — set the winning entry's
  `gc_time = max(gc_time, gc_clock::now())`. The loser's bytes are in a
  segment with `seq <= max allocated seq at that moment`, and the re-clamp
  makes every qualifying snapshot younger than that moment, which is all
  the drop rule needs (proof below). This is per entry, so a burst of late
  writes to deleted keys delays only those keys, whereas `H_ext` stalled
  every drop on the shard until `F` caught up with the present.

**Drop rule.** Tombstone `T` (key `K`, clamped `gc_time`) may be dropped iff
there exists a snapshot `(t, H_t)` with:

```
gc_time < gc_before(t)          (Gate 1 evaluated at snapshot time t)
and  F >= H_t                   (everything scannable at snapshot time is gone)
```

In `timeout` mode `gc_before(t) = t − gc_grace(table)`, so this reduces to a
per-table **purge bound**: `purge_before = max{ t − grace : H_t <= F }`,
recomputed when `F` advances; a tombstone is droppable iff
`gc_time < purge_before`. One comparison per record.

The argument needs `gc_before(t) <= t` in every mode, which holds: a repair
time is in the past, `immediate` uses `t` itself.

#### Correctness argument

Claim: at drop time, every scannable stale copy `R'` of `K` lies in a
segment with `seq <= H_t <= F`, hence is *not* scannable — contradiction; so
no such copy exists and recovery cannot resurrect `K`.

Let `i` be the time of `T`'s insert and `max_seq(x)` the highest seq
allocated by time `x`. Case analysis on `R'`'s index call, relative to `i`:

- **Lost before `i`** (an older value overwritten by `T`, or a loser to that
  value). `R'` was physically written before it lost, so
  `seq(R') <= max_seq(i)`. The clamp gives `gc_time >= i`; the chosen
  snapshot has `gc_before(t) > gc_time`, hence `t > i` and
  `H_t = max_seq(t) >= max_seq(i) >= seq(R')`. (Without the clamp a
  backdated `deletion_time` would let a snapshot *older than the insert*
  satisfy Gate 1 — the hole in sources 4–6 above.)
- **Lost after `i` to `T` itself** — a late write, a streamed record, or a
  copy of the pre-delete value that landed after `i`. The losing call ran at
  some time `j > i` with `seq(R') <= max_seq(j)`, saw a `deletion`-kind
  winner and re-clamped `gc_time >= j`, so every qualifying snapshot has
  `t > j` and `H_t >= seq(R')`. The drop check and the erase run in one
  reactor task, as does the losing call with its re-clamp, so one of the two
  orders holds: the re-clamp is visible to the check (drop blocked until `F`
  catches up), or the erase ran first. In the latter case, if the call is an
  `insert`, it finds no entry and `R'` *wins* — post-purge LWW, accepted. If
  it is `update_record_location`, the copier still holds a reference on
  `R'`'s **source** segment (P3: publish before free), and the source holds a
  pre-delete record of `K`, so it has `seq <= max_seq(i) <= H_t <= F` — but
  `F >= H_t` says that segment was already freed and invalidated, which
  cannot happen while it is referenced. So a copy of a pre-delete value
  cannot land after the drop: this is where the frontier relies on the
  copiers holding their sources until the copies are published.
- **Lost after `i` to a newer non-tombstone `W`.** Then `T` is no longer the
  live entry and will never be dropped (it was itself freed). If `W` is
  later deleted by a new tombstone `T'`, all these copies lost before `T'`'s
  insert — the first case applies to `T'`.

**Copies of the tombstone itself.** A relocation of `T` (compaction or
separator) started before the drop may land after it; its
`update_record_location` finds no entry and, today, would leave a scannable
orphan copy of `T` that recovery re-inserts. That is harmless in itself —
the tombstone comes back and is dropped again — except when a post-purge
write older than `T` arrived in between: the runtime shows that write, the
restarted node shows the tombstone. To keep the runtime and recovery in
agreement, **a copier whose update finds no entry re-inserts a
`deletion`-kind copy** (it has the header, hence the kind); the re-inserted
tombstone shadows the late write exactly as recovery would, and the sweep
drops it again later. A `live`-kind orphan cannot occur under this
mechanism (previous case) and is asserted against.

**Equal timestamps.** Duplicates of `T` at the same timestamp resolve by
`(segment_seq, offset)` as in normal recovery and can at worst re-insert the
tombstone. A same-timestamp *overwrite* of `T` by a live value is the gap
`logstor_discard.md` documents, independent of GC.

Crash consistency: the drop becomes durable only when `T`'s segment stops
being scannable (invalidated or reused) — which happens strictly after the
drop, which happens strictly after all `seq <= H_t` segments were durably
invalidated (that is what `F >= H_t` means, and `F` only advances on
completed `O_DSYNC` invalidation writes). A crash at any intermediate point
re-inserts `T` at recovery — benign; it will be re-dropped. After recovery
the snapshot ring is empty, so GC resumes only after a full frontier pass —
an accepted cost of not persisting GC state (see Alternatives).

#### Frontier liveness — forced compaction

`F` advances only when the lowest-seq scannable segment disappears. Cold,
fully-utilized segments are never selected by utilization-based compaction and
would block GC forever. Add a background **frontier task**: while there is
pending droppable work (per-table `pending_gc_bytes` estimate, from segment
`gc_bytes`/`min_gc_time` and the purge bound) and `F` is below the needed
horizon, rewrite the lowest-seq scannable segments at a bounded rate (config:
`gc_frontier_bandwidth`, bytes/s). Notes:

- The task locates a segment's group via `segment_descriptor::owner` and
  reuses the normal compaction path (rewriting a fully-live segment simply
  copies it forward with a fresh seq). A segment in the free list or the
  invalidation queue is not rewritten; it leaves the set by invalidation.
- Mixed segments awaiting separation can hold the frontier back; nudge them
  with `flush_separator(seq)`.
- Worst-case write amplification is one full-disk rewrite per GC cycle, i.e.
  `disk_size / max(grace, disk_size / gc_frontier_bandwidth)` of forced
  writes — small for the default 10-day grace, large for short grace on a
  cold disk, where the bandwidth knob stretches drop latency instead. The
  **effective frontier** (Alternatives) removes the cost for fully-live
  segments; the [purge journal](#gate-2-candidate-2--purge-journal) removes
  it from the drop latency altogether.
- A time-series workload (inserts with TTL, no overwrites) is the best case:
  the oldest segments expire whole, passing them is a free-without-rewrite.

### Drop mechanisms

Two cooperating consumers of the drop rule:

1. **GC sweep (primary).** A periodic per-shard fiber iterates each logstor
   table's `primary_index` gently (chunked, re-lookup after yield — same
   pattern as `erase_range_gently`). For entries with
   `kind != live && gc_time < purge_before`: erase + count. This reclaims
   **index memory** promptly and turns the record into dead bytes that
   utilization-based compaction picks up naturally — no GC-aware segment
   selection is needed for basic operation. Existing races are already
   handled: compaction's `update_record_location` fails for erased keys (and
   re-inserts a tombstone copy, above); in-flight reads hold an `index_read_guard`,
   and segment freeing already awaits pending reads.
2. **Drop in compaction (opportunistic).** In `do_compaction`'s header
   callback, a record that `is_record_alive` but passes the drop rule (using
   the on-disk header's kind/gc_time) is erased and *not* rewritten
   (`want_data::no`). The check-and-erase is synchronous within the callback.
   This avoids pointlessly copying droppable tombstones forward during normal
   and frontier compaction.

### TTL expiry

**Classification at write time.** A record is `expiring` iff its whole
liveness is expiring: the row marker is expiring and every live cell is
expiring. Then `expiry = max(expiry_i)` over marker and cells (the moment
nothing live remains) and `deletion_time = max(expiry_i − ttl_i)` (for the
uniform single-`INSERT ... USING TTL` case these are the marker's values).
Records with mixed liveness (some non-expiring component) are `live`.
Per-component expiry inside a partially-expiring record is handled by query
compaction at read time, as today; record-level GC only acts when the whole
record is gone.

**Reads must already hide expired rows** via the normal query compaction path
(the record is deserialized to a `mutation` and flows through the standard
reader). This is verified by tests before any physical reclamation ships.

**Unified convert-or-drop.** An expired record *is* a tombstone with
`deletion_time = expiry − ttl` — commonly already past grace at expiry (when
`ttl >= gc_grace`). So:

- The **sweep** drops fully-expired records directly using the conservative
  index rule (`gc_time = expiry`; `expiry < gc_before ⟹ deletion_time <
  gc_before`), i.e. grace-after-expiry. No conversion step, no disk read.
- **Compaction**, when it encounters an expired `expiring` record (header
  `expiry <= now`):
  - if the drop rule passes (using the header's exact `deletion_time`) — drop
    outright;
  - otherwise rewrite it as a **slim tombstone**: a `deletion`-kind record
    with the same key and record timestamp, `deletion_time = expiry − ttl`
    from the header, and the value payload removed. This reclaims the payload
    bytes immediately; the slim tombstone is then dropped later by the normal
    rule. The index entry's kind/gc_time are updated with the new location;
    because the record *content* changed, the cached mutation is evicted
    (unlike plain `update_record_location`, whose keep-the-cache assumption
    holds only for identical data).

**Slim tombstone representation.** Preferred: a record with `value_size == 0`
whose meaning is synthesized from the header (`key`, `timestamp`,
`deletion_time`) — the read path constructs `mutation(schema, key)` with a
partition tombstone. This keeps compaction schema-free and, as a follow-up,
lets *user deletes* be written slim from the start (a delete write shrinks to
header-only). Fallback, if the read-path special case proves invasive: store a
pure-tombstone record value (requires plumbing `schema_ptr`
into compaction via `logstor_group`).

### GC-driven compaction triggering

Basic tombstone GC does not need GC-aware selection (the sweep frees index
entries and bytes; utilization does the rest). Two triggers are still needed:

- **Frontier advancement** (above) — oldest-seq segments, rate-limited.
- **Expired-payload reclamation**: segments whose
  `gc_bytes / record_bytes` ratio is high and whose `min_gc_time` is past
  `purge_before` (or whose expiring payload is past expiry) are added to the
  compaction candidates with a score boost, analogous to the droppable-ratio
  trigger + `tombstone_compaction_interval` in traditional storage. This pulls
  forward the slim-tombstone conversion for TTL-heavy but otherwise "full"
  segments.

### Observability

Counters/gauges (per shard): tombstones dropped (sweep / compaction), expired
records dropped/converted, bytes reclaimed by GC, current `F`, max allocated
seq, invalidation queue depth, re-clamps (by loser site), orphan tombstone
copies re-inserted, per-table purge bound age (`now − purge_before`),
pending `gc_bytes`, frontier-task bytes rewritten, sweep
iterations/duration. `purge bound age` and `max_seq − F` are the two health
signals that show GC falling behind.

### Configuration

- `logstor_gc_enabled` (bool, default on when the feature ships; off until
  then).
- `gc_snapshot_period` (default ~1 min).
- `gc_sweep_period` (default ~10 min; the sweep is cheap — in-memory).
- `gc_frontier_bandwidth` (bytes/s per shard; bounds forced-compaction write
  amplification).
- Gate 1 uses the existing per-table schema (`gc_grace_seconds`,
  `tombstone_gc` mode); no new per-table options.

## Gate 2 candidate 2 — purge journal

The per-key form of `logstor_recovery1`'s erase watermark. Where the frontier
proves that no stale copy of `K` is scannable by *destroying* everything old
enough to hold one, the journal makes stale copies **unrecoverable by
filtering**: a durable record `(table, key, S)` tells recovery to drop every
record of `K` found in a chunk with `segment_seq <= S`, exactly as a
watermark does for a token range. Gate 1, the metadata, the two-gate rule and
the TTL design are unchanged; the drop happens as soon as Gate 1 passes, and
frontier progress only bounds the journal's size.

### Cycle

A per-shard fiber, periodically:

1. **Switch the active segment.** From here on every user write lands in a
   segment with a seq above `S := (new active seq) − 1`. One pool pop, as the
   erase protocol does after every erase.
2. **Drain.** `await_pending_writes()` — every write to a segment `<= S` has
   made its index call (the buffer gate covers the inserts) — and
   `await_relocations()` (a per-shard phased barrier held over every
   copy window: from the copier's liveness check to its
   `update_record_location`; see below). Now every physical record with
   `seq <= S` has had its index call, and every copy in flight has been
   published or has failed.
3. **Mark.** Walk the index gently; entries with `kind != live && gc_time <
   gc_before(now)` are candidates `(key, location)`, in chunks.
4. **Erase.** For each candidate of the chunk, synchronously: re-check the
   whole drop condition — the entry still matches `(key, location)` *and*
   Gate 1 still holds for its current `gc_time` (a loser may have re-clamped
   it since the mark) — and `index.erase(key, location)`. Collect the
   erased keys.
5. **Journal.** Append the chunk's `(table, key, S)` entries to the per-shard
   journal file and wait for the write (`O_DSYNC`). Only then move to the
   next chunk.

The erase comes before the append on purpose. In the other order a late
write that loses to `T` between the append and the erase would re-clamp `T`,
the erase would rightly skip it, and the journal would still say that `T`'s
record is dead at the next recovery — while the runtime keeps it. Erasing
first, the append only ever records drops that happened; a crash between
the two brings the tombstone back at recovery (its record has `seq <= S`
and no entry filters it), which is benign.

Steps 3–5 may take as long as they like: what step 2 established — that the
index is the truth for every record with `seq <= S` — stays true, because
sequences are monotone and nothing writes into a segment `<= S` again.

### Why it is safe

Let `T` of `K` be erased in step 4 with journal entry `(K, S)` durable in
step 5. Any stale copy `R'` of `K` either has `seq <= S` — filtered at
recovery — or `seq > S`. A record with `seq > S` was written after step 1, so
its index call is either a user write or a streamed record — if it loses to
`T` before the erase it re-clamps `T` and step 4 skips `T` (no journal entry,
`T` stays); if it runs after the erase it wins, post-purge LWW, consistent
with recovery since it is not filtered — or a relocation copy. A relocation
copy with `seq > S` that is stale is a copy of a record that was live at its
liveness check and lost before its update: if it lost to `T`, the update
re-clamped `T` and step 4 skips it; if the update ran after the erase, the
copy started after the drain of step 2, so its source record was live after
the drain — the only live record of `K` then is `T` itself, and the copy is
a tombstone copy that the copier re-inserts (the orphan rule above). A copy
of a *pre-delete* value in flight across the drop is excluded by the drain:
its window started before step 2 and was awaited there.

The separator is the one copier whose window is long: a record sits in a
group's buffer until the buffer flushes, possibly for days on a cold group,
and today the buffer copies records that are already dead. The journal
therefore needs the **liveness check at flush**: before a separator buffer
is written out, records that are no longer alive at their source location
are dropped from it, so that the copy window starts at the flush and is
bounded by one segment write — which is what makes `await_relocations()`
cheap. This is a change to `separator_buffer`'s flush and an improvement in
its own right (overwritten-before-separation records are no longer copied).

A crash between steps 4 and 5 re-inserts the erased tombstones at recovery,
with every stale copy dominated as before the cycle. What that window does
not preserve is a write older than `T` that arrived after the erase and won
at runtime: after the restart the returned tombstone shadows it again. That
is the post-purge-LWW class of write within one I/O of the purge, and it is
accepted.

### Retirement and the file

An entry `(K, S)` is dead weight once no scannable record of `K` with
`seq <= S` exists. Two ways to know:

- **Frontier**: once `F >= S` every segment `<= S` is unscannable. The
  scannable set and invalidate-on-free are needed for this; the forced
  compaction task is not on the drop's critical path anymore and runs only
  when the journal exceeds its cap (`gc_journal_max_entries`), so cold
  segments are rewritten only when the journal actually needs them gone.
- **Retirement scan**: a header-only read of every scannable segment;
  entries without a sighting retire. No barrier is needed — a new record of
  `K` with `seq <= S` can never appear — which is what makes this scan
  trivially safe where mark-and-sweep's needs a write barrier. It depends on
  *recovery1*'s body CRC to read open and reused segments safely.

Entries are appended in `S` order, so retirement by frontier is a head
pointer; the file is a per-shard append-only log next to the segment files
(`ls_{shard}-Purge.db`: a header with magic and version, entries of
`table id, token, key hash, S` with a CRC per batch), loaded into a hash set
at recovery before the scan and probed once per scanned record in
`scan_slot`, next to the watermark check. Its maximum `S` feeds the recovered
`_next_segment_seq` like the watermarks' does (Invariant Q of
`logstor_discard.md`). Recovery-time memory is one hash entry per unretired
purge — the reason for the cap; with the frontier retiring entries this is
bounded by the frontier lag, and with the scan by one scan period.

### What it changes in the comparison

Drop latency becomes `grace + one cycle` regardless of the disk's age
structure; no forced rewriting is on the critical path; nothing touches the
write hot path (the phaser is held by copiers only). Its costs are the
journal — a new durable artifact, an append per drop, a hash set at recovery
— the liveness check at separator flush, and the relocation phaser. The
frontier's scannable set and invalidation stay as the retirement engine
(or are replaced by the retirement scan).

## Gate 2 candidate 3 — mark-and-sweep verification

An alternative implementation of Gate 2 only. Gate 1, the GC metadata, the
two-gate purge rule, post-purge LWW semantics, and the TTL design are
unchanged; what differs is *how* we establish that no stale physical copy of
the key can be resurrected by recovery. Kept for the record; the purge
journal reaches the same "no forced rewriting" at a lower cost, and this
section's corrections are noted inline.

### Principle

The frontier proves no stale copy of `K` is scannable by **destroying
history**: a tombstone may be dropped only after every segment old enough to
hold a stale copy has durably left the scannable set (`F >= H_t`).
That is an inductive invariant, and its price is forced compaction — cold
segments must be *rewritten* for `F` to pass them.

Mark-and-sweep proves the same thing by **direct inspection**: scan every
scannable segment once, verify that no older physical record of the candidate
key exists anywhere, and keep that fact true until the drop with a **write
barrier**. The price is a read-only scan of the disk per GC cycle plus a
candidate table in memory — no forced rewriting, no invalidation writes.

The "ensure no old records can be written past some point" requirement does
**not** need a write fence. Backdated writes are legitimate at any time
(client `USING TIMESTAMP`, hints, streaming), so they cannot be rejected.
Instead they are *detected*: the physical bytes of every record land before
its index call, every call that makes the index point at a record goes
through exactly two functions (`primary_index::insert` and
`primary_index::update_record_location`), and while a cycle is active each
such call for a candidate key simply **cancels the candidate** — the drop is
deferred to a later cycle. This is the snapshot-at-the-beginning barrier of
concurrent GC literature, applied to physical record copies.

### GC cycle

A per-shard background fiber runs cycles:

1. **Arm the barrier** (below) and record `S0 = max allocated seq`.
2. **Roll** — corrected in this review: no flush is needed. Every record
   whose index call predates arming had its bytes durable before that call,
   in a segment whose seq was issued before arming, hence `<= S0`. The
   separator buffers hold copies that are not physical yet; their updates
   run after arming and are caught by the barrier. What remains is that the
   sweep must read every scannable segment `<= S0` including the **open
   active segment**, which keeps receiving appends: with *recovery1*'s body
   CRC, a chunk the scan reads while it is being written fails the check and
   the scan stops there; the records in it have not made their index calls
   (bytes first), so the barrier covers them. Without the CRC this scan could
   deliver stale bytes of a reused slot (S6 of `logstor_discard.md`), so the
   CRC is a hard dependency. Force-closing the active segment at arming
   (one pool pop) is the simpler alternative if scanning an open segment
   proves awkward.
3. **Mark**: iterate the `primary_index` gently (chunked, the
   `erase_range_gently` pattern). Every entry with
   `kind != live && gc_time < gc_before(now)` becomes a **candidate**
   `(key_hash, timestamp, location)` in a per-shard candidate table, up to a
   memory cap (`gc_candidate_limit`); the remainder waits for a later cycle.
4. **Sweep**: header-only scan (`scan_segment` with `want_data::no` — the
   recovery scan path) of every scannable segment with `seq <= S0`: all
   compaction-group segments **and free-list segments** (their content is
   intact and recovery-visible, see the threat model). Any record header
   whose key matches a candidate at a **different location** cancels the
   candidate. Rate-limited (`gc_scan_bandwidth`, sequential reads).
5. **Quiesce**: with the barrier still armed, drain in-flight relocations
   (separator and compaction writes started before the sweep ended), so that
   every physical record existing at this point has made its index call.
   The separator's long buffering window makes this a forced flush of every
   separator buffer unless the liveness check at flush (journal section) is
   in place.
6. **Drop**: for each surviving candidate whose index entry still matches
   `(key, location)`: `index.erase`, synchronous per entry, chunked with
   yields (the barrier keeps cancelling not-yet-dropped candidates during
   the yields).
7. Disarm the barrier, discard the table.

### The write barrier

Every call that makes the primary index point at a record goes through
exactly two functions, and they are the only hooks needed (`erase` and
`clear` also mutate the index, but they remove entries and create no
records):

- `primary_index::insert` — user writes (`logstor::write` completion),
  recovery-style inserts, and streamed/restored segments (`load_segment`
  populates the index via `scan_slot`);
- `primary_index::update_record_location` — compaction, separator, and split
  compaction relocations (the call sites in `segment_manager.cc`, including
  the recovery path that separates a mixed segment).

While armed: if the key is in the candidate table, cancel the candidate.
Cancellation is deliberately coarse — *any* touch of a candidate key cancels
it (overwrite, losing backdated write, relocation of the tombstone itself).
Most cancellations are harmless deferrals (a relocated tombstone would simply
be re-dropped next cycle); coarse-and-retry is much simpler than classifying.

How each source of old records maps onto the hooks:

- **Hinted-handoff replay / retries / client `USING TIMESTAMP`** arrive as
  ordinary writes: the record is written physically, then `insert` runs. If
  it loses LWW to the candidate tombstone it has just become a stale copy —
  and that very `insert` call is the cancellation signal. If it *wins*, the
  tombstone is overwritten and freed, and the candidate is invalid anyway
  (the drop step re-verifies `(key, location)`). Either way the separator
  copies the record again later, and that update cancels once more.
- **Migration / streaming / restore.** `load_segment` writes the segment
  locally with a fresh seq, then populates the index via `scan_slot`,
  which calls `insert` **per record** — each record concerning a candidate
  key cancels it. There is no bulk path that makes bytes scannable without
  per-record index calls.
- **Compaction / separator / split-compaction copies** announce themselves
  through `update_record_location`.
- **Records that arrived before the barrier was armed** are covered not by
  the barrier but by the sweep: their bytes lie in segments with
  `seq <= S0`, all of which the sweep reads.

**Hot-path cost.** One branch per index mutation when idle; one hash probe
while a cycle runs. Since scans are long, a cycle is active most of the time
on a GC-busy shard, so the probe must be cheap: an open-addressing table (or a
bloom prefilter) over candidate key hashes. Approximate matching is safe in
exactly one direction — a false positive cancels a drop (deferral), and there
are no false negatives because equal keys hash equally. This cost must be
measured against the write-path budget.

### The hazard, restated

Recovery keeps, per key, the physical record that wins the comparator
`(timestamp, segment_seq, offset)` over all scannable segments. Call a
physical record of `K` at any location other than the live index entry's a
**stale copy** (freed losers, pre-move separator/compaction copies, records
in freed-but-not-reused segments). Today stale copies are harmless because
the live entry always dominates them. Dropping tombstone `T = (K, ts, loc)`
removes the dominating entry; if a stale copy of `K` survives scannable, it
becomes the maximum for `K` and the next recovery resurrects it. The sweep
proves no stale copy exists among segments that predate the cycle; the
barrier extends the proof to everything that appears during it.

### Structural facts

The correctness of the two-hook barrier rests on two properties of the
codebase (to be preserved, and asserted in review, by any future write path):

- **F1 — announcement.** Every path that makes bytes scannable also
  announces every record to the index, and always in the order *bytes first,
  index call second*: the write buffer persists the record before
  `logstor::write`'s continuation calls `insert`; compaction/separator write
  the copy before calling `update_record_location`; `load_segment` writes
  the segment before `scan_slot` inserts each record. No path creates
  scannable bytes silently — this closure property is what makes two hooks
  exhaustive.
- **F2 — choke point.** `primary_index::insert` and
  `primary_index::update_record_location` are the only functions that make
  the index point at a record.

### Invariants

1. **Announcement** (F1 + F2): every physical record's bytes land before its
   index call, and every such call passes through one of the two hooks.
2. **Sealing**: every record whose index call happened before arming resides
   in a segment with `seq <= S0`.
3. **Sweep coverage**: the sweep reads every scannable segment with
   `seq <= S0` — compaction groups *and* the free list. Supporting lemma:
   the sweep observes a superset of what any future recovery could observe
   from those segments — a segment that becomes unscannable mid-cycle
   (reused, first new chunk durable) is unscannable for recovery too, and
   the pin/skip rule for free-list segments closes the handover race. This
   lemma is the implementation's weakest link and gets its own tests.
4. **Barrier**: from arming to disarm, any index call for a candidate key
   cancels the candidate; the drop re-checks cancellation and performs
   gate-check + erase synchronously, with no yield.
5. **Quiesce**: at drop time, every *relocation copy* physically present has
   already executed its `update_record_location` (in-flight separator and
   compaction writes started before the sweep ended are drained, barrier
   still armed).
6. **Domination** (pre-existing recovery invariant, preserved): whenever a
   stale record of `K` is scannable, the live index entry for `K`
   dominates it under the recovery comparator.

### Correctness proof outline

**Claim.** If candidate `T = (K, ts, loc)` is dropped at time `D`, then at
any subsequent recovery the record recovered for `K` (if any) reflects a
state the runtime legitimately reached: no stale copy of `K` is resurrected,
except through post-purge LWW — which is accepted semantics, identical to
the traditional engine after a tombstone purge.

**Proof sketch.** Let `R'` be any physical record of `K` other than `T`'s,
scannable at recovery. By invariant 1, `R'` has an index call. Trichotomy on
when that call executed:

- **Case A — before arming.** By invariant 2, `R'` lies in a segment
  with `seq <= S0`; by invariant 3 the sweep saw its header — key `K` at a
  location other than `loc` — and cancelled `T`. Contradiction with `T`
  being dropped.
- **Case B — between arming and `D`.** The barrier (invariant 4) cancelled
  `T`. Contradiction.
- **Case C — at or after `D`.** Sub-cases:
  - *`R'` is a relocation copy written before `D`* — excluded by invariant 5:
    its index call was drained before the drop, placing it in Case B. (This
    is the one race the quiesce step exists for: without it,
    `update_record_location` would run after the erase, fail, and leave
    the copy — a scannable record of `K` with no live entry, i.e. a
    resurrection.)
  - *`R'` is a relocation copy started after the sweep* — relocations copy
    only records live at copy time; the only live record of `K` is `T`
    itself (the drop re-verified `(key, loc)`), so `R'` is a copy of the
    tombstone. The copier re-inserts it (orphan rule); recovery agrees.
  - *`R'`'s `insert` runs after `D`* — the late hint / backdated write /
    streamed record: `insert` finds either no entry for `K`, in which case
    `R'` **wins** and becomes the live entry — not a stale copy, and
    recovery keeping it agrees with the runtime index (post-purge LWW); or a
    newer live entry `W`, in which case `R'` loses and is dominated by `W`
    (invariant 6) — recovery keeps `W`.

In every branch, either `T` was never dropped (contradiction) or the
recovered state for `K` equals the runtime-visible state. Equal-timestamp
duplicates of `T` resolve by `(segment_seq, offset)` exactly as in normal
recovery and can at worst re-insert the tombstone. ∎

**Crash consistency.** The proof makes no assumption that the cycle
completed. Nothing is persisted; a crash aborts the cycle, losing only
candidates and cancellations. Drops already applied are covered by the
argument above; a dropped tombstone whose own bytes are still scannable may
be re-inserted by recovery — benign, re-dropped in a later cycle.

**What is *not* promised** — by this design or by the traditional engine: a
write older than the tombstone arriving *after* the purge wins, since
nothing shadows it anymore. Gate 1 (grace) is what makes that an operational
error (a replica down longer than `gc_grace_seconds`, or an operator
backdating past grace) rather than a mechanism defect.

### Candidate table

Keyed by a 128-bit key hash; per entry: hash, record timestamp, location,
cancelled bit — ~32 bytes. The cap (`gc_candidate_limit`, e.g. 1–4M per
shard, tens of MB) bounds memory; a mass-delete backlog larger than the cap
drains over several cycles, each requiring its own scan. This is the main
structural cost of the approach versus the frontier's O(1) state.

### Scan-cost reductions

- **Skip fully-live segments.** A segment in which every physical byte
  belongs to a record the index currently points to cannot contain a copy of
  any candidate key other than the candidate's own record (one live entry per
  key). This is the same observation as the *effective frontier* refinement,
  but here it is purely an I/O saving — the correctness carrier remains
  scan + barrier. It requires the `has_stale` accounting of never-inserted
  losers (Stage A).
- **Invalidate-on-free (optional).** Zeroing a freed segment's header (as the
  frontier design mandates) is *optional* here and only shrinks the scan set;
  freed-but-intact segments are otherwise scanned like recovery would scan
  them. Reuse races are handled the way recovery handles them: once the first
  chunk of the new incarnation lands, the old content is unscannable
  (seq mismatch); a segment should be taken off the free list (or skipped)
  while being scanned.

### TTL under mark-and-sweep

Unchanged. Expired records enter the candidate set with the conservative
`gc_time = expiry` rule; the slim-tombstone conversion in compaction is
orthogonal to Gate 2 entirely — it *rewrites* rather than drops, so it needs
no no-resurrection check and ships identically under either mechanism.

One capability is lost: **drop in compaction** (the opportunistic path) is not
available, because compaction cannot locally prove no-resurrection without a
completed sweep. Between cycles, compaction keeps copying droppable
tombstones forward — cheap once deletes are slim, but a real difference for
payload-bearing tombstones until slim deletes ship.

### Risks specific to mark-and-sweep

1. **Quiesce completeness is enumerative.** A missed relocation window is a
   silent resurrection path. A relocation phaser with a debug assertion that
   `update_record_location` is never called outside it turns "did we
   enumerate all windows?" into a runtime check.
2. **Cancellation storms.** A compaction that rewrites a tombstone-dense
   segment mid-cycle relocates many candidates and cancels them all — GC
   starves precisely on the segments most worth reclaiming, and relocation
   is the *common* touch of a cold tombstone (far more common than backdated
   writes). The separator copying dead records adds a cancellation per
   overwritten-before-separation record. Updating the candidate's location
   on `update_record_location` instead of cancelling, and the liveness check
   at flush, are therefore likely requirements, not refinements.
3. **Long armed windows.** Cycle duration is scan-bound; on large disks the
   barrier is armed for hours, so the hot-path probe is effectively
   permanent and cancellation exposure grows with cycle length.
4. **Candidate memory.** The cap means mass-delete backlogs drain over
   several cycles, each a full scan.

## Comparison of the Gate 2 candidates

| Dimension | Frontier + horizons | Purge journal | Mark-and-sweep |
|---|---|---|---|
| Proof shape | inductive: everything old is durably unscannable | filter: everything old is unrecoverable by seq; drain makes the index the truth for `seq <= S` | direct: scanned everything, saw nothing; barrier keeps it true |
| Extra writes | forced compaction of all segments below the horizon; 4K invalidation per freed segment | journal append per drop; invalidation per freed segment (retirement); forced compaction only past the journal cap | none |
| Extra reads | none beyond forced compaction | none (frontier retirement) or a header scan per retirement period | header scan of scannable segments per cycle |
| A 95%-live cold segment | must be **rewritten** for `F` to pass it | untouched until the journal needs it retired | merely **read** |
| Drop latency after grace | until `F` passes the pre-grace horizon (compaction-bandwidth-bound) | one cycle (drain + index walk) | one mark+scan cycle (read-bandwidth-bound) |
| Drop granularity | continuous as `F` advances; plus drop in compaction | batch per cycle; drop in compaction possible with a deferred journal append | batch at end of each cycle; no drop in compaction |
| Persistent state | none | journal file (per-key entries until retired) | none |
| Per-shard memory | O(1): `F`, ring, scannable set | journal hash set at recovery only | candidate table (~32 B/candidate, capped) |
| Write hot path | untouched (re-clamp on losers only) | untouched (phaser held by copiers) | branch + hash probe per index mutation while a cycle runs |
| Coupling | GC progress tied to compaction (frontier task, separator nudge, invalidate-on-free ordering) | drain per cycle: active-segment switch, write phaser, relocation phaser, liveness check at separator flush | two index hooks + recovery's scan path + quiesce |
| Crash | recompute; GC resumes after a full frontier pass | journal is the truth; nothing to recompute | stateless; aborted cycle restarts |
| Failure modes | `F` stuck behind cold/mixed segments; WA spikes | journal cap reached (drops pause); recovery memory | scan bandwidth; candidate cap; churn-cancel starvation |
| Depends on `logstor_recovery1` | rebase only | watermark file pattern, scan-time filter, Invariant Q; CRC for the retirement scan | body CRC for scanning open/reused segments |

The frontier is the smallest complete mechanism and its machinery (segment
`seq`, scannable set, invalidate-on-free, the re-clamp) is also the journal's
retirement engine, so the journal is a strict extension rather than a
replacement. Mark-and-sweep's unique costs — the write-path probe, the
candidate table, cancellation storms, a barrier armed for hours — buy nothing
the journal does not buy more cheaply once the erase-watermark pattern exists.

## Interaction with the discard work (`logstor_recovery1`)

`logstor_recovery1` (design in `logstor_discard.md`, Phases 0–3
implemented, not merged as of this review) changes the recovery model this
design reasons about. What it provides and what it changes here:

- **Rebase.** `logstor_gc` and `logstor_recovery1` fork from the same
  master; the GC work builds on the recovery branch. This document describes
  the code as of that branch.
- **Erase watermarks** (`erase_watermarks.{hh,cc}`, file `ls_{shard}-Erase.db`,
  `segment_manager::erase()`): a durable `(table, token range, S)` filtered
  at scan time before insertion; the recovered `_next_segment_seq` stays
  above every watermark (Invariant Q); watermarks are GC'd at recovery by the
  minimum seq present and by table existence. The purge journal is the
  per-key instance of this pattern and reuses the file, filter and Invariant
  Q idioms; the erase protocol's "drain, then `S = _next_segment_seq − 1`,
  erase, then switch the active segment" is the journal cycle's drain in
  range form — the erase can pick `S` before switching because it pauses the
  table's writes, the journal has no pause and switches first.
- **`discard_segments` is gone**; the erase path's `discard_group_segments`
  still zeroes the first block of the group's emptied segments. The frontier
  needs that zeroing on *every* free — "Option A", which the discard design
  considered and deferred as unnecessary for erase correctness. The two do
  not conflict: the invalidation queue here does the zeroing for both.
- **Rules P2 and P3** at `free_segment` and the three copiers. P3 (publish
  before free: the copier holds its sources until the copies are
  index-visible) is what closes the in-flight-copy case of the frontier's
  proof; it was implicit before and is now written where the code relies on
  it. Any new copier — the frontier task reuses compaction, the slim-tombstone
  rewrite lives in compaction — inherits it.
- **Body CRC and header-last streaming.** A torn or racing chunk is rejected
  by the scan, which is what makes an online header scan of open or reused
  segments safe: a hard dependency of mark-and-sweep and of the journal's
  retirement scan, and what the online scrubber tool needs.
- **Recovery-equivalence harness** (`recovery_divergence()` in
  `test/lib/logstor_test_utils.hh`, `rebuild_index_from_disk(index_resolver)`):
  rebuilds an index from disk through the recovery code and diffs it against
  the live one. It is the oracle for GC's resurrection suite: after every GC
  scenario (and after every injected kill), the rebuilt index must equal the
  live one — dropped keys absent, everything else in place. The journal
  adds its filter to `rebuild_index` the way the watermarks did.
- **Records without a local group are dropped at recovery** and counted; a
  `full` segment straddling two local groups is separated per record. GC
  does not change this.
- **The equal-timestamp gap** (a same-timestamp overwrite lost at recovery
  when the separator has moved the older copy above the active segment) is
  independent of GC: the drop rule reasons about *any* stale copy regardless
  of the comparator. The planned fix wants the segment sequence in
  `segment_descriptor`, which Stage A adds; it should land as one change.
- **Erase during GC.** An erase (cleanup, truncate, drop) fences writes and
  stops the copiers of its groups; the sweep may keep erasing keys of the
  range meanwhile, which `index.erase(range)` would remove anyway, and a
  journal append for an erased key is harmless (the watermark covers the
  key too). Drop-in-compaction is off while compaction is disabled for the
  erase.
- **"Scannable" vs "recoverable".** A record covered by a watermark is
  scannable but not recoverable. This design keeps reasoning about
  scannability, which is conservative.

## Alternatives considered

- **Durable liveness checkpoint**: persist an index/segment-liveness
  manifest; recovery replays only newer segments, so stale records are never
  re-scanned and Gate 2 holds by construction. Strictly stronger and also a
  recovery-speed improvement, but a much larger artifact (checkpoint format,
  crash consistency, replay). Deferred; the frontier machinery remains useful
  with it (frontier trivially equals the checkpoint barrier).
- **Effective frontier** (refinement of the frontier): at drop time, ignore
  scannable segments that hold **no stale copy at all** (`has_stale` clear):
  a fully-live segment cannot contain a stale copy of any key. It must be
  evaluated per drop, not kept as a monotone counter: a cold segment that was
  fully live when the frontier passed it may gain its first stale copy later
  (an overwrite of one of its keys) and then hold a pre-delete value of a key
  deleted later still — so `F_eff = min{seq : scannable and has_stale} − 1`,
  which can move backwards. It depends on the loser accounting of Stage A
  (a losing write leaves a stale copy without any free). This removes most
  forced-compaction write amplification for cold live data. A further step
  — passing a segment by a header scan that records its stale keys, and
  rewriting only when there are too many — is the scan-to-pass hybrid; not
  planned.
- **Global external horizon `H_ext`** (previous revision): a per-shard
  monotone seq bumped on every loser to a tombstone and on every
  `load_segment`. Replaced by the per-entry re-clamp, which is equivalent
  for correctness and does not stall every drop on the shard.
- **Persisting `F`/snapshots**: lets GC resume immediately after restart
  instead of waiting for a fresh frontier pass. Pure optimization with crash-
  consistency cost; deferred. The purge journal makes it moot for drop
  latency.

## Implementation plan

Stages are independently mergeable; tombstone GC (B) and TTL (C) are
independent after A. Each stage lands with its metrics and tests, and every
resurrection test uses `recovery_divergence()` as its oracle.

### Stage A — groundwork and GC metadata

- **A0. Shared groundwork.** `segment_descriptor::seq` (set at
  `get_segment`, rebuilt by recovery) — also what the equal-timestamp fix of
  `logstor_discard.md` needs; the three loser sites report the loser's
  location (`has_stale`) and expose the winning entry to the caller; the
  space-accounting interface carries the record kind.
- **A1. On-disk metadata.** `kind` + `gc_time` + `deletion_time` + `expiry`
  in the `ondisk.hh` record header with the local-time clamp; write-path
  classification (`extract_logstor_record_timestamp` grows into a
  `classify_record` returning timestamp + gc info). Recovery, compaction and
  the separator pass the fields through unchanged (`log_record_bytes_writer`
  already preserves header bytes). No version bump (see above).
- **A2. Index metadata.** `gc_time` in `index_entry`'s padding, `kind` in
  `primary_index_entry`'s flags; populated on write and on recovery; sizes
  pinned by `static_assert`; kind-aware cache eviction hook for future
  conversion.
- **A3. Segment accounting.** `gc_bytes`, `min_gc_time`, `has_stale` in
  `segment_descriptor`, maintained on write/free/lose and rebuilt in
  recovery; gauges.
- **Tests:** header serialization round-trip; classification (live delete /
  expiring / mixed-TTL / backdated timestamps → clamp); accounting under
  write/overwrite/free/lose/recovery.

### Stage B — tombstone GC (timeout mode, frontier)

- **B1. Scannable set + frontier.** Track seqs of scannable segments; the
  invalidation queue and fiber (shared with `discard_group_segments`); `F`
  advances on completed invalidation; recovery rebuilds the set from scanned
  headers and queues the free-list slots for invalidation. Metrics: `F`,
  `max_seq`, queue depth.
- **B2. Horizons.** Snapshot ring; re-clamp at the three loser sites (winner
  kind from the entry); per-table purge bound.
- **B3. Drop paths.** GC sweep fiber (chunked index iteration, drop rule,
  erase); opportunistic drop in `do_compaction`/`do_split_compaction` header
  callbacks; the orphan-tombstone re-insert in the copiers' update failure
  path; config knobs; drop/reclaim metrics.
- **B4. Frontier task.** Forced lowest-seq compaction, rate-limited;
  separator nudge; pending-GC estimate to avoid idle rewriting; the
  effective frontier (needs `has_stale`).
- **Tests:**
  - unit: write→delete→grace→sweep drops entry and bytes; Gate 1 boundary;
    Gate 2 blocks until frontier passes; purge bound math; re-clamp on a
    late loser delays the drop.
  - resurrection (the critical suite, on `recovery_divergence()`): delete,
    GC, **kill at every stage** (before/after invalidation of old segments,
    before/after tombstone-segment free) via error injection, recover, assert
    the key stays deleted. Repeat with a stale old copy parked in a high-seq
    segment (separator/compaction loser), with a backdated delete, with a
    late losing write after grace, and with a relocation of the tombstone
    straddling the drop (orphan re-insert).
  - streaming interplay: stream in a segment with an old copy while a local
    tombstone is pending drop; assert the re-clamp defers the drop; kill +
    recover.
  - liveness: cold fully-live segments + deletes → frontier task advances `F`
    and GC completes within the bandwidth budget; effective frontier skips
    them.
- **Exit measurement:** drop latency after grace and forced-rewrite bytes on
  a cold-disk-with-scattered-deletes stress run decide whether Stage B' is
  needed before Stage C.

### Stage B' — purge journal (extension)

- **B'1. Relocation phaser.** Held over every copy window (compaction,
  split, separator flush, recovery separation) with a debug assertion that
  `update_record_location` never runs outside one; `await_relocations()`.
- **B'2. Liveness check at separator flush.** Drop records no longer alive
  at their source before the buffer is written out.
- **B'3. Journal file and recovery filter.** Append-only per-shard file;
  hash set loaded before the scan; filter in `scan_slot` next to the
  watermark check; Invariant Q extended; retirement by `F` with a head
  pointer; `gc_journal_max_entries` pausing drops at the cap; the frontier
  task demoted to a cap-driven backstop.
- **B'4. Cycle fiber**, replacing the sweep's drop rule with the
  switch–drain–mark–journal–erase cycle; dry-run mode first (report
  would-drop survivors without erasing).
- **Tests:** the resurrection suite unchanged (it is written against the
  drop-rule interface); kills between erase and journal append; a late loser
  between mark and erase leaves the tombstone in place and unjournaled; a
  post-purge older write survives restart; a relocation of a pre-delete value straddling
  the drain; journal cap reached; journal GC at recovery for dropped tables.

### Stage C — TTL

- **C1. Read hiding verification.** Tests (and fixes if needed) that expired
  rows/partitions are invisible through `logstor::read` / `make_reader` /
  cache paths before any reclamation ships.
- **C2. Sweep drop of expired records.** Uses the `gc_time = expiry`
  conservative rule; no new machinery beyond Stage B.
- **C3. Convert in compaction.** Expired `expiring` record → drop if the exact
  rule passes, else slim-tombstone rewrite (empty-payload representation +
  read-path synthesis; index kind/gc_time update; cache eviction).
- **C4. Expiry-driven triggering.** `gc_bytes`-ratio + `min_gc_time` candidate
  boost.
- **Tests:** TTL write → expiry → hidden reads → sweep/compaction reclaim;
  mixed-TTL partitions stay until `max(expiry)`; slim record round-trip
  (read, recovery, compaction rewrite, streaming); resurrection-suite variant
  with TTL data.

### Stage D — repair-aware grace

- Wire `tombstone_gc_state` / `get_gc_before_for_key` into snapshots (the
  repair-history map as of snapshot time, copy-on-write) and the sweep;
  support `repair`/`immediate`/`disabled`; cluster test with repair-mode GC.

### Stage E — optional follow-ups

- Slim *user* deletes (write deletes header-only from the start).
- Online scrubber: the header-only scan with the domination check, useful
  as a consistency tool regardless of the Gate 2 mechanism.
- Durable liveness checkpoint (also a recovery-speed win).

## Settled in the review of 2026-10-01

- **Gate 2 mechanism:** frontier + horizons as the base, purge journal as
  the extension if Stage B's measured drop latency or forced-rewrite cost is
  unacceptable; mark-and-sweep is not pursued.
- **`H_ext` replaced** by the per-entry re-clamp; `load_segment` needs no
  special case (its inserts go through the same loser site).
- **In-flight copies of pre-delete values** are covered by P3 under the
  frontier and by the drain under the journal; **in-flight copies of the
  tombstone itself** are re-inserted by the copier, so runtime and recovery
  agree even when a post-purge older write arrived meanwhile.
- **Invalidate-on-free** runs through a queue before the slot is reusable;
  recovery's free-list slots are invalidated in the background.
- **Metadata layout:** zero per-key memory growth (`gc_time` in
  `index_entry`'s padding, `kind` in the flags byte; the entry stays 72
  bytes); 16 bytes per record on disk, no version bump;
  `segment_descriptor::seq` shared with the equal-timestamp fix.
- **The clamped `gc_time` goes to disk**, so a restart does not restart
  grace.
- **The drop is one call**, `index.erase(key, loc)`; the disposer frees.
- **Effective frontier** is evaluated per drop and needs loser accounting.
- **Dependencies on `logstor_recovery1`** are listed above; the GC branch is
  rebased onto it, and `recovery_divergence()` is the test oracle.

## Open questions

- Exact defaults for `gc_frontier_bandwidth`, `gc_snapshot_period`,
  `gc_sweep_period` — pick after the Stage B stress runs.
- Whether the empty-payload slim tombstone can replace payload-bearing deletes
  everywhere without read-path regressions (Stage C decision point).
- Whether to pack the on-disk GC block to 8 bytes (kind in `key_size`'s high
  bits, kind-dependent time fields) — decide with the record-overhead
  measurements in hand.
- Journal cap default and recovery-time memory sizing, if Stage B' is
  built; whether retirement should be frontier-driven or scan-driven.
- Whether the liveness check at separator flush (B'2) should land earlier,
  as a write-amplification improvement independent of GC.

## Addendum: branches and terms since this document was written

This document was written against `logstor_recovery1`, which is no longer developed. On
Nov 10 2026, its work is in two branches:

- `logstor_recovery_rebuild` (PR 32520) holds the changes to recovery.
- `logstor_erase_range` holds the durable erase, and it stacks on `logstor_recovery_rebuild`.

The erase changed in these ways since `logstor_recovery1`:

- The erase has one entry point, `logstor::erase()`, and the erase finds the groups of the
  table in the range itself, so the caller does not give a list of groups.
- The write fence is on the primary index of the table, for the token range of the erase
  (`primary_index::fence_writes()`), and not on a group.
- The erase does not zero the first block of the segments that it frees. A freed segment keeps
  its chunks on disk, and the erased range covers their records.
- The erase leaves a segment that a migration streams out to its last reference.
- The erase switches the active segment above its sequence also when a step of the erase fails.
- `erased_ranges::with()` drops the erased ranges that later erases cover, so the size of the
  set does not grow with the number of erases.
- The data files are now `ls-{shard}-Data-{file_id}.db`, and the erased range file is
  `ls-{shard}-Erase.db`. These names replace `ls_{shard}-{file_id}-Data.db` and
  `ls_{shard}-Erase.db`.

The terms of the erase changed as follows:

| Old term | Current term |
|----------|--------------|
| erase watermark, the entry {table, token range, S} that an erase saves | erased range (`erased_range`) |
| erase watermarks, the erase watermark file | the erased ranges (`erased_ranges`), the erased range file |
| `replica/logstor/erase_watermarks.{hh,cc}` | `replica/logstor/erased_ranges.{hh,cc}` |
| "the watermark", when it means the level S of an erase | the sequence of the erase |
| `erase_watermarks::erased()` | `erased_ranges::covers()` |
| `erase_watermarks::gc()`, a spent watermark | `erased_ranges::drop_obsolete()`, an obsolete erased range |
| rule P2, "erase implies a watermark" | rule P2, "erase implies an erased range" |
| metric `erase_watermarks` | metric `erased_ranges` |

Every "watermark" in this document is the erase watermark.
