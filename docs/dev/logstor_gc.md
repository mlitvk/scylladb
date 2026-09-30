# Logstor Tombstone GC and TTL Expiry

## Status

Design document. Gate 2 (local no-resurrection) currently has **two candidate
mechanisms** under evaluation:

- the **sequence reclaim-frontier** plus monotone **horizons** (detailed in
  [Gate 2](#gate-2--local-no-resurrection) below);
- **mark-and-sweep verification** — a read-only scan of all scannable
  segments plus a write barrier (see
  [Alternative Gate 2](#alternative-gate-2--mark-and-sweep-verification)).

Everything outside Gate 2 — GC metadata (Stage A), Gate 1, the two-gate rule,
TTL handling — is common to both. The durable liveness checkpoint remains a
future improvement (see [Alternatives](#alternatives-considered)); in either
mechanism nothing new is persisted and GC state is recomputed at recovery.

Remaining minor open items are listed at the [end](#open-questions).

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

Relevant behaviors (see `replica/logstor/`):

- **A delete is just a write.** `logstor::write` inserts the new record; on
  overwrite it calls `free_record(prev)`. The record timestamp is the live
  row-marker timestamp or, for a delete, the partition tombstone timestamp
  (`extract_logstor_record_timestamp`). Nothing in the header distinguishes a
  tombstone from a live record.
- **Conflict resolution is record-level LWW.** `primary_index::insert` keeps
  the entry with the higher timestamp. A write that loses (e.g. a late or
  backdated write) is still written physically and then freed
  (`free_record(new_loc)`).
- **`free_record` frees accounting, not bytes.** It increases
  `segment_descriptor.free_space`; the record bytes physically remain in the
  segment until the segment is rewritten or reused.
- **`free_segment` frees the slot, not the disk.** A segment freed by
  compaction goes back to the free list with its on-disk content intact and
  fully scannable. Only `discard_segments` (table drop) invalidates the
  header block on disk today.
- **Compaction keeps everything the index points to.** `do_compaction`
  rewrites every record for which `index.is_record_alive()` is true and skips
  freed ones. A tombstone/expired record is still the live index entry, so it is
  rewritten forever. Segment selection is utilization-based; tombstones count as
  live data.
- **Recovery rebuilds the index by scanning all physical records**, keeping the
  max `(timestamp, segment_seq, offset)` per key. Freed-but-physical records are
  re-scanned; the live one wins only because it has the highest timestamp.
  Segments freed (but not reused) before a crash are re-scanned too.
- **`segment_seq`** is a monotonic per-shard sequence assigned to a segment when
  it starts (`allocate_segment_seq`). All buffers in a mixed segment carry the
  segment's seq; `scan_segment` stops at the first buffer whose seq differs
  from the segment's first buffer, so a *reused* segment's stale tail is
  unreachable. Streamed-in segments get a **fresh local seq**
  (`streamed_segment_rewriter` rewrites the chunk header).
- **Files are opened with `O_DSYNC`** — a completed DMA write is durable.

### How traditional (LSM) storage does it

In the LSM engine, physical removal of tombstones and expired data happens
only when compaction rewrites sstables. Every fragment that flows through a
compacting reader is processed by the **mutation compactor**
(`compact_mutation_state` in `mutation/mutation_compactor.hh`, instantiated as
`compact_for_compaction` by `compaction/compaction.cc`). Each tombstone it
encounters — partition tombstone, range tombstone, row tombstone, dead
cell/marker — is tested by `can_purge_tombstone()`
(`mutation/mutation_compactor.hh:273`), which is the conjunction of the two
gates below. The same compactor class also runs at query time
(`compact_for_query`), where it only *hides* dead/expired data from results;
purging is exclusive to `compact_for_sstables::yes` mode.

#### Gate 1 — grace (`gc_before`)

The grace gate is `deletion_time < gc_before` (`satisfy_grace_period`,
`mutation_compactor.hh:270`). `gc_before` is computed per partition key by
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

The repair history is populated at the end of repair
(`shared_tombstone_gc_state::update_repair_time` /
`batch_update_repair_time`, called from `repair/row_level.cc:2653,3838`),
persisted in `system.repair_history` so it survives restarts, and reloaded on
boot. Pending updates flush together with commitlog cleanup records
(`flush_pending_repair_time_update`, `tombstone_gc.cc:273`).

**The commitlog clamp.** Every mode's result passes through `check_min`
(`tombstone_gc.cc:148`), which caps `gc_before` at the commitlog's
`min_gc_time` for the table (wired in `main.cc:2102`). This prevents purging
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
(`mutation_compactor.hh:277`).

#### Gate 2 — no-resurrection (`max_purgeable`)

Dropping a tombstone must not un-hide older data in sstables/memtables that
are **not** part of this compaction. For each partition key, compaction
computes a `max_purgeable` bound (`get_max_purgeable_timestamp`,
`compaction/compaction.cc:200`): the minimum **live-data timestamp** among
data sources outside the compacting set that may contain the key:

- **memtables**: `min_memtable_live_timestamp()`, consulted only if the
  memtable actually contains the key and holds data older than the compacting
  sstables' max timestamp (`compaction.cc:243`);
- **overlapping non-compacting sstables**: selected by token overlap
  (`sstable_set::incremental_selector`), filtered by a **bloom-filter** key
  check; each contributes its `min_live_timestamp` from the sstable's
  extended timestamp stats (falling back to `min_timestamp`);
- **compacted-undeleted sstables** (sstables whose deletion failed after
  compaction) are included too.

A tombstone is then purgeable iff `tombstone.timestamp <
max_purgeable.timestamp` — no possibly-shadowed data older than the tombstone
exists elsewhere (`max_purgeable::can_purge`,
`compaction/compaction_garbage_collector.hh:35`, `compaction.cc:90`). Two
refinements:

- **Shadowable tombstones** (from materialized-view updates) compare against
  the minimum live *row-marker* timestamp instead of the minimum live cell
  timestamp (`compaction.cc:220`; issues #20423/#20424 explain why dead data
  does not count).
- An **expiry-threshold elision**: if the tombstone was already expired when
  the outside data sources were created, all their writes arrived after the
  tombstone expired, and the timestamp-overlap check can be skipped
  (`compaction_garbage_collector.hh:82`).

Only live data counts — purgeable tombstones that shadow *dead* data outside
the compacting set may still be purged. The per-key result is cached per
partition in the compactor (`_max_purgeable_regular/_shadowable`,
`mutation_compactor.hh:342`). Purge failures are attributed in
`tombstone_purge_stats` (blocked by memtable vs. by other sstables —
`mutation_compactor.hh:299`), exported as metrics. A config option
(`gc_check_only_compacting_sstables`) can restrict the check to the compacting
set for setups that guarantee non-overlap.

#### TTL expiry

TTL is entirely **lazy** — nothing is scheduled when data is written:

- A live cell optionally carries `(expiry, ttl)` in its on-disk/in-memory
  representation (`mutation/atomic_cell.hh:70`); a `row_marker` carries the
  same (`mutation/mutation_partition.hh:551`). The logical **deletion time of
  an expired cell is `expiry − ttl`** — i.e. as if it was deleted when
  written (`atomic_cell.hh:271`, `mutation_partition.hh:604`), so the grace
  period effectively counts from the write.
- **Reads hide expired data on the fly**: `cell.has_expired(query_time)` makes
  the cell behave as dead during query-time compaction; no storage is touched.
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
  tombstone deletion times.
- **Droppable-ratio trigger**: `sstable::estimate_droppable_tombstone_ratio`
  (`sstables/sstables.cc:1584`) integrates that histogram up to the sstable's
  range `gc_before`. Strategies (`worth_dropping_tombstones`,
  `compaction/compaction_strategy.cc:61`) submit a single-sstable "tombstone
  compaction" when the ratio exceeds `tombstone_threshold` (default 0.2) —
  but only if the sstable is older than `tombstone_compaction_interval`
  (default 1 day) to avoid repeatedly rewriting an sstable whose tombstones
  cannot be purged yet. STCS, TWCS, and ICS all use this as a fallback when
  no regular compaction is pending (`size_tiered_compaction_strategy.cc:244`,
  `time_window_compaction_strategy.cc:404`,
  `incremental_compaction_strategy.cc:248`).
- **Whole-sstable drop**: `get_fully_expired_sstables`
  (`compaction/compaction.cc:2246`) finds compacting sstables with
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
| No-resurrection gate | `max_purgeable` overlap check | N/A locally (one version per key), but freed-but-physical records + recovery-by-scan create a different resurrection risk |
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
  plus monotone **horizons** that bound where stale copies can physically live.
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

**Local-time clamp.** For GC-gating purposes the stored times are clamped to
the node-local wall clock at insert:
`gc_time = max(deletion_time_or_expiry, gc_clock::now())`. This makes the
grace period count from when *this node learned* of the delete, which is
essential for Gate 2 correctness under client-backdated timestamps
(`USING TIMESTAMP`) — see the threat analysis below. It only ever delays GC,
never advances it. The unclamped `deletion_time` is stored too (it is what the
converted tombstone carries semantically).

**On-disk** (`record_header` in `idl/logstor.idl.hh`, buffer `version`
bump in `ondisk.hh`): add `kind` and the two time fields (absent/0 for `live`
records; 32-bit `gc_clock` seconds, as in the sstable format). The on-disk
format may change freely; backward compatibility is not maintained.

**In-memory** (`index_entry` in `types.hh`): add `kind` (2 bits) and a single
32-bit `gc_time` field:

- `deletion` → clamped `deletion_time`;
- `expiring` → clamped `expiry`.

One field suffices for both kinds because using `expiry` in the Gate 1
comparison is *conservative* for expiring records: `deletion_time = expiry −
ttl <= expiry`, so `expiry < gc_before ⟹ deletion_time < gc_before`. The
sweep therefore waits "grace after expiry" instead of "grace after logical
deletion" — safe, and the exact `deletion_time` remains available from the
on-disk header when compaction converts the record. Pack the new fields into
the existing `index_entry`/`primary_index_entry` padding where possible; the
per-key memory budget for this feature is ≤ 8 bytes and should be measured.

**Per-segment accounting** (`segment_descriptor` in `compaction.hh`): add

- `seq` — the segment's sequence, kept in memory (needed by the frontier; also
  set during recovery from the scanned headers);
- `gc_bytes` — bytes of live `deletion` + `expiring` records in the segment
  (maintained on write and on free; the free path knows the record kind from
  the index entry being freed);
- `min_gc_time` — minimum clamped gc_time among records written to the
  segment. Not adjusted on free (a conservative lower bound is fine — it is
  only a triggering heuristic, never a correctness input).

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

After both gates pass, the drop itself is: `index.erase(key, loc)` +
`free_record(loc)` (both synchronous — no yield between the gate check and the
erase). The disposer path evicts any cached mutation. The freed bytes are then
reclaimed by normal compaction. Note that `free_record` **must** be called so
the segment's `record_bytes` accounting reaches zero before `free_segment`.

If, after a purge, a write for the key arrives with an older timestamp, it
wins (there is nothing to shadow it). This matches traditional post-purge LWW
semantics and is inherent to tombstone GC, not a defect of this design.

### Gate 2 — local no-resurrection

#### Threat model

Recovery rebuilds the index by scanning **all scannable segments** and keeping
the max-timestamp record per key. If tombstone `R` of key `K` is physically
dropped while an older, freed-but-physical copy `R'` of `K` is still
*scannable*, recovery re-inserts `R'` — a resurrection. "Scannable" includes
segments freed by compaction but not yet reused (their content is intact) —
only reuse (seq-mismatch stops the scan) or explicit header invalidation makes
a segment unscannable.

Every path that writes a record and then loses a liveness check produces
freed-physical copies, and they can land in **arbitrarily high-seq segments**:

1. **Normal overwrite** — the previous record is freed in place. Its segment
   may have a *higher* seq than the overwriting record's segment (the separator
   may have just moved it into a fresh full segment).
2. **Separator** — moves a just-written record into a new full segment, then
   frees it if the key was overwritten meanwhile.
3. **Compaction / split compaction** — write-then-check; losers freed in a
   fresh output segment.
4. **Late/backdated direct writes** — a write that loses LWW to the live entry
   (hinted-handoff replay, retries, client `USING TIMESTAMP`) is written
   physically into the active segment — at **any** time, including long after
   the key became a tombstone and passed grace.
5. **Streaming / restore ingest** — a peer's segment is loaded
   (`load_segment`); records that lose to the local index become
   freed-physical copies in a fresh-seq local segment, at any time.
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
  segment is freed** (same 4K-zero write `discard_segments` already does;
  files are `O_DSYNC` so completion ⇒ durable). A segment's seq leaves the
  scannable set — and `F` may advance — only after its invalidation write
  completes.
- **Snapshot ring.** Periodically (config: `gc_snapshot_period`, default
  ~1 min) append `(t = gc_clock::now(), H_t = max allocated seq)`. The ring is
  bounded (e.g. 64 entries) by merging the oldest entries. After recovery the
  ring starts empty; the first snapshot's `H` covers everything on disk at
  boot.
- **External horizon `H_ext`.** A monotone per-shard value, bumped to the
  *current max allocated seq* whenever an event may have produced a
  freed-physical copy of a key whose live entry is a tombstone:
  - a completion frees the record it just wrote (`logstor::write` loser
    branch, separator completion, compaction `rewrite_record` loser) **and**
    the winning index entry has `kind == deletion`;
  - a streamed/restored segment is loaded (`load_segment`) — unconditionally;
  - recovery completes (initialize `H_ext = max recovered seq`; this is
    subsumed by the empty snapshot ring but keeps the invariant uniform).

  Bumping to "current max allocated seq" avoids needing the exact seq of the
  segment just written to; it is monotone and cheap
  (`_next_segment_seq − 1`).

**Drop rule.** Tombstone `T` (key `K`, clamped `gc_time`) may be dropped iff
there exists a snapshot `(t, H_t)` with:

```
gc_time < gc_before(t)          (Gate 1 evaluated at snapshot time t)
and  F >= H_t                   (everything scannable at snapshot time is gone)
and  F >= H_ext                 (no unreclaimed racy copies)
```

In `timeout` mode `gc_before(t) = t − gc_grace(table)`, so this reduces to a
per-table **purge bound**: `purge_before = max{ t − grace : H_t <= F }`,
recomputed when `F` advances; a tombstone is droppable iff
`gc_time < purge_before && F >= H_ext`. Both checks are O(1) per record.

#### Correctness argument

Claim: at drop time, every scannable freed-physical copy `R'` of `K` lies in a
segment with `seq <= max(H_t, H_ext) <= F`, hence is *not* scannable —
contradiction; so no such copy exists and recovery cannot resurrect `K`.

Case analysis on when `R'` was freed, relative to `T`'s index insert:

- **Freed before `T`'s insert.** `R'` was physically written before it was
  freed, so `seq(R') <= max_seq(T-insert)`. The clamp guarantees
  `gc_time >= local insert time`, and the chosen snapshot has
  `t > gc_time + grace >= insert time`, so `H_t = max_seq(t) >=
  max_seq(T-insert) >= seq(R')`. (Without the clamp, a backdated
  `deletion_time` would let a snapshot *older than the insert* satisfy Gate 1
  — the hole in sources 4–6 above.)
- **Freed after `T`'s insert, winner is `T`.** The freeing completion saw a
  `deletion`-kind winner and bumped `H_ext >= seq(R')` before returning; the
  drop check reads `H_ext` in the same reactor task as the erase, so either
  the bump is visible (drop blocked until `F` catches up) or the copy was
  freed after the drop — impossible, since after the erase the index has no
  entry for `K` and a late write now *wins* (post-purge LWW, see above).
- **Freed after `T`'s insert, winner is a newer non-tombstone `W`.** Then `T`
  is no longer the live entry and will never be dropped (it was itself freed).
  If `W` is later deleted by a new tombstone `T'`, all these copies were freed
  before `T'`'s insert — first case applies to `T'`.
- **Streamed/restored segments** may carry a tombstone that becomes the live
  entry while local pre-existing copies of the key were freed at load time;
  the unconditional `H_ext` bump on `load_segment` covers all local segments
  existing at that moment.

Crash consistency: the drop becomes durable only when `T`'s segment stops
being scannable (invalidated or reused) — which happens strictly after the
drop, which happens strictly after all `seq <= max(H_t, H_ext)` segments were
durably invalidated (that is what `F >= …` means, and `F` only advances on
completed `O_DSYNC` invalidation writes). A crash at any intermediate point
re-inserts `T` at recovery — benign; it will be re-dropped. After recovery the
snapshot ring and `H_ext` are re-initialized conservatively, so GC resumes
only after a full frontier pass — an accepted cost of not persisting GC state
(see Alternatives).

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
  copies it forward with a fresh seq).
- Mixed segments awaiting separation can hold the frontier back; nudge them
  with `flush_separator(seq)`.
- Worst-case write amplification is one full-disk rewrite per GC cycle; the
  bandwidth knob bounds the cost and stretches the cycle. The
  "effective frontier" refinement (Alternatives) removes most of this cost
  later by letting `F` skip fully-live segments.

### Drop mechanisms

Two cooperating consumers of the drop rule:

1. **GC sweep (primary).** A periodic per-shard fiber iterates each logstor
   table's `primary_index` gently (chunked, re-lookup after yield — same
   pattern as `erase_range_gently`). For entries with
   `kind != live && gc_time < purge_before && F >= H_ext`: erase + free +
   count. This reclaims **index memory** promptly and turns the record into
   dead bytes that utilization-based compaction picks up naturally — no
   GC-aware segment selection is needed for basic operation. Existing races
   are already handled: compaction's `update_record_location` fails for erased
   keys and frees its copy; in-flight reads hold `_reads_phaser`, and segment
   freeing already awaits pending reads.
2. **Drop in compaction (opportunistic).** In `do_compaction`'s header
   callback, a record that `is_record_alive` but passes the drop rule (using
   the on-disk header's kind/gc_time) is erased + freed and *not* rewritten
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
seq, `H_ext`, per-table purge bound age (`now − purge_before`), pending
`gc_bytes`, frontier-task bytes rewritten, sweep iterations/duration.
`purge bound age` and `max_seq − F` are the two health signals that show GC
falling behind.

### Configuration

- `logstor_gc_enabled` (bool, default on when the feature ships; off until
  then).
- `gc_snapshot_period` (default ~1 min).
- `gc_sweep_period` (default ~10 min; the sweep is cheap — in-memory).
- `gc_frontier_bandwidth` (bytes/s per shard; bounds forced-compaction write
  amplification).
- Gate 1 uses the existing per-table schema (`gc_grace_seconds`,
  `tombstone_gc` mode); no new per-table options.

## Alternative Gate 2 — mark-and-sweep verification

An alternative implementation of Gate 2 only. Gate 1, the GC metadata, the
two-gate purge rule, post-purge LWW semantics, and the TTL design are
unchanged; what differs is *how* we establish that no stale physical copy of
the key can be resurrected by recovery.

### Principle

The frontier proves no stale copy of `K` is scannable by **destroying
history**: a tombstone may be dropped only after every segment old enough to
hold a stale copy has durably left the scannable set (`F >= max(H_t, H_ext)`).
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
its index call, every index mutation funnels through exactly two functions
(`primary_index::insert` and `primary_index::update_record_location`), and
while a cycle is active each such call for a candidate key simply **cancels
the candidate** — the drop is deferred to a later cycle. This is the
snapshot-at-the-beginning barrier of concurrent GC literature, applied to
physical record copies.

### GC cycle

A per-shard background fiber runs cycles:

1. **Arm the barrier** (below).
2. **Roll the write path**: `await_pending_writes()` plus a separator flush
   (`flush_all_separator_buffers`), so that every record whose index call
   predates the barrier is durable in a segment with `seq <= S0`. Record
   `S0 = max allocated seq`. Note the roll does **not** seal everything: a
   writeable segment may stay open with room, holding pre-arm records and
   still receiving appends — and it must be swept (`seq <= S0`). Scanning an
   actively-appended segment is safe by this argument: a buffer not yet
   durable when the scan reads it contains only records whose index calls
   have not run yet, so those records are covered by the barrier (Case B of
   the proof) rather than by the sweep. The simpler alternative — force-close
   open segments at roll, at a small space cost — is an open decision.
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
6. **Drop**: for each surviving candidate whose index entry still matches
   `(key, location)`: `index.erase` + `free_record`, synchronous per entry,
   chunked with yields (the barrier keeps cancelling not-yet-dropped
   candidates during the yields).
7. Disarm the barrier, discard the table.

### The write barrier

Every mutation of the primary index goes through exactly two functions, and
they are the only hooks needed:

- `primary_index::insert` — user writes (`logstor::write` completion),
  recovery-style inserts, and streamed/restored segments (`load_segment`
  populates the index via `recover_segment`);
- `primary_index::update_record_location` — compaction, separator, and split
  compaction relocations (the three call sites in `segment_manager.cc`).

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
  (the drop step re-verifies `(key, location)`).
- **Migration / streaming / restore.** `load_segment` writes the segment
  locally with a fresh seq, then populates the index via `recover_segment`,
  which calls `insert` **per record** — each record concerning a candidate
  key cancels it. There is no bulk path that makes bytes scannable without
  per-record index calls.
- **Compaction / separator / split-compaction copies** announce themselves
  through `update_record_location`.
- **Records that arrived before the barrier was armed** are covered not by
  the barrier but by the roll + sweep: their index calls predate arming, so
  their bytes lie in sealed segments with `seq <= S0`, all of which the
  sweep reads.

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
  the segment before `recover_segment` inserts each record. No path creates
  scannable bytes silently — this closure property is what makes two hooks
  exhaustive.
- **F2 — choke point.** `primary_index::insert` and
  `primary_index::update_record_location` are the only functions that mutate
  the primary index.

### Invariants

1. **Announcement** (F1 + F2): every physical record's bytes land before its
   index call, and every index call passes through one of the two hooks.
2. **Roll**: after step 2 of the cycle, every record whose index call
   happened before arming resides in a sealed segment with `seq <= S0`.
3. **Sweep coverage**: the sweep reads every scannable segment with
   `seq <= S0` — compaction groups *and* the free list. Supporting lemma:
   the sweep observes a superset of what any future recovery could observe
   from those segments — a segment that becomes unscannable mid-cycle
   (reused, first new chunk durable) is unscannable for recovery too, and
   the pin/skip rule for free-list segments closes the handover race. This
   lemma is the implementation's weakest link and gets its own tests.
4. **Barrier**: from arming to disarm, any index call for a candidate key
   cancels the candidate; the drop re-checks cancellation and performs
   gate-check + erase + `free_record` synchronously, with no yield.
5. **Quiesce**: at drop time, every *relocation copy* physically present has
   already executed its `update_record_location` (in-flight separator and
   compaction writes started before the sweep ended are drained, barrier
   still armed).
6. **Domination** (pre-existing recovery invariant, preserved): whenever a
   freed-physical record of `K` is scannable, the live index entry for `K`
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

- **Case A — before arming.** By invariant 2, `R'` lies sealed in a segment
  with `seq <= S0`; by invariant 3 the sweep saw its header — key `K` at a
  location other than `loc` — and cancelled `T`. Contradiction with `T`
  being dropped.
- **Case B — between arming and `D`.** The barrier (invariant 4) cancelled
  `T`. Contradiction.
- **Case C — at or after `D`.** Sub-cases:
  - *`R'` is a relocation copy written before `D`* — excluded by invariant 5:
    its index call was drained before the drop, placing it in Case B. (This
    is the one race the quiesce step exists for: without it,
    `update_record_location` would run after the erase, fail, and free the
    copy — a scannable record of `K` with no live entry, i.e. a
    resurrection.)
  - *`R'` is a relocation copy started after the sweep* — relocations copy
    only records live at copy time; the only live record of `K` is `T`
    itself (the drop re-verified `(key, loc)`), so `R'` is a copy of the
    tombstone. Resurrecting it re-inserts `T` — benign, re-dropped next
    cycle.
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
  scan + barrier. It requires accurate per-segment accounting of
  never-inserted loser records (today a losing write is simply never added,
  so `free_space` alone does not identify fully-live segments).
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

### Comparison with the frontier

| Dimension | Frontier + horizons | Mark-and-sweep |
|---|---|---|
| Proof shape | inductive: everything old is durably unscannable | direct: scanned everything, saw nothing; barrier keeps it true |
| Extra writes | forced compaction of all segments below the horizon; 4K invalidation per freed segment | none |
| Extra reads | none beyond forced compaction | header scan of scannable segments per cycle |
| A 95%-live cold segment | must be **rewritten** for `F` to pass it | merely **read** |
| Drop latency after grace | until `F` passes the pre-grace horizon (compaction-bandwidth-bound) | one mark+scan cycle (read-bandwidth-bound) |
| Drop granularity | continuous as `F` advances; plus opportunistic drop in compaction | batch at end of each cycle; no drop-in-compaction |
| Per-shard GC state | O(1): `F`, snapshot ring, `H_ext`, scannable set | candidate table (~32 B/candidate, capped); big backlogs need several cycles |
| Write hot path | untouched (`H_ext` bump on loser completions only) | branch + hash probe per index mutation while a cycle runs |
| Coupling | GC progress tied to compaction (frontier task, separator nudge, invalidate-on-free ordering) | self-contained fiber; two index hooks + recovery's scan path |
| Crash | recompute; GC resumes after a full frontier pass | stateless; aborted cycle restarts |
| Failure modes | `F` stuck behind cold/mixed segments; WA spikes | scan bandwidth; candidate cap; churn-cancel starvation |

The decisive cost difference is the fourth row. Under the frontier, *any*
scannable segment below the horizon must be rewritten — including cold
segments that are 95% live and that utilization-based compaction would never
touch; the effective-frontier refinement exempts only *fully*-live segments.
Under steady scattered overwrites nearly every old segment has some freed
records, so a frontier pass approaches a full-disk **rewrite** while a
mark-and-sweep cycle approaches a full-disk **read** — cheaper at the device
level, no flash wear, no free-space consumption, and easily rate-limited
without starving GC (read bandwidth is far easier to provision than
compaction write bandwidth).

The frontier's structural advantages are the O(1) memory (no cap, no
multi-cycle backlog draining under mass deletes), continuous rather than
batched drops, the opportunistic drop-in-compaction path, and an untouched
write hot path.

Starvation differs in kind: the frontier can be *stuck* (a cold segment holds
`F` back indefinitely without the frontier task); mark-and-sweep can only be
*deferred per key* (a candidate whose key keeps getting touched — e.g. its
tombstone keeps being relocated by compaction — is cancelled each cycle;
updating the candidate's location on `update_record_location` instead of
cancelling removes the common case).

### Revised implementation plan (mark-and-sweep)

Compared with the frontier plan: Stage A shrinks in role but stays — on-disk
`kind`/`gc_time` no longer feed a drop-in-compaction decision, their only
remaining consumer is recovery (rebuilding index metadata without
deserializing mutations); `segment_descriptor::seq` is needed early (the
sweep enumerates the scan set by `seq <= S0`), while `gc_bytes`/`min_gc_time`
move out of the critical path to TTL triggering. Stage B is replaced by the
phases below; Stages C and D are unchanged. Not needed at all: scannable
set, `F`, snapshot ring, `H_ext`, frontier task, invalidate-on-free (the
latter optionally kept later as a scan-set reduction).

Each step is independently mergeable, changes no behavior until S7's final
sub-step, and carries its own verification. The three riskiest components
(scan coverage, quiesce completeness, hot-path cost) ship first, dormant and
individually verifiable.

**Phase 0 — groundwork (zero behavior change):**

- **S1. Online scrubber.** The sweep built as a standalone diagnostic:
  enumerate scannable segments (compaction groups **and** the free list,
  with pin/skip reuse handling), header-only rate-limited scan
  (`scan_segment` with `want_data::no`), no GC logic. Also adds
  `segment_descriptor::seq`. *Verify:* recovery-equivalence test (the online
  scan observes a superset of what an offline recovery scan of the same disk
  sees — invariant 3's lemma); a verification mode checking the domination
  invariant (every live index entry's location is seen; every other sighting
  of a key is dominated by its live entry). Yields a permanent
  scrub/consistency tool regardless of the Gate 2 decision.
- **S2. Relocation phaser (quiesce primitive).** Wrap every physical-copy →
  `update_record_location` window (compaction, separator, split compaction)
  in a per-shard phased barrier; expose `await_relocations()`. *Verify:*
  unit test with injected completion delays; a debug assertion that
  `update_record_location` is never called outside the phaser — turning
  "did we enumerate all windows?" into a runtime check instead of a review
  question.
- **S3. Barrier skeleton.** Armed-flag + probe call sites in `insert` /
  `update_record_location` with a pluggable consumer (initially a counter).
  *Verify:* write-path microbenchmark armed vs. disarmed against a synthetic
  candidate table — producing the hot-path cost number the mechanism
  decision partly hangs on, before any GC code exists.

**Phase 1 — metadata (Stage A, trimmed):**

- **S4. On-disk metadata.** `kind` + `deletion_time` + `expiry` in
  `record_header` with the local-time clamp; buffer `version` bump;
  write-path classification. *Verify:* serialization round-trip;
  classification tests (live / delete / expiring / mixed-TTL / backdated →
  clamp).
- **S5. Index metadata.** `kind` (2 bits) + 32-bit `gc_time` in
  `index_entry`, populated on write and on recovery (from the S4 header
  fields). *Verify:* recovery rebuild tests; per-key memory-budget
  measurement (target: fit in existing padding).

**Phase 2 — GC assembly:**

- **S6. Candidate table.** Pure data structure: key-hash keying,
  cancel-on-collision safety, memory cap, cancellation metrics by source.
  Fully unit-testable in isolation.
- **S7. Cycle fiber — dry-run first.** Wire roll → mark (chunked index walk,
  Gate 1 in `timeout` mode) → sweep (S1) → quiesce (S2) → **report**:
  log/metric the would-drop survivors without erasing anything. *Verify:*
  production-like stress with survivors cross-checked against ground truth
  (full-data audit in tests). Only then enable the actual erase +
  `free_record` behind `logstor_gc_enabled`. Config: `gc_scan_bandwidth`,
  `gc_candidate_limit`, cycle period.
- **S8. Resurrection kill-suite.** Error-injection kills at every cycle
  stage, recover, assert the key stays deleted; backdated write during the
  sweep; relocation straddling the drop; `load_segment` mid-cycle;
  candidate-cap overflow across cycles; table drop / `discard_segments`
  mid-cycle. Written against the drop-rule *interface*, so it survives a
  Gate 2 mechanism switch.

**Phase 3 — TTL** (C1–C4 as in the main plan; the `gc_bytes`/`min_gc_time`
accounting lands with C4). **Phase 4 — repair-aware grace** (Stage D,
unchanged).

### Risks

1. **Roll semantics (open segments).** The roll flushes buffers but does not
   seal open writeable segments, which hold pre-arm records, keep receiving
   appends, and must be swept. Safety of scanning them rests on the
   not-yet-durable-buffer argument in the cycle description (step 2); the
   force-close alternative trades a little space for a simpler invariant.
   Decide before S1.
2. **Quiesce completeness is enumerative.** A missed relocation window is a
   silent resurrection path. S2's phaser plus the runtime assertion is the
   structural fix; the assertion is mandatory, not a nice-to-have.
3. **Cancellation storms.** A compaction that rewrites a tombstone-dense
   segment mid-cycle relocates many candidates and cancels them all — GC
   starves precisely on the segments most worth reclaiming, and relocation
   is the *common* touch of a cold tombstone (far more common than backdated
   writes). Updating the candidate's location on `update_record_location`
   instead of cancelling is therefore a likely requirement, not just a
   refinement (the old location becomes a freed copy of the tombstone
   itself, whose resurrection is benign).
4. **Long armed windows.** Cycle duration is scan-bound; on large disks the
   barrier is armed for hours, so the hot-path probe is effectively
   permanent and cancellation exposure grows with cycle length. S3's early
   measurement and the per-source cancellation metrics decide whether a
   bloom prefilter and the cancel→update change are needed.
5. **Candidate memory.** The cap means mass-delete backlogs drain over
   several cycles, each a full scan; needs a dedicated stress test (S8) and
   sizing guidance for `gc_candidate_limit`.
6. **Gate 1 timing** (no risk, for completeness): candidates are gated at
   mark time; `gc_before` is monotone, so mark-time evaluation only
   under-selects — never unsafe.

### Open questions (mark-and-sweep)

- Roll semantics: force-close open segments vs. the open-segment scan
  argument (risk 1) — decide before S1.
- Measured hot-path cost of the barrier probe on the write path; whether a
  bloom prefilter is needed (S3 answers this).
- Candidate cap default and behavior under mass-delete backlogs (cycles to
  drain vs. scan cost).
- Free-list scan vs. reuse coordination (pin the segment during its scan, or
  skip and rely on the barrier).
- Per-segment accounting for never-inserted losers, needed by the
  skip-fully-live optimization.

## Alternatives considered

- **Durable liveness checkpoint** (previously "candidate 2b"): persist an
  index/segment-liveness manifest; recovery replays only newer segments, so
  freed records are never re-scanned and Gate 2 holds by construction.
  Strictly stronger and also a recovery-speed improvement, but a much larger
  artifact (checkpoint format, crash consistency, replay). Deferred; the
  frontier machinery remains useful with it (frontier trivially equals the
  checkpoint barrier).
- **Purged-tombstone journal**: on drop, append `(key, timestamp)` to a small
  per-shard journal consulted at recovery; journal entries retire once
  `F >= H`. Decouples index-memory reclamation from frontier progress
  (tombstones leave the index immediately after grace). Middle ground between
  the frontier and the checkpoint; adds a new on-disk artifact. Revisit if
  frontier latency proves painful in practice.
- **Effective frontier** (refinement of the chosen design): let `F` skip
  scannable segments that contain **zero freed records** — a fully-live
  segment cannot contain a stale copy of any key (all its records are the
  unique live versions). Records freed later always have a newer live version
  at freeing time, and tombstone winners bump `H_ext`, so per-drop-time
  evaluation stays sound. This removes most forced-compaction write
  amplification for cold live data. Planned as a follow-up once the basic
  frontier is validated by stress tests, since it weakens the very simple
  "everything old is gone" invariant.
- **Persisting `F`/snapshots**: lets GC resume immediately after restart
  instead of waiting for a fresh frontier pass. Pure optimization with crash-
  consistency cost; deferred.

## Implementation plan

This staged plan implements Gate 2 with the **frontier** mechanism. If the
**mark-and-sweep** alternative is chosen instead, Stage B here is superseded
by the [revised plan](#revised-implementation-plan-mark-and-sweep) in the
alternative section (Stages A, C, D remain, with Stage A trimmed as
described there).

Stages are independently mergeable; tombstone GC (B) and TTL (C) are
independent after A. Each stage lands with its metrics and tests.

### Stage A — GC metadata foundation

- **A1. On-disk metadata.** `kind` + `deletion_time` + `expiry` in
  `record_header` (IDL) with the local-time clamp; buffer `version` bump;
  write-path classification (`extract_logstor_record_timestamp` grows into a
  `classify_record` returning timestamp + gc info). Recovery/compaction/
  separator pass the fields through unchanged (`log_record_bytes_writer`
  already preserves header bytes).
- **A2. Index metadata.** `kind` (2 bits) + 32-bit `gc_time` in `index_entry`,
  populated on write and on recovery; memory-budget check; kind-aware cache
  eviction hook for future conversion.
- **A3. Segment accounting.** `seq`, `gc_bytes`, `min_gc_time` in
  `segment_descriptor`, maintained on write/free and rebuilt in recovery;
  gauges.
- **Tests:** header serialization round-trip; classification (live delete /
  expiring / mixed-TTL / backdated timestamps → clamp); accounting under
  write/overwrite/free/recovery.

### Stage B — tombstone GC (timeout mode)

- **B1. Scannable set + frontier.** Track seqs of scannable segments;
  invalidate-on-free (shared helper with `discard_segments`); `F` advances on
  completed invalidation; recovery rebuilds the set from scanned headers
  (including segments that end up in the free list — invalidate those during
  recovery). Metrics: `F`, `max_seq`.
- **B2. Horizons.** Snapshot ring; `H_ext` bumps at the three loser-completion
  sites (winner-kind check via the index), in `load_segment`, and at recovery
  end; per-table purge bound.
- **B3. Drop paths.** GC sweep fiber (chunked index iteration, drop rule,
  erase + free); opportunistic drop in `do_compaction`/`do_split_compaction`
  header callbacks; config knobs; drop/reclaim metrics.
- **B4. Frontier task.** Forced lowest-seq compaction, rate-limited;
  separator nudge; pending-GC estimate to avoid idle rewriting.
- **Tests:**
  - unit: write→delete→grace→sweep drops entry and bytes; Gate 1 boundary;
    Gate 2 blocks until frontier passes; purge bound math.
  - resurrection (the critical suite): delete, GC, **kill at every stage**
    (before/after invalidation of old segments, before/after tombstone-segment
    free) via error injection, recover, assert the key stays deleted. Repeat
    with a freed old copy parked in a high-seq segment (separator/compaction
    loser), with a backdated delete, and with a late losing write after grace.
  - streaming interplay: stream in a segment with an old copy while a local
    tombstone is pending drop; assert `H_ext` defers the drop; kill + recover.
  - liveness: cold fully-live segments + deletes → frontier task advances `F`
    and GC completes within the bandwidth budget.

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

- Wire `tombstone_gc_state` / `get_gc_before_for_key` into snapshots (per-table
  `gc_before` captured at snapshot time) and the sweep; support
  `repair`/`immediate`/`disabled`; cluster test with repair-mode GC.

### Stage E — optional follow-ups

- Effective frontier (skip fully-live segments).
- Slim *user* deletes (write deletes header-only from the start).
- Purged-tombstone journal or durable liveness checkpoint (also a
  recovery-speed win); persist `F`/snapshots.

## Open questions

- **Which Gate 2 mechanism** — frontier + horizons vs. mark-and-sweep
  verification (see the [alternative
  section](#alternative-gate-2--mark-and-sweep-verification) and its
  comparison table). The main trade: forced-compaction write amplification
  and compaction coupling (frontier) vs. full-disk read cycles, a capped
  candidate table, and a write-path barrier probe (mark-and-sweep).
- Exact packing of `kind`/`gc_time` in `index_entry` vs. the
  `primary_index_entry` flag bits — decide when measuring the memory budget
  (target: fit in existing padding).
- Defaults for `gc_frontier_bandwidth` and sweep/snapshot periods — pick after
  the Stage B stress runs.
- Whether the empty-payload slim tombstone can replace payload-bearing deletes
  everywhere without read-path regressions (Stage C decision point).
