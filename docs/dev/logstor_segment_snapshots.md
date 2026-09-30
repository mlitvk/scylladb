# Logstor Segment Snapshots and Pinning

## Status

Implementation plan. This is **stage 0** of the logstor backup/restore effort
(`logstor_backup_restore.md`), but it contains **no backup code** and is
independently shippable: it fixes a pin-cost problem logstor has *today* in
tablet streaming, and tablet streaming is its first and only consumer.

The mechanism it builds — a *snapshot cursor* with demand-driven **rescue
pinning** — is the same mechanism backup will consume later, with no semantic gap
to close: it provides the strong invariant *every record live at snapshot time is
consumed at least once*, which is what backup needs and what streaming needs.

For the general logstor architecture see [logstor.md](logstor.md); for the
compaction cost model see `logstor_compaction.md` (on the
`logstor_compaction_threshold` branch, not yet on master); for how this fits
into backup/restore see [logstor_backup_restore.md](logstor_backup_restore.md).

**Base branch.** Build this on `logstor_recovery1` (design in
`logstor_discard.md`), not on master. That branch
replaces `discard_segments` with `erase()` + `discard_group_segments`, which is
the teardown path this design has to hook (see "Group teardown"), and it adds
the body CRC and header-last streaming that the restore path of the backup
design relies on. The tombstone GC design (`docs/dev/logstor_gc.md` on
`logstor_gc`) changes nothing in the mechanism but is affected by how long a
pin is held — see "Interaction with `logstor_recovery1` and `logstor_gc`".

Reviewed against the tree on 2026-10-01 (master `6e4f4fa3be`). Line numbers
are from that tree and drift; function names are the durable reference.

## Problem

`table::take_logstor_snapshot(token_range)` (`replica/table.cc:1681`) is the only
way to enumerate a token range's durable segments. It funnels into
`segment_manager_impl::make_snapshot(cg)` (`segment_manager.cc:2800`), which pins
**every** segment of the group with a `segment_ref` and returns them all in one
`chunked_vector<segment_snapshot>`.

Its only caller today is `tablet_stream_files_handler`
(`streaming/stream_blob.cc:775`), which holds that whole-tablet pin set for the
entire transfer while it streams segments one at a time
(`stream_blob.cc:578`). Two things go wrong.

### 1. Pins consume allocation capacity, not just disk

Logstor's disk budget is a fixed **segment count**: `_max_segments` is derived
from `logstor_disk_size_in_mb` at construction (`segment_manager.cc:1280`) and
`available_segment_count()` (`:1101`) is the free list plus the segment pool plus
the never-yet-allocated tail. A pinned segment is subtracted from that pool. When
the pool empties, `allocate_segment()` (`:1732`) falls into a 5-second retry wait
(`:1770`) — a write stall.

A 5 GiB tablet is ~40,960 segments; on a shard with 128 GiB of logstor disk that
is ~4% of the pool, against a default `logstor_compaction_trigger_threshold` of
0.05 — i.e. one tablet's pins are most of the free pool compaction is supposed
to maintain. One tablet is survivable only because compaction runs flat out to
compensate. Nothing bounds how many tablets stream concurrently from one shard.

### 2. Compaction is pin-blind, which closes a positive feedback loop

`segment_set::add_segment` (`compaction.hh:287`) contributes one `ref_count`, and
`remove_segment` (`:300`) gives it back. `make_segment_ref`
(`segment_manager.cc:1145`) adds one more per outstanding `segment_ref`. The
compaction free loop (`:2324`) is:

```cpp
ss.remove_segment(desc);
if (desc.ref_count == 0) {
    _sm.free_segment(seg_id);
}
```

So compacting a pinned segment removes it from its group, rewrites its live
records into **new** segments, and frees **nothing**. And candidate selection
cannot tell: `select_segments_for_compaction` (`:2006`) delegates to
`select_compaction_batch` (`compaction.cc:109`), which walks the group's
segments in ascending utilization order, scores every prefix by `n_in - n_out`
estimated from live bytes, and never looks at `ref_count`.

The loop is therefore: pinning lowers `available_segment_count`, which pushes
`should_run_auto_compaction()` (`:2073`) below the low watermark and raises
`compaction_pressure()` (`:1215`) so compaction CPU shares ramp up; compaction
picks segments from the pinned group believing it will reclaim `n_in - n_out`,
reclaims zero, and consumes `n_out` fresh segments; that lowers
`available_segment_count` again, keeping `while (... && should_run_auto_compaction())`
(`:2119`) spinning. A stream can drive a compaction storm that burns IO and CPU at
escalating shares while making free space strictly *worse*.

### 3. Bookkeeping memory

Each `segment_snapshot` (`segment_manager.hh`) holds a `segment_ref` whose
shared state carries two `std::function`s (`compaction.hh:371`) plus the source
`noncopyable_function` — roughly 150–200 bytes. ~6–8 MB per 5 GiB tablet, ~80 MB
for a 50 GiB whole-shard snapshot.

## Constraints from the implementation

These are the facts the design has to respect; all were verified against the
current tree.

1. **A hard link is not a snapshot here.** `segment_id → (file_id, offset)` is
   fixed arithmetic (`segment_id_to_file_location`) and reusing a segment id
   overwrites that file region. Isolation requires that the segment not be
   reused; nothing else provides it.
2. **`free_segment` (`:1781`) hard-asserts** `record_bytes == 0` (`:1791`) and
   `ref_count == 0` (`:1794`) with `on_fatal_internal_error`. Anything that keeps
   a segment alive must keep its `ref_count` above zero, not bypass the free
   path.
3. **The compaction free loop is synchronous.** Between the end of the
   `with_closeable` scope that returns the compaction buffer (`:2311`) plus
   `await_pending_reads()` (`:2319`) and the loop at `:2324` the only yield is
   the test-only `logstor_compaction_wait_before_remove_segments` injection
   (`:2320`). A hook that runs *before* the loop therefore sees a stable input
   set and needs no locking.
4. **Split compaction has two paths.** The fast path (`:2378-2386`) *moves* a
   segment to a child group — `remove_segment` then `add_logstor_segment`, with
   no free. The slow path frees at `:2437-2444`. Only the slow path destroys
   segments.
5. **Mixed (active) segments are never in a group.** They are held alive purely by
   `segment_ref`s — the `writeable_segment`'s own ref (`get_segment`, `:1165`)
   and the separator buffer's `held_segments` (`compaction.hh:410`) — and freed
   by refcount once separated. Compaction only ever iterates
   `cg.logstor_segments()`, so mixed segments are never compaction candidates
   and pins on them never interact with candidacy.
6. **A `segment_ref` is a shared handle.** `make_segment_ref` increments
   `ref_count` once; copies share the state. So `ref_count` counts *group
   membership plus distinct `segment_ref` states*, not copies.
7. **`segment_set::clear()` (`compaction.hh:308`) deliberately does not
   decrement.** Shutdown leaves group-resident segments at `ref_count >= 1`
   forever, so an outstanding pin at shutdown cannot cause a free.
8. **Group teardown destroys data deliberately.** On master `discard_segments`
   (`:1807`) requires `ref_count == 1` (`:1828`, `on_internal_error`) — group
   membership only. On `logstor_recovery1` the path is `erase()` →
   `discard_group_segments`, which instead *leaves a referenced segment to its
   references* (detaches it from the group and lets the last `segment_ref`
   free it) and zeroes the first block of the unreferenced ones. Either way it
   is the one eviction a snapshot must not survive: the records are being
   erased, so a snapshot still pending on them has to fail rather than deliver
   them.

## Design

### The invariant

The current design conflates two different needs:

- protecting *bytes under an active reader* needs a pin, but only for the
  duration of that read;
- protecting *the coverage claim* — "I will deliver all of this data" — needs
  bookkeeping, not a pin.

Separate them:

> Register the snapshot's segment ids in a shard-level pending registry.
> Enumerate ids cheaply, without pins. Pin only the segment currently being
> consumed. **When compaction is about to free a segment that is still pending,
> pin the original segment instead of letting it go.**

Because the snapshot keeps the *original* segment byte-for-byte, nothing filters
its contents, and the invariant is the strong one:

> **Every record live at snapshot time is consumed at least once.**

Duplicates are harmless: the consumer resolves by `(timestamp, seq, offset)` in
the `primary_index`, exactly as recovery and the streaming receive path already
do (`load_segment`, `segment_manager.cc:2950`).

Mechanically the rescue is cheap. Pinning a pending segment before the free loop
leaves `ref_count` at 1 after `remove_segment`: the segment is **detached from
its group but stays allocated with its bytes untouched**. The consumer reads it
later through `create_segment_input_stream` (`:2917`), a plain file-region read
that needs no group. On release `ref_count` reaches 0 and `free_segment` runs;
its `record_bytes == 0` assertion holds because compaction already rewrote the
live records and every index update fired `on_free_record` for the old location.

Cost is demand-driven: a pin is taken only at the moment compaction would
otherwise destroy the data.

- *Worst case* — compaction sweeps the whole tablet during the operation:
  degrades to today's whole-tablet pinning. Never worse.
- *Common case* — compaction touches few pending segments: near-zero pinning.
- The pending set is **monotonically shrinking**, so termination is trivial.

### Why not chase compaction's output instead

The alternative hook substitutes compaction's *output* ids into the pending set
rather than pinning the input. It never grows the pinned set, but it provides only
a weaker invariant, because compaction filters by *current* liveness
(`is_record_alive`, `:2163`): a key live at snapshot time whose new version lands
outside the snapshot set is dropped from the rewritten segment, so the chase
delivers **no version of that key at all**. Tablet streaming happens to tolerate
this — the `streaming` stage runs inside `write_both_read_old`, so concurrent
writes reach the pending replica independently — but backup does not, and the
whole point of stage 0 is to build one mechanism both can use. Rescue-pin is
therefore the only policy implemented. The full comparison, including the
rejected snapshot-scoped-GC-grace alternative, is in
[logstor_backup_restore.md](logstor_backup_restore.md).

### Component design

#### `external_ref_count` on `segment_descriptor`

`ref_count` cannot distinguish a snapshot pin from group membership or from the
transient `writeable_segment` ref that a freshly written segment carries between
`get_segment` (`:1165`) and the end of `write_full_segment` (`:1580-1622`; the
segment is added to the group at `:1621`, before the writeable ref drops). Add a
second counter incremented **only** by snapshot pins:

```cpp
// compaction.hh, segment_descriptor
uint16_t external_ref_count{0};   // snapshot pins only
uint16_t pending_snapshot_count{0}; // registered in a snapshot, not yet consumed
```

Give `make_segment_ref` a kind parameter (`segment_ref_kind::internal` default,
`::external` for snapshots) so the external counter is incremented and
decremented on the same paths as `ref_count`.

`pending_snapshot_count` exists so the two hot checks — "is this segment
pending?" in the free paths, and candidate scoring — are a single load with no
hash lookup in the overwhelmingly common case of no active snapshot.

Layout (`compaction.hh:211`): `segment_descriptor` is 56 bytes. Its
`log_heap_hook` base is two list pointers plus a one-byte `cached_bucket`
(the histogram has fewer than 256 buckets) at offset 16, then tail padding to
24; the descriptor's own members — `free_space`, `record_count`, `owner`,
`ref_count`, `index_in_set` — fill 32 bytes with no padding. The only free
bytes are the base's tail padding, offsets 17–23. The Itanium ABI lets a
derived class place members there because the base is not POD-for-layout, but
only for members declared **first** in `segment_descriptor`, ahead of
`free_space`. So declare the two `uint16_t`s first and pin
`sizeof(segment_descriptor) == 56` with a `static_assert` — there are ~1M
descriptors per shard at 128 GiB, so growing to 64 bytes costs 8 MB per shard.
If the assert does not hold, fall back to a single shard-level "any snapshot
active" counter guarding the map lookup.

Both counters are `uint16_t`: assert on increment that they do not wrap — a
silent wrap to zero would corrupt the A1 skip test and the pending checks, the
same class of corruption `free_segment`'s hard asserts guard `ref_count`
against.

#### A1 — pin-aware compaction candidacy

In `select_compaction_batch` (`compaction.cc:109`), the loop over
`segments._segments` skips descriptors with `external_ref_count > 0`:

```cpp
for (const auto& desc : segments._segments) {
    if (candidates.size() >= batch_cap) {
        break;
    }
    if (desc.external_ref_count > 0) {
        continue;   // NOT break
    }
    candidates.push_back(&desc);
    ...
```

`continue`, not `break`: the loop walks the histogram in ascending utilization
order and its only `break` is the batch cap. Skipping a pinned segment must not
truncate the scan, and a skipped segment does not count against the cap. The
prefix-score contract is unaffected — a skipped segment simply is not a
candidate. `select_compaction_batch` is a free function over a `segment_set`,
so the pinned case slots into its existing unit test
(`test_logstor_select_compaction_batch`, `test/boost/logstor_test.cc:2428`).

Compacting a pinned segment is always pure loss: it reclaims nothing and consumes
output segments. This also means a **rescued segment is never re-selected**,
which is what keeps rescue from cascading.

**Do not apply A1 to split compaction.** `do_split_compaction` (`:2340`) loops
`while (!src_segments._segments.empty())` and must drain the source group; skipping
pinned segments would spin forever. It stays correct without A1 because both its
paths remove the segment from `src_segments` regardless of `ref_count` — the fast
path re-homes it into a child group, the slow path detaches it — so the loop still
makes progress. The slow path's free is covered by the rescue hook instead.

Two known consequences to accept and document:

- With a fully pinned group and space pressure, `find_top_compaction_candidates`
  (`:2029`) may return empty, so `run_auto_compaction` (`:2109`) sleeps 100 ms and
  retries (`:2129`) until pressure clears. That is pre-existing behavior for the
  no-candidate case and strictly better than the storm it replaces, but it is a
  poll loop and should be visible in metrics.
- A1 changes an existing test's expectations — see commit 0.1 below.

#### Pending registry and the snapshot cursor

Key the registry **by segment id on `segment_manager_impl`**, not per group:

```cpp
absl::flat_hash_map<uint32_t /* log_segment_id::value */,
                    utils::small_vector<segment_snapshot_set*, 1>> _pending_segments;
```

(`log_segment_id` is a `uint32_t` wrapper with no `std::hash`; key on `.value` or
add a hasher.)

Group-keyed registration would break on split compaction's fast path
(`:2378-2386`), where a segment is *moved* to a child group without being
rewritten: a group-keyed snapshot would stop tracking it and miss a later
compaction in the child. Id-keying makes group membership — and group lifetime —
irrelevant.

New type in `segment_manager.hh`:

```cpp
class segment_snapshot_set {
public:
    // Pins and returns the next segment; nullopt when exhausted.
    // Throws if the snapshot was aborted (budget exceeded, group discarded, OOM).
    future<std::optional<segment_snapshot>> next();

    size_t pending_count() const noexcept;
    size_t rescued_count() const noexcept;

    ~segment_snapshot_set();   // unregisters, drops all pins
};
```

- **Construction** (replacing today's `make_snapshot` body, `:2800`): under the
  existing `disable_compaction(cg)` guard, walk `_segment_list` recording **ids
  only**, no pins, and register them. Registration increments
  `pending_snapshot_count` and appends to `_pending_segments`. Reserve the
  rescued-id vector to the hard cap `H` here, so the rescue pass never
  allocates (see "The rescue hook").
- **`next()`**: hand out **rescued** segments first — their bytes are pure
  liability, already duplicated in compaction's output, so draining them returns
  segments to the free list fastest; the `segment_ref` handed out *adopts* the
  count the rescue already took. Otherwise pop the next pending id,
  `make_segment_ref(id, external)` it, and unregister it from the pending set (it
  is now pinned, and A1 excludes pinned segments from candidacy, so it needs no
  further protection). Return the same `segment_snapshot` struct consumers use
  today, so no consumer changes are forced by the type.
- **Consumption order**: iterate the pending set in the group's
  compaction-candidate order (lowest utilization first, as recorded at
  construction) so the reader tends to win races against compaction. This is a
  free ordering choice at construction time, not a live re-sort.
- **Destructor**: unregister everything still pending, drop all pins (rescued and
  outstanding). Dropping the last pin on a rescued, group-detached segment runs
  `free_segment`, which is exactly the intended reclamation.

#### The rescue hook

```cpp
// segment_manager_impl
void rescue_pending_segments(std::span<const log_segment_id> inputs) noexcept;
future<> await_rescue_budget(std::span<const log_segment_id> inputs);
```

Call sites, both **before** the free loop and after the compaction buffer has
been returned:

- `do_compaction`, after the `logstor_compaction_wait_before_remove_segments`
  injection (`:2320`) and before the loop at `:2324`;
- `do_split_compaction`'s slow path, before the loop at `:2437`. Its fast path
  frees nothing and needs no hook (constraint 4).

`rescue_pending_segments` is synchronous and must not throw. For each input with
`pending_snapshot_count > 0`, each owning snapshot rescues it by incrementing
`ref_count` and `external_ref_count` directly on the descriptor and appending
the id to its rescued-id vector — **not** by calling `make_segment_ref`, whose
shared state holds two `std::function`s and allocates. Because `remove_segment`
then decrements `ref_count` to 1 rather than 0, the segment stays allocated.
The vector was reserved to the hard cap `H` at construction, so the pass is
genuinely `noexcept`. A `segment_ref` is materialized only when `next()` hands
a rescued segment to the consumer, and it adopts the count the rescue took
rather than incrementing again; the destructor releases the counts of rescued
ids that were never handed out and frees any that reach zero. The only way a
rescue cannot be recorded is the cap, and then the snapshot is **marked
aborted** (so `next()` throws and the consumer's operation fails cleanly)
rather than pinning untracked or silently dropping records.

The budget wait is a separate, awaitable step that runs *before* the synchronous
pass, which is what keeps the free loop yield-free (constraint 3).

#### Rescue budget and escalation

Give each snapshot a rescue budget `B` — a configured fraction of
`_max_segments.configured`, default ~1% — and a hard cap `H`.

1. Under `B`: rescue freely.
2. At `B`: `await_rescue_budget` waits on the snapshot's drain condition
   (signalled when `next()` hands out a rescued segment and when a pin is
   released) before the rescue pass. The compacting group is `running()`, so
   `find_top_compaction_candidates` (`:2054`) skips it while it waits — this is
   the "scoped compaction disable" effect, but scoped to *this group* and bounded
   by *drain rate*, not by the whole operation.
3. Past `H`, or on a wait timeout: mark the snapshot aborted, rescue nothing, let
   compaction free normally. The consumer fails and retries. Never simply stop
   rescuing — that silently drops records.

Deadlock analysis to state in the code comment: the waiting compaction fiber has
already returned its compaction buffer to the pool (the `with_closeable` scope
at `:2295-2311`) and holds no segment allocation; the draining consumer only issues file-region
reads through `create_segment_input_stream` and needs neither compaction nor a
free segment. So the wait cannot be satisfied only by the fiber that is waiting.

#### Group teardown

Deliberate destruction of a group's data — tablet cleanup after migration,
truncate, drop — is the one eviction rescue-pin must not answer with a pin: the
records are being erased, so a snapshot still pending on them has to **fail**,
not deliver them.

On master the path is `discard_segments` (`:1807`), which requires
`ref_count == 1` (`:1828`) and `record_bytes == 0` per segment. On
`logstor_recovery1` it is `segment_manager_impl::erase()` →
`discard_group_segments`, which is more forgiving: a segment something else
still references is only taken out of the group and left to its last
reference ("Leaving referenced segment to its references"), which already
covers the segment the consumer is reading and every rescued one — their
records are below the erase watermark, so the last release freeing them later
is correct. What it does **not** cover is the pending, unpinned ids: those are
discarded (first block zeroed), and a later `next()` would pin and read a dead
slot.

So add one hook, called from `erase()` **before** it disables compaction on
the groups (on master: at the top of `discard_segments`, before its validation
pass): abort every snapshot with a pending or rescued id in any of the groups —
mark aborted, unregister its pending ids, release its rescued counts. The
ordering matters twice: `erase()`'s `disable_compaction` waits for a running
compaction, and that compaction may be parked in `await_rescue_budget` waiting
for *this* snapshot's consumer, so aborting first is what wakes it; and marking
aborted before the discard closes the race where a concurrent `next()` hands
out a segment that is about to be zeroed.

Tablet cleanup runs only after streaming has finished (the migration stages
serialize them; `logstor_discard.md`, "Why the streaming send side is safe",
records the argument), so for streaming the abort is a safety net. For backup
it is the normal outcome of a truncate or a tablet migration racing a backup,
and it turns an internal error into a failed, retryable tablet backup.

`logstor_group::clear_segments()` (`compaction.hh:554`, called on shutdown)
needs nothing: per constraint 7 it does not decrement, so an outstanding pin
cannot trigger a free.

## Commit plan

Seven landable commits. Each is independently testable; 0.1 is independently
valuable against today's whole-tablet pinning.

### 0.1 — `external_ref_count` + A1 pin-aware compaction candidacy

- `compaction.hh`: add `external_ref_count` to `segment_descriptor` (`:211`),
  declared first so it lands in the base's tail padding; `static_assert` the
  `sizeof` stays 56.
- `segment_manager.cc`: `make_segment_ref` (`:1145`) takes a
  `segment_ref_kind`; only `make_snapshot` (`:2814`) passes `external`. The other
  two callers — `get_segment` (`:1165`) and the separator path in
  `add_segment_to_compaction_group` (`:2764`) — stay `internal`.
- `select_compaction_batch` (`compaction.cc:109`): skip `external_ref_count > 0`.
- Metric: `pinned_segments` / `pinned_bytes` gauges in the `logstor_sm` group
  (`segment_manager.cc:1334`), backed by a running shard counter rather than a
  scan.
- **Existing test must change.**
  `test_logstor_group_compaction_rewrites_live_records`
  (`test/boost/logstor_test.cc:2505`) takes `old_snapshot` at `:2551`, holds its
  five pins across `submit(cg)`, and asserts the group compacted down to one
  segment. Under A1 that compaction is now correctly skipped. Fix: capture
  `old_segment_ids` via `snapshot_segment_ids` (`:320`) and **destroy
  `old_snapshot` before submitting compaction**. The other two snapshot tests
  (`test_logstor_write_and_separator_flush`, `:2141`, and
  `test_logstor_disabled_group_does_not_compact_on_submit`, `:2652`) are
  unaffected — the first submits no compaction, the second already runs with
  compaction disabled.
- New test: a pinned group yields no compaction candidate, and releasing the pins
  makes it a candidate again (in `test_logstor_select_compaction_batch`).

### 0.2 — Pending registry and `segment_snapshot_set`

- `pending_snapshot_count` on `segment_descriptor`; `_pending_segments` map on
  `segment_manager_impl`; register/unregister helpers.
- `segment_snapshot_set` as above: construction under `disable_compaction(cg)`,
  `next()`, counters, destructor.
- Keep `make_snapshot(cg) -> chunked_vector<segment_snapshot>` as a thin wrapper
  that drains a cursor, so the three existing unit tests keep working unchanged.
  Note in the code that the wrapper reproduces whole-tablet pinning and exists
  for tests and transitional callers only.
- No hook yet, so nothing rescues: at this point the wrapper is the only safe
  consumer. Do not expose the cursor outside logstor in this commit.

### 0.3 — The rescue hook

- `rescue_pending_segments` + `await_rescue_budget`; call sites in `do_compaction`
  (before `:2324`) and `do_split_compaction`'s slow path (before `:2437`).
- Budget/cap configuration knob (see "Configuration" below).
- Abort-on-record-failure and abort-on-cap paths; `next()` throws when aborted.
- **Soft deprioritization of pending segments** (the companion rule to A1 from
  the main design doc): in `select_segments_for_compaction`, skip descriptors
  with `pending_snapshot_count > 0` — but only while enough other candidates
  exist; fall back to including them rather than returning no candidate, so a
  fully pending group can still be compacted (rescue keeps that correct).
  Best-effort only: it reduces rescues in the common case and must not become a
  hard exclusion, which would recreate an unbounded compaction disable.
- After this commit the cursor is safe for a real consumer.

### 0.4 — Group-teardown abort

- The abort hook of "Group teardown": in `erase()` before its
  `disable_compaction` on `logstor_recovery1` (at the top of `discard_segments`
  if this is ever rebased onto master instead).
- Tests: erasing a group with an open cursor fails the cursor's next `next()`;
  a rescued segment of that group is released by the abort and freed; the
  segment the consumer holds at that moment is left to its reference and freed
  on release (the recovery1 path) rather than tripping an internal error; a
  compaction parked in `await_rescue_budget` on that snapshot wakes up and the
  erase completes.

### 0.5 — Token-range cursor on `table`

- `table::make_logstor_snapshot_cursor(dht::token_range)` alongside
  `take_logstor_snapshot` (`replica/table.cc:1681`): `flush_to_separator()`,
  then flush the range's storage groups' separators, then build one cursor
  spanning their compaction groups.
- It must hold `get_sstable_list_permit()` and the storage-group async gates for
  the **whole iteration**, not just construction. Today
  `take_logstor_snapshot` drops the permit at the end of each loop iteration
  (`table.cc:1688`), which is fine only because it has already pinned everything.
- Preserve `storage_group::take_logstor_snapshot`'s group selection
  (`table.cc:1268`): `_main_cg`, then the non-empty merging groups, then the
  split-ready groups.
- Because the registry is id-keyed, the cursor does not need the groups to stay
  alive for correctness — but the gates keep the *table* alive for `next()`.

### 0.6 — Producer-based `tablet_stream_files`, wired to the cursor

The sender currently takes a fixed `std::list<stream_blob_info>` and loops
(`stream_blob.cc:544`, `:578`). Add an overload taking

```cpp
using stream_blob_producer =
    noncopyable_function<future<std::optional<stream_blob_info>>()>;
```

and reimplement the list version as a wrapper over it.

- The wire protocol needs no change: sinks are created per file inside the loop
  and no global file count is sent.
- The `sources.empty()` early return (`:550`) becomes "pull the first item; if
  none, return 0".
- Logging at `:764` and `:768` uses `sources.size()` and `sources` after the loop
  (where the list has already been emptied by `std::exchange`, `:580`); replace
  with a running counter.
- `tablet_stream_files_handler` (`:775`): the logstor branch (`:785-797`) builds a
  producer that pulls from the cursor and wraps each `segment_snapshot` in a
  `stream_blob_info` exactly as today. Keep `table_stream_op` and the reader
  permit alive for the whole call — unlike the sstable path (`:884`), the logstor
  path cannot release the table early, because `next()` calls back into the
  segment manager.
- The `files.empty()` early return at `:889` and the `files_nr` logging at `:897`
  move to the producer's running count.
- The sstable branch keeps the list overload with zero behavior change.

### 0.7 — Metrics, injection points, tests

Metrics in the `logstor_sm` group (`segment_manager.cc:1334`):

- `pinned_segments` / `pinned_bytes` gauges (from 0.1),
- `snapshot_pending_segments` gauge,
- `snapshot_rescued_segments` counter,
- `snapshot_rescue_budget_waits` counter,
- `snapshot_aborts` counter, by reason.

Tests are listed in the next section.

### Not in stage 0

- **A2** (a shard-level pinned-segment admission semaphore): the cursor already
  bounds streaming to one segment plus rescues. A2 is a backup-fan-out concern.
  Note that when it is added it must be sized in units of *rescued + in-flight*
  segments, i.e. against the rescue budget, **not** against a tablet's segment
  count — the latter is the whole-tablet-pin sizing and would defeat the cursor.
- **A5** (abort under global space pressure) — backup-side policy.
- The **chase variant** and the compaction-output-id plumbing it would need.
  Nothing in the backup plan requires it, and it provides only the weaker
  invariant.
- **A4** as a standalone stopgap; it survives only as the budget's escalation
  step (step 2 above).

## Configuration

One new option, live-updatable, in `db/config.cc` next to the existing
`logstor_compaction_*` knobs (`:986`):

- `logstor_snapshot_rescue_budget` (default ~0.01): the fraction of
  `_max_segments` one snapshot may hold as rescued segments before compaction of
  that group waits for the consumer to drain. A hard cap can be derived from it
  (e.g. `4 x budget`) rather than exposed separately.

## Testing

Unit tests in `test/boost/logstor_test.cc`:

- **A1 candidacy** (0.1): a pinned group produces no compaction candidate;
  releasing the pins restores candidacy. Plus the
  `test_logstor_group_compaction_rewrites_live_records` fix described above.
- **Cursor equivalence** (0.2): draining a cursor yields the same segment id set
  as `make_snapshot` on a quiescent group.
- **Strong invariant under churn** (0.3): open a cursor; force compaction of
  still-pending segments **including keys overwritten mid-run**; drain the cursor;
  assert **every** record live at snapshot time appears, with no exception for
  overwritten keys. This is precisely the case where unpinned enumeration without
  the hook silently loses data, and where the chase variant would lose a key
  entirely.
- **Split compaction** (0.3): the same, driving `do_split_compaction` so both its
  fast path (segment moved between groups — id-keying must keep tracking it) and
  its slow path (segment rewritten and freed — the hook must rescue it) are
  exercised.
- **Rescued segments are reclaimed** (0.3): after the cursor is drained and
  destroyed, the rescued segments return to the free list and
  `available_segment_count` recovers.
- **Budget escalation** (0.3): exceeding the budget makes compaction wait for the
  consumer rather than dropping records; exceeding the hard cap aborts the
  snapshot rather than dropping records.
- **Pin accounting** (0.3): in the steady state with no compaction, pinned bytes
  are one segment, not a whole group.
- **Group teardown** (0.4): discarding a group with an open cursor aborts the
  cursor instead of tripping the `ref_count == 1` internal error.

Topology test (0.6): migrate a logstor tablet while forcing source-side
compaction, using the existing `wait_before_tablet_stream_files_after_snapshot`
(`stream_blob.cc:787`) and `logstor_compaction_wait_before_remove_segments`
(`segment_manager.cc:2320`) injection points. Assert that destination data matches
the source, and that source `free_segments` does not fall monotonically for the
transfer's duration — the observable signature of the storm this work removes.

## Verify before starting

- **Teardown on the base branch.** The design above is written for
  `logstor_recovery1`'s `erase()` / `discard_group_segments`. Re-read them at
  implementation time: the "leave referenced segments to their references"
  rule is what the in-flight pin and the rescued set rely on, and the abort
  hook has to run before `erase()` disables compaction.
- **A rescued (group-detached, pinned) segment surviving a crash.** Its records
  are duplicated in the compaction output with a higher seq, and
  `add_segment_to_compaction_group` (`:2736`) re-attaches it as a full segment
  whose records are all dead, which compaction then frees. This is the same
  shape as the deliberate leak on the `set_flush_failure` path (`:1611`), so it
  should be a no-op — verify with a recovery test, on the base branch with the
  `recovery_divergence()` harness (`test/lib/logstor_test_utils.hh` there).
- **`sizeof(segment_descriptor)`** stays 56 with the two counters declared
  first; pin it with a `static_assert`.
- **The `segment_ref` that adopts a rescue's count.** Today's `segment_ref`
  always increments in `make_segment_ref`; the adopting constructor and the
  external-kind release lambda (which decrements both counters) are new.

## Interaction with `logstor_recovery1` and `logstor_gc`

Neither branch changes the mechanism; both change its surroundings.

`logstor_recovery1` (design in `logstor_discard.md`; Phases 0–3
implemented, not merged as of this review):

- **Teardown** is `erase()` + `discard_group_segments`, handled in "Group
  teardown". Its "leave referenced segments to their references" rule is what
  lets the consumer's in-flight pin and the rescued set survive an erase; only
  the pending ids need the abort.
- **Rules P2/P3** (erase implies a watermark; publish before free) are
  untouched: rescue keeps a segment allocated and byte-stable and frees it only
  through `free_segment`, and a rescued segment's records are all dead in the
  index by the time it is detached.
- **A rescued segment surviving a crash** is benign there too, and its seq
  still bounds the recovered `_next_segment_seq` (Invariant Q) like any other
  reachable chunk.
- **Body CRC** makes a header-only read of a pending segment safe even if the
  slot was reused underneath (the seq mismatch or the CRC rejects it) —
  relevant if a consumer ever wants the seq before pinning.

`logstor_gc` (`docs/dev/logstor_gc.md` there, design only):

- **The frontier.** Gate 2's frontier `F` is the lowest seq of any scannable
  segment, and a pinned or rescued segment is scannable and cannot be rewritten
  by the frontier task (A1 skips it). Every pin therefore holds tombstone GC
  back on the whole shard for as long as it is held. The cursor bounds that to
  the consumed segment plus the rescued set, i.e. to the drain rate — one more
  reason whole-tablet pinning must not survive — and the GC design's purge
  journal removes the dependence of drops on `F` altogether. Nothing to do
  here beyond keeping the rescue budget small; it is recorded in the backup
  design as an input to the GC design's Stage B exit decision.
- **Invalidate-on-free.** GC zeroes a freed slot's first block through a queue
  before the slot is reusable. Releasing a rescued segment goes through
  `free_segment` and inherits that, which retires the crash-survival case above.
- **`segment_descriptor::seq`** (GC stage A0) is the cheap seq the backup
  design wants for its seq-based filters; the cursor itself does not need it.
- **Drop in compaction** means a rescued original may hold a tombstone the
  compaction output dropped. The consumer gets the tombstone; that is the
  conservative direction (a tombstone only hides).

## What backup reuses

Nothing in stage 0 knows about backup, but everything backup needs from the
logstor side is here:

- `table::make_logstor_snapshot_cursor(token_range)` is the enumeration primitive
  the node-side backup worker drains, one segment at a time, instead of pinning a
  tablet for the duration of a throttled object-storage upload. Its abort (on
  erase, on the rescue cap, on a wait timeout) is what turns a truncate or a
  tablet migration racing a backup into a failed, retryable tablet backup.
- The strong invariant is what makes uploading from an unpinned enumeration
  correct; without it, backup would have to fall back to whole-tablet pinning or
  to a materialized on-disk snapshot.
- The rescue budget and its escalation ladder are the residual-cost bound that
  backup's own admission control (A2) composes with.
