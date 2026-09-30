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
compaction cost model see [logstor_compaction.md](logstor_compaction.md); for how
this fits into backup/restore see
[logstor_backup_restore.md](logstor_backup_restore.md).

## Problem

`table::take_logstor_snapshot(token_range)` (`replica/table.cc:1641`) is the only
way to enumerate a token range's durable segments. It funnels into
`segment_manager_impl::make_snapshot(cg)` (`segment_manager.cc:2647`), which pins
**every** segment of the group with a `segment_ref` and returns them all in one
`chunked_vector<segment_snapshot>`.

Its only caller today is `tablet_stream_files_handler`
(`streaming/stream_blob.cc:770`), which holds that whole-tablet pin set for the
entire transfer while it streams segments one at a time
(`stream_blob.cc:562`). Two things go wrong.

### 1. Pins consume allocation capacity, not just disk

Logstor's disk budget is a fixed **segment count**: `_max_segments` is derived
from `logstor_disk_size_in_mb` at construction (`segment_manager.cc:1342`) and
`available_segment_count()` (`:1088`) is the free list plus the segment pool plus
the never-yet-allocated tail. A pinned segment is subtracted from that pool. When
the pool empties, `allocate_segment()` (`:1702`) falls into a 5-second retry wait
(`:1738`) — a write stall.

A 5 GiB tablet is ~40,960 segments; on a shard with 128 GiB of logstor disk that
is ~4% of the pool, against a default `logstor_compaction_trigger_threshold` of
0.05. One tablet is survivable. Nothing bounds how many tablets stream
concurrently from one shard.

### 2. Compaction is pin-blind, which closes a positive feedback loop

`segment_set::add_segment` (`compaction.hh:194`) contributes one `ref_count`, and
`remove_segment` (`:207`) gives it back. `make_segment_ref`
(`segment_manager.cc:1132`) adds one more per outstanding `segment_ref`. The
compaction free loop (`:2186`) is:

```cpp
ss.remove_segment(desc);
if (desc.ref_count == 0) {
    _sm.free_segment(seg_id);
}
```

So compacting a pinned segment removes it from its group, rewrites its live
records into **new** segments, and frees **nothing**. And
`select_segments_for_compaction` (`:1956`) cannot tell: it scores candidates by
`record_bytes` alone (`:1982`) and never looks at `ref_count`.

The loop is therefore: pinning lowers `available_segment_count`, which pushes
`should_run_auto_compaction()` (`:1241`) below the low watermark and raises
`compaction_pressure()` (`:1202`) so compaction CPU shares ramp up; compaction
picks segments from the pinned group believing it will reclaim `n_in - n_out`,
reclaims zero, and consumes `n_out` fresh segments; that lowers
`available_segment_count` again, keeping `while (... && should_run_auto_compaction())`
(`:1285`) spinning. A stream can drive a compaction storm that burns IO and CPU at
escalating shares while making free space strictly *worse*.

### 3. Bookkeeping memory

Each `segment_snapshot` (`segment_manager.hh:87`) holds a `segment_ref` whose
shared state carries two `std::function`s (`compaction.hh:279`) plus the source
`noncopyable_function` — roughly 150–200 bytes. ~6–8 MB per 5 GiB tablet, ~80 MB
for a 50 GiB whole-shard snapshot.

## Constraints from the implementation

These are the facts the design has to respect; all were verified against the
current tree.

1. **A hard link is not a snapshot here.** `segment_id → (file_id, offset)` is
   fixed arithmetic (`segment_manager.cc:1184`) and reusing a segment id
   overwrites that file region. Isolation requires that the segment not be
   reused; nothing else provides it.
2. **`free_segment` (`:1745`) hard-asserts** `record_bytes == 0` (`:1755`) and
   `ref_count == 0` (`:1758`), fatally. Anything that keeps a segment alive must
   keep its `ref_count` above zero, not bypass the free path.
3. **The compaction free loop is synchronous.** Between `cb.close()` (`:2174`)
   plus `await_pending_reads()` (`:2182`) and the loop at `:2186` there are no
   yields. A hook that runs *before* the loop therefore sees a stable input set
   and needs no locking.
4. **Split compaction has two paths.** The fast path (`:2229-2241`) *moves* a
   segment to a child group — `remove_segment` then `add_logstor_segment`, with
   no free. The slow path frees at `:2294-2301`. Only the slow path destroys
   segments.
5. **Mixed (active) segments are never in a group.** They are held alive purely by
   `segment_ref`s — the `writeable_segment`'s own ref (`:1152`) and the separator
   buffer's `held_segments` (`compaction.hh:317`) — and freed by refcount once
   separated (`flush_separator_buffer` → `buf.reset()`, `:2386`). Compaction only
   ever iterates `cg.logstor_segments()`, so mixed segments are never compaction
   candidates and pins on them never interact with candidacy.
6. **A `segment_ref` is a shared handle.** `make_segment_ref` increments
   `ref_count` once; copies share the state. So `ref_count` counts *group
   membership plus distinct `segment_ref` states*, not copies.
7. **`segment_set::clear()` (`compaction.hh:215`) deliberately does not
   decrement.** Shutdown leaves group-resident segments at `ref_count >= 1`
   forever, so an outstanding pin at shutdown cannot cause a free.
8. **`discard_segments` (`:1771`) destroys data deliberately** and requires
   `ref_count == 1` (`:1791`) — group membership only. It is the one eviction a
   snapshot cannot survive.

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
do (`load_segment`, `segment_manager.cc:2792`).

Mechanically the rescue is cheap. Pinning a pending segment before the free loop
leaves `ref_count` at 1 after `remove_segment`: the segment is **detached from
its group but stays allocated with its bytes untouched**. The consumer reads it
later through `create_segment_input_stream` (`:2759`), a plain file-region read
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
`get_segment` (`:1150`) and the end of `write_full_segment` (`:1613`). Add a
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
hash lookup in the overwhelmingly common case of no active snapshot. Check
`sizeof(segment_descriptor)` before and after: there are ~1M descriptors per
shard at 128 GiB, so the two `uint16_t`s must land in existing padding (they
should, next to `int ref_count` / `uint32_t index_in_set`). If they do not, fall
back to a single shard-level "any snapshot active" counter guarding the map
lookup.

Both counters are `uint16_t`: assert on increment that they do not wrap — a
silent wrap to zero would corrupt the A1 skip test and the pending checks, the
same class of corruption `free_segment`'s hard asserts guard `ref_count`
against.

#### A1 — pin-aware compaction candidacy

In `select_segments_for_compaction` (`:1956`), skip descriptors with
`external_ref_count > 0`:

```cpp
if (desc.external_ref_count > 0) {
    continue;   // NOT break
}
```

`continue`, not `break`: the loop walks the histogram in ascending utilization
order and reserves `break` for `used_fraction > max_used_fraction` (`:1983`).
Skipping a pinned segment must not truncate the scan. The prefix-score contract
is unaffected — a skipped segment simply is not a candidate.

Compacting a pinned segment is always pure loss: it reclaims nothing and consumes
output segments. This also means a **rescued segment is never re-selected**,
which is what keeps rescue from cascading.

**Do not apply A1 to split compaction.** `do_split_compaction` (`:2204`) loops
`while (!src_segments._segments.empty())` and must drain the source group; skipping
pinned segments would spin forever. It stays correct without A1 because both its
paths remove the segment from `src_segments` regardless of `ref_count` — the fast
path re-homes it into a child group, the slow path detaches it — so the loop still
makes progress. The slow path's free is covered by the rescue hook instead.

Two known consequences to accept and document:

- With a fully pinned group and space pressure, `find_top_compaction_candidates`
  (`:2026`) may return empty, so `run_auto_compaction` (`:1282`) sleeps 100 ms and
  retries (`:1288`) until pressure clears. That is pre-existing behavior for the
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
(`:2235-2241`), where a segment is *moved* to a child group without being
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

- **Construction** (replacing today's `make_snapshot` body, `:2647`): under the
  existing `disable_compaction(cg)` guard, walk `_segment_list` recording **ids
  only**, no pins, and register them. Registration increments
  `pending_snapshot_count` and appends to `_pending_segments`.
- **`next()`**: hand out **rescued** segments first — their bytes are pure
  liability, already duplicated in compaction's output, so draining them returns
  segments to the free list fastest. Otherwise pop the next pending id,
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

Call sites, both **before** the free loop and after `cb.close()`:

- `do_compaction`, after the `logstor_compaction_wait_before_remove_segments`
  injection (`:2183`) and before the loop at `:2186`;
- `do_split_compaction`'s slow path, before the loop at `:2294`. Its fast path
  frees nothing and needs no hook (constraint 4).

`rescue_pending_segments` is synchronous and must not throw: for each input with
`pending_snapshot_count > 0`, each owning snapshot calls
`make_segment_ref(id, external)` and records the ref in its rescued list. Because
`remove_segment` then decrements to 1 rather than 0, the segment stays allocated.
Recording the ref can allocate; pre-reserve the rescued container against the
budget at construction so the synchronous pass cannot fail. If a rescue still
cannot be recorded, **mark the snapshot aborted** (so `next()` throws and the
consumer's operation fails cleanly) rather than pinning untracked or silently
dropping records.

The budget wait is a separate, awaitable step that runs *before* the synchronous
pass, which is what keeps the free loop yield-free (constraint 3).

#### Rescue budget and escalation

Give each snapshot a rescue budget `B` — a configured fraction of
`_max_segments.configured`, default ~1% — and a hard cap `H`.

1. Under `B`: rescue freely.
2. At `B`: `await_rescue_budget` waits on the snapshot's drain condition
   (signalled when `next()` hands out a rescued segment and when a pin is
   released) before the rescue pass. The compacting group is `running()`, so
   `find_top_compaction_candidates` (`:2050`) skips it while it waits — this is
   the "scoped compaction disable" effect, but scoped to *this group* and bounded
   by *drain rate*, not by the whole operation.
3. Past `H`, or on a wait timeout: mark the snapshot aborted, rescue nothing, let
   compaction free normally. The consumer fails and retries. Never simply stop
   rescuing — that silently drops records.

Deadlock analysis to state in the code comment: the waiting compaction fiber has
already released its compaction buffer (`cb.close()` → `buf.release()`, `:2108`)
and holds no segment allocation; the draining consumer only issues file-region
reads through `create_segment_input_stream` and needs neither compaction nor a
free segment. So the wait cannot be satisfied only by the fiber that is waiting.

#### Group teardown

`discard_segments` (`:1771`) is the one eviction rescue-pin cannot answer: the
data is being destroyed deliberately and the function requires `ref_count == 1`
(`:1791`). Add, before its validation pass, an abort of every snapshot holding a
pending or rescued segment of this group: mark aborted, unregister, drop pins.
Marking aborted also closes the race where a concurrent `next()` would hand out a
segment that is about to be freed.

Today's whole-tablet pinning has the same exposure, so tablet cleanup is
presumably already serialized against streaming upstream (`table::stream_in_progress()`,
the storage-group async gate). Confirm that — but implement the abort regardless:
it is cheap and it turns a fatal internal error into a failed operation.

`logstor_group::clear_segments()` (`compaction.hh:443`, called from
`replica/table.cc:3370` on shutdown) needs nothing: per constraint 7 it does not
decrement, so an outstanding pin cannot trigger a free.

## Commit plan

Seven landable commits. Each is independently testable; 0.1 is independently
valuable against today's whole-tablet pinning.

### 0.1 — `external_ref_count` + A1 pin-aware compaction candidacy

- `compaction.hh`: add `external_ref_count` to `segment_descriptor` (`:118`);
  verify `sizeof` is unchanged.
- `segment_manager.cc`: `make_segment_ref` (`:1132`) takes a
  `segment_ref_kind`; only `make_snapshot` (`:2660`) passes `external`. The other
  two callers — `get_segment` (`:1152`) and the separator path in
  `add_segment_to_compaction_group` (`:2612`) — stay `internal`.
- `select_segments_for_compaction` (`:1977`): skip `external_ref_count > 0`.
- Metric: `pinned_segments` / `pinned_bytes` gauges in the `logstor_sm` group
  (`:1350`), backed by a running shard counter rather than a scan.
- **Existing test must change.**
  `test_logstor_group_compaction_rewrites_live_records`
  (`test/boost/logstor_test.cc:1114`) takes `old_snapshot` at `:1160`, holds its
  five pins across `submit(cg)`, and asserts the group compacted down to one
  segment. Under A1 that compaction is now correctly skipped. Fix: capture
  `old_segment_ids` via `snapshot_segment_ids` (`:206`) and **destroy
  `old_snapshot` before submitting compaction**. The other two snapshot tests
  (`:971`, `:1277`) are unaffected — the first submits no compaction, the second
  already runs with compaction disabled.
- New test: a pinned group yields no compaction candidate, and releasing the pins
  makes it a candidate again.

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
  (before `:2186`) and `do_split_compaction`'s slow path (before `:2294`).
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

- Abort snapshots overlapping the group in `discard_segments` (`:1771`) before its
  validation pass.
- Confirm the upstream serialization claim; add a test that discarding a group
  with an open cursor fails the cursor instead of tripping the internal error at
  `:1791`.

### 0.5 — Token-range cursor on `table`

- `table::make_logstor_snapshot_cursor(dht::token_range)` alongside
  `take_logstor_snapshot` (`replica/table.cc:1641`): flush the range's storage
  groups' separators, then build one cursor spanning their compaction groups.
- It must hold `get_sstable_list_permit()` and the storage-group async gates for
  the **whole iteration**, not just construction. Today
  `take_logstor_snapshot` drops the permit at the end of each loop iteration
  (`table.cc:1646`), which is fine only because it has already pinned everything.
- Preserve `storage_group::take_logstor_snapshot`'s existing group selection
  (`table.cc:1230`): `_main_cg` alone when there are no split-ready or merging
  groups, otherwise the merging and split-ready groups.
- Because the registry is id-keyed, the cursor does not need the groups to stay
  alive for correctness — but the gates keep the *table* alive for `next()`.

### 0.6 — Producer-based `tablet_stream_files`, wired to the cursor

The sender currently takes a fixed `std::list<stream_blob_info>` and loops
(`stream_blob.cc:529`, `:562`). Add an overload taking

```cpp
using stream_blob_producer =
    noncopyable_function<future<std::optional<stream_blob_info>>()>;
```

and reimplement the list version as a wrapper over it.

- The wire protocol needs no change: sinks are created per file inside the loop
  (`:586-592`) and no global file count is sent.
- The `sources.empty()` early return (`:535`) becomes "pull the first item; if
  none, return 0".
- Logging at `:749` and `:753` uses `sources.size()` and `sources` after the loop
  (where the list has already been emptied by `std::exchange`, `:565`); replace
  with a running counter.
- `tablet_stream_files_handler` (`:760`): the logstor branch (`:770-782`) builds a
  producer that pulls from the cursor and wraps each `segment_snapshot` in a
  `stream_blob_info` exactly as today. Keep `table_stream_op` and the reader
  permit alive for the whole call — unlike the sstable path (`:867`), the logstor
  path cannot release the table early, because `next()` calls back into the
  segment manager.
- The `files.empty()` early return at `:872` and the `files_nr` logging at `:876`
  move to the producer's running count.
- The sstable branch keeps the list overload with zero behavior change.

### 0.7 — Metrics, injection points, tests

Metrics in the `logstor_sm` group (`segment_manager.cc:1350`):

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
(`stream_blob.cc:772`) and `logstor_compaction_wait_before_remove_segments`
(`segment_manager.cc:2183`) injection points. Assert that destination data matches
the source, and that source `free_segments` does not fall monotonically for the
transfer's duration — the observable signature of the storm this work removes.

## Verify before starting

- **`discard_segments`'s `ref_count == 1` assertion (`:1791`).** Whole-tablet
  pinning has the same exposure today, so tablet cleanup is presumably serialized
  against streaming upstream — confirm rather than assume, since the cursor changes
  the shape of the window. Decide whether `external_ref_count` should also relax
  that assertion or whether the 0.4 abort is sufficient (preference: the abort;
  keep the assertion strict).
- **A rescued (group-detached, pinned) segment surviving a crash.** Its records
  are duplicated in the compaction output with a higher seq, and
  `add_segment_to_compaction_group` (`:2584`) re-attaches it as a full segment
  whose records are all dead, which compaction then frees. This is the same shape
  as the deliberate leak on the `set_flush_failure` path (`:1601`), so it should
  be a no-op — verify with a recovery test.
- **`sizeof(segment_descriptor)`** before and after adding the two counters, given
  ~1M descriptors per shard.
- **`storage_group::take_logstor_snapshot`'s group selection** (`table.cc:1230`)
  excludes `_main_cg` when split-ready or merging groups exist. Confirm that is
  intentional before replicating it in the cursor.

## What backup reuses

Nothing in stage 0 knows about backup, but everything backup needs from the
logstor side is here:

- `table::make_logstor_snapshot_cursor(token_range)` is the enumeration primitive
  the node-side backup worker drains, one segment at a time, instead of pinning a
  tablet for the duration of a throttled object-storage upload.
- The strong invariant is what makes uploading from an unpinned enumeration
  correct; without it, backup would have to fall back to whole-tablet pinning or
  to a materialized on-disk snapshot.
- The rescue budget and its escalation ladder are the residual-cost bound that
  backup's own admission control (A2) composes with.
