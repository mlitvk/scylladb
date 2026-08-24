# Logstor Compaction

This document describes the cost model behind logstor compaction, compares the candidate scoring
rules that can be used to pick segments for compaction, records the strategy that is implemented,
and keeps the plan for what remains.

For the general logstor architecture see [logstor.md](logstor.md). For tombstone and TTL
reclamation see `logstor_gc.md`.

## Scope and terminology

- **Segment** — the unit of allocation and reclamation, `segment_size` bytes (default 128KB).
- **Record** — one partition value plus its header, located by a `log_location` whose `size` is
  the record's *net data size*. All space accounting is denominated in these bytes.
- **Live / dead** — a record is live while the primary index points at it. `segment_descriptor`
  tracks `free_space` per segment; the descriptors are kept in a histogram keyed by `free_space`,
  so iterating a `segment_set` yields the least-utilized segments first.
- **Utilization** `u` — a segment's live bytes divided by `segment_size`.
- **Group** — a `logstor_group` (one per `replica::compaction_group`, i.e. per tablet). Segment
  sets are per group, and compaction never mixes records from different groups into one output
  segment.
- **`n_in` / `n_out`** — segments read by one compaction job / segments it writes.
  `reclaimed = n_in - n_out`.

### The separator sets the baseline

Every user write lands in a `mixed` active segment and is *also* copied by the separator into a
`full` segment belonging to its group. The mixed segment is freed by reference counting once all
its records have been separated; it is never compacted. So before compaction contributes
anything, logstor already writes each user byte twice:

```
device write bytes per user byte = 1 (mixed) + 1 (separator) + WA_gc
```

where `WA_gc` is compaction's own amplification. This matters when reading the numbers below: a
`WA_gc` of 2 means the device sees 4x the user write rate, not 2x.

## Cost model

### Reclaim requires a batch

A compaction job writes whole output segments, so a single segment with any live data yields
`n_in = n_out = 1` and reclaims nothing. Reclaim needs

```
n_out = ceil(live_bytes / usable_segment_size) < n_in
```

which means the batch's *mean* utilization must be below `1 - 1/n_in`. At a batch cap of 8 the
ceiling is 0.875 — no batch of 8 segments averaging more than 87.5% live can ever reclaim a segment.
Raising the reclaim ceiling under pressure requires a **larger batch cap**, not a more permissive
utilization filter. This is why `min_segments_per_compaction` is 8 rather than something smaller:
below it a batch could not reclaim from a disk that is even moderately packed.

### Read cost is proportional to `n_in`, not to live bytes

`segment_manager_impl::scan_segment` opens the segment with
`buffer_size = max(segment_size, 128KB)` and `read_ahead = 0`, so a victim segment is read in
full regardless of how much of it is live. The `want_data::no` fast path in `do_compaction` avoids
*deserializing* dead records, not reading them. Therefore:

```
compaction read bytes = n_in * segment_size
```

### Steady-state identities

In steady state (fixed dataset size, overwrite-only workload) each user write kills exactly one
older record, and every record is eventually either copied forward or read-and-skipped exactly
once. Hence, per unit time:

```
compaction read bytes = compaction write bytes + user write bytes
```

Read traffic is not an independent variable — it is fully determined by write traffic. This is why
an explicit read-cost term in the candidate score is redundant (confirmed experimentally: weighting
reads against writes anywhere in 0.5x–4x changes nothing about which batch is selected).

Second identity: the space freed per unit time must equal the space consumed per unit time, so

```
WA_gc = 1 / E[ reclaimed * segment_size / live_bytes_copied ]
```

The quantity inside the expectation is a job's **reclamation efficiency** — segments reclaimed per
segment-worth of data copied — and it is exactly the reciprocal of that job's marginal write
amplification. Maximizing it minimizes WA; putting a floor under it is a write-amplification
budget.

## What actually determines write amplification

Compaction has to keep the free-segment count at some target. Holding a target of `s` (as a
fraction of the disk) while live data occupies a fraction `U` of the disk forces the occupied
segments to be packed to

```
U_eff = U / (1 - s)
```

and it is `U_eff`, not the scoring rule, that sets write amplification. Every simulated
configuration below is dominated by this relation.

### Over-provisioning

The tables in this section and the next are comparisons rather than predictions of what a disk will
do: their absolute values were taken with an earlier model and are low, see [The absolute numbers on
this page are due a re-take](#the-absolute-numbers-on-this-page-are-due-a-re-take).

`U = 0.75`, uniform random overwrites, cap 8, one group. "Absolute" is the absolute-reclaimed score
with the output buffer flushed at the end of every job, i.e. the behavior before the resolved
changes in the [Plan](#plan); "Efficiency" is the rule from [Strategy](#strategy), which is what
runs now except for the output residual. "Random victim" is the textbook `U_eff/(1-U_eff)` baseline,
shown to size the benefit of utilization-ordered selection.

| Free target | `U_eff` | Absolute `WA_gc` | Absolute read/user | Mean victim `u` | Efficiency `WA_gc` | Random victim |
|---|---|---|---|---|---|---|
| 3%  | 0.773 | 1.37 | 2.59 | 0.53 | 1.28 | 3.41 |
| 5%  | 0.789 | 1.59 | 2.67 | 0.60 | 1.43 | 3.75 |
| 8%  | 0.815 | 1.81 | 2.97 | 0.61 | 1.71 | 4.41 |
| 10% | 0.833 | 2.09 | 3.29 | 0.64 | 1.97 | 5.00 |
| 15% | 0.882 | 3.13 | 4.27 | 0.73 | 2.99 | 7.50 |
| 20% | 0.938 | 6.30 | 7.89 | 0.80 | 5.75 | 15.00 |

Insisting on 20% free instead of 5% free costs **4x the write amplification**. No scoring rule
comes close to that leverage.

The corollary is worth stating plainly: **at steady state write amplification is set by how much
space you insist on keeping free, not by the admission filter. The filter only controls when you
pay.** Refusing an expensive batch does not avoid the work; it defers it until pressure rises and
the filter widens, by which time the segment is usually no cheaper.

### Disk utilization

Free target 10%:

| `U` | `U_eff` | Absolute `WA_gc` | Efficiency `WA_gc` | Random victim |
|---|---|---|---|---|
| 0.30 | 0.333 | 0.06 | 0.05 | 0.50 |
| 0.50 | 0.556 | 0.34 | 0.33 | 1.25 |
| 0.65 | 0.722 | 0.97 | 0.93 | 2.60 |
| 0.75 | 0.833 | 2.09 | 1.97 | 5.00 |
| 0.82 | 0.911 | 4.31 | 4.06 | 10.25 |

Disk sizing is the operator's write-amplification knob, and it should be documented as such.

### Group fragmentation

Candidate selection is per group, so with `G` groups the greedy pool is `1/G` of the disk and the
emptiest segment available is substantially fuller than the emptiest segment on the disk.
Measured with 2400 segments so that per-group open-segment overhead is negligible, `U = 0.75`,
free target 10%:

| Groups | `WA_gc` | Mean victim `u` |
|---|---|---|
| 1   | 1.93 | 0.66 |
| 8   | 1.97 | 0.66 |
| 32  | 2.15 | 0.68 |
| 128 | 3.22 | 0.76 |

Selecting victims globally and grouping them by owner inside one job (up to 4 groups per job)
recovers part of this at high group counts — 3.22 to 2.98 at `G = 128` — but was *worse* than
per-group selection at `G = 32` under a skewed workload, where per-group locality is itself a form
of temperature separation. This is worth revisiting only for shards carrying many tablets, and
only with measurements.

A second, more mundane cost of many groups: each group holds an open user segment and an open
compaction output segment, so `2 * G` segments are partially filled at any time. At 128 groups
that is 32MB per shard of half-used space, which raises `U_eff` and therefore `WA_gc`.

### Compaction output residual, and why the batch cap is the cheaper fix

`compaction_buffer::close()` flushes whatever is left in the buffer as a partially filled `full`
segment at the end of every job, and the buffer is returned to `_compaction_buffer_pool`. Every
compaction job therefore creates one under-filled segment, which is itself a future compaction
candidate. The residual averages half a segment per job, so it costs roughly `0.5 / n_out` in extra
copy work — and `n_out` grows with the batch cap. The two fixes are therefore not additive.

Measured at a pinned free level (a hysteresis band of one segment, so that every configuration is
compared at the same `U_eff`; see [Batch cap](#batch-cap)), 5% target:

| `U` | Cap | Flush per job (today) | Carry residual | Carrying gains |
|---|---|---|---|---|
| 0.75 | 8  | 1.593 | 1.591 | 0.1% |
| 0.75 | 16 | 1.516 | 1.465 | 3.4% |
| 0.75 | 32 | 1.511 | 1.486 | 1.7% |
| 0.85 | 8  | 3.802 | 3.552 | 6.6% |
| 0.85 | 16 | 3.611 | 3.460 | 4.2% |
| 0.85 | 32 | 3.559 | 3.499 | 1.7% |

Raising the cap from 8 to 32 takes 5–6.4%; carrying the residual on top of that takes another 1.7%.
Since carrying is much the most invasive change on this page — per-group buffers, a `pinned` mark on
`segment_descriptor`, a per-group flush semaphore, and that semaphore extended over `make_snapshot`
and `discard_segments` — the cap is the change to make, and carrying drops to "measure first". The
design is kept in [Carrying the output residual](#carrying-the-output-residual).

### Batch cap

Free level pinned, flush per job, one group, uniform. Ordered by `U_eff = U / (1 - free_level)`,
which is what actually sets write amplification.

| `U` | Free target | Cap 4 | Cap 8 | Cap 16 | Cap 32 | Cap 64 |
|---|---|---|---|---|---|---|
| 0.50 | 5%  | 0.297 | 0.297 | 0.297 | **0.291** | 0.296 |
| 0.75 | 5%  | 1.840 | 1.593 | 1.516 | **1.511** | 1.554 |
| 0.82 | 5%  | 2.907 | 2.827 | **2.624** | 2.656 | out of space |
| 0.85 | 5%  | —     | 3.802 | 3.611 | **3.559** | out of space |
| 0.88 | 5%  | out of space | 5.666 | 5.252 | **5.136** | out of space |
| 0.75 | 10% | 2.299 | 2.224 | 2.087 | **2.079** | 2.137 |
| 0.85 | 10% | —     | 6.915 | 6.785 | **6.712** | 6.870 |
| 0.75 | 20% | —     | 6.533 | 6.148 | **6.105** | 6.224 |

A cap of 32 is at or within 1% of the optimum at every operating point: 5–9% less compaction write
traffic and 4–8% less read traffic than a cap of 8. A cap of 64 is worse, and runs out of space at a
5% target — which is the constraint in [Batch cap and parallelism are one
parameter](#batch-cap-and-parallelism-are-one-parameter).

Two things about this measurement are worth recording, because both are easy to get wrong.

**The free level has to be held equal.** Measured with the real 25% hysteresis band, a cap of 8 looks
*better* than 32 at high `U_eff` — 6.93 against 8.49 at `U = 0.85`, 10% target. That is an artifact:
at a cap of 8 compaction cannot keep up there, so it never reaches the stop watermark and settles at
a lower free level, hence a lower `U_eff`. The two `WA_gc` values are in the ratio the two free
levels predict, to within a percent. Pinning the level removes the inversion at every point.

**Not all of the benefit is batch size.** A cap of 32 with the extension tolerance at 1.0 produces
7.5 segments per job — the same job size a cap of 8 produces — and still beats it by 7%. Widening the
cap widens the *candidate window* the score chooses a stopping point from, which is a separate gain
from the job being larger.

### Batch cap and parallelism are one parameter

A compaction job allocates its output segments as it goes but frees its inputs only when it is done,
so a job in flight holds up to `n_out` segments out of the free-segment target, and `P` concurrent
jobs hold up to `P * n_out`. Since `n_out < n_in <= cap`, the safe relation is

```
auto_compaction_parallelism * max_segments_per_compaction <= watermarks.low
```

Violating it does not degrade gracefully: with the target consumed, every job waits for an output
segment that only another job could free, and normal writes have already stalled, so no record dies
to free one. The simulation reaches exactly this state whenever the cap approaches `low` — the "out
of space" entries in the table above.

`make_compaction_limits()` maintains the relation by deriving both limits from `low`, spending the
target on concurrency first and giving the remainder to the batch:

```
parallelism = clamp(low / min_segments_per_compaction, 1, max_auto_compaction_parallelism)
batch_cap   = clamp(low / parallelism, min_segments_per_compaction, max_segments_per_compaction)
```

| Disk per shard | Segments | `low` at 5% | Parallelism | Batch cap |
|---|---|---|---|---|
| 8 MB (`test/cluster/test_config.yaml`) | 64 | 8 | 1 | 8 |
| 16 MB (`logstor_test.cc`) | 128 | 16 | 2 | 8 |
| 128 MB | 1024 | 52 | 6 | 8 |
| 1 GB | 8192 | 410 | 6 | 32 |
| 64 GB | 524288 | 26215 | 6 | 32 |

The previous defaults had no such relation and violated it wherever the floor bound the target:
`low = 16` against `auto_compaction_parallelism * max_segments_per_compaction = 32`.

The relation is unsatisfiable on a disk of fewer than 128 segments, where the target's floor has
already been capped at `segment_count/8` and a batch of `min_segments_per_compaction` is the least
that can reclaim anything. Such a disk is below the useful range regardless.

## Candidate scoring rules

All rules operate on the same candidate set: a prefix of the group's free-space histogram in
ascending-utilization order, capped at `max_segments_per_compaction`. The histogram already gives
near-optimal victim ordering, so a rule can only choose *where to stop*. That structural
constraint is why the rules differ so little on write amplification.

`U = 0.75`, free target 10%, one group, residual carried, uniform workload:

| Rule | Cap | `WA_gc` | read/user | Mean victim `u` | Segments/job | Jobs |
|---|---|---|---|---|---|---|
| absolute reclaimed | 8  | 1.96 | 2.96 | 0.663 | 8.00  | 2000 |
| reclaimed per live byte (yield) | 8  | 1.95 | 2.95 | 0.661 | 3.75  | 4255 |
| efficiency (`1/marginal WA`) | 8  | 1.95 | 2.95 | 0.661 | 3.75  | 4255 |
| efficiency, tolerance 0.8 (implemented) | 8  | 1.97 | 2.97 | 0.663 | 7.01  | 2286 |
| absolute reclaimed | 32 | 2.05 | 3.05 | 0.671 | 32.00 | 514  |
| efficiency, tolerance 0.8 (implemented) | 32 | 2.05 | 3.05 | 0.672 | 31.59 | 521  |

### Reclamation efficiency with batch-extension tolerance — implemented

```
efficiency = reclaimed * segment_size / estimated_live_bytes
```

By the identity above this is the reciprocal of the job's marginal write amplification, which makes
the score directly interpretable and gives the admission gate a unit operators understand.
`compaction_candidate_score::operator<` ranks by it — comparing the cross products, so the common
`segment_size` factor cancels and no floating point enters the ordering — and ties by `reclaimed()`.
A batch that copies nothing has infinite efficiency and outranks every batch that copies.

On its own, efficiency picks small batches (3.75 segments/job, 2.1x more jobs), which is per-job
overhead for no write-amplification benefit. `select_compaction_prefix()` therefore extends the
most efficient prefix to the longest prefix satisfying

```
efficiency(prefix) >= tolerance * efficiency(best)
```

At `tolerance = 0.8` the batch grows back to 7.0 of 8 segments — 2.1x fewer jobs — for +1% write
amplification. Note that efficiency along the prefix is a *sawtooth*: it jumps whenever one more
segment is reclaimed and decays as live bytes accumulate without reclaiming. So the longest prefix
within tolerance is not necessarily the one just before the first prefix that falls outside it, and
the extension scans from the cap downwards rather than stopping at the first violation.

The tolerance makes batch size adapt to what the histogram offers: large batches when extending is
nearly free, small ones when it is not. `max_segments_per_compaction` is then a pure safety bound
and can be raised under pressure to lift the `1 - 1/n_in` reclaim ceiling without the amplification
blow-up the absolute-reclaimed rule suffers.

### Absolute segments reclaimed — the rule this replaced

`operator<` used to rank first by `reclaimed()`, then by `reclaimed()/estimated_live_bytes`, then by
`n_in`. Because `reclaimed()` is monotone non-decreasing along the prefix until the batch runs out
of cheap segments, that rule almost always took the longest prefix that still reclaims — 8 of 8
segments in the table above.

- **Effect on throughput**: maximal space reclaimed per job, so the fewest jobs, the least
  per-job overhead (buffer allocation, per-group serialization, `await_pending_reads`), and the
  best chance of sequential IO across sorted segment ids. The tolerance term recovers most of this.
- **Effect on write amplification**: neutral at cap 8, but it degrades as the cap grows, because
  it keeps extending the batch into fuller segments for one more reclaimed segment regardless of
  cost. With `tolerance = 1.0` and cap 64 it reaches `WA_gc` 2.88 against 2.56 for the efficiency
  rule.
- **Structural problem**: it conflated *how much work to do in one job* with *how urgent
  compaction is*. Urgency belongs to the controller, which decides how many jobs run concurrently
  and with how many shares. Inflating each job is the wrong lever, and it made
  `max_segments_per_compaction` a write-amplification tuning knob rather than a safety bound.

### Cost-benefit with age — not recommended

The classic LFS rule, `(1-u) * age / (1+u)`, defers segments whose utilization is still falling
so their data can die for free, and prefers old segments whose free space is stable. It requires
new per-segment state (a sequence number or timestamp in `segment_descriptor`).

Measured: **neutral everywhere** — 2.06 vs 2.06 under zipf 0.99, 1.87 vs 1.87 under a 90/10
hot-cold workload, against the efficiency rule. The classic benefit does not materialize here
because the candidate set is already an ascending-utilization prefix over a large pool, and the
efficiency denominator already penalizes copying. Do not add the state for this.

(The `seq` field that `logstor_gc.md` adds to `segment_descriptor` for the reclaim frontier would
make this rule free to try later; the result above says it still would not be worth enabling.)

### Read/write cost weighting — not recommended

A score of the form `reclaimed / (r * n_in + w * live_bytes/segment_size)` looks principled given
that reads cost `n_in` segments and writes cost `live_bytes`. In practice `n_in` and
`live_bytes` move together along the prefix, and the read-equals-write-plus-user identity means
read traffic is not independent. Varying `w/r` from 0.5 to 4 selected identical batches in every
configuration tested. Use the simple efficiency form.

### Generational (hot/cold) output separation — not recommended

Writing compaction output to a per-generation stream, so that repeatedly surviving (colder)
records concentrate in high-generation segments, is standard practice in SSD FTLs. Logstor already
has one level of it: compaction and separator output go to `full` segments distinct from the user
write stream.

Additional generations help only when there is a single group: -7% at `G = 1` on a 90/10 hot-cold
workload, -2.5% under zipf, neutral-to-worse on uniform, and neutral-to-worse at `G >= 32` where
tablet partitioning already provides the separation and the extra open segments cost more than the
separation saves. Not worth the open-segment overhead.

## Strategy

**Selection.** Keep greedy prefix selection over the per-group free-space histogram. It is
near-optimal for victim ordering and costs nothing to maintain.

**Score** (implemented).

```
efficiency = reclaimed * segment_size / estimated_live_bytes    // == 1 / marginal WA
```

- primary key `efficiency`, requiring `reclaimed >= 1`;
- tie-break by `reclaimed`;
- extend the chosen prefix to the longest prefix satisfying
  `efficiency >= 0.8 * best_efficiency`, capped at `max_segments_per_compaction`;
- `find_top_compaction_candidates` ranks groups by the same key, and the score it ranks with is the
  score of the batch that would actually run, so a small tablet holding one nearly-dead segment can
  win — which is correct, since that job is nearly free.

**Admission gate.** `max_used_fraction` is now driven by the trigger's watermarks (see [Admission
gate](#admission-gate-implemented)). The remaining step is to express it as a write-amplification
budget: accept a batch only if `efficiency >= theta(pressure)`, where `theta` ramps from about 1.0
at zero pressure (spend at most one segment-write per segment reclaimed) down to a small floor at
full pressure (take anything with a net gain). Now that the score *is* efficiency, this is the same
admission decision in a unit that can be reasoned about and alerted on, and it applies to the batch
rather than to each segment considered.

**Batch cap and parallelism** (implemented). Both are derived from the free-segment target by
`make_compaction_limits()`, which is what keeps `parallelism * cap <= low` — see [Batch cap and
parallelism are one parameter](#batch-cap-and-parallelism-are-one-parameter). On any disk large
enough for the target to cover them, that is a cap of 32 and 6 jobs in flight; below it the batch
shrinks first, down to `min_segments_per_compaction`, and then the parallelism. The cap is a safety
bound rather than a write-amplification knob only because the efficiency score plus tolerance decides
where inside it the batch actually stops.

**Free-segment target.** A percentage of the disk with an absolute floor, so that the target stays
meaningful on small disks where a percentage rounds down to a segment or two. See
[Configuration](#configuration).

**Parallelism** (implemented). `run_auto_compaction` keeps `compaction_limits::auto_parallelism`
jobs in flight: it holds one semaphore unit per job, for the whole job, and submits the next candidate as
soon as a unit comes back. Candidates are still ranked a batch at a time, to amortize the scan over
the groups, but the batch is a queue rather than a barrier — a queued candidate can be a few jobs
old when it is submitted, so its group is re-checked first; the segments are re-selected by
`do_compaction` regardless. Waiting for the whole batch, as it did before, let the number of jobs
decay to one at the tail of every batch, because job durations vary by nearly the batch cap: a batch
of nearly-dead segments copies almost nothing while one at the reclaim ceiling copies `n_in - 1`
segments. That both left disk bandwidth idle and turned the demand the shares controller samples
into a sawtooth. The parallelism is bounded by `max_auto_compaction_parallelism`, which is
`split_compaction_buffers` below `max_compaction_parallelism`, so that a concurrent split compaction
— which takes two buffers — cannot leave an automatic compaction holding a slot while blocked on
buffer allocation.

Note what this parallelism is *not*: it is not the depth of compaction's IO queue in any useful
sense beyond itself. `scan_segment` opens each input with `read_ahead = 0` and a one-segment buffer,
so a job reads its inputs strictly one at a time and its own read queue depth is 1. Compaction's
total queue depth is therefore exactly the number of jobs in flight, and a larger batch cap makes
each job longer without making it deeper. If compaction turns out to be IO-starved at high shares,
prefetching across a job's inputs is the lever — it costs no free segments, where more concurrent
jobs cost `cap` segments of the target each.

## Carrying the output residual

Not implemented, and no longer planned: at a batch cap of 32 it is worth 1.7% rather than the 6–9% it
was worth at a cap of 8, because raising the cap attacks the same waste — see [Compaction output
residual](#compaction-output-residual-and-why-the-batch-cap-is-the-cheaper-fix). This section records
the design, since it is the fallback if the residue turns out to cost more on real disks than the
model says. The separator already works this way — `separator_buffer` lives in `logstor_group`, is
flushed only when it cannot fit the next write, and holds its source segments across the flush — so
compaction is the odd one out.

### The index moves with the flush

`write_buffer::write()`'s future resolves in `complete_writes()`, which runs from
`write_full_segment`. Until the buffer is flushed the primary index still points at the **input**
segments, so an input whose records are still in the buffer cannot be freed: `free_segment` rejects
a segment that has data. Everything below follows from that one fact.

It also means the buffer only ever holds a *copy* of data that is still on disk and still indexed at
its original location. So the contents can always be **discarded**, at the cost of the copy work
only. Every teardown path takes that route — group removal, shutdown, truncate and drop, tablet
cleanup, tablet split — which keeps them free of a flush that would need a free segment, and
therefore unable to stall on a full disk.

### Pinning: inputs stay in their group

An input segment whose records are still in the buffer stays linked in `cg.logstor_segments()` and
carries a `pinned` mark on its `segment_descriptor`; `select_segments_for_compaction` skips pinned
descriptors. The *flush*, not the job, unpins them and frees those that are now fully dead.

This preserves the invariant that **the index only ever points into a segment owned by a group**,
which is what keeps the rest of the system correct without new hooks:

- `make_snapshot` walks `segment_set::_segment_list`, so tablet streaming still ships the segment
  the index points at.
- `discard_segments` sees the segment, so a dropped or truncated tablet's data is zeroed rather than
  left on disk for recovery to resurrect.
- `logstor_group::empty()` needs no change: a non-empty buffer implies at least one pinned segment
  still in the set, so the split-ACK check and `remove_empty_merging_groups` stay correct.
- Per-group disk accounting (`compaction_group::logstor_disk_space_used`) stays correct.

The alternative — detaching the input from the segment set and having the buffer hold a
`segment_ref`, as the separator does for mixed segments — breaks all four, each needing its own
flush hook, and turns a flush failure or a group destruction into either a permanently leaked
segment or `free_segment`'s "freeing segment that has data". The separator gets away with it because
a mixed segment is in no segment set to begin with.

### The pinned segments are the cost, and they need two bounds

Pinning *k* inputs holds *k* segments; flushing the residual consumes 1. So `k > 1` never pays:

- **Per job.** At the end of a job, flush if the buffer pins more than one input segment. In the
  common case the residual spans a single tail input — records are scanned in sorted segment order —
  so the carry survives. The rule fires exactly where carrying would lose: a job over mostly dead
  segments can otherwise pin all `max_segments_per_compaction` inputs while holding well under a
  segment of live data.
- **Per shard.** A cap on the total number of pinned segments, enforced by flushing residuals until
  the count is back under it. One pinned segment per group times many tablets can otherwise exceed
  the whole free-segment target, in which case auto compaction can never reach its stop watermark
  and goes back to spinning. Proposed pure function alongside the other bounds in `compaction.cc`:
  `max_pinned_output_segments(watermarks, max_compaction_parallelism)`, about
  `max(max_compaction_parallelism, watermarks.low / 4)`.

Both bounds are structural, not tuning: the first is space-neutral arithmetic, the second protects
the trigger's stop condition.

### Serialization

A per-group semaphore held by `do_compaction` for the whole job and by every out-of-job flush or
abort. It cannot go through `submit_group_compaction`: `database::truncate` takes
`disable_compaction()` on every group *before* calling `flush_separator()`, so a flush behind the
admission gate would throw.

`disable_compaction()` on its own is no longer sufficient exclusion. A flush now mutates the segment
set from outside a job, and `segment_set::unlink` keeps the list compact by swapping in the last
element, so a concurrent removal makes an index-based scan skip a segment entirely. `make_snapshot`
and `discard_segments` both scan by index and both document the assumption that compaction is the
only remover; they must hold the group's semaphore across their whole body, and those comments have
to change.

Freeing a segment still requires `index.await_pending_reads()` first. A mid-job flush therefore
frees nothing: it moves the unpinned ids to a reclaim list that the job drains after its single
end-of-job barrier, so the barrier count per job stays what it is today. An out-of-job flush awaits
and drains itself.

### Where the buffer lives

Both options bound the pinned-segment count identically, via the per-shard cap above; they differ in
what bounds the memory.

- **Per group, lazily allocated (recommended).** `logstor_group` owns the buffer, created on the
  group's first compaction and kept for its lifetime. `segment_size` per group that compacts — 50%
  on top of the two separator buffers every group already allocates eagerly, so 6MB per shard at 50
  tablets and 16MB at 128. No pool changes and no cross-group flush path.
- **Pooled with a residual quota.** Keep `_compaction_buffer_pool`, let a group retain its buffer
  after a job, and evict the least-recently-used residual — flushing it — when a job needs a buffer
  and none is free. Memory and pinned segments are both bounded by the pool size, and it degrades to
  today's behaviour when more groups are active than there are buffers, so it can never be worse
  than today. The cost is that the pool has to reach another group's index and flush lock, and keep
  an LRU consistent with group removal.

The first is preferred because the memory is the same order as an already-accepted per-group cost
and the pinned space is bounded by the cap either way. The second is the answer if per-shard tablet
counts go well above 128.

### Consequences elsewhere

- **Concurrency becomes explicit.** `_compaction_buffer_pool` is today the only cap on concurrent
  normal compactions, and `trigger_logstor_compaction` submits one fire-and-forget job *per group*.
  Per-group buffers remove that cap, and `get_segment(write_source::compaction)` bypasses the
  segment pool's reserve, so unbounded jobs could drain the pool and starve normal writes. A
  `max_compaction_parallelism` semaphore in `do_compaction` is a prerequisite, not a cleanup — which
  also makes it the right moment to size `_reserved_for_compaction` from the same constant (open
  defect 5).
- **`abort_source` needs a new home.** Allocating from the pool is currently the only place
  `do_compaction` honours its abort source. Without it, the check belongs in the scan callback, so
  that group removal and shutdown do not wait for a full batch scan. Aborting mid-scan needs no
  unwinding: the inputs are still in the set with their data still indexed.
- **Scoring.** `estimate_required_segments` must be seeded with the buffer's current occupancy, or
  `n_out` is over-estimated for small batches and the gate is more conservative than the model
  above. Keeping `n_out = ceil(...)` is deliberate; switching to `floor` to reflect the unflushed
  tail would loosen the gate materially and needs its own simulation first.
- **Per-job stats stop meaning anything.** `compaction_segments_out` is `flush_count` and
  `compaction_segments_reclaimed` is `n_in - n_out`, both aggregated per job. Once flushes are
  decoupled from jobs, a job that flushes nothing reports `out = 0, reclaimed = n_in` while freeing
  nothing, and a job that completes the previous job's residual is credited with its output. See
  [Observability](#observability).
- **Exception safety.** `compaction_buffer::flush()` leaves the buffer un-reset when
  `write_full_segment` fails, and that path has already closed the write gate — every later
  `write()` would throw for the lifetime of the group. Today the buffer does not survive that: it is
  held by `seastar::with_closeable`, so `compaction_buffer::close()` runs on the way out, calls
  `write_buffer::abort_writes()` to fail the writes that only a flush would otherwise have resolved,
  and hands the buffer back to the pool, which resets it for the next user. A per-group buffer
  outlives the job and has no such reset, so the flush has to reset unconditionally, wait on pending
  updates without short-circuiting on the first failure, and unpin on the way out.

### Rejected: an open output segment on disk

Instead of carrying bytes in memory, the job could flush at the end and leave the output segment
*open*, appending the next job's buffers to it. That removes both the memory and the pinning, since
the index is updated promptly. It is blocked by the on-disk layout: every buffer in a `full` segment
carries its own `segment_header` with the min/max token of *that buffer*, while
`read_segment_header` reads only the first one, and split compaction's fast path uses it to decide
that a whole segment belongs to one side of the split. A multi-buffer output segment would be
mis-classified. Making the fast path aggregate every buffer header in the segment would unblock it.

## Controller

### Today: a linear ramp on the trigger watermarks

`logstor_compaction_controller` is a `backlog_controller` whose backlog is
`compaction_shares_pressure()` directly, so control point inputs are in `[0, 1]`. Pressure is
piecewise linear in available segments, with all three anchors taken from the same
`free_segment_watermarks` the trigger and the admission gate use:

| `available` | pressure | shares | meaning |
|---|---|---|---|
| `>= high` | 0 | 50 | auto compaction has stopped; only explicit compaction runs |
| `== low` | 1/3 | 200 | the free-segment target: intended steady-state operating point |
| `<= low/2` | 1 | 2000 | half the target consumed; compaction is losing |

The top of the ramp is `logstor_compaction_max_shares` (default 2000, live-updatable); the two share
figures below assume the default. It caps the peak only: the lower two points keep their constants,
because the cap is meant to bound what compaction may consume when it is losing, not to move the
steady-state operating point. They follow the cap down only once keeping them would make the ramp
non-monotone — `target = min(200, max * 2/3)` and `idle = min(50, target / 4)` — which reproduces the
constants exactly at any cap of 300 or more, and below that leaves a ramp that still responds to
pressure instead of raising the configured cap back up behind the operator's back.

Shares are interpolated between the anchors, so they grow gently across the hysteresis band — where
the target has already been met — and six times faster below the target: at a 5% target, 50 shares at
6.25% free, 200 at 5%, 920 at 4%, 1640 at 3%, and the 2000 cap at 2.5%. The output at the target is
not critical, since the loop settles wherever shares match the write rate; it only decides how far
below the target that equilibrium sits.

The `1/3` at the target is not a tuning constant, it falls out of the relative hysteresis
(`high = 1.25 * low`), and it is therefore the same on every disk size up to the rounding of the
watermarks to whole segments. `compaction_shares_pressure_at_target` exports it so the middle
control point sits exactly on the target.

Two properties matter more than the slope:

- **The onset is the stop watermark**, not an independent threshold. Above it the trigger agrees
  there is nothing to reclaim. The previous ramp started at
  `logstor_compaction_soft_pressure_threshold` (0.1) while the target defaulted to 0.05, so the
  entire band auto compaction operates in — `[low, high]` = `[5%, 6.25%]` — mapped to pressure
  `[0.375, 0.5]`: shares barely moved across it, and the top of the control curve was reachable only
  at zero free segments.
- **Pressure saturates at `low/2`, not at zero available.** The free-segment count is the integral of
  the mismatch between the write rate and the rate at which compaction reclaims, so a proportional
  map from free segments to shares is integral control on that mismatch: the loop settles at whatever
  shares match the write rate, and the slope only decides *where* in the free-segment range it
  settles. A curve that reaches maximum shares only at zero available therefore makes any workload
  needing maximum shares settle exactly at the point where writes stall. Saturating at half the
  target leaves the write path room, and stalling then means the maximum shares genuinely are not
  enough — which is write throttling's problem, not the controller's.

`logstor_compaction_trigger_threshold` is the only knob on the *shape* of the control loop: it sets
the free-segment target, the hysteresis band, the admission gate and the shares ramp.
`logstor_compaction_max_shares` only caps the ramp's top.

This is an interim controller. It remains a pure space-pressure ratio: at a given free level it asks
for the same shares whether restoring the target requires rewriting one segment or ten thousand, and
on a large disk the absolute deficit per unit of pressure is large, so the response just below the
target is gentle in absolute terms. The work-based backlog below is what fixes that.

### Recommended: work-based backlog

Follow the pattern the sstable strategies use, where backlog is the volume of work outstanding and
in-flight progress reduces it:

```
deficit_segments = max(0, free_target - available_segments)
backlog_bytes    = deficit_segments * segment_size / max(theta_achievable, theta_floor)
                 - bytes_already_copied_by_in_flight_jobs
backlog          = backlog_bytes / available_memory()
```

`theta_achievable` is the efficiency of the best batch currently available, read off the head of
the group histograms — the same quantity the score computes. The division converts "segments I
must reclaim" into "bytes I must copy to reclaim them".

This behaves correctly where the space ratio does not: at the same free level, a disk whose
emptiest segments are 5% live needs far less CPU than one whose emptiest are 60% live. The
`bytes_already_copied` term is the analog of `Ci` in the size-tiered backlog tracker and keeps
shares from staying pinned while a long job drains. If deadline pressure is wanted, divide by the
time to exhaustion (`available_segments / segment_allocation_rate`) to get a required copy rate.

The division by `available_memory()` is there only to match the units the sstable backlog already
uses, so that the two can be summed by one controller. It carries no meaning for logstor, whose
demand is set by the disk rather than by memory; if logstor compaction ever gets its own scheduling
group, a better normalizer is available. See [The
normalizer](#the-normalizer-and-why-it-is-only-a-question-under-b).

`logstor_gc.md`'s frontier compactor is a second, non-space-driven source of demand; it adds its
own term to `backlog_bytes` rather than needing a second controller.

### Later: an integral term, to pin the target

Both the current ramp and the work-based backlog above are proportional-only, so both leave a
**droop**: the free level settles wherever the curve happens to supply the shares that match the
write rate, not at the target. Since `U_eff = U/(1-s)`, write amplification is a function of the free
fraction, so an unpinned level means an unpinned `WA_gc`. Pinning the level is the only way to make
the numbers in [Over-provisioning](#over-provisioning) a prediction rather than a bound.

Note what is *not* missing: the loop already has integral action, because the free-segment count is
the integral of `reclaim_rate - allocation_rate`. That is why throughput error is exactly zero at
equilibrium. What is missing is integral action on the *level* error, which needs a second
integrator, this time inside the controller.

**Where it goes.** In logstor's backlog function, not in `backlog_controller`, which is stateless and
shared with the sstable controller. Keeping the shares mapping a monotone bounded lookup is also what
makes saturation directly detectable for the anti-windup rule below. In integral-time form, so both
terms are in segments and one `theta` division serves both:

```
deficit  = max(0, low - available_segments)
integral = clamp(integral + (low - available_segments) * dt, 0, integral_max)
backlog_bytes = (deficit + integral / T_i) * segment_size / max(theta, theta_floor)
```

`dt` is the controller's own 250ms interval, so the integration runs in the same callback that
computes the backlog and needs no clock. The proportional term uses the one-sided deficit; the
integral accumulates the *signed* error, so that it unwinds when the level is above target, with the
state floored at zero because negative compaction demand is meaningless.

**Tuning.** Linearizing around the operating point with `k = |d(reclaim_rate)/d(available)|` in
1/s gives `e'' + k e' + (k/T_i) e = 0`, hence

```
zeta = 0.5 * sqrt(k * T_i)
```

Three consequences, two of them counter-intuitive:

- A *longer* `T_i` is better damped. A slow integrator is the safe one.
- A *higher* loop gain is better damped, because the plant is a pure integrator and the proportional
  term supplies all of the damping. Do not be tempted to replace the ramp with the integrator: with
  the proportional term removed the characteristic equation is `e'' + (k/T_i) e = 0`, whose roots are
  purely imaginary — undamped oscillation. **The ramp is the damping.**
- Ringing appears when `k * T_i < 4`, i.e. when the gain is *low* and the integrator fast.

`k` is not a constant, which is what makes the ordering matter. Simulated at each controller's own
equilibrium under a fixed load, as `theta` falls from 0.70 to 0.25:

| Controller | `k` at `theta` 0.70 | at 0.25 | spread | smallest safe `T_i` (`zeta >= 1`) |
|---|---|---|---|---|
| proportional ramp | 0.61 | 0.056 | 11x | ~120s |
| work-based (`/theta`) | 0.61 | 0.16 | 4x | ~30s |

So the `/theta` term does not merely make the response consistent: by holding `k` up it buys a **4x
faster integrator at the same damping**. That makes the work-based backlog a prerequisite for the
integral term rather than a companion to it. `T_i = 30s` closes the droop from a step in write rate
within about a minute with no overshoot; `T_i` in the low single digits rings.

**Anti-windup is mandatory, not a refinement.** The target is unachievable whenever compaction is
saturated, IO-starved, capped by `_compaction_buffer_pool`, or `find_top_compaction_candidates`
returns nothing. The error then stays positive, the integral grows without bound, and shares stay
railed long after the load has dropped — which looks exactly like a broken controller. Two cheap
defences, both worth having:

1. **Conditional integration.** Skip accumulation when the error is positive *and* the output is
   already saturated, which here means the proportional-only backlog already exceeds the last control
   point's input. Extend the same condition to a disabled trigger and to an empty candidate set,
   neither of which the integral can work off. This makes the empty-candidate signal in
   [Observability](#observability) an input to the controller rather than only a metric.
2. **A hard ceiling** on `integral / T_i`, of order `low`, bounding the integral's authority to
   roughly doubling the demand at the target.

Usefully, the conditional-integration rule fires in the same low-gain, near-saturation corner where
damping is weakest, so the anti-windup guard doubles as the damping guard.

**Consequences elsewhere.**

- `refresh_free_segment_watermarks()` must reset the integral: the accumulated error was measured
  against a setpoint that no longer exists.
- The trigger's hysteresis loses its purpose. With the level pinned at `low`, compaction runs
  continuously instead of bursting between `low` and `high`. The total work is identical, since
  `reclaim_rate = allocation_rate` either way, but spread evenly rather than in bursts, which is
  better for latency. Either set the integral's setpoint mid-band or accept continuous compaction and
  simplify `should_run_auto_compaction()`; the latter is preferred.
- The integral state must be exported as a gauge. A wound-up integrator is indistinguishable from a
  genuinely busy disk from the outside.
- Once one controller serves both sstable and logstor demand, logstor's integral raises shares for
  both. That is defensible — they are the same resource — but it should be a conscious choice.

Completing the analogy, the three classical terms map onto quantities this document already defines:
proportional is the current deficit, integral pins the level and therefore `WA_gc`, and derivative is
the allocation rate, i.e. the optional division by time to exhaustion above.

### Sharing the `comp` scheduling group: one controller, or two groups

#### Status: Option B implemented, joint cap still open

`main.cc` now creates a dedicated `logstor_compaction` scheduling group (`lcmp`, 1000 initial
shares), separate from the sstable `compaction` group (`comp`). `logstor_compaction_controller`
drives `lcmp` via `_dbcfg.logstor_compaction_scheduling_group`; the sstable `compaction_controller`
keeps driving `comp` as before. This removes the flip-flop described below, since the two
controllers no longer write to the same scheduling group.

What it does **not** yet do is bound the combined CPU envelope of the two groups, or measure
`B(s_max)` for logstor — both called out as prerequisites in the Recommendation below before taking
Option B. The historical analysis is kept below for that reason: the growth in the total compaction
CPU share and the actuator-gain coupling it describes still apply and remain open follow-up work
(see [Open defect #4](#open)).

#### What happened before

`main.cc` used to create one `compaction` scheduling group (`comp`, 1000 initial shares) and hand it
to both the sstable `compaction_manager` (as `compaction_sched_group`) and, via
`_dbcfg.compaction_scheduling_group`, to `logstor_compaction_controller`. Both are
`backlog_controller`s with a 250ms period, and both called `set_shares()` on it.

The usual description of this — "last writer wins, so neither is in charge" — understates it.
Logstor is enabled per table (`schema()->logstor_enabled()`), so a node normally runs both engines,
and on a node whose write-heavy tables are all on logstor the sstable backlog is near zero. Its
controller then computes the *bottom* control point, 50 shares, and writes it every 250ms, while
logstor writes whatever its own ramp asked for. The group's shares do not average out — they
flip-flop between 50 and logstor's value, with a duty cycle set by nothing but timer phase. **An idle
controller actively suppresses a busy one roughly half the time.** That was a live throughput
defect, not only an obstacle to tuning.

There are two ways out, and they are not merely two spellings of the same fix.

#### Option A — one controller, summed backlogs

Register logstor's demand with the existing `compaction_backlog_manager`, reusing
`compaction_backlog_tracker::impl`, so a single `compaction_controller` sees the sum of sstable and
logstor demand and drives the group once.

- **Fixes the defect completely**, with no new scheduling group and no change to the CPU envelope:
  compaction as a whole remains bounded by one controller's ceiling.
- **Forces a common unit.** The sstable backlog is bytes divided by `available_memory()`, so
  logstor's must be too — see [The normalizer](#the-normalizer-and-why-it-is-only-a-question-under-b)
  for why that divisor is a poor fit for logstor, and why it is nevertheless the price of this option.
- **Cannot express priority between the two engines.** A sum says "there is a lot of compaction
  demand"; it cannot say "logstor is minutes from filling, sstable compaction can wait". Which work
  actually runs is then decided by task ordering inside the group, not by either backlog.
- Smallest blast radius in logstor, largest outside it: it touches shared compaction code.

#### Option B — a separate scheduling group for logstor compaction

Give logstor compaction its own scheduling group and let each controller drive its own.

- **Also fixes the defect completely**, and without touching shared code — the two controllers stop
  writing to the same setter.
- **Frees the normalizer.** With nothing to sum against, logstor can pick a unit that means something
  for logstor.
- **Allows priority to be expressed** as the ratio between the two groups' shares, which is the one
  thing Option A structurally cannot do.
- **Changes the CPU envelope, not just the split.** One group gives compaction `s/(s + others)` of
  the CPU. Two groups give `(s_logstor + s_sstable)/(s_logstor + s_sstable + others)`. With both
  railed at 2000 and 1000 that is 75% of the shard against roughly 50% before. This needs a joint cap
  or coordinated maxima, which is some of the coupling returning through the front door.
- **Couples logstor's actuator gain to sstable activity.** Logstor's CPU fraction becomes
  `s_logstor/(s_logstor + s_sstable + others)`, and `s_sstable` moves between 50 and 1000 on a
  schedule logstor cannot see, so `B(s)` acquires a time-varying unmeasured disturbance. Note this is
  not a regression that separation introduces: under Option A the same contention exists inside the
  group's run queue. **Separation moves the coupling from task ordering into the shares denominator;
  neither option removes it.** A backlog normalized by *measured* achievable bandwidth (below) is
  partly immune, because it observes the contention and compensates.

#### Comparison

| | One controller (A) | Separate groups (B) |
|---|---|---|
| Fixes the flip-flop | yes | yes |
| Shared-code changes | yes | no |
| Total compaction CPU envelope | unchanged | grows; needs a joint cap |
| Backlog units | forced to bytes/`available_memory()` | free |
| Priority between engines | not expressible | expressible |
| Actuator-gain coupling | inside the run queue | in the shares denominator |
| New measurement required | none | `B(s_max)` if the deadline form is used |

#### The normalizer, and why it is only a question under B

`compaction_manager` divides its backlog by `available_memory()`, a shard's total memory. This
predates logstor and is not a units convention for sharing: the STCS backlog is
`A = Sum Ei * log4(T/Si)`, which is bytes, and the division makes the controller's input dimensionless
so that the control-point constants (1.5, 30) mean the same thing on a 2GB shard and a 16GB one.
Memory is the right scale there because sstable production is driven by memtable flushes and memtable
size is a fraction of shard memory, so the byte-backlog a shard naturally carries scales with it.

None of that reasoning transfers. Logstor's compaction demand is set by disk free space and
fragmentation, and `logstor_disk_size_in_mb` is configured independently of memory — a node can pair
a 10TB logstor disk with 4GB shards or the reverse. The ratio is arbitrary, so dividing disk-driven
work by memory is not a normalization but a distortion that merely happens to be constant on any one
deployment. Under Option A it is nevertheless the correct thing to do, because being summable matters
more than being meaningful. Under Option B, four candidates, in increasing order of both value and
cost:

1. **Keep bytes over `available_memory()`.** No reason to, once nothing needs summing.
2. **A target's worth of work**: `backlog_bytes / (low * segment_size / theta_ref)`. Reads as "fraction
   of a full rebuild of the target". Dimensionless, disk-scale-free, no new measurement. The cheap
   correct answer.
3. **A time**: `backlog_bytes / measured_copy_bandwidth`. Control points in seconds — "if clearing it
   would take more than N seconds, ramp up" — which is interpretable and directly alertable.
4. **The deadline form**, recommended:

```
required_rate   = backlog_bytes / time_to_exhaustion       // time_to_exhaustion = available / allocation_rate
achievable_rate = theta * B(s_max)                          // measured
backlog         = required_rate / achievable_rate
```

   The backlog becomes dimensionless *and* physical: 1 means "I need every share I am allowed to
   have", above 1 means the disk fills regardless of scheduling. The top control point stops being an
   empirical `logstor_compaction_max_shares`-at-full-pressure and becomes "saturate exactly when the
   required rate reaches the achievable rate", so every constant on the curve acquires a defensible
   reading. It also folds in
   the derivative term, since time to exhaustion is where the allocation rate enters.

   Structurally this is the endpoint of the `/theta` argument rather than a separate idea: dividing by
   `theta` cancels the plant gain and dividing by `B(s_max)` cancels the actuator gain, so the loop
   gain is 1 by construction — which is also what makes the integral term's `T_i` a single portable
   number rather than one that has to be tuned for the worst corner.

   The cost is that `B(s_max)` must be measured rather than assumed: compaction bytes copied over the
   compaction group's CPU runtime, which seastar exposes, scaled to the maximum share fraction and
   smoothed. That is real work, and it is the same instrument needed to calibrate anything else here.

#### Recommendation

This document originally recommended Option A first, on the grounds that Option B should not be
taken until there is a joint cap on compaction CPU across the two groups and a measured `B(s_max)`,
since taking B first means shipping an unbounded compaction envelope. Option B (the separate
`logstor_compaction` group) has since been implemented ahead of that prerequisite work — see
[Status](#status-option-b-implemented-joint-cap-still-open) above — so the joint cap and `B(s_max)`
measurement are now open follow-ups rather than a precondition, and should be prioritized
accordingly (see [Open defect #4](#open)).

## Known control-path defects

### Resolved defects

- **Dead window between the trigger and the admission gate.** The single pressure function that drove
  both the gate and the shares controller was zero until available segments fell below the
  since-removed `logstor_compaction_soft_pressure_threshold` (0.15 at the time)
  while auto compaction triggered at `logstor_compaction_trigger_threshold` (0.20) and
  stopped at 0.25. Across all of `[0.15, 0.25)` pressure was 0, `max_used_fraction` clamped to 0.25,
  no segment qualified, `find_top_compaction_candidates` returned nothing, and available never rose
  to the stop watermark — so `run_auto_compaction` rescanned every group's histogram at 10Hz
  forever. The gate is now driven by `compaction_admission_pressure()`, derived from the trigger's
  own watermarks (see [Admission gate](#admission-gate-implemented)). An empty candidate set now
  means the disk genuinely holds no batch with a net gain.
- **Unreachable filter bound.** `max_used_fraction` of 0.90 at full pressure could not produce a
  reclaiming batch at cap 8. The high bound is now the reclaim ceiling
  `1 - 1/max_segments_per_compaction`, which is that bound by construction.
- **Unsatisfiable free-segment target.** The 20% target with a 25% stop watermark required packing
  occupied segments to `U/0.75`, i.e. to 100% at `U = 0.75`, so auto compaction never reached its
  stop condition on any disk more than ~75% full. See [Free-segment target](#free-segment-target).
- **Automatic compaction ran in barriered batches.** `run_auto_compaction` submitted a batch of
  candidates and waited for all of them to finish before selecting the next batch, so the number of
  concurrent jobs decayed to one at the tail of every batch and the compaction load arrived in
  bursts. It now holds a slot per job and submits the next candidate as soon as one finishes. See
  [Strategy](#strategy).
- **The score conflated job size with urgency.** The absolute-reclaimed rule inflated each job to
  maximize space reclaimed, making `max_segments_per_compaction` a write-amplification knob rather
  than a safety bound. See [Reclamation efficiency with batch-extension
  tolerance](#reclamation-efficiency-with-batch-extension-tolerance--implemented).

- **One constant served three unrelated purposes.** `max_segments_per_compaction` was the batch cap,
  the `segment_pool` compaction reserve, and (doubled) the free-segment target's absolute floor, so
  it could not be changed for one without moving the other two. The reserve is now
  `max_compaction_parallelism` — the quantity it protects is one output segment per concurrent job,
  not one per batch — and the floor is `min_segments_per_compaction`.

- **Nothing related the batch cap to the parallelism.** The two were independent constants whose
  product could exceed the free-segment target, which is a stall rather than a slowdown. Both are now
  derived from the target; see [Batch cap and parallelism are one
  parameter](#batch-cap-and-parallelism-are-one-parameter).

### Open

1. **10Hz rescan spin.** When `find_top_compaction_candidates` returns nothing,
   `run_auto_compaction` still sleeps 100ms and rescans every group's histogram. The fiber should
   re-arm on an event instead of polling. It is armed only from `allocate_segment` once the disk has
   been written through once, and from the trigger-threshold config observer; it should also re-arm
   on segment-freed and free-space events.

2. **Selection runs twice.** `find_top_compaction_candidates` computes a candidate set and score
   to rank groups, then `do_compaction` discards it and re-selects. Besides the wasted scan, the
   batch the job runs is not necessarily the one that won the ranking.

3. **Fully dead segments wait for a job.** A segment whose live bytes reach zero is not freed
   until some compaction job happens to select it. It sorts first in the histogram so this is
   usually quick, but it consumes a job slot on a group that is otherwise serialized.

4. **No joint cap on the two compaction groups.** `logstor_compaction_controller` now drives its own
   `logstor_compaction` scheduling group (`lcmp`) instead of sharing `comp` with the sstable
   `compaction_controller`, which fixed the flip-flop described in the resolved-defect history below.
   What remains open is that the two groups' shares are not jointly bounded, so the combined
   compaction CPU envelope can grow past what one shared group allowed (e.g. both railed at 2000 and
   1000 shares is 75% of a shard against roughly 50% before). `logstor_compaction_max_shares` makes
   logstor's half of that envelope tunable in the field, but the two maxima are still independent: a
   joint cap or coordinated maxima, plus a measured `B(s_max)` for logstor, are needed before shares
   between the two groups can be tuned with confidence. See [Sharing the `comp` scheduling
   group](#sharing-the-comp-scheduling-group-one-controller-or-two-groups).

5. **Explicit compaction is unbounded.** `trigger_logstor_compaction` submits one fire-and-forget
   job per group, so a manual or major compaction queues a fiber per tablet; only
   `_compaction_buffer_pool` bounds how many of them run. Automatic compaction is bounded by
   `compaction_limits::auto_parallelism`, and explicit compaction should be too, or the free-segment
   relation in [Batch cap and parallelism are one
   parameter](#batch-cap-and-parallelism-are-one-parameter) holds only for the automatic path.

### Admission gate (implemented)

`compaction_max_used_fraction()` bounds the utilization of the segments
`select_segments_for_compaction` will consider:

```
gate = 0.25 + admission_pressure * (1 - 1/max_segments_per_compaction - 0.25)
```

`compaction_admission_pressure()` is 0 at or above the auto-compaction stop watermark, 1 at or
below the free-segment target, and linear in between — the same watermarks
`should_run_auto_compaction()` uses. That gives the invariant the old gate lacked: **whenever auto
compaction is triggered, the gate is fully open, so any batch with a net gain is admitted.** The
0.25 floor now applies only where auto compaction is not running, i.e. to compaction submitted
explicitly via `trigger_logstor_compaction`, and inside the hysteresis band.

The high bound is the reclaim ceiling `1 - 1/n_in` rather than the old constant 0.90, which at cap
8 could not produce a reclaiming batch at all. Opening the gate fully at the trigger costs
essentially nothing: `reclaimed() >= 1` is enforced independently, the histogram prefix is already
ordered by ascending utilization so the score still decides where to stop, and at cap 8 the scoring
rules differ by under 1% in write amplification. Per the corollary above, refusing an expensive
batch does not avoid the work — it defers it until the segment is usually no cheaper.

The shares controller is driven by a second pressure function over the same watermarks, saturating at
half the target rather than at it; see [Today](#today-a-linear-ramp-on-the-trigger-watermarks).

## Configuration

| Option | Meaning in write-amplification terms |
|---|---|
| `logstor_disk_size_in_mb` | Sets `U = live_bytes / disk_size`. The dominant WA knob — see the utilization table. |
| `logstor_compaction_trigger_threshold` | The free-segment target, hence `U_eff = U/(1-target)`. Default 0.05 (5%). `0` disables the trigger. Also anchors the admission gate and the shares ramp. |
| `max_segments_per_compaction` (`segment_manager_config`, not user-facing) | Upper bound on job size. The bound actually applied is derived per disk by `make_compaction_limits()`, so this only binds on a disk whose target can cover it. Default 32. |
| `compaction_static_shares` | Pins shares and disables the controller. It is the global option, so it disables the sstable compaction controller at the same time. |
| `logstor_compaction_max_shares` | Top of the shares ramp, reached at full space pressure. Default 2000, live-updatable. Caps the peak only; the target and idle points scale down with it only below a cap of 300. |

The constants that are not configuration at all, because they are structural rather than tuning, all
live in `compaction.hh`:

| Constant | Value | What it is |
|---|---|---|
| `max_compaction_parallelism` | 8 | Concurrent compaction jobs on a shard, counted in output buffers — a normal job takes one, a split takes two. Sizes `_compaction_buffer_pool`, which enforces it, and `segment_pool`'s compaction reserve, which keeps an output segment available per concurrent flush. |
| `split_compaction_buffers` | 2 | Slots automatic compaction leaves free so a tablet split is never queued behind it. |
| `max_auto_compaction_parallelism` | 6 | The two above, subtracted. The ceiling on the derived parallelism. |
| `min_segments_per_compaction` | 8 | Smallest batch worth compacting: `1 - 1/8` is the lowest reclaim ceiling that is any use. Also anchors the free-segment target's absolute floor. |

### Free-segment target

`make_free_segment_watermarks()` derives both watermarks from the trigger threshold:

```
low  = max(trigger_percent% * configured_segments, min(2 * min_segments_per_compaction, configured/8))
high = low + max(1, low/4)
```

The percentage is the write-amplification knob. The absolute floor keeps the target meaningful on
small disks, where a percentage rounds down to a segment or two: a job holds a whole batch of
segments until it is done, and the segment pool holds back `max_compaction_parallelism` segments that
normal writes cannot take, so a target of less than two batches leaves the write path nothing to make
progress with. The floor is capped at `configured/8` so it cannot claim an unreasonable share of a
small disk, and it never clamps an explicitly configured percentage *down*.

The floor is anchored on `min_segments_per_compaction` rather than on the batch cap deliberately: the
cap is derived *from* the target, so anchoring the target on the cap would be circular, and it would
also mean that raising the cap silently raised the free-segment target — which is the dominant
write-amplification knob.

The hysteresis is relative (25% of the target) rather than the previous flat +5 percentage points,
which would be a 100% band at a 5% target — compaction would run until 10% free and pay `WA_gc`
2.09 instead of 1.59.

The default was 20% with a 25% stop watermark. That demanded `U_eff = U/0.75`, so at `U = 0.75` it
required packing occupied segments to 100% — unsatisfiable, meaning auto compaction never reached
its stop condition on any disk more than ~75% full. At 5% the stop condition is satisfiable up to
~94% and `WA_gc` at `U = 0.75` drops from 6.30 to 1.59.

Operators who want lower write amplification than the default gives should get it by sizing the
disk, with the utilization table above as the guide.

## Observability

Available today: `compaction_segments_in`, `compaction_segments_out`,
`compaction_records_rewritten`, `compaction_records_skipped`, `compaction_bytes_read`,
`compaction_data_bytes_written`, `free_segments`, `segments_in_use`, `live_record_bytes`,
`compaction_buffers_in_use`, `compaction_buffer_allocation_waits`, `compaction_buffers_dropped`.

The three buffer metrics make the pool's role as the concurrency cap visible: `in_use` against
`max_compaction_parallelism` is the achieved concurrency, and `allocation_waits` counts the
compactions that queued behind it. `buffers_dropped` is normally zero and only moves when a buffer
could not be reclaimed, which shrinks the pool - and the concurrency cap with it - for the lifetime
of the shard.

`WA_gc` is derivable as `compaction_data_bytes_written / data_bytes_written`, and the model
predicts `compaction_bytes_read == compaction_data_bytes_written + data_bytes_written` in steady
state.

Gaps worth closing:

- No `compaction_controller_backlog`, so the shares controller's input is not visible at all, and no
  `auto_compactions_in_progress` for the driver's own occupancy. The latter is what would show
  whether the driver is running at `compaction_limits::auto_parallelism` or is short of candidates,
  which is otherwise indistinguishable from being between batches.
- No `compaction_segments_reclaimed`, so `n_in - n_out` - the quantity the whole cost model is
  written in - has to be inferred from `compaction_segments_in` minus `compaction_segments_out`.

- `compaction_bytes_read` is charged as `nonempty_segments * segment_size`, which is right for the
  IO but should be stated as such, and does not distinguish skipped from copied bytes.
- No histogram of executed-batch efficiency — i.e. observed marginal write amplification. This is
  the single most useful signal for validating the model and for alerting when the admission gate
  is being forced open by pressure.
- No metric for how often `find_top_compaction_candidates` returns empty while auto compaction is
  active (defect 1). Now that the gate is fully open at the trigger this is a genuine out-of-space
  signal rather than a symptom of the dead window, which makes it worth alerting on.
- Nothing exports the derived `compaction_limits`. They move only when the trigger threshold does,
  so a pair of gauges would be enough, but without them there is no way to tell from outside whether
  a small disk is running at the full cap of 32 or at `min_segments_per_compaction`.
- A histogram of executed-batch size would show where inside the cap the tolerance actually stops,
  which is what decides whether raising the cap changed anything on a given workload.

## Plan

Ordered by value per unit of risk, not by how interesting they are.

### Resolved

- Admission gate driven by the trigger's own watermarks, closing the dead window between
  `compaction_pressure()` and `should_run_auto_compaction()`, with the unreachable 0.90 high bound
  replaced by the reclaim ceiling `1 - 1/max_segments_per_compaction`.
- Free-segment target re-defaulted to 5% with an absolute floor and relative hysteresis, and the
  utilization/write-amplification curve documented for capacity planning.
- Efficiency-first `compaction_candidate_score` with tolerance-based batch extension.
- Shares controller re-anchored on the trigger's watermarks, saturating while half the free-segment
  target is still in hand, and `logstor_compaction_soft_pressure_threshold` removed. Interim, until
  the work-based backlog below.
- Batch cap and automatic-compaction parallelism derived from the free-segment target by
  `make_compaction_limits()`, so that their product cannot exceed it; the `segment_pool` compaction
  reserve re-based on `max_compaction_parallelism` and the target's floor on
  `min_segments_per_compaction`. Worth 5–9% of compaction write bytes on any disk large enough to
  carry the cap of 32, and it takes most of what carrying the output residual would.

### Next steps

Those changes alter steady-state behavior on the strength of simulation, so the first step is
to be able to see whether the model holds on a real cluster.

1. **Executed-batch efficiency histogram**, plus a counter for `find_top_compaction_candidates`
   returning empty while auto compaction is active. Nearly free — `compaction_candidate` already
   carries the executed batch's score — and it is the instrument for everything below: without it
   there is no way to tell whether observed marginal write amplification matches the tables above,
   whether the 5% target is actually being held, or whether the gate is being forced open by
   pressure. The empty-candidate counter is now an out-of-space signal worth alerting on rather
   than a symptom of the dead window.

2. **Pass the candidate from `find_top_compaction_candidates` into `do_compaction`** and re-validate
   it instead of re-selecting (open defect 2). Small and self-contained, and it matters more than it
   did: the efficiency score depends on the exact prefix, so re-selection can run a materially
   different batch from the one that won the ranking, and it doubles the histogram scan per job.

3. **Bound explicit compaction** with the same parallelism limit automatic compaction uses (open
   defect 5), so that `parallelism * cap <= low` holds on every path rather than only the automatic
   one. Small, and it is the last place where compaction concurrency is implicit.

4. **One controller for the `comp` scheduling group** (open defect 4). Not an optimization: today an
   idle sstable controller suppresses logstor's shares roughly half the time, and while that is true
   no change to logstor's backlog can be evaluated. Take the single-controller option, not the
   separate-scheduling-group one, until there is a joint CPU cap across the two — the trade is
   analysed in [Sharing the `comp` scheduling
   group](#sharing-the-comp-scheduling-group-one-controller-or-two-groups). It touches shared code
   outside logstor, so it is the largest-blast-radius item here.

5. **Work-based backlog** in `current_backlog()`, including the in-flight-progress term, once 4
   lands. `theta_achievable` is exactly the quantity the score now computes, so the ingredients are
   in place.

### Later

- Give logstor compaction its own scheduling group, with a deadline-normalized backlog and a joint
  CPU cap across the two compaction groups. The better end state, and the only arrangement that can
  express priority between the two engines, but it needs a measured `B(s_max)` and the cap first —
  see [Sharing the `comp` scheduling
  group](#sharing-the-comp-scheduling-group-one-controller-or-two-groups).
- Add an integral term to pin the free level at the target, and with it `WA_gc`. Only after 4 and 5:
  the `/theta` term is what keeps the loop gain high enough for an integrator to be both fast and
  damped. See [Later: an integral term](#later-an-integral-term-to-pin-the-target).
- Replace the per-segment `max_used_fraction` gate with a per-batch efficiency budget
  `efficiency >= theta(pressure)`. The natural expression now that the score is in efficiency units,
  but by the corollary in [Over-provisioning](#over-provisioning) the gate has almost no leverage on
  write amplification, so it buys clarity rather than performance.
- Remove the 100ms candidate-rescan loop and re-arm the fiber on events (open defect 1). Now a
  CPU-waste cleanup rather than a correctness problem.
- Free a segment eagerly when its live bytes reach zero, instead of waiting for a job to select it
  (open defect 3).
- Close the remaining [Observability](#observability) gaps.

### Conditional, measure first

- **Carry the compaction output residual across jobs** instead of sealing a partial segment at
  every `compaction_buffer::close()`. Design in [Carrying the output
  residual](#carrying-the-output-residual). It was the third step of this plan when the batch cap was
  8; at a cap of 32 it is worth 1.7% rather than 6–9%, against much the largest blast radius of any
  small change here — per-group buffers, a `pinned` mark on `segment_descriptor`, a per-group flush
  semaphore, and that semaphore extended over `make_snapshot` and `discard_segments`. Revisit only if
  the executed-batch efficiency histogram shows the residue costing more on real disks than the model
  says, or on deployments whose disks are too small for the full cap.
- Prefetch across a compaction job's input segments, so that a job's read queue depth is more than 1.
  Only worth doing if compaction is measurably IO-starved at high shares; it is the alternative to
  raising `max_compaction_parallelism`, and unlike that it costs no free segments.
- Global cross-group victim selection capped at ~4 groups per job. Only pays above roughly 64
  groups per shard, and can hurt under skew at moderate counts, so it must be gated on group count
  and validated by measurement.

## Appendix: methodology

The numbers on this page come from a simulation of the segment pool. It lives in
`test/manual/logstor_compaction_sim.cc` and runs the engine's own selection code over a modeled
disk, so that a parameter can be swept without a cluster:

```
ninja build/dev/test/manual/logstor_compaction_sim
build/dev/test/manual/logstor_compaction_sim --smp 1 --utilization 0.75 --trigger-threshold 0.1
build/dev/test/manual/logstor_compaction_sim --smp 1 --sweep trigger-threshold=0.03,0.05,0.1,0.2
build/dev/test/manual/logstor_compaction_sim --smp 1 --self-test
```

`--help` lists every parameter, `--sweep name=v1,v2,...` may be repeated to take the product of
several of them, and `--csv` prints the resulting table for a spreadsheet. Because write
amplification is set by the free level rather than by the rule under test, a configuration that
reclaims faster than another settles at a different level and cannot be compared against it directly;
`--free-band 1` pins the hysteresis to a single segment so that both run at the same `U_eff`.

What the simulator runs rather than reimplements is everything a change to this page's strategy would
touch: `segment_descriptor` and `segment_set` with its `log_heap` free-space histogram, so victim
ordering including the histogram's bucketing is real; `select_compaction_batch()` and with it the
efficiency score, the extension tolerance and `estimate_required_segments()`;
`top_compaction_candidates`, which ranks the groups; `make_free_segment_watermarks()` and
`make_compaction_limits()`; `auto_compaction_wanted()`; and the `ondisk::` sizes and alignments, so
what a segment holds and what it wastes are what the engine would produce. The index, the compaction
job, the driver that keeps jobs in flight, the segment pool and the write path are modeled, since the
engine's versions of them are IO or future bound.

Parameters unless stated otherwise: 900 segments of 128KB, 32 records per segment, `U = 0.75`, a 10%
free-segment target, one group, uniform random overwrites, the dataset written through once before
the run and half the run taken as warm-up. Workloads: uniform, zipf 0.99 and 90/10 hot-cold, all
overwrite-only.

Divergences from the engine:

- The write path is abstracted. A user record goes straight into an open segment of its group, as if
  the separator were instantaneous, so the mixed segments and the separator buffers are not modeled -
  they cost space and one more copy of every user byte, and the device therefore writes one copy more
  than the simulator's device write amplification. What compaction sees - what the segments of a
  group hold, how they are laid out and how they age - is modeled.
- There is no clock. Compaction runs whenever the free level asks for it and is taken to keep up, so
  what is measured is the steady state at the free level the watermarks produce. Compaction falling
  behind is a property of the device and of the shares controller.
- Deletes, TTL expiry and tombstones are not modeled, and neither are recovery and tablet splitting.

### The absolute numbers on this page are due a re-take

The tables above were taken with an earlier, throwaway simulation that is not the one described here.
It modeled an *idealized* cleaner: one victim at a time, always the emptiest segment on the disk, and
an output stream packed perfectly across jobs. A thirty-line model with those two properties
reproduces every entry of the [disk utilization](#disk-utilization) table to within 3%, which is what
identifies it.

The simulator reproduces the comparisons those tables were written to make - the efficiency score and
the absolute-reclaimed rule differ by well under 1% at cap 8, extending the batch within a tolerance
costs a few percent of write amplification for half as many jobs, a cap of 32 is the best of 8 to 90,
and utilization-ordered victims are worth roughly 2x over random ones - but it reports materially
higher absolute write amplification. At `U = 0.75` and a 10% target it gives `WA_gc` 2.9 at a cap of
8 and 2.7 at a cap of 32, against the 1.97 above.

The difference is the two idealizations, and both are properties of the engine rather than of the
model:

- **A job takes a batch, not the emptiest segment.** A single victim can never reclaim, since
  `n_out = ceil(live/usable)` is 1 for any segment holding anything, so the batch has to reach at
  least `min_segments_per_compaction`, and its later members are fuller than its first. The mean
  victim utilization is what the copy cost is proportional to, and over 8 victims it is several
  points above the emptiest segment's.
- **The output is whole segments.** A job flushes its residual as a partly filled segment, so it
  reclaims `n_in - ceil(live/usable)` rather than `n_in - live/usable`. At the operating point above
  that is about a fifth of what the batch would otherwise reclaim.

They interact, which is why the gap grows with utilization: a more expensive cleaner has to clean
more often, its segments get less time to decay, and the victims it finds are fuller. That feedback
is the reason a 25% arithmetic loss shows up as a 45% write-amplification loss.

The practical consequence is that [carrying the output
residual](#carrying-the-output-residual), which this page concluded was worth 1.7% and dropped, is
worth re-examining: carrying it is also what would let a job reclaim from a batch smaller than
`min_segments_per_compaction`, which is where the idealized cleaner's advantage comes from. That
needs the `floor` form of `estimate_required_segments` the same section calls out as needing its own
simulation, and it is now cheap to simulate.
