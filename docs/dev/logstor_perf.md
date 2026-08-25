# Logstor Performance Testing

This document describes how to measure the cost of a logstor operation on one node, which tools
measure what, and how to run them so that two runs can be compared. It is about the CPU and the IO
one read or one write costs - the level at which a change to the hot path is written and reviewed.
For the behaviour of a cluster under a workload, which is a question about metrics rather than about
these tools, see `metrics.md` and the space accounting section of [logstor.md](logstor.md).

For the general logstor architecture see [logstor.md](logstor.md), and for the cost model behind
compaction see [logstor_compaction.md](logstor_compaction.md).

## What to measure

**Instructions per operation is the number to compare between runs**, and cycles per operation the
one to read next to it. Every test reports both, taken from the hardware counters over the
measurement window. Between iterations of one build the instructions hold to a fraction of a percent
on the tests that only use the CPU and to a few percent end to end, which is what makes a difference
of that size worth reading anything into. Cycles hold only as well as the machine does: the same work
takes more of them when the clock is scaled down or an SMT sibling is busy, so cycles say whether a
change moved the work or moved how well it runs, and instructions say whether it moved the work.
Throughput is not that: the throughput of anything that touches the disk is bounded by the disk, and
of anything that does not by whatever else the machine is doing at the time. Throughput still
matters as the sanity check that a change did not trade instructions for waiting, but a regression
or an improvement is argued from the CPU counters.

**The per operation IO and cache counters say which path the run measured.** A read served from the
logstor cache and a read that went to a segment are the same operation and a different measurement,
and a write that batches with others pays for a fraction of a device write. The counters are what
tell them apart, and every run should be checked against them before its numbers are believed:

- a read served from the cache does `0.00 reads/op` and `1.00 cache hits/op`
- a read that went to a segment does `1.00 reads/op` of about a block, and one cache miss
- a write does a fraction of a write per operation, since the writes of a shard are batched into one
  buffer, and the bytes per operation are the share of the buffer its record took
- a run that was meant to exclude compaction shows the write bytes of its own records and no more

A logstor table has neither a commitlog nor sstables, so for a logstor run these counters attribute
all of the disk traffic and all of the cache lookups to logstor.

## The tools

### `scylla perf-simple-query --logstor`

Measures the whole read and write path of one node - CQL, the coordinator, the replica and logstor -
against a logstor table, in one process, with the client loop on the same shards. It is the same
test that measures an sstable backed table, so **the baseline is the same command without the
flag**.

Use it for the cost of an operation as a workload sees it, for comparing the two storage engines,
and for anything that involves the layers above logstor.

```
TMPDIR=/nvme taskset -c 2 build/release/scylla perf-simple-query --logstor --smp 1 \
    --idle-poll-time-us 0 --logstor-disk-size-in-mb 4096 --partitions 1000000 \
    --duration 10 --json-result read.json
```

The flag implies `--tablets`, since logstor is only used with tablets and its compaction groups are
the tablets of the table. It also sizes the segment pool, which is empty by default:

| Option | Meaning |
|---|---|
| `--logstor-disk-size-in-mb` | the segment pool of a shard. Must hold the dataset and the dead records the overwrites leave behind |
| `--logstor-file-size-in-mb` | one data file. The pool is allocated in files of this size |
| `--logstor-format-on-startup` | format the files upfront, so that no write pays for formatting a file. Leave it on |
| `--auto-compaction` | defaults to on for `--logstor`, off otherwise. See below |

The rest of its options work as they always have. The ones that matter here:

| Option | Meaning |
|---|---|
| `--partitions` | the dataset. Whether a read hits the cache is decided by this against the memory of the node |
| `--concurrency` | operations in flight per shard |
| `--duration` / `--operations-per-shard` | one second iterations, or a fixed number of operations per shard |
| `--write` / `--delete` | measure the write or the delete path instead of the read path |
| `--enable-cache 0` | build the index with no cache at all, so that every read goes to a segment |
| `--bypass-cache` | add `BYPASS CACHE` to the query, so that a read neither looks in the cache nor populates it |
| `--collection N` | add a collection column of N cells, for a larger row |
| `--initial-tablets` | tablets of the table, which is how many compaction groups the shard writes into |
| `--json-result` | write the result to a file, see *Comparing runs* |

What the test writes differs on a logstor table. A record takes its timestamp from a row marker or a
partition tombstone, and a table holds a whole row per partition, so the write is an `INSERT` of the
row and the delete removes the whole partition, rather than the cell update and the cell delete an
sstable run measures. `--counters` is refused, since logstor does not store counters.

**Compaction.** A logstor run leaves auto compaction on, because compaction is what gives free
segments back: with it off, a write test stalls once the segment pool is full of dead records, and
how long that takes is a function of the pool size rather than of anything being measured. Turn it
off with `--auto-compaction 0` to measure a write path with nothing else running, and then size the
pool so that the run cannot exhaust it. An sstable run keeps compaction off during the measurement,
as it always has.

### `test/perf/perf_logstor`

Drives a logstor directly - no CQL, no coordinator, no replica - and measures the steps of a read
and of a write both together and one at a time. Use it to iterate on a change to the hot path: it
says which step the change moved, it starts in seconds, and the steps that touch neither the disk
nor the caches have almost no variance.

```
TMPDIR=/nvme taskset -c 2 build/release/test/perf/perf_logstor --smp 1 \
    --idle-poll-time-us 0 --concurrency 100 --test all
```

| Test | What one operation does |
|---|---|
| `index-lookup` | look one key up in the primary index |
| `index-insert` | point the index of one key at a record, which is what a write does once its record is in a segment. Includes the lookup |
| `deserialize` | deserialize one record from a buffer, which parses its header and copies out the bytes of its value. Not a step of a point read any more - it takes the fields it needs from the header at their offsets and decodes the value out of the buffer it read - and kept to measure what that saves |
| `decode` | decode the value of one record into the mutation a read returns |
| `build-mutation` | build the mutation one write is given. Not a step of a write: a node is handed it by the layer above logstor, and only the `write` test builds one per operation, so this is what has to come off `write` before its steps add up |
| `encode` | encode one mutation into the value the record of a write carries: one walk of the partition into the buffer the encoder reuses, and a copy of the exact bytes out of it |
| `record-header` | build the header of one record, which copies the decorated key of the partition into it |
| `record-sizes` | `record-header`, and then what `log_record_writer::compute_sizes()` does: the size of the header and the size of the value, both arithmetic, so that the writer knows how much room to ask the buffer for |
| `append` | copy one record whose sizes are already known into the buffer of the writer |
| `serialize` | what a write pays before its record reaches the buffer: `encode`, `record-header`, `record-sizes` and `append` |
| `freeze` | freeze one mutation into a `canonical_mutation`, which is what a record value was before the record format. Not a step of a write any more: it and `materialize` are kept so that the cost of the two forms can be compared in one run |
| `materialize` | turn one `canonical_mutation` into a mutation, the counterpart of `decode` before the record format |
| `cache-lookup` | look one cached mutation up and copy its partition out of the cache region |
| `cache-populate` | evict the cached partition of one entry and admit one in its place |
| `raw-read` | one DMA read of the size of a record, straight to the data file, with no logstor in it |
| `read-cached` | a whole read the cache serves |
| `read-disk` | a whole read that goes to a segment: index lookup, DMA read, decoding of the value |
| `segment-read` | the read of the record from its segment, without the index lookup and without decoding the value |
| `write` | a whole write, up to and including the flush of the buffer its record went into |

Everything above `read-cached` touches no disk - the two cache tests touch the cache, the rest touch
neither it nor the disk. They run 32 operations per invocation of the measurement loop, since one of
them costs of the order of what the loop itself costs, and they run without concurrency, since they
never wait for anything.

The dataset is written once and serves every test. Its index has the cache enabled, and `read-disk`
takes the segment by bypassing the cache per read rather than by disabling it, so that both read
paths are measured against the same data.

| Option | Meaning |
|---|---|
| `--test` | comma separated test names, or `all` |
| `--partitions` | partitions written per shard |
| `--columns` | value columns of the table. The description of the schema a record carries and the framing of its cells both scale with this, so it is what a change to the format of a record is measured against |
| `--value-size` | bytes of the value of one column |
| `--concurrency` | operations in flight per shard, for the tests that wait for the disk |
| `--duration` / `--operations-per-shard` | one second iterations, or a fixed number of operations per shard |
| `--segment-size-in-kb`, `--file-size-in-mb`, `--disk-size-in-mb` | the geometry of the segment pool of a shard |
| `--compaction` | let compaction run. On by default, for the same reason as above |
| `--dir` | where to put the logstor files. Defaults to a temporary directory |
| `--json-result` | write one file per test, suffixed with the name of the test |

It prints how much of the segment pool the dataset took, what one record of the dataset takes against
the bytes of value it carries - and what the same record would take with a `canonical_mutation` value,
for comparison - and warns when the dataset does not leave the pool room for the dead records the
overwrites of the write test will leave behind.

### `test/perf/perf_logstor_record_format`

Measures the record format on its own: what encoding the partition of a mutation into the value of a
record and decoding it back cost, and what the record takes, for as many row shapes as one run is
given. It builds no logstor and touches no disk, so it runs in seconds and has almost no variance;
use it to iterate on a change to `replica/logstor/record_format.cc`, and `perf_logstor` - whose
`encode` and `decode` tests are the same two steps inside a whole read and a whole write - to see
what the change did to an operation.

```
taskset -c 2 build/release/test/perf/perf_logstor_record_format --smp 1 \
    --columns 1,5,30 --value-size 20,300
```

| Measurement | What one operation does |
|---|---|
| `encode` | encode one partition into the value of a record, which is what a write does: one walk of the partition into the buffer the encoder reuses, and a copy of the exact bytes out of it |
| `encode, sizing walk only` | what `encode` used to spend sizing the buffer it fills, before it encoded in one pass: a walk of the partition that writes nothing, which is what `measure_row_value()` still does. Kept to measure what it cost |
| `freeze (canonical_mutation)` | freeze the same partition into a `canonical_mutation`, which is what a record value was before the format |
| `decode` | decode the value into the mutation a read returns, under the schema version the record was written with |
| `decode, other schema version` | the same under a later version of the schema, which maps the record's columns onto the schema's through a `column_translation`. The translation is built before the measurement, since a read builds one per pair of schema versions and not per record |
| `materialize (canonical_mutation)` | turn the `canonical_mutation` of the same partition into a mutation, which is what `decode` replaced. Measured under both schema versions as well |

Every measurement reports instructions and allocations per operation, which are the numbers to
compare, and a duration for scale. There are no cycles and no IO counters here, and no result file.

Each shape prints its sizes first: the encoded value and the whole record in a segment, both against
the bytes of value the row carries and against what the same partition took with a
`canonical_mutation` value, and then the value split into its head, the description of the schema it
carries and the row itself. That split is what says whether a shape pays for the format or for the
description - phase 2 of the format moves the description to a per-buffer dictionary, and a row of
thirty columns spends 184 bytes on it.

| Option | Meaning |
|---|---|
| `--columns` | comma separated value column counts of the table. The description of the schema a record carries and the framing of its cells both scale with this |
| `--value-size` | comma separated sizes of the value of one column, in bytes. Every combination of the two is measured |
| `--columns-set` | columns of the row that carry a cell, or 0 for all of them. Fewer than all makes the record carry a bitmap of the columns it holds, which is what an update of part of a row writes |
| `--iterations` | samples per measurement, each of a batch of operations, since one operation costs of the order of what the measurement loop itself costs |

## Reading the numbers

The steps are meant to add up, and that is the first thing to check. A whole read that goes to a
segment costs about what the read of its record costs plus the materialization of its mutation plus
the index lookup, and a read the cache serves costs a fraction of either, since it copies a
partition that is already in memory rather than deserializing one:

```
read-disk      ~  segment-read + decode + index-lookup
read-cached    ~  cache-lookup + index-lookup
segment-read   ~  raw-read + what the segment manager puts between them
serialize      ~  encode + record-sizes + append
write          ~  build-mutation + serialize + (index-insert - index-lookup)
                  + the writer machinery and the separator
```

What is left over on either side of one of these is a number in its own right: `serialize` beyond
`encode`, `record-sizes` and `append` is nil, and `write` beyond the rest is what the machinery
around the record costs.

Two subtractions in that last relation are easy to get wrong. `build-mutation` is the test's own
work and has to come off `write` first - at thirty columns it is a third of what the write test
reads. And the write path inserts into the index without looking the key up first, so what it pays
is `index-insert - index-lookup`, not `index-insert`.

If a change moves `read-disk` without moving any of the steps under it, either the change is in the
part of the read that none of the steps cover, or the measurement is not measuring what it looks
like. Check the per operation counters first.

The same holds across the tools. `perf-simple-query --logstor` costs several times what
`perf_logstor` reports for the same operation, and the difference is the CQL, coordinator and
replica work that the first includes and the second does not. A change to logstor should move both
by the same absolute amount; if it moves only the end to end number, it moved something above
logstor.

## Running a measurement

- **Build `release`.** `dev` numbers are not comparable to anything, and the ratios between the
  steps are not the same either.

```
./tools/toolchain/dbuild ninja build/release/scylla build/release/test/perf/perf_logstor \
    build/release/test/perf/perf_logstor_record_format
```

- **One shard, pinned.** `--smp 1` with `taskset -c <cpu>` on an otherwise idle machine, with the
  CPU governor set to performance. Scale to more shards only to look for cross shard or scheduler
  effects; per operation numbers should stay flat as shards are added, and it is worth confirming
  that they do.

- **On a machine that is not yours alone, argue from the instructions.** Neither the governor nor
  what else the machine is doing changes how many instructions an operation retires, and both change
  how many cycles it takes: a run measured here while another user was building read 1812 cycles per
  operation for an index lookup against 523 for the same lookup on a quiet machine, and 713.4
  instructions against 711.2. Record what the machine was doing - the load and the busy time of the
  core the run was pinned to and of its SMT sibling - so that a run taken during someone else's build
  can be recognised rather than trusted.

- **Turn the reactor's idle polling off** with `--idle-poll-time-us 0` on anything that waits for
  the disk. A reactor with nothing to do polls, and the instructions it retires while a test waits
  for the disk are charged to the operations of that test: the write test of `perf_logstor` reads
  47k instructions per operation with the default idle polling and 14.6k without it, at the same
  throughput. Without this, two thirds of the cost of a write is the reactor waiting for the disk,
  and a change that only moved the throughput would read as a change in the cost of a write.

- **Give the tests that wait for the disk enough concurrency**, at least 32, and read `polls/op` to
  confirm they had it. What is left of the reactor's poll loop after the point above is a cost per
  turn of that loop rather than per operation, so it is charged to an operation in proportion to how
  few operations each turn served. Over a concurrency sweep the two separate cleanly:
  `instructions/op = work + cost_of_a_poll * polls/op` fits to within half a percent, and for
  `segment-read` it gives 7150 instructions of work and about 1900 per poll. That fit was taken
  before the record format, which took `segment-read` to 5625; the poll cost per turn is a property
  of the reactor and still holds. At a concurrency of one
  that poll term is 3 polls per operation and most of what the test appears to cost; by 100 it is
  0.03 polls per operation and nothing at all. `polls/op` well under 0.1 means the number being read
  is the operation.

- **Put the data on a real disk.** Both tools write their segments under `TMPDIR`, which on many
  machines is a tmpfs - a "read from disk" measured there is a read from memory. Point `TMPDIR` (or
  `perf_logstor --dir`) at the device the numbers are meant to describe.

- **Fix the seed** with `--random-seed`, so that two runs pick the same keys in the same order.

- **Prefer a fixed amount of work** with `--operations-per-shard` when comparing two builds, and
  several iterations of `--duration` when looking at the spread.

- **On a machine whose disk is shared, take the IO tests more than once and keep the clean ones.**
  The same binary and configuration returned 190k operations per second at `polls/op` 0.03 on one
  run of `read-disk` and 47k at 1.5 on the next, which puts about 1800 instructions per poll onto
  the number. When comparing two builds, run them alternately at each row shape rather than one
  side after the other, so that neither side owns a disk condition, and quote the median of the
  iterations whose `polls/op` was under 0.1. Two to three runs per cell is what that takes here.

- **Take the noise floor from separate processes, not from one.** The `mad` a run reports is the
  spread between its own iterations, which is the smaller of the two. Running the same configuration
  three times as three processes and taking the worst deviation from their median gives what a change
  actually has to beat: on the machine these numbers come from that is 0.05% on the tests that only
  use the CPU and between 0.1% and 0.4% on `write` depending on how quiet the machine is, so a change
  of a tenth of a percent is a change.

- **Measure a write at more than one concurrency.** Sealing a buffer, reserving room for it in a
  segment, submitting its IO and handing its records to the separator are per buffer, and the records
  in the buffer divide them. The same write costs 31.5k instructions and writes eight times its
  record's bytes at a concurrency of one, 15.1k at sixteen, and 14.4k from sixty-four up, where it is
  flat. A number taken at one concurrency describes that concurrency only, and the low end is the
  latency-bound case a workload actually cares about.

- **Give a write test of a wide row a pool it cannot fill, and no compaction.** The write test
  overwrites the dataset, and the wider the row the sooner the dead records fill the segment pool;
  once they do, the test measures compaction rather than the write path, and says so with a `mad` of
  several percent and a throughput a factor lower than the same test one shape narrower. Thirty
  columns of 20 bytes needs `--compaction 0` and a pool of a few GB before its number holds still.

- **Know which of the two write regimes a run is in.** Whether compaction is in the number is decided
  by whether the pool fills, not by `--compaction`: with a pool the test cannot fill, turning
  compaction off moves the write by less than the run to run spread, because there is nothing for it
  to reclaim. Overwriting into a pool a quarter the size of what the test writes puts it in the other
  regime, and the per second lines show the moment it crosses over - here the write went from 14.5k
  instructions and no reads to 16.3k and 651 read bytes per operation once the pool ran out. Both are
  real; a comparison just has to be between two runs in the same one.

- **Change one thing at a time**, and keep the dataset, the concurrency and the pool size identical
  between the runs being compared.

## Comparing runs

`--json-result` writes the parameters of the run, the aggregation over the iterations, and the per
operation counters of the median iteration:

```json
"stats": {
    "median tps": 258581.6,
    "instructions_per_op": 19946.4,
    "cpu_cycles_per_op": 10695.4,
    "allocs_per_op": 20.02,
    "reads_per_op": 1.0,
    "read_bytes_per_op": 4654.0,
    "writes_per_op": 0.0,
    "write_bytes_per_op": 0.0,
    "mad tps": 1455.7,
    "mad instructions_per_op": 8.1
}
```

`instructions_per_op` is the number to diff between two builds, and `mad instructions_per_op` is the
spread of that number within the run, which is the smallest difference between two runs worth
reading anything into; `mad tps` says the same for throughput. `allocs_per_op` and the IO counters
say *why* a difference happened, and are often where the actual finding is. `test_properties.type` names the workload
(`read_logstor`, `write_logstor`, `logstor_read-disk`, ...), so results from a matrix of runs can be
collected into one place.

## Profiling a step

For a profile rather than a number, run the pinned single shard case under `perf`:

```
perf record -g --call-graph dwarf -F 499 -e instructions:u -D 12000 \
    -- build/release/test/perf/perf_logstor --smp 1 --test segment-read --duration 30 ...
```

Three things about that command are the difference between a profile of the hot path and a profile of
something else:

- **Sample the event the argument is made from.** `-e instructions:u` samples what the counters
  count. The default samples cycles, and the two disagree wherever the code is not retiring
  instructions at the same rate - a sleeping syscall and a memory stall are most of a cycle profile
  and almost none of an instruction one.

- **Skip the population of the dataset** with `-D <ms>`, and give the test enough `--duration` to
  dominate what is left. A run writes its dataset before it measures anything, and writing a hundred
  thousand records takes far longer than the seven seconds of measurement that follow: a profile of
  the whole process is mostly the reactor waiting for the disk during the populate phase, which is
  what makes the poll loop look like the hot path when it is not.

- **Do not read the call tree too literally.** Everything past the first `co_await` of a coroutine
  runs as its own task from the reactor's loop, so the work of a step does not appear under whatever
  called it. The flat profile is the honest one; use the step tests to attribute cost, and the
  profile to say what the cost is made of.

And one thing a profile of these tests cannot be used for at all: **the shares in it do not convert
into instructions per operation.** Recording keeps the reactor from sleeping, so the run being
profiled is not the run that was measured - under `perf record` the write test reads 56.8k
instructions and 31.9 polls per operation against 14.5k and 0.045 without it, and about seventy
percent of the samples are a poll loop the measured run does not have. Renormalizing what is left
onto the measured cost is not safe either: doing that here put the sizing pass of a record at ~1900
instructions, and a test written to measure it directly said 291. Use the profile to find which
functions are on the path, then write a test for the ones that look expensive.

Symbols need the perf test binaries to keep them, which they do not by default:

```
./configure.py --mode=release --perf-tests-debuginfo 1
```

## A workload matrix

| Case | Command |
|---|---|
| read, from the cache | `perf-simple-query --logstor`, dataset well under memory |
| read, from a segment | `perf-simple-query --logstor --enable-cache 0`, and separately `--bypass-cache` |
| read, realistic mix | `perf-simple-query --logstor`, dataset several times memory, cache on |
| write | `perf-simple-query --logstor --write` |
| write, no compaction | `perf-simple-query --logstor --write --auto-compaction 0`, pool sized so it cannot fill |
| delete | `perf-simple-query --logstor --delete` |
| sstable baseline | the same commands without `--logstor` |
| one step of a path | `perf_logstor --test <step>` |
| the cost of a row shape | `perf_logstor --columns N --value-size B`, which is what a change to the format of a record is measured against |
| a change to the record format | `perf_logstor_record_format`, which encodes and decodes alone, over several row shapes in one run |
| what a record takes, and on what | `perf_logstor_record_format --columns N --value-size B --columns-set K` |
| what logstor adds to a read | `perf_logstor --test raw-read,segment-read` |
| what a write costs beyond its steps | `perf_logstor --test build-mutation,encode,record-sizes,append,index-insert,index-lookup,write` |
| what a buffer amortizes | `perf_logstor --test write` at several `--concurrency`, from 1 up |
| a write with compaction working | `perf_logstor --test write --duration 60 --disk-size-in-mb`, small enough that the pool fills |

Two things to keep in mind when comparing a logstor run against the sstable baseline. A logstor
write has no commitlog but does wait for its record to reach a segment, while an sstable write goes
to the commitlog and the memtable, so the two write paths are not the same amount of durability. And
an sstable read of a dataset that was never flushed is served from the memtable, which is not a
comparison against anything a logstor read does; flush first, or compare against a dataset that does
not fit in memory.

## Where the code is

- `test/perf/perf_simple_query.cc` - the end to end test, its statements and its per operation
  counters.
- `test/perf/perf_logstor.cc` - the micro benchmark. Adding a step to measure means adding a method
  to `logstor_bench` and a name to `test_kinds`.
- `test/perf/perf_logstor_record_format.cc` - the record format benchmark: the row shapes it builds,
  the size report and its own measurement loop.
- `test/perf/perf.hh`, `test/perf/perf.cc` - the measurement loop, the per operation CPU and IO
  counters, and the result file.
- `test/lib/logstor_test_utils.hh` - the logstor, the cache and the compaction group a test brings
  instead of a database and a table. Shared with the unit tests.
