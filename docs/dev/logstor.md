# Logstor

## Introduction

Logstor is a log-structured storage engine for ScyllaDB optimized for key-value workloads. It provides an alternative storage backend for key-value tables - tables with a partition key only, with no clustering columns, and where rows are written whole, as a single value (individual
columns are usually not updated separately).

Unlike the traditional LSM-tree based storage, logstor uses a log-structured approach with in-memory indexing, making it particularly suitable for workloads with frequent overwrites and point lookups.

## Architecture

Logstor consists of several key components:

### Components

#### Primary Index

The primary index is entirely in memory and it maps a partition key to its location in the log segments. It consists of a B-tree per each table that is ordered token.

#### Cache

The cache holds the deserialized partition of a key next to its entry in the primary index, so that
a read which finds it there is answered without touching the disk. It has no memory, LRU or
reclaimer of its own: a cached partition is allocated in the LSA region of the row cache and linked
in its LRU list, so the two caches share the memory of the shard and compete for it under the same
replacement policy. What logstor keeps is a back-pointer from the index entry to the cached
partition, entangled with the entry so that it survives the moves LSA compaction makes.

Because it shares the row cache's tracker, the logstor cache also reports through the row cache's
`cache_*` metrics, and a node running both storage engines sees the sum of the two:

| Metrics | How the logstor cache accounts for them |
|---|---|
| `bytes_used`, `bytes_total` | Taken from the shared region, so they cover both caches with nothing to do |
| `partition_hits`, `partition_misses` | Per read. A key the index does not hold is a hit: the index is the authority on which keys exist, so that answer is served from memory too |
| `partitions`, `partition_insertions`, `partition_evictions`, `partition_removals` | Per cached partition. Removals are the invalidations, which are a write over the key, an erase of it, or a drain of the cache |
| `rows`, `row_hits`, `row_misses`, `row_insertions`, `row_evictions`, `row_removals` | A logstor table has no clustering columns, so a partition is at most one row, and a partition which is a tombstone alone is none. The row count of a cached partition is derived from it rather than stored, and its rows are not linked in the LRU: the cache evicts a whole partition at a time |
| `reads`, `reads_with_misses`, `active_reads` | Per read, from its start to its end. A read that fell through to the disk is one with a miss |
| `mispopulations` | A read which fetched a record and then found the index entry changed or gone, which makes what it read stale, so it leaves the cache alone |
| `concurrent_misses_same_key` | Two reads missed the same key, and the second found it populated by the first |

The rest of the `cache_*` metrics belong to structure the logstor cache does not have - the merging
of partition versions, the update of the cache on a memtable flush, the stitching of an underlying
reader, dummy rows, and the compaction and tombstone scanning of the cache read path - and stay at
zero for it.

#### Segment Manager

The `segment_manager` handles the allocation and management of fixed-size segments (default 128KB). Segments are grouped into large files (default 32MB). Key responsibilities include:

- **Segment allocation**: Provides segments for writing new data
- **Space reclamation**: Tracks free space in each segment
- **Compaction**: Copies live data from sparse segments to reclaim space
- **Recovery**: Scans segments on startup to rebuild the index
- **Separator**: Writes to all _compaction groups_ (tablets and even tables) go to a single active segment. The separator splits these mixed segments, which have records from different compaction groups, into segments that each has a single compaction group. This separation is useful when migrating tablets.
- **Direct writes**: A group that writes fast enough skips both of those: it writes into a buffer bound to a segment of its own, and the separator never sees its records. See the data flow section below.

The data in the segments consists of records of type `log_record`. Each record holds the partition of one key, encoded in the logstor record format, together with the metadata of the record.

The `segment_manager` receives new writes via a `write_buffer` and writes them sequentially to the active segment with 4k-block alignment.

#### Write Buffer

The `write_buffer` manages a buffer of log records and handles the serialization of the records including headers and alignment. It can be used to write multiple records to the buffer and then write the buffer to the segment manager.

The `buffered_writer` manages multiple write buffers for user writes, an active buffer and multiple flushing ones, to batch writes and manage backpressure.

### Data Flow

**Write Path:**
1. Application writes mutation to logstor
2. Mutation is converted to a log record
3. Record is written to write buffer
4. The buffer is switched and written to the active segment.
5. Index is updated with new record locations
6. Old record locations (for overwrites) are marked as free

Only step 4 writes to the disk - all other steps only update in-memory metadata.

A record written this way reaches the disk twice: once here, into the shared active segment, and
once more when the separator rewrites it into a segment of its own compaction group. The direct
write path below is the same write without the second pass, for the groups that write enough to be
worth it.

**Read Path:**
1. Application requests data for a partition key
2. Cache lookup, which serves the read when the partition is cached
3. Index lookup returns record location
4. Segment manager reads the bytes of the record from disk. The read is issued over the
   4096-byte-aligned range that covers the record and trimmed back to it, rather than over the
   record as it lies: a record is smaller than a block, and the drive charges for a request that
   does not start on a block boundary as such, not for the pages it touches, so the covering read
   is cheaper than the unaligned one over the very same pages. It costs bandwidth - `bytes_read`
   counts what the disk served, so the ratio of it to the record bytes is the read amplification
   this buys the latency with
5. The key of the record is compared against the key of the read, and the value of the record
   is decoded into the mutation the read returns. Neither the header of the record nor its
   value is copied out of the buffer that was read: the read takes the two fields it needs
   from the header at their offsets and decodes the value in place

**Separator:**
1. When a record is written to the active segment, it is also written to its compaction group's separator buffer. The separator buffer holds a reference to the original segment.
2. The separator buffer is flushed when it's full, or requested to flush for other reason. It is written into a new segment in the compaction group, and it updates the location of the records from the original mixed segments to the new segments in the compaction group.
3. After the separator buffer is flushed and all records from the original segment are moved, it releases the reference of the segment. When there are no more reference to the segment it is freed.

**Direct writes:**
1. A compaction group that writes fast enough is given two write buffers of its own, each bound to
   one of the group's segments from the moment the buffer is allocated. A full segment holds exactly
   one buffer, at offset zero, so the final location of a record appended to such a buffer is known
   as soon as it is appended.
2. A write of that group goes straight into the buffer, the index is updated with the final location
   there and then, and the write is acknowledged - with the record still only in memory. Nothing
   goes to the active segment and the separator is given nothing, so the record reaches the disk
   once instead of twice. Because such a record is serialized before the write returns and is never
   retained, it is written out of the mutation's own key and the encoder's own buffer, without a
   record of its own: a write on the ordinary path has to own its value for the separator to replay
   later, and this one does not.
3. The buffer is written out when it fills, or when it has held records for a whole sync period, and
   its segment is linked into the group after that - exactly like the output of the separator or of
   a compaction. While the buffer fills, its segment is unwritten and belongs to no segment set, so
   nothing can pick it for compaction, scan it or free it. The group rotates into its second buffer
   while the first one is being written, and is given a fresh buffer and segment afterwards. A group
   has one flush in flight at a time, so a burst that fills the second buffer before the first
   reaches the disk waits for that write rather than take the ordinary path, which would put the
   record on the disk twice - the same thing the shared write buffer does when its own ring is full.
   The wait is bounded by the write's timeout and costs about what acknowledging from the ordinary
   path would have cost, the disk being the same one, so what a group at that ceiling pays is
   latency rather than bytes. `direct_flush_waits` counts the records that wait.
4. Giving it one is best effort. A buffer and a segment are taken only if both can be had without
   waiting, and a group whose slot stays unbound writes through the shared active segment - which is
   what it already does while a flush is in flight - until a write of the group or the sync fiber
   can bind it. The flush itself must not wait for the next segment: everything that drains a group
   waits for it, so a flush that waited for disk space would hang a table flush, a split, a snapshot
   or shutdown on a shard whose segments have run out. How often this happens is what the no_buffer
   reason of the `direct_fallbacks` metric counts.
5. A read of a record that has been acknowledged but is not on the disk yet is served out of the
   buffer: the segment manager keeps the buffers of the hot groups by the id of the segment each one
   is bound to, and `read_record_bytes` looks there before it looks at the disk - for a segment that
   belongs to no compaction group, which is the only kind a buffer can be bound to.
6. The sequence number of the segment is assigned when the buffer is sealed, not when the segment
   was handed out, so that recovery's tie-break between records of equal timestamp still orders it
   against everything written while the buffer was filling.

The acknowledgement is therefore not durable: a crash loses what the buffers of the shard hold,
bounded by the sync period and by two segments per hot group. That is what the periodic commitlog
sync mode promises, so the path is on exactly when `commitlog_sync` is `periodic`, and its deadline
is `commitlog_sync_period_in_ms`. In `batch` mode every write takes the ordinary path above, whose
acknowledgement waits for the disk.

Which groups get buffers is measured rather than declared. Every write of a group is counted against
the current sync period; a group that wrote at least half a segment over a period is given buffers,
and one that writes less than that for two periods in a row gives them back. Below that rate the
partly filled segment such a group leaves at the end of every period occupies more of the segment
pool than the second write it saves is worth. How many groups of a shard may hold buffers at once
follows from `logstor_direct_write_memory_in_mb`, the memory a shard may hold in these buffers,
divided by the two segments' worth a hot group takes - which is what bounds both the memory and the
loss window. A budget with no room for a single group, `0` included, turns the path off, as does
`batch` mode. Both of those are settled when the shard starts;
`logstor_direct_writes_enabled` is the switch an operator has on a running one. Turning it off
refuses the next write straight away and leaves the sync fiber to write out what the groups are
holding and take their buffers back, which it does within one period; turning it back on lets the
controller hand them out again. The fiber therefore runs whenever the shard is built for the path,
whether or not the switch is on - it is what has to act on the switch either way.

A write that the disk refuses is the one case where the path keeps memory rather than giving it
back: the buffer holds the only copy of records that were acknowledged, and its segment is left
allocated because the index points into it, so both are kept and reads go on being served out of
memory. That is the right trade once and the wrong one forever, so a group whose flushes fail three
times in a row is put back on the ordinary path - which reports the failure to the caller instead of
taking the record into memory and calling it acknowledged - and once the kept buffers hold the whole
`logstor_direct_write_memory_in_mb` budget the path goes off for the rest of the life of the shard.

Every drain writes these buffers out rather than dropping them, discarding a group's segments
included - which a truncate does after clearing the index, so what comes out is a segment of records
nothing points at, which the discard then frees. Dropping them instead would be cheaper and is
wrong: a write taken between the clear and the discard is acknowledged and indexed, and freeing the
segment it went into is fatal. Written out, it is reported the way a record the separator takes late
is, and the truncate fails rather than the shard.

This path is a prototype, and one of the things it does not do yet makes it unsafe for real data.
Read [Direct Writes: State of the Implementation](#direct-writes-state-of-the-implementation) before
relying on it.

**Compaction:**
1. The amount of live data is tracked for each segment in its (in-memory) segment_descriptor. The segment descriptors are stored in a histogram by live data. This histogram is called a `segment_set` and each compaction group owns one.
2. A segment set from a single compaction group is submitted for compaction.
3. Compaction picks segments for compaction from the segment set. It chooses segments with the lowest utilization such that compacting them results in net gain of free segments.
4. It reads the segments, finding all live records, and writing them into a write buffer. When the buffer is full it is flushed into a new segment, and for each recording updating the index location to the new location.
5. After all live records are rewritten the old segments are freed.

### Direct Writes: State of the Implementation

The direct write path described above is a **prototype**. It is complete enough to be correct in
normal operation and to be measured, and it is not complete enough to be relied on for durability.
This section says exactly where the line is, so that nobody has to infer it from the code.

**What is there.** The write path itself (`segment_manager::try_write_direct`, the branch at the top
of `logstor::write`), the per-group buffers and the segments bound to them (`direct_write_buffer` in
`compaction.hh`), the rotation and the write-out, the read-through for records that have been
acknowledged but are not on the disk yet, the seal-time sequence number, the measured-rate
controller and its sync fiber, and the metrics. Every drain a tablet operation goes through - a
table flush, a split, a snapshot, a truncate, the group being removed, the shard stopping - writes
the buffers out rather than discarding them, and `logstor_group::empty()` accounts for them, so a
split does not acknowledge an empty group that is still holding records.

**What is missing, and why it matters.**

- **Superseded segments are not pinned.** When a direct write overwrites a key, the index frees the
  old record's location straight away, while the new record is still only in memory. A crash in that
  window loses both copies: the new one was never written, and the old one's space may already have
  been reclaimed and reused. That is worse than the loss window the periodic mode promises - it is
  not "the last few seconds of writes are gone" but "this key went backwards past where it was".
  The fix is the pattern the separator already uses: hold a reference to the segment the old record
  is in, deduplicated per buffer, until the new record is durable - see
  `separator_buffer::held_segments`.
  **This is the one gap that makes the path unsafe to enable on real data.**
- **Same-timestamp writes can be ordered wrongly.** A write already queued on the ordinary path and
  a later direct write of the same key with the same timestamp can reach the index in the opposite
  order, and the index's `cmp <= 0` tie-break then keeps the older value. The window is
  sub-millisecond and only opens when a group is promoted or when a direct write falls back, and it
  is the same class of race as the fresh-sequence rewrites the separator and compaction already do -
  but it is a race the ordinary path on its own does not have.
- **Tablet operations racing the window are only covered by the drain hooks.** The hooks are the
  right ones and they are exercised - the truncate path, whose window between the flush and the
  discard is the widest of them, gives its buffers up in `discard_segments()` - but there are no
  tests for an operation that starts while a buffer is filling, and none for a migration or a merge
  specifically.
- **Recovery is untested against this path.** No code change was needed - the segments a direct
  write produces are ordinary full segments, and an unwritten pre-bound segment loses on sequence
  number the same way a reused free slot does - but "no change needed" is an argument, not a test.
  What is worth writing: crash with unflushed buffers and check the loss is bounded to the window
  and that older values are intact; crash after a same-timestamp overwrite and check the ordering.
- **It is on by default.** `commitlog_sync` defaults to `periodic`, so a logstor cluster gets this
  path with no configuration. That is deliberate while it is being measured, and it has to be
  revisited - most likely by making it explicit - before the gaps above are closed.

**What has not been measured.** The whole point of the path is the second write going away:
2.02-2.17 times a record's bytes on the ordinary path, against about one times plus the padding of
the seal here. Run `test/perf/perf_logstor --test write` with and without `--direct-writes` and diff
`write_bytes_per_op`; see [logstor_perf.md](logstor_perf.md). Until that number exists, the path is
a hypothesis with an implementation attached.

**Next steps**, in the order they should be taken:

1. Measure, with `perf_logstor`. Everything else is only worth doing if the number is what the
   design predicts.
2. Pin superseded segments. This is what turns the path from a prototype into something that can be
   enabled.
3. Recovery tests, and tests for tablet operations that start mid-window.
4. Decide how the path is turned on. Riding `commitlog_sync` costs no new option and matches the
   durability promise exactly, but it also means a cluster gets it without asking.
5. Then the deferred items from the write-path design: what the `batch` mode can do about its own
   write amplification (lazy separation by compaction), and the `is_record_alive` check in the
   separator, which is a cheap independent win for every group that stays on the ordinary path.

### Space Accounting

Three different quantities describe the space logstor uses, and they are deliberately not the same
number:

- **Live record bytes** - the bytes of the records the index points at, tracked by the primary index
  as records are added and freed, so it is per table. `segment_manager` tracks the same sum for the
  whole shard. This is the live data, and it excludes the space held by dead records.
- **Segment occupancy** - the segments a compaction group owns, times the segment size. A segment
  belongs to a group once the separator or compaction has written it, and it is only given back when
  compaction reclaims it, so occupancy also covers the dead records in those segments. This is what a
  table reports as its disk space, next to its sstables, in `table::live_disk_space_used()`.
- **File footprint** - the files logstor has allocated, times the file size. Files are allocated as
  the disk fills up, up to `logstor_disk_size_in_mb`, and are only retired during recovery, so the
  footprint is a high water mark and it covers the free segments, which belong to no table. This is
  the space logstor takes on disk, reported by `segment_manager::get_disk_usage()`, and it is what a
  node includes in the load it gossips (`database::disk_space_used()`).

The three are ordered: live record bytes of a table <= its segment occupancy, and the sum over all
tables <= the file footprint. The ratio of the first two is the space amplification of the table, and
the difference between the last two is the space compaction has already reclaimed for reuse.

The size a node reports for a **tablet**, on the other hand, is the live bytes of the segments the
tablet's compaction groups own (`compaction_group::live_data_size()`), not their occupancy. The
occupancy of a tablet is not a property of its data: compaction only reclaims once the shard is short
of free segments, and it reclaims wherever that is cheapest across all the groups of the shard, so
how much dead data a tablet is left holding is decided by the space pressure of the disk it sits on
and by the write history, and at steady state the occupied segments of a shard stay at the free-segment
target whatever the data is. Live bytes are the part that is intrinsic to the tablet, that a migration
carries to another node and that a split divides in two, which is what makes them the number to
equalize across the cluster and to compare against the target tablet size. See
`docs/dev/size-based-load-balancing.md`.

The **capacity** those tablet sizes are divided by is the segment pool: the segments the shard is
configured to hold times the segment size, summed over the shards
(`segment_manager::get_segment_pool_size()`). It is the pool rather than the free space of the file
system because logstor allocates the pool for itself, up front with `logstor_format_on_startup`, and
nothing else can write into it: the free space a node reports therefore does not fall as logstor
fills up, and what is left of it outside the pool is not room a logstor tablet can grow into. A node
that also has sstable backed tablet tables is credited with that free space as well, less the part of
the pool logstor has yet to allocate, which is free space but is spoken for. This is computed in
`storage_service::load_stats_for_tablet_based_tables()`.

The pool is counted whole rather than discounted by the space its dead records hold, so the
utilization of a node using logstor is the fraction of its pool that is live data. That understates
how full the pool is by the space amplification of the workload - a pool entirely full of segments
reports the mean utilization of those segments, not 1 - but the amplification is near enough equal
across nodes running the same workload that the comparison between them, which is what the balancer
acts on, survives it. What does not survive it is the absolute value, so a node running out of
segments cannot be recognized from its utilization; the free segment counts are the number that says
that.

The same distinction applies wherever a table is sized by the volume of its data rather than by the
space that data takes: `table::live_data_size()` is the sum over its groups, and it is what the repair
small-table optimization and the initial tablet count of a keyspace migrated from vnodes are derived
from. `table::live_disk_space_used()` remains the space on disk: that is what the metrics and the
nodetool and API disk space of a table report, and what orders the tables of a major compaction.

Metrics: `scylla_column_family_live_disk_space` and `scylla_column_family_logstor_segments` report
the occupancy of a table, `scylla_column_family_logstor_live_record_bytes` its live data,
`scylla_column_family_live_data_size` the data it holds as a tablet reports it - the live bytes of the
segments its groups own, next to the bytes of its sstables - `scylla_logstor_sm_live_record_bytes` the
live data of the shard and `scylla_logstor_sm_disk_usage` its file footprint.

### Segment Utilization

The occupancy of a table says how much space it takes, but not how much of that space compaction can
give back. That is what the distribution of its segments by **utilization** - the fraction of a
segment held by live records - answers. A segment joins a compaction group once it is sealed, and
from there its live bytes only shrink, as the records in it are overwritten or deleted. Compaction
reclaims from the least utilized segments, since those are the ones whose space it can free while
copying the least, so mass at the bottom of the distribution is space that is cheap to get back and
mass at the top is space that is genuinely in use.

Each `segment_set` keeps the distribution of the segments it holds in a histogram of equal buckets
over `[0, 1]`, together with their live bytes, and maintains both as segments are linked, freed from
and unlinked (`replica/logstor/segment_stats.hh`). Reading them is therefore O(1) and exact, which is
what lets them be exported as a metric: computing them by walking the segments would cost a scan per
read and would have to yield, and segments moving during those yields would make the result
approximate.

The statistics are aggregatable, and they are aggregated as they change rather than on read. A
`segment_set` sits at the bottom of a **rollup**: a chain of `segment_stats_node`s that runs from the
set, through the table the group belongs to, up to the shard, and a segment linked, unlinked or freed
from is accounted into every level of it. Reading what a table or a whole shard holds is therefore a
field read as well. Summing the groups on read instead would cost a walk that grows with the number of
tablets the shard carries, and the metrics of a table read these several times per scrape, from a
callback that cannot yield.

A level counts the groups below it, so the group count of a table is the number of its compaction
groups whether or not they are registered with the compaction manager or are being migrated away: the
segments of a group take the space they take either way. Note that the live bytes in the segments are
less than the live record bytes of the table, which also cover the records that have not been sealed
into a segment of a group yet, being still in the active segment or in a separator buffer.

Metrics report the two halves of the question separately. *How much* is per table:
`scylla_column_family_logstor_segment_live_bytes` over
`scylla_column_family_logstor_segment_occupied_bytes` is the mean utilization of the segments of a
table and their difference is what compaction can reclaim from it, and being plain summable gauges
they aggregate over shards and nodes and can be alerted on. *How cheaply* is the distribution itself,
`scylla_logstor_sm_segment_utilization`, kept per shard and aggregated over the shards so that a node
reports one distribution across all of its tables. That is the level the question belongs to: the
segment pool is shared by the whole shard and compaction reclaims wherever it is cheapest in it. The
per table distribution is left to the API below rather than exported, since a series per bucket per
table is cardinality that grows with the number of tables to answer a question that is not asked per
table.

The histogram is a snapshot of a distribution rather than a count of events, so its buckets fall as
well as rise. Prometheus has no type for that, so it is exported as an ordinary histogram and a
dashboard has to read it as it is: `histogram_quantile()` over the raw buckets and a heatmap of them
are meaningful, and `_sum` over `_count` is the mean utilization, but anything that puts a `rate()`
in front of the buckets - which is how a latency histogram is read - is not, as every reclaimed
segment looks to it like a counter reset.

The `/storage_service/logstor_info` API reports the same distribution for a table together with the
numbers derived from it - the space its segments take, the part of it that is live and the part that
is reclaimable - both for the node and per shard, which is where a skew between the shards shows.

### Scheduling Groups

Every stage of logstor other than the foreground read and write runs in a scheduling group of its
own, so that the CPU each one takes is accounted and controlled separately:

| Stage | Group | Shares |
|---|---|---|
| Write buffer flush to the active segment | `clog` (shared with the commitlog) | 1000 |
| Separator | `lsep` | 1000, fixed |
| Direct write sync | `lsep` (shared with the separator) | 1000, fixed |
| Compaction | `lcmp` | driven by `logstor_compaction_controller`, see [logstor_compaction.md](logstor_compaction.md) |
| Split compaction | `mant` (maintenance) | 200 |

The separator used to run in the memtable group, `mt`, whose shares belong to the memtable flush
controller. A logstor table has no memtables, so that controller sees no backlog and leaves the group
at its one-share floor - which starved the separator, since it is on the critical path of every
write: the write path blocks on the separator task queue, so what the separator does not get to do,
no write completes either. It has a group of its own for that reason, and the shares are fixed rather
than controlled because the work is not discretionary - unlike compaction, which trades write
amplification for space and therefore has something to control.

## Usage

### Enabling Logstor

To use logstor, enable the experimental feature in the configuration:

```yaml
experimental_features:
  - logstor
```

### Creating Tables

Tables using logstor must have no clustering columns, and created with the `storage_engine` property equals to 'logstor':

```cql
CREATE TABLE keyspace.user_profiles (
    user_id uuid PRIMARY KEY,
    name text,
    email text,
    metadata frozen<map<text, text>>
) WITH storage_engine = 'logstor';
```

### Basic Operations

**Insert/Update:**

```cql
INSERT INTO keyspace.table_name (pk, v) VALUES (1, 'value1');
INSERT INTO keyspace.table_name (pk, v) VALUES (2, 'value2');

-- Overwrite with new value
INSERT INTO keyspace.table_name (pk, v) VALUES (1, 'updated_value');
```

Currently, updates must write the full row. Updating individual columns is not yet supported. Each write replaces the entire partition.

**Select:**

```cql
SELECT * FROM keyspace.table_name WHERE pk = 1;
-- Returns: (1, 'updated_value')

SELECT pk, v FROM keyspace.table_name WHERE pk = 2;
-- Returns: (2, 'value2')

SELECT * FROM keyspace.table_name;
-- Returns: (1, 'updated_value'), (2, 'value2')
```

**Delete:**

```cql
DELETE FROM keyspace.table_name WHERE pk = 1;
```

## Measuring Performance

Three tests measure the cost of an operation, at three levels. `scylla perf-simple-query --logstor`
runs the whole read and write path of a single node against a logstor table, and it is the same test
that measures an sstable backed table, so the two can be compared by dropping the flag.
`test/perf/perf_logstor` drives a logstor directly and measures the steps of a read and of a write
one at a time. `test/perf/perf_logstor_record_format` measures the encoding and the decoding of a
record value alone, and what a record takes, for several row shapes in one run. Which one answers
which question, how to run either so that two runs can be compared, and how to read what they report
is in [logstor_perf.md](logstor_perf.md).

## On-Disk Format

### Files

Segments are stored in large pre-allocated files on disk. Each file holds a fixed number of segments determined by the `file_size / segment_size` ratio. File names follow the pattern:

```
ls_{shard_id}-{file_id}-Data.db
```

Files are pre-formatted (zero-filled) before use.

A file is opened where it is formatted and held open for the life of the shard, and the read and the
write paths share the one handle, which is why it is opened read-write even when a read is what asked
for it. Nothing in normal operation opens a file: allocating a segment, discarding one and reading a
record all use the open handle, and the `scylla_logstor_sm_file_opens` counter therefore stops moving
once the shard has started. The cost of this is a file descriptor per file for the life of the
process - `logstor_disk_size_in_mb / logstor_file_size_in_mb` of them per shard - so a node with a
large logstor disk wants a correspondingly large `logstor_file_size_in_mb`.

### Segments

Each segment is a contiguous fixed-size region within a file (default 128KB). A segment is identified by a `log_segment_id` (a 32-bit integer index), which maps to a file and offset within that file.

A segment contains one or more **buffers** written sequentially. Each buffer is 4KB-block-aligned. A segment has one of two kinds:

- **mixed**: written by normal user writes; may contain records from multiple tables and compaction groups. Contains multiple buffers.
- **full**: written by compaction or the separator; contains records from a single table and token range. Contains exactly one buffer.

### Buffer Layout

A buffer within a segment has the following layout:

```
buffer_header
(segment_header)?       -- present only when kind == full
record_1
record_2
...
record_n
zero_padding            -- to align the entire buffer to block_alignment (4096 bytes)
```

buffer_header, segment_header, and records are aligned by `record_alignment` (8 bytes).

#### Buffer Header

A serialized form of `write_buffer::buffer_header`.

| Offset | Size | Field       | Description |
|--------|------|-------------|-------------|
| 0      | 4    | `magic`     | `0x4C475342` ("LGSB"). Used to detect valid buffers during recovery. |
| 4      | 1    | `kind`      | Segment kind: `0` = mixed, `1` = full. |
| 5      | 1    | `version`   | Version of the write buffer format. |
| 6      | 2    | `reserved`  | Reserved for future use. Currently written as zero and included in the CRC. |
| 8      | 8    | `segment_seq` | Monotonic segment sequence number used during recovery and segment ordering checks. |
| 16     | 4    | `data_size` | Size in bytes of all record data following the header(s). |
| 20     | 4    | `crc`       | CRC32 of all preceding buffer header fields. Used for validating the header. |

The buffer header is 24 bytes long, which keeps it aligned to `record_alignment` (8 bytes).

#### Segment Header (full segments only)

Immediately follows the buffer header when `kind == full`.

A serialized form of `write_buffer::segment_header`.

| Offset | Size | Field         | Description |
|--------|------|---------------|-------------|
| 24     | 16   | `table`       | UUID of the table this segment belongs to. |
| 40     | 8    | `first_token` | Minimum token of all records in the segment (raw token number). |
| 48     | 8    | `last_token`  | Maximum token of all records in the segment (raw token number). |

#### Records

Each record within the buffer is structured as:

```
record_header        (8 bytes)
log_record_header    (header_size bytes)
record value         (data_size bytes)
zero_padding         -- to align to record_alignment (8 bytes)
```

**Record Header** (`ondisk::record_header`):

| Offset | Size | Field         | Description |
|--------|------|---------------|-------------|
| 0      | 4    | `header_size` | Size in bytes of the serialized `log_record_header` that follows. |
| 4      | 4    | `data_size`   | Size in bytes of the record value that follows `log_record_header`. |

**Log Record Header** (`log_record_header`):

Serialized by hand (`ser::serializer<log_record_header>` in `replica/logstor/ondisk.hh`), 34
bytes plus the partition key:

| Offset | Size | Field       | Description |
|--------|------|-------------|-------------|
| 0      | 8    | `token`     | Raw token of the record's partition key. |
| 8      | 8    | `timestamp` | Timestamp of the record, used to resolve conflicts by keeping the record with the latest timestamp. It is also the base every timestamp in the record value is a delta from. |
| 16     | 16   | `table`     | UUID of the table this record belongs to. |
| 32     | 2    | `key_size`  | Size in bytes of the partition key that follows. |
| 34     | n    | `key`       | The `partition_key` representation, verbatim: the compound blob the key has in memory. |

The fixed-width fields come first so that a point read takes the record's timestamp at a
fixed offset and compares the record's key against the one it is looking for with a memcmp,
without deserializing the header. The offsets are `ondisk::log_record_header_*_offset`.

An IDL definition of the same fields would frame every one of them, serialize the token
through a `bytes` and the key through a vector of its exploded components, and cost several
allocations on each of the read and the write path, for 84 bytes with a 16-byte key against
the 50 this takes.

**Record Value**:

The `data_size` bytes following the log record header are the partition of the record,
encoded in the logstor record format. The layout is in `replica/logstor/record_format.hh`,
next to the code that reads and writes it.

A logstor table has no clustering columns, so a partition is at most one row - the one with
the empty clustering key - plus a partition tombstone, and has neither static columns nor
range tombstones. The encoding is built for that shape and for the point read that decodes
it: one flags byte where the IDL serializer of a `canonical_mutation` frames every field and
writes a variant index per cell, a varint delta from the record's own timestamp for every
timestamp - which the whole-partition write model makes almost always zero - the size of a
value taken from its type where the type is fixed-width, and a non-frozen collection stored
as its `collection_mutation` blob verbatim.

A write encodes the partition in one pass, into a buffer `row_value_encoder` owns and reuses
across the records of the shard, and copies the exact bytes out into the value the record
carries - the record has to own its value, because the separator replays a retained copy of it
after the write buffer has been reset.

A record stays self-contained: it carries the version of the schema it was written under and
a description of that schema's columns, so nothing outside the record is needed to decode
it, and compaction, the separator and segment streaming go on treating a value as bytes. A
read under the same schema version steps over the description without parsing it; a read
under another one translates the record's columns onto the schema's, which is cached per
pair of schema versions. Phase 2 of the format moves the description to a per-buffer
dictionary, where it amortizes to a few bytes per record.

What a record takes against the bytes of value it carries, from `test/perf/perf_logstor`,
before the format and after it:

| Row | Record | Before |
|---|---|---|
| 5 columns x 300 B | 1650 B (1.10x) | 2251 B (1.50x) |
| 5 columns x 20 B | 245 B (2.45x) | 851 B (8.51x) |
| 1 column x 300 B | 422 B (1.41x) | 671 B (2.24x) |
| 1 column x 20 B | 141 B (7.05x) | 391 B (19.55x) |

The narrower the row the larger the difference, because what a `canonical_mutation` spends on
the mapping of the schema and on the framing of a cell does not shrink with the values. A run
of the tool prints the second column of its own row shape as it goes, by encoding the same
partition as a `canonical_mutation` as well; that comparison substitutes only the value, so it
comes out a few dozen bytes below the numbers here, which are whole records of a build from
before the format. `test/perf/perf_logstor_record_format` prints both columns for whatever shapes
it is given, together with the split of a value into its head, the description of the schema it
carries and the row itself.

The `encode` and `decode` tests measure the two halves of the format against `freeze` and
`materialize`, which measure what a `canonical_mutation` value cost. In a release build, 5 x
300 B rows, against a build of the commit before the format:

| | before | after |
|---|---:|---:|
| `decode` / `materialize` | 9115 insns, 10 allocs | **4427**, 9 |
| `encode` / `freeze` | 5512 insns, 5 allocs | **2929**, 1 |
| `read-disk`, whole read from a segment | 15173, 28.2 | **10806**, 24.2 |
| `write`, whole | 20439, 34.8 | **14169**, 19.5 |
| `read-cached`, whole read from the cache | 4038, 10.0 | 3885, 10.0 |

The read is the win: decoding costs 647 instructions per column against materializing's 979
and 872 fixed against 2475, the record is no longer deserialized at all, and a whole read from
a segment costs 34% less at one column, 29% at five and 20% at thirty.

The write took two goes. Framing, sizing and appending a record fell from 2192-2637
instructions to 513-573 - the value reaches the write buffer already encoded, so the append is
a memcpy and both sizes are arithmetic - and the separator no longer serializes the value a
second time. But the first encoder gave that back and more, because it walked the partition
twice, once to size the buffer and once to fill it, and rebuilt the description of the schema
for every record on top of that: 1379 instructions per column against the freeze's 666, and a
whole write 21% cheaper than before the format at one column, unchanged at five and 31% dearer
at thirty. The encoder now walks the partition once, into a buffer it owns and grows to fit,
and copies the exact bytes of the record out of it, and it takes the description from the copy
it built for that schema version:

| `encode`, instructions per operation | 1 x 200 B | 5 x 300 B | 30 x 20 B | per column |
|---|---:|---:|---:|---:|
| `freeze`, before the format | 2376 | 5512 | 21700 | 666 |
| two walks, the description per record | 2848 | 8666 | 42838 | 1379 |
| one walk | 2075 | 6104 | 28371 | 907 |
| one walk, the description cached | **1074** | **2929** | **11638** | **364** |

Removing the second walk takes less than half of the cost off, because the walk that only
measured was the cheaper of the two; caching the description takes more off than that, because
building one interns the type names of the columns, looks every column's type up in that table
by comparing type names, and has to size the whole before writing it. What is left is half of
what the `canonical_mutation` freeze cost at every shape, and a whole write 34% cheaper than
before the format at 1 x 200 B, 31% at 5 x 300 B and 29% at 30 x 20 B, on top of the 13 to 19
allocations and the 27% to 76% of the record's bytes the format already gained.

A read the cache serves is untouched by any of it, and measures identical to the instruction on
both builds. At the level of a node, `scylla perf-simple-query --logstor --write` on a
one-column table went from 48648 to 46315 instructions, 88.0 to 73.8 allocations, 27380 to
21799 cycles and 16.4k to 18.4k operations per second, and the bytes it writes to disk from
2014 to 378 per operation. That node-level measurement predates the single-pass encoder, which
takes another 1774 instructions off a one-column write.

**Record Location** (`log_location`):

The `log_location` stored in the index for each record points to the start of the `record_header`:
- `offset`: byte offset from the start of the segment to the `record_header`.
- `size`: total size including `record_header` + `log_record_header` + the record value
