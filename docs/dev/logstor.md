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

#### Segment Manager

The `segment_manager` handles the allocation and management of fixed-size segments (default 128KB). Segments are grouped into large files (default 32MB). Key responsibilities include:

- **Segment allocation**: Provides segments for writing new data
- **Space reclamation**: Tracks free space in each segment
- **Compaction**: Copies live data from sparse segments to reclaim space
- **Recovery**: Scans segments on startup to rebuild the index
- **Separator**: Writes to all _compaction groups_ (tablets and even tables) go to a single active segment. The separator splits these mixed segments, which have records from different compaction groups, into segments that each has a single compaction group. This separation is useful when migrating tablets.

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

**Read Path:**
1. Application requests data for a partition key
2. Cache lookup, which serves the read when the partition is cached
3. Index lookup returns record location
4. Segment manager reads the bytes of the record from disk
5. The key of the record is compared against the key of the read, and the value of the record
   is decoded into the mutation the read returns. Neither the header of the record nor its
   value is copied out of the buffer that was read: the read takes the two fields it needs
   from the header at their offsets and decodes the value in place

**Separator:**
1. When a record is written to the active segment, it is also written to its compaction group's separator buffer. The separator buffer holds a reference to the original segment.
2. The separator buffer is flushed when it's full, or requested to flush for other reason. It is written into a new segment in the compaction group, and it updates the location of the records from the original mixed segments to the new segments in the compaction group.
3. After the separator buffer is flushed and all records from the original segment are moved, it releases the reference of the segment. When there are no more reference to the segment it is freed.

**Compaction:**
1. The amount of live data is tracked for each segment in its (in-memory) segment_descriptor. The segment descriptors are stored in a histogram by live data. This histogram is called a `segment_set` and each compaction group owns one.
2. A segment set from a single compaction group is submitted for compaction.
3. Compaction picks segments for compaction from the segment set. It chooses segments with the lowest utilization such that compacting them results in net gain of free segments.
4. It reads the segments, finding all live records, and writing them into a write buffer. When the buffer is full it is flushed into a new segment, and for each recording updating the index location to the new location.
5. After all live records are rewritten the old segments are freed.

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

Two tests measure the cost of an operation, at two levels. `scylla perf-simple-query --logstor` runs
the whole read and write path of a single node against a logstor table, and it is the same test that
measures an sstable backed table, so the two can be compared by dropping the flag.
`test/perf/perf_logstor` drives a logstor directly and measures the steps of a read and of a write
one at a time. Which one answers which question, how to run either so that two runs can be compared,
and how to read what they report is in [logstor_perf.md](logstor_perf.md).

## On-Disk Format

### Files

Segments are stored in large pre-allocated files on disk. Each file holds a fixed number of segments determined by the `file_size / segment_size` ratio. File names follow the pattern:

```
ls_{shard_id}-{file_id}-Data.db
```

Files are pre-formatted (zero-filled) before use.

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
before the format.

The `encode` and `decode` tests measure the two halves of the format against `freeze` and
`materialize`, which measure what a `canonical_mutation` value cost. In a release build, 5 x
300 B rows, against a build of the commit before the format:

| | before | after |
|---|---:|---:|
| `decode` / `materialize` | 9115 insns, 10 allocs | **4427**, 9 |
| `encode` / `freeze` | 5511 insns, 5 allocs | **8666**, 1 |
| `read-disk`, whole read from a segment | 15173, 28.2 | **10806**, 24.2 |
| `write`, whole | 21020, 34.8 | 20516, **19.5** |
| `read-cached`, whole read from the cache | 4038, 10.0 | 3885, 10.0 |

The read is the win: decoding costs 647 instructions per column against materializing's 979
and 872 fixed against 2475, the record is no longer deserialized at all, and a whole read from
a segment costs 34% less at one column, 29% at five and 20% at thirty.

The write is a trade, and its sign depends on the row. Framing, sizing and appending a record
fell from 2192-2637 instructions to 513-573 - the value reaches the write buffer already
encoded, so the append is a memcpy and both sizes are arithmetic - and the separator no longer
serializes the value a second time. Against that, `encode_value()` walks the partition twice,
once to size the buffer and once to fill it, at about 690 instructions per column per walk,
where the IDL serializer it replaced walked it once. The two cancel at about six columns: a
whole write is 21% cheaper at 1 x 200 B, unchanged at 5 x 300 B and 31% dearer at 30 x 20 B.
What it gains at every shape is 13 to 19 allocations and 27% to 76% of the record's bytes.
Making the encode single-pass would put the write ahead at every shape.

A read the cache serves is untouched by any of it, and measures identical to the instruction on
both builds. At the level of a node, `scylla perf-simple-query --logstor --write` on a
one-column table went from 48648 to 46315 instructions, 88.0 to 73.8 allocations, 27380 to
21799 cycles and 16.4k to 18.4k operations per second, and the bytes it writes to disk from
2014 to 378 per operation.

**Record Location** (`log_location`):

The `log_location` stored in the index for each record points to the start of the `record_header`:
- `offset`: byte offset from the start of the segment to the `record_header`.
- `size`: total size including `record_header` + `log_record_header` + the record value
