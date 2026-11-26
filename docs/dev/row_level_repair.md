# Row level repair

## How the partition level repair works

- The repair master decides which ranges to work on.
- The repair master splits the ranges to sub ranges which contains around 100
partitions.
- The repair master computes the checksum of the 100 partitions and asks the
related peers to compute the checksum of the 100 partitions.
- If the checksum matches, the data in this sub range is synced.
- If the checksum mismatches, repair master fetches the data from all the peers
and sends back the merged data to peers.

## Major problems with partition level repair

- A mismatch of a single row in any of the 100 partitions causes 100
partitions to be transferred. A single partition can be very large. Not to
mention the size of 100 partitions.

- Checksum (find the mismatch) and streaming (fix the mismatch) will read the
same data twice

## Row level repair

Row level checksum and synchronization: detect row level mismatch and transfer
only the mismatch

## How the row level repair works

- To solve the problem of reading data twice

Read the data only once for both checksum and synchronization between nodes.

We work on a small range which contains only a few mega bytes of rows,
We read all the rows within the small range into memory. Find the
mismatch and send the mismatch rows between peers.

We need to find a sync boundary among the nodes which contains only N bytes of
rows.

- To solve the problem of sending unnecessary data.

We need to find the mismatched rows between nodes and only send the delta.
The problem is called set reconciliation problem which is a common problem in
distributed systems.

For example:

- Node1 has set1 = {row1, row2, row3}

- Node2 has set2 = {      row2, row3}

- Node3 has set3 = {row1, row2, row4}

To repair:

- Node1 fetches nothing from Node2 (set2 - set1), fetches row4 (set3 - set1) from Node3.

- Node1 sends row1 and row4 (set1 + set2 + set3 - set2) to Node2.

- Node1 sends row3 (set1 + set2 + set3 - set3) to Node3.

## How to implement repair with set reconciliation

- Step A: Negotiate sync boundary

class repair_sync_boundary {
    dht::decorated_key pk;
    position_in_partition position
}

Reads rows from disk into row buffers until the size is larger than N
bytes. Return the repair_sync_boundary of the last mutation_fragment we
read from disk. The smallest repair_sync_boundary of all nodes is
set as the current_sync_boundary.

- Step B: Get missing rows from peer nodes so that repair master contains all the rows

Request combined hashes from all nodes between last_sync_boundary and
current_sync_boundary. If the combined hashes from all nodes are identical,
data is synced, goto Step A. If not, request the full hashes from peers.

At this point, the repair master knows exactly what rows are missing. Request the
missing rows from peer nodes.

Now, local node contains all the rows.

- Step C: Send missing rows to the peer nodes

Since local node also knows what peer nodes own, it sends the missing rows to
the peer nodes.

## How the RPC API looks like

Start:
- repair_range_start()

Step A:
- get_sync_boundary()

Step B:
- get_combined_row_hashes()
- get_full_row_hashes()
- get_row_diff()

Step C:
- put_row_diff()

Finish:
- repair_range_stop()

## Performance evaluation

We created a cluster of 3 Scylla nodes on AWS using i3.xlarge instance. We
created a keyspace with a replication factor of 3 and inserted 1 billion
rows to each of the 3 nodes. Each node has 241 GiB of data.
We tested 3 cases below.

### 1) 0% synced: one of the node has zero data. The other two nodes have 1 billion identical rows.

Time to repair:

*    old = 87 min
*    new = 70 min (rebuild took 50 minutes)
*    improvement = 19.54%

### 2) 100% synced: all of the 3 nodes have 1 billion identical rows.

Time to repair:

*    old = 43 min
*    new = 24 min
*    improvement = 44.18%

### 3) 99.9% synced: each node has 1 billion identical rows and 1 billion * 0.1% distinct rows.

Time to repair:

*    old: 211 min
*    new: 44 min
*    improvement: 79.15%

Bytes sent on wire for repair:

*    old: tx= 162 GiB,  rx = 90 GiB
*    new: tx= 1.15 GiB, tx = 0.57 GiB
*    improvement: tx = 99.29%, rx = 99.36%

It is worth noting that row level repair sends and receives exactly the
number of rows needed in theory.

In this test case, repair master needs to receives 2 million rows and
sends 4 million rows. Here are the details: Each node has 1 billion *
0.1% distinct rows, that is 1 million rows. So repair master receives 1
million rows from repair follower 1 and 1 million rows from repair follower 2.
Repair master sends 1 million rows from repair master and 1 million rows
received from repair follower 1 to repair follower 2. Repair master sends
sends 1 million rows from repair master and 1 million rows received from
repair follower 2 to repair follower 1.

In the result, we saw the rows on wire were as expected.

tx_row_nr  = 1000505 + 999619 + 1001257 + 998619 (4 shards, the numbers are for each shard) = 4'000'000
rx_row_nr  =  500233 + 500235 +  499559 + 499973 (4 shards, the numbers are for each shard) = 2'000'000

## RPC stream usage in repair

Some of the RPC verbs used by row level repair can transmit bulk of data,
namely REPAIR_GET_FULL_ROW_HASHES, REPAIR_GET_ROW_DIFF and REPAIR_PUT_ROW_DIFF.
To have better repair bandwidth with high latency link, we want to increase the
row buff size.  It is more efficient to send large amount of data with RPC
stream interface instead of the RPC verb interface.  We want to switch to RPC
stream for such RPC verbs. Three functions, get_full_row_hashes(),
get_row_diff() and put_row_diff(), are converted to use the new RPC stream
interface.


###  1) get_full_row_hashes()
A new REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM rpc veb is introduced.

#### The repair master sends: repair_stream_cmd.

The repair_stream_cmd can be:
- repair_stream_cmd::get_full_row_hashes
Asks the repair follower to send all the hashes in working row buffer.

####  The repair follower replies: repair_hash_with_cmd.

```
struct repair_hash_with_cmd {
    repair_stream_cmd cmd;
    repair_hash hash;
};
```

The repair_stream_cmd in repair_hash_with_cmd can be:

- repair_stream_cmd::hash_data
One of the hashes in the working row buffer. The hash is stored in repair_hash_with_cmd.hash.

- repair_stream_cmd::end_of_current_hash_set
Notifies repair master that  repair follower has send all the hashes in the working row buffer

- repair_stream_cmd::error
Notifies an error has happened on the follower


### 2) get_row_diff()

A new REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM rpc veb is introduced.

```
struct repair_hash_with_cmd {
    repair_stream_cmd cmd;
    repair_hash hash;
};

struct repair_row_on_wire_with_cmd {
    repair_stream_cmd cmd;
    repair_row_on_wire row;
};
```

#### The repair master sends: repair_hash_with_cmd

The repair_stream_cmd in repair_hash_with_cmd can be:

- repair_stream_cmd::needs_all_rows
Asks the repair follower to send all the rows in the working row buffer.

- repair_stream_cmd::hash_data
Contains the hash for the row in the working row buffer that repair master
needs. The hash is stored in repair_hash_with_cmd.hash.

- repair_stream_cmd::end_of_current_hash_set

Notifies repair follower that repair master has sent all the rows it needs.

#### The repair follower replies: repair_row_on_wire_with_cmd

The repair_stream_cmd in repair_row_on_wire_with_cmd can be:

- repair_stream_cmd::row_data
This is one of the row repair master requested. The row data is stored in
repair_row_on_wire_with_cmd.row.

- repair_stream_cmd::end_of_current_rows
Notifies repair follower has sent all the rows repair master requested.

- repair_stream_cmd::error
Notifies an error has happened on the follower.


### 3) put_row_diff()

A new REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM  rpc veb is introduced.

```
struct repair_row_on_wire_with_cmd {
    repair_stream_cmd cmd;
    repair_row_on_wire row;
};
```

#### The repair master sends: repair_row_on_wire_with_cmd

The repair_stream_cmd in repair_row_on_wire_with_cmd can be:

- repair_stream_cmd::row_data
Contains one of the rows that repair master wants to send to repair follower.
The row data is stored in repair_row_on_wire_with_cmd.row.

- repair_stream_cmd::end_of_current_rows
Notifies repair follower that repair master has sent all the rows it wants to send.

#### The repair follower replies: repair_stream_cmd

- repair_stream_cmd::put_rows_done
Notifies repair master that repair follower has received and applied all the
rows repair master sent.

- repair_stream_cmd::error
Notifies an error has happened on the follower.

## Repair of logstor tables

Logstor tables (see `docs/dev/logstor.md`) are repaired with the same wire
protocol and the same row level repair algorithm, with a few adjustments to the
algorithm.

### Why logstor is different

- A logstor record holds a whole partition. Logstor tables have a partition key
only, so a partition is one row at the empty clustering key, or a partition
tombstone, or both.

- A logstor write *replaces* the partition. The index keeps the record with the
highest record timestamp, and two records of the same key are never merged.

- Logstor does not support partial writes (SCYLLADB-2723). Every write sets all
the columns of the row, or deletes the partition.

The LSM storage engine merges all the mutations of a partition at cell level. For
two versions of a logstor partition with different timestamps, this cell level
merge and the replacement rule of logstor give the same result, which is the
newer version. The two rules give different results only for versions that have
the same timestamp (see "Known limitations" below).

Repair still has to reconcile and write whole partitions, because a partition can
produce two repair rows: the partition tombstone and the row. Normal row level
repair sends only the repair rows that differ, so a replica can receive only one
of them. The replica then stores a record that is not the same as the record on
the master, or its index ignores the write because the record timestamp is lower
than the timestamp of the stored record. Reads return the same data, but the
records of the replicas stay different, so each later repair finds the partition
different again. This is the reason for all the adjustments below.

### Sync boundaries are partition aligned

Whole-partition reconciliation only works if all fragments of a partition land in
the same sync window. A logstor partition can produce two repair rows: a
`partition_start` carrying a partition tombstone, plus the row. A sync boundary
between them would make the master converge each part of the partition alone, so
the replicas can store a record without the tombstone or without the row, which
is not the cell level merge of the versions. Logstor sessions therefore align the
sync windows to partitions.

### Whole-partition convergence on the master

After Step B the master's working row buffer holds every distinct version of
every row from all replicas. For logstor tables the master then converges that
buffer (`converge_working_row_buf_for_logstor()`):

1. group the buffered rows by partition key (rows of a partition are contiguous)
2. for every partition that repair touched, that is, one that has a row pulled
from a follower and more than one version to merge, merge all its versions into a
single `mutation`
3. expand the merged mutation back into repair rows, mark them dirty so the
master flushes and sends them, and recompute the combined hash
4. if the expanded rows have the same hashes as the master's own rows, keep the
master's own rows, which are not dirty, and drop the pulled rows, so that the
master does not write a partition that it already stores

This is the same cell level reconciliation the LSM merge does, with the same row
marker, cell and tombstone rules. Repair does not rely on the replacement rule of
the logstor index, which compares only the record timestamps. Repair computes the
reconciled partition first and writes it as a single record.

The rest of the pipeline is unchanged. The master flush writes the converged
partition to the local logstor, replacing the stale version, and Step C computes
`master - peer` over the converged rows.

Step C has one adjustment. Instead of selecting the rows whose hash is in the set
diff, `put_row_diff` uses
`copy_partitions_from_working_row_buf_within_set_diff()`, which sends every row
of a partition that has at least one row in the set diff. A follower must receive
the complete converged partition, because a partial partition replaces its record
with a record that is not the converged partition, or the index of the follower
ignores the partial partition. The `needs_all_rows` case sends the whole row buffer
as before, so it is whole-partition already.

### Applying rows

`make_repair_writer()` picks the writer by storage backend. Normal tables write
streaming SSTables, which logstor does not have. Logstor tables get
`logstor_repair_writer_impl`, which rebuilds logical mutations and applies them
directly. The `repair_writer` interface and the repair protocol are unchanged,
only the sink differs. The master's local flush uses the same writer.

The sink consumes the fragment stream and, per partition:

1. rebuilds one `mutation`
2. computes the destination shards with `sharder.shard_for_writes(token)`
3. sends the frozen mutation to every destination shard
4. on the destination shard,  calls `table::apply()`, which routes to
`logstor::write()`

### Known limitations

- Equal timestamps (SCYLLADB-6194): repair merges two versions that have the same
timestamp cell by cell, but the index replaces a record with a new record of an
equal timestamp. So a retried or replayed write can undo the repair.

- Large merged partitions (SCYLLADB-6644): when the merged partition is larger
than the maximum record size, the write fails. Repair of the tablet then fails on
every run, and read repair of the partition fails.

- Write speed (SCYLLADB-6683): the repair writer applies the partitions one at a
time, so repair of many small partitions is slow.

- Incremental repair (SCYLLADB-5765): logstor tables do not support incremental
repair. A logstor table always gets a full repair, also when an incremental
repair is requested.
