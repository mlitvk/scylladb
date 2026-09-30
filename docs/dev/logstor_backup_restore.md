# Logstor Backup and Restore

## Status

Design document. The overall approach — reuse the tablet-aware cluster
backup/restore pipeline, treat a logstor **full segment** as the unit of
backup, and splice logstor's existing snapshot/stream primitives into the
object-storage IO path — is settled.

- **Object packaging** is **resolved**: multiple segments are packed into
  larger, tablet-scoped **pack objects** (Option 2) by default and restored
  with whole-pack GETs; one object per segment (Option 1) survives only as the
  degenerate case `pack_size = segment_size` for bring-up and tests. The
  manifest carries `{object_key, offset, length}` per segment so both remain
  supported and the choice can be made per-backup. See "Backup unit and object
  packaging" for the request-count analysis behind this. Dedup is
  **manifest-based** (re-reference unchanged segments from the previous
  backup's manifest), not `object_exists`-probe-based — see "Segment identity
  and deduplication".
- **On-disk snapshot** for logstor is intentionally left **open** and
  documented here with tradeoffs: back up directly from the in-memory pinned
  snapshot (Option A, with the A1–A5 improvements), persist an on-disk snapshot
  first (Option B, in a `B-share` or `B-copy` variant), or compact straight to
  object storage (Option C). The manifest/metadata format is identical for all
  of them, so the choice is isolated to the enumerate-and-upload worker.
- **Replica selection** is **resolved in two phases**. Phase 1 backs up **one
  replica per tablet** (Option R3) — a documented property difference against
  the SSTable path, whose repaired/unrepaired dedup captures every write
  present on *any* replica. A later phase (stage 6) restores that union
  guarantee with a **repair seq-watermark** (Option R2): non-master replicas
  contribute only segments sealed since the last completed tablet repair. R2
  assumes tablet repair support for logstor lands first (full repair
  suffices; incremental repair is not required). See "Replica selection and
  the repaired/unrepaired question".

(The three "Option" groups are independent: the packaging options are numbered
1/2, the on-disk-snapshot options are lettered A/B/C, and the replica-selection
options are R1–R4.)

Separately, this document records two **live defects** that exist today,
independent of backup: `auto_snapshot` on `DROP`/`TRUNCATE` silently protects
nothing for logstor tables, and **tablet repair on a logstor table is a silent
no-op** — it reads no logstor data, writes received rows into sstables the
logstor read path never consults, reports success, and advances the tablet's
repair metadata. Neither is a backup feature and neither is gated on anything
in this plan — see the two "Existing defect" sections, "Immediate fix"
(stage 0a) and stage 0b.

Note that improvements **A1** (pin-aware compaction candidate selection) and
**A3** (the rescue-pin snapshot cursor) are **not backup
features**: they fix a pin-cost problem logstor has today in tablet streaming,
are required by every on-disk-snapshot option, and ship first as **stage 0**
with no backup code involved. See "On-disk snapshot" and "Stage 0 implementation
plan" below.

## Problem

Backup-and-restore in ScyllaDB is implemented for LSM/SSTable-based tables. It
is built entirely on top of the SSTable file model:

- **Snapshots** hard-link immutable SSTable component files into a
  `snapshots/<name>/` subdirectory.
- **Backup** lists that directory, groups files by SSTable generation, and
  uploads each component to object storage.
- **Restore** downloads range-matching SSTables and attaches them to a table.

Logstor is an alternative storage engine (see `docs/dev/logstor.md`,
implementation in `replica/logstor/`) that does **not** use SSTables. Its data
lives in large, fixed-size, mutable-in-place, shard-scoped files
(`ls_{shard}-{file_id}-Data.db`) in one flat `logstor/` directory shared by all
tables. There is currently **no backup/restore support for logstor tables**;
`db/snapshot/` has zero logstor references.

The goal is to add cluster/tablet-aware backup-and-restore for logstor tables
reaching parity with the SSTable path.

### Existing defect — `auto_snapshot` is silently a no-op for logstor

Independently of backup/restore, the absence of logstor snapshot support has a
consequence that is **live today** and is a behavioral regression against
SSTable tables: `DROP TABLE` and `TRUNCATE` on a logstor table appear to take a
protective snapshot and in fact protect nothing, without any warning.

`truncate_table_on_all_shards` (`replica/database.cc:3110`) gates the automatic
snapshot on two conditions:

```cpp
if (!sharded_db.local().get_config().auto_snapshot()) {
    with_snapshot = false;
}
if (with_snapshot && !table_shards->get_storage_options().is_local_type()) {
    dblog.warn("Not snapshotting dropped/truncated table {}.{} despite auto_snapshot=true"
               " - table is not using local disk", ...);
    with_snapshot = false;
}
```

`auto_snapshot` defaults to **true** (`db/config.cc:1069`), and a logstor table
**is** local-type storage — `uses_logstor()` is a per-table property, not a
`storage_options` variant — so neither guard fires. `with_snapshot` stays true,
the table is flushed, and `snapshot_table_on_all_shards` is called
(`replica/database.cc:3197-3209`). There, for a logstor table:

- `take_snapshot` hard-links **zero** files, because the table has no sstables;
- `tablets` is derived by iterating the **sstable** sets (`replica/table.cc:4209-4225`),
  so it comes out empty, and the `ssc` callback is therefore never invoked
  (`:4236`) — no `system_distributed` rows are written either;
- `write_manifest` emits a `manifest.json` with an empty sstable list and an
  empty tablet list, and `schema.cql` is written.

The result is a snapshot directory containing `schema.cql` and a
content-free `manifest.json`. Truncate then discards the segments. `DROP TABLE`
takes the same path — `cleanup_drop_table_on_all_shards` calls
`truncate_table_on_all_shards` with a `pre-drop-<timestamp>` tag
(`replica/database.cc:1416-1419`) — so it behaves identically.

Two things are wrong here, of different severity:

1. **The data is unrecoverable**, where an SSTable table would have been
   recoverable from the snapshot directory. That is inherent to logstor having
   no snapshot mechanism, and is only fixed by implementing one.
2. **Nothing says so.** The S3 case immediately above warns; the logstor case is
   silent, and it leaves behind a snapshot directory whose existence actively
   suggests the data was preserved. `nodetool listsnapshots` will show it. This
   is the part that turns a known limitation into a trap, and it is a few lines
   to fix.

Note that `nodetool snapshot` on a logstor table has the same shape — it
succeeds and produces an empty snapshot — but the drop/truncate path is the
dangerous instance, because there the operator did not ask for a snapshot and is
relying on a default to protect them.

The fix is staged: make the no-op **explicit** first (see "Immediate fix"
below), and only later make it **real**, which requires the on-disk snapshot
work of Option B.

### Existing defect — tablet repair is silently a no-op for logstor

A second live defect, unrelated to snapshots: repairing a logstor table
succeeds while repairing nothing. Nothing in `repair/` or the topology
coordinator branches on `uses_logstor()` (zero references); the machinery runs,
but both of its data paths miss logstor entirely:

- **The read side reads no logstor data.** Every repair read strategy funnels
  into `table::make_streaming_reader` — the `local` strategy directly
  (`repair/row_level.cc:293-313`), the `multishard_*` strategies via
  `make_multishard_streaming_reader`, whose per-shard readers are created by
  `streaming_reader_lifecycle_policy::create_reader`
  (`replica/database.hh:2385`). All `make_streaming_reader` overloads read
  memtables + sstables only (`replica/table.cc:316-372`); there is no logstor
  branch. Logstor data is reachable only through `table::make_mutation_reader`,
  which returns the logstor reader exclusively (`replica/table.cc:239-241`).
  Incremental repair is further off still: it builds its input from an sstable
  snapshot (`take_storage_snapshot`, `repair/row_level.cc:1235`), which is
  empty for a logstor table.
- **The write side writes into a black hole.** Rows received from peers go
  through `repair_writer` → `streaming::make_streaming_consumer`
  (`repair/row_level.cc:576`), which unconditionally writes an sstable and
  attaches it to the table's sstable set (`streaming/consumer.cc:59-98`). A
  logstor table's read path never consults sstables, so the repaired rows are
  invisible.

The failure is self-certifying: all replicas read "empty", compare equal, the
repair reports success, and the tablet's `repair_time` (and
`sstables_repaired_at`) advance in the tablet map
(`service/topology_coordinator.cc:2530`). Every consumer of that metadata —
operators watching repair health, repair-based tombstone GC if it ever applies
to logstor, and any backup policy that gates on repair freshness (see "Replica
selection and the repaired/unrepaired question") — is told the tablet is
repaired when nothing was compared.

As with `auto_snapshot`, the fix is staged: make the no-op **explicit** first —
fail a repair that names a logstor table, warn and skip logstor tables in
keyspace-wide repairs, and do not advance repair metadata for skipped tables
(stage 0b) — and make it **real** later by implementing logstor tablet repair
(reads via the logstor mutation source, which exists; writes applied through
the logstor write path instead of an sstable dump). Implementing repair is out
of scope for this document, but this plan **assumes it lands**: stage 6
(Option R2) depends on it, and full tablet repair is sufficient — the
incremental-repair machinery (per-sstable `repaired_at` generations) is not
required.

## Background

### How SSTable backup/restore works today

There are two generations of backup/restore in the tree:

1. **Per-node native backup** — `snapshot_ctl::start_backup`
   (`db/snapshot-ctl.cc:297`) → `db::snapshot::backup_task_impl`
   (`db/snapshot/backup_task.cc`). Uploads one table's snapshot directory to
   object storage. Requires a **local-storage** table (S3 storage does not
   implement `snapshot()`, `sstables/storage.cc:1082`).
2. **Cluster / tablet-aware backup-restore** (the actively developed path) —
   `cluster_backup_task` (`db/snapshot/cluster_backup.cc`) and
   `sstables_loader::restore_tablets` (`sstables_loader.cc:1366`). Selects
   SSTables per-tablet, fans out RPCs to owning nodes, writes a `manifest.json`,
   and records state in `system_distributed` metadata tables.

Everything funnels through two seams:

- **`sstables::storage`** (`sstables/storage.hh:88`) — snapshot/seal/links,
  filesystem vs S3.
- **`sstables::object_storage_client`** (`sstables/object_storage_client.hh:72`)
  — the single interface for all S3/GCS IO: `upload_file`, `make_upload_sink`,
  `make_download_source`, `object_exists`, `put_object`, `make_object_lister`,
  etc.

Persistent metadata lives in the `system_distributed` keyspace
(`db/system_distributed_keyspace.*`, entry structs in `db/snapshot_types.hh`),
and the per-destination `manifest.json` schema is `db::snapshot::manifest_json`
(`db/snapshot/manifest.hh`). Backup and restore run as task-manager tasks in the
`snapshot` and `sstables_loader` modules respectively.

#### Cluster backup flow

```
REST /storage_service/tablets/backup
  -> snapshot_ctl::start_global_backup
    -> cluster_backup_task::do_backup           (coordinator, shard 0)
       - read/create system_distributed snapshot metadata
       - per DC/rack/tablet select sstables (dedup repaired across replicas)
       - RPC backup_snapshot_sstables -> each owning node
       - write manifest.json per destination
         -> db::snapshot::backup_sstables        (receiving node worker)
            - match requested sstable ids to on-disk files
            - upload each component via object_storage_client::upload_file
            - write refs/snapshot-<tag>/<gen> markers, skip if object_exists
            - update per-sstable state in system_distributed
```

#### Tablet-aware restore flow

```
REST /storage_service/tablets/restore
  -> sstables_loader::restore_tablets
     - download manifest(s), populate system_distributed.snapshot_sstables
     - alter tablet count to match manifest
     - storage_service::restore_tablets -> topology coordinator
        - drives each tablet into the `restore` transition
        - issues restore_tablet RPC to the owning node
          -> download_tablet_sstables               (sstables_loader.cc:959)
             - find sstables overlapping the tablet's token range
             - download from object store, attach_sstable, seal
             - update per-sstable download status
     - restore original schema
```

#### Repaired/unrepaired dedup across replicas

The cluster backup does not simply pick one replica per tablet. Its selection
loop (`cluster_backup.cc:224-243`) implements a cross-replica dedup built on
the tablet **incremental repair** machinery:

- `tablet_info.sstables_repaired_at` (`locator/tablets.hh:320`) is a monotonic
  per-tablet **repair generation**, committed through group0 by the topology
  coordinator when a tablet repair round succeeds
  (`service/topology_coordinator.cc:2530`).
- Each sstable carries a matching `repaired_at` generation in its stats
  metadata. A repair round stamps `sstables_repaired_at + 1` onto the
  participating sstables and onto the repair writer's outputs
  (`repair/row_level.cc:2199-2223`) — an in-place metadata update on immutable
  files; committing the tablet counter then atomically promotes them
  (`repair::is_repaired`, `repair/incremental.cc:18`).
- The classification survives compaction **by construction**: each compaction
  group keeps unrepaired / repairing / repaired sstables in separate logical
  views that never compact together (`replica/compaction_group.hh:59-88`); the
  repairing view is non-compacting for the round's duration
  (`replica/table.cc:3279`).
- The backup coordinator then includes an sstable with
  `repaired_at < tablet.repaired_at` from **every** replica, while the
  repaired set is claimed by a single per-tablet "repair master" replica.

The resulting guarantee is stronger than it may look: the backup contains
**every write present on any replica** at snapshot time, whether or not repair
ever ran. Repair is not a correctness requirement in this scheme — it is a
**cost optimization**: without it everything is unrepaired and the backup
degrades to RF× volume; with it, the cold bulk is uploaded once per DC. That
is the baseline the logstor replica-selection options are measured against
(see "Replica selection and the repaired/unrepaired question").

### How logstor stores data (recap)

See `docs/dev/logstor.md` for the full description. The relevant facts for
backup/restore:

- **Files are shared and mutable.** `ls_{shard}-{file_id}-Data.db` files
  (default 32 MiB) live in a single flat `logstor/` directory, hold **mixed**
  segments from multiple tables, and are rewritten in place by
  compaction/separator. You cannot hard-link a per-table unit the way SSTable
  snapshots do — and a segment is a 128 KiB sub-region of a 32 MiB file, so you
  cannot hard-link an individual segment either. Even at file granularity a hard
  link shares the inode, so it gives no isolation from in-place rewrites; see
  "On-disk snapshot".
- **Disk capacity is a fixed segment count, not filesystem space.**
  `logstor_disk_size_in_mb` fixes `_max_segments` per shard
  (`segment_manager.cc:1329`). Holding a segment therefore consumes allocation
  capacity that the write path needs, not just bytes on disk.
- **Full segments are self-describing.** The **separator** rewrites mixed
  segments into **full** segments, each belonging to a single table and token
  range; the on-disk `segment_header` carries `table_id`, `first_token`,
  `last_token` (`replica/logstor/segment_io.hh:24`, `ondisk.hh`). A full segment
  is therefore a self-contained, range-tagged, 128 KiB unit — a natural backup
  unit.
- **`segment_seq`** is a monotonic per-segment sequence assigned at seal
  (`ondisk.hh`), used for recovery tie-breaking. It is **node-local** and is
  reassigned by the streaming rewriter on the destination node.

### Logstor already has the required primitives

Logstor built snapshot/stream primitives for tablet migration; they are exactly
what backup/restore needs.

**Backup source — `table::take_logstor_snapshot(token_range)`**
(`replica/table.cc:1641`):

- Flushes the separator first (`sg->flush_separator()`), so every segment in the
  range is a per-table **full** segment.
- Pins each segment with a `segment_ref` and returns a
  `chunked_vector<segment_snapshot>` where each
  `segment_snapshot{segment_id, seg_ref, source}`
  (`replica/logstor/segment_manager.hh:86`) exposes a lazy
  `input_stream<char>` over the segment (`make_snapshot`,
  `segment_manager.cc:2590`).
- The `segment_ref` pins block segment **reuse/free**, not compaction. This is
  precisely how tablet streaming holds segments while sending them to peers
  (`streaming/stream_blob.cc:771`). Note that this precedent does **not** by
  itself justify holding pins across a long, throttled object-storage upload —
  see the pin cost analysis under "On-disk snapshot".

**Restore sink — `table::create_logstor_segment_sink`**
(`replica/table.cc:1654`):

- Allocates a fresh local segment and returns a `segment_stream_sink`
  (`segment_manager.hh:92`). Bytes written to its `output()` stream go through a
  `streamed_segment_rewriter` (`segment_io.hh:75`) that reassigns the segment's
  **local** seq; `close()` runs `load_segment` which re-indexes the records into
  the table's `primary_index` and reattaches the segment to its compaction group
  (`segment_manager.cc:2711`, `:2737`).
- This is the receive half of tablet streaming (`stream_blob.cc:218`).

So the design is essentially: **splice each segment snapshot's `input_stream`
source into `object_storage_client::make_upload_sink` for backup, and
`make_download_source` into the segment sink for restore**, plus a
manifest/metadata layer that describes segments by token range.

## Gap analysis

| Capability | SSTable path | Logstor today |
|---|---|---|
| Enumerate a table's durable units for a token range | snapshot dir + `parse_path` | `take_logstor_snapshot(range)` (exists, used only for streaming) |
| Stable unit identity for dedup | `sstable_id` (immutable) | none — segment ids recycled; needs `segment_seq` + content hash |
| Snapshot as an on-disk artifact | hard links (`filesystem_storage::snapshot`) | none — files are mutable in place, so a hard link is naming only, never isolation; needs pinning (segment- or file-granular), a copy, or upload from the in-memory snapshot |
| Upload a unit to object storage | `object_storage_client::upload_file(path)` | reuse via `make_upload_sink` + segment `input_stream` |
| Download + ingest a unit | `attach_sstable` | `create_logstor_segment_sink` + rewriter (exists) |
| Route unit to tablet/shard by range | token range from sstable metadata | `first_token`/`last_token` in segment header |
| Manifest / `system_distributed` metadata | sstable-centric (`sstable_id`, `toc_name`) | needs a generalized "storage unit" (segment) representation |
| Cross-replica dedup with union guarantee | `repaired_at` generations + per-tablet repair master (see Background) | none — Phase 1 backs up one replica per tablet (R3); stage 6 adds the repair seq-watermark (R2) |
| Repair (prerequisite for repair-aware backup) | row-level + incremental tablet repair | **silently a no-op** — reads no logstor data, writes invisible sstables — see "Existing defect" above |
| `auto_snapshot` protection on DROP/TRUNCATE | hard-linked snapshot dir, recoverable | **silently produces an empty snapshot** — see "Existing defect" above |

## Design

### Integration target

Target the **tablet-aware cluster path** (`cluster_backup_task` /
`restore_tablets` + topology coordinator). Logstor tables are tablet-based and
already require file-streaming for tablet migration
(`service/storage_service.cc:5418`), so per-tablet segment selection maps
directly onto `take_logstor_snapshot(token_range)`.

At each stage the code branches on `table.uses_logstor()` (`replica/database.hh`)
and adds a logstor path alongside the untouched SSTable path.

### Backup unit and object packaging — packed by default (resolved)

The **logical** unit of backup is always a single logstor **full segment** — a
self-describing, range-tagged 128 KiB unit. Segments are **packaged into
object-storage objects**; the two options below differ only in the
storage/upload/download layer and the object-level identity; the segment
identity, the per-tablet selection, and the ingest primitive
(`create_logstor_segment_sink`) are the same either way. **Option 2 (packed) is
the default**; Option 1 is its degenerate case.

Both options are described here. The manifest/metadata model (below) is designed
to support both by describing each segment with an object key plus an offset and
length within that object; option 1 is the special case where every segment
starts at offset 0 and spans the whole object.

#### Option 1 — One object per segment

Each 128 KiB full segment becomes **one object**.

Object key layout (following the Scylla Manager bucket layout used at
`cluster_backup.cc:491`):

```
{prefix}/{table_id}/{tablet_id}/seg-{segment_seq}-{content_hash}
refs/snapshot-{tag}/{table_id}/{tablet_id}/seg-{segment_seq}   (marker)
```

Upload: copy each segment's `input_stream` source into a dedicated
`make_upload_sink(object_name)`. Download: `make_download_source(object_name)`
for the whole object → segment sink.

- **Pros:** simplest; each object is independently addressable, so restore of a
  single tablet/segment is a plain `GET`; per-segment dedup is exact
  (`object_exists(key)`); no packing/index bookkeeping.
- **Cons:** many small (~128 KiB) objects → high request count and per-object
  overhead (latency, request cost, listing cost) for large tables. Concretely:
  a 5 GiB tablet is ~41k segments → ~41k PUTs per backup (plus one HEAD per
  segment if dedup were done by `object_exists` probing), and a 1 TiB table is
  ~8.4M objects per full backup — roughly $42 of S3 PUT requests alone, before
  HEADs, listing and GC. Small objects are latency-bound, so both backup and
  restore need very high parallelism to reach reasonable throughput.
  Object-store throughput is maximized with larger objects, so this is markedly
  slower and more expensive at scale.

#### Option 2 — Packed multi-segment objects

Multiple segments are concatenated back-to-back into a larger **pack object**
(e.g. one file's worth, ~32 MiB, or a configurable target size). A pack object
holds N segments for the same `{table_id, tablet_id}` (keeping a pack within a
single tablet preserves per-tablet restore granularity).

Object key layout:

```
{prefix}/{table_id}/{tablet_id}/pack-{tag}-{pack_seq}
refs/snapshot-{tag}/{table_id}/{tablet_id}/pack-{pack_seq}     (marker)
```

Pack keys are deliberately **not content-addressed**: the key must be chosen
before the upload sink opens (segments are appended as the snapshot cursor is
drained, so the pack's content is unknown up front), and dedup is per-segment
via the manifest (below), so a content hash in the key would buy nothing.
Re-uploading the same key on a retried backup of the same tag is an idempotent
overwrite.

Each contained segment is described in the manifest/metadata by
`{object_key, offset, length, segment_seq, content_hash, first_token,
last_token}`. Since segments are already 128 KiB (block-aligned) units, a pack is
just their byte concatenation and offsets are exact multiples of the segment
size — no reframing of the on-disk chunk format is required.

- **Upload:** open one `make_upload_sink(pack_object)` and stream a batch of
  segment `input_stream` sources into it sequentially, recording each segment's
  offset as it is appended. Object-storage multipart upload (the sink already
  supports it) turns this into efficient large-object writes.
- **Download / restore:** for a tablet, download only the byte ranges that
  contain segments overlapping the tablet. Two sub-strategies:
  - **Ranged GET per segment** — issue an HTTP range request for
    `[offset, offset+length)` of the pack object and feed it to the segment sink.
    Requires the download source to accept a byte range (see change list). Keeps
    download volume minimal.
  - **Whole-pack GET + split** — download the whole pack once and split it into
    segments locally. Simpler, but downloads unneeded segments if a pack
    straddles a tablet boundary; best when packs are pinned to a single tablet.
- **Dedup:** stays **per-segment and exact** via manifest-based dedup (see
  "Segment identity and deduplication"): when assembling a new pack, a segment
  whose `content_hash` appears in the previous backup's manifest is simply
  **re-referenced** — its entry in the new manifest points
  `{object_key, offset, length}` into the older pack object — rather than
  re-uploaded. A backup's manifest may therefore reference pack objects from
  several earlier backups; that is by design and costs only manifest entries
  (plus reference-aware backup GC).

- **Pros:** far fewer, larger objects → higher upload/download throughput, lower
  request count and cost, cheaper listing. Matches how object stores are meant
  to be used.
- **Cons:** more bookkeeping (pack index: offsets/lengths per segment — but the
  manifest schema carries these fields anyway); partial/single-segment restore
  needs ranged reads or over-download (moot for whole-tablet restore, which
  downloads exactly its packs); a pack that spans a
  tablet boundary complicates per-tablet restore (avoided by never packing
  across tablets).

#### What changes between the two options

| Aspect | Option 1 (per-segment) | Option 2 (packed) |
|---|---|---|
| Object key | `seg-{seq}-{hash}` | `pack-{tag}-{pack_seq}` + per-segment offset/length |
| Manifest/metadata per segment | object key only (offset 0, full length) | object key **+ offset + length** |
| Upload worker | one sink per segment | one sink per pack; append segments, record offsets |
| Restore reader | whole-object GET → sink | whole-pack GET + split → sinks (default); ranged GET optional |
| Dedup granularity | per segment (exact) | per segment (exact) via previous-manifest re-referencing |
| `object_storage_client` needs | existing API | existing API (whole-pack GET); ranged `make_download_source` only for the optional ranged-GET strategy |
| Partial/single-tablet restore | plain GET | ranged GET or over-download |

Because the metadata already carries `{object_key, offset, length}` per segment,
the **restore path is identical** for both options apart from whether the range
is "the whole object" or "a sub-range". This lets the choice be made
per-backup (even configurable) without changing the on-disk logstor format or
the ingest primitive.

**Decision: Option 2 (packed) is the Phase 1 default.** The incremental cost of
packing over per-segment objects is small precisely because of choices already
made elsewhere in this design: the manifest carries `offset`/`length` from day
one, packs never cross tablets (so whole-tablet restore downloads exactly its
packs), and the **whole-pack-GET + split** restore strategy needs no ranged-read
support in `object_storage_client`. The pack-assembly loop on the backup side is
one `make_upload_sink` plus offset tracking, and it composes naturally with the
stage 0 snapshot cursor: drain one segment at a time straight into the open pack
sink, so peak memory stays one segment. Manifest-based dedup (below) keeps dedup
per-segment and exact, removing the last argument for per-segment objects.
Option 1 survives as the degenerate case `pack_size = segment_size` for
bring-up and tests, controlled by the same `pack_size` knob — it is not a
production default.

### Segment identity and deduplication

Deduplication across incremental backups is in scope for v1. SSTable dedup keys
on the immutable `sstable_id`; logstor cannot, because segment ids are recycled
and segment content is mutable. Instead:

- **Identity = `segment_seq` + content hash.** `segment_seq` is monotonic and
  never reused on a node, so it is stable for the lifetime of a segment on the
  source node. The **content hash** (CRC/xxhash over the segment buffer bytes;
  the per-buffer CRC already present in the `chunk_header` may be reusable to
  avoid a second pass) guards against any seq ambiguity and provides
  verification.
- **Dedup is manifest-based, not probe-based.** Before uploading a tablet, the
  node loads the **previous backup's manifest** for the table from the bucket —
  deliberately *not* the `system_distributed` rows, which carry a 3-day TTL
  while the manifest is durable in object storage — and builds a
  `content_hash → {object_key, offset, length}` map. A segment whose hash is in
  the map is **re-referenced**: its entry in the new manifest points into the
  existing pack object and nothing is uploaded or probed for it. Only segments
  with unseen hashes are appended to the current pack. This keeps dedup exactly
  per-segment under packing with **zero per-segment HEAD requests**; the
  per-unit `object_exists` probing the sstable path uses
  (`cluster_backup.cc:517`) would cost one round-trip per 128 KiB segment,
  ~41k HEADs per 5 GiB tablet. Pack uploads are retried by overwriting the same
  key (idempotent), so no `object_exists` probing is needed anywhere on the
  backup path.
- **GC consequence:** because a manifest may reference packs uploaded by
  earlier backups, deleting an old backup must retain any of its pack objects
  still referenced by a newer manifest — backup GC is reference-aware, which is
  what the `refs/…` markers track.

**Documented limitation:** compaction and the separator continuously rewrite
live records into new segments with new seqs. Dedup therefore benefits
cold/stable data most; hot data is re-uploaded on each backup. This is inherent
to log-structured storage, not a defect.

#### Incremental enumeration — `segment_seq` watermark (follow-up)

Manifest-based dedup avoids re-*uploading* unchanged segments, but each backup
still **reads** every segment of the tablet — a full 1× disk pass — just to
compute hashes. The follow-up that removes this cost rests on one fact: on a
given node, `(segment_id, segment_seq)` identifies content. The seq is assigned
at seal and never reused, and a sealed full segment's bytes never change until
the segment is freed (reuse assigns the id a new seq). So the node records the
maximum seq covered by a backup, and on the next backup **skips reading** any
segment whose `(segment_id, segment_seq)` appears in the previous backup's
metadata with `seq <= watermark`, carrying the old manifest entry forward
verbatim. Only segments sealed since the last backup are read and hashed.
(Learning a segment's seq without the full read requires either keeping the seq
in the in-memory descriptor or a header-only read — still ~32× less IO than the
128 KiB segment.) This turns incremental backup IO from O(tablet size) into
O(churn), which is what makes frequent periodic backups viable on large,
mostly-cold tables.

Requirements: the coordinator's tablet → replica choice must be **sticky**
across backups (Phase 1 already picks the primary replica deterministically),
with a full read-and-hash fallback whenever it is not — hash-based dedup still
works after a replica switch, it just costs the full read. The Phase 1 metadata
already carries `segment_seq` per entry, so nothing in the v1 format forecloses
this.

### Replica selection and the repaired/unrepaired question

Logstor segments are per-replica physical artifacts — independent layouts,
seqs, and bytes on every replica, with no cross-replica identity — so the
sstable path's dedup ("upload the repaired set once, the unrepaired sets from
everyone"; see "Repaired/unrepaired dedup across replicas" in Background) has
nothing to key on. The question is what replica-selection policy logstor
backup uses and what guarantee it yields. Four options (the third independent
option group, R1–R4). **Decision: R3 for Phase 1, R2 as stage 6.**

Everything repair-related below sits behind one prerequisite: tablet repair is
currently a silent no-op for logstor (see "Existing defect"). This plan
assumes logstor tablet repair will be implemented — **full** repair is enough;
nothing here needs the incremental-repair machinery.

#### Why the sstable concept does not port literally

Three structural mismatches rule out a mechanical port:

1. **The classification unit is wrong.** Sstable classification rides on
   immutable files; logstor compaction continuously rewrites records from many
   segments into new ones. A repaired/unrepaired split on segments must
   survive that, which means **class-segregated compaction** — never mix
   classes in one output segment — a new constraint on the hottest machinery.
2. **Promotion cannot be an in-place stamp.** Promoting sstables to "repaired"
   rewrites a small metadata component in place. Sealed logstor segments must
   stay byte-stable: both the `content_hash` dedup identity and every
   `segment_seq`-watermark rule in this document depend on "a sealed segment's
   bytes never change". A design that stamps sealed segment headers in place
   would undermine the backup's own dedup foundation.
3. **Logstor keeps one live version per key, and compaction consults the
   global index.** A repaired-class logstor compaction would drop a record the
   moment the index points at a newer (unrepaired) record — unlike LSM, where
   the shadowed old version survives in the repaired sstable because
   cross-class shadowing is invisible to compaction. This does **not** break
   the union guarantee (the shadowing record lives in some backed-up class on
   the same replica), but it means the sstable path's framing — "the repaired
   sets are logically identical across replicas" — never holds for logstor;
   only the per-key-latest union property does. Since logstor is
   whole-row-write key-value storage, per-key-latest is the correct semantic,
   and it actually simplifies the correctness argument.

#### Option R1 — Faithful port: class-segregated log

Give every segment a class (unrepaired / repairing / repaired): class in the
descriptor and in the segment header at seal time; compaction never mixes
classes and its output inherits the input class (headers are written at seal,
so inheritance needs no in-place update); already-sealed segments are promoted
at round end via a durable, node-local per-tablet side record ("this replica's
segments with `seq <= W_r` are generation N"), not by touching their bytes; the separator needs no change
(its input is always new writes, hence unrepaired); the streaming rewriter
must carry the class across tablet migration; recovery must reconstruct
classes from headers plus the side records.

- **Property:** exact parity with the sstable scheme — the union guarantee
  with the repaired bulk deduplicated to one replica, exactly.
- **Cost:** the largest option by far — class-aware candidate selection per
  group, durable classification and its recovery, a repair-round freeze
  (participating segments excluded from compaction for the round's duration,
  the analogue of the non-compacting repairing view), and streaming/migration
  interactions. It also needs incremental-style round bookkeeping, which the
  assumed full-repair support does not provide.
- **Verdict: rejected.** Everything R1 buys over R2 is *exactness* of the
  repaired set; R2 delivers the same guarantee with over-inclusion (extra
  upload volume, never extra risk) as the only penalty, and touches none of
  the hot-path machinery.

#### Option R2 — Repair seq-watermark (chosen for stage 6)

The observation that collapses R1: **`segment_seq` already encodes "sealed
before / after a point in time", and sealed segments are immutable.** No
classes, no compaction changes, no format changes:

- There is **no cluster-wide watermark**. `segment_seq` is node-local, so each
  replica `r` records its **own** watermark `W_r` — the highest seq it has
  sealed for that tablet on its own shard — when a tablet repair round starts
  on it. `W_r` is only ever compared against `r`'s own segment seqs; watermark
  values are never exchanged or compared across nodes. The only cross-node
  identifier is the **round identity** (the tablet's `repair_time` / repair
  task id from the tablet map), stored alongside `W_r`. When the tablet map
  later shows that round **completed**, the record
  `{table, tablet, round-id, W_r}` — persisted in a small node-local system
  table — becomes usable.
- On each replica `r`, **"repaired-class" for backup = its segments with
  `seq <= W_r`** of the last completed round; **"unrepaired-class" = its
  segments with `seq > W_r`**. The numeric values differ per replica; the
  semantic boundary — "sealed on this replica before the last completed
  repair round began" — is the same.
- The coordinator picks a per-tablet master replica exactly as in Phase 1 and
  never sees any watermark; it only assigns roles. The master gets a **full**
  request and contributes all its segments; every other replica gets a
  **delta** request, looks up its own `W_r` for the round-id, and contributes
  only its `seq > W_r` segments. A replica with no usable watermark for that
  round contributes everything (see invalidation below).

**Soundness** (applied on each replica independently). Seqs are assigned at
seal and are monotonic; sealed bytes never change. Therefore every record in a
`seq <= W_r` segment on replica `r` existed on `r` before the round started. A
full tablet repair converges all data live at round start across replicas, so
any such record that is live is, after the round, present on every replica —
in particular on the master, which contributes everything; `r` can therefore
skip that segment without losing anything. Any write *not* guaranteed
converged by the round (it arrived after round start) resides only in
`seq > W_r` segments on whichever replicas received it — and every replica
contributes those. Dead records inside contributed segments are shadowed
duplicates, resolved per key by timestamp on restore. Hence the restored
per-key latest equals the union's per-key latest — the same guarantee class as
the sstable path.

**Over-inclusion is the only failure mode, and it errs in the safe
direction.** Compaction rewriting old cold data after the round moves it into
`seq > W_r` segments, so non-master replicas upload it redundantly — cost, never
correctness. Dense cold segments are poor compaction candidates and settle
below the watermark, so the steady-state extra volume is O(write + compaction
churn since the last repair) per non-master replica — the same churn-bounded
shape as content-hash dedup, and the same degradation profile as the sstable
path, which also uploads hot (unrepaired) data from every replica. With no
completed repair or no watermark the fallback is RF× — exactly what the
sstable path degrades to when repair never runs.

**What R2 needs:**

- **Working logstor tablet repair** — the hard prerequisite (assumed; see
  "Existing defect"). Full repair is sufficient: the watermark keys on *round
  completion identity* — the tablet's `repair_time` or repair task id as
  committed in the tablet map — not on `sstables_repaired_at` generations.
  Incremental repair for logstor, if it ever exists, changes nothing here.
- **The watermark record:** written at round start as
  `{table, tablet, round-id, W_r}` (per shard — a tablet lives on one shard
  per node). At backup time a record is usable iff its round-id matches the
  tablet's last completed round per the tablet map; failed rounds never match,
  and the next round overwrites the record.
- **Cheap seq visibility:** enumerating "segments with `seq > W_r`" wants the
  seq without reading 128 KiB — the same requirement as the stage 4
  `segment_seq`-watermark read skip (seq in the in-memory descriptor or a
  header-only read), so the two features share it.
- **Invalidation → full contribution:** no watermark for the last completed
  round (the replica joined after it via migration/rebuild — its streamed
  segments carry fresh local seqs anyway), intranode migration since the
  round (seqs are per-shard, so a shard change invalidates `W_r`), or any
  doubt: contribute everything. Over-inclusion is always safe.
- **A delta mode in the backup request**, so the node-side handler knows
  whether to filter the snapshot cursor by seq (master = full, others =
  delta).

A separator flush at round start is *not* required for soundness — a record
written before the round but still sitting in a mixed segment at round start
is simply separated later into a `seq > W_r` full segment and over-included —
but flushing at round start reduces that over-inclusion for free.

What R2 deliberately gives up versus R1: the repaired set is approximate
(over-inclusive under churn) rather than exact, and nothing marks records. In
exchange it buys the union guarantee at the sstable path's steady-state cost
profile with **zero changes to compaction, the separator, segment headers,
streaming, or recovery** — and it composes with the rest of this design: the
repair watermark collapses cross-replica duplicates within one backup,
manifest-based dedup collapses cross-backup duplicates within one replica, and
the stage 4 watermark skips re-reading them; all three rest on the same
sealed-segment immutability.

#### Option R3 — Single replica per tablet (Phase 1)

Back up one deterministically chosen replica per tablet — the Phase 1 plan.

- **Property: strictly weaker than the sstable path.** The backup is that
  replica's view. A write acknowledged at CL < ALL that the chosen replica
  missed is absent, even though other replicas hold it. With periodic repair
  the exposure window is (last completed repair, per-tablet snapshot moment];
  without repair it is unbounded. For data older than the last repair the
  guarantee equals the sstable path's repaired-set guarantee.
- **Cost:** exactly 1× — the cheapest possible; no new logstor mechanism.
- **Repair-freshness gating must wait for real repair.** Gating on
  `tablet_info.repair_time` while repair is a silent no-op for logstor would
  be worse than not gating: it would convert "one replica's view" into a false
  claim of repaired consistency. Until logstor repair lands, R3 ships and is
  documented as "single-replica view", full stop.
- One piece is free today: `snapshot_tablet_entry` and the manifest already
  carry per-tablet `repair_time`/`repaired_at`, so stage 1a populates them
  from the tablet map and every backup records its per-tablet consistency
  point.

#### Option R4 — Back up every replica

Every tablet from every replica; restore merges by timestamp.

- **Property:** the full union guarantee with **no dependency on repair at
  all** — the only option that matches the sstable path's guarantee today.
- **Cost:** RF× upload, storage, and restore download, on every backup,
  forever; cross-replica content-hash dedup never hits (each replica's
  segments are physically different bytes).
- **Verdict:** not a default. It survives as the per-replica fallback R2
  degrades to when watermarks are missing, and as a possible explicit opt-in
  for users who need the union property before logstor repair exists.

#### Multi-source merge caveats (R2 and R4)

Both options make restore ingest segments for one tablet from several
replicas. Mechanically nothing changes — all entries are range-tagged
segments, the download loop already groups by object key across sources, and
the primary index resolves each key to the max-timestamp record on insert
(`replica/logstor/index.hh:230-247`). Two semantic caveats to record:

- **Same-timestamp conflicts resolve by ingest order.** The index insert
  comparator is timestamp-only and a tie keeps the last-ingested record —
  unlike the sstable path, which retains all copies and reconciles
  deterministically at read time by CQL rules (value comparison). A
  multi-source logstor restore can therefore pick a different winner for a
  same-timestamp conflicting write than a live cluster would. Narrow, but a
  real divergence to document.
- **Tombstones.** Today logstor never purges tombstone records — a delete is
  an ordinary record that remains the key's live version indefinitely — so
  merging replicas cannot resurrect deleted data. If tombstone GC is ever
  added to logstor compaction, it must be repair-aware (the moral equivalent
  of repair-based tombstone GC) **before** multi-source restore remains safe:
  purging a tombstone on one replica while another still holds the shadowed
  write would resurrect that write in the merged restore.

#### Decision

- **Phase 1 ships R3, ungated:** one deterministically chosen replica per
  tablet, no repair-freshness gate (meaningless until logstor repair is
  real), the property difference documented, and per-tablet
  `repair_time`/`repaired_at` recorded in metadata and manifest for free.
- **Stage 6 ships R2**, once logstor tablet repair (full repair is enough)
  exists. It restores the sstable path's union guarantee at churn-bounded
  extra cost, reuses the seq-immutability foundation shared with content-hash
  dedup and the stage 4 read skip, and changes no on-disk format. The Phase 1
  metadata and manifest need nothing new for it beyond what is already
  carried — entries are per-node, and token-range grouping on restore is
  source-agnostic; only the delta flag in the backup request and the
  node-local watermark table are new.
- **R1 is rejected** (all of R2's guarantee at far higher cost and hot-path
  risk); **R4 survives as R2's fallback**, not as a default.

### On-disk snapshot — options (open)

The backup needs a stable point-in-time set of segments while uploading. The
manifest/metadata is identical for all options below, so the choice is isolated
to the enumerate-and-upload worker.

#### Constraints from the logstor implementation

Four facts decide this section; all of them differ from the SSTable model.

1. **The disk budget is a fixed segment count, not filesystem space.**
   `_max_segments = disk_size / file_size * segments_per_file`
   (`segment_manager.cc:1329`) and `available_segment_count()` = free list +
   segment pool + never-yet-allocated segments (`:1104`). A pinned SSTable
   snapshot merely occupies disk; a pinned logstor segment **subtracts from a
   fixed allocation pool**. When that pool empties, `allocate_segment()` blocks
   in a 5-second retry loop (`:1748`) — i.e. a write stall.
2. **A pin is `segment_descriptor::ref_count`; compaction honours it but does
   not account for it.** `make_segment_ref` (`:1148`) bumps `ref_count`;
   compaction does `ss.remove_segment(desc); if (desc.ref_count == 0)
   free_segment(...)` (`:2198`, `:2305`). Compacting a pinned segment therefore
   removes it from the group, rewrites its live records into **new** segments,
   and frees **nothing**.
3. **Compaction's candidate scoring is blind to pins.**
   `select_segments_for_compaction` (`:1964`) scores candidates by
   `reclaimed() = n_in - n_out` estimated from live bytes only. It cannot tell
   that a pinned input will not be reclaimed.
4. **Data files are mutable in place, so a hard link is not a snapshot.**
   `segment_id → (file_id, offset)` is a fixed arithmetic mapping (`:1200`) and
   reusing a segment id overwrites that file region. A hard link shares the
   inode, so writes through the original path are immediately visible through
   the link. Hard-linking a logstor data file provides **naming**, never
   isolation; isolation still requires that no segment in the file be reused.

#### Option A — Direct from the in-memory pinned snapshot

Back up straight from `take_logstor_snapshot(range)`: the returned
`segment_ref`s pin segments from reuse/free while their `input_stream` sources
are uploaded. Compaction keeps running; only segment reuse is blocked. No
on-disk snapshot directory.

- **Pros:** no extra disk IO or space; smallest change; same primitive as tablet
  streaming, which already holds these refs across a network transfer
  (`stream_blob.cc:771`).
- **Cons:** see the pin-cost analysis below. Naive whole-tablet pinning is
  **not** safe to ship as-is. No `nodetool snapshot`-style artifact.

##### Pin cost analysis (why naive Option A is not enough)

*Static cost is modest.* A 5 GiB tablet (`default_target_tablet_size`,
`service/tablet_allocator_fwd.hh:16`) is ~40,960 segments; on a shard with
128 GiB of logstor disk that is ~4% of the segment pool — survivable against the
20% `logstor_compaction_trigger_threshold_percent`.

*The dynamic cost is a positive feedback loop.* Pinning lowers
`available_segment_count`, which:

- pushes `should_run_auto_compaction()` (`:1252`) below the low watermark, so
  auto compaction starts;
- raises `compaction_pressure()` (`:1218`), so compaction CPU shares ramp up;
- causes compaction to pick segments from the pinned group, believe it will
  reclaim `n_in - n_out`, rewrite their live data into **new** segments, and
  reclaim **zero**;
- therefore lowers `available_segment_count` by `n_out`, raising pressure again
  and keeping the `while (should_run_auto_compaction())` loop (`:1299`) running.

A backup can thus trigger a compaction storm that burns IO and CPU at escalating
shares while making free space strictly *worse*. Worst-case extra consumption
during one tablet's backup is that tablet's live data rewritten — another ~4% of
the shard, so ~8% total against a 20% headroom, for a single tablet.

*The dangerous multiplier is concurrency.* The coordinator fans out one RPC per
tablet (see the Phase 1 plan); nothing in the flow bounds how many tablets a
single node backs up at once. Five concurrent tablets on one shard puts pins plus
non-reclaiming compaction output at ~40% of the segment pool — write-stall
territory. **Bounding this concurrency is an operational-correctness
requirement, not an optimization.**

*Duration matters.* The tablet-streaming precedent is weaker than it looks:
streaming runs at internal-network speed on a tablet that is about to be
discarded anyway, whereas backup runs throttled by
`backup_io_throughput_mb_per_sec` against object storage, with S3 tail latencies
and retries. A throttled 5 GiB tablet at 50 MB/s pins for ~100 s; a stalled
multipart upload extends that with no upper bound.

*Bookkeeping memory is a secondary cost.* Each `segment_snapshot`
(`segment_manager.hh:86`) holds a `segment_ref` whose state carries two
`std::function`s, plus a `noncopyable_function` source closure — roughly
150–200 bytes per segment. That is ~6–8 MB per 5 GiB tablet, and ~80 MB for a
50 GiB whole-shard snapshot. If bulk pinning survives, `segment_ref` should get
an intrusive refcount instead of two heap `std::function`s.

##### Improvements to Option A (in value order)

- **A1 — Make compaction pin-aware.** In `select_segments_for_compaction`
  (`segment_manager.cc:1964`), skip descriptors whose `ref_count` exceeds the
  group's own reference, or at minimum score them as `reclaimed = 0`. Compacting
  a pinned segment is always pure loss: it reclaims nothing and consumes output
  segments. A small, local change that removes the feedback loop at its root.
  **It is needed by every option below, and it also benefits tablet streaming,
  which pins the same way today.**
- **A2 — Bound pinned segments with an admission semaphore.** A shard-level
  semaphore in units of segments (a configurable percentage of `_max_segments`),
  acquired before `take_logstor_snapshot` and released with the pins. Backup
  waits instead of squeezing the write path, and coordinator fan-out is
  implicitly bounded without the coordinator knowing shard-level state. Provides
  the pinned-segment metric stage 3 wants for free.
- **A3 — Pin on eviction ("rescue-pin") instead of pinning the whole tablet
  (the real fix).** `make_snapshot` pins everything today because a segment
  freed between enumeration and consumption takes its live data with it. The
  observation that fixes this is that the current design **conflates two
  different needs**: protecting *bytes under an active reader* requires a pin,
  but only for the duration of the read; protecting *the coverage claim* — "I
  will deliver all of this data" — requires bookkeeping, not a pin. The
  primitive to add is therefore:

  > Register the snapshot in a shard-level pending-segment registry. Enumerate
  > segment ids cheaply, without pins. Pin only the segment currently being
  > consumed. **When compaction is about to free a segment that is still
  > pending, pin the original segment instead of letting it go.**

  Mechanically this is small. The free loop in `do_compaction`
  (`segment_manager.cc:2192`) is `ss.remove_segment(desc); if (desc.ref_count ==
  0) _sm.free_segment(seg_id);`, `remove_segment` decrements
  (`compaction.hh:205`), and the loop is synchronous — no yields between removal
  and free. A hook running just before it that calls `make_segment_ref` on any
  pending segment leaves `ref_count` at 1 after removal: the segment is detached
  from its group but **stays allocated with its bytes untouched**. The consumer
  reads it later via `create_segment_input_stream`, which is a plain file-region
  read needing no group. On release, `ref_count` reaches 0 and `free_segment`
  runs; its `record_bytes == 0` assertion (`:1765`) holds because compaction
  already rewrote the live records and the index updates fired `on_free_record`
  for every old location.

  **Correctness.** Because the snapshot keeps the *original* segment,
  byte-for-byte as it was at snapshot time, nothing filters its contents. The
  invariant is the strong one:

  > *Every record live at snapshot time is consumed at least once.*

  This holds with no dependence on a concurrent write mirror, so it is correct
  for **backup** as well as for tablet streaming. Duplicates are harmless
  because the consumer resolves by `(timestamp, seq, offset)` in the
  `primary_index` (see "Restore correctness").

  **Cost.** Pins are demand-driven: taken only at the moment compaction would
  otherwise destroy data.

  - *Worst case* — compaction sweeps the whole tablet during the operation:
    degrades to whole-tablet pinning, i.e. exactly Option A's cost. Never worse.
  - *Common case* — compaction touches few pending segments: near-zero pinning.
  - The pending set is **monotonically shrinking**, so termination is trivial.

  Two rules keep the common case dominant: **A1** already excludes *pinned*
  segments from compaction candidacy, so a rescued segment is never re-selected;
  add the companion soft rule to **deprioritize pending-but-unpinned segments**
  in `select_segments_for_compaction`, and consume the pending set in
  compaction-candidate order (lowest utilization first) so the reader wins most
  races. Schedule **rescued segments for consumption first** — their data is pure
  liability, already duplicated in the compaction output, so draining them
  returns segments to the free list fastest.

  **Escalation ladder.** Give the snapshot a rescue budget `B` (e.g. 1% of
  `_max_segments`): under `B`, rescue freely; at `B`, stop rescuing and hold
  `disable_compaction(cg)` (**A4**) for that group until the backup drains back
  under `B` — coherent with A1, since either the segment stays alive for the
  reader or compaction waits, and the window is bounded by drain rate rather
  than by the whole tablet; past a hard cap, abort the tablet's operation
  (**A5**).

  ##### Variant: chase compaction output instead of rescuing

  The alternative hook substitutes compaction's *output* segments into the
  pending set rather than pinning the input. It keeps pinning at one segment
  even under heavy compaction and never grows the pinned set — but it provides
  only the **weaker** invariant, because compaction filters by *current*
  liveness (`is_record_alive`, `segment_manager.cc:2170`):

  > Key K is live at snapshot time in pending segment S. K is overwritten during
  > the operation and the new version lands in a segment outside the snapshot
  > set. Compaction then rewrites S and drops K's old record. The chase appends
  > the output, which contains no K. The snapshot carries **no version of K at
  > all** — a lost key, not a stale one.

  What the chase provides is only:

  > *The union of what the snapshot delivers and what the concurrent write path
  > delivers covers every record live at snapshot time.*

  **Tablet streaming satisfies this by construction** — the `streaming` stage
  runs inside `write_both_read_old` (`locator/tablets.hh:360`,
  `docs/dev/topology-over-raft.md:41`), so concurrent writes already reach the
  pending replica independently of the stream; omitting an overwritten record
  just means the destination gets it via the write path instead of twice.
  **Backup does not satisfy it** and must never use this variant.

  The chase also has a cost rescue-pin does not: the pending set can **grow**
  (compaction takes 8 segments of which 3 are pending and emits 5 outputs → net
  +2), so it needs a hard chase budget for termination rather than relying on
  the `n_out < n_in` rule (`:2008`), which split compaction re-processing its own
  output does not respect.

  **Therefore:** rescue-pin is the default and only policy needed. The chase is
  retained here as an opt-in mode for a consumer that both tolerates the weaker
  invariant and is under space pressure — today that means tablet streaming
  alone — and is not required by anything in this plan.

  ##### Rejected alternatives for the backup case

  Two other ways to close the chase's gap were considered and are worse than
  rescue-pin:

  - **Chase post-snapshot segments until the upload catches the group head.**
    Converges only if upload rate exceeds write rate, which is not guaranteed
    under sustained load, and yields a fuzzy per-key instant. Worse, the *final*
    pass still loses keys overwritten during it, so it needs a short
    compaction-disabled pass to close out regardless — arriving at a convergence
    loop plus the same fallback that rescue-pin uses as one step of its ladder.
  - **Snapshot-scoped GC grace in compaction** — retain records that are dead now
    but were live at snapshot time, testable in O(1) against a `segment_seq`
    watermark `W` as `retain if alive || seq(current_location) > W`. This
    collides with logstor's index-driven space accounting: a retained-dead record
    must not update the index, so it adds nothing to the descriptor's
    `record_bytes`; the output segment then looks empty, becomes compaction's
    most attractive candidate, is compacted, and carries its retained-dead
    records into another empty-looking segment — an **infinite churn loop**.
    Fixing it requires a `retained_data_size` field on `segment_descriptor`
    folded into `record_bytes` and cleared when the watermark retires: new
    lifecycle in the hottest accounting path, for a strictly worse outcome.

  Note that **Option B** sidesteps this question entirely, because the snapshot
  is materialized rather than tracked. Rescue-pin is what makes Option A correct
  for backup in the meantime.
- **A4 — Scoped compaction disable.** Hold `disable_compaction(cg)`
  (`compaction.hh:328`) for a group, with a hard timeout. Its primary role is as
  **rescue-pin's budget escalation step** (above), where the window is bounded by
  drain rate rather than by the whole upload. It also remains available as a
  standalone stopgap if A3 slips: compaction of a fully pinned group accomplishes
  nothing anyway, so nothing is lost. Either way, a stuck upload must not wedge
  compaction, and other groups must stay compactable.
- **A5 — Yield to the workload.** If `available_segment_count` crosses the soft
  pressure threshold, abort the in-flight tablet backup and release its pins
  rather than stalling writes. Content-hash dedup makes the retry cheap. Backup
  should be the party that loses.

#### Option B — Persist an on-disk snapshot first, then upload lazily

Two variants. Both give a real on-disk artifact (parity with `nodetool
snapshot`, restartable and inspectable) and decouple upload duration from the
live write path. Note that per constraint 4 above, **hard-linking alone provides
no isolation** — file-granular pinning is inherent to this option, not an
implementation detail.

##### B-share — file-aligned snapshot compaction + hard link + file-granular pin

1. Add a file-granular allocation mode: a writer can reserve a whole file's run
   of `segments_per_file` contiguous segment ids. This is a contained change to
   the free-list / `_next_new_segment_id` allocator (`:1716`).
2. At snapshot time, for each of the tablet's files: if the file is **already
   entirely owned by this group and fully live**, take it as-is. Otherwise run a
   compaction whose output goes into freshly reserved whole files.
3. Hard-link the resulting files into `logstor/snapshots/<tag>/`, named after the
   source file so the link is self-describing.
4. Mark those segment ids no-reuse until the link is dropped. Recovery
   reconstructs the pin set by listing the snapshot directory.

- **Pros:**
  - **No extra copy.** Only the sparse remainder is rewritten, by a compaction
    that was partly wanted anyway.
  - **Whole 32 MiB files, not 40,960 tiny per-segment files per tablet.**
  - **Resolves the packaging question for free:** a 32 MiB snapshot file *is* a
    natural Option 2 pack object — one PUT per file, and the manifest `offset` is
    just `segment_index * segment_size`.
  - **Garbage is dropped**, fixing the unpredictable backup-inflation risk.
  - **Upload is fully decoupled:** restartable, survives restart, throttleable,
    with zero interaction with the live write path.
  - **Good incremental steady state:** cold data settles into dense, stable files
    that are re-linked without rewriting; their content hashes are unchanged, so
    the previous-manifest dedup map re-references them without uploading a byte.
- **Cons / risks:**
  - **It relocates the pin cost, it does not remove it.** The linked files hold
    segments that are also live data, so later compaction wants to rewrite them
    and still cannot free them. **A1 is required here too.** What B-share buys is
    that the pin is durable, coarse, inspectable, and decoupled from upload
    duration — not that it is cheaper.
  - **Write amplification unless the "link dense files as-is, rewrite only the
    sparse remainder" rule is implemented.** Without it, every backup rewrites
    the whole tablet and **dedup drops to zero**, because every segment gets a
    new seq and new content. This refinement is not optional.
  - **Tail waste:** ≥1 partially filled file per tablet, up to
    `file_size × tablets_per_shard` (~3 GiB/shard at 100 tablets). Tunable via
    file size or a shared tail file.
  - **New lifecycle surface:** snapshot-dir GC, recovery-time pin
    reconstruction, orphan links after a crash mid-backup.
  - **Cross-group contamination:** a file holding even one segment from another
    group cannot be linked without pinning that group's segment too. This is
    exactly why the arrange-by-file step is required — and it has independent
    value (compaction locality, cheap whole-file tablet migration).

##### B-copy — dedicated snapshot files

Write the snapshot into **dedicated** files that belong to no live group, then
release the pins. Live data then compacts entirely normally and no pin-awareness
is needed anywhere.

- **Pros:** simplest to reason about; full isolation from the live path; upload
  path identical to SSTables (`upload_file`).
- **Cons:** a genuine extra copy of the tablet's live data, in both IO and disk
  space, on every backup.
- Reasonable fallback if the allocator work for B-share proves too invasive.
  Note it must still write whole files, not one file per 128 KiB segment.

##### Rejected for now — reflink (`FICLONE`) instead of hard link

On XFS with `reflink=1` a reflink copy would give a true copy-on-write snapshot
with no pinning at all: space grows only with actual churn. Rejected because it
is filesystem-dependent (unavailable on ext4) and because O_DIRECT writes into
shared extents cause extent splitting and fragmentation that Scylla's IO
scheduler and space accounting do not model — high risk against a carefully
tuned direct-IO write path. Recorded here so it is not rediscovered as a free
win.

#### Option C — Compact directly to object storage

Reuse `compaction_buffer`, but point its output sink at `make_upload_sink`
instead of a `writeable_segment`, and skip the index update (this is a pure
read; nothing moves). Read the tablet's segments in physical order, pack records
into full-segment-shaped buffers in memory, and upload.

- **Pros:** zero disk space and zero extra writes — only a 1× read of the
  tablet; pins only the segment currently being read; output contains no padding
  and no garbage; the output format is exactly a full segment, so the restore
  path is unchanged; combines naturally with packing (emit 32 MiB objects).
- **Cons:**
  - **Liveness filtering is unsafe on its own.** If a key is overwritten during
    the scan, the old record is skipped as dead while its new version sits in a
    segment outside the scan set — the key vanishes from the backup entirely.
    That is data loss, not fuzziness. Safe rule: copy every record in the frozen
    segment set regardless of liveness (same content as Option A, just densely
    repacked). Note this is the same failure mode as the A3 chase variant, and
    it has the same resolution — the frozen set must be held by rescue-pin.
  - **Dedup is poor** — repacking produces fresh content every time, so the
    previous-manifest dedup map never hits. Best for one-off/full backups;
    B-share is better for periodic incrementals.

#### Recommendation

- **Ship A1 and A2 now, independently of the option choice.** A1 is small and
  removes a real operational hazard that already exists for tablet streaming,
  not just backup. A2 bounds the blast radius.
- **Make A3 (rescue-pin) the stage 0 / Phase 1 target** rather than whole-tablet
  pinning. It is what actually makes Option A safe, it provides the strong
  invariant so backup and streaming share one mechanism, and it is less work
  than any Option B variant. A4 survives only as rescue-pin's budget escalation
  step.
- **Adopt B-share as the Phase 2 / Option B design**, with the "link dense files
  as-is, rewrite only the sparse remainder" rule as a first-class part of it —
  that rule is the difference between viable incremental backup and rewriting
  everything every time.
- **Keep Option C in reserve** for one-off full backups where disk headroom is
  tight and dedup does not matter.

### Backup flow (tablet-aware)

Mirror `cluster_backup_task`, branching for logstor.

**Coordinator (`cluster_backup_task::do_backup`, shard 0):**

1. Reuse the existing per-DC/rack/tablet ownership selection. For each tablet,
   pick **one replica** to back up (Option R3). Logstor segments are
   per-replica physical artifacts with independent layouts/seqs, so there is
   no cross-replica sstable-id dedup. Note this is **weaker** than the sstable
   path's repaired/unrepaired dedup, which additionally includes every
   replica's unrepaired sstables: the logstor backup is one replica's view,
   and writes that replica missed are absent. See "Replica selection and the
   repaired/unrepaired question" for the analysis and for the stage 6 plan
   (Option R2) that restores the union guarantee.
2. Fan out the existing `backup_snapshot_sstables` RPC
   (`idl/snapshot_backup.idl.hh:20`) to the chosen node per tablet, carrying
   `{table_id, tablet_id, token_range, endpoint, bucket, prefix, snapshot_tag}`.
3. Collect the reported segment sets and write a per-destination
   `manifest.json` (extended schema below) via `make_upload_sink`.

**Receiving node — add a logstor branch in `db::snapshot::backup_sstables`
(`cluster_backup.cc:386`):**

1. `auto segs = co_await table.take_logstor_snapshot(req.token_range);`
   (Option A). This flushes the separator, so every segment is a per-table full
   segment tagged with `table_id` + token range.
2. Upload the segments, packed into tablet-scoped pack objects (the default;
   `pack_size = segment_size` degenerates to one object per segment):
   - Load the previous backup's manifest for the table (if any) and build the
     `content_hash → {object_key, offset, length}` dedup map.
   - For each `segment_snapshot`: read it and compute the content hash. On a
     dedup-map hit, **re-reference** the existing object in the metadata and
     upload nothing. On a miss, append the segment's bytes to the current
     `make_upload_sink(pack_object)`, recording its running `offset`/`length`,
     and roll to a new pack when `pack_size` is reached.
   - Either way, throttled by the existing `backup_scheduling_group` /
     `backup_io_throughput_mb_per_sec` and dir/s3 semaphores; write the `refs/…`
     marker per pack and record per-segment metadata (`{object_key, offset,
     length, segment_seq, content_hash, first_token, last_token}`) in
     `system_distributed`.
3. Release the snapshot (drop `seg_ref`s), same teardown as streaming.

Non-logstor tables keep the existing SSTable code path unchanged.

### Restore flow (tablet-aware)

Mirror `sstables_loader::restore_tablets` / `download_tablet_sstables`,
branching for logstor.

1. `restore_tablets`: download manifest(s), populate `system_distributed`
   segment metadata, set tablet count to match the manifest
   (`alter_table_with_tablet_hints`), then drive
   `storage_service::restore_tablets` → topology coordinator issues per-tablet
   `restore_tablet` RPCs (orchestration unchanged).
2. **Per-tablet download on the owning node — logstor branch in
   `download_tablet_sstables` (`sstables_loader.cc:959`):**
   - Look up segment metadata rows whose token range overlaps the tablet; each
     row carries `{object_key, offset, length}`.
   - Group the segment entries by `object_key` and download each pack **once**
     with `make_download_source(object_name)` (whole-pack GET), splitting it at
     the recorded `offset`/`length` boundaries: for each contained segment,
     `create_logstor_segment_sink(db)` → pipe that segment's bytes through the
     sink's `output()` stream → `close()`. A manifest may reference packs from
     earlier backups (re-referenced segments) — they are ordinary object keys;
     skip pack byte ranges not referenced by this tablet. Ranged GETs per
     segment are an optional later optimization for sparse partial restores.
     The `streamed_segment_rewriter` reassigns the local seq and
     `load_segment` re-indexes into the table's
     `primary_index` and reattaches to the compaction group — exactly the
     existing tablet-stream receive path (`stream_blob.cc:218`).
   - The segment header's `table_id`/token range route it to the right
     shard/tablet (reuse the streaming routing).
   - Update per-segment download status in metadata.
3. Other replicas of each restored tablet are populated via the normal
   post-restore tablet replication/rebuild, same as the SSTable path.

### Metadata and manifest schema

The manifest (`db/snapshot/manifest.hh`) and `system_distributed` snapshot
tables are SSTable-centric (`sstable_id`, `toc_name`). Generalize rather than
fork:

- Add a **storage-unit kind** discriminator (`sstable` | `logstor_segment`).
- Extend `manifest_json` with a `segments` chunked list:
  `segment_info{ object_key, offset, length, table_id, tablet_id, first_token,
  last_token, data_size, segment_seq, content_hash }`. The `object_key` +
  `offset` + `length` triple locates the segment bytes within an object and
  supports **both** packaging options: Option 1 always sets `offset=0` and
  `length=segment_size`; Option 2 points into a pack object. Several fields
  already exist on `sstable_info` (`id`, `first_token`, `last_token`,
  `tablet_id`, `data_size`); the new ones are `offset`, `length`, `segment_seq`,
  `content_hash`, and kind.
- Extend the `system_distributed` snapshot metadata (`db/snapshot_types.hh`,
  `snapshot_sstable_entry`) with the same optional fields + kind, so
  `restore_tablets` can locate and range-filter segments the same way it does
  SSTables. `db::snapshot_table_helper`
  (`db/system_distributed_keyspace.cc`) gains read/write for the segment rows.

The same schema serves the degenerate per-segment case (`offset=0`,
`length=segment_size`), re-referenced segments (the `object_key` points into a
pack from an earlier backup), and the later `segment_seq`-watermark read skip —
no format change is needed for any of them.

## Change list (files / seams)

**Reuse unchanged:** `object_storage_client`
(`upload_file`/`make_upload_sink`/`make_download_source`/`object_exists`),
`storage_manager::get_endpoint_client`, task-manager modules (`snapshot`,
`sstables_loader`), REST endpoints `/storage_service/tablets/backup` &
`/tablets/restore`, topology-coordinator restore orchestration,
`backup_snapshot_sstables` / `restore_tablet` RPCs, `take_logstor_snapshot`,
`create_logstor_segment_sink`.

**Modify:**

- `replica/database.cc` (`truncate_table_on_all_shards`) — **stage 0a**: skip the
  automatic snapshot for logstor tables with a warning, instead of silently
  producing an empty one. Independent of everything else here.
- `db/snapshot-ctl.cc` — **stage 0a**: reject an explicitly named logstor table in
  `take_column_family_snapshot`; warn and skip logstor tables in keyspace/cluster
  wide snapshots.
- repair entry points (user-requested repair and the tablet repair scheduler) —
  **stage 0b**: fail a repair that names a logstor table; warn and skip logstor
  tables in keyspace-wide repairs; do not advance
  `repair_time`/`sstables_repaired_at` for skipped tables. See "Existing defect
  — tablet repair is silently a no-op for logstor".
- `db/snapshot/cluster_backup.cc` — logstor branch in the receiving-node
  `backup_sstables`; coordinator per-tablet single-replica selection for
  logstor; manifest emission includes segments; the pack-assembly loop (append
  segments into one `make_upload_sink`, track offsets) and the
  previous-manifest dedup map.
- `sstables_loader.cc` — logstor branch in `download_tablet_sstables` and
  manifest-population (`process_manifest` /
  `populate_snapshot_sstables_from_manifests`) to read segment entries; group
  entries by pack object and split whole-pack downloads by `offset`/`length`.
- `db/snapshot/manifest.hh` / `.cc` — add `segment_info` list (with
  `object_key`/`offset`/`length`) + kind.
- `db/snapshot_types.hh`, `db/system_distributed_keyspace.{hh,cc}` — segment
  metadata rows (incl. `offset`/`length`).
- `replica/logstor/segment_manager.{hh,cc}` — expose a content hash for a
  segment snapshot (compute during `make_snapshot` or via a helper); optionally
  an object-friendly stable id. Also: **A1** pin-aware candidate selection in
  `select_segments_for_compaction`; **A2** pinned-segment admission semaphore;
  **A3** cursor-based `make_snapshot` plus the rescue-pin eviction hook in
  `do_compaction` / `do_split_compaction`.
- `sstables/object_storage_client.{hh,cc}` — **no changes needed** for the
  default whole-pack-GET-and-split strategy. Only the optional ranged-GET
  optimization needs a `make_download_source` variant (or `make_readable_file`
  + positioned read) that accepts a byte range.
- Optionally a thin `table::` helper to restore-ingest a single downloaded
  segment stream (wrap `create_logstor_segment_sink` + copy), to keep
  `sstables_loader.cc` clean.
- A `pack_size` backup parameter (default e.g. 32 MiB; `segment_size`
  degenerates to one object per segment) threaded through the REST/RPC backup
  request.

**Add (only if Option B is chosen):** for **B-share**, a file-granular segment
allocator (reserve a whole file's contiguous id run), a "dense file" predicate
and the snapshot-compaction that rewrites only the sparse remainder, hard-link
creation under `logstor/snapshots/<tag>/` with links named after the source
file, file-granular no-reuse pinning, snapshot lifecycle/GC, and recovery-time
pin reconstruction from the snapshot directory. For **B-copy**, a snapshot-file
writer (whole files, never one file per segment) plus the same lifecycle/GC.
Either way, wire into `snapshot_ctl::take_snapshot`.

**Add (only if Option C is chosen):** a `compaction_buffer` output sink that
targets `make_upload_sink` instead of a `writeable_segment`, with the index
update suppressed.

**Add (stage 6, Option R2):** a node-local watermark table
(`{table, tablet, round-id, seq_watermark}`) written at tablet-repair round
start and validated at backup time against the tablet map's last completed
round; a delta mode in the backup request so a non-master replica enumerates
only `seq > W_r` segments (a seq filter on the snapshot cursor, plus cheap
segment-seq visibility shared with the stage 4 read skip); coordinator
master/delta fan-out per tablet. Requires logstor tablet repair (full repair
suffices) to exist first.

**Docs:** this file; cross-link from `docs/dev/logstor.md`.

## Immediate fix — make the `auto_snapshot` no-op explicit

This is independent of stage 0 and of every backup stage: no new mechanism, no
dependency on any other work here, shippable on its own. It fixes the
[existing defect](#existing-defect--auto_snapshot-is-silently-a-no-op-for-logstor)
described above. Two steps, the first of which should land regardless of whether
the second ever does.

### Step 1 (first, small) — warn and skip instead of pretending

Add a third guard in `truncate_table_on_all_shards`
(`replica/database.cc:3110-3116`), alongside the existing `auto_snapshot` and
`is_local_type()` ones and following the same shape:

```cpp
if (with_snapshot && table_shards->uses_logstor()) {
    dblog.warn("Not snapshotting dropped/truncated table {}.{} despite auto_snapshot=true"
               " - logstor tables do not support snapshots; back up the table first",
               s->ks_name(), s->cf_name());
    with_snapshot = false;
}
```

Why skip rather than let the empty snapshot be created:

- No snapshot directory is left behind, so `nodetool listsnapshots` cannot show
  an entry that protects nothing.
- The operator gets the same signal, in the same log line format, as the already
  handled object-storage case.
- The `DROP TABLE` path inherits it automatically, since it routes through the
  same function (`replica/database.cc:1416-1419`).

Two consequences to check when implementing:

- `with_snapshot` also drives `should_flush` (`replica/database.cc:3156`), so
  the table takes `cf.clear()` instead of `cf.flush()`. That is the intended and
  cheaper behavior — there is nothing to preserve — but confirm `clear()` is
  correct for a logstor table's memtables. The unconditional
  `cf.flush_separator()` (`:3182`) is unaffected.
- `truncate_request_timeout` is documented as being long specifically to allow
  the auto snapshot to complete (`db/config.cc:1138`); no change is needed, but
  logstor truncates will now be faster than the timeout suggests.

Also decide the behavior of an **explicit** `nodetool snapshot` on a logstor
table, which today succeeds and produces an equally empty directory:

- A snapshot of an explicitly named logstor table should **fail** with a clear
  error rather than silently succeed (`snapshot_ctl::take_column_family_snapshot`,
  `db/snapshot-ctl.cc:143`).
- A keyspace-wide or cluster-wide snapshot that happens to include logstor
  tables should **warn and skip** those tables rather than fail the whole
  operation, so mixed-engine keyspaces stay usable.

### Step 2 (optional, later) — make the snapshot real

Turning the warning back into an actual protective snapshot requires a genuine
on-disk artifact, i.e. **Option B** (`B-copy` at minimum, `B-share` preferred),
plus wiring it into `snapshot_ctl::take_snapshot` and giving
`snapshot_table_on_all_shards` a logstor branch that enumerates tablets from the
tablet map instead of from the (empty) sstable sets — which stage 1a needs
anyway.

**Phase 1 does not deliver this as a side effect, and should not be described as
if it does.** It is tempting to assume that once the Option A upload loop exists,
a local snapshot is just a matter of pointing the pack-assembly loop at a local
file instead of `make_upload_sink`. The *plumbing* is indeed close to that — the
manifest format is identical, `local_snapshot_writer` already handles the
directory and `schema.cql`, and restore-from-file is the stage 2 split loop with
a file source instead of a download source. But what that produces is **B-copy**,
with B-copy's properties, not `nodetool snapshot`'s:

- **The cost profile inverts.** An SSTable snapshot is a few thousand `link()`
  calls and approximately zero bytes; a copy-based logstor snapshot writes 1× the
  tablet's segments every time. The operator expectation that snapshots are
  cheap enough to take liberally — the expectation `auto_snapshot` itself relies
  on — no longer holds. Anything shipped under the name `nodetool snapshot` with
  that cost needs to be documented loudly.
- **The bytes need a home in logstor's accounting.** Capacity is a fixed segment
  count derived from `logstor_disk_size_in_mb` (`_max_segments`,
  `segment_manager.cc:1329`), and logstor sizes itself as though it owns its
  space. Snapshot files written as ordinary files under
  `logstor/snapshots/<tag>/` are invisible to that model and can exhaust
  filesystem space while `available_segment_count()` still looks healthy.
  Writing them into logstor-allocated segments instead avoids that but requires
  the file-granular allocator and no-reuse pinning — that is B-share, not a sink
  swap.
- **`listsnapshots` accounting degrades.** The `number_of_links == 1` test that
  distinguishes a snapshot's exclusive size from its total (`replica/table.cc:4364`)
  is meaningless for copies: `live == total` always, so every snapshot costs its
  full size.
- **It cannot feed the backup.** Copying produces fresh bytes and therefore fresh
  content hashes, so the previous-manifest dedup map never hits — the same
  failure the B-share "link dense files as-is" rule exists to avoid. A
  copy-based snapshot and the backup path would remain two independent readers of
  the data, not one feeding the other.
- **It does not avoid stage 0.** Draining the cursor to a local file still runs
  concurrently with compaction, so rescue-pin is still required. The exposure
  window is shorter than a throttled object-storage upload; the mechanism is
  identical.

So step 2 should be scheduled as a deliberate Option B decision — driven
primarily by whether restore **without** an object store is a requirement — and
not as an increment on Phase 1.

## Stage 0 implementation plan — pin-cost containment, streaming first

> **The authoritative, code-level plan for stage 0 now lives in
> [logstor_segment_snapshots.md](logstor_segment_snapshots.md).** It supersedes the
> commit breakdown below, which is kept here as the summary of *why* backup depends
> on stage 0. Where the two differ, the dedicated document is correct — notably: it
> splits the work into seven commits rather than six, drops the compaction-output
> plumbing (0.2 below) because only the rejected chase variant needs it, and adds a
> commit for the group-teardown abort.

**A1 and A3 are not backup features.** They fix a pin-cost problem that logstor
has *today*, in tablet streaming: `tablet_stream_files_handler`
(`streaming/stream_blob.cc:770`) calls `take_logstor_snapshot(req.range)` and
holds a pin on **every** segment of the tablet for the entire transfer, while
compaction happily selects those same pinned segments, rewrites them, and
reclaims nothing (`segment_manager.cc:2198`).

Stage 0 therefore lands A1 and A3 (rescue-pin) as standalone logstor changes with
tablet streaming as the first consumer, **before** any backup code exists.
Because rescue-pin provides the strong invariant — *every record live at snapshot
time is consumed at least once* — the machinery stage 0 builds is the same
machinery backup needs, with no semantic gap to close later.

Six landable commits.

**0.1 — A1: pin-aware compaction candidacy.** Add `external_ref_count` to
`segment_descriptor` (`compaction.hh:56`), incremented only by snapshot pins —
not by group membership and not by `writeable_segment`. In
`select_segments_for_compaction` (`segment_manager.cc:1964`) skip descriptors
with `external_ref_count > 0`. Independently valuable and independently testable
against today's whole-tablet pinning.

**0.2 — Track compaction output segment ids.** `write_full_segment` (`:1571`)
returns its `log_segment_id`; `compaction_buffer` (`:2077`) accumulates
`std::vector<log_segment_id> output_segments`; likewise for split compaction's
two buffers. Pure plumbing, no behavior change. **Only needed for the optional
chase variant** — rescue-pin does not look at compaction's outputs — so this
commit can be dropped or deferred if the chase is never implemented.

**0.3 — Shard-level pending-segment registry + snapshot cursor.** Key the
registry **by segment id on `segment_manager_impl`**, not per group:

```cpp
absl::flat_hash_map<log_segment_id, small_vector<segment_snapshot_set*, 1>> _pending_segments;
```

Group-keyed registration would break on split compaction's fast path
(`:2242-2248`), where a segment is *moved* to a child group without being
rewritten: the snapshot would stop tracking it and miss a later compaction in
the child group. Id-keying makes group membership — and group lifetime —
irrelevant.

New type in `segment_manager.hh`:

```cpp
class segment_snapshot_set {
public:
    // Pins and returns the next pending segment; nullopt when exhausted.
    future<std::optional<segment_snapshot>> next();
    size_t pending_count() const noexcept;
    size_t rescued_count() const noexcept;
    ~segment_snapshot_set();   // unregisters, drops all pins
};
```

- *Construction* (replacing today's `make_snapshot` body, `:2654`): under the
  existing `disable_compaction(cg)` guard, walk `_segment_list` recording **ids
  only**, no pins, and register them.
- *`next()`*: hand out rescued segments first (their bytes are pure liability),
  otherwise pop the front pending id and `make_segment_ref` it. In the steady
  state the caller holds the only pin, so pinned bytes are one segment (128 KiB)
  instead of a whole tablet.
- *Eviction hook* `on_segments_evicted(inputs)`, called from `do_compaction`
  (`:2192`) and `do_split_compaction`'s slow path (`:2300`), **before** the
  inputs are freed and after `cb.close()`. For each input still pending in some
  snapshot, that snapshot calls `make_segment_ref` on it and marks it *rescued*.
  Because `remove_segment` decrements to 1 rather than 0 (`compaction.hh:205`),
  the segment is detached from its group but stays allocated with its bytes
  intact; the loop at `:2192` is synchronous, so the hook only has to run in the
  same continuation ahead of it.
- *Rescue budget*: past a configured `B` (e.g. 1% of `_max_segments`), stop
  rescuing and hold `disable_compaction(cg)` for that group until the consumer
  drains back under `B`; past a hard cap, abort the operation. Do **not** simply
  stop rescuing — that silently drops records.
- *Split compaction's fast path* (`:2242-2248`) moves a segment between groups
  without freeing it, so it needs no hook at all; id-keying keeps the snapshot
  tracking it.
- *Optional chase variant*: a `segment_snapshot_set` policy flag that, instead of
  rescuing, substitutes compaction's output ids (from 0.2) into pending. Weaker
  invariant, streaming-only, needs its own growth budget — see the A3 variant
  discussion. Not required by anything in this plan.

Keep the existing `make_snapshot(cg) -> chunked_vector<segment_snapshot>` as a
thin wrapper that drains a cursor, so the existing unit tests
(`test/boost/logstor_test.cc:971`, `:1160`, `:1277`) keep working unchanged.

**0.4 — Cursor over a token range.**
`table::make_logstor_snapshot_cursor(dht::token_range)` alongside
`take_logstor_snapshot` (`replica/table.cc:1641`): flush the range's storage
groups' separators, then build one cursor spanning them. Because the registry is
id-keyed the cursor does not need the storage groups to stay alive, but it
should hold the async gate / `get_sstable_list_permit` for the whole iteration
rather than only during construction.

**0.5 — Producer-based `tablet_stream_files`.** The sender currently takes a
fixed `std::list<stream_blob_info>` and loops over it (`stream_blob.cc:529`,
`:560`). Add an overload taking

```cpp
using stream_blob_producer = noncopyable_function<future<std::optional<stream_blob_info>>()>;
```

and reimplement the list version as a wrapper over it. The logstor branch in
`tablet_stream_files_handler` (`:770-782`) builds a producer that pulls from the
cursor; the SSTable branch keeps the list overload with zero behavior change.
The wire protocol needs no change — sinks are created per file (`:588`) and no
global file count is sent; only logging and `files_nr` reporting move to a
running counter.

**0.6 — Metrics, injection points, tests.**

- Metrics: `pinned_segments` / `pinned_bytes` gauges,
  `snapshot_rescued_segments` counter, `snapshot_pending_segments` gauge.
- Unit test in `logstor_test.cc`: open a cursor, force compaction of
  still-pending segments (including with keys overwritten mid-run), drain the
  cursor, and assert the strong invariant — **every** record live at snapshot
  time appears, with no exception for overwritten keys. Also assert the rescue
  budget escalates to `disable_compaction` rather than dropping records.
- Topology test: migrate a logstor tablet while forcing source-side compaction,
  using the existing `wait_before_tablet_stream_files_after_snapshot` and
  `logstor_compaction_wait_before_remove_segments` injection points. Verify
  destination data matches and that source `free_segments` does not fall
  monotonically for the transfer's duration.

**Verify before starting:**

- `discard_segments` hard-asserts `ref_count == 1` (`:1799`). Whole-tablet
  pinning has the same exposure today, so tablet cleanup is presumably
  serialized against streaming upstream — confirm rather than assume, since the
  cursor changes the shape of the window. Group teardown is the one eviction
  path rescue-pin cannot answer: the data is being deliberately destroyed, so
  an active snapshot must be aborted or serialized against, not rescued.
- Decide whether `external_ref_count` should also suppress that assertion rather
  than tripping an internal error.
- Confirm a rescued (group-detached, pinned) segment surviving a crash is benign
  for recovery: its records are duplicated in the compaction output with a higher
  seq, and `add_segment_to_compaction_group` (`:2591`) re-attaches it as a full
  segment with all-dead records, which compaction then frees. This is the same
  shape as the deliberate leak on the `set_flush_failure` path (`:1601`), so it
  should be a no-op — verify.

**Not in stage 0:** A2 (the admission semaphore is a backup-fan-out concern; the
cursor already bounds streaming), A5, and the optional chase variant. A4 appears
only as the rescue budget's escalation step, not as a standalone stopgap.

## Phase 1 implementation plan (stages 1–3)

This section is the authoritative, code-level plan for the first deliverable:
a **working** tablet-aware backup-and-restore for logstor using **Option 2**
(tablet-scoped pack objects with whole-pack-GET restore;
`pack_size = segment_size` degenerates to Option 1 for bring-up and tests) and
**Option A** (in-memory pinned snapshot, no on-disk
snapshot dir). It **assumes stage 0 has landed**, so A1 and the A3 rescue-pin
snapshot cursor already exist and provide the strong invariant backup needs;
Phase 1 adds A2 and A5 on top. Naive whole-tablet pinning is not safe to ship.
The ranged-GET restore optimization, the `segment_seq`-watermark incremental
read skip, and the on-disk snapshot (Option B) are deliberately deferred; the
metadata carries `segment_seq`/`offset`/`length` so all of them can be added
later without a format change.

### Phase 1 scope and non-goals

In scope: cluster/tablet-aware backup of a logstor table to object storage, and
tablet-aware restore into a (possibly fresh) cluster; pack objects with
whole-pack restore; per-segment manifest-based dedup; pin-cost
containment (A2 and A5 on top of stage 0's A1 + rescue-pin); metrics; tests.
Out of scope for Phase 1:
ranged-GET restore, the `segment_seq`-watermark incremental read skip,
on-disk snapshots, the cross-replica union guarantee (the repair seq-watermark,
Option R2 — stage 6), repair-aware
grace, cross-DC per-tablet mapping differences, and non-tablet (vnode) logstor
tables (logstor requires tablets, see `service/storage_service.cc:5418`).

### Key architectural decision: fuse snapshot+upload on the node

The SSTable cluster flow is **two-phase**: `snapshot_keyspace` persists the unit
set (hard links + `system_distributed` rows) at snapshot time, and
`cluster_backup_task` uploads later — possibly much later, even after a restart —
because hard links keep the exact files alive. Option A has **no** durable
artifact, so a logstor segment enumerated at snapshot time may be compacted away
before upload.

Phase 1 therefore **fuses** enumeration and upload on the owning node: the
segment set is materialized (via `take_logstor_snapshot`, which pins segments)
**inside** the node-side backup RPC handler and uploaded immediately, then the
pins are released. The coordinator no longer pre-selects per-segment ids; it
selects, per tablet, one replica to back up and hands it the tablet's token
range. Consequences, to be documented as **known limitations**:

- No "snapshot now, back up later" for logstor (the backup request does the
  snapshot). `snapshot_keyspace` on a logstor table only records
  keyspace/table/tablet/node metadata, not segments.
- Point-in-time consistency is **per tablet at its upload moment**, not a global
  instant. For key-value, single-version-per-key data this preserves logical
  correctness (each key's latest value wins by timestamp on restore), matching
  how the SSTable flow's per-tablet repaired sets are also not a global instant.
- Backup includes freed-but-physical records inside live segments (dead-data
  write amplification); source-side compaction bounds it. Option B later gives
  true snapshot-time materialization.

### Segment metadata and hashing (stage 1 foundation)

Add the ability to describe a segment without a second disk pass:

- **Read once into memory.** Each full segment is 128 KiB. The node reads the
  whole segment from the `segment_snapshot.source` `input_stream` into a
  `temporary_buffer`, then: (a) parses the on-disk header
  (`replica/logstor/segment_io.hh:read_segment_info` / `ondisk.hh`) to obtain
  `segment_seq`, `table`, `first_token`, `last_token`; (b) computes a
  `content_hash` (xxhash/CRC32) over the buffer; (c) checks the hash against
  the previous-manifest dedup map — on a hit, re-references the old object and
  uploads nothing; on a miss, appends the buffer to the current pack's
  `make_upload_sink`, recording `offset`/`length`. Hashing before appending is
  what lets a re-referenced segment cost one 128 KiB read and nothing else.
  Peak memory ≈ `concurrency × 128 KiB` plus the in-flight pack sink buffers,
  bounded by `sstables_manager::dir_semaphore()` as the existing SSTable path
  does (`cluster_backup.cc:504`).
- **Object key** (`sstables::object_name`, mirroring `cluster_backup.cc:491`):
  `{prefix}/{table_id}/{tablet_id}/pack-{tag}-{pack_seq}` and a
  `refs/snapshot-{tag}/{table_id}/{tablet_id}/pack-{pack_seq}` marker. Pack
  keys are not content-addressed (the key must exist before the sink opens;
  dedup is per-segment via the manifest), and a retried backup of the same tag
  overwrites the same keys idempotently.
- Optional later optimization: reuse the per-buffer CRC already in
  `ondisk::chunk_header` instead of a second hash pass — verify it covers the
  full segment payload before relying on it.

### Metadata schema (stage 1)

Reuse the existing `snapshot_sstables` machinery by representing each segment as
a pseudo-sstable entry, so the range-query, progress and download-status helpers
work unchanged. Concretely, when the node records an uploaded segment it fills a
`db::snapshot_sstable_entry` (`db/snapshot_types.hh:39`) as:

- `sstable_id` = a UUID **derived deterministically from `content_hash`** (stable
  identity across nodes/backups → enables dedup and restore matching). Reserve
  a fixed marker in the derived UUID (e.g. a fixed version/variant nibble
  pattern) so a segment row can never collide with or be mistaken for a real
  sstable id even if the `kind` column is lost or unread;
- `first_token` / `last_token` = from the segment header;
- `toc_name` = the object key basename (`pack-{tag}-{pack_seq}`); `prefix` = the
  object prefix — together with `offset`/`length` they locate the segment bytes
  for the logstor download path;
- `data_size` = segment length (`index_size` = 0 for Phase 1);
- `tablet_id`, `node`, `state`, `downloaded` = as today.

Add two fields to `snapshot_sstable_entry` and its serializer, the manifest
`sstable_info`/`segment_info`, and the `snapshot_sstables` CQL schema
(`db/system_distributed_keyspace.cc`): **`segment_seq`** (int64) and
**`offset`/`length`** (int64) locating the segment within its pack object
(`offset=0`/`length=data_size` in the degenerate per-segment case). A `kind` discriminator
(`sstable`|`logstor_segment`) distinguishes rows; for Phase 1 the restore side
also branches on `table.uses_logstor()`, so `kind` is informational but should be
stored for forward-compatibility.

Alternative considered: a dedicated `snapshot_logstor_segments` table +
`snapshot_segment_entry` + parallel helpers. Cleaner separation but ~3× the
metadata plumbing; **deferred** — the pseudo-sstable reuse is the Phase 1 choice.

### Stage 1 — Backup

**1a. Snapshot-time metadata (logstor branch in
`database::snapshot_table_on_all_shards`, `replica/table.cc:4130`).**
Today the `ssc` callback is invoked only when `tablets` is non-empty, and
`tablets` is derived by iterating **sstables** (`table.cc:4185`) — so for a
logstor table (no sstables) no metadata is written at all. Add: when
`t.uses_logstor()`, enumerate tablets directly from the tablet map
(`erm->get_token_metadata().tablets().get_tablet_map(s->id())`, as at
`table.cc:4182`) to build `snapshot_tablet_entry`s, and invoke `ssc` with
`snapshot_entries{ .sstables = {}, .tablets = <all tablets> }`. This makes
`insert_snapshot_entries` (`db/system_distributed_keyspace.cc:1160`) write the
node + tablet rows the coordinator needs. The local snapshot dir may be created
empty or skipped (the `local_snapshot_writer` already handles an empty sstable
set). Populate each tablet entry's `repair_time`/`repaired_at` from
`tablet_info` exactly as the sstable path does (`table.cc:4219-4221`) — free,
and it records the per-tablet consistency point in metadata and manifest
(see Option R3).

**1b. Coordinator (logstor branch in `cluster_backup_task::do_backup`,
`db/snapshot/cluster_backup.cc:117`).** Detect logstor via the table schema.
Replace the sstable-selection/repaired-dedup block (`cluster_backup.cc:207-308`)
with: read `get_snapshot_tablets` and `get_snapshot_nodes`; for each tablet pick
**one** replica node deterministically (Phase 1: the tablet's first/primary
replica from the tablet map), and send it the backup RPC with the tablet's
`first_token`/`last_token` and an **empty** `sstable_ids`. Manifest generation
(`cluster_backup.cc:321-363`) then reads the segment rows the nodes wrote
(`get_snapshot_sstables`) and emits them under a `sstables`/`segments` list plus
the existing `tablets`/`nodes` lists.

**1c. RPC.** Reuse the existing verb
`backup_snapshot_sstables` (`idl/snapshot_backup.idl.hh:20`) unchanged: it
already carries `table_id, tag, endpoint, bucket, prefix, first_token,
last_token, sstable_ids, use_move`. For logstor the coordinator passes the
tablet range and empty `sstable_ids`; the handler branches on
`table.uses_logstor()`. (A dedicated verb is optional cleanup, not required.)

**1d. Receiving node (logstor branch in `db::snapshot::backup_sstables`,
`cluster_backup.cc:386`).** When `cf.uses_logstor()`:

0. Acquire the **A2** pinned-segment admission semaphore, so a node never has
   more than a configured fraction of `_max_segments` pinned across concurrent
   tablet backups. Size the acquisition against the stage 0 **rescue budget plus
   in-flight segments**, *not* against the tablet's segment count: with the
   cursor, steady-state pinning is one segment per tablet backup, so reserving a
   tablet's worth would over-reserve massively and defeat the point of A3.
1. `auto cursor = co_await cf.make_logstor_snapshot_cursor(
   dht::token_range{first_token, last_token});` — flushes the separator (all
   segments become full, range-tagged) and enumerates segment ids without
   pinning them. Rescue-pin (stage 0) guarantees no record live at this instant
   is lost even though the segments are unpinned.
2. Load the previous backup's manifest for this table from the bucket (if any)
   and build the `content_hash → {object_key, offset, length}` dedup map.
3. Drain the cursor (the snapshot is taken on the owning shard; read and hash
   ahead of the pack upload, bounded by `dir_semaphore()`). For each segment:
   read fully into memory, parse header, compute `content_hash`; on a dedup-map
   hit, record the re-referenced `{object_key, offset, length}`; on a miss,
   append the buffer to the current pack's `make_upload_sink`
   (`pack-{tag}-{pack_seq}`), recording the running `offset`/`length`, and roll
   to a new pack at `pack_size`; either way write the `snapshot_sstable_entry`
   (pseudo-segment) via `insert_snapshot_sstables`; then drop that segment's
   pin. Write the `refs/…` marker per completed pack.
4. Destroy the cursor (unregisters and drops any rescued pins) and release the
   admission semaphore.

Reuses the existing per-endpoint client (`get_endpoint_client`), abort wiring,
and `backup_scheduling_group` / `backup_io_throughput_mb_per_sec` throttling.

Independently of the above, apply **A1** in
`select_segments_for_compaction` so a pinned segment is never selected as a
compaction candidate (or is scored as reclaiming nothing). Without it, a backup
under space pressure drives a compaction storm that reclaims zero segments while
consuming new ones.

### Stage 2 — Restore

**2a. Manifest population (logstor branch in `process_manifest`,
`sstables_loader.cc:1134`).** Parse the segment entries (with `segment_seq`,
`offset`, `length`, `kind`) and `insert_snapshot_sstable` them as pseudo-sstable
rows (state `remote`), exactly like the current sstable loop but carrying the
extra fields. `restore_tablets` (`sstables_loader.cc:1366`),
`tablet_restore_task_impl`, `alter_table_with_tablet_hints`, the topology
coordinator restore transition, and the `restore_tablet` RPC
(`sstables_loader.cc:904`) are **engine-agnostic and unchanged**.

**2b. Per-tablet download (logstor branch in
`sstables_loader::download_tablet_sstables`, `sstables_loader.cc:959`).** The
range lookup (`get_snapshot_sstables` by tablet token range) and the
`downloaded == no` filtering are reused. When `table.uses_logstor()`, replace the
`get_sstables_from_object_store` + `download_sstable` + `attach_sstable` block
with a logstor ingest loop:

1. Determine the target shard from the transition info:
   `trinfo->pending_replica->shard` (as file-streaming restore does at
   `service/storage_service.cc:5426`).
2. Group the tablet's segment entries by object key (a manifest may reference
   packs from earlier backups — they are ordinary keys). For each pack, on that
   shard: open `client->make_download_source(object_name(bucket, prefix,
   toc_name))` once (whole-pack GET) and walk it in `offset` order; for each
   contained segment, create a sink via
   `table::create_logstor_segment_sink(db)` (`replica/table.cc:1654`) and copy
   that segment's `length` bytes into the sink's `output()` stream; `close()`
   runs `load_segment`, which re-indexes into
   the tablet's `primary_index` and reattaches the segment to its compaction
   group. Skip (read and discard) pack byte ranges not referenced by this
   tablet — re-referenced packs may be only partially live in this manifest.
   The `streamed_segment_rewriter` reassigns a fresh local `segment_seq`,
   so cross-node identity is irrelevant on restore.
3. `update_sstable_download_status(...)` per segment (reuses existing progress).

Add a thin `sstables_loader` helper `download_logstor_segment(...)` (the ingest
copy loop) analogous to `attach_sstable`, and optionally a
`table::restore_logstor_segment(input_stream)` wrapper to keep the loader clean.

**2c. Restore correctness.** Ingesting whole segments re-inserts every physical
record (including freed-but-physical copies); the `primary_index` keeps the
max-`(timestamp, seq, offset)` record per key — identical to normal recovery and
tablet streaming — so the latest value/tombstone wins. No special handling
needed for Phase 1.

### Stage 3 — Hardening

- Metrics: bytes/segments uploaded & downloaded, dedup hits/skips, pinned-segment
  count/bytes during backup, `available_segment_count` during backup, compaction
  segments-in/out and reclaimed during backup (to prove A1 works), per-tablet
  durations.
- Backpressure: cap concurrent in-memory segment buffers; enforce the A2
  pinned-segment admission semaphore; ensure pins are always released on error
  paths (RAII around the snapshot vector); implement A5 (abort the backup when
  available segments cross the soft-pressure threshold).
- Abort/retry: honor the task abort source through upload/download loops; make
  re-entered restore idempotent via the `downloaded` flag (already present).
- Docs: update this file and `docs/dev/object_storage.md` (manifest grammar) and
  cross-link from `docs/dev/logstor.md`.

### File-by-file change summary (Phase 1)

| File | Change |
|---|---|
| `replica/table.cc` (`snapshot_table_on_all_shards`) | logstor branch: enumerate tablets from the tablet map and invoke `ssc` with empty sstables + all tablets |
| `db/snapshot/cluster_backup.cc` (`do_backup`) | logstor branch: per-tablet single-replica selection; send RPC with token range + empty `sstable_ids`; manifest emits segment entries |
| `db/snapshot/cluster_backup.cc` (`backup_sstables`) | logstor branch: snapshot cursor, read-into-memory + hash, previous-manifest dedup, pack-assembly upload, metadata row; release pins |
| `db/snapshot_types.hh` | add `segment_seq`, `offset`, `length`, `kind` to `snapshot_sstable_entry` |
| `idl/snapshot_backup.idl.hh` / serializers | extend `snapshot_sstable_entry` serialization for the new fields (verb signature unchanged) |
| `db/system_distributed_keyspace.{hh,cc}` | add the new columns to the `snapshot_sstables` schema and to insert/read helpers |
| `db/snapshot/manifest.{hh,cc}` | add `segment_seq`/`offset`/`length`/`kind` to `sstable_info` (or a `segments` list) |
| `sstables_loader.cc` (`process_manifest`) | parse and persist the new segment fields |
| `sstables_loader.cc` (`download_tablet_sstables`) | logstor branch: group entries by pack, whole-pack `make_download_source` split by `offset`/`length` → `create_logstor_segment_sink` on `pending_replica->shard`; `download_logstor_segment` helper |
| `replica/logstor/segment_manager.{hh,cc}` | expose content hash + header fields for a `segment_snapshot` (or a read-into-memory helper); **A2** pinned-segment semaphore. **A1** and the **A3** rescue-pin cursor land in stage 0 |
| tests | `test/boost/tablet_aware_restore_test.cc` (logstor variant) + logstor unit tests |

### Phase 1 risks and unresolved issues

1. **Durability tension (resolved by fusing, but with caveats).** Because the
   segment set is materialized at upload time, an aborted/retried cluster backup
   re-snapshots and may produce a *different* segment set (compaction moved on).
   Dedup by `content_hash` limits re-upload, but the manifest reflects the last
   successful node run. Ensure the coordinator reads segment rows **after** each
   node finishes; make node backup idempotent.
2. **Which replica per tablet.** Phase 1 picks the primary replica. If
   replicas diverge, the restored copy is that replica's view — writes the
   replica missed are absent, which the sstable path's repaired/unrepaired
   dedup would have captured. This is a documented Phase 1 property, not a
   bug; the analysis and the stage 6 fix (Option R2, repair seq-watermark) are
   in "Replica selection and the repaired/unrepaired question". A
   repair-freshness gate is deliberately **not** part of Phase 1: tablet
   repair is currently a silent no-op for logstor (see "Existing defect"), so
   gating on `repair_time` would assert consistency that was never checked.
3. **Pin lifetime vs. disk pressure.** Whole-tablet pinning would be the most
   serious Phase 1 risk: because logstor's capacity is a fixed segment count and
   compaction's scoring is pin-blind, a backup could both consume ~4% of a
   shard's segment pool per tablet **and** drive a compaction storm that
   reclaims nothing while consuming more, with unbounded coordinator fan-out
   multiplying it into a write stall. Stage 0's **A1** + rescue-pin removes the
   storm and makes pinning demand-driven, but the worst case (compaction sweeps
   the whole tablet during the upload) still degrades to Option A's cost, so
   Phase 1 adds **A2** (pinned-segment admission semaphore) and **A5** (abort
   under space pressure), with **A4** as rescue-pin's budget escalation. Full
   analysis under "On-disk snapshot". Note Option B relocates this cost rather
   than removing it — it still needs A1.
4. **Metadata reuse hack.** Overloading `snapshot_sstable_entry`/`snapshot_sstables`
   for segments (synthetic `sstable_id`, object key in `toc_name`/`prefix`) is
   pragmatic but semantically muddy; revisit a dedicated table if it causes
   confusion or schema friction.
5. **Shard routing on restore.** Relying on `trinfo->pending_replica->shard`
   assumes a single owning shard per tablet replica (true for tablets). Verify
   for split/merge-in-progress tablets during restore; the topology coordinator
   should serialize restore against tablet migration, but confirm.
6. **Empty/whole-segment inclusion.** Backing up whole segments includes dead
   data and can inflate backup size unpredictably vs. logical data size; surface
   it in metrics and docs so operators aren't surprised. Trigger a source-side
   compaction/separator flush before large backups if needed.
7. **`content_hash` → `sstable_id` derivation.** Must be collision-resistant and
   deterministic; use a 128-bit hash mapped into the UUID, with a reserved
   marker (e.g. a fixed version/variant nibble pattern) so a segment row can
   never clash with or be mistaken for a real sstable id in mixed (non-logstor)
   manifests, even if the `kind` column is lost or unread.
8. **Consistency-level / TTL.** Segment rows reuse the 3-day `snapshot_sstables`
   TTL (`SNAPSHOT_SSTABLES_TTL_SECONDS`); confirm it's long enough for large
   restores and consistent with backup semantics.
9. **Interaction with concurrent compaction/separator during backup.** Pins
   prevent reuse but compaction still runs; confirm `take_logstor_snapshot`'s
   separator flush + pin set is race-free against an in-flight split compaction
   for the same tablet. The rescue-pin eviction hook must cover
   `do_split_compaction`'s slow path (`segment_manager.cc:2211`), not just
   `do_compaction`; its fast path frees nothing and needs no hook.
10. **Snapshot bookkeeping memory.** Each `segment_snapshot` costs roughly
    150–200 bytes (a `segment_ref` whose state holds two `std::function`s, plus
    the source closure): ~6–8 MB per 5 GiB tablet and ~80 MB for a 50 GiB
    whole-shard snapshot. A3 largely removes this by keeping only ids, but a
    rescue-heavy run reintroduces it in proportion to the rescued count, so give
    `segment_ref` an intrusive refcount instead of two heap `std::function`s.
11. **Dedup map source and backup GC.** The dedup map must come from the
    previous backup's manifest in the bucket, not from `system_distributed`
    rows (3-day `SNAPSHOT_SSTABLES_TTL_SECONDS`). Because re-referencing lets a
    newer manifest point into packs uploaded by older backups, backup
    deletion/GC must be reference-aware: a pack object may be deleted only when
    no retained manifest references it (the `refs/…` markers track this).
    A dedup-map load failure must degrade to "upload everything", never fail
    the backup.

## Testing

- **Unit:** segment content-hash stability; object-key generation; manifest
  round-trip with segment entries.
- **Component/integration:** back up a logstor table to a local/mock S3, restore
  into a fresh cluster, verify all keys/values (point lookups + full scan)
  match; verify token-range → tablet routing places segments on the right
  shards.
- **Dedup:** two consecutive backups with no compaction in between upload no
  segment bytes — every segment is re-referenced from the first backup's packs
  via the previous-manifest hash map — and restore from the second backup's
  manifest (which points into the first backup's packs) is correct; after
  forced compaction, only new-content segments are uploaded. Deleting the first
  backup while the second is retained must keep the still-referenced packs.
- **Packing:** verify pack objects contain the expected segments at the
  recorded offsets/lengths; that whole-pack restore reconstructs data identical
  to the degenerate `pack_size = segment_size` case; and that a pack never
  contains segments from more than one tablet.
- **Correctness under churn:** backup concurrent with writes/compaction (pins
  hold; no resurrection of freed records); restart-after-restore recovery scan
  rebuilds the identical index.
- **Pin cost / space pressure:** back up a large tablet while the shard is near
  the compaction trigger threshold and assert that (a) `available_segment_count`
  does not fall monotonically for the duration, (b) compaction does not run on
  the pinned group and reclaim zero (A1), (c) concurrent tablet backups on one
  shard never exceed the configured pinned-segment budget (A2), and (d) writes
  do not stall. Use the existing
  `logstor_compaction_wait_before_remove_segments` injection to widen the race
  window.
- **A3 rescue-pin correctness:** force compaction (including split compaction) of
  segments that are enumerated but not yet uploaded, with keys overwritten
  mid-backup, and verify the restored table contains **every** key that was live
  at snapshot time. This is the case where unpinned batching without rescue-pin
  silently loses data. Also verify that exceeding the rescue budget escalates to
  `disable_compaction` rather than dropping records, and that rescued segments
  are freed once uploaded.
- **`auto_snapshot` (stage 0a):** `TRUNCATE` and `DROP TABLE` on a logstor table
  with `auto_snapshot=true` must **not** leave a snapshot directory behind and
  must emit the warning; assert via `nodetool listsnapshots` /
  `get_snapshot_details` that no entry appears, and that the equivalent SSTable
  table in the same test still does produce a recoverable snapshot. Also cover a
  keyspace-wide `nodetool snapshot` over a keyspace holding both a logstor and an
  SSTable table: the SSTable table is snapshotted, the logstor table is skipped
  with a warning, and the call succeeds; snapshotting the logstor table by name
  fails with a clear error.
- **Stage 6 (R2) correctness (later):** diverge replicas (write at CL ONE with
  a replica isolated), run a tablet repair, write further divergent data after
  the round, back up with watermark dedup, restore, and verify the **union**:
  every acknowledged write is present regardless of which replica was master.
  Also verify the fallbacks — a replica without a usable watermark (fresh
  rebuild, intranode migration since the round) contributes everything; heavy
  compaction churn between repair and backup only increases upload volume,
  never loses data; and a failed repair round's watermark is never used.
- **Parity:** run the existing tablet backup/restore test suite against a
  logstor-engine table variant, for both packaging options.

## Staged rollout

Stage 0 is a **prerequisite that contains no backup code**, detailed in "Stage 0
implementation plan" above. Stages 1–3 are the **Phase 1** deliverable, detailed
at the code level in "Phase 1 implementation plan". Stages 4–6 are follow-ups.
The stage 0a/0b warnings below are ordered first but gated on nothing — they
can land in parallel with any of them.

- **Stage 0a — `auto_snapshot` warning (independent, ship first):** make the
  silent no-op explicit in `truncate_table_on_all_shards`, and decide explicit
  `nodetool snapshot` behavior for logstor tables (fail when named directly, warn
  and skip in a keyspace-wide snapshot). Detailed in "Immediate fix" above. No
  dependency on stage 0 or any backup stage; fixes a live behavioral regression
  against SSTable tables.
- **Stage 0b — repair no-op warning (independent):** make the tablet-repair
  silent no-op explicit, mirroring stage 0a: fail a repair that names a
  logstor table, warn and skip logstor tables in keyspace-wide repairs, and do
  not advance `repair_time`/`sstables_repaired_at` for skipped tables. See
  "Existing defect — tablet repair is silently a no-op for logstor".
  Implementing real logstor repair is out of scope for this document but
  assumed to land; stage 6 depends on it.
- **Stage 0 — Pin-cost containment in logstor, streaming first:** **A1**
  pin-aware compaction candidacy and **A3** the rescue-pin snapshot cursor,
  landed as standalone logstor changes with tablet streaming
  (`stream_blob.cc:770`) as the first consumer. Fixes a problem logstor has
  today and is independently shippable; because rescue-pin provides the strong
  invariant, the same machinery serves backup unchanged.
- **Stage 1 — Backup:** segment identity + hashing; snapshot-time tablet/node
  metadata for logstor; logstor branches in the coordinator `do_backup` and the
  receiving-node `backup_sstables` (Option A snapshot on the stage 0 cursor,
  tablet-scoped pack objects, previous-manifest per-segment dedup); **A2**
  pinned-segment admission and **A5**
  yield-under-pressure on top of stage 0; metadata schema **including
  `segment_seq`/`offset`/`length`**; metrics.
- **Stage 2 — Restore:** logstor branches in `process_manifest` and
  `download_tablet_sstables` (whole-pack download split by `offset`/`length` →
  `create_logstor_segment_sink`
  on the tablet's owning shard); end-to-end restore tests.
- **Stage 3 — Hardening:** dedup edge cases, throttling/backpressure tuning,
  abort/retry idempotency, docs.
- **Stage 4 (optional) — Incremental-backup optimizations:** the
  `segment_seq`-watermark read skip (skip reading segments already covered by
  the previous backup on a sticky replica — turns incremental backup IO into
  O(churn); see "Incremental enumeration"); ranged-GET restore for sparse
  partial restores.
- **Stage 5 (optional) — Option B on-disk snapshot** for `nodetool snapshot`
  parity / decoupled uploads, if needed. This is also what turns stage 0a's
  warning back into a real protective snapshot on DROP/TRUNCATE (step 2 of
  "Immediate fix"); Phase 1 does **not** provide that as a side effect.
  Preferred form is **B-share**:
  file-granular allocation, snapshot compaction that only rewrites the sparse
  remainder, hard links under `logstor/snapshots/<tag>/`, file-granular no-reuse
  pinning, and snapshot lifecycle/GC. Note that B-share composes naturally with
  packing: a 32 MiB snapshot file *is* a pack object.
- **Stage 6 (later) — Cross-replica union via the repair seq-watermark
  (Option R2):** once logstor tablet repair exists (full repair suffices),
  each replica records a per-tablet segment-seq watermark at repair-round
  start; backup then takes everything from the per-tablet master replica and
  only `seq > W_r` segments from the others, restoring the sstable path's
  "every write present on any replica" guarantee at churn-bounded extra cost.
  Comprises the node-local watermark table, the delta mode in the backup
  request, cheap segment-seq visibility (shared with stage 4), and the
  multi-source restore caveats. See "Replica selection and the
  repaired/unrepaired question".

## Risks / open items

- **`auto_snapshot` is a silent no-op for logstor today** (live defect, not a
  backup risk): `DROP`/`TRUNCATE` pass both existing guards, take an empty
  snapshot, and destroy the data with no warning, where an SSTable table would
  be recoverable. Stage 0a makes it explicit; only Option B makes it real. See
  "Existing defect" and "Immediate fix".
- **Object packaging:** resolved — packed (Option 2) by default with
  whole-pack-GET restore; per-segment (Option 1) survives as the degenerate
  `pack_size = segment_size` for bring-up and tests.
- **On-disk snapshot approach:** Option A (+A1–A5) vs Option B (`B-share` /
  `B-copy`) vs Option C (open; A with A1 + A3 rescue-pin + A2 recommended for
  v1, B-share as the follow-up).
- **Pin duration vs disk pressure** (Option A): the top risk. Logstor capacity is
  a fixed segment count and compaction scoring is pin-blind, so a long upload
  both consumes capacity and can drive a compaction storm that reclaims nothing.
  Mitigations: A1 (pin-aware candidacy) plus A3 rescue-pin remove the storm and
  make pinning demand-driven; A2 (pinned-segment budget) and A5 (yield under
  pressure) bound the residual, with A4 as rescue-pin's escalation step. The
  worst case — compaction sweeping the whole tablet mid-upload — still degrades
  to plain Option A cost. Option B relocates this cost rather than removing it
  and still requires A1.
- **A3 correctness hinges on the eviction hook.** Enumerating segments without
  pins and without a hook silently drops records. The hook must cover split
  compaction's slow path too, which is why the pending registry is keyed by
  segment id at shard level rather than per group. Rescue-pin gives the strong
  invariant (*every record live at snapshot time is consumed at least once*) and
  is correct for backup; the optional chase variant gives only the weaker
  union-with-the-write-path invariant and is **streaming-only**.
- **Group teardown is the one eviction rescue-pin cannot answer.**
  `discard_segments` destroys data deliberately, and hard-asserts
  `ref_count == 1`; an active snapshot must be serialized against or aborted.
- **B-share hinges on the "link dense files as-is" rule:** without it, every
  backup rewrites the whole tablet and dedup drops to zero.
- **Hard links are not snapshots here.** Logstor data files are mutable in place
  and a link shares the inode. Reflink (`FICLONE`) would give real CoW but is
  filesystem-dependent and interacts badly with O_DIRECT writes — rejected for
  now, recorded so it is not rediscovered as a free win.
- **Dedup effectiveness** is limited by compaction churn (documented behavior,
  not a bug); packing does not coarsen it — the previous-manifest map keeps
  dedup per-segment and exact. Consequences: the dedup map must come from the
  durable manifest (not the TTL'd `system_distributed` rows), and backup GC
  must be reference-aware because newer manifests re-reference older backups'
  packs.
- **Ranged reads for packs:** not needed for v1 — restore uses whole-pack GETs
  and packs never cross tablet boundaries, so a tablet downloads exactly its
  packs. Ranged GETs are an optional later optimization for sparse partial
  restores.
- **Per-replica backup choice:** confirm the coordinator's tablet → replica
  selection is deterministic/consistent for logstor (no cross-replica dedup);
  restore repopulates non-primary replicas via tablet rebuild. Sticky selection
  across backups is also what the `segment_seq`-watermark read skip needs;
  hash-based dedup keeps working (at the cost of a full re-read) when the
  replica changes. Note the Phase 1 guarantee is **one replica's view** —
  weaker than the sstable path's union of all replicas; stage 6 (Option R2)
  closes that gap. See "Replica selection and the repaired/unrepaired
  question".
- **Tablet repair is silently a no-op for logstor today** (second live
  defect): every repair read strategy reads memtables + sstables only, repair
  writes land in sstables the logstor read path never consults, yet the round
  reports success and advances `repair_time`/`sstables_repaired_at`. Stage 0b
  makes it explicit. Implementing logstor tablet repair (assumed, out of
  scope here; full repair suffices) is stage 6's hard prerequisite and what
  makes any repair-freshness gating meaningful.
- **Multi-source restore semantics (stage 6 / R4):** same-timestamp conflicts
  resolve by ingest order in the primary index rather than by CQL value
  reconciliation — a narrow but real divergence from the sstable restore
  path. And logstor currently never purges tombstone records (no resurrection
  risk today); if tombstone GC is ever added, it must become repair-aware
  before multi-source merge stays safe.
- **`segment_seq` watermark (follow-up):** each backup reads the whole tablet
  once to compute hashes even when little changed; the watermark read skip
  ("Incremental enumeration") removes this, but depends on sticky replica
  selection and on the previous backup's metadata being available — it needs a
  robust fallback to the full read-and-hash pass.
- **Content hash source:** confirm the cheapest place to compute it; the
  per-buffer CRC in the `chunk_header` may be reusable to avoid a second pass.
