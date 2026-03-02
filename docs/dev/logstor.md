# Logstor

## Introduction

Logstor is a log-structured storage engine for ScyllaDB optimized for key-value workloads. It provides an alternative storage backend for key-value tables - tables with a partition key only, with no clustering columns.

Unlike the traditional LSM-tree based storage, logstor uses a log-structured approach with in-memory indexing, making it particularly suitable for workloads with frequent overwrites and point lookups.

## Architecture

Logstor consists of several key components:

### Components

#### Primary Index

The `log_index` is an in-memory hash map that maps a partition to its location in the log segments.

The index key is a hash of the keyspace name, table name, and partition key, and it maps to an index entry that contains the location of the record on disk and additional metadata.

#### Segment Manager

The `segment_manager` handles the allocation and management of fixed-size segments (default 128KB). Segments are grouped into large files (default 32MB). Key responsibilities include:

- **Segment allocation**: Provides segments for writing new data
- **Space reclamation**: Tracks free space in each segment
- **Compaction**: Copies live data from sparse segments to reclaim space
- **Recovery**: Scans segments on startup to rebuild the index
- **Separator**: Rewrites segments that have records from different `group_id`'s into new segments that are separated by `group_id`.

The data in the segments consists of records of type `log_record`. Each record contains the value for some key as a `canonical_mutation` and additional metadata.

The `segment_manager` receives new writes via a `write_buffer` and writes them sequentially to the active segment with 4k-block alignment.

#### Write Buffer

The `write_buffer` manages a buffer of log records and handles the serialization of the records including headers and alignment. It can be used to write multiple records to the buffer and then write the buffer to the segment manager.

The `buffered_writer` manages multiple write buffers for user writes, an active buffer and multiple flushing ones, to batch writes and manage backpressure.

### Data Flow

**Write Path:**
1. Application writes mutation to logstor
2. Mutation is converted to a log record
3. Record is written to write buffer
4. The buffer is switched and written to the active segment
5. Index is updated with new record locations
6. Old record locations (for overwrites) are marked as free

**Read Path:**
1. Application requests data for a partition key
2. Index key is calculated from keyspace, table, and partition key
3. Index lookup returns record location
4. Segment manager reads record from disk
5. Record is deserialized into a mutation and returned

## Usage

### Enabling Logstor

To use logstor, enable it in the configuration:

```yaml
enable_kv_storage: true
experimental_features:
  - kv-storage
```

### Creating Tables

Tables using logstor must have a simple primary key (partition key only, no clustering columns) and specify the `kv_storage` property:

```cql
CREATE TABLE keyspace.user_profiles (
    user_id uuid PRIMARY KEY,
    name text,
    email text,
    metadata frozen<map<text, text>>
) WITH kv_storage = true;
```

### Basic Operations

**Insert/Update:**

```cql
INSERT INTO keyspace.table_name (pk, v) VALUES (1, 'value1');
INSERT INTO keyspace.table_name (pk, v) VALUES (2, 'value2');

-- Overwrite with new value
INSERT INTO keyspace.table_name (pk, v) VALUES (1, 'updated_value');
```

Currently, updates must write the full partition value. Updating individual columns is not yet supported. Each write replaces the entire partition.

**Select:**

```cql
SELECT * FROM keyspace.table_name WHERE pk = 1;
-- Returns: (1, 'updated_value')

SELECT pk, v FROM keyspace.table_name WHERE pk = 2;
-- Returns: (2, 'value2')
```

Currently, only point queries are supported. Range queries and full table scans are not supported.

**Delete:**

```cql
DELETE FROM keyspace.table_name WHERE pk = 1;
```
