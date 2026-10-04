---
title: Status Variables
description: Every TideSQL status variable exposed through SHOW GLOBAL STATUS, grouped by what it measures.
---

# Status Variables

```sql
SHOW GLOBAL STATUS LIKE 'tidesdb%';
```

These are the machine-readable counters for a monitoring agent such as a Prometheus exporter or
PMM. [Monitoring](/administration/monitoring) explains which of them matter and what healthy looks
like. They are refreshed on demand behind a short coalescing window, so reading many of them in one
statement costs a single stats pass.

## Identity

| Variable | Description |
|----------|-------------|
| `Tidesdb_version` | TideSQL plugin version string, for example `5.1.1` |
| `Tidesdb_version_hex` | Plugin version as an integer, for example `327937` for `0x50101` |
| `Tidesdb_library_version` | Linked TidesDB library version string |

## Sequence and transactions

| Variable | Description |
|----------|-------------|
| `Tidesdb_column_families` | Number of active column families |
| `Tidesdb_global_sequence` | Global MVCC sequence number |
| `Tidesdb_min_snapshot_sequence` | Oldest pinned snapshot, the floor compaction cannot reclaim past |
| `Tidesdb_active_transactions` | Transactions currently joined to the MVCC registry |
| `Tidesdb_txn_memory_bytes` | Memory held by in-flight transactions in bytes |

## Transaction errors

Counters bumped each time a library error is mapped to a server error. A failure at COMMIT counts
once in `Tidesdb_commit_conflicts` whatever its cause, and a failure inside a statement counts in
the `Tidesdb_stmt_*` counter for its cause, so real contention can be told apart from engine churn. These are read live rather than through the
refresh window.

| Variable | Description |
|----------|-------------|
| `Tidesdb_commit_conflicts` | COMMIT statements that failed with a deadlock or lock wait timeout error. The server counts a failed COMMIT in `Com_commit` too, so subtract this to get successful commits |
| `Tidesdb_stmt_conflicts` | Statements that failed on a write conflict, returned as a deadlock error |
| `Tidesdb_stmt_locked` | Statements that failed on transient contention the library reports as locked, returned as a lock wait timeout after a short bounded retry |
| `Tidesdb_stmt_memory_limit` | Statements that failed on memtable backpressure, returned as a lock wait timeout |
| `Tidesdb_stmt_txn_expired` | Statements that failed because the transaction outlived `tidesdb_txn_timeout_seconds`, returned as a lock wait timeout |
| `Tidesdb_stmt_bf_aborted` | Statements that failed because a Galera applier brute-force aborted the transaction, returned as a deadlock error |

## Memory and storage

| Variable | Description |
|----------|-------------|
| `Tidesdb_memtable_bytes` | Memory the active memtable occupies, skip-list nodes and version structs included |
| `Tidesdb_total_sstables` | Total SSTable count across all column families |
| `Tidesdb_open_sstables` | Open SSTable file handles |
| `Tidesdb_data_size_bytes` | On-disk key-log bytes summed across every column family and level. The value log is reported separately in `Tidesdb_vlog_file_size` |
| `Tidesdb_immutable_memtables` | Sealed memtables waiting to be flushed |
| `Tidesdb_flush_pending` | Flushes pending (the immutable memtable queue depth) |
| `Tidesdb_compaction_queue` | Compaction jobs queued for the worker pool |
| `Tidesdb_memtable_is_flushing` | 1 while an immutable is queued or flushing |
| `Tidesdb_wal_generation` | Current write-ahead-log generation counter |

## Value log

| Variable | Description |
|----------|-------------|
| `Tidesdb_vlog_file_size` | On-disk size of the value log in bytes |
| `Tidesdb_vlog_value_count` | Values the value log currently indexes |
| `Tidesdb_vlog_used_bytes` | Uncompressed length those values represent |
| `Tidesdb_vlog_bytes_written` | Lifetime bytes appended to the value log, output the flush and compaction counters do not see once values separate |

## Encoding

Aggregate codec-chain totals summed across every chain, so `logical` divided by `stored` is the realized compression ratio for each log. `SHOW ENGINE TIDESDB STATUS` prints the same totals per log with the chain count and the ratio.

| Variable | Description |
|----------|-------------|
| `Tidesdb_klog_logical_bytes` | Key-log bytes before encoding, summed across chains |
| `Tidesdb_klog_stored_bytes` | Key-log bytes after encoding as stored on disk |
| `Tidesdb_vlog_encoded_logical_bytes` | Value-log bytes before encoding, summed across chains |
| `Tidesdb_vlog_encoded_stored_bytes` | Value-log bytes after encoding as stored on disk |

## Device IO

Write accounting from the library, which meters SSTable, WAL, and value-log writes. These counters surface the SSTable and WAL classes, the value log's write volume is in `Tidesdb_vlog_bytes_written` above, and all three classes print with their write timing in `SHOW ENGINE TIDESDB STATUS`. These counters are writes only, there is no read-side figure.

| Variable | Description |
|----------|-------------|
| `Tidesdb_io_sstable_write_ops` | SSTable device writes issued since open |
| `Tidesdb_io_sstable_write_bytes` | Bytes written to the SSTable device since open |
| `Tidesdb_io_wal_write_ops` | WAL device writes issued since open |
| `Tidesdb_io_wal_write_bytes` | Bytes written to the WAL device since open |

## Write amplification

| Variable | Description |
|----------|-------------|
| `Tidesdb_user_bytes_written` | Logical committed bytes, the write-amplification denominator |
| `Tidesdb_flush_bytes_written` | Bytes written to SSTables by flush jobs |
| `Tidesdb_compaction_bytes_written` | Bytes written by compaction jobs |
| `Tidesdb_compaction_bytes_read` | Bytes compaction read as input |
| `Tidesdb_flush_count` | SSTables written by flushes across all column families |
| `Tidesdb_compaction_count` | Compactions completed across all column families |

## Write stalls

| Variable | Description |
|----------|-------------|
| `Tidesdb_writes_throttled` | Commits the L0 admission policy made dwell before admitting |
| `Tidesdb_writes_blocked` | Commits it made wait for the flush queue to drain |
| `Tidesdb_write_stall_us` | Total microseconds commits spent held in admission |
| `Tidesdb_write_stall_ceiling_hits` | Commits admitted only because the wait ceiling expired. Any sustained increase means flush is not keeping up with ingest |

The aggregate counters above sum admission stall time. These per-reason counts split how often a commit stalled by cause, so backpressure can be attributed. The per-reason stall time prints in `SHOW ENGINE TIDESDB STATUS`.

| Variable | Description |
|----------|-------------|
| `Tidesdb_stall_wal_append` | Commits that stalled appending to the write-ahead log |
| `Tidesdb_stall_rotate_lock` | Commits that stalled taking the memtable rotation lock |
| `Tidesdb_stall_rotate_work` | Commits that stalled while a memtable rotation was in progress |
| `Tidesdb_stall_admission` | Commits that stalled on the unflushed-backlog admission gate |
| `Tidesdb_stall_manifest_commit` | Commits that stalled waiting for a manifest commit |

## Block cache

| Variable | Description |
|----------|-------------|
| `Tidesdb_cache_entries` | Cached entry count |
| `Tidesdb_cache_bytes` | Bytes used by the block cache |
| `Tidesdb_cache_hits` | Cache hits since open |
| `Tidesdb_cache_misses` | Cache misses since open |
| `Tidesdb_cache_hit_rate` | Hit rate as a percentage |
| `Tidesdb_cache_partitions` | Number of cache shards |

## Tombstones

| Variable | Description |
|----------|-------------|
| `Tidesdb_total_tombstones` | Total tombstones summed across every column family |
| `Tidesdb_tombstone_ratio` | Database-wide tombstone count divided by entry count, 0.0 to 1.0 |
| `Tidesdb_max_sst_tombstone_density` | Worst single-SSTable tombstone density observed |
| `Tidesdb_max_sst_tombstone_density_level` | 1-based LSM level where the worst SSTable sits |
