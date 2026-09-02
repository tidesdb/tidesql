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
| `Tidesdb_version` | TideSQL plugin version string, for example `5.0.0` |
| `Tidesdb_version_hex` | Plugin version as an integer, for example `327680` for `0x50000` |
| `Tidesdb_library_version` | Linked TidesDB library version string |

## Sequence and transactions

| Variable | Description |
|----------|-------------|
| `Tidesdb_column_families` | Number of active column families |
| `Tidesdb_global_sequence` | Global MVCC sequence number |
| `Tidesdb_min_snapshot_sequence` | Oldest pinned snapshot, the floor compaction cannot reclaim past |
| `Tidesdb_active_transactions` | Transactions currently joined to the MVCC registry |
| `Tidesdb_txn_memory_bytes` | Memory held by in-flight transactions in bytes |

## Memory and storage

| Variable | Description |
|----------|-------------|
| `Tidesdb_memtable_bytes` | Bytes in the active memtable |
| `Tidesdb_total_sstables` | Total SSTable count across all column families |
| `Tidesdb_open_sstables` | Open SSTable file handles |
| `Tidesdb_data_size_bytes` | Total on-disk data size in bytes |
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

## Write amplification

| Variable | Description |
|----------|-------------|
| `Tidesdb_user_bytes_written` | Logical committed bytes, the write-amplification denominator |
| `Tidesdb_flush_bytes_written` | Bytes written to SSTables by flush jobs |
| `Tidesdb_compaction_bytes_written` | Bytes written by compaction jobs |
| `Tidesdb_compaction_bytes_read` | Bytes compaction read as input |
| `Tidesdb_flush_count` | Flushes completed across all column families |
| `Tidesdb_compaction_count` | Compactions completed across all column families |

## Write stalls

| Variable | Description |
|----------|-------------|
| `Tidesdb_writes_throttled` | Commits the L0 admission policy made dwell before admitting |
| `Tidesdb_writes_blocked` | Commits it made wait for the flush queue to drain |
| `Tidesdb_write_stall_us` | Total microseconds commits spent held in admission |
| `Tidesdb_write_stall_ceiling_hits` | Commits admitted only because the wait ceiling expired. Any sustained increase means flush is not keeping up with ingest |

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
