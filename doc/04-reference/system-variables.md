---
title: System Variables
description: Every TideSQL system variable, grouped by scope, with defaults and meaning.
---

# System Variables

TideSQL registers the variables below. The read-only group is set at server startup and cannot
change while the server runs. The dynamic global group can be changed at runtime with
`SET GLOBAL`. The session group can be set per connection with `SET SESSION` (and its global value
is the default new sessions inherit).

## Global, read-only, set at startup

| Variable | Default | Description |
|----------|---------|-------------|
| `tidesdb_flush_threads` | 4 | Background threads flushing memtables to SSTables. 0 lets the library auto-size the shared flush pool to min(CPU count, 4) at open |
| `tidesdb_compaction_threads` | 4 | Background threads running LSM compaction |
| `tidesdb_log_level` | TRACE | Library log level, one of TRACE, INFO, WARN, ERROR, NONE |
| `tidesdb_block_cache_size` | 256 MB | Size in bytes of the global block cache shared across all column families |
| `tidesdb_max_open_sstables` | 256 | Maximum SSTable structures cached in the LRU. 0 means unlimited, bounded only by the process open-file limit |
| `tidesdb_log_to_file` | ON | Write library logs to a LOG file in the data directory instead of stderr |
| `tidesdb_log_truncation_at` | 24 MB | Log file truncation size in bytes. 0 disables truncation |
| `tidesdb_memtable_write_buffer_size` | 256 MB | Write buffer size in bytes for the shared memtable. 0 lets the library auto-size it |
| `tidesdb_memtable_sync_mode` | FULL | WAL durability for every commit, one of NONE, INTERVAL, FULL. See [Durability](/concepts/durability) |
| `tidesdb_memtable_sync_interval` | 128000 | WAL sync interval in microseconds, used only when the sync mode is INTERVAL |
| `tidesdb_memtable_skip_list_max_level` | 0 | Skip-list max level for the memtable. 0 keeps the library default |
| `tidesdb_memtable_skip_list_probability` | 0.0 | Skip-list level-promotion probability for the memtable. 0.0 keeps the library default |
| `tidesdb_vlog_segment_size` | 0 | Size in bytes at which the value log seals a segment and opens a fresh one. 0 keeps the library default |
| `tidesdb_value_separation_threshold` | 0 | Values at or above this size in bytes go to the shared value log instead of inline in the klog, so compaction rewrites only a reference. Database-wide, applied at open. 0 keeps the library default |
| `tidesdb_memtable_l0_queue_stall_threshold` | 0 | Sealed-memtable queue depth at which writes are paced for back-pressure. 0 keeps the library default of 16 |
| `tidesdb_memtable_idle_flush_seconds` | -1 | Seconds of write inactivity after which the shared memtable is flushed even before it fills. -1 keeps the library default |
| `tidesdb_txn_timeout_seconds` | -1 | Seconds a transaction may run before the library aborts it. -1 keeps the library default |
| `tidesdb_data_home_dir` | (auto) | Directory for the data files. Defaults to `<mysql_datadir>/../tidesdb_data` |

## Global, dynamic

| Variable | Default | Description |
|----------|---------|-------------|
| `tidesdb_backup_dir` | (empty) | Set to a path to trigger an online backup. Clear with an empty string. See [Backup](/administration/backup) |
| `tidesdb_checkpoint_dir` | (empty) | Set to a path to write a consistent checkpoint copy of the data directory to, a durable flush of the WAL, value log, and manifest followed by a byte-for-byte copy. Clear with an empty string |
| `tidesdb_fts_min_word_len` | 3 | Minimum word length in characters for full-text indexing |
| `tidesdb_fts_max_word_len` | 84 | Maximum word length in characters for full-text indexing |
| `tidesdb_fts_bm25_k1` | 1.2 | BM25 k1 parameter, term-frequency saturation |
| `tidesdb_fts_bm25_b` | 0.75 | BM25 b parameter, document-length normalization from 0 to 1 |
| `tidesdb_fts_blend_chars` | (empty) | Characters treated as both separators and word characters. Set to `'` for Italian and French elision. See [Full-Text Search](/reference/full-text-search) |
| `tidesdb_ft_stopword_table` | NULL | Custom stop-word table in `db_name/table_name` form. NULL uses the InnoDB default list, empty string disables stop-word filtering |

## Session, with a global default

These are session-scoped. Setting one globally changes the default new sessions inherit. The
`tidesdb_default_*` group supplies the default for the matching [table option](/reference/table-options)
when `CREATE TABLE` does not set it.

| Variable | Default | Description |
|----------|---------|-------------|
| `tidesdb_ttl` | 0 | Per-session TTL in seconds applied to INSERT and UPDATE. 0 uses the table default. Works with `SET SESSION` and `SET STATEMENT`. See [Time-To-Live](/reference/ttl) |
| `tidesdb_skip_unique_check` | OFF | Skip uniqueness checks on the primary key and unique secondary indexes during INSERT. Safe only when the application guarantees no duplicates |
| `tidesdb_single_delete_primary` | OFF | Use single-delete semantics on the primary row CF for this session's DELETEs. See [Write-Path Optimizations](/internals/write-path) |
| `tidesdb_compact_after_range_delete_min_rows` | 0 | After a multi-row DELETE touching at least this many rows, compact the touched primary-key range synchronously. 0 disables it |
| `tidesdb_default_compression` | LZ4 | Default `COMPRESSION` for new tables |
| `tidesdb_default_bloom_filter` | ON | Default `BLOOM_FILTER` for new tables |
| `tidesdb_default_bloom_fpr` | 100 | Default `BLOOM_FPR` in parts per 10,000, 100 is 1% |
| `tidesdb_default_keep_values_inline` | OFF | Default `KEEP_VALUES_INLINE` for new tables |
| `tidesdb_default_btree_klog_block_size` | 4096 | Default `BTREE_KLOG_BLOCK_SIZE` in bytes for new tables. Sizing a node to the block manager first-read window avoids a second read per access |
| `tidesdb_default_l1_file_count_trigger` | 4 | Default `L1_FILE_COUNT_TRIGGER` |
| `tidesdb_default_level_size_ratio` | 10 | Default `LEVEL_SIZE_RATIO` |
| `tidesdb_default_min_levels` | 1 | Default `MIN_LEVELS` |
| `tidesdb_default_dividing_level_offset` | 1 | Default `DIVIDING_LEVEL_OFFSET` |
| `tidesdb_default_isolation_level` | REPEATABLE_READ | Default `ISOLATION_LEVEL` |
| `tidesdb_default_tombstone_density_trigger` | 0 | Default `TOMBSTONE_DENSITY_TRIGGER` in parts per 10,000, 0 disables it |
| `tidesdb_default_tombstone_density_min_entries` | 1024 | Default `TOMBSTONE_DENSITY_MIN_ENTRIES` |

## Setting defaults in my.cnf

```ini
[mysqld]
plugin-load-add=ha_tidesdb.so
tidesdb_memtable_sync_mode=NONE
tidesdb_default_compression=NONE
tidesdb_default_bloom_fpr=10
tidesdb_value_separation_threshold=64
```

```sql
-- new tables inherit the global defaults
CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=TIDESDB;

-- override one option for one table
CREATE TABLE t2 (id INT PRIMARY KEY) ENGINE=TIDESDB COMPRESSION='ZSTD';

-- change a default for this session only
SET SESSION tidesdb_default_bloom_fpr = 50;
```
