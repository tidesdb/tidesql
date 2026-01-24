---
title: TidesDB C API Reference
description: Complete C API reference for TidesDB
---

If you want to download the source of this document, you can find it [here](https://github.com/tidesdb/tidesdb.github.io/blob/master/src/content/docs/reference/c.md).

<hr/>

## Overview

TidesDB is designed to provide a simple and intuitive C API for all your embedded storage needs.  This document is complete reference for the C API covering database operations, transactions, column families, iterators and more.

## Include

:::note
You can use other components of TidesDB such as skip list, bloom filter etc. under `tidesdb/` - this also prevents collisions.
:::

### Choosing Between `tidesdb.h` and `db.h`

TidesDB provides two header files with different purposes:

**tidesdb.h**

Full C implementation header. Mainly use this for native C/C++ applications.

```c
#include <tidesdb/tidesdb.h>
```

**db.h**

db.h is mainly an FFI/Language binding interface with minimal dependencies and simpler ABI.

```c
#include <tidesdb/db.h>
```

:::tip[When to Use Each]
- C/C++ applications ➞ Use `tidesdb.h` for full access to the API
- Language bindings (jextract, rust-bindgen, ctypes, cgo, etc.) ➞ Use `db.h` for a stable FFI interface
- Library developers ➞ Use `db.h` to avoid exposing internal implementation details
:::

## Error Codes

TidesDB provides detailed error codes for production use.

| Code | Value | Description |
|------|-------|-------------|
| `TDB_SUCCESS` | `0` | Operation completed successfully |
| `TDB_ERR_MEMORY` | `-1` | Memory allocation failed |
| `TDB_ERR_INVALID_ARGS` | `-2` | Invalid arguments passed to function (NULL pointers, invalid sizes, etc.) |
| `TDB_ERR_NOT_FOUND` | `-3` | Key not found in column family |
| `TDB_ERR_IO` | `-4` | I/O operation failed (file read/write error) |
| `TDB_ERR_CORRUPTION` | `-5` | Data corruption detected (checksum failure, invalid format version, truncated data) |
| `TDB_ERR_EXISTS` | `-6` | Resource already exists (e.g., column family name collision) |
| `TDB_ERR_CONFLICT` | `-7` | Transaction conflict detected (write-write or read-write conflict in SERIALIZABLE/SNAPSHOT isolation) |
| `TDB_ERR_TOO_LARGE` | `-8` | Key or value size exceeds maximum allowed size |
| `TDB_ERR_MEMORY_LIMIT` | `-9` | Operation would exceed memory limits (safety check to prevent OOM) |
| `TDB_ERR_INVALID_DB` | `-10` | Database handle is invalid (e.g., after close) |
| `TDB_ERR_UNKNOWN` | `-11` | Unknown or unspecified error |
| `TDB_ERR_LOCKED` | `-12` | Database is locked by another process |

**Error categories**
- `TDB_ERR_CORRUPTION` indicates data integrity issues requiring immediate attention
- `TDB_ERR_CONFLICT` indicates transaction conflicts (retry may succeed)
- `TDB_ERR_MEMORY`, `TDB_ERR_MEMORY_LIMIT`, `TDB_ERR_TOO_LARGE` indicate resource constraints
- `TDB_ERR_NOT_FOUND`, `TDB_ERR_EXISTS` are normal operational conditions, not failures

### Example Error Handling

```c
int result = tidesdb_txn_put(txn, cf, key, key_size, value, value_size, -1);
if (result != TDB_SUCCESS)
{
    switch (result)
    {
        case TDB_ERR_MEMORY:
            fprintf(stderr, "out of memory\n");
            break;
        case TDB_ERR_INVALID_ARGS:
            fprintf(stderr, "invalid arguments\n");
            break;
        case TDB_ERR_CONFLICT:
            fprintf(stderr, "transaction conflict detected\n");
            break;
        default:
            fprintf(stderr, "operation failed with error code: %d\n", result);
            break;
    }
    return -1;
}
```

## Storage Engine Operations

### Opening TidesDB

```c
tidesdb_config_t config = {
    .db_path = "./mydb",
    .num_flush_threads = 2,                /* Flush thread pool size (default: 2) */
    .num_compaction_threads = 2,           /* Compaction thread pool size (default: 2) */
    .log_level = TDB_LOG_INFO,             /* Log level: TDB_LOG_DEBUG, TDB_LOG_INFO, TDB_LOG_WARN, TDB_LOG_ERROR, TDB_LOG_FATAL, TDB_LOG_NONE */
    .block_cache_size = 64 * 1024 * 1024,  /* 64MB global block cache (default: 64MB) */
    .max_open_sstables = 256,              /* Max cached SSTable structures (default: 256) */
};

tidesdb_t *db = NULL;
if (tidesdb_open(&config, &db) != 0)
{
    return -1;
}

if (tidesdb_close(db) != 0)
{
    return -1;
}
```

:::note[Multiple DBs Allowed]
Multiple TidesDB instances can be opened in the same process, each with its own configuration and data directory.
:::

### Logging

TidesDB provides structured logging with multiple severity levels.

**Log Levels**
- `TDB_LOG_DEBUG` · Detailed diagnostic information
- `TDB_LOG_INFO` · General informational messages (default)
- `TDB_LOG_WARN` · Warning messages for potential issues
- `TDB_LOG_ERROR` · Error messages for failures
- `TDB_LOG_FATAL` · Critical errors that may cause shutdown
- `TDB_LOG_NONE` · Disable all logging

**Configure at startup**
```c
tidesdb_config_t config = {
    .db_path = "./mydb",
    .log_level = TDB_LOG_DEBUG  /* Enable debug logging */
};

tidesdb_t *db = NULL;
tidesdb_open(&config, &db);
```

**Production configuration**
```c
tidesdb_config_t config = {
    .db_path = "./mydb",
    .log_level = TDB_LOG_WARN  /* Only warnings and errors */
};
```

**Output format**
Logs are written to **stderr** with timestamps:
```
[HH:MM:SS.mmm] [LEVEL] filename:line: message
```

**Example output**
```
[22:58:00.454] [INFO] tidesdb.c:9322: Opening TidesDB with path=./mydb
[22:58:00.456] [INFO] tidesdb.c:9478: Block clock cache created with max_bytes=64.00 MB
```

**Redirect to file**
```bash
./your_program 2> tidesdb.log  # Redirect stderr to file
```

### Backup

`tidesdb_backup` creates an on-disk snapshot of an open database without blocking normal reads/writes.

```c
int tidesdb_backup(tidesdb_t *db, char *dir);
```

**Usage**
```c
tidesdb_t *db = NULL;
tidesdb_open(&config, &db);

if (tidesdb_backup(db, "./mydb_backup") != 0)
{
    fprintf(stderr, "Backup failed\n");
}
```

**Behavior**
- Requires `dir` to be a non-existent directory or an empty directory; returns `TDB_ERR_EXISTS` if not empty.
- Does not copy the `LOCK` file, so the backup can be opened normally.
- Two-phase copy approach:
  - Copies immutable files first (SSTables listed in the manifest plus metadata/config files) and skips WALs.
  - Forces memtable flushes, waits for flush/compaction queues to drain, then copies any remaining files
    (including WALs and updated manifests). Existing SSTables already copied are not recopied.
- Database stays open and usable during backup; no exclusive lock is taken on the source directory.

**Notes**
- The backup represents the database state after the final flush/compaction drain.
- If you need a quiesced backup window, you can pause writes at the application level before calling this API.

## Column Family Operations

### Creating a Column Family

Column families are isolated key-value stores. Use the config struct for customization or use defaults.

```c
/* Create with default configuration */
tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();

if (tidesdb_create_column_family(db, "my_cf", &cf_config) != 0)
{
    return -1;
}
```

**Custom configuration example**
```c
tidesdb_column_family_config_t cf_config = {
    .write_buffer_size = 128 * 1024 * 1024,     /* 128MB memtable flush threshold */
    .level_size_ratio = 10,                     /* Level size multiplier (default: 10) */
    .min_levels = 5,                            /* Minimum LSM levels (default: 5) */
    .dividing_level_offset = 2,                 /* Compaction dividing level offset (default: 2) */
    .skip_list_max_level = 12,                  /* Skip list max level */
    .skip_list_probability = 0.25f,             /* Skip list probability */
    .compression_algorithm = LZ4_COMPRESSION,   /* LZ4_COMPRESSION, SNAPPY_COMPRESSION, or ZSTD_COMPRESSION */
    .enable_bloom_filter = 1,                   /* Enable bloom filters */
    .bloom_fpr = 0.01,                          /* 1% false positive rate */
    .enable_block_indexes = 1,                  /* Enable compact block indexes */
    .index_sample_ratio = 1,                    /* Sample every block for index (default: 1) */
    .block_index_prefix_len = 16,               /* Block index prefix length (default: 16) */
    .sync_mode = TDB_SYNC_FULL,                 /* TDB_SYNC_NONE, TDB_SYNC_INTERVAL, or TDB_SYNC_FULL */
    .sync_interval_us = 1000000,                /* Sync interval in microseconds (1 second, only for TDB_SYNC_INTERVAL) */
    .comparator_name = {0},                     /* Empty = use default "memcmp" */
    .klog_value_threshold = 512,               /* Values > 512 bytes go to vlog (default: 512) */
    .min_disk_space = 100 * 1024 * 1024,        /* Minimum disk space required (default: 100MB) */
    .default_isolation_level = TDB_ISOLATION_READ_COMMITTED,  /* Default transaction isolation */
    .l1_file_count_trigger = 4,                 /* L1 file count trigger for compaction (default: 4) */
    .l0_queue_stall_threshold = 20              /* L0 queue stall threshold (default: 20) */
};

if (tidesdb_create_column_family(db, "my_cf", &cf_config) != 0)
{
    return -1;
}
```

**Using custom comparator**
```c
/* Register comparator after opening database but before creating CF */
tidesdb_register_comparator(db, "reverse", my_reverse_compare, NULL, NULL);

tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
strncpy(cf_config.comparator_name, "reverse", TDB_MAX_COMPARATOR_NAME - 1);  
cf_config.comparator_name[TDB_MAX_COMPARATOR_NAME - 1] = '\0';

if (tidesdb_create_column_family(db, "sorted_cf", &cf_config) != 0)
{
    return -1;
}
```

### Dropping a Column Family

```c
if (tidesdb_drop_column_family(db, "my_cf") != 0)
{
    return -1;
}
```

### Getting a Column Family

Retrieve a column family pointer to use in operations.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (cf == NULL)
{
    /* Column family not found */
    return -1;
}
```

### Listing Column Families

Get all column family names on the TidesDB instance.

```c
char **names = NULL;
int count = 0;

if (tidesdb_list_column_families(db, &names, &count) == 0)
{
    printf("Found %d column families:\n", count);
    for (int i = 0; i < count; i++)
    {
        printf("  - %s\n", names[i]);
        free(names[i]);
    }
    free(names);
}
```

### Column Family Statistics

Get detailed statistics about a column family.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_stats_t *stats = NULL;
if (tidesdb_get_stats(cf, &stats) == 0)
{
    printf("Memtable Size: %zu bytes\n", stats->memtable_size);
    printf("Number of Levels: %d\n", stats->num_levels);
    
    for (int i = 0; i < stats->num_levels; i++)
    {
        printf("Level %d: %d SSTables, %zu bytes\n", 
               i + 1, stats->level_num_sstables[i], stats->level_sizes[i]);
    }
    
    /* Access configuration */
    printf("Write Buffer Size: %zu\n", stats->config->write_buffer_size);
    printf("Compression: %d\n", stats->config->compression_algorithm);
    printf("Bloom Filter: %s\n", stats->config->enable_bloom_filter ? "enabled" : "disabled");
    
    tidesdb_free_stats(stats);
}
```

**Statistics include**
- Memtable size in bytes
- Number of LSM levels
- Per-level SSTable count and total size
- Full column family configuration (via `stats->config`)

### Block Cache Statistics

Get statistics for the global block cache (shared across all column families).

```c
tidesdb_cache_stats_t cache_stats;
if (tidesdb_get_cache_stats(db, &cache_stats) == 0)
{
    if (cache_stats.enabled)
    {
        printf("Cache enabled: yes\n");
        printf("Total entries: %zu\n", cache_stats.total_entries);
        printf("Total bytes: %.2f MB\n", cache_stats.total_bytes / (1024.0 * 1024.0));
        printf("Hits: %lu\n", cache_stats.hits);
        printf("Misses: %lu\n", cache_stats.misses);
        printf("Hit rate: %.1f%%\n", cache_stats.hit_rate * 100.0);
        printf("Partitions: %zu\n", cache_stats.num_partitions);
    }
    else
    {
        printf("Cache enabled: no (block_cache_size = 0)\n");
    }
}
```

**Cache statistics include**
- `enabled` · Whether block cache is active (0 if `block_cache_size` was set to 0)
- `total_entries` · Number of cached blocks
- `total_bytes` · Total memory used by cached blocks
- `hits` · Number of cache hits (blocks served from memory)
- `misses` · Number of cache misses (blocks read from disk)
- `hit_rate` · Hit rate as a decimal (0.0 to 1.0)
- `num_partitions` · Number of cache partitions (scales with CPU cores)

:::note[Block Cache]
The block cache is a database-level resource shared across all column families. It caches deserialized klog blocks to avoid repeated disk I/O and deserialization. Configure cache size via `config.block_cache_size` when opening the database. Set to 0 to disable caching.
:::

### Compression Algorithms

TidesDB supports multiple compression algorithms to reduce storage footprint and I/O bandwidth. Compression is applied to both klog (key-log) and vlog (value-log) blocks before writing to disk.

**Available Algorithms**

- **`NO_COMPRESSION`** · No compression (value: 0)
  - Raw data written directly to disk
  - **Use case** · Pre-compressed data, maximum write throughput, CPU-constrained environments

- **`LZ4_COMPRESSION`** · LZ4 standard compression (value: 2, **default**)
  - Fast compression and decompression with good compression ratios
  - **Use case** · General purpose, balanced performance and compression
  - **Performance** · ~500 MB/s compression, ~2000 MB/s decompression (typical)

- **`LZ4_FAST_COMPRESSION`** · LZ4 fast mode (value: 4)
  - Faster compression than standard LZ4 with slightly lower compression ratio
  - Uses acceleration factor of 2
  - **Use case** · Write-heavy workloads prioritizing speed over compression ratio
  - **Performance** · Higher compression throughput than standard LZ4

- **`ZSTD_COMPRESSION`** · Zstandard compression (value: 3)
  - Best compression ratio with moderate speed (compression level 1)
  - **Use case** · Storage-constrained environments, archival data, read-heavy workloads
  - **Performance** · ~400 MB/s compression, ~1000 MB/s decompression (typical)

- **`SNAPPY_COMPRESSION`** · Snappy compression (value: 1)
  - Fast compression with moderate compression ratios
  - **Availability** · Not available on SunOS/Illumos/OmniOS platforms
  - **Use case** · Legacy compatibility, platforms where Snappy is preferred

**Configuration Example**

```c
tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();

/* Use LZ4 compression (default) */
cf_config.compression_algorithm = LZ4_COMPRESSION;

/* Use Zstandard for better compression ratio */
cf_config.compression_algorithm = ZSTD_COMPRESSION;

/* Use LZ4 fast mode for maximum write throughput */
cf_config.compression_algorithm = LZ4_FAST_COMPRESSION;

/* Disable compression */
cf_config.compression_algorithm = NO_COMPRESSION;

tidesdb_create_column_family(db, "my_cf", &cf_config);
```

**Important Notes**

- Compression algorithm **cannot be changed** after column family creation without corrupting existing SSTables
- Compression is applied at the block level (both klog and vlog blocks)
- Decompression happens automatically during reads
- Block cache stores **decompressed** blocks to avoid repeated decompression overhead
- Different column families can use different compression algorithms

**Choosing a Compression Algorithm**

| Workload | Recommended Algorithm | Rationale |
|----------|----------------------|-----------|
| General purpose | `LZ4_COMPRESSION` | Best balance of speed and compression |
| Write-heavy | `LZ4_FAST_COMPRESSION` | Minimize CPU overhead on writes |
| Storage-constrained | `ZSTD_COMPRESSION` | Maximum compression ratio |
| Read-heavy | `ZSTD_COMPRESSION` | Reduce I/O bandwidth, decompression is fast |
| Pre-compressed data | `NO_COMPRESSION` | Avoid double compression overhead |
| CPU-constrained | `NO_COMPRESSION` or `LZ4_FAST_COMPRESSION` | Minimize CPU usage |

### Updating Column Family Configuration

Update runtime-safe configuration settings. Configuration changes are applied to new operations only.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_column_family_config_t new_config = tidesdb_default_column_family_config();
new_config.write_buffer_size = 256 * 1024 * 1024;  /* 256MB */
new_config.skip_list_max_level = 16;
new_config.skip_list_probability = 0.25f;
new_config.bloom_fpr = 0.001;  /* 0.1% false positive rate */
new_config.index_sample_ratio = 8;  /* sample 1 in 8 keys */

int persist_to_disk = 1;  /* save to config.ini */
if (tidesdb_cf_update_runtime_config(cf, &new_config, persist_to_disk) == 0)
{
    printf("Configuration updated successfully\n");
}
```

**Updatable settings** (safe to change at runtime):
- `write_buffer_size` · Memtable flush threshold
- `skip_list_max_level` · Skip list level for **new** memtables
- `skip_list_probability` · Skip list probability for **new** memtables
- `bloom_fpr` · False positive rate for **new** SSTables
- `index_sample_ratio` · Index sampling ratio for **new** SSTables
- `sync_mode` · Durability mode (TDB_SYNC_NONE, TDB_SYNC_INTERVAL, or TDB_SYNC_FULL)
- `sync_interval_us` · Sync interval in microseconds (only used when sync_mode is TDB_SYNC_INTERVAL)

**Non-updatable settings** (would corrupt existing data):
- `compression_algorithm` · Cannot change on existing SSTables
- `enable_block_indexes` · Cannot change index structure
- `enable_bloom_filter` · Cannot change bloom filter presence
- `comparator_name` · Cannot change sort order
- `level_size_ratio` · Cannot change LSM level sizing
- `klog_value_threshold` · Cannot change klog/vlog separation
- `min_levels` · Cannot change minimum LSM levels
- `dividing_level_offset` · Cannot change compaction strategy
- `block_index_prefix_len` · Cannot change block index structure
- `l1_file_count_trigger` · Cannot change compaction trigger
- `l0_queue_stall_threshold` · Cannot change backpressure threshold

:::note[Backpressure Defaults]
The default `l0_queue_stall_threshold` is 20. The default `l1_file_count_trigger` is 4.
:::

**Configuration persistence**

If `persist_to_disk = 1`, changes are saved to `config.ini` in the column family directory. On restart, the configuration is loaded from this file.

```c
/* Save configuration to custom INI file */
tidesdb_cf_config_save_to_ini("custom_config.ini", "my_cf", &new_config);
```

:::tip[Important Notes]
Changes apply immediately to new operations. Existing SSTables and memtables retain their original settings. The update operation is thread-safe.
:::

## Transactions

All operations in TidesDB are done through transactions for ACID guarantees per column family.

### Basic Transaction

```c
/* Get column family pointer first */
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
if (tidesdb_txn_begin(db, &txn) != 0)
{
    return -1;
}

const uint8_t *key = (uint8_t *)"mykey";
const uint8_t *value = (uint8_t *)"myvalue";

if (tidesdb_txn_put(txn, cf, key, 5, value, 7, -1) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

if (tidesdb_txn_commit(txn) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_free(txn);
```

### With TTL (Time-to-Live)

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

const uint8_t *key = (uint8_t *)"temp_key";
const uint8_t *value = (uint8_t *)"temp_value";

/* TTL is Unix timestamp (seconds since epoch) -- absolute expiration time */
time_t ttl = time(NULL) + 60;  /* Expires 60 seconds from now */

/* Use -1 for no expiration */
tidesdb_txn_put(txn, cf, key, 8, value, 10, ttl);
tidesdb_txn_commit(txn);
tidesdb_txn_free(txn);
```

:::tip[TTL Examples]
```c
/* No expiration */
time_t ttl = -1;

/* Expire in 5 minutes */
time_t ttl = time(NULL) + (5 * 60);

/* Expire in 1 hour */
time_t ttl = time(NULL) + (60 * 60);

/* Expire at specific time (e.g., midnight) */
time_t ttl = 1730592000;  /* Specific Unix timestamp */
```
:::

### Getting a Key-Value Pair

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

const uint8_t *key = (uint8_t *)"mykey";
uint8_t *value = NULL;
size_t value_size = 0;

if (tidesdb_txn_get(txn, cf, key, 5, &value, &value_size) == 0)
{
    /* Use value */
    printf("Value: %.*s\n", (int)value_size, value);
    free(value);
}

tidesdb_txn_free(txn);
```

### Deleting a Key-Value Pair

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

const uint8_t *key = (uint8_t *)"mykey";
tidesdb_txn_delete(txn, cf, key, 5);

tidesdb_txn_commit(txn);
tidesdb_txn_free(txn);
```

### Multi-Operation Transaction

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

/* Multiple operations in one transaction */
tidesdb_txn_put(txn, cf, (uint8_t *)"key1", 4, (uint8_t *)"value1", 6, -1);
tidesdb_txn_put(txn, cf, (uint8_t *)"key2", 4, (uint8_t *)"value2", 6, -1);
tidesdb_txn_delete(txn, cf, (uint8_t *)"old_key", 7);

/* Commit atomically -- all or nothing */
if (tidesdb_txn_commit(txn) != 0)
{
    /* On error, transaction is automatically rolled back */
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_free(txn);
```

### Transaction Rollback

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_txn_put(txn, cf, (uint8_t *)"key", 3, (uint8_t *)"value", 5, -1);

/* Decide to rollback instead of commit */
tidesdb_txn_rollback(txn);
tidesdb_txn_free(txn);
/* No changes were applied */
```

### Savepoints

Savepoints allow partial rollback within a transaction. You can create named savepoints and rollback to them without aborting the entire transaction.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

/* First operation */
tidesdb_txn_put(txn, cf, (uint8_t *)"key1", 4, (uint8_t *)"value1", 6, -1);

/* Create savepoint */
if (tidesdb_txn_savepoint(txn, "sp1") != 0)
{
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return -1;
}

/* Second operation */
tidesdb_txn_put(txn, cf, (uint8_t *)"key2", 4, (uint8_t *)"value2", 6, -1);

/* Rollback to savepoint - key2 is discarded, key1 remains */
if (tidesdb_txn_rollback_to_savepoint(txn, "sp1") != 0)
{
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return -1;
}

/* Add different operation after rollback */
tidesdb_txn_put(txn, cf, (uint8_t *)"key3", 4, (uint8_t *)"value3", 6, -1);

/* Commit transaction - only key1 and key3 are written */
if (tidesdb_txn_commit(txn) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_free(txn);
```

**Savepoint API**
- `tidesdb_txn_savepoint(txn, "name")` · Create a savepoint
- `tidesdb_txn_rollback_to_savepoint(txn, "name")` · Rollback to savepoint
- `tidesdb_txn_release_savepoint(txn, "name")` · Release savepoint without rolling back

**Savepoint behavior**
- Savepoints capture the transaction state at a specific point
- Rolling back to a savepoint discards all operations after that savepoint
- Releasing a savepoint frees its resources without rolling back
- Multiple savepoints can be created with different names
- Creating a savepoint with an existing name updates that savepoint
- Savepoints are automatically freed when the transaction commits or rolls back
- Returns `TDB_ERR_NOT_FOUND` if the savepoint name doesn't exist

### Multi-Column-Family Transactions

TidesDB supports atomic transactions across multiple column families with true all-or-nothing semantics.

```c
tidesdb_column_family_t *users_cf = tidesdb_get_column_family(db, "users");
tidesdb_column_family_t *orders_cf = tidesdb_get_column_family(db, "orders");
if (!users_cf || !orders_cf) return -1;

tidesdb_txn_t *txn = NULL;
if (tidesdb_txn_begin(db, &txn) != 0)
{
    return -1;
}

/* Write to users CF */
tidesdb_txn_put(txn, users_cf, (uint8_t *)"user:1000", 9, 
                (uint8_t *)"John Doe", 8, -1);

/* Write to orders CF */
tidesdb_txn_put(txn, orders_cf, (uint8_t *)"order:5000", 10,
                (uint8_t *)"user:1000|product:A", 19, -1);

/* Atomic commit across both CFs */
if (tidesdb_txn_commit(txn) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_free(txn);
```

**Multi-CF guarantees**
- Either all CFs commit or none do (atomic)
- Automatically detected when operations span multiple CFs
- Uses global sequence numbers for atomic ordering
- Each CF's WAL receives operations with the same commit sequence number
- No two-phase commit or coordinator overhead

### Isolation Levels

TidesDB supports five MVCC isolation levels for fine-grained concurrency control.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;

/* READ UNCOMMITTED - sees all data including uncommitted changes */
tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_READ_UNCOMMITTED, &txn);

/* READ COMMITTED - sees only committed data (default) */
tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_READ_COMMITTED, &txn);

/* REPEATABLE READ - consistent snapshot, phantom reads possible */
tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_REPEATABLE_READ, &txn);

/* SNAPSHOT ISOLATION - write-write conflict detection */
tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_SNAPSHOT, &txn);

/* SERIALIZABLE - full read-write conflict detection (SSI) */
tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_SERIALIZABLE, &txn);

/* Use transaction with operations */
tidesdb_txn_put(txn, cf, (uint8_t *)"key", 3, (uint8_t *)"value", 5, -1);

int result = tidesdb_txn_commit(txn);
if (result == TDB_ERR_CONFLICT)
{
    /* Conflict detected - retry transaction */
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_free(txn);
```

**Isolation level characteristics**
- **READ UNCOMMITTED** · Maximum concurrency, minimal consistency
- **READ COMMITTED** · Balanced for OLTP workloads (default)
- **REPEATABLE READ** · Strong point read consistency
- **SNAPSHOT** · Prevents lost updates with write-write conflict detection
- **SERIALIZABLE** · Strongest guarantees with full SSI, higher abort rates

## Iterators

Iterators provide efficient forward and backward traversal over key-value pairs.

### Forward Iteration

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_iter_t *iter = NULL;
if (tidesdb_iter_new(txn, cf, &iter) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

/* Seek to first entry */
tidesdb_iter_seek_to_first(iter);

while (tidesdb_iter_valid(iter))
{
    uint8_t *key = NULL;
    size_t key_size = 0;
    uint8_t *value = NULL;
    size_t value_size = 0;
    
    if (tidesdb_iter_key(iter, &key, &key_size) == 0 &&
        tidesdb_iter_value(iter, &value, &value_size) == 0)
    {
        printf("Key: %.*s, Value: %.*s\n", 
               (int)key_size, key, (int)value_size, value);
    }
    
    tidesdb_iter_next(iter);
}

tidesdb_iter_free(iter);
tidesdb_txn_free(txn);
```

### Backward Iteration

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_iter_t *iter = NULL;
tidesdb_iter_new(txn, cf, &iter);

tidesdb_iter_seek_to_last(iter);

while (tidesdb_iter_valid(iter))
{
    /* Process entries in reverse order */
    tidesdb_iter_prev(iter);
}

tidesdb_iter_free(iter);
tidesdb_txn_free(txn);
```

### Iterator Seek Operations

TidesDB provides seek operations that allow you to position an iterator at a specific key or key range without scanning from the beginning.

**How Seek Works**

**With Block Indexes Enabled** (`enable_block_indexes = 1`):
- Uses compact block index with parallel arrays (min/max key prefixes and file positions)
- Binary search through sampled keys at configurable ratio (default 1:1 via `index_sample_ratio`, meaning every block is indexed)
- Jumps directly to the target block using the file position
- Scans forward from that block to find the exact key
- **Performance** · O(log n) binary search + O(k) entries per block scan

Block indexes provide dramatic speedup for large SSTables at the cost of ~2-5% storage overhead for the compact index structure (parallel arrays with delta-encoded file positions).

#### Seek to Specific Key

**`tidesdb_iter_seek(iter, key, key_size)`** · Positions iterator at the first key >= target key

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_iter_t *iter = NULL;
tidesdb_iter_new(txn, cf, &iter);

/* Seek to specific key */
const char *target = "user:1000";
if (tidesdb_iter_seek(iter, (uint8_t *)target, strlen(target)) == 0)
{
    /* Iterator is now positioned at "user:1000" or the next key after it */
    if (tidesdb_iter_valid(iter))
    {
        uint8_t *key = NULL;
        size_t key_size = 0;
        tidesdb_iter_key(iter, &key, &key_size);
        printf("Found: %.*s\n", (int)key_size, key);
    }
}

tidesdb_iter_free(iter);
tidesdb_txn_free(txn);
```

**`tidesdb_iter_seek_for_prev(iter, key, key_size)`** · Positions iterator at the last key <= target key

```c
/* Seek for reverse iteration */
const char *target = "user:2000";
if (tidesdb_iter_seek_for_prev(iter, (uint8_t *)target, strlen(target)) == 0)
{
    /* Iterator is now positioned at "user:2000" or the previous key before it */
    while (tidesdb_iter_valid(iter))
    {
        /* Iterate backwards from this point */
        tidesdb_iter_prev(iter);
    }
}
```

#### Prefix Seeking

Since `tidesdb_iter_seek` positions the iterator at the first key >= target, you can use a prefix as the seek target to efficiently scan all keys sharing that prefix:

```c
/* Seek to prefix and iterate all matching keys */
const char *prefix = "user:";
if (tidesdb_iter_seek(iter, (uint8_t *)prefix, strlen(prefix) + 1) == 0)
{
    while (tidesdb_iter_valid(iter))
    {
        uint8_t *key = NULL;
        size_t key_size = 0;
        tidesdb_iter_key(iter, &key, &key_size);
        
        /* Stop when keys no longer match prefix */
        if (strncmp((char *)key, prefix, strlen(prefix)) != 0) break;
        
        /* Process key */
        printf("Found: %.*s\n", (int)key_size, key);
        
        if (tidesdb_iter_next(iter) != TDB_SUCCESS) break;
    }
}
```

This pattern works across both memtables and SSTables. When block indexes are enabled, the seek operation uses binary search to jump directly to the relevant block, making prefix scans efficient even on large datasets.

## Custom Comparators

TidesDB uses comparators to determine the sort order of keys throughout the entire system: memtables, SSTables, block indexes, and iterators all use the same comparison logic. Once a comparator is set for a column family, it **cannot be changed** without corrupting data.

### Built-in Comparators

TidesDB provides six built-in comparators that are automatically registered on database open:

**`"memcmp"` (default)** · Binary byte-by-byte comparison
- Compares min(key1_size, key2_size) bytes using `memcmp()`
- If bytes are equal, shorter key sorts first
- **Use case** · Binary keys, raw byte data, general purpose

**`"lexicographic"`** · Null-terminated string comparison
- Uses `strcmp()` for lexicographic ordering
- Ignores key_size parameters (assumes null-terminated)
- **Use case** · C strings, text keys
- **Warning** · Keys must be null-terminated or behavior is undefined

**`"uint64"`** · Unsigned 64-bit integer comparison
- Interprets 8-byte keys as uint64_t values
- Falls back to memcmp if key_size != 8
- **Use case** · Numeric IDs, timestamps, counters
- **Example** · `uint64_t id = 1000; tidesdb_txn_put(txn, cf, (uint8_t*)&id, 8, ...)`

**`"int64"`** · Signed 64-bit integer comparison
- Interprets 8-byte keys as int64_t values
- Falls back to memcmp if key_size != 8
- **Use case** · Signed numeric keys, relative timestamps
- **Example** · `int64_t offset = -500; tidesdb_txn_put(txn, cf, (uint8_t*)&offset, 8, ...)`

**`"reverse"`** · Reverse binary comparison
- Negates the result of memcmp comparator
- Sorts keys in descending order
- **Use case** · Reverse chronological order, descending IDs

**`"case_insensitive"`** · Case-insensitive ASCII comparison
- Converts A-Z to a-z during comparison
- Compares min(key1_size, key2_size) bytes
- If bytes are equal (ignoring case), shorter key sorts first
- **Use case** · Case-insensitive text keys, usernames, email addresses

### Custom Comparator Registration

```c
/* Define your comparison function */
int my_timestamp_compare(const uint8_t *key1, size_t key1_size,
                         const uint8_t *key2, size_t key2_size, void *ctx)
{
    (void)ctx;  /* unused */
    
    if (key1_size != 8 || key2_size != 8)
    {
        /* fallback for invalid sizes */
        return memcmp(key1, key2, key1_size < key2_size ? key1_size : key2_size);
    }
    
    uint64_t ts1, ts2;
    memcpy(&ts1, key1, 8);
    memcpy(&ts2, key2, 8);
    
    /* reverse order for newest-first */
    if (ts1 > ts2) return -1;
    if (ts1 < ts2) return 1;
    return 0;
}

/* Register before creating column families */
tidesdb_register_comparator(db, "timestamp_desc", my_timestamp_compare, NULL, NULL);

/* Use in column family */
tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
strncpy(cf_config.comparator_name, "timestamp_desc", TDB_MAX_COMPARATOR_NAME - 1);
cf_config.comparator_name[TDB_MAX_COMPARATOR_NAME - 1] = '\0';
tidesdb_create_column_family(db, "events", &cf_config);
```

**Comparator function signature**
```c
int (*comparator_fn)(const uint8_t *key1, size_t key1_size,
                     const uint8_t *key2, size_t key2_size,
                     void *ctx);
```

**Return values**
- `< 0` if key1 < key2
- `0` if key1 == key2
- `> 0` if key1 > key2

**Important notes**
- Comparators must be **registered before** creating column families that use them
- Once set, a comparator **cannot be changed** for a column family
- The same comparator is used across memtables, SSTables, block indexes, and iterators
- Custom comparators can use the `ctx` parameter for runtime configuration


## Sync Modes

Control durability vs performance tradeoff with three sync modes.

```c
tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();

/* TDB_SYNC_NONE - Fastest, least durable (OS handles flushing) */
cf_config.sync_mode = TDB_SYNC_NONE;

/* TDB_SYNC_INTERVAL - Balanced performance with periodic background syncing */
cf_config.sync_mode = TDB_SYNC_INTERVAL;
cf_config.sync_interval_us = 128000;  /* Sync every 128ms (default) */

/* TDB_SYNC_FULL - Most durable (fsync on every write) */
cf_config.sync_mode = TDB_SYNC_FULL;

tidesdb_create_column_family(db, "my_cf", &cf_config);
```

:::note[Sync Mode Options]
- **TDB_SYNC_NONE** · No explicit sync, relies on OS page cache (fastest, least durable)
    - Best for · Maximum throughput, acceptable data loss on crash
    - Use case · Caches, temporary data, reproducible workloads

- **TDB_SYNC_INTERVAL** · Periodic background syncing at configurable intervals (balanced)
    - Best for · Production workloads requiring good performance with bounded data loss
    - Use case · Most applications, configurable durability window
    - Features:
        - Single background sync thread monitors all column families using interval mode
        - Configurable sync interval via `sync_interval_us` (microseconds)
        - Structural operations (flush, compaction, WAL rotation) always enforce durability
        - At most `sync_interval_us` worth of data at risk on crash
        - **Mid-durability correctness**: When memtables rotate, the WAL receives an escalated fsync before entering the flush queue. Sorted run creation and merge operations always propagate full sync to block managers regardless of sync mode.

- **TDB_SYNC_FULL** · Fsync on every write operation (slowest, most durable)
    - Best for · Critical data requiring maximum durability
    - Use case · Financial transactions, audit logs, critical metadata
    - Note: Structural operations always use fsync regardless of sync mode
      :::

### Sync Interval Examples

```c
/* Sync every 100ms (good for low-latency requirements) */
cf_config.sync_mode = TDB_SYNC_INTERVAL;
cf_config.sync_interval_us = 100000;

/* Sync every 128ms (default) */
cf_config.sync_mode = TDB_SYNC_INTERVAL;
cf_config.sync_interval_us = 128000;

/* Sync every 1 second (higher throughput, more data at risk) */
cf_config.sync_mode = TDB_SYNC_INTERVAL;
cf_config.sync_interval_us = 1000000;
```

:::tip[Structural Operations]
Regardless of sync mode, TidesDB **always** enforces durability for structural operations:
- Memtable flush to SSTable
- SSTable compaction and merging
- WAL rotation
- Column family metadata updates

This ensures the database structure remains consistent even if user data syncing is delayed.
:::

## Compaction

TidesDB performs automatic background compaction when L1 reaches the configured file count trigger (default: 4 SSTables). However, you can manually trigger compaction for specific scenarios.

### Manual Compaction

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

/* Trigger compaction manually */
if (tidesdb_compact(cf) != 0)
{
    fprintf(stderr, "Failed to trigger compaction\n");
    return -1;
}
```

**When to use manual compaction**

- **After bulk deletes** · Reclaim disk space by removing tombstones and obsolete versions
- **After bulk updates** · Consolidate multiple versions of keys into single entries
- **Before read-heavy workloads** · Optimize read performance by reducing the number of levels to search
- **During maintenance windows** · Proactively compact during low-traffic periods to avoid compaction during peak load
- **After TTL expiration** · Remove expired entries to reclaim storage
- **Space optimization** · Force compaction to reduce space amplification when storage is constrained

**Behavior**

- Enqueues compaction work in the global compaction thread pool
- Returns immediately (non-blocking) - compaction runs asynchronously in background threads
- If compaction is already running for the column family, the call succeeds but doesn't queue duplicate work
- Compaction merges SSTables across levels, removes tombstones, expired TTL entries, and obsolete versions
- Thread-safe - can be called concurrently from multiple threads

**Performance considerations**

- Manual compaction uses the same thread pool as automatic background compaction
- Configure thread pool size via `config.num_compaction_threads` (default: 2)
- Compaction is I/O intensive - avoid triggering during peak write workloads
- Multiple column families can compact in parallel up to the thread pool limit

See [How does TidesDB work?](/getting-started/how-does-tidesdb-work#6-compaction-policy) for details on compaction algorithms, merge strategies, and parallel compaction.

## Thread Pools

TidesDB uses separate thread pools for flush and compaction operations. Understanding the parallelism model is important for optimal configuration.

**Parallelism semantics:**
- **Cross-CF parallelism** · Multiple flush/compaction workers CAN process different column families in parallel
- **Within-CF serialization** · A single column family can only have one flush and one compaction running at any time (enforced by atomic `is_flushing` and `is_compacting` flags)
- **No intra-CF memtable parallelism** · Even if a CF has multiple immutable memtables queued, they are flushed sequentially

**Thread pool sizing guidance:**
- **Single column family** · Set `num_flush_threads = 1` and `num_compaction_threads = 1`. Additional threads provide no benefit since only one operation per CF can run at a time - extra threads will simply wait idle.
- **Multiple column families** · Set thread counts up to the number of column families for maximum parallelism. With N column families and M workers (where M ≤ N), throughput scales linearly.

**Configuration**
```c
tidesdb_config_t config = {
    .db_path = "./mydb",
    .num_flush_threads = 2,                /* Flush thread pool size (default: 2) */
    .num_compaction_threads = 2,           /* Compaction thread pool size (default: 2) */
    .log_level = TDB_LOG_INFO,
    .block_cache_size = 64 * 1024 * 1024,  /* 64MB global block cache (default: 64MB) */
    .max_open_sstables = 256,              /* LRU cache for SSTable objects (default: 256, each has 2 FDs) */
};

tidesdb_t *db = NULL;
tidesdb_open(&config, &db);
```

:::note
`max_open_sstables` is a **storage-engine-level** configuration, not a column family configuration. It controls the LRU cache size for SSTable structures. Each SSTable uses 2 file descriptors (klog + vlog), so 256 SSTables = 512 file descriptors.
:::

See [How does TidesDB work?](/getting-started/how-does-tidesdb-work#75-thread-pool-architecture) for details on thread pool architecture and work distribution.
