/*
  Copyright (c) 2026 TidesDB

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/**
  @file ha_tidesdb.cc

  @brief
  The ha_tidesdb storage engine implementation backed by TidesDB.

  @details
  Each MySQL/MariaDB table maps to a TidesDB column family.
  Rows are stored as -- primary_key -> serialized_row_data

  For tables without explicit primary keys, we generate a hidden
  auto-increment key.
*/

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation  // gcc: Class implementation
#endif

#define MYSQL_SERVER 1
#include "ha_tidesdb.h"

#include <inttypes.h>
#include <math.h>
#include <my_global.h>
#include <mysql/plugin.h>
#include <mysql/service_encryption.h>
#include <tidesdb/tidesdb_version.h>

#include "key.h"
#include "myisampack.h"
#include "sql_class.h"

/* Fulltext search configuration */
static ulong tidesdb_ft_min_word_len = TIDESDB_DEFAULT_FT_MIN_WORD_LEN;
static ulong tidesdb_ft_max_word_len = TIDESDB_DEFAULT_FT_MAX_WORD_LEN;
static ulong tidesdb_ft_max_query_words = TIDESDB_DEFAULT_FT_MAX_QUERY_WORDS;

/* Portable case-insensitive string comparison */
#ifdef _WIN32
#include <direct.h> /* rmdir */
#define tidesdb_strcasecmp _stricmp
#else
#define tidesdb_strcasecmp strcasecmp
#endif

/* XA error codes (from X/Open XA specification) */
#ifndef XA_OK
#define XA_OK       0  /* Normal execution */
#define XAER_NOTA   -4 /* The XID is not valid */
#define XAER_INVAL  -5 /* Invalid arguments */
#define XAER_RMERR  -3 /* Resource manager error */
#define XAER_RMFAIL -7 /* Resource manager unavailable */
#endif

/* Forward declarations */
static handler *tidesdb_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);
static int tidesdb_init_func(void *p);
static int tidesdb_done_func(void *p);
static int tidesdb_commit(THD *thd, bool all);
static int tidesdb_rollback(THD *thd, bool all);
static uint tidesdb_partition_flags();
static bool tidesdb_show_status(handlerton *hton, THD *thd, stat_print_fn *stat_print,
                                enum ha_stat_type stat_type);

/* XA transaction support forward declarations */
static int tidesdb_xa_prepare(THD *thd, bool all);
static int tidesdb_xa_recover(XID *xid_list, uint len);
static int tidesdb_commit_by_xid(XID *xid);
static int tidesdb_rollback_by_xid(XID *xid);

/* Optimizer cost callback */
static void tidesdb_update_optimizer_costs(OPTIMIZER_COSTS *costs);

/* Consistent snapshot support */
static int tidesdb_start_consistent_snapshot(THD *thd);

/* XA transaction tracking structures (defined early for use in shutdown) */
struct tidesdb_xa_txn_t
{
    XID xid;
    tidesdb_txn_t *txn;
    tidesdb_xa_txn_t *next;
};
static tidesdb_xa_txn_t *tidesdb_prepared_xids = NULL;
static pthread_mutex_t tidesdb_xa_mutex;
static my_bool tidesdb_xa_mutex_initialized = FALSE;

/* Global TidesDB instance -- one database for all tables */
static tidesdb_t *tidesdb_instance = NULL;

static mysql_rwlock_t tidesdb_rwlock;

/* Handlerton for TidesDB */
handlerton *tidesdb_hton;

/* Hash for tracking open tables */
static HASH tidesdb_open_tables;

/* Data directory for TidesDB */
static char *tidesdb_data_dir = NULL;

/* System variables */
static ulong tidesdb_flush_threads = TIDESDB_DEFAULT_FLUSH_THREADS;
static ulong tidesdb_compaction_threads = TIDESDB_DEFAULT_COMPACTION_THREADS;
static ulonglong tidesdb_block_cache_size =
    TIDESDB_DEFAULT_BLOCK_CACHE_SIZE; /* 256MB -- matches InnoDB buffer pool default, we use for
                          block/node cache uses 512MB for global TidesDB instance */
static ulonglong tidesdb_write_buffer_size = TIDESDB_DEFAULT_WRITE_BUFFER_SIZE; /* 64MB */
static my_bool tidesdb_enable_compression = TRUE;
static my_bool tidesdb_enable_bloom_filter = TRUE;

/* Compression algorithm -- 0=none, 1=snappy, 2=lz4, 3=zstd, 4=lz4_fast */
static ulong tidesdb_compression_algo = TIDESDB_DEFAULT_COMPRESSION_ALGO; /* LZ4 default */
static const char *tidesdb_compression_names[] = {"none", "snappy",   "lz4",
                                                  "zstd", "lz4_fast", NullS};
static TYPELIB tidesdb_compression_typelib = {array_elements(tidesdb_compression_names) - 1,
                                              "tidesdb_compression_typelib",
                                              tidesdb_compression_names, NULL, NULL};

/* Sync mode -- 0=none, 1=interval, 2=full */
static ulong tidesdb_sync_mode = TIDESDB_DEFAULT_SYNC_MODE; /* full default (matches InnoDB's
                                                               innodb_flush_log_at_trx_commit=1) */
static const char *tidesdb_sync_mode_names[] = {"none", "interval", "full", NullS};
static TYPELIB tidesdb_sync_mode_typelib = {array_elements(tidesdb_sync_mode_names) - 1,
                                            "tidesdb_sync_mode_typelib", tidesdb_sync_mode_names,
                                            NULL, NULL};

/* Sync interval in microseconds (for interval mode) */
static ulonglong tidesdb_sync_interval_us = TIDESDB_DEFAULT_SYNC_INTERVAL_US; /* 128ms default */

/* Bloom filter false positive rate (0.0 to 1.0) */
static double tidesdb_bloom_fpr = TIDESDB_DEFAULT_BLOOM_FPR; /* 1% default */

/* Encryption settings */
static my_bool tidesdb_enable_encryption = FALSE;
static ulong tidesdb_encryption_key_id = TIDESDB_DEFAULT_ENCRYPTION_KEY_ID; /* Default key ID */

/* Default isolation level -- 0=read_uncommitted, 1=read_committed, 2=repeatable_read, 3=snapshot,
 * 4=serializable */
static ulong tidesdb_default_isolation = TIDESDB_DEFAULT_ISOLATION; /* repeatable_read default */
static const char *tidesdb_isolation_names[] = {
    "read_uncommitted", "read_committed", "repeatable_read", "snapshot", "serializable", NullS};
static TYPELIB tidesdb_isolation_typelib = {array_elements(tidesdb_isolation_names) - 1,
                                            "tidesdb_isolation_typelib", tidesdb_isolation_names,
                                            NULL, NULL};

/* Level size ratio for LSM compaction */
static ulong tidesdb_level_size_ratio = TIDESDB_DEFAULT_LEVEL_SIZE_RATIO;

/* Skip list configuration */
static ulong tidesdb_skip_list_max_level = TIDESDB_DEFAULT_SKIP_LIST_MAX_LEVEL;

/* Block index configuration */
static my_bool tidesdb_enable_block_indexes = TRUE;
static ulong tidesdb_index_sample_ratio = TIDESDB_DEFAULT_INDEX_SAMPLE_RATIO;

/* Default TTL in seconds (0 = no expiration) */
static ulonglong tidesdb_default_ttl = TIDESDB_DEFAULT_TTL;

/* Log level -- 0=debug, 1=info, 2=warn, 3=error, 4=fatal, 5=none */
static ulong tidesdb_log_level = TIDESDB_DEFAULT_LOG_LEVEL; /* info default */
static const char *tidesdb_log_level_names[] = {"debug", "info", "warn", "error",
                                                "fatal", "none", NullS};
static TYPELIB tidesdb_log_level_typelib = {array_elements(tidesdb_log_level_names) - 1,
                                            "tidesdb_log_level_typelib", tidesdb_log_level_names,
                                            NULL, NULL};

/* Additional LSM configuration from TidesDB C API */
static ulong tidesdb_min_levels = TIDESDB_DEFAULT_MIN_LEVELS; /* Minimum LSM levels */
static ulong tidesdb_dividing_level_offset =
    TIDESDB_DEFAULT_DIVIDING_LEVEL_OFFSET; /* Compaction dividing level offset */
static double tidesdb_skip_list_probability =
    TIDESDB_DEFAULT_SKIP_LIST_PROBABILITY; /* Skip list probability */
static ulong tidesdb_block_index_prefix_len =
    TIDESDB_DEFAULT_BLOCK_INDEX_PREFIX_LEN; /* Block index prefix length */
static ulonglong tidesdb_klog_value_threshold =
    TIDESDB_DEFAULT_KLOG_VALUE_THRESHOLD; /* Values > threshold go to vlog */
static ulonglong tidesdb_min_disk_space =
    TIDESDB_DEFAULT_MIN_DISK_SPACE; /* 100MB minimum disk space */
static ulong tidesdb_l1_file_count_trigger =
    TIDESDB_DEFAULT_L1_FILE_COUNT_TRIGGER; /* L1 file count trigger for compaction */
static ulong tidesdb_l0_queue_stall_threshold =
    TIDESDB_DEFAULT_L0_QUEUE_STALL_THRESHOLD; /* L0 queue stall threshold */
static ulong tidesdb_max_open_sstables =
    TIDESDB_DEFAULT_MAX_OPEN_SSTABLES; /* Max cached SSTable structures */

/* B+tree format for column families (faster point lookups via O(log N) traversal) */
static my_bool tidesdb_use_btree = TRUE;

/* Logging configuration */
static my_bool tidesdb_log_to_file = FALSE; /* Log to file instead of stderr */
static ulonglong tidesdb_log_truncation_at =
    TIDESDB_DEFAULT_LOG_TRUNCATION_AT; /* 24MB log truncation */

/* Active transaction buffer size for SSI conflict detection */
static ulonglong tidesdb_active_txn_buffer_size =
    TIDESDB_DEFAULT_ACTIVE_TXN_BUFFER_SIZE; /* 64KB default */

/**
  @brief
  Function to get key from share for hash lookup.
*/
static uchar *tidesdb_get_key(TIDESDB_SHARE *share, size_t *length,
                              my_bool not_used __attribute__((unused)))
{
    *length = share->table_name_length;
    return (uchar *)share->table_name;
}

/**
  @brief
  Get or create a share for the given table name.

*/
static TIDESDB_SHARE *get_share(const char *table_name, TABLE *table)
{
    TIDESDB_SHARE *share;
    uint length = (uint)strlen(table_name);
    char *tmp_name;

    mysql_rwlock_rdlock(&tidesdb_rwlock);
    share = (TIDESDB_SHARE *)my_hash_search(&tidesdb_open_tables, (uchar *)table_name, length);
    if (share)
    {
        my_atomic_add32_explicit((volatile int32 *)&share->use_count, 1, MY_MEMORY_ORDER_RELAXED);
        mysql_rwlock_unlock(&tidesdb_rwlock);
        return share;
    }
    mysql_rwlock_unlock(&tidesdb_rwlock);

    mysql_rwlock_wrlock(&tidesdb_rwlock);

    share = (TIDESDB_SHARE *)my_hash_search(&tidesdb_open_tables, (uchar *)table_name, length);
    if (share)
    {
        my_atomic_add32_explicit((volatile int32 *)&share->use_count, 1, MY_MEMORY_ORDER_RELAXED);
        mysql_rwlock_unlock(&tidesdb_rwlock);
        return share;
    }

    if (!(share =
              (TIDESDB_SHARE *)my_multi_malloc(PSI_INSTRUMENT_ME, MYF(MY_WME | MY_ZEROFILL), &share,
                                               sizeof(*share), &tmp_name, length + 1, NullS)))
    {
        mysql_rwlock_unlock(&tidesdb_rwlock);
        return NULL;
    }

    share->use_count = 1;
    share->table_name_length = length;
    share->table_name = tmp_name;
    strmov(share->table_name, table_name);
    share->cf = NULL;
    share->has_primary_key = false;
    share->pk_parts = 0;
    share->auto_increment_value = 1;
    share->auto_inc_loaded = false;
    share->row_count = 0;
    share->row_count_valid = false;
    share->hidden_pk_value = 0;
    share->cached_stats = NULL; /* unused -- stats fetched realtime */
    share->cached_stats_time = 0;
    share->tablespace_discarded = false;

    if (my_hash_insert(&tidesdb_open_tables, (uchar *)share)) goto error;
    thr_lock_init(&share->lock);
    pthread_mutex_init(&share->mutex, MY_MUTEX_INIT_FAST);
    pthread_mutex_init(&share->auto_inc_mutex, MY_MUTEX_INIT_FAST);
    pthread_mutex_init(&share->hidden_pk_mutex, MY_MUTEX_INIT_FAST);
    mysql_rwlock_unlock(&tidesdb_rwlock);
    return share;

error:
    pthread_mutex_destroy(&share->mutex);
    pthread_mutex_destroy(&share->auto_inc_mutex);
    pthread_mutex_destroy(&share->hidden_pk_mutex);
    my_free(share);
    mysql_rwlock_unlock(&tidesdb_rwlock);
    return NULL;
}

/**
  @brief
  Free a share when no longer needed.
*/
static int free_share(TIDESDB_SHARE *share)
{
    /* Atomic decrement -- if result > 0, no lock needed! */
    int32 new_count =
        my_atomic_add32_explicit((volatile int32 *)&share->use_count, -1, MY_MEMORY_ORDER_ACQ_REL) -
        1;
    if (new_count > 0) return 0;

    mysql_rwlock_wrlock(&tidesdb_rwlock);

    /* Another thread might have incremented use_count */
    if (my_atomic_load32_explicit((volatile int32 *)&share->use_count, MY_MEMORY_ORDER_ACQUIRE) ==
        0)
    {
        my_hash_delete(&tidesdb_open_tables, (uchar *)share);
        thr_lock_delete(&share->lock);
        pthread_mutex_destroy(&share->mutex);
        pthread_mutex_destroy(&share->auto_inc_mutex);
        pthread_mutex_destroy(&share->hidden_pk_mutex);
        if (share->cached_stats)
        {
            tidesdb_free_stats(share->cached_stats);
            share->cached_stats = NULL;
        }
        my_free(share); /* cached_stats kept for ABI compat but unused */
    }

    mysql_rwlock_unlock(&tidesdb_rwlock);
    return 0;
}

/**
  @brief
  Get realtime stats from TidesDB.  No caching -- TidesDB provides
  stats cheaply and caching causes stale counts that break the optimizer
  (e.g. reporting 0 rows when uncommitted txn ops exist).

  Caller must call tidesdb_free_stats() on the returned pointer.
  Returns NULL on failure.
*/
static tidesdb_stats_t *get_realtime_stats(TIDESDB_SHARE *share)
{
    if (!share || !share->cf) return NULL;

    tidesdb_stats_t *stats = NULL;
    if (tidesdb_get_stats(share->cf, &stats) == TDB_SUCCESS) return stats;
    return NULL;
}

/**
  @brief
  Extract the column family name from a full table path.
  Converts "database/table" to "database_table" for CF name.
  Sanitizes the result to remove any path separators that could cause
  issues on Windows or other platforms.
*/
static void get_cf_name(const char *table_path, char *cf_name, size_t cf_name_len)
{
    const char *db_start = table_path;
    const char *tbl_start = NULL;

    /* We find the database and table parts */
    /* Path format -- /path/to/datadir/database/table or C:\path\database\table */
    const char *p = table_path + strlen(table_path);
    int slashes = 0;

    while (p > table_path && slashes < 2)
    {
        p--;
        if (*p == '/' || *p == '\\')
        {
            slashes++;
            if (slashes == 1)
                tbl_start = p + 1;
            else if (slashes == 2)
                db_start = p + 1;
        }
    }

    if (tbl_start && db_start < tbl_start)
    {
        size_t db_len = tbl_start - db_start - 1;
        size_t tbl_len = strlen(tbl_start);

        if (db_len + 1 + tbl_len < cf_name_len)
        {
            memcpy(cf_name, db_start, db_len);
            cf_name[db_len] = '_';
            memcpy(cf_name + db_len + 1, tbl_start, tbl_len + 1);
        }
        else
        {
            strncpy(cf_name, tbl_start, cf_name_len - 1);
            cf_name[cf_name_len - 1] = '\0';
        }
    }
    else
    {
        strncpy(cf_name, table_path, cf_name_len - 1);
        cf_name[cf_name_len - 1] = '\0';
    }

    /*
      Sanitize CF name -- we replace any remaining path separators with underscores.
      This prevents issues on Windows where backslashes in CF names could be
      interpreted as directory separators when TidesDB constructs paths.
    */
    for (char *c = cf_name; *c; c++)
    {
        if (*c == '/' || *c == '\\') *c = '_';
    }
}

/* Savepoint data structure -- we must be defined before init function */
struct tidesdb_savepoint_t
{
    char name[TIDESDB_SAVEPOINT_NAME_LEN]; /* Savepoint name derived from pointer address */
    tidesdb_txn_t *txn;                    /* Transaction at time of savepoint (for reference) */
};

/* Forward declarations for savepoint functions */
static int tidesdb_savepoint_set(THD *thd, void *savepoint);
static int tidesdb_savepoint_rollback(THD *thd, void *savepoint);
static int tidesdb_savepoint_release(THD *thd, void *savepoint);

/**
  @brief
  Update optimizer costs for TidesDB (LSM-tree specific).

  LSM-tree characteristics that affect costs:
  -- Point lookups    -- Check memtable + bloom filters + multiple SSTable levels
  -- Range scans      -- Merge iterator across levels (more expensive than B-tree)
  -- Writes           -- Fast (append to memtable), but read amplification
  -- Block cache      -- Hot data is cached, reducing disk reads

  Cost adjustments vs default B-tree assumptions:
  -- key_lookup_cost      -- Higher due to multi-level search (but bloom filters help)
  -- row_lookup_cost      -- Similar to key lookup (data is with keys in LSM)
  -- key_next_find_cost   -- Higher due to merge iterator overhead
  -- disk_read_ratio      -- Lower if bloom filters enabled (fewer false reads)
*/
static void tidesdb_update_optimizer_costs(OPTIMIZER_COSTS *costs)
{
    /*
     * LSM-tree point lookup cost:
     * -- Memtable check      -- O(log N) in skip list
     * -- Bloom filter checks -- O(1) per SSTable, ~1% false positive
     * -- Level lookups       -- O(log N) per level, but bloom filters eliminate most
     *
     * With bloom filters enabled (default), point lookups are efficient.
     * Without bloom filters, cost increases significantly.
     */
    if (tidesdb_enable_bloom_filter)
    {
        /* Bloom filters reduce effective lookup cost significantly */
        costs->key_lookup_cost = TIDESDB_KEY_LOOKUP_COST_BTREE; /* Slightly higher than B-tree */
        costs->row_lookup_cost = TIDESDB_ROW_LOOKUP_COST_BTREE; /* Row fetch after key lookup */
    }
    else
    {
        /* Without bloom filters, must check more SSTables */
        costs->key_lookup_cost = TIDESDB_KEY_LOOKUP_COST_BLOCK; /* Much higher without bloom */
        costs->row_lookup_cost = TIDESDB_ROW_LOOKUP_COST_BLOCK;
    }

    /*
     * LSM-tree sequential access cost:
     * -- Merge iterator overhead -- O(log S) where S = number of sources
     * -- But sequential within each SSTable is efficient
     */
    costs->key_next_find_cost = TIDESDB_KEY_NEXT_FIND_COST; /* Merge iterator overhead */
    costs->row_next_find_cost = TIDESDB_ROW_NEXT_FIND_COST;

    /*
     * Key/row copy costs -- similar to other engines
     */
    costs->key_copy_cost = TIDESDB_KEY_COPY_COST;
    costs->row_copy_cost = TIDESDB_ROW_COPY_COST;

    /*
     * Disk read characteristics:
     * -- Block cache reduces disk reads significantly
     * -- 64KB blocks are read sequentially
     */
    costs->disk_read_cost = TIDESDB_DISK_READ_COST;   /* Cost per 4KB page */
    costs->disk_read_ratio = TIDESDB_DISK_READ_RATIO; /* 20% of reads hit disk (80% cached) */

    /*
     * Index block copy cost -- TidesDB uses 64KB blocks/nodes
     * (both block-based and B+tree formats use 64KB)
     */
    costs->index_block_copy_cost = TIDESDB_INDEX_BLOCK_COPY_COST;

    /*
     * Key comparison cost -- standard
     */
    costs->key_cmp_cost = TIDESDB_KEY_CMP_COST;

    /*
     * Rowid costs for MRR/rowid filter
     */
    costs->rowid_cmp_cost = TIDESDB_ROWID_CMP_COST;
    costs->rowid_copy_cost = TIDESDB_ROWID_COPY_COST;
}

/**
  Per-table CREATE TABLE options for TidesDB.

  These allow users to configure column family settings per table:
    CREATE TABLE t1 (...) ENGINE=TidesDB COMPRESSION='zstd'
      BLOOM_FILTER=1 BLOOM_FPR=0.001 WRITE_BUFFER_SIZE=134217728
      TTL=3600 USE_BTREE=1;

  When not specified, the global system variable defaults are used.
  A value of 0 for WRITE_BUFFER_SIZE means "use global default".
*/
static ha_create_table_option tidesdb_table_option_list[] = {
    /* Storage format */
    HA_TOPTION_STRING("COMPRESSION", compression), HA_TOPTION_BOOL("USE_BTREE", use_btree, 1),

    /* Memory / write path */
    HA_TOPTION_NUMBER("WRITE_BUFFER_SIZE", write_buffer_size, 0, 0, ULLONG_MAX, 0),
    HA_TOPTION_NUMBER("SKIP_LIST_MAX_LEVEL", skip_list_max_level, 0, 0, 32, 0),
    HA_TOPTION_NUMBER("SKIP_LIST_PROBABILITY", skip_list_probability, 0, 0, 5000, 0), /* x10000 */

    /* Bloom filter */
    HA_TOPTION_BOOL("BLOOM_FILTER", bloom_filter, 1),
    HA_TOPTION_NUMBER("BLOOM_FPR", bloom_fpr, 0, 0, 5000, 0), /* parts per 10000 */

    /* Block indexes */
    HA_TOPTION_BOOL("BLOCK_INDEXES", block_indexes, 1),
    HA_TOPTION_NUMBER("INDEX_SAMPLE_RATIO", index_sample_ratio, 0, 0, 64, 0),
    HA_TOPTION_NUMBER("BLOCK_INDEX_PREFIX_LEN", block_index_prefix_len, 0, 0, 256, 0),

    /* LSM compaction */
    HA_TOPTION_NUMBER("LEVEL_SIZE_RATIO", level_size_ratio, 0, 0, 100, 0),
    HA_TOPTION_NUMBER("MIN_LEVELS", min_levels, 0, 0, 20, 0),
    HA_TOPTION_NUMBER("DIVIDING_LEVEL_OFFSET", dividing_level_offset, 0, 0, 10, 0),
    HA_TOPTION_NUMBER("L1_FILE_COUNT_TRIGGER", l1_file_count_trigger, 0, 0, 100, 0),
    HA_TOPTION_NUMBER("L0_QUEUE_STALL_THRESHOLD", l0_queue_stall_threshold, 0, 0, 1000, 0),

    /* Durability */
    HA_TOPTION_STRING("SYNC_MODE", sync_mode),
    HA_TOPTION_NUMBER("SYNC_INTERVAL_US", sync_interval_us, 0, 0, 10000000, 0),

    /* Value log */
    HA_TOPTION_NUMBER("KLOG_VALUE_THRESHOLD", klog_value_threshold, 0, 0, 1048576, 0),

    /* Disk space */
    HA_TOPTION_NUMBER("MIN_DISK_SPACE", min_disk_space, 0, 0, ULLONG_MAX, 0),

    /* Isolation */
    HA_TOPTION_STRING("ISOLATION_LEVEL", isolation_level),

    /* TTL */
    HA_TOPTION_NUMBER("TTL", ttl, 0, 0, ULLONG_MAX, 0),

    HA_TOPTION_END};

/**
  @brief
  Initialize the TidesDB storage engine.
*/
static int tidesdb_init_func(void *p)
{
    DBUG_ENTER("tidesdb_init_func");

    tidesdb_hton = (handlerton *)p;

    mysql_rwlock_init(0, &tidesdb_rwlock);
    (void)my_hash_init(PSI_INSTRUMENT_ME, &tidesdb_open_tables, system_charset_info,
                       TIDESDB_OPEN_TABLES_HASH_SIZE, 0, 0, (my_hash_get_key)tidesdb_get_key, 0, 0);

    /* we Initialize XA mutex (can't use PTHREAD_MUTEX_INITIALIZER on Windows) */
    pthread_mutex_init(&tidesdb_xa_mutex, MY_MUTEX_INIT_FAST);
    tidesdb_xa_mutex_initialized = TRUE;

    tidesdb_hton->create = tidesdb_create_handler;
    tidesdb_hton->flags = HTON_CLOSE_CURSORS_AT_COMMIT | HTON_SUPPORTS_EXTENDED_KEYS;
    tidesdb_hton->commit = tidesdb_commit;
    tidesdb_hton->rollback = tidesdb_rollback;
    tidesdb_hton->show_status = tidesdb_show_status;

    /* Savepoint support */
    tidesdb_hton->savepoint_offset = sizeof(tidesdb_savepoint_t);
    tidesdb_hton->savepoint_set = tidesdb_savepoint_set;
    tidesdb_hton->savepoint_rollback = tidesdb_savepoint_rollback;
    tidesdb_hton->savepoint_release = tidesdb_savepoint_release;

    /* XA transaction support (2-phase commit) */
    tidesdb_hton->prepare = tidesdb_xa_prepare;
    tidesdb_hton->recover = tidesdb_xa_recover;
    tidesdb_hton->commit_by_xid = tidesdb_commit_by_xid;
    tidesdb_hton->rollback_by_xid = tidesdb_rollback_by_xid;

    /* Partitioning support */
    tidesdb_hton->partition_flags = tidesdb_partition_flags;

    /* LSM-tree specific optimizer costs */
    tidesdb_hton->update_optimizer_costs = tidesdb_update_optimizer_costs;

    /* Consistent snapshot support for START TRANSACTION WITH CONSISTENT SNAPSHOT */
    tidesdb_hton->start_consistent_snapshot = tidesdb_start_consistent_snapshot;

    /* Per-table CREATE TABLE options (compression, bloom, buffer size, TTL, etc.) */
    tidesdb_hton->table_options = tidesdb_table_option_list;

    /* We initialize TidesDB instance */
    static char db_path[FN_REFLEN]; /* Static to ensure it persists */
    if (tidesdb_data_dir)
    {
        strncpy(db_path, tidesdb_data_dir, sizeof(db_path) - 1);
    }
    else
    {
        /*
          Default to MySQL/MariaDB data directory + tidesdb.

          We must use an absolute path here. mysql_data_home can be
          a relative path (e.g., "." or "./data"), which causes problems when
          running parallel MTR tests -- all workers would share the same relative
          path and conflict with each other.

          We use mysql_real_data_home which is the absolute path version.
        */
        snprintf(db_path, sizeof(db_path), "%s" TIDESDB_PATH_SEP_STR "tidesdb",
                 mysql_real_data_home);
    }
    db_path[sizeof(db_path) - 1] = '\0';

    tidesdb_config_t config = tidesdb_default_config();
    config.db_path = db_path;
    config.num_flush_threads = (int)tidesdb_flush_threads;
    config.num_compaction_threads = (int)tidesdb_compaction_threads;
    config.log_level = (tidesdb_log_level_t)tidesdb_log_level;
    config.block_cache_size = tidesdb_block_cache_size;
    config.max_open_sstables = (int)tidesdb_max_open_sstables;
    config.log_to_file = tidesdb_log_to_file ? 1 : 0;
    config.log_truncation_at = tidesdb_log_truncation_at;

    if (tidesdb_open(&config, &tidesdb_instance) != TDB_SUCCESS)
    {
        sql_print_error("TidesDB: Failed to open database at %s", db_path);
        DBUG_RETURN(1);
    }

    sql_print_information("TidesDB: Storage engine initialized at %s", db_path);

    DBUG_RETURN(0);
}

/**
  @brief
  Shutdown the TidesDB storage engine.
*/
static int tidesdb_done_func(void *p)
{
    int error = 0;
    DBUG_ENTER("tidesdb_done_func");

    if (tidesdb_open_tables.records) error = 1;

    my_hash_free(&tidesdb_open_tables);
    mysql_rwlock_destroy(&tidesdb_rwlock);

    /* We clean up any remaining prepared XA transactions */
    if (tidesdb_xa_mutex_initialized)
    {
        pthread_mutex_lock(&tidesdb_xa_mutex);
        while (tidesdb_prepared_xids)
        {
            tidesdb_xa_txn_t *xa_txn = tidesdb_prepared_xids;
            tidesdb_prepared_xids = xa_txn->next;
            if (xa_txn->txn)
            {
                tidesdb_txn_rollback(xa_txn->txn);
                tidesdb_txn_free(xa_txn->txn);
            }
            my_free(xa_txn);
        }
        pthread_mutex_unlock(&tidesdb_xa_mutex);
        pthread_mutex_destroy(&tidesdb_xa_mutex);
        tidesdb_xa_mutex_initialized = FALSE;
    }

    if (tidesdb_instance)
    {
        tidesdb_close(tidesdb_instance);
        tidesdb_instance = NULL;
    }

    sql_print_information("TidesDB: Storage engine shutdown");

    DBUG_RETURN(error);
}

/**
  @brief
  Return partition flags for TidesDB.

  TidesDB supports native partitioning through MariaDB's ha_partition wrapper.
  Each partition maps to a separate TidesDB column family.
*/
static uint tidesdb_partition_flags()
{
    return HA_PARTITION_FUNCTION_SUPPORTED;
}

/**
  Thread-local transaction storage.
  We use thd_get_ha_data/thd_set_ha_data to store the transaction per-thread.
*/
static tidesdb_txn_t *get_thd_txn(THD *thd, handlerton *hton)
{
    return (tidesdb_txn_t *)thd_get_ha_data(thd, hton);
}

static void set_thd_txn(THD *thd, handlerton *hton, tidesdb_txn_t *txn)
{
    thd_set_ha_data(thd, hton, txn);
}

/* Forward declaration -- defined later near external_lock */
static int map_isolation_level(enum_tx_isolation mysql_iso);

/**
  @brief
  Map TidesDB error codes to MariaDB handler error codes.

  Critical for correctness: TDB_ERR_CONFLICT must map to HA_ERR_LOCK_DEADLOCK
  so MariaDB's SQL layer can automatically retry the statement.
*/
static int map_tidesdb_error(int tdb_err)
{
    switch (tdb_err)
    {
        case TDB_SUCCESS:
            return 0;
        case TDB_ERR_CONFLICT:
            return HA_ERR_LOCK_DEADLOCK;
        case TDB_ERR_NOT_FOUND:
            return HA_ERR_KEY_NOT_FOUND;
        case TDB_ERR_EXISTS:
            return HA_ERR_FOUND_DUPP_KEY;
        case TDB_ERR_MEMORY:
        case TDB_ERR_MEMORY_LIMIT:
            return HA_ERR_OUT_OF_MEM;
        case TDB_ERR_CORRUPTION:
            return HA_ERR_CRASHED;
        case TDB_ERR_TOO_LARGE:
            return HA_ERR_TO_BIG_ROW;
        default:
            return HA_ERR_GENERIC;
    }
}

/**
  @brief
  Commit a transaction.

  Called by MySQL/MariaDB when COMMIT is issued or when auto-commit commits
  a statement. For multi-statement transactions, this commits the
  THD-level transaction.
*/
static int tidesdb_commit(THD *thd, bool all)
{
    DBUG_ENTER("tidesdb_commit");

    tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);

    if (!txn) DBUG_RETURN(0);

    /*
      Determine whether this call should actually commit:
      -- all=true  -> always commit (explicit COMMIT or final commit)
      -- all=false -> commit only when NOT inside any form of transaction.
         This handles autocommit=1 single statements (the only case where
         all=false is the real commit).  For autocommit=0 or explicit BEGIN,
         all=false is a statement-end no-op; the real commit comes with
         all=true from COMMIT.
    */
    bool do_commit = all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);

    if (do_commit)
    {
        int ret = tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);
        set_thd_txn(thd, tidesdb_hton, NULL);

        if (ret != TDB_SUCCESS)
        {
            sql_print_error("TidesDB: Failed to commit transaction: %d", ret);
            DBUG_RETURN(map_tidesdb_error(ret));
        }
        DBUG_PRINT("info", ("TidesDB: Transaction committed"));
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Rollback a transaction.

  Called by MySQL/MariaDB when ROLLBACK is issued. For multi-statement
  transactions, this rolls back the THD-level transaction.
*/
static int tidesdb_rollback(THD *thd, bool all)
{
    DBUG_ENTER("tidesdb_rollback");

    tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);

    if (!txn) DBUG_RETURN(0);

    /* Same logic as tidesdb_commit: rollback when all=true or when
       not inside any form of transaction (autocommit=1 single statement). */
    bool do_rollback = all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);

    if (do_rollback)
    {
        int ret = tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
        set_thd_txn(thd, tidesdb_hton, NULL);

        if (ret != TDB_SUCCESS)
        {
            sql_print_error("TidesDB: Failed to rollback transaction: %d", ret);
            DBUG_RETURN(HA_ERR_GENERIC);
        }
        DBUG_PRINT("info", ("TidesDB: Transaction rolled back"));
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Start a consistent snapshot for the current transaction.

  Called when START TRANSACTION WITH CONSISTENT SNAPSHOT is issued.
  TidesDB's MVCC provides snapshot isolation -- we begin a transaction
  with snapshot isolation level to ensure a consistent view of data.

  This allows multiple engines to participate in a consistent snapshot
  across the entire database.
*/
static int tidesdb_start_consistent_snapshot(THD *thd)
{
    DBUG_ENTER("tidesdb_start_consistent_snapshot");

    /* We check if we already have a transaction */
    tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);
    if (txn)
    {
        /* Transaction already exists -- snapshot is already established */
        DBUG_RETURN(0);
    }

    /* We begin a new transaction with REPEATABLE_READ for true snapshot isolation.
       tidesdb_txn_begin() defaults to READ_COMMITTED which refreshes snapshot
       on each read -- that breaks START TRANSACTION WITH CONSISTENT SNAPSHOT. */
    int ret =
        tidesdb_txn_begin_with_isolation(tidesdb_instance, TDB_ISOLATION_REPEATABLE_READ, &txn);
    if (ret != TDB_SUCCESS)
    {
        sql_print_error("TidesDB: Failed to begin consistent snapshot transaction: %d", ret);
        DBUG_RETURN(1);
    }

    /* We store the transaction in THD for later use */
    set_thd_txn(thd, tidesdb_hton, txn);

    /* We register with transaction coordinator so commit/rollback handlers are called */
    trans_register_ha(thd, TRUE, tidesdb_hton, 0);

    DBUG_PRINT("info", ("TidesDB: Consistent snapshot started"));
    DBUG_RETURN(0);
}

/**
  @brief
  Prepare a transaction for XA 2-phase commit.

  This is called during the first phase of 2PC. The transaction
  is prepared but not yet committed. It can be committed or rolled
  back later using commit_by_xid or rollback_by_xid.
*/
static int tidesdb_xa_prepare(THD *thd, bool all)
{
    DBUG_ENTER("tidesdb_xa_prepare");

    tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);

    if (!txn)
    {
        /* No active transaction -- nothing to prepare */
        DBUG_RETURN(0);
    }

    if (!all)
    {
        /* Statement-level prepare -- just return success */
        DBUG_RETURN(0);
    }

    /*
      For XA PREPARE, we need to:
      1. Flush the transaction to WAL (ensures durability)
      2. Keep the transaction in prepared state
      3. Store the XID for later recovery
    */
    const XID *xid = thd->get_xid();

    /* We store the prepared transaction for potential recovery */
    pthread_mutex_lock(&tidesdb_xa_mutex);

    tidesdb_xa_txn_t *xa_txn = (tidesdb_xa_txn_t *)my_malloc(
        PSI_INSTRUMENT_ME, sizeof(tidesdb_xa_txn_t), MYF(MY_WME | MY_ZEROFILL));
    if (xa_txn)
    {
        memcpy(&xa_txn->xid, xid, sizeof(XID));
        xa_txn->txn = txn;
        xa_txn->next = tidesdb_prepared_xids;
        tidesdb_prepared_xids = xa_txn;
    }

    pthread_mutex_unlock(&tidesdb_xa_mutex);

    /*
     ** TidesDB transactions are already durable when operations are performed.
      The prepare phase just marks the transaction as ready for commit.
      We keep the transaction handle for later commit/rollback by XID.
    */
    set_thd_txn(thd, tidesdb_hton, NULL);

    sql_print_information("TidesDB: XA transaction prepared");

    DBUG_RETURN(0);
}

/**
  @brief
  Recover prepared XA transactions after crash.

  This is called during server startup to find any transactions
  that were prepared but not yet committed before a crash.

  @return Number of prepared transactions found
*/
static int tidesdb_xa_recover(XID *xid_list, uint len)
{
    DBUG_ENTER("tidesdb_xa_recover");

    if (len == 0 || xid_list == NULL)
    {
        DBUG_RETURN(0);
    }

    uint count = 0;

    pthread_mutex_lock(&tidesdb_xa_mutex);

    tidesdb_xa_txn_t *xa_txn = tidesdb_prepared_xids;
    while (xa_txn && count < len)
    {
        memcpy(&xid_list[count], &xa_txn->xid, sizeof(XID));
        count++;
        xa_txn = xa_txn->next;
    }

    pthread_mutex_unlock(&tidesdb_xa_mutex);

    if (count > 0)
    {
        sql_print_information("TidesDB: Recovered %u prepared XA transactions", count);
    }

    DBUG_RETURN(count);
}

/**
  @brief
  Commit a prepared XA transaction by XID.

  This is called to commit a transaction that was previously prepared.
*/
static int tidesdb_commit_by_xid(XID *xid)
{
    DBUG_ENTER("tidesdb_commit_by_xid");

    if (!xid)
    {
        DBUG_RETURN(XAER_INVAL);
    }

    pthread_mutex_lock(&tidesdb_xa_mutex);

    /** We find the prepared transaction by XID */
    tidesdb_xa_txn_t *xa_txn = tidesdb_prepared_xids;
    tidesdb_xa_txn_t *prev = NULL;

    while (xa_txn)
    {
        if (memcmp(&xa_txn->xid, xid, sizeof(XID)) == 0)
        {
            /* We found the transaction -- we remove from list */
            if (prev)
                prev->next = xa_txn->next;
            else
                tidesdb_prepared_xids = xa_txn->next;

            pthread_mutex_unlock(&tidesdb_xa_mutex);

            if (xa_txn->txn)
            {
                int ret = tidesdb_txn_commit(xa_txn->txn);
                tidesdb_txn_free(xa_txn->txn);

                if (ret != TDB_SUCCESS)
                {
                    sql_print_error("TidesDB: XA commit failed: %d", ret);
                    my_free(xa_txn);
                    DBUG_RETURN(XAER_RMERR);
                }
            }

            my_free(xa_txn);
            sql_print_information("TidesDB: XA transaction committed by XID");
            DBUG_RETURN(XA_OK);
        }
        prev = xa_txn;
        xa_txn = xa_txn->next;
    }

    pthread_mutex_unlock(&tidesdb_xa_mutex);

    DBUG_RETURN(XAER_NOTA);
}

/**
  @brief
  Rollback a prepared XA transaction by XID.

  This is called to rollback a transaction that was previously prepared.
*/
static int tidesdb_rollback_by_xid(XID *xid)
{
    DBUG_ENTER("tidesdb_rollback_by_xid");

    if (!xid)
    {
        DBUG_RETURN(XAER_INVAL);
    }

    pthread_mutex_lock(&tidesdb_xa_mutex);

    /* We find the prepared transaction by XID */
    tidesdb_xa_txn_t *xa_txn = tidesdb_prepared_xids;
    tidesdb_xa_txn_t *prev = NULL;

    while (xa_txn)
    {
        if (memcmp(&xa_txn->xid, xid, sizeof(XID)) == 0)
        {
            /* We found the transaction -- we remove from list */
            if (prev)
                prev->next = xa_txn->next;
            else
                tidesdb_prepared_xids = xa_txn->next;

            pthread_mutex_unlock(&tidesdb_xa_mutex);

            if (xa_txn->txn)
            {
                int ret = tidesdb_txn_rollback(xa_txn->txn);
                tidesdb_txn_free(xa_txn->txn);

                if (ret != TDB_SUCCESS)
                {
                    sql_print_error("TidesDB: XA rollback failed: %d", ret);
                    my_free(xa_txn);
                    DBUG_RETURN(XAER_RMERR);
                }
            }

            my_free(xa_txn);
            sql_print_information("TidesDB: XA transaction rolled back by XID");
            DBUG_RETURN(XA_OK);
        }
        prev = xa_txn;
        xa_txn = xa_txn->next;
    }

    pthread_mutex_unlock(&tidesdb_xa_mutex);

    DBUG_RETURN(XAER_NOTA);
}

/**
  @brief
  Set a transaction savepoint.

  Creates a savepoint in the current TidesDB transaction using
  tidesdb_txn_savepoint().
*/
static int tidesdb_savepoint_set(THD *thd, void *savepoint)
{
    DBUG_ENTER("tidesdb_savepoint_set");

    tidesdb_savepoint_t *sp = (tidesdb_savepoint_t *)savepoint;

    /* We generate a unique savepoint name from the pointer address */
    snprintf(sp->name, sizeof(sp->name), "sp_%" PRIxPTR, (uintptr_t)savepoint);

    tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);

    if (txn)
    {
        int ret = tidesdb_txn_savepoint(txn, sp->name);
        if (ret != TDB_SUCCESS)
        {
            sql_print_error("TidesDB: Failed to create savepoint '%s': %d", sp->name, ret);
            DBUG_RETURN(HA_ERR_GENERIC);
        }
        sp->txn = txn;
    }
    else
    {
        sp->txn = NULL;
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Rollback to a transaction savepoint.

  Rolls back all changes made after the savepoint was set using
  tidesdb_txn_rollback_to_savepoint().
*/
static int tidesdb_savepoint_rollback(THD *thd, void *savepoint)
{
    DBUG_ENTER("tidesdb_savepoint_rollback");

    tidesdb_savepoint_t *sp = (tidesdb_savepoint_t *)savepoint;

    tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);

    if (txn)
    {
        int ret = tidesdb_txn_rollback_to_savepoint(txn, sp->name);
        if (ret != TDB_SUCCESS)
        {
            sql_print_error("TidesDB: Failed to rollback to savepoint '%s': %d", sp->name, ret);
            DBUG_RETURN(HA_ERR_NO_SAVEPOINT);
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Release a transaction savepoint.

  Releases the savepoint without rolling back using
  tidesdb_txn_release_savepoint().
*/
static int tidesdb_savepoint_release(THD *thd, void *savepoint)
{
    DBUG_ENTER("tidesdb_savepoint_release");

    tidesdb_savepoint_t *sp = (tidesdb_savepoint_t *)savepoint;

    tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);

    if (txn)
    {
        int ret = tidesdb_txn_release_savepoint(txn, sp->name);
        if (ret != TDB_SUCCESS)
        {
            sql_print_error("TidesDB: Failed to release savepoint '%s': %d", sp->name, ret);
            DBUG_RETURN(HA_ERR_NO_SAVEPOINT);
        }
        DBUG_PRINT("info", ("TidesDB: Savepoint '%s' released", sp->name));
    }
    else
    {
        /* No active transaction -- nothing to release */
        DBUG_PRINT("info", ("TidesDB: No active transaction for savepoint release"));
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Show TidesDB engine status.

  Called by SHOW ENGINE TIDESDB STATUS.
*/
static bool tidesdb_show_status(handlerton *hton, THD *thd, stat_print_fn *stat_print,
                                enum ha_stat_type stat_type)
{
    DBUG_ENTER("tidesdb_show_status");

    if (stat_type != HA_ENGINE_STATUS)
    {
        DBUG_RETURN(FALSE);
    }

    const size_t buf_size = TIDESDB_STATUS_BUF_SIZE;
    char *buf = (char *)my_malloc(PSI_INSTRUMENT_ME, buf_size, MYF(MY_WME));
    if (!buf) DBUG_RETURN(TRUE);

    int buf_len = 0;

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "                            TIDESDB ENGINE STATUS\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "\n");

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "BLOCK CACHE\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n");

    tidesdb_cache_stats_t cache_stats;
    if (tidesdb_get_cache_stats(tidesdb_instance, &cache_stats) == TDB_SUCCESS)
    {
        buf_len +=
            snprintf(buf + buf_len, buf_size - buf_len,
                     "| %-24s | %-20s |\n"
                     "| %-24s | %-20s |\n"
                     "| %-24s | %-20zu |\n"
                     "| %-24s | %-17.2f MB |\n"
                     "| %-24s | %-20" PRIu64
                     " |\n"
                     "| %-24s | %-20" PRIu64
                     " |\n"
                     "| %-24s | %-18.2f %% |\n"
                     "| %-24s | %-20zu |\n",
                     "Status", cache_stats.enabled ? "ENABLED" : "DISABLED", "State",
                     cache_stats.enabled ? "ACTIVE" : "INACTIVE", "Entries",
                     cache_stats.total_entries, "Size", cache_stats.total_bytes / (1024.0 * 1024.0),
                     "Hits", cache_stats.hits, "Misses", cache_stats.misses, "Hit Rate",
                     cache_stats.hit_rate * 100.0, "Partitions", cache_stats.num_partitions);
    }
    else
    {
        buf_len += snprintf(buf + buf_len, buf_size - buf_len, "| %-24s | %-20s |\n", "Status",
                            "UNAVAILABLE");
    }

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "THREAD POOLS\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "| %-24s | %-20lu |\n"
        "| %-24s | %-20lu |\n",
        "Flush Threads", tidesdb_flush_threads, "Compaction Threads", tidesdb_compaction_threads);

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "MEMORY\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "| %-24s | %-17.2f MB |\n"
        "| %-24s | %-17.2f MB |\n",
        "Block Cache Size", tidesdb_block_cache_size / (1024.0 * 1024.0), "Write Buffer Size",
        tidesdb_write_buffer_size / (1024.0 * 1024.0));

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "COMPRESSION\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "| %-24s | %-20s |\n"
        "| %-24s | %-20s |\n",
        "Enabled", tidesdb_enable_compression ? "YES" : "NO", "Algorithm",
        tidesdb_compression_names[tidesdb_compression_algo]);

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "BLOOM FILTER\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "| %-24s | %-20s |\n"
        "| %-24s | %-18.2f %% |\n",
        "Enabled", tidesdb_enable_bloom_filter ? "YES" : "NO", "False Positive Rate",
        tidesdb_bloom_fpr * 100.0);

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "DURABILITY\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "| %-24s | %-20s |\n"
        "| %-24s | %-17llu us |\n",
        "Sync Mode", tidesdb_sync_mode_names[tidesdb_sync_mode], "Sync Interval",
        (unsigned long long)tidesdb_sync_interval_us);

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "TRANSACTIONS\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "| %-24s | %-20s |\n"
        "| %-24s | %-20s |\n"
        "| %-24s | %-20s |\n",
        "Default Isolation", tidesdb_isolation_names[tidesdb_default_isolation], "XA Support",
        "YES", "Savepoints", "YES");

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "LSM TREE\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "| %-24s | %-20lu |\n"
        "| %-24s | %-20lu |\n"
        "| %-24s | %-20lu |\n"
        "| %-24s | %-20lu |\n"
        "| %-24s | %-20lu |\n",
        "Level Size Ratio", tidesdb_level_size_ratio, "Min Levels", tidesdb_min_levels,
        "Skip List Max Level", tidesdb_skip_list_max_level, "L1 File Count Trigger",
        tidesdb_l1_file_count_trigger, "L0 Stall Threshold", tidesdb_l0_queue_stall_threshold);

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "STORAGE\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "| %-24s | %-20lu |\n"
        "| %-24s | %-20lu |\n"
        "| %-24s | %-20s |\n",
        "Open Tables", (unsigned long)tidesdb_open_tables.records, "Max Open SSTables",
        tidesdb_max_open_sstables, "Block Indexes",
        tidesdb_enable_block_indexes ? "ENABLED" : "DISABLED");

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "TTL\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "| %-24s | %-17llu s |\n",
        "Default TTL", (unsigned long long)tidesdb_default_ttl);

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "INDEX FORMAT\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "| %-24s | %-20s |\n",
        "Default Format", tidesdb_use_btree ? "B+TREE" : "SKIP-LIST");

    /* Per-column family statistics */
    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "COLUMN FAMILY STATISTICS\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n");

    char **cf_names = NULL;
    int cf_count = 0;
    if (tidesdb_list_column_families(tidesdb_instance, &cf_names, &cf_count) == TDB_SUCCESS)
    {
        buf_len += snprintf(buf + buf_len, buf_size - buf_len, "| %-24s | %-20d |\n\n",
                            "Total Column Families", cf_count);

        for (int i = 0; i < cf_count && buf_len < (int)(buf_size - 2048); i++)
        {
            tidesdb_column_family_t *cf = tidesdb_get_column_family(tidesdb_instance, cf_names[i]);
            if (cf)
            {
                tidesdb_stats_t *cf_stats = NULL;
                if (tidesdb_get_stats(cf, &cf_stats) == TDB_SUCCESS && cf_stats)
                {
                    buf_len +=
                        snprintf(buf + buf_len, buf_size - buf_len, "--- %s ---\n", cf_names[i]);
                    buf_len +=
                        snprintf(buf + buf_len, buf_size - buf_len, "  Format:          %s\n",
                                 cf_stats->use_btree ? "B+TREE" : "SKIP-LIST");
                    buf_len += snprintf(buf + buf_len, buf_size - buf_len,
                                        "  Total Keys:      %" PRIu64 "\n", cf_stats->total_keys);
                    buf_len +=
                        snprintf(buf + buf_len, buf_size - buf_len, "  Data Size:       %.2f MB\n",
                                 cf_stats->total_data_size / (1024.0 * 1024.0));
                    buf_len +=
                        snprintf(buf + buf_len, buf_size - buf_len, "  Memtable Size:   %.2f KB\n",
                                 cf_stats->memtable_size / 1024.0);
                    buf_len += snprintf(buf + buf_len, buf_size - buf_len,
                                        "  LSM Levels:      %d\n", cf_stats->num_levels);
                    buf_len += snprintf(buf + buf_len, buf_size - buf_len,
                                        "  Read Amp:        %.2f\n", cf_stats->read_amp);
                    buf_len += snprintf(buf + buf_len, buf_size - buf_len,
                                        "  Cache Hit Rate:  %.1f%%\n", cf_stats->hit_rate * 100.0);
                    buf_len += snprintf(buf + buf_len, buf_size - buf_len,
                                        "  Avg Key Size:    %.1f bytes\n", cf_stats->avg_key_size);
                    buf_len +=
                        snprintf(buf + buf_len, buf_size - buf_len,
                                 "  Avg Value Size:  %.1f bytes\n", cf_stats->avg_value_size);

                    /* B+tree specific stats */
                    if (cf_stats->use_btree)
                    {
                        buf_len += snprintf(buf + buf_len, buf_size - buf_len,
                                            "  B+tree Nodes:    %" PRIu64 "\n",
                                            cf_stats->btree_total_nodes);
                        buf_len +=
                            snprintf(buf + buf_len, buf_size - buf_len, "  B+tree Max Height: %u\n",
                                     cf_stats->btree_max_height);
                        buf_len +=
                            snprintf(buf + buf_len, buf_size - buf_len,
                                     "  B+tree Avg Height: %.2f\n", cf_stats->btree_avg_height);
                    }

                    /* Per-level stats (compact format) */
                    if (cf_stats->num_levels > 0)
                    {
                        buf_len += snprintf(buf + buf_len, buf_size - buf_len, "  Levels: ");
                        for (int lvl = 0; lvl < cf_stats->num_levels && lvl < 7; lvl++)
                        {
                            buf_len +=
                                snprintf(buf + buf_len, buf_size - buf_len, "L%d(%d/%.1fMB) ", lvl,
                                         cf_stats->level_num_sstables[lvl],
                                         cf_stats->level_sizes[lvl] / (1024.0 * 1024.0));
                        }
                        buf_len += snprintf(buf + buf_len, buf_size - buf_len, "\n");
                    }

                    buf_len += snprintf(buf + buf_len, buf_size - buf_len, "\n");
                    tidesdb_free_stats(cf_stats);
                }
            }
            free(cf_names[i]);
        }
        free(cf_names);
    }
    else
    {
        buf_len += snprintf(buf + buf_len, buf_size - buf_len, "| %-24s | %-20s |\n", "Status",
                            "UNAVAILABLE");
    }

    buf_len += snprintf(
        buf + buf_len, buf_size - buf_len,
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n"
        "                         END OF TIDESDB ENGINE STATUS\n"
        "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n");

    stat_print(thd, "TidesDB", 7, "", 0, buf, buf_len);

    my_free(buf);

    DBUG_RETURN(FALSE);
}

/**
  @brief
  Create a new handler instance.
*/
static handler *tidesdb_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
    return new (mem_root) ha_tidesdb(hton, table);
}

/* File extensions for TidesDB tables */
static const char *ha_tidesdb_exts[] = {NullS};

const char **ha_tidesdb::bas_ext() const
{
    return ha_tidesdb_exts;
}

/**
  @brief
  Constructor for ha_tidesdb handler.
*/
ha_tidesdb::ha_tidesdb(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg),
      share(NULL),
      current_txn(NULL),
      scan_txn_owned(false),
      is_read_only_scan(false),
      scan_iter(NULL),
      scan_initialized(false),
      pk_buffer(NULL),
      pk_buffer_len(0),
      row_buffer(NULL),
      row_buffer_len(0),
      current_key(NULL),
      current_key_len(0),
      current_key_capacity(0),
      bulk_insert_active(false),
      bulk_txn(NULL),
      bulk_insert_rows(0),
      indexes_disabled(false),
      prefetch_active(false),
      skip_dup_check(false),
      pack_buffer(NULL),
      pack_buffer_capacity(0),
      pushed_idx_cond(NULL),
      pushed_idx_cond_keyno(MAX_KEY),
      pushed_cond(NULL),
      keyread_only(false),
      txn_read_only(false),
      cached_now(0),
      semi_consistent_read_enabled(false),
      did_semi_consistent_read(false),
      idx_key_buffer(NULL),
      idx_key_buffer_capacity(0),
      index_iter(NULL),
      index_key_buf(NULL),
      index_key_len(0),
      index_key_buf_capacity(0),
      saved_key_buffer(NULL),
      saved_key_buffer_capacity(0),
      idx_pk_buffer(NULL),
      idx_pk_buffer_capacity(0)
{
}

/**
  @brief
  Destructor for ha_tidesdb handler.
*/
ha_tidesdb::~ha_tidesdb()
{
    if (current_key) my_free(current_key);
    if (pk_buffer) my_free(pk_buffer);
    if (row_buffer) my_free(row_buffer);
    if (scan_iter) tidesdb_iter_free(scan_iter);
    if (index_iter) tidesdb_iter_free(index_iter);
    if (index_key_buf) my_free(index_key_buf);

    if (pack_buffer) my_free(pack_buffer);
    if (idx_key_buffer) my_free(idx_key_buffer);
    if (saved_key_buffer) my_free(saved_key_buffer);
    if (idx_pk_buffer) my_free(idx_pk_buffer);
}

/**
  @brief
  Reset the current key length (buffer is pre-allocated and reused).
*/
void ha_tidesdb::free_current_key()
{
    current_key_len = 0;
}

/**
  @brief
  Pack a MySQL/MariaDB row into a byte buffer for storage.

  Uses MySQL's/MariaDB's native row format.

*/
int ha_tidesdb::pack_row(const uchar *buf, uchar **packed, size_t *packed_len)
{
    DBUG_ENTER("ha_tidesdb::pack_row");

    size_t row_len = table->s->reclength;

    size_t total_len = row_len;
    for (uint i = 0; i < table->s->fields; i++)
    {
        Field *field = table->field[i];
        if (field->type() == MYSQL_TYPE_BLOB || field->type() == MYSQL_TYPE_MEDIUM_BLOB ||
            field->type() == MYSQL_TYPE_LONG_BLOB || field->type() == MYSQL_TYPE_TINY_BLOB ||
            field->type() == MYSQL_TYPE_GEOMETRY)
        {
            total_len += TIDESDB_BLOB_LEN_PREFIX_SIZE;
            if (!field->is_null())
            {
                String str;
                field->val_str(&str);
                total_len += str.length();
            }
        }
    }

    /*
     * We reuse pre-allocated buffer when possible.
     * We only reallocate if current buffer is too small.
     */
    if (total_len > pack_buffer_capacity)
    {
        size_t new_capacity = total_len > TIDESDB_PACK_BUFFER_MIN_CAPACITY
                                  ? total_len * 2
                                  : TIDESDB_PACK_BUFFER_MIN_CAPACITY;
        uchar *new_buf;
        if (pack_buffer == NULL)
            new_buf = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
        else
            new_buf =
                (uchar *)my_realloc(PSI_INSTRUMENT_ME, pack_buffer, new_capacity, MYF(MY_WME));
        if (!new_buf) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        pack_buffer = new_buf;
        pack_buffer_capacity = new_capacity;
    }

    memcpy(pack_buffer, buf, row_len);

    /* We append BLOB/TEXT/GEOMETRY data with length prefix */
    size_t blob_offset = row_len;
    for (uint i = 0; i < table->s->fields; i++)
    {
        Field *field = table->field[i];
        if (field->type() == MYSQL_TYPE_BLOB || field->type() == MYSQL_TYPE_MEDIUM_BLOB ||
            field->type() == MYSQL_TYPE_LONG_BLOB || field->type() == MYSQL_TYPE_TINY_BLOB ||
            field->type() == MYSQL_TYPE_GEOMETRY)
        {
            uint32 blob_len = 0;
            if (!field->is_null())
            {
                String str;
                field->val_str(&str);
                blob_len = str.length();

                int4store(pack_buffer + blob_offset, blob_len);
                blob_offset += TIDESDB_BLOB_LEN_PREFIX_SIZE;

                if (blob_len > 0)
                {
                    memcpy(pack_buffer + blob_offset, str.ptr(), blob_len);
                    blob_offset += blob_len;
                }
            }
            else
            {
                /* NULL blob -- we store 0 length */
                int4store(pack_buffer + blob_offset, 0);
                blob_offset += TIDESDB_BLOB_LEN_PREFIX_SIZE;
            }
        }
    }

    *packed = pack_buffer;
    *packed_len = total_len;

    DBUG_RETURN(0);
}

/**
  @brief
  Unpack a stored row back into MySQL's/MariaDB's row buffer.

  For BLOB/TEXT fields, the data is stored after the fixed-length row portion
  with prefix for each blob field. We copy blob data to
  row_buffer to ensure it persists after the packed buffer is freed.
*/
int ha_tidesdb::unpack_row(uchar *buf, const uchar *packed, size_t packed_len)
{
    DBUG_ENTER("ha_tidesdb::unpack_row");

    size_t row_len = table->s->reclength;
    if (packed_len < row_len)
    {
        DBUG_RETURN(HA_ERR_CRASHED);
    }

    memcpy(buf, packed, row_len);

    /* We calculate total blob data size needed */
    size_t total_blob_size = 0;
    size_t blob_offset = row_len;
    for (uint i = 0; i < table->s->fields; i++)
    {
        Field *field = table->field[i];
        if (field->type() == MYSQL_TYPE_BLOB || field->type() == MYSQL_TYPE_MEDIUM_BLOB ||
            field->type() == MYSQL_TYPE_LONG_BLOB || field->type() == MYSQL_TYPE_TINY_BLOB ||
            field->type() == MYSQL_TYPE_GEOMETRY)
        {
            if (blob_offset + TIDESDB_BLOB_LEN_PREFIX_SIZE > packed_len)
                DBUG_RETURN(HA_ERR_CRASHED);

            uint32 blob_len = uint4korr(packed + blob_offset);
            blob_offset += TIDESDB_BLOB_LEN_PREFIX_SIZE + blob_len;
            total_blob_size += blob_len;
        }
    }

    /* We allocate/reallocate row_buffer for blob data */
    if (total_blob_size > 0)
    {
        if (row_buffer_len < total_blob_size)
        {
            if (row_buffer) my_free(row_buffer);
            row_buffer = (uchar *)my_malloc(PSI_INSTRUMENT_ME, total_blob_size, MYF(MY_WME));
            if (!row_buffer) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            row_buffer_len = (uint)total_blob_size;
        }
    }

    /* We read BLOB/TEXT/GEOMETRY data and set up pointers */
    blob_offset = row_len;
    size_t buffer_offset = 0;
    for (uint i = 0; i < table->s->fields; i++)
    {
        Field *field = table->field[i];
        if (field->type() == MYSQL_TYPE_BLOB || field->type() == MYSQL_TYPE_MEDIUM_BLOB ||
            field->type() == MYSQL_TYPE_LONG_BLOB || field->type() == MYSQL_TYPE_TINY_BLOB ||
            field->type() == MYSQL_TYPE_GEOMETRY)
        {
            Field_blob *blob_field = (Field_blob *)field;
            uint packlength = blob_field->pack_length_no_ptr();
            uchar *field_ptr = buf + (field->ptr - table->record[0]);

            uint32 blob_len = uint4korr(packed + blob_offset);
            blob_offset += TIDESDB_BLOB_LEN_PREFIX_SIZE;

            switch (packlength)
            {
                case 1:
                    field_ptr[0] = (uchar)blob_len;
                    break;
                case 2:
                    int2store(field_ptr, blob_len);
                    break;
                case 3:
                    int3store(field_ptr, blob_len);
                    break;
                case 4:
                    int4store(field_ptr, blob_len);
                    break;
            }

            /* We copy blob data to our persistent buffer and point to it */
            if (blob_len > 0)
            {
                if (blob_offset + blob_len > packed_len) DBUG_RETURN(HA_ERR_CRASHED);

                memcpy(row_buffer + buffer_offset, packed + blob_offset, blob_len);
                uchar *blob_ptr = row_buffer + buffer_offset;
                memcpy(field_ptr + packlength, &blob_ptr, sizeof(char *));
                blob_offset += blob_len;
                buffer_offset += blob_len;
            }
            else
            {
                /* Empty blob -- set pointer to NULL */
                uchar *null_ptr = NULL;
                memcpy(field_ptr + packlength, &null_ptr, sizeof(char *));
            }
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Encrypt data using MariaDB's encryption service.

  @param src       Source data to encrypt
  @param src_len   Length of source data
  @param dst       Output buffer for encrypted data (caller must free)
  @param dst_len   Output length of encrypted data

  @return 0 on success, error code on failure
*/
static int tidesdb_encrypt_data(const uchar *src, size_t src_len, uchar **dst, size_t *dst_len)
{
    if (!tidesdb_enable_encryption)
    {
        *dst = (uchar *)my_malloc(PSI_INSTRUMENT_ME, src_len, MYF(MY_WME));
        if (!*dst) return HA_ERR_OUT_OF_MEM;
        memcpy(*dst, src, src_len);
        *dst_len = src_len;
        return 0;
    }

    uint key_id = tidesdb_encryption_key_id;
    uint key_version = encryption_key_get_latest_version(key_id);

    if (key_version == ENCRYPTION_KEY_VERSION_INVALID)
    {
        sql_print_error("TidesDB: Encryption key %u not found", key_id);
        return HA_ERR_GENERIC;
    }

    /* We calculate encrypted length */
    uint encrypted_len = encryption_encrypted_length((uint)src_len, key_id, key_version);

    /* We allocate output buffer with space for IV + version + data */
    size_t total_len = TIDESDB_ENCRYPTION_VERSION_SIZE + TIDESDB_ENCRYPTION_IV_SIZE + encrypted_len;
    *dst = (uchar *)my_malloc(PSI_INSTRUMENT_ME, total_len, MYF(MY_WME));
    if (!*dst) return HA_ERR_OUT_OF_MEM;

    /* We store key version at start */
    int4store(*dst, key_version);

    /* We generate random IV */
    uchar iv[TIDESDB_ENCRYPTION_IV_SIZE];
    my_random_bytes(iv, sizeof(iv));
    memcpy(*dst + TIDESDB_ENCRYPTION_VERSION_SIZE, iv, TIDESDB_ENCRYPTION_IV_SIZE);

    /* We get encryption key */
    uchar key[TIDESDB_ENCRYPTION_KEY_SIZE];
    uint key_len = sizeof(key);
    if (encryption_key_get(key_id, key_version, key, &key_len) != 0)
    {
        my_free(*dst);
        *dst = NULL;
        return HA_ERR_GENERIC;
    }

    /* We allocate encryption context */
    uint ctx_size = encryption_ctx_size(key_id, key_version);
    void *ctx = my_alloca(ctx_size);

    /* We initialize encryption */
    if (encryption_ctx_init(ctx, key, key_len, iv, sizeof(iv), ENCRYPTION_FLAG_ENCRYPT, key_id,
                            key_version) != 0)
    {
        my_afree(ctx);
        my_free(*dst);
        *dst = NULL;
        return HA_ERR_GENERIC;
    }

    /* We enrypt data */
    uint out_len = 0;
    uchar *out_ptr = *dst + TIDESDB_ENCRYPTION_VERSION_SIZE + TIDESDB_ENCRYPTION_IV_SIZE;

    if (encryption_ctx_update(ctx, src, (uint)src_len, out_ptr, &out_len) != 0)
    {
        my_afree(ctx);
        my_free(*dst);
        *dst = NULL;
        return HA_ERR_GENERIC;
    }

    /* Finalize encryption */
    uint final_len = 0;
    if (encryption_ctx_finish(ctx, out_ptr + out_len, &final_len) != 0)
    {
        my_afree(ctx);
        my_free(*dst);
        *dst = NULL;
        return HA_ERR_GENERIC;
    }

    my_afree(ctx);
    *dst_len = TIDESDB_ENCRYPTION_VERSION_SIZE + TIDESDB_ENCRYPTION_IV_SIZE + out_len + final_len;

    return 0;
}

/**
  @brief
  Decrypt data using MariaDB's encryption service.

  @param src       Source encrypted data
  @param src_len   Length of encrypted data
  @param dst       Output buffer for decrypted data (caller must free)
  @param dst_len   Output length of decrypted data

  @return 0 on success, error code on failure
*/
static int tidesdb_decrypt_data(const uchar *src, size_t src_len, uchar **dst, size_t *dst_len)
{
    if (!tidesdb_enable_encryption)
    {
        *dst = (uchar *)my_malloc(PSI_INSTRUMENT_ME, src_len, MYF(MY_WME));
        if (!*dst) return HA_ERR_OUT_OF_MEM;
        memcpy(*dst, src, src_len);
        *dst_len = src_len;
        return 0;
    }

    if (src_len < TIDESDB_ENCRYPTION_VERSION_SIZE +
                      TIDESDB_ENCRYPTION_IV_SIZE) /* Minimum -- 4 (version) + 16 (IV) */
    {
        sql_print_error("TidesDB: Encrypted data too short");
        return HA_ERR_GENERIC;
    }

    uint key_version = uint4korr(src);
    uint key_id = tidesdb_encryption_key_id;

    const uchar *iv = src + TIDESDB_ENCRYPTION_VERSION_SIZE;

    uchar key[TIDESDB_ENCRYPTION_KEY_SIZE];
    uint key_len = sizeof(key);
    if (encryption_key_get(key_id, key_version, key, &key_len) != 0)
    {
        sql_print_error("TidesDB: Failed to get encryption key %u version %u", key_id, key_version);
        return HA_ERR_GENERIC;
    }

    /* We allocate output buffer (decrypted is same size or smaller) */
    size_t encrypted_len = src_len - TIDESDB_ENCRYPTION_VERSION_SIZE - TIDESDB_ENCRYPTION_IV_SIZE;
    *dst = (uchar *)my_malloc(PSI_INSTRUMENT_ME, encrypted_len, MYF(MY_WME));
    if (!*dst) return HA_ERR_OUT_OF_MEM;

    /* We allocate decryption context */
    uint ctx_size = encryption_ctx_size(key_id, key_version);
    void *ctx = my_alloca(ctx_size);

    /* We initialize decryption */
    if (encryption_ctx_init(ctx, key, key_len, iv, TIDESDB_ENCRYPTION_IV_SIZE,
                            ENCRYPTION_FLAG_DECRYPT, key_id, key_version) != 0)
    {
        my_afree(ctx);
        my_free(*dst);
        *dst = NULL;
        return HA_ERR_GENERIC;
    }

    uint out_len = 0;
    const uchar *encrypted_data =
        src + TIDESDB_ENCRYPTION_VERSION_SIZE + TIDESDB_ENCRYPTION_IV_SIZE;

    if (encryption_ctx_update(ctx, encrypted_data, (uint)encrypted_len, *dst, &out_len) != 0)
    {
        my_afree(ctx);
        my_free(*dst);
        *dst = NULL;
        return HA_ERR_GENERIC;
    }

    uint final_len = 0;
    if (encryption_ctx_finish(ctx, *dst + out_len, &final_len) != 0)
    {
        my_afree(ctx);
        my_free(*dst);
        *dst = NULL;
        return HA_ERR_GENERIC;
    }

    my_afree(ctx);
    *dst_len = out_len + final_len;

    return 0;
}

/**
  @brief
  Build the primary key from the row buffer.

  If the table has a primary key, extract it.
  Otherwise, generate a hidden auto-increment key.
*/
int ha_tidesdb::build_primary_key(const uchar *buf, uchar **key, size_t *key_len)
{
    DBUG_ENTER("ha_tidesdb::build_primary_key");

    if (table->s->primary_key != MAX_KEY)
    {
        /* The table has a primary key -- extract it */
        KEY *pk = &table->key_info[table->s->primary_key];
        uint pk_len = pk->key_length;

        if (pk_buffer_len < pk_len)
        {
            if (pk_buffer) my_free(pk_buffer);
            pk_buffer = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
            if (!pk_buffer) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            pk_buffer_len = pk_len;
        }

        key_copy(pk_buffer, (uchar *)buf, pk, pk_len);

        *key = pk_buffer;
        *key_len = pk_len;
    }
    else
    {
        /* No primary key -- we use hidden auto-increment */
        DBUG_RETURN(build_hidden_pk(key, key_len));
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Build a hidden primary key for tables without explicit PK.
  Uses a per-table auto-incrementing integer that is persisted
  to TidesDB metadata for crash recovery.

  The hidden PK is stored as a special metadata key in the column family:
  Key   -- "__hidden_pk_max__"
  Value -- big-endian counter
*/
int ha_tidesdb::build_hidden_pk(uchar **key, size_t *key_len)
{
    DBUG_ENTER("ha_tidesdb::build_hidden_pk");

    /* We ensure we have a buffer for the key */
    if (pk_buffer_len < TIDESDB_HIDDEN_PK_LEN)
    {
        if (pk_buffer) my_free(pk_buffer);
        pk_buffer = (uchar *)my_malloc(PSI_INSTRUMENT_ME, TIDESDB_HIDDEN_PK_LEN, MYF(MY_WME));
        if (!pk_buffer) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        pk_buffer_len = TIDESDB_HIDDEN_PK_LEN;
    }

    ulonglong pk_val = my_atomic_add64_explicit((volatile int64 *)&share->hidden_pk_value, 1,
                                                MY_MEMORY_ORDER_RELAXED) +
                       1;

    /* We persist the new max value to TidesDB metadata every N inserts for performance.
       We persist every 100 values and on close. On recovery, we'll scan to find
       the actual max, but this reduces write amplification. */
    if ((pk_val % TIDESDB_HIDDEN_PK_PERSIST_INTERVAL) == 0 || pk_val == 1)
    {
        persist_hidden_pk_value(pk_val);
    }

    /* We store as big-endian for proper sort order in TidesDB */
    int8store(pk_buffer, pk_val);

    *key = pk_buffer;
    *key_len = TIDESDB_HIDDEN_PK_LEN;

    DBUG_RETURN(0);
}

/**
  @brief
  Persist the hidden PK max value to TidesDB metadata.
  Called periodically during inserts and on table close.
*/
void ha_tidesdb::persist_hidden_pk_value(ulonglong value)
{
    if (!share || !share->cf || !tidesdb_instance) return;

    /* We use a special metadata key that sorts before all data keys */
    static const char *meta_key = TIDESDB_HIDDEN_PK_META_KEY;
    size_t meta_key_len = TIDESDB_HIDDEN_PK_META_KEY_LEN; /* Including leading null byte */

    uchar value_buf[TIDESDB_HIDDEN_PK_LEN];
    int8store(value_buf, value);

    /* We write directly without transaction for metadata */
    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tidesdb_instance, &txn) == TDB_SUCCESS)
    {
        tidesdb_txn_put(txn, share->cf, (uint8_t *)meta_key, meta_key_len, value_buf,
                        TIDESDB_HIDDEN_PK_LEN, -1);
        tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);
    }
}

/**
  @brief
  Persist the auto-increment value to TidesDB metadata.
*/
void ha_tidesdb::persist_auto_increment_value(ulonglong value)
{
    if (!share || !share->cf || !tidesdb_instance) return;

    /* Only persist if table actually has an AUTO_INCREMENT field */
    if (!table || !table->s->found_next_number_field) return;

    static const char *meta_key = TIDESDB_AUTO_INC_META_KEY;
    size_t meta_key_len = TIDESDB_AUTO_INC_META_KEY_LEN;

    uchar value_buf[TIDESDB_HIDDEN_PK_LEN];
    int8store(value_buf, value);

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tidesdb_instance, &txn) == TDB_SUCCESS)
    {
        tidesdb_txn_put(txn, share->cf, (uint8_t *)meta_key, meta_key_len, value_buf,
                        TIDESDB_HIDDEN_PK_LEN, -1);
        tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);
    }
}

/**
  @brief
  Load the auto-increment value from TidesDB metadata on table open.
  If not found, scans the table to find the maximum existing auto-increment value.
*/
void ha_tidesdb::load_auto_increment_value()
{
    if (!share || !share->cf || !tidesdb_instance) return;

    static const char *meta_key = TIDESDB_AUTO_INC_META_KEY;
    size_t meta_key_len = TIDESDB_AUTO_INC_META_KEY_LEN;

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tidesdb_instance, &txn) == TDB_SUCCESS)
    {
        uint8_t *value = NULL;
        size_t value_len = 0;

        if (tidesdb_txn_get(txn, share->cf, (uint8_t *)meta_key, meta_key_len, &value,
                            &value_len) == TDB_SUCCESS &&
            value_len == TIDESDB_HIDDEN_PK_LEN)
        {
            share->auto_increment_value = uint8korr(value);
            tidesdb_free(value);
        }
        else
        {
            /* No persisted value -- we scan table to find max auto-increment */
            if (table && table->s->primary_key != MAX_KEY)
            {
                KEY *pk = &table->key_info[table->s->primary_key];
                if (pk->user_defined_key_parts == 1)
                {
                    /* Single-column PK -- we scan for max value */
                    tidesdb_iter_t *iter = NULL;
                    if (tidesdb_iter_new(txn, share->cf, &iter) == TDB_SUCCESS)
                    {
                        tidesdb_iter_seek_to_last(iter);
                        while (tidesdb_iter_valid(iter))
                        {
                            uint8_t *key = NULL;
                            size_t key_len = 0;
                            if (tidesdb_iter_key(iter, &key, &key_len) == TDB_SUCCESS)
                            {
                                /* We skip metadata keys */
                                if (key_len > 0 && key[0] == 0)
                                {
                                    tidesdb_iter_prev(iter);
                                    continue;
                                }
                                /* We extract the integer value from the key */
                                if (key_len >= 4)
                                {
                                    ulonglong max_val = 0;
                                    if (key_len == 4)
                                        max_val = uint4korr(key);
                                    else if (key_len >= 8)
                                        max_val = uint8korr(key);
                                    if (max_val >= share->auto_increment_value)
                                        share->auto_increment_value = max_val + 1;
                                }
                                break;
                            }
                            tidesdb_iter_prev(iter);
                        }
                        tidesdb_iter_free(iter);
                    }
                }
            }
        }

        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
    }

    if (share->auto_increment_value == 0) share->auto_increment_value = 1;
}

/**
  @brief
  Persist table statistics to TidesDB metadata.

  Stores row count, data file length, and mean record length as a metadata
  key in the primary column family. This allows statistics to survive server
  restarts without requiring a full table scan or ANALYZE TABLE.

  Format -- [8-byte row_count][8-byte data_file_length][4-byte mean_rec_length]
*/
void ha_tidesdb::persist_table_stats()
{
    if (!share || !share->cf || !tidesdb_instance) return;

    static const char *meta_key = TIDESDB_STATS_META_KEY;
    size_t meta_key_len = TIDESDB_STATS_META_KEY_LEN;

    uchar value_buf[20]; /* 8 + 8 + 4 = 20 bytes */
    int8store(value_buf, (ulonglong)stats.records);
    int8store(value_buf + 8, (ulonglong)stats.data_file_length);
    int4store(value_buf + 16, (uint32)stats.mean_rec_length);

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tidesdb_instance, &txn) == TDB_SUCCESS)
    {
        tidesdb_txn_put(txn, share->cf, (uint8_t *)meta_key, meta_key_len, value_buf, 20, -1);
        tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);
    }
}

/**
  @brief
  Load persisted table statistics from TidesDB metadata on table open.

  If stats were previously persisted (by ANALYZE TABLE), load them into
  the share's row_count cache so info() can use them as a fallback when
  live stats are unavailable.
*/
void ha_tidesdb::load_table_stats()
{
    if (!share || !share->cf || !tidesdb_instance) return;

    static const char *meta_key = TIDESDB_STATS_META_KEY;
    size_t meta_key_len = TIDESDB_STATS_META_KEY_LEN;

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tidesdb_instance, &txn) == TDB_SUCCESS)
    {
        uint8_t *value = NULL;
        size_t value_len = 0;

        if (tidesdb_txn_get(txn, share->cf, (uint8_t *)meta_key, meta_key_len, &value,
                            &value_len) == TDB_SUCCESS &&
            value && value_len >= 20)
        {
            share->row_count = (ha_rows)uint8korr(value);
            share->row_count_valid = true;
            free(value);
        }

        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
    }
}

/**
  @brief
  Load the hidden PK max value from TidesDB metadata on table open.
  If not found, scans the table to find the maximum existing key.
*/
void ha_tidesdb::load_hidden_pk_value()
{
    if (!share || !share->cf || !tidesdb_instance) return;

    if (my_atomic_load64_explicit((volatile int64 *)&share->hidden_pk_value,
                                  MY_MEMORY_ORDER_ACQUIRE) > 0)
        return;

    /*
     * For initialization, we still need a mutex to prevent multiple threads
     * from scanning simultaneously. But this only happens once per table open.
     */
    pthread_mutex_lock(&share->hidden_pk_mutex);

    if (share->hidden_pk_value > 0)
    {
        pthread_mutex_unlock(&share->hidden_pk_mutex);
        return;
    }

    ulonglong max_pk = 0;

    /* We try to read persisted value first */
    static const char *meta_key = TIDESDB_HIDDEN_PK_META_KEY;
    size_t meta_key_len = TIDESDB_HIDDEN_PK_META_KEY_LEN;

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tidesdb_instance, &txn) == TDB_SUCCESS)
    {
        uint8_t *value = NULL;
        size_t value_len = 0;

        if (tidesdb_txn_get(txn, share->cf, (uint8_t *)meta_key, meta_key_len, &value,
                            &value_len) == TDB_SUCCESS &&
            value_len == TIDESDB_HIDDEN_PK_LEN)
        {
            max_pk = uint8korr(value);
            tidesdb_free(value);
        }

        /* If no persisted value or to verify, scan for actual max.
           This handles crash recovery where inserts happened after last persist. */
        if (!share->has_primary_key)
        {
            tidesdb_iter_t *iter = NULL;
            if (tidesdb_iter_new(txn, share->cf, &iter) == TDB_SUCCESS)
            {
                tidesdb_iter_seek_to_last(iter);

                /* We skip metadata keys (starting with null byte) and find max data key */
                while (tidesdb_iter_valid(iter))
                {
                    uint8_t *key = NULL;
                    size_t key_len = 0;

                    if (tidesdb_iter_key(iter, &key, &key_len) == TDB_SUCCESS)
                    {
                        /* We skip metadata keys */
                        if (key_len > 0 && key[0] == 0)
                        {
                            tidesdb_iter_prev(iter);
                            continue;
                        }

                        /* Found a data key -- we extract the hidden PK value */
                        if (key_len == 8)
                        {
                            ulonglong found_pk = uint8korr(key);
                            if (found_pk > max_pk) max_pk = found_pk;
                        }
                        break;
                    }
                    tidesdb_iter_prev(iter);
                }

                tidesdb_iter_free(iter);
            }
        }

        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
    }

    my_atomic_store64_explicit((volatile int64 *)&share->hidden_pk_value, max_pk,
                               MY_MEMORY_ORDER_RELEASE);

    pthread_mutex_unlock(&share->hidden_pk_mutex);
}

/**
  Compute the sort-key length for a secondary index (excluding PK suffix).
  Sort keys use make_sort_key() per field, which omits the 2-byte VARCHAR
  length prefix that key_copy() includes.  This makes the key memcmp-able.
*/
static uint compute_sort_key_length(const KEY *key_info)
{
    uint len = 0;
    for (uint i = 0; i < key_info->user_defined_key_parts; i++)
    {
        const KEY_PART_INFO *kp = &key_info->key_part[i];
        if (kp->null_bit) len += 1;
        len += kp->length;
    }
    return len;
}

/**
  @brief
  Build a secondary index key from the row buffer.

  The index key format is -- index_columns + primary_key
  This ensures uniqueness even for non-unique indexes.

*/
int ha_tidesdb::build_index_key(uint idx, const uchar *buf, uchar **key, size_t *key_len)
{
    DBUG_ENTER("ha_tidesdb::build_index_key");

    if (idx >= table->s->keys) DBUG_RETURN(HA_ERR_WRONG_INDEX);

    KEY *key_info = &table->key_info[idx];

    /* Sort-key length (no VARCHAR length prefixes) */
    uint sort_key_len = compute_sort_key_length(key_info);

    /* We calculate primary key length */
    size_t pk_len;
    if (table->s->primary_key != MAX_KEY)
    {
        pk_len = table->key_info[table->s->primary_key].key_length;
    }
    else
    {
        pk_len = TIDESDB_HIDDEN_PK_LEN;
    }

    /* We reuse pre-allocated buffer when possible */
    size_t total_len = sort_key_len + pk_len;
    if (total_len > idx_key_buffer_capacity)
    {
        size_t new_capacity = total_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                  ? total_len * 2
                                  : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
        uchar *new_buf;
        if (idx_key_buffer == NULL)
            new_buf = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
        else
            new_buf =
                (uchar *)my_realloc(PSI_INSTRUMENT_ME, idx_key_buffer, new_capacity, MYF(MY_WME));
        if (!new_buf) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        idx_key_buffer = new_buf;
        idx_key_buffer_capacity = new_capacity;
    }

    /*
      Build sort key per-part using field->make_sort_key_part().
      This writes:  [null_ind?] [sort_string(length)]
        -- nullable NULL          0x00 + length zero bytes   (length+1 total)
        -- nullable NOT NULL      0x01 + sort data           (length+1 total)
        -- non-nullable           sort data only             (length total)

           Note -- make_sort_key_part's null polarity is OPPOSITE of key_copy's:
                   key_copy: 0=NOT NULL, 1=NULL  |  make_sort_key_part: 0=NULL, 1=NOT NULL
    */
    my_ptrdiff_t rec_offset = buf - table->record[0];
    uint offset = 0;

    for (uint i = 0; i < key_info->user_defined_key_parts; i++)
    {
        KEY_PART_INFO *kp = &key_info->key_part[i];
        Field *field = kp->field;

        field->move_field_offset(rec_offset);
        field->make_sort_key_part(idx_key_buffer + offset, kp->length);
        field->move_field_offset(-rec_offset);

        offset += kp->length;
        if (field->maybe_null()) offset += 1;
    }

    /* We append primary key to ensure uniqueness (key_copy is fine for PK) */
    if (table->s->primary_key != MAX_KEY)
    {
        KEY *pk = &table->key_info[table->s->primary_key];
        key_copy(idx_key_buffer + offset, (uchar *)buf, pk, (uint)pk_len);
    }
    else
    {
        if (current_key && current_key_len == 8)
        {
            memcpy(idx_key_buffer + offset, current_key, 8);
        }
        else
        {
            memset(idx_key_buffer + offset, 0, 8);
        }
    }

    *key = idx_key_buffer;
    *key_len = total_len;

    DBUG_RETURN(0);
}

/**
  Transform a search key from key_copy() format to the sort-key format
  used by build_index_key().

  Strategy: key_restore() the search key into table->record[1], then
  call field->make_sort_key_part() for each provided key part.
  This guarantees bit-identical output for ALL field types (INT,
  VARCHAR, DECIMAL, DATE, etc.) regardless of internal encoding
  differences between key_copy and sort_string.

  @param table      TABLE object (needs record[1] as scratch)
  @param key_info   Index definition
  @param src        Search key in key_copy() format
  @param src_len    Length of search key (may be partial)
  @param dst        Output buffer (must be >= compute_sort_key_length)
  @return           Length of transformed sort key
*/
static uint transform_search_key_to_sort_key(TABLE *table, const KEY *key_info, const uchar *src,
                                             uint src_len, uchar *dst)
{
    key_restore(table->record[1], src, const_cast<KEY *>(key_info), src_len);

    my_ptrdiff_t rec_offset = table->record[1] - table->record[0];
    uint off_in = 0, off_out = 0;

    for (uint i = 0; i < key_info->user_defined_key_parts; i++)
    {
        if (off_in >= src_len) break;

        const KEY_PART_INFO *kp = &key_info->key_part[i];
        Field *field = kp->field;

        field->move_field_offset(rec_offset);
        field->make_sort_key_part(dst + off_out, kp->length);
        field->move_field_offset(-rec_offset);

        off_out += kp->length;
        if (field->maybe_null()) off_out += 1;
        off_in += kp->store_length;
    }

    return off_out;
}

/**
  @brief
  Insert an entry into a secondary index.

  Stores -- index_key -> primary_key (extracted from row buffer)
*/
int ha_tidesdb::insert_index_entry(uint idx, const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::insert_index_entry");

    /* Skip primary key -- it's not a secondary index */
    if (idx == table->s->primary_key) DBUG_RETURN(0);

    /* We check if we have a column family for this index */
    if (idx >= share->num_indexes || !share->index_cf[idx]) DBUG_RETURN(0);

    uchar *idx_key = NULL;
    size_t idx_key_len = 0;

    int ret = build_index_key(idx, buf, &idx_key, &idx_key_len);
    if (ret) DBUG_RETURN(ret);

    /* We extract primary key directly from row buffer (don't use build_primary_key) */
    size_t pk_len;

    if (table->s->primary_key != MAX_KEY)
    {
        KEY *pk = &table->key_info[table->s->primary_key];
        pk_len = pk->key_length;
    }
    else
    {
        /* Hidden PK */
        pk_len = TIDESDB_HIDDEN_PK_LEN;
    }

    if (pk_len > idx_pk_buffer_capacity)
    {
        size_t new_capacity = pk_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                  ? pk_len * 2
                                  : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
        uchar *new_buf;
        if (idx_pk_buffer == NULL)
            new_buf = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
        else
            new_buf =
                (uchar *)my_realloc(PSI_INSTRUMENT_ME, idx_pk_buffer, new_capacity, MYF(MY_WME));
        if (!new_buf) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        idx_pk_buffer = new_buf;
        idx_pk_buffer_capacity = new_capacity;
    }

    if (table->s->primary_key != MAX_KEY)
    {
        KEY *pk = &table->key_info[table->s->primary_key];
        key_copy(idx_pk_buffer, (uchar *)buf, pk, (uint)pk_len);
    }
    else
    {
        /* Hidden PK -- we use current_key if available */
        if (current_key && current_key_len == 8)
        {
            memcpy(idx_pk_buffer, current_key, 8);
        }
        else
        {
            memset(idx_pk_buffer, 0, 8);
        }
    }

    /* For UNIQUE indexes, check for duplicate key before insert */
    KEY *key_info = &table->key_info[idx];
    if ((key_info->flags & HA_NOSAME) && !skip_dup_check)
    {
        uint8_t *existing_pk = NULL;
        size_t existing_pk_len = 0;
        int dup_ret = tidesdb_txn_get(txn, share->index_cf[idx], idx_key, idx_key_len, &existing_pk,
                                      &existing_pk_len);
        if (dup_ret == TDB_SUCCESS && existing_pk)
        {
            /* Entry exists -- we check if it points to a different PK (true duplicate) */
            if (existing_pk_len != pk_len || memcmp(existing_pk, idx_pk_buffer, pk_len) != 0)
            {
                tidesdb_free(existing_pk);
                DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
            }
            tidesdb_free(existing_pk);
        }
    }

    /* We insert into index CF -- index_key -> primary_key */
    ret =
        tidesdb_txn_put(txn, share->index_cf[idx], idx_key, idx_key_len, idx_pk_buffer, pk_len, -1);

    if (ret != TDB_SUCCESS)
    {
        sql_print_error("TidesDB: Failed to insert index entry: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Delete an entry from a secondary index.
*/
int ha_tidesdb::delete_index_entry(uint idx, const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::delete_index_entry");

    /* We skip primary key */
    if (idx == table->s->primary_key) DBUG_RETURN(0);

    if (idx >= share->num_indexes || !share->index_cf[idx]) DBUG_RETURN(0);

    uchar *idx_key = NULL;
    size_t idx_key_len = 0;

    int ret = build_index_key(idx, buf, &idx_key, &idx_key_len);
    if (ret) DBUG_RETURN(ret);

    ret = tidesdb_txn_delete(txn, share->index_cf[idx], idx_key, idx_key_len);

    if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
    {
        sql_print_error("TidesDB: Failed to delete index entry: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Update all secondary index entries when a row is updated.

  Deletes old entries and inserts new ones for each secondary index.
*/
int ha_tidesdb::update_index_entries(const uchar *old_buf, const uchar *new_buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::update_index_entries");

    int ret;

    for (uint i = 0; i < table->s->keys; i++)
    {
        if (i == table->s->primary_key) continue;

        ret = delete_index_entry(i, old_buf, txn);
        if (ret) DBUG_RETURN(ret);

        ret = insert_index_entry(i, new_buf, txn);
        if (ret) DBUG_RETURN(ret);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Create column families for secondary indexes during table creation.
*/
int ha_tidesdb::create_secondary_indexes(const char *table_name)
{
    DBUG_ENTER("ha_tidesdb::create_secondary_indexes");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    char idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    get_cf_name(table_name, cf_name, sizeof(cf_name));

    tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
    cf_config.write_buffer_size = tidesdb_write_buffer_size;
    cf_config.enable_bloom_filter = tidesdb_enable_bloom_filter ? 1 : 0;
    cf_config.bloom_fpr = tidesdb_bloom_fpr;
    cf_config.use_btree = tidesdb_use_btree ? 1 : 0;

    if (tidesdb_enable_compression)
    {
        cf_config.compression_algorithm = (compression_algorithm)tidesdb_compression_algo;
    }

    for (uint i = 0; i < table->s->keys; i++)
    {
        if (i == table->s->primary_key) continue;

        if (table->key_info[i].algorithm == HA_KEY_ALG_FULLTEXT) continue;

        /** We create CF for this secondary index -- tablename_idx_N */
        snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_FMT, cf_name, i);

        int ret = tidesdb_create_column_family(tidesdb_instance, idx_cf_name, &cf_config);
        if (ret != TDB_SUCCESS && ret != TDB_ERR_EXISTS)
        {
            sql_print_error("TidesDB: Failed to create index CF '%s': %d", idx_cf_name, ret);
            DBUG_RETURN(HA_ERR_GENERIC);
        }

        sql_print_information("TidesDB: Created secondary index '%s' for key %u", idx_cf_name, i);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Open column families for secondary indexes.
*/
int ha_tidesdb::open_secondary_indexes(const char *table_name)
{
    DBUG_ENTER("ha_tidesdb::open_secondary_indexes");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    char idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    get_cf_name(table_name, cf_name, sizeof(cf_name));

    share->num_indexes = 0;

    for (uint i = 0; i < table->s->keys && i < TIDESDB_MAX_INDEXES; i++)
    {
        /* We skip primary key */
        if (i == table->s->primary_key)
        {
            share->index_cf[i] = NULL;
            continue;
        }

        /* We skip fulltext keys -- handled separately */
        if (table->key_info[i].algorithm == HA_KEY_ALG_FULLTEXT)
        {
            share->index_cf[i] = NULL;
            continue;
        }

        snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_FMT, cf_name, i);
        share->index_cf[i] = tidesdb_get_column_family(tidesdb_instance, idx_cf_name);

        if (!share->index_cf[i])
        {
            /* We try name-based convention (used by INPLACE ADD INDEX) */
            snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_NAME_FMT, cf_name,
                     table->key_info[i].name.str);
            share->index_cf[i] = tidesdb_get_column_family(tidesdb_instance, idx_cf_name);
        }

        if (share->index_cf[i])
        {
            share->num_indexes = i + 1;
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Create column families for fulltext indexes during table creation.

  Fulltext indexes use an inverted index structure:
  -- Key   -- word (normalized, lowercase)
  -- Value -- list of primary keys containing that word
*/
int ha_tidesdb::create_fulltext_indexes(const char *table_name)
{
    DBUG_ENTER("ha_tidesdb::create_fulltext_indexes");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    char ft_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    get_cf_name(table_name, cf_name, sizeof(cf_name));

    tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
    cf_config.write_buffer_size = tidesdb_write_buffer_size;
    cf_config.enable_bloom_filter = 1;
    cf_config.bloom_fpr = TIDESDB_DEFAULT_BLOOM_FPR;
    cf_config.use_btree = tidesdb_use_btree ? 1 : 0;

    if (tidesdb_enable_compression)
    {
        cf_config.compression_algorithm = (compression_algorithm)tidesdb_compression_algo;
    }

    uint ft_count = 0;
    for (uint i = 0; i < table->s->keys && ft_count < TIDESDB_MAX_FT_INDEXES; i++)
    {
        KEY *key = &table->key_info[i];
        if (key->algorithm != HA_KEY_ALG_FULLTEXT) continue;

        /* We create CF for this fulltext index: tablename_ft_N */
        snprintf(ft_cf_name, sizeof(ft_cf_name), TIDESDB_CF_FT_FMT, cf_name, i);

        int ret = tidesdb_create_column_family(tidesdb_instance, ft_cf_name, &cf_config);
        if (ret != TDB_SUCCESS && ret != TDB_ERR_EXISTS)
        {
            sql_print_error("TidesDB: Failed to create FT index CF '%s': %d", ft_cf_name, ret);
            DBUG_RETURN(HA_ERR_GENERIC);
        }

        sql_print_information("TidesDB: Created fulltext index '%s' for key %u", ft_cf_name, i);
        ft_count++;
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Open fulltext index column families for an existing table.
*/
int ha_tidesdb::open_fulltext_indexes(const char *table_name)
{
    DBUG_ENTER("ha_tidesdb::open_fulltext_indexes");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    char ft_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    get_cf_name(table_name, cf_name, sizeof(cf_name));

    share->num_ft_indexes = 0;

    for (uint i = 0; i < table->s->keys && share->num_ft_indexes < TIDESDB_MAX_FT_INDEXES; i++)
    {
        KEY *key = &table->key_info[i];
        if (key->algorithm != HA_KEY_ALG_FULLTEXT) continue;

        snprintf(ft_cf_name, sizeof(ft_cf_name), TIDESDB_CF_FT_FMT, cf_name, i);

        tidesdb_column_family_t *ft_cf = tidesdb_get_column_family(tidesdb_instance, ft_cf_name);
        if (ft_cf)
        {
            share->ft_cf[share->num_ft_indexes] = ft_cf;
            share->ft_key_nr[share->num_ft_indexes] = i;
            share->num_ft_indexes++;
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Word tokenizer for fulltext indexing.

  Splits text into words, converts to lowercase, and calls callback for each word.
  Skips words shorter than ft_min_word_len or longer than ft_max_word_len.
*/
int ha_tidesdb::tokenize_text(const char *text, size_t len, CHARSET_INFO *cs,
                              void (*callback)(const char *word, size_t word_len, void *arg),
                              void *arg)
{
    DBUG_ENTER("ha_tidesdb::tokenize_text");

    if (!text || len == 0) DBUG_RETURN(0);

    char word_buf[TIDESDB_FT_WORD_BUF_SIZE];
    size_t word_len = 0;

    for (size_t i = 0; i <= len; i++)
    {
        char c = (i < len) ? text[i] : ' ';

        /* We check if character is alphanumeric */
        bool is_word_char = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                            (c >= '0' && c <= '9') || (c == '_');

        if (is_word_char && word_len < sizeof(word_buf) - 1)
        {
            /* We convert to lowercase */
            word_buf[word_len++] = (c >= 'A' && c <= 'Z') ? (c + TIDESDB_ASCII_CASE_OFFSET) : c;
        }
        else if (word_len > 0)
        {
            /* End of word -- we check length constraints */
            if (word_len >= tidesdb_ft_min_word_len && word_len <= tidesdb_ft_max_word_len)
            {
                word_buf[word_len] = '\0';
                callback(word_buf, word_len, arg);
            }
            word_len = 0;
        }
    }

    DBUG_RETURN(0);
}

/* Callback context for inserting FT words */
struct ft_insert_ctx
{
    ha_tidesdb *handler;
    tidesdb_column_family_t *ft_cf;
    tidesdb_txn_t *txn;
    uchar *pk;
    size_t pk_len;
    int result;
};

static void ft_insert_word_callback(const char *word, size_t word_len, void *arg)
{
    ft_insert_ctx *ctx = (ft_insert_ctx *)arg;
    if (ctx->result != 0) return;

    /*
      Inverted index format:
      Key   -- word + '\0' + primary_key
      Value -- empty (presence indicates match)
    */
    size_t key_len = word_len + 1 + ctx->pk_len;
    uchar *key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_len, MYF(MY_WME));
    if (!key)
    {
        ctx->result = HA_ERR_OUT_OF_MEM;
        return;
    }

    memcpy(key, word, word_len);
    key[word_len] = '\0';
    memcpy(key + word_len + 1, ctx->pk, ctx->pk_len);

    /* We insert into FT index -- we use single byte value since empty is not allowed */
    uint8_t dummy_value = 1;
    int ret = tidesdb_txn_put(ctx->txn, ctx->ft_cf, key, key_len, &dummy_value, 1, -1);
    my_free(key);

    if (ret != TDB_SUCCESS && ret != TDB_ERR_EXISTS)
    {
        ctx->result = HA_ERR_GENERIC;
    }
}

/**
  @brief
  Insert words from a row into the fulltext index.
*/
int ha_tidesdb::insert_ft_words(uint ft_idx, const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::insert_ft_words");

    if (ft_idx >= share->num_ft_indexes || !share->ft_cf[ft_idx]) DBUG_RETURN(0);

    uint key_nr = share->ft_key_nr[ft_idx];
    KEY *key = &table->key_info[key_nr];

    uchar *pk = NULL;
    size_t pk_len = 0;
    int ret = build_primary_key(buf, &pk, &pk_len);
    if (ret) DBUG_RETURN(ret);

    /* We save PK since build_primary_key uses shared buffer */
    uchar *saved_pk = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
    if (!saved_pk) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    memcpy(saved_pk, pk, pk_len);

    ft_insert_ctx ctx;
    ctx.handler = this;
    ctx.ft_cf = share->ft_cf[ft_idx];
    ctx.txn = txn;
    ctx.pk = saved_pk;
    ctx.pk_len = pk_len;
    ctx.result = 0;

    /* We process each column in the fulltext key */
    for (uint i = 0; i < key->user_defined_key_parts; i++)
    {
        KEY_PART_INFO *part = &key->key_part[i];
        Field *field = part->field;

        if (field->is_null()) continue;

        /* We get field value as string */
        String str;
        field->val_str(&str);

        if (str.length() > 0)
        {
            tokenize_text(str.ptr(), str.length(), field->charset(), ft_insert_word_callback, &ctx);
        }
    }

    my_free(saved_pk);

    DBUG_RETURN(ctx.result);
}

/* Callback context for deleting FT words */
struct ft_delete_ctx
{
    ha_tidesdb *handler;
    tidesdb_column_family_t *ft_cf;
    tidesdb_txn_t *txn;
    uchar *pk;
    size_t pk_len;
    int result;
};

static void ft_delete_word_callback(const char *word, size_t word_len, void *arg)
{
    ft_delete_ctx *ctx = (ft_delete_ctx *)arg;
    if (ctx->result != 0) return;

    size_t key_len = word_len + 1 + ctx->pk_len;
    uchar *key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_len, MYF(MY_WME));
    if (!key)
    {
        ctx->result = HA_ERR_OUT_OF_MEM;
        return;
    }

    memcpy(key, word, word_len);
    key[word_len] = '\0';
    memcpy(key + word_len + 1, ctx->pk, ctx->pk_len);

    int ret = tidesdb_txn_delete(ctx->txn, ctx->ft_cf, key, key_len);
    my_free(key);

    if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
    {
        ctx->result = HA_ERR_GENERIC;
    }
}

/**
  @brief
  Delete words from a row from the fulltext index.
*/
int ha_tidesdb::delete_ft_words(uint ft_idx, const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::delete_ft_words");

    if (ft_idx >= share->num_ft_indexes) DBUG_RETURN(0);

    uint key_nr = share->ft_key_nr[ft_idx];
    KEY *key = &table->key_info[key_nr];

    uchar *pk = NULL;
    size_t pk_len = 0;
    int ret = build_primary_key(buf, &pk, &pk_len);
    if (ret) DBUG_RETURN(ret);

    uchar *saved_pk = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
    if (!saved_pk) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    memcpy(saved_pk, pk, pk_len);

    ft_delete_ctx ctx;
    ctx.handler = this;
    ctx.ft_cf = share->ft_cf[ft_idx];
    ctx.txn = txn;
    ctx.pk = saved_pk;
    ctx.pk_len = pk_len;
    ctx.result = 0;

    for (uint i = 0; i < key->user_defined_key_parts; i++)
    {
        KEY_PART_INFO *part = &key->key_part[i];
        Field *field = part->field;

        if (field->is_null()) continue;

        String str;
        field->val_str(&str);

        if (str.length() > 0)
        {
            tokenize_text(str.ptr(), str.length(), field->charset(), ft_delete_word_callback, &ctx);
        }
    }

    my_free(saved_pk);

    DBUG_RETURN(ctx.result);
}

/**
  @brief
  Parse foreign key definitions and load referencing table info.

  FK metadata is stored in a special "_fk_metadata" column family:
  -- Key -- "child:<db>.<table>" -> Value  -- serialized FK info (parent table, columns)
  -- Key -- "parent:<db>.<table>" -> Value -- list of child tables that reference it

  This allows efficient lookup of both:
  1. Which parent tables this table references (for INSERT/UPDATE checks)
  2. Which child tables reference this table (for DELETE checks)
*/
int ha_tidesdb::parse_foreign_keys()
{
    DBUG_ENTER("ha_tidesdb::parse_foreign_keys");

    share->num_fk = 0;
    share->num_referencing = 0;

    /* We get or create the FK metadata column family */
    tidesdb_column_family_t *fk_meta_cf =
        tidesdb_get_column_family(tidesdb_instance, TIDESDB_FK_META_CF_NAME);
    if (!fk_meta_cf)
    {
        /* FK metadata CF doesn't exist yet -- no FKs defined */
        DBUG_RETURN(0);
    }

    /* We build key to look up FKs for this table (as child) */
    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    get_cf_name(share->table_name, cf_name, sizeof(cf_name));

    char child_key[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    snprintf(child_key, sizeof(child_key), TIDESDB_FK_CHILD_KEY_FMT, cf_name);

    /* We look up FK definitions for this table */
    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tidesdb_instance, &txn) != TDB_SUCCESS) DBUG_RETURN(0);

    uint8_t *fk_data = NULL;
    size_t fk_data_len = 0;

    int ret = tidesdb_txn_get(txn, fk_meta_cf, (uint8_t *)child_key, strlen(child_key), &fk_data,
                              &fk_data_len);

    if (ret == TDB_SUCCESS && fk_data && fk_data_len > 0)
    {
        /* Parse FK data -- format is "num_fk|fk1_data|fk2_data|..." */
        /* Each fk_data  -- "ref_db\0ref_table\0num_cols|col_idx1|col_idx2|..." */
        const char *ptr = (const char *)fk_data;
        const char *end = ptr + fk_data_len;

        while (ptr < end && share->num_fk < TIDESDB_MAX_FK)
        {
            TIDESDB_FK *fk = &share->fk[share->num_fk];

            /* We read ref_db */
            size_t len = strnlen(ptr, end - ptr);
            if (len == 0 || ptr + len >= end) break;
            strncpy(fk->ref_db, ptr, sizeof(fk->ref_db) - 1);
            fk->ref_db[sizeof(fk->ref_db) - 1] = '\0';
            ptr += len + 1;

            /* We read ref_table */
            len = strnlen(ptr, end - ptr);
            if (len == 0 || ptr + len >= end) break;
            strncpy(fk->ref_table, ptr, sizeof(fk->ref_table) - 1);
            fk->ref_table[sizeof(fk->ref_table) - 1] = '\0';
            ptr += len + 1;

            /* We read num_cols and column indices */
            if (ptr + 1 > end) break;
            fk->num_cols = (uint8_t)*ptr++;
            if (fk->num_cols > TIDESDB_FK_MAX_COLS) fk->num_cols = TIDESDB_FK_MAX_COLS;

            for (uint i = 0; i < fk->num_cols && ptr + 1 <= end; i++)
            {
                fk->fk_col_idx[i] = (uint8_t)*ptr++;
            }

            if (ptr + 2 <= end)
            {
                fk->delete_rule = (int8_t)*ptr++;
                fk->update_rule = (int8_t)*ptr++;
            }

            share->num_fk++;
        }

        tidesdb_free(fk_data);
    }

    /* We now look up tables that reference this table (as parent) */
    char parent_key[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    snprintf(parent_key, sizeof(parent_key), TIDESDB_FK_PARENT_KEY_FMT, cf_name);

    uint8_t *ref_data = NULL;
    size_t ref_data_len = 0;

    ret = tidesdb_txn_get(txn, fk_meta_cf, (uint8_t *)parent_key, strlen(parent_key), &ref_data,
                          &ref_data_len);

    if (ret == TDB_SUCCESS && ref_data && ref_data_len > 0)
    {
        /*
          Parse referencing tables. Extended format:
          For each referencing table:
            -- table_name (null-terminated string)
            -- num_cols (1 byte)
            -- For each column:
              -- col_idx (1 byte) -- column index in child table
              -- offset (4 bytes) -- byte offset in child row
              -- length (4 bytes) -- byte length of column
            -- delete_rule (1 byte)
            -- update_rule (1 byte)

        */
        const uint8_t *ptr = ref_data;
        const uint8_t *end = ptr + ref_data_len;

        while (ptr < end && share->num_referencing < TIDESDB_MAX_FK)
        {
            /* We read table name */
            size_t len = strnlen((const char *)ptr, end - ptr);
            if (len == 0) break;

            uint ref_idx = share->num_referencing;
            strncpy(share->referencing_tables[ref_idx], (const char *)ptr,
                    TIDESDB_TABLE_NAME_MAX_LEN - 1);
            share->referencing_tables[ref_idx][TIDESDB_TABLE_NAME_MAX_LEN - 1] = '\0';
            ptr += len + 1;

            /* We check if extended format follows (starts with non-zero byte for num_cols) */
            if (ptr<end && * ptr> 0 && *ptr <= TIDESDB_FK_MAX_COLS)
            {
                uint8_t num_cols = *ptr++;
                share->referencing_fk_col_count[ref_idx] = num_cols;

                for (uint c = 0; c < num_cols && ptr + TIDESDB_FK_COL_META_SIZE <= end; c++)
                {
                    share->referencing_fk_cols[ref_idx][c] = *ptr++;
                    share->referencing_fk_offsets[ref_idx][c] = uint4korr(ptr);
                    ptr += 4;
                    share->referencing_fk_lengths[ref_idx][c] = uint4korr(ptr);
                    ptr += 4;
                }

                /* We read rules */
                if (ptr + 2 <= end)
                {
                    share->referencing_fk_rules[ref_idx] = (int8_t)*ptr++;
                    ptr++; /* update_rule -- skip for now */
                }
            }
            else
            {
                share->referencing_fk_col_count[ref_idx] = 0;
            }

            share->num_referencing++;
        }

        tidesdb_free(ref_data);
    }

    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);

    DBUG_RETURN(0);
}

/**
  @brief
  Check if parent row exists for a specific FK constraint.

  @param fk_idx  Index into share->fk array
  @param buf     Row buffer with FK column values
  @param txn     Transaction for lookup

  @return 0 if parent exists, HA_ERR_NO_REFERENCED_ROW if not found
*/
int ha_tidesdb::check_fk_parent_exists(uint fk_idx, const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::check_fk_parent_exists");

    if (fk_idx >= share->num_fk) DBUG_RETURN(0);

    TIDESDB_FK *fk = &share->fk[fk_idx];

    /* We build the parent table's column family name */
    char parent_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    snprintf(parent_cf_name, sizeof(parent_cf_name), TIDESDB_CF_PARENT_FMT, fk->ref_db,
             fk->ref_table);

    /* We get the parent column family */
    tidesdb_column_family_t *parent_cf =
        tidesdb_get_column_family(tidesdb_instance, parent_cf_name);
    if (!parent_cf)
    {
        /* Parent table doesnt exist in TidesDB */
        DBUG_RETURN(0);
    }

    /* We build lookup key from FK column values */
    uchar key_buf[TIDESDB_FK_KEY_BUF_SIZE];
    size_t key_len = 0;

    for (uint i = 0; i < fk->num_cols && key_len < sizeof(key_buf) - TIDESDB_FK_KEY_RESERVE; i++)
    {
        uint col_idx = fk->fk_col_idx[i];
        if (col_idx >= table->s->fields) continue;

        Field *field = table->field[col_idx];

        /* We check for NULL -- NULL FK values don't need parent check */
        if (field->is_null()) DBUG_RETURN(0); /* NULL FK is always valid */

        uint key_part_len = field->pack_length();
        if (key_len + key_part_len + 1 < sizeof(key_buf))
        {
            /* We handle NULL indicator if field may be null */
            if (field->maybe_null())
            {
                if (field->is_null())
                {
                    key_buf[key_len++] = 1; /* NULL indicator */
                    memset(key_buf + key_len, 0, key_part_len);
                    key_len += key_part_len;
                    continue;
                }
                key_buf[key_len++] = 0; /* NOT NULL indicator */
            }

            /* We copy field data in storage format */
            memcpy(key_buf + key_len, field->ptr, key_part_len);
            key_len += key_part_len;
        }
    }

    if (key_len == 0) DBUG_RETURN(0);

    /* We look up the key in parent table */
    uint8_t *value = NULL;
    size_t value_len = 0;

    int ret = tidesdb_txn_get(txn, parent_cf, key_buf, key_len, &value, &value_len);

    if (ret == TDB_SUCCESS && value)
    {
        tidesdb_free(value);
        DBUG_RETURN(0); /* Parent row exists */
    }

    DBUG_RETURN(HA_ERR_NO_REFERENCED_ROW);
}

/**
  @brief
  Check FK constraints for INSERT/UPDATE operations.

  Verifies that all FK column values reference existing parent rows.
*/
int ha_tidesdb::check_foreign_key_constraints_insert(const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::check_foreign_key_constraints_insert");

    /* Fast path -- no FK constraints on this table */
    if (share->num_fk == 0) DBUG_RETURN(0);

    THD *thd = ha_thd();

    /* We check if FK checks are disabled */
    if (thd->variables.option_bits & OPTION_NO_FOREIGN_KEY_CHECKS) DBUG_RETURN(0);

    /* We check each FK constraint */
    for (uint i = 0; i < share->num_fk; i++)
    {
        int ret = check_fk_parent_exists(i, buf, txn);
        if (ret != 0) DBUG_RETURN(ret);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Check FK constraints for DELETE operations.

  Handles FK referential actions based on the delete_rule:
  -- RESTRICT/NO ACTION (0, 3)  -- Return error if child rows exist
  -- CASCADE (1)                -- Delete child rows
  -- SET NULL (2)               -- Set FK columns to NULL in child rows

  Uses a secondary index on the FK columns in child tables for efficient lookup.
*/
int ha_tidesdb::check_foreign_key_constraints_delete(const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::check_foreign_key_constraints_delete");

    THD *thd = ha_thd();

    /* We check if FK checks are disabled */
    if (thd->variables.option_bits & OPTION_NO_FOREIGN_KEY_CHECKS) DBUG_RETURN(0);

    /* No referencing tables -- nothing to check */
    if (share->num_referencing == 0) DBUG_RETURN(0);

    /* We build the key from this row's PK (which child FK references) */
    uchar *pk = NULL;
    size_t pk_len = 0;
    int ret = build_primary_key(buf, &pk, &pk_len);
    if (ret) DBUG_RETURN(0);

    /*
      We check each child table for rows referencing this PK.
      The referencing_fk_rules array stores the delete_rule for each referencing table.
      0 = RESTRICT, 1 = CASCADE, 2 = SET NULL, 3 = NO ACTION
    */
    for (uint i = 0; i < share->num_referencing; i++)
    {
        /* We get the child table's FK index column family */
        char fk_idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
        snprintf(fk_idx_cf_name, sizeof(fk_idx_cf_name), TIDESDB_CF_FKIDX_FMT,
                 share->referencing_tables[i]);

        tidesdb_column_family_t *fk_idx_cf =
            tidesdb_get_column_family(tidesdb_instance, fk_idx_cf_name);

        if (!fk_idx_cf) continue;

        /* We look up our PK in the FK index */
        uint8_t *ref_value = NULL;
        size_t ref_value_len = 0;

        ret = tidesdb_txn_get(txn, fk_idx_cf, pk, pk_len, &ref_value, &ref_value_len);

        if (ret == TDB_SUCCESS && ref_value)
        {
            tidesdb_free(ref_value);

            /* Child row exists -- we check the delete rule */
            int delete_rule = share->referencing_fk_rules[i];

            switch (delete_rule)
            {
                case TIDESDB_FK_RULE_CASCADE:
                    ret = execute_fk_cascade_delete(buf, txn);
                    if (ret != 0) DBUG_RETURN(ret);
                    break;

                case TIDESDB_FK_RULE_SET_NULL:
                    ret = execute_fk_set_null(buf, txn);
                    if (ret != 0) DBUG_RETURN(ret);
                    break;

                case TIDESDB_FK_RULE_RESTRICT:
                case TIDESDB_FK_RULE_NO_ACTION:
                default:
                    /* FK violation -- child rows exist */
                    DBUG_RETURN(HA_ERR_ROW_IS_REFERENCED);
            }
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Execute CASCADE DELETE on child tables.

  When a parent row is deleted with ON DELETE CASCADE, this function
  finds and deletes all child rows that reference the parent.

  @param buf  Row buffer of the parent row being deleted
  @param txn  Transaction for the operation

  @return 0 on success, error code on failure
*/
int ha_tidesdb::execute_fk_cascade_delete(const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::execute_fk_cascade_delete");

    if (share->num_referencing == 0) DBUG_RETURN(0);

    /* We build the key from this row's PK */
    uchar *pk = NULL;
    size_t pk_len = 0;
    int ret = build_primary_key(buf, &pk, &pk_len);
    if (ret) DBUG_RETURN(0);

    /* For each child table that references us */
    for (uint i = 0; i < share->num_referencing; i++)
    {
        /* We get the child table's FK index column family */
        char fk_idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
        snprintf(fk_idx_cf_name, sizeof(fk_idx_cf_name), TIDESDB_CF_FKIDX_FMT,
                 share->referencing_tables[i]);

        tidesdb_column_family_t *fk_idx_cf =
            tidesdb_get_column_family(tidesdb_instance, fk_idx_cf_name);

        if (!fk_idx_cf) continue;

        /* We get the child table's main column family */
        tidesdb_column_family_t *child_cf =
            tidesdb_get_column_family(tidesdb_instance, share->referencing_tables[i]);

        if (!child_cf) continue;

        tidesdb_iter_t *iter = NULL;
        ret = tidesdb_iter_new(txn, fk_idx_cf, &iter);
        if (ret != TDB_SUCCESS || !iter) continue;

        /* We seek to our PK prefix */
        tidesdb_iter_seek(iter, pk, pk_len);

        while (tidesdb_iter_valid(iter))
        {
            uint8_t *idx_key = NULL;
            size_t idx_key_len = 0;

            if (tidesdb_iter_key(iter, &idx_key, &idx_key_len) != TDB_SUCCESS) break;

            /* We check if this key still matches our PK prefix */
            if (idx_key_len < pk_len || memcmp(idx_key, pk, pk_len) != 0) break;

            /* We get the child row's PK from the index value */
            uint8_t *child_pk = NULL;
            size_t child_pk_len = 0;

            if (tidesdb_iter_value(iter, &child_pk, &child_pk_len) == TDB_SUCCESS &&
                child_pk_len > 0)
            {
                /* We delete the child row */
                ret = tidesdb_txn_delete(txn, child_cf, child_pk, child_pk_len);
                if (ret != TDB_SUCCESS)
                {
                    sql_print_warning("TidesDB: CASCADE DELETE failed for child row: %d", ret);
                }

                /* We also delete the FK index entry */
                tidesdb_txn_delete(txn, fk_idx_cf, idx_key, idx_key_len);

                /*
                  We also need to clean up secondary, FT, and spatial index
                  entries for the deleted child row.  Look up the child table's
                  share from the global hash so we can access its index CFs.
                */
                const char *child_table_name = share->referencing_tables[i];
                uint child_name_len = (uint)strlen(child_table_name);
                TIDESDB_SHARE *child_share = NULL;

                mysql_rwlock_rdlock(&tidesdb_rwlock);
                child_share = (TIDESDB_SHARE *)my_hash_search(
                    &tidesdb_open_tables, (uchar *)child_table_name, child_name_len);
                mysql_rwlock_unlock(&tidesdb_rwlock);

                if (child_share)
                {
                    /* Delete secondary index entries for this child PK */
                    for (uint si = 0; si < child_share->num_indexes; si++)
                    {
                        if (child_share->index_cf[si])
                        {
                            /*
                              Secondary index entries have the form:
                              index_key + child_pk -> child_pk
                              We cannot reconstruct the full index key without the row
                              data, so we do a prefix scan for entries whose value
                              matches this child PK and delete them.
                            */
                            tidesdb_iter_t *si_iter = NULL;
                            if (tidesdb_iter_new(txn, child_share->index_cf[si], &si_iter) ==
                                    TDB_SUCCESS &&
                                si_iter)
                            {
                                tidesdb_iter_seek_to_first(si_iter);
                                while (tidesdb_iter_valid(si_iter))
                                {
                                    uint8_t *si_val = NULL;
                                    size_t si_val_len = 0;
                                    uint8_t *si_key = NULL;
                                    size_t si_key_len = 0;

                                    if (tidesdb_iter_value(si_iter, &si_val, &si_val_len) ==
                                            TDB_SUCCESS &&
                                        si_val_len == child_pk_len &&
                                        memcmp(si_val, child_pk, child_pk_len) == 0)
                                    {
                                        if (tidesdb_iter_key(si_iter, &si_key, &si_key_len) ==
                                            TDB_SUCCESS)
                                        {
                                            tidesdb_txn_delete(txn, child_share->index_cf[si],
                                                               si_key, si_key_len);
                                        }
                                    }
                                    tidesdb_iter_next(si_iter);
                                }
                                tidesdb_iter_free(si_iter);
                            }
                        }
                    }

                    /* We delete FT index entries for this child PK */
                    for (uint fi = 0; fi < child_share->num_ft_indexes; fi++)
                    {
                        if (child_share->ft_cf[fi])
                        {
                            tidesdb_iter_t *ft_iter = NULL;
                            if (tidesdb_iter_new(txn, child_share->ft_cf[fi], &ft_iter) ==
                                    TDB_SUCCESS &&
                                ft_iter)
                            {
                                tidesdb_iter_seek_to_first(ft_iter);
                                while (tidesdb_iter_valid(ft_iter))
                                {
                                    uint8_t *ft_val = NULL;
                                    size_t ft_val_len = 0;
                                    uint8_t *ft_key = NULL;
                                    size_t ft_key_len = 0;

                                    if (tidesdb_iter_value(ft_iter, &ft_val, &ft_val_len) ==
                                            TDB_SUCCESS &&
                                        ft_val_len == child_pk_len &&
                                        memcmp(ft_val, child_pk, child_pk_len) == 0)
                                    {
                                        if (tidesdb_iter_key(ft_iter, &ft_key, &ft_key_len) ==
                                            TDB_SUCCESS)
                                        {
                                            tidesdb_txn_delete(txn, child_share->ft_cf[fi], ft_key,
                                                               ft_key_len);
                                        }
                                    }
                                    tidesdb_iter_next(ft_iter);
                                }
                                tidesdb_iter_free(ft_iter);
                            }
                        }
                    }

                    /* We delete spatial index entries for this child PK */
                    for (uint spi = 0; spi < child_share->num_spatial_indexes; spi++)
                    {
                        if (child_share->spatial_cf[spi])
                        {
                            tidesdb_iter_t *sp_iter = NULL;
                            if (tidesdb_iter_new(txn, child_share->spatial_cf[spi], &sp_iter) ==
                                    TDB_SUCCESS &&
                                sp_iter)
                            {
                                tidesdb_iter_seek_to_first(sp_iter);
                                while (tidesdb_iter_valid(sp_iter))
                                {
                                    uint8_t *sp_val = NULL;
                                    size_t sp_val_len = 0;
                                    uint8_t *sp_key = NULL;
                                    size_t sp_key_len = 0;

                                    if (tidesdb_iter_value(sp_iter, &sp_val, &sp_val_len) ==
                                            TDB_SUCCESS &&
                                        sp_val_len == child_pk_len &&
                                        memcmp(sp_val, child_pk, child_pk_len) == 0)
                                    {
                                        if (tidesdb_iter_key(sp_iter, &sp_key, &sp_key_len) ==
                                            TDB_SUCCESS)
                                        {
                                            tidesdb_txn_delete(txn, child_share->spatial_cf[spi],
                                                               sp_key, sp_key_len);
                                        }
                                    }
                                    tidesdb_iter_next(sp_iter);
                                }
                                tidesdb_iter_free(sp_iter);
                            }
                        }
                    }
                }
            }

            tidesdb_iter_next(iter);
        }

        tidesdb_iter_free(iter);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Execute SET NULL on child tables.

  When a parent row is deleted with ON DELETE SET NULL, this function
  finds all child rows that reference the parent and sets the FK columns to NULL.

  @param buf  Row buffer of the parent row being deleted
  @param txn  Transaction for the operation

  @return 0 on success, error code on failure
*/
int ha_tidesdb::execute_fk_set_null(const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::execute_fk_set_null");

    if (share->num_referencing == 0) DBUG_RETURN(0);

    /* We build the key from this row's PK */
    uchar *pk = NULL;
    size_t pk_len = 0;
    int ret = build_primary_key(buf, &pk, &pk_len);
    if (ret) DBUG_RETURN(0);

    /* For each child table that references us */
    for (uint i = 0; i < share->num_referencing; i++)
    {
        /* We get the child table's FK index column family */
        char fk_idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
        snprintf(fk_idx_cf_name, sizeof(fk_idx_cf_name), TIDESDB_CF_FKIDX_FMT,
                 share->referencing_tables[i]);

        tidesdb_column_family_t *fk_idx_cf =
            tidesdb_get_column_family(tidesdb_instance, fk_idx_cf_name);

        if (!fk_idx_cf) continue;

        /* We get the child table's main column family */
        tidesdb_column_family_t *child_cf =
            tidesdb_get_column_family(tidesdb_instance, share->referencing_tables[i]);

        if (!child_cf) continue;

        /* We scan the FK index for all entries matching our PK */
        tidesdb_iter_t *iter = NULL;
        ret = tidesdb_iter_new(txn, fk_idx_cf, &iter);
        if (ret != TDB_SUCCESS || !iter) continue;

        /* We seek to our PK prefix */
        tidesdb_iter_seek(iter, pk, pk_len);

        while (tidesdb_iter_valid(iter))
        {
            uint8_t *idx_key = NULL;
            size_t idx_key_len = 0;

            if (tidesdb_iter_key(iter, &idx_key, &idx_key_len) != TDB_SUCCESS) break;

            /* We check if this key still matches our PK prefix */
            if (idx_key_len < pk_len || memcmp(idx_key, pk, pk_len) != 0) break;

            /* We get the child row's PK from the index value */
            uint8_t *child_pk = NULL;
            size_t child_pk_len = 0;

            if (tidesdb_iter_value(iter, &child_pk, &child_pk_len) == TDB_SUCCESS &&
                child_pk_len > 0)
            {
                /* We get the child row data */
                uint8_t *child_row = NULL;
                size_t child_row_len = 0;

                ret = tidesdb_txn_get(txn, child_cf, child_pk, child_pk_len, &child_row,
                                      &child_row_len);

                if (ret == TDB_SUCCESS && child_row)
                {
                    /*
                      SET NULL implementation:

                      The packed row format uses MySQL's/MariaDB's native format where:
                      -- The null bitmap is at offset table->s->null_bytes from start
                      -- Each field has a null bit at field->null_bit in the bitmap

                      We modify the child row in place to set FK columns to NULL,
                      then write the updated row back to storage.
                    */

                    /* We make a mutable copy of the child row */
                    uchar *modified_row =
                        (uchar *)my_malloc(PSI_INSTRUMENT_ME, child_row_len, MYF(MY_WME));
                    if (modified_row)
                    {
                        memcpy(modified_row, child_row, child_row_len);

                        /*
                          We get the child table's FK column indices from the referencing_fk_cols.
                          For each FK column, set its null bit in the null bitmap.

                          The null bitmap location depends on the table structure.
                          In MySQL's/MariaDB's row format, null flags are stored at the beginning
                          of the record, with each nullable field having a bit.

                          We get FK column info from the stored metadata.
                          The referencing_fk_cols array stores which columns in the child
                          table are FK columns pointing to this parent.
                        */
                        uint fk_col_count = share->referencing_fk_col_count[i];

                        for (uint c = 0; c < fk_col_count; c++)
                        {
                            uint col_idx = share->referencing_fk_cols[i][c];

                            /*
                              Set the null bit for this column.
                              The null bitmap is at the start of the row.
                              Each field's null bit position is stored in field->null_bit.
                              The byte offset is (col_idx / 8), bit is (col_idx % 8).
                            */
                            uint byte_offset = col_idx / 8;
                            uint bit_mask = 1 << (col_idx % 8);

                            if (byte_offset < child_row_len)
                            {
                                modified_row[byte_offset] |= bit_mask;
                            }
                        }

                        /* We write the modified row back to storage */
                        ret = tidesdb_txn_put(txn, child_cf, child_pk, child_pk_len, modified_row,
                                              child_row_len, 0);

                        if (ret != TDB_SUCCESS)
                        {
                            sql_print_warning(
                                "TidesDB: Failed to update child row for SET NULL: %d", ret);
                        }

                        my_free(modified_row);
                    }

                    /* We must delete the FK index entry since the reference is now NULL */
                    tidesdb_txn_delete(txn, fk_idx_cf, idx_key, idx_key_len);

                    tidesdb_free(child_row);
                }
            }

            tidesdb_iter_next(iter);
        }

        tidesdb_iter_free(iter);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Check FK constraints for UPDATE operations.

  Verifies that:
  1. If FK columns are changed, new values reference existing parent rows
  2. If this is a parent table and PK is changed, handle child references
     based on update_rule (RESTRICT, CASCADE, SET NULL)

  @param old_buf  Row buffer with old values
  @param new_buf  Row buffer with new values
  @param txn      Transaction for the operation

  @return 0 on success, error code on failure
*/
int ha_tidesdb::check_foreign_key_constraints_update(const uchar *old_buf, const uchar *new_buf,
                                                     tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::check_foreign_key_constraints_update");

    /* Fast path: no FK constraints and no referencing tables */
    if (share->num_fk == 0 && share->num_referencing == 0) DBUG_RETURN(0);

    THD *thd = ha_thd();

    /* We check if FK checks are disabled */
    if (thd->variables.option_bits & OPTION_NO_FOREIGN_KEY_CHECKS) DBUG_RETURN(0);

    int ret;

    /*
      Part 1 -- We check if this table's FK columns changed.
      If so, verify new values reference existing parent rows.
    */
    for (uint i = 0; i < share->num_fk; i++)
    {
        TIDESDB_FK *fk = &share->fk[i];
        bool fk_changed = false;

        /* We check if any FK column value changed by comparing field data */
        for (uint j = 0; j < fk->num_cols; j++)
        {
            uint col_idx = fk->fk_col_idx[j];
            if (col_idx >= table->s->fields) continue;

            Field *field = table->field[col_idx];

            /* W get field offset and length */
            uint field_offset = field->offset(table->record[0]);
            uint field_pack_len = field->pack_length();

            /* We compare old and new field values */
            if (memcmp(old_buf + field_offset, new_buf + field_offset, field_pack_len) != 0)
            {
                fk_changed = true;
                break;
            }
        }

        if (fk_changed)
        {
            /* We verify new FK values reference existing parent */
            ret = check_fk_parent_exists(i, new_buf, txn);
            if (ret != 0) DBUG_RETURN(ret);
        }
    }

    /*
      Part 2 -- If this table is referenced by others (parent table),
      we check if PK changed and handle child references based on update_rule.
    */
    if (share->num_referencing > 0)
    {
        /* We build old and new PKs */
        uchar *old_pk = NULL;
        size_t old_pk_len = 0;
        uchar *new_pk = NULL;
        size_t new_pk_len = 0;

        build_primary_key(old_buf, &old_pk, &old_pk_len);
        build_primary_key(new_buf, &new_pk, &new_pk_len);

        bool pk_changed = (old_pk_len != new_pk_len) ||
                          (old_pk && new_pk && memcmp(old_pk, new_pk, old_pk_len) != 0);

        if (pk_changed)
        {
            /* PK changed? -- we handle child references based on update_rule */
            for (uint i = 0; i < share->num_referencing; i++)
            {
                /* We get the child table's FK index column family */
                char fk_idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
                snprintf(fk_idx_cf_name, sizeof(fk_idx_cf_name), TIDESDB_CF_FKIDX_FMT,
                         share->referencing_tables[i]);

                tidesdb_column_family_t *fk_idx_cf =
                    tidesdb_get_column_family(tidesdb_instance, fk_idx_cf_name);

                if (!fk_idx_cf) continue;

                /* We check if any child rows reference the old PK */
                uint8_t *ref_value = NULL;
                size_t ref_value_len = 0;

                ret =
                    tidesdb_txn_get(txn, fk_idx_cf, old_pk, old_pk_len, &ref_value, &ref_value_len);

                if (ret == TDB_SUCCESS && ref_value)
                {
                    tidesdb_free(ref_value);

                    /* Child rows exist -- we check the update rule */
                    int update_rule = share->referencing_fk_rules[i];

                    switch (update_rule)
                    {
                        case TIDESDB_FK_RULE_CASCADE: /* CASCADE -- we update child FK values to new
                                                         PK */
                            ret = execute_fk_cascade_update(old_buf, new_buf, txn, i);
                            if (ret != 0) DBUG_RETURN(ret);
                            break;

                        case TIDESDB_FK_RULE_SET_NULL: /* SET NULL -- we set child FK columns to
                                                          NULL */
                            ret = execute_fk_set_null(old_buf, txn);
                            if (ret != 0) DBUG_RETURN(ret);
                            break;

                        case TIDESDB_FK_RULE_RESTRICT:
                        case TIDESDB_FK_RULE_NO_ACTION:
                        default:
                            /* FK violation -- child rows reference this PK */
                            DBUG_RETURN(HA_ERR_ROW_IS_REFERENCED);
                    }
                }
            }
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Execute CASCADE UPDATE on child tables.

  When a parent row's PK is updated with ON UPDATE CASCADE, this function
  finds all child rows that reference the old PK and updates their FK
  columns to the new PK value.

  @param old_buf        Row buffer with old parent values
  @param new_buf        Row buffer with new parent values
  @param txn            Transaction for the operation
  @param ref_idx        Index into referencing_tables array

  @return 0 on success, error code on failure
*/
int ha_tidesdb::execute_fk_cascade_update(const uchar *old_buf, const uchar *new_buf,
                                          tidesdb_txn_t *txn, uint ref_idx)
{
    DBUG_ENTER("ha_tidesdb::execute_fk_cascade_update");

    /* We build old and new PKs */
    uchar *old_pk = NULL;
    size_t old_pk_len = 0;
    uchar *new_pk = NULL;
    size_t new_pk_len = 0;

    int ret = build_primary_key(old_buf, &old_pk, &old_pk_len);
    if (ret) DBUG_RETURN(0);

    ret = build_primary_key(new_buf, &new_pk, &new_pk_len);
    if (ret) DBUG_RETURN(0);

    /* We get the child table's FK index column family */
    char fk_idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    snprintf(fk_idx_cf_name, sizeof(fk_idx_cf_name), TIDESDB_CF_FKIDX_FMT,
             share->referencing_tables[ref_idx]);

    tidesdb_column_family_t *fk_idx_cf =
        tidesdb_get_column_family(tidesdb_instance, fk_idx_cf_name);

    if (!fk_idx_cf) DBUG_RETURN(0);

    /* We get the child table's main column family */
    tidesdb_column_family_t *child_cf =
        tidesdb_get_column_family(tidesdb_instance, share->referencing_tables[ref_idx]);

    if (!child_cf) DBUG_RETURN(0);

    /* We scan the FK index for all entries matching old PK */
    tidesdb_iter_t *iter = NULL;
    ret = tidesdb_iter_new(txn, fk_idx_cf, &iter);
    if (ret != TDB_SUCCESS || !iter) DBUG_RETURN(0);

    tidesdb_iter_seek(iter, old_pk, old_pk_len);

    while (tidesdb_iter_valid(iter))
    {
        uint8_t *idx_key = NULL;
        size_t idx_key_len = 0;

        if (tidesdb_iter_key(iter, &idx_key, &idx_key_len) != TDB_SUCCESS) break;

        /* We check if this key still matches old PK prefix */
        if (idx_key_len < old_pk_len || memcmp(idx_key, old_pk, old_pk_len) != 0) break;

        /* We get the child row's PK from the index value */
        uint8_t *child_pk = NULL;
        size_t child_pk_len = 0;

        if (tidesdb_iter_value(iter, &child_pk, &child_pk_len) == TDB_SUCCESS && child_pk_len > 0)
        {
            /* We get the child row data */
            uint8_t *child_row = NULL;
            size_t child_row_len = 0;

            ret =
                tidesdb_txn_get(txn, child_cf, child_pk, child_pk_len, &child_row, &child_row_len);

            if (ret == TDB_SUCCESS && child_row)
            {
                /*
                  CASCADE UPDATE implementation:

                  We need to update the FK column values in the child row from the
                  old parent PK to the new parent PK. The FK columns in the child
                  table store the parent's PK values.

                  Since we store rows using MySQL's/MariaDB's native format, the FK column
                  offsets are stored in share->referencing_fk_offsets[ref_idx][].
                  These offsets were populated when the FK relationship was registered.

                  For each FK column, we copy the corresponding bytes from new_pk
                  to the child row at the stored offset.
                */

                /* We make a mutable copy of the child row */
                uchar *modified_row =
                    (uchar *)my_malloc(PSI_INSTRUMENT_ME, child_row_len, MYF(MY_WME));
                if (modified_row)
                {
                    memcpy(modified_row, child_row, child_row_len);

                    /*
                      We update FK column values in the child row.

                      The FK columns reference the parent's PK columns. We need to
                      copy the new PK values to the FK column positions in the child row.

                      share->referencing_fk_offsets[ref_idx][c] = byte offset in child row
                      share->referencing_fk_lengths[ref_idx][c] = byte length of column
                    */
                    uint fk_col_count = share->referencing_fk_col_count[ref_idx];
                    size_t src_offset = 0; /* Offset within new_pk */

                    for (uint c = 0; c < fk_col_count && src_offset < new_pk_len; c++)
                    {
                        size_t dst_offset = share->referencing_fk_offsets[ref_idx][c];
                        size_t col_len = share->referencing_fk_lengths[ref_idx][c];

                        /* Bounds check */
                        if (dst_offset + col_len <= child_row_len &&
                            src_offset + col_len <= new_pk_len)
                        {
                            memcpy(modified_row + dst_offset, new_pk + src_offset, col_len);
                        }
                        src_offset += col_len;
                    }

                    /* We write the modified row back to storage */
                    ret = tidesdb_txn_put(txn, child_cf, child_pk, child_pk_len, modified_row,
                                          child_row_len, 0);

                    my_free(modified_row);
                }

                /* We delete old FK index entry */
                tidesdb_txn_delete(txn, fk_idx_cf, idx_key, idx_key_len);

                /* We insert new FK index entry with new PK */
                size_t new_idx_key_len = new_pk_len + (idx_key_len - old_pk_len);
                uchar *new_idx_key =
                    (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_idx_key_len, MYF(MY_WME));
                if (new_idx_key)
                {
                    memcpy(new_idx_key, new_pk, new_pk_len);
                    if (idx_key_len > old_pk_len)
                    {
                        memcpy(new_idx_key + new_pk_len, idx_key + old_pk_len,
                               idx_key_len - old_pk_len);
                    }

                    tidesdb_txn_put(txn, fk_idx_cf, new_idx_key, new_idx_key_len, child_pk,
                                    child_pk_len, 0);

                    my_free(new_idx_key);
                }

                tidesdb_free(child_row);
            }
        }

        tidesdb_iter_next(iter);
    }

    tidesdb_iter_free(iter);

    DBUG_RETURN(0);
}

/*
  Z-Order (Morton Code) Spatial Indexing

  TidesDB implements spatial indexing using Z-order curves (Morton codes).
  This maps 2D coordinates to a 1D key while preserving spatial locality,
  enabling efficient range queries on an LSM-tree.

  The encoding interleaves bits of X and Y coordinates:
  X = x3 x2 x1 x0
  Y = y3 y2 y1 y0
  Z = y3 x3 y2 x2 y1 x1 y0 x0

  @brief
  Encode X,Y coordinates as a Z-order (Morton) curve value.

  Interleaves bits of normalized X and Y values to create a space-filling
  curve that preserves 2D locality in 1D ordering.

  @param x  X coordinate (normalized to 0..1 range)
  @param y  Y coordinate (normalized to 0..1 range)

  @return 64-bit Z-order encoded value
*/
static uint64_t encode_zorder(double x, double y)
{
    /* We normalize to 32-bit integer range */
    uint32_t ix = (uint32_t)(x * (double)TIDESDB_ZORDER_MAX_VALUE);
    uint32_t iy = (uint32_t)(y * (double)TIDESDB_ZORDER_MAX_VALUE);

    /* We interleave bits using the "magic bits" method */
    uint64_t z = 0;

    for (int i = 0; i < TIDESDB_ZORDER_BITS; i++)
    {
        z |= ((uint64_t)((ix >> i) & 1) << (2 * i));
        z |= ((uint64_t)((iy >> i) & 1) << (2 * i + 1));
    }

    return z;
}

/**
  @brief
  Decode a Z-order value back to X,Y coordinates.

  @param z  Z-order encoded value
  @param x  Output X coordinate (normalized 0..1)
  @param y  Output Y coordinate (normalized 0..1)
*/
void ha_tidesdb::decode_zorder(uint64_t z, double *x, double *y)
{
    uint32_t ix = 0, iy = 0;

    for (int i = 0; i < TIDESDB_ZORDER_BITS; i++)
    {
        ix |= ((z >> (2 * i)) & 1) << i;
        iy |= ((z >> (2 * i + 1)) & 1) << i;
    }

    *x = (double)ix / (double)TIDESDB_ZORDER_MAX_VALUE;
    *y = (double)iy / (double)TIDESDB_ZORDER_MAX_VALUE;
}

/**
  @brief
  Create a spatial index column family for a table.

  @param table_name  Full table path
  @param key_nr      Key number of the spatial index

  @return 0 on success
*/
int ha_tidesdb::create_spatial_index(const char *table_name, uint key_nr)
{
    DBUG_ENTER("ha_tidesdb::create_spatial_index");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    char spatial_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    get_cf_name(table_name, cf_name, sizeof(cf_name));

    snprintf(spatial_cf_name, sizeof(spatial_cf_name), TIDESDB_CF_SPATIAL_FMT, cf_name, key_nr);

    tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
    cf_config.write_buffer_size = tidesdb_write_buffer_size;
    cf_config.enable_bloom_filter = 1;
    cf_config.bloom_fpr = TIDESDB_DEFAULT_BLOOM_FPR;
    cf_config.use_btree = tidesdb_use_btree ? 1 : 0;

    int ret = tidesdb_create_column_family(tidesdb_instance, spatial_cf_name, &cf_config);
    if (ret != TDB_SUCCESS && ret != TDB_ERR_EXISTS)
    {
        sql_print_error("TidesDB: Failed to create spatial index CF '%s': %d", spatial_cf_name,
                        ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    sql_print_information("TidesDB: Created spatial index '%s'", spatial_cf_name);

    DBUG_RETURN(0);
}

/**
  @brief
  Extract Z-order value from a WKB geometry field.

  Parses the geometry to compute its bounding box center, normalizes
  to WGS84 range, and encodes as a Z-order curve value.

  Supports: POINT (1), LINESTRING (2), POLYGON (3), MULTIPOINT (4).

  @param geom_field  Geometry field to extract from
  @param[out] z_out  Computed Z-order value

  @return 0 on success, non-zero on failure (NULL field, unsupported type, etc.)
*/
static int compute_spatial_zorder(Field *geom_field, uint64_t *z_out)
{
    if (geom_field->is_null()) return -1;

    String geom_str;
    geom_field->val_str(&geom_str);

    if (geom_str.length() < TIDESDB_MIN_WKB_POINT_WITH_SRID) /* Minimum WKB size for a point */
        return -1;

    /*
      Extract bounding box from WKB geometry.
      Supports -- POINT (1), LINESTRING (2), POLYGON (3), MULTIPOINT (4)
      Format   -- byte_order(1) + type(4) + coordinates...
    */
    const uchar *wkb = (const uchar *)geom_str.ptr();
    size_t wkb_len = geom_str.length();

    /* We skip SRID if present (4 bytes) -- MariaDB stores SRID prefix */
    if (wkb_len >= TIDESDB_MIN_WKB_POINT_SIZE)
    {
        wkb += TIDESDB_WKB_SRID_SIZE;
        wkb_len -= TIDESDB_WKB_SRID_SIZE;
    }

    if (wkb_len < TIDESDB_MIN_WKB_POINT_NO_SRID) /* Minimum for a point */
        return -1;

    /* We read byte order */
    int byte_order = wkb[TIDESDB_WKB_BYTE_ORDER_OFFSET];

    /* We read geometry type */
    uint32_t wkb_type;
    if (byte_order == TIDESDB_WKB_LITTLE_ENDIAN) /* Little endian */
    {
        wkb_type = uint4korr(wkb + TIDESDB_WKB_BYTE_ORDER_SIZE);
    }
    else /* Big endian */
    {
        wkb_type = ((uint32_t)wkb[1] << 24) | ((uint32_t)wkb[2] << 16) | ((uint32_t)wkb[3] << 8) |
                   (uint32_t)wkb[4];
    }

    /* We define a helper lambda to read a double with byte order handling */
    auto read_double = [byte_order](const uchar *ptr) -> double
    {
        double val;
        if (byte_order == TIDESDB_WKB_LITTLE_ENDIAN) /* Little endian */
        {
            memcpy(&val, ptr, TIDESDB_WKB_COORD_SIZE);
        }
        else /* Big endian -- swap bytes */
        {
            uchar swapped[TIDESDB_WKB_COORD_SIZE];
            for (int i = 0; i < TIDESDB_WKB_COORD_SIZE; i++)
                swapped[i] = ptr[TIDESDB_WKB_COORD_SIZE - 1 - i];
            memcpy(&val, swapped, TIDESDB_WKB_COORD_SIZE);
        }
        return val;
    };

    double min_x, min_y, max_x, max_y;
    const uchar *coords = wkb + TIDESDB_WKB_HEADER_SIZE; /* Skip byte_order(1) + type(4) */

    switch (wkb_type & TIDESDB_WKB_TYPE_MASK) /* We mask to get base type (ignore Z/M flags) */
    {
        case TIDESDB_WKB_TYPE_POINT: /* POINT */
        {
            min_x = max_x = read_double(coords);
            min_y = max_y = read_double(coords + TIDESDB_WKB_COORD_SIZE);
            break;
        }
        case TIDESDB_WKB_TYPE_LINESTRING: /* LINESTRING */
        {
            if (wkb_len < TIDESDB_WKB_MIN_LINESTRING_LEN) return -1;
            uint32_t num_points;
            if (byte_order == TIDESDB_WKB_LITTLE_ENDIAN)
                num_points = uint4korr(coords);
            else
                num_points = ((uint32_t)coords[0] << 24) | ((uint32_t)coords[1] << 16) |
                             ((uint32_t)coords[2] << 8) | (uint32_t)coords[3];

            if (num_points == 0 ||
                wkb_len < TIDESDB_WKB_MIN_LINESTRING_LEN + num_points * TIDESDB_WKB_POINT_SIZE)
                return -1;

            coords += TIDESDB_WKB_NUM_POINTS_SIZE; /* We skip num_points */
            min_x = max_x = read_double(coords);
            min_y = max_y = read_double(coords + TIDESDB_WKB_COORD_SIZE);

            for (uint32_t i = 1; i < num_points; i++)
            {
                double px = read_double(coords + i * TIDESDB_WKB_POINT_SIZE);
                double py =
                    read_double(coords + i * TIDESDB_WKB_POINT_SIZE + TIDESDB_WKB_COORD_SIZE);
                if (px < min_x) min_x = px;
                if (px > max_x) max_x = px;
                if (py < min_y) min_y = py;
                if (py > max_y) max_y = py;
            }
            break;
        }
        case TIDESDB_WKB_TYPE_POLYGON: /* POLYGON */
        {
            if (wkb_len < TIDESDB_WKB_MIN_LINESTRING_LEN) return -1;
            uint32_t num_rings;
            if (byte_order == TIDESDB_WKB_LITTLE_ENDIAN)
                num_rings = uint4korr(coords);
            else
                num_rings = ((uint32_t)coords[0] << 24) | ((uint32_t)coords[1] << 16) |
                            ((uint32_t)coords[2] << 8) | (uint32_t)coords[3];

            if (num_rings == 0) return -1;

            coords += TIDESDB_WKB_NUM_POINTS_SIZE; /* Skip num_rings */

            /* We read first ring (exterior) to get bounding box */
            uint32_t num_points;
            if (byte_order == TIDESDB_WKB_LITTLE_ENDIAN)
                num_points = uint4korr(coords);
            else
                num_points = ((uint32_t)coords[0] << 24) | ((uint32_t)coords[1] << 16) |
                             ((uint32_t)coords[2] << 8) | (uint32_t)coords[3];

            if (num_points == 0) return -1;

            coords += TIDESDB_WKB_NUM_POINTS_SIZE; /* Skip num_points */
            min_x = max_x = read_double(coords);
            min_y = max_y = read_double(coords + TIDESDB_WKB_COORD_SIZE);

            for (uint32_t i = 1; i < num_points; i++)
            {
                double px = read_double(coords + i * TIDESDB_WKB_POINT_SIZE);
                double py =
                    read_double(coords + i * TIDESDB_WKB_POINT_SIZE + TIDESDB_WKB_COORD_SIZE);
                if (px < min_x) min_x = px;
                if (px > max_x) max_x = px;
                if (py < min_y) min_y = py;
                if (py > max_y) max_y = py;
            }
            break;
        }
        case TIDESDB_WKB_TYPE_MULTIPOINT: /* MULTIPOINT */
        {
            if (wkb_len < TIDESDB_WKB_MIN_LINESTRING_LEN) return -1;
            uint32_t num_points;
            if (byte_order == TIDESDB_WKB_LITTLE_ENDIAN)
                num_points = uint4korr(coords);
            else
                num_points = ((uint32_t)coords[0] << 24) | ((uint32_t)coords[1] << 16) |
                             ((uint32_t)coords[2] << 8) | (uint32_t)coords[3];

            if (num_points == 0) return -1;

            coords += TIDESDB_WKB_NUM_POINTS_SIZE; /* Skip num_points */

            /* Each point has -- byte_order(1) + type(4) + x(8) + y(8) */
            min_x = max_x = read_double(coords + TIDESDB_WKB_HEADER_SIZE);
            min_y = max_y = read_double(coords + TIDESDB_WKB_Y_OFFSET);

            for (uint32_t i = 1; i < num_points; i++)
            {
                const uchar *pt = coords + i * TIDESDB_MIN_WKB_POINT_NO_SRID;
                double px = read_double(pt + TIDESDB_WKB_HEADER_SIZE);
                double py = read_double(pt + TIDESDB_WKB_Y_OFFSET);
                if (px < min_x) min_x = px;
                if (px > max_x) max_x = px;
                if (py < min_y) min_y = py;
                if (py > max_y) max_y = py;
            }
            break;
        }
        default:
            /* Unsupported geometry type */
            return -1;
    }

    /* We use center point for Z-order encoding */
    double x = (min_x + max_x) / 2.0;
    double y = (min_y + max_y) / 2.0;

    /* We normalize coordinates to 0..1 range */
    /* Using WGS84 bounds -- ( lon -180..180, lat -90..90) */
    double norm_x = (x + TIDESDB_WGS84_LON_MAX) / (2.0 * TIDESDB_WGS84_LON_MAX);
    double norm_y = (y + TIDESDB_WGS84_LAT_MAX) / (2.0 * TIDESDB_WGS84_LAT_MAX);

    /* We clamp to valid range */
    if (norm_x < 0) norm_x = 0;
    if (norm_x > 1) norm_x = 1;
    if (norm_y < 0) norm_y = 0;
    if (norm_y > 1) norm_y = 1;

    *z_out = encode_zorder(norm_x, norm_y);
    return 0;
}

/**
  @brief
  Build a spatial index key from a Z-order value and primary key.

  Stores Z-order in big-endian byte order (using mi_int8store) so that
  memcmp-based comparison in the LSM tree preserves Z-order proximity.

  @param z                 Z-order value
  @param pk                Primary key bytes
  @param pk_len            Primary key length
  @param[out] out_key      Allocated spatial key (caller must free)
  @param[out] out_key_len  Length of spatial key

  @return 0 on success
*/
static int build_spatial_key(uint64_t z, const uchar *pk, size_t pk_len, uchar **out_key,
                             size_t *out_key_len)
{
    size_t key_len = TIDESDB_ZORDER_KEY_SIZE + pk_len;
    uchar *spatial_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_len, MYF(MY_WME));
    if (!spatial_key) return HA_ERR_OUT_OF_MEM;

    /* We store Z-order as big-endian for proper sort order in LSM tree */
    mi_int8store(spatial_key, z);
    memcpy(spatial_key + TIDESDB_ZORDER_KEY_SIZE, pk, pk_len);

    *out_key = spatial_key;
    *out_key_len = key_len;
    return 0;
}

/**
  @brief
  Insert an entry into a spatial index.

  Extracts geometry from the row, computes bounding box, encodes as Z-order,
  and stores in the spatial index column family.

  @param idx  Spatial index number (in share->spatial_cf array)
  @param buf  Row buffer
  @param txn  Transaction

  @return 0 on success
*/
int ha_tidesdb::insert_spatial_entry(uint idx, const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::insert_spatial_entry");

    if (idx >= share->num_spatial_indexes || !share->spatial_cf[idx]) DBUG_RETURN(0);

    uint key_nr = share->spatial_key_nr[idx];
    KEY *key = &table->key_info[key_nr];

    /* We get the geometry field */
    if (key->user_defined_key_parts == 0) DBUG_RETURN(0);

    Field *geom_field = key->key_part[0].field;

    uint64_t z;
    if (compute_spatial_zorder(geom_field, &z) != 0) DBUG_RETURN(0);

    /* We build key -- z_order + primary_key */
    uchar *pk = NULL;
    size_t pk_len = 0;
    int ret = build_primary_key(buf, &pk, &pk_len);
    if (ret) DBUG_RETURN(ret);

    uchar *spatial_key = NULL;
    size_t key_len = 0;
    ret = build_spatial_key(z, pk, pk_len, &spatial_key, &key_len);
    if (ret) DBUG_RETURN(ret);

    /* We insert into spatial index: z_key -> pk */
    ret = tidesdb_txn_put(txn, share->spatial_cf[idx], spatial_key, key_len, pk, pk_len, -1);

    my_free(spatial_key);

    if (ret != TDB_SUCCESS)
    {
        sql_print_error("TidesDB: Failed to insert spatial entry: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Delete an entry from a spatial index.

  Uses the same geometry parsing as insert_spatial_entry to compute
  the Z-order key, then deletes it from the spatial column family.

  @param idx  Spatial index number
  @param buf  Row buffer
  @param txn  Transaction

  @return 0 on success
*/
int ha_tidesdb::delete_spatial_entry(uint idx, const uchar *buf, tidesdb_txn_t *txn)
{
    DBUG_ENTER("ha_tidesdb::delete_spatial_entry");

    if (idx >= share->num_spatial_indexes || !share->spatial_cf[idx]) DBUG_RETURN(0);

    uint key_nr = share->spatial_key_nr[idx];
    KEY *key = &table->key_info[key_nr];

    if (key->user_defined_key_parts == 0) DBUG_RETURN(0);

    Field *geom_field = key->key_part[0].field;

    uint64_t z;
    if (compute_spatial_zorder(geom_field, &z) != 0) DBUG_RETURN(0);

    uchar *pk = NULL;
    size_t pk_len = 0;
    int ret = build_primary_key(buf, &pk, &pk_len);
    if (ret) DBUG_RETURN(ret);

    uchar *spatial_key = NULL;
    size_t key_len = 0;
    ret = build_spatial_key(z, pk, pk_len, &spatial_key, &key_len);
    if (ret) DBUG_RETURN(ret);

    ret = tidesdb_txn_delete(txn, share->spatial_cf[idx], spatial_key, key_len);

    my_free(spatial_key);

    if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
    {
        sql_print_error("TidesDB: Failed to delete spatial entry: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Open a table.
*/
int ha_tidesdb::open(const char *name, int mode, uint test_if_locked)
{
    DBUG_ENTER("ha_tidesdb::open");

    if (!(share = get_share(name, table))) DBUG_RETURN(1);

    thr_lock_data_init(&share->lock, &lock, NULL);

    /* We get the column family for this table */
    if (!share->cf)
    {
        char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
        get_cf_name(name, cf_name, sizeof(cf_name));

        share->cf = tidesdb_get_column_family(tidesdb_instance, cf_name);
        if (!share->cf)
        {
            /* The column family doesn't exist -- table wasn't created properly */
            free_share(share);
            DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
        }
    }

    /* We check if table has a primary key */
    share->has_primary_key = (table->s->primary_key != MAX_KEY);
    if (share->has_primary_key)
    {
        share->pk_parts = table->key_info[table->s->primary_key].user_defined_key_parts;
    }

    /* We set ref_length for position() */
    if (share->has_primary_key)
    {
        ref_length = table->key_info[table->s->primary_key].key_length;
    }
    else
    {
        ref_length = TIDESDB_HIDDEN_PK_LEN; /* Hidden 8-byte PK */
    }

    /* We check for TTL column (TTL is the primary name, _ttl for backwards compatibility) */
    share->ttl_field_index = -1;
    for (uint i = 0; i < table->s->fields; i++)
    {
        Field *field = table->field[i];
        const char *field_name = field->field_name.str;
        if (tidesdb_strcasecmp(field_name, TIDESDB_TTL_COLUMN_NAME) == 0 ||
            tidesdb_strcasecmp(field_name, TIDESDB_TTL_COLUMN_NAME_ALT) == 0)
        {
            /* We found TTL column -- must be an integer type */
            if (field->type() == MYSQL_TYPE_LONG || field->type() == MYSQL_TYPE_LONGLONG ||
                field->type() == MYSQL_TYPE_INT24 || field->type() == MYSQL_TYPE_SHORT ||
                field->type() == MYSQL_TYPE_TINY)
            {
                share->ttl_field_index = i;
                sql_print_information("TidesDB: Table '%s' has TTL column at index %d", name, i);
            }
            break;
        }
    }

    /* We load hidden PK value for tables without explicit primary key */
    if (!share->has_primary_key)
    {
        load_hidden_pk_value();
    }

    /* We pre-allocate key buffers based on max key length to avoid per-row realloc */
    {
        size_t max_key_len = ref_length;
        for (uint i = 0; i < table->s->keys; i++)
        {
            if (table->key_info[i].key_length > max_key_len)
                max_key_len = table->key_info[i].key_length;
        }
        /* We add margin for composite key separators and hidden PK suffix */
        size_t prealloc = max_key_len + TIDESDB_HIDDEN_PK_LEN + TIDESDB_KEY_BUF_PADDING;
        if (prealloc < TIDESDB_INITIAL_KEY_BUF_CAPACITY)
            prealloc = TIDESDB_INITIAL_KEY_BUF_CAPACITY;

        if (!current_key || current_key_capacity < prealloc)
        {
            if (current_key) my_free(current_key);
            current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, prealloc, MYF(MY_WME));
            current_key_capacity = current_key ? prealloc : 0;
        }
        if (!saved_key_buffer || saved_key_buffer_capacity < prealloc)
        {
            if (saved_key_buffer) my_free(saved_key_buffer);
            saved_key_buffer = (uchar *)my_malloc(PSI_INSTRUMENT_ME, prealloc, MYF(MY_WME));
            saved_key_buffer_capacity = saved_key_buffer ? prealloc : 0;
        }
        if (!idx_key_buffer || idx_key_buffer_capacity < prealloc)
        {
            if (idx_key_buffer) my_free(idx_key_buffer);
            idx_key_buffer = (uchar *)my_malloc(PSI_INSTRUMENT_ME, prealloc, MYF(MY_WME));
            idx_key_buffer_capacity = idx_key_buffer ? prealloc : 0;
        }
    }

    load_table_stats();

    open_secondary_indexes(name);

    open_fulltext_indexes(name);

    /* We open spatial indexes */
    share->num_spatial_indexes = 0;
    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    get_cf_name(name, cf_name, sizeof(cf_name));
    for (uint i = 0; i < table->s->keys && share->num_spatial_indexes < TIDESDB_MAX_INDEXES; i++)
    {
        KEY *key = &table->key_info[i];
        if (key->algorithm == HA_KEY_ALG_RTREE)
        {
            char spatial_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
            snprintf(spatial_cf_name, sizeof(spatial_cf_name), TIDESDB_CF_SPATIAL_FMT, cf_name, i);
            tidesdb_column_family_t *spatial_cf =
                tidesdb_get_column_family(tidesdb_instance, spatial_cf_name);
            if (spatial_cf)
            {
                share->spatial_cf[share->num_spatial_indexes] = spatial_cf;
                share->spatial_key_nr[share->num_spatial_indexes] = i;
                share->num_spatial_indexes++;
            }
        }
    }

    parse_foreign_keys();

    DBUG_RETURN(0);
}

/**
  @brief
  Clone the handler for parallel operations.

  This is used by:
  -- DS-MRR (Disk-Sweep Multi-Range Read) which needs two handlers
  -- Parallel query execution
  -- Unique hash key lookups (WITHOUT OVERLAPS)

  @param name      Table name
  @param mem_root  Memory root for allocations

  @return Cloned handler or NULL on failure

  @note We use the base class implementation which handles all the
        complexity of cloning properly. TidesDB handlers can be cloned
        because they share the same TIDESDB_SHARE and column family handles.
*/
handler *ha_tidesdb::clone(const char *name, MEM_ROOT *mem_root)
{
    DBUG_ENTER("ha_tidesdb::clone");

    /* We use base class clone implementation */
    handler *new_handler = handler::clone(name, mem_root);

    if (new_handler)
    {
        /* We set optimizer costs for the clone */
        new_handler->set_optimizer_costs(ha_thd());
    }

    DBUG_RETURN(new_handler);
}

/**
  @brief
  Close a table.
*/
int ha_tidesdb::close(void)
{
    DBUG_ENTER("ha_tidesdb::close");

    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
    }

    if (share && !share->has_primary_key && share->hidden_pk_value > 0)
    {
        persist_hidden_pk_value(share->hidden_pk_value);
    }

    DBUG_RETURN(free_share(share));
}

/**
  @brief
  Create a new table (column family in TidesDB).
*/
int ha_tidesdb::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
    DBUG_ENTER("ha_tidesdb::create");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    get_cf_name(name, cf_name, sizeof(cf_name));

    tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
    cf_config.write_buffer_size = tidesdb_write_buffer_size;
    cf_config.enable_bloom_filter = tidesdb_enable_bloom_filter ? 1 : 0;
    cf_config.bloom_fpr = tidesdb_bloom_fpr;
    cf_config.level_size_ratio = tidesdb_level_size_ratio;
    cf_config.skip_list_max_level = tidesdb_skip_list_max_level;
    cf_config.skip_list_probability = (float)tidesdb_skip_list_probability;
    cf_config.enable_block_indexes = tidesdb_enable_block_indexes ? 1 : 0;
    cf_config.index_sample_ratio = tidesdb_index_sample_ratio;
    cf_config.block_index_prefix_len = tidesdb_block_index_prefix_len;
    cf_config.sync_mode = tidesdb_sync_mode;
    cf_config.sync_interval_us = tidesdb_sync_interval_us;
    cf_config.default_isolation_level = (tidesdb_isolation_level_t)tidesdb_default_isolation;
    cf_config.min_levels = tidesdb_min_levels;
    cf_config.dividing_level_offset = tidesdb_dividing_level_offset;
    cf_config.klog_value_threshold = tidesdb_klog_value_threshold;
    cf_config.min_disk_space = tidesdb_min_disk_space;
    cf_config.l1_file_count_trigger = tidesdb_l1_file_count_trigger;
    cf_config.l0_queue_stall_threshold = tidesdb_l0_queue_stall_threshold;
    cf_config.use_btree = tidesdb_use_btree ? 1 : 0;

    if (tidesdb_enable_compression)
    {
        cf_config.compression_algorithm = (compression_algorithm)tidesdb_compression_algo;
    }
    else
    {
        cf_config.compression_algorithm = TDB_COMPRESS_NONE;
    }

    /*
      Apply per-table CREATE TABLE options if specified.
      These override the global system variable defaults.

      Example:
        CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=TidesDB
          COMPRESSION='zstd' BLOOM_FILTER=1 BLOOM_FPR=0.001
          WRITE_BUFFER_SIZE=134217728 TTL=3600 USE_BTREE=0;
    */
    ha_table_option_struct *opts = create_info->option_struct;
    if (opts)
    {
        /* --- Storage format --- */

        /* Compression algorithm */
        if (opts->compression && opts->compression[0] != '\0')
        {
            if (tidesdb_strcasecmp(opts->compression, "none") == 0)
                cf_config.compression_algorithm = TDB_COMPRESS_NONE;
            else if (tidesdb_strcasecmp(opts->compression, "snappy") == 0)
                cf_config.compression_algorithm = (compression_algorithm)1;
            else if (tidesdb_strcasecmp(opts->compression, "lz4") == 0)
                cf_config.compression_algorithm = (compression_algorithm)2;
            else if (tidesdb_strcasecmp(opts->compression, "zstd") == 0)
                cf_config.compression_algorithm = (compression_algorithm)3;
            else if (tidesdb_strcasecmp(opts->compression, "lz4_fast") == 0)
                cf_config.compression_algorithm = (compression_algorithm)4;
            else
                sql_print_warning("TidesDB: Unknown compression '%s', using global default",
                                  opts->compression);
        }

        /* B+tree format */
        cf_config.use_btree = opts->use_btree ? 1 : 0;

        /* --- Memory / write path --- */

        if (opts->write_buffer_size > 0) cf_config.write_buffer_size = opts->write_buffer_size;

        if (opts->skip_list_max_level > 0)
            cf_config.skip_list_max_level = (int)opts->skip_list_max_level;

        if (opts->skip_list_probability > 0)
            cf_config.skip_list_probability = (float)opts->skip_list_probability / 10000.0f;

        /* --- Bloom filter --- */

        cf_config.enable_bloom_filter = opts->bloom_filter ? 1 : 0;

        if (opts->bloom_fpr > 0) cf_config.bloom_fpr = (double)opts->bloom_fpr / 10000.0;

        /* --- Block indexes --- */

        cf_config.enable_block_indexes = opts->block_indexes ? 1 : 0;

        if (opts->index_sample_ratio > 0)
            cf_config.index_sample_ratio = (int)opts->index_sample_ratio;

        if (opts->block_index_prefix_len > 0)
            cf_config.block_index_prefix_len = (int)opts->block_index_prefix_len;

        /* --- LSM compaction --- */

        if (opts->level_size_ratio > 0) cf_config.level_size_ratio = opts->level_size_ratio;

        if (opts->min_levels > 0) cf_config.min_levels = (int)opts->min_levels;

        if (opts->dividing_level_offset > 0)
            cf_config.dividing_level_offset = (int)opts->dividing_level_offset;

        if (opts->l1_file_count_trigger > 0)
            cf_config.l1_file_count_trigger = (int)opts->l1_file_count_trigger;

        if (opts->l0_queue_stall_threshold > 0)
            cf_config.l0_queue_stall_threshold = (int)opts->l0_queue_stall_threshold;

        /* --- Durability --- */

        if (opts->sync_mode && opts->sync_mode[0] != '\0')
        {
            if (tidesdb_strcasecmp(opts->sync_mode, "none") == 0)
                cf_config.sync_mode = 0;
            else if (tidesdb_strcasecmp(opts->sync_mode, "interval") == 0)
                cf_config.sync_mode = 1;
            else if (tidesdb_strcasecmp(opts->sync_mode, "full") == 0)
                cf_config.sync_mode = 2;
            else
                sql_print_warning("TidesDB: Unknown sync_mode '%s', using global default",
                                  opts->sync_mode);
        }

        if (opts->sync_interval_us > 0) cf_config.sync_interval_us = opts->sync_interval_us;

        /* --- Value log --- */

        if (opts->klog_value_threshold > 0)
            cf_config.klog_value_threshold = opts->klog_value_threshold;

        /* --- Disk space --- */

        if (opts->min_disk_space > 0) cf_config.min_disk_space = opts->min_disk_space;

        /* --- Isolation level --- */

        if (opts->isolation_level && opts->isolation_level[0] != '\0')
        {
            if (tidesdb_strcasecmp(opts->isolation_level, "read_uncommitted") == 0)
                cf_config.default_isolation_level = TDB_ISOLATION_READ_UNCOMMITTED;
            else if (tidesdb_strcasecmp(opts->isolation_level, "read_committed") == 0)
                cf_config.default_isolation_level = TDB_ISOLATION_READ_COMMITTED;
            else if (tidesdb_strcasecmp(opts->isolation_level, "repeatable_read") == 0)
                cf_config.default_isolation_level = TDB_ISOLATION_REPEATABLE_READ;
            else if (tidesdb_strcasecmp(opts->isolation_level, "snapshot") == 0)
                cf_config.default_isolation_level = TDB_ISOLATION_SNAPSHOT;
            else if (tidesdb_strcasecmp(opts->isolation_level, "serializable") == 0)
                cf_config.default_isolation_level = TDB_ISOLATION_SERIALIZABLE;
            else
                sql_print_warning("TidesDB: Unknown isolation_level '%s', using global default",
                                  opts->isolation_level);
        }

        sql_print_information(
            "TidesDB: Table '%s' per-table options applied: compression=%s btree=%d "
            "write_buf=%llu bloom=%d bloom_fpr=%.4f levels=%d ttl=%llu",
            cf_name, opts->compression ? opts->compression : "(global)", cf_config.use_btree,
            (unsigned long long)cf_config.write_buffer_size, cf_config.enable_bloom_filter,
            cf_config.bloom_fpr, cf_config.min_levels, (unsigned long long)(opts->ttl));
    }

    int ret = tidesdb_create_column_family(tidesdb_instance, cf_name, &cf_config);
    if (ret != TDB_SUCCESS && ret != TDB_ERR_EXISTS)
    {
        sql_print_error("TidesDB: Failed to create column family '%s': %d", cf_name, ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    /* We create secondary index column families */
    /*** table_arg has the key definitions */
    TABLE *saved_table = table;
    table = table_arg; /* Temporarily set for create_secondary_indexes */
    ret = create_secondary_indexes(name);
    if (ret)
    {
        table = saved_table;
        sql_print_error("TidesDB: Failed to create secondary indexes");
        DBUG_RETURN(ret);
    }

    ret = create_fulltext_indexes(name);

    if (ret)
    {
        table = saved_table;
        sql_print_error("TidesDB: Failed to create fulltext indexes");
        DBUG_RETURN(ret);
    }

    /* We create spatial index column families */
    for (uint i = 0; i < table_arg->s->keys; i++)
    {
        KEY *key = &table_arg->key_info[i];
        if (key->algorithm == HA_KEY_ALG_RTREE)
        {
            ret = create_spatial_index(name, i);
            if (ret)
            {
                table = saved_table;
                sql_print_error("TidesDB: Failed to create spatial index");
                DBUG_RETURN(ret);
            }
        }
    }

    table = saved_table;

    sql_print_information("TidesDB: Created table '%s' (column family: %s)", name, cf_name);

    DBUG_RETURN(0);
}

/**
  @brief
  Delete a table (drop column family in TidesDB).
  Also drops all secondary index, fulltext, and spatial index column families.
*/
int ha_tidesdb::delete_table(const char *name)
{
    DBUG_ENTER("ha_tidesdb::delete_table");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    get_cf_name(name, cf_name, sizeof(cf_name));

    /*
       We drop secondary index column families.
       We don't have access to table->s->keys here since the table may not be open,
       so we try a reasonable range. tidesdb_drop_column_family returns
       TDB_ERR_NOT_FOUND for non-existent CFs which is fine.
    */
    for (uint i = 0; i < TIDESDB_MAX_INDEXES; i++)
    {
        char idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
        snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_FMT, cf_name, i);
        tidesdb_drop_column_family(tidesdb_instance, idx_cf_name);
    }

    /*
      We drop the main column family.
      Use rename first to wait for any in-progress flush/compaction,
      then drop. This works around a race condition in tidesdb_drop_column_family.
    */
    char tmp_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    snprintf(tmp_cf_name, sizeof(tmp_cf_name), "%s" TIDESDB_CF_DROPPING_SUFFIX, cf_name,
             (unsigned long)time(NULL));

    int ret = tidesdb_rename_column_family(tidesdb_instance, cf_name, tmp_cf_name);
    if (ret == TDB_SUCCESS)
    {
        /* Rename succeeded -- we now drop the renamed CF */
        ret = tidesdb_drop_column_family(tidesdb_instance, tmp_cf_name);
    }
    else if (ret == TDB_ERR_NOT_FOUND)
    {
        /* CF doesn't exist -- that's fine */
        ret = TDB_SUCCESS;
    }
    else
    {
        /* Rename failed -- we try direct drop */
        ret = tidesdb_drop_column_family(tidesdb_instance, cf_name);
    }

    if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
    {
        sql_print_error("TidesDB: Failed to drop column family '%s': %d", cf_name, ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    sql_print_information("TidesDB: Dropped table '%s'", name);

    DBUG_RETURN(0);
}

/**
  @brief
  Rename a table using TidesDB's native rename_column_family function.

  This atomically renames the column family and waits for any in-progress
  flush or compaction to complete before renaming.
*/
int ha_tidesdb::rename_table(const char *from, const char *to)
{
    DBUG_ENTER("ha_tidesdb::rename_table");

    int ret;
    char old_cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    char new_cf_name[TIDESDB_CF_NAME_BUF_SIZE];

    get_cf_name(from, old_cf_name, sizeof(old_cf_name));
    get_cf_name(to, new_cf_name, sizeof(new_cf_name));

    ret = tidesdb_rename_column_family(tidesdb_instance, old_cf_name, new_cf_name);
    if (ret != TDB_SUCCESS)
    {
        if (ret == TDB_ERR_NOT_FOUND)
        {
            sql_print_error("TidesDB: Cannot rename - source table '%s' not found", from);
            DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
        }
        if (ret == TDB_ERR_EXISTS)
        {
            sql_print_error("TidesDB: Cannot rename - destination table '%s' already exists", to);
            DBUG_RETURN(HA_ERR_TABLE_EXIST);
        }
        sql_print_error("TidesDB: Failed to rename column family '%s' to '%s': %d", old_cf_name,
                        new_cf_name, ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    /* We rename secondary index column families */
    for (uint i = 0; i < TIDESDB_MAX_INDEXES; i++)
    {
        char old_idx_cf[TIDESDB_IDX_CF_NAME_BUF_SIZE], new_idx_cf[TIDESDB_IDX_CF_NAME_BUF_SIZE];
        snprintf(old_idx_cf, sizeof(old_idx_cf), TIDESDB_CF_IDX_FMT, old_cf_name, i);
        snprintf(new_idx_cf, sizeof(new_idx_cf), TIDESDB_CF_IDX_FMT, new_cf_name, i);

        tidesdb_column_family_t *old_cf = tidesdb_get_column_family(tidesdb_instance, old_idx_cf);
        if (old_cf)
        {
            int rename_ret = tidesdb_rename_column_family(tidesdb_instance, old_idx_cf, new_idx_cf);
            if (rename_ret != TDB_SUCCESS && rename_ret != TDB_ERR_NOT_FOUND)
            {
                sql_print_information(
                    "TidesDB: Rename index CF '%s' to '%s' failed with %d, retrying...", old_idx_cf,
                    new_idx_cf, rename_ret);
                for (int retry = 0; retry < TIDESDB_RENAME_RETRY_COUNT && rename_ret != TDB_SUCCESS;
                     retry++)
                {
                    my_sleep(TIDESDB_RENAME_RETRY_SLEEP_US);
                    rename_ret =
                        tidesdb_rename_column_family(tidesdb_instance, old_idx_cf, new_idx_cf);
                    sql_print_information("TidesDB: Rename retry %d: %s -> %s = %d", retry + 1,
                                          old_idx_cf, new_idx_cf, rename_ret);
                }
                if (rename_ret != TDB_SUCCESS && rename_ret != TDB_ERR_NOT_FOUND)
                {
                    sql_print_error(
                        "TidesDB: Failed to rename index CF '%s' to '%s' after retries: %d",
                        old_idx_cf, new_idx_cf, rename_ret);
                }
            }
        }
    }

    /* We rename fulltext index column families */
    for (uint i = 0; i < TIDESDB_MAX_FT_INDEXES; i++)
    {
        char old_ft_cf[TIDESDB_IDX_CF_NAME_BUF_SIZE], new_ft_cf[TIDESDB_IDX_CF_NAME_BUF_SIZE];
        snprintf(old_ft_cf, sizeof(old_ft_cf), TIDESDB_CF_FT_FMT, old_cf_name, i);
        snprintf(new_ft_cf, sizeof(new_ft_cf), TIDESDB_CF_FT_FMT, new_cf_name, i);

        tidesdb_column_family_t *old_cf = tidesdb_get_column_family(tidesdb_instance, old_ft_cf);
        if (old_cf)
        {
            int rename_ret = tidesdb_rename_column_family(tidesdb_instance, old_ft_cf, new_ft_cf);
            if (rename_ret != TDB_SUCCESS && rename_ret != TDB_ERR_NOT_FOUND)
            {
                sql_print_information(
                    "TidesDB: Rename FT index CF '%s' to '%s' failed with %d, retrying...",
                    old_ft_cf, new_ft_cf, rename_ret);
                /* We retry with delay for file system timing issues */
                for (int retry = 0; retry < TIDESDB_RENAME_RETRY_COUNT && rename_ret != TDB_SUCCESS;
                     retry++)
                {
                    my_sleep(TIDESDB_RENAME_RETRY_SLEEP_US);
                    rename_ret =
                        tidesdb_rename_column_family(tidesdb_instance, old_ft_cf, new_ft_cf);
                    sql_print_information("TidesDB: FT rename retry %d: %s -> %s = %d", retry + 1,
                                          old_ft_cf, new_ft_cf, rename_ret);
                }
                if (rename_ret != TDB_SUCCESS && rename_ret != TDB_ERR_NOT_FOUND)
                {
                    sql_print_error(
                        "TidesDB: Failed to rename FT index CF '%s' to '%s' after retries: %d",
                        old_ft_cf, new_ft_cf, rename_ret);
                }
            }
        }
    }

    /* We rename spatial index column families */
    for (uint i = 0; i < TIDESDB_MAX_INDEXES; i++)
    {
        char old_spatial_cf[TIDESDB_IDX_CF_NAME_BUF_SIZE],
            new_spatial_cf[TIDESDB_IDX_CF_NAME_BUF_SIZE];
        snprintf(old_spatial_cf, sizeof(old_spatial_cf), TIDESDB_CF_SPATIAL_FMT, old_cf_name, i);
        snprintf(new_spatial_cf, sizeof(new_spatial_cf), TIDESDB_CF_SPATIAL_FMT, new_cf_name, i);

        tidesdb_column_family_t *old_cf =
            tidesdb_get_column_family(tidesdb_instance, old_spatial_cf);
        if (old_cf)
        {
            int rename_ret =
                tidesdb_rename_column_family(tidesdb_instance, old_spatial_cf, new_spatial_cf);
            if (rename_ret != TDB_SUCCESS && rename_ret != TDB_ERR_NOT_FOUND)
            {
                sql_print_information(
                    "TidesDB: Rename spatial CF '%s' to '%s' failed with %d, retrying...",
                    old_spatial_cf, new_spatial_cf, rename_ret);

                /* We retry with delay for file system timing issues */
                for (int retry = 0; retry < TIDESDB_RENAME_RETRY_COUNT && rename_ret != TDB_SUCCESS;
                     retry++)
                {
                    my_sleep(TIDESDB_RENAME_RETRY_SLEEP_US);
                    rename_ret = tidesdb_rename_column_family(tidesdb_instance, old_spatial_cf,
                                                              new_spatial_cf);
                    sql_print_information("TidesDB: Spatial rename retry %d: %s -> %s = %d",
                                          retry + 1, old_spatial_cf, new_spatial_cf, rename_ret);
                }
                if (rename_ret != TDB_SUCCESS && rename_ret != TDB_ERR_NOT_FOUND)
                {
                    sql_print_error(
                        "TidesDB: Failed to rename spatial CF '%s' to '%s' after retries: %d",
                        old_spatial_cf, new_spatial_cf, rename_ret);
                }
            }
        }
    }

    sql_print_information("TidesDB: Renamed table '%s' to '%s'", from, to);

    DBUG_RETURN(0);
}

/**
  @brief
  Write a row to the table.
*/
int ha_tidesdb::write_row(const uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::write_row");

    int ret;
    uchar *key;
    size_t key_len;
    uchar *value;
    size_t value_len;

    /* We handle auto_increment -- this sets insert_id_for_cur_row */
    if (table->next_number_field && buf == table->record[0])
    {
        if ((ret = update_auto_increment())) DBUG_RETURN(ret);
    }

    ret = build_primary_key(buf, &key, &key_len);
    if (ret) DBUG_RETURN(ret);

    ret = pack_row(buf, &value, &value_len);
    if (ret) DBUG_RETURN(ret);

    bool free_value = false;

    if (tidesdb_enable_encryption)
    {
        uchar *encrypted = NULL;
        size_t encrypted_len = 0;
        ret = tidesdb_encrypt_data(value, value_len, &encrypted, &encrypted_len);

        if (ret) DBUG_RETURN(ret);
        value = encrypted;
        value_len = encrypted_len;
        free_value = true;
    }

    /* We use bulk transaction if active, otherwise use current/THD transaction */
    tidesdb_txn_t *txn = NULL;
    bool own_txn = false;

    if (bulk_insert_active && bulk_txn)
    {
        txn = bulk_txn;
    }
    else if (current_txn)
    {
        txn = current_txn;
    }
    else
    {
        /* We check for THD-level transaction (multi-statement transaction) */
        THD *thd = ha_thd();
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);
        if (thd_txn)
        {
            txn = thd_txn;
        }
        else
        {
            /* No transaction available, we create one for this operation */
            ret = tidesdb_txn_begin(tidesdb_instance, &txn);
            if (ret != TDB_SUCCESS)
            {
                sql_print_error("TidesDB: Failed to begin transaction: %d", ret);
                DBUG_RETURN(HA_ERR_GENERIC);
            }
            own_txn = true;
        }
    }

    ret = check_foreign_key_constraints_insert(buf, txn);
    if (ret)
    {
        if (own_txn)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
        }
        if (free_value) my_free(value);
        DBUG_RETURN(ret);
    }

    time_t ttl = -1;
    if (share->ttl_field_index >= 0)
    {
        Field *ttl_field = table->field[share->ttl_field_index];
        if (!ttl_field->is_null())
        {
            longlong ttl_seconds = ttl_field->val_int();
            if (ttl_seconds > 0)
            {
                ttl = (cached_now ? cached_now : time(NULL)) + ttl_seconds;
            }
            else if (ttl_seconds == 0)
            {
                ttl = -1;
            }
        }
    }
    else
    {
        /* We check per-table TTL option, then fall back to global default */
        ulonglong table_ttl = 0;
        if (table->s->option_struct && table->s->option_struct->ttl > 0)
        {
            table_ttl = table->s->option_struct->ttl;
        }
        else if (tidesdb_default_ttl > 0)
        {
            table_ttl = tidesdb_default_ttl;
        }

        if (table_ttl > 0)
        {
            ttl = (cached_now ? cached_now : time(NULL)) + table_ttl;
        }
    }

    if (!skip_dup_check && !bulk_insert_active)
    {
        /* We check for duplicate primary key before insert */
        uint8_t *existing_value = NULL;
        size_t existing_len = 0;
        ret = tidesdb_txn_get(txn, share->cf, key, key_len, &existing_value, &existing_len);
        if (ret == TDB_SUCCESS && existing_value)
        {
            tidesdb_free(existing_value);
            if (own_txn)
            {
                tidesdb_txn_rollback(txn);
                tidesdb_txn_free(txn);
            }
            if (free_value) my_free(value);
            DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
        }
    }

    ret = tidesdb_txn_put(txn, share->cf, key, key_len, value, value_len, ttl);

    if (ret != TDB_SUCCESS)
    {
        if (free_value) my_free(value);
        if (own_txn)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
        }

        sql_print_error("TidesDB: Failed to write row: %d", ret);
        DBUG_RETURN(map_tidesdb_error(ret));
    }

    if (free_value) my_free(value);

    /* We insert secondary index entries (skip if indexes disabled for bulk load) */
    if (!indexes_disabled)
    {
        for (uint i = 0; i < table->s->keys; i++)
        {
            ret = insert_index_entry(i, buf, txn);
            if (ret)
            {
                if (own_txn)
                {
                    tidesdb_txn_rollback(txn);
                    tidesdb_txn_free(txn);
                }
                DBUG_RETURN(ret);
            }
        }

        /* We insert fulltext index entries */
        for (uint i = 0; i < share->num_ft_indexes; i++)
        {
            ret = insert_ft_words(i, buf, txn);
            if (ret)
            {
                sql_print_error("TidesDB: Failed to insert FT words for index %u: %d", i, ret);
                if (own_txn)
                {
                    tidesdb_txn_rollback(txn);
                    tidesdb_txn_free(txn);
                }
                DBUG_RETURN(ret);
            }
        }

        /* We insert spatial index entries */
        for (uint i = 0; i < share->num_spatial_indexes; i++)
        {
            ret = insert_spatial_entry(i, buf, txn);
            if (ret)
            {
                sql_print_error("TidesDB: Failed to insert spatial entry for index %u: %d", i, ret);
                if (own_txn)
                {
                    tidesdb_txn_rollback(txn);
                    tidesdb_txn_free(txn);
                }
                DBUG_RETURN(ret);
            }
        }
    } /* end if (!indexes_disabled) */

    if (own_txn)
    {
        ret = tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);

        if (ret != TDB_SUCCESS)
        {
            sql_print_error("TidesDB: Failed to commit transaction: %d", ret);
            DBUG_RETURN(map_tidesdb_error(ret));
        }
    }
    else if (bulk_insert_active && bulk_txn)
    {
        /*
         * For bulk inserts with our own transaction (not THD-level),
         * commit periodically to avoid transaction log overflow and
         * reduce memory pressure from large memtables.
         */
        THD *thd = ha_thd();
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);

        if (bulk_txn != thd_txn)
        {
            bulk_insert_count++;

            if (bulk_insert_count >= BULK_COMMIT_THRESHOLD)
            {
                ret = tidesdb_txn_commit(bulk_txn);
                tidesdb_txn_free(bulk_txn);
                bulk_txn = NULL;

                if (ret != TDB_SUCCESS)
                {
                    sql_print_error("TidesDB: Failed intermediate bulk commit: %d", ret);
                    bulk_insert_active = false;
                    DBUG_RETURN(HA_ERR_GENERIC);
                }

                ret = tidesdb_txn_begin(tidesdb_instance, &bulk_txn);
                if (ret != TDB_SUCCESS)
                {
                    sql_print_error("TidesDB: Failed to begin new bulk transaction: %d", ret);
                    bulk_insert_active = false;
                    DBUG_RETURN(HA_ERR_GENERIC);
                }

                bulk_insert_count = 0;
            }
        }
    }

    stats.records++;

    /* Online statistics -- we increment shared row count atomically */
    if (share->row_count_valid)
        my_atomic_add64_explicit((volatile int64 *)&share->row_count, 1, MY_MEMORY_ORDER_RELAXED);

    /* Bump auto-increment counter when an explicit value exceeds the current max.
       Without this, INSERT INTO t (id) VALUES (100) followed by INSERT INTO t (name)
       VALUES ('x') would assign id=5 instead of id=101. */
    if (table->next_number_field && buf == table->record[0])
    {
        ulonglong inserted_val = table->next_number_field->val_int();
        if (inserted_val > 0)
        {
            ulonglong cur_val = my_atomic_load64_explicit(
                (volatile int64 *)&share->auto_increment_value, MY_MEMORY_ORDER_RELAXED);
            if (inserted_val >= cur_val)
            {
                pthread_mutex_lock(&share->auto_inc_mutex);
                cur_val = share->auto_increment_value;
                if (inserted_val >= cur_val)
                {
                    my_atomic_store64_explicit((volatile int64 *)&share->auto_increment_value,
                                               (int64)(inserted_val + 1), MY_MEMORY_ORDER_RELEASE);
                    persist_auto_increment_value(inserted_val + 1);
                }
                pthread_mutex_unlock(&share->auto_inc_mutex);
            }
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Update a row.
*/
int ha_tidesdb::update_row(const uchar *old_data, const uchar *new_data)
{
    DBUG_ENTER("ha_tidesdb::update_row");

    int ret;
    uchar *old_key;
    size_t old_key_len;
    uchar *new_key;
    size_t new_key_len;
    uchar *value;
    size_t value_len;

    /* We build keys for old and new rows.
       For tables with hidden PK, we must use current_key (set during scan)
       since build_primary_key would generate a NEW key instead of returning
       the existing row's key. */
    if (table->s->primary_key == MAX_KEY && current_key && current_key_len > 0)
    {
        /* Hidden PK table -- we use the key from the current scan position */
        old_key = current_key;
        old_key_len = current_key_len;
    }
    else
    {
        ret = build_primary_key(old_data, &old_key, &old_key_len);
        if (ret) DBUG_RETURN(ret);
    }

    /* We save old key using pooled buffer since build_primary_key reuses pk_buffer */
    if (old_key_len > saved_key_buffer_capacity)
    {
        size_t new_capacity = old_key_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                  ? old_key_len * 2
                                  : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
        uchar *new_buf;
        if (saved_key_buffer == NULL)
            new_buf = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
        else
            new_buf =
                (uchar *)my_realloc(PSI_INSTRUMENT_ME, saved_key_buffer, new_capacity, MYF(MY_WME));
        if (!new_buf) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        saved_key_buffer = new_buf;
        saved_key_buffer_capacity = new_capacity;
    }
    memcpy(saved_key_buffer, old_key, old_key_len);

    /* For hidden PK tables, the new row keeps the same key as the old row
       (there's no PK column that could change). For tables with explicit PK,
       we extract the key from the new row data. */
    if (table->s->primary_key == MAX_KEY)
    {
        /* Hidden PK -- key stays the same */
        new_key = saved_key_buffer;
        new_key_len = old_key_len;
    }
    else
    {
        ret = build_primary_key(new_data, &new_key, &new_key_len);
        if (ret)
        {
            DBUG_RETURN(ret);
        }
    }

    ret = pack_row(new_data, &value, &value_len);
    if (ret)
    {
        DBUG_RETURN(ret);
    }

    bool free_value = false;

    if (tidesdb_enable_encryption)
    {
        uchar *encrypted = NULL;
        size_t encrypted_len = 0;
        ret = tidesdb_encrypt_data(value, value_len, &encrypted, &encrypted_len);
        if (ret) DBUG_RETURN(ret);
        value = encrypted;
        value_len = encrypted_len;
        free_value = true;
    }

    tidesdb_txn_t *txn = NULL;
    bool own_txn = false;

    if (current_txn)
    {
        txn = current_txn;
    }
    else
    {
        /* We check for THD-level transaction (multi-statement transaction) */
        THD *thd = ha_thd();
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);
        if (thd_txn)
        {
            txn = thd_txn;
        }
        else
        {
            ret = tidesdb_txn_begin(tidesdb_instance, &txn);
            if (ret != TDB_SUCCESS)
            {
                sql_print_error("TidesDB: Failed to begin transaction: %d", ret);
                DBUG_RETURN(HA_ERR_GENERIC);
            }
            own_txn = true;
        }
    }

    ret = check_foreign_key_constraints_insert(new_data, txn);
    if (ret)
    {
        if (own_txn)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
        }
        if (free_value) my_free(value);
        DBUG_RETURN(ret);
    }

    /* We check if primary key changed */
    bool pk_changed =
        (old_key_len != new_key_len || memcmp(saved_key_buffer, new_key, old_key_len) != 0);

    if (pk_changed)
    {
        /* We delete old row, insert new row */
        ret = tidesdb_txn_delete(txn, share->cf, saved_key_buffer, old_key_len);
        if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
        {
            if (own_txn)
            {
                tidesdb_txn_rollback(txn);
                tidesdb_txn_free(txn);
            }
            sql_print_error("TidesDB: Failed to delete old row: %d", ret);
            DBUG_RETURN(map_tidesdb_error(ret));
        }
    }

    /* We calculate TTL from _ttl/TTL column or use per-table/global default */
    time_t ttl = -1;
    if (share->ttl_field_index >= 0)
    {
        Field *ttl_field = table->field[share->ttl_field_index];
        if (!ttl_field->is_null())
        {
            longlong ttl_seconds = ttl_field->val_int();
            if (ttl_seconds > 0)
            {
                ttl = (cached_now ? cached_now : time(NULL)) + ttl_seconds;
            }
            else if (ttl_seconds == 0)
            {
                ttl = -1;
            }
        }
    }
    else
    {
        /* We check per-table TTL option, then fall back to global default */
        ulonglong table_ttl = 0;
        if (table->s->option_struct && table->s->option_struct->ttl > 0)
        {
            table_ttl = table->s->option_struct->ttl;
        }
        else if (tidesdb_default_ttl > 0)
        {
            table_ttl = tidesdb_default_ttl;
        }

        if (table_ttl > 0)
        {
            ttl = (cached_now ? cached_now : time(NULL)) + table_ttl;
        }
    }

    ret = tidesdb_txn_put(txn, share->cf, new_key, new_key_len, value, value_len, ttl);

    if (ret != TDB_SUCCESS)
    {
        if (free_value) my_free(value);
        if (own_txn)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
        }
        sql_print_error("TidesDB: Failed to update row: %d", ret);
        DBUG_RETURN(map_tidesdb_error(ret));
    }

    if (free_value) my_free(value);

    ret = update_index_entries(old_data, new_data, txn);
    if (ret)
    {
        if (own_txn)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
        }
        DBUG_RETURN(ret);
    }

    /* We update fulltext index entries */
    for (uint i = 0; i < share->num_ft_indexes; i++)
    {
        ret = delete_ft_words(i, old_data, txn);
        if (ret)
        {
            if (own_txn)
            {
                tidesdb_txn_rollback(txn);
                tidesdb_txn_free(txn);
            }
            DBUG_RETURN(ret);
        }

        ret = insert_ft_words(i, new_data, txn);
        if (ret)
        {
            if (own_txn)
            {
                tidesdb_txn_rollback(txn);
                tidesdb_txn_free(txn);
            }
            DBUG_RETURN(ret);
        }
    }

    for (uint i = 0; i < share->num_spatial_indexes; i++)
    {
        ret = delete_spatial_entry(i, old_data, txn);
        if (ret)
        {
            if (own_txn)
            {
                tidesdb_txn_rollback(txn);
                tidesdb_txn_free(txn);
            }
            DBUG_RETURN(ret);
        }

        ret = insert_spatial_entry(i, new_data, txn);
        if (ret)
        {
            if (own_txn)
            {
                tidesdb_txn_rollback(txn);
                tidesdb_txn_free(txn);
            }
            DBUG_RETURN(ret);
        }
    }

    if (own_txn)
    {
        ret = tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);

        if (ret != TDB_SUCCESS)
        {
            sql_print_error("TidesDB: Failed to commit transaction: %d", ret);
            DBUG_RETURN(map_tidesdb_error(ret));
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Delete a row.
*/
int ha_tidesdb::delete_row(const uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::delete_row");

    int ret;
    uchar *key;
    size_t key_len;

    if (current_key && current_key_len > 0)
    {
        key = current_key;
        key_len = current_key_len;
    }
    else
    {
        ret = build_primary_key(buf, &key, &key_len);
        if (ret) DBUG_RETURN(ret);
    }

    tidesdb_txn_t *txn = NULL;
    bool own_txn = false;

    if (current_txn)
    {
        txn = current_txn;
    }
    else
    {
        /* We check for THD-level transaction (multi-statement transaction) */
        THD *thd = ha_thd();
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);
        if (thd_txn)
        {
            txn = thd_txn;
        }
        else
        {
            /* No transaction available, we create one for this operation */
            ret = tidesdb_txn_begin(tidesdb_instance, &txn);
            if (ret != TDB_SUCCESS)
            {
                sql_print_error("TidesDB: Failed to begin transaction for delete: %d", ret);
                DBUG_RETURN(HA_ERR_GENERIC);
            }
            own_txn = true;
        }
    }

    ret = check_foreign_key_constraints_delete(buf, txn);
    if (ret)
    {
        if (own_txn)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
        }
        DBUG_RETURN(ret);
    }

    for (uint i = 0; i < table->s->keys; i++)
    {
        ret = delete_index_entry(i, buf, txn);
        if (ret)
        {
            if (own_txn)
            {
                tidesdb_txn_rollback(txn);
                tidesdb_txn_free(txn);
            }
            DBUG_RETURN(ret);
        }
    }

    for (uint i = 0; i < share->num_ft_indexes; i++)
    {
        ret = delete_ft_words(i, buf, txn);
        if (ret)
        {
            if (own_txn)
            {
                tidesdb_txn_rollback(txn);
                tidesdb_txn_free(txn);
            }
            DBUG_RETURN(ret);
        }
    }

    for (uint i = 0; i < share->num_spatial_indexes; i++)
    {
        ret = delete_spatial_entry(i, buf, txn);
        if (ret)
        {
            if (own_txn)
            {
                tidesdb_txn_rollback(txn);
                tidesdb_txn_free(txn);
            }
            DBUG_RETURN(ret);
        }
    }

    ret = tidesdb_txn_delete(txn, share->cf, key, key_len);
    if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
    {
        if (own_txn)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
        }
        sql_print_error("TidesDB: Failed to delete row: %d", ret);
        DBUG_RETURN(map_tidesdb_error(ret));
    }

    if (own_txn)
    {
        ret = tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);

        if (ret != TDB_SUCCESS)
        {
            sql_print_error("TidesDB: Failed to commit delete transaction: %d", ret);
            DBUG_RETURN(map_tidesdb_error(ret));
        }
    }

    stats.records--;

    /* Online statistics -- we decrement shared row count atomically */
    if (share->row_count_valid && share->row_count > 0)
        my_atomic_add64_explicit((volatile int64 *)&share->row_count, -1, MY_MEMORY_ORDER_RELAXED);
    else if (!share->row_count_valid)
    {
        /*
          Deleting from a table whose row_count was never bootstrapped
          (e.g. opened from disk without any write_row in this session).
          Fall back to tidesdb_get_stats and bootstrap now.
        */
        tidesdb_stats_t *tdb_stats = get_realtime_stats(share);
        if (tdb_stats)
        {
            ha_rows metadata_keys = 0;
            if (!share->has_primary_key) metadata_keys++;
            if (table->s->found_next_number_field) metadata_keys++;
            ha_rows count =
                tdb_stats->total_keys > metadata_keys ? tdb_stats->total_keys - metadata_keys : 0;
            tidesdb_free_stats(tdb_stats);
            /* The stats include the row we just tombstoned, so subtract 1 */
            share->row_count = count > 0 ? count - 1 : 0;
            share->row_count_valid = true;
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Initialize a table scan.
*/
int ha_tidesdb::rnd_init(bool scan)
{
    DBUG_ENTER("ha_tidesdb::rnd_init");

    int ret;

    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
    }

    /* current_txn is always set by external_lock before we get here.
       If somehow it's not (e.g. internal scan), fall back to THD txn. */
    if (!current_txn)
    {
        THD *thd = ha_thd();
        current_txn = get_thd_txn(thd, tidesdb_hton);
        if (!current_txn)
        {
            sql_print_error("TidesDB: No transaction available for scan");
            DBUG_RETURN(HA_ERR_GENERIC);
        }
    }

    ret = tidesdb_iter_new(current_txn, share->cf, &scan_iter);
    if (ret != TDB_SUCCESS)
    {
        sql_print_error("TidesDB: Failed to create iterator: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    tidesdb_iter_seek_to_first(scan_iter);
    scan_initialized = true;

    DBUG_RETURN(0);
}

/**
  @brief
  End a table scan.
*/
int ha_tidesdb::rnd_end()
{
    DBUG_ENTER("ha_tidesdb::rnd_end");

    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
    }
    scan_initialized = false;

    DBUG_RETURN(0);
}

/**
  @brief
  Read the next row in a table scan.
  Skips internal metadata keys (those starting with null byte).
*/
int ha_tidesdb::rnd_next(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::rnd_next");

    int ret;

    if (!scan_iter || !scan_initialized) DBUG_RETURN(HA_ERR_END_OF_FILE);

    /*
      The TidesDB iterator natively merges committed data with the
      transaction's uncommitted write buffer (MERGE_SOURCE_TXN_OPS),
      so we simply iterate and skip metadata keys.

      IMPORTANT -- tidesdb_iter_key/value return pointers to internal iterator
      memory that is invalidated by tidesdb_iter_next.  We must copy all
      data we need BEFORE calling tidesdb_iter_next.
    */

    for (;;)
    {
        if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

        uint8_t *iter_key_ptr = NULL;
        size_t iter_key_size = 0;
        ret = tidesdb_iter_key(scan_iter, &iter_key_ptr, &iter_key_size);
        if (ret != TDB_SUCCESS)
        {
            tidesdb_iter_next(scan_iter);
            continue;
        }

        /* We skip internal metadata keys (null byte prefix) */
        if (iter_key_size > 0 && iter_key_ptr[0] == 0)
        {
            tidesdb_iter_next(scan_iter);
            continue;
        }

        uint8_t *iter_val_ptr = NULL;
        size_t iter_val_size = 0;
        ret = tidesdb_iter_value(scan_iter, &iter_val_ptr, &iter_val_size);
        if (ret != TDB_SUCCESS)
        {
            tidesdb_iter_next(scan_iter);
            continue;
        }

        /* We copy key and value before advancing the iterator */
        size_t klen = iter_key_size;
        if (klen > current_key_capacity)
        {
            size_t new_capacity = klen > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                      ? klen * 2
                                      : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
            uchar *new_k = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
            if (!new_k) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            if (current_key) my_free(current_key);
            current_key = new_k;
            current_key_capacity = new_capacity;
        }
        memcpy(current_key, iter_key_ptr, klen);
        current_key_len = klen;

        const uchar *row_value = iter_val_ptr;
        size_t row_value_len = iter_val_size;
        uchar *val_copy = NULL;

        /* We must copy the value since iter_next invalidates it */
        val_copy = (uchar *)my_malloc(PSI_INSTRUMENT_ME, iter_val_size, MYF(MY_WME));
        if (!val_copy) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        memcpy(val_copy, iter_val_ptr, iter_val_size);
        row_value = val_copy;
        row_value_len = iter_val_size;

        tidesdb_iter_next(scan_iter);

        /* We decrypt and unpack */
        const uchar *row_data = row_value;
        size_t row_data_len = row_value_len;
        uchar *decrypted = NULL;

        if (tidesdb_enable_encryption && row_value_len > 0)
        {
            size_t decrypted_len = 0;
            ret = tidesdb_decrypt_data(row_value, row_value_len, &decrypted, &decrypted_len);
            my_free(val_copy);
            val_copy = NULL;
            if (ret) continue;
            row_data = decrypted;
            row_data_len = decrypted_len;
        }

        ret = unpack_row(buf, row_data, row_data_len);
        if (decrypted) my_free(decrypted);
        if (val_copy)
        {
            my_free(val_copy);
            val_copy = NULL;
        }
        if (ret) continue;

        /* Table Condition Pushdown */
        if (pushed_cond && !const_cast<COND *>(pushed_cond)->val_int()) continue;

        DBUG_RETURN(0);
    }

    DBUG_RETURN(HA_ERR_END_OF_FILE);
}

/**
  @brief
  Store the current row position.
*/
void ha_tidesdb::position(const uchar *record)
{
    DBUG_ENTER("ha_tidesdb::position");

    /* We store the current key as the position */
    if (current_key && current_key_len > 0 && current_key_len <= ref_length)
    {
        memcpy(ref, current_key, current_key_len);
        if (current_key_len < ref_length)
            memset(ref + current_key_len, 0, ref_length - current_key_len);
    }

    DBUG_VOID_RETURN;
}

/**
  @brief
  Read a row by position.
*/
int ha_tidesdb::rnd_pos(uchar *buf, uchar *pos)
{
    DBUG_ENTER("ha_tidesdb::rnd_pos");

    int ret;

    if (!current_txn)
    {
        sql_print_error("TidesDB: No transaction available for rnd_pos");
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    uint8_t *value = NULL;
    size_t value_size = 0;

    ret = tidesdb_txn_get(current_txn, share->cf, pos, ref_length, &value, &value_size);

    if (ret == TDB_ERR_NOT_FOUND)
    {
        DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }
    else if (ret != TDB_SUCCESS)
    {
        sql_print_error("TidesDB: Failed to get row by position: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    if (ref_length > current_key_capacity)
    {
        size_t new_capacity = ref_length > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                  ? ref_length * 2
                                  : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
        uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
        if (!new_key)
        {
            tidesdb_free(value);
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
        if (current_key) my_free(current_key);
        current_key = new_key;
        current_key_capacity = new_capacity;
    }
    memcpy(current_key, pos, ref_length);
    current_key_len = ref_length;

    if (tidesdb_enable_encryption && value_size > 0)
    {
        uchar *decrypted = NULL;
        size_t decrypted_len = 0;
        ret = tidesdb_decrypt_data(value, value_size, &decrypted, &decrypted_len);
        tidesdb_free(value);
        if (ret) DBUG_RETURN(ret);
        ret = unpack_row(buf, decrypted, decrypted_len);
        my_free(decrypted);
    }
    else
    {
        ret = unpack_row(buf, value, value_size);
        tidesdb_free(value);
    }

    if (ret)
    {
        DBUG_RETURN(ret);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Initialize index scan.
*/
int ha_tidesdb::index_init(uint idx, bool sorted)
{
    DBUG_ENTER("ha_tidesdb::index_init");
    active_index = idx;
    index_txn_owned = false;

    /* We ensure we have a transaction -- we check THD-level transaction if handler doesn't have one
     */
    if (!current_txn)
    {
        THD *thd = ha_thd();
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);
        if (thd_txn)
        {
            current_txn = thd_txn;
        }
        else
        {
            /* No THD transaction -- we create owned scan transaction (same as rnd_init) */
            tidesdb_isolation_level_t iso_level =
                thd ? (tidesdb_isolation_level_t)map_isolation_level(
                          (enum_tx_isolation)thd->variables.tx_isolation)
                    : (tidesdb_isolation_level_t)tidesdb_default_isolation;

            int ret = tidesdb_txn_begin_with_isolation(tidesdb_instance, iso_level, &current_txn);
            if (ret != TDB_SUCCESS)
            {
                sql_print_error("TidesDB: Failed to begin transaction for index scan: %d", ret);
                DBUG_RETURN(HA_ERR_GENERIC);
            }
            index_txn_owned = true;
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  End index scan.
*/
int ha_tidesdb::index_end()
{
    DBUG_ENTER("ha_tidesdb::index_end");
    active_index = MAX_KEY;
    index_key_len = 0;

    if (index_iter)
    {
        tidesdb_iter_free(index_iter);
        index_iter = NULL;
    }
    if (index_key_buf)
    {
        my_free(index_key_buf);
        index_key_buf = NULL;
        index_key_buf_capacity = 0;
        index_key_len = 0;
    }

    /* Clean up owned transaction created in index_init() */
    if (index_txn_owned && current_txn)
    {
        tidesdb_txn_rollback(current_txn);
        tidesdb_txn_free(current_txn);
        current_txn = NULL;
        index_txn_owned = false;
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Read a row by index key.

  For primary key     -- directly lookup in main CF
  For secondary index -- lookup in index CF to get PK, then fetch row from main CF
*/
int ha_tidesdb::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                               enum ha_rkey_function find_flag)
{
    DBUG_ENTER("ha_tidesdb::index_read_map");

    int ret;

    /* We ensure we have a transaction -- we check THD-level transaction if handler doesn't have one
     */
    if (!current_txn)
    {
        THD *thd = ha_thd();
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);
        if (thd_txn)
        {
            current_txn = thd_txn;
        }
        else
        {
            sql_print_error("TidesDB: No transaction available for index_read_map");
            DBUG_RETURN(HA_ERR_GENERIC);
        }
    }

    uint key_len = calculate_key_len(table, active_index, key, keypart_map);

    uint8_t *value = NULL;
    size_t value_size = 0;

    if (active_index == table->s->primary_key)
    {
        /*
          For primary key lookups, we need to distinguish between:
          1. Full key lookup (exact match)            -- use tidesdb_txn_get for efficiency
          2. Partial key prefix lookup (composite PK) -- we use iterator with prefix match
          3. Range scans (>=, >, etc.)                -- we use iterator
        */
        uint full_pk_len = table->key_info[table->s->primary_key].key_length;
        bool is_partial_key = (key_len < full_pk_len);
        bool needs_iterator = is_partial_key || find_flag == HA_READ_KEY_OR_NEXT ||
                              find_flag == HA_READ_AFTER_KEY || find_flag == HA_READ_PREFIX_LAST ||
                              find_flag == HA_READ_PREFIX_LAST_OR_PREV;

        if (!needs_iterator && find_flag == HA_READ_KEY_EXACT)
        {
            /* Full key exact match -- we use direct get for efficiency */
            ret = tidesdb_txn_get(current_txn, share->cf, key, key_len, &value, &value_size);

            if (ret == TDB_ERR_NOT_FOUND)
            {
                DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
            }
            else if (ret != TDB_SUCCESS)
            {
                sql_print_error("TidesDB: Failed to get row by PK: %d", ret);
                DBUG_RETURN(HA_ERR_GENERIC);
            }

            if (key_len > current_key_capacity)
            {
                size_t new_capacity = key_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                          ? key_len * 2
                                          : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
                uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
                if (!new_key)
                {
                    tidesdb_free(value);
                    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                }
                if (current_key) my_free(current_key);
                current_key = new_key;
                current_key_capacity = new_capacity;
            }
            memcpy(current_key, key, key_len);
            current_key_len = key_len;
        }
        else
        {
            /*
              Partial key prefix or range scan -- we use iterator + merge scan.
              We create an iterator seeked to the search key AND position the
              merge cursor in the write buffer, then call rnd_next() to get
              the first merged row.  This ensures we see uncommitted writes
              and avoids the duplicate-row bug where index_read_map and the
              subsequent index_next both return the same iterator position.
            */
            tidesdb_iter_t *iter = NULL;
            ret = tidesdb_iter_new(current_txn, share->cf, &iter);
            if (ret != TDB_SUCCESS)
            {
                DBUG_RETURN(HA_ERR_GENERIC);
            }

            /* Seek iterator; it may land past uncommitted keys or be invalid */
            tidesdb_iter_seek(iter, (uint8_t *)key, key_len);

            /* We install as the scan iterator for rnd_next */
            if (scan_iter) tidesdb_iter_free(scan_iter);
            scan_iter = iter;
            scan_initialized = true;

            /* We save search key prefix for boundary checking in index_next */
            if (key_len > index_key_buf_capacity)
            {
                uint new_capacity = key_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                        ? key_len * 2
                                        : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
                uchar *new_buf;
                if (index_key_buf == NULL)
                    new_buf = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
                else
                    new_buf = (uchar *)my_realloc(PSI_INSTRUMENT_ME, index_key_buf, new_capacity,
                                                  MYF(MY_WME));
                if (new_buf)
                {
                    index_key_buf = new_buf;
                    index_key_buf_capacity = new_capacity;
                }
            }
            if (index_key_buf)
            {
                memcpy(index_key_buf, key, key_len);
                /*
                  We only enable the prefix boundary check for actual prefix scans
                  (partial composite PK).  Range scans (>=, >) must iterate
                  until EOF -- setting index_key_len would cause index_next to
                  terminate as soon as the key differs from the search key.
                */
                if (is_partial_key)
                    index_key_len = key_len;
                else
                    index_key_len = 0;
            }

            ret = rnd_next(buf);
            if (ret) DBUG_RETURN(ret);

            /* We validate result against find_flag */
            switch (find_flag)
            {
                case HA_READ_KEY_EXACT:
                case HA_READ_PREFIX:
                    if (current_key_len < key_len || memcmp(current_key, key, key_len) != 0)
                        DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
                    break;
                case HA_READ_AFTER_KEY:
                    /* Must be strictly greater; skip exact matches */
                    while (current_key_len >= key_len && memcmp(current_key, key, key_len) == 0)
                    {
                        ret = rnd_next(buf);
                        if (ret) DBUG_RETURN(ret);
                    }
                    break;
                case HA_READ_KEY_OR_NEXT:
                    /* Any key >= search key is fine (already guaranteed by seek) */
                    break;
                case HA_READ_PREFIX_LAST:
                case HA_READ_PREFIX_LAST_OR_PREV:
                    break;
                default:
                    if (current_key_len < key_len || memcmp(current_key, key, key_len) != 0)
                        DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
                    break;
            }

            /* Row already unpacked by rnd_next into buf -- return directly */
            DBUG_RETURN(0);
        }
    }
    else
    {
        /* Secondary index lookup */
        if (active_index >= TIDESDB_MAX_INDEXES || !share->index_cf[active_index])
        {
            DBUG_RETURN(HA_ERR_WRONG_COMMAND);
        }

        tidesdb_column_family_t *idx_cf = share->index_cf[active_index];
        KEY *idx_key_info = &table->key_info[active_index];

        /*
          Transform the search key from key_copy() format to sort-key format.
          build_index_key() stores sort keys (using make_sort_key per field),
          so we must seek with a sort-key as well.
        */
        uint sort_key_capacity = key_len; /* sort key is <= key_copy key */
        uchar sort_search_key[TIDESDB_INITIAL_KEY_BUF_CAPACITY];
        uchar *sort_key_ptr = sort_search_key;
        if (sort_key_capacity > sizeof(sort_search_key))
        {
            sort_key_ptr = (uchar *)my_malloc(PSI_INSTRUMENT_ME, sort_key_capacity, MYF(MY_WME));
            if (!sort_key_ptr) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
        uint sort_key_len =
            transform_search_key_to_sort_key(table, idx_key_info, key, key_len, sort_key_ptr);

        /* For secondary index, we need to use an iterator to find matching keys */
        /* The index stores -- index_key -> primary_key */
        tidesdb_iter_t *iter = NULL;
        ret = tidesdb_iter_new(current_txn, idx_cf, &iter);
        if (ret != TDB_SUCCESS)
        {
            if (sort_key_ptr != sort_search_key) my_free(sort_key_ptr);
            DBUG_RETURN(HA_ERR_GENERIC);
        }

        ret = tidesdb_iter_seek(iter, (uint8_t *)sort_key_ptr, sort_key_len);
        if (ret != TDB_SUCCESS || !tidesdb_iter_valid(iter))
        {
            tidesdb_iter_free(iter);
            if (sort_key_ptr != sort_search_key) my_free(sort_key_ptr);
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }

        /* We get the index key and check if it matches our search key */
        uint8_t *idx_key = NULL;
        size_t idx_key_len = 0;
        ret = tidesdb_iter_key(iter, &idx_key, &idx_key_len);
        if (ret != TDB_SUCCESS)
        {
            tidesdb_iter_free(iter);
            if (sort_key_ptr != sort_search_key) my_free(sort_key_ptr);
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }

        /*
          We check if the found key matches based on find_flag.
          All comparisons use the transformed sort key.
        */
        bool key_matches = false;
        switch (find_flag)
        {
            case HA_READ_KEY_EXACT:
            case HA_READ_PREFIX:
                key_matches = (idx_key_len >= sort_key_len &&
                               memcmp(idx_key, sort_key_ptr, sort_key_len) == 0);
                break;
            case HA_READ_KEY_OR_NEXT:
            case HA_READ_AFTER_KEY:
                key_matches = true;
                if (find_flag == HA_READ_AFTER_KEY && idx_key_len >= sort_key_len &&
                    memcmp(idx_key, sort_key_ptr, sort_key_len) == 0)
                {
                    tidesdb_iter_next(iter);
                    if (!tidesdb_iter_valid(iter))
                    {
                        tidesdb_iter_free(iter);
                        if (sort_key_ptr != sort_search_key) my_free(sort_key_ptr);
                        DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
                    }
                    ret = tidesdb_iter_key(iter, &idx_key, &idx_key_len);
                    if (ret != TDB_SUCCESS)
                    {
                        tidesdb_iter_free(iter);
                        if (sort_key_ptr != sort_search_key) my_free(sort_key_ptr);
                        DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
                    }
                }
                break;
            case HA_READ_PREFIX_LAST:
            case HA_READ_PREFIX_LAST_OR_PREV:
                key_matches = true;
                break;
            default:
                key_matches = (idx_key_len >= sort_key_len &&
                               memcmp(idx_key, sort_key_ptr, sort_key_len) == 0);
                break;
        }

        if (!key_matches)
        {
            tidesdb_iter_free(iter);
            if (sort_key_ptr != sort_search_key) my_free(sort_key_ptr);
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }

        /* We get the primary key (value of the index entry) */
        uint8_t *pk_value = NULL;
        size_t pk_len = 0;
        ret = tidesdb_iter_value(iter, &pk_value, &pk_len);
        if (ret != TDB_SUCCESS || pk_len == 0)
        {
            tidesdb_iter_free(iter);
            if (sort_key_ptr != sort_search_key) my_free(sort_key_ptr);
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }

        if (pk_len > current_key_capacity)
        {
            size_t new_capacity = pk_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                      ? pk_len * 2
                                      : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
            uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
            if (!new_key)
            {
                tidesdb_iter_free(iter);
                if (sort_key_ptr != sort_search_key) my_free(sort_key_ptr);
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            }
            if (current_key) my_free(current_key);
            current_key = new_key;
            current_key_capacity = new_capacity;
        }
        memcpy(current_key, pk_value, pk_len);
        current_key_len = pk_len;

        if (index_iter)
        {
            tidesdb_iter_free(index_iter);
        }
        index_iter = iter;

        /* We save transformed sort key for boundary checking in index_next */
        if (sort_key_len > index_key_buf_capacity)
        {
            uint new_capacity = sort_key_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                    ? sort_key_len * 2
                                    : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
            uchar *new_buf;
            if (index_key_buf == NULL)
                new_buf = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
            else
                new_buf = (uchar *)my_realloc(PSI_INSTRUMENT_ME, index_key_buf, new_capacity,
                                              MYF(MY_WME));
            if (new_buf)
            {
                index_key_buf = new_buf;
                index_key_buf_capacity = new_capacity;
            }
        }
        if (index_key_buf)
        {
            memcpy(index_key_buf, sort_key_ptr, sort_key_len);
            index_key_len = sort_key_len;
        }
        if (sort_key_ptr != sort_search_key) my_free(sort_key_ptr);

        /* We now fetch the actual row using the primary key */
        ret = tidesdb_txn_get(current_txn, share->cf, current_key, current_key_len, &value,
                              &value_size);
        if (ret == TDB_ERR_NOT_FOUND)
        {
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }
        else if (ret != TDB_SUCCESS)
        {
            sql_print_error("TidesDB: Failed to get row by PK from secondary index: %d", ret);
            DBUG_RETURN(HA_ERR_GENERIC);
        }
    }

    if (tidesdb_enable_encryption && value_size > 0)
    {
        uchar *decrypted = NULL;
        size_t decrypted_len = 0;
        ret = tidesdb_decrypt_data(value, value_size, &decrypted, &decrypted_len);
        tidesdb_free(value);
        if (ret) DBUG_RETURN(ret);
        ret = unpack_row(buf, decrypted, decrypted_len);
        my_free(decrypted);
    }
    else
    {
        ret = unpack_row(buf, value, value_size);
        tidesdb_free(value);
    }

    if (ret)
    {
        DBUG_RETURN(ret);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Read last row matching a key prefix (for ORDER BY ... DESC LIMIT 1).

  Delegates to index_read_map with HA_READ_PREFIX_LAST which seeks to the
  last entry matching the key prefix. This avoids a full forward scan +
  filesort for descending queries.
*/
int ha_tidesdb::index_read_last_map(uchar *buf, const uchar *key, key_part_map keypart_map)
{
    DBUG_ENTER("ha_tidesdb::index_read_last_map");
    DBUG_RETURN(index_read_map(buf, key, keypart_map, HA_READ_PREFIX_LAST));
}

/**
  @brief
  Read next row in index order.

  For secondary indexes -- uses index_iter to find next entry
  For primary key -- uses scan_iter
*/
int ha_tidesdb::index_next(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_next");

    int ret;

    /* For secondary index, we use the saved iterator */
    if (active_index != table->s->primary_key && index_iter)
    {
        for (;;)
        {
            tidesdb_iter_next(index_iter);

            if (!tidesdb_iter_valid(index_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

            uint8_t *idx_key = NULL;
            size_t idx_key_len = 0;
            ret = tidesdb_iter_key(index_iter, &idx_key, &idx_key_len);
            if (ret != TDB_SUCCESS) DBUG_RETURN(HA_ERR_END_OF_FILE);

            uint8_t *pk_value = NULL;
            size_t pk_len = 0;
            ret = tidesdb_iter_value(index_iter, &pk_value, &pk_len);
            if (ret != TDB_SUCCESS || pk_len == 0) DBUG_RETURN(HA_ERR_END_OF_FILE);

            if (pk_len > current_key_capacity)
            {
                size_t new_capacity = pk_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                          ? pk_len * 2
                                          : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
                uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
                if (!new_key) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                if (current_key) my_free(current_key);
                current_key = new_key;
                current_key_capacity = new_capacity;
            }
            memcpy(current_key, pk_value, pk_len);
            current_key_len = pk_len;

            if (!current_txn)
            {
                sql_print_error("TidesDB: No transaction available for index_next");
                DBUG_RETURN(HA_ERR_GENERIC);
            }

            uint8_t *value = NULL;
            size_t value_size = 0;
            ret = tidesdb_txn_get(current_txn, share->cf, pk_value, pk_len, &value, &value_size);

            if (ret != TDB_SUCCESS) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);

            if (tidesdb_enable_encryption && value_size > 0)
            {
                uchar *decrypted = NULL;
                size_t decrypted_len = 0;
                ret = tidesdb_decrypt_data(value, value_size, &decrypted, &decrypted_len);
                tidesdb_free(value);
                if (ret) DBUG_RETURN(ret);
                ret = unpack_row(buf, decrypted, decrypted_len);
                my_free(decrypted);
            }
            else
            {
                ret = unpack_row(buf, value, value_size);
                tidesdb_free(value);
            }

            if (ret) DBUG_RETURN(ret);

            /* ICP -- we evaluate pushed index condition; skip non-matching rows */
            if (pushed_idx_cond && pushed_idx_cond_keyno == active_index)
            {
                if (!pushed_idx_cond->val_bool())
                    continue; /* row rejected by ICP, advance to next */
            }

            DBUG_RETURN(0);
        }
    }

    /*
      For primary key scans, delegate to rnd_next() which implements
      the merge scan (combining committed iterator + handler write buffer).
      After rnd_next sets current_key, check the prefix boundary.
    */
    if (!scan_iter || !scan_initialized) DBUG_RETURN(HA_ERR_END_OF_FILE);

    ret = rnd_next(buf);
    if (ret) DBUG_RETURN(ret);

    /* Prefix boundary check for composite primary key scans */
    if (active_index == table->s->primary_key && index_key_len > 0)
    {
        if (current_key_len < index_key_len ||
            memcmp(current_key, index_key_buf, index_key_len) != 0)
        {
            DBUG_RETURN(HA_ERR_END_OF_FILE);
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Read next row with the same key prefix.

  For secondary indexes -- uses index_iter to find next matching entry
  For primary key       -- uses scan_iter
*/
int ha_tidesdb::index_next_same(uchar *buf, const uchar *key, uint keylen)
{
    DBUG_ENTER("ha_tidesdb::index_next_same");

    int ret;

    /* For secondary index, we use the saved iterator */
    if (active_index != table->s->primary_key && index_iter)
    {
        for (;;)
        {
            tidesdb_iter_next(index_iter);

            if (!tidesdb_iter_valid(index_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

            uint8_t *idx_key = NULL;
            size_t idx_key_len = 0;
            ret = tidesdb_iter_key(index_iter, &idx_key, &idx_key_len);
            if (ret != TDB_SUCCESS) DBUG_RETURN(HA_ERR_END_OF_FILE);

            /* We check if key prefix still matches */
            uint check_len = index_key_len > 0 ? index_key_len : keylen;
            if (idx_key_len < check_len ||
                memcmp(idx_key, index_key_buf ? index_key_buf : key, check_len) != 0)
            {
                DBUG_RETURN(HA_ERR_END_OF_FILE);
            }

            /* We get the primary key from the index entry */
            uint8_t *pk_value = NULL;
            size_t pk_len = 0;
            ret = tidesdb_iter_value(index_iter, &pk_value, &pk_len);
            if (ret != TDB_SUCCESS || pk_len == 0) DBUG_RETURN(HA_ERR_END_OF_FILE);

            if (pk_len > current_key_capacity)
            {
                size_t new_capacity = pk_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                          ? pk_len * 2
                                          : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
                uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
                if (!new_key) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                if (current_key) my_free(current_key);
                current_key = new_key;
                current_key_capacity = new_capacity;
            }
            memcpy(current_key, pk_value, pk_len);
            current_key_len = pk_len;

            if (!current_txn)
            {
                sql_print_error("TidesDB: No transaction available for index_next_same");
                DBUG_RETURN(HA_ERR_GENERIC);
            }

            uint8_t *value = NULL;
            size_t value_size = 0;
            ret = tidesdb_txn_get(current_txn, share->cf, current_key, current_key_len, &value,
                                  &value_size);

            if (ret != TDB_SUCCESS) DBUG_RETURN(HA_ERR_END_OF_FILE);

            if (tidesdb_enable_encryption && value_size > 0)
            {
                uchar *decrypted = NULL;
                size_t decrypted_len = 0;
                ret = tidesdb_decrypt_data(value, value_size, &decrypted, &decrypted_len);
                tidesdb_free(value);
                if (ret) DBUG_RETURN(ret);
                ret = unpack_row(buf, decrypted, decrypted_len);
                my_free(decrypted);
            }
            else
            {
                ret = unpack_row(buf, value, value_size);
                tidesdb_free(value);
            }

            if (ret) DBUG_RETURN(ret);

            /* ICP -- we evaluate pushed index condition; skip non-matching rows */
            if (pushed_idx_cond && pushed_idx_cond_keyno == active_index)
            {
                if (!pushed_idx_cond->val_bool())
                    continue; /* row rejected by ICP, advance to next */
            }

            DBUG_RETURN(0);
        }
    }

    /* For primary key, delegate to rnd_next which has merge scan logic */
    if (!scan_iter || !scan_initialized) DBUG_RETURN(HA_ERR_END_OF_FILE);

    ret = rnd_next(buf);
    if (ret) DBUG_RETURN(ret);

    /* We check if still within same key prefix */
    if (current_key_len < keylen || memcmp(current_key, key, keylen) != 0)
        DBUG_RETURN(HA_ERR_END_OF_FILE);

    DBUG_RETURN(0);
}

/**
  @brief
  Read previous row in index order.
  Skips internal metadata keys (those starting with null byte).
*/
int ha_tidesdb::index_prev(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_prev");

    int ret;

    /* For secondary index, we use index_iter */
    if (active_index != table->s->primary_key && index_iter)
    {
        for (;;)
        {
            tidesdb_iter_prev(index_iter);

            if (!tidesdb_iter_valid(index_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

            uint8_t *idx_key = NULL;
            size_t idx_key_len = 0;
            ret = tidesdb_iter_key(index_iter, &idx_key, &idx_key_len);
            if (ret != TDB_SUCCESS) DBUG_RETURN(HA_ERR_END_OF_FILE);

            uint8_t *pk_value = NULL;
            size_t pk_len = 0;
            ret = tidesdb_iter_value(index_iter, &pk_value, &pk_len);
            if (ret != TDB_SUCCESS || pk_len == 0) DBUG_RETURN(HA_ERR_END_OF_FILE);

            if (pk_len > current_key_capacity)
            {
                size_t new_capacity = pk_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                          ? pk_len * 2
                                          : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
                uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
                if (!new_key) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                if (current_key) my_free(current_key);
                current_key = new_key;
                current_key_capacity = new_capacity;
            }
            memcpy(current_key, pk_value, pk_len);
            current_key_len = pk_len;

            if (!current_txn)
            {
                sql_print_error("TidesDB: No transaction available for index_prev");
                DBUG_RETURN(HA_ERR_GENERIC);
            }

            uint8_t *value = NULL;
            size_t value_size = 0;
            ret = tidesdb_txn_get(current_txn, share->cf, pk_value, pk_len, &value, &value_size);
            if (ret != TDB_SUCCESS) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);

            if (tidesdb_enable_encryption && value_size > 0)
            {
                uchar *decrypted = NULL;
                size_t decrypted_len = 0;
                ret = tidesdb_decrypt_data(value, value_size, &decrypted, &decrypted_len);
                tidesdb_free(value);
                if (ret) DBUG_RETURN(ret);
                ret = unpack_row(buf, decrypted, decrypted_len);
                my_free(decrypted);
            }
            else
            {
                ret = unpack_row(buf, value, value_size);
                tidesdb_free(value);
            }

            if (ret) DBUG_RETURN(ret);

            /* ICP -- we evaluate pushed index condition; skip non-matching rows */
            if (pushed_idx_cond && pushed_idx_cond_keyno == active_index)
            {
                if (!pushed_idx_cond->val_bool())
                    continue; /* row rejected by ICP, advance to prev */
            }

            DBUG_RETURN(0);
        }
    }

    /* Primary key scan -- we use scan_iter */
    if (!scan_iter || !scan_initialized) DBUG_RETURN(HA_ERR_END_OF_FILE);

    uint8_t *key = NULL;
    size_t key_size = 0;
    uint8_t *value = NULL;
    size_t value_size = 0;

    while (true)
    {
        tidesdb_iter_prev(scan_iter);

        if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

        ret = tidesdb_iter_key(scan_iter, &key, &key_size);
        if (ret != TDB_SUCCESS)
        {
            DBUG_RETURN(HA_ERR_END_OF_FILE);
        }

        if (key_size > 0 && key[0] == 0)
        {
            continue;
        }

        break;
    }

    ret = tidesdb_iter_value(scan_iter, &value, &value_size);
    if (ret != TDB_SUCCESS)
    {
        DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    if (key_size > current_key_capacity)
    {
        size_t new_capacity = key_size > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                  ? key_size * 2
                                  : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
        uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
        if (!new_key) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        if (current_key) my_free(current_key);
        current_key = new_key;
        current_key_capacity = new_capacity;
    }
    memcpy(current_key, key, key_size);
    current_key_len = key_size;

    /* We copy value before it's invalidated */
    uchar *val_copy = (uchar *)my_malloc(PSI_INSTRUMENT_ME, value_size, MYF(MY_WME));
    if (!val_copy) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    memcpy(val_copy, value, value_size);

    if (tidesdb_enable_encryption && value_size > 0)
    {
        uchar *decrypted = NULL;
        size_t decrypted_len = 0;
        ret = tidesdb_decrypt_data(val_copy, value_size, &decrypted, &decrypted_len);
        my_free(val_copy);
        if (ret) DBUG_RETURN(ret);
        ret = unpack_row(buf, decrypted, decrypted_len);
        my_free(decrypted);
    }
    else
    {
        ret = unpack_row(buf, val_copy, value_size);
        my_free(val_copy);
    }

    DBUG_RETURN(ret);
}

/**
  @brief
  Read first row in index order.
*/
int ha_tidesdb::index_first(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_first");

    /* We clear prefix boundary so index_next does not truncate a full scan */
    index_key_len = 0;

    /* For primary key scans, use rnd_init + rnd_next (iterates primary CF) */
    if (active_index == table->s->primary_key)
    {
        int ret = rnd_init(true);
        if (ret) DBUG_RETURN(ret);

        DBUG_RETURN(rnd_next(buf));
    }

    /* Secondary index scan -- we iterate the index CF in key order */
    if (active_index >= TIDESDB_MAX_INDEXES || !share->index_cf[active_index])
    {
        DBUG_RETURN(HA_ERR_WRONG_COMMAND);
    }

    if (!current_txn)
    {
        THD *thd = ha_thd();
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);
        if (thd_txn)
        {
            current_txn = thd_txn;
        }
        else
        {
            DBUG_RETURN(HA_ERR_GENERIC);
        }
    }

    tidesdb_column_family_t *idx_cf = share->index_cf[active_index];

    /* We clean up any existing index iterator */
    if (index_iter)
    {
        tidesdb_iter_free(index_iter);
        index_iter = NULL;
    }

    int ret = tidesdb_iter_new(current_txn, idx_cf, &index_iter);
    if (ret != TDB_SUCCESS)
    {
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    tidesdb_iter_seek_to_first(index_iter);

    /*
      No metadata key skip here -- secondary index CFs don't store internal
      metadata keys.  The null-byte prefix filter is only for the primary CF.
      (MariaDB's key format for nullable fields starts with 0x00 meaning
      "not null", which would be incorrectly filtered.)
    */

    if (!tidesdb_iter_valid(index_iter))
    {
        tidesdb_iter_free(index_iter);
        index_iter = NULL;
        DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    /* We get the PK value from the index entry */
    uint8_t *pk_value = NULL;
    size_t pk_len = 0;
    ret = tidesdb_iter_value(index_iter, &pk_value, &pk_len);
    if (ret != TDB_SUCCESS || pk_len == 0)
    {
        tidesdb_iter_free(index_iter);
        index_iter = NULL;
        DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    /* We save PK as current_key */
    if (pk_len > current_key_capacity)
    {
        size_t new_capacity = pk_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                  ? pk_len * 2
                                  : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
        uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
        if (!new_key) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        if (current_key) my_free(current_key);
        current_key = new_key;
        current_key_capacity = new_capacity;
    }
    memcpy(current_key, pk_value, pk_len);
    current_key_len = pk_len;

    /* We fetch the actual row using PK */
    uint8_t *value = NULL;
    size_t value_size = 0;
    ret =
        tidesdb_txn_get(current_txn, share->cf, current_key, current_key_len, &value, &value_size);
    if (ret != TDB_SUCCESS)
    {
        DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }

    if (tidesdb_enable_encryption && value_size > 0)
    {
        uchar *decrypted = NULL;
        size_t decrypted_len = 0;
        ret = tidesdb_decrypt_data(value, value_size, &decrypted, &decrypted_len);
        tidesdb_free(value);
        if (ret) DBUG_RETURN(ret);
        ret = unpack_row(buf, decrypted, decrypted_len);
        my_free(decrypted);
    }
    else
    {
        ret = unpack_row(buf, value, value_size);
        tidesdb_free(value);
    }

    DBUG_RETURN(ret);
}

/**
  @brief
  Read last row in index order.
  Skips internal metadata keys (those starting with null byte).
*/
int ha_tidesdb::index_last(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_last");

    int ret;

    if (!current_txn)
    {
        THD *thd = ha_thd();
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);
        if (thd_txn)
        {
            current_txn = thd_txn;
        }
        else
        {
            tidesdb_isolation_level_t iso_level =
                thd ? (tidesdb_isolation_level_t)map_isolation_level(
                          (enum_tx_isolation)thd->variables.tx_isolation)
                    : (tidesdb_isolation_level_t)tidesdb_default_isolation;

            ret = tidesdb_txn_begin_with_isolation(tidesdb_instance, iso_level, &current_txn);
            if (ret != TDB_SUCCESS)
            {
                sql_print_error("TidesDB: Failed to begin transaction for index_last: %d", ret);
                DBUG_RETURN(HA_ERR_GENERIC);
            }
            index_txn_owned = true;
        }
    }

    /*** We choose the correct CF -- secondary index CF or primary CF */
    bool is_secondary = (active_index != table->s->primary_key &&
                         active_index < TIDESDB_MAX_INDEXES && share->index_cf[active_index]);

    tidesdb_column_family_t *target_cf = is_secondary ? share->index_cf[active_index] : share->cf;
    tidesdb_iter_t **target_iter = is_secondary ? &index_iter : &scan_iter;

    /* We clean up existing iterator */
    if (*target_iter)
    {
        tidesdb_iter_free(*target_iter);
        *target_iter = NULL;
    }

    ret = tidesdb_iter_new(current_txn, target_cf, target_iter);
    if (ret != TDB_SUCCESS)
    {
        sql_print_error("TidesDB: Failed to create iterator: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    tidesdb_iter_seek_to_last(*target_iter);
    if (!is_secondary) scan_initialized = true;

    uint8_t *key = NULL;
    size_t key_size = 0;
    uint8_t *value = NULL;
    size_t value_size = 0;

    /*
      Skip metadata keys (null byte prefix) only for the primary CF.
      Secondary index CFs don't have metadata keys, and their key format
      for nullable fields starts with 0x00 (null indicator = "not null").
    */
    if (!is_secondary)
    {
        while (tidesdb_iter_valid(*target_iter))
        {
            ret = tidesdb_iter_key(*target_iter, &key, &key_size);
            if (ret != TDB_SUCCESS)
            {
                DBUG_RETURN(HA_ERR_END_OF_FILE);
            }

            if (key_size > 0 && key[0] == 0)
            {
                tidesdb_iter_prev(*target_iter);
                continue;
            }

            break;
        }
    }

    if (!tidesdb_iter_valid(*target_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

    if (is_secondary)
    {
        /* For secondary index, value is the PK */
        uint8_t *pk_value = NULL;
        size_t pk_len = 0;
        ret = tidesdb_iter_value(*target_iter, &pk_value, &pk_len);
        if (ret != TDB_SUCCESS || pk_len == 0) DBUG_RETURN(HA_ERR_END_OF_FILE);

        if (pk_len > current_key_capacity)
        {
            size_t new_capacity = pk_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                      ? pk_len * 2
                                      : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
            uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
            if (!new_key) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            if (current_key) my_free(current_key);
            current_key = new_key;
            current_key_capacity = new_capacity;
        }
        memcpy(current_key, pk_value, pk_len);
        current_key_len = pk_len;

        /* We fetch full row via PK */
        ret = tidesdb_txn_get(current_txn, share->cf, current_key, current_key_len, &value,
                              &value_size);
        if (ret != TDB_SUCCESS) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }
    else
    {
        /* Primary CF -- key IS the PK, value IS the row data */
        if (key_size > current_key_capacity)
        {
            size_t new_capacity = key_size > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                      ? key_size * 2
                                      : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
            uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
            if (!new_key) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            if (current_key) my_free(current_key);
            current_key = new_key;
            current_key_capacity = new_capacity;
        }
        memcpy(current_key, key, key_size);
        current_key_len = key_size;

        ret = tidesdb_iter_value(*target_iter, &value, &value_size);
        if (ret != TDB_SUCCESS) DBUG_RETURN(HA_ERR_END_OF_FILE);

        /* For primary CF, we need to copy the value since we don't own it */
        uchar *val_copy = (uchar *)my_malloc(PSI_INSTRUMENT_ME, value_size, MYF(MY_WME));
        if (!val_copy) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        memcpy(val_copy, value, value_size);
        value = val_copy;
    }

    if (tidesdb_enable_encryption && value_size > 0)
    {
        uchar *decrypted = NULL;
        size_t decrypted_len = 0;
        ret = tidesdb_decrypt_data(value, value_size, &decrypted, &decrypted_len);
        if (is_secondary)
            tidesdb_free(value);
        else
            my_free(value);
        if (ret) DBUG_RETURN(ret);
        ret = unpack_row(buf, decrypted, decrypted_len);
        my_free(decrypted);
    }
    else
    {
        ret = unpack_row(buf, value, value_size);
        if (is_secondary)
            tidesdb_free(value);
        else
            my_free(value);
    }

    DBUG_RETURN(ret);
}

/**
  @brief
  Return table information to the optimizer.
*/
int ha_tidesdb::info(uint flag)
{
    DBUG_ENTER("ha_tidesdb::info");

    if (flag & HA_STATUS_VARIABLE)
    {
        stats.deleted = 0;

        if (share && share->cf)
        {
            /*
              When row_count_valid is true, write_row/delete_row have been
              maintaining share->row_count accurately.  Prefer it over
              tidesdb_get_stats() which counts tombstones in the LSM tree
              and therefore over-reports after DELETEs.
            */
            if (share->row_count_valid)
            {
                stats.records = share->row_count;
            }
            else
            {
                tidesdb_stats_t *tdb_stats = get_realtime_stats(share);
                if (tdb_stats)
                {
                    ha_rows metadata_keys = 0;
                    if (!share->has_primary_key) metadata_keys++;
                    if (table->s->found_next_number_field) metadata_keys++;

                    stats.records = tdb_stats->total_keys > metadata_keys
                                        ? tdb_stats->total_keys - metadata_keys
                                        : 0;

                    tidesdb_free_stats(tdb_stats);
                }
                else
                {
                    stats.records = TIDESDB_FALLBACK_RECORD_COUNT;
                }
            }

            /* We always fetch size stats from storage */
            tidesdb_stats_t *tdb_stats = get_realtime_stats(share);
            if (tdb_stats)
            {
                stats.data_file_length = tdb_stats->total_data_size;

                if (tdb_stats->avg_key_size > 0 || tdb_stats->avg_value_size > 0)
                {
                    stats.mean_rec_length =
                        (ulong)(tdb_stats->avg_key_size + tdb_stats->avg_value_size);
                }
                else
                {
                    stats.mean_rec_length = table->s->reclength;
                }
                tidesdb_free_stats(tdb_stats);
            }
            else
            {
                stats.data_file_length = TIDESDB_FALLBACK_DATA_FILE_LENGTH;
                stats.mean_rec_length = table->s->reclength;
            }
        }
        else
        {
            stats.records = TIDESDB_FALLBACK_RECORD_COUNT;
            stats.data_file_length = TIDESDB_FALLBACK_DATA_FILE_LENGTH;
            stats.mean_rec_length = table->s->reclength;
        }
        stats.index_file_length = 0;
    }

    if (flag & HA_STATUS_CONST)
    {
        stats.max_data_file_length = LLONG_MAX;
        stats.max_index_file_length = LLONG_MAX;

        /*
          Set per-index cardinality estimates (rec_per_key).
          rec_per_key[i] = average number of rows sharing the same key
          prefix up to and including key part i.

          For unique indexes -- rec_per_key = 1
          For non-unique:    -- estimate based on total rows / estimated distinct values
        */
        ha_rows total = stats.records > 0 ? stats.records : 1;
        for (uint i = 0; i < table->s->keys; i++)
        {
            KEY *key = &table->key_info[i];
            bool is_unique = (key->flags & HA_NOSAME) != 0;

            for (uint j = 0; j < key->user_defined_key_parts; j++)
            {
                if (is_unique && j == key->user_defined_key_parts - 1)
                {
                    /* Last part of unique key: exactly 1 row per key */
                    key->rec_per_key[j] = 1;
                }
                else
                {
                    /*
                      Estimate -- fewer distinct values for leading key parts,
                      more as we add parts. Use sqrt heuristic scaled by
                      number of parts already included.
                    */
                    ulong distinct = (ulong)sqrt((double)total);
                    if (distinct < 1) distinct = 1;
                    /* Each additional part roughly doubles selectivity */
                    for (uint k = 0; k < j; k++)
                        distinct = (ulong)(distinct * 2 > total ? total : distinct * 2);

                    key->rec_per_key[j] = (ulong)(total / distinct);
                    if (key->rec_per_key[j] < 1) key->rec_per_key[j] = 1;
                }
            }
        }
    }

    if (flag & HA_STATUS_AUTO)
    {
        if (share)
        {
            stats.auto_increment_value = my_atomic_load64_explicit(
                (volatile int64 *)&share->auto_increment_value, MY_MEMORY_ORDER_RELAXED);
        }
        else
        {
            stats.auto_increment_value = 1;
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Handle extra hints from the optimizer.
*/
int ha_tidesdb::extra(enum ha_extra_function operation)
{
    DBUG_ENTER("ha_tidesdb::extra");

    switch (operation)
    {
        case HA_EXTRA_KEYREAD:

            keyread_only = true;
            break;

        case HA_EXTRA_NO_KEYREAD:
            keyread_only = false;
            break;

        case HA_EXTRA_IGNORE_DUP_KEY:
            skip_dup_check = true;
            break;

        case HA_EXTRA_NO_IGNORE_DUP_KEY:
            skip_dup_check = false;
            break;

        case HA_EXTRA_WRITE_CAN_REPLACE:
            skip_dup_check = true;
            break;

        case HA_EXTRA_WRITE_CANNOT_REPLACE:
            skip_dup_check = false;
            break;

        case HA_EXTRA_INSERT_WITH_UPDATE:
            skip_dup_check = true;
            break;

        case HA_EXTRA_FLUSH:
            if (share && share->cf)
            {
                tidesdb_flush_memtable(share->cf);
                int flush_wait = 0;
                while (tidesdb_is_flushing(share->cf) &&
                       flush_wait < TIDESDB_FLUSH_WAIT_MAX_ITERATIONS)
                {
                    my_sleep(TIDESDB_FLUSH_WAIT_SLEEP_US);
                    flush_wait++;
                }
            }
            break;

        case HA_EXTRA_CACHE:
            /*
              Sequential scan prefetch hint. When the optimizer signals
              HA_EXTRA_CACHE, we warm the block cache by doing a quick
              read-ahead pass. We create a throwaway iterator and read
              PREFETCH_BATCH_SIZE keys forward from the current scan
              position. If no position is known yet, seek to first.
            */
            prefetch_active = true;
            if (share && share->cf && tidesdb_instance)
            {
                tidesdb_txn_t *pf_txn = NULL;
                if (tidesdb_txn_begin(tidesdb_instance, &pf_txn) == TDB_SUCCESS)
                {
                    tidesdb_iter_t *pf_iter = NULL;
                    if (tidesdb_iter_new(pf_txn, share->cf, &pf_iter) == TDB_SUCCESS && pf_iter)
                    {
                        if (current_key && current_key_len > 0)
                            tidesdb_iter_seek(pf_iter, current_key, current_key_len);
                        else
                            tidesdb_iter_seek_to_first(pf_iter);

                        for (uint i = 0; i < PREFETCH_BATCH_SIZE && tidesdb_iter_valid(pf_iter);
                             i++)
                        {
                            uint8_t *k = NULL;
                            size_t klen = 0;
                            tidesdb_iter_key(pf_iter, &k, &klen);
                            tidesdb_iter_next(pf_iter);
                        }
                        tidesdb_iter_free(pf_iter);
                    }
                    tidesdb_txn_rollback(pf_txn);
                    tidesdb_txn_free(pf_txn);
                }
            }
            break;

        case HA_EXTRA_NO_CACHE:
            prefetch_active = false;
            break;

        case HA_EXTRA_WRITE_CACHE:
            break;

        case HA_EXTRA_PREPARE_FOR_UPDATE:
            break;

        case HA_EXTRA_PREPARE_FOR_DROP:
            /*
              Table is about to be dropped. Skip flushing the memtable
              since the data will be deleted anyway. This avoids
              unnecessary I/O during DROP TABLE.
            */
            break;

        case HA_EXTRA_PREPARE_FOR_RENAME:
            /*
              Table is about to be renamed. Flush memtable to ensure
              all data is persisted before the CF rename.
            */
            if (share && share->cf)
            {
                tidesdb_flush_memtable(share->cf);
                int rename_wait = 0;
                while (tidesdb_is_flushing(share->cf) &&
                       rename_wait < TIDESDB_FLUSH_WAIT_MAX_ITERATIONS)
                {
                    my_sleep(TIDESDB_FLUSH_WAIT_SLEEP_US);
                    rename_wait++;
                }
            }
            break;

        case HA_EXTRA_BEGIN_ALTER_COPY:
            /*
              ALTER TABLE ... ALGORITHM=COPY is starting. Switch to
              aggressive write mode -- the entire table will be rewritten.
            */
            skip_dup_check = true;
            break;

        case HA_EXTRA_END_ALTER_COPY:
            /*
              ALTER TABLE ... ALGORITHM=COPY is finishing. Flush to
              ensure all data is persisted.
            */
            skip_dup_check = false;
            if (share && share->cf)
            {
                tidesdb_flush_memtable(share->cf);
                int alter_wait = 0;
                while (tidesdb_is_flushing(share->cf) &&
                       alter_wait < TIDESDB_FLUSH_WAIT_MAX_ITERATIONS)
                {
                    my_sleep(TIDESDB_FLUSH_WAIT_SLEEP_US);
                    alter_wait++;
                }
            }
            break;

        case HA_EXTRA_STARTING_ORDERED_INDEX_SCAN:
            /*
              Ordered index scan is starting (used by partitioning).
              Pre-warm the block cache by reading keys ahead.
            */
            prefetch_active = true;
            if (share && share->cf && tidesdb_instance)
            {
                tidesdb_txn_t *pf_txn = NULL;
                if (tidesdb_txn_begin(tidesdb_instance, &pf_txn) == TDB_SUCCESS)
                {
                    tidesdb_iter_t *pf_iter = NULL;
                    if (tidesdb_iter_new(pf_txn, share->cf, &pf_iter) == TDB_SUCCESS && pf_iter)
                    {
                        tidesdb_iter_seek_to_first(pf_iter);
                        for (uint i = 0; i < PREFETCH_BATCH_SIZE && tidesdb_iter_valid(pf_iter);
                             i++)
                        {
                            uint8_t *k = NULL;
                            size_t klen = 0;
                            tidesdb_iter_key(pf_iter, &k, &klen);
                            tidesdb_iter_next(pf_iter);
                        }
                        tidesdb_iter_free(pf_iter);
                    }
                    tidesdb_txn_rollback(pf_txn);
                    tidesdb_txn_free(pf_txn);
                }
            }
            break;

        case HA_EXTRA_RESET_STATE:
            skip_dup_check = false;
            keyread_only = false;
            pushed_idx_cond = NULL;
            pushed_idx_cond_keyno = MAX_KEY;
            prefetch_active = false;
            break;

        default:
            break;
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Index Condition Pushdown (ICP).

  Accept pushed index conditions for evaluation during index scan.
  This allows the storage engine to filter rows before sending them
  to the SQL layer, reducing data transfer overhead.
*/
Item *ha_tidesdb::idx_cond_push(uint keyno, Item *idx_cond)
{
    DBUG_ENTER("ha_tidesdb::idx_cond_push");

    pushed_idx_cond = idx_cond;
    pushed_idx_cond_keyno = keyno;
    in_range_check_pushed_down = true;

    /* We return NULL to indicate we accept all conditions */
    DBUG_RETURN(NULL);
}

/**
  @brief
  Accept a rowid filter for semi-join optimization.

  The rowid filter allows the optimizer to skip rows during index scans
  that don't match a pre-built filter from another table in a semi-join.
  The base handler class manages the filter lifecycle; we just accept it.

  @param rowid_filter  The filter to push down

  @return false if accepted (always)
*/
bool ha_tidesdb::rowid_filter_push(Rowid_filter *rowid_filter)
{
    DBUG_ENTER("ha_tidesdb::rowid_filter_push");
    DBUG_ASSERT(rowid_filter != NULL);
    pushed_rowid_filter = rowid_filter;
    DBUG_RETURN(false);
}

/**
  @brief
  Check if ALTER TABLE data is compatible with the current table.

  Returns COMPATIBLE_DATA_YES if the ALTER only changes metadata
  (e.g., comments, default values) and doesn't require a table rebuild.
  This avoids unnecessary COPY operations for trivial ALTERs.

  @param info           HA_CREATE_INFO describing the new table
  @param table_changes  IS_EQUAL_YES if columns are unchanged

  @return COMPATIBLE_DATA_YES or COMPATIBLE_DATA_NO
*/
bool ha_tidesdb::check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes)
{
    DBUG_ENTER("ha_tidesdb::check_if_incompatible_data");

    /* If column definitions changed, we need a rebuild */
    if (table_changes != IS_EQUAL_YES)
    {
        DBUG_RETURN(COMPATIBLE_DATA_NO);
    }

    /* If AUTO_INCREMENT value is being explicitly set, need rebuild */
    if ((info->used_fields & HA_CREATE_USED_AUTO) && info->auto_increment_value != 0)
    {
        DBUG_RETURN(COMPATIBLE_DATA_NO);
    }

    /* If row format changed, need rebuild */
    if ((info->used_fields & HA_CREATE_USED_ROW_FORMAT))
    {
        DBUG_RETURN(COMPATIBLE_DATA_NO);
    }

    /* If KEY_BLOCK_SIZE changed, need rebuild */
    if (info->used_fields & HA_CREATE_USED_KEY_BLOCK_SIZE)
    {
        DBUG_RETURN(COMPATIBLE_DATA_NO);
    }

    /*
      Otherwise the data is compatible -- only metadata changed
      (e.g., table comment, default values, engine options).
    */
    DBUG_RETURN(COMPATIBLE_DATA_YES);
}

/**
  @brief
  Table Condition Pushdown (full WHERE clause).

  Accept pushed conditions for evaluation during table scans.
  This allows the storage engine to filter rows during rnd_next()
  before sending them to the SQL layer.

  @param cond  The condition to push down

  @return
    NULL if we accept the entire condition (we will filter rows)
    The original condition if we cannot handle it (SQL layer filters)

  @note
    TidesDB evaluates pushed conditions during rnd_next() by calling
    cond->val_int() on each row. Rows where the condition evaluates
    to FALSE are skipped, reducing data transfer to the SQL layer.
*/
const COND *ha_tidesdb::cond_push(const COND *cond)
{
    DBUG_ENTER("ha_tidesdb::cond_push");

    /*
      We store the pushed condition for evaluation during scans.
      We evaluate it in rnd_next() for early row filtering.

      Return the condition back to the SQL layer so it re-evaluates it
      as well. This is the safe approach -- TidesDB uses val_int() on the
      full Item tree which works for simple column comparisons but may
      not handle all expression types correctly (subqueries, stored
      functions, non-deterministic expressions). Letting the SQL layer
      re-check guarantees correctness while we still benefit from
      skipping unpack_row() on non-matching rows.
    */
    pushed_cond = cond;
    DBUG_RETURN(cond);
}

/**
  @brief
  Pop the top condition from the condition stack.

  Called when the pushed condition is no longer needed.
*/
void ha_tidesdb::cond_pop()
{
    DBUG_ENTER("ha_tidesdb::cond_pop");

    pushed_cond = NULL;

    DBUG_VOID_RETURN;
}

/**
  @brief
  Helper -- drop a column family and remove its on-disk directory.
  tidesdb_drop_column_family removes the in-memory state but may leave
  the on-disk directory.  We clean it up so tidesdb_create_column_family
  starts with a fresh, empty directory.
*/
static int drop_cf_and_cleanup(const char *cf_name)
{
    int ret = tidesdb_drop_column_family(tidesdb_instance, cf_name);
    if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND) return ret;

    /*
      We build the on-disk path -- <tidesdb_data_dir>/<cf_name>
      If the directory still exists after drop, remove it recursively.
    */
    char cf_dir[FN_REFLEN];
    snprintf(cf_dir, sizeof(cf_dir), "%s" TIDESDB_PATH_SEP_STR "tidesdb" TIDESDB_PATH_SEP_STR "%s",
             mysql_real_data_home, cf_name);

    MY_DIR *dir = my_dir(cf_dir, MYF(0));
    if (dir)
    {
        /* Directory still exists -- we remove all files then the directory */
        for (uint i = 0; i < dir->number_of_files; i++)
        {
            char filepath[FN_REFLEN];
            snprintf(filepath, sizeof(filepath), "%s" TIDESDB_PATH_SEP_STR "%s", cf_dir,
                     dir->dir_entry[i].name);
            my_delete(filepath, MYF(0));
        }
        my_dirend(dir);
        rmdir(cf_dir);
    }

    return 0;
}

/**
  @brief
  Delete all rows in the table.

  Drops and recreates the column family (and all secondary index / fulltext CFs).
  This is the fastest way to clear all data.
*/
int ha_tidesdb::delete_all_rows()
{
    DBUG_ENTER("ha_tidesdb::delete_all_rows");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    get_cf_name(share->table_name, cf_name, sizeof(cf_name));

    tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
    cf_config.write_buffer_size = tidesdb_write_buffer_size;
    cf_config.enable_bloom_filter = tidesdb_enable_bloom_filter ? 1 : 0;
    cf_config.use_btree = tidesdb_use_btree ? 1 : 0;

    if (tidesdb_enable_compression)
        cf_config.compression_algorithm = (compression_algorithm)tidesdb_compression_algo;
    else
        cf_config.compression_algorithm = TDB_COMPRESS_NONE;

    int ret;

    /* We drop and recreate the main column family */
    ret = drop_cf_and_cleanup(cf_name);
    if (ret != 0)
    {
        sql_print_error("TidesDB: delete_all_rows: failed to drop main CF '%s': %d", cf_name, ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    ret = tidesdb_create_column_family(tidesdb_instance, cf_name, &cf_config);
    if (ret != TDB_SUCCESS)
    {
        sql_print_error("TidesDB: delete_all_rows: failed to recreate main CF '%s': %d", cf_name,
                        ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }
    share->cf = tidesdb_get_column_family(tidesdb_instance, cf_name);

    /* We drop and recreate secondary index CFs */
    for (uint i = 0; i < table->s->keys; i++)
    {
        if (i == table->s->primary_key) continue;
        if (table->key_info[i].algorithm == HA_KEY_ALG_FULLTEXT) continue;
        if (!share->index_cf[i]) continue;

        char idx_cf_name[TIDESDB_CF_NAME_BUF_SIZE];
        snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_FMT, cf_name, i);

        drop_cf_and_cleanup(idx_cf_name);
        ret = tidesdb_create_column_family(tidesdb_instance, idx_cf_name, &cf_config);
        if (ret == TDB_SUCCESS)
            share->index_cf[i] = tidesdb_get_column_family(tidesdb_instance, idx_cf_name);
    }

    /* We drop and recreate fulltext index CFs */
    for (uint i = 0; i < share->num_ft_indexes; i++)
    {
        if (!share->ft_cf[i]) continue;

        char ft_cf_name[TIDESDB_CF_NAME_BUF_SIZE];
        snprintf(ft_cf_name, sizeof(ft_cf_name), TIDESDB_CF_FT_FMT, cf_name, share->ft_key_nr[i]);

        drop_cf_and_cleanup(ft_cf_name);
        ret = tidesdb_create_column_family(tidesdb_instance, ft_cf_name, &cf_config);
        if (ret == TDB_SUCCESS)
            share->ft_cf[i] = tidesdb_get_column_family(tidesdb_instance, ft_cf_name);
    }

    stats.records = 0;
    share->row_count = 0;
    share->row_count_valid = true;

    my_atomic_store64_explicit((volatile int64 *)&share->auto_increment_value, 1,
                               MY_MEMORY_ORDER_RELEASE);
    my_atomic_store64_explicit((volatile int64 *)&share->hidden_pk_value, 0,
                               MY_MEMORY_ORDER_RELEASE);

    DBUG_RETURN(0);
}

/**
  @brief
  Truncate table -- faster than delete_all_rows.

  For TidesDB, truncate is the same as delete_all_rows since we
  drop and recreate the column family. We also reset auto_increment.
*/
int ha_tidesdb::truncate()
{
    DBUG_ENTER("ha_tidesdb::truncate");

    int error = delete_all_rows();
    if (error) DBUG_RETURN(error);

    error = reset_auto_increment(0);
    DBUG_RETURN(error);
}

/**
  @brief
  Return exact row count.

  Called when HA_HAS_RECORDS is set. TidesDB can provide exact counts
  from its statistics.
*/
ha_rows ha_tidesdb::records()
{
    DBUG_ENTER("ha_tidesdb::records");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    get_cf_name(share->table_name, cf_name, sizeof(cf_name));

    if (share && share->row_count_valid)
    {
        sql_print_information("TidesDB: records() for CF '%s' - returning cached row_count=%llu",
                              cf_name, (unsigned long long)share->row_count);
        DBUG_RETURN(share->row_count);
    }

    if (share && share->cf)
    {
        tidesdb_stats_t *tdb_stats = get_realtime_stats(share);
        if (tdb_stats)
        {
            /*
              We subtract metadata keys (same logic as info()) to avoid
              over-counting by 1-2 rows.
            */
            ha_rows metadata_keys = 0;
            if (!share->has_primary_key) metadata_keys++;
            if (table->s->found_next_number_field) metadata_keys++;

            ha_rows count = tdb_stats->total_keys > metadata_keys
                                ? tdb_stats->total_keys - metadata_keys
                                : 2; /* never 0 -- uncommitted txn ops invisible to stats */
            tidesdb_free_stats(tdb_stats);
            share->row_count = count;
            share->row_count_valid = true;
            sql_print_information("TidesDB: records() for CF '%s' - returning stats count=%llu",
                                  cf_name, (unsigned long long)count);
            DBUG_RETURN(count);
        }
    }

    sql_print_information("TidesDB: records() for CF '%s' - returning stats.records=%llu", cf_name,
                          (unsigned long long)stats.records);
    DBUG_RETURN(stats.records);
}

/**
  @brief
  Check if the last read was a semi-consistent read.

  Semi-consistent reads allow UPDATE/DELETE under READ COMMITTED to
  read the last committed version of a row when the current version
  is locked by another transaction. The SQL layer then re-evaluates
  the WHERE clause -- if the row no longer matches, it is skipped
  without waiting for the lock.

  For TidesDB with MVCC, this maps naturally -- when we detect a
  conflict during read (TDB_ERR_CONFLICT), we can read the last
  committed version instead of blocking.
*/
bool ha_tidesdb::was_semi_consistent_read()
{
    DBUG_ENTER("ha_tidesdb::was_semi_consistent_read");
    DBUG_RETURN(did_semi_consistent_read);
}

/**
  @brief
  Enable or disable semi-consistent reads.

  Called by the SQL layer before UPDATE/DELETE scans under
  READ COMMITTED isolation. When enabled, the engine should
  attempt to read the last committed version of conflicting rows
  rather than waiting or returning an error.
*/
void ha_tidesdb::try_semi_consistent_read(bool yes)
{
    DBUG_ENTER("ha_tidesdb::try_semi_consistent_read");

    /*
      We only enable semi-consistent reads for READ COMMITTED or lower.
      Higher isolation levels require strict snapshot consistency.
    */
    THD *thd = ha_thd();
    if (yes && thd && thd->variables.tx_isolation <= ISO_READ_COMMITTED)
    {
        semi_consistent_read_enabled = true;
    }
    else
    {
        semi_consistent_read_enabled = false;
    }
    did_semi_consistent_read = false;

    DBUG_VOID_RETURN;
}

/**
  @brief
  Statement-level transaction handling.

  Called at the start of each statement. For TidesDB, we ensure
  a transaction exists for the statement.
*/
int ha_tidesdb::start_stmt(THD *thd, thr_lock_type lock_type)
{
    DBUG_ENTER("ha_tidesdb::start_stmt");

    /*
      Like InnoDB, we sync the handler's transaction with the THD-level
      transaction at the start of each statement. This is critical for
      multi-statement transactions where a new handler instance may be
      created for each statement.
    */
    bool in_transaction = thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
    if (in_transaction)
    {
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);
        if (thd_txn)
        {
            current_txn = thd_txn;
        }
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Custom error messages for TidesDB errors.

  Provides human-readable error messages for TidesDB-specific errors.
*/
bool ha_tidesdb::get_error_message(int error, String *buf)
{
    DBUG_ENTER("ha_tidesdb::get_error_message");

    const char *msg = NULL;
    switch (error)
    {
        case HA_ERR_LOCK_DEADLOCK:
            msg = "TidesDB: Transaction conflict (MVCC)";
            break;
        case HA_ERR_LOCK_WAIT_TIMEOUT:
            msg = "TidesDB: Transaction timeout";
            break;
        case HA_ERR_CRASHED:
            msg = "TidesDB: Column family corrupted";
            break;
        case HA_ERR_OUT_OF_MEM:
            msg = "TidesDB: Out of memory";
            break;
        default:
            DBUG_RETURN(false);
    }

    if (msg)
    {
        buf->copy(msg, strlen(msg), system_charset_info);
        DBUG_RETURN(true);
    }
    DBUG_RETURN(false);
}

/**
  @brief
  Index flags indicating how the storage engine implements indexes.

  Primary key is clustered in TidesDB -- row data is stored with the key.
  Secondary indexes store (index_key -> primary_key) mappings.
*/
ulong ha_tidesdb::index_flags(uint inx, uint part, bool all_parts) const
{
    ulong flags =
        HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE | HA_DO_INDEX_COND_PUSHDOWN;

    /*
      Primary key is clustered in TidesDB -- row data is stored with the key.
      For clustered indexes:
      -- HA_CLUSTERED_INDEX             -- data is stored with the key (no secondary lookup)
      -- No HA_KEYREAD_ONLY             -- keyread doesn't make sense for clustered PK
      -- No HA_DO_RANGE_FILTER_PUSHDOWN -- not applicable for clustered index
    */
    if (table_share && inx == table_share->primary_key)
    {
        flags |= HA_CLUSTERED_INDEX;
    }
    else
    {
        /* Secondary indexes support keyread and rowid filter */
        flags |= HA_KEYREAD_ONLY | HA_DO_RANGE_FILTER_PUSHDOWN;
    }

    return flags;
}

/**
  @brief
  Estimate the cost of a full table scan.

  TidesDB LSM-tree scan cost model based on architecture:

  Read path for full scan:
  1. Active memtable (in-memory skip list)
  2. Immutable memtables awaiting flush (in-memory)
  3. SSTables level by level (L1, L2, ..., Ln)

  For each SSTable during scan:
  -- Sequential block reads (64KB blocks)
  -- Merge iterator maintains min-heap across all sources
  -- Values > 512 bytes require vlog lookup (extra seek)

  Cost components:
  -- I/O -- (total_blocks * cache_miss_rate) + vlog_seeks
  -- CPU -- merge_heap_overhead + deserialization

  B+tree vs Block-based format:
  -- B+tree -- Doubly-linked leaf nodes enable O(1) next-leaf traversal
  -- Block-based -- Sequential block reads, no extra overhead for scans
*/
IO_AND_CPU_COST ha_tidesdb::scan_time()
{
    DBUG_ENTER("ha_tidesdb::scan_time");

    IO_AND_CPU_COST cost;
    cost.io = TIDESDB_MIN_IO_COST;
    cost.cpu = 0.0;

    if (share && share->cf)
    {
        tidesdb_stats_t *tdb_stats = get_realtime_stats(share);
        if (tdb_stats)
        {
            /*
              I/O cost based on total data size
              TidesDB uses fixed 64KB blocks for klog
            */
            double data_bytes = (double)tdb_stats->total_data_size;
            double block_size =
                TIDESDB_BLOCK_SIZE; /* 64KB fixed block size; block layout and btree same */
            double num_blocks = data_bytes / block_size;
            if (num_blocks < TIDESDB_MIN_IO_COST) num_blocks = TIDESDB_MIN_IO_COST;

            /*
              Merge iterator overhead (min-heap across all sources)
              Sources -- 1 active memtable + immutable memtables + all SSTables
              Heap operations -- O(log S) per row where S = number of sources
            */
            int total_sources = 1; /* Active memtable */
            for (int i = 0; i < tdb_stats->num_levels; i++)
            {
                total_sources += tdb_stats->level_num_sstables[i];
            }
            /* Merge overhead scales with log(sources) for heap operations */
            double merge_overhead =
                TIDESDB_MIN_IO_COST + (log2((double)total_sources) * TIDESDB_MERGE_OVERHEAD_FACTOR);
            if (merge_overhead < TIDESDB_MIN_IO_COST) merge_overhead = TIDESDB_MIN_IO_COST;

            /*
              Cache effectiveness
              TidesDB uses clock cache with zero-copy reads
              High hit rate means blocks served from memory
            */
            double cache_factor =
                TIDESDB_MIN_IO_COST - (tdb_stats->hit_rate * TIDESDB_CACHE_EFFECTIVENESS_FACTOR);
            if (cache_factor < TIDESDB_MIN_CACHE_FACTOR) cache_factor = TIDESDB_MIN_CACHE_FACTOR;

            /*
              Vlog indirection cost for large values (> TIDESDB_VLOG_LARGE_VALUE_THRESHOLD bytes
              default) Each large value requires additional seek to vlog file Estimate -- ~20% of
              values are large (heuristic)
            */
            double vlog_overhead = TIDESDB_MIN_IO_COST;
            if (tdb_stats->avg_value_size > TIDESDB_VLOG_LARGE_VALUE_THRESHOLD)
            {
                /* Most values are large, significant vlog overhead */
                vlog_overhead = TIDESDB_VLOG_OVERHEAD_LARGE;
            }
            else if (tdb_stats->avg_value_size > TIDESDB_VLOG_MEDIUM_VALUE_THRESHOLD)
            {
                /* Some values are large */
                vlog_overhead = TIDESDB_VLOG_OVERHEAD_MEDIUM;
            }

            /*
              B+tree vs Block-based format for sequential scans
              -- B+tree -- Leaf nodes doubly-linked, O(1) traversal via next_offset
              -- Block-based -- Sequential reads, slightly more efficient for full scans
              For full table scans, block-based is marginally better (no tree overhead)
            */
            double format_factor = TIDESDB_MIN_IO_COST;
            if (tdb_stats->use_btree)
            {
                /* B+tree has slight overhead for leaf link traversal */
                format_factor = TIDESDB_BTREE_FORMAT_OVERHEAD;
            }
            /* Block-based is baseline (TIDESDB_MIN_IO_COST) for sequential scans */

            /*
              CPU cost -- deserialization + merge heap operations
              -- Varint decoding for each entry
              -- Prefix decompression for B+tree keys
              -- Heap sift operations
            */
            double cpu_cost =
                (double)tdb_stats->total_keys * TIDESDB_CPU_COST_PER_KEY * merge_overhead;

            /* Total I/O cost */
            cost.io = num_blocks * merge_overhead * cache_factor * vlog_overhead * format_factor;
            cost.cpu = cpu_cost;
            tidesdb_free_stats(tdb_stats);
        }
        else
        {
            /* Fallback without stats */
            ha_rows rows = stats.records;
            if (rows == 0) rows = TIDESDB_FALLBACK_ROW_ESTIMATE;
            cost.io = (double)rows / (double)TIDESDB_FALLBACK_ROW_ESTIMATE + TIDESDB_MIN_IO_COST;
            cost.cpu = 0.0;
        }
    }

    if (cost.io < TIDESDB_MIN_IO_COST) cost.io = TIDESDB_MIN_IO_COST;

    DBUG_RETURN(cost);
}

/**
  @brief
  Estimate the cost of reading rows via index.

  TidesDB point lookup cost model based on architecture:

  For each point lookup, search order:
  1. Active memtable (skip list binary search)
  2. Immutable memtables (skip list binary search each)
  3. For each SSTable (L1, L2, ..., Ln):
     a. Check min/max key bounds (O(1))
     b. Check bloom filter if enabled (O(k) hash ops, 1% FPR default)
     c. If bloom positive -- block index binary search O(log B)
     d. Read block and binary search within block
     e. If vlog offset, read value from vlog (extra seek)

  Bloom filter effectiveness (critical for LSM performance):
  -- 1% FPR means 99% of absent-key lookups skip disk I/O
  -- Expected reads for absent key -- 1 + L*0.01 where L = num_levels
  -- For present key -- must read actual block (bloom doesn't help)

  B+tree vs Block-based format:
  -- B+tree         -- O(log N) tree traversal, binary search at each node
  -- Block-based    -- O(log B) block index + O(log E) within 64KB block
  -- B+tree excels at point lookups due to better cache locality
*/
IO_AND_CPU_COST ha_tidesdb::read_time(uint index, uint ranges, ha_rows rows)
{
    DBUG_ENTER("ha_tidesdb::read_time");

    IO_AND_CPU_COST cost;
    cost.io = TIDESDB_READ_TIME_BASE_IO;
    cost.cpu = 0.0;

    if (rows == 0) DBUG_RETURN(cost);

    if (share && share->cf)
    {
        tidesdb_stats_t *tdb_stats = get_realtime_stats(share);
        if (tdb_stats)
        {
            /*
              Read amplification from TidesDB stats
              With bloom filters (1% FPR) -- read_amp ≈ 1.0-1.5
              Without bloom filters -- read_amp ≈ num_levels
            */
            double read_amp = tdb_stats->read_amp;
            if (read_amp < TIDESDB_MIN_IO_COST) read_amp = TIDESDB_MIN_IO_COST;

            /*
              Bloom filter benefit for point lookups
              TidesDB checks bloom filter before any disk I/O
              1% FPR eliminates 99% of unnecessary SSTable reads
            */
            double bloom_benefit = TIDESDB_MIN_IO_COST;
            if (tdb_stats->config && tdb_stats->config->enable_bloom_filter)
            {
                /* Bloom filter reduces effective read_amp significantly */
                double fpr = tdb_stats->config->bloom_fpr;
                if (fpr <= 0) fpr = TIDESDB_DEFAULT_BLOOM_FPR; /* Default 1% */
                /* Expected SSTable reads = 1 + (L-1)*FPR for present keys */
                bloom_benefit = TIDESDB_BLOOM_BENEFIT_BASE + (fpr * tdb_stats->num_levels);
                if (bloom_benefit > TIDESDB_MIN_IO_COST) bloom_benefit = TIDESDB_MIN_IO_COST;
            }

            /*
              B+tree vs Block-based format for point lookups

              B+tree advantages:
              -- O(log N) tree traversal with binary search at each node
              -- Nodes cached independently (hot nodes stay in cache)
              -- Key indirection table enables O(1) random access within node
              -- Prefix compression reduces memory bandwidth

              Block-based:
              -- O(log B) block index lookup + O(log E) binary search in 64KB block
              -- Must read entire 64KB block even for single key lookup
              -- Block index uses prefixes (may need multiple blocks)
            */
            double format_factor = TIDESDB_MIN_IO_COST;
            if (tdb_stats->use_btree)
            {
                /* B+tree is significantly faster for point lookups */
                if (tdb_stats->btree_avg_height > 0)
                {
                    /* Cost based on actual tree height (typically 2-4 levels) */
                    /* Each level = 1 node read, but nodes are often cached */
                    format_factor =
                        TIDESDB_BTREE_HEIGHT_COST_BASE +
                        (tdb_stats->btree_avg_height * TIDESDB_BTREE_HEIGHT_COST_PER_LEVEL);
                    if (format_factor > TIDESDB_BTREE_MAX_FORMAT_FACTOR)
                        format_factor = TIDESDB_BTREE_MAX_FORMAT_FACTOR;
                }
                else
                {
                    /* Default B+tree benefit -- ~50% faster than block-based */
                    format_factor = TIDESDB_BTREE_DEFAULT_FORMAT_FACTOR;
                }
            }
            else
            {
                /* Block-based format baseline */
                /* Must read 64KB block for each lookup */
                format_factor = TIDESDB_MIN_IO_COST;
            }

            /*
              Seek cost per range
              Includes -- memtable search + bloom checks + block index + block read
            */
            double seek_cost = TIDESDB_SEEK_COST_BASE * read_amp * bloom_benefit * format_factor;

            /*
              Row fetch cost per row
              After initial seek, subsequent rows in same block are cheap
            */
            double avg_row_size = tdb_stats->avg_key_size + tdb_stats->avg_value_size;
            if (avg_row_size < TIDESDB_MIN_AVG_ROW_SIZE) avg_row_size = TIDESDB_MIN_AVG_ROW_SIZE;

            double rows_per_block = TIDESDB_BLOCK_SIZE / avg_row_size;
            if (rows_per_block < TIDESDB_MIN_IO_COST) rows_per_block = TIDESDB_MIN_IO_COST;

            /* Amortized cost per row within a block */
            double row_fetch_cost = (TIDESDB_MIN_IO_COST / rows_per_block) * format_factor;

            /*
              Vlog indirection for large values
              Values > TIDESDB_VLOG_LARGE_VALUE_THRESHOLD bytes stored in vlog, require extra seek
            */
            double vlog_factor = TIDESDB_MIN_IO_COST;
            if (tdb_stats->avg_value_size > TIDESDB_VLOG_LARGE_VALUE_THRESHOLD)
            {
                vlog_factor = TIDESDB_VLOG_FACTOR_LARGE; /* Extra seek for each row */
            }
            else if (tdb_stats->avg_value_size > TIDESDB_VLOG_MEDIUM_VALUE_THRESHOLD)
            {
                vlog_factor = TIDESDB_VLOG_FACTOR_MEDIUM; /* Some values in vlog */
            }

            /*
              Cache effectiveness
              Clock cache with zero-copy reads
              Hot blocks/nodes stay in memory
            */
            double cache_factor =
                TIDESDB_MIN_IO_COST - (tdb_stats->hit_rate * TIDESDB_CACHE_EFFECTIVENESS_FACTOR);
            if (cache_factor < TIDESDB_MIN_CACHE_FACTOR) cache_factor = TIDESDB_MIN_CACHE_FACTOR;

            /*
              Secondary index overhead
              Secondary index lookup requires:
              1. Index CF lookup to get PK
              2. Main CF lookup using PK
              This doubles the effective read cost
            */
            double secondary_idx_factor = TIDESDB_MIN_IO_COST;
            if (index != table->s->primary_key)
            {
                secondary_idx_factor = TIDESDB_SECONDARY_IDX_FACTOR;
            }

            /* Total cost */
            cost.io = (ranges * seek_cost * cache_factor * secondary_idx_factor) +
                      (rows * row_fetch_cost * cache_factor * vlog_factor * secondary_idx_factor);
            cost.cpu = (double)rows * TIDESDB_CPU_COST_PER_KEY;
            tidesdb_free_stats(tdb_stats);
        }
        else
        {
            /* Fallback without stats */
            cost.io = (double)ranges * TIDESDB_FALLBACK_RANGE_COST +
                      (double)rows * TIDESDB_FALLBACK_ROW_COST;
            cost.cpu = 0.0;
        }
    }

    if (cost.io < TIDESDB_READ_TIME_BASE_IO) cost.io = TIDESDB_READ_TIME_BASE_IO;

    DBUG_RETURN(cost);
}

/**
  @brief
  Estimate records in a range.

  Uses TidesDB statistics for accurate estimation:
  -- total_keys -- actual key count for base estimate
  -- Selectivity heuristics based on key parts and condition type

  This is critical for optimizer decisions:
  -- Low estimate -> optimizer prefers index scan
  -- High estimate -> optimizer prefers table scan
*/
ha_rows ha_tidesdb::records_in_range(uint inx, const key_range *min_key, const key_range *max_key,
                                     page_range *pages)
{
    DBUG_ENTER("ha_tidesdb::records_in_range");

    ha_rows total_rows = stats.records;
    if (share && share->cf)
    {
        tidesdb_stats_t *tdb_stats = get_realtime_stats(share);
        if (tdb_stats)
        {
            total_rows = tdb_stats->total_keys;
            tidesdb_free_stats(tdb_stats);
        }
    }

    if (total_rows == 0)
        total_rows = TIDESDB_FALLBACK_ROW_ESTIMATE; /* Minimum estimate for empty tables */

    /* If no key bounds, return all rows (full index scan) */
    if (!min_key && !max_key) DBUG_RETURN(total_rows);

    /* We count key parts used in the condition */
    KEY *key_info = &table->key_info[inx];
    uint key_parts_used = 0;

    if (min_key)
    {
        for (uint i = 0; i < key_info->user_defined_key_parts; i++)
        {
            if (min_key->keypart_map & (1 << i)) key_parts_used++;
        }
    }

    /*
      Selectivity estimation:
      -- First key part -- assume 1/sqrt(total_rows) selectivity (moderate cardinality)
      -- Each additional key part -- divide by N (compound key selectivity)
      -- This is a heuristic; real cardinality stats would be better
    */
    double selectivity = TIDESDB_DEFAULT_SELECTIVITY;
    if (key_parts_used > 0)
    {
        /* First key part -- we assume moderate cardinality */
        double first_part_sel = 1.0 / sqrt((double)total_rows);
        if (first_part_sel > TIDESDB_SELECTIVITY_CAP)
            first_part_sel = TIDESDB_SELECTIVITY_CAP; /* Cap at n% for first part */
        selectivity = first_part_sel;

        /* Additional key parts increase selectivity */
        for (uint i = 1; i < key_parts_used; i++)
        {
            selectivity /= TIDESDB_SELECTIVITY_DIVISOR;
        }
    }

    /* We check for equality condition (min_key == max_key) */
    bool is_equality = (min_key && max_key && min_key->length == max_key->length &&
                        memcmp(min_key->key, max_key->key, min_key->length) == 0);

    if (is_equality)
    {
        /* Primary key equality -- exactly 1 row */
        if (inx == table->s->primary_key)
        {
            DBUG_RETURN(1);
        }

        /* Unique secondary index: exactly 1 row */
        if (key_info->flags & HA_NOSAME)
        {
            DBUG_RETURN(1);
        }

        /* Non-unique secondary index -- we estimate based on selectivity */
        ha_rows estimate = (ha_rows)(total_rows * selectivity);
        if (estimate < 1) estimate = 1;
        /* Non-unique indexes can have many duplicates */
        if (estimate > total_rows / TIDESDB_NONUNIQUE_MAX_FRACTION)
            estimate = total_rows / TIDESDB_NONUNIQUE_MAX_FRACTION;
        if (estimate < 1) estimate = 1;
        DBUG_RETURN(estimate);
    }

    /*
      Data sampling -- for primary key ranges, use an iterator to sample
      actual keys in the range. We scan up to TIDESDB_RANGE_SAMPLE_LIMIT
      keys and extrapolate if the sample is exhausted. This gives much
      better estimates than pure heuristics.
    */
    if (inx == table->s->primary_key && min_key && current_txn && share->cf)
    {
        tidesdb_iter_t *sample_iter = NULL;
        int sret = tidesdb_iter_new(current_txn, share->cf, &sample_iter);
        if (sret == TDB_SUCCESS && sample_iter)
        {
            sret = tidesdb_iter_seek(sample_iter, (uint8_t *)min_key->key, min_key->length);
            if (sret == TDB_SUCCESS)
            {
                ha_rows sample_count = 0;
                bool exhausted = true;

                while (tidesdb_iter_valid(sample_iter) && sample_count < TIDESDB_RANGE_SAMPLE_LIMIT)
                {
                    if (max_key)
                    {
                        uint8_t *iter_key = NULL;
                        size_t iter_key_len = 0;
                        if (tidesdb_iter_key(sample_iter, &iter_key, &iter_key_len) != TDB_SUCCESS)
                            break;

                        /* We stop if we've passed the max key */
                        int cmp =
                            memcmp(iter_key, max_key->key,
                                   iter_key_len < max_key->length ? iter_key_len : max_key->length);
                        if (cmp > 0 || (cmp == 0 && iter_key_len > max_key->length))
                        {
                            exhausted = false;
                            break;
                        }
                    }
                    sample_count++;
                    tidesdb_iter_next(sample_iter);
                }

                tidesdb_iter_free(sample_iter);

                if (!exhausted || sample_count < TIDESDB_RANGE_SAMPLE_LIMIT)
                {
                    /* We scanned the full range or it's small enough */
                    DBUG_RETURN(sample_count < 1 ? 1 : sample_count);
                }
                /* Sample exhausted -- extrapolate from heuristics but use sample as floor */
            }
            else
            {
                tidesdb_iter_free(sample_iter);
            }
        }
    }

    double range_factor = TIDESDB_RANGE_FACTOR;
    ha_rows estimate = (ha_rows)(total_rows * selectivity * range_factor);

    if (estimate < 1) estimate = 1;

    /* We cap at n% of table for range scans */
    ha_rows max_estimate = total_rows * TIDESDB_RANGE_MAX_PERCENT / TIDESDB_PERCENT_DENOMINATOR;
    if (max_estimate < TIDESDB_MIN_RANGE_CAP) max_estimate = TIDESDB_MIN_RANGE_CAP;
    if (estimate > max_estimate) estimate = max_estimate;

    DBUG_RETURN(estimate);
}

/**
  @brief
  Map MySQL/MariaDB isolation level to TidesDB isolation level.

  MySQL/MariaDB -- ISO_READ_UNCOMMITTED=0, ISO_READ_COMMITTED=1,
         ISO_REPEATABLE_READ=2, ISO_SERIALIZABLE=3
  TidesDB -- READ_UNCOMMITTED=0, READ_COMMITTED=1, REPEATABLE_READ=2,
           SNAPSHOT=3, SERIALIZABLE=4
*/
static int map_isolation_level(enum_tx_isolation mysql_iso)
{
    switch (mysql_iso)
    {
        case ISO_READ_UNCOMMITTED:
            return 0; /* TDB_ISOLATION_READ_UNCOMMITTED */
        case ISO_READ_COMMITTED:
            return 1; /* TDB_ISOLATION_READ_COMMITTED */
        case ISO_REPEATABLE_READ:
            return 2; /* TDB_ISOLATION_REPEATABLE_READ */
        case ISO_SERIALIZABLE:
            return 4; /* TDB_ISOLATION_SERIALIZABLE */
        default:
            return 1; /* Default to READ_COMMITTED */
    }
}

/**
  @brief
  Handle external locking (transaction boundaries).

  For proper savepoint support, we store the transaction at the THD level
  when in a multi-statement transaction (BEGIN...COMMIT), and at the
  handler level for auto-commit mode.

  IMPORTANT -- Despite the name "external_lock", TidesDB does NOT perform
  any actual locking. TidesDB uses MVCC (Multi-Version Concurrency Control):
  -- Reads see a consistent snapshot (never block)
  -- Writes use optimistic concurrency (conflict detection at commit)
  -- No row-level or table-level locks are held

  This method is used purely for transaction lifecycle management:
  -- F_WRLCK/F_RDLCK -- Begin a transaction (or join existing THD transaction)
  -- F_UNLCK         -- End transaction (commit in auto-commit mode, or detach in explicit txn)
*/
int ha_tidesdb::external_lock(THD *thd, int lock_type)
{
    DBUG_ENTER("ha_tidesdb::external_lock");

    /* Cache current time for this statement to avoid per-row time() syscalls */
    cached_now = time(NULL);

    if (lock_type != F_UNLCK)
    {
        /*
          F_WRLCK / F_RDLCK -- ensure a THD-level transaction exists.
          TidesDB fully owns MVCC, concurrency and transaction lifecycle.
          All we do here is:
            1. Create the THD-level TidesDB txn if it doesn't exist yet
            2. Point current_txn at it
            3. Register with MariaDB's transaction coordinator so that
               tidesdb_commit / tidesdb_rollback get called at the right time

          We never commit or free transactions here.
        */
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);

        if (!thd_txn)
        {
            int isolation = map_isolation_level((enum_tx_isolation)thd->variables.tx_isolation);

            int ret = tidesdb_txn_begin_with_isolation(
                tidesdb_instance, (tidesdb_isolation_level_t)isolation, &thd_txn);
            if (ret != TDB_SUCCESS)
            {
                sql_print_error("TidesDB: Failed to begin transaction: %d", ret);
                DBUG_RETURN(HA_ERR_GENERIC);
            }
            set_thd_txn(thd, tidesdb_hton, thd_txn);
        }

        current_txn = thd_txn;
        txn_read_only = (lock_type == F_RDLCK);

        /* Always register at statement level so tidesdb_commit / tidesdb_rollback
           are called for every statement. */
        trans_register_ha(thd, FALSE, tidesdb_hton, 0);

        /* Only register at global level when inside an explicit or implicit
           multi-statement transaction.  For autocommit=1 single statements,
           the statement-level registration is sufficient, and registering
           globally would block subsequent DDL (e.g. RENAME TABLE). */
        if (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
            trans_register_ha(thd, TRUE, tidesdb_hton, 0);
    }
    else
    {
        /*
          F_UNLCK -- statement is done.  Just detach the handler's pointer.
          The actual commit/rollback happens in tidesdb_commit / tidesdb_rollback
          called by MariaDB's transaction coordinator.
        */
        current_txn = NULL;
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Store lock information.

  Required by MariaDB handler interface, but TidesDB is lock-free via MVCC.
  We simply record the requested lock type for MariaDB's internal bookkeeping.
  No actual locking occurs -- TidesDB uses:
  -- Snapshot isolation for reads (never blocks)
  -- Optimistic concurrency for writes (conflict detection at commit)
*/
THR_LOCK_DATA **ha_tidesdb::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
    /*
      TidesDB MVCC -- We accept any lock type but don't actually lock.
      The lock.type is used by MariaDB for query planning, not actual locking.
    */
    if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK) lock.type = lock_type;
    *to++ = &lock;
    return to;
}

/**
  @brief
  Optimize table -- triggers manual compaction in TidesDB.

  This is called by OPTIMIZE TABLE statement.
*/
int ha_tidesdb::optimize(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::optimize");

    if (!share || !share->cf)
    {
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    sql_print_information("TidesDB: Triggering compaction for table");

    /*
      First flush the memtable to ensure all data is on disk before compaction.
      This also ensures the column family is in a consistent state.
    */
    int ret = tidesdb_flush_memtable(share->cf);
    if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
    {
        sql_print_warning("TidesDB: Memtable flush returned: %d", ret);
    }

    /* We wait for async flush to complete before triggering compaction */
    int wait_count = 0;
    while (tidesdb_is_flushing(share->cf) && wait_count < TIDESDB_FLUSH_WAIT_MAX_ITERATIONS)
    {
        my_sleep(TIDESDB_FLUSH_WAIT_SLEEP_US);
        wait_count++;
    }

    /* Now we trigger compaction */
    ret = tidesdb_compact(share->cf);
    if (ret != TDB_SUCCESS)
    {
        sql_print_warning("TidesDB: Compaction returned: %d (non-fatal)", ret);
        /* Don't fail -- compaction is best-effort */
    }

    /* Wait for compaction to complete before returning */
    wait_count = 0;
    while (tidesdb_is_compacting(share->cf) && wait_count < TIDESDB_FLUSH_WAIT_MAX_ITERATIONS)
    {
        my_sleep(TIDESDB_FLUSH_WAIT_SLEEP_US);
        wait_count++;
    }

    sql_print_information("TidesDB: OPTIMIZE TABLE completed");
    DBUG_RETURN(HA_ADMIN_OK);
}

/**
  @brief
  Analyze table -- updates statistics.

  This is called by ANALYZE TABLE statement.
*/
int ha_tidesdb::analyze(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::analyze");

    if (!share || !share->cf)
    {
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    /* First call info() to get fresh statistics from TidesDB */
    info(HA_STATUS_VARIABLE | HA_STATUS_CONST);

    tidesdb_stats_t *tdb_stats = NULL;
    int ret = tidesdb_get_stats(share->cf, &tdb_stats);
    if (ret == TDB_SUCCESS && tdb_stats)
    {
        /* We update handler statistics */
        stats.data_file_length = 0;
        for (int i = 0; i < tdb_stats->num_levels; i++)
        {
            stats.data_file_length += tdb_stats->level_sizes[i];
        }
        stats.mean_rec_length = table->s->reclength;

        tidesdb_free_stats(tdb_stats);
    }

    /* We update the share's row count cache */
    share->row_count = stats.records;
    share->row_count_valid = true;

    persist_table_stats();

    DBUG_RETURN(HA_ADMIN_OK);
}

/* TidesDB fulltext search info structure */
struct tidesdb_ft_info
{
    struct _ft_vft *please;
    ha_tidesdb *handler;
    uint ft_idx;
    char **matched_pks;
    size_t *matched_pk_lens;
    float *relevance_scores; /* Per-document TF-IDF relevance scores */
    uint matched_count;
    uint current_match;
    float relevance; /* Relevance of the last read row */
};

static int tidesdb_ft_read_next(FT_INFO *fts, char *record);
static float tidesdb_ft_find_relevance(FT_INFO *fts, uchar *record, uint length);
static void tidesdb_ft_close_search(FT_INFO *fts);
static float tidesdb_ft_get_relevance(FT_INFO *fts);
static void tidesdb_ft_reinit_search(FT_INFO *fts);

static struct _ft_vft tidesdb_ft_vft = {tidesdb_ft_read_next, tidesdb_ft_find_relevance,
                                        tidesdb_ft_close_search, tidesdb_ft_get_relevance,
                                        tidesdb_ft_reinit_search};

static int tidesdb_ft_read_next(FT_INFO *fts, char *record)
{
    return HA_ERR_END_OF_FILE;
}

static float tidesdb_ft_find_relevance(FT_INFO *fts, uchar *record, uint length)
{
    tidesdb_ft_info *info = (tidesdb_ft_info *)fts;
    if (!info || !info->relevance_scores) return 0.0f;
    /* We return relevance of the last-read document */
    uint idx = info->current_match > 0 ? info->current_match - 1 : 0;
    if (idx < info->matched_count) return info->relevance_scores[idx];
    return 0.0f;
}

static void tidesdb_ft_close_search(FT_INFO *fts)
{
    tidesdb_ft_info *info = (tidesdb_ft_info *)fts;
    if (info)
    {
        if (info->matched_pks)
        {
            for (uint i = 0; i < info->matched_count; i++)
            {
                if (info->matched_pks[i]) my_free(info->matched_pks[i]);
            }
            my_free(info->matched_pks);
        }
        if (info->matched_pk_lens) my_free(info->matched_pk_lens);
        if (info->relevance_scores) my_free(info->relevance_scores);
        my_free(info);
    }
}

static float tidesdb_ft_get_relevance(FT_INFO *fts)
{
    tidesdb_ft_info *info = (tidesdb_ft_info *)fts;
    if (!info) return 0.0f;
    /* We return relevance of the current document */
    if (info->relevance_scores && info->current_match > 0 &&
        info->current_match <= info->matched_count)
        return info->relevance_scores[info->current_match - 1];
    return info->relevance;
}

static void tidesdb_ft_reinit_search(FT_INFO *fts)
{
    tidesdb_ft_info *info = (tidesdb_ft_info *)fts;
    if (info) info->current_match = 0;
}

/**
  @brief
  Search for a single word in the fulltext index.

  Returns matching primary keys in the provided arrays.
  Uses prefix seek for efficient lookup.

  @param txn  Transaction to use for the search (must not be NULL)
*/
static int ft_search_word(tidesdb_txn_t *txn, tidesdb_column_family_t *ft_cf, const char *word,
                          size_t word_len, char ***out_pks, size_t **out_pk_lens, uint *out_count,
                          size_t max_matches)
{
    if (word_len == 0) return 0;

    if (!txn) return -1;

    /* We create prefix to search -- word + '\0' */
    char prefix[TIDESDB_FT_PREFIX_BUF_SIZE];
    if (word_len > TIDESDB_FT_PREFIX_BUF_SIZE - 3) word_len = TIDESDB_FT_PREFIX_BUF_SIZE - 3;
    memcpy(prefix, word, word_len);
    prefix[word_len] = '\0';
    size_t prefix_len = word_len + 1;

    *out_pks = (char **)my_malloc(PSI_INSTRUMENT_ME, max_matches * sizeof(char *),
                                  MYF(MY_WME | MY_ZEROFILL));
    *out_pk_lens = (size_t *)my_malloc(PSI_INSTRUMENT_ME, max_matches * sizeof(size_t),
                                       MYF(MY_WME | MY_ZEROFILL));
    *out_count = 0;

    if (!*out_pks || !*out_pk_lens)
    {
        if (*out_pks) my_free(*out_pks);
        if (*out_pk_lens) my_free(*out_pk_lens);
        *out_pks = NULL;
        *out_pk_lens = NULL;
        return -1;
    }

    tidesdb_iter_t *iter = NULL;
    if (tidesdb_iter_new(txn, ft_cf, &iter) != TDB_SUCCESS)
    {
        my_free(*out_pks);
        my_free(*out_pk_lens);
        *out_pks = NULL;
        *out_pk_lens = NULL;
        return -1;
    }

    /* We seek to prefix using block index for O(log n) lookup */
    tidesdb_iter_seek(iter, (uint8_t *)prefix, prefix_len);

    while (tidesdb_iter_valid(iter) && *out_count < max_matches)
    {
        uint8_t *iter_key = NULL;
        size_t iter_key_len = 0;

        if (tidesdb_iter_key(iter, &iter_key, &iter_key_len) != TDB_SUCCESS) break;

        /* We check if key starts with our prefix */
        if (iter_key_len < prefix_len || memcmp(iter_key, prefix, prefix_len) != 0) break;

        /* We extract primary key (after word + '\0') */
        size_t pk_len = iter_key_len - prefix_len;
        if (pk_len > 0)
        {
            char *pk = (char *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
            if (pk)
            {
                memcpy(pk, iter_key + prefix_len, pk_len);
                (*out_pks)[*out_count] = pk;
                (*out_pk_lens)[*out_count] = pk_len;
                (*out_count)++;
            }
        }

        tidesdb_iter_next(iter);
    }

    tidesdb_iter_free(iter);

    return 0;
}

/**
  @brief
  Hash function for PK bytes.
*/
static const uchar *ft_pk_get_key(const void *entry_ptr, size_t *length,
                                  my_bool not_used __attribute__((unused)))
{
    /* Entry format -- [length][pk data] */
    const uchar *entry = (const uchar *)entry_ptr;
    uint32 len = uint4korr(entry);
    *length = len;
    return entry + TIDESDB_FT_HASH_ENTRY_LEN_SIZE;
}

/**
  @brief
  Free function for hash entries.
*/
static void ft_pk_free(void *entry)
{
    my_free((uchar *)entry);
}

/**
  @brief
  Intersect two PK arrays (AND operation) using hash set.

  O(n+m) complexity using hash lookup instead of O(n*m) nested loop.
  Returns a new array containing only PKs present in both inputs.
*/
static void ft_intersect_results(char **pks1, size_t *lens1, uint count1, char **pks2,
                                 size_t *lens2, uint count2, char ***out_pks, size_t **out_lens,
                                 uint *out_count)
{
    size_t max_out = (count1 < count2) ? count1 : count2;
    *out_pks =
        (char **)my_malloc(PSI_INSTRUMENT_ME, max_out * sizeof(char *), MYF(MY_WME | MY_ZEROFILL));
    *out_lens =
        (size_t *)my_malloc(PSI_INSTRUMENT_ME, max_out * sizeof(size_t), MYF(MY_WME | MY_ZEROFILL));
    *out_count = 0;

    if (!*out_pks || !*out_lens) return;

    /* For small sets, we use simple O(n*m) -- hash overhead not worth it */
    if (count1 <= TIDESDB_FT_SMALL_SET_THRESHOLD || count2 <= TIDESDB_FT_SMALL_SET_THRESHOLD)
    {
        for (uint i = 0; i < count1 && *out_count < max_out; i++)
        {
            for (uint j = 0; j < count2; j++)
            {
                if (lens1[i] == lens2[j] && memcmp(pks1[i], pks2[j], lens1[i]) == 0)
                {
                    char *pk = (char *)my_malloc(PSI_INSTRUMENT_ME, lens1[i], MYF(MY_WME));
                    if (pk)
                    {
                        memcpy(pk, pks1[i], lens1[i]);
                        (*out_pks)[*out_count] = pk;
                        (*out_lens)[*out_count] = lens1[i];
                        (*out_count)++;
                    }
                    break;
                }
            }
        }
        return;
    }

    /* We build hash set from smaller array for O(n+m) lookup */
    char **smaller_pks, **larger_pks;
    size_t *smaller_lens, *larger_lens;
    uint smaller_count, larger_count;

    if (count1 <= count2)
    {
        smaller_pks = pks1;
        smaller_lens = lens1;
        smaller_count = count1;
        larger_pks = pks2;
        larger_lens = lens2;
        larger_count = count2;
    }
    else
    {
        smaller_pks = pks2;
        smaller_lens = lens2;
        smaller_count = count2;
        larger_pks = pks1;
        larger_lens = lens1;
        larger_count = count1;
    }

    /* We create hash table from smaller set */
    HASH pk_hash;
    if (my_hash_init(PSI_INSTRUMENT_ME, &pk_hash, &my_charset_bin, smaller_count, 0, 0,
                     ft_pk_get_key, ft_pk_free, HASH_UNIQUE))
    {
        /* Hash init failed -- fall back to O(n*m) */
        for (uint i = 0; i < count1 && *out_count < max_out; i++)
        {
            for (uint j = 0; j < count2; j++)
            {
                if (lens1[i] == lens2[j] && memcmp(pks1[i], pks2[j], lens1[i]) == 0)
                {
                    char *pk = (char *)my_malloc(PSI_INSTRUMENT_ME, lens1[i], MYF(MY_WME));
                    if (pk)
                    {
                        memcpy(pk, pks1[i], lens1[i]);
                        (*out_pks)[*out_count] = pk;
                        (*out_lens)[*out_count] = lens1[i];
                        (*out_count)++;
                    }
                    break;
                }
            }
        }
        return;
    }

    /* We insert smaller set into hash -- format -- [len][pk data] */
    for (uint i = 0; i < smaller_count; i++)
    {
        size_t entry_size = 4 + smaller_lens[i];
        uchar *entry = (uchar *)my_malloc(PSI_INSTRUMENT_ME, entry_size, MYF(MY_WME));
        if (entry)
        {
            int4store(entry, (uint32)smaller_lens[i]);
            memcpy(entry + 4, smaller_pks[i], smaller_lens[i]);
            if (my_hash_insert(&pk_hash, entry)) my_free(entry);
        }
    }

    /* We probe hash with larger set */
    for (uint i = 0; i < larger_count && *out_count < max_out; i++)
    {
        uchar *found = (uchar *)my_hash_search(&pk_hash, (uchar *)larger_pks[i], larger_lens[i]);
        if (found)
        {
            char *pk = (char *)my_malloc(PSI_INSTRUMENT_ME, larger_lens[i], MYF(MY_WME));
            if (pk)
            {
                memcpy(pk, larger_pks[i], larger_lens[i]);
                (*out_pks)[*out_count] = pk;
                (*out_lens)[*out_count] = larger_lens[i];
                (*out_count)++;
            }
        }
    }

    my_hash_free(&pk_hash);
}

/**
  @brief
  Union two PK arrays (OR operation).

  Returns a new array containing PKs from either input (deduplicated).
*/
static void ft_union_results(char **pks1, size_t *lens1, uint count1, char **pks2, size_t *lens2,
                             uint count2, char ***out_pks, size_t **out_lens, uint *out_count)
{
    size_t max_out = count1 + count2;
    *out_pks =
        (char **)my_malloc(PSI_INSTRUMENT_ME, max_out * sizeof(char *), MYF(MY_WME | MY_ZEROFILL));
    *out_lens =
        (size_t *)my_malloc(PSI_INSTRUMENT_ME, max_out * sizeof(size_t), MYF(MY_WME | MY_ZEROFILL));
    *out_count = 0;

    if (!*out_pks || !*out_lens) return;

    /* We use hash table for O(n) deduplication instead of O(n²) nested loop */
    HASH pk_hash;
    if (my_hash_init(PSI_INSTRUMENT_ME, &pk_hash, &my_charset_bin, count1 + count2, 0, 0,
                     ft_pk_get_key, ft_pk_free, HASH_UNIQUE))
    {
        /* Hash init failed -- fall back to O(n²) */
        for (uint i = 0; i < count1; i++)
        {
            char *pk = (char *)my_malloc(PSI_INSTRUMENT_ME, lens1[i], MYF(MY_WME));
            if (pk)
            {
                memcpy(pk, pks1[i], lens1[i]);
                (*out_pks)[*out_count] = pk;
                (*out_lens)[*out_count] = lens1[i];
                (*out_count)++;
            }
        }
        for (uint i = 0; i < count2; i++)
        {
            bool found = false;
            for (uint j = 0; j < count1; j++)
            {
                if (lens2[i] == lens1[j] && memcmp(pks2[i], pks1[j], lens2[i]) == 0)
                {
                    found = true;
                    break;
                }
            }
            if (!found && *out_count < max_out)
            {
                char *pk = (char *)my_malloc(PSI_INSTRUMENT_ME, lens2[i], MYF(MY_WME));
                if (pk)
                {
                    memcpy(pk, pks2[i], lens2[i]);
                    (*out_pks)[*out_count] = pk;
                    (*out_lens)[*out_count] = lens2[i];
                    (*out_count)++;
                }
            }
        }
        return;
    }

    /* We add all from first set to hash and output */
    for (uint i = 0; i < count1; i++)
    {
        /* Hash entry format -- [len][pk data] */
        size_t entry_size = TIDESDB_FT_HASH_ENTRY_LEN_SIZE + lens1[i];
        uchar *entry = (uchar *)my_malloc(PSI_INSTRUMENT_ME, entry_size, MYF(MY_WME));
        if (entry)
        {
            int4store(entry, (uint32)lens1[i]);
            memcpy(entry + TIDESDB_FT_HASH_ENTRY_LEN_SIZE, pks1[i], lens1[i]);
            if (my_hash_insert(&pk_hash, entry))
            {
                my_free(entry);
            }
        }

        /* We add to output */
        char *pk = (char *)my_malloc(PSI_INSTRUMENT_ME, lens1[i], MYF(MY_WME));
        if (pk)
        {
            memcpy(pk, pks1[i], lens1[i]);
            (*out_pks)[*out_count] = pk;
            (*out_lens)[*out_count] = lens1[i];
            (*out_count)++;
        }
    }

    /* We add from second set only if not in hash */
    for (uint i = 0; i < count2; i++)
    {
        uchar *found = (uchar *)my_hash_search(&pk_hash, (uchar *)pks2[i], lens2[i]);
        if (!found && *out_count < max_out)
        {
            /* We add to hash */
            size_t entry_size = TIDESDB_FT_HASH_ENTRY_LEN_SIZE + lens2[i];
            uchar *entry = (uchar *)my_malloc(PSI_INSTRUMENT_ME, entry_size, MYF(MY_WME));
            if (entry)
            {
                int4store(entry, (uint32)lens2[i]);
                memcpy(entry + TIDESDB_FT_HASH_ENTRY_LEN_SIZE, pks2[i], lens2[i]);
                if (my_hash_insert(&pk_hash, entry))
                {
                    my_free(entry);
                }
            }

            /* We add to output */
            char *pk = (char *)my_malloc(PSI_INSTRUMENT_ME, lens2[i], MYF(MY_WME));
            if (pk)
            {
                memcpy(pk, pks2[i], lens2[i]);
                (*out_pks)[*out_count] = pk;
                (*out_lens)[*out_count] = lens2[i];
                (*out_count)++;
            }
        }
    }

    my_hash_free(&pk_hash);
}

/**
  @brief
  Free a PK result set.
*/
static void ft_free_results(char **pks, size_t *lens, uint count)
{
    if (pks)
    {
        for (uint i = 0; i < count; i++)
        {
            if (pks[i]) my_free(pks[i]);
        }
        my_free(pks);
    }
    if (lens) my_free(lens);
}

/**
  @brief
  Initialize full-text search.

  Supports multi-word search:
  -- Natural language mode       -- words are OR'd together
  -- Boolean mode (FT_BOOL flag) -- words are AND'd together
  -- Respects ft_min_word_len and ft_max_word_len
*/
FT_INFO *ha_tidesdb::ft_init_ext(uint flags, uint inx, String *key)
{
    DBUG_ENTER("ha_tidesdb::ft_init_ext");

    /*** We find the fulltext index for this key number */
    uint ft_idx = UINT_MAX;
    for (uint i = 0; i < share->num_ft_indexes; i++)
    {
        if (share->ft_key_nr[i] == inx)
        {
            ft_idx = i;
            break;
        }
    }

    if (ft_idx == UINT_MAX)
    {
        my_error(ER_NOT_SUPPORTED_YET, MYF(0), "FULLTEXT index not found");
        DBUG_RETURN(NULL);
    }

    tidesdb_ft_info *info = (tidesdb_ft_info *)my_malloc(PSI_INSTRUMENT_ME, sizeof(tidesdb_ft_info),
                                                         MYF(MY_WME | MY_ZEROFILL));
    if (!info) DBUG_RETURN(NULL);

    info->please = &tidesdb_ft_vft;
    info->handler = this;
    info->ft_idx = ft_idx;
    info->matched_pks = NULL;
    info->matched_pk_lens = NULL;
    info->matched_count = 0;
    info->current_match = 0;
    info->relevance = TIDESDB_FT_DEFAULT_RELEVANCE;

    /* We tokenize the search query into words */
    const char *query = key->ptr();
    size_t query_len = key->length();

    uint max_words = (uint)tidesdb_ft_max_query_words;
    if (max_words > TIDESDB_FT_MAX_QUERY_WORDS_CAP)
        max_words = TIDESDB_FT_MAX_QUERY_WORDS_CAP; /* Safety cap */

    char(*words)[TIDESDB_FT_WORD_BUF_SIZE] = (char(*)[TIDESDB_FT_WORD_BUF_SIZE])my_malloc(
        PSI_INSTRUMENT_ME, max_words * TIDESDB_FT_WORD_BUF_SIZE, MYF(MY_WME));
    size_t *word_lens =
        (size_t *)my_malloc(PSI_INSTRUMENT_ME, max_words * sizeof(size_t), MYF(MY_WME));
    if (!words || !word_lens)
    {
        if (words) my_free(words);
        if (word_lens) my_free(word_lens);
        my_free(info);
        DBUG_RETURN(NULL);
    }
    uint word_count = 0;

    char word_buf[TIDESDB_FT_WORD_BUF_SIZE];
    size_t word_len = 0;

    for (size_t i = 0; i <= query_len; i++)
    {
        char c = (i < query_len) ? query[i] : ' ';
        bool is_word_char = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                            (c >= '0' && c <= '9') || (c == '_');

        if (is_word_char && word_len < sizeof(word_buf) - 1)
        {
            /* We convert to lowercase */
            word_buf[word_len++] = (c >= 'A' && c <= 'Z') ? (c + TIDESDB_ASCII_CASE_OFFSET) : c;
        }
        else if (word_len > 0)
        {
            /* End of word -- we check length constraints */
            if (word_len >= tidesdb_ft_min_word_len && word_len <= tidesdb_ft_max_word_len &&
                word_count < max_words)
            {
                memcpy(words[word_count], word_buf, word_len);
                words[word_count][word_len] = '\0';
                word_lens[word_count] = word_len;
                word_count++;
            }
            word_len = 0;
        }
    }

    if (word_count == 0)
    {
        my_free(words);
        my_free(word_lens);
        ft_handler = (FT_INFO *)info;
        DBUG_RETURN((FT_INFO *)info);
    }

    tidesdb_column_family_t *ft_cf = share->ft_cf[ft_idx];
    size_t max_matches = TIDESDB_FT_MAX_MATCHES;

    /* Boolean mode uses AND, natural language uses OR */
    bool use_and = (flags & FT_BOOL) != 0;

    if (!current_txn)
    {
        my_error(ER_NOT_SUPPORTED_YET, MYF(0), "FULLTEXT search without transaction");
        my_free(words);
        my_free(word_lens);
        tidesdb_ft_close_search((FT_INFO *)info);
        DBUG_RETURN(NULL);
    }

    /* We search for first word */
    char **result_pks = NULL;
    size_t *result_lens = NULL;
    uint result_count = 0;

    if (ft_search_word(current_txn, ft_cf, words[0], word_lens[0], &result_pks, &result_lens,
                       &result_count, max_matches) != 0)
    {
        my_free(words);
        my_free(word_lens);
        tidesdb_ft_close_search((FT_INFO *)info);
        DBUG_RETURN(NULL);
    }

    /* We process remaining words */
    for (uint w = 1; w < word_count && result_count > 0; w++)
    {
        char **word_pks = NULL;
        size_t *word_lens_arr = NULL;
        uint word_pk_count = 0;

        if (ft_search_word(current_txn, ft_cf, words[w], word_lens[w], &word_pks, &word_lens_arr,
                           &word_pk_count, max_matches) != 0)
        {
            my_free(words);
            my_free(word_lens);
            ft_free_results(result_pks, result_lens, result_count);
            tidesdb_ft_close_search((FT_INFO *)info);
            DBUG_RETURN(NULL);
        }

        /* We combine results */
        char **new_pks = NULL;
        size_t *new_lens = NULL;
        uint new_count = 0;

        if (use_and)
        {
            ft_intersect_results(result_pks, result_lens, result_count, word_pks, word_lens_arr,
                                 word_pk_count, &new_pks, &new_lens, &new_count);
        }
        else
        {
            ft_union_results(result_pks, result_lens, result_count, word_pks, word_lens_arr,
                             word_pk_count, &new_pks, &new_lens, &new_count);
        }

        ft_free_results(result_pks, result_lens, result_count);
        ft_free_results(word_pks, word_lens_arr, word_pk_count);

        result_pks = new_pks;
        result_lens = new_lens;
        result_count = new_count;
    }

    /* We store results in info structure */
    info->matched_pks = result_pks;
    info->matched_pk_lens = result_lens;
    info->matched_count = result_count;

    /*
      Compute per-document TF-IDF relevance scores.

      For each document we accumulate:
        score += TF(word, doc) * IDF(word)
      where:
        TF  = 1 (binary: word present in document)
        IDF = log(1 + total_documents / documents_containing_word)

      We approximate total_documents from cached stats.
    */
    info->relevance_scores = NULL;
    if (result_count > 0)
    {
        info->relevance_scores = (float *)my_malloc(PSI_INSTRUMENT_ME, result_count * sizeof(float),
                                                    MYF(MY_WME | MY_ZEROFILL));

        /* We get approximate total document count for IDF denominator */
        ha_rows total_docs = TIDESDB_FT_FALLBACK_TOTAL_DOCS;
        tidesdb_stats_t *tdb_stats = get_realtime_stats(share);
        if (tdb_stats && tdb_stats->total_keys > 0)
        {
            total_docs = tdb_stats->total_keys;
            tidesdb_free_stats(tdb_stats);
        }
        else if (tdb_stats)
        {
            tidesdb_free_stats(tdb_stats);
        }

        if (info->relevance_scores)
        {
            /* For each query word, find which result docs contain it and add IDF */
            for (uint w = 0; w < word_count; w++)
            {
                char **word_pks = NULL;
                size_t *word_pk_lens = NULL;
                uint word_pk_count = 0;

                if (ft_search_word(current_txn, ft_cf, words[w], word_lens[w], &word_pks,
                                   &word_pk_lens, &word_pk_count, max_matches) == 0 &&
                    word_pk_count > 0)
                {
                    /* IDF = log(1 + N / df) where df = documents containing this word */
                    float idf = logf(1.0f + (float)total_docs / (float)word_pk_count);

                    /* For each result document, check if this word matches */
                    for (uint d = 0; d < result_count; d++)
                    {
                        for (uint wp = 0; wp < word_pk_count; wp++)
                        {
                            if (result_lens[d] == word_pk_lens[wp] &&
                                memcmp(result_pks[d], word_pks[wp], result_lens[d]) == 0)
                            {
                                info->relevance_scores[d] += idf;
                                break;
                            }
                        }
                    }

                    ft_free_results(word_pks, word_pk_lens, word_pk_count);
                }
            }

            /* We sort results by relevance descending (simple selection sort for stability) */
            for (uint i = 0; i < result_count; i++)
            {
                uint best = i;
                for (uint j = i + 1; j < result_count; j++)
                {
                    if (info->relevance_scores[j] > info->relevance_scores[best]) best = j;
                }
                if (best != i)
                {
                    /* Swap PKs, lens, and scores */
                    char *tmp_pk = info->matched_pks[i];
                    info->matched_pks[i] = info->matched_pks[best];
                    info->matched_pks[best] = tmp_pk;

                    size_t tmp_len = info->matched_pk_lens[i];
                    info->matched_pk_lens[i] = info->matched_pk_lens[best];
                    info->matched_pk_lens[best] = tmp_len;

                    float tmp_score = info->relevance_scores[i];
                    info->relevance_scores[i] = info->relevance_scores[best];
                    info->relevance_scores[best] = tmp_score;
                }
            }
        }

        info->relevance = info->relevance_scores ? info->relevance_scores[0] : (float)word_count;
    }

    my_free(words);
    my_free(word_lens);

    ft_handler = (FT_INFO *)info;
    ft_current_idx = ft_idx;
    ft_matched_pks = info->matched_pks;
    ft_matched_pk_lens = info->matched_pk_lens;
    ft_matched_count = info->matched_count;
    ft_current_match = 0;

    DBUG_RETURN((FT_INFO *)info);
}

/**
  @brief
  Read next full-text search result.

  Fetches the next matching row by primary key.
*/
int ha_tidesdb::ft_read(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::ft_read");

    if (!ft_handler || ft_current_match >= ft_matched_count) DBUG_RETURN(HA_ERR_END_OF_FILE);

    /* We get the next matched primary key */
    char *pk = ft_matched_pks[ft_current_match];
    size_t pk_len = ft_matched_pk_lens[ft_current_match];
    ft_current_match++;

    if (!current_txn)
    {
        sql_print_error("TidesDB: No transaction available for ft_read");
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    uint8_t *value = NULL;
    size_t value_len = 0;
    int ret = tidesdb_txn_get(current_txn, share->cf, (uint8_t *)pk, pk_len, &value, &value_len);

    if (ret != TDB_SUCCESS)
    {
        DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }

    ret = unpack_row(buf, value, value_len);
    tidesdb_free(value);

    if (ret) DBUG_RETURN(ret);

    if (pk_len > current_key_capacity)
    {
        size_t new_capacity = pk_len > TIDESDB_INITIAL_KEY_BUF_CAPACITY
                                  ? pk_len * 2
                                  : TIDESDB_INITIAL_KEY_BUF_CAPACITY;
        uchar *new_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, new_capacity, MYF(MY_WME));
        if (!new_key) DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        if (current_key) my_free(current_key);
        current_key = new_key;
        current_key_capacity = new_capacity;
    }
    memcpy(current_key, pk, pk_len);
    current_key_len = pk_len;

    DBUG_RETURN(0);
}

/**
  @brief
  Get auto-increment value for INSERT.
*/
void ha_tidesdb::get_auto_increment(ulonglong offset, ulonglong increment,
                                    ulonglong nb_desired_values, ulonglong *first_value,
                                    ulonglong *nb_reserved_values)
{
    DBUG_ENTER("ha_tidesdb::get_auto_increment");

    if (!my_atomic_load32_explicit((volatile int32 *)&share->auto_inc_loaded,
                                   MY_MEMORY_ORDER_ACQUIRE))
    {
        pthread_mutex_lock(&share->auto_inc_mutex);
        if (!share->auto_inc_loaded)
        {
            load_auto_increment_value();
            if (share->auto_increment_value == 0) share->auto_increment_value = 1;
            my_atomic_store32_explicit((volatile int32 *)&share->auto_inc_loaded, 1,
                                       MY_MEMORY_ORDER_RELEASE);
        }
        pthread_mutex_unlock(&share->auto_inc_mutex);
    }

    ulonglong reserve_amount = nb_desired_values * increment;
    ulonglong old_val = my_atomic_add64_explicit((volatile int64 *)&share->auto_increment_value,
                                                 reserve_amount, MY_MEMORY_ORDER_RELAXED);

    *first_value = old_val;
    *nb_reserved_values = nb_desired_values;

    /* We batch persist every TIDESDB_AUTO_INC_PERSIST_INTERVAL values to reduce I/O overhead */
    ulonglong new_val = old_val + reserve_amount;
    if ((new_val / TIDESDB_AUTO_INC_PERSIST_INTERVAL) >
        (old_val / TIDESDB_AUTO_INC_PERSIST_INTERVAL))
    {
        persist_auto_increment_value(new_val);
    }

    DBUG_VOID_RETURN;
}

/**
  @brief
  Reset auto-increment value.
*/
int ha_tidesdb::reset_auto_increment(ulonglong value)
{
    DBUG_ENTER("ha_tidesdb::reset_auto_increment");
    my_atomic_store64_explicit((volatile int64 *)&share->auto_increment_value, value,
                               MY_MEMORY_ORDER_RELEASE);
    persist_auto_increment_value(value);

    DBUG_RETURN(0);
}

/**
  @brief
  Start bulk insert operation.

  Optimizes for large inserts by batching operations in a single transaction.
  If we're in a multi-statement transaction, use that transaction instead of
  creating a new one (to support savepoints and proper rollback).
*/
void ha_tidesdb::start_bulk_insert(ha_rows rows, uint flags)
{
    DBUG_ENTER("ha_tidesdb::start_bulk_insert");
    DBUG_PRINT("info", ("start_bulk_insert: rows %lu", (ulong)rows));

    bulk_insert_rows = rows;
    bulk_insert_count = 0;

    skip_dup_check = true;

    /* We check if we're in a multi-statement transaction */
    THD *thd = ha_thd();
    tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);

    if (thd_txn)
    {
        bulk_txn = thd_txn;
        bulk_insert_active = true;
        DBUG_VOID_RETURN;
    }

    /* Not in a multi-statement transaction, we create our own */
    bulk_insert_active = true;

    if (!bulk_txn)
    {
        int ret = tidesdb_txn_begin(tidesdb_instance, &bulk_txn);
        if (ret != TDB_SUCCESS)
        {
            sql_print_warning("TidesDB: Failed to begin bulk insert transaction");
            bulk_txn = NULL;
            bulk_insert_active = false;
        }
    }

    DBUG_VOID_RETURN;
}

/**
  @brief
  End bulk insert operation.

  Commits the batched transaction (unless it's a THD-level transaction
  which is managed by the transaction coordinator).
*/
int ha_tidesdb::end_bulk_insert()
{
    DBUG_ENTER("ha_tidesdb::end_bulk_insert");

    int ret = 0;

    if (bulk_txn)
    {
        /* We check if this is a THD-level transaction -- don't commit it here */
        THD *thd = ha_thd();
        tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);

        if (bulk_txn == thd_txn)
        {
            /* THD transaction -- we don't commit, just clear our reference */
            bulk_txn = NULL;
        }
        else
        {
            /* Our own transaction -- commit it */
            ret = tidesdb_txn_commit(bulk_txn);
            if (ret != TDB_SUCCESS)
            {
                sql_print_error("TidesDB: Failed to commit bulk insert: %d", ret);
                tidesdb_txn_rollback(bulk_txn);
                ret = HA_ERR_GENERIC;
            }
            tidesdb_txn_free(bulk_txn);
            bulk_txn = NULL;
        }
    }

    bulk_insert_active = false;
    bulk_insert_rows = 0;

    skip_dup_check = false;

    /* We invalidate row count cache since we inserted rows */
    share->row_count_valid = false;

    DBUG_RETURN(ret);
}

/**
  @brief
  Disable secondary indexes for bulk load optimization.

  When loading large amounts of data, maintaining secondary indexes
  row-by-row is expensive. Disabling indexes allows the bulk load
  to skip secondary index maintenance entirely. The indexes are
  rebuilt in a single sorted pass when enable_indexes() is called.

  This is the same pattern used by MyISAM for LOAD DATA INFILE
  and ALTER TABLE ... DISABLE KEYS.
*/
int ha_tidesdb::disable_indexes(key_map map, bool persist)
{
    DBUG_ENTER("ha_tidesdb::disable_indexes");

    if (!share || !share->cf)
    {
        DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
    }

    indexes_disabled = true;

    sql_print_information("TidesDB: Secondary indexes disabled for bulk load on '%s'",
                          share->table_name);

    DBUG_RETURN(0);
}

/**
  @brief
  Re-enable and rebuild secondary indexes after bulk load.

  Scans all rows in the primary CF and rebuilds each secondary
  index in batch. This is much faster than maintaining indexes
  row-by-row during bulk insert because:
  1. No per-row transaction overhead for index updates
  2. Sequential scan of primary data is cache-friendly
  3. Index entries are written in batches
*/
int ha_tidesdb::enable_indexes(key_map map, bool persist)
{
    DBUG_ENTER("ha_tidesdb::enable_indexes");

    if (!indexes_disabled)
    {
        DBUG_RETURN(0);
    }

    if (!share || !share->cf)
    {
        DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
    }

    int error = 0;

    /* We rebuild each secondary index by scanning primary data */
    for (uint i = 0; i < table->s->keys; i++)
    {
        if (i == table->s->primary_key) continue;

        KEY *key = &table->key_info[i];

        /* We skip fulltext and spatial -- they have their own CFs */
        if (key->algorithm == HA_KEY_ALG_FULLTEXT) continue;
        if (key->algorithm == HA_KEY_ALG_RTREE) continue;

        error = rebuild_secondary_index(key, key->name.str, table);
        if (error)
        {
            sql_print_error("TidesDB: Failed to rebuild index '%s': %d", key->name.str, error);
            DBUG_RETURN(error);
        }

        sql_print_information("TidesDB: Rebuilt secondary index '%s'", key->name.str);
    }

    indexes_disabled = false;

    sql_print_information("TidesDB: All secondary indexes re-enabled for '%s'", share->table_name);

    DBUG_RETURN(0);
}

/**
  @brief
  Check if indexes are currently disabled.

  @return 0 if indexes are enabled, 1 if disabled
*/
int ha_tidesdb::indexes_are_disabled(void)
{
    DBUG_ENTER("ha_tidesdb::indexes_are_disabled");
    DBUG_RETURN(indexes_disabled ? 1 : 0);
}

/**
  @brief
  Return an upper bound estimate of the number of rows in the table.

  Uses SSTable metadata (total_keys from tidesdb_get_stats) to provide
  a tighter estimate than the default (stats.records + EXTRA_RECORDS).
  This helps the optimizer make better join order decisions on large tables.
*/
ha_rows ha_tidesdb::estimate_rows_upper_bound()
{
    DBUG_ENTER("ha_tidesdb::estimate_rows_upper_bound");

    if (share && share->cf)
    {
        tidesdb_stats_t *tdb_stats = get_realtime_stats(share);
        if (tdb_stats)
        {
            ha_rows upper = tdb_stats->total_keys;
            tidesdb_free_stats(tdb_stats);

            /* We add small margin for concurrent inserts */
            if (upper < HA_POS_ERROR - TIDESDB_ROW_ESTIMATE_MARGIN)
                upper += TIDESDB_ROW_ESTIMATE_MARGIN;

            DBUG_RETURN(upper);
        }
    }

    /* Fallback to default behavior */
    DBUG_RETURN(stats.records + EXTRA_RECORDS);
}

/**
  @brief
  Register this table with the query cache.

  For transactional engines, the query cache needs to know when
  cached results are invalidated. TidesDB uses MVCC, so we allow
  caching only when no uncommitted changes exist for this table.

  This follows the same pattern as InnoDB's implementation.
*/
my_bool ha_tidesdb::register_query_cache_table(THD *thd, const char *table_key, uint key_length,
                                               qc_engine_callback *callback, ulonglong *engine_data)
{
    DBUG_ENTER("ha_tidesdb::register_query_cache_table");

    /*
      Allow query cache for this table. The callback is set to NULL
      which means the default invalidation mechanism is used --
      any write to the table invalidates all cached queries for it.
    */
    *callback = NULL;
    *engine_data = 0;

    /*
      If there's an active transaction with pending writes,
      don't cache -- the results may not reflect committed state.
    */
    tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);
    if (txn)
    {
        /* Active transaction -- don't cache to avoid stale results */
        DBUG_RETURN(FALSE);
    }

    DBUG_RETURN(TRUE);
}

/**
  @brief
  Check table for errors.
*/
int ha_tidesdb::check(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::check");

    if (!share || !share->cf)
    {
        DBUG_RETURN(HA_ADMIN_CORRUPT);
    }

    /** TidesDB has built-in checksums and corruption detection */
    /* A simple scan will verify data integrity */

    tidesdb_txn_t *txn = NULL;
    int ret = tidesdb_txn_begin(tidesdb_instance, &txn);
    if (ret != TDB_SUCCESS)
    {
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    tidesdb_iter_t *iter = NULL;
    ret = tidesdb_iter_new(txn, share->cf, &iter);
    if (ret != TDB_SUCCESS)
    {
        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    ha_rows count = 0;
    tidesdb_iter_seek_to_first(iter);

    while (tidesdb_iter_valid(iter))
    {
        count++;
        tidesdb_iter_next(iter);
    }

    tidesdb_iter_free(iter);
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);

    share->row_count = count;
    share->row_count_valid = true;

    sql_print_information("TidesDB: Table check completed, %lu rows verified", (ulong)count);

    DBUG_RETURN(HA_ADMIN_OK);
}

/**
  @brief
  Preload keys into the block cache.

  LOAD INDEX INTO CACHE triggers this method to warm up the cache
  by scanning all index data. For TidesDB, this scans the primary
  column family and all secondary indexes, populating the block cache.

  TidesDB's clock cache will retain frequently accessed blocks,
  improving subsequent read performance.
*/
int ha_tidesdb::preload_keys(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::preload_keys");

    if (!share || !share->cf)
    {
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    /*
      We can through the primary column family to populate block cache.
      TidesDB's iterator will read blocks into cache as it traverses.
      We need a transaction for the iterator.
    */
    tidesdb_txn_t *txn = NULL;
    int ret = tidesdb_txn_begin(tidesdb_instance, &txn);
    if (ret != TDB_SUCCESS)
    {
        sql_print_warning("TidesDB: Failed to begin transaction for preload: %d", ret);
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    tidesdb_iter_t *iter = NULL;
    ret = tidesdb_iter_new(txn, share->cf, &iter);
    if (ret != TDB_SUCCESS || !iter)
    {
        sql_print_warning("TidesDB: Failed to create iterator for preload: %d", ret);
        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    tidesdb_iter_seek_to_first(iter);

    ha_rows rows_scanned = 0;
    while (tidesdb_iter_valid(iter))
    {
        /* We just iterate to populate cache -- we don't need to process data */
        tidesdb_iter_next(iter);
        rows_scanned++;

        /* We check for user interrupt */
        if (thd_killed(thd))
        {
            tidesdb_iter_free(iter);
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
            DBUG_RETURN(HA_ADMIN_FAILED);
        }
    }

    tidesdb_iter_free(iter);

    /* We also preload secondary indexes */
    for (uint i = 0; i < table->s->keys; i++)
    {
        if (share->index_cf[i])
        {
            ret = tidesdb_iter_new(txn, share->index_cf[i], &iter);
            if (ret == TDB_SUCCESS && iter)
            {
                tidesdb_iter_seek_to_first(iter);
                while (tidesdb_iter_valid(iter))
                {
                    tidesdb_iter_next(iter);
                    if (thd_killed(thd))
                    {
                        tidesdb_iter_free(iter);
                        tidesdb_txn_rollback(txn);
                        tidesdb_txn_free(txn);
                        DBUG_RETURN(HA_ADMIN_FAILED);
                    }
                }
                tidesdb_iter_free(iter);
            }
        }
    }

    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);

    sql_print_information("TidesDB: Preloaded %llu rows into cache", (ulonglong)rows_scanned);

    DBUG_RETURN(HA_ADMIN_OK);
}

/**
  @brief
  Repair table -- triggers compaction to clean up any issues.
*/
int ha_tidesdb::repair(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::repair");

    if (!share || !share->cf)
    {
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    int ret = tidesdb_flush_memtable(share->cf);
    if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
    {
        sql_print_warning("TidesDB: Memtable flush returned: %d", ret);
    }

    /* Wait for async flush to complete before triggering compaction */
    int wait_count = 0;
    while (tidesdb_is_flushing(share->cf) && wait_count < TIDESDB_FLUSH_WAIT_MAX_ITERATIONS)
    {
        my_sleep(TIDESDB_FLUSH_WAIT_SLEEP_US);
        wait_count++;
    }

    ret = tidesdb_compact(share->cf);
    if (ret != TDB_SUCCESS)
    {
        sql_print_warning("TidesDB: Repair (compaction) returned: %d (non-fatal)", ret);
    }

    /* Wait for compaction to complete before returning */
    wait_count = 0;
    while (tidesdb_is_compacting(share->cf) && wait_count < TIDESDB_FLUSH_WAIT_MAX_ITERATIONS)
    {
        my_sleep(TIDESDB_FLUSH_WAIT_SLEEP_US);
        wait_count++;
    }

    sql_print_information("TidesDB: Table repair completed");

    DBUG_RETURN(HA_ADMIN_OK);
}

/**
  @brief
  Discard or import tablespace.

  For TidesDB, this allows exporting/importing column family data.
  Similar to InnoDB's approach:

  DISCARD (discard=TRUE):
    1. Flush memtable to ensure all data is on disk
    2. Drop the column family (closes handles, releases files)
    3. Mark table as discarded
    4. User can now copy/replace the CF directory files

  IMPORT (discard=FALSE):
    1. Verify table was previously discarded
    2. Recreate column family (picks up new files)
    3. Clear discarded flag
*/
int ha_tidesdb::discard_or_import_tablespace(my_bool discard)
{
    DBUG_ENTER("ha_tidesdb::discard_or_import_tablespace");

    if (!share)
    {
        DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
    }

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    get_cf_name(share->table_name, cf_name, sizeof(cf_name));

    if (discard)
    {
        /* Already discarded? */
        if (share->tablespace_discarded)
        {
            sql_print_warning("TidesDB: Tablespace already discarded for %s", cf_name);
            DBUG_RETURN(0);
        }

        if (share->cf)
        {
            sql_print_information("TidesDB: Flushing memtable for %s", cf_name);
            int ret = tidesdb_flush_memtable(share->cf);
            if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
            {
                sql_print_warning("TidesDB: Flush returned: %d", ret);
            }

            /* We wait for async flush to complete before dropping CF */
            int wait_count = 0;
            while (tidesdb_is_flushing(share->cf) && wait_count < TIDESDB_FLUSH_WAIT_MAX_ITERATIONS)
            {
                my_sleep(TIDESDB_FLUSH_WAIT_SLEEP_US);
                wait_count++;
            }

            /* Wait for any in-progress compaction to complete */
            wait_count = 0;
            while (tidesdb_is_compacting(share->cf) &&
                   wait_count < TIDESDB_FLUSH_WAIT_MAX_ITERATIONS)
            {
                my_sleep(TIDESDB_FLUSH_WAIT_SLEEP_US);
                wait_count++;
            }
        }

        sql_print_information("TidesDB: Dropping column family %s for DISCARD", cf_name);
        int ret = tidesdb_drop_column_family(tidesdb_instance, cf_name);
        if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
        {
            sql_print_error("TidesDB: Failed to drop column family for DISCARD: %d", ret);
            DBUG_RETURN(HA_ERR_GENERIC);
        }

        for (uint i = 0; i < table->s->keys; i++)
        {
            char idx_cf_name[TIDESDB_CF_NAME_BUF_SIZE];
            snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_FMT, cf_name, i);
            tidesdb_drop_column_family(tidesdb_instance, idx_cf_name);
        }

        for (uint i = 0; i < share->num_ft_indexes; i++)
        {
            char ft_cf_name[TIDESDB_CF_NAME_BUF_SIZE];
            snprintf(ft_cf_name, sizeof(ft_cf_name), TIDESDB_CF_FT_FMT, cf_name,
                     share->ft_key_nr[i]);
            tidesdb_drop_column_family(tidesdb_instance, ft_cf_name);
        }

        for (uint i = 0; i < share->num_spatial_indexes; i++)
        {
            char spatial_cf_name[TIDESDB_CF_NAME_BUF_SIZE];
            snprintf(spatial_cf_name, sizeof(spatial_cf_name), TIDESDB_CF_SPATIAL_FMT, cf_name,
                     share->spatial_key_nr[i]);
            tidesdb_drop_column_family(tidesdb_instance, spatial_cf_name);
        }

        share->cf = NULL;
        share->tablespace_discarded = true;

        sql_print_information(
            "TidesDB: Tablespace discarded for %s - "
            "copy new data files to CF directory, then run IMPORT TABLESPACE",
            cf_name);
    }
    else
    {
        if (!share->tablespace_discarded)
        {
            sql_print_error(
                "TidesDB: Cannot import - tablespace not discarded. "
                "Run ALTER TABLE ... DISCARD TABLESPACE first.");
            DBUG_RETURN(HA_ERR_TABLESPACE_EXISTS);
        }

        sql_print_information("TidesDB: Importing tablespace for %s", cf_name);

        tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
        cf_config.write_buffer_size = tidesdb_write_buffer_size;
        cf_config.enable_bloom_filter = tidesdb_enable_bloom_filter ? 1 : 0;
        cf_config.bloom_fpr = tidesdb_bloom_fpr;
        cf_config.level_size_ratio = tidesdb_level_size_ratio;
        cf_config.skip_list_max_level = tidesdb_skip_list_max_level;
        cf_config.enable_block_indexes = tidesdb_enable_block_indexes ? 1 : 0;
        cf_config.sync_mode = tidesdb_sync_mode;
        cf_config.use_btree = tidesdb_use_btree ? 1 : 0;

        if (tidesdb_enable_compression)
            cf_config.compression_algorithm = (compression_algorithm)tidesdb_compression_algo;
        else
            cf_config.compression_algorithm = TDB_COMPRESS_NONE;

        int ret = tidesdb_create_column_family(tidesdb_instance, cf_name, &cf_config);
        if (ret != TDB_SUCCESS && ret != TDB_ERR_EXISTS)
        {
            sql_print_error("TidesDB: Failed to create column family for IMPORT: %d", ret);
            DBUG_RETURN(HA_ERR_TABLESPACE_MISSING);
        }

        share->cf = tidesdb_get_column_family(tidesdb_instance, cf_name);
        if (!share->cf)
        {
            sql_print_error("TidesDB: Failed to get column family after IMPORT");
            DBUG_RETURN(HA_ERR_TABLESPACE_MISSING);
        }

        for (uint i = 0; i < table->s->keys; i++)
        {
            if (i == table->s->primary_key) continue;

            char idx_cf_name[TIDESDB_CF_NAME_BUF_SIZE];
            snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_FMT, cf_name, i);
            tidesdb_create_column_family(tidesdb_instance, idx_cf_name, &cf_config);

            share->index_cf[i] = tidesdb_get_column_family(tidesdb_instance, idx_cf_name);
        }

        share->tablespace_discarded = false;

        sql_print_information("TidesDB: Tablespace imported successfully for %s", cf_name);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Backup the TidesDB database using native tidesdb_backup API.

  Creates an on-disk snapshot of the database without blocking normal
  reads/writes. The backup is consistent and can be opened as a new
  TidesDB instance.

  The backup is created at /tmp/tidesdb_backup_YYYYMMDD_HHMMSS
*/
int ha_tidesdb::backup(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::backup");

    if (!tidesdb_instance)
    {
        sql_print_error("TidesDB: Cannot backup - database not initialized");
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    /* We generate backup directory name with timestamp */
    char backup_dir[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    time_t now = time(NULL);
    struct tm tm_buf;
    struct tm *tm_info;
#ifdef _WIN32
    /* Windows uses localtime_s with reversed parameter order */
    localtime_s(&tm_buf, &now);
    tm_info = &tm_buf;
#else
    /* POSIX uses localtime_r (thread-safe) */
    tm_info = localtime_r(&now, &tm_buf);
#endif
    char timestamp[TIDESDB_TIMESTAMP_BUF_SIZE];
    strftime(timestamp, sizeof(timestamp), "%Y%m%d_%H%M%S", tm_info);

    /*
      We use platform-appropriate temp directory:
      -- Windows    -- uses TEMP/TMP environment variable or current directory
      -- Unix/Linux -- uses /tmp
    */
#ifdef _WIN32
    const char *tmp_dir = getenv("TEMP");
    if (!tmp_dir) tmp_dir = getenv("TMP");
    if (!tmp_dir) tmp_dir = ".";
    snprintf(backup_dir, sizeof(backup_dir), "%s" TIDESDB_PATH_SEP_STR "tidesdb_backup_%s", tmp_dir,
             timestamp);
#else
    snprintf(backup_dir, sizeof(backup_dir), "/tmp/tidesdb_backup_%s", timestamp);
#endif

    sql_print_information("TidesDB: Starting backup to '%s' using tidesdb_backup API", backup_dir);

    int ret = tidesdb_backup(tidesdb_instance, backup_dir);
    if (ret != TDB_SUCCESS)
    {
        sql_print_error("TidesDB: Backup failed with error: %d", ret);
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    sql_print_information("TidesDB: Backup completed successfully to '%s'", backup_dir);

    DBUG_RETURN(HA_ADMIN_OK);
}

/**
  @brief
  Check and repair table if needed.
*/
bool ha_tidesdb::check_and_repair(THD *thd)
{
    DBUG_ENTER("ha_tidesdb::check_and_repair");

    HA_CHECK_OPT check_opt;
    check_opt.init();

    if (check(thd, &check_opt) == HA_ADMIN_CORRUPT)
    {
        repair(thd, &check_opt);
        DBUG_RETURN(TRUE);
    }

    DBUG_RETURN(FALSE);
}

/**
  @brief
  Check if table is crashed.

  TidesDB has built-in corruption detection, so this is rarely true.
*/
bool ha_tidesdb::is_crashed() const
{
    DBUG_ENTER("ha_tidesdb::is_crashed");
    /* TidesDB handles corruption internally */
    DBUG_RETURN(FALSE);
}

/**
  @brief
  Get foreign key creation info for SHOW CREATE TABLE.

  Returns the foreign key constraints as a string that can be
  appended to the CREATE TABLE statement.
*/
char *ha_tidesdb::get_foreign_key_create_info()
{
    DBUG_ENTER("ha_tidesdb::get_foreign_key_create_info");

    /*
      Foreign key metadata is stored in MySQL's/Maria's data dictionary (.frm files).
      TidesDB enforces FK constraints at the storage engine level during
      write operations. This method returns NULL to indicate that FK info
      should be retrieved from the data dictionary.
    */
    DBUG_RETURN(NULL);
}

/**
  @brief
  Get list of foreign keys for this table.

  Populates f_key_list with FOREIGN_KEY_INFO structures describing
  each foreign key constraint on this table.
*/
int ha_tidesdb::get_foreign_key_list(THD *thd, List<FOREIGN_KEY_INFO> *f_key_list)
{
    DBUG_ENTER("ha_tidesdb::get_foreign_key_list");

    if (!share || !table_share) DBUG_RETURN(0);

    /*
      We populate the list with FK constraints where this table is the child
      (i.e., this table has FOREIGN KEY columns referencing another table).
    */
    for (uint i = 0; i < share->num_fk; i++)
    {
        FOREIGN_KEY_INFO *fk_info = (FOREIGN_KEY_INFO *)thd->alloc(sizeof(FOREIGN_KEY_INFO));
        if (!fk_info) DBUG_RETURN(HA_ERR_OUT_OF_MEM);

        /* We use value initialization instead of memset for C++ struct */
        *fk_info = FOREIGN_KEY_INFO();

        /* FK constraint name */
        fk_info->foreign_id =
            thd_make_lex_string(thd, NULL, share->fk[i].fk_name, strlen(share->fk[i].fk_name), 1);

        /* Child table (this table) */
        fk_info->foreign_db =
            thd_make_lex_string(thd, NULL, table_share->db.str, table_share->db.length, 1);
        fk_info->foreign_table = thd_make_lex_string(thd, NULL, table_share->table_name.str,
                                                     table_share->table_name.length, 1);

        /* Parent table (referenced table) */
        fk_info->referenced_db =
            thd_make_lex_string(thd, NULL, share->fk[i].ref_db, strlen(share->fk[i].ref_db), 1);
        fk_info->referenced_table = thd_make_lex_string(thd, NULL, share->fk[i].ref_table,
                                                        strlen(share->fk[i].ref_table), 1);

        /* Referential actions */
        switch (share->fk[i].delete_rule)
        {
            case TIDESDB_FK_RULE_CASCADE:
                fk_info->delete_method = FK_OPTION_CASCADE;
                break;
            case TIDESDB_FK_RULE_SET_NULL:
                fk_info->delete_method = FK_OPTION_SET_NULL;
                break;
            case TIDESDB_FK_RULE_RESTRICT:
            default:
                fk_info->delete_method = FK_OPTION_RESTRICT;
                break;
        }

        switch (share->fk[i].update_rule)
        {
            case TIDESDB_FK_RULE_CASCADE:
                fk_info->update_method = FK_OPTION_CASCADE;
                break;
            case TIDESDB_FK_RULE_SET_NULL:
                fk_info->update_method = FK_OPTION_SET_NULL;
                break;
            case TIDESDB_FK_RULE_RESTRICT:
            default:
                fk_info->update_method = FK_OPTION_RESTRICT;
                break;
        }

        f_key_list->push_back(fk_info, thd->mem_root);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Get list of foreign keys where this table is the parent (referenced table).

  This is used during ALTER TABLE to properly handle self-referencing FKs
  and to check for FK constraints that would be affected by the ALTER.

  @param thd        Thread handle
  @param f_key_list Output list of FOREIGN_KEY_INFO structures

  @return 0 on success
*/
int ha_tidesdb::get_parent_foreign_key_list(THD *thd, List<FOREIGN_KEY_INFO> *f_key_list)
{
    DBUG_ENTER("ha_tidesdb::get_parent_foreign_key_list");

    if (!share) DBUG_RETURN(0);

    /*
      We populate the list with FK constraints where this table is the parent
      (i.e., other tables have FOREIGN KEY columns referencing this table).
      This information is stored in share->referencing_tables[].
    */
    for (uint i = 0; i < share->num_referencing; i++)
    {
        FOREIGN_KEY_INFO *fk_info = (FOREIGN_KEY_INFO *)thd->alloc(sizeof(FOREIGN_KEY_INFO));
        if (!fk_info) DBUG_RETURN(HA_ERR_OUT_OF_MEM);

        /* We use value initialization instead of memset for C++ struct */
        *fk_info = FOREIGN_KEY_INFO();

        /*
          We parse the referencing table name (stored as "db.table" format).
          We need to split it into db and table components.
        */
        const char *ref_table_full = share->referencing_tables[i];
        const char *dot = strchr(ref_table_full, '.');
        if (dot)
        {
            size_t db_len = dot - ref_table_full;
            fk_info->foreign_db = thd_make_lex_string(thd, NULL, ref_table_full, db_len, 1);
            fk_info->foreign_table = thd_make_lex_string(thd, NULL, dot + 1, strlen(dot + 1), 1);
        }
        else
        {
            /* No dot found, we assume current database */
            fk_info->foreign_db =
                thd_make_lex_string(thd, NULL, table_share->db.str, table_share->db.length, 1);
            fk_info->foreign_table =
                thd_make_lex_string(thd, NULL, ref_table_full, strlen(ref_table_full), 1);
        }

        /* This table is the parent (referenced table) */
        fk_info->referenced_db =
            thd_make_lex_string(thd, NULL, table_share->db.str, table_share->db.length, 1);
        fk_info->referenced_table = thd_make_lex_string(thd, NULL, table_share->table_name.str,
                                                        table_share->table_name.length, 1);

        /* Referential action from the child's perspective */
        switch (share->referencing_fk_rules[i])
        {
            case TIDESDB_FK_RULE_CASCADE:
                fk_info->delete_method = FK_OPTION_CASCADE;
                break;
            case TIDESDB_FK_RULE_SET_NULL:
                fk_info->delete_method = FK_OPTION_SET_NULL;
                break;
            case TIDESDB_FK_RULE_RESTRICT:
            default:
                fk_info->delete_method = FK_OPTION_RESTRICT;
                break;
        }

        /* Default update method to RESTRICT if not tracked separately */
        fk_info->update_method = FK_OPTION_RESTRICT;

        f_key_list->push_back(fk_info, thd->mem_root);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Check if this table is referenced by foreign keys from other tables.

  Returns true if there are foreign keys that reference this table.
  This is used to prevent dropping tables that are referenced.
*/
bool ha_tidesdb::referenced_by_foreign_key() const noexcept
{
    DBUG_ENTER("ha_tidesdb::referenced_by_foreign_key");

    if (!share) DBUG_RETURN(false);

    DBUG_RETURN(share->num_referencing > 0);
}

/**
  @brief
  Free memory allocated by get_foreign_key_create_info().
*/
void ha_tidesdb::free_foreign_key_create_info(char *str)
{
    DBUG_ENTER("ha_tidesdb::free_foreign_key_create_info");

    if (str) my_free(str);

    DBUG_VOID_RETURN;
}

/**
  @brief
  Check if the storage engine can be switched for this table.

  Returns FALSE if the table has foreign key constraints that would
  prevent switching to another storage engine.
*/
bool ha_tidesdb::can_switch_engines()
{
    DBUG_ENTER("ha_tidesdb::can_switch_engines");

    /*
      We allow engine switching. If there are FK constraints, MySQL/MariaDB will
      handle the validation at a higher level.
    */
    DBUG_RETURN(TRUE);
}

/**
  @brief
  Reset handler state between statements.
*/
int ha_tidesdb::reset(void)
{
    DBUG_ENTER("ha_tidesdb::reset");

    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
    }
    scan_initialized = false;

    free_current_key();

    pushed_idx_cond = NULL;
    pushed_idx_cond_keyno = MAX_KEY;
    pushed_cond = NULL;
    keyread_only = false;
    semi_consistent_read_enabled = false;
    did_semi_consistent_read = false;

    DBUG_RETURN(0);
}

/**
  @brief
  Check if the requested ALTER TABLE can be done in-place.

  @param altered_table   TABLE object for new version of table
  @param ha_alter_info   Structure describing changes to be done

  @return Enum indicating support level for in-place alter
*/
enum_alter_inplace_result ha_tidesdb::check_if_supported_inplace_alter(
    TABLE *altered_table, Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::check_if_supported_inplace_alter");

    alter_table_operations flags = ha_alter_info->handler_flags;

    /*
      Operations that require table rebuild (COPY algorithm).
      These modify the row format or require full data transformation.
    */
    alter_table_operations copy_flags =
        ALTER_CHANGE_COLUMN |                             /* CHANGE/MODIFY column definition */
        ALTER_COLUMN_TYPE_CHANGE_BY_ENGINE |              /* Engine-specific type change */
        ALTER_STORED_COLUMN_ORDER |                       /* Reorder stored columns */
        ALTER_RECREATE_TABLE |                            /* FORCE/ENGINE rebuild */
        ALTER_PARTITIONED |                               /* Partition changes */
        ALTER_ADD_STORED_BASE_COLUMN |                    /* ADD stored column */
        ALTER_ADD_STORED_GENERATED_COLUMN |               /* ADD stored generated column */
        ALTER_DROP_STORED_COLUMN |                        /* DROP stored column */
        ALTER_STORED_COLUMN_TYPE |                        /* Change stored column type */
        ALTER_VIRTUAL_COLUMN_TYPE |                       /* Change virtual column type */
        ALTER_STORED_GCOL_EXPR |                          /* Change stored generated expr */
        ALTER_COLUMN_NULLABLE |                           /* NOT NULL -> NULL */
        ALTER_COLUMN_NOT_NULLABLE |                       /* NULL -> NOT NULL */
        ALTER_ORDER |                                     /* ORDER BY clause */
        ALTER_CONVERT_TO |                                /* CONVERT TO charset */
        ALTER_COLUMN_VCOL |                               /* Virtual column affecting storage */
        ALTER_ADD_SYSTEM_VERSIONING |                     /* System versioning */
        ALTER_DROP_SYSTEM_VERSIONING | ALTER_ADD_PERIOD | /* Period for system time */
        ALTER_DROP_PERIOD;

    if (flags & copy_flags)
    {
        ha_alter_info->unsupported_reason = "TidesDB requires table rebuild for this operation";
        DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
    }

    /*
      Operations we can do instantly (metadata only).
      These don't require any data changes, just .frm updates.
    */
    alter_table_operations instant_flags =
        ALTER_PARSER_ADD_COLUMN |   /* Parser flag for ADD COLUMN */
        ALTER_PARSER_DROP_COLUMN |  /* Parser flag for DROP COLUMN */
        ALTER_DROP_VIRTUAL_COLUMN | /* Virtual columns are computed */
        ALTER_ADD_VIRTUAL_COLUMN | ALTER_VIRTUAL_COLUMN_ORDER |
        ALTER_VIRTUAL_GCOL_EXPR |                                /* Virtual generated expr change */
        ALTER_COLUMN_NAME |                                      /* Rename column */
        ALTER_RENAME |                                           /* Rename table */
        ALTER_RENAME_INDEX |                                     /* Rename index */
        ALTER_RENAME_COLUMN |                                    /* Rename column (parser flag) */
        ALTER_INDEX_IGNORABILITY |                               /* Index visibility */
        ALTER_CHANGE_COLUMN_DEFAULT |                            /* Default value change */
        ALTER_COLUMN_OPTION |                                    /* Column options */
        ALTER_COLUMN_COLUMN_FORMAT |                             /* Column format */
        ALTER_CHANGE_INDEX_COMMENT |                             /* Index comment */
        ALTER_OPTIONS |                                          /* Table options (comment, etc) */
        ALTER_COLUMN_INDEX_LENGTH |                              /* Index length change */
        ALTER_ADD_CHECK_CONSTRAINT |                             /* CHECK constraints (metadata) */
        ALTER_DROP_CHECK_CONSTRAINT | ALTER_COLUMN_UNVERSIONED | /* Unversioned column flag */
        ALTER_KEYS_ONOFF |                                       /* ENABLE/DISABLE KEYS */
        ALTER_VERS_EXPLICIT;                                     /* Explicit versioning flag */

    /*
      Operations we can do in-place (index operations).
      These require scanning data but don't change row format.
    */
    alter_table_operations inplace_flags =
        ALTER_ADD_INDEX |                            /* Generic ADD INDEX */
        ALTER_DROP_INDEX |                           /* Generic DROP INDEX */
        ALTER_ADD_UNIQUE_INDEX |                     /* ADD UNIQUE INDEX */
        ALTER_DROP_UNIQUE_INDEX |                    /* DROP UNIQUE INDEX */
        ALTER_ADD_PK_INDEX |                         /* ADD PRIMARY KEY */
        ALTER_DROP_PK_INDEX |                        /* DROP PRIMARY KEY */
        ALTER_ADD_NON_UNIQUE_NON_PRIM_INDEX |        /* ADD non-unique non-primary */
        ALTER_DROP_NON_UNIQUE_NON_PRIM_INDEX |       /* DROP non-unique non-primary */
        ALTER_INDEX_ORDER |                          /* Index order change */
        ALTER_ADD_FOREIGN_KEY |                      /* Foreign key (metadata only for TidesDB) */
        ALTER_DROP_FOREIGN_KEY | ALTER_COLUMN_ORDER; /* Column order (metadata) */

    /* We check if all flags are covered */
    alter_table_operations all_supported = instant_flags | inplace_flags;

    if (flags & ~all_supported)
    {
        sql_print_information(
            "TidesDB: Unsupported inplace flags: 0x%llx "
            "(supported: 0x%llx, unsupported: 0x%llx)",
            (unsigned long long)flags, (unsigned long long)all_supported,
            (unsigned long long)(flags & ~all_supported));
        ha_alter_info->unsupported_reason =
            "TidesDB does not support this ALTER operation in-place";
        DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
    }

    if (!(flags & inplace_flags))
    {
        DBUG_RETURN(HA_ALTER_INPLACE_INSTANT);
    }

    if ((flags &
         (ALTER_ADD_INDEX | ALTER_DROP_INDEX | ALTER_ADD_UNIQUE_INDEX | ALTER_DROP_UNIQUE_INDEX |
          ALTER_ADD_NON_UNIQUE_NON_PRIM_INDEX | ALTER_DROP_NON_UNIQUE_NON_PRIM_INDEX)) &&
        !(flags & (ALTER_ADD_PK_INDEX | ALTER_DROP_PK_INDEX)))
    {
        DBUG_RETURN(HA_ALTER_INPLACE_NO_LOCK);
    }

    /*** PK changes and other operations need shared lock */
    DBUG_RETURN(HA_ALTER_INPLACE_SHARED_LOCK);
}

/**
  @brief
  Prepare for in-place ALTER TABLE.

  Called with exclusive lock to allow preparation work.
  For TidesDB, we validate the operation and prepare any needed structures.
*/
bool ha_tidesdb::prepare_inplace_alter_table(TABLE *altered_table,
                                             Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::prepare_inplace_alter_table");

    alter_table_operations flags = ha_alter_info->handler_flags;

    /* We validate ADD INDEX operations */
    if (flags & (ALTER_ADD_INDEX | ALTER_ADD_UNIQUE_INDEX))
    {
        for (uint i = 0; i < ha_alter_info->index_add_count; i++)
        {
            uint key_idx = ha_alter_info->index_add_buffer[i];
            KEY *key = &ha_alter_info->key_info_buffer[key_idx];

            /* We check for unsupported index types */
            if (key->algorithm == HA_KEY_ALG_HASH)
            {
                my_error(ER_ILLEGAL_HA_CREATE_OPTION, MYF(0), "TidesDB", "HASH index");
                DBUG_RETURN(true);
            }

            sql_print_information("TidesDB: Preparing to add index '%s'", key->name.str);
        }
    }

    /* We validate DROP PRIMARY KEY */
    if (flags & ALTER_DROP_PK_INDEX)
    {
        /* We need to rebuild the table for PK changes */
        my_error(ER_ALTER_OPERATION_NOT_SUPPORTED, MYF(0), "DROP PRIMARY KEY", "TidesDB");
        DBUG_RETURN(true);
    }

    /* We validate ADD PRIMARY KEY */
    if (flags & ALTER_ADD_PK_INDEX)
    {
        /* We need to rebuild the table for PK changes */
        my_error(ER_ALTER_OPERATION_NOT_SUPPORTED, MYF(0), "ADD PRIMARY KEY", "TidesDB");
        DBUG_RETURN(true);
    }

    DBUG_RETURN(false);
}

/**
  @brief
  Execute the in-place ALTER TABLE operation.

  This is where the actual work happens. For TidesDB:
  -- ADD INDEX      -- Create new column family and populate it
  -- DROP INDEX     -- Mark column family for deletion
  -- ADD COLUMN     -- No action needed (schema-on-read)
  -- DROP COLUMN    -- No action needed (just stop reading)
*/
bool ha_tidesdb::inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::inplace_alter_table");

    alter_table_operations flags = ha_alter_info->handler_flags;
    int ret;

    if (flags & (ALTER_ADD_INDEX | ALTER_ADD_UNIQUE_INDEX | ALTER_ADD_NON_UNIQUE_NON_PRIM_INDEX))
    {
        ret = add_index_inplace(altered_table, ha_alter_info);
        if (ret)
        {
            my_error(ER_GET_ERRNO, MYF(0), ret, "TidesDB");
            DBUG_RETURN(true);
        }
    }
    if (flags & (ALTER_DROP_INDEX | ALTER_DROP_UNIQUE_INDEX | ALTER_DROP_NON_UNIQUE_NON_PRIM_INDEX))
    {
        ret = drop_index_inplace(ha_alter_info);
        if (ret)
        {
            my_error(ER_GET_ERRNO, MYF(0), ret, "TidesDB");
            DBUG_RETURN(true);
        }
    }

    /* ADD COLUMN  -- No action needed -- TidesDB uses schema-on-read */
    /* New columns will be NULL/default for existing rows */

    /* DROP COLUMN -- No action needed -- we just stop reading the column */
    /* Old data remains but is ignored */

    DBUG_RETURN(false);
}

/**
  @brief
  Commit or rollback the in-place ALTER TABLE.

  @param altered_table   TABLE object for new version
  @param ha_alter_info   Structure with alter info
  @param commit          true = commit, false = rollback
*/
bool ha_tidesdb::commit_inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info,
                                            bool commit)
{
    DBUG_ENTER("ha_tidesdb::commit_inplace_alter_table");

    if (!commit)
    {
        /* Rollback -- we clean up any partially created indexes */
        alter_table_operations flags = ha_alter_info->handler_flags;

        if (flags & (ALTER_ADD_INDEX | ALTER_ADD_UNIQUE_INDEX))
        {
            /* We drop any indexes we created */
            for (uint i = 0; i < ha_alter_info->index_add_count; i++)
            {
                uint key_idx = ha_alter_info->index_add_buffer[i];
                KEY *key = &ha_alter_info->key_info_buffer[key_idx];

                char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
                char idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
                get_cf_name(share->table_name, cf_name, sizeof(cf_name));
                snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_NAME_FMT, cf_name,
                         key->name.str);

                tidesdb_drop_column_family(tidesdb_instance, idx_cf_name);
                sql_print_information("TidesDB: Rolled back index '%s'", key->name.str);
            }
        }

        DBUG_RETURN(false);
    }

    /* We commit -- indexes are already created, we update share and log success */
    alter_table_operations flags = ha_alter_info->handler_flags;

    if (flags & (ALTER_ADD_INDEX | ALTER_ADD_UNIQUE_INDEX | ALTER_ADD_NON_UNIQUE_NON_PRIM_INDEX))
    {
        /* We update share->index_cf for newly added indexes */
        char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
        char idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
        get_cf_name(share->table_name, cf_name, sizeof(cf_name));

        for (uint i = 0; i < ha_alter_info->index_add_count; i++)
        {
            uint key_idx = ha_alter_info->index_add_buffer[i];
            KEY *key = &ha_alter_info->key_info_buffer[key_idx];

            /* We find the index position in altered_table */
            for (uint j = 0; j < altered_table->s->keys && j < TIDESDB_MAX_INDEXES; j++)
            {
                if (strcmp(altered_table->key_info[j].name.str, key->name.str) == 0)
                {
                    snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_NAME_FMT, cf_name,
                             key->name.str);
                    share->index_cf[j] = tidesdb_get_column_family(tidesdb_instance, idx_cf_name);
                    if (share->index_cf[j] && j >= share->num_indexes)
                    {
                        share->num_indexes = j + 1;
                    }
                    break;
                }
            }
        }

        sql_print_information("TidesDB: Committed %u new index(es)",
                              ha_alter_info->index_add_count);
    }

    if (flags & (ALTER_DROP_INDEX | ALTER_DROP_UNIQUE_INDEX | ALTER_DROP_NON_UNIQUE_NON_PRIM_INDEX))
    {
        /* We clear share->index_cf for dropped indexes */
        for (uint i = 0; i < ha_alter_info->index_drop_count; i++)
        {
            KEY *key = ha_alter_info->index_drop_buffer[i];

            /* We find and clear the index in share */
            for (uint j = 0; j < table->s->keys && j < TIDESDB_MAX_INDEXES; j++)
            {
                if (strcmp(table->key_info[j].name.str, key->name.str) == 0)
                {
                    share->index_cf[j] = NULL;
                    break;
                }
            }
        }

        sql_print_information("TidesDB: Committed drop of %u index(es)",
                              ha_alter_info->index_drop_count);
    }

    /* We signal that all handlers are committed */
    ha_alter_info->group_commit_ctx = NULL;

    DBUG_RETURN(false);
}

/**
  @brief
  Add indexes in-place during ALTER TABLE.

  Creates new column families for each index and populates them
  by scanning the existing table data.
*/
int ha_tidesdb::add_index_inplace(TABLE *altered_table, Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::add_index_inplace");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    get_cf_name(share->table_name, cf_name, sizeof(cf_name));

    tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
    cf_config.write_buffer_size = tidesdb_write_buffer_size;
    cf_config.enable_bloom_filter = tidesdb_enable_bloom_filter ? 1 : 0;
    cf_config.bloom_fpr = tidesdb_bloom_fpr;
    cf_config.sync_mode = tidesdb_sync_mode;
    cf_config.use_btree = tidesdb_use_btree ? 1 : 0;

    if (tidesdb_enable_compression)
    {
        cf_config.compression_algorithm = (compression_algorithm)tidesdb_compression_algo;
    }
    else
    {
        cf_config.compression_algorithm = TDB_COMPRESS_NONE;
    }

    for (uint i = 0; i < ha_alter_info->index_add_count; i++)
    {
        uint key_idx = ha_alter_info->index_add_buffer[i];
        KEY *key = &ha_alter_info->key_info_buffer[key_idx];

        char idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
        snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_NAME_FMT, cf_name, key->name.str);

        int ret = tidesdb_create_column_family(tidesdb_instance, idx_cf_name, &cf_config);
        if (ret != TDB_SUCCESS && ret != TDB_ERR_EXISTS)
        {
            sql_print_error("TidesDB: Failed to create index CF '%s': %d", idx_cf_name, ret);
            DBUG_RETURN(HA_ERR_GENERIC);
        }

        tidesdb_column_family_t *idx_cf = tidesdb_get_column_family(tidesdb_instance, idx_cf_name);
        if (!idx_cf)
        {
            sql_print_error("TidesDB: Failed to open index CF '%s'", idx_cf_name);
            DBUG_RETURN(HA_ERR_GENERIC);
        }

        /* We populate the index by scanning existing data */
        ret = rebuild_secondary_index(key, key->name.str, altered_table);
        if (ret)
        {
            sql_print_error("TidesDB: Failed to populate index '%s': %d", key->name.str, ret);
            tidesdb_drop_column_family(tidesdb_instance, idx_cf_name);
            DBUG_RETURN(ret);
        }

        sql_print_information("TidesDB: Created and populated index '%s'", key->name.str);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Drop indexes in-place during ALTER TABLE.

  Drops the column families for the specified indexes.
*/
int ha_tidesdb::drop_index_inplace(Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::drop_index_inplace");

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    get_cf_name(share->table_name, cf_name, sizeof(cf_name));

    for (uint i = 0; i < ha_alter_info->index_drop_count; i++)
    {
        KEY *key = ha_alter_info->index_drop_buffer[i];

        /* We skip primary key -- handled separately */
        if (key == &table->key_info[table->s->primary_key]) continue;

        char idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
        snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_NAME_FMT, cf_name, key->name.str);

        int ret = tidesdb_drop_column_family(tidesdb_instance, idx_cf_name);
        if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
        {
            sql_print_warning("TidesDB: Failed to drop index CF '%s': %d", idx_cf_name, ret);
            /* We continue anyway -- index might not exist */
        }

        sql_print_information("TidesDB: Dropped index '%s'", key->name.str);
    }

    DBUG_RETURN(0);
}

/**
  @brief
  Rebuild a secondary index by scanning all table data.

  @param key_info       KEY structure for the index being built
  @param key_name       Name of the key (for CF lookup)
  @param target_table   TABLE object (used for key_copy with new key definitions)
*/
int ha_tidesdb::rebuild_secondary_index(KEY *key_info, const char *key_name, TABLE *target_table)
{
    DBUG_ENTER("ha_tidesdb::rebuild_secondary_index");

    if (!share->cf) DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);

    if (!table || !table->s)
    {
        sql_print_error("TidesDB: No table available for rebuild_secondary_index");
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    char cf_name[TIDESDB_CF_NAME_BUF_SIZE];
    char idx_cf_name[TIDESDB_IDX_CF_NAME_BUF_SIZE];
    get_cf_name(share->table_name, cf_name, sizeof(cf_name));

    snprintf(idx_cf_name, sizeof(idx_cf_name), TIDESDB_CF_IDX_NAME_FMT, cf_name, key_name);

    tidesdb_column_family_t *idx_cf = tidesdb_get_column_family(tidesdb_instance, idx_cf_name);
    if (!idx_cf)
    {
        sql_print_error("TidesDB: Index CF '%s' not found", idx_cf_name);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    /* We use table->s->reclength since stored data matches the original table format */
    uchar *row_buf = (uchar *)my_malloc(PSI_INSTRUMENT_ME, table->s->reclength, MYF(MY_WME));
    if (!row_buf) DBUG_RETURN(HA_ERR_OUT_OF_MEM);

    size_t idx_key_buf_capacity =
        key_info->key_length + TIDESDB_INITIAL_KEY_BUF_CAPACITY; /* We add extra space for PK */
    uchar *idx_key_buf = (uchar *)my_malloc(PSI_INSTRUMENT_ME, idx_key_buf_capacity, MYF(MY_WME));
    if (!idx_key_buf)
    {
        my_free(row_buf);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }

    ha_rows rows_indexed = 0;
    ha_rows batch_count = 0;
    const ha_rows BATCH_SIZE = TIDESDB_INDEX_REBUILD_BATCH_SIZE;
    int ret;

    tidesdb_txn_t *read_txn = NULL;
    ret = tidesdb_txn_begin(tidesdb_instance, &read_txn);
    if (ret != TDB_SUCCESS)
    {
        sql_print_error("TidesDB: Failed to begin read transaction: %d", ret);
        my_free(row_buf);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    tidesdb_iter_t *iter = NULL;
    ret = tidesdb_iter_new(read_txn, share->cf, &iter);
    if (ret != TDB_SUCCESS || !iter)
    {
        tidesdb_txn_rollback(read_txn);
        tidesdb_txn_free(read_txn);
        my_free(row_buf);
        my_free(idx_key_buf);
        sql_print_error("TidesDB: Failed to create rebuild iterator: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    tidesdb_txn_t *write_txn = NULL;
    ret = tidesdb_txn_begin(tidesdb_instance, &write_txn);
    if (ret != TDB_SUCCESS)
    {
        tidesdb_iter_free(iter);
        tidesdb_txn_rollback(read_txn);
        tidesdb_txn_free(read_txn);
        my_free(row_buf);
        my_free(idx_key_buf);
        sql_print_error("TidesDB: Failed to begin write transaction: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    tidesdb_iter_seek_to_first(iter);

    while (tidesdb_iter_valid(iter))
    {
        uint8_t *pk_data = NULL;
        size_t pk_len = 0;
        uint8_t *row_data = NULL;
        size_t row_len = 0;

        int key_ret = tidesdb_iter_key(iter, &pk_data, &pk_len);
        int val_ret = tidesdb_iter_value(iter, &row_data, &row_len);

        if (key_ret != TDB_SUCCESS || val_ret != TDB_SUCCESS)
        {
            tidesdb_iter_next(iter);
            continue;
        }

        if (pk_len > 0 && pk_data[0] == 0)
        {
            tidesdb_iter_next(iter);
            continue;
        }

        if (unpack_row(row_buf, row_data, row_len) == 0)
        {
            /*
              We build index key for this row.

              We need to extract the key columns from row_buf and build the index key.
              The key_info passed from add_index_inplace has key_parts that reference
              altered_table's fields, but our row_buf is in the original table's format.

              We use the column indices from key_info->key_part[].fieldnr to find the
              corresponding fields in the original table.
            */
            uint idx_key_length = key_info->key_length;
            size_t total_key_len = idx_key_length + pk_len;

            if (total_key_len > idx_key_buf_capacity)
            {
                size_t new_capacity = total_key_len * 2;
                uchar *new_buf =
                    (uchar *)my_realloc(PSI_INSTRUMENT_ME, idx_key_buf, new_capacity, MYF(MY_WME));
                if (!new_buf)
                {
                    tidesdb_iter_next(iter);
                    continue;
                }
                idx_key_buf = new_buf;
                idx_key_buf_capacity = new_capacity;
            }

            {
                uchar *key_ptr = idx_key_buf;
                for (uint p = 0; p < key_info->user_defined_key_parts; p++)
                {
                    KEY_PART_INFO *key_part = &key_info->key_part[p];
                    uint fieldnr = key_part->fieldnr;

                    if (fieldnr > 0 && fieldnr <= table->s->fields)
                    {
                        Field *field = table->field[fieldnr - 1];
                        uint key_part_len = key_part->length;

                        if (field->is_null())
                        {
                            if (key_part->null_bit)
                            {
                                *key_ptr++ = 1;
                                memset(key_ptr, 0, key_part_len);
                                key_ptr += key_part_len;
                            }
                        }
                        else
                        {
                            if (key_part->null_bit)
                            {
                                *key_ptr++ = 0;
                            }
                            uint bytes = field->pack_length();
                            if (bytes > key_part_len) bytes = key_part_len;
                            memcpy(key_ptr, field->ptr, bytes);
                            if (bytes < key_part_len)
                                memset(key_ptr + bytes, 0, key_part_len - bytes);
                            key_ptr += key_part_len;
                        }
                    }
                }

                memcpy(key_ptr, pk_data, pk_len);
                size_t idx_key_len = (key_ptr - idx_key_buf) + pk_len;

                ret = tidesdb_txn_put(write_txn, idx_cf, idx_key_buf, idx_key_len, pk_data, pk_len,
                                      -1);

                if (ret != TDB_SUCCESS)
                {
                    sql_print_error("TidesDB: Failed to write index entry at row %llu: %d",
                                    (unsigned long long)rows_indexed, ret);
                    tidesdb_iter_free(iter);
                    tidesdb_txn_rollback(read_txn);
                    tidesdb_txn_free(read_txn);
                    tidesdb_txn_rollback(write_txn);
                    tidesdb_txn_free(write_txn);
                    my_free(row_buf);
                    my_free(idx_key_buf);
                    DBUG_RETURN(HA_ERR_GENERIC);
                }

                rows_indexed++;
                batch_count++;

                if (batch_count >= BATCH_SIZE)
                {
                    ret = tidesdb_txn_commit(write_txn);
                    tidesdb_txn_free(write_txn);

                    if (ret != TDB_SUCCESS)
                    {
                        sql_print_error("TidesDB: Failed to commit batch at %llu rows: %d",
                                        (unsigned long long)rows_indexed, ret);
                        tidesdb_iter_free(iter);
                        tidesdb_txn_rollback(read_txn);
                        tidesdb_txn_free(read_txn);
                        my_free(row_buf);
                        my_free(idx_key_buf);
                        DBUG_RETURN(HA_ERR_GENERIC);
                    }

                    ret = tidesdb_txn_begin(tidesdb_instance, &write_txn);
                    if (ret != TDB_SUCCESS)
                    {
                        sql_print_error("TidesDB: Failed to begin new batch transaction: %d", ret);
                        tidesdb_iter_free(iter);
                        tidesdb_txn_rollback(read_txn);
                        tidesdb_txn_free(read_txn);
                        my_free(row_buf);
                        my_free(idx_key_buf);
                        DBUG_RETURN(HA_ERR_GENERIC);
                    }

                    batch_count = 0;
                }
            }
        }

        tidesdb_iter_next(iter);
    }

    tidesdb_iter_free(iter);
    tidesdb_txn_rollback(read_txn);
    tidesdb_txn_free(read_txn);
    my_free(row_buf);
    my_free(idx_key_buf);

    if (batch_count > 0)
    {
        ret = tidesdb_txn_commit(write_txn);
        tidesdb_txn_free(write_txn);

        if (ret != TDB_SUCCESS)
        {
            sql_print_error("TidesDB: Failed to commit final index batch: %d", ret);
            DBUG_RETURN(HA_ERR_GENERIC);
        }
    }
    else
    {
        tidesdb_txn_rollback(write_txn);
        tidesdb_txn_free(write_txn);
    }

    sql_print_information("TidesDB: Indexed %llu rows for '%s'", (unsigned long long)rows_indexed,
                          key_name);

    DBUG_RETURN(0);
}

/****************************************************************************
 * DS-MRR (Disk-Sweep Multi-Range Read) Implementation
 *
 * DS-MRR batches multiple key lookups, sorts them, and reads in disk order.
 * This significantly reduces random I/O for:
 * -- Range scans with multiple ranges
 * -- Batched Key Access (BKA) joins
 * -- Secondary index lookups that need PK fetch
 *
 * For TidesDB (LSMB+), MRR helps by:
 * -- Sorting keys before lookup improves block cache hit rate
 * -- Batching secondary index -> PK lookups reduces transaction overhead
 * -- Key-ordered access is more efficient for LSM merge iterators
 ***************************************************************************/

/**
  Initialize MRR scan.

  @param seq             Range sequence interface
  @param seq_init_param  Sequence initialization parameter
  @param n_ranges        Number of ranges
  @param mode            MRR mode flags
  @param buf             Buffer for MRR

  @return 0 on success, error code otherwise
*/
int ha_tidesdb::multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param, uint n_ranges,
                                      uint mode, HANDLER_BUFFER *buf)
{
    DBUG_ENTER("ha_tidesdb::multi_range_read_init");
    DBUG_RETURN(m_ds_mrr.dsmrr_init(this, seq, seq_init_param, n_ranges, mode, buf));
}

/**
  Get next record from MRR scan.

  @param range_info  OUT Range identifier for the returned record

  @return 0 on success, HA_ERR_END_OF_FILE at end, error code otherwise
*/
int ha_tidesdb::multi_range_read_next(range_id_t *range_info)
{
    DBUG_ENTER("ha_tidesdb::multi_range_read_next");
    DBUG_RETURN(m_ds_mrr.dsmrr_next(range_info));
}

/**
  Get MRR cost estimate for constant number of ranges.

  @param keyno           Index number
  @param seq             Range sequence interface
  @param seq_init_param  Sequence initialization parameter
  @param n_ranges        Number of ranges
  @param bufsz           IN/OUT Buffer size
  @param flags           IN/OUT MRR flags
  @param limit           Maximum rows to return
  @param cost            OUT Cost estimate

  @return Estimated number of rows
*/
ha_rows ha_tidesdb::multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq, void *seq_init_param,
                                                uint n_ranges, uint *bufsz, uint *flags,
                                                ha_rows limit, Cost_estimate *cost)
{
    DBUG_ENTER("ha_tidesdb::multi_range_read_info_const");

    m_ds_mrr.init(this, table);

    /*
     * For TidesDB, we can benefit from DS-MRR for secondary index scans
     * because it batches the PK lookups. However, for primary key scans
     * on LSM-trees, the benefit is smaller since data is already sorted.
     */
    ha_rows rows =
        m_ds_mrr.dsmrr_info_const(keyno, seq, seq_init_param, n_ranges, bufsz, flags, limit, cost);
    DBUG_RETURN(rows);
}

/**
  Get MRR cost estimate.

  @param keyno      Index number
  @param n_ranges   Number of ranges
  @param keys       Number of keys
  @param key_parts  Number of key parts
  @param bufsz      IN/OUT Buffer size
  @param flags      IN/OUT MRR flags
  @param cost       OUT Cost estimate

  @return Estimated number of rows
*/
ha_rows ha_tidesdb::multi_range_read_info(uint keyno, uint n_ranges, uint keys, uint key_parts,
                                          uint *bufsz, uint *flags, Cost_estimate *cost)
{
    DBUG_ENTER("ha_tidesdb::multi_range_read_info");

    m_ds_mrr.init(this, table);
    ha_rows rows = m_ds_mrr.dsmrr_info(keyno, n_ranges, keys, key_parts, bufsz, flags, cost);
    DBUG_RETURN(rows);
}

/**
  Get MRR explanation info for EXPLAIN output.

  @param mrr_mode  MRR mode flags
  @param str       OUT Buffer for explanation string
  @param size      Size of buffer

  @return Number of characters written
*/
int ha_tidesdb::multi_range_read_explain_info(uint mrr_mode, char *str, size_t size)
{
    DBUG_ENTER("ha_tidesdb::multi_range_read_explain_info");
    DBUG_RETURN(m_ds_mrr.dsmrr_explain_info(mrr_mode, str, size));
}

/*
  Plugin declaration
*/

struct st_mysql_storage_engine tidesdb_storage_engine = {MYSQL_HANDLERTON_INTERFACE_VERSION};

static MYSQL_SYSVAR_STR(data_dir, tidesdb_data_dir, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "TidesDB data directory", NULL, NULL, NULL);

static MYSQL_SYSVAR_ULONG(flush_threads, tidesdb_flush_threads,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY, "Number of flush threads",
                          NULL, NULL, TIDESDB_DEFAULT_FLUSH_THREADS, 1, 16, 0);

static MYSQL_SYSVAR_ULONG(compaction_threads, tidesdb_compaction_threads,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY, "Number of compaction threads",
                          NULL, NULL, TIDESDB_DEFAULT_COMPACTION_THREADS, 1, 16, 0);

static MYSQL_SYSVAR_ULONGLONG(
    block_cache_size, tidesdb_block_cache_size, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "Block cache size in bytes (default 256MB, matches InnoDB buffer pool)", NULL, NULL,
    TIDESDB_DEFAULT_BLOCK_CACHE_SIZE, 0, ULLONG_MAX, 0);

static MYSQL_SYSVAR_ULONGLONG(write_buffer_size, tidesdb_write_buffer_size,
                              PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                              "Write buffer (memtable) size in bytes", NULL, NULL,
                              TIDESDB_DEFAULT_WRITE_BUFFER_SIZE, 1024 * 1024, ULLONG_MAX, 0);

static MYSQL_SYSVAR_BOOL(enable_compression, tidesdb_enable_compression, PLUGIN_VAR_RQCMDARG,
                         "Enable LZ4 compression", NULL, NULL, TRUE);

static MYSQL_SYSVAR_BOOL(enable_bloom_filter, tidesdb_enable_bloom_filter, PLUGIN_VAR_RQCMDARG,
                         "Enable bloom filters", NULL, NULL, TRUE);

static MYSQL_SYSVAR_ENUM(compression_algo, tidesdb_compression_algo, PLUGIN_VAR_RQCMDARG,
                         "Compression algorithm: none, snappy, lz4, zstd, lz4_fast", NULL, NULL, 2,
                         &tidesdb_compression_typelib);

static MYSQL_SYSVAR_ENUM(sync_mode, tidesdb_sync_mode, PLUGIN_VAR_RQCMDARG,
                         "Sync mode: none (fastest), interval (balanced), full (safest, default)",
                         NULL, NULL, 2, &tidesdb_sync_mode_typelib);

static MYSQL_SYSVAR_ULONGLONG(sync_interval_us, tidesdb_sync_interval_us, PLUGIN_VAR_RQCMDARG,
                              "Sync interval in microseconds (for interval sync mode)", NULL, NULL,
                              TIDESDB_DEFAULT_SYNC_INTERVAL_US, 1000, 10000000, 0);

static MYSQL_SYSVAR_DOUBLE(bloom_fpr, tidesdb_bloom_fpr, PLUGIN_VAR_RQCMDARG,
                           "Bloom filter false positive rate (0.0 to 1.0)", NULL, NULL,
                           TIDESDB_DEFAULT_BLOOM_FPR, 0.0001, 0.5, 0);

static MYSQL_SYSVAR_ENUM(default_isolation, tidesdb_default_isolation, PLUGIN_VAR_RQCMDARG,
                         "Default transaction isolation level", NULL, NULL,
                         TIDESDB_DEFAULT_ISOLATION, &tidesdb_isolation_typelib);

static MYSQL_SYSVAR_ULONG(level_size_ratio, tidesdb_level_size_ratio,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "LSM level size ratio (multiplier between levels)", NULL, NULL,
                          TIDESDB_DEFAULT_LEVEL_SIZE_RATIO, 2, 100, 0);

static MYSQL_SYSVAR_ULONG(skip_list_max_level, tidesdb_skip_list_max_level, PLUGIN_VAR_RQCMDARG,
                          "Skip list maximum level for memtables", NULL, NULL,
                          TIDESDB_DEFAULT_SKIP_LIST_MAX_LEVEL, 4, 32, 0);

static MYSQL_SYSVAR_BOOL(enable_block_indexes, tidesdb_enable_block_indexes,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Enable compact block indexes for faster seeks", NULL, NULL, TRUE);

static MYSQL_SYSVAR_ULONG(index_sample_ratio, tidesdb_index_sample_ratio, PLUGIN_VAR_RQCMDARG,
                          "Block index sampling ratio (1 = every block, 8 = every 8th block)", NULL,
                          NULL, TIDESDB_DEFAULT_INDEX_SAMPLE_RATIO, 1, 64, 0);

static MYSQL_SYSVAR_ULONGLONG(default_ttl, tidesdb_default_ttl, PLUGIN_VAR_RQCMDARG,
                              "Default TTL in seconds for new rows (0 = no expiration)", NULL, NULL,
                              0, 0, ULLONG_MAX, 0);

static MYSQL_SYSVAR_ENUM(log_level, tidesdb_log_level, PLUGIN_VAR_RQCMDARG,
                         "TidesDB log level: debug, info, warn, error, fatal, none", NULL, NULL,
                         TIDESDB_DEFAULT_LOG_LEVEL, &tidesdb_log_level_typelib);

static MYSQL_SYSVAR_ULONG(ft_min_word_len, tidesdb_ft_min_word_len, PLUGIN_VAR_RQCMDARG,
                          "Minimum word length for fulltext indexing", NULL, NULL,
                          TIDESDB_DEFAULT_FT_MIN_WORD_LEN, 1, 84, 0);

static MYSQL_SYSVAR_ULONG(ft_max_word_len, tidesdb_ft_max_word_len, PLUGIN_VAR_RQCMDARG,
                          "Maximum word length for fulltext indexing", NULL, NULL,
                          TIDESDB_DEFAULT_FT_MAX_WORD_LEN, 1, 255, 0);

static MYSQL_SYSVAR_ULONG(ft_max_query_words, tidesdb_ft_max_query_words, PLUGIN_VAR_RQCMDARG,
                          "Maximum number of words in fulltext search query", NULL, NULL,
                          TIDESDB_DEFAULT_FT_MAX_QUERY_WORDS, 1, TIDESDB_FT_MAX_QUERY_WORDS_CAP, 0);

static MYSQL_SYSVAR_ULONG(min_levels, tidesdb_min_levels, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "Minimum number of LSM levels", NULL, NULL, TIDESDB_DEFAULT_MIN_LEVELS, 1,
                          20, 0);

static MYSQL_SYSVAR_ULONG(dividing_level_offset, tidesdb_dividing_level_offset,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "Compaction dividing level offset", NULL, NULL,
                          TIDESDB_DEFAULT_DIVIDING_LEVEL_OFFSET, 0, 10, 0);

static MYSQL_SYSVAR_DOUBLE(skip_list_probability, tidesdb_skip_list_probability,
                           PLUGIN_VAR_RQCMDARG, "Skip list probability for memtable", NULL, NULL,
                           TIDESDB_DEFAULT_SKIP_LIST_PROBABILITY, 0.01, 0.5, 0);

static MYSQL_SYSVAR_ULONG(block_index_prefix_len, tidesdb_block_index_prefix_len,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "Block index prefix length in bytes", NULL, NULL,
                          TIDESDB_DEFAULT_BLOCK_INDEX_PREFIX_LEN, 1, 256, 0);

static MYSQL_SYSVAR_ULONGLONG(klog_value_threshold, tidesdb_klog_value_threshold,
                              PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                              "Values larger than this go to vlog (bytes)", NULL, NULL,
                              TIDESDB_DEFAULT_KLOG_VALUE_THRESHOLD, 0, 1048576, 0);

static MYSQL_SYSVAR_ULONGLONG(min_disk_space, tidesdb_min_disk_space, PLUGIN_VAR_RQCMDARG,
                              "Minimum disk space required (bytes)", NULL, NULL,
                              TIDESDB_DEFAULT_MIN_DISK_SPACE, 0, ULLONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(l1_file_count_trigger, tidesdb_l1_file_count_trigger,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "L1 file count trigger for compaction", NULL, NULL,
                          TIDESDB_DEFAULT_L1_FILE_COUNT_TRIGGER, 1, 100, 0);

static MYSQL_SYSVAR_ULONG(l0_queue_stall_threshold, tidesdb_l0_queue_stall_threshold,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "L0 queue stall threshold for backpressure", NULL, NULL,
                          TIDESDB_DEFAULT_L0_QUEUE_STALL_THRESHOLD, 1, 1000, 0);

static MYSQL_SYSVAR_ULONG(max_open_sstables, tidesdb_max_open_sstables,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "Maximum cached SSTable structures (each uses 2 FDs)", NULL, NULL,
                          TIDESDB_DEFAULT_MAX_OPEN_SSTABLES, 16, 4096, 0);

static MYSQL_SYSVAR_BOOL(log_to_file, tidesdb_log_to_file,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Log TidesDB debug output to file instead of stderr", NULL, NULL, FALSE);

static MYSQL_SYSVAR_ULONGLONG(log_truncation_at, tidesdb_log_truncation_at,
                              PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                              "Size in bytes at which to truncate the log file (0 = no truncation)",
                              NULL, NULL, TIDESDB_DEFAULT_LOG_TRUNCATION_AT, 0, ULLONG_MAX, 0);

static MYSQL_SYSVAR_ULONGLONG(
    active_txn_buffer_size, tidesdb_active_txn_buffer_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "Size of active transaction buffer for SSI conflict detection (bytes)", NULL, NULL,
    TIDESDB_DEFAULT_ACTIVE_TXN_BUFFER_SIZE, 1024, 1048576, 0);

static MYSQL_SYSVAR_BOOL(enable_encryption, tidesdb_enable_encryption,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Enable encryption at rest using MariaDB's encryption service", NULL, NULL,
                         FALSE);

static MYSQL_SYSVAR_ULONG(encryption_key_id, tidesdb_encryption_key_id,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "Encryption key ID to use (from key management plugin)", NULL, NULL,
                          TIDESDB_DEFAULT_ENCRYPTION_KEY_ID, 1, UINT_MAX, 0);

static MYSQL_SYSVAR_BOOL(use_btree, tidesdb_use_btree, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Use B+tree format for column families (faster point lookups)", NULL, NULL,
                         FALSE);

static struct st_mysql_sys_var *tidesdb_system_variables[] = {
    MYSQL_SYSVAR(data_dir),
    MYSQL_SYSVAR(flush_threads),
    MYSQL_SYSVAR(compaction_threads),
    MYSQL_SYSVAR(block_cache_size),
    MYSQL_SYSVAR(write_buffer_size),
    MYSQL_SYSVAR(enable_compression),
    MYSQL_SYSVAR(enable_bloom_filter),
    MYSQL_SYSVAR(compression_algo),
    MYSQL_SYSVAR(sync_mode),
    MYSQL_SYSVAR(sync_interval_us),
    MYSQL_SYSVAR(bloom_fpr),
    MYSQL_SYSVAR(default_isolation),
    MYSQL_SYSVAR(level_size_ratio),
    MYSQL_SYSVAR(skip_list_max_level),
    MYSQL_SYSVAR(enable_block_indexes),
    MYSQL_SYSVAR(index_sample_ratio),
    MYSQL_SYSVAR(default_ttl),
    MYSQL_SYSVAR(log_level),
    MYSQL_SYSVAR(ft_min_word_len),
    MYSQL_SYSVAR(ft_max_word_len),
    MYSQL_SYSVAR(ft_max_query_words),
    MYSQL_SYSVAR(min_levels),
    MYSQL_SYSVAR(dividing_level_offset),
    MYSQL_SYSVAR(skip_list_probability),
    MYSQL_SYSVAR(block_index_prefix_len),
    MYSQL_SYSVAR(klog_value_threshold),
    MYSQL_SYSVAR(min_disk_space),
    MYSQL_SYSVAR(l1_file_count_trigger),
    MYSQL_SYSVAR(l0_queue_stall_threshold),
    MYSQL_SYSVAR(max_open_sstables),
    MYSQL_SYSVAR(log_to_file),
    MYSQL_SYSVAR(log_truncation_at),
    MYSQL_SYSVAR(active_txn_buffer_size),
    MYSQL_SYSVAR(enable_encryption),
    MYSQL_SYSVAR(encryption_key_id),
    MYSQL_SYSVAR(use_btree),
    NULL};

maria_declare_plugin(tidesdb){MYSQL_STORAGE_ENGINE_PLUGIN,
                              &tidesdb_storage_engine,
                              "TidesDB",
                              "TidesDB Authors",
                              "TidesDB LSMB+ storage engine with ACID transactions",
                              PLUGIN_LICENSE_GPL,
                              tidesdb_init_func,
                              tidesdb_done_func,
                              0x0140,
                              NULL,
                              tidesdb_system_variables,
                              "1.4.0",
                              MariaDB_PLUGIN_MATURITY_STABLE} maria_declare_plugin_end;