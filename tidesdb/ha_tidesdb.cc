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
#include "ha_tidesdb.h"

#include <mysql/plugin.h>

#include <cstring>
#include <string>
#include <vector>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"

/*
  Lightweight trace macro.  The srv_debug_trace check compiles to a single
  branch on a static bool -- essentially free when disabled.
  When enabled, logs function + message + elapsed microseconds.
*/
#define TDB_TRACE(fmt, ...)                                                      \
    do                                                                           \
    {                                                                            \
        if (unlikely(srv_debug_trace))                                           \
            sql_print_information("TDB_TRACE %s " fmt, __func__, ##__VA_ARGS__); \
    } while (0)

static inline long long tdb_now_us()
{
    return (long long)microsecond_interval_timer();
}

/* Hex-dump helper for trace logging (up to 32 bytes) */
static inline void tdb_hex(const uchar *data, uint len, char *out, uint out_sz)
{
    uint p = 0;
    uint lim = len > 32 ? 32 : len;
    for (uint i = 0; i < lim && p + 4 < out_sz; i++)
        p += snprintf(out + p, out_sz - p, "%02X ", data[i]);
    if (len > 32 && p + 4 < out_sz) p += snprintf(out + p, out_sz - p, "...");
    if (p > 0 && out[p - 1] == ' ') out[p - 1] = '\0';
}

/* MariaDB data directory */
extern MYSQL_PLUGIN_IMPORT char mysql_real_data_home[];

/* Global TidesDB database handle */
static tidesdb_t *tdb_global = NULL;
static std::string tdb_path;

static handlerton *tidesdb_hton;

static handler *tidesdb_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);

/* File extensions -- TidesDB manages its own files */
static const char *ha_tidesdb_exts[] = {NullS};

/* ******************** System variables (global DB config) ******************** */

static ulong srv_flush_threads = 2;
static ulong srv_compaction_threads = 2;
static ulong srv_log_level = 2;                                      /* TDB_LOG_WARN */
static my_bool srv_debug_trace = 0;                                  /* per-op trace logging */
static ulonglong srv_block_cache_size = TIDESDB_DEFAULT_BLOCK_CACHE; /* 256MB */
static ulong srv_max_open_sstables = 512;

static const char *log_level_names[] = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL", "NONE", NullS};
#if MYSQL_VERSION_ID >= 120000
static TYPELIB log_level_typelib = {array_elements(log_level_names) - 1, "log_level_typelib",
                                    log_level_names, NULL, NULL};
#else
static TYPELIB log_level_typelib = {array_elements(log_level_names) - 1, "log_level_typelib",
                                    log_level_names, NULL};
#endif

static MYSQL_SYSVAR_ULONG(flush_threads, srv_flush_threads,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "Number of TidesDB flush threads", NULL, NULL, 2, 1, 64, 0);

static MYSQL_SYSVAR_ULONG(compaction_threads, srv_compaction_threads,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "Number of TidesDB compaction threads", NULL, NULL, 2, 1, 64, 0);

static MYSQL_SYSVAR_ENUM(log_level, srv_log_level, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "TidesDB log level (DEBUG, INFO, WARN, ERROR, FATAL, NONE)", NULL, NULL, 2,
                         &log_level_typelib);

static MYSQL_SYSVAR_BOOL(debug_trace, srv_debug_trace, PLUGIN_VAR_RQCMDARG,
                         "Enable per-operation trace logging to error log (expensive, debug only)",
                         NULL, NULL, 0);

static MYSQL_SYSVAR_ULONGLONG(block_cache_size, srv_block_cache_size,
                              PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                              "TidesDB global block cache size in bytes", NULL, NULL,
                              TIDESDB_DEFAULT_BLOCK_CACHE, 0, ULONGLONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(max_open_sstables, srv_max_open_sstables,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "Max cached SSTable structures in LRU cache", NULL, NULL, 512, 1, 65536,
                          0);

/* ******************** Online backup via system variable ******************** */

static char *srv_backup_dir = NULL;

static void tidesdb_backup_dir_update(THD *thd, struct st_mysql_sys_var *, void *var_ptr,
                                      const void *save)
{
    const char *new_dir = *static_cast<const char *const *>(save);

    if (!new_dir || !new_dir[0])
    {
        /* Empty string -- we just clear the variable */
        *static_cast<char **>(var_ptr) = NULL;
        return;
    }

    if (!tdb_global)
    {
        my_error(ER_UNKNOWN_ERROR, MYF(0), "TidesDB is not open");
        return;
    }

    sql_print_information("TIDESDB: Starting online backup to '%s'", new_dir);

    int rc = tidesdb_backup(tdb_global, const_cast<char *>(new_dir));
    if (rc != TDB_SUCCESS)
    {
        sql_print_error("TIDESDB: Backup to '%s' failed (err=%d)", new_dir, rc);
        my_printf_error(ER_UNKNOWN_ERROR, "TIDESDB: Backup to '%s' failed (err=%d)", MYF(0),
                        new_dir, rc);
        /* We leave variable unchanged on failure */
        return;
    }

    sql_print_information("TIDESDB: Online backup to '%s' completed successfully", new_dir);

    /* We store the path so SHOW VARIABLES reflects the last successful backup */
    *static_cast<const char **>(var_ptr) = new_dir;
}

static MYSQL_SYSVAR_STR(backup_dir, srv_backup_dir, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Set to a directory path to trigger an online TidesDB backup. "
                        "The directory must not exist or be empty. "
                        "Example: SET GLOBAL tidesdb_backup_dir = '/path/to/backup'",
                        NULL, tidesdb_backup_dir_update, NULL);

static struct st_mysql_sys_var *tidesdb_system_variables[] = {
    MYSQL_SYSVAR(flush_threads),     MYSQL_SYSVAR(compaction_threads),
    MYSQL_SYSVAR(log_level),         MYSQL_SYSVAR(block_cache_size),
    MYSQL_SYSVAR(max_open_sstables), MYSQL_SYSVAR(backup_dir),
    MYSQL_SYSVAR(debug_trace),       NULL};

/* ******************** Table options (per-table CF config) ******************** */

struct ha_table_option_struct
{
    ulonglong write_buffer_size;
    ulonglong min_disk_space;
    ulonglong klog_value_threshold;
    ulonglong sync_interval_us;
    ulonglong index_sample_ratio;
    ulonglong block_index_prefix_len;
    ulonglong level_size_ratio;
    ulonglong min_levels;
    ulonglong dividing_level_offset;
    ulonglong skip_list_max_level;
    ulonglong skip_list_probability; /* percentage      -- 25 = 0.25 */
    ulonglong bloom_fpr;             /* parts per 10000 -- 100 = 1% */
    ulonglong l1_file_count_trigger;
    ulonglong l0_queue_stall_threshold;
    uint compression;
    uint sync_mode;
    uint isolation_level;
    bool bloom_filter;
    bool block_indexes;
    bool use_btree;
    ulonglong ttl;               /* default TTL in seconds (0 = no expiration) */
    bool encrypted;              /* ENCRYPTED=YES enables data-at-rest encryption */
    ulonglong encryption_key_id; /* ENCRYPTION_KEY_ID (default 1) */
};

ha_create_table_option tidesdb_table_option_list[] = {
    HA_TOPTION_NUMBER("WRITE_BUFFER_SIZE", write_buffer_size, 32 * 1024 * 1024, 1024, ULONGLONG_MAX,
                      1024),
    HA_TOPTION_NUMBER("MIN_DISK_SPACE", min_disk_space, 100ULL * 1024 * 1024, 0, ULONGLONG_MAX,
                      1024),
    HA_TOPTION_NUMBER("KLOG_VALUE_THRESHOLD", klog_value_threshold, 512, 0, ULONGLONG_MAX, 1),
    HA_TOPTION_NUMBER("SYNC_INTERVAL_US", sync_interval_us, 128000, 0, ULONGLONG_MAX, 1),
    HA_TOPTION_NUMBER("INDEX_SAMPLE_RATIO", index_sample_ratio, 1, 1, 1024, 1),
    HA_TOPTION_NUMBER("BLOCK_INDEX_PREFIX_LEN", block_index_prefix_len, 16, 1, 256, 1),
    HA_TOPTION_NUMBER("LEVEL_SIZE_RATIO", level_size_ratio, 10, 2, 100, 1),
    HA_TOPTION_NUMBER("MIN_LEVELS", min_levels, 5, 1, 64, 1),
    HA_TOPTION_NUMBER("DIVIDING_LEVEL_OFFSET", dividing_level_offset, 2, 0, 64, 1),
    HA_TOPTION_NUMBER("SKIP_LIST_MAX_LEVEL", skip_list_max_level, 12, 1, 64, 1),
    HA_TOPTION_NUMBER("SKIP_LIST_PROBABILITY", skip_list_probability, 25, 1, 100, 1),
    HA_TOPTION_NUMBER("BLOOM_FPR", bloom_fpr, 100, 1, 10000, 1),
    HA_TOPTION_NUMBER("L1_FILE_COUNT_TRIGGER", l1_file_count_trigger, 4, 1, 1024, 1),
    HA_TOPTION_NUMBER("L0_QUEUE_STALL_THRESHOLD", l0_queue_stall_threshold, 20, 1, 1024, 1),
    HA_TOPTION_ENUM("COMPRESSION", compression, "NONE,SNAPPY,LZ4,ZSTD,LZ4_FAST", 2),
    HA_TOPTION_ENUM("SYNC_MODE", sync_mode, "NONE,INTERVAL,FULL", 2),
    HA_TOPTION_ENUM("ISOLATION_LEVEL", isolation_level,
                    "READ_UNCOMMITTED,READ_COMMITTED,REPEATABLE_READ,SNAPSHOT,SERIALIZABLE", 2),
    HA_TOPTION_BOOL("BLOOM_FILTER", bloom_filter, 1),
    HA_TOPTION_BOOL("BLOCK_INDEXES", block_indexes, 1),
    HA_TOPTION_BOOL("USE_BTREE", use_btree, 0),
    HA_TOPTION_NUMBER("TTL", ttl, 0, 0, ULONGLONG_MAX, 1),
    HA_TOPTION_BOOL("ENCRYPTED", encrypted, 0),
    HA_TOPTION_NUMBER("ENCRYPTION_KEY_ID", encryption_key_id, 1, 1, 255, 1),
    HA_TOPTION_END};

/* ******************** Field options (per-column) ******************** */

struct ha_field_option_struct
{
    bool ttl; /* marks this column as the per-row TTL source (seconds) */
};

ha_create_table_option tidesdb_field_option_list[] = {HA_FOPTION_BOOL("TTL", ttl, 0),
                                                      HA_FOPTION_END};

/* ******************** Big-endian helpers for hidden PK ******************** */

static void encode_be64(uint64_t id, uint8_t *buf)
{
    buf[0] = (uint8_t)(id >> 56);
    buf[1] = (uint8_t)(id >> 48);
    buf[2] = (uint8_t)(id >> 40);
    buf[3] = (uint8_t)(id >> 32);
    buf[4] = (uint8_t)(id >> 24);
    buf[5] = (uint8_t)(id >> 16);
    buf[6] = (uint8_t)(id >> 8);
    buf[7] = (uint8_t)(id);
}

static uint64_t decode_be64(const uint8_t *buf)
{
    return ((uint64_t)buf[0] << 56) | ((uint64_t)buf[1] << 48) | ((uint64_t)buf[2] << 40) |
           ((uint64_t)buf[3] << 32) | ((uint64_t)buf[4] << 24) | ((uint64_t)buf[5] << 16) |
           ((uint64_t)buf[6] << 8) | (uint64_t)buf[7];
}

/*
  Return true if a TidesDB key is a data key (starts with KEY_NS_DATA).
*/
static inline bool is_data_key(const uint8_t *key, size_t key_size)
{
    return key_size > 0 && key[0] == KEY_NS_DATA;
}

/* ----- Shared enum-to-constant maps (used by create, open, prepare_inplace) ----- */

static const int tdb_compression_map[] = {TDB_COMPRESS_NONE, TDB_COMPRESS_SNAPPY, TDB_COMPRESS_LZ4,
                                          TDB_COMPRESS_ZSTD, TDB_COMPRESS_LZ4_FAST};

static const int tdb_sync_mode_map[] = {TDB_SYNC_NONE, TDB_SYNC_INTERVAL, TDB_SYNC_FULL};

static const int tdb_isolation_map[] = {TDB_ISOLATION_READ_UNCOMMITTED,
                                        TDB_ISOLATION_READ_COMMITTED, TDB_ISOLATION_REPEATABLE_READ,
                                        TDB_ISOLATION_SNAPSHOT, TDB_ISOLATION_SERIALIZABLE};

/* Single-byte placeholder value for secondary index entries (all info is in the key) */
static const uint8_t tdb_empty_val = 0;

/*
  Build a tidesdb_column_family_config_t from table options.
  Centralises the option-to-config mapping so create() and
  prepare_inplace_alter_table() stay in sync.
*/
static tidesdb_column_family_config_t build_cf_config(const ha_table_option_struct *opts)
{
    tidesdb_column_family_config_t cfg = tidesdb_default_column_family_config();
    if (!opts) return cfg;

    cfg.write_buffer_size = (size_t)opts->write_buffer_size;
    cfg.compression_algorithm = (compression_algorithm)tdb_compression_map[opts->compression];
    cfg.enable_bloom_filter = opts->bloom_filter ? 1 : 0;
    cfg.bloom_fpr = (double)opts->bloom_fpr / TIDESDB_BLOOM_FPR_DIVISOR;
    cfg.enable_block_indexes = opts->block_indexes ? 1 : 0;
    cfg.index_sample_ratio = (int)opts->index_sample_ratio;
    cfg.block_index_prefix_len = (int)opts->block_index_prefix_len;
    cfg.sync_mode = tdb_sync_mode_map[opts->sync_mode];
    cfg.sync_interval_us = (uint64_t)opts->sync_interval_us;
    cfg.klog_value_threshold = (size_t)opts->klog_value_threshold;
    cfg.min_disk_space = (size_t)opts->min_disk_space;
    cfg.default_isolation_level =
        (tidesdb_isolation_level_t)tdb_isolation_map[opts->isolation_level];
    cfg.level_size_ratio = (int)opts->level_size_ratio;
    cfg.min_levels = (int)opts->min_levels;
    cfg.dividing_level_offset = (int)opts->dividing_level_offset;
    cfg.skip_list_max_level = (int)opts->skip_list_max_level;
    cfg.skip_list_probability = (float)opts->skip_list_probability / TIDESDB_SKIP_LIST_PROB_DIV;
    cfg.l1_file_count_trigger = (int)opts->l1_file_count_trigger;
    cfg.l0_queue_stall_threshold = (int)opts->l0_queue_stall_threshold;
    cfg.use_btree = opts->use_btree ? 1 : 0;
    return cfg;
}

/*
  Resolve a secondary index CF by name.
  Returns the CF pointer (may be NULL if not found).
  Writes the CF name into out_name.
*/
static tidesdb_column_family_t *resolve_idx_cf(tidesdb_t *db, const std::string &table_cf,
                                               const char *key_name, std::string &out_name)
{
    out_name = table_cf + CF_INDEX_INFIX + key_name;
    return tidesdb_get_column_family(db, out_name.c_str());
}

/* ******************** TidesDB_share ******************** */

TidesDB_share::TidesDB_share()
    : cf(NULL),
      has_user_pk(false),
      pk_index(0),
      pk_key_len(0),
      next_row_id(1),
      isolation_level(TDB_ISOLATION_REPEATABLE_READ),
      default_ttl(0),
      ttl_field_idx(-1),
      has_blobs(false),
      has_ttl(false),
      num_secondary_indexes(0)
{
    memset(idx_comp_key_len, 0, sizeof(idx_comp_key_len));
}

TidesDB_share::~TidesDB_share()
{
}

/* ******************** Per-connection transaction helpers ******************** */

/*
  Get or create the per-connection TidesDB transaction context.
  The txn lives for the entire BEGIN...COMMIT block (or single auto-commit
  statement).  All handler objects on the same connection share it.
*/
static tidesdb_trx_t *get_or_create_trx(THD *thd, handlerton *hton, tidesdb_isolation_level_t iso)
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, hton);
    if (trx)
    {
        if (!trx->txn)
        {
            int rc = tidesdb_txn_begin_with_isolation(tdb_global, iso, &trx->txn);
            if (rc != TDB_SUCCESS)
            {
                sql_print_warning("TIDESDB: get_or_create_trx txn_begin failed rc=%d", rc);
                return NULL;
            }
            trx->dirty = false;
            trx->isolation_level = iso;
        }
        return trx;
    }

    trx = (tidesdb_trx_t *)my_malloc(PSI_NOT_INSTRUMENTED, sizeof(tidesdb_trx_t), MYF(MY_ZEROFILL));
    if (!trx) return NULL;

    int rc = tidesdb_txn_begin_with_isolation(tdb_global, iso, &trx->txn);
    if (rc != TDB_SUCCESS)
    {
        my_free(trx);
        sql_print_warning("TIDESDB: get_or_create_trx txn_begin failed rc=%d", rc);
        return NULL;
    }
    trx->dirty = false;
    trx->isolation_level = iso;
    thd_set_ha_data(thd, hton, trx);
    return trx;
}

/* ******************** Handlerton transaction callbacks ******************** */

#if MYSQL_VERSION_ID >= 120000
static int tidesdb_commit(THD *thd, bool all)
#else
static int tidesdb_commit(handlerton *, THD *thd, bool all)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx || !trx->txn) return 0;

    /* Determine whether this is the final commit for the transaction.
       all=true  -> explicit COMMIT or transaction-level end
       all=false -> statement-level; only a real commit when autocommit */
    bool is_real_commit = all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);

    if (!is_real_commit)
    {
        /* Statement-level commit inside a multi-statement transaction.
           Defer the actual commit -- writes stay buffered in the txn,
           avoiding expensive txn_begin + commit per statement.

           If this statement had writes, create/update a savepoint marking
           the last known-good state.  If a later statement fails,
           tidesdb_rollback(all=false) can rollback to here instead of
           aborting the entire txn.  Per TidesDB docs -- creating a
           savepoint with an existing name updates it; savepoints are
           auto-freed on commit/rollback. */
        if (trx->stmt_was_dirty)
        {
            tidesdb_txn_savepoint(trx->txn, "stmt");
            trx->stmt_savepoint_active = true;
        }
        trx->stmt_was_dirty = false;
        return 0;
    }

    /* Release any active statement savepoint before final commit/rollback.
       Savepoints must be explicitly released before txn_commit. */
    if (trx->stmt_savepoint_active)
    {
        tidesdb_txn_release_savepoint(trx->txn, "stmt");
        trx->stmt_savepoint_active = false;
    }

    /* Real commit -- flush to storage */
    if (trx->dirty)
    {
        int rc = tidesdb_txn_commit(trx->txn);
        if (rc != TDB_SUCCESS)
        {
            sql_print_warning("TIDESDB: hton commit failed rc=%d", rc);
            tidesdb_txn_free(trx->txn);
            trx->txn = NULL;
            trx->dirty = false;
            return HA_ERR_GENERIC;
        }
        /* We free the txn so that the next get_or_create_trx() starts
           a fresh transaction with a new read snapshot AND a distinct
           pointer -- allowing cached iterators to detect staleness via
           scan_iter_txn_ != stmt_txn and invalidate themselves. */
        tidesdb_txn_free(trx->txn);
        trx->txn = NULL;
    }
    else
    {
        /* Read-only transaction -- rollback and free.  Like the dirty
           path, we must free so the next get_or_create_trx() begins a
           fresh txn with a current read snapshot and a new pointer for
           stale-iterator detection. */
        tidesdb_txn_rollback(trx->txn);
        tidesdb_txn_free(trx->txn);
        trx->txn = NULL;
    }
    trx->dirty = false;
    trx->stmt_savepoint_active = false;
    return 0;
}

#if MYSQL_VERSION_ID >= 120000
static int tidesdb_rollback(THD *thd, bool all)
#else
static int tidesdb_rollback(handlerton *, THD *thd, bool all)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx || !trx->txn) return 0;

    bool is_real_rollback = all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);

    if (!is_real_rollback)
    {
        /* Statement-level rollback inside a multi-statement transaction.
           If a savepoint exists from a prior successful statement,
           rollback to it -- undoes only this statement's writes.
           If no savepoint (this is the first statement), rollback
           the entire txn since there's nothing to preserve. */
        if (trx->stmt_savepoint_active)
        {
            tidesdb_txn_rollback_to_savepoint(trx->txn, "stmt");
            return 0;
        }
        /* First statement failed -- no prior good state to restore.
           Fall through to full rollback. */
    }

    /* Release any active savepoint before full rollback. */
    if (trx->stmt_savepoint_active)
    {
        tidesdb_txn_release_savepoint(trx->txn, "stmt");
        trx->stmt_savepoint_active = false;
    }

    /* Full rollback -- real transaction end, autocommit, or first
       statement failure with no savepoint to restore to. */
    tidesdb_txn_rollback(trx->txn);
    tidesdb_txn_free(trx->txn);
    trx->txn = NULL;
    trx->dirty = false;
    trx->stmt_savepoint_active = false;
    return 0;
}

#if MYSQL_VERSION_ID >= 120000
static int tidesdb_close_connection(THD *thd)
#else
static int tidesdb_close_connection(handlerton *, THD *thd)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (trx)
    {
        if (trx->txn)
        {
            tidesdb_txn_rollback(trx->txn);
            tidesdb_txn_free(trx->txn);
        }
        my_free(trx);
        thd_set_ha_data(thd, tidesdb_hton, NULL);
    }
    return 0;
}

/* ******************** Plugin init / deinit ******************** */

static int tidesdb_init_func(void *p)
{
    DBUG_ENTER("tidesdb_init_func");

    tidesdb_hton = (handlerton *)p;
    tidesdb_hton->create = tidesdb_create_handler;
    tidesdb_hton->flags = 0;
    tidesdb_hton->tablefile_extensions = ha_tidesdb_exts;
    tidesdb_hton->table_options = tidesdb_table_option_list;
    tidesdb_hton->field_options = tidesdb_field_option_list;
    tidesdb_hton->drop_table = [](handlerton *, const char *) { return -1; };

    /* Handlerton transaction callbacks -- one TidesDB txn per BEGIN..COMMIT */
    tidesdb_hton->commit = tidesdb_commit;
    tidesdb_hton->rollback = tidesdb_rollback;
    tidesdb_hton->close_connection = tidesdb_close_connection;

    tidesdb_init(NULL, NULL, NULL, NULL);

    /* We place TidesDB data as a sibling of the MariaDB data directory,
       e.g. /path/to/tidesdb_data alongside /path/to/data/
       This avoids MariaDB's schema discovery detecting it. */
    std::string data_home(mysql_real_data_home);
    while (!data_home.empty() && data_home.back() == '/') data_home.pop_back();
    size_t slash_pos = data_home.rfind('/');
    if (slash_pos != std::string::npos)
        tdb_path = data_home.substr(0, slash_pos + 1) + "tidesdb_data";
    else
        tdb_path = "tidesdb_data";

    /* We map log level enum index to TidesDB constants */
    static const int log_level_map[] = {TDB_LOG_DEBUG, TDB_LOG_INFO,  TDB_LOG_WARN,
                                        TDB_LOG_ERROR, TDB_LOG_FATAL, TDB_LOG_NONE};

    tidesdb_config_t cfg = tidesdb_default_config();
    cfg.db_path = const_cast<char *>(tdb_path.c_str());
    cfg.num_flush_threads = (int)srv_flush_threads;
    cfg.num_compaction_threads = (int)srv_compaction_threads;
    cfg.log_level = (tidesdb_log_level_t)log_level_map[srv_log_level];
    cfg.block_cache_size = (size_t)srv_block_cache_size;
    cfg.max_open_sstables = (int)srv_max_open_sstables;

    int rc = tidesdb_open(&cfg, &tdb_global);
    if (rc != TDB_SUCCESS)
    {
        sql_print_error("TIDESDB: Failed to open TidesDB at %s (err=%d)", tdb_path.c_str(), rc);
        tidesdb_finalize();
        DBUG_RETURN(1);
    }

    sql_print_information("TIDESDB: TidesDB opened at %s", tdb_path.c_str());

    DBUG_RETURN(0);
}

static int tidesdb_deinit_func(void *p)
{
    DBUG_ENTER("tidesdb_deinit_func");

    if (tdb_global)
    {
        tidesdb_close(tdb_global);
        tdb_global = NULL;
    }
    tidesdb_finalize();

    sql_print_information("TIDESDB: TidesDB closed");
    DBUG_RETURN(0);
}

/* ******************** path_to_cf_name ******************** */

std::string ha_tidesdb::path_to_cf_name(const char *path)
{
    std::string p(path);

    if (p.size() >= 2 && p[0] == '.' && p[1] == '/') p = p.substr(2);

    size_t last_slash = p.rfind('/');
    if (last_slash == std::string::npos) return p;

    std::string tblname = p.substr(last_slash + 1);

    size_t prev_slash = (last_slash > 0) ? p.rfind('/', last_slash - 1) : std::string::npos;
    std::string dbname;
    if (prev_slash == std::string::npos)
        dbname = p.substr(0, last_slash);
    else
        dbname = p.substr(prev_slash + 1, last_slash - prev_slash - 1);

    std::string result = dbname + "__" + tblname;

    /* We replace '#' with '_' (MariaDB temp table names contain '#') */
    for (size_t i = 0; i < result.size(); i++)
        if (result[i] == '#') result[i] = '_';

    return result;
}

/* ******************** Factory / Constructor ******************** */

static handler *tidesdb_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
    return new (mem_root) ha_tidesdb(hton, table);
}

ha_tidesdb::ha_tidesdb(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg),
      share(NULL),
      stmt_txn(NULL),
      stmt_txn_dirty(false),
      scan_txn(NULL),
      scan_iter(NULL),
      scan_cf_(NULL),
      scan_iter_cf_(NULL),
      scan_iter_txn_(NULL),
      idx_pk_exact_done_(false),
      scan_dir_(DIR_NONE),
      current_pk_len_(0),
      idx_search_comp_len_(0),
      in_bulk_insert_(false),
      keyread_only_(false)
{
}

/* ******************** get_share ******************** */

TidesDB_share *ha_tidesdb::get_share()
{
    TidesDB_share *tmp_share;
    DBUG_ENTER("ha_tidesdb::get_share");

    lock_shared_ha_data();
    if (!(tmp_share = static_cast<TidesDB_share *>(get_ha_share_ptr())))
    {
        tmp_share = new TidesDB_share;
        if (!tmp_share) goto err;
        set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
    }
err:
    unlock_shared_ha_data();
    DBUG_RETURN(tmp_share);
}

/* ******************** PK / Index key helpers ******************** */

/*
  Build memcmp-comparable key bytes from record fields for a given KEY.
  Uses Field::make_sort_key_part() so that big-endian, sign-bit-flipped encoding
  is produced for numeric types -- which sorts correctly under memcmp.

  The record may point to record[0] or record[1]; we adjust field pointers
  via move_field_offset to read from the correct buffer.
*/
uint ha_tidesdb::make_comparable_key(KEY *key_info, const uchar *record, uint num_parts, uchar *out)
{
    uint pos = 0;
    my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(record - table->record[0]);

    for (uint p = 0; p < num_parts && p < key_info->user_defined_key_parts; p++)
    {
        KEY_PART_INFO *kp = &key_info->key_part[p];
        Field *field = kp->field;

        /* make_sort_key_part handles nullable fields internally:
           it writes a 1-byte null indicator (0x00 for NULL, 0x01 for NOT NULL)
           followed by kp->length sort bytes.  Total output for nullable fields
           is kp->length + 1 bytes. */
        field->move_field_offset(ptrdiff);
        field->make_sort_key_part(out + pos, kp->length);
        field->move_field_offset(-ptrdiff);
        pos += kp->length;
        if (field->real_maybe_null()) pos++;
    }

    return pos;
}

/*
  Convert a key_copy-format search key (as passed to index_read_map)
  into the comparable format that we store in TidesDB.
  Uses key_restore to unpack into record[1], then make_comparable_key.
*/
uint ha_tidesdb::key_copy_to_comparable(KEY *key_info, const uchar *key_buf, uint key_len,
                                        uchar *out)
{
    key_restore(table->record[1], key_buf, key_info, key_len);

    /* We count how many key parts are covered by key_len */
    uint parts = 0;
    uint len = 0;
    for (parts = 0; parts < key_info->user_defined_key_parts; parts++)
    {
        uint part_len = key_info->key_part[parts].store_length;
        if (len + part_len > key_len) break;
        len += part_len;
    }
    if (parts == 0) parts = 1;

    return make_comparable_key(key_info, table->record[1], parts, out);
}

/*
  Build PK bytes from a record.
  -- With user PK  -- use make_comparable_key for memcmp-correct ordering.
  -- Without PK    -- not applicable for NEW rows (caller generates hidden id);
                     for EXISTING rows current_pk already holds the key.
*/
uint ha_tidesdb::pk_from_record(const uchar *record, uchar *out)
{
    if (share->has_user_pk)
    {
        return make_comparable_key(&table->key_info[share->pk_index], record,
                                   table->key_info[share->pk_index].user_defined_key_parts, out);
    }
    else
    {
        /* Hidden PK -- copy current_pk (must have been set by a prior read) */
        memcpy(out, current_pk_buf_, current_pk_len_);
        return current_pk_len_;
    }
}

/*
  Compute the comparable key byte length for a KEY.
  Matches what make_comparable_key() actually produces:
    sum of (nullable ? 1 : 0) + kp->length for each key part.

  NOTE -- ki->key_length includes store_length overhead (e.g. 2 bytes
  per VARCHAR part for length prefix in key_copy format) which is
  not present in the comparable key output.
*/
static uint comparable_key_length(const KEY *ki)
{
    uint len = 0;
    for (uint p = 0; p < ki->user_defined_key_parts; p++)
    {
        if (ki->key_part[p].field->real_maybe_null()) len++;
        len += ki->key_part[p].length;
    }
    return len;
}

/*
  Build a secondary index CF entry key:
    [comparable index-column bytes] + [comparable PK bytes]
*/
uint ha_tidesdb::sec_idx_key(uint idx, const uchar *record, uchar *out)
{
    KEY *key_info = &table->key_info[idx];
    uint pos = make_comparable_key(key_info, record, key_info->user_defined_key_parts, out);
    /* Append PK for uniqueness */
    pos += pk_from_record(record, out + pos);
    return pos;
}

/*
  Try to fill record buf with column values decoded from the secondary
  index key, avoiding the expensive PK point-lookup.  Used when
  keyread_only_ is true (covering index scan).

  The secondary index key layout is:
    [comparable_idx_cols | comparable_pk]

  We reverse the sort-key encoding (big-endian + sign-flip) back to
  native record format.  Only supports integer types where the
  transformation is bijective.  Returns true on success.
*/
bool ha_tidesdb::try_keyread_from_index(const uint8_t *ik, size_t iks, uint idx, uchar *buf)
{
    if (!share->has_user_pk) return false;

    KEY *pk_key = &table->key_info[share->pk_index];
    KEY *idx_key = &table->key_info[idx];
    uint idx_col_len = share->idx_comp_key_len[idx];

    /* We check every column in read_set -- it must be a PK part or an index
       part, and must be an integer type we can reverse-decode. */
    for (uint c = bitmap_get_first_set(table->read_set); c != MY_BIT_NONE;
         c = bitmap_get_next_set(table->read_set, c))
    {
        Field *f = table->field[c];
        bool found = false;
        for (uint p = 0; p < pk_key->user_defined_key_parts; p++)
            if ((uint)(pk_key->key_part[p].fieldnr - 1) == c)
            {
                found = true;
                break;
            }
        if (!found)
            for (uint p = 0; p < idx_key->user_defined_key_parts; p++)
                if ((uint)(idx_key->key_part[p].fieldnr - 1) == c)
                {
                    found = true;
                    break;
                }
        if (!found) return false;

        switch (f->real_type())
        {
            case MYSQL_TYPE_TINY:
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_INT24:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_LONGLONG:
                break;
            default:
                return false;
        }
    }

    /* Helper lambda -- reverse sort_string for an integer key part.
       big-endian -> little-endian, un-flip sign bit for signed types. */
    auto decode_int_part = [&](const uint8_t *src, uint sort_len, Field *f, uchar *buf_base) -> bool
    {
        uchar *to = buf_base + (uintptr_t)(f->ptr - table->record[0]);
        bool is_signed = !f->is_unsigned();
        switch (sort_len)
        {
            case 1:
                to[0] = is_signed ? (src[0] ^ 0x80) : src[0];
                return true;
            case 2:
                to[0] = src[1];
                to[1] = is_signed ? (src[0] ^ 0x80) : src[0];
                return true;
            case 3:
                to[0] = src[2];
                to[1] = src[1];
                to[2] = is_signed ? (src[0] ^ 0x80) : src[0];
                return true;
            case 4:
                to[0] = src[3];
                to[1] = src[2];
                to[2] = src[1];
                to[3] = is_signed ? (src[0] ^ 0x80) : src[0];
                return true;
            case 8:
                to[0] = src[7];
                to[1] = src[6];
                to[2] = src[5];
                to[3] = src[4];
                to[4] = src[3];
                to[5] = src[2];
                to[6] = src[1];
                to[7] = is_signed ? (src[0] ^ 0x80) : src[0];
                return true;
            default:
                return false;
        }
    };

    /* We decode index column parts from the head of the key */
    const uint8_t *pos = ik;
    for (uint p = 0; p < idx_key->user_defined_key_parts; p++)
    {
        KEY_PART_INFO *kp = &idx_key->key_part[p];
        Field *f = kp->field;
        if (f->real_maybe_null())
        {
            if (pos >= ik + iks) return false;
            if (*pos == 0)
            {
                f->set_null();
                pos++;
                continue;
            }
            f->set_notnull();
            pos++;
        }
        if (pos + kp->length > ik + iks) return false;
        if (bitmap_is_set(table->read_set, kp->fieldnr - 1))
        {
            if (!decode_int_part(pos, kp->length, f, buf)) return false;
        }
        pos += kp->length;
    }

    /* We decode PK parts from the tail of the key */
    const uint8_t *pk_start = ik + idx_col_len;
    pos = pk_start;
    for (uint p = 0; p < pk_key->user_defined_key_parts; p++)
    {
        KEY_PART_INFO *kp = &pk_key->key_part[p];
        Field *f = kp->field;
        if (f->real_maybe_null())
        {
            if (pos >= ik + iks) return false;
            if (*pos == 0)
            {
                f->set_null();
                pos++;
                continue;
            }
            f->set_notnull();
            pos++;
        }
        if (pos + kp->length > ik + iks) return false;
        if (bitmap_is_set(table->read_set, kp->fieldnr - 1))
        {
            if (!decode_int_part(pos, kp->length, f, buf)) return false;
        }
        pos += kp->length;
    }

    /* Set current_pk for position() */
    uint pk_bytes = (uint)(iks - idx_col_len);
    memcpy(current_pk_buf_, pk_start, pk_bytes);
    current_pk_len_ = pk_bytes;

    return true;
}

/* ******************** ICP (Index Condition Pushdown) helpers ******************** */

/*
  Reverse a single integer sort-key part (big-endian, sign-bit-flipped)
  back to native little-endian field format in the record buffer.
  Returns true on success, false for unsupported field types.
*/
bool ha_tidesdb::decode_int_sort_key(const uint8_t *src, uint sort_len, Field *f, uchar *buf)
{
    uchar *to = buf + (uintptr_t)(f->ptr - f->table->record[0]);
    bool is_signed = !f->is_unsigned();
    switch (sort_len)
    {
        case 1:
            to[0] = is_signed ? (src[0] ^ 0x80) : src[0];
            return true;
        case 2:
            to[0] = src[1];
            to[1] = is_signed ? (src[0] ^ 0x80) : src[0];
            return true;
        case 3:
            to[0] = src[2];
            to[1] = src[1];
            to[2] = is_signed ? (src[0] ^ 0x80) : src[0];
            return true;
        case 4:
            to[0] = src[3];
            to[1] = src[2];
            to[2] = src[1];
            to[3] = is_signed ? (src[0] ^ 0x80) : src[0];
            return true;
        case 8:
            to[0] = src[7];
            to[1] = src[6];
            to[2] = src[5];
            to[3] = src[4];
            to[4] = src[3];
            to[5] = src[2];
            to[6] = src[1];
            to[7] = is_signed ? (src[0] ^ 0x80) : src[0];
            return true;
        default:
            return false;
    }
}

/*
  Evaluate pushed index condition on a secondary-index entry before
  the expensive PK point-lookup (InnoDB pattern).

  Decodes the index key column values and PK column values from the
  comparable-format index key into the record buffer, then calls
  handler_index_cond_check() which evaluates the pushed condition,
  checks end_range, and handles THD kill signals.

  Only supports integer column types for which the comparable encoding
  (big-endian + sign-bit flip) is bijectively reversible.  If any
  index or PK key part is a non-integer type, ICP is skipped and
  CHECK_POS is returned so the caller falls through to the PK lookup.
*/
check_result_t ha_tidesdb::icp_check_secondary(const uint8_t *ik, size_t iks, uint idx, uchar *buf)
{
    if (!pushed_idx_cond || pushed_idx_cond_keyno != idx) return CHECK_POS;

    KEY *idx_key = &table->key_info[idx];
    uint idx_col_len = share->idx_comp_key_len[idx];

    /* Decode index column parts from the head of the key */
    const uint8_t *pos = ik;
    for (uint p = 0; p < idx_key->user_defined_key_parts; p++)
    {
        KEY_PART_INFO *kp = &idx_key->key_part[p];
        Field *f = kp->field;

        /* Verify type is a reversible integer */
        switch (f->real_type())
        {
            case MYSQL_TYPE_TINY:
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_INT24:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_LONGLONG:
                break;
            default:
                return CHECK_POS; /* unsupported type -- skip ICP */
        }

        if (f->real_maybe_null())
        {
            if (pos >= ik + iks) return CHECK_POS;
            if (*pos == 0)
            {
                f->set_null();
                pos++;
                continue;
            }
            f->set_notnull();
            pos++;
        }
        if (pos + kp->length > ik + iks) return CHECK_POS;
        if (!decode_int_sort_key(pos, kp->length, f, buf)) return CHECK_POS;
        pos += kp->length;
    }

    /* Decode PK parts from the tail of the key (pushed condition may
       reference PK columns since they are appended to every secondary
       index entry for uniqueness). */
    if (share->has_user_pk)
    {
        KEY *pk_key = &table->key_info[share->pk_index];
        pos = ik + idx_col_len;
        for (uint p = 0; p < pk_key->user_defined_key_parts; p++)
        {
            KEY_PART_INFO *kp = &pk_key->key_part[p];
            Field *f = kp->field;

            switch (f->real_type())
            {
                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_INT24:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_LONGLONG:
                    break;
                default:
                    return CHECK_POS; /* PK has non-integer column -- skip ICP */
            }

            if (f->real_maybe_null())
            {
                if (pos >= ik + iks) return CHECK_POS;
                if (*pos == 0)
                {
                    f->set_null();
                    pos++;
                    continue;
                }
                f->set_notnull();
                pos++;
            }
            if (pos + kp->length > ik + iks) return CHECK_POS;
            if (!decode_int_sort_key(pos, kp->length, f, buf)) return CHECK_POS;
            pos += kp->length;
        }
    }

    /* All index + PK columns decoded -- delegate to MariaDB's handler
       ICP evaluator which checks kill state, end_range, and pushed_idx_cond. */
    return handler_index_cond_check(this);
}

/* ******************** Counter recovery ******************** */

/*
  Recover hidden-PK next_row_id from the last data key.
  Also seed auto_inc_val for tables with AUTO_INCREMENT user-defined PKs
  so that get_auto_increment() can return O(1) instead of doing index_last()
  on every INSERT.
*/
void ha_tidesdb::recover_counters()
{
    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return;

    tidesdb_iter_t *iter = NULL;
    if (tidesdb_iter_new(txn, share->cf, &iter) == TDB_SUCCESS)
    {
        tidesdb_iter_seek_to_last(iter);
        if (tidesdb_iter_valid(iter))
        {
            uint8_t *key = NULL;
            size_t key_size = 0;
            if (tidesdb_iter_key(iter, &key, &key_size) == TDB_SUCCESS &&
                is_data_key(key, key_size))
            {
                if (!share->has_user_pk && key_size == 1 + HIDDEN_PK_SIZE)
                {
                    /* Hidden PK -- we decode the big-endian row-id */
                    uint64_t max_id = decode_be64(key + 1);
                    share->next_row_id.store(max_id + 1, std::memory_order_relaxed);
                }

                if (share->has_user_pk && table->found_next_number_field)
                {
                    /* User PK with AUTO_INCREMENT -- we read the last row to seed
                       the in-memory counter from the max PK value. */
                    uint8_t *val = NULL;
                    size_t val_size = 0;
                    if (tidesdb_iter_value(iter, &val, &val_size) == TDB_SUCCESS)
                    {
                        /* Temporarily populate record[1] to read the field value */
                        if (!share->has_blobs && !share->encrypted &&
                            val_size >= table->s->reclength)
                        {
                            memcpy(table->record[1], val, table->s->reclength);
                            ulonglong max_val = table->found_next_number_field->val_int_offset(
                                table->s->rec_buff_length);
                            share->auto_inc_val.store(max_val, std::memory_order_relaxed);
                        }
                    }
                }
            }
        }
        tidesdb_iter_free(iter);
    }

    if (!share->has_user_pk && share->next_row_id.load(std::memory_order_relaxed) == 0)
        share->next_row_id.store(1, std::memory_order_relaxed);

    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
}

/* ******************** open / close / create ******************** */

int ha_tidesdb::open(const char *name, int mode, uint test_if_locked)
{
    DBUG_ENTER("ha_tidesdb::open");
    long long t_open0 = 0;
    if (unlikely(srv_debug_trace)) t_open0 = tdb_now_us();

    if (!(share = get_share())) DBUG_RETURN(1);

    /*
      We resolve CF pointers only once (first open).  Subsequent opens by
      other connections reuse the already-resolved share.  We hold
      lock_shared_ha_data() to prevent concurrent open() calls from
      racing on the shared vectors.
    */
    lock_shared_ha_data();
    if (!share->cf)
    {
        share->cf_name = path_to_cf_name(name);
        share->cf = tidesdb_get_column_family(tdb_global, share->cf_name.c_str());
        if (!share->cf)
        {
            unlock_shared_ha_data();
            sql_print_error("TIDESDB: CF '%s' not found for table '%s'", share->cf_name.c_str(),
                            name);
            DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
        }

        /* We determine PK info from table metadata */
        if (table->s->primary_key != MAX_KEY)
        {
            share->has_user_pk = true;
            share->pk_index = table->s->primary_key;
            share->pk_key_len = comparable_key_length(&table->key_info[share->pk_index]);
        }
        else
        {
            share->has_user_pk = false;
            share->pk_index = MAX_KEY;
            share->pk_key_len = HIDDEN_PK_SIZE;
        }

        /* We read isolation level from table options */
        if (table->s->option_struct)
        {
            uint iso_idx = table->s->option_struct->isolation_level;
            if (iso_idx < array_elements(tdb_isolation_map))
                share->isolation_level = (tidesdb_isolation_level_t)tdb_isolation_map[iso_idx];
        }

        /* We read TTL configuration from table + field options */
        if (table->s->option_struct) share->default_ttl = table->s->option_struct->ttl;

        /* We read encryption configuration from table options */
        share->encrypted = false;
        share->encryption_key_id = 1;
        share->encryption_key_version = 0;
        if (table->s->option_struct && table->s->option_struct->encrypted)
        {
            share->encrypted = true;
            share->encryption_key_id = (uint)table->s->option_struct->encryption_key_id;
            uint ver = encryption_key_get_latest_version(share->encryption_key_id);
            if (ver == ENCRYPTION_KEY_VERSION_INVALID)
            {
                sql_print_error("TIDESDB: encryption key %u not available",
                                share->encryption_key_id);
                DBUG_RETURN(HA_ERR_GENERIC);
            }
            share->encryption_key_version = ver;
        }

        share->ttl_field_idx = -1;
        for (uint i = 0; i < table->s->fields; i++)
        {
            if (table->s->field[i]->option_struct && table->s->field[i]->option_struct->ttl)
            {
                share->ttl_field_idx = (int)i;
                break;
            }
        }

        /* We cache table shape flags for hot-path short-circuiting */
        share->has_blobs = false;
        for (uint i = 0; i < table->s->fields; i++)
        {
            if (table->s->field[i]->flags & BLOB_FLAG)
            {
                share->has_blobs = true;
                break;
            }
        }
        share->has_ttl = (share->default_ttl > 0 || share->ttl_field_idx >= 0);

        /* We precompute comparable key lengths per index */
        for (uint i = 0; i < table->s->keys; i++)
            share->idx_comp_key_len[i] = comparable_key_length(&table->key_info[i]);

        /* We resolve secondary index CFs */
        for (uint i = 0; i < table->s->keys; i++)
        {
            if (share->has_user_pk && i == share->pk_index)
            {
                share->idx_cfs.push_back(NULL);
                share->idx_cf_names.push_back("");
                continue;
            }
            std::string idx_name;
            tidesdb_column_family_t *icf =
                resolve_idx_cf(tdb_global, share->cf_name, table->key_info[i].name.str, idx_name);
            share->idx_cfs.push_back(icf);
            share->idx_cf_names.push_back(idx_name);
        }

        /* We count active secondary index CFs for fast-path skipping */
        share->num_secondary_indexes = 0;
        for (uint i = 0; i < share->idx_cfs.size(); i++)
            if (share->idx_cfs[i]) share->num_secondary_indexes++;

        /* We recover hidden-PK counter (auto-inc is derived at runtime via index_last) */
        recover_counters();
    }
    unlock_shared_ha_data();

    /* We set ref_length for position()/rnd_pos() */
    ref_length = share->pk_key_len;

    if (unlikely(srv_debug_trace))
        TDB_TRACE("table=%s took=%lldus", share->cf_name.c_str(), tdb_now_us() - t_open0);
    DBUG_RETURN(0);
}

int ha_tidesdb::close(void)
{
    DBUG_ENTER("ha_tidesdb::close");
    TDB_TRACE("closing handler");
    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_cf_ = NULL;
        scan_iter_txn_ = NULL;
    }
    /* stmt_txn is a borrowed pointer into the per-connection trx->txn.
       We do not free it here -- the txn is owned by the per-connection trx
       and will be freed in tidesdb_close_connection(). */
    stmt_txn = NULL;
    stmt_txn_dirty = false;
    DBUG_RETURN(0);
}

int ha_tidesdb::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
    DBUG_ENTER("ha_tidesdb::create");

    std::string cf_name = path_to_cf_name(name);

    ha_table_option_struct *opts = table_arg->s->option_struct;
    DBUG_ASSERT(opts);

    tidesdb_column_family_config_t cfg = build_cf_config(opts);

    /* We create main data CF (skip if it already exists, e.g. crash recovery) */
    if (!tidesdb_get_column_family(tdb_global, cf_name.c_str()))
    {
        int rc = tidesdb_create_column_family(tdb_global, cf_name.c_str(), &cfg);
        if (rc != TDB_SUCCESS)
        {
            sql_print_error("TIDESDB: Failed to create CF '%s' (err=%d)", cf_name.c_str(), rc);
            DBUG_RETURN(HA_ERR_GENERIC);
        }
    }

    /* We create one CF per secondary index (named by key name for stability) */
    for (uint i = 0; i < table_arg->s->keys; i++)
    {
        if (table_arg->s->primary_key != MAX_KEY && i == table_arg->s->primary_key) continue;

        std::string idx_cf = cf_name + CF_INDEX_INFIX + table_arg->key_info[i].name.str;
        if (!tidesdb_get_column_family(tdb_global, idx_cf.c_str()))
        {
            int rc = tidesdb_create_column_family(tdb_global, idx_cf.c_str(), &cfg);
            if (rc != TDB_SUCCESS)
            {
                sql_print_error("TIDESDB: Failed to create index CF '%s' (err=%d)", idx_cf.c_str(),
                                rc);
                DBUG_RETURN(HA_ERR_GENERIC);
            }
        }
    }

    DBUG_RETURN(0);
}

/* ******************** Data-at-rest encryption helpers ******************** */

/*
  Encrypt plaintext into enc_buf_.  Format is [IV (16 bytes)] [ciphertext].
  Returns the encrypted blob as a std::string.
*/
static std::string tidesdb_encrypt_row(const std::string &plain, uint key_id, uint key_version)
{
    unsigned char key[TIDESDB_ENC_KEY_LEN];
    unsigned int klen = sizeof(key);
    encryption_key_get(key_id, key_version, key, &klen);

    unsigned char iv[TIDESDB_ENC_IV_LEN];
    my_random_bytes(iv, TIDESDB_ENC_IV_LEN);

    unsigned int slen = (unsigned int)plain.size();
    unsigned int enc_len = encryption_encrypted_length(slen, key_id, key_version);
    std::string out;
    out.resize(TIDESDB_ENC_IV_LEN + enc_len);

    memcpy(&out[0], iv, TIDESDB_ENC_IV_LEN);

    unsigned int dlen = enc_len;
    int rc = encryption_crypt((const unsigned char *)plain.data(), slen,
                              (unsigned char *)&out[TIDESDB_ENC_IV_LEN], &dlen, key, klen, iv,
                              TIDESDB_ENC_IV_LEN, ENCRYPTION_FLAG_ENCRYPT, key_id, key_version);
    if (rc != 0)
    {
        sql_print_error("TIDESDB: encryption_crypt(encrypt) failed rc=%d", rc);
        return plain; /* fallback -- we store unencrypted */
    }
    out.resize(TIDESDB_ENC_IV_LEN + dlen);
    return out;
}

/*
  Decrypt a row stored as [IV (16)] [ciphertext] back to plaintext.
*/
static std::string tidesdb_decrypt_row(const char *data, size_t len, uint key_id, uint key_version)
{
    if (len <= TIDESDB_ENC_IV_LEN)
    {
        sql_print_error("TIDESDB: encrypted row too short (%zu bytes)", len);
        return std::string(data, len);
    }

    unsigned char key[TIDESDB_ENC_KEY_LEN];
    unsigned int klen = sizeof(key);
    encryption_key_get(key_id, key_version, key, &klen);

    const unsigned char *iv = (const unsigned char *)data;
    const unsigned char *src = (const unsigned char *)data + TIDESDB_ENC_IV_LEN;
    unsigned int slen = (unsigned int)(len - TIDESDB_ENC_IV_LEN);

    std::string out;
    unsigned int dlen = slen + TIDESDB_ENC_KEY_LEN; /* padding slack */
    out.resize(dlen);

    int rc = encryption_crypt(src, slen, (unsigned char *)&out[0], &dlen, key, klen, iv,
                              TIDESDB_ENC_IV_LEN, ENCRYPTION_FLAG_DECRYPT, key_id, key_version);
    if (rc != 0)
    {
        sql_print_error("TIDESDB: encryption_crypt(decrypt) failed rc=%d", rc);
        return std::string(data, len);
    }
    out.resize(dlen);
    return out;
}

/* ******************** serialize / deserialize (BLOB deep-copy) ******************** */

const std::string &ha_tidesdb::serialize_row(const uchar *buf)
{
    my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(buf - table->record[0]);

    /* Upper-bound packed size -- null_bytes + reclength covers fixed fields.
       For BLOBs, add actual data sizes since Field_blob::pack() inlines data. */
    size_t est = table->s->null_bytes + table->s->reclength;
    if (share->has_blobs)
    {
        for (uint i = 0; i < table->s->fields; i++)
        {
            Field *f = table->field[i];
            if (!(f->flags & BLOB_FLAG) || f->is_real_null(ptrdiff)) continue;
            Field_blob *blob = (Field_blob *)f;
            est += blob->get_length(buf + (uintptr_t)(f->ptr - table->record[0]));
        }
    }

    row_buf_.resize(est);
    uchar *start = (uchar *)&row_buf_[0];
    uchar *pos = start;

    /* Null bitmap */
    memcpy(pos, buf, table->s->null_bytes);
    pos += table->s->null_bytes;

    /* Pack each non-null field using Field::pack().
       -- Fixed-size fields (INT, BIGINT, DATE) -- copies pack_length() bytes
       -- CHAR                                  -- strips trailing spaces, stores length + data
       -- VARCHAR                               -- stores actual length + data (not padded to max)
       -- BLOB                                  -- stores length + blob data inline */
    for (uint i = 0; i < table->s->fields; i++)
    {
        Field *f = table->field[i];
        if (f->is_real_null(ptrdiff)) continue;
        pos = f->pack(pos, buf + (uintptr_t)(f->ptr - table->record[0]));
    }

    row_buf_.resize((size_t)(pos - start));

    if (share->encrypted)
        row_buf_ =
            tidesdb_encrypt_row(row_buf_, share->encryption_key_id, share->encryption_key_version);

    return row_buf_;
}

void ha_tidesdb::deserialize_row(uchar *buf, const uchar *data, size_t len)
{
    const uchar *from = data;
    const uchar *from_end = data + len;

    /* Null bitmap */
    if (len < table->s->null_bytes) return;
    memcpy(buf, from, table->s->null_bytes);
    from += table->s->null_bytes;

    /* Unpack each non-null field using Field::unpack().
       Field::unpack() returns pointer past consumed bytes. */
    my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(buf - table->record[0]);
    for (uint i = 0; i < table->s->fields; i++)
    {
        Field *f = table->field[i];
        if (f->is_real_null(ptrdiff)) continue;
        uchar *to = buf + (uintptr_t)(f->ptr - table->record[0]);
        const uchar *next = f->unpack(to, from, from_end);
        if (!next) break;
        from = next;
    }
}

void ha_tidesdb::deserialize_row(uchar *buf, const std::string &row)
{
    const std::string *plain = &row;
    std::string decrypted;

    if (share->encrypted)
    {
        decrypted = tidesdb_decrypt_row(row.data(), row.size(), share->encryption_key_id,
                                        share->encryption_key_version);
        /* Copy to last_row so BLOB data pointers (set by Field_blob::unpack)
           remain valid after this function returns. */
        last_row = decrypted;
        plain = &last_row;
    }

    deserialize_row(buf, (const uchar *)plain->data(), plain->size());
}

/* ******************** fetch_row_by_pk ******************** */

/*
  Point-lookup a row by its PK bytes (without namespace prefix).
  Sets current_pk + last_row.  Returns 0 or HA_ERR_KEY_NOT_FOUND.
*/
int ha_tidesdb::fetch_row_by_pk(tidesdb_txn_t *txn, const uchar *pk, uint pk_len, uchar *buf)
{
    long long t0 = 0;
    if (unlikely(srv_debug_trace)) t0 = tdb_now_us();

    uchar dk[MAX_KEY_LENGTH + 2];
    uint dk_len = build_data_key(pk, pk_len, dk);

    uint8_t *value = NULL;
    size_t value_size = 0;
    int rc = tidesdb_txn_get(txn, share->cf, dk, dk_len, &value, &value_size);
    if (rc != TDB_SUCCESS)
    {
        TDB_TRACE("GET miss pk_len=%u", pk_len);
        return HA_ERR_KEY_NOT_FOUND;
    }

    long long t1 = 0;
    if (unlikely(srv_debug_trace)) t1 = tdb_now_us();

    if (!share->has_blobs && !share->encrypted)
    {
        /* Unpack directly from TidesDB's value buffer (no copy needed) */
        deserialize_row(buf, (const uchar *)value, value_size);
        tidesdb_free(value);
    }
    else
    {
        /* Copy to last_row so BLOB data pointers remain valid */
        last_row.assign((const char *)value, value_size);
        tidesdb_free(value);
        deserialize_row(buf, last_row);
    }
    memcpy(current_pk_buf_, pk, pk_len);
    current_pk_len_ = pk_len;

    if (unlikely(srv_debug_trace))
        TDB_TRACE("GET hit txn_get=%lld deser=%lldus val_sz=%zu", t1 - t0, tdb_now_us() - t1,
                  value_size);
    return 0;
}

/* ******************** compute_row_ttl ******************** */

/*
  Compute the absolute TTL timestamp for a row being written.
  Priority -- per-row TTL_COL value > table-level TTL option > no expiration.
  Returns -1 (no expiration) or a future absolute Unix timestamp.
*/
time_t ha_tidesdb::compute_row_ttl(const uchar *buf)
{
    long long ttl_seconds = 0;

    if (share->ttl_field_idx >= 0)
    {
        Field *f = table->field[share->ttl_field_idx];
        my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(buf - table->record[0]);
        if (!f->is_real_null(ptrdiff))
        {
            f->move_field_offset(ptrdiff);
            ttl_seconds = f->val_int();
            f->move_field_offset(-ptrdiff);
        }
    }

    if (ttl_seconds <= 0 && share->default_ttl > 0) ttl_seconds = (long long)share->default_ttl;

    if (ttl_seconds <= 0) return TIDESDB_TTL_NONE;

    return (time_t)(time(NULL) + ttl_seconds);
}

/* ******************** iter_read_current ******************** */

/*
  Read the current iterator position in the main data CF.
  Skips non-data keys (meta keys).  Sets current_pk + last_row.
  Does not advance the iterator.
*/
int ha_tidesdb::iter_read_current(uchar *buf)
{
    while (scan_iter && tidesdb_iter_valid(scan_iter))
    {
        uint8_t *key = NULL;
        size_t key_size = 0;
        if (tidesdb_iter_key(scan_iter, &key, &key_size) != TDB_SUCCESS) return HA_ERR_END_OF_FILE;

        /* We skip non-data keys (meta namespace) */
        if (!is_data_key(key, key_size))
        {
            tidesdb_iter_next(scan_iter);
            continue;
        }

        /* We extract PK bytes (everything after the namespace prefix) */
        current_pk_len_ = (uint)(key_size - 1);
        memcpy(current_pk_buf_, key + 1, current_pk_len_);

        uint8_t *value = NULL;
        size_t value_size = 0;
        if (tidesdb_iter_value(scan_iter, &value, &value_size) != TDB_SUCCESS)
            return HA_ERR_END_OF_FILE;

        if (!share->has_blobs && !share->encrypted)
        {
            /* Unpack directly from iterator buffer (no copy) */
            deserialize_row(buf, (const uchar *)value, value_size);
        }
        else
        {
            last_row.assign((const char *)value, value_size);
            deserialize_row(buf, last_row);
        }
        return 0;
    }
    return HA_ERR_END_OF_FILE;
}

/* ******************** write_row (INSERT) ******************** */

int ha_tidesdb::write_row(const uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::write_row");
    long long t0 = 0, t1 = 0, t2 = 0, t3 = 0, t4 = 0, t5 = 0;
    if (unlikely(srv_debug_trace)) t0 = tdb_now_us();

    /* We need all columns readable for PK extraction, secondary index
       key building, serialization, and TTL computation. */
    MY_BITMAP *old_map = tmp_use_all_columns(table, &table->read_set);

    /*
      The engine is responsible for calling update_auto_increment() to
      fill in the auto-generated value before using the record.
      (Same pattern as InnoDB -- see ha_innodb.cc::write_row.)
    */
    if (table->next_number_field && buf == table->record[0])
    {
        int ai_err = update_auto_increment();
        if (ai_err)
        {
            tmp_restore_column_map(&table->read_set, old_map);
            DBUG_RETURN(ai_err);
        }
        /* Keep the shared counter ahead of any explicitly-supplied value
           so that future auto-generated values don't collide. */
        ulonglong val = table->next_number_field->val_int();
        ulonglong cur = share->auto_inc_val.load(std::memory_order_relaxed);
        while (val > cur)
        {
            if (share->auto_inc_val.compare_exchange_weak(cur, val, std::memory_order_relaxed))
                break;
        }
    }

    /* We build PK bytes for this new row */
    uchar pk[MAX_KEY_LENGTH];
    uint pk_len;
    if (share->has_user_pk)
    {
        pk_len = pk_from_record(buf, pk);
    }
    else
    {
        /* Hidden PK -- we generate next row-id */
        uint64_t row_id = share->next_row_id.fetch_add(1, std::memory_order_relaxed);
        encode_be64(row_id, pk);
        pk_len = HIDDEN_PK_SIZE;
    }

    uchar dk[MAX_KEY_LENGTH + 2];
    uint dk_len = build_data_key(pk, pk_len, dk);

    const std::string &row_data = serialize_row(buf);
    const uint8_t *row_ptr = (const uint8_t *)row_data.data();
    size_t row_len = row_data.size();
    if (unlikely(srv_debug_trace))
    {
        t1 = tdb_now_us();
    }

    /* Lazy txn -- we ensure stmt_txn exists on first data access */
    {
        int erc = ensure_stmt_txn();
        if (erc) DBUG_RETURN(erc);
    }
    tidesdb_txn_t *txn = stmt_txn;
    stmt_txn_dirty = true;
    {
        tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(ha_thd(), ht);
        if (trx)
        {
            trx->dirty = true;
            trx->stmt_was_dirty = true;
        }
    }
    if (unlikely(srv_debug_trace))
    {
        t2 = tdb_now_us();
    }

    /* We compute TTL only when the table has TTL configured */
    time_t row_ttl = share->has_ttl ? compute_row_ttl(buf) : TIDESDB_TTL_NONE;

    /* We insert data row */
    int rc = tidesdb_txn_put(txn, share->cf, dk, dk_len, row_ptr, row_len, row_ttl);
    if (rc != TDB_SUCCESS) goto err;
    if (unlikely(srv_debug_trace))
    {
        t3 = tdb_now_us();
    }

    /* We maintain secondary indexes */
    memcpy(current_pk_buf_, pk, pk_len);
    current_pk_len_ = pk_len;
    if (share->num_secondary_indexes > 0)
        for (uint i = 0; i < table->s->keys; i++)
        {
            if (share->has_user_pk && i == share->pk_index) continue;
            if (i >= share->idx_cfs.size() || !share->idx_cfs[i]) continue;

            uchar ik[MAX_KEY_LENGTH * 2 + 2];
            uint ik_len = sec_idx_key(i, buf, ik);
            /* Index entries have an empty value; just the key matters.
               We use same TTL as the data row so index and data expire together. */
            rc = tidesdb_txn_put(txn, share->idx_cfs[i], ik, ik_len, &tdb_empty_val, 1, row_ttl);
            if (rc != TDB_SUCCESS) goto err;
        }
    if (unlikely(srv_debug_trace))
    {
        t4 = tdb_now_us();
    }

    if (unlikely(srv_debug_trace))
    {
        t5 = tdb_now_us();
        TDB_TRACE("pk+ser=%lld ensure_txn=%lld txn_put=%lld sec_idx=%lld total=%lldus row_len=%zu",
                  t1 - t0, t2 - t1, t3 - t2, t4 - t3, t5 - t0, row_len);
    }

    /* Commit happens in external_lock(F_UNLCK). */
    tmp_restore_column_map(&table->read_set, old_map);
    DBUG_RETURN(0);

err:
    tmp_restore_column_map(&table->read_set, old_map);
    sql_print_warning("TIDESDB: write_row put failed rc=%d", rc);
    DBUG_RETURN(HA_ERR_GENERIC);
}

/* ******************** AUTO_INCREMENT (O(1) atomic counter) ******************** */

/*
  Override the default get_auto_increment() which calls index_last() on every
  single auto-commit INSERT.  That creates and destroys a TidesDB merge-heap
  iterator each time -- O(N sources).  Instead, we maintain an in-memory atomic
  counter on TidesDB_share that is seeded once from the table data at open time
  and atomically incremented thereafter -- O(1).
*/
void ha_tidesdb::get_auto_increment(ulonglong offset, ulonglong increment,
                                    ulonglong nb_desired_values, ulonglong *first_value,
                                    ulonglong *nb_reserved_values)
{
    DBUG_ENTER("ha_tidesdb::get_auto_increment");

    /* Atomic fetch-and-add -- each caller gets a unique range.
       The counter stores the last value that was handed out. */
    ulonglong cur = share->auto_inc_val.load(std::memory_order_relaxed);
    ulonglong next;
    do
    {
        next = cur + nb_desired_values;
    } while (!share->auto_inc_val.compare_exchange_weak(cur, next, std::memory_order_relaxed));

    *first_value = cur + 1;
    /*
      Reserve exactly what was asked for.  MariaDB's update_auto_increment()
      will call us again when the interval is exhausted.
    */
    *nb_reserved_values = nb_desired_values;

    DBUG_VOID_RETURN;
}

/* ******************** Table scan (SELECT) ******************** */

int ha_tidesdb::rnd_init(bool scan)
{
    DBUG_ENTER("ha_tidesdb::rnd_init");
    long long ri_t0 = 0;
    if (unlikely(srv_debug_trace)) ri_t0 = tdb_now_us();

    current_pk_len_ = 0;

    /* Lazy txn -- we ensure stmt_txn exists */
    {
        int erc = ensure_stmt_txn();
        if (erc) DBUG_RETURN(erc);
    }
    scan_txn = stmt_txn;

    /* Reuse cached iterator if it belongs to the same CF AND same txn.
       tidesdb_iter_new() is extremely expensive (builds merge heap from
       all SSTables).  tidesdb_iter_seek() reuses cached SST sources and
       just repositions them -- orders of magnitude cheaper.
       If the txn changed (e.g. after COMMIT created a new one), the
       iterator holds a stale txn pointer and must be recreated. */
    if (scan_iter && (scan_iter_cf_ != share->cf || scan_iter_txn_ != scan_txn))
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_cf_ = NULL;
        scan_iter_txn_ = NULL;
    }

    if (!scan_iter)
    {
        int rc = tidesdb_iter_new(scan_txn, share->cf, &scan_iter);
        if (rc != TDB_SUCCESS)
        {
            scan_txn = NULL;
            DBUG_RETURN(HA_ERR_GENERIC);
        }
        scan_iter_cf_ = share->cf;
        scan_iter_txn_ = scan_txn;
    }

    /* We seek past meta keys to the first data key */
    uint8_t data_prefix = KEY_NS_DATA;
    tidesdb_iter_seek(scan_iter, &data_prefix, 1);

    if (unlikely(srv_debug_trace)) TDB_TRACE("iter_new+seek took %lldus", tdb_now_us() - ri_t0);
    DBUG_RETURN(0);
}

int ha_tidesdb::rnd_end()
{
    DBUG_ENTER("ha_tidesdb::rnd_end");

    /* We not not free scan_iter -- keep cached for reuse within this statement.
       Iterator is freed in external_lock(F_UNLCK) or close(). */
    scan_txn = NULL;

    DBUG_RETURN(0);
}

int ha_tidesdb::rnd_next(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::rnd_next");
    long long rn_t0 = 0;
    if (unlikely(srv_debug_trace)) rn_t0 = tdb_now_us();

    int ret = iter_read_current(buf);
    if (ret == 0) tidesdb_iter_next(scan_iter);

    if (unlikely(srv_debug_trace)) TDB_TRACE("ret=%d took=%lldus", ret, tdb_now_us() - rn_t0);
    DBUG_RETURN(ret);
}

/* ******************** position / rnd_pos ******************** */

void ha_tidesdb::position(const uchar *record)
{
    DBUG_ENTER("ha_tidesdb::position");
    /* We store current PK bytes into ref for later rnd_pos() retrieval */
    memcpy(ref, current_pk_buf_, current_pk_len_);
    DBUG_VOID_RETURN;
}

int ha_tidesdb::rnd_pos(uchar *buf, uchar *pos)
{
    DBUG_ENTER("ha_tidesdb::rnd_pos");

    /* Lazy txn -- we ensure stmt_txn exists */
    {
        int erc = ensure_stmt_txn();
        if (erc) DBUG_RETURN(erc);
    }

    int ret = fetch_row_by_pk(stmt_txn, pos, ref_length, buf);
    DBUG_RETURN(ret);
}

/* ******************** Index scan ******************** */

int ha_tidesdb::index_init(uint idx, bool sorted)
{
    DBUG_ENTER("ha_tidesdb::index_init");
    active_index = idx;
    idx_pk_exact_done_ = false;
    scan_dir_ = DIR_NONE;

    {
        int erc = ensure_stmt_txn();
        if (erc) DBUG_RETURN(erc);
    }
    scan_txn = stmt_txn;

    /* We determine which CF to iterate (lazily -- iterator created on demand) */
    tidesdb_column_family_t *target_cf;
    if (share->has_user_pk && idx == share->pk_index)
        target_cf = share->cf;
    else if (idx < share->idx_cfs.size() && share->idx_cfs[idx])
        target_cf = share->idx_cfs[idx];
    else
    {
        scan_txn = NULL;
        scan_cf_ = NULL;
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    scan_cf_ = target_cf;

    /* Reuse cached iterator if it belongs to the same CF AND same txn.
       In nested-loop joins, index_init/index_end cycle N times on the
       same index; reusing the iterator avoids N expensive iter_new() calls
       (each builds a merge heap from all SSTables).
       If the txn changed (e.g. after COMMIT created a new one), the
       iterator holds a stale txn pointer and must be recreated. */
    if (scan_iter && (scan_iter_cf_ != target_cf || scan_iter_txn_ != scan_txn))
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_cf_ = NULL;
        scan_iter_txn_ = NULL;
    }
    /* If scan_iter is non-NULL here, ensure_scan_iter() will reuse it. */

    DBUG_RETURN(0);
}

/*
  Lazily create the scan iterator from scan_cf_ when first needed.
  Returns 0 on success or HA_ERR_GENERIC.
*/
int ha_tidesdb::ensure_scan_iter()
{
    if (scan_iter) return 0;
    if (!scan_txn || !scan_cf_) return HA_ERR_GENERIC;
    int rc = tidesdb_iter_new(scan_txn, scan_cf_, &scan_iter);
    if (rc == TDB_SUCCESS)
    {
        scan_iter_cf_ = scan_cf_;
        scan_iter_txn_ = scan_txn;
        return 0;
    }
    return HA_ERR_GENERIC;
}

int ha_tidesdb::index_end()
{
    DBUG_ENTER("ha_tidesdb::index_end");

    /* Do not free scan_iter -- keep cached for reuse within this statement.
       Iterator is freed in external_lock(F_UNLCK) or close(). */
    scan_txn = NULL;
    active_index = MAX_KEY;

    DBUG_RETURN(0);
}

int ha_tidesdb::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                               enum ha_rkey_function find_flag)
{
    DBUG_ENTER("ha_tidesdb::index_read_map");

    /* key_copy_to_comparable uses key_restore + make_comparable_key,
       which reads fields via make_sort_key_part. */
    MY_BITMAP *old_map = tmp_use_all_columns(table, &table->read_set);

    uint key_len = calculate_key_len(table, active_index, key, keypart_map);

    /* We convert the key_copy-format search key to our comparable format */
    KEY *ki = &table->key_info[active_index];
    uchar comp_key[MAX_KEY_LENGTH];
    uint comp_len = key_copy_to_comparable(ki, key, key_len, comp_key);

    tmp_restore_column_map(&table->read_set, old_map);

    memcpy(idx_search_comp_, comp_key, comp_len);
    idx_search_comp_len_ = comp_len;

    if (unlikely(srv_debug_trace))
    {
        char hx[128];
        tdb_hex(comp_key, comp_len, hx, sizeof(hx));
        TDB_TRACE("idx=%u flag=%d comp_len=%u comp_key=%s", active_index, (int)find_flag, comp_len,
                  hx);
    }

    bool is_pk = share->has_user_pk && active_index == share->pk_index;

    if (is_pk)
    {
        /* We build the full data key -- KEY_NS_DATA + comparable_pk_bytes */
        uchar seek_key[MAX_KEY_LENGTH + 2];
        uint seek_len = build_data_key(comp_key, comp_len, seek_key);

        if (find_flag == HA_READ_KEY_EXACT)
        {
            /* Exact PK match -- point lookup only, no iterator needed.
               If index_next is called later, ensure_scan_iter will create it. */
            int ret = fetch_row_by_pk(scan_txn, comp_key, comp_len, buf);
            if (ret == 0) idx_pk_exact_done_ = true;
            DBUG_RETURN(ret);
        }

        /* All other PK scan modes need the iterator */
        {
            int irc = ensure_scan_iter();
            if (irc) DBUG_RETURN(irc);
        }

        if (find_flag == HA_READ_KEY_OR_NEXT || find_flag == HA_READ_AFTER_KEY)
        {
            tidesdb_iter_seek(scan_iter, seek_key, seek_len);

            if (find_flag == HA_READ_AFTER_KEY && tidesdb_iter_valid(scan_iter))
            {
                /* We skip exact match if present */
                uint8_t *ik = NULL;
                size_t iks = 0;
                if (tidesdb_iter_key(scan_iter, &ik, &iks) == TDB_SUCCESS && iks == seek_len &&
                    memcmp(ik, seek_key, iks) == 0)
                    tidesdb_iter_next(scan_iter);
            }

            int ret = iter_read_current(buf);
            if (ret == 0)
            {
                tidesdb_iter_next(scan_iter);
                scan_dir_ = DIR_FORWARD;
            }
            DBUG_RETURN(ret);
        }
        else if (find_flag == HA_READ_KEY_OR_PREV || find_flag == HA_READ_BEFORE_KEY ||
                 find_flag == HA_READ_PREFIX_LAST || find_flag == HA_READ_PREFIX_LAST_OR_PREV)
        {
            tidesdb_iter_seek_for_prev(scan_iter, seek_key, seek_len);
            if (find_flag == HA_READ_BEFORE_KEY && tidesdb_iter_valid(scan_iter))
            {
                uint8_t *ik = NULL;
                size_t iks = 0;
                if (tidesdb_iter_key(scan_iter, &ik, &iks) == TDB_SUCCESS && iks == seek_len &&
                    memcmp(ik, seek_key, iks) == 0)
                    tidesdb_iter_prev(scan_iter);
            }

            int ret = iter_read_current(buf);
            if (ret == 0) scan_dir_ = DIR_BACKWARD;
            DBUG_RETURN(ret);
        }

        /* Fallback is to seek forward */
        tidesdb_iter_seek(scan_iter, seek_key, seek_len);
        int ret = iter_read_current(buf);
        if (ret == 0)
        {
            tidesdb_iter_next(scan_iter);
            scan_dir_ = DIR_FORWARD;
        }
        DBUG_RETURN(ret);
    }
    else
    {
        /* -- Secondary index read -- needs an iterator */
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);

        if (find_flag == HA_READ_KEY_EXACT || find_flag == HA_READ_KEY_OR_NEXT)
        {
            tidesdb_iter_seek(scan_iter, comp_key, comp_len);
        }
        else if (find_flag == HA_READ_AFTER_KEY)
        {
            /* We seek, then skip past any exact prefix matches */
            tidesdb_iter_seek(scan_iter, comp_key, comp_len);
            while (tidesdb_iter_valid(scan_iter))
            {
                uint8_t *ik = NULL;
                size_t iks = 0;
                if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS) break;
                if (iks < comp_len || memcmp(ik, comp_key, comp_len) != 0) break;
                tidesdb_iter_next(scan_iter);
            }
        }
        else if (find_flag == HA_READ_KEY_OR_PREV || find_flag == HA_READ_BEFORE_KEY ||
                 find_flag == HA_READ_PREFIX_LAST || find_flag == HA_READ_PREFIX_LAST_OR_PREV)
        {
            /* We build upper bound -- comp_key with all 0xFF appended for pk portion */
            uchar upper[MAX_KEY_LENGTH * 2 + 2];
            memcpy(upper, comp_key, comp_len);
            memset(upper + comp_len, 0xFF, share->pk_key_len);
            uint upper_len = comp_len + share->pk_key_len;
            tidesdb_iter_seek_for_prev(scan_iter, upper, upper_len);
        }
        else
        {
            tidesdb_iter_seek(scan_iter, comp_key, comp_len);
        }

        /* We read the current entry from the secondary index.
           ICP loop -- evaluate pushed index condition before the expensive
           PK point-lookup.  Entries that fail the condition are skipped
           without touching the data CF (same pattern as InnoDB). */
        bool is_backward =
            (find_flag == HA_READ_KEY_OR_PREV || find_flag == HA_READ_BEFORE_KEY ||
             find_flag == HA_READ_PREFIX_LAST || find_flag == HA_READ_PREFIX_LAST_OR_PREV);

        uint idx_col_len = share->idx_comp_key_len[active_index];

        for (;;)
        {
            if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);

            uint8_t *ik = NULL;
            size_t iks = 0;
            if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS)
                DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);

            /* For EXACT match, we verify the index prefix matches */
            if (find_flag == HA_READ_KEY_EXACT)
            {
                if (iks < comp_len || memcmp(ik, comp_key, comp_len) != 0)
                    DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
            }

            if (iks <= idx_col_len) DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);

            if (unlikely(srv_debug_trace))
            {
                char hx[128];
                tdb_hex(ik, (uint)iks, hx, sizeof(hx));
                TDB_TRACE("sec found iks=%zu idx_col_len=%u ik=%s", iks, idx_col_len, hx);
            }

            /* ICP -- we evaluate pushed condition on index columns before PK lookup */
            check_result_t icp = icp_check_secondary(ik, iks, active_index, buf);
            if (icp == CHECK_NEG)
            {
                if (is_backward)
                    tidesdb_iter_prev(scan_iter);
                else
                    tidesdb_iter_next(scan_iter);
                continue; /* skip this entry */
            }
            if (icp == CHECK_OUT_OF_RANGE) DBUG_RETURN(HA_ERR_END_OF_FILE);
            if (icp == CHECK_ABORTED_BY_USER) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

            /* CHECK_POS -- condition satisfied (or ICP not applicable) */
            int ret;
            if (keyread_only_ && try_keyread_from_index(ik, iks, active_index, buf))
                ret = 0;
            else
                ret = fetch_row_by_pk(scan_txn, ik + idx_col_len, (uint)(iks - idx_col_len), buf);
            if (ret == 0)
            {
                if (is_backward)
                {
                    scan_dir_ = DIR_BACKWARD;
                }
                else
                {
                    tidesdb_iter_next(scan_iter);
                    scan_dir_ = DIR_FORWARD;
                }
            }

            DBUG_RETURN(ret);
        }
    }
}

int ha_tidesdb::index_next(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_next");

    bool is_pk = share->has_user_pk && active_index == share->pk_index;

    if (idx_pk_exact_done_)
    {
        idx_pk_exact_done_ = false;
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);
        uchar seek_key[MAX_KEY_LENGTH + 2];
        uint seek_len = build_data_key(current_pk_buf_, current_pk_len_, seek_key);
        tidesdb_iter_seek(scan_iter, seek_key, seek_len);
        if (tidesdb_iter_valid(scan_iter)) tidesdb_iter_next(scan_iter);
        /* Iterator is now one ahead -- matches DIR_FORWARD contract */
    }
    else
    {
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);
        /* Direction switch -- if last op was backward, iterator is AT the
           last-read row.  We skip past it so we read the next one. */
        if (scan_dir_ == DIR_BACKWARD) tidesdb_iter_next(scan_iter);
    }

    if (is_pk)
    {
        int ret = iter_read_current(buf);
        if (ret == 0) tidesdb_iter_next(scan_iter);
        scan_dir_ = DIR_FORWARD;
        DBUG_RETURN(ret);
    }
    else
    {
        /* Secondary index -- ICP loop -- we skip entries that fail the pushed
           condition without the expensive PK point-lookup. */
        uint idx_key_len = share->idx_comp_key_len[active_index];
        for (;;)
        {
            if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

            uint8_t *ik = NULL;
            size_t iks = 0;
            if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS)
                DBUG_RETURN(HA_ERR_END_OF_FILE);

            if (iks <= idx_key_len) DBUG_RETURN(HA_ERR_END_OF_FILE);

            /* ICP -- we evaluate pushed condition before PK lookup */
            check_result_t icp = icp_check_secondary(ik, iks, active_index, buf);
            if (icp == CHECK_NEG)
            {
                tidesdb_iter_next(scan_iter);
                continue;
            }
            if (icp == CHECK_OUT_OF_RANGE) DBUG_RETURN(HA_ERR_END_OF_FILE);
            if (icp == CHECK_ABORTED_BY_USER) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

            int ret;
            if (keyread_only_ && try_keyread_from_index(ik, iks, active_index, buf))
                ret = 0;
            else
                ret = fetch_row_by_pk(scan_txn, ik + idx_key_len, (uint)(iks - idx_key_len), buf);
            if (ret == 0) tidesdb_iter_next(scan_iter);
            scan_dir_ = DIR_FORWARD;
            DBUG_RETURN(ret);
        }
    }
}

int ha_tidesdb::index_prev(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_prev");

    /* If PK exact match was done without iterator, create it now and
       seek to the matched key so that prev() steps before it. */
    if (idx_pk_exact_done_)
    {
        idx_pk_exact_done_ = false;
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);
        uchar seek_key[MAX_KEY_LENGTH + 2];
        uint seek_len = build_data_key(current_pk_buf_, current_pk_len_, seek_key);
        tidesdb_iter_seek(scan_iter, seek_key, seek_len);
        /* Iterator is AT the matched key -- fall through to prev() */
    }
    else
    {
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);
        if (scan_dir_ == DIR_FORWARD) tidesdb_iter_prev(scan_iter);
    }

    tidesdb_iter_prev(scan_iter);

    bool is_pk = share->has_user_pk && active_index == share->pk_index;
    if (is_pk)
    {
        /* We skip meta keys going backwards */
        while (tidesdb_iter_valid(scan_iter))
        {
            uint8_t *key = NULL;
            size_t ks = 0;
            if (tidesdb_iter_key(scan_iter, &key, &ks) != TDB_SUCCESS)
                DBUG_RETURN(HA_ERR_END_OF_FILE);
            if (is_data_key(key, ks)) break;
            tidesdb_iter_prev(scan_iter);
        }
        scan_dir_ = DIR_BACKWARD;
        DBUG_RETURN(iter_read_current(buf));
    }
    else
    {
        /* Secondary index -- ICP loop (backward direction) */
        uint idx_key_len = share->idx_comp_key_len[active_index];
        for (;;)
        {
            if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

            uint8_t *ik = NULL;
            size_t iks = 0;
            if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS)
                DBUG_RETURN(HA_ERR_END_OF_FILE);

            if (iks <= idx_key_len) DBUG_RETURN(HA_ERR_END_OF_FILE);

            /* ICP -- we evaluate pushed condition before PK lookup */
            check_result_t icp = icp_check_secondary(ik, iks, active_index, buf);
            if (icp == CHECK_NEG)
            {
                tidesdb_iter_prev(scan_iter);
                continue;
            }
            if (icp == CHECK_OUT_OF_RANGE) DBUG_RETURN(HA_ERR_END_OF_FILE);
            if (icp == CHECK_ABORTED_BY_USER) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

            scan_dir_ = DIR_BACKWARD;
            int ret;
            if (keyread_only_ && try_keyread_from_index(ik, iks, active_index, buf))
                ret = 0;
            else
                ret = fetch_row_by_pk(scan_txn, ik + idx_key_len, (uint)(iks - idx_key_len), buf);
            DBUG_RETURN(ret);
        }
    }
}

int ha_tidesdb::index_first(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_first");

    idx_pk_exact_done_ = false;
    int irc = ensure_scan_iter();
    if (irc) DBUG_RETURN(irc);

    bool is_pk = share->has_user_pk && active_index == share->pk_index;
    if (is_pk)
    {
        /* We seek to first data key */
        uint8_t data_prefix = KEY_NS_DATA;
        tidesdb_iter_seek(scan_iter, &data_prefix, 1);
        int ret = iter_read_current(buf);
        if (ret == 0)
        {
            tidesdb_iter_next(scan_iter);
            scan_dir_ = DIR_FORWARD;
        }
        DBUG_RETURN(ret);
    }
    else
    {
        tidesdb_iter_seek_to_first(scan_iter);
        scan_dir_ = DIR_NONE; /* index_next will set DIR_FORWARD */
        DBUG_RETURN(index_next(buf));
    }
}

int ha_tidesdb::index_last(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_last");

    idx_pk_exact_done_ = false;
    int irc = ensure_scan_iter();
    if (irc) DBUG_RETURN(irc);

    bool is_pk = share->has_user_pk && active_index == share->pk_index;
    if (is_pk)
    {
        tidesdb_iter_seek_to_last(scan_iter);
        /* The last key might be a data key already, but skip backwards
           past any non-data keys just in case. */
        while (tidesdb_iter_valid(scan_iter))
        {
            uint8_t *key = NULL;
            size_t ks = 0;
            if (tidesdb_iter_key(scan_iter, &key, &ks) != TDB_SUCCESS)
                DBUG_RETURN(HA_ERR_END_OF_FILE);
            if (is_data_key(key, ks)) break;
            tidesdb_iter_prev(scan_iter);
        }
        scan_dir_ = DIR_BACKWARD;
        DBUG_RETURN(iter_read_current(buf));
    }
    else
    {
        tidesdb_iter_seek_to_last(scan_iter);
        if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

        uint8_t *ik = NULL;
        size_t iks = 0;
        if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS) DBUG_RETURN(HA_ERR_END_OF_FILE);

        uint idx_key_len = share->idx_comp_key_len[active_index];
        if (iks <= idx_key_len) DBUG_RETURN(HA_ERR_END_OF_FILE);

        scan_dir_ = DIR_BACKWARD;
        DBUG_RETURN(fetch_row_by_pk(scan_txn, ik + idx_key_len, (uint)(iks - idx_key_len), buf));
    }
}

int ha_tidesdb::index_next_same(uchar *buf, const uchar *key, uint keylen)
{
    DBUG_ENTER("ha_tidesdb::index_next_same");

    bool is_pk = share->has_user_pk && active_index == share->pk_index;

    if (is_pk)
    {
        /* PK is unique -- after the first match there are no more */
        DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    /* Secondary index -- ICP loop -- we skip entries that fail the pushed
       condition without the expensive PK point-lookup. */
    uint idx_col_len = share->idx_comp_key_len[active_index];
    for (;;)
    {
        if (!scan_iter || !tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

        uint8_t *ik = NULL;
        size_t iks = 0;
        if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS) DBUG_RETURN(HA_ERR_END_OF_FILE);

        if (iks < idx_search_comp_len_ || memcmp(ik, idx_search_comp_, idx_search_comp_len_) != 0)
        {
            if (unlikely(srv_debug_trace))
            {
                char hx1[128], hx2[128];
                tdb_hex(ik, (uint)iks, hx1, sizeof(hx1));
                tdb_hex(idx_search_comp_, idx_search_comp_len_, hx2, sizeof(hx2));
                TDB_TRACE("prefix MISMATCH ik(%zu)=%s search(%u)=%s", iks, hx1,
                          idx_search_comp_len_, hx2);
            }
            DBUG_RETURN(HA_ERR_END_OF_FILE);
        }

        if (unlikely(srv_debug_trace))
        {
            char hx[128];
            tdb_hex(ik, (uint)iks, hx, sizeof(hx));
            TDB_TRACE("prefix MATCH ik(%zu)=%s", iks, hx);
        }

        if (iks <= idx_col_len) DBUG_RETURN(HA_ERR_END_OF_FILE);

        /* ICP -- we evaluate pushed condition before PK lookup */
        check_result_t icp = icp_check_secondary(ik, iks, active_index, buf);
        if (icp == CHECK_NEG)
        {
            tidesdb_iter_next(scan_iter);
            continue;
        }
        if (icp == CHECK_OUT_OF_RANGE) DBUG_RETURN(HA_ERR_END_OF_FILE);
        if (icp == CHECK_ABORTED_BY_USER) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

        int ret;
        if (keyread_only_ && try_keyread_from_index(ik, iks, active_index, buf))
            ret = 0;
        else
            ret = fetch_row_by_pk(scan_txn, ik + idx_col_len, (uint)(iks - idx_col_len), buf);
        if (ret == 0) tidesdb_iter_next(scan_iter);
        DBUG_RETURN(ret);
    }
}

/* ******************** update_row (UPDATE) ******************** */

int ha_tidesdb::update_row(const uchar *old_data, const uchar *new_data)
{
    DBUG_ENTER("ha_tidesdb::update_row");
    long long ur_t0 = 0;
    if (unlikely(srv_debug_trace)) ur_t0 = tdb_now_us();

    MY_BITMAP *old_map = tmp_use_all_columns(table, &table->read_set);

    /* We use handler-owned pk buffer for old/new PK to avoid large stack arrays.
       old_pk is saved from current_pk_buf_ before we overwrite it. */
    uchar old_pk[MAX_KEY_LENGTH];
    uint old_pk_len = current_pk_len_;
    memcpy(old_pk, current_pk_buf_, old_pk_len);

    /* new_pk is built directly into current_pk_buf_ (reused below) */
    uchar *new_pk = current_pk_buf_;
    uint new_pk_len = pk_from_record(new_data, new_pk);

    const std::string &new_row = serialize_row(new_data);
    const uint8_t *row_ptr = (const uint8_t *)new_row.data();
    size_t row_len = new_row.size();

    {
        int erc = ensure_stmt_txn();
        if (erc) DBUG_RETURN(erc);
    }
    tidesdb_txn_t *txn = stmt_txn;
    stmt_txn_dirty = true;
    {
        tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(ha_thd(), ht);
        if (trx)
        {
            trx->dirty = true;
            trx->stmt_was_dirty = true;
        }
    }

    int rc;
    bool pk_changed = (old_pk_len != new_pk_len || memcmp(old_pk, new_pk, old_pk_len) != 0);

    /* We compute TTL only when the table has TTL configured */
    time_t row_ttl = share->has_ttl ? compute_row_ttl(new_data) : TIDESDB_TTL_NONE;

    /* If PK changed, we delete old entry and insert new */
    if (pk_changed)
    {
        uchar old_dk[MAX_KEY_LENGTH + 2];
        uint old_dk_len = build_data_key(old_pk, old_pk_len, old_dk);
        rc = tidesdb_txn_delete(txn, share->cf, old_dk, old_dk_len);
        if (rc != TDB_SUCCESS) goto err;
    }

    {
        uchar new_dk[MAX_KEY_LENGTH + 2];
        uint new_dk_len = build_data_key(new_pk, new_pk_len, new_dk);
        rc = tidesdb_txn_put(txn, share->cf, new_dk, new_dk_len, row_ptr, row_len, row_ttl);
        if (rc != TDB_SUCCESS) goto err;
    }

    /* We update secondary indexes -- skip unchanged entries to avoid
       redundant txn_delete + txn_put pairs (big win for updates that
       only touch non-indexed columns).
       Buffers declared outside the loop to reduce stack frame size. */
    if (share->num_secondary_indexes > 0)
    {
        uchar old_ik[MAX_KEY_LENGTH * 2 + 2];
        uchar new_ik[MAX_KEY_LENGTH * 2 + 2];
        for (uint i = 0; i < table->s->keys; i++)
        {
            if (share->has_user_pk && i == share->pk_index) continue;
            if (i >= share->idx_cfs.size() || !share->idx_cfs[i]) continue;

            /* We build old index entry key */
            KEY *ki = &table->key_info[i];
            memcpy(current_pk_buf_, old_pk, old_pk_len);
            current_pk_len_ = old_pk_len;
            uint old_ik_len = make_comparable_key(ki, old_data, ki->user_defined_key_parts, old_ik);
            memcpy(old_ik + old_ik_len, old_pk, old_pk_len);
            old_ik_len += old_pk_len;

            /* We build new index entry key */
            memcpy(current_pk_buf_, new_pk, new_pk_len);
            current_pk_len_ = new_pk_len;
            uint new_ik_len = sec_idx_key(i, new_data, new_ik);

            /* We skip if the index key is identical (indexed columns + PK unchanged) */
            if (old_ik_len == new_ik_len && memcmp(old_ik, new_ik, old_ik_len) == 0) continue;

            tidesdb_txn_delete(txn, share->idx_cfs[i], old_ik, old_ik_len);
            tidesdb_txn_put(txn, share->idx_cfs[i], new_ik, new_ik_len, &tdb_empty_val, 1, row_ttl);
        }
    }

    memcpy(current_pk_buf_, new_pk, new_pk_len);
    current_pk_len_ = new_pk_len;

    if (unlikely(srv_debug_trace))
        TDB_TRACE("pk_changed=%d took=%lldus", (int)pk_changed, tdb_now_us() - ur_t0);
    /* Commit happens in external_lock(F_UNLCK). */
    tmp_restore_column_map(&table->read_set, old_map);
    DBUG_RETURN(0);

err:
    tmp_restore_column_map(&table->read_set, old_map);
    sql_print_warning("TIDESDB: update_row put/delete failed rc=%d", rc);
    DBUG_RETURN(HA_ERR_GENERIC);
}

/* ******************** delete_row (DELETE) ******************** */

int ha_tidesdb::delete_row(const uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::delete_row");
    long long dr_t0 = 0;
    if (unlikely(srv_debug_trace)) dr_t0 = tdb_now_us();

    MY_BITMAP *old_map = tmp_use_all_columns(table, &table->read_set);

    {
        int erc = ensure_stmt_txn();
        if (erc)
        {
            tmp_restore_column_map(&table->read_set, old_map);
            DBUG_RETURN(erc);
        }
    }
    tidesdb_txn_t *txn = stmt_txn;
    stmt_txn_dirty = true;
    {
        tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(ha_thd(), ht);
        if (trx)
        {
            trx->dirty = true;
            trx->stmt_was_dirty = true;
        }
    }

    /* We delete data row */
    uchar dk[MAX_KEY_LENGTH + 2];
    uint dk_len = build_data_key(current_pk_buf_, current_pk_len_, dk);
    int rc = tidesdb_txn_delete(txn, share->cf, dk, dk_len);
    if (rc != TDB_SUCCESS)
    {
        tmp_restore_column_map(&table->read_set, old_map);
        sql_print_warning("TIDESDB: delete_row failed rc=%d", rc);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    /* We delete secondary index entries */
    if (share->num_secondary_indexes > 0)
        for (uint i = 0; i < table->s->keys; i++)
        {
            if (share->has_user_pk && i == share->pk_index) continue;
            if (i >= share->idx_cfs.size() || !share->idx_cfs[i]) continue;

            uchar ik[MAX_KEY_LENGTH * 2 + 2];
            uint ik_len = sec_idx_key(i, buf, ik);
            tidesdb_txn_delete(txn, share->idx_cfs[i], ik, ik_len);
        }

    if (unlikely(srv_debug_trace)) TDB_TRACE("took=%lldus", tdb_now_us() - dr_t0);

    tmp_restore_column_map(&table->read_set, old_map);
    DBUG_RETURN(0);
}

/* ******************** delete_all_rows (TRUNCATE) ******************** */

int ha_tidesdb::delete_all_rows(void)
{
    DBUG_ENTER("ha_tidesdb::delete_all_rows");

    /* Free cached iterator before dropping/recreating CFs.
       The iterator holds refs to SSTables in the CF being dropped. */
    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_cf_ = NULL;
        scan_iter_txn_ = NULL;
    }

    /* Discard the connection txn before drop/recreate.  The txn may have
       buffered INSERT/UPDATE ops from earlier statements; committing them
       after the CF is recreated would re-insert stale data. */
    {
        THD *thd = ha_thd();
        tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, ht);
        if (trx && trx->txn)
        {
            tidesdb_txn_rollback(trx->txn);
            tidesdb_txn_free(trx->txn);
            trx->txn = NULL;
            trx->dirty = false;
        }
        stmt_txn = NULL;
        stmt_txn_dirty = false;
    }

    tidesdb_column_family_config_t cfg = build_cf_config(table->s->option_struct);

    /* We drop and recreate the main data CF (O(1) instead of iterating all keys) */
    {
        std::string cf_name = share->cf_name;
        int rc = tidesdb_drop_column_family(tdb_global, cf_name.c_str());
        if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
        {
            sql_print_error("TIDESDB: truncate: failed to drop CF '%s' (err=%d)", cf_name.c_str(),
                            rc);
            DBUG_RETURN(HA_ERR_GENERIC);
        }

        rc = tidesdb_create_column_family(tdb_global, cf_name.c_str(), &cfg);
        if (rc != TDB_SUCCESS)
        {
            sql_print_error("TIDESDB: truncate: failed to recreate CF '%s' (err=%d)",
                            cf_name.c_str(), rc);
            DBUG_RETURN(HA_ERR_GENERIC);
        }

        share->cf = tidesdb_get_column_family(tdb_global, cf_name.c_str());
        if (!share->cf)
        {
            sql_print_error("TIDESDB: truncate: CF '%s' not found after recreate", cf_name.c_str());
            DBUG_RETURN(HA_ERR_GENERIC);
        }
    }

    /* We drop and recreate each secondary index CF */
    for (uint i = 0; i < share->idx_cfs.size(); i++)
    {
        if (!share->idx_cfs[i]) continue;

        const std::string &idx_name = share->idx_cf_names[i];
        tidesdb_drop_column_family(tdb_global, idx_name.c_str());

        int rc = tidesdb_create_column_family(tdb_global, idx_name.c_str(), &cfg);
        if (rc != TDB_SUCCESS)
        {
            sql_print_warning("TIDESDB: truncate: failed to recreate idx CF '%s' (err=%d)",
                              idx_name.c_str(), rc);
            share->idx_cfs[i] = NULL;
            continue;
        }

        share->idx_cfs[i] = tidesdb_get_column_family(tdb_global, idx_name.c_str());
    }

    share->next_row_id.store(1, std::memory_order_relaxed);

    DBUG_RETURN(0);
}

/* ******************** Bulk insert ******************** */

void ha_tidesdb::start_bulk_insert(ha_rows rows, uint flags)
{
    TDB_TRACE("rows=%llu flags=%u", (unsigned long long)rows, flags);
    in_bulk_insert_ = true;
}

int ha_tidesdb::end_bulk_insert()
{
    TDB_TRACE("end");
    in_bulk_insert_ = false;
    return 0;
}

/* ******************** Index Condition Pushdown (ICP) ******************** */

Item *ha_tidesdb::idx_cond_push(uint keyno, Item *idx_cond)
{
    DBUG_ENTER("ha_tidesdb::idx_cond_push");

    /* Accept the pushed condition -- the server will evaluate it for us
       during index scans via handler::pushed_idx_cond.  For secondary
       index scans the condition is checked before the PK lookup, saving
       the most expensive operation when the condition filters rows. */
    pushed_idx_cond = idx_cond;
    pushed_idx_cond_keyno = keyno;
    in_range_check_pushed_down = true;

    /* Return NULL to indicate we accepted the entire condition */
    DBUG_RETURN(NULL);
}

/* ******************** info ******************** */

int ha_tidesdb::info(uint flag)
{
    DBUG_ENTER("ha_tidesdb::info");
    long long ti0 = 0;
    if (unlikely(srv_debug_trace)) ti0 = tdb_now_us();

    if (share) ref_length = share->pk_key_len;

    if ((flag & (HA_STATUS_VARIABLE | HA_STATUS_CONST)) && share && share->cf)
    {
        long long now = tdb_now_us();
        long long last = share->stats_refresh_us.load(std::memory_order_relaxed);
        if (now - last > TIDESDB_STATS_REFRESH_US)
        {
            tidesdb_stats_t *st = NULL;
            if (tidesdb_get_stats(share->cf, &st) == TDB_SUCCESS && st)
            {
                share->cached_records.store(st->total_keys, std::memory_order_relaxed);
                share->cached_data_size.store(st->total_data_size, std::memory_order_relaxed);
                uint32_t mrl = (uint32_t)(st->avg_key_size + st->avg_value_size);
                if (mrl == 0) mrl = table->s->reclength;
                share->cached_mean_rec_len.store(mrl, std::memory_order_relaxed);
                share->cached_read_amp = st->read_amp > 0 ? st->read_amp : 1.0;

                /* We sum secondary index CF sizes for index_file_length */
                uint64_t idx_total = 0;
                for (uint i = 0; i < share->idx_cfs.size(); i++)
                {
                    if (!share->idx_cfs[i]) continue;
                    tidesdb_stats_t *ist = NULL;
                    if (tidesdb_get_stats(share->idx_cfs[i], &ist) == TDB_SUCCESS && ist)
                    {
                        idx_total += ist->total_data_size;
                        tidesdb_free_stats(ist);
                    }
                }
                share->cached_idx_data_size.store(idx_total, std::memory_order_relaxed);

                tidesdb_free_stats(st);
            }
            share->stats_refresh_us.store(now, std::memory_order_relaxed);
        }

        /* We feed all cached values to the optimizer */
        stats.records = share->cached_records.load(std::memory_order_relaxed);
        if (stats.records == 0) stats.records = TIDESDB_MIN_STATS_RECORDS;
        stats.data_file_length = share->cached_data_size.load(std::memory_order_relaxed);
        stats.index_file_length = share->cached_idx_data_size.load(std::memory_order_relaxed);
        stats.mean_rec_length = share->cached_mean_rec_len.load(std::memory_order_relaxed);
        stats.delete_length = 0;
        stats.mrr_length_per_rec = ref_length + 8;
    }

    /* HA_STATUS_CONST       -- set rec_per_key for index selectivity estimates.
       For PK (unique)       -- rec_per_key = 1.
       For secondary indexes -- we estimate from total_keys / distinct count.
       Without per-index distinct-key stats from TidesDB, we approximate
       using total_keys (worst case = every key is unique = 1). */
    if ((flag & HA_STATUS_CONST) && share)
    {
        for (uint i = 0; i < table->s->keys; i++)
        {
            KEY *key = &table->key_info[i];
            bool is_pk = share->has_user_pk && i == share->pk_index;
            for (uint j = 0; j < key->ext_key_parts; j++)
            {
                if (is_pk || (j + 1 == key->user_defined_key_parts))
                    key->rec_per_key[j] = 1; /* unique or last part -- 1 */
                else
                    key->rec_per_key[j] = (ulong)MY_MIN(stats.records / 4 + 1, stats.records);
            }
        }
    }

    if (unlikely(srv_debug_trace))
        TDB_TRACE("flag=0x%x records=%llu data=%llu idx=%llu mrl=%lu took=%lldus", flag,
                  (unsigned long long)stats.records, (unsigned long long)stats.data_file_length,
                  (unsigned long long)stats.index_file_length, stats.mean_rec_length,
                  tdb_now_us() - ti0);

    DBUG_RETURN(0);
}

/* ******************** analyze ******************** */

/*
  ANALYZE TABLE -- refresh cached stats and output CF statistics as notes.
  The notes appear as additional Msg_type='note' rows in the ANALYZE TABLE
  result set, giving the user visibility into TidesDB internals.
*/
int ha_tidesdb::analyze(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::analyze");

    if (!share || !share->cf) DBUG_RETURN(HA_ADMIN_FAILED);

    share->stats_refresh_us.store(0, std::memory_order_relaxed);
    info(HA_STATUS_VARIABLE | HA_STATUS_CONST);

    tidesdb_stats_t *st = NULL;
    if (tidesdb_get_stats(share->cf, &st) != TDB_SUCCESS || !st)
    {
        push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                            "TIDESDB: unable to retrieve column family stats");
        DBUG_RETURN(HA_ADMIN_OK);
    }

    /* Summary line */
    push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                        "TIDESDB: CF '%s'  total_keys=%llu  data_size=%llu bytes"
                        "  memtable=%zu bytes  levels=%d  read_amp=%.2f"
                        "  cache_hit=%.1f%%",
                        share->cf_name.c_str(), (unsigned long long)st->total_keys,
                        (unsigned long long)st->total_data_size, st->memtable_size, st->num_levels,
                        st->read_amp, st->hit_rate * 100.0);

    /* Average sizes */
    push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                        "TIDESDB: avg_key=%.1f bytes  avg_value=%.1f bytes", st->avg_key_size,
                        st->avg_value_size);

    /* Per-level detail */
    for (int i = 0; i < st->num_levels; i++)
    {
        push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                            "TIDESDB: level %d  sstables=%d  size=%zu bytes"
                            "  keys=%llu",
                            i + 1, st->level_num_sstables[i], st->level_sizes[i],
                            (unsigned long long)st->level_key_counts[i]);
    }

    /* B+tree stats (only when use_btree=1) */
    if (st->use_btree)
    {
        push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                            "TIDESDB: btree  nodes=%llu  max_height=%u"
                            "  avg_height=%.2f",
                            (unsigned long long)st->btree_total_nodes, st->btree_max_height,
                            st->btree_avg_height);
    }

    tidesdb_free_stats(st);

    /* Secondary index CF stats */
    for (uint i = 0; i < share->idx_cfs.size(); i++)
    {
        if (!share->idx_cfs[i]) continue;

        tidesdb_stats_t *ist = NULL;
        if (tidesdb_get_stats(share->idx_cfs[i], &ist) == TDB_SUCCESS && ist)
        {
            push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                                "TIDESDB: idx CF '%s'  keys=%llu  data_size=%llu bytes"
                                "  levels=%d",
                                share->idx_cf_names[i].c_str(), (unsigned long long)ist->total_keys,
                                (unsigned long long)ist->total_data_size, ist->num_levels);
            tidesdb_free_stats(ist);
        }
    }

    DBUG_RETURN(HA_ADMIN_OK);
}

/* ******************** optimize ******************** */

/*
  OPTIMIZE TABLE -- trigger compaction on all CFs (data + secondary indexes).
  Compaction merges SSTables, removes tombstones, and reduces read
  amplification.  TidesDB enqueues the work to background compaction
  threads and returns immediately.
*/
int ha_tidesdb::optimize(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::optimize");

    if (!share || !share->cf) DBUG_RETURN(HA_ADMIN_FAILED);

    int rc = tidesdb_compact(share->cf);
    if (rc != TDB_SUCCESS)
        sql_print_warning("TIDESDB: optimize: compact data CF '%s' failed (err=%d)",
                          share->cf_name.c_str(), rc);

    for (uint i = 0; i < share->idx_cfs.size(); i++)
    {
        if (!share->idx_cfs[i]) continue;
        rc = tidesdb_compact(share->idx_cfs[i]);
        if (rc != TDB_SUCCESS)
            sql_print_warning("TIDESDB: optimize: compact idx CF '%s' failed (err=%d)",
                              share->idx_cf_names[i].c_str(), rc);
    }

    /* Refresh stats so the optimizer sees the post-compaction state sooner */
    share->stats_refresh_us.store(0, std::memory_order_relaxed);

    DBUG_RETURN(HA_ADMIN_OK);
}

IO_AND_CPU_COST ha_tidesdb::scan_time()
{
    IO_AND_CPU_COST cost;
    cost.io = 0.0;
    cost.cpu = 0.0;

    if (!share || !share->cf) return cost;

    /* Use tidesdb_range_cost over the full key space of the data CF.
       This accounts for the actual LSM structure (number of levels,
       SSTables, compression, merge overhead) rather than the generic
       data_file_length / IO_SIZE estimate. */
    uchar lo[2] = {KEY_NS_DATA};
    uchar hi[MAX_KEY_LENGTH + 2];
    memset(hi, 0xFF, sizeof(hi));
    uint hi_len = 1 + share->pk_key_len;
    if (hi_len > sizeof(hi)) hi_len = sizeof(hi);

    double full_cost = 0.0;
    if (tidesdb_range_cost(share->cf, lo, 1, hi, hi_len, &full_cost) == TDB_SUCCESS &&
        full_cost > 0.0)
    {
        /* Split the cost -- block reads are I/O, per-entry processing is CPU.
           tidesdb_range_cost weights blocks at ~1.0-1.5x and entries at 0.01x,
           so I/O dominates.  We assign 90% to I/O, 10% to CPU. */
        cost.io = full_cost * 0.9;
        cost.cpu = full_cost * 0.1;
    }
    else
    {
        /* Fallback to base implementation */
        cost = handler::scan_time();
    }

    return cost;
}

ha_rows ha_tidesdb::records_in_range(uint inx, const key_range *min_key, const key_range *max_key,
                                     page_range *pages)
{
    if (!share) return 10;

    ha_rows total = share->cached_records.load(std::memory_order_relaxed);
    if (total == 0) total = TIDESDB_MIN_STATS_RECORDS;

    /* Determine which CF this index lives in */
    tidesdb_column_family_t *cf;
    bool is_pk = share->has_user_pk && inx == share->pk_index;
    if (is_pk)
        cf = share->cf;
    else if (inx < share->idx_cfs.size() && share->idx_cfs[inx])
        cf = share->idx_cfs[inx];
    else
        return (total / 4) + 1; /* fallback -- no CF for this index */

    /* Convert min_key / max_key to our comparable format.
       If a bound is missing we use the natural boundary of the key space. */
    uchar lo_buf[MAX_KEY_LENGTH + 2];
    uchar hi_buf[MAX_KEY_LENGTH + 2];
    uint lo_len = 0, hi_len = 0;

    MY_BITMAP *old_map = tmp_use_all_columns(table, &table->read_set);

    if (min_key && min_key->key)
    {
        KEY *ki = &table->key_info[inx];
        uint kl = calculate_key_len(table, inx, min_key->key, min_key->keypart_map);
        if (is_pk)
        {
            uchar comp[MAX_KEY_LENGTH];
            uint comp_len = key_copy_to_comparable(ki, min_key->key, kl, comp);
            lo_len = build_data_key(comp, comp_len, lo_buf);
        }
        else
        {
            lo_len = key_copy_to_comparable(ki, min_key->key, kl, lo_buf);
        }
    }
    else
    {
        /* No lower bound -- use smallest possible key */
        if (is_pk)
        {
            lo_buf[0] = KEY_NS_DATA;
            lo_len = 1;
        }
        else
        {
            memset(lo_buf, 0x00, 1);
            lo_len = 1;
        }
    }

    if (max_key && max_key->key)
    {
        KEY *ki = &table->key_info[inx];
        uint kl = calculate_key_len(table, inx, max_key->key, max_key->keypart_map);
        if (is_pk)
        {
            uchar comp[MAX_KEY_LENGTH];
            uint comp_len = key_copy_to_comparable(ki, max_key->key, kl, comp);
            hi_len = build_data_key(comp, comp_len, hi_buf);
        }
        else
        {
            hi_len = key_copy_to_comparable(ki, max_key->key, kl, hi_buf);
        }
    }
    else
    {
        /* No upper bound -- use largest possible key */
        memset(hi_buf, 0xFF, sizeof(hi_buf));
        hi_len = is_pk ? (1 + share->pk_key_len) : share->idx_comp_key_len[inx] + share->pk_key_len;
        if (hi_len > sizeof(hi_buf)) hi_len = sizeof(hi_buf);
    }

    tmp_restore_column_map(&table->read_set, old_map);

    /* Detect point equality -- both bounds provided with identical comparable
       bytes.  tidesdb_range_cost is an I/O cost metric, not a cardinality
       metric -- for memtable-only data it cannot distinguish a point range
       from a full scan.  For equalities we return rec_per_key directly. */
    if (min_key && max_key && lo_len > 0 && hi_len > 0 && lo_len == hi_len &&
        memcmp(lo_buf, hi_buf, lo_len) == 0)
    {
        KEY *ki = &table->key_info[inx];
        uint parts_used = my_count_bits(min_key->keypart_map);
        if (parts_used > 0 && parts_used <= ki->user_defined_key_parts)
        {
            ulong rpk = ki->rec_per_key[parts_used - 1];
            ha_rows est = (rpk > 0) ? (ha_rows)rpk : 1;
            if (est > total) est = total;
            return est;
        }
        return 1;
    }

    /* Ask TidesDB for the range cost (no disk I/O -- uses in-memory
       block indexes, SSTable min/max keys, and entry counts). */
    double range_cost = 0.0;
    int rc = tidesdb_range_cost(cf, lo_buf, lo_len, hi_buf, hi_len, &range_cost);
    if (rc != TDB_SUCCESS || range_cost <= 0.0) return (total / 4) + 1; /* fallback */

    /* Get full-range cost for normalization.  We use the natural boundaries
       of the key space so that range_cost / full_cost ≈ fraction of data. */
    double full_cost = 0.0;
    {
        uchar full_lo[2] = {(uchar)(is_pk ? KEY_NS_DATA : 0x00)};
        uchar full_hi[MAX_KEY_LENGTH + 2];
        memset(full_hi, 0xFF, sizeof(full_hi));
        uint full_hi_len = hi_len; /* same width as hi_buf */
        tidesdb_range_cost(cf, full_lo, 1, full_hi, full_hi_len, &full_cost);
    }

    if (full_cost <= 0.0) return (total / 4) + 1; /* fallback */

    /* Estimate records proportionally -- narrower range -> fewer records */
    double fraction = range_cost / full_cost;
    if (fraction > 1.0) fraction = 1.0;
    if (fraction < 0.0) fraction = 0.0;

    ha_rows est = (ha_rows)(total * fraction);
    if (est == 0) est = 1; /* never return 0 -- optimizer treats it as "empty" */

    if (unlikely(srv_debug_trace))
        TDB_TRACE("idx=%u range_cost=%.2f full_cost=%.2f fraction=%.4f est=%llu total=%llu", inx,
                  range_cost, full_cost, fraction, (unsigned long long)est,
                  (unsigned long long)total);

    return est;
}

int ha_tidesdb::extra(enum ha_extra_function operation)
{
    switch (operation)
    {
        case HA_EXTRA_KEYREAD:
            keyread_only_ = true;
            break;
        case HA_EXTRA_NO_KEYREAD:
            keyread_only_ = false;
            break;
        case HA_EXTRA_PREPARE_FOR_DROP:
            /* Table is about to be dropped -- skip fsync overhead */
            break;
        default:
            break;
    }
    TDB_TRACE("op=%d keyread=%d", (int)operation, (int)keyread_only_);
    return 0;
}

/* ******************** Locking ******************** */

/*
  Lazy txn creation.  Gets the per-connection TidesDB txn (shared by
  all handler objects on this connection).  The txn spans the entire
  BEGIN...COMMIT block, not just one statement.
*/
int ha_tidesdb::ensure_stmt_txn()
{
    if (stmt_txn)
    {
        TDB_TRACE("reuse existing txn");
        return 0;
    }
    long long t0 = 0;
    if (unlikely(srv_debug_trace)) t0 = tdb_now_us();

    THD *thd = ha_thd();
    tidesdb_trx_t *trx = get_or_create_trx(thd, ht, share->isolation_level);
    if (!trx) return HA_ERR_GENERIC;

    stmt_txn = trx->txn;
    if (unlikely(srv_debug_trace)) TDB_TRACE("txn from conn ctx took %lldus", tdb_now_us() - t0);
    return 0;
}

int ha_tidesdb::external_lock(THD *thd, int lock_type)
{
    DBUG_ENTER("ha_tidesdb::external_lock");

    if (lock_type != F_UNLCK)
    {
        /* Statement start (F_RDLCK or F_WRLCK).
           Get or create the per-connection txn and register with the
           server's transaction coordinator (InnoDB pattern). */
        tidesdb_trx_t *trx = get_or_create_trx(thd, ht, share->isolation_level);
        if (!trx) DBUG_RETURN(HA_ERR_GENERIC);

        stmt_txn = trx->txn;
        stmt_txn_dirty = false;

        /* Register at statement level (always) */
        trans_register_ha(thd, false, ht, 0);

        /* Register at transaction level if inside BEGIN block */
        if (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
        {
            trans_register_ha(thd, true, ht, 0);

            /* Savepoint for statement-level rollback is managed by
               tidesdb_commit(all=false) and tidesdb_rollback(all=false). */
        }
    }
    else
    {
        /* Statement end (F_UNLCK).
           The iterator holds a live pointer into the txn (iter->txn is
           dereferenced by seek/next).  In autocommit mode the txn will
           be freed by the upcoming hton->commit(all=true), so we must
           free the iterator here.

           Inside a multi-statement transaction (BEGIN...COMMIT) the txn
           survives across statements.  Keeping the iterator alive avoids
           the catastrophically expensive tidesdb_iter_new() on every
           statement -- iter_seek() reuses the cached merge heap and is
           orders of magnitude cheaper.  The iterator will be:
             -- reused by the next statement on the same CF/txn,
             -- invalidated by rnd_init/index_init if the txn changes,
             -- freed in close() when the handler is destroyed. */
        bool in_multi_stmt_txn = thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
        if (scan_iter && (!in_multi_stmt_txn || stmt_txn_dirty))
        {
            /* Free the iterator when:
               (a) autocommit                -- txn will be freed by hton->commit(all=true), or
               (b) this statement had writes -- the cached merge heap was
                   built from a snapshot of the txn write buffer at iter_new()
                   time (tidesdb_merge_source_from_txn_ops); subsequent writes
                   are invisible to the old heap, so the next scan must get a
                   fresh iterator to see its own writes. */
            tidesdb_iter_free(scan_iter);
            scan_iter = NULL;
            scan_iter_cf_ = NULL;
            scan_iter_txn_ = NULL;
        }
        stmt_txn = NULL;
        stmt_txn_dirty = false;
    }

    DBUG_RETURN(0);
}

THR_LOCK_DATA **ha_tidesdb::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
    /* With lock_count()=0 MariaDB skips THR_LOCK entirely.
       store_lock is still called for informational purposes but we
       do not push into the 'to' array (same pattern as InnoDB). */
    return to;
}

/* ******************** Online DDL ******************** */

/*
  Classify ALTER TABLE operations into INSTANT / INPLACE / COPY.

  INSTANT  -- metadata-only changes (.frm rewrite, no engine work):
             rename column/index, change default, change table options
  INPLACE  -- add/drop secondary indexes (create/drop CFs, populate)
  COPY     -- everything else (add/drop columns, change PK, type changes)
*/
enum_alter_inplace_result ha_tidesdb::check_if_supported_inplace_alter(
    TABLE *altered_table, Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::check_if_supported_inplace_alter");

    alter_table_operations flags = ha_alter_info->handler_flags;

    /* Operations that are pure metadata (INSTANT) */
    static const alter_table_operations TIDESDB_INSTANT =
        ALTER_COLUMN_NAME | ALTER_RENAME_COLUMN | ALTER_CHANGE_COLUMN_DEFAULT |
        ALTER_COLUMN_DEFAULT | ALTER_COLUMN_OPTION | ALTER_CHANGE_CREATE_OPTION |
        ALTER_DROP_CHECK_CONSTRAINT | ALTER_VIRTUAL_GCOL_EXPR | ALTER_RENAME | ALTER_RENAME_INDEX |
        ALTER_INDEX_IGNORABILITY;

    /* Operations we can do inplace (add/drop secondary indexes) */
    static const alter_table_operations TIDESDB_INPLACE_INDEX =
        ALTER_ADD_NON_UNIQUE_NON_PRIM_INDEX | ALTER_DROP_NON_UNIQUE_NON_PRIM_INDEX |
        ALTER_ADD_UNIQUE_INDEX | ALTER_DROP_UNIQUE_INDEX | ALTER_ADD_INDEX | ALTER_DROP_INDEX |
        ALTER_INDEX_ORDER;

    /* If only instant operations, return INSTANT */
    if (!(flags & ~TIDESDB_INSTANT)) DBUG_RETURN(HA_ALTER_INPLACE_INSTANT);

    /* If only instant + index operations, return INPLACE with no lock.
       TidesDB handles all concurrency via MVCC internally -- the index
       population scan runs inside its own transaction and does not need
       server-level MDL blocking. */
    if (!(flags & ~(TIDESDB_INSTANT | TIDESDB_INPLACE_INDEX)))
    {
        /* Changing PK requires full rebuild */
        if (flags & (ALTER_ADD_PK_INDEX | ALTER_DROP_PK_INDEX))
        {
            ha_alter_info->unsupported_reason = "TidesDB cannot change PRIMARY KEY inplace";
            DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
        }
        DBUG_RETURN(HA_ALTER_INPLACE_NO_LOCK);
    }

    /* Everything else requires COPY */
    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
}

/*
  Create CFs for newly added indexes.
  Called with shared MDL lock (concurrent DML is allowed).
*/
bool ha_tidesdb::prepare_inplace_alter_table(TABLE *altered_table,
                                             Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::prepare_inplace_alter_table");

    ha_tidesdb_inplace_ctx *ctx;
    try
    {
        ctx = new ha_tidesdb_inplace_ctx();
    }
    catch (...)
    {
        DBUG_RETURN(true);
    }
    ha_alter_info->handler_ctx = ctx;

    tidesdb_column_family_config_t cfg = build_cf_config(table->s->option_struct);

    std::string base_cf = share->cf_name;

    /* We create CFs for added indexes */
    if (ha_alter_info->index_add_count > 0)
    {
        for (uint a = 0; a < ha_alter_info->index_add_count; a++)
        {
            uint key_num = ha_alter_info->index_add_buffer[a];
            KEY *new_key = &ha_alter_info->key_info_buffer[key_num];

            /* We skip PK -- shouldn't happen (blocked in check_if_supported) */
            if (new_key->flags & HA_NOSAME &&
                altered_table->s->primary_key < altered_table->s->keys &&
                key_num == altered_table->s->primary_key)
                continue;

            std::string idx_cf = base_cf + CF_INDEX_INFIX + new_key->name.str;

            /* We drop stale CF if it exists from a previous failed ALTER */
            tidesdb_drop_column_family(tdb_global, idx_cf.c_str());

            int rc = tidesdb_create_column_family(tdb_global, idx_cf.c_str(), &cfg);
            if (rc != TDB_SUCCESS)
            {
                sql_print_error("TIDESDB: inplace ADD INDEX: failed to create CF '%s' (err=%d)",
                                idx_cf.c_str(), rc);
                my_error(ER_INTERNAL_ERROR, MYF(0), "TidesDB: failed to create index CF");
                DBUG_RETURN(true);
            }

            tidesdb_column_family_t *icf = tidesdb_get_column_family(tdb_global, idx_cf.c_str());
            if (!icf)
            {
                sql_print_error("TIDESDB: inplace ADD INDEX: CF '%s' not found after create",
                                idx_cf.c_str());
                my_error(ER_INTERNAL_ERROR, MYF(0), "TidesDB: index CF not found after create");
                DBUG_RETURN(true);
            }

            ctx->add_cfs.push_back(icf);
            ctx->add_cf_names.push_back(idx_cf);
            ctx->add_key_nums.push_back(key_num);
        }
    }

    /* We record CF names to drop for removed indexes */
    if (ha_alter_info->index_drop_count > 0)
    {
        for (uint d = 0; d < ha_alter_info->index_drop_count; d++)
        {
            KEY *old_key = ha_alter_info->index_drop_buffer[d];
            /* We find the key number in the old table */
            uint old_key_num = (uint)(old_key - table->key_info);
            if (old_key_num < share->idx_cf_names.size() &&
                !share->idx_cf_names[old_key_num].empty())
            {
                ctx->drop_cf_names.push_back(share->idx_cf_names[old_key_num]);
            }
        }
    }

    DBUG_RETURN(false);
}

/*
  Inplace phase -- we populate newly added indexes by scanning the table.
  Called with no MDL lock blocking (HA_ALTER_INPLACE_NO_LOCK).
*/
bool ha_tidesdb::inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::inplace_alter_table");

    ha_tidesdb_inplace_ctx *ctx = static_cast<ha_tidesdb_inplace_ctx *>(ha_alter_info->handler_ctx);

    if (!ctx || ctx->add_cfs.empty())
        DBUG_RETURN(false); /* Nothing to populate (drop-only or instant) */

    /* We mark all columns readable on the altered table since we read
       fields via make_sort_key_part during index key construction. */
    MY_BITMAP *old_map = tmp_use_all_columns(altered_table, &altered_table->read_set);

    TDB_TRACE("ENTER add_cfs=%u", (uint)ctx->add_cfs.size());

    /* We do a full table scan to populate the new secondary indexes.
       We use the altered_table's key_info for building index keys,
       since that matches the new key numbering. */

    tidesdb_txn_t *txn = NULL;
    int rc = tidesdb_txn_begin_with_isolation(tdb_global, share->isolation_level, &txn);
    if (rc != TDB_SUCCESS || !txn)
    {
        sql_print_error("TIDESDB: inplace ADD INDEX: txn_begin failed (err=%d)", rc);
        my_error(ER_INTERNAL_ERROR, MYF(0), "TidesDB: failed to begin txn for index build");
        tmp_restore_column_map(&altered_table->read_set, old_map);
        DBUG_RETURN(true);
    }

    tidesdb_iter_t *iter = NULL;
    rc = tidesdb_iter_new(txn, share->cf, &iter);
    if (rc != TDB_SUCCESS || !iter)
    {
        tidesdb_txn_free(txn);
        sql_print_error("TIDESDB: inplace ADD INDEX: iter_new failed (err=%d)", rc);
        my_error(ER_INTERNAL_ERROR, MYF(0), "TidesDB: failed to create iterator for index build");
        tmp_restore_column_map(&altered_table->read_set, old_map);
        DBUG_RETURN(true);
    }
    tidesdb_iter_seek_to_first(iter);

    ha_rows rows_processed = 0;

    /* We remember the last data key so we can seek directly to it after
       a batch commit, instead of walking from the beginning (O(n²)). */
    uchar last_data_key[MAX_KEY_LENGTH + 2];
    size_t last_data_key_len = 0;

    while (tidesdb_iter_valid(iter))
    {
        uint8_t *key_data = NULL;
        size_t key_size = 0;
        uint8_t *val_data = NULL;
        size_t val_size = 0;

        if (tidesdb_iter_key(iter, &key_data, &key_size) != TDB_SUCCESS ||
            tidesdb_iter_value(iter, &val_data, &val_size) != TDB_SUCCESS)
        {
            tidesdb_iter_next(iter);
            continue;
        }

        /* We skip non-data keys (meta namespace) */
        if (key_size < 1 || key_data[0] != KEY_NS_DATA)
        {
            tidesdb_iter_next(iter);
            continue;
        }

        /* We save this data key for potential batch re-seek */
        if (key_size <= sizeof(last_data_key))
        {
            memcpy(last_data_key, key_data, key_size);
            last_data_key_len = key_size;
        }

        /* We extract PK from the data key (skip KEY_NS_DATA prefix) */
        const uchar *pk = key_data + 1;
        uint pk_len = (uint)(key_size - 1);

        /* We decode the row into table->record[0].  The field pointers from
           altered_table->key_info will be temporarily repointed (via
           move_field_offset) to read from this buffer. */
        /* Always unpack using the packed row format */
        if (share->has_blobs || share->encrypted)
        {
            std::string row_data((const char *)val_data, val_size);
            deserialize_row(table->record[0], row_data);
        }
        else
        {
            deserialize_row(table->record[0], (const uchar *)val_data, val_size);
        }

        /* For each newly added index, we build and insert the index entry.
           altered_table->key_info fields have ptr into altered_table->record[0],
           but data is in table->record[0].

           We use move_field_offset with ptdiff = table->record[0] -- altered_table->record[0]
           to temporarily rebase field pointers (same pattern as make_comparable_key). */
        my_ptrdiff_t ptdiff = (my_ptrdiff_t)(table->record[0] - altered_table->record[0]);

        for (uint a = 0; a < ctx->add_cfs.size(); a++)
        {
            uint key_num = ctx->add_key_nums[a];
            KEY *ki = &altered_table->key_info[key_num];

            uchar ik[MAX_KEY_LENGTH * 2 + 2];
            uint pos = 0;
            for (uint p = 0; p < ki->user_defined_key_parts; p++)
            {
                KEY_PART_INFO *kp = &ki->key_part[p];
                Field *field = kp->field;

                /* make_sort_key_part handles nullable fields internally:
                   writes 1-byte null indicator + kp->length sort bytes. */
                field->move_field_offset(ptdiff);
                field->make_sort_key_part(ik + pos, kp->length);
                field->move_field_offset(-ptdiff);
                pos += kp->length;
                if (field->real_maybe_null()) pos++;
            }
            /* We append PK to make the key unique */
            memcpy(ik + pos, pk, pk_len);
            pos += pk_len;

            rc =
                tidesdb_txn_put(txn, ctx->add_cfs[a], ik, pos, &tdb_empty_val, 1, TIDESDB_TTL_NONE);
            if (rc != TDB_SUCCESS)
            {
                sql_print_error("TIDESDB: inplace ADD INDEX: put failed for key %u (err=%d)",
                                key_num, rc);
                /* Continue -- best effort for remaining rows */
            }
        }

        rows_processed++;

        /* We commit in batches to avoid unbounded txn buffer growth */
        if (rows_processed % TIDESDB_INDEX_BUILD_BATCH == 0)
        {
            tidesdb_txn_commit(txn);
            tidesdb_txn_free(txn);
            tidesdb_iter_free(iter);

            txn = NULL;
            rc = tidesdb_txn_begin_with_isolation(tdb_global, share->isolation_level, &txn);
            if (rc != TDB_SUCCESS || !txn)
            {
                sql_print_error("TIDESDB: inplace ADD INDEX: batch txn_begin failed");
                my_error(ER_INTERNAL_ERROR, MYF(0), "TidesDB: batch txn failed during index build");
                tmp_restore_column_map(&altered_table->read_set, old_map);
                DBUG_RETURN(true);
            }
            iter = NULL;
            rc = tidesdb_iter_new(txn, share->cf, &iter);
            if (rc != TDB_SUCCESS || !iter)
            {
                tidesdb_txn_free(txn);
                my_error(ER_INTERNAL_ERROR, MYF(0),
                         "TidesDB: batch iter failed during index build");
                tmp_restore_column_map(&altered_table->read_set, old_map);
                DBUG_RETURN(true);
            }
            /* We seek directly to the last processed key and advance past it,
               instead of seeking to first and skipping N rows (O(n²)). */
            tidesdb_iter_seek(iter, last_data_key, last_data_key_len);
            if (tidesdb_iter_valid(iter)) tidesdb_iter_next(iter);
            continue; /* Don't call iter_next again */
        }

        tidesdb_iter_next(iter);
    }

    tidesdb_iter_free(iter);

    /* We commit remaining entries */
    rc = tidesdb_txn_commit(txn);
    tidesdb_txn_free(txn);

    if (rc != TDB_SUCCESS)
    {
        sql_print_error("TIDESDB: inplace ADD INDEX: final commit failed (err=%d)", rc);
        my_error(ER_INTERNAL_ERROR, MYF(0), "TidesDB: final commit failed during index build");
        tmp_restore_column_map(&altered_table->read_set, old_map);
        DBUG_RETURN(true);
    }

    sql_print_information("TIDESDB: inplace ADD INDEX: populated %llu rows into %u new index(es)",
                          (unsigned long long)rows_processed, (uint)ctx->add_cfs.size());

    tmp_restore_column_map(&altered_table->read_set, old_map);
    DBUG_RETURN(false);
}

/*
  Commit or rollback the inplace ALTER.
  On commit    -- drop old index CFs, update share->idx_cfs for new table shape.
  On rollback  -- drop newly created CFs.
*/
bool ha_tidesdb::commit_inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info,
                                            bool commit)
{
    DBUG_ENTER("ha_tidesdb::commit_inplace_alter_table");

    ha_tidesdb_inplace_ctx *ctx = static_cast<ha_tidesdb_inplace_ctx *>(ha_alter_info->handler_ctx);

    ha_alter_info->group_commit_ctx = NULL;

    if (!ctx) DBUG_RETURN(false);

    if (!commit)
    {
        /* Rollback -- we drop any CFs we created for new indexes */
        for (const auto &cf_name : ctx->add_cf_names)
            tidesdb_drop_column_family(tdb_global, cf_name.c_str());
        DBUG_RETURN(false);
    }

    /* Commit -- we drop CFs for removed indexes */
    for (const auto &cf_name : ctx->drop_cf_names)
    {
        int rc = tidesdb_drop_column_family(tdb_global, cf_name.c_str());
        if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
            sql_print_warning("TIDESDB: commit ALTER: failed to drop CF '%s' (err=%d)",
                              cf_name.c_str(), rc);
    }

    /* We rebuild share->idx_cfs and idx_cf_names based on the new table's keys.
       Since we hold exclusive MDL, no other handler is using the share. */
    lock_shared_ha_data();
    share->idx_cfs.clear();
    share->idx_cf_names.clear();

    TDB_TRACE("COMMIT rebuild idx_cfs: altered keys=%u pk=%u", altered_table->s->keys,
              altered_table->s->primary_key);

    uint new_pk = altered_table->s->primary_key;
    for (uint i = 0; i < altered_table->s->keys; i++)
    {
        if (new_pk != MAX_KEY && i == new_pk)
        {
            share->idx_cfs.push_back(NULL);
            share->idx_cf_names.push_back("");
            TDB_TRACE("  key[%u] = PRIMARY (NULL cf)", i);
            continue;
        }
        std::string idx_name;
        tidesdb_column_family_t *icf = resolve_idx_cf(
            tdb_global, share->cf_name, altered_table->key_info[i].name.str, idx_name);
        share->idx_cfs.push_back(icf);
        share->idx_cf_names.push_back(idx_name);
        TDB_TRACE("  key[%u] = %s cf=%p cf_name=%s", i, altered_table->key_info[i].name.str,
                  (void *)icf, idx_name.c_str());
    }

    /* We recompute cached index metadata for the new table shape */
    for (uint i = 0; i < altered_table->s->keys; i++)
        share->idx_comp_key_len[i] = comparable_key_length(&altered_table->key_info[i]);
    share->num_secondary_indexes = 0;
    for (uint i = 0; i < share->idx_cfs.size(); i++)
        if (share->idx_cfs[i]) share->num_secondary_indexes++;

    /* We force a stats refresh on next info() call */
    share->stats_refresh_us.store(0, std::memory_order_relaxed);
    unlock_shared_ha_data();

    DBUG_RETURN(false);
}

/*
  Tell MariaDB whether changing table options requires a rebuild.
  For TidesDB, changing options like SYNC_MODE, TTL, etc. is always
  compatible -- the .frm is rewritten and re-read on next open().
*/
bool ha_tidesdb::check_if_incompatible_data(HA_CREATE_INFO *create_info, uint table_changes)
{
    /* If only table options changed (not column types), data is compatible */
    if (table_changes == IS_EQUAL_YES) return COMPATIBLE_DATA_YES;
    return COMPATIBLE_DATA_NO;
}

/* ******************** rename_table (ALTER TABLE / RENAME) ******************** */

int ha_tidesdb::rename_table(const char *from, const char *to)
{
    DBUG_ENTER("ha_tidesdb::rename_table");

    std::string old_cf = path_to_cf_name(from);
    std::string new_cf = path_to_cf_name(to);

    /* If the destination CF already exists (stale from a previous ALTER),
       drop it first so the rename can proceed. */
    tidesdb_drop_column_family(tdb_global, new_cf.c_str());

    /* We rename main data CF */
    int rc = tidesdb_rename_column_family(tdb_global, old_cf.c_str(), new_cf.c_str());
    if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
    {
        sql_print_error("TIDESDB: Failed to rename CF '%s' -> '%s' (err=%d)", old_cf.c_str(),
                        new_cf.c_str(), rc);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    /* We rename secondary index CFs by enumerating all CFs with the old prefix. */
    {
        std::string prefix = old_cf + CF_INDEX_INFIX;
        char **names = NULL;
        int count = 0;
        if (tidesdb_list_column_families(tdb_global, &names, &count) == TDB_SUCCESS && names)
        {
            for (int i = 0; i < count; i++)
            {
                if (!names[i]) continue;
                std::string cf_str(names[i]);
                if (cf_str.compare(0, prefix.size(), prefix) == 0)
                {
                    /* We replace old table prefix with new one */
                    std::string suffix = cf_str.substr(prefix.size());
                    std::string new_idx = new_cf + CF_INDEX_INFIX + suffix;

                    tidesdb_drop_column_family(tdb_global, new_idx.c_str());
                    rc = tidesdb_rename_column_family(tdb_global, cf_str.c_str(), new_idx.c_str());
                    if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
                        sql_print_error("TIDESDB: Failed to rename idx CF '%s' -> '%s' (err=%d)",
                                        cf_str.c_str(), new_idx.c_str(), rc);
                }
                free(names[i]);
            }
            free(names);
        }
    }

    DBUG_RETURN(0);
}

/* ******************** delete_table (DROP TABLE) ******************** */

/*
  Force-remove a directory tree from disk.  Used as a safety net after
  tidesdb_drop_column_family() because the library's internal
  remove_directory() can fail silently (e.g. open fds from block cache,
  mmap, or background workers).  If stale SSTables survive, the next
  CREATE TABLE with the same name inherits them -- catastrophic for
  performance (bloom filters pass on every SSTable since keys overlap).
*/
static void force_remove_cf_dir(const std::string &cf_name)
{
    char dir[FN_REFLEN];
    const char sep[] = {FN_LIBCHAR, 0};
    strxnmov(dir, sizeof(dir) - 1, tdb_path.c_str(), sep, cf_name.c_str(), NullS);

    MY_STAT st;
    if (!my_stat(dir, &st, MYF(0))) return; /* already gone */

    /* my_rmtree() is MariaDB's portable recursive directory removal
       (handles Windows, symlinks, read-only attrs, etc.). */
    if (my_rmtree(dir, MYF(0)) != 0)
        sql_print_warning("TIDESDB: force_remove_cf_dir failed for %s", dir);
    else
        TDB_TRACE("force-removed stale CF dir %s", dir);
}

int ha_tidesdb::delete_table(const char *name)
{
    DBUG_ENTER("ha_tidesdb::delete_table");

    std::string cf_name = path_to_cf_name(name);

    /* We collect secondary index CF names before dropping so we can
       force-remove their directories afterwards. */
    std::vector<std::string> idx_cf_names;
    {
        std::string prefix = cf_name + CF_INDEX_INFIX;
        char **names = NULL;
        int count = 0;
        if (tidesdb_list_column_families(tdb_global, &names, &count) == TDB_SUCCESS && names)
        {
            for (int i = 0; i < count; i++)
            {
                if (!names[i]) continue;
                if (strncmp(names[i], prefix.c_str(), prefix.size()) == 0)
                    idx_cf_names.push_back(names[i]);
                free(names[i]);
            }
            free(names);
        }
    }

    int rc = tidesdb_drop_column_family(tdb_global, cf_name.c_str());
    if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
    {
        sql_print_error("TIDESDB: Failed to drop CF '%s' (err=%d)", cf_name.c_str(), rc);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    for (const auto &idx_name : idx_cf_names)
        tidesdb_drop_column_family(tdb_global, idx_name.c_str());

    /* We force-remove CF directories from disk in case the
       library's internal remove_directory() failed silently.  Without
       this, the next CREATE TABLE reuses the stale directory and
       inherits all old SSTables -- making reads scan 100s of SSTables
       with overlapping keys (bloom filters useless). */
    force_remove_cf_dir(cf_name);
    for (const auto &idx_name : idx_cf_names) force_remove_cf_dir(idx_name);

    DBUG_RETURN(0);
}

/* ******************** Plugin declaration ******************** */

static struct st_mysql_storage_engine tidesdb_storage_engine = {MYSQL_HANDLERTON_INTERFACE_VERSION};

maria_declare_plugin(tidesdb){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &tidesdb_storage_engine,
    "TIDESDB",
    "TidesDB",
    "Supports ACID transactions, lock-free concurrency, indexing, and encryption for tables",
    PLUGIN_LICENSE_GPL,
    tidesdb_init_func,
    tidesdb_deinit_func,
    0x30200,
    NULL,
    tidesdb_system_variables,
    "3.2.0",
    MariaDB_PLUGIN_MATURITY_EXPERIMENTAL} maria_declare_plugin_end;
