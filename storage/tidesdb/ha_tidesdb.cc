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
  
  TidesDB is an LSM-based embedded key-value store with:
  - Multi-column families (one per MySQL table)
  - Atomic transactions with 5 isolation levels
  - Lockless operations using atomics
  - Compression (LZ4, Zstd, Snappy)
  - Bloom filters and block indexes
  
  @details
  Each MySQL table maps to a TidesDB column family.
  Rows are stored as: primary_key -> serialized_row_data
  
  For tables without explicit primary keys, we generate a hidden
  auto-increment key.
*/

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#define MYSQL_SERVER 1
#include "mysql_priv.h"

#include "ha_tidesdb.h"
#include <mysql/plugin.h>

/* Forward declarations */
static handler *tidesdb_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root);
static int tidesdb_init_func(void *p);
static int tidesdb_done_func(void *p);
static int tidesdb_commit(handlerton *hton, THD *thd, bool all, bool async);
static int tidesdb_rollback(handlerton *hton, THD *thd, bool all);
static bool tidesdb_show_status(handlerton *hton, THD *thd, 
                                stat_print_fn *stat_print, 
                                enum ha_stat_type stat_type);

/* Global TidesDB instance - one database for all tables */
static tidesdb_t *tidesdb_instance = NULL;
static pthread_mutex_t tidesdb_mutex;

/* Handlerton for TidesDB */
handlerton *tidesdb_hton;

/* Hash for tracking open tables */
static HASH tidesdb_open_tables;

/* Data directory for TidesDB */
static char *tidesdb_data_dir = NULL;

/* System variables */
static ulong tidesdb_flush_threads = 2;
static ulong tidesdb_compaction_threads = 2;
static ulonglong tidesdb_block_cache_size = 64 * 1024 * 1024;  /* 64MB */
static ulonglong tidesdb_write_buffer_size = 64 * 1024 * 1024; /* 64MB */
static my_bool tidesdb_enable_compression = TRUE;
static my_bool tidesdb_enable_bloom_filter = TRUE;

/* Compression algorithm: 0=none, 1=snappy, 2=lz4, 3=zstd, 4=lz4_fast */
static ulong tidesdb_compression_algo = 2;  /* LZ4 default */
static const char *tidesdb_compression_names[] = {
  "none", "snappy", "lz4", "zstd", "lz4_fast", NullS
};
static TYPELIB tidesdb_compression_typelib = {
  array_elements(tidesdb_compression_names) - 1,
  "tidesdb_compression_typelib",
  tidesdb_compression_names,
  NULL
};

/* Sync mode: 0=none, 1=interval, 2=full */
static ulong tidesdb_sync_mode = 1;  /* interval default */
static const char *tidesdb_sync_mode_names[] = {
  "none", "interval", "full", NullS
};
static TYPELIB tidesdb_sync_mode_typelib = {
  array_elements(tidesdb_sync_mode_names) - 1,
  "tidesdb_sync_mode_typelib",
  tidesdb_sync_mode_names,
  NULL
};

/* Sync interval in microseconds (for interval mode) */
static ulonglong tidesdb_sync_interval_us = 128000;  /* 128ms default */

/* Bloom filter false positive rate (0.0 to 1.0) */
static double tidesdb_bloom_fpr = 0.01;  /* 1% default */

/* Default isolation level: 0=read_uncommitted, 1=read_committed, 2=repeatable_read, 3=snapshot, 4=serializable */
static ulong tidesdb_default_isolation = 1;  /* read_committed default */
static const char *tidesdb_isolation_names[] = {
  "read_uncommitted", "read_committed", "repeatable_read", "snapshot", "serializable", NullS
};
static TYPELIB tidesdb_isolation_typelib = {
  array_elements(tidesdb_isolation_names) - 1,
  "tidesdb_isolation_typelib",
  tidesdb_isolation_names,
  NULL
};

/* Level size ratio for LSM compaction */
static ulong tidesdb_level_size_ratio = 10;

/* Skip list configuration */
static ulong tidesdb_skip_list_max_level = 12;

/* Block index configuration */
static my_bool tidesdb_enable_block_indexes = TRUE;
static ulong tidesdb_index_sample_ratio = 1;

/* Default TTL in seconds (0 = no expiration) */
static ulonglong tidesdb_default_ttl = 0;

/* Log level: 0=debug, 1=info, 2=warn, 3=error, 4=fatal, 5=none */
static ulong tidesdb_log_level = 1;  /* info default */
static const char *tidesdb_log_level_names[] = {
  "debug", "info", "warn", "error", "fatal", "none", NullS
};
static TYPELIB tidesdb_log_level_typelib = {
  array_elements(tidesdb_log_level_names) - 1,
  "tidesdb_log_level_typelib",
  tidesdb_log_level_names,
  NULL
};

/**
  @brief
  Function to get key from share for hash lookup.
*/
static uchar* tidesdb_get_key(TIDESDB_SHARE *share, size_t *length,
                              my_bool not_used __attribute__((unused)))
{
  *length = share->table_name_length;
  return (uchar*) share->table_name;
}

/**
  @brief
  Get or create a share for the given table name.
*/
static TIDESDB_SHARE *get_share(const char *table_name, TABLE *table)
{
  TIDESDB_SHARE *share;
  uint length;
  char *tmp_name;

  pthread_mutex_lock(&tidesdb_mutex);
  length = (uint) strlen(table_name);

  if (!(share = (TIDESDB_SHARE*) hash_search(&tidesdb_open_tables,
                                              (uchar*) table_name,
                                              length)))
  {
    if (!(share = (TIDESDB_SHARE *)
          my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                          &share, sizeof(*share),
                          &tmp_name, length + 1,
                          NullS)))
    {
      pthread_mutex_unlock(&tidesdb_mutex);
      return NULL;
    }

    share->use_count = 0;
    share->table_name_length = length;
    share->table_name = tmp_name;
    strmov(share->table_name, table_name);
    share->cf = NULL;
    share->has_primary_key = false;
    share->pk_parts = 0;
    
    if (my_hash_insert(&tidesdb_open_tables, (uchar*) share))
      goto error;
    thr_lock_init(&share->lock);
    pthread_mutex_init(&share->mutex, MY_MUTEX_INIT_FAST);
  }
  share->use_count++;
  pthread_mutex_unlock(&tidesdb_mutex);

  return share;

error:
  pthread_mutex_destroy(&share->mutex);
  my_free(share, MYF(0));
  pthread_mutex_unlock(&tidesdb_mutex);
  return NULL;
}

/**
  @brief
  Free a share when no longer needed.
*/
static int free_share(TIDESDB_SHARE *share)
{
  pthread_mutex_lock(&tidesdb_mutex);
  if (!--share->use_count)
  {
    hash_delete(&tidesdb_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    pthread_mutex_destroy(&share->mutex);
    /* Note: We don't drop the column family here, just release the share */
    my_free(share, MYF(0));
  }
  pthread_mutex_unlock(&tidesdb_mutex);

  return 0;
}

/**
  @brief
  Extract the column family name from a full table path.
  Converts "database/table" to "database_table" for CF name.
*/
static void get_cf_name(const char *table_path, char *cf_name, size_t cf_name_len)
{
  const char *db_start = table_path;
  const char *tbl_start = NULL;
  
  /* Find the database and table parts */
  /* Path format: /path/to/datadir/database/table */
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
}

/**
  @brief
  Initialize the TidesDB storage engine.
*/
static int tidesdb_init_func(void *p)
{
  DBUG_ENTER("tidesdb_init_func");

  tidesdb_hton = (handlerton *)p;
  
  VOID(pthread_mutex_init(&tidesdb_mutex, MY_MUTEX_INIT_FAST));
  (void) hash_init(&tidesdb_open_tables, system_charset_info, 32, 0, 0,
                   (hash_get_key) tidesdb_get_key, 0, 0);

  tidesdb_hton->state = SHOW_OPTION_YES;
  tidesdb_hton->create = tidesdb_create_handler;
  tidesdb_hton->flags = HTON_CAN_RECREATE | HTON_CLOSE_CURSORS_AT_COMMIT;
  tidesdb_hton->commit = tidesdb_commit;
  tidesdb_hton->rollback = tidesdb_rollback;
  tidesdb_hton->show_status = tidesdb_show_status;
  
  /* Initialize TidesDB instance */
  char db_path[FN_REFLEN];
  if (tidesdb_data_dir)
  {
    strncpy(db_path, tidesdb_data_dir, sizeof(db_path) - 1);
  }
  else
  {
    /* Default to MySQL data directory + tidesdb */
    snprintf(db_path, sizeof(db_path), "%s/tidesdb", mysql_data_home);
  }
  
  tidesdb_config_t config = {
    .db_path = db_path,
    .num_flush_threads = (int)tidesdb_flush_threads,
    .num_compaction_threads = (int)tidesdb_compaction_threads,
    .log_level = (int)tidesdb_log_level,
    .block_cache_size = tidesdb_block_cache_size,
    .max_open_sstables = 256
  };
  
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

  if (tidesdb_open_tables.records)
    error = 1;
  
  hash_free(&tidesdb_open_tables);
  pthread_mutex_destroy(&tidesdb_mutex);
  
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
  Commit a transaction.
*/
static int tidesdb_commit(handlerton *hton, THD *thd, bool all, bool async)
{
  DBUG_ENTER("tidesdb_commit");
  /* Transaction commit is handled per-statement in external_lock */
  DBUG_RETURN(0);
}

/**
  @brief
  Rollback a transaction.
*/
static int tidesdb_rollback(handlerton *hton, THD *thd, bool all)
{
  DBUG_ENTER("tidesdb_rollback");
  /* Transaction rollback is handled per-statement in external_lock */
  DBUG_RETURN(0);
}

/**
  @brief
  Show TidesDB engine status.
  
  Called by SHOW ENGINE TIDESDB STATUS.
*/
static bool tidesdb_show_status(handlerton *hton, THD *thd,
                                stat_print_fn *stat_print,
                                enum ha_stat_type stat_type)
{
  DBUG_ENTER("tidesdb_show_status");
  
  if (stat_type != HA_ENGINE_STATUS)
  {
    DBUG_RETURN(FALSE);
  }
  
  char buf[4096];
  int buf_len = 0;
  
  /* Get block cache statistics */
  tidesdb_cache_stats_t cache_stats;
  if (tidesdb_get_cache_stats(tidesdb_instance, &cache_stats) == TDB_SUCCESS)
  {
    buf_len += snprintf(buf + buf_len, sizeof(buf) - buf_len,
      "=====================================\n"
      "TidesDB Engine Status\n"
      "=====================================\n\n"
      "Block Cache:\n"
      "  Enabled: %s\n"
      "  Total entries: %zu\n"
      "  Total bytes: %.2f MB\n"
      "  Hits: %lu\n"
      "  Misses: %lu\n"
      "  Hit rate: %.1f%%\n"
      "  Partitions: %zu\n\n",
      cache_stats.enabled ? "yes" : "no",
      cache_stats.total_entries,
      cache_stats.total_bytes / (1024.0 * 1024.0),
      cache_stats.hits,
      cache_stats.misses,
      cache_stats.hit_rate * 100.0,
      cache_stats.num_partitions);
  }
  
  /* Configuration summary */
  buf_len += snprintf(buf + buf_len, sizeof(buf) - buf_len,
    "Configuration:\n"
    "  Flush threads: %lu\n"
    "  Compaction threads: %lu\n"
    "  Block cache size: %.2f MB\n"
    "  Write buffer size: %.2f MB\n"
    "  Compression: %s (%s)\n"
    "  Bloom filter: %s (FPR: %.2f%%)\n"
    "  Sync mode: %s\n"
    "  Default isolation: %s\n"
    "  Default TTL: %llu seconds\n\n",
    tidesdb_flush_threads,
    tidesdb_compaction_threads,
    tidesdb_block_cache_size / (1024.0 * 1024.0),
    tidesdb_write_buffer_size / (1024.0 * 1024.0),
    tidesdb_enable_compression ? "enabled" : "disabled",
    tidesdb_compression_names[tidesdb_compression_algo],
    tidesdb_enable_bloom_filter ? "enabled" : "disabled",
    tidesdb_bloom_fpr * 100.0,
    tidesdb_sync_mode_names[tidesdb_sync_mode],
    tidesdb_isolation_names[tidesdb_default_isolation],
    (unsigned long long)tidesdb_default_ttl);
  
  /* List column families (open tables) */
  buf_len += snprintf(buf + buf_len, sizeof(buf) - buf_len,
    "Open Tables: %lu\n",
    (unsigned long)tidesdb_open_tables.records);
  
  stat_print(thd, "TidesDB", 7, "", 0, buf, buf_len);
  
  DBUG_RETURN(FALSE);
}

/**
  @brief
  Create a new handler instance.
*/
static handler* tidesdb_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) ha_tidesdb(hton, table);
}

/* File extensions for TidesDB tables */
static const char *ha_tidesdb_exts[] = {
  NullS
};

const char **ha_tidesdb::bas_ext() const
{
  return ha_tidesdb_exts;
}

/**
  @brief
  Constructor for ha_tidesdb handler.
*/
ha_tidesdb::ha_tidesdb(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg),
   share(NULL),
   current_txn(NULL),
   scan_iter(NULL),
   scan_initialized(false),
   pk_buffer(NULL),
   pk_buffer_len(0),
   row_buffer(NULL),
   row_buffer_len(0),
   current_key(NULL),
   current_key_len(0)
{
}

/**
  @brief
  Destructor for ha_tidesdb handler.
*/
ha_tidesdb::~ha_tidesdb()
{
  free_current_key();
  if (pk_buffer)
    my_free(pk_buffer, MYF(0));
  if (row_buffer)
    my_free(row_buffer, MYF(0));
}

/**
  @brief
  Free the current key buffer.
*/
void ha_tidesdb::free_current_key()
{
  if (current_key)
  {
    my_free(current_key, MYF(0));
    current_key = NULL;
    current_key_len = 0;
  }
}

/**
  @brief
  Pack a MySQL row into a byte buffer for storage.
  
  Uses MySQL's native row format for simplicity.
*/
int ha_tidesdb::pack_row(uchar *buf, uchar **packed, size_t *packed_len)
{
  DBUG_ENTER("ha_tidesdb::pack_row");
  
  /* For now, store the entire row buffer as-is */
  /* This is the simplest approach - MySQL's internal format */
  size_t row_len = table->s->reclength;
  
  if (row_buffer_len < row_len)
  {
    if (row_buffer)
      my_free(row_buffer, MYF(0));
    row_buffer = (uchar *)my_malloc(row_len, MYF(MY_WME));
    if (!row_buffer)
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    row_buffer_len = row_len;
  }
  
  memcpy(row_buffer, buf, row_len);
  *packed = row_buffer;
  *packed_len = row_len;
  
  DBUG_RETURN(0);
}

/**
  @brief
  Unpack a stored row back into MySQL's row buffer.
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
  
  DBUG_RETURN(0);
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
    /* Table has a primary key - extract it */
    KEY *pk = &table->key_info[table->s->primary_key];
    uint pk_len = pk->key_length;
    
    if (pk_buffer_len < pk_len)
    {
      if (pk_buffer)
        my_free(pk_buffer, MYF(0));
      pk_buffer = (uchar *)my_malloc(pk_len, MYF(MY_WME));
      if (!pk_buffer)
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      pk_buffer_len = pk_len;
    }
    
    /* Use MySQL's key_copy to extract the key */
    key_copy(pk_buffer, (uchar *)buf, pk, pk_len);
    
    *key = pk_buffer;
    *key_len = pk_len;
  }
  else
  {
    /* No primary key - use hidden auto-increment */
    DBUG_RETURN(build_hidden_pk(key, key_len));
  }
  
  DBUG_RETURN(0);
}

/**
  @brief
  Build a hidden primary key for tables without explicit PK.
  Uses an auto-incrementing 8-byte integer.
*/
int ha_tidesdb::build_hidden_pk(uchar **key, size_t *key_len)
{
  DBUG_ENTER("ha_tidesdb::build_hidden_pk");
  
  /* For hidden PK, we use a simple incrementing counter */
  /* In production, this should be persisted and atomic */
  static ulonglong hidden_pk_counter = 0;
  
  if (pk_buffer_len < 8)
  {
    if (pk_buffer)
      my_free(pk_buffer, MYF(0));
    pk_buffer = (uchar *)my_malloc(8, MYF(MY_WME));
    if (!pk_buffer)
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    pk_buffer_len = 8;
  }
  
  ulonglong pk_val = __sync_add_and_fetch(&hidden_pk_counter, 1);
  int8store(pk_buffer, pk_val);
  
  *key = pk_buffer;
  *key_len = 8;
  
  DBUG_RETURN(0);
}

/**
  @brief
  Open a table.
*/
int ha_tidesdb::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_tidesdb::open");

  if (!(share = get_share(name, table)))
    DBUG_RETURN(1);
  
  thr_lock_data_init(&share->lock, &lock, NULL);
  
  /* Get the column family for this table */
  if (!share->cf)
  {
    char cf_name[256];
    get_cf_name(name, cf_name, sizeof(cf_name));
    
    share->cf = tidesdb_get_column_family(tidesdb_instance, cf_name);
    if (!share->cf)
    {
      /* Column family doesn't exist - table wasn't created properly */
      free_share(share);
      DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
    }
  }
  
  /* Check if table has a primary key */
  share->has_primary_key = (table->s->primary_key != MAX_KEY);
  if (share->has_primary_key)
  {
    share->pk_parts = table->key_info[table->s->primary_key].key_parts;
  }
  
  /* Set ref_length for position() */
  if (share->has_primary_key)
  {
    ref_length = table->key_info[table->s->primary_key].key_length;
  }
  else
  {
    ref_length = 8;  /* Hidden 8-byte PK */
  }
  
  /* Check for TTL column (_tidesdb_ttl or _ttl) */
  share->ttl_field_index = -1;
  for (uint i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    const char *field_name = field->field_name;
    if (strcasecmp(field_name, "_tidesdb_ttl") == 0 ||
        strcasecmp(field_name, "_ttl") == 0)
    {
      /* Found TTL column - must be an integer type */
      if (field->type() == MYSQL_TYPE_LONG ||
          field->type() == MYSQL_TYPE_LONGLONG ||
          field->type() == MYSQL_TYPE_INT24 ||
          field->type() == MYSQL_TYPE_SHORT ||
          field->type() == MYSQL_TYPE_TINY)
      {
        share->ttl_field_index = i;
        sql_print_information("TidesDB: Table '%s' has TTL column at index %d", 
                              name, i);
      }
      break;
    }
  }

  DBUG_RETURN(0);
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
  
  DBUG_RETURN(free_share(share));
}

/**
  @brief
  Create a new table (column family in TidesDB).
*/
int ha_tidesdb::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_tidesdb::create");
  
  char cf_name[256];
  get_cf_name(name, cf_name, sizeof(cf_name));
  
  /* Configure the column family */
  tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
  cf_config.write_buffer_size = tidesdb_write_buffer_size;
  cf_config.enable_bloom_filter = tidesdb_enable_bloom_filter ? 1 : 0;
  cf_config.bloom_fpr = tidesdb_bloom_fpr;
  cf_config.level_size_ratio = tidesdb_level_size_ratio;
  cf_config.skip_list_max_level = tidesdb_skip_list_max_level;
  cf_config.enable_block_indexes = tidesdb_enable_block_indexes ? 1 : 0;
  cf_config.index_sample_ratio = tidesdb_index_sample_ratio;
  cf_config.sync_mode = tidesdb_sync_mode;
  cf_config.sync_interval_us = tidesdb_sync_interval_us;
  cf_config.default_isolation_level = tidesdb_default_isolation;
  
  /* Set compression algorithm */
  if (tidesdb_enable_compression)
  {
    cf_config.compression_algo = tidesdb_compression_algo;
  }
  else
  {
    cf_config.compression_algo = NO_COMPRESSION;
  }
  
  /* Create the column family */
  int ret = tidesdb_create_column_family(tidesdb_instance, cf_name, &cf_config);
  if (ret != TDB_SUCCESS && ret != TDB_ERR_EXISTS)
  {
    sql_print_error("TidesDB: Failed to create column family '%s': %d", cf_name, ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }
  
  sql_print_information("TidesDB: Created table '%s' (column family: %s)", name, cf_name);
  
  DBUG_RETURN(0);
}

/**
  @brief
  Delete a table (drop column family in TidesDB).
*/
int ha_tidesdb::delete_table(const char *name, my_bool delayed_drop)
{
  DBUG_ENTER("ha_tidesdb::delete_table");
  
  char cf_name[256];
  get_cf_name(name, cf_name, sizeof(cf_name));
  
  int ret = tidesdb_drop_column_family(tidesdb_instance, cf_name);
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
  Rename a table.
*/
int ha_tidesdb::rename_table(const char *from, const char *to)
{
  DBUG_ENTER("ha_tidesdb::rename_table");
  
  /* TidesDB doesn't support renaming column families directly */
  /* We would need to copy all data to a new CF and drop the old one */
  /* For now, return an error */
  sql_print_error("TidesDB: Rename table not yet supported");
  
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  Write a row to the table.
*/
int ha_tidesdb::write_row(uchar *buf)
{
  DBUG_ENTER("ha_tidesdb::write_row");
  
  int ret;
  uchar *key;
  size_t key_len;
  uchar *value;
  size_t value_len;
  
  /* Build the primary key */
  ret = build_primary_key(buf, &key, &key_len);
  if (ret)
    DBUG_RETURN(ret);
  
  /* Pack the row data */
  ret = pack_row(buf, &value, &value_len);
  if (ret)
    DBUG_RETURN(ret);
  
  /* Begin a transaction if we don't have one */
  tidesdb_txn_t *txn = NULL;
  bool own_txn = false;
  
  if (current_txn)
  {
    txn = current_txn;
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
  
  /* Calculate TTL from _ttl column or use default */
  time_t ttl = -1;
  if (share->ttl_field_index >= 0)
  {
    /* Extract TTL from the designated column */
    Field *ttl_field = table->field[share->ttl_field_index];
    if (!ttl_field->is_null())
    {
      longlong ttl_seconds = ttl_field->val_int();
      if (ttl_seconds > 0)
      {
        ttl = time(NULL) + ttl_seconds;
      }
      else if (ttl_seconds == 0)
      {
        /* TTL of 0 means no expiration */
        ttl = -1;
      }
    }
  }
  else if (tidesdb_default_ttl > 0)
  {
    /* Fall back to global default TTL */
    ttl = time(NULL) + tidesdb_default_ttl;
  }
  
  /* Insert the row */
  ret = tidesdb_txn_put(txn, share->cf, key, key_len, value, value_len, ttl);
  if (ret != TDB_SUCCESS)
  {
    if (own_txn)
    {
      tidesdb_txn_rollback(txn);
      tidesdb_txn_free(txn);
    }
    
    if (ret == TDB_ERR_EXISTS)
      DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    
    sql_print_error("TidesDB: Failed to write row: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }
  
  /* Commit if we own the transaction */
  if (own_txn)
  {
    ret = tidesdb_txn_commit(txn);
    tidesdb_txn_free(txn);
    
    if (ret != TDB_SUCCESS)
    {
      sql_print_error("TidesDB: Failed to commit transaction: %d", ret);
      DBUG_RETURN(HA_ERR_GENERIC);
    }
  }
  
  stats.records++;
  
  DBUG_RETURN(0);
}

/**
  @brief
  Update a row.
*/
int ha_tidesdb::update_row(const uchar *old_data, uchar *new_data)
{
  DBUG_ENTER("ha_tidesdb::update_row");
  
  int ret;
  uchar *old_key;
  size_t old_key_len;
  uchar *new_key;
  size_t new_key_len;
  uchar *value;
  size_t value_len;
  
  /* Build keys for old and new rows */
  ret = build_primary_key(old_data, &old_key, &old_key_len);
  if (ret)
    DBUG_RETURN(ret);
  
  /* Save old key since build_primary_key reuses the buffer */
  uchar *saved_old_key = (uchar *)my_malloc(old_key_len, MYF(MY_WME));
  if (!saved_old_key)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  memcpy(saved_old_key, old_key, old_key_len);
  
  ret = build_primary_key(new_data, &new_key, &new_key_len);
  if (ret)
  {
    my_free(saved_old_key, MYF(0));
    DBUG_RETURN(ret);
  }
  
  /* Pack the new row data */
  ret = pack_row(new_data, &value, &value_len);
  if (ret)
  {
    my_free(saved_old_key, MYF(0));
    DBUG_RETURN(ret);
  }
  
  /* Begin a transaction if we don't have one */
  tidesdb_txn_t *txn = NULL;
  bool own_txn = false;
  
  if (current_txn)
  {
    txn = current_txn;
  }
  else
  {
    ret = tidesdb_txn_begin(tidesdb_instance, &txn);
    if (ret != TDB_SUCCESS)
    {
      my_free(saved_old_key, MYF(0));
      sql_print_error("TidesDB: Failed to begin transaction: %d", ret);
      DBUG_RETURN(HA_ERR_GENERIC);
    }
    own_txn = true;
  }
  
  /* Check if primary key changed */
  bool pk_changed = (old_key_len != new_key_len || 
                     memcmp(saved_old_key, new_key, old_key_len) != 0);
  
  if (pk_changed)
  {
    /* Delete old row, insert new row */
    ret = tidesdb_txn_delete(txn, share->cf, saved_old_key, old_key_len);
    if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
    {
      if (own_txn)
      {
        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
      }
      my_free(saved_old_key, MYF(0));
      sql_print_error("TidesDB: Failed to delete old row: %d", ret);
      DBUG_RETURN(HA_ERR_GENERIC);
    }
  }
  
  /* Calculate TTL from _ttl column or use default */
  time_t ttl = -1;
  if (share->ttl_field_index >= 0)
  {
    Field *ttl_field = table->field[share->ttl_field_index];
    if (!ttl_field->is_null())
    {
      longlong ttl_seconds = ttl_field->val_int();
      if (ttl_seconds > 0)
      {
        ttl = time(NULL) + ttl_seconds;
      }
    }
  }
  else if (tidesdb_default_ttl > 0)
  {
    ttl = time(NULL) + tidesdb_default_ttl;
  }
  
  /* Insert/update the new row */
  ret = tidesdb_txn_put(txn, share->cf, new_key, new_key_len, value, value_len, ttl);
  if (ret != TDB_SUCCESS)
  {
    if (own_txn)
    {
      tidesdb_txn_rollback(txn);
      tidesdb_txn_free(txn);
    }
    my_free(saved_old_key, MYF(0));
    sql_print_error("TidesDB: Failed to update row: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }
  
  my_free(saved_old_key, MYF(0));
  
  /* Commit if we own the transaction */
  if (own_txn)
  {
    ret = tidesdb_txn_commit(txn);
    tidesdb_txn_free(txn);
    
    if (ret != TDB_SUCCESS)
    {
      sql_print_error("TidesDB: Failed to commit transaction: %d", ret);
      DBUG_RETURN(HA_ERR_GENERIC);
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
  
  /* Build the primary key */
  ret = build_primary_key(buf, &key, &key_len);
  if (ret)
    DBUG_RETURN(ret);
  
  /* Begin a transaction if we don't have one */
  tidesdb_txn_t *txn = NULL;
  bool own_txn = false;
  
  if (current_txn)
  {
    txn = current_txn;
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
  
  /* Delete the row */
  ret = tidesdb_txn_delete(txn, share->cf, key, key_len);
  if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
  {
    if (own_txn)
    {
      tidesdb_txn_rollback(txn);
      tidesdb_txn_free(txn);
    }
    sql_print_error("TidesDB: Failed to delete row: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }
  
  /* Commit if we own the transaction */
  if (own_txn)
  {
    ret = tidesdb_txn_commit(txn);
    tidesdb_txn_free(txn);
    
    if (ret != TDB_SUCCESS)
    {
      sql_print_error("TidesDB: Failed to commit transaction: %d", ret);
      DBUG_RETURN(HA_ERR_GENERIC);
    }
  }
  
  stats.records--;
  
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
  
  /* Clean up any existing iterator */
  if (scan_iter)
  {
    tidesdb_iter_free(scan_iter);
    scan_iter = NULL;
  }
  
  /* Begin a transaction for the scan with configured isolation level */
  if (!current_txn)
  {
    ret = tidesdb_txn_begin_with_isolation(tidesdb_instance,
                                            (int)tidesdb_default_isolation,
                                            &current_txn);
    if (ret != TDB_SUCCESS)
    {
      sql_print_error("TidesDB: Failed to begin transaction for scan: %d", ret);
      DBUG_RETURN(HA_ERR_GENERIC);
    }
  }
  
  /* Create an iterator */
  ret = tidesdb_iter_new(current_txn, share->cf, &scan_iter);
  if (ret != TDB_SUCCESS)
  {
    sql_print_error("TidesDB: Failed to create iterator: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }
  
  /* Seek to the first entry */
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
*/
int ha_tidesdb::rnd_next(uchar *buf)
{
  DBUG_ENTER("ha_tidesdb::rnd_next");
  
  int ret;
  
  if (!scan_iter || !scan_initialized)
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  
  /* Check if iterator is valid */
  if (!tidesdb_iter_valid(scan_iter))
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  
  /* Get the current key and value */
  uint8_t *key = NULL;
  size_t key_size = 0;
  uint8_t *value = NULL;
  size_t value_size = 0;
  
  ret = tidesdb_iter_key(scan_iter, &key, &key_size);
  if (ret != TDB_SUCCESS)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }
  
  ret = tidesdb_iter_value(scan_iter, &value, &value_size);
  if (ret != TDB_SUCCESS)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }
  
  /* Save the current key for position() */
  free_current_key();
  current_key = (uchar *)my_malloc(key_size, MYF(MY_WME));
  if (!current_key)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  memcpy(current_key, key, key_size);
  current_key_len = key_size;
  
  /* Unpack the row */
  ret = unpack_row(buf, value, value_size);
  if (ret)
  {
    DBUG_RETURN(ret);
  }
  
  /* Move to next entry */
  tidesdb_iter_next(scan_iter);
  
  DBUG_RETURN(0);
}

/**
  @brief
  Store the current row position.
*/
void ha_tidesdb::position(const uchar *record)
{
  DBUG_ENTER("ha_tidesdb::position");
  
  /* Store the current key as the position */
  if (current_key && current_key_len <= ref_length)
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
  
  /* Begin a transaction if we don't have one */
  tidesdb_txn_t *txn = NULL;
  bool own_txn = false;
  
  if (current_txn)
  {
    txn = current_txn;
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
  
  /* Get the row by key */
  uint8_t *value = NULL;
  size_t value_size = 0;
  
  ret = tidesdb_txn_get(txn, share->cf, pos, ref_length, &value, &value_size);
  
  if (own_txn)
  {
    tidesdb_txn_free(txn);
  }
  
  if (ret == TDB_ERR_NOT_FOUND)
  {
    DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
  }
  else if (ret != TDB_SUCCESS)
  {
    sql_print_error("TidesDB: Failed to get row by position: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }
  
  /* Save the key */
  free_current_key();
  current_key = (uchar *)my_malloc(ref_length, MYF(MY_WME));
  if (!current_key)
  {
    free(value);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  memcpy(current_key, pos, ref_length);
  current_key_len = ref_length;
  
  /* Unpack the row */
  ret = unpack_row(buf, value, value_size);
  free(value);
  
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
  DBUG_RETURN(0);
}

/**
  @brief
  Read a row by index key.
*/
int ha_tidesdb::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map,
                               enum ha_rkey_function find_flag)
{
  DBUG_ENTER("ha_tidesdb::index_read_map");
  
  /* For now, only support primary key lookups */
  if (active_index != table->s->primary_key)
  {
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
  }
  
  int ret;
  
  /* Begin a transaction if we don't have one */
  tidesdb_txn_t *txn = NULL;
  bool own_txn = false;
  
  if (current_txn)
  {
    txn = current_txn;
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
  
  /* Get the key length */
  KEY *pk = &table->key_info[active_index];
  uint key_len = calculate_key_len(table, active_index, key, keypart_map);
  
  /* Get the row by key */
  uint8_t *value = NULL;
  size_t value_size = 0;
  
  ret = tidesdb_txn_get(txn, share->cf, key, key_len, &value, &value_size);
  
  if (own_txn)
  {
    tidesdb_txn_free(txn);
  }
  
  if (ret == TDB_ERR_NOT_FOUND)
  {
    DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
  }
  else if (ret != TDB_SUCCESS)
  {
    sql_print_error("TidesDB: Failed to get row by index: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }
  
  /* Save the key */
  free_current_key();
  current_key = (uchar *)my_malloc(key_len, MYF(MY_WME));
  if (!current_key)
  {
    free(value);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  memcpy(current_key, key, key_len);
  current_key_len = key_len;
  
  /* Unpack the row */
  ret = unpack_row(buf, value, value_size);
  free(value);
  
  if (ret)
  {
    DBUG_RETURN(ret);
  }
  
  DBUG_RETURN(0);
}

/**
  @brief
  Read next row in index order.
*/
int ha_tidesdb::index_next(uchar *buf)
{
  DBUG_ENTER("ha_tidesdb::index_next");
  /* For now, use table scan */
  DBUG_RETURN(rnd_next(buf));
}

/**
  @brief
  Read previous row in index order.
*/
int ha_tidesdb::index_prev(uchar *buf)
{
  DBUG_ENTER("ha_tidesdb::index_prev");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  Read first row in index order.
*/
int ha_tidesdb::index_first(uchar *buf)
{
  DBUG_ENTER("ha_tidesdb::index_first");
  
  int ret = rnd_init(true);
  if (ret)
    DBUG_RETURN(ret);
  
  DBUG_RETURN(rnd_next(buf));
}

/**
  @brief
  Read last row in index order.
*/
int ha_tidesdb::index_last(uchar *buf)
{
  DBUG_ENTER("ha_tidesdb::index_last");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  Return table information to the optimizer.
*/
int ha_tidesdb::info(uint flag)
{
  DBUG_ENTER("ha_tidesdb::info");
  
  /* Provide basic statistics */
  if (flag & HA_STATUS_VARIABLE)
  {
    stats.records = 1000;  /* Estimate - should query TidesDB for real count */
    stats.deleted = 0;
    stats.data_file_length = 1024 * 1024;  /* Estimate */
    stats.index_file_length = 0;
    stats.mean_rec_length = table->s->reclength;
  }
  
  if (flag & HA_STATUS_CONST)
  {
    stats.max_data_file_length = LLONG_MAX;
    stats.max_index_file_length = LLONG_MAX;
  }
  
  if (flag & HA_STATUS_AUTO)
  {
    stats.auto_increment_value = 1;
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
  DBUG_RETURN(0);
}

/**
  @brief
  Delete all rows in the table.
*/
int ha_tidesdb::delete_all_rows(void)
{
  DBUG_ENTER("ha_tidesdb::delete_all_rows");
  
  /* Drop and recreate the column family */
  char cf_name[256];
  get_cf_name(share->table_name, cf_name, sizeof(cf_name));
  
  int ret = tidesdb_drop_column_family(tidesdb_instance, cf_name);
  if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
  {
    sql_print_error("TidesDB: Failed to drop column family for truncate: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }
  
  tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
  cf_config.write_buffer_size = tidesdb_write_buffer_size;
  cf_config.enable_bloom_filter = tidesdb_enable_bloom_filter ? 1 : 0;
  
  if (tidesdb_enable_compression)
  {
    cf_config.compression_algo = LZ4_COMPRESSION;
  }
  
  ret = tidesdb_create_column_family(tidesdb_instance, cf_name, &cf_config);
  if (ret != TDB_SUCCESS)
  {
    sql_print_error("TidesDB: Failed to recreate column family for truncate: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }
  
  /* Update the share's CF handle */
  share->cf = tidesdb_get_column_family(tidesdb_instance, cf_name);
  
  stats.records = 0;
  
  DBUG_RETURN(0);
}

/**
  @brief
  Estimate records in a range.
*/
ha_rows ha_tidesdb::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_tidesdb::records_in_range");
  DBUG_RETURN(10);  /* Low estimate to encourage index usage */
}

/**
  @brief
  Map MySQL isolation level to TidesDB isolation level.
  
  MySQL: ISO_READ_UNCOMMITTED=0, ISO_READ_COMMITTED=1, 
         ISO_REPEATABLE_READ=2, ISO_SERIALIZABLE=3
  TidesDB: READ_UNCOMMITTED=0, READ_COMMITTED=1, REPEATABLE_READ=2,
           SNAPSHOT=3, SERIALIZABLE=4
*/
static int map_isolation_level(enum_tx_isolation mysql_iso)
{
  switch (mysql_iso)
  {
    case ISO_READ_UNCOMMITTED:
      return 0;  /* TDB_ISOLATION_READ_UNCOMMITTED */
    case ISO_READ_COMMITTED:
      return 1;  /* TDB_ISOLATION_READ_COMMITTED */
    case ISO_REPEATABLE_READ:
      return 2;  /* TDB_ISOLATION_REPEATABLE_READ */
    case ISO_SERIALIZABLE:
      return 4;  /* TDB_ISOLATION_SERIALIZABLE */
    default:
      return 1;  /* Default to READ_COMMITTED */
  }
}

/**
  @brief
  Handle external locking (transaction boundaries).
*/
int ha_tidesdb::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_tidesdb::external_lock");
  
  int ret;
  
  if (lock_type != F_UNLCK)
  {
    /* Starting a new statement/transaction */
    if (!current_txn)
    {
      /* Get isolation level from THD (SET TRANSACTION ISOLATION LEVEL) */
      int isolation = map_isolation_level(
        (enum_tx_isolation)thd->variables.tx_isolation);
      
      ret = tidesdb_txn_begin_with_isolation(tidesdb_instance, 
                                              isolation,
                                              &current_txn);
      if (ret != TDB_SUCCESS)
      {
        sql_print_error("TidesDB: Failed to begin transaction: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
      }
    }
  }
  else
  {
    /* Ending statement/transaction */
    if (current_txn)
    {
      /* For now, auto-commit on unlock */
      ret = tidesdb_txn_commit(current_txn);
      tidesdb_txn_free(current_txn);
      current_txn = NULL;
      
      if (ret != TDB_SUCCESS)
      {
        sql_print_error("TidesDB: Failed to commit transaction: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
      }
    }
  }
  
  DBUG_RETURN(0);
}

/**
  @brief
  Store lock information.
*/
THR_LOCK_DATA **ha_tidesdb::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type = lock_type;
  *to++ = &lock;
  return to;
}

/**
  @brief
  Optimize table - triggers manual compaction in TidesDB.
  
  This is called by OPTIMIZE TABLE statement.
*/
int ha_tidesdb::optimize(THD* thd, HA_CHECK_OPT* check_opt)
{
  DBUG_ENTER("ha_tidesdb::optimize");
  
  if (!share || !share->cf)
  {
    DBUG_RETURN(HA_ADMIN_FAILED);
  }
  
  sql_print_information("TidesDB: Triggering compaction for table");
  
  int ret = tidesdb_compact(share->cf);
  if (ret != TDB_SUCCESS)
  {
    sql_print_error("TidesDB: Compaction failed: %d", ret);
    DBUG_RETURN(HA_ADMIN_FAILED);
  }
  
  DBUG_RETURN(HA_ADMIN_OK);
}

/**
  @brief
  Analyze table - updates statistics.
  
  This is called by ANALYZE TABLE statement.
*/
int ha_tidesdb::analyze(THD* thd, HA_CHECK_OPT* check_opt)
{
  DBUG_ENTER("ha_tidesdb::analyze");
  
  if (!share || !share->cf)
  {
    DBUG_RETURN(HA_ADMIN_FAILED);
  }
  
  /* Get statistics from TidesDB */
  tidesdb_stats_t *tdb_stats = NULL;
  int ret = tidesdb_get_stats(share->cf, &tdb_stats);
  if (ret == TDB_SUCCESS && tdb_stats)
  {
    /* Update handler statistics */
    stats.data_file_length = 0;
    for (int i = 0; i < tdb_stats->num_levels; i++)
    {
      stats.data_file_length += tdb_stats->level_sizes[i];
    }
    stats.mean_rec_length = table->s->reclength;
    
    tidesdb_free_stats(tdb_stats);
  }
  
  DBUG_RETURN(HA_ADMIN_OK);
}

/**
  @brief
  Initialize full-text search.
  
  TODO: Implement inverted index for full-text search support.
  Currently returns error as full-text is not yet implemented.
*/
FT_INFO *ha_tidesdb::ft_init_ext(uint flags, uint inx, String *key)
{
  DBUG_ENTER("ha_tidesdb::ft_init_ext");
  
  /* Full-text search not yet implemented */
  my_error(ER_NOT_SUPPORTED_YET, MYF(0), "FULLTEXT indexes with TidesDB");
  DBUG_RETURN(NULL);
}

/**
  @brief
  Read next full-text search result.
  
  TODO: Implement when inverted index is available.
*/
int ha_tidesdb::ft_read(uchar *buf)
{
  DBUG_ENTER("ha_tidesdb::ft_read");
  DBUG_RETURN(HA_ERR_END_OF_FILE);
}

/*
  Plugin declaration
*/

struct st_mysql_storage_engine tidesdb_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static MYSQL_SYSVAR_STR(
  data_dir,
  tidesdb_data_dir,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "TidesDB data directory",
  NULL,
  NULL,
  NULL);

static MYSQL_SYSVAR_ULONG(
  flush_threads,
  tidesdb_flush_threads,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Number of flush threads",
  NULL,
  NULL,
  2,
  1,
  16,
  0);

static MYSQL_SYSVAR_ULONG(
  compaction_threads,
  tidesdb_compaction_threads,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Number of compaction threads",
  NULL,
  NULL,
  2,
  1,
  16,
  0);

static MYSQL_SYSVAR_ULONGLONG(
  block_cache_size,
  tidesdb_block_cache_size,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Block cache size in bytes",
  NULL,
  NULL,
  64 * 1024 * 1024,
  0,
  ULLONG_MAX,
  0);

static MYSQL_SYSVAR_ULONGLONG(
  write_buffer_size,
  tidesdb_write_buffer_size,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Write buffer (memtable) size in bytes",
  NULL,
  NULL,
  64 * 1024 * 1024,
  1024 * 1024,
  ULLONG_MAX,
  0);

static MYSQL_SYSVAR_BOOL(
  enable_compression,
  tidesdb_enable_compression,
  PLUGIN_VAR_RQCMDARG,
  "Enable LZ4 compression",
  NULL,
  NULL,
  TRUE);

static MYSQL_SYSVAR_BOOL(
  enable_bloom_filter,
  tidesdb_enable_bloom_filter,
  PLUGIN_VAR_RQCMDARG,
  "Enable bloom filters",
  NULL,
  NULL,
  TRUE);

static MYSQL_SYSVAR_ENUM(
  compression_algo,
  tidesdb_compression_algo,
  PLUGIN_VAR_RQCMDARG,
  "Compression algorithm: none, snappy, lz4, zstd, lz4_fast",
  NULL,
  NULL,
  2,  /* lz4 default */
  &tidesdb_compression_typelib);

static MYSQL_SYSVAR_ENUM(
  sync_mode,
  tidesdb_sync_mode,
  PLUGIN_VAR_RQCMDARG,
  "Sync mode: none (fastest), interval (balanced), full (safest)",
  NULL,
  NULL,
  1,  /* interval default */
  &tidesdb_sync_mode_typelib);

static MYSQL_SYSVAR_ULONGLONG(
  sync_interval_us,
  tidesdb_sync_interval_us,
  PLUGIN_VAR_RQCMDARG,
  "Sync interval in microseconds (for interval sync mode)",
  NULL,
  NULL,
  128000,  /* 128ms default */
  1000,
  10000000,
  0);

static MYSQL_SYSVAR_DOUBLE(
  bloom_fpr,
  tidesdb_bloom_fpr,
  PLUGIN_VAR_RQCMDARG,
  "Bloom filter false positive rate (0.0 to 1.0)",
  NULL,
  NULL,
  0.01,  /* 1% default */
  0.0001,
  0.5,
  0);

static MYSQL_SYSVAR_ENUM(
  default_isolation,
  tidesdb_default_isolation,
  PLUGIN_VAR_RQCMDARG,
  "Default transaction isolation level",
  NULL,
  NULL,
  1,  /* read_committed default */
  &tidesdb_isolation_typelib);

static MYSQL_SYSVAR_ULONG(
  level_size_ratio,
  tidesdb_level_size_ratio,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "LSM level size ratio (multiplier between levels)",
  NULL,
  NULL,
  10,
  2,
  100,
  0);

static MYSQL_SYSVAR_ULONG(
  skip_list_max_level,
  tidesdb_skip_list_max_level,
  PLUGIN_VAR_RQCMDARG,
  "Skip list maximum level for memtables",
  NULL,
  NULL,
  12,
  4,
  32,
  0);

static MYSQL_SYSVAR_BOOL(
  enable_block_indexes,
  tidesdb_enable_block_indexes,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Enable compact block indexes for faster seeks",
  NULL,
  NULL,
  TRUE);

static MYSQL_SYSVAR_ULONG(
  index_sample_ratio,
  tidesdb_index_sample_ratio,
  PLUGIN_VAR_RQCMDARG,
  "Block index sampling ratio (1 = every block, 8 = every 8th block)",
  NULL,
  NULL,
  1,
  1,
  64,
  0);

static MYSQL_SYSVAR_ULONGLONG(
  default_ttl,
  tidesdb_default_ttl,
  PLUGIN_VAR_RQCMDARG,
  "Default TTL in seconds for new rows (0 = no expiration)",
  NULL,
  NULL,
  0,
  0,
  ULLONG_MAX,
  0);

static MYSQL_SYSVAR_ENUM(
  log_level,
  tidesdb_log_level,
  PLUGIN_VAR_RQCMDARG,
  "TidesDB log level: debug, info, warn, error, fatal, none",
  NULL,
  NULL,
  1,  /* info default */
  &tidesdb_log_level_typelib);

static struct st_mysql_sys_var* tidesdb_system_variables[] = {
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
  NULL
};

mysql_declare_plugin(tidesdb)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &tidesdb_storage_engine,
  "TidesDB",
  "TidesDB Authors",
  "TidesDB LSM-based storage engine with ACID transactions",
  PLUGIN_LICENSE_GPL,
  tidesdb_init_func,                            /* Plugin Init */
  tidesdb_done_func,                            /* Plugin Deinit */
  0x0001 /* 0.1 */,
  NULL,                                         /* status variables */
  tidesdb_system_variables,                     /* system variables */
  NULL                                          /* config options */
}
mysql_declare_plugin_end;
