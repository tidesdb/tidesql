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
    .log_level = TDB_LOG_INFO,
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
  cf_config.bloom_fpr = 0.01;  /* 1% false positive rate */
  
  if (tidesdb_enable_compression)
  {
    cf_config.compression_algorithm = LZ4_COMPRESSION;
  }
  else
  {
    cf_config.compression_algorithm = NO_COMPRESSION;
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
  
  /* Insert the row */
  ret = tidesdb_txn_put(txn, share->cf, key, key_len, value, value_len, -1);
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
  
  /* Insert/update the new row */
  ret = tidesdb_txn_put(txn, share->cf, new_key, new_key_len, value, value_len, -1);
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
  
  /* Begin a transaction for the scan */
  if (!current_txn)
  {
    ret = tidesdb_txn_begin(tidesdb_instance, &current_txn);
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
    cf_config.compression_algorithm = LZ4_COMPRESSION;
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
      ret = tidesdb_txn_begin(tidesdb_instance, &current_txn);
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

static struct st_mysql_sys_var* tidesdb_system_variables[] = {
  MYSQL_SYSVAR(data_dir),
  MYSQL_SYSVAR(flush_threads),
  MYSQL_SYSVAR(compaction_threads),
  MYSQL_SYSVAR(block_cache_size),
  MYSQL_SYSVAR(write_buffer_size),
  MYSQL_SYSVAR(enable_compression),
  MYSQL_SYSVAR(enable_bloom_filter),
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
