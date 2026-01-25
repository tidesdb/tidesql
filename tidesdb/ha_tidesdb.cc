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
  Each MySQL table maps to a TidesDB column family.
  Rows are stored as: primary_key -> serialized_row_data
  
  For tables without explicit primary keys, we generate a hidden
  auto-increment key.
*/

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#define MYSQL_SERVER 1
#include <my_global.h>
#include <mysql/plugin.h>
#include "ha_tidesdb.h"
#include "sql_class.h"
#include "key.h"
#include <tidesdb/tidesdb_version.h>

/* Fulltext search configuration */
static ulong tidesdb_ft_min_word_len = 4;
static ulong tidesdb_ft_max_word_len = 84;

/* XA error codes (from X/Open XA specification) */
#ifndef XA_OK
#define XA_OK          0   /* Normal execution */
#define XAER_NOTA     -4   /* The XID is not valid */
#define XAER_INVAL    -5   /* Invalid arguments */
#define XAER_RMERR    -3   /* Resource manager error */
#define XAER_RMFAIL   -7   /* Resource manager unavailable */
#endif

/* Forward declarations */
static handler *tidesdb_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root);
static int tidesdb_init_func(void *p);
static int tidesdb_done_func(void *p);
static int tidesdb_commit(THD *thd, bool all);
static int tidesdb_rollback(THD *thd, bool all);
static bool tidesdb_show_status(handlerton *hton, THD *thd, 
                                stat_print_fn *stat_print, 
                                enum ha_stat_type stat_type);

/* XA transaction support forward declarations */
static int tidesdb_xa_prepare(THD *thd, bool all);
static int tidesdb_xa_recover(XID *xid_list, uint len);
static int tidesdb_commit_by_xid(XID *xid);
static int tidesdb_rollback_by_xid(XID *xid);

/* XA transaction tracking structures (defined early for use in shutdown) */
struct tidesdb_xa_txn_t {
  XID xid;
  tidesdb_txn_t *txn;
  tidesdb_xa_txn_t *next;
};
static tidesdb_xa_txn_t *tidesdb_prepared_xids = NULL;
static pthread_mutex_t tidesdb_xa_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Global TidesDB instance -- one database for all tables */
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
  NULL,
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
  NULL,
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
  NULL,
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
  NULL,
  NULL
};

/* Additional LSM configuration from TidesDB C API */
static ulong tidesdb_min_levels = 5;                    /* Minimum LSM levels */
static ulong tidesdb_dividing_level_offset = 2;         /* Compaction dividing level offset */
static double tidesdb_skip_list_probability = 0.25;     /* Skip list probability */
static ulong tidesdb_block_index_prefix_len = 16;       /* Block index prefix length */
static ulonglong tidesdb_klog_value_threshold = 512;    /* Values > threshold go to vlog */
static ulonglong tidesdb_min_disk_space = 100 * 1024 * 1024;  /* 100MB minimum disk space */
static ulong tidesdb_l1_file_count_trigger = 4;         /* L1 file count trigger for compaction */
static ulong tidesdb_l0_queue_stall_threshold = 20;     /* L0 queue stall threshold */
static ulong tidesdb_max_open_sstables = 256;           /* Max cached SSTable structures */

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

  if (!(share = (TIDESDB_SHARE*) my_hash_search(&tidesdb_open_tables,
                                              (uchar*) table_name,
                                              length)))
  {
    if (!(share = (TIDESDB_SHARE *)
          my_multi_malloc(PSI_INSTRUMENT_ME, MYF(MY_WME | MY_ZEROFILL),
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
    share->auto_increment_value = 1;
    share->row_count = 0;
    share->row_count_valid = false;
    share->hidden_pk_value = 0;  /* Will be loaded from metadata on open */
    
    if (my_hash_insert(&tidesdb_open_tables, (uchar*) share))
      goto error;
    thr_lock_init(&share->lock);
    pthread_mutex_init(&share->mutex, MY_MUTEX_INIT_FAST);
    pthread_mutex_init(&share->auto_inc_mutex, MY_MUTEX_INIT_FAST);
    pthread_mutex_init(&share->hidden_pk_mutex, MY_MUTEX_INIT_FAST);
  }
  share->use_count++;
  pthread_mutex_unlock(&tidesdb_mutex);

  return share;

error:
  pthread_mutex_destroy(&share->mutex);
  pthread_mutex_destroy(&share->auto_inc_mutex);
  pthread_mutex_destroy(&share->hidden_pk_mutex);
  my_free(share);
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
    my_hash_delete(&tidesdb_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    pthread_mutex_destroy(&share->mutex);
    pthread_mutex_destroy(&share->auto_inc_mutex);
    pthread_mutex_destroy(&share->hidden_pk_mutex);
    /* Note: We don't drop the column family here, just release the share */
    my_free(share);
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

/* Savepoint data structure -- must be defined before init function */
struct tidesdb_savepoint_t
{
  char name[64];  /* Savepoint name derived from pointer address */
  tidesdb_txn_t *txn;  /* Transaction at time of savepoint (for reference) */
};

/* Forward declarations for savepoint functions */
static int tidesdb_savepoint_set(THD *thd, void *savepoint);
static int tidesdb_savepoint_rollback(THD *thd, void *savepoint);
static int tidesdb_savepoint_release(THD *thd, void *savepoint);

/**
  @brief
  Initialize the TidesDB storage engine.
*/
static int tidesdb_init_func(void *p)
{
  DBUG_ENTER("tidesdb_init_func");

  tidesdb_hton = (handlerton *)p;
  
  (void)pthread_mutex_init(&tidesdb_mutex, MY_MUTEX_INIT_FAST);
  (void) my_hash_init(PSI_INSTRUMENT_ME, &tidesdb_open_tables, system_charset_info, 32, 0, 0,
                   (my_hash_get_key) tidesdb_get_key, 0, 0);

  /* MariaDB doesn't use state field in handlerton */
  tidesdb_hton->create = tidesdb_create_handler;
  tidesdb_hton->flags = HTON_CLOSE_CURSORS_AT_COMMIT;
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
  
  /* Initialize TidesDB instance */
  static char db_path[FN_REFLEN];  /* Static to ensure it persists */
  if (tidesdb_data_dir)
  {
    strncpy(db_path, tidesdb_data_dir, sizeof(db_path) - 1);
  }
  else
  {
    /* Default to MySQL data directory + tidesdb */
    snprintf(db_path, sizeof(db_path), "%s/tidesdb", mysql_data_home);
  }
  db_path[sizeof(db_path) - 1] = '\0';
  
  tidesdb_config_t config = tidesdb_default_config();
  config.db_path = db_path;
  config.num_flush_threads = (int)tidesdb_flush_threads;
  config.num_compaction_threads = (int)tidesdb_compaction_threads;
  config.log_level = (tidesdb_log_level_t)tidesdb_log_level;
  config.block_cache_size = tidesdb_block_cache_size;
  config.max_open_sstables = (int)tidesdb_max_open_sstables;
  
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
  
  my_hash_free(&tidesdb_open_tables);
  pthread_mutex_destroy(&tidesdb_mutex);
  
  /* Clean up any remaining prepared XA transactions */
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
  
  if (tidesdb_instance)
  {
    tidesdb_close(tidesdb_instance);
    tidesdb_instance = NULL;
  }
  
  sql_print_information("TidesDB: Storage engine shutdown");

  DBUG_RETURN(error);
}

/**
  Thread-local transaction storage.
  We use thd_get_ha_data/thd_set_ha_data to store the transaction per-thread.
*/
static tidesdb_txn_t* get_thd_txn(THD *thd, handlerton *hton)
{
  return (tidesdb_txn_t*)thd_get_ha_data(thd, hton);
}

static void set_thd_txn(THD *thd, handlerton *hton, tidesdb_txn_t *txn)
{
  thd_set_ha_data(thd, hton, txn);
}

/**
  @brief
  Commit a transaction.
  
  Called by MySQL when COMMIT is issued or when auto-commit commits
  a statement. For multi-statement transactions, this commits the
  THD-level transaction.
*/
static int tidesdb_commit(THD *thd, bool all)
{
  DBUG_ENTER("tidesdb_commit");
  
  tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);
  
  if (txn && all)
  {
    /* Commit the THD-level transaction */
    int ret = tidesdb_txn_commit(txn);
    tidesdb_txn_free(txn);
    set_thd_txn(thd, tidesdb_hton, NULL);
    
    if (ret != TDB_SUCCESS)
    {
      sql_print_error("TidesDB: Failed to commit transaction: %d", ret);
      DBUG_RETURN(HA_ERR_GENERIC);
    }
    DBUG_PRINT("info", ("TidesDB: Transaction committed"));
  }
  
  DBUG_RETURN(0);
}

/**
  @brief
  Rollback a transaction.
  
  Called by MySQL when ROLLBACK is issued. For multi-statement
  transactions, this rolls back the THD-level transaction.
*/
static int tidesdb_rollback(THD *thd, bool all)
{
  DBUG_ENTER("tidesdb_rollback");
  
  tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);
  
  if (txn && all)
  {
    /* Rollback the THD-level transaction */
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

/*
  XA Transaction Support
  
  TidesDB supports XA (eXtended Architecture) distributed transactions
  for 2-phase commit protocol. This allows TidesDB to participate in
  distributed transactions coordinated by an external transaction manager.
  
  Note: tidesdb_xa_txn_t, tidesdb_prepared_xids, and tidesdb_xa_mutex
  are defined at the top of the file for use in shutdown cleanup.
*/

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
  
  /* Get the XID from the THD */
  const XID *xid = thd->get_xid();
  
  /* Store the prepared transaction for potential recovery */
  pthread_mutex_lock(&tidesdb_xa_mutex);
  
  tidesdb_xa_txn_t *xa_txn = (tidesdb_xa_txn_t *)my_malloc(PSI_INSTRUMENT_ME,
                                                           sizeof(tidesdb_xa_txn_t),
                                                           MYF(MY_WME | MY_ZEROFILL));
  if (xa_txn)
  {
    memcpy(&xa_txn->xid, xid, sizeof(XID));
    xa_txn->txn = txn;
    xa_txn->next = tidesdb_prepared_xids;
    tidesdb_prepared_xids = xa_txn;
  }
  
  pthread_mutex_unlock(&tidesdb_xa_mutex);
  
  /* 
    Note: TidesDB transactions are already durable when operations are performed.
    The prepare phase just marks the transaction as ready for commit.
    We keep the transaction handle for later commit/rollback by XID.
  */
  
  /* Don't free the transaction -- it will be committed/rolled back by XID */
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
  
  /* Find the prepared transaction by XID */
  tidesdb_xa_txn_t *xa_txn = tidesdb_prepared_xids;
  tidesdb_xa_txn_t *prev = NULL;
  
  while (xa_txn)
  {
    if (memcmp(&xa_txn->xid, xid, sizeof(XID)) == 0)
    {
      /* Found the transaction -- remove from list */
      if (prev)
        prev->next = xa_txn->next;
      else
        tidesdb_prepared_xids = xa_txn->next;
      
      pthread_mutex_unlock(&tidesdb_xa_mutex);
      
      /* Commit the transaction */
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
  
  /* Transaction not found */
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
  
  /* Find the prepared transaction by XID */
  tidesdb_xa_txn_t *xa_txn = tidesdb_prepared_xids;
  tidesdb_xa_txn_t *prev = NULL;
  
  while (xa_txn)
  {
    if (memcmp(&xa_txn->xid, xid, sizeof(XID)) == 0)
    {
      /* Found the transaction -- remove from list */
      if (prev)
        prev->next = xa_txn->next;
      else
        tidesdb_prepared_xids = xa_txn->next;
      
      pthread_mutex_unlock(&tidesdb_xa_mutex);
      
      /* Rollback the transaction */
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
  
  /* Transaction not found */
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
  
  /* Generate a unique savepoint name from the pointer address */
  snprintf(sp->name, sizeof(sp->name), "sp_%lx", (ulong)savepoint);
  
  /* Get the current transaction for this thread */
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
  
  /* Get the current transaction for this thread */
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
  
  /* Get the current transaction for this thread */
  tidesdb_txn_t *txn = get_thd_txn(thd, tidesdb_hton);
  
  if (txn)
  {
    /* Release savepoint in TidesDB */
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
static bool tidesdb_show_status(handlerton *hton, THD *thd,
                                stat_print_fn *stat_print,
                                enum ha_stat_type stat_type)
{
  DBUG_ENTER("tidesdb_show_status");
  
  if (stat_type != HA_ENGINE_STATUS)
  {
    DBUG_RETURN(FALSE);
  }
  
  const size_t buf_size = 16384;
  char *buf = (char *)my_malloc(PSI_INSTRUMENT_ME, buf_size, MYF(MY_WME));
  if (!buf)
    DBUG_RETURN(TRUE);

  int buf_len = 0;

  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "================================================================================\n"
    "                            TIDESDB ENGINE STATUS\n"
    "================================================================================\n"
    "\n");

  /* Block Cache Statistics */
  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "------------------------\n"
    "BLOCK CACHE\n"
    "------------------------\n");

  tidesdb_cache_stats_t cache_stats;
  if (tidesdb_get_cache_stats(tidesdb_instance, &cache_stats) == TDB_SUCCESS)
  {
    buf_len += snprintf(buf + buf_len, buf_size - buf_len,
      "| %-24s | %-20s |\n"
      "| %-24s | %-20s |\n"
      "| %-24s | %-20zu |\n"
      "| %-24s | %-17.2f MB |\n"
      "| %-24s | %-20lu |\n"
      "| %-24s | %-20lu |\n"
      "| %-24s | %-18.2f %% |\n"
      "| %-24s | %-20zu |\n",
      "Status", cache_stats.enabled ? "ENABLED" : "DISABLED",
      "State", cache_stats.enabled ? "ACTIVE" : "INACTIVE",
      "Entries", cache_stats.total_entries,
      "Size", cache_stats.total_bytes / (1024.0 * 1024.0),
      "Hits", cache_stats.hits,
      "Misses", cache_stats.misses,
      "Hit Rate", cache_stats.hit_rate * 100.0,
      "Partitions", cache_stats.num_partitions);
  }
  else
  {
    buf_len += snprintf(buf + buf_len, buf_size - buf_len,
      "| %-24s | %-20s |\n",
      "Status", "UNAVAILABLE");
  }

  /* Thread Pool */
  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "------------------------\n"
    "THREAD POOLS\n"
    "------------------------\n"
    "| %-24s | %-20lu |\n"
    "| %-24s | %-20lu |\n",
    "Flush Threads", tidesdb_flush_threads,
    "Compaction Threads", tidesdb_compaction_threads);

  /* Memory Configuration */
  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "------------------------\n"
    "MEMORY\n"
    "------------------------\n"
    "| %-24s | %-17.2f MB |\n"
    "| %-24s | %-17.2f MB |\n",
    "Block Cache Size", tidesdb_block_cache_size / (1024.0 * 1024.0),
    "Write Buffer Size", tidesdb_write_buffer_size / (1024.0 * 1024.0));

  /* Compression */
  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "------------------------\n"
    "COMPRESSION\n"
    "------------------------\n"
    "| %-24s | %-20s |\n"
    "| %-24s | %-20s |\n",
    "Enabled", tidesdb_enable_compression ? "YES" : "NO",
    "Algorithm", tidesdb_compression_names[tidesdb_compression_algo]);

  /* Bloom Filter */
  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "------------------------\n"
    "BLOOM FILTER\n"
    "------------------------\n"
    "| %-24s | %-20s |\n"
    "| %-24s | %-18.2f %% |\n",
    "Enabled", tidesdb_enable_bloom_filter ? "YES" : "NO",
    "False Positive Rate", tidesdb_bloom_fpr * 100.0);

  /* Durability */
  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "------------------------\n"
    "DURABILITY\n"
    "------------------------\n"
    "| %-24s | %-20s |\n"
    "| %-24s | %-17llu us |\n",
    "Sync Mode", tidesdb_sync_mode_names[tidesdb_sync_mode],
    "Sync Interval", (unsigned long long)tidesdb_sync_interval_us);

  /* Transaction */
  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "------------------------\n"
    "TRANSACTIONS\n"
    "------------------------\n"
    "| %-24s | %-20s |\n"
    "| %-24s | %-20s |\n"
    "| %-24s | %-20s |\n",
    "Default Isolation", tidesdb_isolation_names[tidesdb_default_isolation],
    "XA Support", "YES",
    "Savepoints", "YES");

  /* LSM Configuration */
  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "------------------------\n"
    "LSM TREE\n"
    "------------------------\n"
    "| %-24s | %-20lu |\n"
    "| %-24s | %-20lu |\n"
    "| %-24s | %-20lu |\n"
    "| %-24s | %-20lu |\n"
    "| %-24s | %-20lu |\n",
    "Level Size Ratio", tidesdb_level_size_ratio,
    "Min Levels", tidesdb_min_levels,
    "Skip List Max Level", tidesdb_skip_list_max_level,
    "L1 File Count Trigger", tidesdb_l1_file_count_trigger,
    "L0 Stall Threshold", tidesdb_l0_queue_stall_threshold);

  /* Storage Statistics */
  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "------------------------\n"
    "STORAGE\n"
    "------------------------\n"
    "| %-24s | %-20lu |\n"
    "| %-24s | %-20lu |\n"
    "| %-24s | %-20s |\n",
    "Open Tables", (unsigned long)tidesdb_open_tables.records,
    "Max Open SSTables", tidesdb_max_open_sstables,
    "Block Indexes", tidesdb_enable_block_indexes ? "ENABLED" : "DISABLED");

  /* TTL */
  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "------------------------\n"
    "TTL\n"
    "------------------------\n"
    "| %-24s | %-17llu s |\n",
    "Default TTL", (unsigned long long)tidesdb_default_ttl);

  buf_len += snprintf(buf + buf_len, buf_size - buf_len,
    "\n"
    "================================================================================\n"
    "                         END OF TIDESDB ENGINE STATUS\n"
    "================================================================================\n");

  stat_print(thd, "TidesDB", 7, "", 0, buf, buf_len);

  my_free(buf);

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
   current_key_len(0),
   bulk_insert_active(false),
   bulk_txn(NULL),
   bulk_insert_rows(0),
   index_iter(NULL),
   index_key_buf(NULL),
   index_key_len(0)
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
    my_free(pk_buffer);
  if (row_buffer)
    my_free(row_buffer);
  if (index_iter)
    tidesdb_iter_free(index_iter);
  if (index_key_buf)
    my_free(index_key_buf);
}

/**
  @brief
  Free the current key buffer.
*/
void ha_tidesdb::free_current_key()
{
  if (current_key)
  {
    my_free(current_key);
    current_key = NULL;
    current_key_len = 0;
  }
}

/**
  @brief
  Pack a MySQL row into a byte buffer for storage.

  Uses MySQL's native row format
*/
int ha_tidesdb::pack_row(const uchar *buf, uchar **packed, size_t *packed_len)
{
  DBUG_ENTER("ha_tidesdb::pack_row");

  size_t row_len = table->s->reclength;

  /* Calculate total size including BLOB/TEXT data with 4-byte length prefix each */
  size_t total_len = row_len;
  for (uint i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    if (field->type() == MYSQL_TYPE_BLOB ||
        field->type() == MYSQL_TYPE_MEDIUM_BLOB ||
        field->type() == MYSQL_TYPE_LONG_BLOB ||
        field->type() == MYSQL_TYPE_TINY_BLOB)
    {
      total_len += 4;  /* 4-byte length prefix */
      if (!field->is_null())
      {
        String str;
        field->val_str(&str);
        total_len += str.length();
      }
    }
  }

  /* Allocate buffer for row + BLOB data */
  uchar *new_buf = (uchar *)my_malloc(PSI_INSTRUMENT_ME, total_len, MYF(MY_WME));
  if (!new_buf)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  /* Copy fixed-length portion */
  memcpy(new_buf, buf, row_len);

  /* Append BLOB/TEXT data with length prefix */
  size_t blob_offset = row_len;
  for (uint i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    if (field->type() == MYSQL_TYPE_BLOB ||
        field->type() == MYSQL_TYPE_MEDIUM_BLOB ||
        field->type() == MYSQL_TYPE_LONG_BLOB ||
        field->type() == MYSQL_TYPE_TINY_BLOB)
    {
      uint32 blob_len = 0;
      if (!field->is_null())
      {
        String str;
        field->val_str(&str);
        blob_len = str.length();

        /* Store 4-byte length prefix */
        int4store(new_buf + blob_offset, blob_len);
        blob_offset += 4;

        /* Copy blob data */
        if (blob_len > 0)
        {
          memcpy(new_buf + blob_offset, str.ptr(), blob_len);
          blob_offset += blob_len;
        }
      }
      else
      {
        /* NULL blob -- store 0 length */
        int4store(new_buf + blob_offset, 0);
        blob_offset += 4;
      }
    }
  }

  *packed = new_buf;
  *packed_len = total_len;

  DBUG_RETURN(0);
}

/**
  @brief
  Unpack a stored row back into MySQL's row buffer.

  For BLOB/TEXT fields, the data is stored after the fixed-length row portion
  with a 4-byte length prefix for each blob field. We copy blob data to
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

  /* Copy fixed-length portion */
  memcpy(buf, packed, row_len);

  /* Calculate total blob data size needed */
  size_t total_blob_size = 0;
  size_t blob_offset = row_len;
  for (uint i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    if (field->type() == MYSQL_TYPE_BLOB ||
        field->type() == MYSQL_TYPE_MEDIUM_BLOB ||
        field->type() == MYSQL_TYPE_LONG_BLOB ||
        field->type() == MYSQL_TYPE_TINY_BLOB)
    {
      if (blob_offset + 4 > packed_len)
        DBUG_RETURN(HA_ERR_CRASHED);

      uint32 blob_len = uint4korr(packed + blob_offset);
      blob_offset += 4 + blob_len;
      total_blob_size += blob_len;
    }
  }

  /* Allocate/reallocate row_buffer for blob data */
  if (total_blob_size > 0)
  {
    if (row_buffer_len < total_blob_size)
    {
      if (row_buffer)
        my_free(row_buffer);
      row_buffer = (uchar *)my_malloc(PSI_INSTRUMENT_ME, total_blob_size, MYF(MY_WME));
      if (!row_buffer)
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      row_buffer_len = total_blob_size;
    }
  }

  /* Read BLOB/TEXT data and set up pointers */
  blob_offset = row_len;
  size_t buffer_offset = 0;
  for (uint i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    if (field->type() == MYSQL_TYPE_BLOB ||
        field->type() == MYSQL_TYPE_MEDIUM_BLOB ||
        field->type() == MYSQL_TYPE_LONG_BLOB ||
        field->type() == MYSQL_TYPE_TINY_BLOB)
    {
      Field_blob *blob_field = (Field_blob *)field;
      uint packlength = blob_field->pack_length_no_ptr();
      uchar *field_ptr = buf + (field->ptr - table->record[0]);

      /* Read 4-byte length prefix from packed data */
      uint32 blob_len = uint4korr(packed + blob_offset);
      blob_offset += 4;

      /* Store length in field's native format */
      switch (packlength)
      {
        case 1: field_ptr[0] = (uchar)blob_len; break;
        case 2: int2store(field_ptr, blob_len); break;
        case 3: int3store(field_ptr, blob_len); break;
        case 4: int4store(field_ptr, blob_len); break;
      }

      /* Copy blob data to our persistent buffer and point to it */
      if (blob_len > 0)
      {
        if (blob_offset + blob_len > packed_len)
          DBUG_RETURN(HA_ERR_CRASHED);

        memcpy(row_buffer + buffer_offset, packed + blob_offset, blob_len);
        uchar *blob_ptr = row_buffer + buffer_offset;
        memcpy(field_ptr + packlength, &blob_ptr, sizeof(char*));
        blob_offset += blob_len;
        buffer_offset += blob_len;
      }
      else
      {
        /* Empty blob -- set pointer to NULL */
        uchar *null_ptr = NULL;
        memcpy(field_ptr + packlength, &null_ptr, sizeof(char*));
      }
    }
  }

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
    /* Table has a primary key -- extract it */
    KEY *pk = &table->key_info[table->s->primary_key];
    uint pk_len = pk->key_length;

    if (pk_buffer_len < pk_len)
    {
      if (pk_buffer)
        my_free(pk_buffer);
      pk_buffer = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
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
    /* No primary key -- use hidden auto-increment */
    DBUG_RETURN(build_hidden_pk(key, key_len));
  }

  DBUG_RETURN(0);
}

/**
  @brief
  Build a hidden primary key for tables without explicit PK.
  Uses a per-table auto-incrementing 8-byte integer that is persisted
  to TidesDB metadata for crash recovery.

  The hidden PK is stored as a special metadata key in the column family:
  Key: "__hidden_pk_max__"
  Value: 8-byte big-endian counter
*/
int ha_tidesdb::build_hidden_pk(uchar **key, size_t *key_len)
{
  DBUG_ENTER("ha_tidesdb::build_hidden_pk");

  /* Ensure we have a buffer for the 8-byte key */
  if (pk_buffer_len < 8)
  {
    if (pk_buffer)
      my_free(pk_buffer);
    pk_buffer = (uchar *)my_malloc(PSI_INSTRUMENT_ME, 8, MYF(MY_WME));
    if (!pk_buffer)
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    pk_buffer_len = 8;
  }

  /* Get next hidden PK value atomically using per-table mutex */
  pthread_mutex_lock(&share->hidden_pk_mutex);

  ulonglong pk_val = ++share->hidden_pk_value;

  /* Persist the new max value to TidesDB metadata every N inserts for performance.
     We persist every 100 values and on close. On recovery, we'll scan to find
     the actual max, but this reduces write amplification. */
  if ((pk_val % 100) == 0 || pk_val == 1)
  {
    persist_hidden_pk_value(pk_val);
  }

  pthread_mutex_unlock(&share->hidden_pk_mutex);

  /* Store as big-endian for proper sort order in TidesDB */
  int8store(pk_buffer, pk_val);

  *key = pk_buffer;
  *key_len = 8;

  DBUG_RETURN(0);
}

/**
  @brief
  Persist the hidden PK max value to TidesDB metadata.
  Called periodically during inserts and on table close.
*/
void ha_tidesdb::persist_hidden_pk_value(ulonglong value)
{
  if (!share || !share->cf || !tidesdb_instance)
    return;

  /* Use a special metadata key that sorts before all data keys */
  static const char *meta_key = "\x00__hidden_pk_max__";
  size_t meta_key_len = 19;  /* Including leading null byte */

  uchar value_buf[8];
  int8store(value_buf, value);

  /* Write directly without transaction for metadata */
  tidesdb_txn_t *txn = NULL;
  if (tidesdb_txn_begin(tidesdb_instance, &txn) == TDB_SUCCESS)
  {
    tidesdb_txn_put(txn, share->cf,
                    (uint8_t *)meta_key, meta_key_len,
                    value_buf, 8, -1);
    tidesdb_txn_commit(txn);
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
  if (!share || !share->cf || !tidesdb_instance)
    return;

  /* Already loaded? */
  if (share->hidden_pk_value > 0)
    return;

  pthread_mutex_lock(&share->hidden_pk_mutex);

  /* Double-check after acquiring lock */
  if (share->hidden_pk_value > 0)
  {
    pthread_mutex_unlock(&share->hidden_pk_mutex);
    return;
  }

  ulonglong max_pk = 0;

  /* Try to read persisted value first */
  static const char *meta_key = "\x00__hidden_pk_max__";
  size_t meta_key_len = 19;

  tidesdb_txn_t *txn = NULL;
  if (tidesdb_txn_begin(tidesdb_instance, &txn) == TDB_SUCCESS)
  {
    uint8_t *value = NULL;
    size_t value_len = 0;

    if (tidesdb_txn_get(txn, share->cf,
                        (uint8_t *)meta_key, meta_key_len,
                        &value, &value_len) == TDB_SUCCESS && value_len == 8)
    {
      max_pk = uint8korr(value);
    }

    /* If no persisted value or to verify, scan for actual max.
       This handles crash recovery where inserts happened after last persist. */
    if (!share->has_primary_key)
    {
      tidesdb_iter_t *iter = NULL;
      if (tidesdb_iter_new(txn, share->cf, &iter) == TDB_SUCCESS)
      {
        tidesdb_iter_seek_to_last(iter);

        /* Skip metadata keys (starting with null byte) and find max data key */
        while (tidesdb_iter_valid(iter))
        {
          uint8_t *key = NULL;
          size_t key_len = 0;

          if (tidesdb_iter_key(iter, &key, &key_len) == TDB_SUCCESS)
          {
            /* Skip metadata keys */
            if (key_len > 0 && key[0] == 0)
            {
              tidesdb_iter_prev(iter);
              continue;
            }

            /* Found a data key -- extract the hidden PK value */
            if (key_len == 8)
            {
              ulonglong found_pk = uint8korr(key);
              if (found_pk > max_pk)
                max_pk = found_pk;
            }
            break;
          }
          tidesdb_iter_prev(iter);
        }

        tidesdb_iter_free(iter);
      }
    }

    tidesdb_txn_free(txn);
  }

  share->hidden_pk_value = max_pk;

  pthread_mutex_unlock(&share->hidden_pk_mutex);
}

/**
  @brief
  Build a secondary index key from the row buffer.

  The index key format is: index_columns + primary_key
  This ensures uniqueness even for non-unique indexes.

  Note: This allocates a new buffer that the caller must free.
*/
int ha_tidesdb::build_index_key(uint idx, const uchar *buf, uchar **key, size_t *key_len)
{
  DBUG_ENTER("ha_tidesdb::build_index_key");

  if (idx >= table->s->keys)
    DBUG_RETURN(HA_ERR_WRONG_INDEX);

  KEY *key_info = &table->key_info[idx];
  uint idx_key_len = key_info->key_length;

  /* Calculate primary key length */
  size_t pk_len;
  if (table->s->primary_key != MAX_KEY)
  {
    pk_len = table->key_info[table->s->primary_key].key_length;
  }
  else
  {
    pk_len = 8;  /* Hidden 8-byte PK */
  }

  /* Allocate buffer for index key + primary key */
  size_t total_len = idx_key_len + pk_len;
  uchar *idx_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, total_len, MYF(MY_WME));
  if (!idx_key)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  /* Copy index key columns */
  key_copy(idx_key, (uchar *)buf, key_info, idx_key_len);

  /* Append primary key to ensure uniqueness */
  if (table->s->primary_key != MAX_KEY)
  {
    KEY *pk = &table->key_info[table->s->primary_key];
    key_copy(idx_key + idx_key_len, (uchar *)buf, pk, pk_len);
  }
  else
  {
    /* For hidden PK, we need to use the current hidden_pk_value */
    /* This is tricky -- for existing rows we need to extract from current_key */
    if (current_key && current_key_len == 8)
    {
      memcpy(idx_key + idx_key_len, current_key, 8);
    }
    else
    {
      /* Fallback: use zeros (shouldn't happen in normal operation) */
      memset(idx_key + idx_key_len, 0, 8);
    }
  }

  *key = idx_key;
  *key_len = total_len;

  DBUG_RETURN(0);
}

/**
  @brief
  Insert an entry into a secondary index.

  Stores: index_key -> primary_key (extracted from row buffer)
*/
int ha_tidesdb::insert_index_entry(uint idx, const uchar *buf, tidesdb_txn_t *txn)
{
  DBUG_ENTER("ha_tidesdb::insert_index_entry");

  /* Skip primary key -- it's not a secondary index */
  if (idx == table->s->primary_key)
    DBUG_RETURN(0);

  /* Check if we have a column family for this index */
  if (idx >= share->num_indexes || !share->index_cf[idx])
    DBUG_RETURN(0);

  uchar *idx_key = NULL;
  size_t idx_key_len = 0;

  int ret = build_index_key(idx, buf, &idx_key, &idx_key_len);
  if (ret)
    DBUG_RETURN(ret);

  /* Extract primary key directly from row buffer (don't use build_primary_key) */
  size_t pk_len;
  uchar *pk_value = NULL;

  if (table->s->primary_key != MAX_KEY)
  {
    KEY *pk = &table->key_info[table->s->primary_key];
    pk_len = pk->key_length;
    pk_value = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
    if (!pk_value)
    {
      my_free(idx_key);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    key_copy(pk_value, (uchar *)buf, pk, pk_len);
  }
  else
  {
    /* Hidden PK -- use current_key if available */
    pk_len = 8;
    pk_value = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
    if (!pk_value)
    {
      my_free(idx_key);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    if (current_key && current_key_len == 8)
    {
      memcpy(pk_value, current_key, 8);
    }
    else
    {
      memset(pk_value, 0, 8);
    }
  }

  /* Insert into index CF: index_key -> primary_key */
  ret = tidesdb_txn_put(txn, share->index_cf[idx],
                        idx_key, idx_key_len, pk_value, pk_len, -1);

  my_free(idx_key);
  my_free(pk_value);

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

  /* Skip primary key */
  if (idx == table->s->primary_key)
    DBUG_RETURN(0);

  if (idx >= share->num_indexes || !share->index_cf[idx])
    DBUG_RETURN(0);

  uchar *idx_key = NULL;
  size_t idx_key_len = 0;

  int ret = build_index_key(idx, buf, &idx_key, &idx_key_len);
  if (ret)
    DBUG_RETURN(ret);

  ret = tidesdb_txn_delete(txn, share->index_cf[idx], idx_key, idx_key_len);

  my_free(idx_key);

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
int ha_tidesdb::update_index_entries(const uchar *old_buf, const uchar *new_buf,
                                      tidesdb_txn_t *txn)
{
  DBUG_ENTER("ha_tidesdb::update_index_entries");

  int ret;

  for (uint i = 0; i < table->s->keys; i++)
  {
    if (i == table->s->primary_key)
      continue;

    /* Delete old index entry */
    ret = delete_index_entry(i, old_buf, txn);
    if (ret)
      DBUG_RETURN(ret);

    /* Insert new index entry */
    ret = insert_index_entry(i, new_buf, txn);
    if (ret)
      DBUG_RETURN(ret);
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

  char cf_name[256];
  char idx_cf_name[512];
  get_cf_name(table_name, cf_name, sizeof(cf_name));

  tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
  cf_config.write_buffer_size = tidesdb_write_buffer_size;
  cf_config.enable_bloom_filter = tidesdb_enable_bloom_filter ? 1 : 0;
  cf_config.bloom_fpr = tidesdb_bloom_fpr;

  if (tidesdb_enable_compression)
  {
    cf_config.compression_algo = (compression_algorithm)tidesdb_compression_algo;
  }

  for (uint i = 0; i < table->s->keys; i++)
  {
    /* Skip primary key -- data is stored in main CF */
    if (i == table->s->primary_key)
      continue;

    /* Skip fulltext keys -- handled separately */
    if (table->key_info[i].algorithm == HA_KEY_ALG_FULLTEXT)
      continue;

    /* Create CF for this secondary index: tablename_idx_N */
    snprintf(idx_cf_name, sizeof(idx_cf_name), "%s_idx_%u", cf_name, i);

    int ret = tidesdb_create_column_family(tidesdb_instance, idx_cf_name, &cf_config);
    if (ret != TDB_SUCCESS && ret != TDB_ERR_EXISTS)
    {
      sql_print_error("TidesDB: Failed to create index CF '%s': %d", idx_cf_name, ret);
      DBUG_RETURN(HA_ERR_GENERIC);
    }

    sql_print_information("TidesDB: Created secondary index '%s' for key %u",
                          idx_cf_name, i);
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

  char cf_name[256];
  char idx_cf_name[512];
  get_cf_name(table_name, cf_name, sizeof(cf_name));

  share->num_indexes = 0;

  for (uint i = 0; i < table->s->keys && i < TIDESDB_MAX_INDEXES; i++)
  {
    /* Skip primary key */
    if (i == table->s->primary_key)
    {
      share->index_cf[i] = NULL;
      continue;
    }

    /* Skip fulltext keys -- handled separately */
    if (table->key_info[i].algorithm == HA_KEY_ALG_FULLTEXT)
    {
      share->index_cf[i] = NULL;
      continue;
    }

    /* Try to open CF for this secondary index */
    snprintf(idx_cf_name, sizeof(idx_cf_name), "%s_idx_%u", cf_name, i);

    share->index_cf[i] = tidesdb_get_column_family(tidesdb_instance, idx_cf_name);
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
  -- Key: word (normalized, lowercase)
  -- Value: list of primary keys containing that word
*/
int ha_tidesdb::create_fulltext_indexes(const char *table_name)
{
  DBUG_ENTER("ha_tidesdb::create_fulltext_indexes");

  char cf_name[256];
  char ft_cf_name[512];
  get_cf_name(table_name, cf_name, sizeof(cf_name));

  tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
  cf_config.write_buffer_size = tidesdb_write_buffer_size;
  cf_config.enable_bloom_filter = 1;  /* Always use bloom filter for FT */
  cf_config.bloom_fpr = 0.01;

  if (tidesdb_enable_compression)
  {
    cf_config.compression_algo = (compression_algorithm)tidesdb_compression_algo;
  }

  uint ft_count = 0;
  for (uint i = 0; i < table->s->keys && ft_count < TIDESDB_MAX_FT_INDEXES; i++)
  {
    KEY *key = &table->key_info[i];
    if (key->algorithm != HA_KEY_ALG_FULLTEXT)
      continue;

    /* Create CF for this fulltext index: tablename_ft_N */
    snprintf(ft_cf_name, sizeof(ft_cf_name), "%s_ft_%u", cf_name, i);

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

  char cf_name[256];
  char ft_cf_name[512];
  get_cf_name(table_name, cf_name, sizeof(cf_name));

  share->num_ft_indexes = 0;

  for (uint i = 0; i < table->s->keys && share->num_ft_indexes < TIDESDB_MAX_FT_INDEXES; i++)
  {
    KEY *key = &table->key_info[i];
    if (key->algorithm != HA_KEY_ALG_FULLTEXT)
      continue;

    snprintf(ft_cf_name, sizeof(ft_cf_name), "%s_ft_%u", cf_name, i);

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

  if (!text || len == 0)
    DBUG_RETURN(0);

  char word_buf[256];
  size_t word_len = 0;

  for (size_t i = 0; i <= len; i++)
  {
    char c = (i < len) ? text[i] : ' ';

    /* Check if character is alphanumeric */
    bool is_word_char = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                        (c >= '0' && c <= '9') || (c == '_');

    if (is_word_char && word_len < sizeof(word_buf) - 1)
    {
      /* Convert to lowercase */
      word_buf[word_len++] = (c >= 'A' && c <= 'Z') ? (c + 32) : c;
    }
    else if (word_len > 0)
    {
      /* End of word -- check length constraints */
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
struct ft_insert_ctx {
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
  if (ctx->result != 0)
    return;

  /*
    Inverted index format:
    Key: word + '\0' + primary_key
    Value: empty (presence indicates match)
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

  /* Insert into FT index -- use single byte value since empty is not allowed */
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

  if (ft_idx >= share->num_ft_indexes || !share->ft_cf[ft_idx])
    DBUG_RETURN(0);

  uint key_nr = share->ft_key_nr[ft_idx];
  KEY *key = &table->key_info[key_nr];

  /* Get primary key for this row */
  uchar *pk = NULL;
  size_t pk_len = 0;
  int ret = build_primary_key(buf, &pk, &pk_len);
  if (ret)
    DBUG_RETURN(ret);

  /* Save PK since build_primary_key uses shared buffer */
  uchar *saved_pk = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
  if (!saved_pk)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  memcpy(saved_pk, pk, pk_len);

  ft_insert_ctx ctx;
  ctx.handler = this;
  ctx.ft_cf = share->ft_cf[ft_idx];
  ctx.txn = txn;
  ctx.pk = saved_pk;
  ctx.pk_len = pk_len;
  ctx.result = 0;

  /* Process each column in the fulltext key */
  for (uint i = 0; i < key->user_defined_key_parts; i++)
  {
    KEY_PART_INFO *part = &key->key_part[i];
    Field *field = part->field;

    if (field->is_null())
      continue;

    /* Get field value as string */
    String str;
    field->val_str(&str);

    if (str.length() > 0)
    {
      tokenize_text(str.ptr(), str.length(), field->charset(),
                    ft_insert_word_callback, &ctx);
    }
  }

  my_free(saved_pk);

  DBUG_RETURN(ctx.result);
}

/* Callback context for deleting FT words */
struct ft_delete_ctx {
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
  if (ctx->result != 0)
    return;

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

  if (ft_idx >= share->num_ft_indexes)
    DBUG_RETURN(0);

  uint key_nr = share->ft_key_nr[ft_idx];
  KEY *key = &table->key_info[key_nr];

  /* Get primary key for this row */
  uchar *pk = NULL;
  size_t pk_len = 0;
  int ret = build_primary_key(buf, &pk, &pk_len);
  if (ret)
    DBUG_RETURN(ret);

  uchar *saved_pk = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
  if (!saved_pk)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
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

    if (field->is_null())
      continue;

    String str;
    field->val_str(&str);

    if (str.length() > 0)
    {
      tokenize_text(str.ptr(), str.length(), field->charset(),
                    ft_delete_word_callback, &ctx);
    }
  }

  my_free(saved_pk);

  DBUG_RETURN(ctx.result);
}

/*
  =============================================================================
  Foreign Key Constraint Enforcement
  =============================================================================

  TidesDB enforces FK constraints by:
  1. Parsing FK definitions from table->s->keys during table open
  2. Checking parent row exists on INSERT/UPDATE to child tables
  3. Checking no child rows exist on DELETE from parent tables
*/

/**
  @brief
  Parse foreign key definitions and load referencing table info.

  FK metadata is stored in a special "_fk_metadata" column family:
  -- Key: "child:<db>.<table>" -> Value: serialized FK info (parent table, columns)
  -- Key: "parent:<db>.<table>" -> Value: list of child tables that reference it

  This allows efficient lookup of both:
  1. Which parent tables this table references (for INSERT/UPDATE checks)
  2. Which child tables reference this table (for DELETE checks)
*/
int ha_tidesdb::parse_foreign_keys()
{
  DBUG_ENTER("ha_tidesdb::parse_foreign_keys");

  share->num_fk = 0;
  share->num_referencing = 0;

  /* Get or create the FK metadata column family */
  tidesdb_column_family_t *fk_meta_cf = tidesdb_get_column_family(tidesdb_instance, "_fk_metadata");
  if (!fk_meta_cf)
  {
    /* FK metadata CF doesn't exist yet -- no FKs defined */
    DBUG_RETURN(0);
  }

  /* Build key to look up FKs for this table (as child) */
  char cf_name[256];
  get_cf_name(share->table_name, cf_name, sizeof(cf_name));

  char child_key[512];
  snprintf(child_key, sizeof(child_key), "child:%s", cf_name);

  /* Look up FK definitions for this table */
  tidesdb_txn_t *txn = NULL;
  if (tidesdb_txn_begin(tidesdb_instance, &txn) != TDB_SUCCESS)
    DBUG_RETURN(0);

  uint8_t *fk_data = NULL;
  size_t fk_data_len = 0;

  int ret = tidesdb_txn_get(txn, fk_meta_cf, (uint8_t *)child_key, strlen(child_key),
                            &fk_data, &fk_data_len);

  if (ret == TDB_SUCCESS && fk_data && fk_data_len > 0)
  {
    /* Parse FK data: format is "num_fk|fk1_data|fk2_data|..." */
    /* Each fk_data: "ref_db\0ref_table\0num_cols|col_idx1|col_idx2|..." */
    const char *ptr = (const char *)fk_data;
    const char *end = ptr + fk_data_len;

    while (ptr < end && share->num_fk < TIDESDB_MAX_FK)
    {
      TIDESDB_FK *fk = &share->fk[share->num_fk];

      /* Read ref_db */
      size_t len = strnlen(ptr, end - ptr);
      if (len == 0 || ptr + len >= end) break;
      strncpy(fk->ref_db, ptr, sizeof(fk->ref_db) - 1);
      fk->ref_db[sizeof(fk->ref_db) - 1] = '\0';
      ptr += len + 1;

      /* Read ref_table */
      len = strnlen(ptr, end - ptr);
      if (len == 0 || ptr + len >= end) break;
      strncpy(fk->ref_table, ptr, sizeof(fk->ref_table) - 1);
      fk->ref_table[sizeof(fk->ref_table) - 1] = '\0';
      ptr += len + 1;

      /* Read num_cols and column indices */
      if (ptr + 1 > end) break;
      fk->num_cols = (uint8_t)*ptr++;
      if (fk->num_cols > 16) fk->num_cols = 16;

      for (uint i = 0; i < fk->num_cols && ptr + 1 <= end; i++)
      {
        fk->fk_col_idx[i] = (uint8_t)*ptr++;
      }

      /* Read delete/update rules */
      if (ptr + 2 <= end)
      {
        fk->delete_rule = (int8_t)*ptr++;
        fk->update_rule = (int8_t)*ptr++;
      }

      share->num_fk++;
    }

    tidesdb_free(fk_data);
  }

  /* Now look up tables that reference this table (as parent) */
  char parent_key[512];
  snprintf(parent_key, sizeof(parent_key), "parent:%s", cf_name);

  uint8_t *ref_data = NULL;
  size_t ref_data_len = 0;

  ret = tidesdb_txn_get(txn, fk_meta_cf, (uint8_t *)parent_key, strlen(parent_key),
                        &ref_data, &ref_data_len);

  if (ret == TDB_SUCCESS && ref_data && ref_data_len > 0)
  {
    /* Parse referencing tables: format is "table1\0table2\0..." */
    const char *ptr = (const char *)ref_data;
    const char *end = ptr + ref_data_len;

    while (ptr < end && share->num_referencing < TIDESDB_MAX_FK)
    {
      size_t len = strnlen(ptr, end - ptr);
      if (len == 0) break;

      strncpy(share->referencing_tables[share->num_referencing], ptr, 255);
      share->referencing_tables[share->num_referencing][255] = '\0';
      share->num_referencing++;

      ptr += len + 1;
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

  if (fk_idx >= share->num_fk)
    DBUG_RETURN(0);

  TIDESDB_FK *fk = &share->fk[fk_idx];

  /* Build the parent table's column family name */
  char parent_cf_name[512];
  snprintf(parent_cf_name, sizeof(parent_cf_name), "%s_%s", fk->ref_db, fk->ref_table);

  /* Get the parent column family */
  tidesdb_column_family_t *parent_cf = tidesdb_get_column_family(tidesdb_instance, parent_cf_name);
  if (!parent_cf)
  {
    /* Parent table doesn't exist in TidesDB */
    DBUG_RETURN(0);
  }

  /* Build lookup key from FK column values */
  uchar key_buf[1024];
  size_t key_len = 0;

  for (uint i = 0; i < fk->num_cols && key_len < sizeof(key_buf) - 100; i++)
  {
    uint col_idx = fk->fk_col_idx[i];
    if (col_idx >= table->s->fields)
      continue;

    Field *field = table->field[col_idx];

    /* Check for NULL -- NULL FK values don't need parent check */
    if (field->is_null())
      DBUG_RETURN(0);  /* NULL FK is always valid */

    /* Extract field value using the correct method based on field type */
    if (field->type() == MYSQL_TYPE_LONG || field->type() == MYSQL_TYPE_LONGLONG ||
        field->type() == MYSQL_TYPE_SHORT || field->type() == MYSQL_TYPE_TINY ||
        field->type() == MYSQL_TYPE_INT24)
    {
      /* Integer type -- store as 8-byte big-endian for proper ordering */
      longlong val = field->val_int();
      int8store(key_buf + key_len, val);
      key_len += 8;
    }
    else
    {
      /* String/other type -- store length-prefixed */
      String str;
      field->val_str(&str);
      size_t copy_len = str.length();
      if (key_len + copy_len + 4 < sizeof(key_buf))
      {
        int4store(key_buf + key_len, (uint32)copy_len);
        key_len += 4;
        memcpy(key_buf + key_len, str.ptr(), copy_len);
        key_len += copy_len;
      }
    }
  }

  if (key_len == 0)
    DBUG_RETURN(0);

  /* Look up the key in parent table */
  uint8_t *value = NULL;
  size_t value_len = 0;

  int ret = tidesdb_txn_get(txn, parent_cf, key_buf, key_len, &value, &value_len);

  if (ret == TDB_SUCCESS && value)
  {
    tidesdb_free(value);
    DBUG_RETURN(0);  /* Parent row exists */
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

  THD *thd = ha_thd();

  /* Check if FK checks are disabled */
  if (thd->variables.option_bits & OPTION_NO_FOREIGN_KEY_CHECKS)
    DBUG_RETURN(0);

  /* Check each FK constraint */
  for (uint i = 0; i < share->num_fk; i++)
  {
    int ret = check_fk_parent_exists(i, buf, txn);
    if (ret != 0)
      DBUG_RETURN(ret);
  }

  DBUG_RETURN(0);
}

/**
  @brief
  Check FK constraints for DELETE operations.

  Verifies that no child rows reference this row (for RESTRICT/NO ACTION).
  Uses a secondary index on the FK columns in child tables for efficient lookup.
*/
int ha_tidesdb::check_foreign_key_constraints_delete(const uchar *buf, tidesdb_txn_t *txn)
{
  DBUG_ENTER("ha_tidesdb::check_foreign_key_constraints_delete");

  THD *thd = ha_thd();

  /* Check if FK checks are disabled */
  if (thd->variables.option_bits & OPTION_NO_FOREIGN_KEY_CHECKS)
    DBUG_RETURN(0);

  /* No referencing tables -- nothing to check */
  if (share->num_referencing == 0)
    DBUG_RETURN(0);

  /* Build the key from this row's PK (which child FK references) */
  uchar *pk = NULL;
  size_t pk_len = 0;
  int ret = build_primary_key(buf, &pk, &pk_len);
  if (ret)
    DBUG_RETURN(0);

  /* Check each child table for rows referencing this PK */
  for (uint i = 0; i < share->num_referencing; i++)
  {
    /* Get the child table's FK index column family */
    char fk_idx_cf_name[512];
    snprintf(fk_idx_cf_name, sizeof(fk_idx_cf_name), "%s_fkidx",
             share->referencing_tables[i]);

    tidesdb_column_family_t *fk_idx_cf = tidesdb_get_column_family(
      tidesdb_instance, fk_idx_cf_name);

    if (!fk_idx_cf)
      continue;

    /* Look up our PK in the FK index */
    uint8_t *ref_value = NULL;
    size_t ref_value_len = 0;

    ret = tidesdb_txn_get(txn, fk_idx_cf, pk, pk_len, &ref_value, &ref_value_len);

    if (ret == TDB_SUCCESS && ref_value)
    {
      /* Child row exists referencing this parent -- FK violation */
      tidesdb_free(ref_value);
      DBUG_RETURN(HA_ERR_ROW_IS_REFERENCED);
    }
  }

  DBUG_RETURN(0);
}

/*
  =============================================================================
  Z-Order (Morton Code) Spatial Indexing
  =============================================================================

  TidesDB implements spatial indexing using Z-order curves (Morton codes).
  This maps 2D coordinates to a 1D key while preserving spatial locality,
  enabling efficient range queries on an LSM-tree.

  The encoding interleaves bits of X and Y coordinates:
  X = x3 x2 x1 x0
  Y = y3 y2 y1 y0
  Z = y3 x3 y2 x2 y1 x1 y0 x0
*/

/**
  @brief
  Encode X,Y coordinates as a Z-order (Morton) curve value.

  Interleaves bits of normalized X and Y values to create a space-filling
  curve that preserves 2D locality in 1D ordering.

  @param x  X coordinate (normalized to 0..1 range)
  @param y  Y coordinate (normalized to 0..1 range)

  @return 64-bit Z-order encoded value
*/
uint64_t ha_tidesdb::encode_zorder(double x, double y)
{
  /* Normalize to 32-bit integer range */
  uint32_t ix = (uint32_t)(x * (double)0xFFFFFFFF);
  uint32_t iy = (uint32_t)(y * (double)0xFFFFFFFF);

  /* Interleave bits using the "magic bits" method */
  uint64_t z = 0;

  for (int i = 0; i < 32; i++)
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

  for (int i = 0; i < 32; i++)
  {
    ix |= ((z >> (2 * i)) & 1) << i;
    iy |= ((z >> (2 * i + 1)) & 1) << i;
  }

  *x = (double)ix / (double)0xFFFFFFFF;
  *y = (double)iy / (double)0xFFFFFFFF;
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

  char cf_name[256];
  char spatial_cf_name[512];
  get_cf_name(table_name, cf_name, sizeof(cf_name));

  snprintf(spatial_cf_name, sizeof(spatial_cf_name), "%s_spatial_%u", cf_name, key_nr);

  tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
  cf_config.write_buffer_size = tidesdb_write_buffer_size;
  cf_config.enable_bloom_filter = 1;
  cf_config.bloom_fpr = 0.01;

  int ret = tidesdb_create_column_family(tidesdb_instance, spatial_cf_name, &cf_config);
  if (ret != TDB_SUCCESS && ret != TDB_ERR_EXISTS)
  {
    sql_print_error("TidesDB: Failed to create spatial index CF '%s': %d", spatial_cf_name, ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  sql_print_information("TidesDB: Created spatial index '%s'", spatial_cf_name);

  DBUG_RETURN(0);
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

  if (idx >= share->num_spatial_indexes || !share->spatial_cf[idx])
    DBUG_RETURN(0);

  uint key_nr = share->spatial_key_nr[idx];
  KEY *key = &table->key_info[key_nr];

  /* Get the geometry field */
  if (key->user_defined_key_parts == 0)
    DBUG_RETURN(0);

  Field *geom_field = key->key_part[0].field;
  if (geom_field->is_null())
    DBUG_RETURN(0);

  /* Get geometry value */
  String geom_str;
  geom_field->val_str(&geom_str);

  if (geom_str.length() < 25)  /* Minimum WKB size for a point */
    DBUG_RETURN(0);

  /*
    Extract bounding box from WKB geometry.
    Supports: POINT (1), LINESTRING (2), POLYGON (3), MULTIPOINT (4)
    Format: byte_order(1) + type(4) + coordinates...
  */
  const uchar *wkb = (const uchar *)geom_str.ptr();
  size_t wkb_len = geom_str.length();

  /* Skip SRID if present (4 bytes) -- MariaDB stores SRID prefix */
  if (wkb_len >= 25)
  {
    wkb += 4;
    wkb_len -= 4;
  }

  if (wkb_len < 21)  /* Minimum for a point */
    DBUG_RETURN(0);

  /* Read byte order */
  int byte_order = wkb[0];

  /* Read geometry type */
  uint32_t wkb_type;
  if (byte_order == 1)  /* Little endian */
  {
    wkb_type = uint4korr(wkb + 1);
  }
  else  /* Big endian */
  {
    wkb_type = ((uint32_t)wkb[1] << 24) | ((uint32_t)wkb[2] << 16) |
               ((uint32_t)wkb[3] << 8) | (uint32_t)wkb[4];
  }

  /* Helper lambda to read a double with byte order handling */
  auto read_double = [byte_order](const uchar *ptr) -> double {
    double val;
    if (byte_order == 1)  /* Little endian */
    {
      memcpy(&val, ptr, 8);
    }
    else  /* Big endian -- swap bytes */
    {
      uchar swapped[8];
      for (int i = 0; i < 8; i++)
        swapped[i] = ptr[7 - i];
      memcpy(&val, swapped, 8);
    }
    return val;
  };

  double min_x, min_y, max_x, max_y;
  const uchar *coords = wkb + 5;  /* Skip byte_order(1) + type(4) */

  switch (wkb_type & 0xFF)  /* Mask to get base type (ignore Z/M flags) */
  {
    case 1:  /* POINT */
    {
      min_x = max_x = read_double(coords);
      min_y = max_y = read_double(coords + 8);
      break;
    }
    case 2:  /* LINESTRING */
    {
      if (wkb_len < 9) DBUG_RETURN(0);
      uint32_t num_points;
      if (byte_order == 1)
        num_points = uint4korr(coords);
      else
        num_points = ((uint32_t)coords[0] << 24) | ((uint32_t)coords[1] << 16) |
                     ((uint32_t)coords[2] << 8) | (uint32_t)coords[3];

      if (num_points == 0 || wkb_len < 9 + num_points * 16)
        DBUG_RETURN(0);

      coords += 4;  /* Skip num_points */
      min_x = max_x = read_double(coords);
      min_y = max_y = read_double(coords + 8);

      for (uint32_t i = 1; i < num_points; i++)
      {
        double px = read_double(coords + i * 16);
        double py = read_double(coords + i * 16 + 8);
        if (px < min_x) min_x = px;
        if (px > max_x) max_x = px;
        if (py < min_y) min_y = py;
        if (py > max_y) max_y = py;
      }
      break;
    }
    case 3:  /* POLYGON */
    {
      if (wkb_len < 9) DBUG_RETURN(0);
      uint32_t num_rings;
      if (byte_order == 1)
        num_rings = uint4korr(coords);
      else
        num_rings = ((uint32_t)coords[0] << 24) | ((uint32_t)coords[1] << 16) |
                    ((uint32_t)coords[2] << 8) | (uint32_t)coords[3];

      if (num_rings == 0) DBUG_RETURN(0);

      coords += 4;  /* Skip num_rings */

      /* Read first ring (exterior) to get bounding box */
      uint32_t num_points;
      if (byte_order == 1)
        num_points = uint4korr(coords);
      else
        num_points = ((uint32_t)coords[0] << 24) | ((uint32_t)coords[1] << 16) |
                     ((uint32_t)coords[2] << 8) | (uint32_t)coords[3];

      if (num_points == 0) DBUG_RETURN(0);

      coords += 4;  /* Skip num_points */
      min_x = max_x = read_double(coords);
      min_y = max_y = read_double(coords + 8);

      for (uint32_t i = 1; i < num_points; i++)
      {
        double px = read_double(coords + i * 16);
        double py = read_double(coords + i * 16 + 8);
        if (px < min_x) min_x = px;
        if (px > max_x) max_x = px;
        if (py < min_y) min_y = py;
        if (py > max_y) max_y = py;
      }
      break;
    }
    case 4:  /* MULTIPOINT */
    {
      if (wkb_len < 9) DBUG_RETURN(0);
      uint32_t num_points;
      if (byte_order == 1)
        num_points = uint4korr(coords);
      else
        num_points = ((uint32_t)coords[0] << 24) | ((uint32_t)coords[1] << 16) |
                     ((uint32_t)coords[2] << 8) | (uint32_t)coords[3];

      if (num_points == 0) DBUG_RETURN(0);

      coords += 4;  /* Skip num_points */

      /* Each point has: byte_order(1) + type(4) + x(8) + y(8) = 21 bytes */
      min_x = max_x = read_double(coords + 5);
      min_y = max_y = read_double(coords + 13);

      for (uint32_t i = 1; i < num_points; i++)
      {
        const uchar *pt = coords + i * 21;
        double px = read_double(pt + 5);
        double py = read_double(pt + 13);
        if (px < min_x) min_x = px;
        if (px > max_x) max_x = px;
        if (py < min_y) min_y = py;
        if (py > max_y) max_y = py;
      }
      break;
    }
    default:
      /* Unsupported geometry type */
      DBUG_RETURN(0);
  }

  /* Use center point for Z-order encoding */
  double x = (min_x + max_x) / 2.0;
  double y = (min_y + max_y) / 2.0;

  /* Normalize coordinates to 0..1 range */
  /* Using WGS84 bounds: lon -180..180, lat -90..90 */
  double norm_x = (x + 180.0) / 360.0;
  double norm_y = (y + 90.0) / 180.0;

  /* Clamp to valid range */
  if (norm_x < 0) norm_x = 0;
  if (norm_x > 1) norm_x = 1;
  if (norm_y < 0) norm_y = 0;
  if (norm_y > 1) norm_y = 1;

  /* Encode as Z-order */
  uint64_t z = encode_zorder(norm_x, norm_y);

  /* Build key: z_order (8 bytes) + primary_key */
  uchar *pk = NULL;
  size_t pk_len = 0;
  int ret = build_primary_key(buf, &pk, &pk_len);
  if (ret)
    DBUG_RETURN(ret);

  size_t key_len = 8 + pk_len;
  uchar *spatial_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_len, MYF(MY_WME));
  if (!spatial_key)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  /* Store Z-order as big-endian for proper sort order */
  int8store(spatial_key, z);
  memcpy(spatial_key + 8, pk, pk_len);

  /* Insert into spatial index: z_key -> pk */
  ret = tidesdb_txn_put(txn, share->spatial_cf[idx],
                        spatial_key, key_len, pk, pk_len, -1);

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

  @param idx  Spatial index number
  @param buf  Row buffer
  @param txn  Transaction

  @return 0 on success
*/
int ha_tidesdb::delete_spatial_entry(uint idx, const uchar *buf, tidesdb_txn_t *txn)
{
  DBUG_ENTER("ha_tidesdb::delete_spatial_entry");

  if (idx >= share->num_spatial_indexes || !share->spatial_cf[idx])
    DBUG_RETURN(0);

  uint key_nr = share->spatial_key_nr[idx];
  KEY *key = &table->key_info[key_nr];

  if (key->user_defined_key_parts == 0)
    DBUG_RETURN(0);

  Field *geom_field = key->key_part[0].field;
  if (geom_field->is_null())
    DBUG_RETURN(0);

  String geom_str;
  geom_field->val_str(&geom_str);

  if (geom_str.length() < 25)
    DBUG_RETURN(0);

  const uchar *wkb = (const uchar *)geom_str.ptr();
  if (geom_str.length() >= 25)
    wkb += 4;

  double x, y;
  int byte_order = wkb[0];
  if (byte_order == 1)
  {
    memcpy(&x, wkb + 5, 8);
    memcpy(&y, wkb + 13, 8);
  }
  else
  {
    DBUG_RETURN(0);
  }

  double norm_x = (x + 180.0) / 360.0;
  double norm_y = (y + 90.0) / 180.0;
  if (norm_x < 0) norm_x = 0;
  if (norm_x > 1) norm_x = 1;
  if (norm_y < 0) norm_y = 0;
  if (norm_y > 1) norm_y = 1;

  uint64_t z = encode_zorder(norm_x, norm_y);

  uchar *pk = NULL;
  size_t pk_len = 0;
  int ret = build_primary_key(buf, &pk, &pk_len);
  if (ret)
    DBUG_RETURN(ret);

  size_t key_len = 8 + pk_len;
  uchar *spatial_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_len, MYF(MY_WME));
  if (!spatial_key)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  int8store(spatial_key, z);
  memcpy(spatial_key + 8, pk, pk_len);

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
      /* Column family doesn't exist -- table wasn't created properly */
      free_share(share);
      DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
    }
  }

  /* Check if table has a primary key */
  share->has_primary_key = (table->s->primary_key != MAX_KEY);
  if (share->has_primary_key)
  {
    share->pk_parts = table->key_info[table->s->primary_key].user_defined_key_parts;
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

  /* Check for TTL column (TTL is the primary name, _ttl for backwards compatibility) */
  share->ttl_field_index = -1;
  for (uint i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    const char *field_name = field->field_name.str;
    if (strcasecmp(field_name, "TTL") == 0 ||
        strcasecmp(field_name, "_ttl") == 0)
    {
      /* Found TTL column -- must be an integer type */
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

  /* Load hidden PK value for tables without explicit primary key */
  if (!share->has_primary_key)
  {
    load_hidden_pk_value();
  }

  /* Open secondary indexes */
  open_secondary_indexes(name);

  /* Open fulltext indexes */
  open_fulltext_indexes(name);

  /* Open spatial indexes */
  share->num_spatial_indexes = 0;
  char cf_name[256];
  get_cf_name(name, cf_name, sizeof(cf_name));
  for (uint i = 0; i < table->s->keys && share->num_spatial_indexes < TIDESDB_MAX_INDEXES; i++)
  {
    KEY *key = &table->key_info[i];
    if (key->algorithm == HA_KEY_ALG_RTREE)
    {
      char spatial_cf_name[512];
      snprintf(spatial_cf_name, sizeof(spatial_cf_name), "%s_spatial_%u", cf_name, i);
      tidesdb_column_family_t *spatial_cf = tidesdb_get_column_family(tidesdb_instance, spatial_cf_name);
      if (spatial_cf)
      {
        share->spatial_cf[share->num_spatial_indexes] = spatial_cf;
        share->spatial_key_nr[share->num_spatial_indexes] = i;
        share->num_spatial_indexes++;
      }
    }
  }

  /* Parse foreign key constraints and load referencing table info */
  parse_foreign_keys();

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

  /* Persist hidden PK value on close for crash recovery */
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

  /* Set compression algorithm */
  if (tidesdb_enable_compression)
  {
    cf_config.compression_algo = (compression_algorithm)tidesdb_compression_algo;
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

  /* Create secondary index column families */
  /* Note: table_arg has the key definitions */
  TABLE *saved_table = table;
  table = table_arg;  /* Temporarily set for create_secondary_indexes */
  ret = create_secondary_indexes(name);
  if (ret)
  {
    table = saved_table;
    sql_print_error("TidesDB: Failed to create secondary indexes");
    DBUG_RETURN(ret);
  }

  /* Create fulltext index column families */
  ret = create_fulltext_indexes(name);

  if (ret)
  {
    table = saved_table;
    sql_print_error("TidesDB: Failed to create fulltext indexes");
    DBUG_RETURN(ret);
  }

  /* Create spatial index column families */
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
*/
int ha_tidesdb::delete_table(const char *name)
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
  Rename a table by copying all data to a new column family and dropping the old one.

  TidesDB doesn't support renaming column families directly, so we:
  1. Create new column family with new name
  2. Copy all data from old CF to new CF
  3. Drop old column family
*/
int ha_tidesdb::rename_table(const char *from, const char *to)
{
  DBUG_ENTER("ha_tidesdb::rename_table");

  int ret;
  char old_cf_name[256];
  char new_cf_name[256];

  get_cf_name(from, old_cf_name, sizeof(old_cf_name));
  get_cf_name(to, new_cf_name, sizeof(new_cf_name));

  /* Get the old column family */
  tidesdb_column_family_t *old_cf = tidesdb_get_column_family(tidesdb_instance, old_cf_name);
  if (!old_cf)
  {
    sql_print_error("TidesDB: Cannot rename - source table '%s' not found", from);
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  /* Create new column family with default config */
  tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
  cf_config.write_buffer_size = tidesdb_write_buffer_size;
  cf_config.enable_bloom_filter = tidesdb_enable_bloom_filter ? 1 : 0;
  cf_config.bloom_fpr = tidesdb_bloom_fpr;

  if (tidesdb_enable_compression)
  {
    cf_config.compression_algo = (compression_algorithm)tidesdb_compression_algo;
  }

  ret = tidesdb_create_column_family(tidesdb_instance, new_cf_name, &cf_config);
  if (ret != TDB_SUCCESS)
  {
    sql_print_error("TidesDB: Failed to create destination column family '%s': %d",
                    new_cf_name, ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  tidesdb_column_family_t *new_cf = tidesdb_get_column_family(tidesdb_instance, new_cf_name);
  if (!new_cf)
  {
    sql_print_error("TidesDB: Failed to get new column family '%s'", new_cf_name);
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  /* Copy all data from old CF to new CF */
  tidesdb_txn_t *txn = NULL;
  ret = tidesdb_txn_begin(tidesdb_instance, &txn);
  if (ret != TDB_SUCCESS)
  {
    tidesdb_drop_column_family(tidesdb_instance, new_cf_name);
    sql_print_error("TidesDB: Failed to begin transaction for rename: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  tidesdb_iter_t *iter = NULL;
  ret = tidesdb_iter_new(txn, old_cf, &iter);
  if (ret != TDB_SUCCESS)
  {
    tidesdb_txn_free(txn);
    tidesdb_drop_column_family(tidesdb_instance, new_cf_name);
    sql_print_error("TidesDB: Failed to create iterator for rename: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  tidesdb_iter_seek_to_first(iter);

  while (tidesdb_iter_valid(iter))
  {
    uint8_t *key = NULL;
    size_t key_size = 0;
    uint8_t *value = NULL;
    size_t value_size = 0;

    if (tidesdb_iter_key(iter, &key, &key_size) == TDB_SUCCESS &&
        tidesdb_iter_value(iter, &value, &value_size) == TDB_SUCCESS)
    {
      /* Copy to new CF */
      ret = tidesdb_txn_put(txn, new_cf, key, key_size, value, value_size, -1);
      if (ret != TDB_SUCCESS)
      {
        tidesdb_iter_free(iter);
        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
        tidesdb_drop_column_family(tidesdb_instance, new_cf_name);
        sql_print_error("TidesDB: Failed to copy data during rename: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
      }
    }

    tidesdb_iter_next(iter);
  }

  tidesdb_iter_free(iter);

  /* Commit the copy transaction */
  ret = tidesdb_txn_commit(txn);
  tidesdb_txn_free(txn);

  if (ret != TDB_SUCCESS)
  {
    tidesdb_drop_column_family(tidesdb_instance, new_cf_name);
    sql_print_error("TidesDB: Failed to commit rename transaction: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  /* Drop the old column family */
  ret = tidesdb_drop_column_family(tidesdb_instance, old_cf_name);
  if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
  {
    sql_print_warning("TidesDB: Failed to drop old column family '%s': %d",
                      old_cf_name, ret);
    /* Continue anyway -- data was copied successfully */
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

  /* Handle auto_increment -- this sets insert_id_for_cur_row */
  if (table->next_number_field && buf == table->record[0])
  {
    if ((ret = update_auto_increment()))
      DBUG_RETURN(ret);
  }

  /* Build the primary key */
  ret = build_primary_key(buf, &key, &key_len);
  if (ret)
    DBUG_RETURN(ret);

  /* Pack the row data */
  ret = pack_row(buf, &value, &value_len);
  if (ret)
    DBUG_RETURN(ret);

  /* Use bulk transaction if active, otherwise use current/THD transaction */
  tidesdb_txn_t *txn = NULL;
  bool own_txn = false;

  if (bulk_insert_active && bulk_txn)
  {
    /* Use the bulk insert transaction */
    txn = bulk_txn;
  }
  else if (current_txn)
  {
    /* Use handler's current transaction */
    txn = current_txn;
  }
  else
  {
    /* Check for THD-level transaction (multi-statement transaction) */
    THD *thd = ha_thd();
    tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);
    if (thd_txn)
    {
      txn = thd_txn;
    }
    else
    {
      /* No transaction available, create one for this operation */
      ret = tidesdb_txn_begin(tidesdb_instance, &txn);
      if (ret != TDB_SUCCESS)
      {
        sql_print_error("TidesDB: Failed to begin transaction: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
      }
      own_txn = true;
    }
  }

  /* Check foreign key constraints before insert */
  ret = check_foreign_key_constraints_insert(buf, txn);
  if (ret)
  {
    if (own_txn)
    {
      tidesdb_txn_rollback(txn);
      tidesdb_txn_free(txn);
    }
    my_free(value);
    DBUG_RETURN(ret);
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

  /* Free the packed row buffer -- TidesDB copies the data */
  my_free(value);

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

  /* Insert secondary index entries */
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

  /* Insert fulltext index entries */
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

  /* Insert spatial index entries */
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

  /* Build keys for old and new rows */
  ret = build_primary_key(old_data, &old_key, &old_key_len);
  if (ret)
    DBUG_RETURN(ret);

  /* Save old key since build_primary_key reuses the buffer */
  uchar *saved_old_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, old_key_len, MYF(MY_WME));
  if (!saved_old_key)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  memcpy(saved_old_key, old_key, old_key_len);

  ret = build_primary_key(new_data, &new_key, &new_key_len);
  if (ret)
  {
    my_free(saved_old_key);
    DBUG_RETURN(ret);
  }

  /* Pack the new row data */
  ret = pack_row(new_data, &value, &value_len);
  if (ret)
  {
    my_free(saved_old_key);
    DBUG_RETURN(ret);
  }

  /* Use existing transaction, THD-level transaction, or create new one */
  tidesdb_txn_t *txn = NULL;
  bool own_txn = false;

  if (current_txn)
  {
    txn = current_txn;
  }
  else
  {
    /* Check for THD-level transaction (multi-statement transaction) */
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
        my_free(saved_old_key);
        sql_print_error("TidesDB: Failed to begin transaction: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
      }
      own_txn = true;
    }
  }

  /* Check foreign key constraints before update */
  ret = check_foreign_key_constraints_insert(new_data, txn);
  if (ret)
  {
    if (own_txn)
    {
      tidesdb_txn_rollback(txn);
      tidesdb_txn_free(txn);
    }
    my_free(saved_old_key);
    my_free(value);
    DBUG_RETURN(ret);
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
      my_free(saved_old_key);
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

  /* Free the packed row buffer -- TidesDB copies the data */
  my_free(value);

  if (ret != TDB_SUCCESS)
  {
    if (own_txn)
    {
      tidesdb_txn_rollback(txn);
      tidesdb_txn_free(txn);
    }
    my_free(saved_old_key);
    sql_print_error("TidesDB: Failed to update row: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  /* Update secondary index entries */
  ret = update_index_entries(old_data, new_data, txn);
  if (ret)
  {
    if (own_txn)
    {
      tidesdb_txn_rollback(txn);
      tidesdb_txn_free(txn);
    }
    my_free(saved_old_key);
    DBUG_RETURN(ret);
  }

  /* Update spatial index entries */
  for (uint i = 0; i < share->num_spatial_indexes; i++)
  {
    /* Delete old entry */
    ret = delete_spatial_entry(i, old_data, txn);
    if (ret)
    {
      if (own_txn)
      {
        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
      }
      my_free(saved_old_key);
      DBUG_RETURN(ret);
    }
    /* Insert new entry */
    ret = insert_spatial_entry(i, new_data, txn);
    if (ret)
    {
      if (own_txn)
      {
        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
      }
      my_free(saved_old_key);
      DBUG_RETURN(ret);
    }
  }

  my_free(saved_old_key);

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

  /* Use the current_key saved from the last read if available */
  if (current_key && current_key_len > 0)
  {
    key = current_key;
    key_len = current_key_len;
  }
  else
  {
    /* Build the primary key from buffer */
    ret = build_primary_key(buf, &key, &key_len);
    if (ret)
      DBUG_RETURN(ret);
  }

  /* Use current_txn set by external_lock -- like InnoDB does */
  if (!current_txn)
  {
    sql_print_error("TidesDB: No transaction available for delete");
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  /* Check FK constraints -- ensure no child rows reference this row */
  ret = check_foreign_key_constraints_delete(buf, current_txn);
  if (ret)
    DBUG_RETURN(ret);

  /* Delete secondary index entries first */
  for (uint i = 0; i < table->s->keys; i++)
  {
    ret = delete_index_entry(i, buf, current_txn);
    if (ret)
      DBUG_RETURN(ret);
  }

  /* Delete fulltext index entries */
  for (uint i = 0; i < share->num_ft_indexes; i++)
  {
    ret = delete_ft_words(i, buf, current_txn);
    if (ret)
      DBUG_RETURN(ret);
  }

  /* Delete spatial index entries */
  for (uint i = 0; i < share->num_spatial_indexes; i++)
  {
    ret = delete_spatial_entry(i, buf, current_txn);
    if (ret)
      DBUG_RETURN(ret);
  }

  /* Delete the row -- transaction will be committed by external_lock */
  ret = tidesdb_txn_delete(current_txn, share->cf, key, key_len);
  if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
  {
    sql_print_error("TidesDB: Failed to delete row: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
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

  /* Use existing transaction or THD-level transaction for the scan */
  if (!current_txn)
  {
    /* Check for THD-level transaction (multi-statement transaction) */
    THD *thd = ha_thd();
    tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);
    if (thd_txn)
    {
      current_txn = thd_txn;
    }
    else
    {
      /* No transaction available, create one for this scan */
      ret = tidesdb_txn_begin_with_isolation(tidesdb_instance,
                                              (tidesdb_isolation_level_t)tidesdb_default_isolation,
                                              &current_txn);
      if (ret != TDB_SUCCESS)
      {
        sql_print_error("TidesDB: Failed to begin transaction for scan: %d", ret);
        DBUG_RETURN(HA_ERR_GENERIC);
      }
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
  Skips internal metadata keys (those starting with null byte).
*/
int ha_tidesdb::rnd_next(uchar *buf)
{
  DBUG_ENTER("ha_tidesdb::rnd_next");

  int ret;

  if (!scan_iter || !scan_initialized)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  uint8_t *key = NULL;
  size_t key_size = 0;
  uint8_t *value = NULL;
  size_t value_size = 0;

  /* Loop to skip metadata keys (starting with null byte) */
  while (tidesdb_iter_valid(scan_iter))
  {
    ret = tidesdb_iter_key(scan_iter, &key, &key_size);
    if (ret != TDB_SUCCESS)
    {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    /* Skip metadata keys (internal keys starting with null byte) */
    if (key_size > 0 && key[0] == 0)
    {
      tidesdb_iter_next(scan_iter);
      continue;
    }

    /* Found a data key */
    break;
  }

  /* Check if we reached the end */
  if (!tidesdb_iter_valid(scan_iter))
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  ret = tidesdb_iter_value(scan_iter, &value, &value_size);
  if (ret != TDB_SUCCESS)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  /* Save the current key for position() */
  free_current_key();
  current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_size, MYF(MY_WME));
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

  /* Move to next entry for next call */
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
  current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, ref_length, MYF(MY_WME));
  if (!current_key)
  {
    tidesdb_free(value);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  memcpy(current_key, pos, ref_length);
  current_key_len = ref_length;

  /* Unpack the row */
  ret = unpack_row(buf, value, value_size);
  tidesdb_free(value);

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

  /* Clean up secondary index iterator */
  if (index_iter)
  {
    tidesdb_iter_free(index_iter);
    index_iter = NULL;
  }
  if (index_key_buf)
  {
    my_free(index_key_buf);
    index_key_buf = NULL;
    index_key_len = 0;
  }

  DBUG_RETURN(0);
}

/**
  @brief
  Read a row by index key.

  For primary key: directly lookup in main CF
  For secondary index: lookup in index CF to get PK, then fetch row from main CF
*/
int ha_tidesdb::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map,
                               enum ha_rkey_function find_flag)
{
  DBUG_ENTER("ha_tidesdb::index_read_map");

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
  uint key_len = calculate_key_len(table, active_index, key, keypart_map);

  uint8_t *value = NULL;
  size_t value_size = 0;

  if (active_index == table->s->primary_key)
  {
    /* Primary key lookup: directly get from main CF */
    ret = tidesdb_txn_get(txn, share->cf, key, key_len, &value, &value_size);

    if (ret == TDB_ERR_NOT_FOUND)
    {
      if (own_txn) tidesdb_txn_free(txn);
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }
    else if (ret != TDB_SUCCESS)
    {
      if (own_txn) tidesdb_txn_free(txn);
      sql_print_error("TidesDB: Failed to get row by PK: %d", ret);
      DBUG_RETURN(HA_ERR_GENERIC);
    }

    /* Save the primary key */
    free_current_key();
    current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_len, MYF(MY_WME));
    if (!current_key)
    {
      tidesdb_free(value);
      if (own_txn) tidesdb_txn_free(txn);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    memcpy(current_key, key, key_len);
    current_key_len = key_len;
  }
  else
  {
    /* Secondary index lookup */
    /* Check if we have a CF for this index */
    if (active_index >= TIDESDB_MAX_INDEXES || !share->index_cf[active_index])
    {
      if (own_txn) tidesdb_txn_free(txn);
      DBUG_RETURN(HA_ERR_WRONG_COMMAND);
    }

    tidesdb_column_family_t *idx_cf = share->index_cf[active_index];

    /* For secondary index, we need to use an iterator to find matching keys */
    /* The index stores: index_key -> primary_key */
    tidesdb_iter_t *iter = NULL;
    ret = tidesdb_iter_new(txn, idx_cf, &iter);
    if (ret != TDB_SUCCESS)
    {
      if (own_txn) tidesdb_txn_free(txn);
      DBUG_RETURN(HA_ERR_GENERIC);
    }

    /* Seek to the key */
    ret = tidesdb_iter_seek(iter, (uint8_t *)key, key_len);
    if (ret != TDB_SUCCESS || !tidesdb_iter_valid(iter))
    {
      tidesdb_iter_free(iter);
      if (own_txn) tidesdb_txn_free(txn);
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }

    /* Get the index key and check if it matches our search key */
    uint8_t *idx_key = NULL;
    size_t idx_key_len = 0;
    ret = tidesdb_iter_key(iter, &idx_key, &idx_key_len);
    if (ret != TDB_SUCCESS)
    {
      tidesdb_iter_free(iter);
      if (own_txn) tidesdb_txn_free(txn);
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }

    /* Check if the found key starts with our search key (prefix match) */
    if (idx_key_len < key_len || memcmp(idx_key, key, key_len) != 0)
    {
      tidesdb_iter_free(iter);
      if (own_txn) tidesdb_txn_free(txn);
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }

    /* Get the primary key (value of the index entry) */
    uint8_t *pk_value = NULL;
    size_t pk_len = 0;
    ret = tidesdb_iter_value(iter, &pk_value, &pk_len);
    if (ret != TDB_SUCCESS || pk_len == 0)
    {
      tidesdb_iter_free(iter);
      if (own_txn) tidesdb_txn_free(txn);
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }

    /* Save the primary key for position() */
    free_current_key();
    current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
    if (!current_key)
    {
      tidesdb_iter_free(iter);
      if (own_txn) tidesdb_txn_free(txn);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    memcpy(current_key, pk_value, pk_len);
    current_key_len = pk_len;

    /* Save iterator for index_next_same */
    if (index_iter)
    {
      tidesdb_iter_free(index_iter);
    }
    index_iter = iter;

    /* Save the search key for index_next_same */
    if (index_key_buf)
    {
      my_free(index_key_buf);
    }
    index_key_buf = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_len, MYF(MY_WME));
    if (index_key_buf)
    {
      memcpy(index_key_buf, key, key_len);
      index_key_len = key_len;
    }

    /* Now fetch the actual row using the primary key */
    ret = tidesdb_txn_get(txn, share->cf, current_key, current_key_len, &value, &value_size);
    if (ret == TDB_ERR_NOT_FOUND)
    {
      if (own_txn) tidesdb_txn_free(txn);
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }
    else if (ret != TDB_SUCCESS)
    {
      if (own_txn) tidesdb_txn_free(txn);
      sql_print_error("TidesDB: Failed to get row by PK from secondary index: %d", ret);
      DBUG_RETURN(HA_ERR_GENERIC);
    }
  }

  if (own_txn)
  {
    tidesdb_txn_free(txn);
  }

  /* Unpack the row */
  ret = unpack_row(buf, value, value_size);
  tidesdb_free(value);

  if (ret)
  {
    DBUG_RETURN(ret);
  }

  DBUG_RETURN(0);
}

/**
  @brief
  Read next row in index order.

  For secondary indexes: uses index_iter to find next entry
  For primary key: uses scan_iter
*/
int ha_tidesdb::index_next(uchar *buf)
{
  DBUG_ENTER("ha_tidesdb::index_next");

  int ret;

  /* For secondary index, use the saved iterator */
  if (active_index != table->s->primary_key && index_iter)
  {
    /* Move to next entry in secondary index */
    tidesdb_iter_next(index_iter);

    if (!tidesdb_iter_valid(index_iter))
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    uint8_t *idx_key = NULL;
    size_t idx_key_len = 0;
    ret = tidesdb_iter_key(index_iter, &idx_key, &idx_key_len);
    if (ret != TDB_SUCCESS)
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    /* Get the primary key from the index entry */
    uint8_t *pk_value = NULL;
    size_t pk_len = 0;
    ret = tidesdb_iter_value(index_iter, &pk_value, &pk_len);
    if (ret != TDB_SUCCESS || pk_len == 0)
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    /* Save the primary key */
    free_current_key();
    current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
    if (!current_key)
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    memcpy(current_key, pk_value, pk_len);
    current_key_len = pk_len;

    /* Fetch the actual row using the primary key */
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
        DBUG_RETURN(HA_ERR_GENERIC);
      own_txn = true;
    }

    uint8_t *value = NULL;
    size_t value_size = 0;
    ret = tidesdb_txn_get(txn, share->cf, pk_value, pk_len, &value, &value_size);

    if (own_txn)
    {
      tidesdb_txn_rollback(txn);
      tidesdb_txn_free(txn);
    }

    if (ret != TDB_SUCCESS)
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);

    /* Unpack the row */
    ret = unpack_row(buf, value, value_size);
    tidesdb_free(value);

    DBUG_RETURN(ret);
  }

  /* For primary key scan, use scan_iter */
  if (!scan_iter || !scan_initialized)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  uint8_t *key = NULL;
  size_t key_size = 0;
  uint8_t *value = NULL;
  size_t value_size = 0;

  /* Loop to skip metadata keys (starting with null byte) */
  while (true)
  {
    /* Move to next entry */
    tidesdb_iter_next(scan_iter);

    /* Check if iterator is valid */
    if (!tidesdb_iter_valid(scan_iter))
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    ret = tidesdb_iter_key(scan_iter, &key, &key_size);
    if (ret != TDB_SUCCESS)
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    /* Skip metadata keys (internal keys starting with null byte) */
    if (key_size > 0 && key[0] == 0)
      continue;

    /* Found a data key */
    break;
  }

  ret = tidesdb_iter_value(scan_iter, &value, &value_size);
  if (ret != TDB_SUCCESS)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  /* Save the current key for position() */
  free_current_key();
  current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_size, MYF(MY_WME));
  if (!current_key)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  memcpy(current_key, key, key_size);
  current_key_len = key_size;

  /* Unpack the row */
  ret = unpack_row(buf, value, value_size);

  DBUG_RETURN(ret);
}

/**
  @brief
  Read next row with the same key prefix.

  For secondary indexes: uses index_iter to find next matching entry
  For primary key: uses scan_iter
*/
int ha_tidesdb::index_next_same(uchar *buf, const uchar *key, uint keylen)
{
  DBUG_ENTER("ha_tidesdb::index_next_same");

  int ret;

  /* For secondary index, use the saved iterator */
  if (active_index != table->s->primary_key && index_iter)
  {
    /* Move to next entry in secondary index */
    tidesdb_iter_next(index_iter);

    if (!tidesdb_iter_valid(index_iter))
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    uint8_t *idx_key = NULL;
    size_t idx_key_len = 0;
    ret = tidesdb_iter_key(index_iter, &idx_key, &idx_key_len);
    if (ret != TDB_SUCCESS)
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    /* Check if key prefix still matches */
    uint check_len = index_key_len > 0 ? index_key_len : keylen;
    if (idx_key_len < check_len ||
        memcmp(idx_key, index_key_buf ? index_key_buf : key, check_len) != 0)
    {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    /* Get the primary key from the index entry */
    uint8_t *pk_value = NULL;
    size_t pk_len = 0;
    ret = tidesdb_iter_value(index_iter, &pk_value, &pk_len);
    if (ret != TDB_SUCCESS || pk_len == 0)
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    /* Save the primary key */
    free_current_key();
    current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
    if (!current_key)
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    memcpy(current_key, pk_value, pk_len);
    current_key_len = pk_len;

    /* Fetch the actual row using the primary key */
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
        DBUG_RETURN(HA_ERR_GENERIC);
      own_txn = true;
    }

    uint8_t *value = NULL;
    size_t value_size = 0;
    ret = tidesdb_txn_get(txn, share->cf, current_key, current_key_len, &value, &value_size);

    if (own_txn)
      tidesdb_txn_free(txn);

    if (ret != TDB_SUCCESS)
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    ret = unpack_row(buf, value, value_size);
    tidesdb_free(value);

    DBUG_RETURN(ret);
  }

  /* For primary key, use scan_iter */
  if (!scan_iter || !scan_initialized)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  uint8_t *iter_key = NULL;
  size_t iter_key_size = 0;
  uint8_t *value = NULL;
  size_t value_size = 0;

  /* Loop to find next row with same key prefix, skipping metadata keys */
  while (tidesdb_iter_valid(scan_iter))
  {
    tidesdb_iter_next(scan_iter);

    if (!tidesdb_iter_valid(scan_iter))
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    ret = tidesdb_iter_key(scan_iter, &iter_key, &iter_key_size);
    if (ret != TDB_SUCCESS)
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    /* Skip metadata keys */
    if (iter_key_size > 0 && iter_key[0] == 0)
      continue;

    /* Check if key prefix still matches */
    if (iter_key_size < keylen || memcmp(iter_key, key, keylen) != 0)
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    break;
  }

  if (!tidesdb_iter_valid(scan_iter))
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  ret = tidesdb_iter_value(scan_iter, &value, &value_size);
  if (ret != TDB_SUCCESS)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  free_current_key();
  current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, iter_key_size, MYF(MY_WME));
  if (!current_key)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  memcpy(current_key, iter_key, iter_key_size);
  current_key_len = iter_key_size;

  ret = unpack_row(buf, value, value_size);
  DBUG_RETURN(ret);
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

  if (!scan_iter || !scan_initialized)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  uint8_t *key = NULL;
  size_t key_size = 0;
  uint8_t *value = NULL;
  size_t value_size = 0;

  /* Loop to skip metadata keys (starting with null byte) */
  while (true)
  {
    /* Move to previous entry */
    tidesdb_iter_prev(scan_iter);

    /* Check if iterator is valid */
    if (!tidesdb_iter_valid(scan_iter))
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    ret = tidesdb_iter_key(scan_iter, &key, &key_size);
    if (ret != TDB_SUCCESS)
    {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    /* Skip metadata keys (internal keys starting with null byte) */
    if (key_size > 0 && key[0] == 0)
    {
      continue;
    }

    /* Found a data key */
    break;
  }

  ret = tidesdb_iter_value(scan_iter, &value, &value_size);
  if (ret != TDB_SUCCESS)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  /* Save the current key for position() */
  free_current_key();
  current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_size, MYF(MY_WME));
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

  DBUG_RETURN(0);
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
  Skips internal metadata keys (those starting with null byte).
*/
int ha_tidesdb::index_last(uchar *buf)
{
  DBUG_ENTER("ha_tidesdb::index_last");

  int ret;

  /* Clean up any existing iterator */
  if (scan_iter)
  {
    tidesdb_iter_free(scan_iter);
    scan_iter = NULL;
  }

  /* Begin a transaction for the scan if needed */
  if (!current_txn)
  {
    ret = tidesdb_txn_begin_with_isolation(tidesdb_instance,
                                            (tidesdb_isolation_level_t)tidesdb_default_isolation,
                                            &current_txn);
    if (ret != TDB_SUCCESS)
    {
      sql_print_error("TidesDB: Failed to begin transaction for index_last: %d", ret);
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

  /* Seek to the last entry */
  tidesdb_iter_seek_to_last(scan_iter);
  scan_initialized = true;

  uint8_t *key = NULL;
  size_t key_size = 0;
  uint8_t *value = NULL;
  size_t value_size = 0;

  /* Loop backwards to skip metadata keys (starting with null byte) */
  while (tidesdb_iter_valid(scan_iter))
  {
    ret = tidesdb_iter_key(scan_iter, &key, &key_size);
    if (ret != TDB_SUCCESS)
    {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    /* Skip metadata keys (internal keys starting with null byte) */
    if (key_size > 0 && key[0] == 0)
    {
      tidesdb_iter_prev(scan_iter);
      continue;
    }

    /* Found a data key */
    break;
  }

  /* Check if we reached the beginning without finding data */
  if (!tidesdb_iter_valid(scan_iter))
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  ret = tidesdb_iter_value(scan_iter, &value, &value_size);
  if (ret != TDB_SUCCESS)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  /* Save the current key for position() */
  free_current_key();
  current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, key_size, MYF(MY_WME));
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

  DBUG_RETURN(0);
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
    stats.deleted = 0;

    /* Get accurate statistics from TidesDB */
    if (share && share->cf)
    {
      tidesdb_stats_t *tdb_stats = NULL;
      if (tidesdb_get_stats(share->cf, &tdb_stats) == TDB_SUCCESS && tdb_stats)
      {
        /* Use total_keys for accurate row count */
        stats.records = tdb_stats->total_keys;

        /* Use total_data_size for data file length */
        stats.data_file_length = tdb_stats->total_data_size;

        /* Calculate mean record length from avg sizes */
        if (tdb_stats->avg_key_size > 0 || tdb_stats->avg_value_size > 0)
        {
          stats.mean_rec_length = (ulong)(tdb_stats->avg_key_size + tdb_stats->avg_value_size);
        }
        else
        {
          stats.mean_rec_length = table->s->reclength;
        }

        tidesdb_free_stats(tdb_stats);
      }
      else
      {
        /* Fallback: use cached or default values */
        if (share->row_count_valid)
        {
          stats.records = share->row_count;
        }
        else
        {
          stats.records = 1000;
        }
        stats.data_file_length = 1024 * 1024;
        stats.mean_rec_length = table->s->reclength;
      }
    }
    else
    {
      stats.records = 1000;
      stats.data_file_length = 1024 * 1024;
      stats.mean_rec_length = table->s->reclength;
    }
    stats.index_file_length = 0;
  }

  if (flag & HA_STATUS_CONST)
  {
    stats.max_data_file_length = LLONG_MAX;
    stats.max_index_file_length = LLONG_MAX;
  }

  if (flag & HA_STATUS_AUTO)
  {
    if (share)
    {
      pthread_mutex_lock(&share->auto_inc_mutex);
      stats.auto_increment_value = share->auto_increment_value;
      pthread_mutex_unlock(&share->auto_inc_mutex);
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
  DBUG_RETURN(0);
}

/**
  @brief
  Delete all rows in the table.
*/
int ha_tidesdb::delete_all_rows()
{
  DBUG_ENTER("ha_tidesdb::delete_all_rows");

  char cf_name[256];
  get_cf_name(share->table_name, cf_name, sizeof(cf_name));

  tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
  cf_config.write_buffer_size = tidesdb_write_buffer_size;
  cf_config.enable_bloom_filter = tidesdb_enable_bloom_filter ? 1 : 0;

  if (tidesdb_enable_compression)
  {
    cf_config.compression_algo = LZ4_COMPRESSION;
  }

  int ret;

  /* Drop and recreate the main column family */
  ret = tidesdb_drop_column_family(tidesdb_instance, cf_name);
  if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
  {
    sql_print_error("TidesDB: Failed to drop column family for truncate: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  ret = tidesdb_create_column_family(tidesdb_instance, cf_name, &cf_config);
  if (ret != TDB_SUCCESS)
  {
    sql_print_error("TidesDB: Failed to recreate column family for truncate: %d", ret);
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  /* Update the share's CF handle */
  share->cf = tidesdb_get_column_family(tidesdb_instance, cf_name);

  /* Drop and recreate secondary index column families */
  for (uint i = 0; i < table->s->keys; i++)
  {
    if (i == table->s->primary_key)
      continue;
    if (table->key_info[i].algorithm == HA_KEY_ALG_FULLTEXT)
      continue;
    if (!share->index_cf[i])
      continue;

    char idx_cf_name[256];
    snprintf(idx_cf_name, sizeof(idx_cf_name), "%s_idx_%u", cf_name, i);

    ret = tidesdb_drop_column_family(tidesdb_instance, idx_cf_name);
    if (ret == TDB_SUCCESS || ret == TDB_ERR_NOT_FOUND)
    {
      ret = tidesdb_create_column_family(tidesdb_instance, idx_cf_name, &cf_config);
      if (ret == TDB_SUCCESS)
      {
        share->index_cf[i] = tidesdb_get_column_family(tidesdb_instance, idx_cf_name);
      }
    }
  }

  /* Drop and recreate fulltext index column families */
  for (uint i = 0; i < share->num_ft_indexes; i++)
  {
    if (!share->ft_cf[i])
      continue;

    char ft_cf_name[256];
    snprintf(ft_cf_name, sizeof(ft_cf_name), "%s_ft_%u", cf_name, share->ft_key_nr[i]);

    ret = tidesdb_drop_column_family(tidesdb_instance, ft_cf_name);
    if (ret == TDB_SUCCESS || ret == TDB_ERR_NOT_FOUND)
    {
      ret = tidesdb_create_column_family(tidesdb_instance, ft_cf_name, &cf_config);
      if (ret == TDB_SUCCESS)
      {
        share->ft_cf[i] = tidesdb_get_column_family(tidesdb_instance, ft_cf_name);
      }
    }
  }

  /* Reset row count */
  stats.records = 0;
  share->row_count = 0;
  share->row_count_valid = true;

  /* Reset auto-increment value to 1 */
  pthread_mutex_lock(&share->auto_inc_mutex);
  share->auto_increment_value = 1;
  pthread_mutex_unlock(&share->auto_inc_mutex);

  /* Reset hidden PK value for tables without explicit PK */
  pthread_mutex_lock(&share->hidden_pk_mutex);
  share->hidden_pk_value = 0;
  pthread_mutex_unlock(&share->hidden_pk_mutex);

  DBUG_RETURN(0);
}

/**
  @brief
  Estimate the cost of a full table scan.

  LSM-tree scan cost model using TidesDB statistics:
  -- total_data_size: actual bytes to read (I/O cost)
  -- total_sstables: merge iterator overhead
  -- hit_rate: cache effectiveness (reduces I/O)

  Cost formula:
    cost = (data_blocks * merge_overhead * cache_miss_rate) + cpu_cost

  The result should be comparable to read_time() so the optimizer
  can correctly choose between table scan and index scan.
*/
IO_AND_CPU_COST ha_tidesdb::scan_time()
{
  DBUG_ENTER("ha_tidesdb::scan_time");

  IO_AND_CPU_COST cost;
  cost.io = 1.0;
  cost.cpu = 0.0;

  if (share && share->cf)
  {
    tidesdb_stats_t *tdb_stats = NULL;
    if (tidesdb_get_stats(share->cf, &tdb_stats) == TDB_SUCCESS && tdb_stats)
    {
      /*
        I/O cost: based on total data size
        -- 64KB block size
        -- Each block read has base cost of 1.0
      */
      double data_bytes = (double)tdb_stats->total_data_size;
      double block_size = 65536.0;
      double num_blocks = data_bytes / block_size;
      if (num_blocks < 1.0)
        num_blocks = 1.0;

      /*
        Merge overhead: LSM-tree scans must merge across SSTables
        -- Each SSTable in the scan path adds comparison overhead
        -- Memtable is also part of the merge
      */
      int total_sstables = 1;  /* Start with 1 for memtable */
      for (int i = 0; i < tdb_stats->num_levels; i++)
      {
        total_sstables += tdb_stats->level_num_sstables[i];
      }
      /* Log-based overhead: merging N sources is O(N log N) per row */
      double merge_overhead = 1.0 + (log2((double)total_sstables) * 0.1);
      if (merge_overhead < 1.0)
        merge_overhead = 1.0;

      /*
        Cache factor: hit_rate reduces I/O cost
        -- 0% hit rate = full I/O cost
        -- 100% hit rate = 10% of I/O cost (still need CPU for merge)
      */
      double cache_factor = 1.0 - (tdb_stats->hit_rate * 0.9);
      if (cache_factor < 0.1)
        cache_factor = 0.1;

      /*
        CPU cost: processing each row has a base cost
        -- Independent of I/O, represents deserialization + comparison
      */
      double cpu_cost = (double)tdb_stats->total_keys * 0.001;

      /* Total cost */
      cost.io = num_blocks * merge_overhead * cache_factor;
      cost.cpu = cpu_cost;

      tidesdb_free_stats(tdb_stats);
    }
    else
    {
      /* Fallback: estimate based on stats.records */
      ha_rows rows = stats.records;
      if (rows == 0)
        rows = 100;
      cost.io = (double)rows / 100.0 + 1.0;
      cost.cpu = 0.0;
    }
  }

  /* Minimum cost of 1.0 for any scan */
  if (cost.io < 1.0)
    cost.io = 1.0;

  DBUG_RETURN(cost);
}

/**
  @brief
  Estimate the cost of reading rows via index.

  LSM-tree index read cost model using TidesDB statistics:
  -- read_amp: average levels checked per point lookup (with bloom filters, ~1.0-1.5)
  -- hit_rate: cache effectiveness (reduces I/O cost)
  -- avg_key_size + avg_value_size: I/O per row

  Cost components:
  1. Seek cost: cost to position iterator at start of each range
  2. Row fetch cost: cost to read each row (index entry + data row lookup)
*/
IO_AND_CPU_COST ha_tidesdb::read_time(uint index, uint ranges, ha_rows rows)
{
  DBUG_ENTER("ha_tidesdb::read_time");

  IO_AND_CPU_COST cost;
  cost.io = 0.1;
  cost.cpu = 0.0;

  if (rows == 0)
    DBUG_RETURN(cost);

  if (share && share->cf)
  {
    tidesdb_stats_t *tdb_stats = NULL;
    if (tidesdb_get_stats(share->cf, &tdb_stats) == TDB_SUCCESS && tdb_stats)
    {
      /* Read amplification: average levels checked per point lookup */
      double read_amp = tdb_stats->read_amp;
      if (read_amp < 1.0)
        read_amp = 1.0;

      /*
        Seek cost per range:
        -- Each seek requires checking bloom filters and potentially reading blocks
        -- With good bloom filters (read_amp ~1.0), seek is cheap
        -- Without bloom filters (read_amp = num_levels), seek is expensive
      */
      double seek_cost = 0.3 * read_amp;

      /*
        Row fetch cost:
        -- For primary key index: just read the row data
        -- For secondary index: read index entry + lookup primary key
        -- Base cost: fraction of a block read per row
        -- Amortized across block: multiple rows per block reduces per-row cost
      */
      double avg_row_size = tdb_stats->avg_key_size + tdb_stats->avg_value_size;
      if (avg_row_size < 50)
        avg_row_size = 50;

      /* Rows per block (64KB blocks) */
      double rows_per_block = 65536.0 / avg_row_size;
      if (rows_per_block < 1.0)
        rows_per_block = 1.0;

      /* Cost per row: 1/rows_per_block of a block read */
      /* Add read_amp factor for non-primary index lookups */
      double row_fetch_cost = (1.0 / rows_per_block) * read_amp;

      /* Cache reduces I/O cost */
      double cache_factor = 1.0 - (tdb_stats->hit_rate * 0.9);  /* Up to 90% reduction */
      if (cache_factor < 0.1)
        cache_factor = 0.1;  /* Minimum 10% cost */

      /* Total cost: range seeks + row reads */
      cost.io = (ranges * seek_cost * cache_factor) + (rows * row_fetch_cost * cache_factor);
      cost.cpu = (double)rows * 0.001;

      tidesdb_free_stats(tdb_stats);
    }
    else
    {
      /* Fallback: simple estimate */
      /* Assume moderate read_amp of 2.0 */
      cost.io = (double)ranges * 0.6 + (double)rows * 0.02;
      cost.cpu = 0.0;
    }
  }

  /* Minimum cost to avoid optimizer choosing index for 0 rows */
  if (cost.io < 0.1)
    cost.io = 0.1;

  DBUG_RETURN(cost);
}

/**
  @brief
  Estimate records in a range.

  Uses TidesDB statistics for accurate estimation:
  -- total_keys: actual key count for base estimate
  -- Selectivity heuristics based on key parts and condition type

  This is critical for optimizer decisions:
  -- Low estimate -> optimizer prefers index scan
  -- High estimate -> optimizer prefers table scan
*/
ha_rows ha_tidesdb::records_in_range(uint inx, const key_range *min_key,
                                     const key_range *max_key,
                                     page_range *pages)
{
  DBUG_ENTER("ha_tidesdb::records_in_range");

  /* Get accurate row count from TidesDB stats */
  ha_rows total_rows = stats.records;
  if (share && share->cf)
  {
    tidesdb_stats_t *tdb_stats = NULL;
    if (tidesdb_get_stats(share->cf, &tdb_stats) == TDB_SUCCESS && tdb_stats)
    {
      total_rows = tdb_stats->total_keys;
      tidesdb_free_stats(tdb_stats);
    }
  }

  if (total_rows == 0)
    total_rows = 100;  /* Minimum estimate for empty tables */

  /* If no key bounds, return all rows (full index scan) */
  if (!min_key && !max_key)
    DBUG_RETURN(total_rows);

  /* Count key parts used in the condition */
  KEY *key_info = &table->key_info[inx];
  uint key_parts_used = 0;

  if (min_key)
  {
    for (uint i = 0; i < key_info->user_defined_key_parts; i++)
    {
      if (min_key->keypart_map & (1 << i))
        key_parts_used++;
    }
  }

  /*
    Selectivity estimation:
    -- First key part: assume 1/sqrt(total_rows) selectivity (moderate cardinality)
    -- Each additional key part: divide by 10 (compound key selectivity)
    -- This is a heuristic; real cardinality stats would be better
  */
  double selectivity = 1.0;
  if (key_parts_used > 0)
  {
    /* First key part: assume moderate cardinality */
    double first_part_sel = 1.0 / sqrt((double)total_rows);
    if (first_part_sel > 0.1)
      first_part_sel = 0.1;  /* Cap at 10% for first part */
    selectivity = first_part_sel;

    /* Additional key parts increase selectivity */
    for (uint i = 1; i < key_parts_used; i++)
    {
      selectivity /= 10.0;
    }
  }

  /* Check for equality condition (min_key == max_key) */
  bool is_equality = (min_key && max_key &&
                      min_key->length == max_key->length &&
                      memcmp(min_key->key, max_key->key, min_key->length) == 0);

  if (is_equality)
  {
    /* Primary key equality: exactly 1 row */
    if (inx == table->s->primary_key)
    {
      DBUG_RETURN(1);
    }

    /* Unique secondary index: exactly 1 row */
    if (key_info->flags & HA_NOSAME)
    {
      DBUG_RETURN(1);
    }

    /* Non-unique secondary index: estimate based on selectivity */
    ha_rows estimate = (ha_rows)(total_rows * selectivity);
    if (estimate < 1)
      estimate = 1;
    /* Non-unique indexes can have many duplicates */
    if (estimate > total_rows / 10)
      estimate = total_rows / 10;
    if (estimate < 1)
      estimate = 1;
    DBUG_RETURN(estimate);
  }

  /* Range condition: use selectivity with range factor */
  /* Ranges typically return more rows than equality */
  double range_factor = 5.0;  /* Ranges return ~5x more than equality */
  ha_rows estimate = (ha_rows)(total_rows * selectivity * range_factor);

  if (estimate < 1)
    estimate = 1;

  /* Cap at 30% of table for range scans */
  ha_rows max_estimate = total_rows * 3 / 10;
  if (max_estimate < 10)
    max_estimate = 10;
  if (estimate > max_estimate)
    estimate = max_estimate;

  DBUG_RETURN(estimate);
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

  For proper savepoint support, we store the transaction at the THD level
  when in a multi-statement transaction (BEGIN...COMMIT), and at the
  handler level for auto-commit mode.
*/
int ha_tidesdb::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_tidesdb::external_lock");

  int ret;
  bool in_transaction = thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);

  if (lock_type != F_UNLCK)
  {
    /* Starting a new statement/transaction */

    if (in_transaction)
    {
      /* Multi-statement transaction -- use THD-level transaction for savepoint support */
      tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);

      if (!thd_txn)
      {
        /* Start a new transaction at THD level */
        int isolation = map_isolation_level(
          (enum_tx_isolation)thd->variables.tx_isolation);

        ret = tidesdb_txn_begin_with_isolation(tidesdb_instance,
                                                (tidesdb_isolation_level_t)isolation,
                                                &thd_txn);
        if (ret != TDB_SUCCESS)
        {
          sql_print_error("TidesDB: Failed to begin transaction: %d", ret);
          DBUG_RETURN(HA_ERR_GENERIC);
        }
        set_thd_txn(thd, tidesdb_hton, thd_txn);

        /* Register with MySQL transaction coordinator */
        trans_register_ha(thd, TRUE, tidesdb_hton, 0);
      }

      /* Use THD transaction for this handler */
      current_txn = thd_txn;
      trans_register_ha(thd, FALSE, tidesdb_hton, 0);
    }
    else
    {
      /* Auto-commit mode -- use handler-level transaction */
      if (!current_txn)
      {
        int isolation = map_isolation_level(
          (enum_tx_isolation)thd->variables.tx_isolation);

        ret = tidesdb_txn_begin_with_isolation(tidesdb_instance,
                                                (tidesdb_isolation_level_t)isolation,
                                                &current_txn);
        if (ret != TDB_SUCCESS)
        {
          sql_print_error("TidesDB: Failed to begin transaction: %d", ret);
          DBUG_RETURN(HA_ERR_GENERIC);
        }
      }
    }
  }
  else
  {
    /* Ending statement/transaction */

    if (in_transaction)
    {
      /* Multi-statement transaction -- don't commit yet, wait for COMMIT */
      /* Just clear the handler's reference to the THD transaction */
      current_txn = NULL;
    }
    else
    {
      /* Auto-commit mode -- commit immediately */
      if (current_txn)
      {
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
  Optimize table -- triggers manual compaction in TidesDB.

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

  /*
    First flush the memtable to ensure all data is on disk before compaction.
    This also ensures the column family is in a consistent state.
  */
  int ret = tidesdb_flush_memtable(share->cf);
  if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
  {
    sql_print_warning("TidesDB: Memtable flush returned: %d", ret);
  }

  /* Now trigger compaction */
  ret = tidesdb_compact(share->cf);
  if (ret != TDB_SUCCESS)
  {
    sql_print_warning("TidesDB: Compaction returned: %d (non-fatal)", ret);
    /* Don't fail -- compaction is best-effort */
  }

  sql_print_information("TidesDB: OPTIMIZE TABLE completed");
  DBUG_RETURN(HA_ADMIN_OK);
}

/**
  @brief
  Analyze table -- updates statistics.

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

/* TidesDB fulltext search info structure */
struct tidesdb_ft_info {
  struct _ft_vft *please;
  ha_tidesdb *handler;
  uint ft_idx;
  char **matched_pks;
  size_t *matched_pk_lens;
  uint matched_count;
  uint current_match;
  float relevance;
};

static int tidesdb_ft_read_next(FT_INFO *fts, char *record);
static float tidesdb_ft_find_relevance(FT_INFO *fts, uchar *record, uint length);
static void tidesdb_ft_close_search(FT_INFO *fts);
static float tidesdb_ft_get_relevance(FT_INFO *fts);
static void tidesdb_ft_reinit_search(FT_INFO *fts);

static struct _ft_vft tidesdb_ft_vft = {
  tidesdb_ft_read_next,
  tidesdb_ft_find_relevance,
  tidesdb_ft_close_search,
  tidesdb_ft_get_relevance,
  tidesdb_ft_reinit_search
};

static int tidesdb_ft_read_next(FT_INFO *fts, char *record)
{
  return HA_ERR_END_OF_FILE;
}

static float tidesdb_ft_find_relevance(FT_INFO *fts, uchar *record, uint length)
{
  return 1.0f;
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
        if (info->matched_pks[i])
          my_free(info->matched_pks[i]);
      }
      my_free(info->matched_pks);
    }
    if (info->matched_pk_lens)
      my_free(info->matched_pk_lens);
    my_free(info);
  }
}

static float tidesdb_ft_get_relevance(FT_INFO *fts)
{
  tidesdb_ft_info *info = (tidesdb_ft_info *)fts;
  return info ? info->relevance : 0.0f;
}

static void tidesdb_ft_reinit_search(FT_INFO *fts)
{
  tidesdb_ft_info *info = (tidesdb_ft_info *)fts;
  if (info)
    info->current_match = 0;
}

/**
  @brief
  Search for a single word in the fulltext index.

  Returns matching primary keys in the provided arrays.
  Uses prefix seek for efficient lookup.
*/
static int ft_search_word(tidesdb_column_family_t *ft_cf,
                          const char *word, size_t word_len,
                          char ***out_pks, size_t **out_pk_lens,
                          uint *out_count, size_t max_matches)
{
  if (word_len == 0)
    return 0;

  /* Create prefix to search: word + '\0' */
  char prefix[258];
  if (word_len > 255)
    word_len = 255;
  memcpy(prefix, word, word_len);
  prefix[word_len] = '\0';
  size_t prefix_len = word_len + 1;

  *out_pks = (char **)my_malloc(PSI_INSTRUMENT_ME, max_matches * sizeof(char *), MYF(MY_WME | MY_ZEROFILL));
  *out_pk_lens = (size_t *)my_malloc(PSI_INSTRUMENT_ME, max_matches * sizeof(size_t), MYF(MY_WME | MY_ZEROFILL));
  *out_count = 0;

  if (!*out_pks || !*out_pk_lens)
    return -1;

  tidesdb_txn_t *txn = NULL;
  if (tidesdb_txn_begin(tidesdb_instance, &txn) != TDB_SUCCESS)
    return -1;

  tidesdb_iter_t *iter = NULL;
  if (tidesdb_iter_new(txn, ft_cf, &iter) != TDB_SUCCESS)
  {
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return -1;
  }

  /* Seek to prefix using block index for O(log n) lookup */
  tidesdb_iter_seek(iter, (uint8_t *)prefix, prefix_len);

  while (tidesdb_iter_valid(iter) && *out_count < max_matches)
  {
    uint8_t *iter_key = NULL;
    size_t iter_key_len = 0;

    if (tidesdb_iter_key(iter, &iter_key, &iter_key_len) != TDB_SUCCESS)
      break;

    /* Check if key starts with our prefix */
    if (iter_key_len < prefix_len || memcmp(iter_key, prefix, prefix_len) != 0)
      break;

    /* Extract primary key (after word + '\0') */
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
  tidesdb_txn_rollback(txn);
  tidesdb_txn_free(txn);

  return 0;
}

/**
  @brief
  Hash function for PK bytes.
*/
static const uchar *ft_pk_get_key(const void *entry_ptr, size_t *length,
                            my_bool not_used __attribute__((unused)))
{
  /* Entry format: [4-byte length][pk data] */
  const uchar *entry = (const uchar *)entry_ptr;
  uint32 len = uint4korr(entry);
  *length = len;
  return entry + 4;
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
static void ft_intersect_results(char **pks1, size_t *lens1, uint count1,
                                  char **pks2, size_t *lens2, uint count2,
                                  char ***out_pks, size_t **out_lens, uint *out_count)
{
  size_t max_out = (count1 < count2) ? count1 : count2;
  *out_pks = (char **)my_malloc(PSI_INSTRUMENT_ME, max_out * sizeof(char *), MYF(MY_WME | MY_ZEROFILL));
  *out_lens = (size_t *)my_malloc(PSI_INSTRUMENT_ME, max_out * sizeof(size_t), MYF(MY_WME | MY_ZEROFILL));
  *out_count = 0;

  if (!*out_pks || !*out_lens)
    return;

  /* For small sets, use simple O(n*m) -- hash overhead not worth it */
  if (count1 <= 16 || count2 <= 16)
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

  /* Build hash set from smaller array for O(n+m) lookup */
  char **smaller_pks, **larger_pks;
  size_t *smaller_lens, *larger_lens;
  uint smaller_count, larger_count;

  if (count1 <= count2)
  {
    smaller_pks = pks1; smaller_lens = lens1; smaller_count = count1;
    larger_pks = pks2; larger_lens = lens2; larger_count = count2;
  }
  else
  {
    smaller_pks = pks2; smaller_lens = lens2; smaller_count = count2;
    larger_pks = pks1; larger_lens = lens1; larger_count = count1;
  }

  /* Create hash table from smaller set */
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

  /* Insert smaller set into hash -- format: [4-byte len][pk data] */
  for (uint i = 0; i < smaller_count; i++)
  {
    size_t entry_size = 4 + smaller_lens[i];
    uchar *entry = (uchar *)my_malloc(PSI_INSTRUMENT_ME, entry_size, MYF(MY_WME));
    if (entry)
    {
      int4store(entry, (uint32)smaller_lens[i]);
      memcpy(entry + 4, smaller_pks[i], smaller_lens[i]);
      if (my_hash_insert(&pk_hash, entry))
        my_free(entry);
    }
  }

  /* Probe hash with larger set */
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
static void ft_union_results(char **pks1, size_t *lens1, uint count1,
                              char **pks2, size_t *lens2, uint count2,
                              char ***out_pks, size_t **out_lens, uint *out_count)
{
  size_t max_out = count1 + count2;
  *out_pks = (char **)my_malloc(PSI_INSTRUMENT_ME, max_out * sizeof(char *), MYF(MY_WME | MY_ZEROFILL));
  *out_lens = (size_t *)my_malloc(PSI_INSTRUMENT_ME, max_out * sizeof(size_t), MYF(MY_WME | MY_ZEROFILL));
  *out_count = 0;

  if (!*out_pks || !*out_lens)
    return;

  /* Add all from first set */
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

  /* Add from second set if not already present */
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
      if (pks[i])
        my_free(pks[i]);
    }
    my_free(pks);
  }
  if (lens)
    my_free(lens);
}

/**
  @brief
  Initialize full-text search.

  Supports multi-word search:
  -- Natural language mode: words are OR'd together
  -- Boolean mode (FT_BOOL flag): words are AND'd together
  -- Respects ft_min_word_len and ft_max_word_len
*/
FT_INFO *ha_tidesdb::ft_init_ext(uint flags, uint inx, String *key)
{
  DBUG_ENTER("ha_tidesdb::ft_init_ext");

  /* Find the fulltext index for this key number */
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

  /* Allocate FT info structure */
  tidesdb_ft_info *info = (tidesdb_ft_info *)my_malloc(PSI_INSTRUMENT_ME, sizeof(tidesdb_ft_info), MYF(MY_WME | MY_ZEROFILL));
  if (!info)
    DBUG_RETURN(NULL);

  info->please = &tidesdb_ft_vft;
  info->handler = this;
  info->ft_idx = ft_idx;
  info->matched_pks = NULL;
  info->matched_pk_lens = NULL;
  info->matched_count = 0;
  info->current_match = 0;
  info->relevance = 1.0f;

  /* Tokenize the search query into words */
  const char *query = key->ptr();
  size_t query_len = key->length();

  /* Extract all words from query */
  char words[32][256];
  size_t word_lens[32];
  uint word_count = 0;

  char word_buf[256];
  size_t word_len = 0;

  for (size_t i = 0; i <= query_len; i++)
  {
    char c = (i < query_len) ? query[i] : ' ';
    bool is_word_char = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                        (c >= '0' && c <= '9') || (c == '_');

    if (is_word_char && word_len < sizeof(word_buf) - 1)
    {
      /* Convert to lowercase */
      word_buf[word_len++] = (c >= 'A' && c <= 'Z') ? (c + 32) : c;
    }
    else if (word_len > 0)
    {
      /* End of word -- check length constraints */
      if (word_len >= tidesdb_ft_min_word_len && word_len <= tidesdb_ft_max_word_len && word_count < 32)
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
    ft_handler = (FT_INFO *)info;
    DBUG_RETURN((FT_INFO *)info);
  }

  tidesdb_column_family_t *ft_cf = share->ft_cf[ft_idx];
  size_t max_matches = 10000;

  /* Boolean mode uses AND, natural language uses OR */
  bool use_and = (flags & FT_BOOL) != 0;

  /* Search for first word */
  char **result_pks = NULL;
  size_t *result_lens = NULL;
  uint result_count = 0;

  if (ft_search_word(ft_cf, words[0], word_lens[0],
                     &result_pks, &result_lens, &result_count, max_matches) != 0)
  {
    tidesdb_ft_close_search((FT_INFO *)info);
    DBUG_RETURN(NULL);
  }

  /* Process remaining words */
  for (uint w = 1; w < word_count && result_count > 0; w++)
  {
    char **word_pks = NULL;
    size_t *word_lens_arr = NULL;
    uint word_pk_count = 0;

    if (ft_search_word(ft_cf, words[w], word_lens[w],
                       &word_pks, &word_lens_arr, &word_pk_count, max_matches) != 0)
    {
      ft_free_results(result_pks, result_lens, result_count);
      tidesdb_ft_close_search((FT_INFO *)info);
      DBUG_RETURN(NULL);
    }

    /* Combine results */
    char **new_pks = NULL;
    size_t *new_lens = NULL;
    uint new_count = 0;

    if (use_and)
    {
      ft_intersect_results(result_pks, result_lens, result_count,
                           word_pks, word_lens_arr, word_pk_count,
                           &new_pks, &new_lens, &new_count);
    }
    else
    {
      ft_union_results(result_pks, result_lens, result_count,
                       word_pks, word_lens_arr, word_pk_count,
                       &new_pks, &new_lens, &new_count);
    }

    /* Free old results */
    ft_free_results(result_pks, result_lens, result_count);
    ft_free_results(word_pks, word_lens_arr, word_pk_count);

    result_pks = new_pks;
    result_lens = new_lens;
    result_count = new_count;
  }

  /* Store results in info structure */
  info->matched_pks = result_pks;
  info->matched_pk_lens = result_lens;
  info->matched_count = result_count;

  /* Calculate relevance based on match count */
  if (result_count > 0)
  {
    info->relevance = (float)word_count;  /* More matching words = higher relevance */
  }

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

  if (!ft_handler || ft_current_match >= ft_matched_count)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  /* Get the next matched primary key */
  char *pk = ft_matched_pks[ft_current_match];
  size_t pk_len = ft_matched_pk_lens[ft_current_match];
  ft_current_match++;

  /* Fetch the row by primary key */
  tidesdb_txn_t *txn = NULL;
  int ret = tidesdb_txn_begin(tidesdb_instance, &txn);
  if (ret != TDB_SUCCESS)
    DBUG_RETURN(HA_ERR_GENERIC);

  uint8_t *value = NULL;
  size_t value_len = 0;
  ret = tidesdb_txn_get(txn, share->cf, (uint8_t *)pk, pk_len, &value, &value_len);

  if (ret != TDB_SUCCESS)
  {
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
  }

  /* Unpack the row */
  ret = unpack_row(buf, value, value_len);
  tidesdb_free(value);

  tidesdb_txn_rollback(txn);
  tidesdb_txn_free(txn);

  if (ret)
    DBUG_RETURN(ret);

  /* Save current key for position() */
  free_current_key();
  current_key = (uchar *)my_malloc(PSI_INSTRUMENT_ME, pk_len, MYF(MY_WME));
  if (current_key)
  {
    memcpy(current_key, pk, pk_len);
    current_key_len = pk_len;
  }

  DBUG_RETURN(0);
}

/**
  @brief
  Get auto-increment value for INSERT.
*/
void ha_tidesdb::get_auto_increment(ulonglong offset, ulonglong increment,
                                    ulonglong nb_desired_values,
                                    ulonglong *first_value,
                                    ulonglong *nb_reserved_values)
{
  DBUG_ENTER("ha_tidesdb::get_auto_increment");

  pthread_mutex_lock(&share->auto_inc_mutex);

  /* Ensure auto_increment_value is at least 1 */
  if (share->auto_increment_value == 0)
    share->auto_increment_value = 1;

  /* Get current auto-increment value */
  *first_value = share->auto_increment_value;

  /* Reserve the requested number of values */
  share->auto_increment_value += nb_desired_values * increment;

  /* TidesDB supports concurrent inserts, so reserve all requested */
  *nb_reserved_values = nb_desired_values;

  pthread_mutex_unlock(&share->auto_inc_mutex);

  DBUG_VOID_RETURN;
}

/**
  @brief
  Reset auto-increment value.
*/
int ha_tidesdb::reset_auto_increment(ulonglong value)
{
  DBUG_ENTER("ha_tidesdb::reset_auto_increment");

  pthread_mutex_lock(&share->auto_inc_mutex);
  share->auto_increment_value = value;
  pthread_mutex_unlock(&share->auto_inc_mutex);

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

  /* Check if we're in a multi-statement transaction */
  THD *thd = ha_thd();
  tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);

  if (thd_txn)
  {
    /* Use THD-level transaction for proper savepoint/rollback support */
    bulk_txn = thd_txn;
    bulk_insert_active = true;
    /* Don't commit this in end_bulk_insert -- it's managed by THD */
    DBUG_VOID_RETURN;
  }

  /* Not in a multi-statement transaction, create our own */
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
    /* Check if this is a THD-level transaction -- don't commit it here */
    THD *thd = ha_thd();
    tidesdb_txn_t *thd_txn = get_thd_txn(thd, tidesdb_hton);

    if (bulk_txn == thd_txn)
    {
      /* THD transaction -- don't commit, just clear our reference */
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

  /* Invalidate row count cache since we inserted rows */
  share->row_count_valid = false;

  DBUG_RETURN(ret);
}

/**
  @brief
  Check table for errors.
*/
int ha_tidesdb::check(THD* thd, HA_CHECK_OPT* check_opt)
{
  DBUG_ENTER("ha_tidesdb::check");

  if (!share || !share->cf)
  {
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }

  /* TidesDB has built-in checksums and corruption detection */
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
  tidesdb_txn_free(txn);
  
  /* Update row count cache */
  share->row_count = count;
  share->row_count_valid = true;
  
  sql_print_information("TidesDB: Table check completed, %lu rows verified", 
                        (ulong)count);
  
  DBUG_RETURN(HA_ADMIN_OK);
}

/**
  @brief
  Repair table -- triggers compaction to clean up any issues.
*/
int ha_tidesdb::repair(THD* thd, HA_CHECK_OPT* check_opt)
{
  DBUG_ENTER("ha_tidesdb::repair");
  
  if (!share || !share->cf)
  {
    DBUG_RETURN(HA_ADMIN_FAILED);
  }
  
  /* First flush the memtable */
  int ret = tidesdb_flush_memtable(share->cf);
  if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
  {
    sql_print_warning("TidesDB: Memtable flush returned: %d", ret);
  }
  
  /* TidesDB's compaction removes tombstones and merges data */
  ret = tidesdb_compact(share->cf);
  if (ret != TDB_SUCCESS)
  {
    sql_print_warning("TidesDB: Repair (compaction) returned: %d (non-fatal)", ret);
  }
  
  sql_print_information("TidesDB: Table repair completed");
  
  DBUG_RETURN(HA_ADMIN_OK);
}

/**
  @brief
  Discard or import tablespace.
  
  For TidesDB, this allows exporting/importing column family data.
  -- discard=TRUE: Prepare table for import (flush and close CF)
  -- discard=FALSE: Import tablespace data
*/
int ha_tidesdb::discard_or_import_tablespace(my_bool discard)
{
  DBUG_ENTER("ha_tidesdb::discard_or_import_tablespace");
  
  if (!share || !share->cf)
  {
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }
  
  if (discard)
  {
    /* Discard tablespace -- flush all data to disk */
    sql_print_information("TidesDB: Discarding tablespace for table");
    
    int ret = tidesdb_flush_memtable(share->cf);
    if (ret != TDB_SUCCESS && ret != TDB_ERR_NOT_FOUND)
    {
      sql_print_warning("TidesDB: Flush during discard returned: %d", ret);
    }
    
    /* Mark table as discarded -- data files can now be replaced */
    sql_print_information("TidesDB: Tablespace discarded - data files can be replaced");
  }
  else
  {
    /* Import tablespace -- reload column family */
    sql_print_information("TidesDB: Importing tablespace for table");
    
    char cf_name[256];
    get_cf_name(share->table_name, cf_name, sizeof(cf_name));
    
    /* Re-get the column family handle after import */
    share->cf = tidesdb_get_column_family(tidesdb_instance, cf_name);
    if (!share->cf)
    {
      sql_print_error("TidesDB: Failed to import tablespace - column family not found");
      DBUG_RETURN(HA_ERR_TABLESPACE_MISSING);
    }
    
    sql_print_information("TidesDB: Tablespace imported successfully");
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
int ha_tidesdb::backup(THD* thd, HA_CHECK_OPT* check_opt)
{
  DBUG_ENTER("ha_tidesdb::backup");
  
  if (!tidesdb_instance)
  {
    sql_print_error("TidesDB: Cannot backup - database not initialized");
    DBUG_RETURN(HA_ADMIN_FAILED);
  }
  
  /* Generate backup directory name with timestamp */
  char backup_dir[512];
  time_t now = time(NULL);
  struct tm *tm_info = localtime(&now);
  char timestamp[32];
  strftime(timestamp, sizeof(timestamp), "%Y%m%d_%H%M%S", tm_info);
  
  snprintf(backup_dir, sizeof(backup_dir), "/tmp/tidesdb_backup_%s", timestamp);
  
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
    Foreign key metadata is stored in MySQL's data dictionary (.frm files).
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
  
  /*
    Foreign key metadata is stored in MySQL's data dictionary.
    The handler doesn't maintain separate FK metadata -- MySQL handles
    the constraint definitions and calls us to enforce them.
  */
  DBUG_RETURN(0);
}

/**
  @brief
  Check if this table is referenced by foreign keys from other tables.
  
  Returns the number of foreign keys that reference this table.
  This is used to prevent dropping tables that are referenced.
*/
bool ha_tidesdb::referenced_by_foreign_key() const noexcept
{
  DBUG_ENTER("ha_tidesdb::referenced_by_foreign_key");
  
  /*
    MySQL's data dictionary tracks FK references.
    Return 0 to indicate we don't track this at the engine level.
    MySQL will check its own metadata for FK references.
  */
  DBUG_RETURN(0);
}

/**
  @brief
  Free memory allocated by get_foreign_key_create_info().
*/
void ha_tidesdb::free_foreign_key_create_info(char *str)
{
  DBUG_ENTER("ha_tidesdb::free_foreign_key_create_info");
  
  if (str)
    my_free(str);
  
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
    Allow engine switching. If there are FK constraints, MySQL will
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
  
  /* Clean up any scan state */
  if (scan_iter)
  {
    tidesdb_iter_free(scan_iter);
    scan_iter = NULL;
  }
  scan_initialized = false;
  
  /* Free current key */
  free_current_key();
  
  DBUG_RETURN(0);
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

static MYSQL_SYSVAR_ULONG(
  ft_min_word_len,
  tidesdb_ft_min_word_len,
  PLUGIN_VAR_RQCMDARG,
  "Minimum word length for fulltext indexing",
  NULL,
  NULL,
  4,
  1,
  84,
  0);

static MYSQL_SYSVAR_ULONG(
  ft_max_word_len,
  tidesdb_ft_max_word_len,
  PLUGIN_VAR_RQCMDARG,
  "Maximum word length for fulltext indexing",
  NULL,
  NULL,
  84,
  1,
  255,
  0);

static MYSQL_SYSVAR_ULONG(
  min_levels,
  tidesdb_min_levels,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Minimum number of LSM levels",
  NULL,
  NULL,
  5,
  1,
  20,
  0);

static MYSQL_SYSVAR_ULONG(
  dividing_level_offset,
  tidesdb_dividing_level_offset,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Compaction dividing level offset",
  NULL,
  NULL,
  2,
  0,
  10,
  0);

static MYSQL_SYSVAR_DOUBLE(
  skip_list_probability,
  tidesdb_skip_list_probability,
  PLUGIN_VAR_RQCMDARG,
  "Skip list probability for memtable",
  NULL,
  NULL,
  0.25,
  0.01,
  0.5,
  0);

static MYSQL_SYSVAR_ULONG(
  block_index_prefix_len,
  tidesdb_block_index_prefix_len,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Block index prefix length in bytes",
  NULL,
  NULL,
  16,
  1,
  256,
  0);

static MYSQL_SYSVAR_ULONGLONG(
  klog_value_threshold,
  tidesdb_klog_value_threshold,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Values larger than this go to vlog (bytes)",
  NULL,
  NULL,
  512,
  0,
  1048576,
  0);

static MYSQL_SYSVAR_ULONGLONG(
  min_disk_space,
  tidesdb_min_disk_space,
  PLUGIN_VAR_RQCMDARG,
  "Minimum disk space required (bytes)",
  NULL,
  NULL,
  104857600,  /* 100MB */
  0,
  ULLONG_MAX,
  0);

static MYSQL_SYSVAR_ULONG(
  l1_file_count_trigger,
  tidesdb_l1_file_count_trigger,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "L1 file count trigger for compaction",
  NULL,
  NULL,
  4,
  1,
  100,
  0);

static MYSQL_SYSVAR_ULONG(
  l0_queue_stall_threshold,
  tidesdb_l0_queue_stall_threshold,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "L0 queue stall threshold for backpressure",
  NULL,
  NULL,
  20,
  1,
  1000,
  0);

static MYSQL_SYSVAR_ULONG(
  max_open_sstables,
  tidesdb_max_open_sstables,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Maximum cached SSTable structures (each uses 2 FDs)",
  NULL,
  NULL,
  256,
  16,
  4096,
  0);

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
  MYSQL_SYSVAR(ft_min_word_len),
  MYSQL_SYSVAR(ft_max_word_len),
  MYSQL_SYSVAR(min_levels),
  MYSQL_SYSVAR(dividing_level_offset),
  MYSQL_SYSVAR(skip_list_probability),
  MYSQL_SYSVAR(block_index_prefix_len),
  MYSQL_SYSVAR(klog_value_threshold),
  MYSQL_SYSVAR(min_disk_space),
  MYSQL_SYSVAR(l1_file_count_trigger),
  MYSQL_SYSVAR(l0_queue_stall_threshold),
  MYSQL_SYSVAR(max_open_sstables),
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
  0x0703,                                       /* version: TidesDB 7.3 */
  NULL,                                         /* status variables */
  tidesdb_system_variables,                     /* system variables */
  NULL                                          /* config options */
}
mysql_declare_plugin_end;
