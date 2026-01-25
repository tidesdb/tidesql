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

/** @file ha_tidesdb.h

    @brief
  The ha_tidesdb engine is a storage engine backed by TidesDB, an LSM-based
  embedded key-value store with multi-column families, atomic transactions,
  5 isolation levels, and lockless operations.

    @note
  TidesDB provides:
  - Column families (one per MySQL table)
  - ACID transactions with savepoints
  - 5 isolation levels (READ_UNCOMMITTED to SERIALIZABLE)
  - Compression (LZ4, Zstd, Snappy)
  - Bloom filters
  - TTL support
  - Efficient iterators with seek

   @see
  /sql/handler.h and /storage/tidesdb/ha_tidesdb.cc
*/

#ifndef HA_TIDESDB_H
#define HA_TIDESDB_H

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include <ft_global.h>

/* Use db.h which has C++-compatible opaque struct definitions */
#ifdef __cplusplus
extern "C" {
#endif
#include <tidesdb/db.h>
#ifdef __cplusplus
}
#endif

/** @brief
  TIDESDB_SHARE is a structure that will be shared among all open handlers
  for the same table. Contains the column family handle and table metadata.
*/
/* Maximum number of secondary indexes per table */
#define TIDESDB_MAX_INDEXES 64

/* Maximum number of fulltext indexes per table */
#define TIDESDB_MAX_FT_INDEXES 16

typedef struct st_tidesdb_share {
  char *table_name;
  uint table_name_length;
  uint use_count;
  pthread_mutex_t mutex;
  THR_LOCK lock;
  
  /* TidesDB column family for this table (primary data) */
  tidesdb_column_family_t *cf;
  
  /* Secondary index column families (one per non-primary index) */
  tidesdb_column_family_t *index_cf[TIDESDB_MAX_INDEXES];
  uint num_indexes;
  
  /* Primary key info */
  bool has_primary_key;
  uint pk_parts;  /* Number of key parts in primary key */
  
  /* TTL column index (-1 if no TTL column) */
  int ttl_field_index;
  
  /* Auto-increment tracking */
  ulonglong auto_increment_value;
  pthread_mutex_t auto_inc_mutex;
  
  /* Row count cache */
  ha_rows row_count;
  bool row_count_valid;
  
  /* Hidden primary key counter for tables without explicit PK */
  ulonglong hidden_pk_value;
  pthread_mutex_t hidden_pk_mutex;
  
  /* Fulltext index column families (inverted indexes) */
  tidesdb_column_family_t *ft_cf[TIDESDB_MAX_FT_INDEXES];
  uint ft_key_nr[TIDESDB_MAX_FT_INDEXES];  /* Key number for each FT index */
  uint num_ft_indexes;
} TIDESDB_SHARE;

/** @brief
  Class definition for the TidesDB storage engine handler
*/
class ha_tidesdb: public handler
{
  THR_LOCK_DATA lock;           ///< MySQL lock
  TIDESDB_SHARE *share;         ///< Shared lock info and CF handle
  
  /* Current transaction for this handler */
  tidesdb_txn_t *current_txn;
  
  /* Iterator for table scans */
  tidesdb_iter_t *scan_iter;
  bool scan_initialized;
  
  /* Buffer for current row's primary key */
  uchar *pk_buffer;
  uint pk_buffer_len;
  
  /* Buffer for serialized row data */
  uchar *row_buffer;
  uint row_buffer_len;
  
  /* Current row position (for rnd_pos) */
  uchar *current_key;
  size_t current_key_len;
  
  /* Bulk insert state */
  bool bulk_insert_active;
  tidesdb_txn_t *bulk_txn;
  ha_rows bulk_insert_rows;
  
  /* Fulltext search state */
  uint ft_current_idx;              /* Current FT index being searched */
  tidesdb_iter_t *ft_iter;          /* Iterator for FT results */
  char **ft_matched_pks;            /* Array of matched primary keys */
  size_t *ft_matched_pk_lens;       /* Lengths of matched PKs */
  uint ft_matched_count;            /* Number of matched PKs */
  uint ft_current_match;            /* Current position in matches */

  /* Helper methods */
  int pack_row(uchar *buf, uchar **packed, size_t *packed_len);
  int unpack_row(uchar *buf, const uchar *packed, size_t packed_len);
  int build_primary_key(const uchar *buf, uchar **key, size_t *key_len);
  int build_hidden_pk(uchar **key, size_t *key_len);
  void persist_hidden_pk_value(ulonglong value);
  void load_hidden_pk_value();
  void free_current_key();
  
  /* Secondary index helper methods */
  int build_index_key(uint idx, const uchar *buf, uchar **key, size_t *key_len);
  int insert_index_entry(uint idx, const uchar *buf, tidesdb_txn_t *txn);
  int delete_index_entry(uint idx, const uchar *buf, tidesdb_txn_t *txn);
  int update_index_entries(const uchar *old_buf, const uchar *new_buf, tidesdb_txn_t *txn);
  int create_secondary_indexes(const char *table_name);
  int open_secondary_indexes(const char *table_name);
  
  /* Fulltext index helper methods */
  int create_fulltext_indexes(const char *table_name);
  int open_fulltext_indexes(const char *table_name);
  int insert_ft_words(uint ft_idx, const uchar *buf, tidesdb_txn_t *txn);
  int delete_ft_words(uint ft_idx, const uchar *buf, tidesdb_txn_t *txn);
  int tokenize_text(const char *text, size_t len, CHARSET_INFO *cs,
                    void (*callback)(const char *word, size_t word_len, void *arg), void *arg);
  
public:
  ha_tidesdb(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_tidesdb();

  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const { return "TidesDB"; }

  /** @brief
    The name of the index type that will be used for display.
   */
  const char *index_type(uint inx) { return "LSM"; }

  /** @brief
    The file extensions used by TidesDB.
   */
  const char **bas_ext() const;

  /** @brief
    Table flags indicating what functionality the storage engine implements.
    
    TidesDB uses MVCC (Multi-Version Concurrency Control) for row-level
    concurrency - no table-level locking is needed. Each transaction sees
    a consistent snapshot based on its isolation level.
  */
  ulonglong table_flags() const
  {
    return HA_BINLOG_ROW_CAPABLE |
           HA_BINLOG_STMT_CAPABLE |
           HA_REC_NOT_IN_SEQ |        /* Records not in sequential order */
           HA_NULL_IN_KEY |           /* Nulls allowed in keys */
           HA_CAN_INDEX_BLOBS |       /* Can index blob columns */
           HA_CAN_FULLTEXT |          /* Supports FULLTEXT indexes */
           HA_PRIMARY_KEY_IN_READ_INDEX |
           HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |
           HA_STATS_RECORDS_IS_EXACT |  /* We can provide exact row counts */
           HA_CAN_SQL_HANDLER;          /* Supports HANDLER interface */
           /* TidesDB uses MVCC for row-level concurrency control */
           /* Hidden PK is supported for tables without explicit PK */
  }

  /** @brief
    Index flags indicating how the storage engine implements indexes.
  */
  ulong index_flags(uint inx, uint part, bool all_parts) const
  {
    return HA_READ_NEXT |
           HA_READ_PREV |
           HA_READ_ORDER |
           HA_READ_RANGE |
           HA_KEYREAD_ONLY;
  }

  /** @brief
    Limits for the storage engine.
   */
  uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }
  uint max_supported_keys()          const { return MAX_KEY; }
  uint max_supported_key_parts()     const { return MAX_REF_PARTS; }
  uint max_supported_key_length()    const { return 3072; }

  /** @brief
    Cost estimates for the optimizer.
  */
  virtual double scan_time() { return (double)(stats.records + stats.deleted) / 20.0 + 10; }
  virtual double read_time(uint index, uint ranges, ha_rows rows)
  { return (double)rows / 20.0 + 1; }

  /*
    Handler methods - implemented in ha_tidesdb.cc
  */
  
  /* Table lifecycle */
  int open(const char *name, int mode, uint test_if_locked);
  int close(void);
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);
  int delete_table(const char *name, my_bool delayed_drop);
  int rename_table(const char *from, const char *to);
  
  /* Row operations */
  int write_row(uchar *buf);
  int update_row(const uchar *old_data, uchar *new_data);
  int delete_row(const uchar *buf);
  
  /* Table scans */
  int rnd_init(bool scan);
  int rnd_end();
  int rnd_next(uchar *buf);
  int rnd_pos(uchar *buf, uchar *pos);
  void position(const uchar *record);
  
  /* Index operations (basic support) */
  int index_init(uint idx, bool sorted);
  int index_end();
  int index_read_map(uchar *buf, const uchar *key,
                     key_part_map keypart_map, enum ha_rkey_function find_flag);
  int index_next(uchar *buf);
  int index_next_same(uchar *buf, const uchar *key, uint keylen);
  int index_prev(uchar *buf);
  int index_first(uchar *buf);
  int index_last(uchar *buf);
  
  /* Full-text search (TODO: implement inverted index) */
  int ft_init() { return ft_handler ? 0 : HA_ERR_WRONG_COMMAND; }
  FT_INFO *ft_init_ext(uint flags, uint inx, String *key);
  int ft_read(uchar *buf);
  
  /* Statistics and info */
  int info(uint flag);
  int extra(enum ha_extra_function operation);
  int delete_all_rows(void);
  ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);
  
  /* Table maintenance */
  int optimize(THD* thd, HA_CHECK_OPT* check_opt);
  int analyze(THD* thd, HA_CHECK_OPT* check_opt);
  int check(THD* thd, HA_CHECK_OPT* check_opt);
  int repair(THD* thd, HA_CHECK_OPT* check_opt);
  int backup(THD* thd, HA_CHECK_OPT* check_opt);
  bool check_and_repair(THD *thd);
  bool is_crashed() const;
  
  /* Foreign key support */
  char *get_foreign_key_create_info();
  int get_foreign_key_list(THD *thd, List<FOREIGN_KEY_INFO> *f_key_list);
  uint referenced_by_foreign_key();
  void free_foreign_key_create_info(char *str);
  bool can_switch_engines();
  
  /* Auto-increment */
  virtual void get_auto_increment(ulonglong offset, ulonglong increment,
                                  ulonglong nb_desired_values,
                                  ulonglong *first_value,
                                  ulonglong *nb_reserved_values);
  int reset_auto_increment(ulonglong value);
  
  /* Bulk insert optimization */
  void start_bulk_insert(ha_rows rows);
  int end_bulk_insert();
  
  /* Locking */
  int external_lock(THD *thd, int lock_type);
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);
  
  /* Reset handler state */
  int reset(void);
};

#endif /* HA_TIDESDB_H */
