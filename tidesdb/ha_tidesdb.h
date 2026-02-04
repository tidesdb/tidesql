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
  The ha_tidesdb engine is a storage engine backed by TidesDB.

   @see
  /sql/handler.h and /storage/tidesdb/ha_tidesdb.cc
*/

#ifndef HA_TIDESDB_H
#define HA_TIDESDB_H

#ifdef USE_PRAGMA_INTERFACE
#pragma interface /* gcc class implementation */
#endif

#include "handler.h"
#ifdef __cplusplus
extern "C"
{
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

/* Maximum number of foreign keys per table */
#define TIDESDB_MAX_FK 32

/* Maximum columns in a foreign key */
#define TIDESDB_FK_MAX_COLS 16

/* FK/table name buffer size */
#define TIDESDB_FK_NAME_MAX_LEN 128

/* Table name buffer size */
#define TIDESDB_TABLE_NAME_MAX_LEN 256

/* Max referencing tables for FK */
#define TIDESDB_MAX_REFERENCING 16

/* Max supported key length */
#define TIDESDB_MAX_KEY_LENGTH 3072

/* Buffer sizes */
#define TIDESDB_CF_NAME_BUF_SIZE         256
#define TIDESDB_CF_DROPPING_SUFFIX       "__dropping_%lu"
#define TIDESDB_CF_TRUNCATING_SUFFIX     "__truncating_%lu"
#define TIDESDB_IDX_CF_NAME_BUF_SIZE     512
#define TIDESDB_STATUS_BUF_SIZE          16384
#define TIDESDB_SAVEPOINT_NAME_LEN       64
#define TIDESDB_TIMESTAMP_BUF_SIZE       32
#define TIDESDB_ENCRYPTION_IV_SIZE       16
#define TIDESDB_ENCRYPTION_VERSION_SIZE  4
#define TIDESDB_ENCRYPTION_KEY_SIZE      32
#define TIDESDB_FK_KEY_BUF_SIZE          1024
#define TIDESDB_FK_KEY_RESERVE           100
#define TIDESDB_FK_COL_META_SIZE         9
#define TIDESDB_FT_PREFIX_BUF_SIZE       258
#define TIDESDB_FT_MAX_MATCHES           10000
#define TIDESDB_FT_SMALL_SET_THRESHOLD   16
#define TIDESDB_FT_HASH_ENTRY_LEN_SIZE   4
#define TIDESDB_INITIAL_KEY_BUF_CAPACITY 256
#define TIDESDB_BLOB_LEN_PREFIX_SIZE     4
#define TIDESDB_PACK_BUFFER_MIN_CAPACITY 4096
#define TIDESDB_FT_WORD_BUF_SIZE         256
#define TIDESDB_FT_MAX_QUERY_WORDS_CAP   256
#define TIDESDB_ASCII_CASE_OFFSET        32

/* Spatial/geometry constants */
#define TIDESDB_ZORDER_BITS             32
#define TIDESDB_ZORDER_MAX_VALUE        0xFFFFFFFF
#define TIDESDB_ZORDER_KEY_SIZE         8
#define TIDESDB_MIN_WKB_POINT_WITH_SRID 25
#define TIDESDB_WKB_SRID_OFFSET         4
#define TIDESDB_WKB_SRID_SIZE           4
#define TIDESDB_WKB_BYTE_ORDER_OFFSET   0
#define TIDESDB_WKB_BYTE_ORDER_SIZE     1
#define TIDESDB_WKB_TYPE_SIZE           4
#define TIDESDB_WKB_HEADER_SIZE         5
#define TIDESDB_WKB_X_OFFSET            5
#define TIDESDB_WKB_Y_OFFSET            13
#define TIDESDB_WKB_COORD_SIZE          8
#define TIDESDB_WKB_POINT_SIZE          16
#define TIDESDB_WKB_NUM_POINTS_SIZE     4
#define TIDESDB_WKB_MIN_LINESTRING_LEN  9
#define TIDESDB_WKB_TYPE_POINT          1
#define TIDESDB_WKB_TYPE_LINESTRING     2
#define TIDESDB_WKB_TYPE_POLYGON        3
#define TIDESDB_WKB_TYPE_MULTIPOINT     4
#define TIDESDB_WKB_TYPE_MASK           0xFF
#define TIDESDB_WKB_LITTLE_ENDIAN       1

/* Vlog threshold constants */
#define TIDESDB_VLOG_LARGE_VALUE_THRESHOLD  512
#define TIDESDB_VLOG_MEDIUM_VALUE_THRESHOLD 256
#define TIDESDB_VLOG_OVERHEAD_LARGE         1.3
#define TIDESDB_VLOG_OVERHEAD_MEDIUM        1.1
#define TIDESDB_VLOG_FACTOR_LARGE           1.4
#define TIDESDB_VLOG_FACTOR_MEDIUM          1.15

/* Cost model constants */
#define TIDESDB_BLOCK_SIZE                  65536.0
#define TIDESDB_MIN_AVG_ROW_SIZE            50
#define TIDESDB_MERGE_OVERHEAD_FACTOR       0.05
#define TIDESDB_CACHE_EFFECTIVENESS_FACTOR  0.9
#define TIDESDB_MIN_CACHE_FACTOR            0.1
#define TIDESDB_BTREE_FORMAT_OVERHEAD       1.02
#define TIDESDB_CPU_COST_PER_KEY            0.001
#define TIDESDB_FALLBACK_ROW_ESTIMATE       100
#define TIDESDB_FALLBACK_RECORD_COUNT       1000
#define TIDESDB_FALLBACK_DATA_FILE_LENGTH   (1024 * 1024)
#define TIDESDB_MIN_IO_COST                 1.0
#define TIDESDB_READ_TIME_BASE_IO           0.1
#define TIDESDB_SEEK_COST_BASE              0.2
#define TIDESDB_BTREE_HEIGHT_COST_BASE      0.2
#define TIDESDB_BTREE_HEIGHT_COST_PER_LEVEL 0.15
#define TIDESDB_BTREE_MAX_FORMAT_FACTOR     0.8
#define TIDESDB_BTREE_DEFAULT_FORMAT_FACTOR 0.5
#define TIDESDB_BLOOM_BENEFIT_BASE          0.3
#define TIDESDB_SECONDARY_IDX_FACTOR        2.0
#define TIDESDB_FALLBACK_RANGE_COST         0.4
#define TIDESDB_FALLBACK_ROW_COST           0.02
#define TIDESDB_SELECTIVITY_CAP             0.1
#define TIDESDB_SELECTIVITY_DIVISOR         10.0
#define TIDESDB_RANGE_FACTOR                5.0
#define TIDESDB_RANGE_MAX_PERCENT           30

/* Configuration defaults */
#define TIDESDB_DEFAULT_FLUSH_THREADS            2
#define TIDESDB_DEFAULT_COMPACTION_THREADS       2
#define TIDESDB_DEFAULT_BLOCK_CACHE_SIZE         (256ULL * 1024 * 1024)
#define TIDESDB_DEFAULT_WRITE_BUFFER_SIZE        (64ULL * 1024 * 1024)
#define TIDESDB_DEFAULT_COMPRESSION_ALGO         2
#define TIDESDB_DEFAULT_SYNC_MODE                2
#define TIDESDB_DEFAULT_SYNC_INTERVAL_US         128000
#define TIDESDB_DEFAULT_BLOOM_FPR                0.01
#define TIDESDB_DEFAULT_ENCRYPTION_KEY_ID        1
#define TIDESDB_DEFAULT_CHANGE_BUFFER_SIZE       1024
#define TIDESDB_DEFAULT_ISOLATION                1
#define TIDESDB_DEFAULT_LEVEL_SIZE_RATIO         10
#define TIDESDB_DEFAULT_SKIP_LIST_MAX_LEVEL      12
#define TIDESDB_DEFAULT_INDEX_SAMPLE_RATIO       1
#define TIDESDB_DEFAULT_TTL                      0
#define TIDESDB_DEFAULT_LOG_LEVEL                1
#define TIDESDB_DEFAULT_MIN_LEVELS               5
#define TIDESDB_DEFAULT_DIVIDING_LEVEL_OFFSET    2
#define TIDESDB_DEFAULT_SKIP_LIST_PROBABILITY    0.25
#define TIDESDB_DEFAULT_BLOCK_INDEX_PREFIX_LEN   16
#define TIDESDB_DEFAULT_KLOG_VALUE_THRESHOLD     512
#define TIDESDB_DEFAULT_MIN_DISK_SPACE           (100ULL * 1024 * 1024)
#define TIDESDB_DEFAULT_L1_FILE_COUNT_TRIGGER    4
#define TIDESDB_DEFAULT_L0_QUEUE_STALL_THRESHOLD 20
#define TIDESDB_DEFAULT_MAX_OPEN_SSTABLES        256
#define TIDESDB_DEFAULT_LOG_TRUNCATION_AT        (24ULL * 1024 * 1024)
#define TIDESDB_DEFAULT_ACTIVE_TXN_BUFFER_SIZE   (64ULL * 1024)

/* Fulltext search defaults */
#define TIDESDB_DEFAULT_FT_MIN_WORD_LEN    4
#define TIDESDB_DEFAULT_FT_MAX_WORD_LEN    84
#define TIDESDB_DEFAULT_FT_MAX_QUERY_WORDS 32

/* Encryption constants */
#define TIDESDB_ENCRYPTION_KEY_VERSION_LEN 4
#define TIDESDB_ENCRYPTION_IV_LEN          16
#define TIDESDB_ENCRYPTION_KEY_LEN         32
#define TIDESDB_ENCRYPTION_MIN_SRC_LEN     20

/* Primary key / Hidden PK constants */
#define TIDESDB_HIDDEN_PK_LEN              8
#define TIDESDB_HIDDEN_PK_PERSIST_INTERVAL 100
#define TIDESDB_HIDDEN_PK_META_KEY         "\x00__hidden_pk_max__"
#define TIDESDB_HIDDEN_PK_META_KEY_LEN     19
#define TIDESDB_AUTO_INC_META_KEY          "\x00__auto_inc_max__"
#define TIDESDB_AUTO_INC_META_KEY_LEN      17

/* Row packing constants */
#define TIDESDB_BLOB_LENGTH_PREFIX    4
#define TIDESDB_PACK_BUFFER_THRESHOLD 4096
#define TIDESDB_MB_DIVISOR            (1024.0 * 1024.0)

/* Optimizer cost model constants */
#define TIDESDB_KEY_LOOKUP_COST_BTREE 0.0008
#define TIDESDB_KEY_LOOKUP_COST_BLOCK 0.0020
#define TIDESDB_ROW_LOOKUP_COST_BTREE 0.0010
#define TIDESDB_ROW_LOOKUP_COST_BLOCK 0.0025
#define TIDESDB_KEY_NEXT_FIND_COST    0.00012
#define TIDESDB_ROW_NEXT_FIND_COST    0.00015
#define TIDESDB_KEY_COPY_COST         0.000015
#define TIDESDB_ROW_COPY_COST         0.000060
#define TIDESDB_DISK_READ_COST        0.000875
#define TIDESDB_DISK_READ_RATIO       0.20
#define TIDESDB_INDEX_BLOCK_COPY_COST 0.000030
#define TIDESDB_KEY_CMP_COST          0.000011
#define TIDESDB_ROWID_CMP_COST        0.000006
#define TIDESDB_ROWID_COPY_COST       0.000012

/* Spatial/Geometry constants */
#define TIDESDB_MIN_WKB_POINT_SIZE    25
#define TIDESDB_WKB_SRID_LEN          4
#define TIDESDB_MIN_WKB_POINT_NO_SRID 21
#define TIDESDB_WGS84_LON_MAX         180.0
#define TIDESDB_WGS84_LAT_MAX         90.0
#define TIDESDB_ZORDER_BITS           32

/* FK rule constants */
#define TIDESDB_FK_RULE_RESTRICT  0
#define TIDESDB_FK_RULE_CASCADE   1
#define TIDESDB_FK_RULE_SET_NULL  2
#define TIDESDB_FK_RULE_NO_ACTION 3

/* Miscellaneous constants */
#define TIDESDB_OPEN_TABLES_HASH_SIZE     32
#define TIDESDB_DEFAULT_ROW_ESTIMATE      1000
#define TIDESDB_DEFAULT_DATA_FILE_LENGTH  (1024ULL * 1024)
#define TIDESDB_FLUSH_WAIT_MAX_ITERATIONS 1000
#define TIDESDB_FLUSH_WAIT_SLEEP_US       1000
#define TIDESDB_AUTO_INC_PERSIST_INTERVAL 1000
#define TIDESDB_INDEX_REBUILD_BATCH_SIZE  1000
#define TIDESDB_BLOCK_SIZE                65536.0

/* Foreign key constraint structure */
typedef struct st_tidesdb_fk
{
    char fk_name[TIDESDB_FK_NAME_MAX_LEN];   /* FK constraint name */
    char ref_db[TIDESDB_FK_NAME_MAX_LEN];    /* Referenced database */
    char ref_table[TIDESDB_FK_NAME_MAX_LEN]; /* Referenced table */
    uint num_cols;                           /* Number of columns in FK */
    uint fk_col_idx[TIDESDB_FK_MAX_COLS];    /* Column indices in this table */
    char ref_col_names[TIDESDB_FK_MAX_COLS][TIDESDB_FK_NAME_MAX_LEN]; /* Referenced column names */
    int delete_rule; /* TIDESDB_FK_RULE_RESTRICT, CASCADE, SET_NULL, NO_ACTION */
    int update_rule; /* TIDESDB_FK_RULE_RESTRICT, CASCADE, SET_NULL, NO_ACTION */
} TIDESDB_FK;

/**
  Shared table metadata structure.

  Note on locking -- The mutex and THR_LOCK here are for MariaDB's internal
  table management (open/close coordination), not for row-level locking.
  TidesDB uses MVCC and is fundamentally lock-free for data access.
*/
typedef struct st_tidesdb_share
{
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
    uint pk_parts; /* Number of key parts in primary key */

    /* TTL column index (-1 if no TTL column) */
    int ttl_field_index;

    /* Auto-increment tracking */
    ulonglong auto_increment_value;
    pthread_mutex_t auto_inc_mutex;
    bool auto_inc_loaded; /* Whether auto-increment was loaded from storage */

    /* Row count cache */
    ha_rows row_count;
    bool row_count_valid;

    /* Hidden primary key counter for tables without explicit PK */
    ulonglong hidden_pk_value;
    pthread_mutex_t hidden_pk_mutex;

    /* Fulltext index column families (inverted indexes) */
    tidesdb_column_family_t *ft_cf[TIDESDB_MAX_FT_INDEXES];
    uint ft_key_nr[TIDESDB_MAX_FT_INDEXES]; /* Key number for each FT index */
    uint num_ft_indexes;

    /* Spatial index column families (Z-order encoded) */
    tidesdb_column_family_t *spatial_cf[TIDESDB_MAX_INDEXES];
    uint spatial_key_nr[TIDESDB_MAX_INDEXES]; /* Key number for each spatial index */
    uint num_spatial_indexes;

    /* Foreign key constraints on this table (child FKs) */
    TIDESDB_FK fk[TIDESDB_MAX_FK];
    uint num_fk;

    /* Tables that reference this table (parent FKs) -- for DELETE/UPDATE checks */
    char referencing_tables[TIDESDB_MAX_FK][TIDESDB_TABLE_NAME_MAX_LEN]; /* "db.table" format */
    int referencing_fk_rules[TIDESDB_MAX_FK]; /* delete_rule for each referencing FK */
    uint referencing_fk_cols[TIDESDB_MAX_FK]
                            [TIDESDB_FK_MAX_COLS]; /* FK column indices in child table */
    uint referencing_fk_col_count[TIDESDB_MAX_FK]; /* Number of FK columns per reference */
    size_t referencing_fk_offsets[TIDESDB_MAX_FK][TIDESDB_FK_MAX_COLS]; /* Byte offset of each FK
                                                                           col in child row */
    size_t referencing_fk_lengths[TIDESDB_MAX_FK]
                                 [TIDESDB_FK_MAX_COLS]; /* Byte length of each FK column */

    /* Change buffer for secondary index updates */
    struct
    {
        bool enabled;
        uint pending_count;
        pthread_mutex_t mutex;
    } change_buffer;
    uint num_referencing;

    /* Tablespace state -- for DISCARD/IMPORT TABLESPACE */
    bool tablespace_discarded;
} TIDESDB_SHARE;

/** @brief
  Class definition for the TidesDB storage engine handler
*/
class ha_tidesdb : public handler
{
    /**
      THR_LOCK_DATA is required by the MariaDB handler interface.
      However, TidesDB uses MVCC and is fundamentally lock-free:
      -- Reads never block (snapshot isolation)
      -- Writes use optimistic concurrency (conflict detection at commit)
      -- No row-level or table-level locking
      This lock structure is only used for MariaDB's internal bookkeeping.
    */
    THR_LOCK_DATA lock;
    TIDESDB_SHARE *share;  ///< Shared table metadata and CF handle

    /* Current transaction for this handler */
    tidesdb_txn_t *current_txn;
    bool scan_txn_owned;    /* True if we created the scan transaction */
    bool is_read_only_scan; /* True if current operation is read-only */

    /* Iterator for table scans */
    tidesdb_iter_t *scan_iter;
    bool scan_initialized;

    /* Buffer for current row's primary key */
    uchar *pk_buffer;
    uint pk_buffer_len;

    /* Buffer for serialized row data */
    uchar *row_buffer;
    uint row_buffer_len;

    /* Current row position (for rnd_pos) -- pre-allocated buffer */
    uchar *current_key;
    size_t current_key_len;
    size_t current_key_capacity;

    /* Bulk insert state */
    bool bulk_insert_active;
    tidesdb_txn_t *bulk_txn;
    ha_rows bulk_insert_rows;
    ha_rows bulk_insert_count;                          /* Rows inserted in current batch */
    static const ha_rows BULK_COMMIT_THRESHOLD = 10000; /* Commit every N rows */

    /* Skip redundant duplicate key check */
    bool skip_dup_check;

    /* Buffer pooling for pack_row() */
    uchar *pack_buffer;
    size_t pack_buffer_capacity;

    /* Index Condition Pushdown */
    Item *pushed_idx_cond;
    uint pushed_idx_cond_keyno;

    /* Table Condition Pushdown (full WHERE clause) */
    const COND *pushed_cond;

    /* Index-only scan mode */
    bool keyread_only;

    /* Track if current transaction is read-only (skip commit overhead) */
    bool txn_read_only;

    /* Buffer pooling for build_index_key() */
    uchar *idx_key_buffer;
    size_t idx_key_buffer_capacity;

    /* Fulltext search state */
    uint ft_current_idx;        /* Current FT index being searched */
    tidesdb_iter_t *ft_iter;    /* Iterator for FT results */
    char **ft_matched_pks;      /* Array of matched primary keys */
    size_t *ft_matched_pk_lens; /* Lengths of matched PKs */
    uint ft_matched_count;      /* Number of matched PKs */
    uint ft_current_match;      /* Current position in matches */

    /* Secondary index scan state */
    tidesdb_iter_t *index_iter;  /* Iterator for secondary index scans */
    uchar *index_key_buf;        /* Saved search key for index_next_same */
    uint index_key_len;          /* Length of saved search key */
    uint index_key_buf_capacity; /* Pre-allocated capacity */

    /* Buffer for saved old key in update_row */
    uchar *saved_key_buffer;
    size_t saved_key_buffer_capacity;

    /* SCALABILITY FIX: Buffer pooling for insert_index_entry() PK value */
    uchar *idx_pk_buffer;
    size_t idx_pk_buffer_capacity;

    /* Helper methods */
    int pack_row(const uchar *buf, uchar **packed, size_t *packed_len);
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

    /* Foreign key enforcement helper methods */
    int parse_foreign_keys();
    int check_fk_parent_exists(uint fk_idx, const uchar *buf, tidesdb_txn_t *txn);
    int check_fk_children_exist(const uchar *buf, tidesdb_txn_t *txn);
    int check_foreign_key_constraints_insert(const uchar *buf, tidesdb_txn_t *txn);
    int check_foreign_key_constraints_delete(const uchar *buf, tidesdb_txn_t *txn);
    int execute_fk_cascade_delete(const uchar *buf, tidesdb_txn_t *txn);
    int execute_fk_set_null(const uchar *buf, tidesdb_txn_t *txn);
    int execute_fk_cascade_update(const uchar *old_buf, const uchar *new_buf, tidesdb_txn_t *txn,
                                  uint ref_idx);
    int check_foreign_key_constraints_update(const uchar *old_buf, const uchar *new_buf,
                                             tidesdb_txn_t *txn);

    /* Spatial index helper methods (Z-order encoding) */
    uint64_t encode_zorder(double x, double y);
    void decode_zorder(uint64_t z, double *x, double *y);
    int create_spatial_index(const char *table_name, uint key_nr);
    int insert_spatial_entry(uint idx, const uchar *buf, tidesdb_txn_t *txn);
    int delete_spatial_entry(uint idx, const uchar *buf, tidesdb_txn_t *txn);

    /* Auto-increment persistence */
    void persist_auto_increment_value(ulonglong value);
    void load_auto_increment_value();

   public:
    ha_tidesdb(handlerton *hton, TABLE_SHARE *table_arg);
    ~ha_tidesdb();

    /** @brief
      The name that will be used for display purposes.
     */
    const char *table_type() const
    {
        return "TIDESDB";
    }

    /** @brief
      The name of the index type that will be used for display.
     */
    const char *index_type(uint inx)
    {
        return "LSMB+";
    }

    /** @brief
      The file extensions used by TidesDB.
     */
    const char **bas_ext() const;

    /** @brief
      Table flags indicating what functionality the storage engine implements.

      TidesDB uses MVCC (Multi-Version Concurrency Control) for row-level
      concurrency -- no table-level locking is needed. Each transaction sees
      a consistent snapshot based on its isolation level.
    */
    ulonglong table_flags() const
    {
        return HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |
               HA_REC_NOT_IN_SEQ |      /* Records not in sequential order */
               HA_NULL_IN_KEY |         /* Nulls allowed in keys */
               HA_CAN_INDEX_BLOBS |     /* Can index blob columns */
               HA_CAN_FULLTEXT |        /* Supports FULLTEXT indexes */
               HA_CAN_VIRTUAL_COLUMNS | /* Supports virtual/generated columns */
               HA_CAN_GEOMETRY |        /* Supports spatial/geometry types */
               HA_PRIMARY_KEY_IN_READ_INDEX | HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |
               HA_STATS_RECORDS_IS_EXACT |       /* We can provide exact row counts */
               HA_CAN_SQL_HANDLER |              /* Supports HANDLER interface */
               HA_CAN_EXPORT |                   /* Supports transportable tablespaces */
               HA_CAN_ONLINE_BACKUPS |           /* Supports online backup */
               HA_CONCURRENT_OPTIMIZE |          /* OPTIMIZE doesn't block */
               HA_CAN_RTREEKEYS |                /* Supports spatial indexes via Z-order */
               HA_TABLE_SCAN_ON_INDEX |          /* Can scan table via index */
               HA_CAN_REPAIR |                   /* Supports REPAIR TABLE (via compaction) */
               HA_CRASH_SAFE |                   /* Crash-safe via WAL */
               HA_ONLINE_ANALYZE |               /* No need to evict from cache after ANALYZE */
               HA_CAN_TABLE_CONDITION_PUSHDOWN | /* Supports WHERE pushdown during scans */
               HA_CAN_SKIP_LOCKED | /* MVCC: SELECT FOR UPDATE SKIP LOCKED (no blocking) */
               HA_HAS_RECORDS |     /* records() returns exact count */
               HA_CAN_FULLTEXT_EXT; /* Extended fulltext API (relevance, boolean mode) */
    }

    /** @brief
      Index flags indicating how the storage engine implements indexes.

      HA_DO_INDEX_COND_PUSHDOWN enables Index Condition Pushdown (ICP) which
      allows the storage engine to evaluate WHERE conditions during index scan,
      filtering rows before fetching full row data.

      HA_DO_RANGE_FILTER_PUSHDOWN enables rowid filter pushdown for semi-joins.
    */
    ulong index_flags(uint inx, uint part, bool all_parts) const override;
    bool pk_is_clustering_key(uint index) const
    {
        return true;
    }

    /** @brief
      Limits for the storage engine.
     */
    uint max_supported_record_length() const
    {
        return HA_MAX_REC_LENGTH;
    }
    uint max_supported_keys() const
    {
        return MAX_KEY;
    }
    uint max_supported_key_parts() const
    {
        return MAX_REF_PARTS;
    }
    uint max_supported_key_length() const
    {
        return TIDESDB_MAX_KEY_LENGTH;
    }

    /** @brief
      Query cache type for this table.
      Transactional tables should return HA_CACHE_TBL_TRANSACT.
    */
    uint8 table_cache_type() override
    {
        return HA_CACHE_TBL_TRANSACT;
    }

    /** @brief
      Return exact row count. Called when HA_HAS_RECORDS is set.
    */
    ha_rows records() override;

    /** @brief
      Truncate table -- faster than delete_all_rows.
    */
    int truncate() override;

    /** @brief
      Release row lock after read (MVCC -- no-op for TidesDB).
    */
    void unlock_row() override
    {
    }

    /** @brief
      Statement-level transaction handling.
    */
    int start_stmt(THD *thd, thr_lock_type lock_type) override;

    /** @brief
      Custom error messages for TidesDB errors.
    */
    bool get_error_message(int error, String *buf) override;

    /** @brief
      Cost estimates for the optimizer.

      LSM-tree cost model considerations:
      -- Point lookups -- memtable check + bloom filter checks + level lookups
      -- Range scans -- merge across multiple levels (more expensive than B-tree)
      -- Block cache hits are much faster than disk reads
    */
    virtual IO_AND_CPU_COST scan_time() override;
    IO_AND_CPU_COST read_time(uint index, uint ranges, ha_rows rows);

    /* Table lifecycle */
    int open(const char *name, int mode, uint test_if_locked);
    int close(void);
    int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);
    int delete_table(const char *name) override;
    int rename_table(const char *from, const char *to);

    /* Handler cloning for parallel operations (DS-MRR, parallel scans) */
    handler *clone(const char *name, MEM_ROOT *mem_root) override;

    /* Row operations */
    int write_row(const uchar *buf) override;
    int update_row(const uchar *old_data, const uchar *new_data) override;
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
    int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                       enum ha_rkey_function find_flag);
    int index_next(uchar *buf);
    int index_next_same(uchar *buf, const uchar *key, uint keylen);
    int index_prev(uchar *buf);
    int index_first(uchar *buf);
    int index_last(uchar *buf);

    /* Full-text search using inverted index */
    int ft_init()
    {
        return ft_handler ? 0 : HA_ERR_WRONG_COMMAND;
    }
    FT_INFO *ft_init_ext(uint flags, uint inx, String *key);
    int ft_read(uchar *buf);

    /* Statistics and info */
    int info(uint flag);
    int extra(enum ha_extra_function operation);
    int delete_all_rows() override;
    ha_rows records_in_range(uint inx, const key_range *min_key, const key_range *max_key,
                             page_range *pages) override;

    /* Table maintenance */
    int optimize(THD *thd, HA_CHECK_OPT *check_opt);
    int analyze(THD *thd, HA_CHECK_OPT *check_opt);
    int check(THD *thd, HA_CHECK_OPT *check_opt);
    int repair(THD *thd, HA_CHECK_OPT *check_opt);
    int backup(THD *thd, HA_CHECK_OPT *check_opt);
    int preload_keys(THD *thd, HA_CHECK_OPT *check_opt) override;
    bool check_and_repair(THD *thd);
    bool is_crashed() const;

    /* Foreign key support */
    char *get_foreign_key_create_info();
    int get_foreign_key_list(THD *thd, List<FOREIGN_KEY_INFO> *f_key_list);
    bool referenced_by_foreign_key() const noexcept override;
    void free_foreign_key_create_info(char *str);
    bool can_switch_engines();

    /* Auto-increment */
    virtual void get_auto_increment(ulonglong offset, ulonglong increment,
                                    ulonglong nb_desired_values, ulonglong *first_value,
                                    ulonglong *nb_reserved_values);
    int reset_auto_increment(ulonglong value);

    /* Bulk insert optimization */
    void start_bulk_insert(ha_rows rows, uint flags) override;
    int end_bulk_insert();

    /* Tablespace support */
    int discard_or_import_tablespace(my_bool discard);

    /* Locking */
    int external_lock(THD *thd, int lock_type);
    THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);

    /* Reset handler state */
    int reset(void);

    /* Index Condition Pushdown */
    Item *idx_cond_push(uint keyno, Item *idx_cond) override;

    /* Table Condition Pushdown (full WHERE clause during table scans) */
    const COND *cond_push(const COND *cond) override;
    void cond_pop() override;

    /* Multi-Range Read (MRR) interface for batch key lookups */
    int multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param, uint n_ranges, uint mode,
                              HANDLER_BUFFER *buf) override;
    int multi_range_read_next(range_id_t *range_info) override;
    ha_rows multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq, void *seq_init_param,
                                        uint n_ranges, uint *bufsz, uint *flags, ha_rows limit,
                                        Cost_estimate *cost) override;
    ha_rows multi_range_read_info(uint keyno, uint n_ranges, uint keys, uint key_parts, uint *bufsz,
                                  uint *flags, Cost_estimate *cost) override;
    int multi_range_read_explain_info(uint mrr_mode, char *str, size_t size) override;

    /* Online DDL support */
    enum_alter_inplace_result check_if_supported_inplace_alter(
        TABLE *altered_table, Alter_inplace_info *ha_alter_info) override;

   protected:
    bool prepare_inplace_alter_table(TABLE *altered_table,
                                     Alter_inplace_info *ha_alter_info) override;
    bool inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info) override;
    bool commit_inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info,
                                    bool commit) override;

   private:
    /* DS-MRR implementation object */
    DsMrr_impl m_ds_mrr;

    /* Online DDL helper methods */
    int add_index_inplace(TABLE *altered_table, Alter_inplace_info *ha_alter_info);
    int drop_index_inplace(Alter_inplace_info *ha_alter_info);
    int rebuild_secondary_index(KEY *key_info, const char *key_name, TABLE *target_table);
};

#endif /* HA_TIDESDB_H */
