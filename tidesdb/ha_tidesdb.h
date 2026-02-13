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
#pragma once

#include "my_global.h"

#include <atomic>
#include <mutex>
#include <string>
#include <vector>

#include "handler.h"
#include "my_base.h"
#include "thr_lock.h"

extern "C"
{
#include <tidesdb/db.h>
}

/* ----- Key namespace prefixes (first byte of every TidesDB key) ------------------------------- */
static constexpr uint8_t KEY_NS_META = 0x00;
static constexpr uint8_t KEY_NS_DATA = 0x01;

/* ----- CF naming ------------------------------------------------------------------------------ */
static constexpr const char CF_INDEX_INFIX[] = "__idx_";

/* ----- Hidden primary key size (tables without explicit PK) ----------------------------------- */
static constexpr size_t HIDDEN_PK_SIZE = sizeof(uint64_t);

/* ----- Maximum number of secondary indexes we support ----------------------------------------- */
static constexpr uint MAX_TIDESDB_KEYS = MAX_KEY;

/* ----- Cost model constants for the optimizer ------------------------------------------------ */
static constexpr double TIDESDB_COST_SEQ_READ = 0.00005;
static constexpr double TIDESDB_COST_KEY_READ = 0.00003;
static constexpr double TIDESDB_COST_RANGE_SETUP = 0.0001;
static constexpr double TIDESDB_DEFAULT_READ_AMP = 1.0;

/* ----- Stats cache refresh interval (microseconds) ------------------------------------------- */
static constexpr long long TIDESDB_STATS_REFRESH_US = 2000000LL; /* 2 seconds */

/* ----- Minimum stats.records to avoid optimizer edge cases with 0 rows ----------------------- */
static constexpr ha_rows TIDESDB_MIN_STATS_RECORDS = 2;

/* ----- Inplace index build batch commit size ------------------------------------------------- */
static constexpr ha_rows TIDESDB_INDEX_BUILD_BATCH = 10000;

/* ----- Encryption ---------------------------------------------------------------------------- */
static constexpr uint TIDESDB_ENC_IV_LEN = 16;
static constexpr uint TIDESDB_ENC_KEY_LEN = 32;

/* ----- Bloom filter FPR conversion (table option stores parts per 10000) --------------------- */
static constexpr double TIDESDB_BLOOM_FPR_DIVISOR = 10000.0;

/* ----- Skip list probability conversion (table option stores percentage) --------------------- */
static constexpr float TIDESDB_SKIP_LIST_PROB_DIV = 100.0f;

/* ----- TTL sentinel value meaning no expiration ---------------------------------------------- */
static constexpr time_t TIDESDB_TTL_NONE = (time_t)-1;

/* ----- Default block cache size (bytes) ------------------------------------------------------ */
static constexpr ulonglong TIDESDB_DEFAULT_BLOCK_CACHE = 256ULL * 1024 * 1024;

/*
  TidesDB_share -- shared state for one table, visible to ALL handler objects.
*/
class TidesDB_share : public Handler_share
{
   public:
    /* Main data CF */
    tidesdb_column_family_t *cf;
    std::string cf_name;

    /* Primary key info */
    bool has_user_pk; /* true when table has an explicit PRIMARY KEY */
    uint pk_index;    /* MariaDB key number of the PK (usually 0)   */
    uint pk_key_len;  /* byte-length of PK in MariaDB key format    */

    /* Hidden PK row-id generator (used when has_user_pk == false) */
    std::atomic<uint64_t> next_row_id;

    /* In-memory AUTO_INCREMENT counter (avoids index_last() per INSERT).
       Seeded once from index_last() at open time; incremented atomically. */
    std::atomic<ulonglong> auto_inc_val{0};

    /* Per-table isolation level (from CREATE TABLE options) */
    tidesdb_isolation_level_t isolation_level;

    /* TTL support */
    ulonglong default_ttl; /* table-level default TTL in seconds (0 = none) */
    int ttl_field_idx;     /* field index of TTL_COL column (-1 = none)     */

    /* Data-at-rest encryption */
    bool encrypted;              /* true when ENCRYPTED=YES table option set */
    uint encryption_key_id;      /* ENCRYPTION_KEY_ID table option (default 1) */
    uint encryption_key_version; /* cached latest key version */

    /* Cached table shape flags (set once at open time) */
    bool has_blobs;             /* true when table contains any BLOB/TEXT columns */
    bool has_ttl;               /* true when TTL is configured (default_ttl or ttl_field_idx) */
    uint num_secondary_indexes; /* count of non-NULL secondary index CFs */

    /* Cached stats -- avoid expensive tidesdb_get_stats per statement.
       Refreshed at most every 2 seconds; read with relaxed atomics. */
    std::atomic<ha_rows> cached_records{0};
    std::atomic<uint64_t> cached_data_size{0};     /* total_data_size from CF */
    std::atomic<uint64_t> cached_idx_data_size{0}; /* sum of secondary CF sizes */
    std::atomic<uint32_t> cached_mean_rec_len{0};  /* avg_key_size + avg_value_size */
    std::atomic<long long> stats_refresh_us{0};    /* last refresh timestamp (µs) */
    double cached_read_amp{1.0};                   /* read amplification factor */

    /* Precomputed comparable key length per index (avoids per-row recomputation) */
    uint idx_comp_key_len[MAX_KEY];

    /* Secondary index CFs (one per secondary key) */
    std::vector<tidesdb_column_family_t *> idx_cfs;
    std::vector<std::string> idx_cf_names;

    TidesDB_share();
    ~TidesDB_share();
};

/*
  Context passed between Online DDL phases (prepare -> inplace -> commit).
  Holds the new/dropped CF pointers so commit can finalize atomically.
*/
class ha_tidesdb_inplace_ctx : public inplace_alter_handler_ctx
{
   public:
    /* CFs created for newly added indexes (populated during inplace phase) */
    std::vector<tidesdb_column_family_t *> add_cfs;
    std::vector<std::string> add_cf_names;
    std::vector<uint> add_key_nums; /* position in new key_info */

    /* CF names to drop for removed indexes (dropped during commit phase) */
    std::vector<std::string> drop_cf_names;

    virtual ~ha_tidesdb_inplace_ctx()
    {
    }
};

/*
  Per-connection TidesDB transaction context.
  Stored via thd_set_ha_data(); shared by ALL handler objects on the
  same connection.  The TidesDB txn spans the entire BEGIN...COMMIT
  block (or a single auto-commit statement).
*/
struct tidesdb_trx_t
{
    tidesdb_txn_t *txn;
    bool dirty;                                /* true once any DML uses txn */
    bool stmt_savepoint_active;                /* true while a "stmt" savepoint exists */
    bool stmt_was_dirty;                       /* true if current stmt had writes */
    tidesdb_isolation_level_t isolation_level; /* from first table opened */
};

/*
  ha_tidesdb -- per-connection handler object.
*/
class ha_tidesdb : public handler
{
    TidesDB_share *share;

    /* Points into the per-connection tidesdb_trx_t::txn.
       Set in external_lock(), cleared in external_lock(F_UNLCK). */
    tidesdb_txn_t *stmt_txn;
    bool stmt_txn_dirty; /* true once any DML uses stmt_txn */

    /* Scan / index-scan state (iterator lives on stmt_txn when available) */
    tidesdb_txn_t *scan_txn;
    tidesdb_iter_t *scan_iter;
    tidesdb_column_family_t *scan_cf_;      /* CF for lazy iterator creation */
    tidesdb_column_family_t *scan_iter_cf_; /* CF the cached scan_iter was created for */
    bool idx_pk_exact_done_;                /* deferred seek after PK exact */
    enum scan_dir_t
    {
        DIR_NONE,
        DIR_FORWARD,
        DIR_BACKWARD
    } scan_dir_;
    std::string last_row; /* keeps BLOB data alive across calls */

    /* Current row's PK key bytes (without namespace prefix).
       Fixed buffer eliminates std::string heap allocation per row. */
    uchar current_pk_buf_[MAX_KEY_LENGTH];
    uint current_pk_len_;

    /* Reusable buffer for serialize_row (retains heap capacity) */
    std::string row_buf_;

    /* Cached comparable search key from index_read_map for index_next_same */
    uchar idx_search_comp_[MAX_KEY_LENGTH];
    uint idx_search_comp_len_;

    /* Bulk insert state */
    bool in_bulk_insert_;

    /* Covering-index mode (HA_EXTRA_KEYREAD) */
    bool keyread_only_;

    /* ----- private helpers ----------------------------------------------------------------------
     */
    int ensure_stmt_txn(); /* lazy txn creation on first data access */
    TidesDB_share *get_share();
    const std::string &serialize_row(const uchar *buf);
    void deserialize_row(uchar *buf, const uchar *data, size_t len);
    void deserialize_row(uchar *buf, const std::string &row);
    static std::string path_to_cf_name(const char *path);

    /* Build memcmp-comparable key bytes into out[]; returns byte count */
    uint make_comparable_key(KEY *key_info, const uchar *record, uint num_parts, uchar *out);

    /* Convert key_copy-format search key directly to comparable bytes */
    uint key_copy_to_comparable(KEY *key_info, const uchar *key_buf, uint key_len, uchar *out);

    /* Build PK bytes from a record buffer into out[]; returns byte count */
    uint pk_from_record(const uchar *record, uchar *out);

    /* Build KEY_NS_DATA + pk into out[]; returns byte count */
    static uint build_data_key(const uchar *pk, uint pk_len, uchar *out)
    {
        out[0] = KEY_NS_DATA;
        memcpy(out + 1, pk, pk_len);
        return pk_len + 1;
    }

    /* Build a secondary-index entry key into out[]; returns byte count */
    uint sec_idx_key(uint idx, const uchar *record, uchar *out);

    /* Fetch a row by its PK bytes into buf; sets current_pk + last_row */
    int fetch_row_by_pk(tidesdb_txn_t *txn, const uchar *pk, uint pk_len, uchar *buf);

    /* Compute the absolute TTL timestamp for a row being written.
       Reads per-row TTL_COL value if present, else uses table default.
       Returns -1 (no expiration) or a future Unix timestamp. */
    time_t compute_row_ttl(const uchar *buf);

    /* Read current iterator entry (data-CF), decode row into buf.
       Returns 0 or HA_ERR_END_OF_FILE / HA_ERR_KEY_NOT_FOUND. */
    int iter_read_current(uchar *buf);

    /* Lazily create scan_iter from scan_cf_ when first needed */
    int ensure_scan_iter();

    /* Try to decode record from secondary index key (keyread-only) */
    bool try_keyread_from_index(const uint8_t *ik, size_t iks, uint idx, uchar *buf);

    /* Recover hidden-PK counter by scanning the CF */
    void recover_counters();

   public:
    ha_tidesdb(handlerton *hton, TABLE_SHARE *table_arg);
    ~ha_tidesdb() = default;

    ulonglong table_flags() const override
    {
        return HA_BINLOG_STMT_CAPABLE | HA_BINLOG_ROW_CAPABLE | HA_NULL_IN_KEY |
               HA_PRIMARY_KEY_IN_READ_INDEX | HA_TABLE_SCAN_ON_INDEX | HA_CAN_VIRTUAL_COLUMNS |
               HA_FAST_KEY_READ | HA_REC_NOT_IN_SEQ | HA_CAN_SQL_HANDLER |
               HA_REQUIRES_KEY_COLUMNS_FOR_DELETE | HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |
               HA_ONLINE_ANALYZE | HA_CAN_ONLINE_BACKUPS | HA_CONCURRENT_OPTIMIZE |
               HA_CAN_TABLES_WITHOUT_ROLLBACK;
    }

    ulong index_flags(uint idx, uint part, bool all_parts) const override
    {
        return HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE | HA_KEYREAD_ONLY;
    }

    uint max_supported_record_length() const override
    {
        return HA_MAX_REC_LENGTH;
    }
    uint max_supported_keys() const override
    {
        return MAX_TIDESDB_KEYS;
    }
    uint max_supported_key_parts() const override
    {
        return MAX_REF_PARTS;
    }
    uint max_supported_key_length() const override
    {
        return MAX_KEY_LENGTH;
    }

    IO_AND_CPU_COST scan_time() override
    {
        /* Full table scan -- sequential merge across LSM levels.
           Cost is proportional to data size, not just row count,
           because larger values mean more bytes to read. */
        IO_AND_CPU_COST cost;
        cost.io = 0;
        double rows = (double)(stats.records + stats.deleted);
        cost.cpu = rows * TIDESDB_COST_SEQ_READ;
        return cost;
    }

    IO_AND_CPU_COST keyread_time(uint index, ulong ranges, ha_rows rows, ulonglong blocks) override
    {
        /* Index read -- each point lookup touches read_amp levels.
           Range scans amortize the merge-heap cost across rows. */
        IO_AND_CPU_COST cost;
        cost.io = 0;
        double amp = share ? share->cached_read_amp : TIDESDB_DEFAULT_READ_AMP;
        cost.cpu =
            (double)rows * TIDESDB_COST_KEY_READ * amp + (double)ranges * TIDESDB_COST_RANGE_SETUP;
        return cost;
    }

    IO_AND_CPU_COST rnd_pos_time(ha_rows rows) override
    {
        /* Random position lookup -- each is a point-get through LSM levels.
           More expensive than sequential due to read amplification. */
        IO_AND_CPU_COST cost;
        cost.io = 0;
        double amp = share ? share->cached_read_amp : TIDESDB_DEFAULT_READ_AMP;
        cost.cpu = (double)rows * TIDESDB_COST_SEQ_READ * amp;
        return cost;
    }

    /* DDL */
    int open(const char *name, int mode, uint test_if_locked) override;
    int close(void) override;
    int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info) override;
    int delete_table(const char *name) override;
    int rename_table(const char *from, const char *to) override;

    /* Full table scan */
    int rnd_init(bool scan) override;
    int rnd_end() override;
    int rnd_next(uchar *buf) override;
    int rnd_pos(uchar *buf, uchar *pos) override;
    void position(const uchar *record) override;

    /* Index scan */
    int index_init(uint idx, bool sorted) override;
    int index_end() override;
    int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                       enum ha_rkey_function find_flag) override;
    int index_next(uchar *buf) override;
    int index_prev(uchar *buf) override;
    int index_first(uchar *buf) override;
    int index_last(uchar *buf) override;
    int index_next_same(uchar *buf, const uchar *key, uint keylen) override;

    /* DML */
    int write_row(const uchar *buf) override;
    int update_row(const uchar *old_data, const uchar *new_data) override;
    int delete_row(const uchar *buf) override;
    int delete_all_rows(void) override;

    /* Bulk insert hint (LOAD DATA, multi-row INSERT, sysbench prepare) */
    void start_bulk_insert(ha_rows rows, uint flags) override;
    int end_bulk_insert() override;

    /* Index Condition Pushdown (ICP) */
    Item *idx_cond_push(uint keyno, Item *idx_cond) override;

    /* AUTO_INCREMENT -- O(1) atomic counter instead of index_last() per INSERT */
    void get_auto_increment(ulonglong offset, ulonglong increment, ulonglong nb_desired_values,
                            ulonglong *first_value, ulonglong *nb_reserved_values) override;

    /* Stats / Maintenance */
    int info(uint flag) override;
    int analyze(THD *thd, HA_CHECK_OPT *check_opt) override;
    int optimize(THD *thd, HA_CHECK_OPT *check_opt) override;
    ha_rows records_in_range(uint inx, const key_range *min_key, const key_range *max_key,
                             page_range *pages) override;
    int extra(enum ha_extra_function operation) override;

    /* Locking -- TidesDB handles all concurrency via MVCC internally.
       lock_count()=0 bypasses MariaDB's THR_LOCK (same pattern as InnoDB). */
    uint lock_count(void) const override
    {
        return 0;
    }
    int external_lock(THD *thd, int lock_type) override;
    THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) override;

    /* Online DDL -- instant metadata, inplace indexes, copy for columns */
    enum_alter_inplace_result check_if_supported_inplace_alter(
        TABLE *altered_table, Alter_inplace_info *ha_alter_info) override;
    bool prepare_inplace_alter_table(TABLE *altered_table,
                                     Alter_inplace_info *ha_alter_info) override;
    bool inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info) override;
    bool commit_inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info,
                                    bool commit) override;
    bool check_if_incompatible_data(HA_CREATE_INFO *create_info, uint table_changes) override;
};
