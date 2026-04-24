/*
  Copyright (c) 2026 TidesDB Corp.

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

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "handler.h"
#include "my_base.h"
#include "my_global.h"
#include "thr_lock.h"

extern "C"
{
#include <tidesdb/db.h>
}

/* Key namespace prefixes (first byte of every TidesDB key) */
static constexpr uint8_t KEY_NS_META = 0x00;
static constexpr uint8_t KEY_NS_DATA = 0x01;

/* Size of the namespace prefix that every TidesDB key starts with. */
static constexpr uint KEY_NAMESPACE_LEN = 1;

/* Buffer size for a data CF key, namespace byte + comparable PK + 1 byte slack.
   Used by every site that builds KEY_NS_DATA + pk via build_data_key. */
static constexpr uint DATA_KEY_BUF_LEN = KEY_NAMESPACE_LEN + MAX_KEY_LENGTH + 1;

/* Buffer size for a secondary-index CF entry key, comparable index-column
   bytes (up to MAX_KEY_LENGTH) + appended PK bytes (up to MAX_KEY_LENGTH)
   + 2 bytes of slack that covers VARBINARY length-byte overflow emitted
   by make_comparable_key. */
static constexpr uint SEC_IDX_KEY_BUF_LEN = (MAX_KEY_LENGTH * 2) + 2;

/* Number of doubles in a 2-D minimum bounding rectangle.  Always four
   (xmin, ymin, xmax, ymax); used for the on-disk spatial value layout
   and the in-memory query-MBR cache on the handler. */
static constexpr uint SPATIAL_MBR_DIMS = 4;

/* CF naming */
static constexpr const char CF_INDEX_INFIX[] = "__idx_";

/* Reserved CF for schema discovery (object store mode only) */
static constexpr const char SCHEMA_CF_NAME[] = "__tidesql_schema";

/* Hidden primary key size (tables without explicit PK) */
static constexpr size_t HIDDEN_PK_SIZE = sizeof(uint64_t);

/* Maximum number of secondary indexes we support */
static constexpr uint MAX_TIDESDB_KEYS = MAX_KEY;

/* Cost model constants for the optimizer */
static constexpr double TIDESDB_COST_SEQ_READ = 0.00005;
static constexpr double TIDESDB_COST_KEY_READ = 0.00003;
static constexpr double TIDESDB_COST_RANGE_SETUP = 0.0001;
static constexpr double TIDESDB_DEFAULT_READ_AMP = 1.0;

/* Stats cache refresh interval (microseconds) */
static constexpr long long TIDESDB_STATS_REFRESH_US = 2000000LL; /* 2 seconds */

/* Minimum stats.records to avoid optimizer edge cases with 0 rows */
static constexpr ha_rows TIDESDB_MIN_STATS_RECORDS = 2;

/* scan_time() -- split the opaque cost returned by tidesdb_range_cost
   between MariaDB's I/O and CPU cost buckets.  LSM scans are mostly
   block-read bound, so 90% I/O / 10% CPU matches observed profiles. */
static constexpr double TIDESDB_SCAN_IO_WEIGHT = 0.9;
static constexpr double TIDESDB_SCAN_CPU_WEIGHT = 0.1;

/* records_in_range() fallbacks when we can't get a useful estimate. */
static constexpr ha_rows TIDESDB_RIR_DEFAULT_EST = 10;         /* no share available */
static constexpr ha_rows TIDESDB_RIR_UNKNOWN_DENOM = 4;        /* total/4 + 1 quarter fallback */
static constexpr double TIDESDB_RIR_FRACTION_UNRELIABLE = 0.8; /* fall back to rec_per_key */

/* Inplace index build batch commit size.  Larger batches amortize the
   commit+iter-recreate+seek cost that fires every batch boundary (see
   inplace_alter_table -- each batch boundary does a free+reset txn and
   re-seeks to last_data_key, which is O(merge-heap) per seek).  50k
   matches TIDESDB_BULK_INSERT_BATCH_OPS and keeps txn memory bounded. */
static constexpr ha_rows TIDESDB_INDEX_BUILD_BATCH = 50000;

/* Bulk insert mid-txn commit threshold (ops, not rows) */
static constexpr ha_rows TIDESDB_BULK_INSERT_BATCH_OPS = 50000;

/* Encryption */
static constexpr uint TIDESDB_ENC_IV_LEN = 16;
static constexpr uint TIDESDB_ENC_KEY_LEN = 32;

/* Bloom filter FPR conversion (table option stores parts per 10000) */
static constexpr double TIDESDB_BLOOM_FPR_DIVISOR = 10000.0;

/* Skip list probability conversion (table option stores percentage) */
static constexpr float TIDESDB_SKIP_LIST_PROB_DIV = 100.0f;

/* TTL sentinel value meaning no expiration */
static constexpr time_t TIDESDB_TTL_NONE = (time_t)-1;

/* Default block cache size (bytes) */
static constexpr ulonglong TIDESDB_DEFAULT_BLOCK_CACHE = 256ULL * 1024 * 1024; /* 256M */

/*
  TidesDB_share -- shared state for one table, visible to all handler objects.
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
    size_t cached_row_est{0};   /* cached serialize_row size estimate for non-BLOB tables */

    /* Field indices of BLOB/TEXT columns -- populated at open() when
       has_blobs is true.  serialize_row iterates only these instead of
       scanning all fields for the BLOB_FLAG. */
    std::vector<uint16> blob_field_indices;

    /* Cached scan_time range cost (refreshed every TIDESDB_STATS_REFRESH_US) */
    std::atomic<double> cached_scan_cost{0.0};
    std::atomic<long long> scan_cost_time{0};

    /* Table timestamps for information_schema.TABLES */
    time_t create_time{0};              /* from .frm stat at first open */
    std::atomic<time_t> update_time{0}; /* bumped on DML (write/update/delete) */

    /* Cached stats -- avoid expensive tidesdb_get_stats per statement.
       Refreshed at most every 2 seconds; read with relaxed atomics. */
    std::atomic<ha_rows> cached_records{0};
    std::atomic<uint64_t> cached_data_size{0};     /* total_data_size from CF */
    std::atomic<uint64_t> cached_idx_data_size{0}; /* sum of secondary CF sizes */
    std::atomic<uint32_t> cached_mean_rec_len{0};  /* avg_key_size + avg_value_size */
    std::atomic<long long> stats_refresh_us{0};    /* last refresh timestamp (µs) */
    std::atomic<double> cached_read_amp{1.0};      /* read amplification factor */

    /* Precomputed comparable key length per index (avoids per-row recomputation) */
    uint idx_comp_key_len[MAX_KEY];

    /* Precomputed index-type flags (avoid ki->algorithm dereference per row
       in DML secondary-index loops).  Populated at open() and refreshed
       during online DDL. */
    bool idx_is_fts[MAX_KEY];
    bool idx_is_spatial[MAX_KEY];

    /* Cached rec_per_key for secondary indexes (populated by ANALYZE TABLE).
       0 = not yet computed, use heuristic; >0 = sampled value. */
    std::atomic<ulong> cached_rec_per_key[MAX_KEY];

    /* Secondary index CFs (one per secondary key) */
    std::vector<tidesdb_column_family_t *> idx_cfs;
    std::vector<std::string> idx_cf_names;

    /* Per-index covered-field map used by try_keyread_from_index.  For each
       index i, idx_cover[i][field_c] == true when field `c` can be
       reconstructed from the index key bytes (i.e. field is in the index's
       key parts or -- for secondary indexes -- in the PK parts appended
       to the key).  Replaces the O(read_set_bits * (pk_parts + idx_parts))
       nested scan the old code did on every covered read. */
    std::vector<std::vector<bool>> idx_cover;

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
  Stored via thd_set_ha_data(); shared by all handler objects on the
  same connection.  The TidesDB txn spans the entire BEGIN...COMMIT
  block (or a single auto-commit statement).
*/
struct tidesdb_trx_t
{
    tidesdb_txn_t *txn;
    bool dirty;                 /* true once any DML uses txn */
    bool stmt_savepoint_active; /* true while a "stmt" savepoint exists */
    bool stmt_was_dirty;        /* true if current stmt had writes */
    bool needs_reset;           /* true after commit/rollback; cleared after txn_reset */
    tidesdb_isolation_level_t isolation_level; /* from first table opened */
    uint64_t txn_generation; /* monotonic counter; incremented each time a new txn is created */

    /* Plugin-level row locks -- lock table entries held by this txn.
       Acquired during SELECT FOR UPDATE (index_read_map with write intent)
       and UPDATE/DELETE (update_row/delete_row).  Released on commit/rollback.
       Implements pessimistic row locking with deadlock detection via a global
       lock table (hash table with wait queues and wait-for graph traversal).
       This in a way emulates InnoDB-style row locks for workloads like TPC-C that
       require read-modify-write serialization on the same key. */

    struct tdb_row_lock_t *held_locks_head; /* singly-linked list of held lock entries */
    uint64_t lock_txn_id; /* unique ID for deadlock detection (= txn_generation) */
    /* `waiting_on` is read by other threads during deadlock graph walks (from
       arbitrary partitions) without holding this trx's partition mutex.
       Atomic access guarantees the reader sees a published pointer value and
       never a torn write. */
    std::atomic<struct tdb_row_lock_t *> waiting_on;
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
    tidesdb_txn_t *scan_iter_txn_;          /* txn the cached scan_iter was created on */
    uint64_t scan_iter_txn_gen_;            /* txn_generation when scan_iter was created */
    bool idx_pk_exact_done_;                /* deferred seek after PK exact */
    enum scan_dir_t
    {
        DIR_NONE,
        DIR_FORWARD,
        DIR_BACKWARD
    } scan_dir_;
    std::string last_row;  /* keeps BLOB data alive for record[0] */
    std::string last_row2; /* keeps BLOB data alive for record[1] */

    /* Spatial index scan state */
    bool spatial_scan_active_{false};
    enum ha_rkey_function spatial_mode_
    {
        HA_READ_KEY_EXACT
    };
    double spatial_qmbr_[SPATIAL_MBR_DIMS]{}; /* query MBR (xmin, ymin, xmax, ymax) */

    /* Hilbert range decomposition are sorted non-overlapping [lo, hi] ranges
       covering the query box.  spatial_range_idx_ tracks which range we're
       currently scanning. */
    std::vector<std::pair<uint64_t, uint64_t>> spatial_ranges_; /* {lo, hi} */
    size_t spatial_range_idx_{0};

    /* Spatial scan continuation -- scans Hilbert range with MBR post-filter */
    int spatial_scan_next(uchar *buf);

    /* Current row's PK key bytes (without namespace prefix).
       Fixed buffer eliminates std::string heap allocation per row. */
    uchar current_pk_buf_[MAX_KEY_LENGTH];
    uint current_pk_len_;

    /* Reusable buffer for serialize_row (retains heap capacity) */
    std::string row_buf_;

    /* Cached comparable search key from index_read_map for index_next_same */
    uchar idx_search_comp_[MAX_KEY_LENGTH];
    uint idx_search_comp_len_;

    /* Reusable buffers for secondary index key construction in update_row.
       Avoids heap allocation per row and keeps the stack frame small. */
    uchar upd_old_ik_[SEC_IDX_KEY_BUF_LEN];
    uchar upd_new_ik_[SEC_IDX_KEY_BUF_LEN];

    /* Cached dup-check iterators for UNIQUE secondary indexes.
       tidesdb_iter_new() is O(num_sstables) -- caching avoids rebuilding
       the merge heap on every INSERT for tables with unique indexes. */
    tidesdb_iter_t *dup_iter_cache_[MAX_KEY];
    tidesdb_txn_t *dup_iter_txn_[MAX_KEY]; /* txn each was created on */
    uint64_t dup_iter_txn_gen_[MAX_KEY];   /* txn_generation when created */
    uint dup_iter_count_;                  /* number of slots populated */

    /* Reusable buffer for tidesdb_txn_get values -- avoids malloc/free per
       point-lookup.  Retains heap capacity across calls. */
    std::string get_val_buf_;

    /* Separate encryption output buffer so row_buf_ retains its capacity
       across calls (tidesdb_encrypt_row used to replace row_buf_). */
    std::string enc_buf_;

    /* Per-statement cached encryption key version -- avoids calling
       encryption_key_get_latest_version() on every single row write. */
    uint cached_enc_key_ver_;
    bool enc_key_ver_valid_;

    /* Per-statement cached time(NULL) -- avoids the vDSO/syscall on every
       row for TTL computation.  1-second granularity is sufficient for TTL. */
    time_t cached_time_;
    bool cached_time_valid_;

    /* Per-statement cached THDVAR lookups -- avoids the indirect
       thd_get_ha_data + offset computation on every row. */
    ulonglong cached_sess_ttl_;
    bool cached_skip_unique_;
    bool cached_single_delete_primary_;
    bool cached_thdvars_valid_;

    /* Write-lock mode -- set when store_lock detects FOR UPDATE / write intent.
       Used to decide whether to acquire row locks in index_read_map. */
    bool stmt_has_write_lock_;

    /* Cached "is this scan on the primary key" flag.  Set once in index_init
       so the navigation methods (index_next/prev/first/last/next_same) skip
       the per-row `share->has_user_pk && active_index == share->pk_index`
       recomputation. */
    bool is_pk_;

    /* Cached last tidesdb_iter_new failure for the current scan CF/txn.
       When non-zero and the scan_cf_/scan_txn haven't changed, ensure_scan_iter
       returns the prior error immediately instead of retrying + re-logging. */
    int scan_iter_last_err_;
    tidesdb_column_family_t *scan_iter_last_err_cf_;
    tidesdb_txn_t *scan_iter_last_err_txn_;

    /* Handler mirrors of share->has_blobs / share->encrypted.  Per-row
       fetches and scans branch on these; reading them from a handler member
       avoids the shared-memory dereference that dominates when the L1 line
       for `share` isn't already hot. */
    bool has_blobs_;
    bool encrypted_;

    /* Cached bounds of table->record[1] so the BLOB path of fetch_row_by_pk
       and iter_read_current can classify `buf` against record[0] vs record[1]
       without dereferencing `table->record[1]` and `table->s->reclength` on
       every row. */
    const uchar *record1_lo_;
    const uchar *record1_hi_;

    /* Cached per-statement THD query shape so ensure_stmt_txn() and
       external_lock() don't each re-evaluate thd_sql_command() and
       thd_test_options().  Populated by external_lock(F_WRLCK/F_RDLCK);
       invalidated by external_lock(F_UNLCK) along with the other per-stmt
       caches. */
    int cached_sql_cmd_;
    bool cached_is_autocommit_;
    bool cached_stmt_shape_valid_;

    /* Cached per-statement pointers to avoid repeated hash lookups.
       Set in external_lock(lock), cleared in external_lock(F_UNLCK).
       InnoDB caches these as m_user_thd / m_prebuilt->trx. */
    THD *cached_thd_;           /* avoids ha_thd() virtual dispatch */
    tidesdb_trx_t *cached_trx_; /* avoids thd_get_ha_data() hash lookup */
    bool trx_registered_;       /* true once trans_register_ha() called this txn */

    /* Bulk DML state.  The ops counter is shared across insert/update/delete
       bulk modes since only one can be active at a time and they all use the
       same TIDESDB_BULK_INSERT_BATCH_OPS threshold. */
    bool in_bulk_insert_;
    bool in_bulk_update_;
    bool in_bulk_delete_;
    ha_rows bulk_insert_ops_; /* ops buffered since last mid-txn commit */

    /* Multi-Range Read state.  We accept MRR when every range the optimizer
       hands us is UNIQUE_RANGE|EQ_RANGE (i.e. the WHERE col IN (...) case on
       a full key) and fall back to the default MRR->read_range_first path for
       everything else.  Accepted ranges are buffered + sorted by comparable
       key bytes so the LSM sees a monotone stream of seeks. */
    struct tdb_mrr_entry
    {
        std::string comp_key; /* comparable PK / index bytes */
        range_id_t ptr;       /* value returned to caller as *range_info */
    };
    bool mrr_custom_active_;
    bool mrr_no_assoc_;
    uint mrr_keyno_;
    std::vector<tdb_mrr_entry> mrr_entries_;
    size_t mrr_next_idx_;

    /* Covering-index mode (HA_EXTRA_KEYREAD) */
    bool keyread_only_;
    bool write_can_replace_; /* true during REPLACE INTO (HA_EXTRA_WRITE_CAN_REPLACE) */

    /* private helpers
     */
    int ensure_stmt_txn(); /* lazy txn creation on first data access */
    TidesDB_share *get_share();
    const std::string &serialize_row(const uchar *buf);
    void deserialize_row(uchar *buf, const uchar *data, size_t len);
    void deserialize_row(uchar *buf, const std::string &row);

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
        memcpy(out + KEY_NAMESPACE_LEN, pk, pk_len);
        return pk_len + KEY_NAMESPACE_LEN;
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

    /* Evaluate pushed index condition on a secondary-index entry before
       the expensive PK point-lookup.  Decodes the index key columns into
       buf and calls handler_index_cond_check().
       Returns CHECK_POS                -- condition satisfied, proceed with PK lookup
               CHECK_NEG                -- condition not satisfied, skip this entry
               CHECK_OUT_OF_RANGE       -- past end of scan range
               CHECK_ABORTED_BY_USER    -- query killed */
    check_result_t icp_check_secondary(const uint8_t *ik, size_t iks, uint idx, uchar *buf);

    /* Reverse a single integer sort-key part back to native little-endian
       at `to` (destination byte pointer computed once by the caller).
       Returns true on success, false for unsupported sort_len. */
    static bool decode_int_sort_key(const uint8_t *src, uint sort_len, bool is_signed, uchar *to);

    /* Extended sort-key decoder -- handles integers, DATE, DATETIME,
       TIMESTAMP, YEAR, and fixed-length CHAR/BINARY.  Returns true on
       success, false for unsupported types.  Used by covering index
       reads and ICP evaluation to avoid PK point-lookups. */
    static bool decode_sort_key_part(const uint8_t *src, uint sort_len, Field *f, uchar *buf);

    /* Free all cached dup-check iterators */
    void free_dup_iter_cache();

    /* Commit the current txn mid-statement when a bulk op crosses the batch
       threshold, then reset it to READ_COMMITTED for the next batch.  Shared
       between bulk INSERT/UPDATE/DELETE.  Invalidates cached iterators.
       Returns 0 on success, handler error code on fatal failure. */
    int maybe_bulk_commit(tidesdb_trx_t *trx);

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
               HA_CAN_TABLES_WITHOUT_ROLLBACK | HA_CAN_FULLTEXT | HA_CAN_FULLTEXT_EXT |
               HA_CAN_GEOMETRY | HA_CAN_RTREEKEYS | HA_CAN_EXPORT;
    }

    ulong index_flags(uint idx, uint part, bool all_parts) const override;

    const char *index_type(uint key_number) override;

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

    IO_AND_CPU_COST keyread_time(uint index, ulong ranges, ha_rows rows, ulonglong blocks) override
    {
        /* Index read -- each point lookup touches read_amp levels.
           Range scans amortize the merge-heap cost across rows. */
        IO_AND_CPU_COST cost;
        cost.io = 0;
        double amp = share ? share->cached_read_amp.load(std::memory_order_relaxed)
                           : TIDESDB_DEFAULT_READ_AMP;
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
        double amp = share ? share->cached_read_amp.load(std::memory_order_relaxed)
                           : TIDESDB_DEFAULT_READ_AMP;
        cost.cpu = (double)rows * TIDESDB_COST_SEQ_READ * amp;
        return cost;
    }

    /* Convert a MariaDB table path to a TidesDB column family name */
    static std::string path_to_cf_name(const char *path);

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

    /* Full-text search */
    int ft_init() override;
    void ft_end() override;
    FT_INFO *ft_init_ext(uint flags, uint inx, String *key) override;
    int ft_read(uchar *buf) override;

    /* Bulk insert hint (LOAD DATA, multi-row INSERT) */
    void start_bulk_insert(ha_rows rows, uint flags) override;
    int end_bulk_insert() override;

    /* Bulk UPDATE / DELETE hints -- let multi-row UPDATE/DELETE share the
       same mid-txn commit batching as bulk INSERT so long statements don't
       blow past TDB_MAX_TXN_OPS or balloon txn memory. */
    bool start_bulk_update() override;
    int end_bulk_update() override;
    int bulk_update_row(const uchar *old_data, const uchar *new_data,
                        ha_rows *dup_key_found) override;
    bool start_bulk_delete() override;
    int end_bulk_delete() override;

    /* Index Condition Pushdown (ICP) */
    Item *idx_cond_push(uint keyno, Item *idx_cond) override;

    /* Multi-Range Read (MRR).  We opt into a custom implementation for
       point-only range sequences and defer to the base handler for
       everything else by leaving HA_MRR_USE_DEFAULT_IMPL set. */
    ha_rows multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq, void *seq_init_param,
                                        uint n_ranges, uint *bufsz, uint *mrr_mode, ha_rows limit,
                                        Cost_estimate *cost) override;
    int multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param, uint n_ranges, uint mrr_mode,
                              HANDLER_BUFFER *buf) override;
    int multi_range_read_next(range_id_t *range_info) override;

    /* AUTO_INCREMENT -- O(1) atomic counter */
    void get_auto_increment(ulonglong offset, ulonglong increment, ulonglong nb_desired_values,
                            ulonglong *first_value, ulonglong *nb_reserved_values) override;

    /* Reset the in-memory auto-increment counter so `TRUNCATE TABLE t` and
       `ALTER TABLE t AUTO_INCREMENT=N` take effect.  Base default is a no-op,
       which left TidesDB's cached counter running past TRUNCATE -- the next
       INSERT would return a stale value instead of restarting at 1 (or N). */
    int reset_auto_increment(ulonglong value) override;

    /* Stats / Maintenance */
    int info(uint flag) override;
    int analyze(THD *thd, HA_CHECK_OPT *check_opt) override;
    int optimize(THD *thd, HA_CHECK_OPT *check_opt) override;
    int check(THD *thd, HA_CHECK_OPT *check_opt) override;
    int repair(THD *thd, HA_CHECK_OPT *check_opt) override;
    ha_rows records_in_range(uint inx, const key_range *min_key, const key_range *max_key,
                             page_range *pages) override;
    int extra(enum ha_extra_function operation) override;

    /* Semi-consistent read for UPDATE/DELETE optimization */
    bool was_semi_consistent_read() override;
    void try_semi_consistent_read(bool yes) override;

   private:
    bool semi_consistent_read_{false};     /* try optimistic reads on locked rows */
    bool did_semi_consistent_read_{false}; /* last read was optimistic */

   public:
   protected:
    IO_AND_CPU_COST scan_time() override;

   public:
    /* Locking -- TidesDB handles concurrency via MVCC internally.
       lock_count()=0 bypasses MariaDB's THR_LOCK. */
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
