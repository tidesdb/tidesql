/*
 * TideSQL - Part of TidesDB
 * Copyright (C) 2025 Alex Gaetano Padula
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
#ifndef TIDESQL_HA_TIDESQL_HPP
#define TIDESQL_HA_TIDESQL_HPP

#ifdef USE_PRAGMA_INTERFACE
#pragma interface
#endif

#include <mysql/plugin.h>
#include <sql/field.h>
#include <sql/handler.h>
#include <sql/sql_class.h>
#include <sql/table.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "kv_store.hpp"
#include "kv_tidesdb.hpp"

namespace tidesql
{

/*
 * BM25 Fulltext Index Implementation
 *
 * Storage format in 'fulltext' column family:
 * - "ft:df:<term>" -> document frequency (uint64_t)
 * - "ft:tf:<term>:<row_id>" -> term frequency (uint32_t)
 * - "ft:dl:<row_id>" -> document length (uint32_t)
 * - "ft:total_docs" -> total document count (uint64_t)
 * - "ft:avg_dl" -> average document length (double)
 * - "ft:idx:<idx_no>:<row_id>" -> list of terms for this doc (for deletion)
 */
class BM25Index
{
   public:
    /* BM25 parameters */
    static constexpr double k1 = 1.2; /* Term frequency saturation */
    static constexpr double b = 0.75; /* Length normalization */

    /* Tokenize text into terms */
    static std::vector<std::string> tokenize(const std::string& text)
    {
        std::vector<std::string> tokens;
        std::string current;

        for (char c : text)
        {
            if (std::isalnum(static_cast<unsigned char>(c)))
            {
                current += std::tolower(static_cast<unsigned char>(c));
            }
            else if (!current.empty())
            {
                if (current.length() >= 2)
                { /* Skip single chars */
                    tokens.push_back(current);
                }
                current.clear();
            }
        }
        if (!current.empty() && current.length() >= 2)
        {
            tokens.push_back(current);
        }

        return tokens;
    }

    /* Calculate BM25 score for a single term */
    static double score_term(uint32_t tf, uint64_t df, uint64_t total_docs, uint32_t doc_len,
                             double avg_doc_len)
    {
        if (df == 0 || total_docs == 0) return 0.0;

        /* IDF component */
        double idf = std::log((total_docs - df + 0.5) / (df + 0.5) + 1.0);

        /* TF component with length normalization */
        double tf_norm = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (doc_len / avg_doc_len)));

        return idf * tf_norm;
    }
};

/* Forward declarations */
class TideSQLShare;
class TideSQLHandler;

/*
 * Per-connection transaction context
 * Stored in THD's ha_data slot for this storage engine
 */
struct TideSQLTrx
{
    std::unique_ptr<KVTransaction> txn; /* Active transaction (if any) */
    bool started;                       /* Transaction has been started */
    bool autocommit;                    /* Autocommit mode */
    int isolation_level;                /* Per-transaction isolation level (from THD) */

    TideSQLTrx() : started(false), autocommit(true), isolation_level(1)
    {
    }
};

/*
 * Shared table data structure
 * One instance per table, shared across all handler instances
 */
class TideSQLShare
{
   public:
    TideSQLShare(const char* table_name);
    ~TideSQLShare();

    /* Reference counting */
    void inc_ref()
    {
        ++ref_count_;
    }
    void dec_ref()
    {
        --ref_count_;
    }
    int ref_count() const
    {
        return ref_count_;
    }

    /* Table metadata */
    const std::string& table_name() const
    {
        return table_name_;
    }

    /* KV store access */
    KVStore* kv_store()
    {
        return kv_store_.get();
    }

    /* Row counter for table_scan */
    uint64_t next_row_id()
    {
        return ++row_id_counter_;
    }
    uint64_t current_row_id() const
    {
        return row_id_counter_;
    }
    void set_row_id(uint64_t id)
    {
        row_id_counter_ = id;
    }

    /* Auto-increment support (shared across all handlers for this table) */
    ulonglong next_auto_inc(ulonglong increment, ulonglong offset);
    ulonglong current_auto_inc() const
    {
        return auto_inc_value_.load();
    }
    void set_auto_inc(ulonglong val)
    {
        auto_inc_value_.store(val);
    }

    /* Lock for concurrent access */
    std::mutex& mutex()
    {
        return mutex_;
    }

   private:
    std::string table_name_;
    std::unique_ptr<KVStore> kv_store_;
    uint64_t row_id_counter_;
    std::atomic<ulonglong> auto_inc_value_{0};
    int ref_count_;
    std::mutex mutex_;
};

/*
 * Global share manager
 * Manages TideSQLShare instances across all tables
 */
class TideSQLShareManager
{
   public:
    static TideSQLShareManager& instance()
    {
        static TideSQLShareManager mgr;
        return mgr;
    }

    TideSQLShare* get_share(const char* table_name);
    void release_share(TideSQLShare* share);

   private:
    TideSQLShareManager() = default;
    std::unordered_map<std::string, std::unique_ptr<TideSQLShare>> shares_;
    std::mutex mutex_;
};

/*
 * TideSQL Handler - Main storage engine handler class
 *
 * This class implements the MySQL handler interface.
 * Each open table gets its own handler instance.
 */
class TideSQLHandler : public handler
{
   public:
    TideSQLHandler(handlerton* hton, TABLE_SHARE* table_share);
    ~TideSQLHandler() override;

    /* Handler name */
    const char* table_type() const override
    {
        return "TIDESQL";
    }

    /* Index type for SHOW CREATE TABLE */
    const char* index_type(uint key_number) override
    {
        return "BTREE";
    }

    /* Table flags - capabilities of this storage engine */
    ulonglong table_flags() const override
    {
        return HA_REC_NOT_IN_SEQ |         /* Records not in sequential order */
               HA_NULL_IN_KEY |            /* NULLs allowed in keys */
               HA_CAN_INDEX_BLOBS |        /* Can index blob columns */
               HA_STATS_RECORDS_IS_EXACT | /* Exact record count */
               HA_HAS_OWN_BINLOGGING |     /* Has own binlog handling */
               HA_CAN_FULLTEXT;            /* Fulltext index support (BM25) */
    }

    /* Index flags */
    ulong index_flags(uint idx, uint part, bool all_parts) const override
    {
        return HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE | HA_KEYREAD_ONLY;
    }

    /* Maximum supported keys and key parts */
    uint max_supported_keys() const override
    {
        return MAX_KEY;
    }
    uint max_supported_key_parts() const override
    {
        return MAX_REF_PARTS;
    }
    uint max_supported_key_length() const override
    {
        return 3072;
    }
    uint max_supported_key_part_length() const override
    {
        return 3072;
    }

    /* Foreign key support */
    bool can_switch_engines() override
    {
        return true;
    }

    /* Check if referenced key exists (for FK constraint) */
    int check_if_supported_inplace_alter(TABLE* altered_table,
                                         Alter_inplace_info* ha_alter_info) override
    {
        return HA_ALTER_INPLACE_NOT_SUPPORTED;
    }

    /* Get foreign key list for this table */
    char* get_foreign_key_create_info() override;

    /* Free FK create info */
    void free_foreign_key_create_info(char* str) override;

    /* Get referenced table list */
    int get_foreign_key_list(THD* thd, List<FOREIGN_KEY_INFO>* f_key_list) override;

    /* Get parent table list (tables that reference this table) */
    int get_parent_foreign_key_list(THD* thd, List<FOREIGN_KEY_INFO>* f_key_list) override;

    /* Check if FK constraints are satisfied */
    uint referenced_by_foreign_key() override;

    /* Initialize fulltext index scan */
    int ft_init() override;

    /* Read next fulltext match */
    int ft_read(uchar* buf) override;

    /* Initialize fulltext extension (for MATCH...AGAINST) */
    FT_INFO* ft_init_ext(uint flags, uint inx, String* key) override;

    /* Create a new table */
    int create(const char* name, TABLE* form, HA_CREATE_INFO* create_info) override;

    /* Open an existing table */
    int open(const char* name, int mode, uint test_if_locked) override;

    /* Close the table */
    int close() override;

    /* Write a row */
    int write_row(uchar* buf) override;

    /* Update a row */
    int update_row(const uchar* old_data, uchar* new_data) override;

    /* Delete a row */
    int delete_row(const uchar* buf) override;

    /* Start a full table scan */
    int rnd_init(bool scan) override;

    /* End table scan */
    int rnd_end() override;

    /* Get next row in table scan */
    int rnd_next(uchar* buf) override;

    /* Get row by position */
    int rnd_pos(uchar* buf, uchar* pos) override;

    /* Store current position */
    void position(const uchar* record) override;

    /* Initialize index scan */
    int index_init(uint idx, bool sorted) override;

    /* End index scan */
    int index_end() override;

    /* Read first matching row */
    int index_read_map(uchar* buf, const uchar* key, key_part_map keypart_map,
                       enum ha_rkey_function find_flag) override;

    /* Read next row with same key */
    int index_next_same(uchar* buf, const uchar* key, uint keylen) override;

    /* Read next row */
    int index_next(uchar* buf) override;

    /* Read previous row */
    int index_prev(uchar* buf) override;

    /* Read first row */
    int index_first(uchar* buf) override;

    /* Read last row */
    int index_last(uchar* buf) override;

    /* Get table info for optimizer */
    int info(uint flag) override;

    /* Estimate rows in range */
    ha_rows records_in_range(uint inx, key_range* min_key, key_range* max_key) override;

    /* External lock (called at statement start/end) */
    int external_lock(THD* thd, int lock_type) override;

    /* Store lock */
    THR_LOCK_DATA** store_lock(THD* thd, THR_LOCK_DATA** to, enum thr_lock_type lock_type) override;

    /* Delete table */
    int delete_table(const char* name) override;

    /* Rename table */
    int rename_table(const char* from, const char* to) override;

    /* Truncate table */
    int truncate() override;

   private:
    /* Shared table data */
    TideSQLShare* share_;

    /* Thread lock data */
    THR_LOCK lock_;
    THR_LOCK_DATA lock_data_;

    /* Current position in table scan */
    std::unique_ptr<KVIterator> scan_iterator_;
    bool scan_initialized_;

    /* Current index being used */
    uint active_index_;

    /* Buffer for current row key */
    std::string current_key_;

    /* Index search state for index_next_same */
    std::string index_search_key_;
    uint index_search_key_len_;

    /* Column families (resolved at open) */
    KVColumnFamily* rows_cf_;
    KVColumnFamily* index_cf_;
    KVColumnFamily* fulltext_cf_;

    /* Current fulltext search results (row_id -> BM25 score) */
    std::vector<std::pair<uint64_t, double>> ft_results_;
    size_t ft_current_pos_;
    bool ft_initialized_;

    /* Get auto-increment info */
    void get_auto_increment(ulonglong offset, ulonglong increment, ulonglong nb_desired_values,
                            ulonglong* first_value, ulonglong* nb_reserved_values) override;

    /* Helper methods */
    std::string make_row_key(uint64_t row_id);
    std::string make_index_key(uint idx, const uchar* key, uint key_len);
    std::string build_index_key_from_record(uint idx, const uchar* record);
    int pack_row(uchar* buf, std::string* packed);
    int unpack_row(const std::string& packed, uchar* buf);
    uint64_t extract_row_id(const std::string& key);

    /* Index management */
    int update_indexes(uint64_t row_id, const uchar* old_record, const uchar* new_record);
    int delete_indexes(uint64_t row_id, const uchar* record);

    /* Fulltext index management */
    int update_fulltext_index(uint64_t row_id, const uchar* old_record, const uchar* new_record);
    int delete_fulltext_index(uint64_t row_id, const uchar* record);
    std::string get_field_text(const uchar* record, Field* field);
};

} /* namespace tidesql */

/* C linkage for MySQL plugin API */
extern "C"
{
    /* Plugin initialization */
    int tidesql_init(void* p);

    /* Plugin deinitialization */
    int tidesql_deinit(void* p);

    /* Handler creation */
    handler* tidesql_create_handler(handlerton* hton, TABLE_SHARE* table, MEM_ROOT* mem_root);

    /* Transaction callbacks */
    int tidesql_commit(handlerton* hton, THD* thd, bool all);
    int tidesql_rollback(handlerton* hton, THD* thd, bool all);

    /* Get/create transaction context for a THD */
    tidesql::TideSQLTrx* get_or_create_trx(THD* thd, handlerton* hton);

} /* extern "C" */

#endif /* TIDESQL_HA_TIDESQL_HPP */
