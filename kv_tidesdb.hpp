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
#ifndef TIDESQL_KV_TIDESDB_HPP
#define TIDESQL_KV_TIDESDB_HPP

#include <tidesdb/tidesdb.h>

#include <atomic>
#include <cstring>
#include <utility>
#include <vector>

#include "kv_store.hpp"

namespace tidesql
{

/* TidesDB Column Family wrapper */
class TidesDBColumnFamily : public KVColumnFamily
{
   public:
    TidesDBColumnFamily(std::string name, tidesdb_column_family_t* cf)
        : name_(std::move(name)), cf_(cf)
    {
    }

    const std::string& name() const override
    {
        return name_;
    }
    tidesdb_column_family_t* native() const
    {
        return cf_;
    }
    tidesdb_column_family_t* native_cf() const
    {
        return cf_;
    } /* Alias for update_cf_config */

   private:
    std::string name_;
    tidesdb_column_family_t* cf_;
};

/* TidesDB Iterator wrapper */
class TidesDBIterator : public KVIterator
{
   public:
    TidesDBIterator(tidesdb_t* db, tidesdb_column_family_t* cf, tidesdb_isolation_level_t isolation)
        : db_(db), cf_(cf), txn_(nullptr), iter_(nullptr), valid_(false), isolation_(isolation)
    {
        /* Create a read transaction for iteration */
        if (tidesdb_txn_begin_with_isolation(db_, isolation_, &txn_) == TDB_SUCCESS)
        {
            tidesdb_iter_new(txn_, cf_, &iter_);
        }
    }

    ~TidesDBIterator() override
    {
        if (iter_)
        {
            tidesdb_iter_free(iter_);
        }
        if (txn_)
        {
            /* Read-only transaction - rollback is safe and releases resources */
            tidesdb_txn_rollback(txn_);
            tidesdb_txn_free(txn_);
        }
    }

    void seek(const std::string& target) override
    {
        if (!iter_) return;
        valid_ = (tidesdb_iter_seek(iter_, reinterpret_cast<const uint8_t*>(target.data()),
                                    target.size()) == TDB_SUCCESS);
        if (valid_)
        {
            valid_ = tidesdb_iter_valid(iter_);
        }
    }

    void seek_to_first() override
    {
        if (!iter_) return;
        tidesdb_iter_seek_to_first(iter_);
        valid_ = tidesdb_iter_valid(iter_);
    }

    void seek_to_last() override
    {
        if (!iter_) return;
        tidesdb_iter_seek_to_last(iter_);
        valid_ = tidesdb_iter_valid(iter_);
    }

    void next() override
    {
        if (!iter_ || !valid_) return;
        if (tidesdb_iter_next(iter_) == TDB_SUCCESS)
        {
            valid_ = tidesdb_iter_valid(iter_);
        }
        else
        {
            valid_ = false;
        }
    }

    void prev() override
    {
        if (!iter_ || !valid_) return;
        if (tidesdb_iter_prev(iter_) == TDB_SUCCESS)
        {
            valid_ = tidesdb_iter_valid(iter_);
        }
        else
        {
            valid_ = false;
        }
    }

    bool valid() const override
    {
        return valid_ && iter_ && tidesdb_iter_valid(iter_);
    }

    std::string key() const override
    {
        if (!valid_ || !iter_) return std::string();

        uint8_t* key_data = nullptr;
        size_t key_size = 0;
        if (tidesdb_iter_key(iter_, &key_data, &key_size) == TDB_SUCCESS)
        {
            std::string result(reinterpret_cast<char*>(key_data), key_size);
            free(key_data); /* TidesDB allocates memory that caller must free */
            return result;
        }
        return std::string();
    }

    std::string value() const override
    {
        if (!valid_ || !iter_) return std::string();

        uint8_t* value_data = nullptr;
        size_t value_size = 0;
        if (tidesdb_iter_value(iter_, &value_data, &value_size) == TDB_SUCCESS)
        {
            std::string result(reinterpret_cast<char*>(value_data), value_size);
            free(value_data); /* TidesDB allocates memory that caller must free */
            return result;
        }
        return std::string();
    }

    KVStatus status() const override
    {
        return KVStatus::OK;
    }

   private:
    tidesdb_t* db_;
    tidesdb_column_family_t* cf_;
    tidesdb_txn_t* txn_;
    tidesdb_iter_t* iter_;
    bool valid_;
    tidesdb_isolation_level_t isolation_;
};

/* TidesDB WriteBatch - accumulates operations for atomic commit */
class TidesDBWriteBatch : public KVWriteBatch
{
   public:
    struct Operation
    {
        enum Type
        {
            PUT,
            DELETE
        };
        Type type;
        std::string key;
        std::string value;
        time_t ttl;
    };

    void put(const std::string& key, const std::string& value) override
    {
        operations_.push_back({Operation::PUT, key, value, -1});
    }

    void put_with_ttl(const std::string& key, const std::string& value, time_t ttl)
    {
        operations_.push_back({Operation::PUT, key, value, ttl});
    }

    void remove(const std::string& key) override
    {
        operations_.push_back({Operation::DELETE, key, std::string(), -1});
    }

    void clear() override
    {
        operations_.clear();
    }

    size_t count() const override
    {
        return operations_.size();
    }

    const std::vector<Operation>& operations() const
    {
        return operations_;
    }

   private:
    std::vector<Operation> operations_;
};

/* TidesDB Transaction wrapper */
class TidesDBTransaction : public KVTransaction
{
   public:
    TidesDBTransaction(tidesdb_t* db, tidesdb_column_family_t* cf,
                       tidesdb_isolation_level_t isolation)
        : db_(db), cf_(cf), txn_(nullptr), committed_(false), isolation_(isolation)
    {
        tidesdb_txn_begin_with_isolation(db_, isolation_, &txn_);
    }

    ~TidesDBTransaction() override
    {
        if (txn_ && !committed_)
        {
            tidesdb_txn_rollback(txn_);
            tidesdb_txn_free(txn_);
        }
    }

    KVResult get(const std::string& key, std::string* value) override
    {
        if (!txn_) return KVResult(KVStatus::IO_ERROR, "Transaction not started");

        uint8_t* val_data = nullptr;
        size_t val_size = 0;

        int rc = tidesdb_txn_get(txn_, cf_, reinterpret_cast<const uint8_t*>(key.data()),
                                 key.size(), &val_data, &val_size);

        if (rc == TDB_ERR_NOT_FOUND)
        {
            return KVResult(KVStatus::NOT_FOUND);
        }
        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Get failed");
        }

        value->assign(reinterpret_cast<char*>(val_data), val_size);
        free(val_data);
        return KVResult(KVStatus::OK);
    }

    KVResult put(const std::string& key, const std::string& value) override
    {
        if (!txn_) return KVResult(KVStatus::IO_ERROR, "Transaction not started");

        int rc = tidesdb_txn_put(txn_, cf_, reinterpret_cast<const uint8_t*>(key.data()),
                                 key.size(), reinterpret_cast<const uint8_t*>(value.data()),
                                 value.size(), -1); /* No TTL */

        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Put failed");
        }
        return KVResult(KVStatus::OK);
    }

    KVResult remove(const std::string& key) override
    {
        if (!txn_) return KVResult(KVStatus::IO_ERROR, "Transaction not started");

        int rc =
            tidesdb_txn_delete(txn_, cf_, reinterpret_cast<const uint8_t*>(key.data()), key.size());

        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Delete failed");
        }
        return KVResult(KVStatus::OK);
    }

    KVResult commit() override
    {
        if (!txn_) return KVResult(KVStatus::IO_ERROR, "Transaction not started");

        int rc = tidesdb_txn_commit(txn_);
        if (rc == TDB_ERR_CONFLICT)
        {
            return KVResult(KVStatus::BUSY, "Transaction conflict");
        }
        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Commit failed");
        }

        committed_ = true;
        tidesdb_txn_free(txn_);
        txn_ = nullptr;
        return KVResult(KVStatus::OK);
    }

    KVResult rollback() override
    {
        if (!txn_) return KVResult(KVStatus::IO_ERROR, "Transaction not started");

        tidesdb_txn_rollback(txn_);
        tidesdb_txn_free(txn_);
        txn_ = nullptr;
        committed_ = true; /* Prevent double-free in destructor */
        return KVResult(KVStatus::OK);
    }

   private:
    tidesdb_t* db_;
    tidesdb_column_family_t* cf_;
    tidesdb_txn_t* txn_;
    bool committed_;
    tidesdb_isolation_level_t isolation_;
};

/* TidesDB KV Store implementation */
class TidesDBKVStore : public KVStore
{
   public:
    TidesDBKVStore()
        : db_(nullptr),
          cf_rows_(nullptr),
          cf_index_(nullptr),
          cf_meta_(nullptr),
          cf_fulltext_(nullptr),
          open_(false)
    {
    }

    ~TidesDBKVStore() override
    {
        close();
    }

    KVResult open(const KVOptions& options) override
    {
        if (open_)
        {
            return KVResult(KVStatus::ALREADY_EXISTS, "Already open");
        }

        options_ = options;

        switch (options.txn_isolation)
        {
            case 0:
                isolation_level_ = TDB_ISOLATION_READ_UNCOMMITTED;
                break;
            case 1:
                isolation_level_ = TDB_ISOLATION_READ_COMMITTED;
                break;
            case 2:
                isolation_level_ = TDB_ISOLATION_REPEATABLE_READ;
                break;
            case 3:
                isolation_level_ = TDB_ISOLATION_SNAPSHOT;
                break;
            case 4:
                isolation_level_ = TDB_ISOLATION_SERIALIZABLE;
                break;
            default:
                isolation_level_ = TDB_ISOLATION_READ_COMMITTED;
                break;
        }

        /* Map log level from options */
        tidesdb_log_level_t tdb_log_level;
        switch (options.log_level)
        {
            case 0:
                tdb_log_level = TDB_LOG_DEBUG;
                break;
            case 1:
                tdb_log_level = TDB_LOG_INFO;
                break;
            case 2:
                tdb_log_level = TDB_LOG_WARN;
                break;
            case 3:
                tdb_log_level = TDB_LOG_ERROR;
                break;
            case 4:
                tdb_log_level = TDB_LOG_FATAL;
                break;
            case 5:
                tdb_log_level = TDB_LOG_NONE;
                break;
            default:
                tdb_log_level = TDB_LOG_INFO;
                break;
        }

        /* Configure TidesDB */
        tidesdb_config_t config = {
            .db_path = nullptr,
            .num_flush_threads = options.flush_threads > 0 ? options.flush_threads : 2,
            .num_compaction_threads =
                options.compaction_threads > 0 ? options.compaction_threads : 2,
            .log_level = tdb_log_level,
            .block_cache_size = options.block_cache_size,
            .max_open_sstables =
                static_cast<int>(options.max_open_files > 0 ? options.max_open_files : 256)};

        /* Copy path - TidesDB needs a mutable string */
        data_path_ = options.data_dir.empty() ? "./tidesql_data" : options.data_dir;
        config.db_path = const_cast<char*>(data_path_.c_str());

        /* Open TidesDB */
        int rc = tidesdb_open(&config, &db_);
        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Failed to open TidesDB");
        }

        tidesdb_column_family_config_t base_cf_config = tidesdb_default_column_family_config();
        base_cf_config.write_buffer_size = options.write_buffer_size;

        if (options.enable_compression)
        {
            if (options.compression_type == "zstd")
            {
                base_cf_config.compression_algorithm = ZSTD_COMPRESSION;
            }
            else if (options.compression_type == "snappy")
            {
                base_cf_config.compression_algorithm = SNAPPY_COMPRESSION;
            }
            else
            {
                base_cf_config.compression_algorithm = LZ4_COMPRESSION; /* Default */
            }
        }
        else
        {
            base_cf_config.compression_algorithm = NO_COMPRESSION;
        }

        base_cf_config.enable_bloom_filter = options.enable_bloom_filter ? 1 : 0;
        base_cf_config.bloom_fpr = options.bloom_fpr;
        base_cf_config.enable_block_indexes = options.enable_block_indexes ? 1 : 0;

        /* Map sync mode from options */
        switch (options.sync_mode)
        {
            case 0:
                base_cf_config.sync_mode = TDB_SYNC_NONE;
                break;
            case 1:
                base_cf_config.sync_mode = TDB_SYNC_INTERVAL;
                break;
            case 2:
                base_cf_config.sync_mode = TDB_SYNC_FULL;
                break;
            default:
                base_cf_config.sync_mode = TDB_SYNC_INTERVAL;
                break;
        }
        base_cf_config.sync_interval_us = options.sync_interval_us;

        auto ensure_cf = [&](const char* name, tidesdb_column_family_t** out) -> KVResult
        {
            tidesdb_column_family_t* existing = tidesdb_get_column_family(db_, name);
            if (existing)
            {
                *out = existing;
                return KVResult(KVStatus::OK);
            }

            int cfrc = tidesdb_create_column_family(db_, name, &base_cf_config);
            if (cfrc != TDB_SUCCESS && cfrc != TDB_ERR_EXISTS)
            {
                return KVResult(KVStatus::IO_ERROR, "Failed to create column family");
            }

            existing = tidesdb_get_column_family(db_, name);
            if (!existing)
            {
                return KVResult(KVStatus::IO_ERROR, "Failed to get column family");
            }
            *out = existing;
            return KVResult(KVStatus::OK);
        };

        KVResult cfres = ensure_cf("rows", &cf_rows_);
        if (!cfres.ok())
        {
            tidesdb_close(db_);
            db_ = nullptr;
            return cfres;
        }
        cfres = ensure_cf("index", &cf_index_);
        if (!cfres.ok())
        {
            tidesdb_close(db_);
            db_ = nullptr;
            return cfres;
        }
        cfres = ensure_cf("meta", &cf_meta_);
        if (!cfres.ok())
        {
            tidesdb_close(db_);
            db_ = nullptr;
            return cfres;
        }
        cfres = ensure_cf("fulltext", &cf_fulltext_);
        if (!cfres.ok())
        {
            tidesdb_close(db_);
            db_ = nullptr;
            return cfres;
        }

        rows_cf_handle_ = std::make_unique<TidesDBColumnFamily>("rows", cf_rows_);
        index_cf_handle_ = std::make_unique<TidesDBColumnFamily>("index", cf_index_);
        meta_cf_handle_ = std::make_unique<TidesDBColumnFamily>("meta", cf_meta_);
        fulltext_cf_handle_ = std::make_unique<TidesDBColumnFamily>("fulltext", cf_fulltext_);

        open_ = true;
        return KVResult(KVStatus::OK);
    }

    KVResult close() override
    {
        if (!open_ || !db_)
        {
            return KVResult(KVStatus::OK);
        }

        tidesdb_close(db_);
        db_ = nullptr;
        cf_rows_ = nullptr;
        cf_index_ = nullptr;
        cf_meta_ = nullptr;
        cf_fulltext_ = nullptr;
        rows_cf_handle_.reset();
        index_cf_handle_.reset();
        meta_cf_handle_.reset();
        fulltext_cf_handle_.reset();
        created_cf_handles_.clear();
        open_ = false;
        return KVResult(KVStatus::OK);
    }

    bool is_open() const override
    {
        return open_;
    }

    KVResult get(const std::string& key, std::string* value) override
    {
        if (!open_ || !db_)
        {
            return KVResult(KVStatus::IO_ERROR, "Store not open");
        }

        tidesdb_column_family_t* cf = choose_cf_for_key(key);
        if (!cf)
        {
            return KVResult(KVStatus::IO_ERROR, "Column family not available");
        }

        tidesdb_txn_t* txn = nullptr;
        if (tidesdb_txn_begin_with_isolation(db_, isolation_level_, &txn) != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Failed to begin transaction");
        }

        uint8_t* val_data = nullptr;
        size_t val_size = 0;

        int rc = tidesdb_txn_get(txn, cf, reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                                 &val_data, &val_size);

        tidesdb_txn_free(txn);

        if (rc == TDB_ERR_NOT_FOUND)
        {
            return KVResult(KVStatus::NOT_FOUND);
        }
        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Get failed");
        }

        value->assign(reinterpret_cast<char*>(val_data), val_size);
        free(val_data);
        return KVResult(KVStatus::OK);
    }

    KVResult put(const std::string& key, const std::string& value) override
    {
        if (!open_ || !db_)
        {
            return KVResult(KVStatus::IO_ERROR, "Store not open");
        }

        tidesdb_column_family_t* cf = choose_cf_for_key(key);
        if (!cf)
        {
            return KVResult(KVStatus::IO_ERROR, "Column family not available");
        }

        tidesdb_txn_t* txn = nullptr;
        if (tidesdb_txn_begin_with_isolation(db_, isolation_level_, &txn) != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Failed to begin transaction");
        }

        int rc = tidesdb_txn_put(txn, cf, reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                                 reinterpret_cast<const uint8_t*>(value.data()), value.size(),
                                 -1); /* No TTL */

        if (rc != TDB_SUCCESS)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
            return KVResult(KVStatus::IO_ERROR, "Put failed");
        }

        rc = tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);

        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Commit failed");
        }

        return KVResult(KVStatus::OK);
    }

    KVResult remove(const std::string& key) override
    {
        if (!open_ || !db_)
        {
            return KVResult(KVStatus::IO_ERROR, "Store not open");
        }

        tidesdb_column_family_t* cf = choose_cf_for_key(key);
        if (!cf)
        {
            return KVResult(KVStatus::IO_ERROR, "Column family not available");
        }

        tidesdb_txn_t* txn = nullptr;
        if (tidesdb_txn_begin_with_isolation(db_, isolation_level_, &txn) != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Failed to begin transaction");
        }

        int rc =
            tidesdb_txn_delete(txn, cf, reinterpret_cast<const uint8_t*>(key.data()), key.size());

        if (rc != TDB_SUCCESS)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
            return KVResult(KVStatus::IO_ERROR, "Delete failed");
        }

        rc = tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);

        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Commit failed");
        }

        return KVResult(KVStatus::OK);
    }

    KVResult exists(const std::string& key, bool* result) override
    {
        std::string value;
        KVResult res = get(key, &value);
        *result = res.ok();
        return KVResult(KVStatus::OK);
    }

    KVResult get(KVColumnFamily* cf, const std::string& key, std::string* value) override
    {
        if (!cf)
        {
            return get(key, value);
        }
        if (!open_ || !db_)
        {
            return KVResult(KVStatus::IO_ERROR, "Store not open");
        }

        auto* tcf = dynamic_cast<TidesDBColumnFamily*>(cf);
        if (!tcf || !tcf->native())
        {
            return KVResult(KVStatus::INVALID_ARGUMENT, "Invalid column family");
        }

        tidesdb_txn_t* txn = nullptr;
        if (tidesdb_txn_begin_with_isolation(db_, isolation_level_, &txn) != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Failed to begin transaction");
        }

        uint8_t* val_data = nullptr;
        size_t val_size = 0;
        int rc = tidesdb_txn_get(txn, tcf->native(), reinterpret_cast<const uint8_t*>(key.data()),
                                 key.size(), &val_data, &val_size);
        tidesdb_txn_free(txn);

        if (rc == TDB_ERR_NOT_FOUND)
        {
            return KVResult(KVStatus::NOT_FOUND);
        }
        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Get failed");
        }
        value->assign(reinterpret_cast<char*>(val_data), val_size);
        free(val_data);
        return KVResult(KVStatus::OK);
    }

    KVResult put(KVColumnFamily* cf, const std::string& key, const std::string& value) override
    {
        if (!cf)
        {
            return put(key, value);
        }
        if (!open_ || !db_)
        {
            return KVResult(KVStatus::IO_ERROR, "Store not open");
        }
        auto* tcf = dynamic_cast<TidesDBColumnFamily*>(cf);
        if (!tcf || !tcf->native())
        {
            return KVResult(KVStatus::INVALID_ARGUMENT, "Invalid column family");
        }

        tidesdb_txn_t* txn = nullptr;
        if (tidesdb_txn_begin_with_isolation(db_, isolation_level_, &txn) != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Failed to begin transaction");
        }

        int rc = tidesdb_txn_put(txn, tcf->native(), reinterpret_cast<const uint8_t*>(key.data()),
                                 key.size(), reinterpret_cast<const uint8_t*>(value.data()),
                                 value.size(), -1);
        if (rc != TDB_SUCCESS)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
            return KVResult(KVStatus::IO_ERROR, "Put failed");
        }
        rc = tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);
        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Commit failed");
        }
        return KVResult(KVStatus::OK);
    }

    std::unique_ptr<KVWriteBatch> create_write_batch() override
    {
        return std::make_unique<TidesDBWriteBatch>();
    }

    KVResult write(KVWriteBatch* batch) override
    {
        if (!open_ || !db_)
        {
            return KVResult(KVStatus::IO_ERROR, "Store not open");
        }

        auto* tdb_batch = dynamic_cast<TidesDBWriteBatch*>(batch);
        if (!tdb_batch)
        {
            return KVResult(KVStatus::INVALID_ARGUMENT, "Invalid batch type");
        }

        tidesdb_txn_t* txn = nullptr;
        if (tidesdb_txn_begin_with_isolation(db_, isolation_level_, &txn) != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Failed to begin transaction");
        }

        for (const auto& op : tdb_batch->operations())
        {
            int rc;
            if (op.type == TidesDBWriteBatch::Operation::PUT)
            {
                tidesdb_column_family_t* cf = choose_cf_for_key(op.key);
                if (!cf)
                {
                    tidesdb_txn_rollback(txn);
                    tidesdb_txn_free(txn);
                    return KVResult(KVStatus::IO_ERROR, "Column family not available");
                }
                rc = tidesdb_txn_put(
                    txn, cf, reinterpret_cast<const uint8_t*>(op.key.data()), op.key.size(),
                    reinterpret_cast<const uint8_t*>(op.value.data()), op.value.size(), op.ttl);
            }
            else
            {
                tidesdb_column_family_t* cf = choose_cf_for_key(op.key);
                if (!cf)
                {
                    tidesdb_txn_rollback(txn);
                    tidesdb_txn_free(txn);
                    return KVResult(KVStatus::IO_ERROR, "Column family not available");
                }
                rc = tidesdb_txn_delete(txn, cf, reinterpret_cast<const uint8_t*>(op.key.data()),
                                        op.key.size());
            }

            if (rc != TDB_SUCCESS)
            {
                tidesdb_txn_rollback(txn);
                tidesdb_txn_free(txn);
                return KVResult(KVStatus::IO_ERROR, "Batch operation failed");
            }
        }

        int rc = tidesdb_txn_commit(txn);
        tidesdb_txn_free(txn);

        if (rc == TDB_ERR_CONFLICT)
        {
            return KVResult(KVStatus::BUSY, "Transaction conflict");
        }
        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Commit failed");
        }

        return KVResult(KVStatus::OK);
    }

    std::unique_ptr<KVIterator> new_iterator() override
    {
        if (!open_ || !db_ || !cf_rows_)
        {
            return nullptr;
        }
        return std::make_unique<TidesDBIterator>(db_, cf_rows_, isolation_level_);
    }

    std::unique_ptr<KVIterator> new_iterator(KVColumnFamily* cf) override
    {
        if (!open_ || !db_)
        {
            return nullptr;
        }
        if (!cf)
        {
            return new_iterator();
        }
        auto* tcf = dynamic_cast<TidesDBColumnFamily*>(cf);
        if (!tcf || !tcf->native())
        {
            return nullptr;
        }
        return std::make_unique<TidesDBIterator>(db_, tcf->native(), isolation_level_);
    }

    KVResult create_column_family(const std::string& name, KVColumnFamily** cf) override
    {
        if (!open_ || !db_)
        {
            return KVResult(KVStatus::IO_ERROR, "Store not open");
        }

        tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
        int rc = tidesdb_create_column_family(db_, name.c_str(), &cf_config);

        if (rc == TDB_ERR_EXISTS)
        {
            return KVResult(KVStatus::ALREADY_EXISTS, "Column family exists");
        }
        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Failed to create column family");
        }

        tidesdb_column_family_t* created = tidesdb_get_column_family(db_, name.c_str());
        if (!created)
        {
            return KVResult(KVStatus::IO_ERROR, "Failed to get column family");
        }
        created_cf_handles_.push_back(std::make_unique<TidesDBColumnFamily>(name, created));
        *cf = created_cf_handles_.back().get();
        return KVResult(KVStatus::OK);
    }

    KVResult get_column_family(const std::string& name, KVColumnFamily** cf) override
    {
        if (!open_ || !db_)
        {
            return KVResult(KVStatus::IO_ERROR, "Store not open");
        }
        if (!cf)
        {
            return KVResult(KVStatus::INVALID_ARGUMENT, "Null output");
        }

        if (name == "rows")
        {
            *cf = rows_cf_handle_.get();
            return KVResult(KVStatus::OK);
        }
        if (name == "index")
        {
            *cf = index_cf_handle_.get();
            return KVResult(KVStatus::OK);
        }
        if (name == "meta")
        {
            *cf = meta_cf_handle_.get();
            return KVResult(KVStatus::OK);
        }

        tidesdb_column_family_t* existing = tidesdb_get_column_family(db_, name.c_str());
        if (!existing)
        {
            return KVResult(KVStatus::NOT_FOUND, "Column family not found");
        }
        created_cf_handles_.push_back(std::make_unique<TidesDBColumnFamily>(name, existing));
        *cf = created_cf_handles_.back().get();
        return KVResult(KVStatus::OK);
    }

    KVResult drop_column_family(KVColumnFamily* cf) override
    {
        /* TidesDB manages column families internally */
        return KVResult(KVStatus::NOT_SUPPORTED);
    }

    std::unique_ptr<KVTransaction> begin_transaction() override
    {
        if (!open_ || !db_ || !cf_rows_)
        {
            return nullptr;
        }
        return std::make_unique<TidesDBTransaction>(db_, cf_rows_, isolation_level_);
    }

    bool supports_transactions() const override
    {
        return true;
    }

    KVResult flush() override
    {
        /* TidesDB handles flushing automatically */
        return KVResult(KVStatus::OK);
    }

    KVResult compact() override
    {
        if (!open_ || !cf_rows_)
        {
            return KVResult(KVStatus::IO_ERROR, "Store not open");
        }

        /* Compact all column families */
        int rc = tidesdb_compact(cf_rows_);
        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Compaction failed for rows CF");
        }

        if (cf_index_)
        {
            rc = tidesdb_compact(cf_index_);
            if (rc != TDB_SUCCESS)
            {
                return KVResult(KVStatus::IO_ERROR, "Compaction failed for index CF");
            }
        }

        if (cf_meta_)
        {
            rc = tidesdb_compact(cf_meta_);
            if (rc != TDB_SUCCESS)
            {
                return KVResult(KVStatus::IO_ERROR, "Compaction failed for meta CF");
            }
        }

        if (cf_fulltext_)
        {
            rc = tidesdb_compact(cf_fulltext_);
            if (rc != TDB_SUCCESS)
            {
                return KVResult(KVStatus::IO_ERROR, "Compaction failed for fulltext CF");
            }
        }

        return KVResult(KVStatus::OK);
    }

    KVResult update_cf_config(KVColumnFamily* cf, const KVOptions& options,
                              bool persist = true) override
    {
        if (!open_ || !db_)
        {
            return KVResult(KVStatus::IO_ERROR, "Store not open");
        }

        /* Get the native CF handle */
        TidesDBColumnFamily* tdb_cf = dynamic_cast<TidesDBColumnFamily*>(cf);
        if (!tdb_cf)
        {
            return KVResult(KVStatus::INVALID_ARGUMENT, "Invalid column family");
        }

        tidesdb_column_family_t* native_cf = tdb_cf->native_cf();
        if (!native_cf)
        {
            return KVResult(KVStatus::INVALID_ARGUMENT, "Column family not initialized");
        }

        /* Build new config with updatable settings only */
        tidesdb_column_family_config_t new_config = tidesdb_default_column_family_config();
        new_config.write_buffer_size = options.write_buffer_size;
        new_config.bloom_fpr = options.bloom_fpr;

        /* Map sync mode */
        switch (options.sync_mode)
        {
            case 0:
                new_config.sync_mode = TDB_SYNC_NONE;
                break;
            case 1:
                new_config.sync_mode = TDB_SYNC_INTERVAL;
                break;
            case 2:
                new_config.sync_mode = TDB_SYNC_FULL;
                break;
            default:
                new_config.sync_mode = TDB_SYNC_INTERVAL;
                break;
        }
        new_config.sync_interval_us = options.sync_interval_us;

        int rc = tidesdb_cf_update_runtime_config(native_cf, &new_config, persist ? 1 : 0);
        if (rc != TDB_SUCCESS)
        {
            return KVResult(KVStatus::IO_ERROR, "Failed to update CF config");
        }

        return KVResult(KVStatus::OK);
    }

    const char* backend_name() const override
    {
        return "tidesdb";
    }

    std::string backend_version() const override
    {
        return "1.0.0";
    }

    tidesdb_t* native_db()
    {
        return db_;
    }

    tidesdb_column_family_t* rows_cf()
    {
        return cf_rows_;
    }
    tidesdb_column_family_t* index_cf()
    {
        return cf_index_;
    }
    tidesdb_column_family_t* meta_cf()
    {
        return cf_meta_;
    }
    tidesdb_column_family_t* fulltext_cf()
    {
        return cf_fulltext_;
    }

    KVColumnFamily* fulltext_cf_handle()
    {
        return fulltext_cf_handle_.get();
    }

   private:
    tidesdb_column_family_t* choose_cf_for_key(const std::string& key) const
    {
        if (key.size() >= 3 && key[2] == ':')
        {
            if (key[0] == 'f' && key[1] == 't') return cf_fulltext_;
        }
        if (key.size() >= 2 && key[1] == ':')
        {
            if (key[0] == 'r') return cf_rows_;
            if (key[0] == 'i') return cf_index_;
            if (key[0] == 'm') return cf_meta_;
        }
        return cf_rows_;
    }

    tidesdb_t* db_;
    tidesdb_column_family_t* cf_rows_;
    tidesdb_column_family_t* cf_index_;
    tidesdb_column_family_t* cf_meta_;
    tidesdb_column_family_t* cf_fulltext_;
    std::unique_ptr<TidesDBColumnFamily> rows_cf_handle_;
    std::unique_ptr<TidesDBColumnFamily> index_cf_handle_;
    std::unique_ptr<TidesDBColumnFamily> meta_cf_handle_;
    std::unique_ptr<TidesDBColumnFamily> fulltext_cf_handle_;
    std::vector<std::unique_ptr<TidesDBColumnFamily>> created_cf_handles_;
    KVOptions options_;
    std::string data_path_;
    bool open_;
    tidesdb_isolation_level_t isolation_level_;
};

REGISTER_KV_BACKEND("tidesdb", TidesDBKVStore);

} /* namespace tidesql */

#endif /* TIDESQL_KV_TIDESDB_HPP */
