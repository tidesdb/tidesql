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
#include "ha_tidesql.hpp"

#include <dirent.h>
#include <mysql/plugin.h>
#include <sql/field.h>
#include <sql/handler.h>
#include <sql/log.h>
#include <sql/sql_class.h>
#include <sql/table.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <sstream>

/* Forward declarations for config access */
extern "C"
{
    extern char* tidesql_data_dir;
    extern char* tidesql_compression;
    extern unsigned long long tidesql_write_buffer_size;
    extern unsigned long long tidesql_block_cache_size;
    extern unsigned int tidesql_flush_threads;
    extern unsigned int tidesql_compaction_threads;
    extern unsigned int tidesql_sync_mode;
    extern unsigned long long tidesql_sync_interval_us;
    extern my_bool tidesql_enable_bloom_filter;
    extern double tidesql_bloom_fpr;
    extern my_bool tidesql_enable_block_indexes;
    extern unsigned int tidesql_max_open_sstables;
    extern unsigned int tidesql_log_level;
    extern unsigned int tidesql_txn_isolation;

    /* Status counters */
    extern unsigned long long tidesql_rows_read;
    extern unsigned long long tidesql_rows_written;
    extern unsigned long long tidesql_rows_updated;
    extern unsigned long long tidesql_rows_deleted;
    extern unsigned long long tidesql_tables_opened;
    extern unsigned long long tidesql_tables_created;
}

namespace tidesql
{

/* -------------- TideSQLShare Implementation -------------- */

TideSQLShare::TideSQLShare(const char* table_name)
    : table_name_(table_name), row_id_counter_(0), ref_count_(0)
{
    /* Create the KV store - using TidesDB backend */
    kv_store_ = std::make_unique<TidesDBKVStore>();

    KVOptions opts;
    opts.create_if_missing = true;

    /* Use configured data directory */
    std::string base_dir = tidesql_data_dir ? tidesql_data_dir : "./tidesql_data";
    opts.data_dir = base_dir + "/" + table_name;

    /* Use configured compression */
    std::string compression = tidesql_compression ? tidesql_compression : "lz4";
    if (compression == "none")
    {
        opts.enable_compression = false;
    }
    else
    {
        opts.enable_compression = true;
        opts.compression_type = compression;
    }

    /* Apply other configuration options */
    opts.write_buffer_size = tidesql_write_buffer_size;
    opts.block_cache_size = tidesql_block_cache_size;
    opts.max_open_files = tidesql_max_open_sstables;
    opts.flush_threads = tidesql_flush_threads;
    opts.compaction_threads = tidesql_compaction_threads;
    opts.num_threads = tidesql_flush_threads + tidesql_compaction_threads;
    opts.sync_mode = tidesql_sync_mode;
    opts.sync_interval_us = tidesql_sync_interval_us;
    opts.sync_writes = (tidesql_sync_mode == 2); /* full sync */
    opts.txn_isolation = static_cast<int>(tidesql_txn_isolation);
    opts.enable_bloom_filter = tidesql_enable_bloom_filter;
    opts.bloom_fpr = tidesql_bloom_fpr;
    opts.enable_block_indexes = tidesql_enable_block_indexes;
    opts.log_level = tidesql_log_level;

    kv_store_->open(opts);

    /* Load persisted row_id_counter from meta CF */
    std::string meta_value;
    if (kv_store_->get("m:row_id_counter", &meta_value).ok() &&
        meta_value.size() == sizeof(uint64_t))
    {
        memcpy(&row_id_counter_, meta_value.data(), sizeof(uint64_t));
    }

    /* Load persisted auto_inc_value from meta CF */
    ulonglong auto_inc = 0;
    if (kv_store_->get("m:auto_inc_value", &meta_value).ok() &&
        meta_value.size() == sizeof(ulonglong))
    {
        memcpy(&auto_inc, meta_value.data(), sizeof(ulonglong));
        auto_inc_value_.store(auto_inc);
    }
}

TideSQLShare::~TideSQLShare()
{
    if (kv_store_)
    {
        /* Persist row_id_counter to meta CF before closing */
        std::string meta_value(reinterpret_cast<const char*>(&row_id_counter_), sizeof(uint64_t));
        kv_store_->put("m:row_id_counter", meta_value);

        /* Persist auto_inc_value to meta CF before closing */
        ulonglong auto_inc = auto_inc_value_.load();
        std::string auto_inc_meta(reinterpret_cast<const char*>(&auto_inc), sizeof(ulonglong));
        kv_store_->put("m:auto_inc_value", auto_inc_meta);

        kv_store_->close();
    }
}

ulonglong TideSQLShare::next_auto_inc(ulonglong increment, ulonglong offset)
{
    ulonglong current = auto_inc_value_.load();
    ulonglong next_val;
    if (current == 0)
    {
        next_val = offset;
    }
    else
    {
        next_val = ((current / increment) + 1) * increment + offset;
    }
    auto_inc_value_.store(next_val);
    return next_val;
}

/* -------------- TideSQLShareManager Implementation -------------- */

TideSQLShare* TideSQLShareManager::get_share(const char* table_name)
{
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = shares_.find(table_name);
    if (it != shares_.end())
    {
        it->second->inc_ref();
        return it->second.get();
    }

    auto share = std::make_unique<TideSQLShare>(table_name);
    share->inc_ref();
    TideSQLShare* ptr = share.get();
    shares_[table_name] = std::move(share);
    return ptr;
}

void TideSQLShareManager::release_share(TideSQLShare* share)
{
    if (!share) return;

    std::lock_guard<std::mutex> lock(mutex_);
    share->dec_ref();

    if (share->ref_count() <= 0)
    {
        shares_.erase(share->table_name());
    }
}

/* -------------- TideSQLHandler Implementation -------------- */

TideSQLHandler::TideSQLHandler(handlerton* hton, TABLE_SHARE* table_share)
    : handler(hton, table_share),
      share_(nullptr),
      scan_initialized_(false),
      active_index_(MAX_KEY),
      rows_cf_(nullptr),
      index_cf_(nullptr)
{
}

TideSQLHandler::~TideSQLHandler()
{
}

/* -------------- Table Operations -------------- */

int TideSQLHandler::create(const char* name, TABLE* form, HA_CREATE_INFO* create_info)
{
    /* Table creation is handled lazily when opened */
    /* We could store table metadata here if needed */

    /* Update status counter */
    __sync_fetch_and_add(&tidesql_tables_created, 1);

    return 0;
}

int TideSQLHandler::open(const char* name, int mode, uint test_if_locked)
{
    /* Get or create shared table data */
    share_ = TideSQLShareManager::instance().get_share(name);
    if (!share_)
    {
        return HA_ERR_OUT_OF_MEM;
    }

    /* Initialize thread lock */
    thr_lock_init(&lock_);
    thr_lock_data_init(&lock_, &lock_data_, this);

    /* Resolve column families */
    rows_cf_ = nullptr;
    index_cf_ = nullptr;
    fulltext_cf_ = nullptr;
    if (share_->kv_store())
    {
        share_->kv_store()->get_column_family("rows", &rows_cf_);
        share_->kv_store()->get_column_family("index", &index_cf_);
        share_->kv_store()->get_column_family("fulltext", &fulltext_cf_);
    }

    /* Initialize fulltext search state */
    ft_results_.clear();
    ft_current_pos_ = 0;
    ft_initialized_ = false;

    /* Update status counter */
    __sync_fetch_and_add(&tidesql_tables_opened, 1);

    return 0;
}

int TideSQLHandler::close()
{
    thr_lock_delete(&lock_);
    if (share_)
    {
        TideSQLShareManager::instance().release_share(share_);
        share_ = nullptr;
    }
    rows_cf_ = nullptr;
    index_cf_ = nullptr;
    fulltext_cf_ = nullptr;
    ft_results_.clear();
    return 0;
}

/* -------------- Row Operations -------------- */

int TideSQLHandler::write_row(uchar* buf)
{
    if (!share_ || !share_->kv_store())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Handle auto-increment if present */
    if (table->next_number_field && buf == table->record[0])
    {
        int err = update_auto_increment();
        if (err) return err;
    }

    /* Generate row ID */
    uint64_t row_id = share_->next_row_id();

    /* Pack the row data */
    std::string packed;
    int err = pack_row(buf, &packed);
    if (err) return err;

    /* Create row key */
    std::string key = make_row_key(row_id);

    /* Store in KV */
    KVResult result = share_->kv_store()->put(key, packed);
    if (!result.ok())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Update secondary indexes */
    err = update_indexes(row_id, nullptr, buf);
    if (err) return err;

    /* Update fulltext indexes */
    err = update_fulltext_index(row_id, nullptr, buf);
    if (err) return err;

    /* Update status counter */
    __sync_fetch_and_add(&tidesql_rows_written, 1);

    return 0;
}

int TideSQLHandler::update_row(const uchar* old_data, uchar* new_data)
{
    if (!share_ || !share_->kv_store())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Use the current key from the last read */
    if (current_key_.empty())
    {
        return HA_ERR_KEY_NOT_FOUND;
    }

    /* Pack the new row data */
    std::string packed;
    int err = pack_row(new_data, &packed);
    if (err) return err;

    /* Update in KV */
    KVResult result = share_->kv_store()->put(current_key_, packed);
    if (!result.ok())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Update secondary indexes */
    uint64_t row_id = extract_row_id(current_key_);
    err = update_indexes(row_id, old_data, new_data);
    if (err) return err;

    /* Update fulltext indexes */
    err = update_fulltext_index(row_id, old_data, new_data);
    if (err) return err;

    /* Update status counter */
    __sync_fetch_and_add(&tidesql_rows_updated, 1);

    return 0;
}

int TideSQLHandler::delete_row(const uchar* buf)
{
    if (!share_ || !share_->kv_store())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Use the current key from the last read */
    if (current_key_.empty())
    {
        return HA_ERR_KEY_NOT_FOUND;
    }

    /* Remove secondary index entries */
    uint64_t row_id = extract_row_id(current_key_);
    int err = delete_indexes(row_id, buf);
    if (err) return err;

    /* Remove fulltext index entries */
    err = delete_fulltext_index(row_id, buf);
    if (err) return err;

    /* Delete from KV */
    KVResult result = share_->kv_store()->remove(current_key_);
    if (!result.ok())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Update status counter */
    __sync_fetch_and_add(&tidesql_rows_deleted, 1);

    return 0;
}

/* -------------- Table Scan -------------- */

int TideSQLHandler::rnd_init(bool scan)
{
    if (!share_ || !share_->kv_store())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    scan_iterator_ = share_->kv_store()->new_iterator(rows_cf_);
    if (!scan_iterator_)
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Rows CF contains only row keys; seek to first is enough */
    scan_iterator_->seek_to_first();
    scan_initialized_ = true;

    return 0;
}

int TideSQLHandler::rnd_end()
{
    scan_iterator_.reset();
    scan_initialized_ = false;
    return 0;
}

int TideSQLHandler::rnd_next(uchar* buf)
{
    if (!scan_initialized_ || !scan_iterator_)
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    while (scan_iterator_->valid())
    {
        current_key_ = scan_iterator_->key();
        std::string value = scan_iterator_->value();
        int err = unpack_row(value, buf);
        scan_iterator_->next();
        if (err == 0)
        {
            __sync_fetch_and_add(&tidesql_rows_read, 1);
            return 0;
        }
    }

    return HA_ERR_END_OF_FILE;
}

int TideSQLHandler::rnd_pos(uchar* buf, uchar* pos)
{
    if (!share_ || !share_->kv_store())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* pos contains the row key */
    uint64_t row_id;
    memcpy(&row_id, pos, sizeof(row_id));

    std::string key = make_row_key(row_id);
    std::string value;

    KVResult result = share_->kv_store()->get(key, &value);
    if (result.not_found())
    {
        return HA_ERR_KEY_NOT_FOUND;
    }
    if (!result.ok())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    current_key_ = key;
    return unpack_row(value, buf);
}

void TideSQLHandler::position(const uchar* record)
{
    /* Store the row ID in ref for later retrieval */
    uint64_t row_id = extract_row_id(current_key_);
    memcpy(ref, &row_id, sizeof(row_id));
}

/* -------------- Index Operations -------------- */

int TideSQLHandler::index_init(uint idx, bool sorted)
{
    if (!share_ || !share_->kv_store())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    active_index_ = idx;
    scan_iterator_ = share_->kv_store()->new_iterator(index_cf_);

    return 0;
}

int TideSQLHandler::index_end()
{
    scan_iterator_.reset();
    active_index_ = MAX_KEY;
    return 0;
}

int TideSQLHandler::index_read_map(uchar* buf, const uchar* key, key_part_map keypart_map,
                                   enum ha_rkey_function find_flag)
{
    if (!scan_iterator_)
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Build index key prefix */
    uint key_len = calculate_key_len(table, active_index_, key, keypart_map);
    std::string index_key = make_index_key(active_index_, key, key_len);

    /* Build index prefix for boundary checking */
    std::ostringstream prefix_oss;
    prefix_oss << "i:" << active_index_ << ":";
    std::string idx_prefix = prefix_oss.str();

    /* Store the search key for index_next_same */
    index_search_key_ = index_key;
    index_search_key_len_ = key_len;

    switch (find_flag)
    {
        case HA_READ_KEY_EXACT:
            /* Find exact match */
            scan_iterator_->seek(index_key);
            if (!scan_iterator_->valid())
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            /* Must match the search key exactly (prefix match for partial keys) */
            if (scan_iterator_->key().compare(0, index_key.size(), index_key) != 0)
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            break;

        case HA_READ_KEY_OR_NEXT:
            /* Find key or first key greater */
            scan_iterator_->seek(index_key);
            if (!scan_iterator_->valid())
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            /* Verify we're still in the same index */
            if (scan_iterator_->key().compare(0, idx_prefix.size(), idx_prefix) != 0)
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            break;

        case HA_READ_KEY_OR_PREV:
            /* Find key or first key less than */
            scan_iterator_->seek(index_key);
            if (!scan_iterator_->valid())
            {
                /* Seek past end, go to last */
                scan_iterator_->seek_to_last();
            }
            else if (scan_iterator_->key().compare(0, index_key.size(), index_key) != 0)
            {
                /* Not exact match, go back one */
                scan_iterator_->prev();
            }
            if (!scan_iterator_->valid())
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            if (scan_iterator_->key().compare(0, idx_prefix.size(), idx_prefix) != 0)
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            break;

        case HA_READ_AFTER_KEY:
            /* Find first key strictly greater than */
            scan_iterator_->seek(index_key);
            if (scan_iterator_->valid() &&
                scan_iterator_->key().compare(0, index_key.size(), index_key) == 0)
            {
                /* Skip all matching keys */
                while (scan_iterator_->valid() &&
                       scan_iterator_->key().compare(0, index_key.size(), index_key) == 0)
                {
                    scan_iterator_->next();
                }
            }
            if (!scan_iterator_->valid())
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            if (scan_iterator_->key().compare(0, idx_prefix.size(), idx_prefix) != 0)
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            break;

        case HA_READ_BEFORE_KEY:
            /* Find first key strictly less than */
            scan_iterator_->seek(index_key);
            if (scan_iterator_->valid())
            {
                scan_iterator_->prev();
            }
            else
            {
                scan_iterator_->seek_to_last();
            }
            if (!scan_iterator_->valid())
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            if (scan_iterator_->key().compare(0, idx_prefix.size(), idx_prefix) != 0)
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            break;

        case HA_READ_PREFIX:
            /* Find first key with given prefix */
            scan_iterator_->seek(index_key);
            if (!scan_iterator_->valid())
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            if (scan_iterator_->key().compare(0, index_key.size(), index_key) != 0)
            {
                return HA_ERR_KEY_NOT_FOUND;
            }
            break;

        case HA_READ_PREFIX_LAST:
        case HA_READ_PREFIX_LAST_OR_PREV:
            /* Find last key with given prefix */
            {
                /* Seek to prefix, then find last matching */
                scan_iterator_->seek(index_key);
                std::string last_valid_key;
                std::string last_valid_value;
                while (scan_iterator_->valid() &&
                       scan_iterator_->key().compare(0, index_key.size(), index_key) == 0)
                {
                    last_valid_key = scan_iterator_->key();
                    last_valid_value = scan_iterator_->value();
                    scan_iterator_->next();
                }
                if (last_valid_key.empty())
                {
                    if (find_flag == HA_READ_PREFIX_LAST_OR_PREV)
                    {
                        scan_iterator_->seek(index_key);
                        if (scan_iterator_->valid())
                        {
                            scan_iterator_->prev();
                        }
                        if (!scan_iterator_->valid() ||
                            scan_iterator_->key().compare(0, idx_prefix.size(), idx_prefix) != 0)
                        {
                            return HA_ERR_KEY_NOT_FOUND;
                        }
                    }
                    else
                    {
                        return HA_ERR_KEY_NOT_FOUND;
                    }
                }
                else
                {
                    /* Re-seek to the last valid position */
                    scan_iterator_->seek(last_valid_key);
                }
            }
            break;

        default:
            /* HA_READ_MBR_CONTAIN, HA_READ_MBR_INTERSECT, etc. not supported */
            return HA_ERR_WRONG_COMMAND;
    }

    /* Get the row from index value */
    std::string row_key = scan_iterator_->value();
    current_key_ = row_key;

    std::string row_value;
    KVResult result = share_->kv_store()->get(row_key, &row_value);
    if (!result.ok())
    {
        return HA_ERR_KEY_NOT_FOUND;
    }

    return unpack_row(row_value, buf);
}

int TideSQLHandler::index_next_same(uchar* buf, const uchar* key, uint keylen)
{
    if (!scan_iterator_)
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    scan_iterator_->next();

    if (!scan_iterator_->valid())
    {
        return HA_ERR_END_OF_FILE;
    }

    /* Must match the original search key prefix */
    if (scan_iterator_->key().compare(0, index_search_key_.size(), index_search_key_) != 0)
    {
        return HA_ERR_END_OF_FILE;
    }

    std::string row_key = scan_iterator_->value();
    current_key_ = row_key;

    std::string row_value;
    KVResult result = share_->kv_store()->get(row_key, &row_value);
    if (!result.ok())
    {
        return HA_ERR_KEY_NOT_FOUND;
    }

    return unpack_row(row_value, buf);
}

int TideSQLHandler::index_next(uchar* buf)
{
    if (!scan_iterator_)
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    scan_iterator_->next();

    if (!scan_iterator_->valid())
    {
        return HA_ERR_END_OF_FILE;
    }

    /* Enforce index prefix boundary */
    {
        std::ostringstream oss;
        oss << "i:" << active_index_ << ":";
        const std::string prefix = oss.str();
        const std::string k = scan_iterator_->key();
        if (k.size() < prefix.size() || k.compare(0, prefix.size(), prefix) != 0)
        {
            return HA_ERR_END_OF_FILE;
        }
    }

    std::string row_key = scan_iterator_->value();
    current_key_ = row_key;

    std::string row_value;
    KVResult result = share_->kv_store()->get(row_key, &row_value);
    if (!result.ok())
    {
        return HA_ERR_KEY_NOT_FOUND;
    }

    return unpack_row(row_value, buf);
}

int TideSQLHandler::index_prev(uchar* buf)
{
    if (!scan_iterator_)
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    scan_iterator_->prev();

    if (!scan_iterator_->valid())
    {
        return HA_ERR_END_OF_FILE;
    }

    /* Enforce index prefix boundary */
    {
        std::ostringstream oss;
        oss << "i:" << active_index_ << ":";
        const std::string prefix = oss.str();
        const std::string k = scan_iterator_->key();
        if (k.size() < prefix.size() || k.compare(0, prefix.size(), prefix) != 0)
        {
            return HA_ERR_END_OF_FILE;
        }
    }

    std::string row_key = scan_iterator_->value();
    current_key_ = row_key;

    std::string row_value;
    KVResult result = share_->kv_store()->get(row_key, &row_value);
    if (!result.ok())
    {
        return HA_ERR_KEY_NOT_FOUND;
    }

    return unpack_row(row_value, buf);
}

int TideSQLHandler::index_first(uchar* buf)
{
    if (!scan_iterator_)
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Seek to first index entry for this index */
    std::ostringstream oss;
    oss << "i:" << active_index_ << ":";
    scan_iterator_->seek(oss.str());

    if (!scan_iterator_->valid())
    {
        return HA_ERR_END_OF_FILE;
    }

    /* Enforce index prefix boundary */
    {
        std::ostringstream p;
        p << "i:" << active_index_ << ":";
        const std::string prefix = p.str();
        const std::string k = scan_iterator_->key();
        if (k.size() < prefix.size() || k.compare(0, prefix.size(), prefix) != 0)
        {
            return HA_ERR_END_OF_FILE;
        }
    }

    std::string row_key = scan_iterator_->value();
    current_key_ = row_key;

    std::string row_value;
    KVResult result = share_->kv_store()->get(row_key, &row_value);
    if (!result.ok())
    {
        return HA_ERR_KEY_NOT_FOUND;
    }

    return unpack_row(row_value, buf);
}

int TideSQLHandler::index_last(uchar* buf)
{
    if (!scan_iterator_)
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Seek past the last index entry, then go back */
    std::ostringstream oss;
    oss << "i:" << (active_index_ + 1) << ":";
    scan_iterator_->seek(oss.str());
    scan_iterator_->prev();

    if (!scan_iterator_->valid())
    {
        return HA_ERR_END_OF_FILE;
    }

    /* Enforce index prefix boundary */
    {
        std::ostringstream p;
        p << "i:" << active_index_ << ":";
        const std::string prefix = p.str();
        const std::string k = scan_iterator_->key();
        if (k.size() < prefix.size() || k.compare(0, prefix.size(), prefix) != 0)
        {
            return HA_ERR_END_OF_FILE;
        }
    }

    std::string row_key = scan_iterator_->value();
    current_key_ = row_key;

    std::string row_value;
    KVResult result = share_->kv_store()->get(row_key, &row_value);
    if (!result.ok())
    {
        return HA_ERR_KEY_NOT_FOUND;
    }

    return unpack_row(row_value, buf);
}

/* -------------- Table Info -------------- */

int TideSQLHandler::info(uint flag)
{
    if (flag & HA_STATUS_VARIABLE)
    {
        /* Count actual rows by iterating (more accurate than row_id_counter after deletes) */
        ha_rows count = 0;
        if (share_ && share_->kv_store())
        {
            auto iter = share_->kv_store()->new_iterator(rows_cf_);
            if (iter)
            {
                iter->seek_to_first();
                while (iter->valid())
                {
                    count++;
                    iter->next();
                }
            }
        }
        stats.records = count;
        stats.deleted = 0;
    }

    if (flag & HA_STATUS_CONST)
    {
        stats.max_data_file_length = LLONG_MAX;
        stats.max_index_file_length = LLONG_MAX;
        stats.create_time = 0;
    }

    if (flag & HA_STATUS_AUTO)
    {
        stats.auto_increment_value = share_ ? share_->current_auto_inc() : 0;
    }

    return 0;
}

ha_rows TideSQLHandler::records_in_range(uint inx, key_range* min_key, key_range* max_key)
{
    /* Return estimate - optimizer hint */
    return 10;
}

/* -------------- Locking -------------- */

int TideSQLHandler::external_lock(THD* thd, int lock_type)
{
    /* Called at statement start (F_RDLCK/F_WRLCK) and end (F_UNLCK) */
    return 0;
}

THR_LOCK_DATA** TideSQLHandler::store_lock(THD* thd, THR_LOCK_DATA** to,
                                           enum thr_lock_type lock_type)
{
    if (lock_type != TL_IGNORE && lock_data_.type == TL_UNLOCK)
    {
        lock_data_.type = lock_type;
    }
    *to++ = &lock_data_;
    return to;
}

/* -------------- DDL Operations -------------- */

static int remove_directory_recursive(const std::string& path)
{
    DIR* dir = opendir(path.c_str());
    if (!dir)
    {
        if (errno == ENOENT) return 0;
        return -1;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr)
    {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }

        std::string full_path = path + "/" + entry->d_name;
        struct stat st;
        if (stat(full_path.c_str(), &st) == 0)
        {
            if (S_ISDIR(st.st_mode))
            {
                remove_directory_recursive(full_path);
            }
            else
            {
                unlink(full_path.c_str());
            }
        }
    }
    closedir(dir);
    return rmdir(path.c_str());
}

int TideSQLHandler::delete_table(const char* name)
{
    std::string base_dir = tidesql_data_dir ? tidesql_data_dir : "./tidesql_data";
    std::string table_dir = base_dir + "/" + name;

    /* Remove share from cache first */
    {
        std::lock_guard<std::mutex> lock(share_mutex);
        auto it = share_map.find(name);
        if (it != share_map.end())
        {
            if (it->second->ref_count() > 0)
            {
                return HA_ERR_TABLE_EXIST;
            }
            share_map.erase(it);
        }
    }

    /* Recursively remove the table's data directory */
    if (remove_directory_recursive(table_dir) != 0 && errno != ENOENT)
    {
        return HA_ERR_GENERIC;
    }

    return 0;
}

int TideSQLHandler::rename_table(const char* from, const char* to)
{
    std::string base_dir = tidesql_data_dir ? tidesql_data_dir : "./tidesql_data";
    std::string from_dir = base_dir + "/" + from;
    std::string to_dir = base_dir + "/" + to;

    /* Close any existing share for the old table */
    {
        std::lock_guard<std::mutex> lock(share_mutex);
        auto it = share_map.find(from);
        if (it != share_map.end())
        {
            /* Wait for all references to be released or force close */
            if (it->second->ref_count() > 0)
            {
                /* Table is in use, cannot rename */
                return HA_ERR_TABLE_EXIST;
            }
            share_map.erase(it);
        }
    }

    /* Rename the data directory */
    if (rename(from_dir.c_str(), to_dir.c_str()) != 0)
    {
        if (errno == ENOENT)
        {
            return HA_ERR_NO_SUCH_TABLE;
        }
        return HA_ERR_GENERIC;
    }

    return 0;
}

int TideSQLHandler::truncate()
{
    if (!share_ || !share_->kv_store())
    {
        return HA_ERR_INTERNAL_ERROR;
    }

    /* Delete all rows by iterating and removing */
    auto iter = share_->kv_store()->new_iterator(rows_cf_);
    if (iter)
    {
        iter->seek_to_first();
        while (iter->valid())
        {
            std::string key = iter->key();
            share_->kv_store()->remove(key);
            iter->next();
        }
    }

    /* Delete all index entries */
    auto idx_iter = share_->kv_store()->new_iterator(index_cf_);
    if (idx_iter)
    {
        idx_iter->seek_to_first();
        while (idx_iter->valid())
        {
            std::string key = idx_iter->key();
            share_->kv_store()->remove(key);
            idx_iter->next();
        }
    }

    /* Reset row ID counter */
    share_->set_row_id(0);

    return 0;
}

/* -------------- Helper Methods -------------- */

std::string TideSQLHandler::make_row_key(uint64_t row_id)
{
    std::ostringstream oss;
    oss << "r:" << std::setfill('0') << std::setw(20) << row_id;
    return oss.str();
}

std::string TideSQLHandler::make_index_key(uint idx, const uchar* key, uint key_len)
{
    std::ostringstream oss;
    oss << "i:" << idx << ":";
    oss.write(reinterpret_cast<const char*>(key), key_len);
    return oss.str();
}

int TideSQLHandler::pack_row(uchar* buf, std::string* packed)
{
    uint row_len = table->s->reclength;
    packed->assign(reinterpret_cast<char*>(buf), row_len);
    return 0;
}

int TideSQLHandler::unpack_row(const std::string& packed, uchar* buf)
{
    uint row_len = table->s->reclength;
    if (packed.size() < row_len)
    {
        return HA_ERR_INTERNAL_ERROR;
    }
    memcpy(buf, packed.data(), row_len);
    return 0;
}

uint64_t TideSQLHandler::extract_row_id(const std::string& key)
{
    /* Key format: "r:XXXXXXXXXXXXXXXXXXXX" */
    if (key.size() < 3 || key.substr(0, 2) != "r:")
    {
        return 0;
    }
    return std::stoull(key.substr(2));
}

/* -------------- Auto-increment -------------- */

void TideSQLHandler::get_auto_increment(ulonglong offset, ulonglong increment,
                                        ulonglong nb_desired_values, ulonglong* first_value,
                                        ulonglong* nb_reserved_values)
{
    if (!share_)
    {
        *first_value = 1;
        *nb_reserved_values = 1;
        return;
    }

    /* Get next auto-increment value from shared table data */
    ulonglong next_val = share_->next_auto_inc(increment, offset);

    /* Reserve the requested number of values */
    for (ulonglong i = 1; i < nb_desired_values; i++)
    {
        share_->next_auto_inc(increment, offset);
    }

    *first_value = next_val;
    *nb_reserved_values = nb_desired_values;
}

/* -------------- Secondary Index Management -------------- */

std::string TideSQLHandler::build_index_key_from_record(uint idx, const uchar* record)
{
    if (!table || idx >= table->s->keys)
    {
        return std::string();
    }

    KEY* key_info = &table->key_info[idx];
    std::ostringstream oss;
    oss << "i:" << idx << ":";

    /* Build key from each key part */
    for (uint i = 0; i < key_info->user_defined_key_parts; i++)
    {
        KEY_PART_INFO* key_part = &key_info->key_part[i];
        Field* field = key_part->field;

        /* Get field value from record */
        const uchar* field_ptr = record + field->offset(table->record[0]);

        /* Check for NULL */
        if (field->is_null_in_record(record))
        {
            oss << '\0'; /* NULL marker */
            continue;
        }

        /* Append field value based on type */
        uint len = field->pack_length();
        oss.write(reinterpret_cast<const char*>(field_ptr), len);
    }

    return oss.str();
}

int TideSQLHandler::update_indexes(uint64_t row_id, const uchar* old_record,
                                   const uchar* new_record)
{
    if (!share_ || !share_->kv_store() || !table)
    {
        return 0; /* No error if no table */
    }

    /* Skip if no indexes (other than primary key at index 0) */
    if (table->s->keys <= 1)
    {
        return 0;
    }

    std::string row_key = make_row_key(row_id);

    /* Process each secondary index */
    for (uint idx = 1; idx < table->s->keys; idx++)
    {
        /* Delete old index entry if updating */
        if (old_record)
        {
            std::string old_idx_key = build_index_key_from_record(idx, old_record);
            if (!old_idx_key.empty())
            {
                /* Append row_id to make unique */
                old_idx_key += ":" + std::to_string(row_id);
                share_->kv_store()->remove(old_idx_key);
            }
        }

        /* Insert new index entry */
        if (new_record)
        {
            std::string new_idx_key = build_index_key_from_record(idx, new_record);
            if (!new_idx_key.empty())
            {
                /* Append row_id to make unique */
                new_idx_key += ":" + std::to_string(row_id);
                KVResult result = share_->kv_store()->put(new_idx_key, row_key);
                if (!result.ok())
                {
                    return HA_ERR_INTERNAL_ERROR;
                }
            }
        }
    }

    return 0;
}

int TideSQLHandler::delete_indexes(uint64_t row_id, const uchar* record)
{
    if (!share_ || !share_->kv_store() || !table)
    {
        return 0;
    }

    /* Skip if no indexes (other than primary key) */
    if (table->s->keys <= 1)
    {
        return 0;
    }

    /* Delete index entries for each secondary index */
    for (uint idx = 1; idx < table->s->keys; idx++)
    {
        std::string idx_key = build_index_key_from_record(idx, record);
        if (!idx_key.empty())
        {
            /* Append row_id to make unique */
            idx_key += ":" + std::to_string(row_id);
            share_->kv_store()->remove(idx_key);
        }
    }

    return 0;
}

/* -------------- Foreign Key Support -------------- */

char* TideSQLHandler::get_foreign_key_create_info()
{
    /* Return FK definitions stored in meta CF */
    if (!share_ || !share_->kv_store())
    {
        return nullptr;
    }

    std::string fk_info;
    KVResult result = share_->kv_store()->get("m:fk_create_info", &fk_info);
    if (!result.ok() || fk_info.empty())
    {
        return nullptr;
    }

    /* Return a copy that MySQL will free */
    char* str = static_cast<char*>(my_malloc(PSI_NOT_INSTRUMENTED, fk_info.size() + 1, MYF(0)));
    if (str)
    {
        memcpy(str, fk_info.c_str(), fk_info.size() + 1);
    }
    return str;
}

void TideSQLHandler::free_foreign_key_create_info(char* str)
{
    if (str)
    {
        my_free(str);
    }
}

int TideSQLHandler::get_foreign_key_list(THD* thd, List<FOREIGN_KEY_INFO>* f_key_list)
{
    if (!share_ || !share_->kv_store() || !f_key_list)
    {
        return 0;
    }

    /* FK metadata stored as "m:fk_count" -> number of FKs
     * "m:fk:<idx>" ->
     * "fk_name|ref_db|ref_table|col1,col2|ref_col1,ref_col2|update_rule|delete_rule"
     */
    std::string fk_count_str;
    KVResult result = share_->kv_store()->get("m:fk_count", &fk_count_str);
    if (!result.ok() || fk_count_str.empty())
    {
        return 0;
    }

    uint fk_count = static_cast<uint>(std::stoul(fk_count_str));
    for (uint i = 0; i < fk_count; i++)
    {
        std::string fk_key = "m:fk:" + std::to_string(i);
        std::string fk_data;
        result = share_->kv_store()->get(fk_key, &fk_data);
        if (!result.ok() || fk_data.empty())
        {
            continue;
        }

        /* Parse FK data: fk_name|ref_db|ref_table|cols|ref_cols|update|delete */
        std::vector<std::string> parts;
        std::istringstream iss(fk_data);
        std::string part;
        while (std::getline(iss, part, '|'))
        {
            parts.push_back(part);
        }

        if (parts.size() < 7) continue;

        FOREIGN_KEY_INFO* fk_info = new (thd->mem_root) FOREIGN_KEY_INFO;
        if (!fk_info) continue;

        /* Initialize LEX_CSTRING fields */
        fk_info->foreign_id =
            thd_make_lex_string(thd, nullptr, parts[0].c_str(), parts[0].length(), true);
        fk_info->referenced_db =
            thd_make_lex_string(thd, nullptr, parts[1].c_str(), parts[1].length(), true);
        fk_info->referenced_table =
            thd_make_lex_string(thd, nullptr, parts[2].c_str(), parts[2].length(), true);

        /* Parse column lists */
        std::istringstream col_iss(parts[3]);
        std::string col;
        while (std::getline(col_iss, col, ','))
        {
            LEX_CSTRING* col_name =
                thd_make_lex_string(thd, nullptr, col.c_str(), col.length(), true);
            if (col_name)
            {
                fk_info->foreign_fields.push_back(col_name);
            }
        }

        std::istringstream ref_col_iss(parts[4]);
        while (std::getline(ref_col_iss, col, ','))
        {
            LEX_CSTRING* col_name =
                thd_make_lex_string(thd, nullptr, col.c_str(), col.length(), true);
            if (col_name)
            {
                fk_info->referenced_fields.push_back(col_name);
            }
        }

        /* Set referential actions */
        fk_info->update_method = static_cast<enum_fk_option>(std::stoi(parts[5]));
        fk_info->delete_method = static_cast<enum_fk_option>(std::stoi(parts[6]));

        f_key_list->push_back(fk_info);
    }

    return 0;
}

int TideSQLHandler::get_parent_foreign_key_list(THD* thd, List<FOREIGN_KEY_INFO>* f_key_list)
{
    if (!share_ || !share_->kv_store() || !f_key_list)
    {
        return 0;
    }

    /* Parent FK metadata stored as "m:parent_fk_count" -> number
     * "m:parent_fk:<idx>" -> same format as child FKs
     */
    std::string fk_count_str;
    KVResult result = share_->kv_store()->get("m:parent_fk_count", &fk_count_str);
    if (!result.ok() || fk_count_str.empty())
    {
        return 0;
    }

    uint fk_count = static_cast<uint>(std::stoul(fk_count_str));
    for (uint i = 0; i < fk_count; i++)
    {
        std::string fk_key = "m:parent_fk:" + std::to_string(i);
        std::string fk_data;
        result = share_->kv_store()->get(fk_key, &fk_data);
        if (!result.ok() || fk_data.empty())
        {
            continue;
        }

        std::vector<std::string> parts;
        std::istringstream iss(fk_data);
        std::string part;
        while (std::getline(iss, part, '|'))
        {
            parts.push_back(part);
        }

        if (parts.size() < 7) continue;

        FOREIGN_KEY_INFO* fk_info = new (thd->mem_root) FOREIGN_KEY_INFO;
        if (!fk_info) continue;

        fk_info->foreign_id =
            thd_make_lex_string(thd, nullptr, parts[0].c_str(), parts[0].length(), true);
        fk_info->referenced_db =
            thd_make_lex_string(thd, nullptr, parts[1].c_str(), parts[1].length(), true);
        fk_info->referenced_table =
            thd_make_lex_string(thd, nullptr, parts[2].c_str(), parts[2].length(), true);

        std::istringstream col_iss(parts[3]);
        std::string col;
        while (std::getline(col_iss, col, ','))
        {
            LEX_CSTRING* col_name =
                thd_make_lex_string(thd, nullptr, col.c_str(), col.length(), true);
            if (col_name)
            {
                fk_info->foreign_fields.push_back(col_name);
            }
        }

        std::istringstream ref_col_iss(parts[4]);
        while (std::getline(ref_col_iss, col, ','))
        {
            LEX_CSTRING* col_name =
                thd_make_lex_string(thd, nullptr, col.c_str(), col.length(), true);
            if (col_name)
            {
                fk_info->referenced_fields.push_back(col_name);
            }
        }

        fk_info->update_method = static_cast<enum_fk_option>(std::stoi(parts[5]));
        fk_info->delete_method = static_cast<enum_fk_option>(std::stoi(parts[6]));

        f_key_list->push_back(fk_info);
    }

    return 0;
}

uint TideSQLHandler::referenced_by_foreign_key()
{
    /*
     * Return non-zero if this table is referenced by foreign keys
     * This affects DELETE/UPDATE behavior
     */
    if (!share_ || !share_->kv_store())
    {
        return 0;
    }

    /* We check if there's a parent FK list stored */
    std::string parent_fk;
    KVResult result = share_->kv_store()->get("m:parent_fk_count", &parent_fk);
    if (result.ok() && !parent_fk.empty())
    {
        return static_cast<uint>(std::stoul(parent_fk));
    }

    return 0;
}

/* -------------- Fulltext Index Support (BM25) -------------- */

int TideSQLHandler::ft_init()
{
    ft_current_pos_ = 0;
    ft_initialized_ = true;
    return 0;
}

int TideSQLHandler::ft_read(uchar* buf)
{
    if (!ft_initialized_ || ft_current_pos_ >= ft_results_.size())
    {
        return HA_ERR_END_OF_FILE;
    }

    /* Get the row_id from results (sorted by BM25 score descending) */
    uint64_t row_id = ft_results_[ft_current_pos_].first;
    ft_current_pos_++;

    /* Fetch the actual row */
    std::string row_key = make_row_key(row_id);
    std::string row_value;
    KVResult result = share_->kv_store()->get(row_key, &row_value);
    if (!result.ok())
    {
        return HA_ERR_KEY_NOT_FOUND;
    }

    current_key_ = row_key;
    return unpack_row(row_value, buf);
}

FT_INFO* TideSQLHandler::ft_init_ext(uint flags, uint inx, String* key)
{
    if (!share_ || !share_->kv_store() || !key)
    {
        return nullptr;
    }

    ft_results_.clear();
    ft_current_pos_ = 0;
    ft_initialized_ = false;

    /* Get search query */
    std::string query(key->ptr(), key->length());
    std::vector<std::string> search_terms = BM25Index::tokenize(query);

    if (search_terms.empty())
    {
        return reinterpret_cast<FT_INFO*>(1); /* Return non-null but empty results */
    }

    /* Get global stats from fulltext CF */
    std::string total_docs_str, avg_dl_str;
    uint64_t total_docs = 0;
    double avg_doc_len = 1.0;

    if (share_->kv_store()->get("ft:total_docs", &total_docs_str).ok() &&
        total_docs_str.size() == sizeof(uint64_t))
    {
        memcpy(&total_docs, total_docs_str.data(), sizeof(uint64_t));
    }
    if (share_->kv_store()->get("ft:avg_dl", &avg_dl_str).ok() &&
        avg_dl_str.size() == sizeof(double))
    {
        memcpy(&avg_doc_len, avg_dl_str.data(), sizeof(double));
    }

    if (total_docs == 0)
    {
        return reinterpret_cast<FT_INFO*>(1);
    }

    /* Collect matching documents and their scores */
    std::unordered_map<uint64_t, double> doc_scores;

    for (const auto& term : search_terms)
    {
        /* Get document frequency for this term */
        std::string df_key = "ft:df:" + term;
        std::string df_str;
        uint64_t df = 0;

        if (share_->kv_store()->get(df_key, &df_str).ok() && df_str.size() == sizeof(uint64_t))
        {
            memcpy(&df, df_str.data(), sizeof(uint64_t));
        }

        if (df == 0) continue;

        /* Scan for all documents containing this term */
        std::string tf_prefix = "ft:tf:" + term + ":";
        auto iter = share_->kv_store()->new_iterator(fulltext_cf_);
        if (!iter) continue;

        iter->seek(tf_prefix);
        while (iter->valid())
        {
            std::string iter_key = iter->key();
            if (iter_key.substr(0, tf_prefix.size()) != tf_prefix)
            {
                break;
            }

            /* Extract row_id from key */
            uint64_t row_id = std::stoull(iter_key.substr(tf_prefix.size()));

            /* Get term frequency */
            std::string tf_str = iter->value();
            uint32_t tf = 0;
            if (tf_str.size() == sizeof(uint32_t))
            {
                memcpy(&tf, tf_str.data(), sizeof(uint32_t));
            }

            /* Get document length */
            std::string dl_key = "ft:dl:" + std::to_string(row_id);
            std::string dl_str;
            uint32_t doc_len = 1;
            if (share_->kv_store()->get(dl_key, &dl_str).ok() && dl_str.size() == sizeof(uint32_t))
            {
                memcpy(&doc_len, dl_str.data(), sizeof(uint32_t));
            }

            /* Calculate BM25 score for this term */
            double score = BM25Index::score_term(tf, df, total_docs, doc_len, avg_doc_len);
            doc_scores[row_id] += score;

            iter->next();
        }
    }

    /* Sort results by score (descending) */
    for (const auto& pair : doc_scores)
    {
        ft_results_.push_back(pair);
    }
    std::sort(ft_results_.begin(), ft_results_.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    ft_initialized_ = true;

    /* Return non-null to indicate success (MySQL uses this as opaque handle) */
    return reinterpret_cast<FT_INFO*>(1);
}

int TideSQLHandler::update_fulltext_index(uint64_t row_id, const uchar* old_record,
                                          const uchar* new_record)
{
    if (!share_ || !share_->kv_store() || !table)
    {
        return 0;
    }

    /* Find fulltext indexes */
    for (uint idx = 0; idx < table->s->keys; idx++)
    {
        KEY* key_info = &table->key_info[idx];
        if (!(key_info->flags & HA_FULLTEXT))
        {
            continue;
        }

        /* Process each field in the fulltext index */
        for (uint i = 0; i < key_info->user_defined_key_parts; i++)
        {
            KEY_PART_INFO* key_part = &key_info->key_part[i];
            Field* field = key_part->field;

            /* Get old and new text */
            std::string old_text = old_record ? get_field_text(old_record, field) : "";
            std::string new_text = new_record ? get_field_text(new_record, field) : "";

            std::vector<std::string> old_terms = BM25Index::tokenize(old_text);
            std::vector<std::string> new_terms = BM25Index::tokenize(new_text);

            /* Count term frequencies */
            std::unordered_map<std::string, int> old_tf, new_tf;
            for (const auto& t : old_terms) old_tf[t]++;
            for (const auto& t : new_terms) new_tf[t]++;

            /* Update term frequencies and document frequencies */
            std::string row_id_str = std::to_string(row_id);

            /* Remove old terms */
            for (const auto& pair : old_tf)
            {
                std::string tf_key = "ft:tf:" + pair.first + ":" + row_id_str;
                share_->kv_store()->remove(tf_key);

                /* Decrement document frequency */
                std::string df_key = "ft:df:" + pair.first;
                std::string df_str;
                if (share_->kv_store()->get(df_key, &df_str).ok() &&
                    df_str.size() == sizeof(uint64_t))
                {
                    uint64_t df;
                    memcpy(&df, df_str.data(), sizeof(uint64_t));
                    if (df > 0) df--;
                    df_str.assign(reinterpret_cast<char*>(&df), sizeof(uint64_t));
                    share_->kv_store()->put(df_key, df_str);
                }
            }

            /* Add new terms */
            for (const auto& pair : new_tf)
            {
                std::string tf_key = "ft:tf:" + pair.first + ":" + row_id_str;
                uint32_t tf = static_cast<uint32_t>(pair.second);
                std::string tf_str(reinterpret_cast<char*>(&tf), sizeof(uint32_t));
                share_->kv_store()->put(tf_key, tf_str);

                /* Increment document frequency (only if term is new to this doc) */
                if (old_tf.find(pair.first) == old_tf.end())
                {
                    std::string df_key = "ft:df:" + pair.first;
                    std::string df_str;
                    uint64_t df = 0;
                    if (share_->kv_store()->get(df_key, &df_str).ok() &&
                        df_str.size() == sizeof(uint64_t))
                    {
                        memcpy(&df, df_str.data(), sizeof(uint64_t));
                    }
                    df++;
                    df_str.assign(reinterpret_cast<char*>(&df), sizeof(uint64_t));
                    share_->kv_store()->put(df_key, df_str);
                }
            }

            /* Update document length */
            std::string dl_key = "ft:dl:" + row_id_str;
            uint32_t doc_len = static_cast<uint32_t>(new_terms.size());
            std::string dl_str(reinterpret_cast<char*>(&doc_len), sizeof(uint32_t));
            share_->kv_store()->put(dl_key, dl_str);
        }
    }

    /* Update global stats */
    if (new_record && !old_record)
    {
        /* New document - increment total_docs */
        std::string total_str;
        uint64_t total = 0;
        if (share_->kv_store()->get("ft:total_docs", &total_str).ok() &&
            total_str.size() == sizeof(uint64_t))
        {
            memcpy(&total, total_str.data(), sizeof(uint64_t));
        }
        total++;
        total_str.assign(reinterpret_cast<char*>(&total), sizeof(uint64_t));
        share_->kv_store()->put("ft:total_docs", total_str);
    }

    return 0;
}

int TideSQLHandler::delete_fulltext_index(uint64_t row_id, const uchar* record)
{
    return update_fulltext_index(row_id, record, nullptr);
}

std::string TideSQLHandler::get_field_text(const uchar* record, Field* field)
{
    if (!field || !record) return "";

    if (field->is_null_in_record(record))
    {
        return "";
    }

    /* Get field value as string */
    String buffer;
    field->val_str(&buffer, &buffer);
    return std::string(buffer.ptr(), buffer.length());
}

} /* namespace tidesql */

/* -------------- Plugin Interface (C Linkage) -------------- */

extern "C"
{
    static handlerton* tidesql_hton = nullptr;

    /* -------------- Global Configuration Variables -------------- */

    /* Data directory for TidesDB storage */
    static char* tidesql_data_dir = nullptr;

    /* Compression algorithm: none, lz4, zstd, snappy */
    static char* tidesql_compression = nullptr;

    /* Write buffer size (memtable flush threshold) in bytes */
    static unsigned long long tidesql_write_buffer_size = 64 * 1024 * 1024; /* 64MB */

    /* Block cache size in bytes */
    static unsigned long long tidesql_block_cache_size = 128 * 1024 * 1024; /* 128MB */

    /* Number of flush threads */
    static unsigned int tidesql_flush_threads = 2;

    /* Number of compaction threads */
    static unsigned int tidesql_compaction_threads = 2;

    /* Sync mode: 0=none, 1=interval, 2=full */
    static unsigned int tidesql_sync_mode = 1; /* interval */

    /* Sync interval in microseconds (for interval mode) */
    static unsigned long long tidesql_sync_interval_us = 128000; /* 128ms */

    /* Enable bloom filters */
    static my_bool tidesql_enable_bloom_filter = TRUE;

    /* Bloom filter false positive rate (0.0 - 1.0) */
    static double tidesql_bloom_fpr = 0.01; /* 1% */

    /* Enable block indexes */
    static my_bool tidesql_enable_block_indexes = TRUE;

    /* Maximum open SSTables */
    static unsigned int tidesql_max_open_sstables = 256;

    /* Log level - 0=debug, 1=info, 2=warn, 3=error, 4=fatal, 5=none */
    static unsigned int tidesql_log_level = 1; /* info */

    /* Transaction isolation: 0=READ_UNCOMMITTED, 1=READ_COMMITTED, 2=REPEATABLE_READ, 3=SNAPSHOT,
     * 4=SERIALIZABLE */
    static unsigned int tidesql_txn_isolation = 1; /* READ_COMMITTED */

    /* -------------- Status Variables (Counters) -------------- */

    static unsigned long long tidesql_rows_read = 0;
    static unsigned long long tidesql_rows_written = 0;
    static unsigned long long tidesql_rows_updated = 0;
    static unsigned long long tidesql_rows_deleted = 0;
    static unsigned long long tidesql_tables_opened = 0;
    static unsigned long long tidesql_tables_created = 0;

    /* -------------- Handler Creation -------------- */

    handler* tidesql_create_handler(handlerton* hton, TABLE_SHARE* table, MEM_ROOT* mem_root)
    {
        return new (mem_root) tidesql::TideSQLHandler(hton, table);
    }

    /* -------------- Transaction Support -------------- */

    /*
     * Map MySQL isolation level to TidesDB isolation level
     * MySQL - ISO_READ_UNCOMMITTED=0, ISO_READ_COMMITTED=1, ISO_REPEATABLE_READ=2,
     * ISO_SERIALIZABLE=3 TidesDB - READ_UNCOMMITTED=0, READ_COMMITTED=1, REPEATABLE_READ=2,
     * SNAPSHOT=3, SERIALIZABLE=4
     */
    static int map_mysql_isolation_to_tidesdb(int mysql_iso)
    {
        switch (mysql_iso)
        {
            case 0:
                return 0; /* READ_UNCOMMITTED -> READ_UNCOMMITTED */
            case 1:
                return 1; /* READ_COMMITTED -> READ_COMMITTED */
            case 2:
                return 2; /* REPEATABLE_READ -> REPEATABLE_READ */
            case 3:
                return 4; /* SERIALIZABLE -> SERIALIZABLE */
            default:
                return 1; /* Default to READ_COMMITTED */
        }
    }

    tidesql::TideSQLTrx* get_or_create_trx(THD* thd, handlerton* hton)
    {
        tidesql::TideSQLTrx* trx = static_cast<tidesql::TideSQLTrx*>(thd_get_ha_data(thd, hton));

        if (!trx)
        {
            trx = new tidesql::TideSQLTrx();
            thd_set_ha_data(thd, hton, trx);
        }

        /* We update isolation level from THD's current transaction isolation setting */
        /* This respects SET TRANSACTION ISOLATION LEVEL and SET SESSION tx_isolation */
        int mysql_iso = thd_tx_isolation(thd);
        trx->isolation_level = map_mysql_isolation_to_tidesdb(mysql_iso);

        return trx;
    }

    int tidesql_commit(handlerton* hton, THD* thd, bool all)
    {
        tidesql::TideSQLTrx* trx = static_cast<tidesql::TideSQLTrx*>(thd_get_ha_data(thd, hton));

        if (!trx || !trx->txn)
        {
            return 0; /* No active transaction */
        }

        /* Only commit on full transaction commit (not statement) or autocommit */
        if (all || trx->autocommit)
        {
            KVResult result = trx->txn->commit();
            trx->txn.reset();
            trx->started = false;

            if (!result.ok())
            {
                return HA_ERR_GENERIC;
            }
        }

        return 0;
    }

    int tidesql_rollback(handlerton* hton, THD* thd, bool all)
    {
        tidesql::TideSQLTrx* trx = static_cast<tidesql::TideSQLTrx*>(thd_get_ha_data(thd, hton));

        if (!trx || !trx->txn)
        {
            return 0; /* No active transaction */
        }

        if (all || trx->autocommit)
        {
            trx->txn->rollback();
            trx->txn.reset();
            trx->started = false;
        }

        return 0;
    }

    static int tidesql_close_connection(handlerton* hton, THD* thd)
    {
        tidesql::TideSQLTrx* trx = static_cast<tidesql::TideSQLTrx*>(thd_get_ha_data(thd, hton));

        if (trx)
        {
            if (trx->txn)
            {
                trx->txn->rollback();
                trx->txn.reset();
            }
            delete trx;
            thd_set_ha_data(thd, hton, nullptr);
        }

        return 0;
    }

    /*
     * Handler for START TRANSACTION WITH CONSISTENT SNAPSHOT
     * Creates a snapshot-isolated transaction for consistent reads
     */
    static int tidesql_start_consistent_snapshot(handlerton* hton, THD* thd)
    {
        tidesql::TideSQLTrx* trx = get_or_create_trx(thd, hton);

        /* We force SNAPSHOT isolation for WITH CONSISTENT SNAPSHOT */
        trx->isolation_level = 3; /* TDB_ISOLATION_SNAPSHOT */
        trx->started = true;
        trx->autocommit = false;

        return 0;
    }

    /* -------------- Plugin Initialization -------------- */

    int tidesql_init(void* p)
    {
        tidesql_hton = static_cast<handlerton*>(p);

        tidesql_hton->state = SHOW_OPTION_YES;
        tidesql_hton->create = tidesql_create_handler;
        tidesql_hton->flags = HTON_CAN_RECREATE;

        tidesql_hton->commit = tidesql_commit;
        tidesql_hton->rollback = tidesql_rollback;
        tidesql_hton->close_connection = tidesql_close_connection;
        tidesql_hton->start_consistent_snapshot = tidesql_start_consistent_snapshot;

        if (!tidesql_data_dir || strlen(tidesql_data_dir) == 0)
        {
            tidesql_data_dir = strdup("./tidesql_data");
        }

        if (!tidesql_compression || strlen(tidesql_compression) == 0)
        {
            tidesql_compression = strdup("lz4");
        }

        return 0;
    }

    int tidesql_deinit(void* p)
    {
        return 0;
    }

    /* -------------- System Variable Definitions -------------- */

    static MYSQL_SYSVAR_STR(data_dir, tidesql_data_dir, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                            "Base directory for TidesDB data files", nullptr, /* check */
                            nullptr,                                          /* update */
                            "./tidesql_data");

    static MYSQL_SYSVAR_STR(compression, tidesql_compression,
                            PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                            "Compression algorithm: none, lz4, zstd, snappy", nullptr, nullptr,
                            "lz4");

    static MYSQL_SYSVAR_ULONGLONG(write_buffer_size, tidesql_write_buffer_size, PLUGIN_VAR_RQCMDARG,
                                  "Write buffer (memtable) size in bytes before flush", nullptr,
                                  nullptr, 64 * 1024 * 1024,    /* default: 64MB */
                                  1 * 1024 * 1024,              /* min: 1MB */
                                  1024ULL * 1024 * 1024 * 1024, /* max: 1TB */
                                  1024 * 1024                   /* block size: 1MB */
    );

    static MYSQL_SYSVAR_ULONGLONG(block_cache_size, tidesql_block_cache_size, PLUGIN_VAR_RQCMDARG,
                                  "Block cache size in bytes (0 to disable)", nullptr, nullptr,
                                  128 * 1024 * 1024,            /* default: 128MB */
                                  0,                            /* min: 0 (disabled) */
                                  1024ULL * 1024 * 1024 * 1024, /* max: 1TB */
                                  1024 * 1024                   /* block size: 1MB */
    );

    static MYSQL_SYSVAR_UINT(flush_threads, tidesql_flush_threads, PLUGIN_VAR_RQCMDARG,
                             "Number of background flush threads", nullptr, nullptr,
                             2,  /* default */
                             1,  /* min */
                             64, /* max */
                             1   /* block size */
    );

    static MYSQL_SYSVAR_UINT(txn_isolation, tidesql_txn_isolation, PLUGIN_VAR_RQCMDARG,
                             "Transaction isolation: 0=READ_UNCOMMITTED, 1=READ_COMMITTED, "
                             "2=REPEATABLE_READ, 3=SNAPSHOT, 4=SERIALIZABLE",
                             nullptr, nullptr, 1, 0, 4, 1);

    static MYSQL_SYSVAR_UINT(compaction_threads, tidesql_compaction_threads, PLUGIN_VAR_RQCMDARG,
                             "Number of background compaction threads", nullptr, nullptr,
                             2,  /* default */
                             1,  /* min */
                             64, /* max */
                             1   /* block size */
    );

    static MYSQL_SYSVAR_UINT(sync_mode, tidesql_sync_mode, PLUGIN_VAR_RQCMDARG,
                             "Sync mode: 0=none (fastest), 1=interval (balanced), 2=full (safest)",
                             nullptr, nullptr, 1, /* default: interval */
                             0,                   /* min */
                             2,                   /* max */
                             1                    /* block size */
    );

    static MYSQL_SYSVAR_ULONGLONG(sync_interval, tidesql_sync_interval_us, PLUGIN_VAR_RQCMDARG,
                                  "Sync interval in microseconds (for sync_mode=1)", nullptr,
                                  nullptr, 128000, /* default: 128ms */
                                  1000,            /* min: 1ms */
                                  60000000,        /* max: 60s */
                                  1000             /* block size: 1ms */
    );

    static MYSQL_SYSVAR_BOOL(enable_bloom_filter, tidesql_enable_bloom_filter, PLUGIN_VAR_RQCMDARG,
                             "Enable bloom filters for faster key lookups", nullptr, nullptr, TRUE);

    static MYSQL_SYSVAR_DOUBLE(bloom_fpr, tidesql_bloom_fpr, PLUGIN_VAR_RQCMDARG,
                               "Bloom filter false positive rate (0.001 to 0.1)", nullptr, nullptr,
                               0.01,  /* default: 1% */
                               0.001, /* min: 0.1% */
                               0.1,   /* max: 10% */
                               0      /* no block size for double */
    );

    static MYSQL_SYSVAR_BOOL(enable_block_indexes, tidesql_enable_block_indexes,
                             PLUGIN_VAR_RQCMDARG, "Enable block indexes for faster seeks", nullptr,
                             nullptr, TRUE);

    static MYSQL_SYSVAR_UINT(max_open_sstables, tidesql_max_open_sstables, PLUGIN_VAR_RQCMDARG,
                             "Maximum number of open SSTable file handles", nullptr, nullptr,
                             256,   /* default */
                             16,    /* min */
                             65535, /* max */
                             1      /* block size */
    );

    static MYSQL_SYSVAR_UINT(log_level, tidesql_log_level, PLUGIN_VAR_RQCMDARG,
                             "Log level: 0=debug, 1=info, 2=warn, 3=error, 4=fatal, 5=none",
                             nullptr, nullptr, 1, /* default: info */
                             0,                   /* min */
                             5,                   /* max */
                             1                    /* block size */
    );

    /* -------------- Status Variable Definitions -------------- */

    static struct st_mysql_show_var tidesql_status_vars[] = {
        {"tidesql_rows_read", reinterpret_cast<char*>(&tidesql_rows_read), SHOW_LONGLONG,
         SHOW_SCOPE_GLOBAL},
        {"tidesql_rows_written", reinterpret_cast<char*>(&tidesql_rows_written), SHOW_LONGLONG,
         SHOW_SCOPE_GLOBAL},
        {"tidesql_rows_updated", reinterpret_cast<char*>(&tidesql_rows_updated), SHOW_LONGLONG,
         SHOW_SCOPE_GLOBAL},
        {"tidesql_rows_deleted", reinterpret_cast<char*>(&tidesql_rows_deleted), SHOW_LONGLONG,
         SHOW_SCOPE_GLOBAL},
        {"tidesql_tables_opened", reinterpret_cast<char*>(&tidesql_tables_opened), SHOW_LONGLONG,
         SHOW_SCOPE_GLOBAL},
        {"tidesql_tables_created", reinterpret_cast<char*>(&tidesql_tables_created), SHOW_LONGLONG,
         SHOW_SCOPE_GLOBAL},
        {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_GLOBAL}};

    /* -------------- System Variables Array -------------- */

    static struct st_mysql_sys_var* tidesql_system_vars[] = {MYSQL_SYSVAR(data_dir),
                                                             MYSQL_SYSVAR(compression),
                                                             MYSQL_SYSVAR(write_buffer_size),
                                                             MYSQL_SYSVAR(block_cache_size),
                                                             MYSQL_SYSVAR(flush_threads),
                                                             MYSQL_SYSVAR(compaction_threads),
                                                             MYSQL_SYSVAR(sync_mode),
                                                             MYSQL_SYSVAR(sync_interval),
                                                             MYSQL_SYSVAR(enable_bloom_filter),
                                                             MYSQL_SYSVAR(bloom_fpr),
                                                             MYSQL_SYSVAR(enable_block_indexes),
                                                             MYSQL_SYSVAR(max_open_sstables),
                                                             MYSQL_SYSVAR(log_level),
                                                             MYSQL_SYSVAR(txn_isolation),
                                                             nullptr};

    /* Storage engine plugin declaration */
    struct st_mysql_storage_engine tidesql_storage_engine = {MYSQL_HANDLERTON_INTERFACE_VERSION};

    /*
     * Plugin declaration
     * What MySQL/MariaDB uses to load the plugin
     */
    mysql_declare_plugin(tidesql){MYSQL_STORAGE_ENGINE_PLUGIN,
                                  &tidesql_storage_engine,
                                  "TIDESQL",
                                  "TideSQL Authors",
                                  "TideSQL storage engine with pluggable KV backend",
                                  PLUGIN_LICENSE_GPL,
                                  tidesql_init,
                                  tidesql_deinit,
                                  0x0001,
                                  tidesql_status_vars,
                                  tidesql_system_vars,
                                  nullptr,
                                  0} mysql_declare_plugin_end;

} /* extern "C" */
