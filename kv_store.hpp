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
#ifndef TIDESQL_KV_STORE_HPP
#define TIDESQL_KV_STORE_HPP

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace tidesql
{

/* Status codes for KV operations */
enum class KVStatus
{
    OK = 0,
    NOT_FOUND,
    CORRUPTION,
    IO_ERROR,
    INVALID_ARGUMENT,
    NOT_SUPPORTED,
    ALREADY_EXISTS,
    BUSY,
    UNKNOWN_ERROR
};

/* Result wrapper for KV operations */
struct KVResult
{
    KVStatus status;
    std::string message;

    KVResult() : status(KVStatus::OK)
    {
    }
    KVResult(KVStatus s) : status(s)
    {
    }
    KVResult(KVStatus s, const std::string& msg) : status(s), message(msg)
    {
    }

    bool ok() const
    {
        return status == KVStatus::OK;
    }
    bool not_found() const
    {
        return status == KVStatus::NOT_FOUND;
    }
};

/* Iterator interface for range scans */
class KVIterator
{
   public:
    virtual ~KVIterator() = default;

    /* Position the iterator at the first key >= target */
    virtual void seek(const std::string& target) = 0;

    /* Position at the first key */
    virtual void seek_to_first() = 0;

    /* Position at the last key */
    virtual void seek_to_last() = 0;

    /* Move to next entry */
    virtual void next() = 0;

    /* Move to previous entry */
    virtual void prev() = 0;

    /* Check if iterator is valid */
    virtual bool valid() const = 0;

    /* Get current key (only valid if valid() is true) */
    virtual std::string key() const = 0;

    /* Get current value (only valid if valid() is true) */
    virtual std::string value() const = 0;

    /* Get any error status */
    virtual KVStatus status() const = 0;
};

/* Write batch for atomic multi-key updates */
class KVWriteBatch
{
   public:
    virtual ~KVWriteBatch() = default;

    /* Add a put operation to the batch */
    virtual void put(const std::string& key, const std::string& value) = 0;

    /* Add a delete operation to the batch */
    virtual void remove(const std::string& key) = 0;

    /* Clear all operations in the batch */
    virtual void clear() = 0;

    /* Get the number of operations in the batch */
    virtual size_t count() const = 0;
};

/* Transaction interface for ACID operations (optional support) */
class KVTransaction
{
   public:
    virtual ~KVTransaction() = default;

    virtual KVResult get(const std::string& key, std::string* value) = 0;
    virtual KVResult put(const std::string& key, const std::string& value) = 0;
    virtual KVResult remove(const std::string& key) = 0;
    virtual KVResult commit() = 0;
    virtual KVResult rollback() = 0;
};

/* Column family / namespace handle */
class KVColumnFamily
{
   public:
    virtual ~KVColumnFamily() = default;
    virtual const std::string& name() const = 0;
};

/* Configuration options for KV store */
struct KVOptions
{
    bool create_if_missing = true;
    bool error_if_exists = false;
    size_t write_buffer_size = 64 * 1024 * 1024; /* 64MB */
    size_t max_open_files = 1000;
    size_t block_cache_size = 128 * 1024 * 1024; /* 128MB */
    bool enable_compression = true;
    std::string compression_type = "snappy";
    bool sync_writes = false;
    int num_threads = 4;
    int flush_threads = 2;
    int compaction_threads = 2;

    /* Transaction isolation (backend-specific)
     * For TidesDB: 0=READ_UNCOMMITTED, 1=READ_COMMITTED, 2=REPEATABLE_READ,
     *             3=SNAPSHOT, 4=SERIALIZABLE
     */
    int txn_isolation = 1;

    /* Sync mode: 0=none, 1=interval, 2=full */
    int sync_mode = 1;
    uint64_t sync_interval_us = 128000; /* 128ms */

    /* Bloom filter settings */
    bool enable_bloom_filter = true;
    double bloom_fpr = 0.01; /* 1% false positive rate */

    /* Block index settings */
    bool enable_block_indexes = true;

    /* Log level: 0=debug, 1=info, 2=warn, 3=error, 4=fatal, 5=none */
    int log_level = 1;

    /* Path for persistent storage (ignored by memory backend) */
    std::string data_dir;
};

/*
 * Abstract KV Store Interface
 *
 * Implement this interface to add a new storage backend.
 * See MemoryKVStore for a reference implementation.
 */
class KVStore
{
   public:
    virtual ~KVStore() = default;

    /* Open/initialize the store with given options */
    virtual KVResult open(const KVOptions& options) = 0;

    /* Close the store and release resources */
    virtual KVResult close() = 0;

    /* Check if the store is open */
    virtual bool is_open() const = 0;

    /* Get value for a key */
    virtual KVResult get(const std::string& key, std::string* value) = 0;

    /* Put a key-value pair */
    virtual KVResult put(const std::string& key, const std::string& value) = 0;

    /* Delete a key */
    virtual KVResult remove(const std::string& key) = 0;

    /* Check if a key exists */
    virtual KVResult exists(const std::string& key, bool* result) = 0;

    /* Create a new write batch */
    virtual std::unique_ptr<KVWriteBatch> create_write_batch() = 0;

    /* Execute a write batch atomically */
    virtual KVResult write(KVWriteBatch* batch) = 0;

    /* Create an iterator for scanning */
    virtual std::unique_ptr<KVIterator> new_iterator() = 0;

    /* Create an iterator for scanning within a column family (if supported) */
    virtual std::unique_ptr<KVIterator> new_iterator(KVColumnFamily* cf)
    {
        (void)cf;
        return new_iterator();
    }

    /* Get an existing column family by name */
    virtual KVResult get_column_family(const std::string& name, KVColumnFamily** cf)
    {
        (void)name;
        (void)cf;
        return KVResult(KVStatus::NOT_SUPPORTED);
    }

    /* Create a column family / namespace */
    virtual KVResult create_column_family(const std::string& name, KVColumnFamily** cf)
    {
        return KVResult(KVStatus::NOT_SUPPORTED);
    }

    /* Drop a column family */
    virtual KVResult drop_column_family(KVColumnFamily* cf)
    {
        return KVResult(KVStatus::NOT_SUPPORTED);
    }

    /* Get with column family */
    virtual KVResult get(KVColumnFamily* cf, const std::string& key, std::string* value)
    {
        return get(key, value);
    }

    /* Put with column family */
    virtual KVResult put(KVColumnFamily* cf, const std::string& key, const std::string& value)
    {
        return put(key, value);
    }

    /* Begin a transaction */
    virtual std::unique_ptr<KVTransaction> begin_transaction()
    {
        return nullptr; /* Not supported by default */
    }

    /* Check if transactions are supported */
    virtual bool supports_transactions() const
    {
        return false;
    }

    /* Flush memtable to disk (no-op for memory backend) */
    virtual KVResult flush()
    {
        return KVResult(KVStatus::OK);
    }

    /* Compact the database */
    virtual KVResult compact()
    {
        return KVResult(KVStatus::OK);
    }

    /* Update runtime configuration for a column family */
    virtual KVResult update_cf_config(KVColumnFamily* cf, const KVOptions& options,
                                      bool persist = true)
    {
        (void)cf;
        (void)options;
        (void)persist;
        return KVResult(KVStatus::NOT_SUPPORTED);
    }

    /* Get approximate size of key range */
    virtual KVResult get_approximate_size(const std::string& start, const std::string& end,
                                          uint64_t* size)
    {
        *size = 0;
        return KVResult(KVStatus::NOT_SUPPORTED);
    }

    /* Get the backend name (e.g., "memory", "rocksdb", "tidesdb") */
    virtual const char* backend_name() const = 0;

    /* Get backend version string */
    virtual std::string backend_version() const
    {
        return "unknown";
    }
};

/* Factory function type for creating KV stores */
using KVStoreFactory = std::function<std::unique_ptr<KVStore>()>;

/* Registry for KV store backends */
class KVStoreRegistry
{
   public:
    static KVStoreRegistry& instance()
    {
        static KVStoreRegistry registry;
        return registry;
    }

    void register_backend(const std::string& name, KVStoreFactory factory)
    {
        factories_[name] = std::move(factory);
    }

    std::unique_ptr<KVStore> create(const std::string& name)
    {
        auto it = factories_.find(name);
        if (it != factories_.end())
        {
            return it->second();
        }
        return nullptr;
    }

    std::vector<std::string> available_backends() const
    {
        std::vector<std::string> names;
        for (const auto& kv : factories_)
        {
            names.push_back(kv.first);
        }
        return names;
    }

   private:
    KVStoreRegistry() = default;
    std::map<std::string, KVStoreFactory> factories_;
};

/* Helper macro for registering backends */
#define REGISTER_KV_BACKEND(name, class_name)                       \
    static bool _kv_registered_##class_name = []()                  \
    {                                                               \
        KVStoreRegistry::instance().register_backend(               \
            name, []() { return std::make_unique<class_name>(); }); \
        return true;                                                \
    }()

} /* namespace tidesql */

#endif /* TIDESQL_KV_STORE_HPP */
