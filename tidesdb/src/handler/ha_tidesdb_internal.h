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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/

#ifndef HA_TIDESDB_INTERNAL_H
#define HA_TIDESDB_INTERNAL_H

#include "src/core/row_format.h"

/* shared plumbing the split handler translation units lean on. these are thin forwarders onto the
   library transaction api, kept as named wrappers so the many historical call sites stay unchanged.
   tidesdb owns transaction semantics, locking, and write back-pressure, so each wrapper is a direct
   pass; the THD parameter is unused now that the library blocks internally rather than the plugin
   retrying. include after ha_tidesdb.h so the library and server types are already visible. */

/* the process-wide tidesdb database handle, opened at plugin init and shared by every column
   family and transaction. defined in ha_tidesdb.cc. */
extern tidesdb_t *tdb_global;

/* the resolved data-home directory the database was opened at. defined in ha_tidesdb.cc. */
extern std::string tdb_path;

/* the plugin's handlerton, allocated by the server at init and shared by the transaction callbacks
   that pass it back to the server (start_consistent_snapshot, external_lock). defined in
   ha_tidesdb.cc. */
extern handlerton *tidesdb_hton;

/**
 * tdb_rc_to_ha
 * map a tidesdb library result code to a mariadb handler error code, routing a commit-time write
 * conflict to HA_ERR_LOCK_DEADLOCK so the server's retry logic engages instead of surfacing an
 * opaque HA_ERR_GENERIC, and transient contention and memtable backpressure to
 * HA_ERR_LOCK_WAIT_TIMEOUT
 * @param rc the library result code
 * @param ctx a short call-site tag for the log line on an unexpected error
 * @return the mapped HA_ERR_* code (0 for TDB_SUCCESS)
 */
int tdb_rc_to_ha(int rc, const char *ctx);

/* a one-byte zero value stored for secondary-index entries, whose meaning is carried entirely by
   the key; its address and single-byte size are all that matter, so each translation unit having
   its own copy is fine. */
static const uint8_t tdb_empty_val = 0;

/* named forwarders onto the row-format core: hidden-pk rows are keyed by an 8-byte big-endian
   uint64 so memcmp on the bytes matches numeric row-id order, and a data key is one that starts
   with the KEY_NS_DATA namespace byte. */
static inline void encode_be64(uint64_t id, uint8_t *buf)
{
    tidesdb::row_format::encode_be64(id, buf);
}

static inline uint64_t decode_be64(const uint8_t *buf)
{
    return tidesdb::row_format::decode_be64(buf);
}

static inline bool is_data_key(const uint8_t *key, size_t key_size)
{
    return tidesdb::row_format::is_data_key(key, key_size);
}

static inline int tidesdb_txn_delete_cf(tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                                        const uint8_t *key, size_t key_size, bool use_single_delete)
{
    return use_single_delete ? tidesdb_txn_single_delete(txn, cf, key, key_size)
                             : tidesdb_txn_delete(txn, cf, key, key_size);
}

static inline int tdb_txn_put_blocking(THD *thd, tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                                       const uint8_t *key, size_t key_size, const uint8_t *value,
                                       size_t value_size, time_t ttl)
{
    (void)thd;
    return tidesdb_txn_put(txn, cf, key, key_size, value, value_size, ttl);
}

static inline int tdb_txn_commit_blocking(THD *thd, tidesdb_txn_t *txn)
{
    (void)thd;
    return tidesdb_txn_commit(txn);
}

static inline int tdb_txn_delete_cf_blocking(THD *thd, tidesdb_txn_t *txn,
                                             tidesdb_column_family_t *cf, const uint8_t *key,
                                             size_t key_size, bool use_single_delete)
{
    (void)thd;
    return tidesdb_txn_delete_cf(txn, cf, key, key_size, use_single_delete);
}

static inline int tdb_iter_new_blocking(THD *thd, tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                                        tidesdb_iter_t **out)
{
    (void)thd;
    return tidesdb_iter_new(txn, cf, out);
}

/* Range-bounded iterator.  Opens only the sstables whose own key range can
   overlap [lower, upper], so a scan of a narrow band costs what that band
   costs rather than what the whole column family costs, which is what lets
   concurrent range scans scale. */
static inline int tdb_iter_new_range_blocking(THD *thd, tidesdb_txn_t *txn,
                                              tidesdb_column_family_t *cf, const uint8_t *lower,
                                              size_t lower_size, const uint8_t *upper,
                                              size_t upper_size, tidesdb_iter_t **out)
{
    (void)thd;
    return tidesdb_iter_new_range(txn, cf, lower, lower_size, upper, upper_size, out);
}

#endif /* HA_TIDESDB_INTERNAL_H */
