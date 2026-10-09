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
   tidesdb owns transaction semantics, locking, and write back-pressure; the one thing the wrappers
   add is a short bounded retry when the library reports transient contention as TDB_ERR_LOCKED,
   which is what the THD parameter is for, so a killed session stops retrying. include after
   ha_tidesdb.h so the library and server types are already visible. */

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

/* The library reports transient contention on its read, iterator and write paths as
   TDB_ERR_LOCKED, which its own contract describes as nothing having been written and asking
   again being the remedy, typically a memtable rotation that outran the pin retries of a single
   call.  Handing that straight to the client as a lock wait timeout turned a few milliseconds of
   internal churn into a failed statement, which is what the mid-statement 1205 errors under a
   contended OLTP run were.  So each wrapper below asks again a bounded number of times with a
   short growing backoff, about sixty milliseconds in all, and stops early when the session is
   killed.  Commit is deliberately not retried, a failed commit leaves the transaction aborted and
   the caller has to roll back. */
static constexpr int TDB_LOCKED_RETRY_MAX = 16;
static constexpr unsigned TDB_LOCKED_BACKOFF_START_US = 50;
/* Sleeps run 50, 100, ... 3200 microseconds and then stay at the cap, about 64 ms over the 16
   retries. */
static constexpr unsigned TDB_LOCKED_BACKOFF_CAP_US = 6400;

template <typename Op>
static inline int tdb_retry_locked(THD *thd, Op &&op)
{
    unsigned wait_us = TDB_LOCKED_BACKOFF_START_US;
    for (int attempt = 0;; attempt++)
    {
        int rc = op();
        if (rc != TDB_ERR_LOCKED || attempt >= TDB_LOCKED_RETRY_MAX) return rc;
        if (thd && thd_killed(thd)) return rc;
        my_sleep(wait_us); /* mysys, portable, microseconds */
        wait_us = wait_us * 2 < TDB_LOCKED_BACKOFF_CAP_US ? wait_us * 2 : TDB_LOCKED_BACKOFF_CAP_US;
    }
}

/* The reserved column family holding uniqueness sentinels, see TIDESDB_UNIQ_SENTINEL_CF.  Created
   on first use the way the galera meta family is; a racer that loses the create sees
   TDB_ERR_EXISTS and simply looks it up.  With create false this only looks it up, for the purge
   paths, which must not bring it into being on a database that never needed it. */
static inline tidesdb_column_family_t *tdb_unique_sentinel_cf(bool create)
{
    if (!tdb_global) return NULL;
    tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, TIDESDB_UNIQ_SENTINEL_CF);
    if (cf || !create) return cf;
    tidesdb_column_family_config_t cfg = tidesdb_default_column_family_config();
    int rc = tidesdb_create_column_family(tdb_global, TIDESDB_UNIQ_SENTINEL_CF, &cfg);
    if (rc != TDB_SUCCESS && rc != TDB_ERR_EXISTS)
    {
        sql_print_error("[TIDESDB] cannot create the uniqueness sentinel column family (err=%d)",
                        rc);
        return NULL;
    }
    return tidesdb_get_column_family(tdb_global, TIDESDB_UNIQ_SENTINEL_CF);
}

/* Drop every sentinel a table wrote, keyed by its data column family name, when the table is
   dropped, truncated or renamed away.  Stale sentinels never affect correctness, a later writer of
   the value overwrites one and a committed occupant below a snapshot never conflicts, so this is
   space hygiene and a failure only warns. */
static inline void tdb_unique_sentinel_purge(const std::string &cf_name)
{
    tidesdb_column_family_t *ucf = tdb_unique_sentinel_cf(false);
    if (!ucf) return;
    std::string prefix = cf_name;
    prefix.push_back('\0');
    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin_with_isolation(tdb_global, TDB_ISOLATION_READ_COMMITTED, &txn) !=
            TDB_SUCCESS ||
        !txn)
        return;
    int rc = tidesdb_txn_delete_prefix(txn, ucf, (const uint8_t *)prefix.data(), prefix.size());
    if (rc == TDB_SUCCESS) rc = tidesdb_txn_commit(txn);
    if (rc != TDB_SUCCESS)
    {
        tidesdb_txn_rollback(txn);
        sql_print_warning("[TIDESDB] could not purge the uniqueness sentinels of '%s' (err=%d)",
                          cf_name.c_str(), rc);
    }
    tidesdb_txn_free(txn);
}

/* Move the connection onto a fresh library transaction at the given level and keep the one it
   leaves in retired_txns, so iterators other handlers still have open under the old one keep
   reading, see the note on retired_txns.  On failure the connection stays where it was. */
static inline int tdb_trx_retire_and_begin(tidesdb_trx_t *trx, tidesdb_isolation_level_t iso)
{
    tidesdb_txn_t *fresh = NULL;
    int rc = tidesdb_txn_begin_with_isolation(tdb_global, iso, &fresh);
    if (rc != TDB_SUCCESS || !fresh) return rc != TDB_SUCCESS ? rc : TDB_ERR_MEMORY;
    trx->retired_txns.push_back(trx->txn);
    trx->txn = fresh;
    return TDB_SUCCESS;
}

/* Free the transactions retired during the statement or transaction that just ended.  A retired
   transaction is either committed or clean, so rolling it back first only settles a clean one. */
static inline void tdb_trx_free_retired(tidesdb_trx_t *trx)
{
    if (!trx) return;
    for (tidesdb_txn_t *t : trx->retired_txns)
    {
        (void)tidesdb_txn_rollback(t);
        tidesdb_txn_free(t);
    }
    trx->retired_txns.clear();
}

static inline int tdb_txn_get_blocking(THD *thd, tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                                       const uint8_t *key, size_t key_size, uint8_t **value,
                                       size_t *value_size)
{
    return tdb_retry_locked(
        thd, [&] { return tidesdb_txn_get(txn, cf, key, key_size, value, value_size); });
}

static inline int tdb_txn_put_blocking(THD *thd, tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                                       const uint8_t *key, size_t key_size, const uint8_t *value,
                                       size_t value_size, time_t ttl)
{
    return tdb_retry_locked(
        thd, [&] { return tidesdb_txn_put(txn, cf, key, key_size, value, value_size, ttl); });
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
    return tdb_retry_locked(
        thd, [&] { return tidesdb_txn_delete_cf(txn, cf, key, key_size, use_single_delete); });
}

/* Whether an iterator call failed outright.  TDB_ERR_NOT_FOUND only means the stream has no entry
   at the position asked for, which a caller handles as the end of the data. */
static inline bool tdb_iter_failed(int rc)
{
    return rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND;
}

/* Seeks retry a transient TDB_ERR_LOCKED, since each repositions from scratch.  Steps have no
   such wrapper, because a failed next or prev does not promise where it left the iterator, so a
   caller treats any failed step as an error. */
static inline int tdb_iter_seek_blocking(THD *thd, tidesdb_iter_t *it, const void *key, size_t len)
{
    return tdb_retry_locked(thd, [&] { return tidesdb_iter_seek(it, (const uint8_t *)key, len); });
}

static inline int tdb_iter_seek_to_first_blocking(THD *thd, tidesdb_iter_t *it)
{
    return tdb_retry_locked(thd, [&] { return tidesdb_iter_seek_to_first(it); });
}

static inline int tdb_iter_seek_to_last_blocking(THD *thd, tidesdb_iter_t *it)
{
    return tdb_retry_locked(thd, [&] { return tidesdb_iter_seek_to_last(it); });
}

static inline int tdb_iter_new_blocking(THD *thd, tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                                        tidesdb_iter_t **out)
{
    return tdb_retry_locked(thd, [&] { return tidesdb_iter_new(txn, cf, out); });
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
    return tdb_retry_locked(
        thd,
        [&] { return tidesdb_iter_new_range(txn, cf, lower, lower_size, upper, upper_size, out); });
}

#endif /* HA_TIDESDB_INTERNAL_H */
