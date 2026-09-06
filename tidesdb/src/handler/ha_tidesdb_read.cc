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

/* the point-read path that materializes a stored row into a server buffer: fetch_row_by_pk does a
   transactional get of the data key and decodes it, iter_read_current decodes the row the scan
   cursor is currently positioned on, and probe_pk_exists tests for a primary key without pulling
   the row into the transaction read set.  the decode itself is deserialize_row; this unit only
   drives the library get or iterator and hands the bytes off. */

#include "ha_tidesdb.h"

#include <mysql/plugin.h>

#include <cstring>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/handler/ha_tidesdb_internal.h"

/* ******************** fetch_row_by_pk ******************** */

/*
  Point-lookup a row by its PK bytes (without namespace prefix).
  Sets current_pk + last_row.  Returns 0, HA_ERR_KEY_NOT_FOUND,
  HA_ERR_LOCK_DEADLOCK (on TDB_ERR_CONFLICT), or HA_ERR_LOCK_WAIT_TIMEOUT
  (on TDB_ERR_LOCKED / TDB_ERR_MEMORY_LIMIT).
*/
int ha_tidesdb::fetch_row_by_pk(tidesdb_txn_t *txn, const uchar *pk, uint pk_len, uchar *buf)
{
    uchar dk[DATA_KEY_BUF_LEN];
    uint dk_len = build_data_key(pk, pk_len, dk);

    uint8_t *value = NULL;
    size_t value_size = 0;
    int rc = tidesdb_txn_get(txn, share->cf, dk, dk_len, &value, &value_size);
    if (rc == TDB_ERR_NOT_FOUND) return HA_ERR_KEY_NOT_FOUND;
    if (rc != TDB_SUCCESS) return tdb_rc_to_ha(rc, "fetch_row_by_pk");

    if (likely(!has_blobs_ && !encrypted_))
    {
        /* Zero-copy path, we deserialize directly from API buffer */
        deserialize_row(buf, (const uchar *)value, value_size);
        tidesdb_free(value);
    }
    else
    {
        /* For BLOB tables, Field_blob::unpack() stores pointers into the
           source buffer.  These pointers must remain valid until the next
           fetch into the SAME record buffer.  The MariaDB handler API
           (e.g., mhnsw vector index maintenance) may interleave reads into
           record[0] and record[1], so we maintain two backing buffers:
           last_row for record[0] fetches, last_row2 for record[1] fetches.
           This prevents a fetch into record[1] from invalidating BLOB
           pointers that record[0] still references.

           We identify record[1] using the precomputed bounds set in open(). */
        bool is_rec1 = record1_lo_ && buf >= record1_lo_ && buf < record1_hi_;
        std::string &backing = is_rec1 ? last_row2 : last_row;
        backing.assign((const char *)value, value_size);
        tidesdb_free(value);
        deserialize_row(buf, backing);
    }
    memcpy(current_pk_buf_, pk, pk_len);
    current_pk_len_ = pk_len;

    return 0;
}

/* ******************** compute_row_ttl ******************** */

/*
  Compute the time-to-live for a row being written, expressed as a count of
  seconds from now the way the library expects it.  Priority runs per-row
  TTL_COL value first, then the session override, then the table-level TTL
  option, and finally no expiration.  Returns TIDESDB_TTL_NONE for a row that
  never expires, or a positive number of seconds the row should live.
*/
time_t ha_tidesdb::compute_row_ttl(const uchar *buf)
{
    long long ttl_seconds = 0;

    if (share->ttl_field_idx >= 0)
    {
        Field *f = table->field[share->ttl_field_idx];
        my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(buf - table->record[0]);
        if (!f->is_real_null(ptrdiff))
        {
            f->move_field_offset(ptrdiff);
            ttl_seconds = f->val_int();
            f->move_field_offset(-ptrdiff);
        }
    }

    /* Session TTL override, we use cached value to avoid THDVAR + ha_thd()
       on every row.  The cache is populated once per statement in write_row
       / update_row and invalidated in external_lock(F_UNLCK). */
    if (ttl_seconds <= 0)
    {
        if (cached_sess_ttl_ > 0) ttl_seconds = (long long)cached_sess_ttl_;
    }

    if (ttl_seconds <= 0 && share->default_ttl > 0) ttl_seconds = (long long)share->default_ttl;

    if (ttl_seconds <= 0) return TIDESDB_TTL_NONE;

    /* The library treats the ttl argument as a duration in seconds from now and
       converts it to an absolute deadline itself, so we hand it the relative
       seconds directly.  We still prime cached_time_ here because the commit
       path reads it to stamp update_time without another time() syscall. */
    if (!cached_time_valid_)
    {
        cached_time_ = time(NULL);
        cached_time_valid_ = true;
    }

    return (time_t)ttl_seconds;
}

/* ******************** iter_read_current ******************** */

/*
  Read the current iterator position in the main data CF.
  Skips non-data keys (meta keys).  Sets current_pk + last_row.
  Does not advance the iterator.
*/
int ha_tidesdb::iter_read_current(uchar *buf)
{
    while (scan_iter && tidesdb_iter_valid(scan_iter))
    {
        uint8_t *key = NULL;
        size_t key_size = 0;
        uint8_t *value = NULL;
        size_t value_size = 0;
        if (tidesdb_iter_key_value(scan_iter, &key, &key_size, &value, &value_size) != TDB_SUCCESS)
            return HA_ERR_END_OF_FILE;
        tdb_owned_buf key_g(key), value_g(value);

        if (!is_data_key(key, key_size))
        {
            tidesdb_iter_next(scan_iter);
            continue;
        }

        current_pk_len_ = (uint)(key_size - KEY_NAMESPACE_LEN);
        memcpy(current_pk_buf_, key + KEY_NAMESPACE_LEN, current_pk_len_);

        if (likely(!has_blobs_ && !encrypted_))
        {
            deserialize_row(buf, (const uchar *)value, value_size);
        }
        else
        {
            bool is_rec1 = record1_lo_ && buf >= record1_lo_ && buf < record1_hi_;
            std::string &backing = is_rec1 ? last_row2 : last_row;
            backing.assign((const char *)value, value_size);
            deserialize_row(buf, backing);
        }
        return 0;
    }
    return HA_ERR_END_OF_FILE;
}

/*
  Probe whether a PK already exists for an INSERT/UPDATE uniqueness check
  without recording the read into the write transaction's conflict footprint.

  tidesdb_txn_contains resolves at the write txn's snapshot but does not track
  the read, so an absent-key probe cannot seed read-seq 0 into the first-
  committer-wins reservation base -- a tracking get would, and a stale
  reservation slot left by a prior write of the same key would then trip a
  spurious TDB_ERR_CONFLICT under load.  Uniqueness against a writer that
  commits the same key after our snapshot is still enforced by this insert's
  own reservation at commit time.  The non-tracking check reads at the txn
  snapshot but resolves the txn's own buffered writes first (read-your-own-
  writes), so a same-txn pending insert reads as present and a same-txn delete
  reads as absent -- no separate shadow map of pending keys is needed.
*/
int ha_tidesdb::probe_pk_exists(tidesdb_trx_t *trx, const uchar *dk, uint dk_len)
{
    tidesdb_txn_t *probe_txn = (trx && trx->txn) ? trx->txn : stmt_txn;
    int g = tidesdb_txn_contains(probe_txn, share->cf, dk, dk_len);
    if (g == TDB_SUCCESS) return 1;
    if (g == TDB_ERR_NOT_FOUND) return 0;
    return -tdb_rc_to_ha(g, "probe_pk_exists contains");
}
