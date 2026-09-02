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

/* the table-scan and cursor-lifecycle handler surface: rnd_init/rnd_next/rnd_end drive a full
   column-family sweep, position/rnd_pos stash a row id and re-fetch that exact row later,
   index_init/index_end open and close the ordered index cursor that the index-read methods in
   ha_tidesdb_scan's sibling ha_tidesdb_index unit then walk, and spatial_scan_next advances a
   hilbert-ordered spatial range scan.  ordering, snapshot visibility, and read-your-own-writes
   merge all live in the library iterator; this unit sets the cursor up and materializes each hit
   back into a server row. */

#include "ha_tidesdb.h"

#include <ft_global.h>
#include <mysql/plugin.h>

#include <cstring>
#include <string>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/core/rir.h"
#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_keycodec.h"
#include "src/handler/ha_tidesdb_spatial.h"

/* ******************** Table scan (SELECT) ******************** */

int ha_tidesdb::rnd_init(bool scan)
{
    DBUG_ENTER("ha_tidesdb::rnd_init");

    current_pk_len_ = 0;
    scan_dir_ = DIR_NONE;
    scan_range_valid_ = false; /* full table scan is unbounded */

    /* Lazy txn, we ensure stmt_txn exists */
    {
        int erc = ensure_stmt_txn();
        if (erc) DBUG_RETURN(erc);
    }
    scan_txn = stmt_txn;

    /* We use cached trx pointer (set in external_lock) to avoid
       ha_thd() virtual dispatch + thd_get_ha_data() hash lookup
       on every scan init -- this is a hot path in nested-loop joins. */
    uint64_t cur_gen = cached_trx_ ? cached_trx_->txn_generation : 0;

    if (scan_iter &&
        (scan_iter_cf_ != share->cf || scan_iter_txn_ != scan_txn || scan_iter_txn_gen_ != cur_gen))
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_cf_ = NULL;
        scan_iter_txn_ = NULL;
    }

    if (!scan_iter)
    {
        int rc = tdb_iter_new_blocking(ha_thd(), scan_txn, share->cf, &scan_iter);
        if (rc != TDB_SUCCESS)
        {
            scan_txn = NULL;
            DBUG_RETURN(tdb_rc_to_ha(rc, "rnd_init txn_begin"));
        }
        scan_iter_cf_ = share->cf;
        scan_iter_txn_ = scan_txn;
        scan_iter_txn_gen_ = cur_gen;
    }

    uint8_t data_prefix = KEY_NS_DATA;
    tidesdb_iter_seek(scan_iter, &data_prefix, 1);

    DBUG_RETURN(0);
}

int ha_tidesdb::rnd_end()
{
    DBUG_ENTER("ha_tidesdb::rnd_end");

    /* We do not free scan_iter, we keep cached for reuse within this statement.
       Iterator is freed in external_lock(F_UNLCK) or close(). */
    scan_txn = NULL;

    DBUG_RETURN(0);
}

int ha_tidesdb::rnd_next(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::rnd_next");

    if (cached_thd_ && thd_killed(cached_thd_)) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

    /* We advance past the last-read entry.  on the first call after rnd_init
     * the iterator is already positioned at the first data key by the seek
     * in rnd_init, so we skip the advance (scan_dir_ == DIR_NONE). */
    if (scan_dir_ != DIR_NONE) tidesdb_iter_next(scan_iter);

    int ret = iter_read_current(buf);
    if (ret == 0) scan_dir_ = DIR_FORWARD;

    DBUG_RETURN(ret);
}

/* ******************** position / rnd_pos ******************** */

void ha_tidesdb::position(const uchar *record)
{
    DBUG_ENTER("ha_tidesdb::position");
    /* The ref must identify the given record, not whichever row the plugin last
       read.  Row-based replication and multi-pass reads call position() on a
       record the engine did not just fetch, notably the binlog before-image, so
       we derive the primary key straight from record.  A hidden-rowid table
       cannot rebuild its rowid from the columns, so it keeps the last read key,
       which the server only trusts for a declared primary key anyway. */
    if (share->has_user_pk)
    {
        KEY *pk = &table->key_info[share->pk_index];
        make_comparable_key(pk, record, pk->user_defined_key_parts, ref);
    }
    else
    {
        memcpy(ref, current_pk_buf_, current_pk_len_);
    }
    DBUG_VOID_RETURN;
}

int ha_tidesdb::rnd_pos(uchar *buf, uchar *pos)
{
    DBUG_ENTER("ha_tidesdb::rnd_pos");

    /* Lazy txn, we ensure stmt_txn exists */
    {
        int erc = ensure_stmt_txn();
        if (erc) DBUG_RETURN(erc);
    }

    int ret = fetch_row_by_pk(stmt_txn, pos, ref_length, buf);
    DBUG_RETURN(ret);
}

/* ******************** Index scan ******************** */

int ha_tidesdb::index_init(uint idx, bool sorted)
{
    DBUG_ENTER("ha_tidesdb::index_init");
    active_index = idx;
    idx_pk_exact_done_ = false;
    scan_dir_ = DIR_NONE;
    spatial_scan_active_ = false;
    scan_range_valid_ = false; /* set only by read_range_first when a range is known */
    /* Cache is_pk for the duration of the scan so navigation methods can
       read a member instead of re-deriving the answer per row. */
    is_pk_ = share->has_user_pk && idx == share->pk_index;

    {
        int erc = ensure_stmt_txn();
        if (erc) DBUG_RETURN(erc);
    }
    scan_txn = stmt_txn;

    tidesdb_column_family_t *target_cf;
    if (share->has_user_pk && idx == share->pk_index)
        target_cf = share->cf;
    else if (idx < share->idx_cfs.size() && share->idx_cfs[idx])
        target_cf = share->idx_cfs[idx];
    else
    {
        scan_txn = NULL;
        scan_cf_ = NULL;
        sql_print_error("[TIDESDB] index_init: no CF for index %u", idx);
        DBUG_RETURN(HA_ERR_GENERIC);
    }

    scan_cf_ = target_cf;

    /* We reuse cached iterator if it belongs to the same CF and same txn.
       In nested-loop joins, index_init/index_end cycle N times on the
       same index; reusing the iterator avoids N expensive iter_new() calls
       (each builds a merge heap from all SSTables).

       If the txn changed (e.g. after COMMIT created a new one), the
       iterator holds a stale txn pointer and must be recreated.
       We compare both the pointer and a monotonic generation counter
       because the allocator can reuse the same address for a new txn.

       We use cached_trx_ (set in external_lock) to avoid ha_thd() virtual
       dispatch + thd_get_ha_data() hash lookup on every iteration of
       the outer loop in nested-loop joins. */
    uint64_t cur_gen = cached_trx_ ? cached_trx_->txn_generation : 0;

    if (scan_iter &&
        (scan_iter_cf_ != target_cf || scan_iter_txn_ != scan_txn || scan_iter_txn_gen_ != cur_gen))
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_cf_ = NULL;
        scan_iter_txn_ = NULL;
    }
    /* If scan_iter is non-NULL here, ensure_scan_iter() will reuse it. */

    DBUG_RETURN(0);
}

/*
  Lazily create the scan iterator from scan_cf_ when first needed.
  Returns 0 on success or a handler error code.
*/
int ha_tidesdb::ensure_scan_iter()
{
    if (scan_iter) return 0;

    /* If a prior attempt with this exact (scan_cf_, scan_txn) combination
       already failed, short-circuit instead of re-logging and re-failing.
       The cache is invalidated whenever the caller changes scan_cf_ or
       scan_txn (natural since those moves imply a new attempt). */
    if (scan_iter_last_err_ && scan_iter_last_err_cf_ == scan_cf_ &&
        scan_iter_last_err_txn_ == scan_txn)
        return scan_iter_last_err_;

    if (!scan_txn || !scan_cf_)
    {
        sql_print_error("[TIDESDB] ensure_scan_iter: no txn or CF");
        scan_iter_last_err_ = HA_ERR_GENERIC;
        scan_iter_last_err_cf_ = scan_cf_;
        scan_iter_last_err_txn_ = scan_txn;
        return HA_ERR_GENERIC;
    }
    int rc;
    if (scan_range_valid_)
        rc = tdb_iter_new_range_blocking(ha_thd(), scan_txn, scan_cf_, scan_range_lo_,
                                         scan_range_lo_len_, scan_range_hi_, scan_range_hi_len_,
                                         &scan_iter);
    else
        rc = tdb_iter_new_blocking(ha_thd(), scan_txn, scan_cf_, &scan_iter);
    if (rc == TDB_SUCCESS)
    {
        scan_iter_cf_ = scan_cf_;
        scan_iter_txn_ = scan_txn;
        scan_iter_txn_gen_ = cached_trx_ ? cached_trx_->txn_generation : 0;
        scan_iter_last_err_ = 0;
        return 0;
    }
    int herr = tdb_rc_to_ha(rc, "ensure_scan_iter");
    scan_iter_last_err_ = herr;
    scan_iter_last_err_cf_ = scan_cf_;
    scan_iter_last_err_txn_ = scan_txn;
    return herr;
}

int ha_tidesdb::index_end()
{
    DBUG_ENTER("ha_tidesdb::index_end");

    scan_txn = NULL;
    active_index = MAX_KEY;
    spatial_scan_active_ = false;
    pk_partial_exact_active_ = false;
    scan_range_valid_ = false;

    DBUG_RETURN(0);
}

/*
  Range scan entry point.  MariaDB hands us both bounds, so we encode them the
  same way records_in_range does for tidesdb_range_stats -- which is the same
  span tidesdb_iter_new_range prunes sstables against -- and flag the scan so
  ensure_scan_iter builds a range-bounded iterator instead of one over the whole
  column family.  The old iterator is freed so this range gets one matching its
  own bounds rather than reusing a prior range's.  The server stops the scan at
  end_range, so we never step past the iterator's upper bound.  Point lookups and
  full scans do not come through here and stay unbounded.
*/
int ha_tidesdb::read_range_first(const key_range *start_key, const key_range *end_key,
                                 bool eq_range_arg, bool sorted)
{
    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_last_err_ = 0;
    }
    uint lo_len = 0, hi_len = 0;
    rir_encode_bounds(active_index, is_pk_, start_key, end_key, scan_range_lo_, lo_len,
                      scan_range_hi_, hi_len);
    scan_range_lo_len_ = lo_len;
    scan_range_hi_len_ = hi_len;
    scan_range_valid_ = (lo_len > 0 && hi_len > 0);

    return handler::read_range_first(start_key, end_key, eq_range_arg, sorted);
}

/* ******************** Spatial scan continuation ******************** */

int ha_tidesdb::spatial_scan_next(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::spatial_scan_next");

    tdb_mbr_t query_mbr;
    query_mbr.xmin = spatial_qmbr_[MBR_XMIN_IDX];
    query_mbr.ymin = spatial_qmbr_[MBR_YMIN_IDX];
    query_mbr.xmax = spatial_qmbr_[MBR_XMAX_IDX];
    query_mbr.ymax = spatial_qmbr_[MBR_YMAX_IDX];

    while (spatial_range_idx_ < spatial_ranges_.size())
    {
        uint64_t cur_hi = spatial_ranges_[spatial_range_idx_].second;

        while (tidesdb_iter_valid(scan_iter))
        {
            if (cached_thd_ && thd_killed(cached_thd_)) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

            uint8_t *ik = NULL;
            size_t iks = 0;
            if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS) break;
            tdb_owned_buf ik_g(ik);

            if (iks <= SPATIAL_HILBERT_KEY_LEN)
            {
                tidesdb_iter_next(scan_iter);
                continue;
            }

            uint64_t h = decode_hilbert_be(ik);
            if (h > cur_hi) break; /* advance to next range */

            uint8_t *val = NULL;
            size_t vlen = 0;
            if (tidesdb_iter_value(scan_iter, &val, &vlen) != TDB_SUCCESS ||
                vlen < SPATIAL_MBR_VALUE_LEN)
            {
                tidesdb_iter_next(scan_iter);
                continue;
            }
            tdb_owned_buf val_g(val);

            /* The on-disk spatial value is exactly SPATIAL_MBR_VALUE_LEN bytes
               laid out as [xmin,ymin,xmax,ymax] (4 doubles in native order),
               matching tdb_mbr_t's field order.  We assert the struct size
               against the wire size so adding a field to tdb_mbr_t will
               fire the static_assert rather than silently corrupt reads. */
            static_assert(sizeof(tdb_mbr_t) == SPATIAL_MBR_VALUE_LEN,
                          "tdb_mbr_t must match on-disk spatial value layout");
            tdb_mbr_t entry_mbr;
            memcpy(&entry_mbr, val, SPATIAL_MBR_VALUE_LEN);

            /* We apply MBR predicate */
            if (!spatial_mbr_predicate(spatial_mode_, &query_mbr, &entry_mbr))
            {
                tidesdb_iter_next(scan_iter);
                continue;
            }

            /* A match, we extract PK from key suffix and fetch full row */
            const uchar *pk = ik + SPATIAL_HILBERT_KEY_LEN;
            uint pk_len = (uint)(iks - SPATIAL_HILBERT_KEY_LEN);

            int ret = fetch_row_by_pk(scan_txn, pk, pk_len, buf);
            if (ret == HA_ERR_KEY_NOT_FOUND)
            {
                tidesdb_iter_next(scan_iter);
                continue;
            }
            if (ret)
            {
                table->status = STATUS_NOT_FOUND;
                DBUG_RETURN(ret);
            }

            scan_dir_ = DIR_FORWARD;
            table->status = 0;
            DBUG_RETURN(0);
        }

        /* The current range exhausted, thus we advance to next range and seek */
        spatial_range_idx_++;
        if (spatial_range_idx_ < spatial_ranges_.size())
        {
            uchar seek_key[SPATIAL_HILBERT_KEY_LEN];
            encode_hilbert_be(spatial_ranges_[spatial_range_idx_].first, seek_key);
            tidesdb_iter_seek(scan_iter, seek_key, SPATIAL_HILBERT_KEY_LEN);
        }
    }

    table->status = STATUS_NOT_FOUND;
    DBUG_RETURN(HA_ERR_END_OF_FILE);
}
