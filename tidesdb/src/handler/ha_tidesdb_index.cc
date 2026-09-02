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

/* the ordered index-read handler surface: index_read_map seeks the cursor to the first key matching
   a lookup and its find-flag, index_next/prev/first/last walk the index in either direction,
   index_next_same continues a same-key group scan, and multi_range_read_next drives a batch of
   ranges through one cursor.  key comparison, snapshot visibility, and read-your-own-writes all live
   in the library iterator; this unit builds the seek keys and materializes each hit into a row. */

#include "ha_tidesdb.h"

#include <ft_global.h>
#include <mysql/plugin.h>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"

#include <cstring>
#include <string>

#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_keycodec.h"
#include "src/handler/ha_tidesdb_spatial.h"
int ha_tidesdb::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                               enum ha_rkey_function find_flag)
{
    DBUG_ENTER("ha_tidesdb::index_read_map");

    /* key_copy_to_comparable uses key_restore + make_comparable_key,
       which reads fields via make_sort_key_part. */
    MY_BITMAP *old_map = tmp_use_all_columns(table, &table->read_set);

    uint key_len = calculate_key_len(table, active_index, key, keypart_map);

    /* We convert the key_copy-format search key straight into idx_search_comp_,
       the buffer index_next_same reuses, so the point lookup needs no separate
       staging buffer or copy.  This still runs before the read_set is restored
       because key_copy_to_comparable reads the key fields. */
    KEY *ki = &table->key_info[active_index];
    uint comp_len = key_copy_to_comparable(ki, key, key_len, idx_search_comp_);
    idx_search_comp_len_ = comp_len;

    tmp_restore_column_map(&table->read_set, old_map);

    /* Reset by default; only the partial-PK exact branch re-sets it. */
    pk_partial_exact_active_ = false;

    if (is_pk_) DBUG_RETURN(index_read_pk(buf, idx_search_comp_, comp_len, find_flag));

    if (is_spatial_index(ki) && find_flag >= HA_READ_MBR_CONTAIN &&
        find_flag <= HA_READ_MBR_EQUAL)
        DBUG_RETURN(index_read_spatial(buf, key, find_flag));

    DBUG_RETURN(index_read_secondary(buf, idx_search_comp_, comp_len, find_flag));
}

int ha_tidesdb::index_read_pk(uchar *buf, const uchar *comp_key, uint comp_len,
                              enum ha_rkey_function find_flag)
{
    if (find_flag == HA_READ_KEY_EXACT)
    {
        uint full_pk_comp_len = share->idx_comp_key_len[share->pk_index];
        if (comp_len >= full_pk_comp_len)
        {
            /* Full PK match, point lookup only, no iterator needed.  This is the
               hot OLTP point-select path, and fetch_row_by_pk builds its own data
               key from comp_key, so we do not build a seek key here that this
               branch would never read. */
            int ret = fetch_row_by_pk(scan_txn, comp_key, comp_len, buf);
            if (ret == 0) idx_pk_exact_done_ = true;
            return ret;
        }

        /* Partial PK prefix (e.g. first column of composite PK).
           We need an iterator-based prefix scan -- seek to the first
           matching data key and let index_next_same iterate through
           all entries sharing this prefix. */
        uchar seek_key[DATA_KEY_BUF_LEN];
        uint seek_len = build_data_key(comp_key, comp_len, seek_key);
        int irc = ensure_scan_iter();
        if (irc) return irc;
        tidesdb_iter_seek(scan_iter, seek_key, seek_len);
        pk_partial_exact_active_ = true;
        int ret = iter_read_current(buf);
        if (ret == 0) scan_dir_ = DIR_FORWARD;
        return ret;
    }

    /* All other PK scan modes need the iterator and a seek key */
    uchar seek_key[DATA_KEY_BUF_LEN];
    uint seek_len = build_data_key(comp_key, comp_len, seek_key);
    int irc = ensure_scan_iter();
    if (irc) return irc;

    if (find_flag == HA_READ_KEY_OR_NEXT || find_flag == HA_READ_AFTER_KEY)
    {
        tidesdb_iter_seek(scan_iter, seek_key, seek_len);

        if (find_flag == HA_READ_AFTER_KEY && tidesdb_iter_valid(scan_iter))
        {
            uint8_t *ik = NULL;
            size_t iks = 0;
            tdb_owned_buf ik_g(ik);
            if (tidesdb_iter_key(scan_iter, &ik, &iks) == TDB_SUCCESS && iks == seek_len &&
                memcmp(ik, seek_key, iks) == 0)
                tidesdb_iter_next(scan_iter);
        }

        int ret = iter_read_current(buf);
        if (ret == 0) scan_dir_ = DIR_FORWARD;
        return ret;
    }
    else if (find_flag == HA_READ_KEY_OR_PREV || find_flag == HA_READ_BEFORE_KEY ||
             find_flag == HA_READ_PREFIX_LAST || find_flag == HA_READ_PREFIX_LAST_OR_PREV)
    {
        tidesdb_iter_seek_for_prev(scan_iter, seek_key, seek_len);
        if (find_flag == HA_READ_BEFORE_KEY && tidesdb_iter_valid(scan_iter))
        {
            uint8_t *ik = NULL;
            size_t iks = 0;
            tdb_owned_buf ik_g(ik);
            if (tidesdb_iter_key(scan_iter, &ik, &iks) == TDB_SUCCESS && iks == seek_len &&
                memcmp(ik, seek_key, iks) == 0)
                tidesdb_iter_prev(scan_iter);
        }

        int ret = iter_read_current(buf);
        if (ret == 0) scan_dir_ = DIR_BACKWARD;
        return ret;
    }

    /* Fallback is to seek forward */
    tidesdb_iter_seek(scan_iter, seek_key, seek_len);
    int ret = iter_read_current(buf);
    if (ret == 0) scan_dir_ = DIR_FORWARD;
    return ret;
}

int ha_tidesdb::index_read_spatial(uchar *buf, const uchar *key, enum ha_rkey_function find_flag)
{
    /* Spatial index MBR query, hilbert range scan with MBR post-filter */
    tdb_mbr_t qmbr;
    spatial_parse_query_mbr(key, &qmbr);
    spatial_qmbr_[MBR_XMIN_IDX] = qmbr.xmin;
    spatial_qmbr_[MBR_YMIN_IDX] = qmbr.ymin;
    spatial_qmbr_[MBR_XMAX_IDX] = qmbr.xmax;
    spatial_qmbr_[MBR_YMAX_IDX] = qmbr.ymax;
    spatial_mode_ = find_flag;

    spatial_scan_active_ = true;

    int irc = ensure_scan_iter();
    if (irc) return irc;

    /* We decompose the query box into hilbert curve ranges.
       For DISJOINT, we must scan everything (disjoint entries
       can be anywhere on the curve). For other predicates,
       we compute a tight set of ranges covering only the cells
       that overlap the query box. */
    if (find_flag == HA_READ_MBR_DISJOINT)
    {
        spatial_ranges_.clear();
        spatial_ranges_.push_back({HILBERT_RANGE_FULL_LO, HILBERT_RANGE_FULL_HI});
    }
    else
    {
        uint32_t qx0 = double_to_lex_uint32(qmbr.xmin);
        uint32_t qy0 = double_to_lex_uint32(qmbr.ymin);
        uint32_t qx1 = double_to_lex_uint32(qmbr.xmax);
        uint32_t qy1 = double_to_lex_uint32(qmbr.ymax);
        spatial_decompose_ranges(qx0, qy0, qx1, qy1, spatial_ranges_);
    }
    spatial_range_idx_ = 0;

    if (!spatial_ranges_.empty())
    {
        uchar seek_key[SPATIAL_HILBERT_KEY_LEN];
        encode_hilbert_be(spatial_ranges_[0].first, seek_key);
        tidesdb_iter_seek(scan_iter, seek_key, SPATIAL_HILBERT_KEY_LEN);
    }

    return spatial_scan_next(buf);
}

void ha_tidesdb::index_seek_secondary(const uchar *comp_key, uint comp_len,
                                      enum ha_rkey_function find_flag)
{
    if (find_flag == HA_READ_KEY_EXACT || find_flag == HA_READ_KEY_OR_NEXT)
    {
        tidesdb_iter_seek(scan_iter, comp_key, comp_len);
    }
    else if (find_flag == HA_READ_AFTER_KEY)
    {
        /* Skip every entry sharing this index-column prefix in one seek
           instead of stepping over them one at a time.  Secondary entries
           are [comparable idx cols][pk], so the greatest entry with this
           prefix is comp_key followed by the maximum pk (all 0xFF), the same
           upper bound the PREV branch below builds.  Seeking to it lands on
           the first strictly-greater index value -- the only prefix match it
           can land on is the pathological row whose pk itself encodes to all
           0xFF, which we then step past. */
        uchar upper[SEC_IDX_KEY_BUF_LEN];
        memcpy(upper, comp_key, comp_len);
        memset(upper + comp_len, KEY_INF_HI_BYTE, share->pk_key_len);
        uint upper_len = comp_len + share->pk_key_len;
        tidesdb_iter_seek(scan_iter, upper, upper_len);
        if (tidesdb_iter_valid(scan_iter))
        {
            uint8_t *ik = NULL;
            size_t iks = 0;
            tdb_owned_buf ik_g(ik);
            if (tidesdb_iter_key(scan_iter, &ik, &iks) == TDB_SUCCESS && iks >= comp_len &&
                memcmp(ik, comp_key, comp_len) == 0)
                tidesdb_iter_next(scan_iter);
        }
    }
    else if (find_flag == HA_READ_KEY_OR_PREV || find_flag == HA_READ_BEFORE_KEY ||
             find_flag == HA_READ_PREFIX_LAST || find_flag == HA_READ_PREFIX_LAST_OR_PREV)
    {
        /* We build upper bound, comp_key with all 0xFF appended for pk portion */
        uchar upper[SEC_IDX_KEY_BUF_LEN];
        memcpy(upper, comp_key, comp_len);
        memset(upper + comp_len, KEY_INF_HI_BYTE, share->pk_key_len);
        uint upper_len = comp_len + share->pk_key_len;
        tidesdb_iter_seek_for_prev(scan_iter, upper, upper_len);
    }
    else
    {
        tidesdb_iter_seek(scan_iter, comp_key, comp_len);
    }
}

int ha_tidesdb::index_read_secondary(uchar *buf, const uchar *comp_key, uint comp_len,
                                     enum ha_rkey_function find_flag)
{
    int irc = ensure_scan_iter();
    if (irc) return irc;

    index_seek_secondary(comp_key, comp_len, find_flag);

    /* We read the current entry from the secondary index.
       ICP loop, we evaluate pushed index condition before the expensive
       PK point-lookup.  Entries that fail the condition are skipped
       without touching the data CF (same pattern as InnoDB). */
    bool is_backward = (find_flag == HA_READ_KEY_OR_PREV || find_flag == HA_READ_BEFORE_KEY ||
                        find_flag == HA_READ_PREFIX_LAST || find_flag == HA_READ_PREFIX_LAST_OR_PREV);

    uint idx_col_len = share->idx_comp_key_len[active_index];

    for (;;)
    {
        if (!tidesdb_iter_valid(scan_iter)) return HA_ERR_KEY_NOT_FOUND;

        uint8_t *ik = NULL;
        size_t iks = 0;
        if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS) return HA_ERR_KEY_NOT_FOUND;
        tdb_owned_buf ik_g(ik);

        /* For EXACT match, we verify the index prefix matches */
        if (find_flag == HA_READ_KEY_EXACT)
        {
            if (iks < comp_len || memcmp(ik, comp_key, comp_len) != 0) return HA_ERR_KEY_NOT_FOUND;
        }

        if (iks <= idx_col_len) return HA_ERR_KEY_NOT_FOUND;

        /* ICP -- we evaluate pushed condition on index columns before PK lookup */
        check_result_t icp = icp_check_secondary(ik, iks, active_index, buf);
        if (icp == CHECK_NEG)
        {
            if (is_backward)
                tidesdb_iter_prev(scan_iter);
            else
                tidesdb_iter_next(scan_iter);
            continue; /* skip this entry */
        }
        if (icp == CHECK_OUT_OF_RANGE) return HA_ERR_END_OF_FILE;
        if (icp == CHECK_ABORTED_BY_USER) return HA_ERR_ABORTED_BY_USER;

        /* CHECK_POS -- condition satisfied (or ICP not applicable) */
        int ret;
        if (keyread_only_ && try_keyread_from_index(ik, iks, active_index, buf))
            ret = 0;
        else
            ret = fetch_row_by_pk(scan_txn, ik + idx_col_len, (uint)(iks - idx_col_len), buf);
        if (ret == 0) scan_dir_ = is_backward ? DIR_BACKWARD : DIR_FORWARD;
        return ret;
    }
}

int ha_tidesdb::index_next(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_next");

    if (cached_thd_ && thd_killed(cached_thd_)) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

    /* Spatial idx continuation */
    if (spatial_scan_active_)
    {
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);
        if (scan_dir_ != DIR_NONE) tidesdb_iter_next(scan_iter);
        DBUG_RETURN(spatial_scan_next(buf));
    }

    if (idx_pk_exact_done_)
    {
        idx_pk_exact_done_ = false;
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);
        uchar seek_key[DATA_KEY_BUF_LEN];
        uint seek_len = build_data_key(current_pk_buf_, current_pk_len_, seek_key);
        tidesdb_iter_seek(scan_iter, seek_key, seek_len);
        if (tidesdb_iter_valid(scan_iter)) tidesdb_iter_next(scan_iter);
        /* iterator is now past the PK exact match -- advance+read below */
    }
    else
    {
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);
        /* We advance past the last-read entry (iterator stays at current
         * with no pre-advance).  On the first call after index_first
         * sets DIR_NONE, the iterator is already at the correct position
         * so we must not advance. */
        if (scan_dir_ != DIR_NONE) tidesdb_iter_next(scan_iter);
    }

    if (is_pk_)
    {
        int ret = iter_read_current(buf);
        if (ret == 0 && pk_partial_exact_active_ && idx_search_comp_len_ > 0)
        {
            /* Continuation of a partial-PK HA_READ_KEY_EXACT scan, the
               iterator might have stepped past the prefix.  Validate the
               PK still starts with the original search bytes; if not the
               scan is finished.  index_next_same already does this; the
               PK branch never did, so a plan that called index_next
               (not index_next_same) after a partial-PK exact seek would
               return rows from beyond the requested prefix. */
            if (current_pk_len_ < idx_search_comp_len_ ||
                memcmp(current_pk_buf_, idx_search_comp_, idx_search_comp_len_) != 0)
            {
                DBUG_RETURN(HA_ERR_END_OF_FILE);
            }
        }
        scan_dir_ = DIR_FORWARD;
        DBUG_RETURN(ret);
    }
    else
    {
        /* Secondary index -- ICP loop -- we skip entries that fail the pushed
           condition without the expensive PK point-lookup. */
        uint idx_key_len = share->idx_comp_key_len[active_index];
        for (;;)
        {
            if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

            uint8_t *ik = NULL;
            size_t iks = 0;
            if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS)
                DBUG_RETURN(HA_ERR_END_OF_FILE);
            tdb_owned_buf ik_g(ik);

            if (iks <= idx_key_len) DBUG_RETURN(HA_ERR_END_OF_FILE);

            /* ICP -- we evaluate pushed condition before PK lookup */
            check_result_t icp = icp_check_secondary(ik, iks, active_index, buf);
            if (icp == CHECK_NEG)
            {
                tidesdb_iter_next(scan_iter);
                continue;
            }
            if (icp == CHECK_OUT_OF_RANGE) DBUG_RETURN(HA_ERR_END_OF_FILE);
            if (icp == CHECK_ABORTED_BY_USER) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

            int ret;
            if (keyread_only_ && try_keyread_from_index(ik, iks, active_index, buf))
                ret = 0;
            else
                ret = fetch_row_by_pk(scan_txn, ik + idx_key_len, (uint)(iks - idx_key_len), buf);
            scan_dir_ = DIR_FORWARD;
            DBUG_RETURN(ret);
        }
    }
}

int ha_tidesdb::index_prev(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_prev");

    if (cached_thd_ && thd_killed(cached_thd_)) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

    /* If PK exact match was done without iterator, we create it now and
       seek to the matched key so that prev() steps before it. */
    if (idx_pk_exact_done_)
    {
        idx_pk_exact_done_ = false;
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);
        uchar seek_key[DATA_KEY_BUF_LEN];
        uint seek_len = build_data_key(current_pk_buf_, current_pk_len_, seek_key);
        tidesdb_iter_seek(scan_iter, seek_key, seek_len);
        /* iterator is at the matched key -- fall through to prev() */
    }
    else
    {
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);
    }

    tidesdb_iter_prev(scan_iter);

    if (is_pk_)
    {
        while (tidesdb_iter_valid(scan_iter))
        {
            uint8_t *key = NULL;
            size_t ks = 0;
            if (tidesdb_iter_key(scan_iter, &key, &ks) != TDB_SUCCESS)
                DBUG_RETURN(HA_ERR_END_OF_FILE);
            tdb_owned_buf key_g(key);
            if (is_data_key(key, ks)) break;
            tidesdb_iter_prev(scan_iter);
        }
        scan_dir_ = DIR_BACKWARD;
        DBUG_RETURN(iter_read_current(buf));
    }
    else
    {
        /* Secondary index -- ICP loop (backward direction) */
        uint idx_key_len = share->idx_comp_key_len[active_index];
        for (;;)
        {
            if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

            uint8_t *ik = NULL;
            size_t iks = 0;
            if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS)
                DBUG_RETURN(HA_ERR_END_OF_FILE);
            tdb_owned_buf ik_g(ik);

            if (iks <= idx_key_len) DBUG_RETURN(HA_ERR_END_OF_FILE);

            /* ICP -- we evaluate pushed condition before PK lookup */
            check_result_t icp = icp_check_secondary(ik, iks, active_index, buf);
            if (icp == CHECK_NEG)
            {
                tidesdb_iter_prev(scan_iter);
                continue;
            }
            if (icp == CHECK_OUT_OF_RANGE) DBUG_RETURN(HA_ERR_END_OF_FILE);
            if (icp == CHECK_ABORTED_BY_USER) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

            scan_dir_ = DIR_BACKWARD;
            int ret;
            if (keyread_only_ && try_keyread_from_index(ik, iks, active_index, buf))
                ret = 0;
            else
                ret = fetch_row_by_pk(scan_txn, ik + idx_key_len, (uint)(iks - idx_key_len), buf);
            DBUG_RETURN(ret);
        }
    }
}

int ha_tidesdb::index_first(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_first");

    idx_pk_exact_done_ = false;
    int irc = ensure_scan_iter();
    if (irc) DBUG_RETURN(irc);

    if (is_pk_)
    {
        uint8_t data_prefix = KEY_NS_DATA;
        tidesdb_iter_seek(scan_iter, &data_prefix, 1);
        int ret = iter_read_current(buf);
        if (ret == 0) scan_dir_ = DIR_FORWARD;
        DBUG_RETURN(ret);
    }
    else
    {
        tidesdb_iter_seek_to_first(scan_iter);
        scan_dir_ = DIR_NONE; /* index_next will set DIR_FORWARD */
        DBUG_RETURN(index_next(buf));
    }
}

int ha_tidesdb::index_last(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::index_last");

    idx_pk_exact_done_ = false;
    int irc = ensure_scan_iter();
    if (irc) DBUG_RETURN(irc);

    if (is_pk_)
    {
        /* Seek-for-prev(sentinel) lands on the last existing data key in one
           operation, where seek_to_last walks every source's max key first
           and then we'd still need a backward scan past KEY_NS_META.
           KEY_NS_DATA (0x01) sorts after KEY_NS_META (0x00) so any data
           key is greater than every meta key, and the sentinel below is
           larger than any real data key in the CF. */
        uchar sentinel[DATA_KEY_BUF_LEN];
        sentinel[0] = KEY_NS_DATA;
        uint sentinel_len = KEY_NAMESPACE_LEN + share->pk_key_len;
        if (sentinel_len > sizeof(sentinel)) sentinel_len = sizeof(sentinel);
        memset(sentinel + KEY_NAMESPACE_LEN, KEY_INF_HI_BYTE, sentinel_len - KEY_NAMESPACE_LEN);
        tidesdb_iter_seek_for_prev(scan_iter, sentinel, sentinel_len);
        /* Defensive backward scan in case the seek lands on a meta key
           in a CF with no data rows yet. */
        while (tidesdb_iter_valid(scan_iter))
        {
            uint8_t *key = NULL;
            size_t ks = 0;
            if (tidesdb_iter_key(scan_iter, &key, &ks) != TDB_SUCCESS)
                DBUG_RETURN(HA_ERR_END_OF_FILE);
            tdb_owned_buf key_g(key);
            if (is_data_key(key, ks)) break;
            tidesdb_iter_prev(scan_iter);
        }
        scan_dir_ = DIR_BACKWARD;
        DBUG_RETURN(iter_read_current(buf));
    }
    else
    {
        /* Secondary CFs hold only index entries; seek_for_prev(0xFF...)
           lands on the last one without the per-source max-key walk that
           seek_to_last performs. */
        uchar sentinel[SEC_IDX_KEY_BUF_LEN];
        uint sentinel_len = share->idx_comp_key_len[active_index] + share->pk_key_len;
        if (sentinel_len > sizeof(sentinel)) sentinel_len = sizeof(sentinel);
        memset(sentinel, KEY_INF_HI_BYTE, sentinel_len);
        tidesdb_iter_seek_for_prev(scan_iter, sentinel, sentinel_len);
        if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

        uint8_t *ik = NULL;
        size_t iks = 0;
        if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS) DBUG_RETURN(HA_ERR_END_OF_FILE);
        tdb_owned_buf ik_g(ik);

        uint idx_key_len = share->idx_comp_key_len[active_index];
        if (iks <= idx_key_len) DBUG_RETURN(HA_ERR_END_OF_FILE);

        scan_dir_ = DIR_BACKWARD;
        DBUG_RETURN(fetch_row_by_pk(scan_txn, ik + idx_key_len, (uint)(iks - idx_key_len), buf));
    }
}

int ha_tidesdb::index_next_same(uchar *buf, const uchar *key, uint keylen)
{
    DBUG_ENTER("ha_tidesdb::index_next_same");

    if (cached_thd_ && thd_killed(cached_thd_)) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

    /* Spatial index continuation */
    if (spatial_scan_active_)
    {
        if (!scan_iter) DBUG_RETURN(HA_ERR_END_OF_FILE);
        tidesdb_iter_next(scan_iter);
        DBUG_RETURN(spatial_scan_next(buf));
    }

    if (is_pk_)
    {
        uint full_pk_comp_len = share->idx_comp_key_len[share->pk_index];
        if (idx_search_comp_len_ >= full_pk_comp_len)
        {
            /* Full PK is unique -- after the first match there are no more */
            DBUG_RETURN(HA_ERR_END_OF_FILE);
        }

        /* Partial PK prefix on a composite PK -- we iterate through data keys
           that share this prefix-- KEY_NS_DATA + comparable_pk_prefix... */
        if (!scan_iter) DBUG_RETURN(HA_ERR_END_OF_FILE);

        tidesdb_iter_next(scan_iter);
        if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

        uint8_t *ik = NULL;
        size_t iks = 0;
        if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS) DBUG_RETURN(HA_ERR_END_OF_FILE);
        tdb_owned_buf ik_g(ik);

        /* Data key format-- KEY_NS_DATA + comparable_pk.
           We check if the PK prefix still matches (skip the namespace byte). */
        if (iks < KEY_NAMESPACE_LEN + idx_search_comp_len_ ||
            memcmp(ik + KEY_NAMESPACE_LEN, idx_search_comp_, idx_search_comp_len_) != 0)
            DBUG_RETURN(HA_ERR_END_OF_FILE);

        int ret = iter_read_current(buf);
        if (ret == 0) scan_dir_ = DIR_FORWARD;
        DBUG_RETURN(ret);
    }

    /* Secondary index -- we advance past the last-read entry, then ICP loop */
    if (!scan_iter) DBUG_RETURN(HA_ERR_END_OF_FILE);
    tidesdb_iter_next(scan_iter);

    uint idx_col_len = share->idx_comp_key_len[active_index];
    for (;;)
    {
        if (!tidesdb_iter_valid(scan_iter)) DBUG_RETURN(HA_ERR_END_OF_FILE);

        uint8_t *ik = NULL;
        size_t iks = 0;
        if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS) DBUG_RETURN(HA_ERR_END_OF_FILE);
        tdb_owned_buf ik_g(ik);

        if (iks < idx_search_comp_len_ || memcmp(ik, idx_search_comp_, idx_search_comp_len_) != 0)
        {
            DBUG_RETURN(HA_ERR_END_OF_FILE);
        }

        if (iks <= idx_col_len) DBUG_RETURN(HA_ERR_END_OF_FILE);

        /* ICP -- we evaluate pushed condition before PK lookup */
        check_result_t icp = icp_check_secondary(ik, iks, active_index, buf);
        if (icp == CHECK_NEG)
        {
            tidesdb_iter_next(scan_iter);
            continue;
        }
        if (icp == CHECK_OUT_OF_RANGE) DBUG_RETURN(HA_ERR_END_OF_FILE);
        if (icp == CHECK_ABORTED_BY_USER) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

        int ret;
        if (keyread_only_ && try_keyread_from_index(ik, iks, active_index, buf))
            ret = 0;
        else
            ret = fetch_row_by_pk(scan_txn, ik + idx_col_len, (uint)(iks - idx_col_len), buf);
        DBUG_RETURN(ret);
    }
}



/*
  Deliver the next row from the sorted list of point lookups.  PK lookups
  bypass the iterator entirely via fetch_row_by_pk; secondary index lookups
  reuse the cached scan iterator and a single seek per entry.  Rows that
  the index knew about but the data CF no longer has (stale entries after
  concurrent delete) are silently skipped.
*/
int ha_tidesdb::multi_range_read_next(range_id_t *range_info)
{
    DBUG_ENTER("ha_tidesdb::multi_range_read_next");

    if (!mrr_custom_active_) DBUG_RETURN(handler::multi_range_read_next(range_info));

    if (cached_thd_ && thd_killed(cached_thd_)) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

    /* Lazy txn -- the optimizer may invoke MRR without a prior rnd_init / index_init. */
    int erc = ensure_stmt_txn();
    if (erc) DBUG_RETURN(erc);
    if (!scan_txn) scan_txn = stmt_txn;

    bool is_pk_scan = share->has_user_pk && mrr_keyno_ == share->pk_index;
    uint idx_col_len = share->idx_comp_key_len[mrr_keyno_];

    while (mrr_next_idx_ < mrr_entries_.size())
    {
        const tdb_mrr_entry &e = mrr_entries_[mrr_next_idx_++];
        if (!mrr_no_assoc_) *range_info = e.ptr;

        if (is_pk_scan)
        {
            int rc = fetch_row_by_pk(scan_txn, (const uchar *)e.comp_key.data(),
                                     (uint)e.comp_key.size(), table->record[0]);
            if (rc == HA_ERR_KEY_NOT_FOUND) continue; /* stale range, try next */
            DBUG_RETURN(rc);
        }

        /* Secondary index point lookup -- seek, verify prefix match, then
           either cover-read from the index or PK-fetch. */
        if (mrr_keyno_ >= share->idx_cfs.size() || !share->idx_cfs[mrr_keyno_])
            continue; /* missing CF for this index -- skip defensively */
        scan_cf_ = share->idx_cfs[mrr_keyno_];
        int irc = ensure_scan_iter();
        if (irc) DBUG_RETURN(irc);

        tidesdb_iter_seek(scan_iter, (const uint8_t *)e.comp_key.data(), (uint)e.comp_key.size());
        if (!tidesdb_iter_valid(scan_iter)) continue;

        uint8_t *ik = NULL;
        size_t iks = 0;
        if (tidesdb_iter_key(scan_iter, &ik, &iks) != TDB_SUCCESS) continue;
        tdb_owned_buf ik_g(ik);
        if (iks < e.comp_key.size() || memcmp(ik, e.comp_key.data(), e.comp_key.size()) != 0)
            continue; /* no entry for this point */
        if (iks <= idx_col_len) continue;

        int rc;
        if (keyread_only_ && try_keyread_from_index(ik, iks, mrr_keyno_, table->record[0]))
            rc = 0;
        else
            rc = fetch_row_by_pk(scan_txn, ik + idx_col_len, (uint)(iks - idx_col_len),
                                 table->record[0]);
        if (rc == HA_ERR_KEY_NOT_FOUND) continue;
        DBUG_RETURN(rc);
    }

    DBUG_RETURN(HA_ERR_END_OF_FILE);
}

