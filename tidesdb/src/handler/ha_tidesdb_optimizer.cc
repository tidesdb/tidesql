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

/* the optimizer-facing cost and descriptor surface: scan_time prices a full-table sweep,
   records_in_range estimates how many rows a key range spans from the cached cardinality,
   index_flags advertises which read capabilities each index exposes (ordered reads, index-only
   fetch, condition pushdown, spatial ranges), and index_type names the access method.  the server
   reads these while planning; none touch stored data, they only translate table statistics and
   index metadata into the handler's cost model. */

#include "ha_tidesdb.h"

#include <mysql/plugin.h>

#include <cstring>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/core/rir.h"
#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_keycodec.h"
#include "src/handler/ha_tidesdb_spatial.h"
IO_AND_CPU_COST ha_tidesdb::scan_time()
{
    /* We start from the standard volume-based scan cost the base handler derives
       from the row count and mean row length that info() publishes, so a large
       table is never mispriced as cheap and the planner keeps preferring an index
       lookup wherever one is genuinely cheaper than a full scan.  On top of that
       baseline we add a surcharge for the LSM merge fanout, the number of sorted
       runs a full scan has to merge, which the volume cost alone cannot see. */
    IO_AND_CPU_COST cost = handler::scan_time();

    if (!share || !share->cf) return cost;

    /* We cache the overlap count on the share with the same refresh interval as
       stats (TIDESDB_STATS_REFRESH_US = 2 seconds).  tidesdb_range_stats reports
       how many sorted runs a scan would merge, drawn from in-memory metadata
       without disk I/O, but the computation still has measurable CPU cost when
       called per query plan.  We stamp the cache on every successful probe,
       a zero-overlap memtable-resident table included, so the refresh gate
       actually holds instead of re-probing the library on every call. */
    auto now = std::chrono::steady_clock::now();
    long long now_us =
        std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    long long cached_time = share->scan_cost_time.load(std::memory_order_relaxed);
    double overlap = share->cached_scan_cost.load(std::memory_order_relaxed);

    bool stale = (cached_time == 0) || (now_us - cached_time > TIDESDB_STATS_REFRESH_US);

    if (stale)
    {
        uchar lo[KEY_NAMESPACE_LEN] = {KEY_NS_DATA};
        uchar hi[DATA_KEY_BUF_LEN];
        memset(hi, KEY_INF_HI_BYTE, sizeof(hi));
        uint hi_len = KEY_NAMESPACE_LEN + share->pk_key_len;
        if (hi_len > sizeof(hi)) hi_len = sizeof(hi);

        tidesdb_range_stats_t rs;
        if (tidesdb_range_stats(tdb_global, share->cf, lo, KEY_NAMESPACE_LEN, hi, hi_len, &rs) ==
            TDB_SUCCESS)
        {
            overlap = (double)rs.sstables_overlapping;
            share->cached_scan_cost.store(overlap, std::memory_order_relaxed);
            share->scan_cost_time.store(now_us, std::memory_order_relaxed);
        }
    }

    if (overlap > 0.0)
    {
        cost.io += overlap * TIDESDB_SCAN_IO_WEIGHT;
        cost.cpu += overlap * TIDESDB_SCAN_CPU_WEIGHT;
    }

    return cost;
}

void ha_tidesdb::rir_encode_bounds(uint inx, bool is_pk, const key_range *min_key,
                                   const key_range *max_key, uchar *lo_buf, uint &lo_len,
                                   uchar *hi_buf, uint &hi_len)
{
    MY_BITMAP *old_map = tmp_use_all_columns(table, &table->read_set);

    if (min_key && min_key->key)
    {
        KEY *ki = &table->key_info[inx];
        uint kl = calculate_key_len(table, inx, min_key->key, min_key->keypart_map);
        if (is_pk)
        {
            uchar comp[MAX_KEY_LENGTH];
            uint comp_len = key_copy_to_comparable(ki, min_key->key, kl, comp);
            lo_len = build_data_key(comp, comp_len, lo_buf);
        }
        else
        {
            lo_len = key_copy_to_comparable(ki, min_key->key, kl, lo_buf);
        }
    }
    else
    {
        /* No lower bound, we use smallest possible key */
        lo_buf[0] = is_pk ? KEY_NS_DATA : KEY_INF_LO_BYTE;
        lo_len = KEY_NAMESPACE_LEN;
    }

    if (max_key && max_key->key)
    {
        KEY *ki = &table->key_info[inx];
        uint kl = calculate_key_len(table, inx, max_key->key, max_key->keypart_map);
        if (is_pk)
        {
            uchar comp[MAX_KEY_LENGTH];
            uint comp_len = key_copy_to_comparable(ki, max_key->key, kl, comp);
            hi_len = build_data_key(comp, comp_len, hi_buf);
        }
        else
        {
            hi_len = key_copy_to_comparable(ki, max_key->key, kl, hi_buf);
        }
    }
    else
    {
        /* No upper bound, we use largest possible key */
        memset(hi_buf, KEY_INF_HI_BYTE, DATA_KEY_BUF_LEN);
        hi_len = is_pk ? (KEY_NAMESPACE_LEN + share->pk_key_len)
                       : share->idx_comp_key_len[inx] + share->pk_key_len;
        if (hi_len > DATA_KEY_BUF_LEN) hi_len = DATA_KEY_BUF_LEN;
    }

    tmp_restore_column_map(&table->read_set, old_map);
}

/**
 * rir_prefix_successor
 * build the smallest key strictly greater than every key that begins with the given prefix, so a
 * point-equality probe can count exactly the rows carrying one index value as the half-open range
 * from the value to its successor
 * @param in the comparable prefix bytes, not NULL
 * @param in_len the prefix length in bytes, greater than zero
 * @param out receives the successor bytes, must hold at least in_len bytes
 * @param out_len receives the successor length, at most in_len
 * @return true when a finite successor exists, false when the prefix is all 0xFF and none does
 */
static bool rir_prefix_successor(const uchar *in, uint in_len, uchar *out, uint &out_len)
{
    if (!in || in_len == 0) return false;
    memcpy(out, in, in_len);
    /* increment the last byte below the max and drop everything after it; a trailing run of max
       bytes carries left, and an all-max prefix has no finite successor */
    for (uint i = in_len; i > 0; i--)
    {
        if (out[i - 1] != KEY_INF_HI_BYTE)
        {
            out[i - 1]++;
            out_len = i;
            return true;
        }
    }
    return false;
}

ha_rows ha_tidesdb::records_in_range(uint inx, const key_range *min_key, const key_range *max_key,
                                     page_range *pages)
{
    if (!share) return TIDESDB_RIR_DEFAULT_EST;

    ha_rows total = share->cached_records.load(std::memory_order_relaxed);
    if (total == 0) total = TIDESDB_MIN_STATS_RECORDS;

    tidesdb_column_family_t *cf;
    bool is_pk = share->has_user_pk && inx == share->pk_index;
    if (is_pk)
        cf = share->cf;
    else if (inx < share->idx_cfs.size() && share->idx_cfs[inx])
        cf = share->idx_cfs[inx];
    else
        return (ha_rows)tidesdb::rir::unknown_fallback(total); /* no CF for this index */

    uchar lo_buf[DATA_KEY_BUF_LEN];
    uchar hi_buf[DATA_KEY_BUF_LEN];
    uint lo_len = 0, hi_len = 0;
    rir_encode_bounds(inx, is_pk, min_key, max_key, lo_buf, lo_len, hi_buf, hi_len);

    /* a point equality gives both bounds as the same comparable value bytes.  a unique
       or primary key matches one row, and a non-unique index that ANALYZE or the
       open-time pass has already sampled carries a trustworthy cached rec_per_key, so
       both read the cheap cached estimate.  only a non-unique index with no sample yet
       needs more, and there the value bytes encode the index value without its pk
       suffix, so every matching row stores a key with that value as a prefix and the
       matches are exactly the half-open range from the value to its successor.  a
       tidesdb_range_stats probe of that span counts a single value from metadata and is
       right for a low- or high-cardinality index alike, where the records/10 fallback a
       never-analyzed table would otherwise use reads one row per value and full-scans. */
    if (min_key && max_key && lo_len > 0 && hi_len > 0 && lo_len == hi_len &&
        memcmp(lo_buf, hi_buf, lo_len) == 0)
    {
        KEY *ki = &table->key_info[inx];
        uint parts_used = my_count_bits(min_key->keypart_map);
        ulong rpk = (parts_used > 0 && parts_used <= ki->user_defined_key_parts)
                        ? ki->rec_per_key[parts_used - 1]
                        : 0;
        bool is_unique = (ki->flags & HA_NOSAME);
        ulong sampled_rpk =
            (inx < MAX_KEY) ? share->cached_rec_per_key[inx].load(std::memory_order_relaxed) : 0;

        if (!is_pk && !is_unique && sampled_rpk == 0)
        {
            uchar succ_buf[DATA_KEY_BUF_LEN];
            uint succ_len = 0;
            if (rir_prefix_successor(lo_buf, lo_len, succ_buf, succ_len))
            {
                tidesdb_range_stats_t rs;
                if (tidesdb_range_stats(tdb_global, cf, lo_buf, lo_len, succ_buf, succ_len, &rs) ==
                    TDB_SUCCESS)
                {
                    ha_rows est = (ha_rows)rs.estimated_keys;
                    if (est < 1) est = 1;
                    if (total > 0 && est > total) est = total;
                    return est;
                }
            }
        }
        return (ha_rows)tidesdb::rir::point_estimate(rpk, total);
    }

    /* We ask TidesDB for the range's live key count.  tidesdb_range_stats is
       memtable-aware and returns an absolute cardinality (counted exactly for a
       range short enough to walk, otherwise estimated from SSTable metadata), so
       we use estimated_keys directly rather than deriving a fraction from a scan
       cost.  Both bounds are always valid here since rir_encode_bounds
       substitutes the key-space boundary for a missing side. */
    tidesdb_range_stats_t rs;
    int rc = tidesdb_range_stats(tdb_global, cf, lo_buf, lo_len, hi_buf, hi_len, &rs);
    if (rc != TDB_SUCCESS) return (ha_rows)tidesdb::rir::unknown_fallback(total);

    ha_rows est = (ha_rows)rs.estimated_keys;
    if (est < 1) est = 1;

    /* The library counts flushed sstable entries including superseded mvcc
       versions, so a hot recently updated range can report more keys than the
       table actually holds.  We cap the estimate at the live row count when we
       have a real one so the optimizer never prices a range wider than the whole
       table, which is the same ceiling the point and fallback paths already
       apply through point_estimate and unknown_fallback. */
    ha_rows live_rows = share->cached_records.load(std::memory_order_relaxed);
    if (live_rows > 0 && est > live_rows) est = live_rows;
    return est;
}

ulong ha_tidesdb::index_flags(uint idx, uint part, bool all_parts) const
{
    /* FULLTEXT indexes do not support ordered reads or ICP */
    if (table_share && idx < table_share->keys &&
        table_share->key_info[idx].algorithm == HA_KEY_ALG_FULLTEXT)
        return 0;

    /* SPATIAL indexes support MBR range scans and forward iteration */
    if (table_share && idx < table_share->keys && is_spatial_index(&table_share->key_info[idx]))
        return HA_READ_NEXT | HA_READ_RANGE;

    ulong flags =
        HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE | HA_DO_INDEX_COND_PUSHDOWN;
    if (table_share && table_share->primary_key != MAX_KEY && idx == table_share->primary_key)
        flags |= HA_CLUSTERED_INDEX;
    else
    {
        /* The server builds field->part_of_key (the covering-index bitmap) by
           calling index_flags(idx, part, 0) once per key part, so advertise
           HA_KEYREAD_ONLY for a part only when that part's field reconstructs
           from its sort key via decode_sort_key_part.  A query that reads only
           decodable columns of a mixed-type index still gets an index-only
           plan; a read of an undecodable column (VARCHAR, DECIMAL, floating
           point, multi-byte CHAR) leaves part_of_key clear so the optimizer
           prices the primary-key row fetch instead of a phantom covering scan
           that try_keyread_from_index would fall back on at runtime. */
        if (index_part_is_decodable(table_share, idx, part)) flags |= HA_KEYREAD_ONLY;
    }
    return flags;
}

const char *ha_tidesdb::index_type(uint key_number)
{
    if (key_number < table->s->keys)
    {
        if (table->key_info[key_number].algorithm == HA_KEY_ALG_FULLTEXT) return "FULLTEXT";
        if (is_spatial_index(&table->key_info[key_number])) return "RTREE";
    }
    /* every non-fulltext, non-spatial index is an lsm b+tree. */
    return "BTREE";
}
