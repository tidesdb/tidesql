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

/* the table-maintenance and statistics handler surface: info refreshes the cached row-count and
   index cardinalities the optimizer reads, analyze recomputes them from the column families,
   optimize and repair compact the data and index column families, and check verifies the column
   families are readable.  none of
   these sit on a hot path; they translate an admin request into the matching library operation and
   report the outcome back through the handler admin-status codes. */

#include "ha_tidesdb.h"

#include <mysql/plugin.h>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"

#include <string>

#include "src/handler/ha_tidesdb_internal.h"
/* ******************** info ******************** */

void ha_tidesdb::info_refresh_cf_stats()
{
    long long now = (long long)microsecond_interval_timer();
    long long last = share->stats_refresh_us.load(std::memory_order_relaxed);
    if (now - last <= TIDESDB_STATS_REFRESH_US ||
        !share->stats_refresh_us.compare_exchange_weak(last, now, std::memory_order_relaxed))
        return;

    tidesdb_cf_stats_t st;
    if (tidesdb_get_cf_stats(share->cf, &st) != TDB_SUCCESS) return;

    /* Row estimate comes from this single get_cf_stats pass.  total_keys counts the
       flushed sstable entries including mvcc versions; unflushed_key_count adds the
       memtable-resident rows a busy table has not spilled yet, so a freshly inserted
       table is not reported as empty.  The sum is an upper bound the optimizer treats
       as conservative. */
    uint64_t rows = st.total_keys + st.unflushed_key_count;
    share->cached_records.store(rows, std::memory_order_relaxed);

    /* total_data_size counts sstable klog+vlog only, so fall back to the row estimate
       times the average entry size when little or nothing has flushed, keeping
       DATA_LENGTH non-zero for a fresh table.  With no sstable yet the average is
       unknown, so the record length stands in. */
    uint64_t data_sz = st.total_data_size;
    if (data_sz == 0 && rows > 0)
    {
        double avg_entry = st.avg_key_size + st.avg_value_size;
        if (avg_entry <= 0) avg_entry = table->s->reclength;
        data_sz = (uint64_t)(rows * avg_entry);
    }
    share->cached_data_size.store(data_sz, std::memory_order_relaxed);
    uint32_t mrl = (uint32_t)(st.avg_key_size + st.avg_value_size);
    if (mrl == 0) mrl = table->s->reclength;
    share->cached_mean_rec_len.store(mrl, std::memory_order_relaxed);
    share->cached_read_amp.store(st.read_amp > 0 ? st.read_amp : READ_AMP_NONE,
                                 std::memory_order_relaxed);

    /* We sum secondary index CF sizes for index_file_length */
    uint64_t idx_total = 0;
    for (uint i = 0; i < share->idx_cfs.size(); i++)
    {
        if (!share->idx_cfs[i]) continue;
        tidesdb_cf_stats_t ist;
        if (tidesdb_get_cf_stats(share->idx_cfs[i], &ist) == TDB_SUCCESS)
        {
            uint64_t ikeys = ist.total_keys + ist.unflushed_key_count;
            uint64_t isz = ist.total_data_size;
            if (isz == 0 && ikeys > 0)
            {
                double iavg = ist.avg_key_size + ist.avg_value_size;
                if (iavg <= 0) iavg = (double)share->pk_key_len;
                isz = (uint64_t)(ikeys * iavg);
            }
            idx_total += isz;
        }
    }
    share->cached_idx_data_size.store(idx_total, std::memory_order_relaxed);
}

void ha_tidesdb::info_fill_rec_per_key()
{
    for (uint i = 0; i < table->s->keys; i++)
    {
        KEY *key = &table->key_info[i];
        bool is_pk = share->has_user_pk && i == share->pk_index;
        bool is_unique = (key->flags & HA_NOSAME);
        ulong cached_rpk =
            (i < MAX_KEY) ? share->cached_rec_per_key[i].load(std::memory_order_relaxed) : 0;
        for (uint j = 0; j < key->ext_key_parts; j++)
        {
            if (is_pk || is_unique)
            {
                if (j + 1 >= key->user_defined_key_parts)
                {
                    /* Full unique key, exactly 1 row per distinct value */
                    key->rec_per_key[j] = REC_PER_KEY_UNIQUE;
                }
                else
                {
                    /* Intermediate prefix of a composite unique key.
                       Estimate assuming uniform distribution:
                         cardinality(prefix_k) ≈ total^(k/N)
                         rec_per_key[j] = total^((N - j - 1) / N)
                       E.g. for PK(a,b,c) with 300K rows:
                         rec_per_key[0] ≈ 4481  (per distinct a)
                         rec_per_key[1] ≈ 67    (per distinct a,b)
                         rec_per_key[2] = 1     (unique)          */
                    uint N = key->user_defined_key_parts;
                    double rpk = pow((double)stats.records, (double)(N - j - 1) / (double)N);
                    key->rec_per_key[j] = (ulong)MY_MAX((ulong)rpk, REC_PER_KEY_FLOOR);
                }
            }
            else if (j + 1 == key->user_defined_key_parts)
            {
                /* Last user key part of a non-unique index.
                   We use ANALYZE-sampled value if available, else heuristic. */
                if (cached_rpk > 0)
                    key->rec_per_key[j] = cached_rpk;
                else
                    key->rec_per_key[j] = (ulong)MY_MAX(
                        stats.records / STATS_REC_PER_KEY_FALLBACK_DIVISOR + 1, REC_PER_KEY_FLOOR);
            }
            else
            {
                /* Intermediate prefix of a non-unique index.
                   Geometrically interpolate between stats.records
                   (single leading column) and the last-part rec_per_key.
                   Formula is total / (total/last_rpk)^((j+1)/N) */
                ulong last_rpk = (cached_rpk > 0)
                                     ? cached_rpk
                                     : (ulong)(stats.records / STATS_REC_PER_KEY_FALLBACK_DIVISOR +
                                               1);
                uint N = key->user_defined_key_parts;
                double base = (last_rpk > 0) ? (double)stats.records / (double)last_rpk
                                             : (double)stats.records;
                double rpk = (double)stats.records / pow(base, (double)(j + 1) / (double)N);
                key->rec_per_key[j] =
                    (ulong)MY_MAX(MY_MIN((ulong)rpk, stats.records), REC_PER_KEY_FLOOR);
            }
        }
    }
}

int ha_tidesdb::info(uint flag)
{
    DBUG_ENTER("ha_tidesdb::info");

    if (share) ref_length = share->pk_key_len;

    if ((flag & (HA_STATUS_VARIABLE | HA_STATUS_CONST)) && share && share->cf)
    {
        info_refresh_cf_stats();

        stats.records = share->cached_records.load(std::memory_order_relaxed);
        if (stats.records == 0) stats.records = TIDESDB_MIN_STATS_RECORDS;
        stats.data_file_length = share->cached_data_size.load(std::memory_order_relaxed);
        stats.index_file_length = share->cached_idx_data_size.load(std::memory_order_relaxed);
        stats.mean_rec_length = share->cached_mean_rec_len.load(std::memory_order_relaxed);
        stats.delete_length = 0;
        stats.mrr_length_per_rec = ref_length + sizeof(uint64_t);
    }

    /* HA_STATUS_TIME -- we create_time from .frm stat and update_time from last DML */
    if ((flag & HA_STATUS_TIME) && share)
    {
        stats.create_time = share->create_time;
        stats.update_time = share->update_time.load(std::memory_order_relaxed);
    }

    /* HA_STATUS_CONST -- set rec_per_key for index selectivity estimates.  Measure
       real secondary-index cardinality first, once per populated table, so the fill
       uses sampled selectivity instead of the records/10 fallback that full-scans
       high-cardinality IN-lists and ranges. */
    if ((flag & HA_STATUS_CONST) && share)
    {
        if (share->cf) auto_sample_index_stats();
        info_fill_rec_per_key();
    }

    /* HA_STATUS_AUTO -- report the next auto-increment value.  get_auto_increment hands out
       values from share->auto_inc_val (the last one used), so the next is one past it.  SHOW TABLE
       STATUS reads this, and the partition wrapper seeds its shared counter from each partition's
       value here, so leaving it unset makes AUTO_INCREMENT on a partitioned table go out of range. */
    if ((flag & HA_STATUS_AUTO) && share && table->found_next_number_field)
        stats.auto_increment_value = share->auto_inc_val.load(std::memory_order_relaxed) + 1;

    DBUG_RETURN(0);
}

/* ******************** analyze ******************** */

/*
  ANALYZE TABLE -- refresh cached stats and output CF statistics as notes.
  The notes appear as additional Msg_type='note' rows in the ANALYZE TABLE
  result set, giving the user visibility into TidesDB internals.
*/
void ha_tidesdb::analyze_report_cf_stats(THD *thd, const tidesdb_cf_stats_t &st)
{
    /* Summary line.  The memtable and block cache are db-level in TidesDB 10, so their
       figures come from SHOW ENGINE TIDESDB STATUS rather than per-cf here. */
    push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                        "[TIDESDB] CF '%s'  total_keys=%llu  data_size=%llu bytes"
                        "  levels=%d  read_amp=%.2f",
                        share->cf_name.c_str(),
                        (unsigned long long)(st.total_keys + st.unflushed_key_count),
                        (unsigned long long)st.total_data_size, st.num_levels, st.read_amp);

    /* Average sizes */
    push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                        "[TIDESDB] avg_key=%.1f bytes  avg_value=%.1f bytes", st.avg_key_size,
                        st.avg_value_size);

    /* Per-level detail */
    for (int i = 0; i < st.num_levels; i++)
    {
        push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                            "[TIDESDB] level %d  sstables=%d  size=%zu bytes"
                            "  keys=%llu",
                            i + 1, st.level_num_sstables[i], st.level_sizes[i],
                            (unsigned long long)st.level_key_counts[i]);
    }

    /* B+tree stats, present when the cf holds any btree nodes. */
    if (st.btree_total_nodes > 0)
    {
        push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                            "[TIDESDB] btree  nodes=%llu  max_height=%u"
                            "  avg_height=%.2f",
                            (unsigned long long)st.btree_total_nodes, st.btree_max_height,
                            st.btree_avg_height);
    }

    /* Partition-range filter memory this cf holds resident outside the block
       cache, one routing directory per sstable.  Reported when it keeps any. */
    if (st.filter_resident_bytes > 0)
    {
        push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                            "[TIDESDB] filters  resident=%llu bytes",
                            (unsigned long long)st.filter_resident_bytes);
    }

    /* Per-CF write-amplification counters from the lib's stats patch.
       Reported when any logical bytes have been committed against this
       CF -- a freshly created CF that has never accepted a write would
       produce zeros across the board and adding a row of zeros would
       only add noise. */
    if (st.user_bytes_written > 0)
    {
        const uint64_t out_bytes =
            st.wal_bytes_written + st.flush_bytes_written + st.compaction_bytes_written;
        const double wa = (double)out_bytes / (double)st.user_bytes_written;
        push_warning_printf(
            thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
            "[TIDESDB] WA  user=%llu  wal=%llu  flush=%llu"
            "  compact_write=%llu (%llu ssts)  compact_read=%llu"
            "  ratio=%.2fx",
            (unsigned long long)st.user_bytes_written, (unsigned long long)st.wal_bytes_written,
            (unsigned long long)st.flush_bytes_written,
            (unsigned long long)st.compaction_bytes_written,
            (unsigned long long)st.compaction_count, (unsigned long long)st.compaction_bytes_read,
            wa);
    }
}

void ha_tidesdb::analyze_sample_indexes(THD *thd, tidesdb_txn_t *txn, bool verbose)
{
    for (uint i = 0; i < table->s->keys; i++)
    {
        if (share->has_user_pk && i == share->pk_index) continue;
        if (i >= share->idx_cfs.size() || !share->idx_cfs[i]) continue;
        KEY *ki = &table->key_info[i];

        tidesdb_cf_stats_t ist;
        uint64_t idx_total_keys = 0;
        if (tidesdb_get_cf_stats(share->idx_cfs[i], &ist) == TDB_SUCCESS)
        {
            idx_total_keys = ist.total_keys + ist.unflushed_key_count;
            if (verbose)
                push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                                    "[TIDESDB] idx CF '%s'  keys=%llu  data_size=%llu bytes"
                                    "  levels=%d",
                                    share->idx_cf_names[i].c_str(),
                                    (unsigned long long)idx_total_keys,
                                    (unsigned long long)ist.total_data_size, ist.num_levels);
        }

        /* We sample the index to estimate distinct prefix count.
           For unique indexes rec_per_key is always 1.
           For non-unique indexes, scan up to ANALYZE_SAMPLE_LIMIT entries
           and count distinct index-column prefixes. */
        if (ki->flags & HA_NOSAME)
        {
            share->cached_rec_per_key[i].store(REC_PER_KEY_UNIQUE, std::memory_order_relaxed);
            continue;
        }

        uint idx_prefix_len = share->idx_comp_key_len[i];
        if (idx_prefix_len == 0) continue;

        tidesdb_iter_t *ait = NULL;
        if (tdb_iter_new_blocking(ha_thd(), txn, share->idx_cfs[i], &ait) != TDB_SUCCESS || !ait)
            continue;

        tidesdb_iter_seek_to_first(ait);

        static constexpr uint64_t ANALYZE_SAMPLE_LIMIT = 100000;
        uint64_t sampled = 0, distinct = 0;
        uchar prev_prefix[MAX_KEY_LENGTH];
        uint prev_len = 0;

        while (tidesdb_iter_valid(ait) && sampled < ANALYZE_SAMPLE_LIMIT)
        {
            uint8_t *ik = NULL;
            size_t iks = 0;
            if (tidesdb_iter_key(ait, &ik, &iks) != TDB_SUCCESS) break;
            tdb_owned_buf ik_g(ik);

            uint cmp_len = (iks >= idx_prefix_len) ? idx_prefix_len : (uint)iks;
            if (sampled == 0 || cmp_len != prev_len || memcmp(ik, prev_prefix, cmp_len) != 0)
            {
                distinct++;
                prev_len = cmp_len;
                memcpy(prev_prefix, ik, cmp_len);
            }
            sampled++;
            tidesdb_iter_next(ait);
        }
        tidesdb_iter_free(ait);

        if (distinct == 0) continue;

        uint64_t total = (idx_total_keys > 0) ? idx_total_keys : sampled;
        if (sampled < total)
        {
            /* Extrapolate -- distinct_full ≈ distinct * (total / sampled) */
            double ratio = (double)total / (double)sampled;
            uint64_t est_distinct = (uint64_t)(distinct * ratio);
            if (est_distinct == 0) est_distinct = 1; /* divide-by-zero guard */
            ulong rpk = (ulong)(total / est_distinct);
            if (rpk == 0) rpk = REC_PER_KEY_FLOOR;
            share->cached_rec_per_key[i].store(rpk, std::memory_order_relaxed);
        }
        else
        {
            ulong rpk = (ulong)(sampled / distinct);
            if (rpk == 0) rpk = REC_PER_KEY_FLOOR;
            share->cached_rec_per_key[i].store(rpk, std::memory_order_relaxed);
        }

        if (verbose)
            push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                                "[TIDESDB] idx '%s' sampled=%llu distinct=%llu rec_per_key=%lu",
                                ki->name.str, (unsigned long long)sampled,
                                (unsigned long long)distinct,
                                share->cached_rec_per_key[i].load(std::memory_order_relaxed));
    }
}

/*
  auto_sample_index_stats -- the ANALYZE-free path to real index selectivity.

  A workload that never issues ANALYZE TABLE (sysbench, most ORMs) would
  otherwise plan every secondary-index lookup off the records/10 fallback, which
  is an order of magnitude too high for a high-cardinality index and drives the
  optimizer into full scans of IN-lists and ranges.  The first time such a table
  is asked for const stats and has crossed STATS_AUTO_SAMPLE_MIN_ROWS, we sample
  its cardinality for real, exactly as ANALYZE does, on a private read-committed
  transaction so nothing touches the handler's statement txn.

  Runs at most once per share.  The claim is a compare-exchange so a burst of
  concurrent first queries yields one scan, not one per connection, and the done
  flag is only set after a populated table is measured, so a table sampled while
  still small is left to re-sample once it grows.
*/
void ha_tidesdb::auto_sample_index_stats()
{
    if (!share || share->idx_stats_sampled.load(std::memory_order_acquire)) return;
    if (share->num_secondary_indexes == 0) return;
    if (stats.records < STATS_AUTO_SAMPLE_MIN_ROWS) return;

    bool expected = false;
    if (!share->idx_stats_sampling.compare_exchange_strong(expected, true,
                                                           std::memory_order_acq_rel))
        return; /* another thread is already sampling this share */

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin_with_isolation(tdb_global, TDB_ISOLATION_READ_COMMITTED, &txn) ==
            TDB_SUCCESS &&
        txn)
    {
        analyze_sample_indexes(ha_thd(), txn, /*verbose=*/false);
        tidesdb_txn_free(txn);
        share->idx_stats_sampled.store(true, std::memory_order_release);
    }

    share->idx_stats_sampling.store(false, std::memory_order_release);
}

int ha_tidesdb::analyze(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::analyze");

    if (!share || !share->cf) DBUG_RETURN(HA_ADMIN_FAILED);

    share->stats_refresh_us.store(0, std::memory_order_relaxed);
    info(HA_STATUS_VARIABLE | HA_STATUS_CONST);

    tidesdb_cf_stats_t st;
    if (tidesdb_get_cf_stats(share->cf, &st) != TDB_SUCCESS)
    {
        push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR,
                            "[TIDESDB] unable to retrieve column family stats");
        DBUG_RETURN(HA_ADMIN_OK);
    }

    analyze_report_cf_stats(thd, st);

    /* Secondary index CF stats + cardinality sampling.
       We iterate each secondary index CF, counting distinct index-column
       prefixes (everything before the PK suffix) to compute rec_per_key. */
    if (ensure_stmt_txn()) DBUG_RETURN(HA_ADMIN_OK); /* non-fatal -- stats just won't update */
    analyze_sample_indexes(thd, stmt_txn, /*verbose=*/true);
    /* An explicit ANALYZE is authoritative, so the open-time auto-sample must
       not run again and overwrite it with a lighter pass. */
    share->idx_stats_sampled.store(true, std::memory_order_release);

    info(HA_STATUS_CONST);

    DBUG_RETURN(HA_ADMIN_OK);
}

/* ******************** optimize ******************** */

/*
  OPTIMIZE TABLE -- trigger compaction on all CFs (data + secondary indexes).
  Compaction merges SSTables, removes tombstones, and reduces read
  amplification.  TidesDB enqueues the work to background compaction
  threads and returns immediately.
*/
int ha_tidesdb::optimize(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::optimize");

    if (!share || !share->cf) DBUG_RETURN(HA_ADMIN_FAILED);

    /* tidesdb_compact() runs a full compaction inline, blocking until complete, which is the
       right semantic for OPTIMIZE TABLE -- the caller expects the table fully compacted when
       the statement returns. */
    bool any_locked = false;
    int rc = tidesdb_compact(tdb_global, share->cf);
    if (rc == TDB_ERR_LOCKED)
        any_locked = true;
    else if (rc != TDB_SUCCESS)
        sql_print_warning("[TIDESDB] optimize: compact data CF '%s' failed (err=%d)",
                          share->cf_name.c_str(), rc);

    for (uint i = 0; i < share->idx_cfs.size(); i++)
    {
        if (!share->idx_cfs[i]) continue;
        rc = tidesdb_compact(tdb_global, share->idx_cfs[i]);
        if (rc == TDB_ERR_LOCKED)
            any_locked = true;
        else if (rc != TDB_SUCCESS)
            sql_print_warning("[TIDESDB] optimize: compact idx CF '%s' failed (err=%d)",
                              share->idx_cf_names[i].c_str(), rc);
    }

    share->stats_refresh_us.store(0, std::memory_order_relaxed);

    /* TDB_ERR_LOCKED means "another compaction was already running and
       will subsume this work".  Surface HA_ADMIN_TRY_ALTER so the user
       sees something other than silent success -- they can retry, or
       confirm via SHOW ENGINE TIDESDB STATUS that compaction finished. */
    if (any_locked)
    {
        push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, HA_ADMIN_TRY_ALTER,
                            "OPTIMIZE TABLE: one or more column families had a "
                            "compaction already in flight; retry shortly if you "
                            "need the post-OPTIMIZE state.");
        DBUG_RETURN(HA_ADMIN_TRY_ALTER);
    }
    DBUG_RETURN(HA_ADMIN_OK);
}

int ha_tidesdb::check(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::check");

    if (!share || !share->cf) DBUG_RETURN(HA_ADMIN_CORRUPT);

    /* CHECK TABLE verifies all CFs are readable by fetching stats.
       tidesdb_get_cf_stats reads metadata from all SSTables, which validates
       that manifests, block indexes, bloom filters, and metadata blocks
       are intact. For a deeper check, users can run REPAIR TABLE which
       does a full compaction pass that reads and re-checksums every block. */
    tidesdb_cf_stats_t st;
    int rc = tidesdb_get_cf_stats(share->cf, &st);
    if (rc != TDB_SUCCESS)
    {
        sql_print_error("[TIDESDB] CHECK TABLE '%s': data CF check failed (err=%d)",
                        share->cf_name.c_str(), rc);
        DBUG_RETURN(HA_ADMIN_CORRUPT);
    }

    for (uint i = 0; i < share->idx_cfs.size(); i++)
    {
        if (!share->idx_cfs[i]) continue;
        tidesdb_cf_stats_t ist;
        rc = tidesdb_get_cf_stats(share->idx_cfs[i], &ist);
        if (rc != TDB_SUCCESS)
        {
            sql_print_error("[TIDESDB] CHECK TABLE '%s': index CF '%s' check failed (err=%d)",
                            share->cf_name.c_str(), share->idx_cf_names[i].c_str(), rc);
            DBUG_RETURN(HA_ADMIN_CORRUPT);
        }
    }

    DBUG_RETURN(HA_ADMIN_OK);
}

int ha_tidesdb::repair(THD *thd, HA_CHECK_OPT *check_opt)
{
    DBUG_ENTER("ha_tidesdb::repair");

    if (!share || !share->cf) DBUG_RETURN(HA_ADMIN_FAILED);

    /* REPAIR TABLE runs a full compaction of the data CF and every index CF. */
    int rc = tidesdb_compact(tdb_global, share->cf);
    if (rc != TDB_SUCCESS)
    {
        sql_print_error("[TIDESDB] REPAIR TABLE '%s': compact data CF failed (err=%d)",
                        share->cf_name.c_str(), rc);
        DBUG_RETURN(HA_ADMIN_FAILED);
    }

    for (uint i = 0; i < share->idx_cfs.size(); i++)
    {
        if (!share->idx_cfs[i]) continue;
        rc = tidesdb_compact(tdb_global, share->idx_cfs[i]);
        if (rc != TDB_SUCCESS)
            sql_print_warning("[TIDESDB] REPAIR TABLE '%s': compact idx CF '%s' failed (err=%d)",
                              share->cf_name.c_str(), share->idx_cf_names[i].c_str(), rc);
    }

    share->stats_refresh_us.store(0, std::memory_order_relaxed);
    DBUG_RETURN(HA_ADMIN_OK);
}
