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

/* engine status reporting: SHOW ENGINE TIDESDB STATUS and the SHOW STATUS LIKE 'tidesdb%' counters.
   the counters are refreshed on demand behind a short ttl (no background thread); tombstone and
   density figures are aggregated lazily in their SHOW_FUNC callbacks. */

#include "ha_tidesdb.h"

#include <mysql/plugin.h>
#include <tidesdb/tidesdb_version.h>

#include <atomic>
#include <cstring>
#include <string>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/engine/ha_tidesdb_status.h"
#include "src/handler/ha_tidesdb_internal.h"

static void tidesdb_refresh_status_vars();

/* Forward declarations for the tombstone aggregates so tidesdb_show_status
   (defined earlier than the storage block) can read them. */
static long long srv_stat_total_tombstones;
static double srv_stat_tombstone_ratio;
static double srv_stat_max_sst_density;
static long long srv_stat_max_sst_density_level;

/* ******************** Status variables (SHOW GLOBAL STATUS LIKE 'tidesdb%') ********************
 */

/* Static holders for status variable values.  Populated by the SHOW_FUNC
   callback which queries tidesdb_get_db_stats / tidesdb_get_cache_stats.
   These are global (not per-connection) since they reflect database-wide state. */
static long long srv_stat_column_families;
static long long srv_stat_global_seq;
static long long srv_stat_memtable_bytes;
static long long srv_stat_txn_memory_bytes;
static long long srv_stat_total_sstables;
static long long srv_stat_open_sstables;
static long long srv_stat_data_size_bytes;
static long long srv_stat_immutable_memtables;
static long long srv_stat_flush_pending;
static long long srv_stat_compaction_queue;
static long long srv_stat_cache_entries;
static long long srv_stat_cache_bytes;
static long long srv_stat_cache_hits;
static long long srv_stat_cache_misses;
static double srv_stat_cache_hit_rate;
static long long srv_stat_cache_partitions;

/* Memtable flush state.  memtable_is_flushing is 1 while the shared memtable is
   rotating to sstables, and wal_generation counts wal rotations; both come from the
   db-level stats for monitoring. */
static long long srv_stat_memtable_is_flushing;
static long long srv_stat_wal_generation;

/* Write-amplification counters surfaced by the lib's recent stats patch.
   Lifetime since open, on-disk framed bytes for every byte the engine
   wrote, plus the logical denominator and the number of sstables produced
   by flush and compaction.  Monitoring tools divide the byte counters
   by user_bytes_written for the database-wide WA ratio. */
static long long srv_stat_flush_bytes_written;
static long long srv_stat_compaction_bytes_written;
static long long srv_stat_compaction_bytes_read;
static long long srv_stat_user_bytes_written;
static long long srv_stat_flush_count;
static long long srv_stat_compaction_count;

/* Live transaction and garbage-collection floor.  active_transactions is how many
   transactions are joined to the mvcc registry right now, and min_snapshot_seq is
   the oldest snapshot still pinned, the sequence compaction will not reclaim past.
   A min_snapshot_seq lagging far behind global_sequence points at a long-running
   transaction holding back reclamation. */
static long long srv_stat_active_transactions;
static long long srv_stat_min_snapshot_seq;

/* Value log observability.  vlog_file_size is the on-disk size of the value log,
   vlog_value_count the number of values it currently indexes, vlog_used_bytes
   the uncompressed length those values represent, and vlog_bytes_written the
   lifetime bytes appended to the value log, which is output the flush and
   compaction counters do not see once values separate. */
static long long srv_stat_vlog_file_size;
static long long srv_stat_vlog_value_count;
static long long srv_stat_vlog_used_bytes;
static long long srv_stat_vlog_bytes_written;

/* L0 admission backpressure surfaced by TidesDB 10.  writes_throttled counts
   commits the admission policy made dwell before admitting, writes_blocked commits
   it made wait for the flush queue to drain, write_stall_us the total microseconds
   commits spent held in admission, and write_stall_ceiling_hits the commits admitted
   only because the wait ceiling expired.  Any ceiling hits mean flush is not keeping
   up with ingestion. */
static long long srv_stat_writes_throttled;
static long long srv_stat_writes_blocked;
static long long srv_stat_write_stall_us;
static long long srv_stat_write_stall_ceiling_hits;

/* Device-write accounting from tidesdb_get_io_stats.  The library meters writes
   through its file-descriptor manager, which covers the sstable and wal devices;
   the value log keeps its own byte accounting in the Value Log section above, so
   only sstable and wal are surfaced as counters here.  ops and bytes are lifetime
   totals; the per-write timing the api also reports is nondeterministic and stays
   in the free-text SHOW ENGINE STATUS only. */
static long long srv_stat_io_sstable_write_ops;
static long long srv_stat_io_sstable_write_bytes;
static long long srv_stat_io_wal_write_ops;
static long long srv_stat_io_wal_write_bytes;

/* Write-stall counts by reason from tidesdb_get_stall_stats, each the number of
   times a commit stalled for that reason since open.  The admission reason is the
   unflushed-backlog backpressure whose aggregate microseconds the Write Stalls
   section already sums; splitting the count per reason lets a monitor separate wal
   append waits from memtable rotation and manifest commit waits.  The stall time
   is nondeterministic and prints only in the free-text status. */
static long long srv_stat_stall_wal_append;
static long long srv_stat_stall_rotate_lock;
static long long srv_stat_stall_rotate_work;
static long long srv_stat_stall_admission;
static long long srv_stat_stall_manifest_commit;

/* Codec-chain encoding aggregates from tidesdb_get_klog_encoding_stats and its vlog
   counterpart, summed across every chain.  logical is the size before encoding and
   stored the size after, so logical divided by stored is the realized compression
   ratio for the key log and the value log.  The per-chain codec breakdown prints in
   the free-text status. */
static long long srv_stat_klog_logical_bytes;
static long long srv_stat_klog_stored_bytes;
static long long srv_stat_vlog_encoded_logical_bytes;
static long long srv_stat_vlog_encoded_stored_bytes;

/* Tombstone aggregates are forward-declared near the top of this file so
   tidesdb_show_status can read them directly.  Their definitions live up
   there. */

static const char *srv_stat_version = TIDESQL_VERSION_STR;
static long long srv_stat_version_hex = TIDESQL_VERSION_HEX;

/* Version of the vendored libtidesdb storage library this engine is built
   against (TIDESQL_VERSION above is the plugin/handler version, which is
   independent of the underlying library release). */
static const char *srv_stat_library_version = TIDESDB_VERSION;

/* Tombstone and density figures are diagnostics that only SHOW ENGINE TIDESDB
   STATUS and the SHOW GLOBAL STATUS vars below read, never query planning.  Each
   needs a per column family stats pass that walks the shared memtable, so
   they are computed on demand when queried rather than folded into the on-demand
   db-stats refresh pass.  A short TTL folds the four reads of one SHOW STATUS
   statement into a single pass. */
static long long srv_tombstone_stats_last_us = 0;
static void tidesdb_compute_tombstone_stats()
{
    if (!tdb_global) return;
    long long now = (long long)microsecond_interval_timer();
    if (srv_tombstone_stats_last_us != 0 && now - srv_tombstone_stats_last_us < 500000) return;
    srv_tombstone_stats_last_us = now;

    char **cf_names = NULL;
    int cf_count = 0;
    if (tidesdb_list_column_families(tdb_global, &cf_names, &cf_count) == TDB_SUCCESS && cf_names)
    {
        uint64_t total_tomb = 0, total_keys = 0;
        double max_density = 0.0;
        int max_density_level = 0;
        for (int i = 0; i < cf_count; i++)
        {
            if (!cf_names[i]) continue;
            tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, cf_names[i]);
            if (!cf) continue;
            tidesdb_cf_stats_t st;
            if (tidesdb_get_cf_stats(cf, &st) == TDB_SUCCESS)
            {
                total_tomb += st.total_tombstones;
                total_keys += st.total_keys;
                if (st.max_sst_density > max_density)
                {
                    max_density = st.max_sst_density;
                    max_density_level = st.max_sst_density_level;
                }
            }
        }
        for (int i = 0; i < cf_count; i++) tidesdb_free(cf_names[i]);
        tidesdb_free(cf_names);

        srv_stat_total_tombstones = (long long)total_tomb;
        srv_stat_tombstone_ratio = total_keys > 0 ? (double)total_tomb / (double)total_keys : 0.0;
        srv_stat_max_sst_density = max_density;
        srv_stat_max_sst_density_level = (long long)max_density_level;
    }
}
static int tdb_show_total_tombstones(MYSQL_THD, struct st_mysql_show_var *var, void *,
                                     struct system_status_var *, enum enum_var_type)
{
    tidesdb_compute_tombstone_stats();
    var->type = SHOW_LONGLONG;
    var->value = (char *)&srv_stat_total_tombstones;
    return 0;
}
static int tdb_show_tombstone_ratio(MYSQL_THD, struct st_mysql_show_var *var, void *,
                                    struct system_status_var *, enum enum_var_type)
{
    tidesdb_compute_tombstone_stats();
    var->type = SHOW_DOUBLE;
    var->value = (char *)&srv_stat_tombstone_ratio;
    return 0;
}
static int tdb_show_max_sst_density(MYSQL_THD, struct st_mysql_show_var *var, void *,
                                    struct system_status_var *, enum enum_var_type)
{
    tidesdb_compute_tombstone_stats();
    var->type = SHOW_DOUBLE;
    var->value = (char *)&srv_stat_max_sst_density;
    return 0;
}
static int tdb_show_max_sst_density_level(MYSQL_THD, struct st_mysql_show_var *var, void *,
                                          struct system_status_var *, enum enum_var_type)
{
    tidesdb_compute_tombstone_stats();
    var->type = SHOW_LONGLONG;
    var->value = (char *)&srv_stat_max_sst_density_level;
    return 0;
}

/* The db-level and cache counters.  Names carry no tidesdb_ prefix; the SHOW_ARRAY export below is
   registered under the outer name "tidesdb", and SHOW joins the two with an underscore, so each
   surfaces as tidesdb_<name>.  The write-amplification counters are lifetime-since-open; divide a
   byte counter by user_bytes_written for its per-domain WA ratio, and flush_count /
   compaction_count count output sstables, not logical runs. */
static struct st_mysql_show_var tidesdb_status_vars_inner[] = {
    {"version", (char *)&srv_stat_version, SHOW_CHAR_PTR},
    {"version_hex", (char *)&srv_stat_version_hex, SHOW_LONGLONG},
    {"library_version", (char *)&srv_stat_library_version, SHOW_CHAR_PTR},
    {"column_families", (char *)&srv_stat_column_families, SHOW_LONGLONG},
    {"global_sequence", (char *)&srv_stat_global_seq, SHOW_LONGLONG},
    {"memtable_bytes", (char *)&srv_stat_memtable_bytes, SHOW_LONGLONG},
    {"txn_memory_bytes", (char *)&srv_stat_txn_memory_bytes, SHOW_LONGLONG},
    {"total_sstables", (char *)&srv_stat_total_sstables, SHOW_LONGLONG},
    {"open_sstables", (char *)&srv_stat_open_sstables, SHOW_LONGLONG},
    {"data_size_bytes", (char *)&srv_stat_data_size_bytes, SHOW_LONGLONG},
    {"immutable_memtables", (char *)&srv_stat_immutable_memtables, SHOW_LONGLONG},
    {"flush_pending", (char *)&srv_stat_flush_pending, SHOW_LONGLONG},
    {"compaction_queue", (char *)&srv_stat_compaction_queue, SHOW_LONGLONG},
    {"cache_entries", (char *)&srv_stat_cache_entries, SHOW_LONGLONG},
    {"cache_bytes", (char *)&srv_stat_cache_bytes, SHOW_LONGLONG},
    {"cache_hits", (char *)&srv_stat_cache_hits, SHOW_LONGLONG},
    {"cache_misses", (char *)&srv_stat_cache_misses, SHOW_LONGLONG},
    {"cache_hit_rate", (char *)&srv_stat_cache_hit_rate, SHOW_DOUBLE},
    {"cache_partitions", (char *)&srv_stat_cache_partitions, SHOW_LONGLONG},
    SHOW_FUNC_ENTRY("total_tombstones", &tdb_show_total_tombstones),
    SHOW_FUNC_ENTRY("tombstone_ratio", &tdb_show_tombstone_ratio),
    SHOW_FUNC_ENTRY("max_sst_tombstone_density", &tdb_show_max_sst_density),
    SHOW_FUNC_ENTRY("max_sst_tombstone_density_level", &tdb_show_max_sst_density_level),
    {"memtable_is_flushing", (char *)&srv_stat_memtable_is_flushing, SHOW_LONGLONG},
    {"wal_generation", (char *)&srv_stat_wal_generation, SHOW_LONGLONG},
    {"flush_bytes_written", (char *)&srv_stat_flush_bytes_written, SHOW_LONGLONG},
    {"compaction_bytes_written", (char *)&srv_stat_compaction_bytes_written, SHOW_LONGLONG},
    {"compaction_bytes_read", (char *)&srv_stat_compaction_bytes_read, SHOW_LONGLONG},
    {"user_bytes_written", (char *)&srv_stat_user_bytes_written, SHOW_LONGLONG},
    {"flush_count", (char *)&srv_stat_flush_count, SHOW_LONGLONG},
    {"compaction_count", (char *)&srv_stat_compaction_count, SHOW_LONGLONG},
    {"active_transactions", (char *)&srv_stat_active_transactions, SHOW_LONGLONG},
    {"min_snapshot_sequence", (char *)&srv_stat_min_snapshot_seq, SHOW_LONGLONG},
    {"vlog_file_size", (char *)&srv_stat_vlog_file_size, SHOW_LONGLONG},
    {"vlog_value_count", (char *)&srv_stat_vlog_value_count, SHOW_LONGLONG},
    {"vlog_used_bytes", (char *)&srv_stat_vlog_used_bytes, SHOW_LONGLONG},
    {"vlog_bytes_written", (char *)&srv_stat_vlog_bytes_written, SHOW_LONGLONG},
    {"writes_throttled", (char *)&srv_stat_writes_throttled, SHOW_LONGLONG},
    {"writes_blocked", (char *)&srv_stat_writes_blocked, SHOW_LONGLONG},
    {"write_stall_us", (char *)&srv_stat_write_stall_us, SHOW_LONGLONG},
    {"write_stall_ceiling_hits", (char *)&srv_stat_write_stall_ceiling_hits, SHOW_LONGLONG},
    {"io_sstable_write_ops", (char *)&srv_stat_io_sstable_write_ops, SHOW_LONGLONG},
    {"io_sstable_write_bytes", (char *)&srv_stat_io_sstable_write_bytes, SHOW_LONGLONG},
    {"io_wal_write_ops", (char *)&srv_stat_io_wal_write_ops, SHOW_LONGLONG},
    {"io_wal_write_bytes", (char *)&srv_stat_io_wal_write_bytes, SHOW_LONGLONG},
    {"stall_wal_append", (char *)&srv_stat_stall_wal_append, SHOW_LONGLONG},
    {"stall_rotate_lock", (char *)&srv_stat_stall_rotate_lock, SHOW_LONGLONG},
    {"stall_rotate_work", (char *)&srv_stat_stall_rotate_work, SHOW_LONGLONG},
    {"stall_admission", (char *)&srv_stat_stall_admission, SHOW_LONGLONG},
    {"stall_manifest_commit", (char *)&srv_stat_stall_manifest_commit, SHOW_LONGLONG},
    {"klog_logical_bytes", (char *)&srv_stat_klog_logical_bytes, SHOW_LONGLONG},
    {"klog_stored_bytes", (char *)&srv_stat_klog_stored_bytes, SHOW_LONGLONG},
    {"vlog_encoded_logical_bytes", (char *)&srv_stat_vlog_encoded_logical_bytes, SHOW_LONGLONG},
    {"vlog_encoded_stored_bytes", (char *)&srv_stat_vlog_encoded_stored_bytes, SHOW_LONGLONG},
    {NullS, NullS, SHOW_LONGLONG}};

/* SHOW STATUS export: refresh the db-level counters once for this SHOW, then hand back the inner
   array.  Replacing the old background refresher, it keeps the counters current whenever they are
   observed while costing nothing when no one is looking. */
static int tidesdb_show_status_vars_export(MYSQL_THD, struct st_mysql_show_var *var, void *,
                                           struct system_status_var *, enum enum_var_type)
{
    tidesdb_refresh_status_vars();
    var->type = SHOW_ARRAY;
    var->value = (char *)&tidesdb_status_vars_inner;
    return 0;
}

struct st_mysql_show_var tidesdb_status_variables[] = {
    SHOW_FUNC_ENTRY("tidesdb", &tidesdb_show_status_vars_export), {NullS, NullS, SHOW_LONGLONG}};

/* Refresh the static status variables from live tidesdb stats.  Cost is
   paid by the caller (SHOW ENGINE STATUS / SHOW GLOBAL STATUS), never on
   the write path. */
static void tidesdb_refresh_status_vars()
{
    if (!tdb_global) return;

    /* On-demand refresh coalesced under a short ttl, so the many counters in one SHOW STATUS
       trigger a single db-stats pass and rapid pollers reuse the snapshot.  The compare-exchange
       elects one refresher per window, keeping concurrent SHOWs off each other's writes.  This
       replaces the old per-second background thread -- the counters are current whenever they are
       observed and cost nothing when no one looks. */
    static std::atomic<long long> refresh_last_us{0};
    long long now = (long long)microsecond_interval_timer();
    long long last = refresh_last_us.load(std::memory_order_relaxed);
    /* last == 0 means no refresh has run yet, so the counters are still at their
       static zero and must be populated regardless of how little time the timer
       reads since boot.  Only once a window has actually run do we coalesce the
       rapid follow-up reads of one SHOW under the ttl. */
    if (last != 0 && now - last <= TIDESDB_STATS_REFRESH_US) return;
    if (!refresh_last_us.compare_exchange_strong(last, now, std::memory_order_relaxed)) return;

    tidesdb_db_stats_t db_st;
    memset(&db_st, 0, sizeof(db_st));
    tidesdb_get_db_stats(tdb_global, &db_st);

    tidesdb_cache_stats_t cache_st;
    memset(&cache_st, 0, sizeof(cache_st));
    tidesdb_get_cache_stats(tdb_global, &cache_st);

    srv_stat_column_families = db_st.num_column_families;
    srv_stat_global_seq = (long long)db_st.global_seq;
    srv_stat_memtable_bytes = (long long)db_st.memtable_bytes;
    srv_stat_txn_memory_bytes = (long long)db_st.txn_memory_bytes;
    srv_stat_total_sstables = db_st.total_sstable_count;
    srv_stat_open_sstables = db_st.num_open_sstables;
    srv_stat_data_size_bytes = (long long)db_st.total_data_size_bytes;
    srv_stat_immutable_memtables = db_st.immutable_memtable_count;
    srv_stat_flush_pending = db_st.immutable_memtable_count;
    srv_stat_compaction_queue = (long long)db_st.compaction_pending_count;
    srv_stat_cache_entries = (long long)cache_st.total_entries;
    srv_stat_cache_bytes = (long long)cache_st.total_bytes;
    srv_stat_cache_hits = (long long)cache_st.hits;
    srv_stat_cache_misses = (long long)cache_st.misses;
    srv_stat_cache_hit_rate = cache_st.hit_rate * PERCENT_SCALE;
    srv_stat_cache_partitions = (long long)cache_st.num_partitions;

    /* The memtable is db-level in TidesDB 10. */
    srv_stat_memtable_is_flushing = db_st.is_flushing ? 1 : 0;
    srv_stat_wal_generation = (long long)db_st.wal_generation;

    /* Write-amplification counters.  Per-WAL byte counters are no longer surfaced. */
    srv_stat_flush_bytes_written = (long long)db_st.flush_bytes_written;
    srv_stat_compaction_bytes_written = (long long)db_st.compaction_bytes_written;
    srv_stat_compaction_bytes_read = (long long)db_st.compaction_bytes_read;
    srv_stat_user_bytes_written = (long long)db_st.user_bytes_written;
    srv_stat_flush_count = (long long)db_st.flush_count;
    srv_stat_compaction_count = (long long)db_st.compaction_count;

    /* Live transaction and gc floor. */
    srv_stat_active_transactions = db_st.active_txn_count;
    srv_stat_min_snapshot_seq = (long long)db_st.min_snapshot_seq;

    /* Value log. */
    srv_stat_vlog_file_size = (long long)db_st.vlog_file_size;
    srv_stat_vlog_value_count = (long long)db_st.vlog_value_count;
    srv_stat_vlog_used_bytes = (long long)db_st.vlog_used_bytes;
    srv_stat_vlog_bytes_written = (long long)db_st.vlog_bytes_written;

    /* L0 admission backpressure. */
    srv_stat_writes_throttled = (long long)db_st.writes_throttled;
    srv_stat_writes_blocked = (long long)db_st.writes_blocked;
    srv_stat_write_stall_us = (long long)db_st.write_stall_us;
    srv_stat_write_stall_ceiling_hits = (long long)db_st.write_stall_ceiling_hits;

    /* Device-write counters.  A transient TDB_ERR_LOCKED leaves the prior snapshot
       in place rather than zeroing a live counter, so a failed sample is stale, not
       wrong. */
    tidesdb_io_stats_t io_st;
    memset(&io_st, 0, sizeof(io_st));
    if (tidesdb_get_io_stats(tdb_global, &io_st) == TDB_SUCCESS)
    {
        srv_stat_io_sstable_write_ops = (long long)io_st.classes[TDB_IO_SSTABLE].ops;
        srv_stat_io_sstable_write_bytes = (long long)io_st.classes[TDB_IO_SSTABLE].bytes;
        srv_stat_io_wal_write_ops = (long long)io_st.classes[TDB_IO_WAL].ops;
        srv_stat_io_wal_write_bytes = (long long)io_st.classes[TDB_IO_WAL].bytes;
    }

    /* Write-stall counts split by reason. */
    tidesdb_stall_stats_t stall_st;
    memset(&stall_st, 0, sizeof(stall_st));
    if (tidesdb_get_stall_stats(tdb_global, &stall_st) == TDB_SUCCESS)
    {
        srv_stat_stall_wal_append = (long long)stall_st.reasons[TDB_STALL_WAL_APPEND].count;
        srv_stat_stall_rotate_lock = (long long)stall_st.reasons[TDB_STALL_ROTATE_LOCK].count;
        srv_stat_stall_rotate_work = (long long)stall_st.reasons[TDB_STALL_ROTATE_WORK].count;
        srv_stat_stall_admission = (long long)stall_st.reasons[TDB_STALL_ADMISSION].count;
        srv_stat_stall_manifest_commit =
            (long long)stall_st.reasons[TDB_STALL_MANIFEST_COMMIT].count;
    }

    /* Codec-chain encoding aggregates, summed over every chain for the key log and
       the value log so a monitor can track the realized compression ratio. */
    tidesdb_encoding_stats_t enc[TDB_MAX_ENCODING_CHAINS];
    size_t enc_count = 0;
    memset(enc, 0, sizeof(enc));
    if (tidesdb_get_klog_encoding_stats(tdb_global, enc, TDB_MAX_ENCODING_CHAINS, &enc_count) ==
        TDB_SUCCESS)
    {
        uint64_t logical = 0, stored = 0;
        for (size_t i = 0; i < enc_count; i++)
        {
            logical += enc[i].logical_bytes;
            stored += enc[i].stored_bytes;
        }
        srv_stat_klog_logical_bytes = (long long)logical;
        srv_stat_klog_stored_bytes = (long long)stored;
    }

    enc_count = 0;
    memset(enc, 0, sizeof(enc));
    if (tidesdb_get_vlog_encoding_stats(tdb_global, enc, TDB_MAX_ENCODING_CHAINS, &enc_count) ==
        TDB_SUCCESS)
    {
        uint64_t logical = 0, stored = 0;
        for (size_t i = 0; i < enc_count; i++)
        {
            logical += enc[i].logical_bytes;
            stored += enc[i].stored_bytes;
        }
        srv_stat_vlog_encoded_logical_bytes = (long long)logical;
        srv_stat_vlog_encoded_stored_bytes = (long long)stored;
    }

    /* Tombstone and density figures are computed on demand in their SHOW_FUNC
       callbacks, keeping this on-demand refresh clear of the per-CF stats pass. */
}

/* format the database-level sections (identity, memory, storage, background, write amplification)
   of SHOW ENGINE TIDESDB STATUS into buf starting at pos; returns the new write position. */
static int status_format_db_stats(char *buf, size_t sz, int pos, const tidesdb_db_stats_t &db_st)
{
    pos += snprintf(buf + pos, sz - pos,
                    "================== TidesDB Engine Status ==================\n");
    pos += snprintf(buf + pos, sz - pos, "Data directory: %s\n", tdb_path.c_str());
    pos += snprintf(buf + pos, sz - pos, "Column families: %d\n", db_st.num_column_families);
    pos += snprintf(buf + pos, sz - pos, "Global sequence: %lu\n", (unsigned long)db_st.global_seq);
    pos += snprintf(buf + pos, sz - pos, "Min snapshot sequence: %lu\n",
                    (unsigned long)db_st.min_snapshot_seq);
    pos += snprintf(buf + pos, sz - pos, "Active transactions: %d\n", db_st.active_txn_count);
    pos += snprintf(buf + pos, sz - pos, "\n=+=+= Memory =+=+=\n");
    pos += snprintf(buf + pos, sz - pos, "Memtable bytes: %ld\n", (long)db_st.memtable_bytes);
    pos += snprintf(buf + pos, sz - pos, "Transaction memory bytes: %ld\n",
                    (long)db_st.txn_memory_bytes);
    pos += snprintf(buf + pos, sz - pos, "\n=+=+= Storage =+=+=\n");
    pos += snprintf(buf + pos, sz - pos, "Total SSTables: %d\n", db_st.total_sstable_count);
    pos += snprintf(buf + pos, sz - pos, "Open SSTable handles: %d\n", db_st.num_open_sstables);
    pos += snprintf(buf + pos, sz - pos, "Total data size: %lu bytes\n",
                    (unsigned long)db_st.total_data_size_bytes);
    pos +=
        snprintf(buf + pos, sz - pos, "Immutable memtables: %d\n", db_st.immutable_memtable_count);
    pos += snprintf(buf + pos, sz - pos, "\n=+=+= Background =+=+=\n");
    pos += snprintf(buf + pos, sz - pos, "Flush pending: %d\n", db_st.immutable_memtable_count);
    pos +=
        snprintf(buf + pos, sz - pos, "Compaction pending: %d\n", db_st.compaction_pending_count);
    pos +=
        snprintf(buf + pos, sz - pos, "Currently flushing: %s\n", db_st.is_flushing ? "YES" : "NO");
    pos +=
        snprintf(buf + pos, sz - pos, "WAL generation: %lu\n", (unsigned long)db_st.wal_generation);
    pos += snprintf(buf + pos, sz - pos, "Next CF index: %u\n", db_st.next_cf_index);

    /* Write amplification counters, lifetime since open.  Value-log appends are
       output the flush and compaction counters never see, so a store with value
       separation active under-reports its true write amplification without them. */
    const uint64_t total_out_bytes =
        db_st.flush_bytes_written + db_st.compaction_bytes_written + db_st.vlog_bytes_written;
    double wa_total = 0.0;
    if (db_st.user_bytes_written > 0)
        wa_total = (double)total_out_bytes / (double)db_st.user_bytes_written;
    pos += snprintf(buf + pos, sz - pos, "\n=+=+= Write Amplification =+=+=\n");
    pos += snprintf(buf + pos, sz - pos, "User bytes written: %lu\n",
                    (unsigned long)db_st.user_bytes_written);
    pos += snprintf(buf + pos, sz - pos, "Flush bytes written: %lu (%lu sstables)\n",
                    (unsigned long)db_st.flush_bytes_written, (unsigned long)db_st.flush_count);
    pos += snprintf(buf + pos, sz - pos, "Compaction bytes written: %lu (%lu sstables)\n",
                    (unsigned long)db_st.compaction_bytes_written,
                    (unsigned long)db_st.compaction_count);
    pos += snprintf(buf + pos, sz - pos, "Compaction bytes read: %lu\n",
                    (unsigned long)db_st.compaction_bytes_read);
    pos += snprintf(buf + pos, sz - pos, "Vlog bytes written: %lu\n",
                    (unsigned long)db_st.vlog_bytes_written);
    pos += snprintf(buf + pos, sz - pos, "Total WA ratio: %.2fx\n", wa_total);

    pos += snprintf(buf + pos, sz - pos, "\n=+=+= Value Log =+=+=\n");
    pos += snprintf(buf + pos, sz - pos, "File size: %lu bytes\n",
                    (unsigned long)db_st.vlog_file_size);
    pos += snprintf(buf + pos, sz - pos, "Indexed values: %lu\n",
                    (unsigned long)db_st.vlog_value_count);
    pos += snprintf(buf + pos, sz - pos, "Used bytes: %lu\n", (unsigned long)db_st.vlog_used_bytes);
    pos += snprintf(buf + pos, sz - pos, "Stored bytes: %lu\n",
                    (unsigned long)db_st.vlog_stored_bytes);
    pos += snprintf(buf + pos, sz - pos, "Live bytes: %lu\n", (unsigned long)db_st.vlog_live_bytes);
    pos += snprintf(buf + pos, sz - pos, "Dead bytes: %lu\n", (unsigned long)db_st.vlog_dead_bytes);
    pos += snprintf(buf + pos, sz - pos, "Segments: %lu (drainable %lu, retired %lu)\n",
                    (unsigned long)db_st.vlog_segment_count,
                    (unsigned long)db_st.vlog_segments_drainable,
                    (unsigned long)db_st.vlog_segments_retired);
    pos +=
        snprintf(buf + pos, sz - pos, "Reclaim: %lu calls, %lu passes\n",
                 (unsigned long)db_st.vlog_reclaim_calls, (unsigned long)db_st.vlog_reclaim_passes);

    pos += snprintf(buf + pos, sz - pos, "\n=+=+= Write Stalls =+=+=\n");
    pos += snprintf(buf + pos, sz - pos, "Writes throttled: %lu\n",
                    (unsigned long)db_st.writes_throttled);
    pos +=
        snprintf(buf + pos, sz - pos, "Writes blocked: %lu\n", (unsigned long)db_st.writes_blocked);
    pos +=
        snprintf(buf + pos, sz - pos, "Stall time: %lu us\n", (unsigned long)db_st.write_stall_us);
    pos += snprintf(buf + pos, sz - pos, "Admission ceiling hits: %lu\n",
                    (unsigned long)db_st.write_stall_ceiling_hits);
    return pos;
}

/* format the block-cache section into buf starting at pos; returns the new write position. */
static int status_format_cache_stats(char *buf, size_t sz, int pos,
                                     const tidesdb_cache_stats_t &cache_st)
{
    pos += snprintf(buf + pos, sz - pos, "\n=+=+= Block Cache =+=+=\n");
    pos += snprintf(buf + pos, sz - pos, "Enabled: %s\n", cache_st.enabled ? "YES" : "NO");
    pos += snprintf(buf + pos, sz - pos, "Entries: %lu\n", (unsigned long)cache_st.total_entries);
    pos += snprintf(buf + pos, sz - pos, "Size: %lu bytes\n", (unsigned long)cache_st.total_bytes);
    pos += snprintf(buf + pos, sz - pos, "Hits: %lu\n", (unsigned long)cache_st.hits);
    pos += snprintf(buf + pos, sz - pos, "Misses: %lu\n", (unsigned long)cache_st.misses);
    pos += snprintf(buf + pos, sz - pos, "Hit rate: %.1f%%\n", cache_st.hit_rate * PERCENT_SCALE);
    pos +=
        snprintf(buf + pos, sz - pos, "Partitions: %lu\n", (unsigned long)cache_st.num_partitions);
    return pos;
}

/* format the tombstone-observability section into buf starting at pos; returns the new write
   position.  the aggregates come from the tidesdb_refresh_status_vars pass the caller ran. */
static int status_format_tombstones(char *buf, size_t sz, int pos)
{
    pos += snprintf(buf + pos, sz - pos, "\n=+=+= Tombstones =+=+=\n");
    pos +=
        snprintf(buf + pos, sz - pos, "Total tombstones: %ld\n", (long)srv_stat_total_tombstones);
    pos += snprintf(buf + pos, sz - pos, "Tombstone ratio: %.2f%%\n",
                    srv_stat_tombstone_ratio * PERCENT_SCALE);
    pos += snprintf(buf + pos, sz - pos, "Worst SSTable density: %.2f%% at level %ld\n",
                    srv_stat_max_sst_density * PERCENT_SCALE, (long)srv_stat_max_sst_density_level);
    return pos;
}

/* format one line per column family into buf starting at pos; returns the new write position.
   Each family reports its level distribution so a scan's true cost, the number of overlapping
   runs a range opens, is visible per table, alongside read amplification, the b+tree shape, and
   the unflushed key count still resident in the shared memtable.  The loop leaves a margin before
   the buffer end so a family with many levels never runs snprintf past the fixed status buffer. */
static int status_format_cf_stats(char *buf, size_t sz, int pos)
{
    char **cf_names = NULL;
    int cf_count = 0;
    if (tidesdb_list_column_families(tdb_global, &cf_names, &cf_count) != TDB_SUCCESS || !cf_names)
        return pos;

    /* Stop formatting once we are within this many bytes of the buffer end so a long family
       line always has room and pos never overruns sizeof(buf). */
    static constexpr int STATUS_CF_MARGIN = 512;

    pos += snprintf(buf + pos, sz - pos, "\n=+=+= Column Families =+=+=\n");
    for (int i = 0; i < cf_count; i++)
    {
        if (!cf_names[i]) continue;
        /* Internal engine column families such as the foreign-key catalog are not
           user tables, so we leave them out of the per-family report. */
        if (strncmp(cf_names[i], "__tidesdb", 9) == 0) continue;
        if (pos >= (int)sz - STATUS_CF_MARGIN)
        {
            pos += snprintf(buf + pos, sz - pos, "... %d more families truncated\n", cf_count - i);
            break;
        }
        tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, cf_names[i]);
        if (!cf) continue;
        tidesdb_cf_stats_t st;
        memset(&st, 0, sizeof(st));
        if (tidesdb_get_cf_stats(cf, &st) != TDB_SUCCESS) continue;

        pos += snprintf(buf + pos, sz - pos,
                        "%s: %d levels, %lu keys (%lu unflushed), %lu bytes, read amp %.2f\n",
                        cf_names[i], st.num_levels, (unsigned long)st.total_keys,
                        (unsigned long)st.unflushed_key_count, (unsigned long)st.total_data_size,
                        st.read_amp);
        pos +=
            snprintf(buf + pos, sz - pos,
                     "  avg key %.0f, avg value %.0f, btree height %.1f/%u over %lu nodes, "
                     "tombstone ratio %.2f%%\n",
                     st.avg_key_size, st.avg_value_size, st.btree_avg_height, st.btree_max_height,
                     (unsigned long)st.btree_total_nodes, st.tombstone_ratio * PERCENT_SCALE);
        pos += snprintf(buf + pos, sz - pos, "  levels [sstables/keys/tombstones/bytes]:");
        for (int l = 0; l < st.num_levels && l < TDB_MAX_LEVELS; l++)
            pos += snprintf(buf + pos, sz - pos, " L%d[%d/%lu/%lu/%lu]", l + 1,
                            st.level_num_sstables[l], (unsigned long)st.level_key_counts[l],
                            (unsigned long)st.level_tombstone_counts[l],
                            (unsigned long)st.level_sizes[l]);
        pos += snprintf(buf + pos, sz - pos, "\n  WAL %lu bytes, %lu compactions\n",
                        (unsigned long)st.wal_bytes_written, (unsigned long)st.compaction_count);
    }

    for (int i = 0; i < cf_count; i++) tidesdb_free(cf_names[i]);
    tidesdb_free(cf_names);
    return pos;
}

/* format the device-write section into buf starting at pos; returns the new write position.  the
   library meters writes through its descriptor manager, so this covers the sstable and wal devices,
   and reports per-write timing whose average and worst case help spot a slow disk.  a class the api
   does not meter, such as the value log which keeps its own accounting, shows zero here. */
static int status_format_io_stats(char *buf, size_t sz, int pos, const tidesdb_io_stats_t &io_st)
{
    pos += snprintf(buf + pos, sz - pos, "\n=+=+= IO Device Writes =+=+=\n");
    for (int c = 0; c < TDB_IO_COUNT; c++)
    {
        const tidesdb_io_stat_t &s = io_st.classes[c];
        double avg_us = s.ops > 0 ? (double)s.total_us / (double)s.ops : 0.0;
        pos += snprintf(buf + pos, sz - pos, "%s: %lu writes, %lu bytes, avg %.1f us, max %lu us\n",
                        tidesdb_io_class_name((tidesdb_io_class_t)c), (unsigned long)s.ops,
                        (unsigned long)s.bytes, avg_us, (unsigned long)s.max_us);
    }
    return pos;
}

/* format the write-stall-by-reason section into buf starting at pos; returns the new write
   position.  each reason a commit can stall on carries its count and the total and worst time
   spent there, so backpressure can be attributed to wal appends, memtable rotation, admission
   backlog, or manifest commits. */
static int status_format_stall_stats(char *buf, size_t sz, int pos,
                                     const tidesdb_stall_stats_t &stall_st)
{
    pos += snprintf(buf + pos, sz - pos, "\n=+=+= Write Stalls By Reason =+=+=\n");
    for (int r = 0; r < TDB_STALL_COUNT; r++)
    {
        const tidesdb_stall_stat_t &s = stall_st.reasons[r];
        pos += snprintf(buf + pos, sz - pos, "%s: %lu stalls, %lu us total, %lu us max\n",
                        tidesdb_stall_reason_name((tidesdb_stall_reason_t)r),
                        (unsigned long)s.count, (unsigned long)s.total_us, (unsigned long)s.max_us);
    }
    return pos;
}

/* format the encoding summary for one log (the key log or the value log) into buf starting at pos;
   returns the new write position.  it rolls the per-chain codec stats into the chain count and the
   total logical and stored bytes, so the realized compression ratio is visible while the section
   keeps a fixed shape rather than a variable-length per-chain table, whose line count depends on
   which sstables and value segments currently exist and so is not reproducible in a test. */
static int status_format_encoding_summary(char *buf, size_t sz, int pos, const char *title,
                                          const tidesdb_encoding_stats_t *enc, size_t count)
{
    uint64_t logical = 0, stored = 0;
    for (size_t i = 0; i < count; i++)
    {
        logical += enc[i].logical_bytes;
        stored += enc[i].stored_bytes;
    }
    double ratio = stored > 0 ? (double)logical / (double)stored : 0.0;
    pos += snprintf(buf + pos, sz - pos, "\n=+=+= %s Encoding =+=+=\n", title);
    pos +=
        snprintf(buf + pos, sz - pos, "Chains: %zu, logical %lu bytes, stored %lu bytes, %.2fx\n",
                 count, (unsigned long)logical, (unsigned long)stored, ratio);
    return pos;
}

bool tidesdb_show_status(handlerton *hton, THD *thd, stat_print_fn *print, enum ha_stat_type stat)
{
    if (stat != HA_ENGINE_STATUS) return false;
    if (!tdb_global) return false;

    tidesdb_refresh_status_vars();

    tidesdb_db_stats_t db_st;
    memset(&db_st, 0, sizeof(db_st));
    tidesdb_get_db_stats(tdb_global, &db_st);

    tidesdb_cache_stats_t cache_st;
    memset(&cache_st, 0, sizeof(cache_st));
    tidesdb_get_cache_stats(tdb_global, &cache_st);

    tidesdb_io_stats_t io_st;
    memset(&io_st, 0, sizeof(io_st));
    tidesdb_get_io_stats(tdb_global, &io_st);

    tidesdb_stall_stats_t stall_st;
    memset(&stall_st, 0, sizeof(stall_st));
    tidesdb_get_stall_stats(tdb_global, &stall_st);

    tidesdb_encoding_stats_t klog_enc[TDB_MAX_ENCODING_CHAINS];
    tidesdb_encoding_stats_t vlog_enc[TDB_MAX_ENCODING_CHAINS];
    size_t klog_enc_count = 0, vlog_enc_count = 0;
    memset(klog_enc, 0, sizeof(klog_enc));
    memset(vlog_enc, 0, sizeof(vlog_enc));
    tidesdb_get_klog_encoding_stats(tdb_global, klog_enc, TDB_MAX_ENCODING_CHAINS, &klog_enc_count);
    tidesdb_get_vlog_encoding_stats(tdb_global, vlog_enc, TDB_MAX_ENCODING_CHAINS, &vlog_enc_count);

    /* Output buffer for SHOW ENGINE TIDESDB STATUS.  32 KiB holds the fixed
       sections plus a per-family block for a schema with dozens of column
       families; families past that margin are truncated with a trailing note.
       Heap-allocated rather than stacked so a concurrent handler thread keeps a
       modest frame. */
    static constexpr size_t TIDESDB_STATUS_BUF_LEN = 32768;
    char *buf = (char *)my_malloc(PSI_NOT_INSTRUMENTED, TIDESDB_STATUS_BUF_LEN, MYF(MY_WME));
    if (!buf) return false;
    int pos = 0;

    pos = status_format_db_stats(buf, TIDESDB_STATUS_BUF_LEN, pos, db_st);
    pos = status_format_cache_stats(buf, TIDESDB_STATUS_BUF_LEN, pos, cache_st);
    pos = status_format_io_stats(buf, TIDESDB_STATUS_BUF_LEN, pos, io_st);
    pos = status_format_stall_stats(buf, TIDESDB_STATUS_BUF_LEN, pos, stall_st);
    pos = status_format_encoding_summary(buf, TIDESDB_STATUS_BUF_LEN, pos, "Key Log", klog_enc,
                                         klog_enc_count);
    pos = status_format_encoding_summary(buf, TIDESDB_STATUS_BUF_LEN, pos, "Value Log", vlog_enc,
                                         vlog_enc_count);
    pos = status_format_tombstones(buf, TIDESDB_STATUS_BUF_LEN, pos);
    pos = status_format_cf_stats(buf, TIDESDB_STATUS_BUF_LEN, pos);

    static constexpr const char TIDESDB_ENGINE_NAME[] = "TIDESDB";
    static constexpr uint TIDESDB_ENGINE_NAME_LEN = sizeof(TIDESDB_ENGINE_NAME) - 1;
    bool rc = print(thd, TIDESDB_ENGINE_NAME, TIDESDB_ENGINE_NAME_LEN, "", 0, buf, (size_t)pos);
    my_free(buf);
    return rc;
}
