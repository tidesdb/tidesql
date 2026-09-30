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

#ifndef HA_TIDESDB_STATUS_H
#define HA_TIDESDB_STATUS_H

/* the engine's status reporting, extracted from the monolith: SHOW ENGINE TIDESDB STATUS and the
   SHOW STATUS LIKE 'tidesdb%' counters, all computed on demand (no background thread).  Only the
   two registration surfaces the plugin wires up are exposed here; the counters, the refresh, and
   the tombstone aggregation stay private to ha_tidesdb_status.cc.  include after ha_tidesdb.h. */

/**
 * tidesdb_show_status
 * the handlerton show_status hook: format SHOW ENGINE TIDESDB STATUS from the db-level stats,
 * refreshing them on demand first
 * @return false on success, matching the handlerton contract
 */
bool tidesdb_show_status(handlerton *hton, THD *thd, stat_print_fn *print, enum ha_stat_type stat);

/* the plugin's status-variable array, one SHOW_ARRAY export under the "tidesdb" prefix; registered
   in the maria_declare_plugin block. */
extern struct st_mysql_show_var tidesdb_status_variables[];

#include <atomic>

/* Engine error counters, bumped in tdb_rc_to_ha where every library error is mapped, and read by
   SHOW STATUS straight from the atomics, which is fine for an aligned 64-bit counter.
   commit_conflicts is the number of COMMIT statements that failed with a conflict-class error, a
   deadlock or a lock wait timeout, so a client-side transactions-per-minute figure can subtract it
   from Com_commit, which the server increments for a failed COMMIT as much as a successful one.
   The stmt_ counters classify the errors raised inside a statement by the library cause behind
   them, a write conflict, transient contention reported as locked, memtable backpressure, a
   transaction that outlived its timeout, and a galera brute-force abort, so a run can tell real
   contention from engine churn without guessing.  Defined in ha_tidesdb_status.cc. */
extern std::atomic<long long> tdb_stat_commit_conflicts;
extern std::atomic<long long> tdb_stat_stmt_conflicts;
extern std::atomic<long long> tdb_stat_stmt_locked;
extern std::atomic<long long> tdb_stat_stmt_memory_limit;
extern std::atomic<long long> tdb_stat_stmt_txn_expired;
extern std::atomic<long long> tdb_stat_stmt_bf_aborted;

#endif /* HA_TIDESDB_STATUS_H */
