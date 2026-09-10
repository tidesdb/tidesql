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

#endif /* HA_TIDESDB_STATUS_H */
