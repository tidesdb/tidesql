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

#ifndef HA_TIDESDB_LIFECYCLE_H
#define HA_TIDESDB_LIFECYCLE_H

/* the handlerton table and database drop callbacks.  the plugin wires these into the handlerton at
   init so a DROP TABLE or DROP DATABASE that bypasses an open handler still removes the underlying
   column families and on-disk directories.  the handler-method side of the same story
   (rename_table, delete_table) lives on ha_tidesdb and is declared with the class. */

/**
 * tidesdb_hton_drop_table
 * remove a table's column families and directory in response to a handlerton drop_table
 * @param path the server table path being dropped
 * @return 0 on success or a handler error code
 */
int tidesdb_hton_drop_table(handlerton *, const char *path);

/**
 * tidesdb_hton_drop_database
 * remove every column family belonging to a dropped database directory
 * @param path the server database directory being dropped
 */
void tidesdb_hton_drop_database(handlerton *, char *path);

#endif /* HA_TIDESDB_LIFECYCLE_H */
