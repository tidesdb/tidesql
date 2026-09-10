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

#ifndef HA_TIDESDB_TXN_H
#define HA_TIDESDB_TXN_H

/* the transaction and savepoint handlerton surface.  a single TidesDB transaction spans a whole
   BEGIN..COMMIT (or one autocommit statement) and is shared by every handler on the connection;
   the commit, rollback, savepoint, connection-close, consistent-snapshot, and two-phase-commit
   (XA prepare / commit_by_xid / rollback_by_xid / recover) callbacks live in ha_tidesdb_txn.cc, and
   the plugin installs them through tidesdb_txn_register at init.  the handler-method side
   (ensure_stmt_txn, external_lock, store_lock) belongs to ha_tidesdb. */

/**
 * tidesdb_txn_register
 * install the transaction, savepoint, consistent-snapshot, two-phase-commit (XA), and
 * connection-close callbacks on the handlerton and set the per-transaction savepoint storage size
 * @param hton the plugin's handlerton, already allocated by the server at init
 */
void tidesdb_txn_register(handlerton *hton);

#endif /* HA_TIDESDB_TXN_H */
