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
   the commit, rollback, savepoint, connection-close and consistent-snapshot callbacks live in
   ha_tidesdb_txn.cc, the two-phase-commit ones (XA prepare / commit_by_xid / rollback_by_xid /
   recover) in ha_tidesdb_txn_xa.cc, and the plugin installs them all through tidesdb_txn_register
   at init.  the handler-method side (ensure_stmt_txn, external_lock, store_lock) belongs to
   ha_tidesdb.  the helpers below are shared between the two files. */

/**
 * tidesdb_txn_register
 * install the transaction, savepoint, consistent-snapshot, two-phase-commit (XA), and
 * connection-close callbacks on the handlerton and set the per-transaction savepoint storage size
 * @param hton the plugin's handlerton, already allocated by the server at init
 */
void tidesdb_txn_register(handlerton *hton);

/**
 * tidesdb_xa_register
 * install the two-phase-commit callbacks, prepare, recover, commit_by_xid and rollback_by_xid, on
 * the handlerton, called from tidesdb_txn_register
 * @param hton the plugin's handlerton
 */
void tidesdb_xa_register(handlerton *hton);

/**
 * stmt_savepoint_disarm
 * release the per-statement savepoint a statement armed and drop the statement's undo state, which
 * has to happen before the transaction is committed or prepared since the library refuses either
 * while a savepoint is open
 * @param trx the connection's transaction
 */
void stmt_savepoint_disarm(tidesdb_trx_t *trx);

/**
 * tdb_trx_abandon
 * end a transaction whose prepare or commit failed, rolling the library transaction back in
 * whatever state it reached, freeing it, dropping the foreign key and galera holds once the
 * library has let go of its claims, and leaving the connection with no transaction so its next
 * statement begins a fresh one
 * @param trx the connection's transaction, whose txn is gone on return
 * @param rc the library result code of the failure
 * @param what a short call-site tag for the log line
 * @return the handler error for rc
 */
int tdb_trx_abandon(tidesdb_trx_t *trx, int rc, const char *what);

/**
 * tdb_drop_prepared_entry
 * drop a connection's entry in the prepared-transaction registry when its own commit or rollback
 * resolves the XA transaction, so a later commit_by_xid cannot touch the same transaction
 * @param trx the connection's transaction
 */
void tdb_drop_prepared_entry(tidesdb_trx_t *trx);

#ifdef WITH_WSREP
/**
 * tdb_wsrep_commit_failed
 * stop the node when a transaction Galera already certified fails to commit here, since every
 * other node commits it and this one could no longer match the cluster; DDL and a thread with
 * wsrep off keep their ordinary error
 * @param thd the session
 * @param rc the library result code of the failure
 */
void tdb_wsrep_commit_failed(THD *thd, int rc);
#endif

#endif /* HA_TIDESDB_TXN_H */
