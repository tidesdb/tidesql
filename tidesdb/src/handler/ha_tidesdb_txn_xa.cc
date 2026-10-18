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

/* the two-phase commit side of the transaction surface.  phase one folds the pending fts meta into
   the transaction and durably prepares its write batch under the connection's XID, an external
   XA PREPARE additionally hands the prepared transaction to a registry and detaches it from the
   connection, so a later XA COMMIT or ROLLBACK on any connection, or after a restart, can resolve
   it by XID, and recover hands the server the XIDs the library found in doubt at startup.  under
   Galera the same prepare runs for every local write transaction before certification, so a
   conflict is refused before anything replicates.  the commit, rollback and savepoint side is in
   ha_tidesdb_txn.cc. */

#include "ha_tidesdb.h"

#include <mysql/plugin.h>

#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "sql_class.h"
#include "sql_priv.h"
#include "src/handler/ha_tidesdb_fts.h"
#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_txn.h"

/* Prepared transactions awaiting their phase-two decision, keyed by the serialized XID.  An
   external XA PREPARE hands its library transaction here and detaches it from the connection, and
   crash recovery repopulates it from tidesdb_recover_prepared at startup, so commit_by_xid /
   rollback_by_xid can resolve a transaction from any connection or after a restart.  The map owns
   each transaction until it is resolved (then freed) -- guarded by tdb_prepared_mtx. */
static std::mutex tdb_prepared_mtx;
static std::map<std::string, tidesdb_txn_t *> tdb_prepared_txns;

/* Recovery enumeration state, loaded once from the library on the first handlerton recover() call.
   The server drains the in-doubt XIDs in batches, so we keep a cursor over the loaded set. */
static std::vector<std::string> tdb_recovery_xids;
static size_t tdb_recovery_pos = 0;
static bool tdb_recovery_loaded = false;

/* Serialize a server XID into the opaque byte string the library and the prepared-txn map key on.
   XID::length() spans formatID plus the gtrid/bqual lengths and their data, the full identity the
   server compares transactions by. */
static std::string tdb_xid_key(const XID *xid)
{
    /* XID::length() reads the identity but was declared non-const before MariaDB made it const in a
       later release, so calling it through a const pointer fails to compile on the older servers.
       The call does not mutate the xid, so cast away const to bind to whichever declaration the
       linked server provides. */
    return std::string((const char *)xid, const_cast<XID *>(xid)->length());
}

/* Drop a connection's prepared-transaction registry entry when its own commit/rollback resolves the
   XA transaction, so a later commit_by_xid cannot touch the same (reused or freed) transaction. */
void tdb_drop_prepared_entry(tidesdb_trx_t *trx)
{
    if (trx->prepared_xid.empty()) return;
    {
        std::lock_guard<std::mutex> lk(tdb_prepared_mtx);
        tdb_prepared_txns.erase(trx->prepared_xid);
    }
    trx->prepared_xid.clear();
}

/* Prepare trx under the connection's XID, folding its pending full-text meta deltas in first.  A
   failure rolls the transaction back and ends it, and is returned as a handler error. */
static int tdb_prepare_trx(THD *thd, tidesdb_trx_t *trx, std::string &key)
{
    stmt_savepoint_disarm(trx);

    if (trx->dirty)
    {
        int frc = flush_trx_fts_meta_pending(thd, trx);
        if (frc != TDB_SUCCESS) return tdb_trx_abandon(trx, frc, "hton_prepare fts_meta_flush");
    }

    XID xid;
    thd_get_xid(thd, (MYSQL_XID *)&xid);
    key = tdb_xid_key(&xid);

    int rc = tidesdb_txn_prepare(trx->txn, (const uint8_t *)key.data(), key.size());
    if (rc != TDB_SUCCESS) return tdb_trx_abandon(trx, rc, "hton_prepare");
    return 0;
}

static bool tdb_trx_prepared(const tidesdb_trx_t *trx)
{
    tidesdb_txn_state_t st = TDB_TXN_STATE_ACTIVE;
    tidesdb_txn_state(trx->txn, &st);
    return st == TDB_TXN_STATE_PREPARED;
}

#if MYSQL_VERSION_ID >= 110800
static int tidesdb_prepare(THD *thd, bool all)
#else
static int tidesdb_prepare(handlerton *, THD *thd, bool all)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx || !trx->txn) return 0;
    if (!all) return 0; /* statement-level prepare defers, like statement-level commit */

    /* under Galera a write transaction was already prepared before it was certified */
    if (tdb_trx_prepared(trx)) return 0;
#ifdef WITH_WSREP
    /* a local transaction lets the appliers that wrote what it read finish first, an applier lets
       the local transactions it aborted end first */
    if (!tdb_wsrep_wait_for_appliers(thd, trx))
        return tdb_rc_to_ha(TDB_ERR_CONFLICT, "hton_prepare");
    if (!tdb_wsrep_wait_for_victims(trx)) tdb_wsrep_commit_failed(thd, TDB_ERR_LOCKED);
#endif

    bool is_external = (thd_sql_command(thd) == SQLCOM_XA_PREPARE);

    /* A read-only transaction driven by the internal 2PC coordinator has nothing durable to
       prepare; leave it for commit() to roll back.  An external XA PREPARE still records it so the
       later XA COMMIT/ROLLBACK can resolve the XID. */
    if (!trx->dirty && !is_external)
    {
        stmt_savepoint_disarm(trx);
        return 0;
    }

    std::string key;
#ifdef WITH_WSREP
    if (int rc = tdb_wsrep_before_engine_commit(thd, trx)) return rc;
#endif
    if (int rc = tdb_prepare_trx(thd, trx, key)) return rc;

    if (is_external)
    {
        /* Publish the prepared transaction so it can also be resolved by XID -- from another
           connection, or after a restart via recover().  The transaction stays attached to this
           connection: a same-connection XA COMMIT/ROLLBACK runs through commit()/rollback(), which
           drop this registry entry, while a disconnect leaves it in-doubt for the registry to own.
         */
        std::lock_guard<std::mutex> lk(tdb_prepared_mtx);
        tdb_prepared_txns[key] = trx->txn;
        trx->prepared_xid = std::move(key);
    }
    return 0;
}

#ifdef WITH_WSREP
/* Galera certifies a transaction before the engine commits it, and every node commits what it
   certified, so the engine's own conflict check must not be left to run after certification.  The
   engine is optimistic, it finds conflicts by validating a transaction as it commits, and its
   prepare is that validation together with a durable record of the writes, after which committing
   cannot conflict.  So a local write transaction is prepared here, from the hook the server runs
   before certification, and a conflict refuses it before anything replicates, with the error a
   conflict at commit gives without Galera.  Once certified it commits the prepared writes.  A
   statement ending inside a transaction is not the commit Galera certifies and is left alone.  The
   prepare carries the server's own transaction id, so after a crash before the commit the server's
   recovery rolls it back.  A transaction that read what an applier in flight wrote waits for that
   applier first, so the prepare validates the read against the applied version rather than
   claiming it under the applier. */
int tdb_wsrep_prepare_before_certification(THD *thd)
{
    if (!wsrep_thd_is_local(thd)) return 0;
    if (ha_count_rw_2pc(thd, false) > 0 &&
        thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
        return 0;
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx || !trx->txn || !trx->dirty || tdb_trx_prepared(trx)) return 0;
    std::string key;
    int rc =
        tdb_wsrep_wait_for_appliers(thd, trx) ? 0 : tdb_rc_to_ha(TDB_ERR_CONFLICT, "hton_prepare");
    if (!rc) rc = tdb_wsrep_before_engine_commit(thd, trx);
    if (!rc) rc = tdb_prepare_trx(thd, trx, key);
    if (rc) my_error(ER_ERROR_DURING_COMMIT, MYF(0), rc);
    return rc;
}
#endif

#if MYSQL_VERSION_ID >= 110800
static int tidesdb_commit_by_xid(XID *xid)
#else
static int tidesdb_commit_by_xid(handlerton *, XID *xid)
#endif
{
    std::string key = tdb_xid_key(xid);
    tidesdb_txn_t *txn = NULL;
    {
        std::lock_guard<std::mutex> lk(tdb_prepared_mtx);
        auto it = tdb_prepared_txns.find(key);
        if (it == tdb_prepared_txns.end()) return 0; /* not one of ours */
        txn = it->second;
        tdb_prepared_txns.erase(it);
    }
    int rc = tidesdb_txn_commit_prepared(txn);
    if (rc != TDB_SUCCESS)
    {
        /* A transient failure leaves the transaction prepared; re-register so the coordinator can
           retry the decision. */
        std::lock_guard<std::mutex> lk(tdb_prepared_mtx);
        tdb_prepared_txns[key] = txn;
        return tdb_rc_to_ha(rc, "commit_by_xid");
    }
    tidesdb_txn_free(txn);
    return 0;
}

#if MYSQL_VERSION_ID >= 110800
static int tidesdb_rollback_by_xid(XID *xid)
#else
static int tidesdb_rollback_by_xid(handlerton *, XID *xid)
#endif
{
    std::string key = tdb_xid_key(xid);
    tidesdb_txn_t *txn = NULL;
    {
        std::lock_guard<std::mutex> lk(tdb_prepared_mtx);
        auto it = tdb_prepared_txns.find(key);
        if (it == tdb_prepared_txns.end()) return 0; /* not one of ours */
        txn = it->second;
        tdb_prepared_txns.erase(it);
    }
    int rc = tidesdb_txn_rollback_prepared(txn);
    if (rc != TDB_SUCCESS)
    {
        std::lock_guard<std::mutex> lk(tdb_prepared_mtx);
        tdb_prepared_txns[key] = txn;
        return tdb_rc_to_ha(rc, "rollback_by_xid");
    }
    tidesdb_txn_free(txn);
    return 0;
}

/* load the transactions the library recovered as in-doubt after the last shutdown into the prepared
   registry and the recovery cursor, once.  caller holds tdb_prepared_mtx. */
static void tdb_load_recovered_prepared()
{
    if (tdb_recovery_loaded) return;
    tdb_recovery_loaded = true;
    if (!tdb_global) return;

    int count = 0;
    if (tidesdb_recover_prepared(tdb_global, NULL, 0, &count) != TDB_SUCCESS || count <= 0) return;

    std::vector<tidesdb_prepared_txn_t> buf((size_t)count);
    int got = 0;
    if (tidesdb_recover_prepared(tdb_global, buf.data(), count, &got) != TDB_SUCCESS) return;

    for (int i = 0; i < got; i++)
    {
        std::string key((const char *)buf[i].xid, buf[i].xid_size);
        tdb_prepared_txns[key] = buf[i].txn;
        tdb_recovery_xids.push_back(std::move(key));
    }
}

/* hand the server the in-doubt XIDs recovered from storage, in batches drained across calls, so it
   can rebuild its transaction cache and let the operator resolve each with XA COMMIT/ROLLBACK. */
#if MYSQL_VERSION_ID >= 110800
static int tidesdb_recover(XID *xid_list, uint len)
#else
static int tidesdb_recover(handlerton *, XID *xid_list, uint len)
#endif
{
    if (!xid_list || len == 0) return 0;

    std::lock_guard<std::mutex> lk(tdb_prepared_mtx);
    tdb_load_recovered_prepared();

    uint n = 0;
    while (tdb_recovery_pos < tdb_recovery_xids.size() && n < len)
    {
        const std::string &k = tdb_recovery_xids[tdb_recovery_pos++];
        size_t clen = k.size() < sizeof(XID) ? k.size() : sizeof(XID);
        memset(&xid_list[n], 0, sizeof(XID));
        memcpy(&xid_list[n], k.data(), clen);
        n++;
    }
    return (int)n;
}

void tidesdb_xa_register(handlerton *hton)
{
    /* prepare durably logs the write batch under the XID, the by-xid resolvers finish a prepared
       transaction from any connection, and recover replays the ones the library found in doubt
       after a restart */
    hton->prepare = tidesdb_prepare;
    hton->recover = tidesdb_recover;
    hton->commit_by_xid = tidesdb_commit_by_xid;
    hton->rollback_by_xid = tidesdb_rollback_by_xid;
}
