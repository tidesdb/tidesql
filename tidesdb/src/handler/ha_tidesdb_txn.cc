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

#include "src/handler/ha_tidesdb_txn.h"

#include <mysql/plugin.h>

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "ha_tidesdb.h"
#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/engine/ha_tidesdb_config.h"
#include "src/handler/ha_tidesdb_fts.h"
#include "src/handler/ha_tidesdb_internal.h"

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
    return std::string((const char *)xid, xid->length());
}

/* Commit a transaction honoring its 2PC state: a prepared transaction takes the phase-two
   commit-prepared path, an active one the normal blocking commit. */
static int tdb_txn_commit_stateful(THD *thd, tidesdb_txn_t *txn)
{
    tidesdb_txn_state_t st = TDB_TXN_STATE_ACTIVE;
    tidesdb_txn_state(txn, &st);
    if (st == TDB_TXN_STATE_PREPARED) return tidesdb_txn_commit_prepared(txn);
    return tdb_txn_commit_blocking(thd, txn);
}

/* Roll back a transaction honoring its 2PC state: a prepared transaction takes the phase-two
   rollback-prepared path, an active one the normal rollback. */
static void tdb_txn_rollback_stateful(tidesdb_txn_t *txn)
{
    tidesdb_txn_state_t st = TDB_TXN_STATE_ACTIVE;
    tidesdb_txn_state(txn, &st);
    if (st == TDB_TXN_STATE_PREPARED)
        tidesdb_txn_rollback_prepared(txn);
    else
        tidesdb_txn_rollback(txn);
}

/* Drop a connection's prepared-transaction registry entry when its own commit/rollback resolves the
   XA transaction, so a later commit_by_xid cannot touch the same (reused or freed) transaction. */
static void tdb_drop_prepared_entry(tidesdb_trx_t *trx)
{
    if (trx->prepared_xid.empty()) return;
    {
        std::lock_guard<std::mutex> lk(tdb_prepared_mtx);
        tdb_prepared_txns.erase(trx->prepared_xid);
    }
    trx->prepared_xid.clear();
}
/* Reserved name of the per-statement savepoint.  The SQL SAVEPOINT callbacks
   synthesize "sv_%p" names, so this never collides with a user savepoint. */
static constexpr const char TIDESDB_STMT_SAVEPOINT[] = "stmt";

/* Arm a statement savepoint at statement start.  Caller guarantees we are in a
   multi-statement transaction (not autocommit/DDL), trx->txn exists, and no
   statement savepoint is currently armed.  On any failure we leave the savepoint
   unarmed so a later statement rollback safely falls back to full rollback. */
static void stmt_savepoint_arm(tidesdb_trx_t *trx)
{
    /* Defensive: a prior statement's savepoint should already be gone (released
       on success, removed on rollback), but a user ROLLBACK TO SAVEPOINT can
       collaterally drop it; releasing again is a cheap no-op if absent. */
    (void)tidesdb_txn_release_savepoint(trx->txn, TIDESDB_STMT_SAVEPOINT);
    if (tidesdb_txn_savepoint(trx->txn, TIDESDB_STMT_SAVEPOINT) != TDB_SUCCESS) return;

    trx->stmt_savepoint_active = true;
    trx->stmt_fts_snapshot = trx->fts_meta_pending;
    trx->stmt_fts_dirty_snapshot = trx->fts_meta_dirty;
}

/* Statement completed successfully -- the statement's writes are now permanent
   within the (still uncommitted) txn.  Drop the savepoint and per-statement undo
   state so the next statement re-arms cleanly. */
static void stmt_savepoint_disarm(tidesdb_trx_t *trx)
{
    if (!trx->stmt_savepoint_active) return;
    (void)tidesdb_txn_release_savepoint(trx->txn, TIDESDB_STMT_SAVEPOINT);
    trx->stmt_savepoint_active = false;
    trx->stmt_fts_snapshot.clear();
}

/* Roll back just the current statement's effects to the armed savepoint.
   Returns true if the partial rollback was performed, false if the caller must
   fall back to a full transaction rollback (no savepoint armed, or the library
   savepoint is gone because a bulk mid-commit reset the txn or a user ROLLBACK
   TO SAVEPOINT removed it). */
static bool stmt_savepoint_rollback(tidesdb_trx_t *trx)
{
    if (!trx->stmt_savepoint_active) return false;

    int rc = tidesdb_txn_rollback_to_savepoint(trx->txn, TIDESDB_STMT_SAVEPOINT);
    if (rc != TDB_SUCCESS)
    {
        /* Savepoint vanished under us -- cannot do a partial rollback. */
        trx->stmt_savepoint_active = false;
        trx->stmt_fts_snapshot.clear();
        return false;
    }

    trx->fts_meta_pending = trx->stmt_fts_snapshot;
    trx->fts_meta_dirty = trx->stmt_fts_dirty_snapshot;
    trx->stmt_fts_snapshot.clear();

    /* The library freed the txn ops appended after the savepoint, so any cached
       scan iterator over this txn on every handler on this connection now has a
       stale view.  Bumping the generation makes each handler rebuild lazily,
       exactly as a bulk mid-commit does. */
    trx->txn_generation++;
    trx->stmt_savepoint_active = false;
    return true;
}

/* ******************** Per-connection transaction helpers ******************** */

/*
  Get or create the per-connection TidesDB transaction context.
  The txn lives for the entire BEGIN...COMMIT block (or single auto-commit
  statement).  All handler objects on the same connection share it.
*/
static tidesdb_trx_t *get_or_create_trx(THD *thd, handlerton *hton, tidesdb_isolation_level_t iso)
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, hton);
    if (trx)
    {
        if (!trx->txn)
        {
            int rc = tidesdb_txn_begin_with_isolation(tdb_global, iso, &trx->txn);
            if (rc != TDB_SUCCESS)
            {
                (void)tdb_rc_to_ha(rc, "get_or_create_trx txn_begin(reuse)");
                return NULL;
            }
            trx->dirty = false;
            trx->isolation_level = iso;
            trx->txn_generation++;
        }
        else if (trx->needs_reset)
        {
            /* Txn object kept alive from previous commit/rollback (see
               tidesdb_commit).  We reset it to get a fresh MVCC snapshot at
               current-transaction-start.  This avoids the expensive
               free+begin cycle while ensuring we see the latest data.
               The bulk-insert path already uses commit+reset successfully.
               Only reset when needs_reset is true (set after real commit/
               rollback) to preserve snapshot within multi-statement txns. */
            int rrc = tidesdb_txn_reset(trx->txn, iso);
            if (rrc != TDB_SUCCESS)
            {
                /* Reset failed -- we fall back to free + begin.  Surface the
                   failure so we can spot regressions in txn recycling instead
                   of silently degrading to per-statement free+begin. */
                sql_print_warning(
                    "[TIDESDB] tidesdb_txn_reset failed (rc=%d), falling back to "
                    "free+begin -- expect higher per-statement overhead until "
                    "this is investigated",
                    rrc);
                tidesdb_txn_free(trx->txn);
                trx->txn = NULL;
                int rc = tidesdb_txn_begin_with_isolation(tdb_global, iso, &trx->txn);
                if (rc != TDB_SUCCESS)
                {
                    (void)tdb_rc_to_ha(rc, "get_or_create_trx txn_begin(reset_fallback)");
                    return NULL;
                }
            }
            trx->needs_reset = false;
            trx->isolation_level = iso;
            trx->txn_generation++;
        }
        return trx;
    }

    /* The trx struct owns a std::vector (fts_meta_pending), so it must be
       constructed and destroyed properly.  Switching from MY_ZEROFILL/my_free
       to new/delete runs the std::vector's ctor/dtor and gives every field
       its default value via the header's member initialisers. */
    trx = new tidesdb_trx_t{};
    if (!trx) return NULL;

    int rc = tidesdb_txn_begin_with_isolation(tdb_global, iso, &trx->txn);
    if (rc != TDB_SUCCESS)
    {
        delete trx;
        (void)tdb_rc_to_ha(rc, "get_or_create_trx txn_begin(new)");
        return NULL;
    }
    trx->isolation_level = iso;
    trx->txn_generation = 1;
    thd_set_ha_data(thd, hton, trx);
    return trx;
}

/* ******************** Handlerton transaction callbacks ******************** */

/* Maximum length of a TidesDB savepoint name, including the trailing NUL.
   Names are synthesized via TIDESDB_SAVEPOINT_NAME_FMT below; 32 bytes
   fits the decoded pointer plus prefix on all supported platforms. */
static constexpr uint TIDESDB_SAVEPOINT_NAME_MAX = 32;
/* Format used to synthesize a unique savepoint name for the TidesDB
   transaction layer.  The pointer to the SQL-layer savepoint slot is
   the only handle we have that survives across the set/rollback/release
   callbacks, so we encode it as the engine-level savepoint name. */
static constexpr const char TIDESDB_SAVEPOINT_NAME_FMT[] = "sv_%p";

struct tidesdb_savepoint_t
{
    char name[TIDESDB_SAVEPOINT_NAME_MAX];
#ifdef WITH_WSREP
    /* how many galera write-intent keys the transaction held when this savepoint was set, so a
       rollback to it can drop the intents the reverted statements registered past this point. */
    size_t wsrep_intent_mark;
#endif
};

#if MYSQL_VERSION_ID >= 110800
static int tidesdb_savepoint_set(THD *thd, void *sv)
#else
static int tidesdb_savepoint_set(handlerton *, THD *thd, void *sv)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx || !trx->txn || !sv) return 0;

    tidesdb_savepoint_t *sp = (tidesdb_savepoint_t *)sv;
    snprintf(sp->name, sizeof(sp->name), TIDESDB_SAVEPOINT_NAME_FMT, sv);
#ifdef WITH_WSREP
    sp->wsrep_intent_mark = trx->wsrep_intent_keys.size();
#endif

    int rc = tidesdb_txn_savepoint(trx->txn, sp->name);
    if (rc == TDB_SUCCESS) return 0;
    return tdb_rc_to_ha(rc, "savepoint_set");
}

#if MYSQL_VERSION_ID >= 110800
static int tidesdb_savepoint_rollback(THD *thd, void *sv)
#else
static int tidesdb_savepoint_rollback(handlerton *, THD *thd, void *sv)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx || !trx->txn || !sv) return 0;

    tidesdb_savepoint_t *sp = (tidesdb_savepoint_t *)sv;
    if (!sp->name[0]) snprintf(sp->name, sizeof(sp->name), TIDESDB_SAVEPOINT_NAME_FMT, sv);

    int rc = tidesdb_txn_rollback_to_savepoint(trx->txn, sp->name);
    if (rc == TDB_SUCCESS)
    {
#ifdef WITH_WSREP
        /* drop the galera write intents the reverted statements registered past this savepoint. */
        tidesdb_wsrep_intent_truncate(trx, sp->wsrep_intent_mark);
#endif
        /* The TidesDB library may drop the savepoint as part of the rollback.
           SQL semantics require the savepoint to still exist after rollback,
           so we re-create it here to allow RELEASE SAVEPOINT to succeed. */
        (void)tidesdb_txn_savepoint(trx->txn, sp->name);
        return 0;
    }
    if (rc == TDB_ERR_NOT_FOUND) return HA_ERR_NO_SAVEPOINT;
    return tdb_rc_to_ha(rc, "savepoint_rollback");
}

#if MYSQL_VERSION_ID >= 110800
static bool tidesdb_savepoint_rollback_can_release_mdl(THD *)
#else
static bool tidesdb_savepoint_rollback_can_release_mdl(handlerton *, THD *)
#endif
{
    return true;
}

#if MYSQL_VERSION_ID >= 110800
static int tidesdb_savepoint_release(THD *thd, void *sv)
#else
static int tidesdb_savepoint_release(handlerton *, THD *thd, void *sv)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx || !trx->txn || !sv) return 0;

    tidesdb_savepoint_t *sp = (tidesdb_savepoint_t *)sv;
    if (!sp->name[0]) snprintf(sp->name, sizeof(sp->name), TIDESDB_SAVEPOINT_NAME_FMT, sv);

    int rc = tidesdb_txn_release_savepoint(trx->txn, sp->name);
    if (rc == TDB_SUCCESS) return 0;
    if (rc == TDB_ERR_NOT_FOUND) return HA_ERR_NO_SAVEPOINT;
    return tdb_rc_to_ha(rc, "savepoint_release");
}

/* Perform the durable final commit of a real (non statement-level) transaction, leaving the txn
   object alive and reset-pending for reuse.  Shared by commit() and commit_ordered() so the actual
   commit happens exactly once whichever path the server drives. */
static int tdb_finalize_commit(THD *thd, tidesdb_trx_t *trx)
{
    /* We must release any active statement savepoint before final commit/rollback.
       Savepoints must be explicitly released before txn_commit.  Disarm also
       clears the per-statement undo journal and fts snapshot; it leaves
       fts_meta_pending intact for the flush below. */
    stmt_savepoint_disarm(trx);

    /* This connection is resolving its own transaction, so no by-XID resolver should reach it. */
    tdb_drop_prepared_entry(trx);

    /* Real commit -- flush to storage.
       After a successful commit, we keep the txn object alive and let
       get_or_create_trx() call tidesdb_txn_reset() to get a fresh
       snapshot.  This avoids the expensive free+begin cycle on every
       autocommit statement (saves malloc/free + internal buffer
       reallocation).  The bulk-insert path already uses commit+reset
       successfully, so the pattern is proven safe.
       If commit fails, fall back to rollback+free. */
    if (trx->dirty)
    {
        /* Fold the per-txn FTS meta deltas into this same txn before it
           commits so the meta update is atomic with the row writes that
           produced it. */
        int frc = flush_trx_fts_meta_pending(thd, trx);
        if (frc != TDB_SUCCESS)
        {
            sql_print_error(
                "[TIDESDB] hton_commit: flush_trx_fts_meta_pending returned %d (gen=%lu)", frc,
                (unsigned long)trx->txn_generation);
            tidesdb_txn_rollback(trx->txn);
            tidesdb_txn_free(trx->txn);
            trx->txn = NULL;
            trx->txn_generation++;
            trx->dirty = false;
            trx->stmt_savepoint_active = false;
            return tdb_rc_to_ha(frc, "hton_commit fts_meta_flush");
        }

        int rc = tdb_txn_commit_stateful(thd, trx->txn);
        if (rc != TDB_SUCCESS)
        {
            /* Only log truly unexpected errors (not transient conflicts). */
            if (rc != TDB_ERR_CONFLICT && rc != TDB_ERR_LOCKED && rc != TDB_ERR_MEMORY_LIMIT)
                sql_print_error(
                    "[TIDESDB] hton_commit: tidesdb_txn_commit returned %d "
                    "(dirty=%d gen=%lu)",
                    rc, trx->dirty, (unsigned long)trx->txn_generation);
            tdb_txn_rollback_stateful(trx->txn);
            tidesdb_txn_free(trx->txn);
            trx->txn = NULL;
            trx->txn_generation++;
            trx->dirty = false;
            trx->stmt_savepoint_active = false;
            return tdb_rc_to_ha(rc, "hton_commit");
        }
        /* We keep txn alive for reuse via txn_reset on next use. */
        trx->txn_generation++;
        trx->needs_reset = true;
    }
    else
    {
        /* Read-only transaction -- we rollback, keep alive for reuse. */
        trx->fts_meta_pending.clear();
        trx->fts_meta_dirty = false;
        tidesdb_txn_rollback(trx->txn);
        trx->txn_generation++;
        trx->needs_reset = true;
    }
#ifdef WITH_WSREP
    /* the transaction resolved, so drop its galera write-intent keys from the shared map. */
    tidesdb_wsrep_intent_clear(trx);
#endif
    trx->dirty = false;
    trx->stmt_savepoint_active = false;
    return 0;
}

#if MYSQL_VERSION_ID >= 110800
static int tidesdb_commit(THD *thd, bool all)
#else
static int tidesdb_commit(handlerton *, THD *thd, bool all)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx) return 0;

    /* commit_ordered already ran the durable commit in binlog order.  Report its outcome and skip a
       second commit.  Checked before the txn-null guard because a failed ordered commit frees txn.
     */
    if (trx->commit_ordered_done)
    {
        trx->commit_ordered_done = false;
        int rc = trx->commit_ordered_rc;
        trx->commit_ordered_rc = 0;
        return rc;
    }

    if (!trx->txn) return 0;

    /* We determine whether this is the final commit for the transaction.
       all=true         -> explicit COMMIT or transaction-level end
       all=false        -> statement-level; only a real commit when autocommit */
    bool is_real_commit = all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);

    if (!is_real_commit)
    {
        /* Statement-level commit inside a multi-statement transaction.
           Defer the actual commit -- writes stay buffered in the txn,
           avoiding an expensive txn_begin + commit per statement.  The
           statement succeeded, so its writes become permanent within the
           still-open txn; drop the statement savepoint armed in external_lock
           so the next statement re-arms at the new boundary.  (tidesdb_txn
           savepoints are O(1) -- they record op/cf counts, they do not copy
           the write-set -- so this per-statement arm/disarm is cheap.) */
        stmt_savepoint_disarm(trx);
        return 0;
    }

    return tdb_finalize_commit(thd, trx);
}

/* Group commit.  The server calls this in the binlog commit order for the whole transaction, ahead
   of commit(), so we run the durable commit here to land it in that order, stash the outcome, and
   let the following commit() report it.  The server can then run commit() outside the commit-order
   lock, which is the throughput win group commit exists for. */
static void tidesdb_commit_ordered(THD *thd, bool all)
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx || !trx->txn) return;

    /* commit_ordered is only invoked to commit the whole transaction, never at a statement end. */
    if (!(all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) return;

    trx->commit_ordered_rc = tdb_finalize_commit(thd, trx);
    trx->commit_ordered_done = true;
}

#if MYSQL_VERSION_ID >= 110800
static int tidesdb_rollback(THD *thd, bool all)
#else
static int tidesdb_rollback(handlerton *, THD *thd, bool all)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx) return 0;

    /* If commit_ordered already committed this transaction in binlog order, the decision was
       commit, so honor it and clear the handoff rather than rolling back a committed transaction.
     */
    if (trx->commit_ordered_done)
    {
        trx->commit_ordered_done = false;
        int rc = trx->commit_ordered_rc;
        trx->commit_ordered_rc = 0;
        return rc;
    }

    if (!trx->txn) return 0;

    bool is_real_rollback = all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);

    if (!is_real_rollback)
    {
        /* Statement-level rollback inside a multi-statement transaction.
           Roll back only this statement's effects to the savepoint armed in
           external_lock, preserving the rest of the transaction and its
           snapshot.  stmt_savepoint_rollback reverts the library op array and
           the plugin-side fts meta snapshot together.  It returns false when a partial
           rollback is impossible -- no savepoint armed, or a bulk mid-commit or
           user ROLLBACK TO SAVEPOINT removed it -- and we fall through to a full
           transaction rollback in that case. */
        if (stmt_savepoint_rollback(trx))
        {
            /* Leave trx->dirty as-is: earlier statements' writes remain in the
               still-open txn, and its snapshot is deliberately preserved. */
            return 0;
        }
    }

    if (trx->stmt_savepoint_active)
    {
        tidesdb_txn_release_savepoint(trx->txn, TIDESDB_STMT_SAVEPOINT);
        trx->stmt_savepoint_active = false;
    }

    /* This connection is resolving its own transaction, so no by-XID resolver should reach it. */
    tdb_drop_prepared_entry(trx);

    /* The accumulated FTS meta deltas track the rows being rolled back,
       so discard them along with the txn's other write state. */
    trx->fts_meta_pending.clear();
    trx->fts_meta_dirty = false;
    trx->stmt_fts_snapshot.clear();

    /* Full rollback -- we keep txn alive for reuse via reset on next use.  A transaction that was
       XA PREPAREd on this same connection takes the phase-two rollback path. */
    tdb_txn_rollback_stateful(trx->txn);
    trx->txn_generation++;
#ifdef WITH_WSREP
    /* the transaction resolved, so drop its galera write-intent keys from the shared map. */
    tidesdb_wsrep_intent_clear(trx);
#endif
    trx->needs_reset = true;
    trx->dirty = false;
    trx->stmt_savepoint_active = false;
    return 0;
}

#if MYSQL_VERSION_ID >= 110800
static int tidesdb_close_connection(THD *thd)
#else
static int tidesdb_close_connection(handlerton *, THD *thd)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (trx)
    {
        if (!trx->prepared_xid.empty())
        {
            /* A transaction XA PREPAREd on this connection stays in-doubt when the connection goes
               away: the registry keeps owning it so a later XA COMMIT/ROLLBACK -- or recovery after
               a restart -- can still resolve it.  Do not roll it back or free it here. */
        }
        else if (trx->txn)
        {
            tidesdb_txn_rollback(trx->txn);
            tidesdb_txn_free(trx->txn);
        }
        delete trx;
        thd_set_ha_data(thd, tidesdb_hton, NULL);
    }
    return 0;
}

/*
  START TRANSACTION WITH CONSISTENT SNAPSHOT callback.
  Eagerly creates a TidesDB transaction so the snapshot sequence number
  is captured now, not lazily at first data access.  Without this, rows
  committed by other connections between START TRANSACTION and the first
  SELECT would be visible.

  Uses the session's isolation level (SET TRANSACTION ISOLATION LEVEL)
  rather than hard-coding REPEATABLE_READ.  Falls back to RR if the
  session is at the default.
*/
#if MYSQL_VERSION_ID >= 110800
static int tidesdb_start_consistent_snapshot(THD *thd)
#else
static int tidesdb_start_consistent_snapshot(handlerton *, THD *thd)
#endif
{
    /* START TRANSACTION WITH CONSISTENT SNAPSHOT explicitly requests a
       point-in-time snapshot.  Always use at least SNAPSHOT isolation
       so the snapshot persists for the entire transaction, regardless of
       the session's default isolation level (e.g. READ_COMMITTED would
       refresh the snapshot on each read, violating CONSISTENT_SNAPSHOT
       semantics). */
    tidesdb_isolation_level_t iso = resolve_effective_isolation(thd, TDB_ISOLATION_REPEATABLE_READ);
    if (iso < TDB_ISOLATION_SNAPSHOT) iso = TDB_ISOLATION_SNAPSHOT;
    tidesdb_trx_t *trx = get_or_create_trx(thd, tidesdb_hton, iso);
    if (!trx) return 1;

    /* We register at both statement and transaction level so the server
       knows TidesDB is participating in this BEGIN block. */
    trans_register_ha(thd, false, tidesdb_hton, 0);
    trans_register_ha(thd, true, tidesdb_hton, 0);
    return 0;
}

/* ******************** Locking ******************** */

/*
  Lazy txn creation.  Gets the per-connection TidesDB txn (shared by
  all handler objects on this connection).  The txn spans the entire
  BEGIN...COMMIT block, not just one statement.
*/
int ha_tidesdb::ensure_stmt_txn()
{
    if (stmt_txn) return 0;

    THD *thd = cached_thd_ ? cached_thd_ : ha_thd();

    /* Isolation resolution mirrors the external_lock path:
         DDL                        -> READ_COMMITTED (avoids unbounded
                                       read-set growth across a long scan).
         autocommit single-stmt DML -> READ_COMMITTED (no concurrent
                                       modification within the same txn).
         multi-statement txn        -> session isolation so write-write
                                       conflict detection stays active.
       Prefer the per-statement cache populated by external_lock; fall
       back to the live THD call only when external_lock hasn't run yet
       (e.g. some DDL callbacks). */
    int sql_cmd;
    bool is_autocommit;
    if (cached_stmt_shape_valid_)
    {
        sql_cmd = cached_sql_cmd_;
        is_autocommit = cached_is_autocommit_;
    }
    else
    {
        sql_cmd = thd_sql_command(thd);
        is_autocommit = !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
    }
    bool is_ddl =
        (sql_cmd == SQLCOM_ALTER_TABLE || sql_cmd == SQLCOM_CREATE_INDEX ||
         sql_cmd == SQLCOM_DROP_INDEX || sql_cmd == SQLCOM_TRUNCATE || sql_cmd == SQLCOM_OPTIMIZE ||
         sql_cmd == SQLCOM_CREATE_TABLE || sql_cmd == SQLCOM_DROP_TABLE);
    tidesdb_isolation_level_t effective_iso;
    if (is_ddl || is_autocommit)
        effective_iso = TDB_ISOLATION_READ_COMMITTED;
    else
        /* Honour the session's chosen isolation. The library's OCC enforces it,
           snapshot reads under SNAPSHOT, read-set tracking under REPEATABLE_READ,
           and full SSI under SERIALIZABLE. */
        effective_iso = resolve_effective_isolation(
            thd, share ? share->isolation_level : TDB_ISOLATION_SNAPSHOT);
    tidesdb_trx_t *trx = get_or_create_trx(thd, ht, effective_iso);
    if (!trx) return HA_ERR_OUT_OF_MEM;

    stmt_txn = trx->txn;
    return 0;
}

int ha_tidesdb::external_lock_acquire(THD *thd)
{
    /* We resolve per-statement THD shape once and cache to ensure_stmt_txn
       reads the cache instead of re-calling thd_sql_command() and
       thd_test_options(). */
    int sql_cmd = thd_sql_command(thd);
    bool is_ddl =
        (sql_cmd == SQLCOM_ALTER_TABLE || sql_cmd == SQLCOM_CREATE_INDEX ||
         sql_cmd == SQLCOM_DROP_INDEX || sql_cmd == SQLCOM_TRUNCATE || sql_cmd == SQLCOM_OPTIMIZE ||
         sql_cmd == SQLCOM_CREATE_TABLE || sql_cmd == SQLCOM_DROP_TABLE);
    bool is_autocommit = !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);

    cached_sql_cmd_ = sql_cmd;
    cached_is_autocommit_ = is_autocommit;
    cached_stmt_shape_valid_ = true;

    tidesdb_isolation_level_t effective_iso;
    if (is_ddl || is_autocommit)
        effective_iso = TDB_ISOLATION_READ_COMMITTED;
    else
        effective_iso = resolve_effective_isolation(
            thd, share ? share->isolation_level : TDB_ISOLATION_SNAPSHOT);
    tidesdb_trx_t *trx = get_or_create_trx(thd, ht, effective_iso);
    if (!trx) return HA_ERR_OUT_OF_MEM;

    stmt_txn = trx->txn;
    stmt_txn_dirty = false;

    /* We cache THD and trx pointers for fast access in hot paths
       (index_read_map, update_row, delete_row, ensure_stmt_txn).
       Eliminates ha_thd() virtual dispatch and thd_get_ha_data()
       hash lookup on every row operation. */
    cached_thd_ = thd;
    cached_trx_ = trx;

    trans_register_ha(thd, false, ht, 0);

    if (!is_autocommit) trans_register_ha(thd, true, ht, 0);

    /* Arm a statement savepoint so a statement error inside BEGIN...COMMIT
       rolls back only this statement, not the whole transaction.  Only for
       real multi-statement transactions -- autocommit and DDL statements
       are their own transaction, where statement rollback is already a full
       rollback.  external_lock fires once per table per statement, so the
       !stmt_savepoint_active guard arms it exactly once, at the first table,
       before any of the statement's row writes. */
    if (!is_autocommit && !is_ddl && trx->txn && !trx->stmt_savepoint_active)
        stmt_savepoint_arm(trx);
    return 0;
}

void ha_tidesdb::external_lock_release(THD *thd)
{
    /* For multi-statement transactions (BEGIN...COMMIT), the txn stays the
       same across statements.  Preserve the cached scan iterator across
       read-only statements so the next statement reuses it (avoids the
       O(sstables) merge-heap rebuild).  After a write statement it must be
       freed: an iterator snapshots the txn's writeset when created, so one
       built before this statement's puts/deletes would not see them.  For
       autocommit, always free. */
    bool in_multi_stmt = cached_stmt_shape_valid_
                             ? !cached_is_autocommit_
                             : (bool)thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
    if (!in_multi_stmt || stmt_txn_dirty)
    {
        if (scan_iter)
        {
            tidesdb_iter_free(scan_iter);
            scan_iter = NULL;
            scan_iter_cf_ = NULL;
            scan_iter_txn_ = NULL;
        }
    }

    /* We bump update_time once per write-statement for information_schema.
       We use cached_time_ if available to avoid another time() syscall. */
    if (stmt_txn_dirty && share)
        share->update_time.store(cached_time_valid_ ? cached_time_ : time(0),
                                 std::memory_order_relaxed);

    /* We invalidate all per-statement caches so the next statement
       picks up any changes (key rotation, session variable changes,
       clock advance). */
    enc_key_ver_valid_ = false;
    cached_time_valid_ = false;
    cached_thdvars_valid_ = false;

    stmt_txn = NULL;
    stmt_txn_dirty = false;
    cached_thd_ = NULL;
    cached_trx_ = NULL;

    /* We invalidate statement shape cache last so the above checks still see it. */
    cached_stmt_shape_valid_ = false;
}

int ha_tidesdb::external_lock(THD *thd, int lock_type)
{
    DBUG_ENTER("ha_tidesdb::external_lock");

    if (lock_type != F_UNLCK) DBUG_RETURN(external_lock_acquire(thd));

    external_lock_release(thd);
    DBUG_RETURN(0);
}

THR_LOCK_DATA **ha_tidesdb::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
    /* With lock_count()=0 MariaDB skips THR_LOCK entirely.  store_lock is still
       called but we do not push into the 'to' array (same pattern as InnoDB);
       TidesDB owns concurrency control through its own MVCC and commit-time
       conflict detection, so there is nothing to record here. */
    (void)thd;
    (void)lock_type;
    return to;
}

/* ******************** two-phase commit (XA) ******************** */

/* two-phase-commit phase one.  fold the pending fts meta into the transaction, then durably prepare
   its write batch under the connection's XID.  an external XA PREPARE additionally hands the
   prepared transaction to the global registry and detaches it from the connection, so a later
   XA COMMIT/ROLLBACK on any connection -- or after a restart -- can resolve it. */
#if MYSQL_VERSION_ID >= 110800
static int tidesdb_prepare(THD *thd, bool all)
#else
static int tidesdb_prepare(handlerton *, THD *thd, bool all)
#endif
{
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    if (!trx || !trx->txn) return 0;
    if (!all) return 0; /* statement-level prepare defers, like statement-level commit */

    stmt_savepoint_disarm(trx);

    bool is_external = (thd_sql_command(thd) == SQLCOM_XA_PREPARE);

    /* A read-only transaction driven by the internal 2PC coordinator has nothing durable to
       prepare; leave it for commit() to roll back.  An external XA PREPARE still records it so the
       later XA COMMIT/ROLLBACK can resolve the XID. */
    if (!trx->dirty && !is_external) return 0;

    if (trx->dirty)
    {
        int frc = flush_trx_fts_meta_pending(thd, trx);
        if (frc != TDB_SUCCESS)
        {
            tdb_txn_rollback_stateful(trx->txn);
            tidesdb_txn_free(trx->txn);
            trx->txn = NULL;
            trx->txn_generation++;
            trx->dirty = false;
            return tdb_rc_to_ha(frc, "hton_prepare fts_meta_flush");
        }
    }

    XID xid;
    thd_get_xid(thd, (MYSQL_XID *)&xid);
    std::string key = tdb_xid_key(&xid);

    int rc = tidesdb_txn_prepare(trx->txn, (const uint8_t *)key.data(), key.size());
    if (rc != TDB_SUCCESS)
    {
        tdb_txn_rollback_stateful(trx->txn);
        tidesdb_txn_free(trx->txn);
        trx->txn = NULL;
        trx->txn_generation++;
        trx->dirty = false;
        return tdb_rc_to_ha(rc, "hton_prepare");
    }

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

/* ******************** handlerton registration ******************** */

void tidesdb_txn_register(handlerton *hton)
{
    /* per-transaction savepoint storage carved out of the server's transaction area */
    hton->savepoint_offset = sizeof(tidesdb_savepoint_t);

    /* one TidesDB txn per BEGIN..COMMIT, shared by every handler on the connection */
    hton->commit = tidesdb_commit;
    hton->commit_ordered = tidesdb_commit_ordered;
    hton->rollback = tidesdb_rollback;
    hton->close_connection = tidesdb_close_connection;

    hton->savepoint_set = tidesdb_savepoint_set;
    hton->savepoint_rollback = tidesdb_savepoint_rollback;
    hton->savepoint_rollback_can_release_mdl = tidesdb_savepoint_rollback_can_release_mdl;
    hton->savepoint_release = tidesdb_savepoint_release;
    hton->start_consistent_snapshot = tidesdb_start_consistent_snapshot;

    /* two-phase commit (XA) -- prepare durably logs the write batch under the XID, the by-xid
       resolvers finish a prepared transaction from any connection, and recover replays the ones the
       library found in-doubt after a restart. */
    hton->prepare = tidesdb_prepare;
    hton->recover = tidesdb_recover;
    hton->commit_by_xid = tidesdb_commit_by_xid;
    hton->rollback_by_xid = tidesdb_rollback_by_xid;
}
