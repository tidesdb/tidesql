/* Copyright (c) 2026 TidesDB Corp.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

/*
  The cluster position under Galera, recorded with every commit so a crashed node rejoins from
  exactly where its store's commits stop.  See the comment at the position records below.
*/

#include "ha_tidesdb.h"

#ifdef WITH_WSREP

#include <atomic>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>

#include "sql_class.h"
#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_wsrep_internal.h"

/* ---- cluster position checkpoint -------------------------------------- */

/* An XID is three lengths and a data buffer, so store the three as fixed width
   little endian followed by the used bytes of the buffer, which reads back on any
   host that ran this build. */
static void put_i64(std::string &out, int64_t v)
{
    for (int i = 0; i < 8; i++) out.push_back((char)((uint64_t)v >> (i * 8)));
}

static int64_t get_i64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return (int64_t)v;
}

static std::string wsrep_xid_blob(const XID *xid)
{
    std::string blob;
    put_i64(blob, xid->formatID);
    put_i64(blob, xid->gtrid_length);
    put_i64(blob, xid->bqual_length);
    if (xid->formatID != -1)
        blob.append(xid->data, (size_t)(xid->gtrid_length + xid->bqual_length));
    return blob;
}

/* Decode a stored XID.  Returns false for a damaged record; a record of an unknown position decodes
   to a null xid. */
static bool wsrep_xid_from_blob(const uint8_t *val, size_t val_len, XID *xid)
{
    xid->null();
    if (!val || val_len < 24) return false;
    long formatID = (long)get_i64(val);
    long gtrid = (long)get_i64(val + 8);
    long bqual = (long)get_i64(val + 16);
    if (formatID == -1) return true;
    if (gtrid < 0 || bqual < 0 || (size_t)(gtrid + bqual) > XIDDATASIZE ||
        val_len < 24 + (size_t)(gtrid + bqual))
        return false;
    xid->formatID = formatID;
    xid->gtrid_length = gtrid;
    xid->bqual_length = bqual;
    memcpy(xid->data, val + 24, (size_t)(gtrid + bqual));
    return true;
}

static bool wsrep_xid_has_seqno(const XID *xid)
{
    return wsrep_is_wsrep_xid(xid) && wsrep_xid_seqno(xid) >= 0;
}

/* The cluster position is recorded with every commit, as InnoDB records it in each commit's undo
   header, so a node that crashes rejoins from exactly where its data stops.  Each position is a key
   of its own, its seqno big-endian after a prefix, so committers never write the same key and a
   position committed out of order cannot hide a newer one; the newest key is the position.

   A transaction whose seqno is known when the engine commits it, an applier above all, writes its
   position into the same transaction, so the two are durable together.  A local transaction is
   prepared before Galera certifies it and so before it has a seqno, so it writes instead a pending
   marker of its own, and once it has committed a small transaction of its own records the position
   and removes the marker.  A marker left behind means a commit reached the store without its
   position, from a crash between the two, and the position is then unknown and the node rejoins
   through a full state transfer.  The server's own checkpoints, at view changes, DDL and state
   transfers, record a position the same way, and a new cluster identity clears the old ones.  Old
   positions are pruned as new ones arrive.

   The single record an earlier release kept is still read when no per-commit position exists, and
   is kept for a release that reads only it.  It is marked unknown while this release runs, so a
   crash followed by a downgrade takes a full state transfer, and written exactly at a clean
   shutdown. */
static std::string wsrep_pos_key(int64_t seqno)
{
    std::string key(TDB_WSREP_POS_PREFIX);
    for (int i = 7; i >= 0; i--) key.push_back((char)((uint64_t)seqno >> (i * 8)));
    return key;
}

/* Run ops in a READ COMMITTED transaction of its own on the meta family and commit it.  Returns 0
   or a library error. */
template <typename Ops>
static int wsrep_meta_txn(Ops &&ops)
{
    tidesdb_column_family_t *cf = wsrep_meta_cf();
    if (!cf) return TDB_ERR_IO;
    tidesdb_txn_t *txn = nullptr;
    int rc = tidesdb_txn_begin_with_isolation(tdb_global, TDB_ISOLATION_READ_COMMITTED, &txn);
    if (rc != TDB_SUCCESS) return rc;
    rc = ops(txn, cf);
    if (rc == TDB_SUCCESS)
        rc = tidesdb_txn_commit(txn);
    else
        tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return rc;
}

static int wsrep_put_legacy(tidesdb_txn_t *txn, tidesdb_column_family_t *cf, const XID *xid)
{
    const std::string blob = wsrep_xid_blob(xid);
    return tidesdb_txn_put(txn, cf, (const uint8_t *)WSREP_POSITION_KEY,
                           sizeof(WSREP_POSITION_KEY) - 1, (const uint8_t *)blob.data(),
                           blob.size(), TIDESDB_TTL_NONE);
}

static int wsrep_put_pos(tidesdb_txn_t *txn, tidesdb_column_family_t *cf, const XID *xid)
{
    const std::string key = wsrep_pos_key(wsrep_xid_seqno(xid));
    const std::string blob = wsrep_xid_blob(xid);
    return tidesdb_txn_put(txn, cf, (const uint8_t *)key.data(), key.size(),
                           (const uint8_t *)blob.data(), blob.size(), TIDESDB_TTL_NONE);
}

/* The first key at or after prefix that still begins with it, copied into key.  Returns false when
   none does or the family cannot be read. */
static bool wsrep_first_under(tidesdb_txn_t *txn, tidesdb_column_family_t *cf, const char *prefix,
                              std::string &key, std::string *value)
{
    tidesdb_iter_t *it = nullptr;
    if (tidesdb_iter_new(txn, cf, &it) != TDB_SUCCESS) return false;
    bool found = false;
    const size_t plen = strlen(prefix);
    if (tidesdb_iter_seek(it, (const uint8_t *)prefix, plen) == TDB_SUCCESS &&
        tidesdb_iter_valid(it))
    {
        uint8_t *k = nullptr, *v = nullptr;
        size_t klen = 0, vlen = 0;
        if (tidesdb_iter_key(it, &k, &klen) == TDB_SUCCESS && klen >= plen &&
            memcmp(k, prefix, plen) == 0)
        {
            key.assign((const char *)k, klen);
            found = !value || (tidesdb_iter_value(it, &v, &vlen) == TDB_SUCCESS &&
                               (value->assign((const char *)v, vlen), true));
        }
    }
    tidesdb_iter_free(it);
    return found;
}

/* The newest per-commit position, decoded into xid.  Returns false when none is recorded.  The
   positions sort by seqno and pruning keeps only the newest few thousand, so they are walked in
   order and the last one kept. */
static bool wsrep_newest_pos(tidesdb_txn_t *txn, tidesdb_column_family_t *cf, XID *xid)
{
    const size_t plen = sizeof(TDB_WSREP_POS_PREFIX) - 1;
    tidesdb_iter_t *it = nullptr;
    if (tidesdb_iter_new(txn, cf, &it) != TDB_SUCCESS) return false;
    std::string newest;
    if (tidesdb_iter_seek(it, (const uint8_t *)TDB_WSREP_POS_PREFIX, plen) == TDB_SUCCESS)
        for (; tidesdb_iter_valid(it); tidesdb_iter_next(it))
        {
            uint8_t *k = nullptr, *v = nullptr;
            size_t klen = 0, vlen = 0;
            if (tidesdb_iter_key(it, &k, &klen) != TDB_SUCCESS || klen < plen ||
                memcmp(k, TDB_WSREP_POS_PREFIX, plen) != 0)
                break;
            if (klen == plen + 8 && tidesdb_iter_value(it, &v, &vlen) == TDB_SUCCESS)
                newest.assign((const char *)v, vlen);
        }
    tidesdb_iter_free(it);
    return !newest.empty() &&
           wsrep_xid_from_blob((const uint8_t *)newest.data(), newest.size(), xid) &&
           wsrep_xid_has_seqno(xid);
}

enum tdb_wsrep_recorded
{
    TDB_WSREP_POS_NONE,    /* nothing recorded, a store that never ran under Galera */
    TDB_WSREP_POS_KNOWN,   /* xid holds exactly where the store's commits stop */
    TDB_WSREP_POS_UNKNOWN, /* the store holds commits past any recorded position */
};

/* Where the store's commits stop, read from the per-commit positions, or from the single record of
   an earlier release when there are none. */
static tdb_wsrep_recorded wsrep_recorded_position(XID *xid)
{
    xid->null();
    tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, WSREP_META_CF);
    if (!cf) return TDB_WSREP_POS_NONE;
    tidesdb_txn_t *txn = nullptr;
    if (tidesdb_txn_begin_with_isolation(tdb_global, TDB_ISOLATION_READ_COMMITTED, &txn) !=
        TDB_SUCCESS)
        return TDB_WSREP_POS_UNKNOWN;

    tdb_wsrep_recorded result;
    std::string key, value;
    if (wsrep_first_under(txn, cf, TDB_WSREP_PENDING_PREFIX, key, nullptr))
        result = TDB_WSREP_POS_UNKNOWN;
    else if (wsrep_newest_pos(txn, cf, xid))
        result = TDB_WSREP_POS_KNOWN;
    else if (wsrep_first_under(txn, cf, WSREP_POSITION_KEY, key, &value) &&
             key == WSREP_POSITION_KEY)
        result = !wsrep_xid_from_blob((const uint8_t *)value.data(), value.size(), xid)
                     ? TDB_WSREP_POS_UNKNOWN
                 : wsrep_xid_has_seqno(xid) ? TDB_WSREP_POS_KNOWN
                                            : TDB_WSREP_POS_UNKNOWN;
    else
        result = TDB_WSREP_POS_NONE;
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return result;
}

static std::atomic<uint64_t> g_wsrep_pos_written{0};

/* Every so often drop the positions well behind the newest, which nothing reads any more. */
static void wsrep_prune_positions(int64_t newest)
{
    if (g_wsrep_pos_written.fetch_add(1, std::memory_order_relaxed) % TDB_WSREP_POS_PRUNE_EVERY)
        return;
    if (newest <= (int64_t)TDB_WSREP_POS_KEEP) return;
    const std::string lo = TDB_WSREP_POS_PREFIX;
    const std::string hi = wsrep_pos_key(newest - (int64_t)TDB_WSREP_POS_KEEP);
    const int rc = wsrep_meta_txn(
        [&](tidesdb_txn_t *txn, tidesdb_column_family_t *cf)
        {
            return tidesdb_txn_delete_range(txn, cf, (const uint8_t *)lo.data(), lo.size(),
                                            (const uint8_t *)hi.data(), hi.size());
        });
    if (rc != TDB_SUCCESS)
        sql_print_warning("[TIDESDB] Could not prune old Galera positions (err=%d)", rc);
}

int tdb_wsrep_before_engine_commit(THD *thd, tidesdb_trx_t *trx)
{
    if (!thd || !(wsrep_on(thd)) || !trx || !trx->txn || !trx->dirty) return 0;
    if (trx->wsrep_pos_in_txn || !trx->wsrep_pending_marker.empty()) return 0;
    tidesdb_column_family_t *cf = wsrep_meta_cf();
    if (!cf) return tdb_rc_to_ha(TDB_ERR_IO, "wsrep position");
    XID xid;
    thd_get_xid(thd, (MYSQL_XID *)&xid);
    int rc;
    if (wsrep_xid_has_seqno(&xid))
    {
        rc = wsrep_put_pos(trx->txn, cf, &xid);
        trx->wsrep_pos_in_txn = rc == TDB_SUCCESS;
    }
    else
    {
        std::string marker(TDB_WSREP_PENDING_PREFIX);
        put_i64(marker, (int64_t)thd_get_thread_id(thd));
        put_i64(marker, (int64_t)trx->txn_generation);
        rc = tidesdb_txn_put(trx->txn, cf, (const uint8_t *)marker.data(), marker.size(),
                             (const uint8_t *)"", 0, TIDESDB_TTL_NONE);
        if (rc == TDB_SUCCESS) trx->wsrep_pending_marker = std::move(marker);
    }
    return rc == TDB_SUCCESS ? 0 : tdb_rc_to_ha(rc, "wsrep position");
}

void tidesdb_wsrep_note_commit(THD *thd, tidesdb_trx_t *trx)
{
    if (!trx) return;
    XID xid;
    if (thd) thd_get_xid(thd, (MYSQL_XID *)&xid);
    const bool known = thd && wsrep_xid_has_seqno(&xid);
    if (!trx->wsrep_pending_marker.empty())
    {
        const std::string marker = std::move(trx->wsrep_pending_marker);
        trx->wsrep_pending_marker.clear();
        const int rc = wsrep_meta_txn(
            [&](tidesdb_txn_t *txn, tidesdb_column_family_t *cf)
            {
                int r = known ? wsrep_put_pos(txn, cf, &xid) : TDB_SUCCESS;
                if (r == TDB_SUCCESS)
                    r = tidesdb_txn_delete(txn, cf, (const uint8_t *)marker.data(), marker.size());
                return r;
            });
        /* the marker stays, so a crash before the next clean shutdown takes a full state transfer
         */
        if (rc != TDB_SUCCESS)
            sql_print_warning("[TIDESDB] Could not record the Galera position of a commit (err=%d)",
                              rc);
    }
    trx->wsrep_pos_in_txn = false;
    if (known) wsrep_prune_positions(wsrep_xid_seqno(&xid));
}

/* The server records a position of its own at view changes, DDL, state transfer and similar
   events.  A position from a new cluster replaces every older one, and an undefined position
   records that the store's position is not known. */
int tidesdb_wsrep_set_checkpoint(handlerton *, const XID *xid)
{
    /* The provider hands its own xid at checkpoint time; a foreign one is not ours
       to record, matching InnoDB which stores only a wsrep xid. */
    if (!wsrep_is_wsrep_xid(xid)) return 1;
    XID newest;
    const bool have = wsrep_recorded_position(&newest) == TDB_WSREP_POS_KNOWN;
    const bool known = wsrep_xid_has_seqno(xid);
    const bool new_cluster = !known || (have && memcmp(wsrep_xid_uuid(&newest), wsrep_xid_uuid(xid),
                                                       sizeof(wsrep_uuid_t)) != 0);
    const int rc = wsrep_meta_txn(
        [&](tidesdb_txn_t *txn, tidesdb_column_family_t *cf)
        {
            int r = TDB_SUCCESS;
            if (new_cluster)
                r = tidesdb_txn_delete_prefix(txn, cf, (const uint8_t *)TDB_WSREP_POS_PREFIX,
                                              sizeof(TDB_WSREP_POS_PREFIX) - 1);
            if (r == TDB_SUCCESS)
                r = known ? wsrep_put_pos(txn, cf, xid) : wsrep_put_legacy(txn, cf, xid);
            return r;
        });
    return rc == TDB_SUCCESS ? 0 : 1;
}

/* Runs for every committing read-write transaction ahead of certification, so a failure here
   refuses the commit cleanly.  It prepares a local write transaction, which settles every engine
   conflict before certification.  Returns 0, or ULONGLONG_MAX when the prepare found a conflict. */
ulonglong tidesdb_wsrep_prepare_commit_versioned(THD *thd, ulonglong *)
{
    if (!thd || !(wsrep_on(thd))) return 0;
    return tdb_wsrep_prepare_before_certification(thd) ? ULONGLONG_MAX : 0;
}

void tidesdb_wsrep_position_init()
{
    if (!tdb_global || !WSREP_ON) return;
    XID xid;
    const tdb_wsrep_recorded recorded = wsrep_recorded_position(&xid);
    if (recorded == TDB_WSREP_POS_NONE) return;
    /* carry a position an earlier release left over into the per-commit records, then mark the
       single record unknown for as long as this release runs */
    XID unknown;
    unknown.null();
    const int rc = wsrep_meta_txn(
        [&](tidesdb_txn_t *txn, tidesdb_column_family_t *cf)
        {
            int r = recorded == TDB_WSREP_POS_KNOWN ? wsrep_put_pos(txn, cf, &xid) : TDB_SUCCESS;
            if (r == TDB_SUCCESS) r = wsrep_put_legacy(txn, cf, &unknown);
            return r;
        });
    if (rc != TDB_SUCCESS)
        sql_print_warning("[TIDESDB] Could not prepare the stored Galera position (err=%d)", rc);
}

void tidesdb_wsrep_store_last_position()
{
    if (!tdb_global) return;
    XID xid;
    if (wsrep_recorded_position(&xid) != TDB_WSREP_POS_KNOWN) return;
    const int rc = wsrep_meta_txn([&](tidesdb_txn_t *txn, tidesdb_column_family_t *cf)
                                  { return wsrep_put_legacy(txn, cf, &xid); });
    if (rc != TDB_SUCCESS)
        sql_print_warning("[TIDESDB] Could not write the Galera position at shutdown (err=%d)", rc);
}

/* Whether a is a known position later than b in the same cluster, or b is not known at all. */
static bool wsrep_xid_newer(const XID *a, const XID *b)
{
    if (!wsrep_xid_has_seqno(a)) return false;
    if (!wsrep_xid_has_seqno(b)) return true;
    return memcmp(wsrep_xid_uuid(a), wsrep_xid_uuid(b), sizeof(wsrep_uuid_t)) != 0 ||
           wsrep_xid_seqno(a) > wsrep_xid_seqno(b);
}

/* The newest position any other engine reports, read from each engine directly. */
static my_bool wsrep_newest_other_cb(THD *, plugin_ref plugin, void *arg)
{
    handlerton *h = plugin_hton(plugin);
    if (!h || h == tidesdb_hton || !h->get_checkpoint) return FALSE;
    XID x;
    x.null();
    h->get_checkpoint(h, &x);
    XID *best = static_cast<XID *>(arg);
    if (wsrep_xid_newer(&x, best)) *best = x;
    return FALSE;
}

static std::atomic<bool> g_wsrep_pos_aligned{false};

/* The server asks every engine in turn, in no fixed order, and keeps whatever the last one leaves,
   while every other engine reports only the position of its own last commit.  TidesDB's commits
   are invisible to them, so a node whose last commits were TidesDB ones would restart from a point
   it already holds.  So the first time the server asks, which is when it settles where the node
   rejoins, TidesDB works out the node's real position and hands it to every engine through the
   server, so the answer is the same whichever engine speaks last.  An unknown position means
   TidesDB holds commits past any recorded point, so every engine is told the position is undefined
   and the node rejoins by a full state transfer.  Later calls only report. */
int tidesdb_wsrep_get_checkpoint(handlerton *, XID *xid)
{
    XID recorded_xid;
    const tdb_wsrep_recorded recorded = wsrep_recorded_position(&recorded_xid);
    if (recorded == TDB_WSREP_POS_NONE) return 0;
    const bool unknown = recorded == TDB_WSREP_POS_UNKNOWN;

    XID newest;
    newest.null();
    plugin_foreach(NULL, wsrep_newest_other_cb, MYSQL_STORAGE_ENGINE_PLUGIN, &newest);
    if (!unknown && wsrep_xid_newer(&recorded_xid, &newest)) newest = recorded_xid;

    if (!g_wsrep_pos_aligned.exchange(true))
    {
        if (unknown)
            wsrep_set_SE_checkpoint(wsrep::gtid::undefined(), wsrep_gtid_server.undefined());
        else
            wsrep_set_SE_checkpoint(newest);
    }
    if (unknown)
        xid->null();
    else
        *xid = newest;
    return 0;
}

#endif /* WITH_WSREP */
