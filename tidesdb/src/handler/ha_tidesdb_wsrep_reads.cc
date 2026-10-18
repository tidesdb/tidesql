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
  Reads that would refuse a Galera applier.

  At REPEATABLE READ and SERIALIZABLE the engine validates a transaction's reads when it prepares,
  and a prepared transaction keeps every key it read and every range it scanned claimed until it
  ends.  A claim refuses any write to what it covers, whatever the writer's level, so it refuses an
  applier replaying a cluster write too, and an applier must not fail.  Under Galera a local write
  transaction is prepared before it is certified, so such claims are live whenever an applier runs.

  So a local transaction at either level records here the keys it reads and the ranges its
  iterators open over, through the same wrappers its reads go through.  The library's footprint
  for a scan runs from the key it was opened or seeked at to the key it was sitting on when it was
  freed, or to the end of the family when it ran off it, and its bounded iterator uses the upper
  bound only to choose which tables to read, not to stop, so a scan steps onto the first key past
  the caller's range before the caller stops it and that key is in the footprint.  A range is
  therefore recorded from the lower bound it was opened with, or from below every key, up to the
  end of the family, never narrower than what the library records when the iterator is freed.

  An applier records every key it writes here too, for as long as it is in flight, and as it
  writes a key it brute-force aborts every local transaction whose read covers it, the way
  InnoDB's applier aborts the holder of a shared or gap lock it needs, and later waits for them
  to end before it commits.  A local transaction that reads a key an applier has written but not
  yet committed is not aborted and not refused, it waits before it prepares until that applier
  has ended, so its prepare validates the read against the applied version and refuses the
  transaction itself when the read is stale, which is the engine's own conflict.  The applier
  never has to look again at commit time, which matters because with the binary log on the server
  releases the commit order before it runs the engine's ordered commit, and a transaction ordered
  behind the applier can by then be queued in the group commit the applier leads, so an applier
  waiting there for it would wait forever.  Local readers against local writers need nothing
  here, the engine settles those when the later of them prepares.
*/

#include "ha_tidesdb.h"

#ifdef WITH_WSREP

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "sql_class.h"
#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_wsrep_internal.h"
#include "wsrep_on.h"

/* a transaction on this node, a reader or an applier, as its connection context, thread id, the
   epoch it was recorded at and the counter that epoch is read from */
struct tdb_wsrep_reader
{
    tidesdb_trx_t *trx{nullptr};
    my_thread_id thd_id{0};
    uint64_t gen{0};
    std::shared_ptr<std::atomic<uint64_t>> life;
};

/* a scanned range, bounds inclusive, a bound with its flag set standing for no bound.  Every
   range a scan records is open above, see the note at the top, and the upper bound is kept for
   the day a scan's end can be recorded exactly. */
struct tdb_wsrep_range
{
    std::string lo, hi;
    bool lo_open{false}, hi_open{false};
    tdb_wsrep_reader reader;
};

struct tdb_wsrep_family_reads
{
    std::map<std::string, std::vector<tdb_wsrep_reader>> keys; /* key read, by whom */
    std::vector<tdb_wsrep_range> ranges;                       /* ranges scanned */
    std::map<std::string, std::vector<tdb_wsrep_reader>>
        applied; /* key written, by which applier */
};

struct tdb_wsrep_reads_shard
{
    std::mutex mtx;
    std::unordered_map<const void *, tdb_wsrep_family_reads> families;
};

static tdb_wsrep_reads_shard g_wsrep_reads[TDB_WSREP_READ_SHARDS];

static tdb_wsrep_reads_shard &wsrep_reads_shard_for(const void *cf)
{
    return g_wsrep_reads[std::hash<const void *>{}(cf) & (TDB_WSREP_READ_SHARDS - 1)];
}

static bool wsrep_range_covers(const tdb_wsrep_range &r, const std::string &key)
{
    return (r.lo_open || r.lo <= key) && (r.hi_open || key <= r.hi);
}

/* The connection's own transaction, when this access runs inside it under Galera. */
static tidesdb_trx_t *wsrep_reads_trx(THD *thd, const tidesdb_txn_t *txn)
{
    if (!thd || !(wsrep_on(thd))) return nullptr;
    tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, tidesdb_hton);
    return trx && trx->txn == txn ? trx : nullptr;
}

/* whether the level claims the transaction's reads when it prepares, which is what makes them
   able to refuse an applier */
static bool wsrep_reads_claimed(const tidesdb_trx_t *trx)
{
    return trx->isolation_level == TDB_ISOLATION_REPEATABLE_READ ||
           trx->isolation_level == TDB_ISOLATION_SERIALIZABLE;
}

static tdb_wsrep_reader wsrep_reader_of(THD *thd, tidesdb_trx_t *trx)
{
    tdb_wsrep_reader me;
    me.trx = trx;
    me.thd_id = (my_thread_id)thd_get_thread_id(thd);
    me.gen = trx->wsrep_life->load();
    me.life = trx->wsrep_life;
    return me;
}

/* add a transaction to a key's holders once */
static void wsrep_hold_once(std::vector<tdb_wsrep_reader> &holders, const tdb_wsrep_reader &me)
{
    for (const tdb_wsrep_reader &h : holders)
        if (h.trx == me.trx) return;
    holders.push_back(me);
}

void tdb_wsrep_note_read(THD *thd, const tidesdb_txn_t *txn, const tidesdb_column_family_t *cf,
                         const uint8_t *lo, size_t lo_len, const uint8_t *hi, size_t hi_len)
{
    tidesdb_trx_t *trx = wsrep_reads_trx(thd, txn);
    if (!trx || !wsrep_thd_is_local(thd) || !wsrep_reads_claimed(trx)) return;

    const tdb_wsrep_reader me = wsrep_reader_of(thd, trx);
    const bool point = lo && hi && lo_len == hi_len && memcmp(lo, hi, lo_len) == 0;

    tdb_wsrep_reads_shard &sh = wsrep_reads_shard_for(cf);
    std::lock_guard<std::mutex> lk(sh.mtx);
    tdb_wsrep_family_reads &f = sh.families[cf];
    if (point)
    {
        std::string key((const char *)lo, lo_len);
        std::vector<tdb_wsrep_reader> &readers = f.keys[key];
        const size_t before = readers.size();
        wsrep_hold_once(readers, me);
        if (readers.size() > before) trx->wsrep_read_keys.emplace_back(cf, std::move(key));
        return;
    }
    tdb_wsrep_range r;
    r.lo_open = !lo;
    r.hi_open = true;
    if (lo) r.lo.assign((const char *)lo, lo_len);
    r.reader = me;
    f.ranges.push_back(std::move(r));
    trx->wsrep_read_range_cfs.push_back(cf);
}

/* Record a key an applier writes and brute-force abort every local reader whose recorded key or
   range covers it.  The readers are collected under the stripe lock and aborted outside it, so
   the stripe and the THD locks never nest. */
void tdb_wsrep_note_write(THD *thd, const tidesdb_txn_t *txn, const tidesdb_column_family_t *cf,
                          const uint8_t *key, size_t key_len)
{
    tidesdb_trx_t *trx = wsrep_reads_trx(thd, txn);
    if (!trx || !wsrep_thd_is_BF(thd, false)) return;
    std::string k((const char *)key, key_len);
    std::vector<tdb_wsrep_reader> victims;
    {
        tdb_wsrep_reads_shard &sh = wsrep_reads_shard_for(cf);
        std::lock_guard<std::mutex> lk(sh.mtx);
        tdb_wsrep_family_reads &f = sh.families[cf];
        auto readers = f.keys.find(k);
        if (readers != f.keys.end()) victims = readers->second;
        for (const tdb_wsrep_range &r : f.ranges)
            if (wsrep_range_covers(r, k)) victims.push_back(r.reader);
        wsrep_hold_once(f.applied[k], wsrep_reader_of(thd, trx));
    }
    for (const tdb_wsrep_reader &v : victims) tdb_wsrep_bf_abort_trx(thd, v.thd_id, v.trx, v.gen);
    trx->wsrep_applied_keys.emplace_back(cf, std::move(k));
}

/* The appliers in flight that wrote a key this transaction's recorded reads cover, each once. */
static void wsrep_appliers_over_reads(const tidesdb_trx_t *trx, std::vector<tdb_wsrep_reader> &out)
{
    for (const auto &rk : trx->wsrep_read_keys)
    {
        tdb_wsrep_reads_shard &sh = wsrep_reads_shard_for(rk.first);
        std::lock_guard<std::mutex> lk(sh.mtx);
        auto fam = sh.families.find(rk.first);
        if (fam == sh.families.end()) continue;
        auto it = fam->second.applied.find(rk.second);
        if (it == fam->second.applied.end()) continue;
        for (const tdb_wsrep_reader &a : it->second) wsrep_hold_once(out, a);
    }
    for (const void *cf : trx->wsrep_read_range_cfs)
    {
        tdb_wsrep_reads_shard &sh = wsrep_reads_shard_for(cf);
        std::lock_guard<std::mutex> lk(sh.mtx);
        auto fam = sh.families.find(cf);
        if (fam == sh.families.end()) continue;
        const tdb_wsrep_family_reads &f = fam->second;
        for (const tdb_wsrep_range &r : f.ranges)
        {
            if (r.reader.trx != trx) continue;
            auto it = r.lo_open ? f.applied.begin() : f.applied.lower_bound(r.lo);
            for (; it != f.applied.end() && (r.hi_open || it->first <= r.hi); ++it)
                for (const tdb_wsrep_reader &a : it->second) wsrep_hold_once(out, a);
        }
    }
}

/* A local transaction about to prepare waits for every applier in flight that wrote a key its
   reads cover, so the prepare validates those reads against the applied version instead of
   claiming them under the applier and refusing it.  An applier never waits on a transaction that
   has not prepared, so the wait cannot form a cycle, and a transaction the applier aborted
   meanwhile gives up at once, its commit fails anyway.  The wait is bounded, and a transaction
   that reaches the bound is refused rather than prepared. */
bool tdb_wsrep_wait_for_appliers(THD *thd, tidesdb_trx_t *trx)
{
    if (!trx || !wsrep_reads_claimed(trx)) return true;
    std::vector<tdb_wsrep_reader> appliers;
    wsrep_appliers_over_reads(trx, appliers);
    unsigned long waited_us = 0;
    for (const tdb_wsrep_reader &a : appliers)
    {
        while (tdb_wsrep_trx_live(a.life, a.gen))
        {
            wsrep_thd_LOCK(thd);
            const bool aborted = wsrep_thd_is_aborting(thd);
            wsrep_thd_UNLOCK(thd);
            if (aborted || waited_us >= TDB_WSREP_APPLIER_WAIT_MAX_US) return false;
            my_sleep(TDB_WSREP_VICTIM_POLL_US);
            waited_us += TDB_WSREP_VICTIM_POLL_US;
        }
    }
    return true;
}

static void wsrep_drop_reader(std::vector<tdb_wsrep_reader> &v, const tidesdb_trx_t *trx)
{
    for (size_t i = 0; i < v.size(); i++)
        if (v[i].trx == trx)
        {
            v[i] = v.back();
            v.pop_back();
            return;
        }
}

/* drop a transaction from one key of a key map, and the key once no one holds it.  Called with
   the shard locked. */
static void wsrep_drop_from(std::map<std::string, std::vector<tdb_wsrep_reader>> &m,
                            const std::string &key, const tidesdb_trx_t *trx)
{
    auto it = m.find(key);
    if (it == m.end()) return;
    wsrep_drop_reader(it->second, trx);
    if (it->second.empty()) m.erase(it);
}

/* a family no transaction reads or applies any longer leaves the shard, so a dropped table leaves
   nothing behind.  Called with the shard locked. */
static void wsrep_family_tidy(tdb_wsrep_reads_shard &sh, const void *cf)
{
    auto it = sh.families.find(cf);
    if (it != sh.families.end() && it->second.keys.empty() && it->second.ranges.empty() &&
        it->second.applied.empty())
        sh.families.erase(it);
}

void tdb_wsrep_reads_release(tidesdb_trx_t *trx)
{
    if (!trx) return;
    for (const auto &rk : trx->wsrep_read_keys)
    {
        tdb_wsrep_reads_shard &sh = wsrep_reads_shard_for(rk.first);
        std::lock_guard<std::mutex> lk(sh.mtx);
        auto fam = sh.families.find(rk.first);
        if (fam == sh.families.end()) continue;
        wsrep_drop_from(fam->second.keys, rk.second, trx);
        wsrep_family_tidy(sh, rk.first);
    }
    for (const void *cf : trx->wsrep_read_range_cfs)
    {
        tdb_wsrep_reads_shard &sh = wsrep_reads_shard_for(cf);
        std::lock_guard<std::mutex> lk(sh.mtx);
        auto fam = sh.families.find(cf);
        if (fam == sh.families.end()) continue;
        std::vector<tdb_wsrep_range> &ranges = fam->second.ranges;
        for (size_t i = 0; i < ranges.size();)
            if (ranges[i].reader.trx == trx)
            {
                ranges[i] = std::move(ranges.back());
                ranges.pop_back();
            }
            else
                i++;
        wsrep_family_tidy(sh, cf);
    }
    for (const auto &ak : trx->wsrep_applied_keys)
    {
        tdb_wsrep_reads_shard &sh = wsrep_reads_shard_for(ak.first);
        std::lock_guard<std::mutex> lk(sh.mtx);
        auto fam = sh.families.find(ak.first);
        if (fam == sh.families.end()) continue;
        wsrep_drop_from(fam->second.applied, ak.second, trx);
        wsrep_family_tidy(sh, ak.first);
    }
    trx->wsrep_read_keys.clear();
    trx->wsrep_read_range_cfs.clear();
    trx->wsrep_applied_keys.clear();
}

#endif /* WITH_WSREP */
