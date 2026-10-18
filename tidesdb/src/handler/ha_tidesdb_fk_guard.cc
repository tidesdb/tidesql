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
  Foreign key concurrency guard.

  A child row is only valid while its parent row exists, but the child and the parent are
  different rows.  Under snapshot isolation a transaction deleting a parent and one inserting a
  child of it never write the same key, and each one's check reads a snapshot that cannot see the
  other's uncommitted write, so both used to commit and leave an orphan.  InnoDB prevents this with
  a shared lock the child check takes on the parent row, which the parent's exclusive lock waits
  for.

  The engine is lock-free, so the guard records the same relationship and refuses rather than
  waits.  A child write holds the referenced parent key shared, and a parent delete, or a change to
  a referenced key, holds it exclusive.  Two shared holds coexist, any other pair conflicts, and
  the later claimer is refused with a deadlock the application retries.  A hold lasts until its
  transaction ends.

  A hold only covers transactions that overlap.  One that committed after the claimer's snapshot
  was taken is invisible to the claimer's check and gone from the holds, so each commit stamps its
  guard keys with a sequence at or above its own, and a claim whose snapshot predates a conflicting
  stamp is refused the same way.  A stamp is pruned once no live snapshot is old enough to conflict
  with it.  Everything here is memory only and nothing reaches the store.
*/

#include "ha_tidesdb.h"

#include <cstdint>
#include <iterator>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/handler/ha_tidesdb_internal.h"

/* A transaction holding a key, with the thread and generation a galera applier needs to reach and
   abort it safely, the same identity the write-intent map records. */
struct tdb_fk_guard_holder
{
    tidesdb_trx_t *trx{nullptr};
    my_thread_id thd_id{0};
    uint64_t gen{0};
};

struct tdb_fk_guard_hold
{
    tdb_fk_guard_holder exclusive;
    std::vector<tdb_fk_guard_holder> shared;
};

struct tdb_fk_guard_shard
{
    std::mutex mtx;
    std::unordered_map<std::string, tdb_fk_guard_hold> holds;
    /* the sequence each key last committed at under each kind of hold */
    std::unordered_map<std::string, uint64_t> committed_shared;
    std::unordered_map<std::string, uint64_t> committed_exclusive;
};

static tdb_fk_guard_shard g_fk_guard[TDB_FK_GUARD_SHARDS];

static tdb_fk_guard_shard &fk_guard_shard_for(const std::string &key)
{
    return g_fk_guard[std::hash<std::string>{}(key) & (TDB_FK_GUARD_SHARDS - 1)];
}

std::string tdb_fk_guard_key(const tdb_fk_def &d, const uchar *value, uint value_len)
{
    std::string key = d.parent_cf;
    key.push_back('\0');
    if (!d.parent_is_pk) key += d.parent_index_name;
    key.push_back('\0');
    key.append((const char *)value, value_len);
    return key;
}

/* The newest commit a claim by trx could have read, its snapshot at REPEATABLE READ and above.  A
   READ COMMITTED read already sees every commit up to the moment it runs, and READ UNCOMMITTED sees
   more, so neither has a stale view to guard. */
static uint64_t fk_guard_ceiling(const tidesdb_trx_t *trx)
{
    if (!trx->txn || trx->isolation_level < TDB_ISOLATION_REPEATABLE_READ) return UINT64_MAX;
    return tidesdb_txn_read_snapshot(trx->txn);
}

static bool fk_guard_committed_after(const std::unordered_map<std::string, uint64_t> &m,
                                     const std::string &key, uint64_t ceiling)
{
    auto it = m.find(key);
    return it != m.end() && it->second > ceiling;
}

/* The other transactions whose holds on h conflict with trx taking it in the requested mode. */
static std::vector<tdb_fk_guard_holder> fk_guard_conflicts(const tdb_fk_guard_hold &h,
                                                           const tidesdb_trx_t *trx, bool exclusive)
{
    std::vector<tdb_fk_guard_holder> out;
    if (h.exclusive.trx && h.exclusive.trx != trx) out.push_back(h.exclusive);
    if (exclusive)
        for (const tdb_fk_guard_holder &s : h.shared)
            if (s.trx != trx) out.push_back(s);
    return out;
}

/* Whether a commit since trx's snapshot conflicts with it taking key in the requested mode. */
static bool fk_guard_stale(tdb_fk_guard_shard &sh, const std::string &key, const tidesdb_trx_t *trx,
                           bool exclusive)
{
    const uint64_t ceiling = fk_guard_ceiling(trx);
    if (fk_guard_committed_after(sh.committed_exclusive, key, ceiling)) return true;
    return exclusive && fk_guard_committed_after(sh.committed_shared, key, ceiling);
}

/* Record trx as a holder of key in the requested mode, unless it already holds it so. */
static void fk_guard_hold(tdb_fk_guard_shard &sh, std::string key, tidesdb_trx_t *trx, THD *thd,
                          bool exclusive)
{
    tdb_fk_guard_hold &h = sh.holds[key];
    tdb_fk_guard_holder me;
    me.trx = trx;
    me.thd_id = thd ? (my_thread_id)thd_get_thread_id(thd) : 0;
#ifdef WITH_WSREP
    me.gen = trx->wsrep_life->load();
#else
    me.gen = trx->txn_generation;
#endif
    if (h.exclusive.trx == trx) return;
    if (exclusive)
        h.exclusive = me;
    else
    {
        for (const tdb_fk_guard_holder &s : h.shared)
            if (s.trx == trx) return;
        h.shared.push_back(me);
    }
    trx->fk_guard_keys.emplace_back(std::move(key), exclusive);
}

/* A galera applier replays a write every other node commits and may not be refused, so it aborts
   the local transactions in its way, the way it would win an InnoDB lock, and takes the key. */
static bool fk_guard_is_applier(THD *thd)
{
#ifdef WITH_WSREP
    return thd && wsrep_thd_is_BF(thd, false);
#else
    (void)thd;
    return false;
#endif
}

int tdb_fk_guard_claim(THD *thd, tidesdb_trx_t *trx, std::string key, bool exclusive)
{
    if (!trx) return 0;
    tdb_fk_guard_shard &sh = fk_guard_shard_for(key);
    std::vector<tdb_fk_guard_holder> victims;
    {
        std::lock_guard<std::mutex> lk(sh.mtx);
        auto found = sh.holds.find(key);
        if (found != sh.holds.end()) victims = fk_guard_conflicts(found->second, trx, exclusive);
        if (!fk_guard_is_applier(thd))
        {
            if (!victims.empty() || fk_guard_stale(sh, key, trx, exclusive))
                return tdb_rc_to_ha(TDB_ERR_CONFLICT, "foreign key guard");
            fk_guard_hold(sh, std::move(key), trx, thd, exclusive);
            return 0;
        }
        fk_guard_hold(sh, key, trx, thd, exclusive);
    }
#ifdef WITH_WSREP
    /* outside the shard lock, so the stripe and the THD locks never nest */
    for (const tdb_fk_guard_holder &v : victims)
        tdb_wsrep_bf_abort_trx(thd, v.thd_id, v.trx, v.gen);
#endif
    return 0;
}

/* A parent row is being deleted, or the key constraint d references is changing, so take that
   referenced value exclusive before the children are checked or cascaded.  Encoded value-only,
   the same bytes a child's check builds from its referencing columns. */
int ha_tidesdb::fk_guard_parent(const tdb_fk_def &d, const uchar *old_row)
{
    KEY *ki = &table->key_info[d.parent_key_no];
    uchar value[MAX_KEY_LENGTH];
    uint len = make_comparable_key(ki, old_row, (uint)d.parent_fields.size(), value, true);
    return tdb_fk_guard_claim(cached_thd_, cached_trx_, tdb_fk_guard_key(d, value, len), true);
}

/* Drop the stamps no live snapshot is old enough to conflict with.  The oldest live snapshot
   reads as the largest sequence when no transaction is open, which empties the maps. */
static void fk_guard_prune(tdb_fk_guard_shard &sh)
{
    tidesdb_db_stats_t st;
    if (!tdb_global || tidesdb_get_db_stats(tdb_global, &st) != TDB_SUCCESS) return;
    std::lock_guard<std::mutex> lk(sh.mtx);
    for (auto *m : {&sh.committed_shared, &sh.committed_exclusive})
        for (auto it = m->begin(); it != m->end();)
            it = it->second <= st.min_snapshot_seq ? m->erase(it) : std::next(it);
}

/* A sequence at or above every commit so far, taken just after the transaction committed.  It may
   sit above that commit by whatever committed in between, which only refuses a claim a little more
   often.  With no snapshot to take it is the largest sequence, which refuses every older claim
   until the stamp is pruned. */
static uint64_t fk_guard_commit_stamp()
{
    tidesdb_snapshot_t *snap = NULL;
    if (!tdb_global || tidesdb_snapshot_create(tdb_global, &snap) != TDB_SUCCESS || !snap)
        return UINT64_MAX;
    const uint64_t seq = tidesdb_snapshot_seq(snap);
    tidesdb_snapshot_release(snap);
    return seq;
}

static void fk_guard_drop(tdb_fk_guard_shard &sh, const std::string &key, tidesdb_trx_t *trx)
{
    auto it = sh.holds.find(key);
    if (it == sh.holds.end()) return;
    tdb_fk_guard_hold &h = it->second;
    if (h.exclusive.trx == trx) h.exclusive = tdb_fk_guard_holder();
    for (size_t i = 0; i < h.shared.size(); i++)
        if (h.shared[i].trx == trx)
        {
            h.shared[i] = h.shared.back();
            h.shared.pop_back();
            break;
        }
    if (!h.exclusive.trx && h.shared.empty()) sh.holds.erase(it);
}

void tdb_fk_guard_release(tidesdb_trx_t *trx, bool committed, size_t keep)
{
    if (!trx || trx->fk_guard_keys.size() <= keep) return;
    const uint64_t stamp = committed ? fk_guard_commit_stamp() : 0;
    for (size_t i = keep; i < trx->fk_guard_keys.size(); i++)
    {
        const std::string &key = trx->fk_guard_keys[i].first;
        tdb_fk_guard_shard &sh = fk_guard_shard_for(key);
        bool prune = false;
        {
            std::lock_guard<std::mutex> lk(sh.mtx);
            if (committed)
            {
                auto &m =
                    trx->fk_guard_keys[i].second ? sh.committed_exclusive : sh.committed_shared;
                uint64_t &last = m[key];
                if (stamp > last) last = stamp;
                prune = m.size() >= TDB_FK_GUARD_PRUNE_AT;
            }
            fk_guard_drop(sh, key, trx);
        }
        if (prune) fk_guard_prune(sh);
    }
    trx->fk_guard_keys.resize(keep);
}
