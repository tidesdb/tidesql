/*
  Galera participation for the TidesDB storage engine.

  A Galera cluster keeps its nodes in sync by certifying every transaction's
  write set against the ones already ordered by the group.  For that to work the
  engine has to tell the provider which rows a local transaction touched, so a
  concurrent write to the same row on another node is caught as a conflict rather
  than silently diverging the replicas.  MariaDB drives this through the engine,
  so the row operations append a certification key for each row they change, the
  handlerton advertises HTON_WSREP_REPLICATION, and it persists the cluster
  position so a restarted node knows where it left off.  A certified transaction
  must commit on every node, so a local write transaction is prepared, which is the
  engine's own conflict check, before Galera certifies it.

  This unit holds the certification keys, the write intents and the brute-force
  abort.  The cluster position is in ha_tidesdb_wsrep_pos.cc, the state transfer
  and the store identity in ha_tidesdb_wsrep_sst.cc, and the reads an applier must
  abort in ha_tidesdb_wsrep_reads.cc.

  The whole file compiles out where the server was built without wsrep.  The
  build turns WITH_WSREP on through my_config.h, and the handler header pulls in
  the wsrep service header under the same guard.
*/

#include "ha_tidesdb.h"

#ifdef WITH_WSREP

#include <atomic>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <random>
#include <string>

#include "key.h"
#include "sql_class.h"
#include "sql_show.h"
#include "src/handler/ha_tidesdb_internal.h"

/* WSREP_ON, the cheap global gate the wsrep_on() service macro folds in front of
   the provider call so a non-clustered server pays nothing per row. */
#include "wsrep_on.h"

/* wsrep_key_t and wsrep_buf_t, the certification key shape the provider expects.
   The service header only forward declares them, so pull the full definition from
   the wsrep API that is already on the plugin include path.  A wsrep-enabled server
   install ships it under server/private alongside the wsrep-lib headers that gate
   WITH_WSREP, so it is present wherever this translation unit is compiled. */
#include "src/handler/ha_tidesdb_wsrep_internal.h"
#include "wsrep_api.h"

/* ---- certification key append ----------------------------------------- */

/* Hand one row key to the provider, tagged with the table so keys from different
   tables never collide.  wsrep_prepare_key_for_innodb turns the table cache key
   and the row bytes into the two or three part wsrep key the provider certifies,
   and wsrep_thd_append_key records it against this thread's write set. */
static int tdb_wsrep_append(THD *thd, const uchar *cache_key, size_t cache_key_len,
                            const uchar *row_key, size_t row_key_len,
                            enum Wsrep_service_key_type key_type)
{
    wsrep_buf_t wkey_part[3];
    wsrep_key_t wkey = {wkey_part, 3};

    if (!wsrep_prepare_key_for_innodb(thd, cache_key, cache_key_len, row_key, row_key_len,
                                      wkey_part, &wkey.key_parts_num))
        return HA_ERR_INTERNAL_ERROR;

    if (wsrep_thd_append_key(thd, &wkey, 1, key_type)) return HA_ERR_INTERNAL_ERROR;

    return 0;
}

/* A unique or primary key with a NULL part does not constrain the row, so it is
   not something another node can conflict on and there is no key to certify. */
static bool key_part_is_null(KEY *ki, uint num_parts, const uchar *record)
{
    for (uint j = 0; j < num_parts; j++)
        if (ki->key_part[j].field->is_null_in_record(record)) return true;
    return false;
}

/* The provider has had an update key since protocol 4, and before that the exclusive key stands in
   for it, the same threshold InnoDB applies. */
enum Wsrep_service_key_type tdb_wsrep_update_key_type()
{
    return wsrep_protocol_version >= TDB_WSREP_UPDATE_KEY_PROTOCOL ? WSREP_SERVICE_KEY_UPDATE
                                                                   : WSREP_SERVICE_KEY_EXCLUSIVE;
}

int ha_tidesdb::wsrep_certify_row(THD *thd, const uchar *rec0, const uchar *rec1,
                                  enum Wsrep_service_key_type key_type)
{
    /* Only a locally originated transaction certifies its writes.  An applier is
       replaying an already certified write set, and a thread with wsrep off or a
       table the provider ignores has nothing to append.  The wsrep_on() service
       macro is unparenthesized, so it is folded into one && chain here rather than
       negated in place, where the leading ! would bind to only its first term. */
    const bool replicating =
        wsrep_on(thd) && wsrep_thd_is_local(thd) && !wsrep_thd_ignore_table(thd);
    if (!replicating) return 0;

    const uchar *cache_key = (const uchar *)table->s->table_cache_key.str;
    const size_t cache_key_len = table->s->table_cache_key.length;
    const uint num_keys = table->s->keys;

    /* Whether the table has any unique key at all decides the strategy.  With one,
       certifying the unique keys pins the row.  Without one there is no key that
       identifies the row, so every ordinary index is certified to still catch a
       same-row conflict, mirroring how InnoDB handles a table with no primary key. */
    bool has_unique = false;
    for (uint i = 0; i < num_keys; i++)
    {
        if (share->idx_is_fts[i] || share->idx_is_spatial[i]) continue;
        if (table->key_info[i].flags & HA_NOSAME)
        {
            has_unique = true;
            break;
        }
    }

    bool appended_any = false;

    for (uint i = 0; i < num_keys; i++)
    {
        /* Fulltext and spatial indexes carry no equality key to certify on. */
        if (share->idx_is_fts[i] || share->idx_is_spatial[i]) continue;

        KEY *ki = &table->key_info[i];
        const bool is_unique = (ki->flags & HA_NOSAME) != 0;
        if (has_unique && !is_unique) continue;

        const uint nparts = ki->user_defined_key_parts;

        /* The row key is the index ordinal followed by the comparable bytes of the
           key parts, so the same logical value on two nodes produces identical key
           material and certifies as a conflict. */
        uchar kb[1 + MAX_KEY_LENGTH];
        kb[0] = (uchar)i;

        /* For an update the key material may have changed.  When it did, the
           before image is certified too so the row's old identity is not left
           uncovered, and a changed unique key is an exclusive operation. */
        enum Wsrep_service_key_type this_type = key_type;
        if (rec1)
        {
            const bool null0 = key_part_is_null(ki, nparts, rec0);
            const bool null1 = key_part_is_null(ki, nparts, rec1);
            uchar kb1[1 + MAX_KEY_LENGTH];
            kb1[0] = (uchar)i;
            uint len0 = null0 ? 0 : make_comparable_key(ki, rec0, nparts, kb + 1);
            uint len1 = null1 ? 0 : make_comparable_key(ki, rec1, nparts, kb1 + 1);

            const bool changed =
                null0 != null1 || len0 != len1 || (len0 && memcmp(kb + 1, kb1 + 1, len0) != 0);
            if (changed && is_unique) this_type = WSREP_SERVICE_KEY_EXCLUSIVE;

            if (changed && !null1)
            {
                if (int rc =
                        tdb_wsrep_append(thd, cache_key, cache_key_len, kb1, len1 + 1, this_type))
                    return rc;
                appended_any = true;
            }
            if (null0) continue;
            if (int rc = tdb_wsrep_append(thd, cache_key, cache_key_len, kb, len0 + 1, this_type))
                return rc;
            appended_any = true;
            continue;
        }

        if (key_part_is_null(ki, nparts, rec0)) continue;
        uint klen = make_comparable_key(ki, rec0, nparts, kb + 1);
        if (int rc = tdb_wsrep_append(thd, cache_key, cache_key_len, kb, klen + 1, this_type))
            return rc;
        appended_any = true;
    }

    /* A table with no certifiable key still has to be safe.  Append a table level
       exclusive key so a write is serialized against every other write to the same
       table across the cluster, trading concurrency for never losing a conflict. */
    if (!appended_any)
    {
        if (wsrep_thd_append_table_key(thd, table->s->db.str, table->s->table_name.str,
                                       WSREP_SERVICE_KEY_EXCLUSIVE))
            return HA_ERR_INTERNAL_ERROR;
    }

    return 0;
}

/* ---- galera write-intent map ------------------------------------------
   Maps each row identity, the data family's handle followed by the primary key, and each non-NULL
   unique value, the bytes of its uniqueness sentinel, to the transactions on this node that wrote
   it and have not ended, each recorded as its thread id, its transaction, the generation it wrote
   at, and whether it is an applier replaying a cluster write.

   This is the lock-free analogue of the row lock InnoDB's applier waits on.  When an applier
   replays a write, every local transaction that wrote the same row or unique value is brute-force
   aborted through the server, as InnoDB's applier aborts the holder of a lock it needs, and the
   applier holds the key while it applies, so a local write that arrives meanwhile is refused.  A
   local writer would otherwise prepare before the applier commits and then commit over a row it
   never saw.

   Two local transactions at SNAPSHOT or SERIALIZABLE are left to the engine, which checks the later
   one's writes against the first when it prepares, before either is certified, exactly as it does
   without Galera.  Below SNAPSHOT a commit does not check its writes against other writers, and two
   writers of one row break the replay on the other nodes even though each commits here, an insert
   over another insert's row arriving as a duplicate and an update of a row a delete removed finding
   nothing.  A row lock prevents both in InnoDB at every level, so a write by a transaction below
   SNAPSHOT is refused while another local transaction holds the key, and while the key was
   committed after its statement began.  The
   second test reads recent, the commit sequence each key last committed at, which a commit records
   before it drops its holds so a writer always sees one or the other.  An entry is pruned once no
   live snapshot is old enough to conflict with it.

   A victim is reached through find_thread_by_id with its kill lock held, so it cannot be freed
   under the aborter, and the generation check skips a holder that has moved on.  A shard's mutex
   is dropped before any THD lock is taken, and the map is striped by key hash so writers of
   different keys rarely share a mutex. */
struct tdb_wsrep_intent
{
    my_thread_id thd_id;
    tidesdb_trx_t *trx;
    uint64_t gen;
    bool applier;
    bool validates; /* at a level whose commit validates it against other writers */
};
struct tdb_wsrep_intent_shard
{
    std::mutex mtx;
    std::unordered_map<std::string, std::vector<tdb_wsrep_intent>> map;
    std::unordered_map<std::string, uint64_t> recent;
};

/* whether the engine's own commit checks trx's writes against another writer of the same key */
static bool wsrep_validates_writes(const tidesdb_trx_t *trx)
{
    return trx->isolation_level >= TDB_ISOLATION_SNAPSHOT;
}
static constexpr size_t TDB_WSREP_INTENT_SHARDS = 256; /* power of two for a mask index */
static tdb_wsrep_intent_shard g_wsrep_intent[TDB_WSREP_INTENT_SHARDS];

static inline tdb_wsrep_intent_shard &wsrep_intent_shard_for(const std::string &key)
{
    return g_wsrep_intent[std::hash<std::string>{}(key) & (TDB_WSREP_INTENT_SHARDS - 1)];
}

/* The map key for unique secondary index i from record, the same bytes as the row's uniqueness
   sentinel.  Empty when the index holds no sentinel for this row (the primary key, a non-unique,
   fulltext or spatial index, or a NULL key part). */
std::string ha_tidesdb::wsrep_intent_key_bytes(uint i, const uchar *record)
{
    uchar sk[TIDESDB_UNIQ_SENTINEL_BUF_LEN];
    uint sk_len = unique_sentinel_key(i, record, sk);
    return std::string((const char *)sk, sk_len);
}

/* The row-identity map key, the data family's handle followed by the primary key, where a local
   write and an applier replaying a same-row cluster write meet. */
std::string ha_tidesdb::wsrep_intent_row_key_bytes(const uchar *pk, uint pk_len)
{
    tidesdb_column_family_t *cf = share->cf;
    std::string key((const char *)&cf, sizeof(cf));
    key.append((const char *)pk, pk_len);
    return key;
}

/* Record trx as a holder of an intent key, unless it already holds it.  Called with the key's
   shard locked. */
void ha_tidesdb::wsrep_intent_hold(tdb_wsrep_intent_shard &sh, std::string key, tidesdb_trx_t *trx)
{
    std::vector<tdb_wsrep_intent> &holders = sh.map[key];
    for (const tdb_wsrep_intent &h : holders)
        if (h.trx == trx) return;
    holders.push_back({(my_thread_id)thd_get_thread_id(cached_thd_), trx, trx->wsrep_life->load(),
                       wsrep_thd_is_BF(cached_thd_, false) != 0, wsrep_validates_writes(trx)});
    trx->wsrep_intent_keys.push_back(std::move(key));
}

/* The commit sequence a write by trx below SNAPSHOT must not see a newer commit of its key than,
   its snapshot at REPEATABLE READ, the ceiling its statement began at under READ COMMITTED, and
   none at READ UNCOMMITTED. */
static uint64_t wsrep_write_ceiling(const tidesdb_trx_t *trx)
{
    if (!trx->txn || trx->isolation_level == TDB_ISOLATION_READ_UNCOMMITTED) return UINT64_MAX;
    if (trx->isolation_level == TDB_ISOLATION_READ_COMMITTED) return trx->wsrep_stmt_ceiling;
    return tidesdb_txn_read_snapshot(trx->txn);
}

/* A local transaction is about to write an intent key.  It is refused while an applier holds the
   key, since the applier's write commits first and this one would then commit over it unseen, and,
   when either it or another local holder is below SNAPSHOT, while that holder holds the key or, for
   this writer, when the key committed after what it could have read.  Shared by the
   unique-value and the row-identity paths. */
int ha_tidesdb::wsrep_intent_claim(std::string key, tidesdb_trx_t *trx)
{
    const bool validates = wsrep_validates_writes(trx);
    tdb_wsrep_intent_shard &sh = wsrep_intent_shard_for(key);
    std::lock_guard<std::mutex> lk(sh.mtx);
    auto held = sh.map.find(key);
    if (held != sh.map.end())
        for (const tdb_wsrep_intent &h : held->second)
            if (h.trx != trx && (h.applier || !validates || !h.validates))
                return tdb_rc_to_ha(TDB_ERR_CONFLICT, "wsrep local write");
    if (!validates)
    {
        auto last = sh.recent.find(key);
        if (last != sh.recent.end() && last->second > wsrep_write_ceiling(trx))
            return tdb_rc_to_ha(TDB_ERR_CONFLICT, "wsrep local write");
    }
    wsrep_intent_hold(sh, std::move(key), trx);
    return 0;
}

/* find_thread_by_id returns the victim with its kill lock held so it cannot be freed here.  Take
   the data lock too, the pair wsrep_thd_bf_abort asserts it owns, then re-check under both locks
   that this is still the same logical transaction before aborting it.  A victim that is itself a
   cluster writer, or that Galera refuses to abort because it is already ordered ahead, is left
   alone. */
void tdb_wsrep_bf_abort_trx(THD *bf_thd, my_thread_id victim_id, tidesdb_trx_t *victim_trx,
                            uint64_t victim_gen)
{
    if (!bf_thd || !victim_trx) return;
    THD *victim_thd = find_thread_by_id(victim_id);
    if (!victim_thd) return;
    wsrep_thd_LOCK(victim_thd);
    tidesdb_trx_t *cur = (tidesdb_trx_t *)thd_get_ha_data(victim_thd, tidesdb_hton);
    if (cur == victim_trx && victim_trx->txn && victim_trx->wsrep_life->load() == victim_gen &&
        !wsrep_thd_is_BF(victim_thd, false))
    {
        if (wsrep_thd_bf_abort(bf_thd, victim_thd, true))
            tidesdb_txn_request_abort(victim_trx->txn);
        /* aborted now, by an earlier applier, or ordered ahead, it ends before this applier
           commits, and until then a prepared victim's reads refuse this applier's writes */
        if (tidesdb_trx_t *bf_trx = (tidesdb_trx_t *)thd_get_ha_data(bf_thd, tidesdb_hton))
            bf_trx->wsrep_victims.push_back(
                {victim_id, victim_trx, victim_gen, victim_trx->wsrep_life});
    }
    wsrep_thd_UNLOCK(victim_thd);
    wsrep_thd_kill_UNLOCK(victim_thd);
}

/* An applier's writes are refused by the reads of a local transaction that is still prepared, and
   an applier must not fail, so before its own prepare or commit it waits for every local
   transaction it aborted as it wrote to end.  Each one ends without waiting on this applier.  A
   victim ordered after it rolls back on being aborted, and one ordered ahead of it commits first,
   as Galera commits in order, and neither can be queued behind the applier in a group commit,
   since the abort came before the applier entered the commit order.  A victim has ended once its
   generation counter moved on.  The wait is bounded, and a victim still live when the bound is
   reached is reported and the caller stops the node, since the applier can do nothing else. */
bool tdb_wsrep_wait_for_victims(tidesdb_trx_t *trx)
{
    if (!trx) return true;
    unsigned long waited_us = 0;
    for (const tidesdb_trx_t::tdb_wsrep_victim &v : trx->wsrep_victims)
    {
        while (tdb_wsrep_trx_live(v.life, v.gen))
        {
            if (waited_us >= TDB_WSREP_VICTIM_WAIT_MAX_US)
            {
                sql_print_error(
                    "[TIDESDB] A cluster write waited %lu seconds for the local "
                    "transaction on connection %lu it aborted to end, and it has not.",
                    TDB_WSREP_VICTIM_WAIT_MAX_US / 1000000UL, (unsigned long)v.thd_id);
                return false;
            }
            my_sleep(TDB_WSREP_VICTIM_POLL_US);
            waited_us += TDB_WSREP_VICTIM_POLL_US;
        }
    }
    trx->wsrep_victims.clear();
    return true;
}

/* A local child write certifies a REFERENCE key on the parent row it depends on, the same key the
   parent's own delete or key change certifies as exclusive, so the two conflict when they come
   from different nodes and neither saw the other.  The referenced key is NOT NULL, so its
   value-only bytes match the parent's comparable key exactly. */
int ha_tidesdb::wsrep_certify_parent_ref(const tdb_fk_def &d, const uchar *value, uint value_len)
{
    THD *thd = cached_thd_ ? cached_thd_ : ha_thd();
    const bool replicating =
        wsrep_on(thd) && wsrep_thd_is_local(thd) && !wsrep_thd_ignore_table(thd);
    if (!replicating || d.parent_cert_key_no < 0 || value_len > MAX_KEY_LENGTH) return 0;

    std::string cache_key = d.ref_db;
    cache_key.push_back('\0');
    cache_key += d.ref_table;
    cache_key.push_back('\0');
    uchar kb[1 + MAX_KEY_LENGTH];
    kb[0] = (uchar)d.parent_cert_key_no;
    memcpy(kb + 1, value, value_len);
    return tdb_wsrep_append(thd, (const uchar *)cache_key.data(), cache_key.size(), kb,
                            value_len + 1, WSREP_SERVICE_KEY_REFERENCE);
}

/* An applier is about to write an intent key.  It holds the key itself, so a local write that
   arrives while the cluster write applies is refused, and it brute-force aborts every local
   transaction that holds the key, through the server, the lock-free analogue of the row lock
   InnoDB's applier would abort the holder of.

   Reading the holders and installing the applier happen under one shard lock.  Done as two steps,
   a local claim could land between them, find no applier, and never be aborted.  The aborts run
   after the shard lock is dropped so the map lock and the THD locks never nest. */
void ha_tidesdb::wsrep_intent_take(const std::string &key)
{
    std::vector<tdb_wsrep_intent> victims;
    {
        tdb_wsrep_intent_shard &sh = wsrep_intent_shard_for(key);
        std::lock_guard<std::mutex> lk(sh.mtx);
        auto it = sh.map.find(key);
        if (it != sh.map.end())
            for (const tdb_wsrep_intent &h : it->second)
                if (!h.applier && h.trx != cached_trx_) victims.push_back(h);
        if (cached_trx_) wsrep_intent_hold(sh, key, cached_trx_);
    }
    for (const tdb_wsrep_intent &v : victims)
        tdb_wsrep_bf_abort_trx(cached_thd_, v.thd_id, v.trx, v.gen);
}

int ha_tidesdb::wsrep_intent_register(uint i, const uchar *record, tidesdb_trx_t *trx)
{
    if (!trx) return 0;
    std::string key = wsrep_intent_key_bytes(i, record);
    if (key.empty()) return 0;
    return wsrep_intent_claim(std::move(key), trx);
}

/* Claim the row identity, when pk is given, and every value-keyed unique entry of record.  Returns
   the first refusal, or 0. */
int ha_tidesdb::wsrep_intent_register_all(const uchar *pk, uint pk_len, const uchar *record,
                                          tidesdb_trx_t *trx)
{
    if (int rc = wsrep_intent_register_row(pk, pk_len, trx)) return rc;
    for (uint i = 0; i < table->s->keys; i++)
        if (int rc = wsrep_intent_register(i, record, trx)) return rc;
    return 0;
}

void ha_tidesdb::wsrep_intent_check(uint i, const uchar *record)
{
    std::string key = wsrep_intent_key_bytes(i, record);
    if (key.empty()) return;
    wsrep_intent_take(key);
}

int ha_tidesdb::wsrep_intent_register_row(const uchar *pk, uint pk_len, tidesdb_trx_t *trx)
{
    if (!trx || !pk || !pk_len) return 0;
    return wsrep_intent_claim(wsrep_intent_row_key_bytes(pk, pk_len), trx);
}

void ha_tidesdb::wsrep_intent_check_row(const uchar *pk, uint pk_len)
{
    if (!pk || !pk_len) return;
    wsrep_intent_take(wsrep_intent_row_key_bytes(pk, pk_len));
}

/* Drop trx from the holders of key, and the key once nobody holds it.  Called with the key's
   shard locked. */
static void wsrep_intent_drop(tdb_wsrep_intent_shard &sh, const std::string &key,
                              const tidesdb_trx_t *trx)
{
    auto it = sh.map.find(key);
    if (it == sh.map.end()) return;
    std::vector<tdb_wsrep_intent> &holders = it->second;
    for (size_t i = 0; i < holders.size(); i++)
        if (holders[i].trx == trx)
        {
            holders[i] = holders.back();
            holders.pop_back();
            break;
        }
    if (holders.empty()) sh.map.erase(it);
}

/* Each key may live in a different shard, so lock the owning shard per key.  The holds are brief
   and uncontended in the common case, and never nest. */
void tidesdb_wsrep_intent_clear(tidesdb_trx_t *trx)
{
    if (!trx) return;
    /* every path that drops the write intents ends the transaction, so its recorded reads and the
       victims it was waiting on go too */
    tdb_wsrep_reads_release(trx);
    trx->wsrep_victims.clear();
    for (const std::string &k : trx->wsrep_intent_keys)
    {
        tdb_wsrep_intent_shard &sh = wsrep_intent_shard_for(k);
        std::lock_guard<std::mutex> lk(sh.mtx);
        wsrep_intent_drop(sh, k, trx);
    }
    trx->wsrep_intent_keys.clear();
    trx->wsrep_pending_marker.clear();
    trx->wsrep_pos_in_txn = false;
    /* every caller has already had the library end the transaction, so an applier waiting on it
       may go on */
    trx->wsrep_life->fetch_add(1);
}

/* Drop the recent entries no live snapshot is old enough to conflict with.  The oldest live
   snapshot reads as the largest sequence when no transaction is open, which empties the shard. */
static void wsrep_recent_prune(tdb_wsrep_intent_shard &sh)
{
    tidesdb_db_stats_t st;
    if (!tdb_global || tidesdb_get_db_stats(tdb_global, &st) != TDB_SUCCESS) return;
    std::lock_guard<std::mutex> lk(sh.mtx);
    for (auto it = sh.recent.begin(); it != sh.recent.end();)
        it = it->second <= st.min_snapshot_seq ? sh.recent.erase(it) : std::next(it);
}

/* Record that key committed at seq. */
static void wsrep_recent_stamp(std::string key, uint64_t seq)
{
    tdb_wsrep_intent_shard &sh = wsrep_intent_shard_for(key);
    bool prune;
    {
        std::lock_guard<std::mutex> lk(sh.mtx);
        uint64_t &last = sh.recent[std::move(key)];
        if (seq > last) last = seq;
        prune = sh.recent.size() >= TDB_WSREP_RECENT_PRUNE_AT;
    }
    if (prune) wsrep_recent_prune(sh);
}

/* The library's change capture calls these after a commit is durable and visible but before the
   commit returns, for local, applier, prepared and batched commits alike, with the exact sequence
   the batch committed at.  A committer's holds are dropped only after its commit returns, so a
   writer that no longer finds the hold always finds the stamp.  The data family hook rebuilds the
   row-identity key from its context, the family handle, and each data key; the sentinel family
   hook's keys are the unique intent keys as they stand. */
static int wsrep_data_commit_hook(const tidesdb_commit_op_t *ops, int num_ops, uint64_t commit_seq,
                                  void *ctx)
{
    for (int i = 0; i < num_ops; i++)
    {
        if (!ops[i].key || ops[i].key_size < 2 || ops[i].key[0] != KEY_NS_DATA) continue;
        std::string key((const char *)&ctx, sizeof(ctx));
        key.append((const char *)ops[i].key + 1, ops[i].key_size - 1);
        wsrep_recent_stamp(std::move(key), commit_seq);
    }
    return 0;
}

static int wsrep_uniq_commit_hook(const tidesdb_commit_op_t *ops, int num_ops, uint64_t commit_seq,
                                  void *)
{
    for (int i = 0; i < num_ops; i++)
        if (ops[i].key && ops[i].key_size)
            wsrep_recent_stamp(std::string((const char *)ops[i].key, ops[i].key_size), commit_seq);
    return 0;
}

void tidesdb_wsrep_watch_cf(tidesdb_column_family_t *cf, bool uniq_sentinel)
{
    if (!cf || !tdb_global || !WSREP_ON) return;
    /* a failure only leaves the family unwatched, which the intent holds still partly cover */
    (void)tidesdb_cf_set_commit_hook(
        tdb_global, cf, uniq_sentinel ? wsrep_uniq_commit_hook : wsrep_data_commit_hook,
        uniq_sentinel ? nullptr : (void *)cf);
}

void tidesdb_wsrep_intent_truncate(tidesdb_trx_t *trx, size_t keep)
{
    if (!trx || trx->wsrep_intent_keys.size() <= keep) return;
    for (size_t j = keep; j < trx->wsrep_intent_keys.size(); j++)
    {
        const std::string &k = trx->wsrep_intent_keys[j];
        tdb_wsrep_intent_shard &sh = wsrep_intent_shard_for(k);
        std::lock_guard<std::mutex> lk(sh.mtx);
        wsrep_intent_drop(sh, k, trx);
    }
    trx->wsrep_intent_keys.resize(keep);
}

/* ---- brute-force abort ------------------------------------------------- */

/* When the provider certifies an incoming write set that conflicts with a local
   transaction, it brute-force aborts the local victim so the higher priority
   write can apply.  An uncommitted victim parks no waiter in the library and
   holds no reservations, so there is nothing to cancel, but its commit would
   otherwise still run on its own first-committer-wins merits, which is why a
   certification loser could commit anyway and diverge the replicas.  So flag the
   victim's transaction for abort.  tidesdb_txn_request_abort is a single atomic
   store the library reads at the victim's next operation, its commit included, so
   from there every operation on it returns TDB_ERR_TXN_ABORTED, which the plugin
   maps to a deadlock.  Galera holds the victim's THD lock across this call, so
   reading its transaction handle here is safe. */
static void tidesdb_wsrep_abort_transaction(handlerton *, THD *bf_thd, THD *victim_thd,
                                            my_bool signal)
{
    (void)bf_thd;
    (void)signal;
    tidesdb_trx_t *victim = (tidesdb_trx_t *)thd_get_ha_data(victim_thd, tidesdb_hton);
    if (victim && victim->txn) tidesdb_txn_request_abort(victim->txn);
}

/* ---- registration ------------------------------------------------------ */

void tidesdb_wsrep_register(handlerton *hton)
{
    hton->flags |= HTON_WSREP_REPLICATION;
    hton->abort_transaction = tidesdb_wsrep_abort_transaction;
    hton->set_checkpoint = tidesdb_wsrep_set_checkpoint;
    hton->get_checkpoint = tidesdb_wsrep_get_checkpoint;
    hton->prepare_commit_versioned = tidesdb_wsrep_prepare_commit_versioned;
    hton->disable_internal_writes = tidesdb_wsrep_disable_internal_writes;
}

#endif /* WITH_WSREP */
