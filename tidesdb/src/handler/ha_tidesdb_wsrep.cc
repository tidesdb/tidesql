/*
  Galera participation for the TidesDB storage engine.

  A Galera cluster keeps its nodes in sync by certifying every transaction's
  write set against the ones already ordered by the group.  For that to work the
  engine has to tell the provider which rows a local transaction touched, so a
  concurrent write to the same row on another node is caught as a conflict rather
  than silently diverging the replicas.  MariaDB drives this through the engine,
  so the row operations append a certification key for each row they change, the
  handlerton advertises HTON_WSREP_REPLICATION, and it persists the cluster
  position so a restarted node knows where it left off.

  The whole file compiles out where the server was built without wsrep.  The
  build turns WITH_WSREP on through my_config.h, and the handler header pulls in
  the wsrep service header under the same guard.
*/

#include "ha_tidesdb.h"

#ifdef WITH_WSREP

#include <cstdint>
#include <cstring>
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
   the wsrep API that is already on the plugin include path. */
#include "wsrep_api.h"

/* The cluster position lives under one key in a dedicated internal column family,
   so a restarted node reads back the last checkpoint the group set on it. */
static constexpr const char WSREP_META_CF[] = "__tidesdb_wsrep_meta";
static constexpr const char WSREP_POSITION_KEY[] = "position";

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
   Maps a value-keyed unique entry (index cf name followed by the comparable
   value) to the local transaction that wrote it, recorded as its thread id, its
   library transaction handle, and the txn_generation it wrote it at.

   This is the lock-free analogue of the row lock InnoDB leans on for a cross-node
   duplicate.  InnoDB catches a same-value insert from another node when its
   applier goes to insert the row and blocks on the lock the local uncommitted
   transaction still holds, at which point it brute-force aborts that local victim
   before the victim ever certifies.  A lock-free store parks no such waiter, so
   the plugin records the intent here at write time and, when an applier replays a
   conflicting cluster writeset, drives the same brute-force abort on the local
   holder through the server.  The victim then rolls back at its own commit before
   certification instead of certifying a doomed write set the peers cannot apply.

   The map value carries the thread id rather than a bare THD pointer so the victim
   is reached the safe way through find_thread_by_id, which hands it back with its
   kill lock held and so cannot be freed under the aborter.  The generation guard
   makes a leaked entry benign, a holder that has moved on to another logical
   transaction or been reused is skipped rather than wrongly aborted, and its slot
   is overwritten by the next writer of the key.  A shard's mutex is dropped before
   the THD locks are taken so the two lock orders never nest.

   The map is striped into independent shards keyed by the hash of the intent key,
   so two writers of different keys almost never touch the same mutex.  A single
   global mutex here serializes every galera row write against every other one and
   collapses under concurrency; the stripes keep the structure off the shared write
   path while a shard stays as simple as the one map it replaces. */
struct tdb_wsrep_intent
{
    my_thread_id thd_id;
    tidesdb_trx_t *trx;
    uint64_t gen;
};
struct tdb_wsrep_intent_shard
{
    std::mutex mtx;
    std::unordered_map<std::string, tdb_wsrep_intent> map;
};
static constexpr size_t TDB_WSREP_INTENT_SHARDS = 256; /* power of two for a mask index */
static tdb_wsrep_intent_shard g_wsrep_intent[TDB_WSREP_INTENT_SHARDS];

static inline tdb_wsrep_intent_shard &wsrep_intent_shard_for(const std::string &key)
{
    return g_wsrep_intent[std::hash<std::string>{}(key) & (TDB_WSREP_INTENT_SHARDS - 1)];
}

/* The map key for value-keyed unique index i from record; empty when the index is
   not a value-keyed unique entry for this row (non-unique, or a NULL key part). */
std::string ha_tidesdb::wsrep_intent_key_bytes(uint i, const uchar *record)
{
    if (!sec_idx_value_keyed(i, record)) return std::string();
    uchar vbuf[MAX_KEY_LENGTH];
    KEY *ki = &table->key_info[i];
    uint vlen = make_comparable_key(ki, record, ki->user_defined_key_parts, vbuf);
    std::string key = share->idx_cf_names[i];
    key.append((const char *)vbuf, vlen);
    return key;
}

/* The row-identity map key, the data cf name followed by the primary key, so a
   local write and an applier replaying a same-row cluster write meet on it. */
std::string ha_tidesdb::wsrep_intent_row_key_bytes(const uchar *pk, uint pk_len)
{
    std::string key = share->cf_name;
    key.append((const char *)pk, pk_len);
    return key;
}

/* Record this local transaction as the holder of an intent key.  Shared by the
   value-keyed unique and the row-identity paths. */
void ha_tidesdb::wsrep_intent_add(std::string key, tidesdb_trx_t *trx)
{
    tdb_wsrep_intent_shard &sh = wsrep_intent_shard_for(key);
    std::lock_guard<std::mutex> lk(sh.mtx);
    sh.map[key] = {(my_thread_id)thd_get_thread_id(cached_thd_), trx, trx->txn_generation};
    trx->wsrep_intent_keys.push_back(std::move(key));
}

/* An applier is about to write an intent key.  If a local transaction still holds
   it, brute-force abort that local victim through the server so it rolls back at
   its own commit before certifying, the lock-free analogue of the row lock InnoDB
   would have blocked the applier on. */
void ha_tidesdb::wsrep_intent_abort_holder(const std::string &key)
{
    /* Read the holder under the map mutex, then drop it before touching any THD
       lock so the map lock and the THD locks never nest. */
    my_thread_id victim_id;
    tidesdb_trx_t *victim_trx;
    uint64_t victim_gen;
    {
        tdb_wsrep_intent_shard &sh = wsrep_intent_shard_for(key);
        std::lock_guard<std::mutex> lk(sh.mtx);
        auto it = sh.map.find(key);
        if (it == sh.map.end()) return;
        if (it->second.trx == cached_trx_) return;
        victim_id = it->second.thd_id;
        victim_trx = it->second.trx;
        victim_gen = it->second.gen;
    }
    if (!victim_trx) return;

    /* find_thread_by_id returns the victim with its kill lock held so it cannot be
       freed here.  Take the data lock too, the pair wsrep_thd_bf_abort asserts it
       owns, then re-check under both locks that this is still the same logical
       transaction before aborting it. */
    THD *victim_thd = find_thread_by_id(victim_id);
    if (!victim_thd) return;
    wsrep_thd_LOCK(victim_thd);
    tidesdb_trx_t *cur = (tidesdb_trx_t *)thd_get_ha_data(victim_thd, tidesdb_hton);
    if (cur == victim_trx && victim_trx->txn && victim_trx->txn_generation == victim_gen &&
        !wsrep_thd_is_BF(victim_thd, false))
    {
        if (wsrep_thd_bf_abort(cached_thd_, victim_thd, true))
            tidesdb_txn_request_abort(victim_trx->txn);
    }
    wsrep_thd_UNLOCK(victim_thd);
    wsrep_thd_kill_UNLOCK(victim_thd);
}

void ha_tidesdb::wsrep_intent_register(uint i, const uchar *record, tidesdb_trx_t *trx)
{
    if (!trx) return;
    std::string key = wsrep_intent_key_bytes(i, record);
    if (key.empty()) return;
    wsrep_intent_add(std::move(key), trx);
}

void ha_tidesdb::wsrep_intent_check(uint i, const uchar *record)
{
    std::string key = wsrep_intent_key_bytes(i, record);
    if (key.empty()) return;
    wsrep_intent_abort_holder(key);
}

void ha_tidesdb::wsrep_intent_register_row(const uchar *pk, uint pk_len, tidesdb_trx_t *trx)
{
    if (!trx || !pk || !pk_len) return;
    wsrep_intent_add(wsrep_intent_row_key_bytes(pk, pk_len), trx);
}

void ha_tidesdb::wsrep_intent_check_row(const uchar *pk, uint pk_len)
{
    if (!pk || !pk_len) return;
    wsrep_intent_abort_holder(wsrep_intent_row_key_bytes(pk, pk_len));
}

/* Each key may live in a different shard, so lock the owning shard per key.  The
   holds are brief and uncontended in the common case, and never nest. */
void tidesdb_wsrep_intent_clear(tidesdb_trx_t *trx)
{
    if (!trx || trx->wsrep_intent_keys.empty()) return;
    for (const std::string &k : trx->wsrep_intent_keys)
    {
        tdb_wsrep_intent_shard &sh = wsrep_intent_shard_for(k);
        std::lock_guard<std::mutex> lk(sh.mtx);
        auto it = sh.map.find(k);
        if (it != sh.map.end() && it->second.trx == trx) sh.map.erase(it);
    }
    trx->wsrep_intent_keys.clear();
}

void tidesdb_wsrep_intent_truncate(tidesdb_trx_t *trx, size_t keep)
{
    if (!trx || trx->wsrep_intent_keys.size() <= keep) return;
    for (size_t j = keep; j < trx->wsrep_intent_keys.size(); j++)
    {
        const std::string &k = trx->wsrep_intent_keys[j];
        tdb_wsrep_intent_shard &sh = wsrep_intent_shard_for(k);
        std::lock_guard<std::mutex> lk(sh.mtx);
        auto it = sh.map.find(k);
        if (it != sh.map.end() && it->second.trx == trx) sh.map.erase(it);
    }
    trx->wsrep_intent_keys.resize(keep);
}

/* ---- cluster position checkpoint -------------------------------------- */

/* The meta column family, created on first use like the foreign key catalog. */
static tidesdb_column_family_t *wsrep_meta_cf()
{
    tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, WSREP_META_CF);
    if (cf) return cf;
    tidesdb_column_family_config_t cfg = tidesdb_default_column_family_config();
    if (tidesdb_create_column_family(tdb_global, WSREP_META_CF, &cfg) != TDB_SUCCESS)
        return tidesdb_get_column_family(tdb_global, WSREP_META_CF);
    return tidesdb_get_column_family(tdb_global, WSREP_META_CF);
}

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

static int tidesdb_wsrep_set_checkpoint(handlerton *, const XID *xid)
{
    /* The provider hands its own xid at checkpoint time; a foreign one is not ours
       to record, matching InnoDB which stores only a wsrep xid. */
    if (!wsrep_is_wsrep_xid(xid)) return 1;

    std::string blob;
    put_i64(blob, xid->formatID);
    put_i64(blob, xid->gtrid_length);
    put_i64(blob, xid->bqual_length);
    blob.append(xid->data, (size_t)(xid->gtrid_length + xid->bqual_length));

    tidesdb_column_family_t *cf = wsrep_meta_cf();
    if (!cf) return 1;

    tidesdb_txn_t *txn = nullptr;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return 1;
    int rc = tidesdb_txn_put(txn, cf, (const uint8_t *)WSREP_POSITION_KEY,
                             sizeof(WSREP_POSITION_KEY) - 1, (const uint8_t *)blob.data(),
                             blob.size(), TIDESDB_TTL_NONE);
    if (rc != TDB_SUCCESS)
    {
        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
        return 1;
    }
    rc = tidesdb_txn_commit(txn);
    tidesdb_txn_free(txn);
    return rc == TDB_SUCCESS ? 0 : 1;
}

static int tidesdb_wsrep_get_checkpoint(handlerton *, XID *xid)
{
    /* A node with no recorded position reports a null xid, which the server reads
       as a fresh join. */
    xid->formatID = -1;
    xid->gtrid_length = 0;
    xid->bqual_length = 0;

    tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, WSREP_META_CF);
    if (!cf) return 0;

    tidesdb_txn_t *txn = nullptr;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return 0;

    uint8_t *val = nullptr;
    size_t val_len = 0;
    int rc = tidesdb_txn_get_notrack(txn, cf, (const uint8_t *)WSREP_POSITION_KEY,
                                     sizeof(WSREP_POSITION_KEY) - 1, &val, &val_len);
    if (rc == TDB_SUCCESS && val && val_len >= 24)
    {
        long formatID = (long)get_i64(val);
        long gtrid = (long)get_i64(val + 8);
        long bqual = (long)get_i64(val + 16);
        if (gtrid >= 0 && bqual >= 0 && (size_t)(gtrid + bqual) <= XIDDATASIZE &&
            val_len >= 24 + (size_t)(gtrid + bqual))
        {
            xid->formatID = formatID;
            xid->gtrid_length = gtrid;
            xid->bqual_length = bqual;
            memcpy(xid->data, val + 24, (size_t)(gtrid + bqual));
        }
    }
    if (val) tidesdb_free(val);
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return 0;
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
}

#endif /* WITH_WSREP */
