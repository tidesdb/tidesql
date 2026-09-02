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

/* ha_tidesdb::update_row -- the UPDATE data path: rewrite the primary row, and where a key part
   changed, delete the old secondary/fts/spatial index entries and insert the new ones, enforcing
   UNIQUE secondary constraints and folding fts meta deltas into the transaction.  the per-concern
   steps are split into helpers so the driver stays a readable sequence. */

#include <ft_global.h>
#include <mysql/plugin.h>

#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

#include "ha_tidesdb.h"
#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/core/fts_text.h"
#include "src/handler/ha_tidesdb_fts.h"
#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_spatial.h"

/* ******************** update_row helpers ******************** */

/* Enforce PK and UNIQUE-secondary uniqueness before any txn mutation.  A TidesDB put silently
   overwrites, so an UPDATE that moves a row onto an existing primary key would destroy the
   colliding row, and one that moves it onto an existing UNIQUE secondary value would create a
   duplicate.  On a violation we set errkey/dup_ref and return the HA_ERR_* the caller surfaces; 0
   means clear. */
int ha_tidesdb::update_check_unique(const uchar *old_data, const uchar *new_data,
                                    const uchar *old_pk, uint old_pk_len, const uchar *new_pk,
                                    uint new_pk_len, bool pk_changed)
{
    tidesdb_trx_t *trx = cached_trx_;
    tidesdb_txn_t *txn = stmt_txn;

    if (pk_changed && share->has_user_pk)
    {
        uchar chk_dk[DATA_KEY_BUF_LEN];
        uint chk_dk_len = build_data_key(new_pk, new_pk_len, chk_dk);
        int ex = probe_pk_exists(trx, chk_dk, chk_dk_len);
        if (ex < 0) return -ex;
        if (ex)
        {
            errkey = lookup_errkey = share->pk_index;
            memcpy(dup_ref, new_pk, new_pk_len);
            return HA_ERR_FOUND_DUPP_KEY;
        }
    }

    if (share->num_secondary_indexes == 0) return 0;

    const my_ptrdiff_t nd_ptrdiff = (my_ptrdiff_t)(new_data - table->record[0]);
    for (uint i = 0; i < table->s->keys; i++)
    {
        if (share->has_user_pk && i == share->pk_index) continue;
        if (i >= share->idx_cfs.size() || !share->idx_cfs[i]) continue;
        if (share->idx_is_fts[i] || share->idx_is_spatial[i]) continue;
        if (!(table->key_info[i].flags & HA_NOSAME)) continue;

        KEY *ki = &table->key_info[i];

        /* SQL gives NULL no identity, so a UNIQUE index never constrains a row whose indexed value
           is NULL in any part.  Skip the check entirely in that case, matching InnoDB.  This also
           keeps the engine off the server's internal MHNSW graph table, whose UNIQUE(tref) column
           is NULL for the graph metadata rows. */
        bool any_null = false;
        for (uint p = 0; p < ki->user_defined_key_parts; p++)
        {
            Field *f = ki->key_part[p].field;
            if (f->real_maybe_null() && f->is_real_null(nd_ptrdiff))
            {
                any_null = true;
                break;
            }
        }
        if (any_null) continue;

        /* Compare the old and new comparable index keys.  Equal keys mean the indexed value did not
           change, so no new collision is possible no matter whether the primary key moved.  When
           they differ, this row's own existing entry sits under the old key, so any entry found
           under the new key necessarily belongs to a different row. */
        uchar *old_prefix = upd_old_ik_;
        uchar *new_prefix = upd_new_ik_;
        uint old_prefix_len =
            make_comparable_key(ki, old_data, ki->user_defined_key_parts, old_prefix);
        uint new_prefix_len =
            make_comparable_key(ki, new_data, ki->user_defined_key_parts, new_prefix);
        if (old_prefix_len == new_prefix_len && memcmp(old_prefix, new_prefix, new_prefix_len) == 0)
            continue;

        /* Read the index cf through a fresh iterator: it merges the txn's buffered writes when
           created, so it sees earlier same-statement rows and committed data alike, whereas a
           reused iterator would carry a stale writeset snapshot. */
        tidesdb_iter_t *dup_iter = NULL;
        int irc = tdb_iter_new_blocking(ha_thd(), txn, share->idx_cfs[i], &dup_iter);
        if (irc != TDB_SUCCESS || !dup_iter) return tdb_rc_to_ha(irc, "update_row dup_iter_new");
        tidesdb_iter_seek(dup_iter, new_prefix, new_prefix_len);
        bool dup = false;
        if (tidesdb_iter_valid(dup_iter))
        {
            uint8_t *fk = NULL;
            size_t fks = 0;
            tdb_owned_buf fk_g(fk);
            if (tidesdb_iter_key(dup_iter, &fk, &fks) == TDB_SUCCESS && fks >= new_prefix_len &&
                memcmp(fk, new_prefix, new_prefix_len) == 0)
            {
                dup = true;
                size_t suffix_len = fks - new_prefix_len;
                if (suffix_len > 0 && suffix_len <= ref_length)
                    memcpy(dup_ref, fk + new_prefix_len, suffix_len);
            }
        }
        tidesdb_iter_free(dup_iter);

        if (dup)
        {
            errkey = lookup_errkey = i;
            return HA_ERR_FOUND_DUPP_KEY;
        }
    }
    return 0;
}

/* Maintain one full-text index across the update: tokenize old and new docs and emit only the
   minimum set of deletes/puts.  When the PK moves, every old (term, old_pk) is deleted and every
   new (term, new_pk) inserted; when it is stable, a term-level diff writes only what changed.
   Returns TDB_SUCCESS on a no-op skip or success, else the library error. */
int ha_tidesdb::update_fts_index(uint i, const uchar *old_data, const uchar *new_data,
                                 const uchar *old_pk, uint old_pk_len, const uchar *new_pk,
                                 uint new_pk_len, bool pk_changed, time_t row_ttl)
{
    tidesdb_trx_t *trx = cached_trx_;
    tidesdb_txn_t *txn = stmt_txn;
    KEY *ki = &table->key_info[i];

    /* We skip if no indexed column actually changed */
    bool fts_changed = false;
    for (uint p = 0; p < ki->user_defined_key_parts; p++)
    {
        uint fieldnr = ki->key_part[p].fieldnr - 1;
        if (bitmap_is_set(table->write_set, fieldnr))
        {
            fts_changed = true;
            break;
        }
    }
    if (!fts_changed) return TDB_SUCCESS;

    CHARSET_INFO *fts_cs = ki->key_part[0].field->charset();

    std::vector<std::string> old_tokens, new_tokens;
    fts_extract_and_tokenize(table, ki, old_data, fts_cs, old_tokens);
    fts_extract_and_tokenize(table, ki, new_data, fts_cs, new_tokens);

    std::unordered_map<std::string, uint16> old_tf, new_tf;
    for (auto &tok : old_tokens) old_tf[tok]++;
    for (auto &tok : new_tokens) new_tf[tok]++;
    uint32 old_wc = (uint32)old_tokens.size();
    uint32 new_wc = (uint32)new_tokens.size();

    int rc;
    if (pk_changed)
    {
        /* PK changed -- the row identity changed so every old (term, old_pk) must be deleted and
           every new (term, new_pk) inserted.  No diffing possible across different PKs. */
        for (auto &kv : old_tf)
        {
            const auto &term = kv.first;
            uchar fk[FTS_KEY_BUF_LEN];
            uint fk_len = fts_build_key(term.data(), (uint)term.size(), old_pk, old_pk_len, fk);
            tdb_txn_delete_cf_blocking(cached_thd_, txn, share->idx_cfs[i], fk, fk_len, true);
        }
        for (auto &kv : new_tf)
        {
            const auto &term = kv.first;
            auto &tf = kv.second;
            uchar fk[FTS_KEY_BUF_LEN];
            uint fk_len = fts_build_key(term.data(), (uint)term.size(), new_pk, new_pk_len, fk);
            uchar fv[FTS_VALUE_LEN];
            fts_build_value(tf, new_wc, fv);
            rc = tdb_txn_put_blocking(cached_thd_, txn, share->idx_cfs[i], fk, fk_len, fv,
                                      FTS_VALUE_LEN, row_ttl);
            if (rc != TDB_SUCCESS) return rc;
        }
    }
    else
    {
        bool doc_len_changed = (old_wc != new_wc);
        rc = update_fts_index_diff(i, old_tf, new_tf, old_pk, old_pk_len, new_pk, new_pk_len,
                                   new_wc, doc_len_changed, row_ttl);
        if (rc != TDB_SUCCESS) return rc;
    }

    /* The doc count stays the same, only the word count moves.  Fold into the txn-level accumulator
       which flushes before commit so the meta update lands in the same txn as the row updates that
       produced it. */
    int64_t wc_delta = (int64_t)new_wc - (int64_t)old_wc;
    if (wc_delta != 0) trx_fts_meta_accumulate(trx, share->cf, i, 0, wc_delta);
    return TDB_SUCCESS;
}

int ha_tidesdb::update_fts_index_diff(uint i, const std::unordered_map<std::string, uint16> &old_tf,
                                      const std::unordered_map<std::string, uint16> &new_tf,
                                      const uchar *old_pk, uint old_pk_len, const uchar *new_pk,
                                      uint new_pk_len, uint32 new_wc, bool doc_len_changed,
                                      time_t row_ttl)
{
    tidesdb_txn_t *txn = stmt_txn;

    /* PK stable -- apply term-level diff.  Only delete a term when it disappears and only write
       a term when it is new, its tf changes, or doc_len changes (doc_len is part of the stored
       value used by BM25). */
    for (auto &kv : old_tf)
    {
        const auto &term = kv.first;
        if (new_tf.find(term) != new_tf.end()) continue;
        uchar fk[FTS_KEY_BUF_LEN];
        uint fk_len = fts_build_key(term.data(), (uint)term.size(), old_pk, old_pk_len, fk);
        tdb_txn_delete_cf_blocking(cached_thd_, txn, share->idx_cfs[i], fk, fk_len, true);
    }

    for (auto &kv : new_tf)
    {
        const auto &term = kv.first;
        auto &new_cnt = kv.second;
        auto it = old_tf.find(term);
        bool need_put;
        if (it == old_tf.end())
            need_put = true;
        else if (doc_len_changed)
            need_put = true;
        else
            need_put = (it->second != new_cnt);

        if (!need_put) continue;

        uchar fk[FTS_KEY_BUF_LEN];
        uint fk_len = fts_build_key(term.data(), (uint)term.size(), new_pk, new_pk_len, fk);
        uchar fv[FTS_VALUE_LEN];
        fts_build_value(new_cnt, new_wc, fv);
        int rc = tdb_txn_put_blocking(cached_thd_, txn, share->idx_cfs[i], fk, fk_len, fv,
                                      FTS_VALUE_LEN, row_ttl);
        if (rc != TDB_SUCCESS) return rc;
    }
    return TDB_SUCCESS;
}

/* Maintain one spatial index across the update: when the geometry column changed, delete the old
   hilbert entry and insert the new one.  Returns TDB_SUCCESS on a no-op skip or success. */
int ha_tidesdb::update_spatial_index(uint i, const uchar *old_data, const uchar *new_data,
                                     const uchar *old_pk, uint old_pk_len, const uchar *new_pk,
                                     uint new_pk_len, time_t row_ttl)
{
    tidesdb_txn_t *txn = stmt_txn;
    KEY *ki = &table->key_info[i];

    /* Skip when the geometry column is unchanged. */
    uint fieldnr = ki->key_part[0].fieldnr - 1;
    if (!bitmap_is_set(table->write_set, fieldnr)) return TDB_SUCCESS;

    Field *geom_field = ki->key_part[0].field;

    /* Delete old spatial entry */
    {
        my_ptrdiff_t ptd = (my_ptrdiff_t)(old_data - table->record[0]);
        if (ptd) geom_field->move_field_offset(ptd);
        String gs;
        geom_field->val_str(&gs, &gs);
        if (ptd) geom_field->move_field_offset(-ptd);
        double xmn, ymn, xmx, ymx;
        if (gs.length() > 0 &&
            spatial_compute_mbr((const uchar *)gs.ptr(), gs.length(), &xmn, &ymn, &xmx, &ymx))
        {
            uchar sk[SPATIAL_HILBERT_KEY_LEN + MAX_KEY_LENGTH];
            uint sk_len = spatial_build_key((xmn + xmx) / MBR_CENTROID_DIV,
                                            (ymn + ymx) / MBR_CENTROID_DIV, old_pk, old_pk_len, sk);
            tdb_txn_delete_cf_blocking(cached_thd_, txn, share->idx_cfs[i], sk, sk_len, true);
        }
    }

    /* Insert new spatial entry */
    {
        my_ptrdiff_t ptd = (my_ptrdiff_t)(new_data - table->record[0]);
        if (ptd) geom_field->move_field_offset(ptd);
        String gs;
        geom_field->val_str(&gs, &gs);
        if (ptd) geom_field->move_field_offset(-ptd);
        double xmn, ymn, xmx, ymx;
        if (gs.length() > 0 &&
            spatial_compute_mbr((const uchar *)gs.ptr(), gs.length(), &xmn, &ymn, &xmx, &ymx))
        {
            uchar sk[SPATIAL_HILBERT_KEY_LEN + MAX_KEY_LENGTH];
            uint sk_len = spatial_build_key((xmn + xmx) / MBR_CENTROID_DIV,
                                            (ymn + ymx) / MBR_CENTROID_DIV, new_pk, new_pk_len, sk);
            uchar sv[SPATIAL_MBR_VALUE_LEN];
            spatial_build_value(xmn, ymn, xmx, ymx, sv);
            int rc = tdb_txn_put_blocking(cached_thd_, txn, share->idx_cfs[i], sk, sk_len, sv,
                                          SPATIAL_MBR_VALUE_LEN, row_ttl);
            if (rc != TDB_SUCCESS) return rc;
        }
    }
    return TDB_SUCCESS;
}

/* Maintain one regular secondary index across the update: when the indexed value or the PK changed,
   delete the old (index_value, old_pk) entry and insert the new one.  Returns TDB_SUCCESS on a
   no-op skip or success. */
int ha_tidesdb::update_regular_index(uint i, const uchar *old_data, const uchar *new_data,
                                     const uchar *old_pk, uint old_pk_len, const uchar *new_pk,
                                     uint new_pk_len, bool pk_changed, time_t row_ttl)
{
    tidesdb_txn_t *txn = stmt_txn;
    KEY *ki = &table->key_info[i];

    /* Skip before building keys when no indexed column changed and the PK is stable.  Saves the
       per-row make_comparable_key / sec_idx_key cost on wide updates that touch only unrelated
       columns. */
    if (!pk_changed)
    {
        bool idx_changed = false;
        for (uint p = 0; p < ki->user_defined_key_parts; p++)
        {
            uint fieldnr = ki->key_part[p].fieldnr - 1;
            if (bitmap_is_set(table->write_set, fieldnr))
            {
                idx_changed = true;
                break;
            }
        }
        if (!idx_changed) return TDB_SUCCESS;
    }

    uchar *old_ik = upd_old_ik_;
    uchar *new_ik = upd_new_ik_;

    /* We build the old index entry key.  make_comparable_key reads only old_data
       and the old pk is appended explicitly just below, so the old side needs no
       current_pk_buf_ priming, only the new side does. */
    uint old_ik_len = make_comparable_key(ki, old_data, ki->user_defined_key_parts, old_ik);
    memcpy(old_ik + old_ik_len, old_pk, old_pk_len);
    old_ik_len += old_pk_len;

    /* We build the new index entry key.  current_pk_buf_ is set to the new PK so
       sec_idx_key's pk_from_record path works for hidden-PK tables. */
    memcpy(current_pk_buf_, new_pk, new_pk_len);
    current_pk_len_ = new_pk_len;
    uint new_ik_len = sec_idx_key(i, new_data, new_ik);

    if (old_ik_len == new_ik_len && memcmp(old_ik, new_ik, old_ik_len) == 0) return TDB_SUCCESS;

    int rc =
        tdb_txn_delete_cf_blocking(cached_thd_, txn, share->idx_cfs[i], old_ik, old_ik_len, true);
    if (rc != TDB_SUCCESS) return rc;
    rc = tdb_txn_put_blocking(cached_thd_, txn, share->idx_cfs[i], new_ik, new_ik_len,
                              &tdb_empty_val, sizeof(tdb_empty_val), row_ttl);
    if (rc != TDB_SUCCESS) return rc;
    return TDB_SUCCESS;
}

/* ******************** update_row (UPDATE) ******************** */

int ha_tidesdb::update_row(const uchar *old_data, const uchar *new_data)
{
    DBUG_ENTER("ha_tidesdb::update_row");

    MY_BITMAP *old_map = tmp_use_all_columns(table, &table->read_set);

    /* We cache THD and trx once (set in external_lock) to avoid per-row ha_thd() virtual dispatch
       and thd_get_ha_data() hash lookup throughout this function. */
    tidesdb_trx_t *trx = cached_trx_;

    /* We use handler-owned pk buffers for old/new PK to avoid large stack arrays.  old_pk is saved
       from current_pk_buf_ before the secondary loop overwrites it; new_pk uses its own buffer so
       it survives that manipulation (avoids overlapping-memcpy UB). */
    uchar old_pk[MAX_KEY_LENGTH];
    uint old_pk_len = current_pk_len_;
    memcpy(old_pk, current_pk_buf_, old_pk_len);

    uchar new_pk[MAX_KEY_LENGTH];
    uint new_pk_len = pk_from_record(new_data, new_pk);

    const std::string &new_row = serialize_row(new_data);
    if (share->encrypted && new_row.empty())
    {
        tmp_restore_column_map(&table->read_set, old_map);
        DBUG_RETURN(HA_ERR_GENERIC);
    }
    const uint8_t *row_ptr = (const uint8_t *)new_row.data();
    size_t row_len = new_row.size();

    {
        int erc = ensure_stmt_txn();
        if (erc)
        {
            tmp_restore_column_map(&table->read_set, old_map);
            DBUG_RETURN(erc);
        }
    }
    stmt_txn_dirty = true;
    if (trx) trx->dirty = true;

    cache_stmt_thdvars();

    bool pk_changed = (old_pk_len != new_pk_len || memcmp(old_pk, new_pk, old_pk_len) != 0);

    /* We compute TTL when the table has TTL configured or the session overrides it.  Uses
       cached_sess_ttl_ to avoid THDVAR + ha_thd() per row. */
    time_t row_ttl =
        (share->has_ttl || cached_sess_ttl_ > 0) ? compute_row_ttl(new_data) : TIDESDB_TTL_NONE;

    if (!cached_skip_unique_)
    {
        int uc = update_check_unique(old_data, new_data, old_pk, old_pk_len, new_pk, new_pk_len,
                                     pk_changed);
        if (uc != 0)
        {
            tmp_restore_column_map(&table->read_set, old_map);
            DBUG_RETURN(uc);
        }
    }

    /* Foreign keys, both directions.  As a child, the new referencing values
       must still point at an existing parent.  As a parent, a referenced key
       may not be changed out from under a child that still references it. */
    if (int frc = fk_check_child(new_data))
    {
        tmp_restore_column_map(&table->read_set, old_map);
        DBUG_RETURN(frc);
    }
    if (int frc = fk_enforce_parent_update(old_data, new_data))
    {
        tmp_restore_column_map(&table->read_set, old_map);
        DBUG_RETURN(frc);
    }

    int rc = update_rewrite_primary(old_pk, old_pk_len, new_pk, new_pk_len, row_ptr, row_len,
                                    row_ttl, pk_changed);
    if (rc != TDB_SUCCESS)
    {
        tmp_restore_column_map(&table->read_set, old_map);
        DBUG_RETURN(tdb_rc_to_ha(rc, "update_row"));
    }

    rc = update_maintain_indexes(old_data, new_data, old_pk, old_pk_len, new_pk, new_pk_len,
                                 pk_changed, row_ttl);
    if (rc != TDB_SUCCESS)
    {
        tmp_restore_column_map(&table->read_set, old_map);
        DBUG_RETURN(tdb_rc_to_ha(rc, "update_row"));
    }

#ifdef WITH_WSREP
    /* Galera.  Certify both the before and after images so the row's old identity and any changed
       unique value stay covered, and record or, on an applier, abort the write intents for both
       images so a cross-node conflict on either the old or the new key is caught. */
    if (wsrep_on(cached_thd_))
    {
        if (wsrep_thd_is_local(cached_thd_))
        {
            if (int wrc =
                    wsrep_certify_row(cached_thd_, old_data, new_data, WSREP_SERVICE_KEY_EXCLUSIVE))
            {
                tmp_restore_column_map(&table->read_set, old_map);
                DBUG_RETURN(wrc);
            }
            wsrep_intent_register_row(new_pk, new_pk_len, trx);
            if (pk_changed) wsrep_intent_register_row(old_pk, old_pk_len, trx);
            for (uint i = 0; i < table->s->keys; i++)
            {
                wsrep_intent_register(i, new_data, trx);
                wsrep_intent_register(i, old_data, trx);
            }
        }
        else
        {
            wsrep_intent_check_row(new_pk, new_pk_len);
            if (pk_changed) wsrep_intent_check_row(old_pk, old_pk_len);
            for (uint i = 0; i < table->s->keys; i++)
            {
                wsrep_intent_check(i, new_data);
                wsrep_intent_check(i, old_data);
            }
        }
    }
#endif

    memcpy(current_pk_buf_, new_pk, new_pk_len);
    current_pk_len_ = new_pk_len;

    /* Bulk UPDATE mid-txn commit.  Symmetric to write_row's bulk path.  One UPDATE op counts as 1
       data put + 1 data delete (when PK changed) + up to num_secondary_indexes entries rewritten;
       we overestimate as `1 + 2 * num_secondary_indexes`. */
    if (in_bulk_update_)
    {
        if (int mrc = bulk_flush_if_threshold(trx, 1 + 2 * (ha_rows)share->num_secondary_indexes))
        {
            tmp_restore_column_map(&table->read_set, old_map);
            DBUG_RETURN(mrc);
        }
    }

    /* Commit happens in external_lock(F_UNLCK). */
    tmp_restore_column_map(&table->read_set, old_map);
    DBUG_RETURN(0);
}

int ha_tidesdb::update_rewrite_primary(const uchar *old_pk, uint old_pk_len, const uchar *new_pk,
                                       uint new_pk_len, const uint8_t *row_ptr, size_t row_len,
                                       time_t row_ttl, bool pk_changed)
{
    tidesdb_txn_t *txn = stmt_txn;

    /* If PK changed, we delete the old primary entry before writing the new one. */
    if (pk_changed)
    {
        uchar old_dk[DATA_KEY_BUF_LEN];
        uint old_dk_len = build_data_key(old_pk, old_pk_len, old_dk);
        int rc = tdb_txn_delete_cf_blocking(cached_thd_, txn, share->cf, old_dk, old_dk_len,
                                            cached_single_delete_primary_);
        if (rc != TDB_SUCCESS) return rc;
    }

    uchar new_dk[DATA_KEY_BUF_LEN];
    uint new_dk_len = build_data_key(new_pk, new_pk_len, new_dk);
    return tdb_txn_put_blocking(cached_thd_, txn, share->cf, new_dk, new_dk_len, row_ptr, row_len,
                                row_ttl);
}

int ha_tidesdb::update_maintain_indexes(const uchar *old_data, const uchar *new_data,
                                        const uchar *old_pk, uint old_pk_len, const uchar *new_pk,
                                        uint new_pk_len, bool pk_changed, time_t row_ttl)
{
    /* One walk of the secondary indexes, dispatching each to its maintainer.  Each maintainer
       short-circuits on an unchanged index. */
    if (share->num_secondary_indexes == 0) return TDB_SUCCESS;

    const uint num_keys = table->s->keys;
    const bool has_user_pk = share->has_user_pk;
    const uint pk_index = share->pk_index;
    const size_t idx_cfs_sz = share->idx_cfs.size();

    for (uint i = 0; i < num_keys; i++)
    {
        if (has_user_pk && i == pk_index) continue;
        if (i >= idx_cfs_sz || !share->idx_cfs[i]) continue;

        int rc;
        if (share->idx_is_fts[i])
            rc = update_fts_index(i, old_data, new_data, old_pk, old_pk_len, new_pk, new_pk_len,
                                  pk_changed, row_ttl);
        else if (share->idx_is_spatial[i])
            rc = update_spatial_index(i, old_data, new_data, old_pk, old_pk_len, new_pk, new_pk_len,
                                      row_ttl);
        else
            rc = update_regular_index(i, old_data, new_data, old_pk, old_pk_len, new_pk, new_pk_len,
                                      pk_changed, row_ttl);
        if (rc != TDB_SUCCESS) return rc;
    }
    return TDB_SUCCESS;
}
