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

/* the online-DDL handler surface: check_if_supported_inplace_alter classifies an ALTER into
   instant, in-place, or copy; prepare/inplace/commit_inplace_alter_table create the column
   families for newly added secondary indexes, populate them from the existing rows, and either
   publish or discard the change under the server's alter locking; check_if_incompatible_data
   reports when a metadata-only change can skip a table rebuild.  the column-family config the new
   indexes are built from comes from the shared config builders. */

#include <ft_global.h>
#include <mysql/plugin.h>

#include <string>
#include <vector>

#include "ha_tidesdb.h"
#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/engine/ha_tidesdb_config.h"
#include "src/handler/ha_tidesdb_fts.h"
#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_keycodec.h"
#include "src/handler/ha_tidesdb_spatial.h"

/*
  Classify ALTER TABLE operations into INSTANT / INPLACE / COPY.

  INSTANT     metadata-only changes (.frm rewrite, no engine work):
              rename column/index, change default, change table options,
              ADD COLUMN, DROP COLUMN (row format is self-describing via
              the ROW_HEADER_MAGIC header written by serialize_row)
  INPLACE     add/drop secondary indexes (create/drop CFs, populate)
  COPY        column type changes, PK changes
*/
enum_alter_inplace_result ha_tidesdb::check_if_supported_inplace_alter(
    TABLE *altered_table, Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::check_if_supported_inplace_alter");

    alter_table_operations flags = ha_alter_info->handler_flags;

    /* Operations that are pure metadata (INSTANT).
       ADD/DROP COLUMN is instant because the packed row format includes
       a header with the stored null_bytes and field_count, so
       deserialize_row adapts to rows written with any prior schema. */
    static const alter_table_operations TIDESDB_INSTANT =
        ALTER_COLUMN_NAME | ALTER_RENAME_COLUMN | ALTER_CHANGE_COLUMN_DEFAULT |
        ALTER_COLUMN_DEFAULT | ALTER_COLUMN_OPTION | ALTER_CHANGE_CREATE_OPTION |
        ALTER_DROP_CHECK_CONSTRAINT | ALTER_VIRTUAL_GCOL_EXPR | ALTER_RENAME | ALTER_RENAME_INDEX |
        ALTER_INDEX_IGNORABILITY | ALTER_ADD_COLUMN | ALTER_DROP_COLUMN |
        ALTER_STORED_COLUMN_ORDER | ALTER_VIRTUAL_COLUMN_ORDER;

    /* Operations we can do inplace (add/drop secondary indexes) */
    static const alter_table_operations TIDESDB_INPLACE_INDEX =
        ALTER_ADD_NON_UNIQUE_NON_PRIM_INDEX | ALTER_DROP_NON_UNIQUE_NON_PRIM_INDEX |
        ALTER_ADD_UNIQUE_INDEX | ALTER_DROP_UNIQUE_INDEX | ALTER_ADD_INDEX | ALTER_DROP_INDEX |
        ALTER_INDEX_ORDER;

    /* If only instant operations, return INSTANT */
    if (!(flags & ~TIDESDB_INSTANT)) DBUG_RETURN(HA_ALTER_INPLACE_INSTANT);

    /* If only instant + index operations, return INPLACE with no lock.
       TidesDB handles all concurrency via MVCC internally -- the index
       population scan runs inside its own transaction and does not need
       server-level MDL blocking. */
    if (!(flags & ~(TIDESDB_INSTANT | TIDESDB_INPLACE_INDEX)))
    {
        /**** Changing PK requires full rebuild */
        if (flags & (ALTER_ADD_PK_INDEX | ALTER_DROP_PK_INDEX))
        {
            ha_alter_info->unsupported_reason = "TidesDB cannot change PRIMARY KEY inplace";
            DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
        }
        /* FULLTEXT and SPATIAL indexes ride ALTER_ADD_INDEX but cannot be
           populated by the inplace builder -- the per-row loops in
           inplace_alter_table skip them, so the CF would stay empty and
           later MATCH AGAINST / MBRWithin would silently return no rows
           against pre-existing data.  Forcing COPY routes every row
           through write_row, which knows how to maintain these CFs. */
        if (ha_alter_info->index_add_count > 0)
        {
            for (uint a = 0; a < ha_alter_info->index_add_count; a++)
            {
                uint key_num = ha_alter_info->index_add_buffer[a];
                KEY *new_key = &ha_alter_info->key_info_buffer[key_num];
                if (is_fts_index(new_key))
                {
                    ha_alter_info->unsupported_reason = "TidesDB cannot add FULLTEXT index inplace";
                    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
                }
                if (is_spatial_index(new_key))
                {
                    ha_alter_info->unsupported_reason = "TidesDB cannot add SPATIAL index inplace";
                    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
                }
            }
        }
        DBUG_RETURN(HA_ALTER_INPLACE_NO_LOCK);
    }

    /* Everything else requires COPY */
    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
}

/*
  Create CFs for newly added indexes.
  Called with shared MDL lock (concurrent DML is allowed).
*/
bool ha_tidesdb::prepare_inplace_alter_table(TABLE *altered_table,
                                             Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::prepare_inplace_alter_table");

    ha_tidesdb_inplace_ctx *ctx;
    try
    {
        ctx = new ha_tidesdb_inplace_ctx();
    }
    catch (...)
    {
        DBUG_RETURN(true);
    }
    ha_alter_info->handler_ctx = ctx;

    tidesdb_column_family_config_t cfg = build_cf_config(TDB_TABLE_OPTIONS(table));

    std::string base_cf = share->cf_name;

    if (ha_alter_info->index_add_count > 0)
    {
        for (uint a = 0; a < ha_alter_info->index_add_count; a++)
        {
            uint key_num = ha_alter_info->index_add_buffer[a];
            KEY *new_key = &ha_alter_info->key_info_buffer[key_num];

            if (new_key->flags & HA_NOSAME &&
                altered_table->s->primary_key < altered_table->s->keys &&
                key_num == altered_table->s->primary_key)
                continue;

            std::string idx_cf = base_cf + CF_INDEX_INFIX + new_key->name.str;

            tidesdb_drop_column_family(tdb_global, idx_cf.c_str());

            tidesdb_column_family_config_t idx_cfg = cfg;

            int rc = tidesdb_create_column_family(tdb_global, idx_cf.c_str(), &idx_cfg);
            if (rc != TDB_SUCCESS)
            {
                sql_print_error("[TIDESDB] inplace ADD INDEX: failed to create CF '%s' (err=%d)",
                                idx_cf.c_str(), rc);
                my_error(ER_INTERNAL_ERROR, MYF(0), "[TIDESDB] failed to create index CF");
                DBUG_RETURN(true);
            }

            tidesdb_column_family_t *icf = tidesdb_get_column_family(tdb_global, idx_cf.c_str());
            if (!icf)
            {
                sql_print_error("[TIDESDB] inplace ADD INDEX: CF '%s' not found after create",
                                idx_cf.c_str());
                my_error(ER_INTERNAL_ERROR, MYF(0), "[TIDESDB] index CF not found after create");
                DBUG_RETURN(true);
            }

            ctx->add_cfs.push_back(icf);
            ctx->add_cf_names.push_back(idx_cf);
            ctx->add_key_nums.push_back(key_num);
        }
    }

    if (ha_alter_info->index_drop_count > 0)
    {
        for (uint d = 0; d < ha_alter_info->index_drop_count; d++)
        {
            KEY *old_key = ha_alter_info->index_drop_buffer[d];
            uint old_key_num = (uint)(old_key - table->key_info);
            if (old_key_num < share->idx_cf_names.size() &&
                !share->idx_cf_names[old_key_num].empty())
            {
                ctx->drop_cf_names.push_back(share->idx_cf_names[old_key_num]);
            }
        }
    }

    DBUG_RETURN(false);
}

/*
  Inplace phase -- we populate newly added indexes by scanning the table.
  Called with no MDL lock blocking (HA_ALTER_INPLACE_NO_LOCK).
*/
uint ha_tidesdb::inplace_build_index_key(KEY *ki, my_ptrdiff_t ptdiff, uchar *ik,
                                         bool &row_has_null)
{
    uint pos = 0;
    for (uint p = 0; p < ki->user_defined_key_parts; p++)
    {
        KEY_PART_INFO *kp = &ki->key_part[p];
        Field *field = kp->field;

        /* A descending key part stores its span byte-inverted, matching make_comparable_key. */
        const uint part_start = pos;
        const bool desc = (kp->key_part_flag & HA_REVERSE_SORT) != 0;

        field->move_field_offset(ptdiff);
        if (field->real_maybe_null())
        {
            if (field->is_null())
            {
                ik[pos++] = SORT_KEY_NULL;
                bzero(ik + pos, kp->length);
                pos += kp->length;
                field->move_field_offset(-ptdiff);
                row_has_null = true;
                if (desc) invert_key_part(ik + part_start, pos - part_start);
                continue;
            }
            ik[pos++] = SORT_KEY_NOT_NULL;
        }
        field->sort_string(ik + pos, kp->length);
        field->move_field_offset(-ptdiff);
        pos += kp->length;
        if (desc) invert_key_part(ik + part_start, pos - part_start);
    }
    return pos;
}

int ha_tidesdb::inplace_add_row_entries(ha_tidesdb_inplace_ctx *ctx, TABLE *altered_table,
                                        const uint8_t *key_data, size_t key_size,
                                        const uint8_t *val_data, size_t val_size,
                                        tidesdb_txn_t *txn,
                                        std::vector<std::unordered_set<std::string>> &idx_seen,
                                        const std::vector<bool> &idx_is_unique, uint &fail_key_num)
{
    const uchar *pk = key_data + KEY_NAMESPACE_LEN;
    uint pk_len = (uint)(key_size - KEY_NAMESPACE_LEN);

    /* We decode the row into table->record[0].  The field pointers from
       altered_table->key_info will be temporarily repointed (via
       move_field_offset) to read from this buffer. */
    if (share->has_blobs || share->encrypted)
    {
        std::string row_data((const char *)val_data, val_size);
        deserialize_row(table->record[0], row_data);
    }
    else
    {
        deserialize_row(table->record[0], (const uchar *)val_data, val_size);
    }

    /* For each newly added index, build the index entry key.
       altered_table->key_info fields have ptr into altered_table->record[0],
       but the data lives in table->record[0].  We compute ptdiff to
       rebase field pointers to read from the correct buffer.
       Key format matches make_comparable_key()= [null_byte] + sort_string. */
    my_ptrdiff_t ptdiff = (my_ptrdiff_t)(table->record[0] - altered_table->record[0]);

    for (uint a = 0; a < ctx->add_cfs.size(); a++)
    {
        uint key_num = ctx->add_key_nums[a];
        KEY *ki = &altered_table->key_info[key_num];

        /* FULLTEXT and SPATIAL indexes use different population paths */
        if (is_fts_index(ki)) continue;
        if (is_spatial_index(ki)) continue;

        uchar ik[SEC_IDX_KEY_BUF_LEN];
        bool row_has_null = false;
        uint pos = inplace_build_index_key(ki, ptdiff, ik, row_has_null);

        /* A UNIQUE index never constrains a row whose indexed value is NULL in any part, so
           multiple such rows are allowed; only dedup rows whose key is entirely non-NULL. */
        if (idx_is_unique[a] && !row_has_null)
        {
            std::string prefix((const char *)ik, pos);
            if (!idx_seen[a].insert(prefix).second)
            {
                fail_key_num = key_num;
                return 1;
            }
        }

        memcpy(ik + pos, pk, pk_len);
        pos += pk_len;

        int rc = tdb_txn_put_blocking(ha_thd(), txn, ctx->add_cfs[a], ik, pos, &tdb_empty_val,
                                      sizeof(tdb_empty_val), TIDESDB_TTL_NONE);
        if (rc != TDB_SUCCESS)
        {
            /* A per-row put failure leaves a hole in the new index, which
               commit_inplace_alter_table would then ship as a silently
               incomplete index.  Abort the ALTER instead. */
            sql_print_error(
                "[TIDESDB] inplace ADD INDEX: put failed for key %u (err=%d), "
                "aborting to avoid a partial index",
                key_num, rc);
            fail_key_num = key_num;
            return 2;
        }
    }
    return 0;
}

int ha_tidesdb::inplace_batch_commit_reseek(tidesdb_txn_t *&txn, tidesdb_iter_t *&iter,
                                            const uchar *last_data_key, size_t last_data_key_len)
{
    int crc = tidesdb_txn_commit(txn);
    if (crc != TDB_SUCCESS)
    {
        /* A failed batch commit drops this batch of index entries.  Carrying
           on would report success with an index silently missing rows. */
        sql_print_error(
            "[TIDESDB] inplace ADD INDEX: batch commit failed rc=%d, "
            "aborting to avoid a partial index",
            crc);
        return 1; /* iter and txn still live -- caller frees both */
    }
    tidesdb_iter_free(iter);
    iter = NULL;

    /* We reset the txn with READ_COMMITTED -- index builds
       don't need snapshot consistency across batches. */
    int rrc = tidesdb_txn_reset(txn, TDB_ISOLATION_READ_COMMITTED);
    if (rrc != TDB_SUCCESS)
    {
        sql_print_warning(
            "[TIDESDB] inplace ADD INDEX: tidesdb_txn_reset failed (rc=%d), "
            "falling back to free+begin",
            rrc);
        tidesdb_txn_free(txn);
        txn = NULL;
        int bec = tidesdb_txn_begin_with_isolation(tdb_global, TDB_ISOLATION_READ_COMMITTED, &txn);
        if (bec != TDB_SUCCESS || !txn)
        {
            sql_print_error("[TIDESDB] inplace ADD INDEX: batch txn_begin failed");
            txn = NULL;
            return 1;
        }
    }

    int rc = tdb_iter_new_blocking(ha_thd(), txn, share->cf, &iter);
    if (rc != TDB_SUCCESS || !iter)
    {
        tidesdb_txn_free(txn);
        txn = NULL;
        return 1;
    }
    int src = tidesdb_iter_seek(iter, last_data_key, last_data_key_len);
    if (src != TDB_SUCCESS)
    {
        sql_print_warning("[TIDESDB] inplace ADD INDEX: iter_seek failed rc=%d", src);
        return 2; /* end scan gracefully */
    }
    if (tidesdb_iter_valid(iter)) tidesdb_iter_next(iter);
    return 0;
}

bool ha_tidesdb::inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info)
{
    DBUG_ENTER("ha_tidesdb::inplace_alter_table");

    ha_tidesdb_inplace_ctx *ctx = static_cast<ha_tidesdb_inplace_ctx *>(ha_alter_info->handler_ctx);

    if (!ctx || ctx->add_cfs.empty())
        DBUG_RETURN(false); /* Nothing to populate (drop-only or instant) */

    /* We mark all columns readable on the altered table since we read
       fields via make_sort_key_part during index key construction. */
    MY_BITMAP *old_map = tmp_use_all_columns(altered_table, &altered_table->read_set);

    /* We do a full table scan to populate the new secondary indexes.
       We use the altered_table's key_info for building index keys,
       since that matches the new key numbering. */

    /* We always use READ_COMMITTED for index population.  The scan reads
       potentially millions of rows; higher isolation levels would track
       each key in the read-set, causing unbounded memory growth.  Index
       builds are DDL and never need OCC conflict detection. */
    tidesdb_txn_t *txn = NULL;
    int rc = tidesdb_txn_begin_with_isolation(tdb_global, TDB_ISOLATION_READ_COMMITTED, &txn);
    if (rc != TDB_SUCCESS || !txn)
    {
        sql_print_error("[TIDESDB] inplace ADD INDEX: txn_begin failed (err=%d)", rc);
        my_error(ER_INTERNAL_ERROR, MYF(0), "[TIDESDB] failed to begin txn for index build");
        tmp_restore_column_map(&altered_table->read_set, old_map);
        DBUG_RETURN(true);
    }

    tidesdb_iter_t *iter = NULL;
    rc = tdb_iter_new_blocking(ha_thd(), txn, share->cf, &iter);
    if (rc != TDB_SUCCESS || !iter)
    {
        tidesdb_txn_free(txn);
        sql_print_error("[TIDESDB] inplace ADD INDEX: iter_new failed (err=%d)", rc);
        my_error(ER_INTERNAL_ERROR, MYF(0), "[TIDESDB] failed to create iterator for index build");
        tmp_restore_column_map(&altered_table->read_set, old_map);
        DBUG_RETURN(true);
    }
    tidesdb_iter_seek_to_first(iter);

    DBUG_RETURN(inplace_scan_and_build(ctx, altered_table, txn, iter, old_map));
}

std::vector<bool> ha_tidesdb::inplace_build_unique_flags(ha_tidesdb_inplace_ctx *ctx,
                                                         TABLE *altered_table)
{
    std::vector<bool> flags(ctx->add_cfs.size(), false);
    for (uint a = 0; a < ctx->add_cfs.size(); a++)
    {
        KEY *ki = &altered_table->key_info[ctx->add_key_nums[a]];
        if (ki->flags & HA_NOSAME) flags[a] = true;
    }
    return flags;
}

bool ha_tidesdb::inplace_abort_build(tidesdb_iter_t *iter, tidesdb_txn_t *txn, TABLE *altered_table,
                                     MY_BITMAP *old_map)
{
    if (iter) tidesdb_iter_free(iter);
    if (txn)
    {
        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
    }
    tmp_restore_column_map(&altered_table->read_set, old_map);
    return true;
}

bool ha_tidesdb::inplace_scan_and_build(ha_tidesdb_inplace_ctx *ctx, TABLE *altered_table,
                                        tidesdb_txn_t *&txn, tidesdb_iter_t *&iter,
                                        MY_BITMAP *old_map)
{
    ha_rows rows_processed = 0;

    /* For UNIQUE indexes, we track seen index-column prefixes to detect
       duplicates.  If a duplicate is found we must abort the ALTER.
       unordered_set gives O(1) amortized lookup vs O(log n) for std::set,
       which matters for tables with millions of rows. */
    std::vector<bool> idx_is_unique = inplace_build_unique_flags(ctx, altered_table);
    std::vector<std::unordered_set<std::string>> idx_seen(ctx->add_cfs.size());

    /* We remember the last data key so we can seek directly to it after a
       batch commit, which avoids an O(n^2) rescan from the start each batch. */
    uchar last_data_key[DATA_KEY_BUF_LEN];
    size_t last_data_key_len = 0;

    while (tidesdb_iter_valid(iter))
    {
        uint8_t *key_data = NULL;
        size_t key_size = 0;
        uint8_t *val_data = NULL;
        size_t val_size = 0;

        if (tidesdb_iter_key_value(iter, &key_data, &key_size, &val_data, &val_size) != TDB_SUCCESS)
        {
            tidesdb_iter_next(iter);
            continue;
        }
        tdb_owned_buf key_data_g(key_data), val_data_g(val_data);

        if (key_size < KEY_NAMESPACE_LEN || key_data[0] != KEY_NS_DATA)
        {
            tidesdb_iter_next(iter);
            continue;
        }

        if (key_size <= sizeof(last_data_key))
        {
            memcpy(last_data_key, key_data, key_size);
            last_data_key_len = key_size;
        }

        uint fail_key_num = 0;
        int prc = inplace_add_row_entries(ctx, altered_table, key_data, key_size, val_data,
                                          val_size, txn, idx_seen, idx_is_unique, fail_key_num);
        if (prc != 0)
        {
            if (prc == 1)
                my_error(ER_DUP_ENTRY, MYF(0), "?", altered_table->key_info[fail_key_num].name.str);
            else
                my_error(ER_INTERNAL_ERROR, MYF(0),
                         "[TIDESDB] per-row put failed during index build");
            return inplace_abort_build(iter, txn, altered_table, old_map);
        }

        rows_processed++;

        /* We check for KILL signal periodically so the user can cancel
           long-running index builds via KILL <thread_id>. */
        if ((rows_processed % TIDESDB_INDEX_BUILD_BATCH) == 0 && thd_killed(ha_thd()))
        {
            my_error(ER_QUERY_INTERRUPTED, MYF(0));
            return inplace_abort_build(iter, txn, altered_table, old_map);
        }

        if (rows_processed % TIDESDB_INDEX_BUILD_BATCH == 0)
        {
            int brc = inplace_batch_commit_reseek(txn, iter, last_data_key, last_data_key_len);
            if (brc == 1)
            {
                my_error(ER_INTERNAL_ERROR, MYF(0), "[TIDESDB] batch failed during index build");
                return inplace_abort_build(iter, txn, altered_table, old_map);
            }
            if (brc == 2) break; /* iter_seek failed -- end scan gracefully */
            continue;            /* Don't call iter_next again */
        }

        tidesdb_iter_next(iter);
    }

    tidesdb_iter_free(iter);

    int rc = tidesdb_txn_commit(txn);
    if (rc != TDB_SUCCESS) tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);

    if (rc != TDB_SUCCESS)
    {
        sql_print_error("[TIDESDB] inplace ADD INDEX: final commit failed (err=%d)", rc);
        my_error(ER_INTERNAL_ERROR, MYF(0), "[TIDESDB] final commit failed during index build");
        tmp_restore_column_map(&altered_table->read_set, old_map);
        return true;
    }
    tmp_restore_column_map(&altered_table->read_set, old_map);
    return false;
}

/*
  Commit or rollback the inplace ALTER.
  On commit       drop old index CFs, update share->idx_cfs for new table shape.
  On rollback     drop newly created CFs.
*/
bool ha_tidesdb::commit_inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info,
                                            bool commit)
{
    DBUG_ENTER("ha_tidesdb::commit_inplace_alter_table");

    ha_tidesdb_inplace_ctx *ctx = static_cast<ha_tidesdb_inplace_ctx *>(ha_alter_info->handler_ctx);

    ha_alter_info->group_commit_ctx = NULL;

    if (!ctx) DBUG_RETURN(false);

    /* We free the cached scan iterator before dropping CFs.  It may hold
       merge-heap references to SSTables in CFs about to be dropped. */
    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_cf_ = NULL;
        scan_iter_txn_ = NULL;
    }

    if (!commit)
    {
        /* Rollback, we drop any CFs we created for new indexes */
        for (const auto &cf_name : ctx->add_cf_names)
            tidesdb_drop_column_family(tdb_global, cf_name.c_str());
        DBUG_RETURN(false);
    }

    /* Commit, we drop CFs for removed indexes */
    for (const auto &cf_name : ctx->drop_cf_names)
    {
        int rc = tidesdb_drop_column_family(tdb_global, cf_name.c_str());
        if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
            sql_print_warning("[TIDESDB] commit ALTER: failed to drop CF '%s' (err=%d)",
                              cf_name.c_str(), rc);
    }

    /* We rebuild share->idx_cfs and idx_cf_names based on the new table's keys.
       Since we hold exclusive MDL, no other handler is using the share. */
    lock_shared_ha_data();
    commit_rebuild_index_meta(altered_table);

    /* If table options changed (COMPRESSION, BLOOM_FPR, TTL, etc.), we apply
       them to the live CF(s) so they take effect immediately instead of only
       being persisted in the .frm. */
    if (ha_alter_info->handler_flags & ALTER_CHANGE_CREATE_OPTION)
        commit_apply_runtime_config(altered_table, ha_alter_info);

    share->stats_refresh_us.store(0, std::memory_order_relaxed);
    unlock_shared_ha_data();

    DBUG_RETURN(false);
}

void ha_tidesdb::commit_rebuild_index_meta(TABLE *altered_table)
{
    share->idx_cfs.clear();
    share->idx_cf_names.clear();

    uint new_pk = altered_table->s->primary_key;
    for (uint i = 0; i < altered_table->s->keys; i++)
    {
        if (new_pk != MAX_KEY && i == new_pk)
        {
            share->idx_cfs.push_back(NULL);
            share->idx_cf_names.push_back("");
            continue;
        }
        std::string idx_name;
        tidesdb_column_family_t *icf = resolve_idx_cf(
            tdb_global, share->cf_name, altered_table->key_info[i].name.str, idx_name);
        share->idx_cfs.push_back(icf);
        share->idx_cf_names.push_back(idx_name);
    }

    for (uint i = 0; i < altered_table->s->keys; i++)
    {
        share->idx_comp_key_len[i] = comparable_key_length(&altered_table->key_info[i]);
        share->idx_is_fts[i] = is_fts_index(&altered_table->key_info[i]);
        share->idx_is_spatial[i] = is_spatial_index(&altered_table->key_info[i]);
    }

    share->idx_cover.assign(altered_table->s->keys,
                            std::vector<bool>(altered_table->s->fields, false));
    for (uint i = 0; i < altered_table->s->keys; i++)
    {
        const KEY *ki = &altered_table->key_info[i];
        for (uint p = 0; p < ki->user_defined_key_parts; p++)
        {
            uint fnr = ki->key_part[p].fieldnr;
            if (fnr > 0 && fnr - 1 < altered_table->s->fields) share->idx_cover[i][fnr - 1] = true;
        }
        if (altered_table->s->primary_key != MAX_KEY && i != altered_table->s->primary_key)
        {
            const KEY *pk_key = &altered_table->key_info[altered_table->s->primary_key];
            for (uint p = 0; p < pk_key->user_defined_key_parts; p++)
            {
                uint fnr = pk_key->key_part[p].fieldnr;
                if (fnr > 0 && fnr - 1 < altered_table->s->fields)
                    share->idx_cover[i][fnr - 1] = true;
            }
        }
    }

    share->num_secondary_indexes = 0;
    for (uint i = 0; i < share->idx_cfs.size(); i++)
        if (share->idx_cfs[i]) share->num_secondary_indexes++;
}

void ha_tidesdb::commit_apply_runtime_config(TABLE *altered_table,
                                             Alter_inplace_info *ha_alter_info)
{
    /* ALTER TABLE ... AUTO_INCREMENT=N raises the counter to N (never lowers it below the current
       maximum) and persists it so the raised value survives a restart. */
    if (ha_alter_info->create_info && ha_alter_info->create_info->auto_increment_value > 1)
    {
        ulonglong n = ha_alter_info->create_info->auto_increment_value;
        ulonglong cur = share->auto_inc_val.load(std::memory_order_relaxed);
        if (n - 1 > cur) share->auto_inc_val.store(n - 1, std::memory_order_relaxed);
        write_auto_inc_meta(share->cf, n);
    }

    const ha_table_option_struct *a_opts = TDB_TABLE_OPTIONS(altered_table);
    tidesdb_column_family_config_t cfg = build_cf_config(a_opts);
    tidesdb_column_family_config_t data_cfg = data_cf_config(cfg, a_opts && a_opts->encrypted);

    /* Main data CF */
    if (share->cf)
    {
        int rc = tidesdb_cf_update_runtime_config(tdb_global, share->cf, &data_cfg, 1);
        if (rc != TDB_SUCCESS)
            sql_print_warning(
                "[TIDESDB] ALTER: failed to update runtime config for "
                "data CF '%s' (err=%d)",
                share->cf_name.c_str(), rc);
    }

    for (uint i = 0; i < share->idx_cfs.size(); i++)
    {
        if (share->idx_cfs[i])
        {
            tidesdb_column_family_config_t idx_cfg = cfg;

            int rc = tidesdb_cf_update_runtime_config(tdb_global, share->idx_cfs[i], &idx_cfg, 1);
            if (rc != TDB_SUCCESS)
                sql_print_warning(
                    "[TIDESDB] ALTER: failed to update runtime config for "
                    "index CF '%s' (err=%d)",
                    share->idx_cf_names[i].c_str(), rc);
        }
    }

    if (TDB_TABLE_OPTIONS(altered_table))
    {
        uint iso_idx = TDB_TABLE_OPTIONS(altered_table)->isolation_level;
        if (iso_idx < array_elements(tdb_isolation_map))
            share->isolation_level = (tidesdb_isolation_level_t)tdb_isolation_map[iso_idx];
        share->default_ttl = TDB_TABLE_OPTIONS(altered_table)->ttl;
        share->has_ttl = (share->default_ttl > 0 || share->ttl_field_idx >= 0);
        share->encrypted = TDB_TABLE_OPTIONS(altered_table)->encrypted;
        if (share->encrypted)
            share->encryption_key_id = (uint)TDB_TABLE_OPTIONS(altered_table)->encryption_key_id;
    }
}

/*
  Tell MariaDB whether changing table options requires a rebuild.
  For TidesDB, changing options like COMPRESSION, TTL, etc. is always
  compatible -- the .frm is rewritten and re-read on next open().
*/
bool ha_tidesdb::check_if_incompatible_data(HA_CREATE_INFO *create_info, uint table_changes)
{
    /* If only table options changed (not column types), data is compatible */
    if (table_changes == IS_EQUAL_YES) return COMPATIBLE_DATA_YES;
    return COMPATIBLE_DATA_NO;
}
