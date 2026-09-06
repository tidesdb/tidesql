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

/* the table-open and create path: the constructor wires per-handler state, create builds the data
   and secondary-index column families for a new table, open resolves an existing table's families
   and cached key geometry into the shared TidesDB_share, and recover_counters rebuilds the
   auto-increment and row-count state from the stored rows.  the column-family configuration these
   build from comes from the shared config builders. */

#include "ha_tidesdb.h"

#include <ft_global.h>
#include <mysql/plugin.h>

#include <string>
#include <vector>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/engine/ha_tidesdb_config.h"
#include "src/handler/ha_tidesdb_fts.h"
#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_keycodec.h"
#include "src/handler/ha_tidesdb_spatial.h"
ha_tidesdb::ha_tidesdb(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg),
      share(NULL),
      stmt_txn(NULL),
      stmt_txn_dirty(false),
      scan_txn(NULL),
      scan_iter(NULL),
      scan_cf_(NULL),
      scan_iter_cf_(NULL),
      scan_iter_txn_(NULL),
      scan_iter_txn_gen_(0),
      idx_pk_exact_done_(false),
      scan_dir_(DIR_NONE),
      current_pk_len_(0),
      idx_search_comp_len_(0),
      cached_enc_key_ver_(0),
      enc_key_ver_valid_(false),
      cached_time_(0),
      cached_time_valid_(false),
      cached_sess_ttl_(0),
      cached_skip_unique_(false),
      cached_single_delete_primary_(false),
      cached_thdvars_valid_(false),
      is_pk_(false),
      scan_iter_last_err_(0),
      scan_iter_last_err_cf_(NULL),
      scan_iter_last_err_txn_(NULL),
      has_blobs_(false),
      encrypted_(false),
      record1_lo_(NULL),
      record1_hi_(NULL),
      cached_sql_cmd_(0),
      cached_is_autocommit_(false),
      cached_stmt_shape_valid_(false),
      cached_thd_(NULL),
      cached_trx_(NULL),
      in_bulk_insert_(false),
      in_bulk_update_(false),
      in_bulk_delete_(false),
      bulk_insert_ops_(0),
      cached_compact_after_range_delete_min_rows_(0),
      bulk_delete_rows_(0),
      mrr_custom_active_(false),
      mrr_no_assoc_(false),
      mrr_keyno_(MAX_KEY),
      mrr_next_idx_(0),
      keyread_only_(false),
      write_can_replace_(false)
{
}

/* ******************** get_share ******************** */

TidesDB_share *ha_tidesdb::get_share()
{
    TidesDB_share *tmp_share;
    DBUG_ENTER("ha_tidesdb::get_share");

    lock_shared_ha_data();
    if (!(tmp_share = static_cast<TidesDB_share *>(get_ha_share_ptr())))
    {
        tmp_share = new TidesDB_share;
        if (!tmp_share) goto err;
        set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
    }
err:
    unlock_shared_ha_data();
    DBUG_RETURN(tmp_share);
}

/* ******************** Counter recovery ******************** */

/*
  Recover hidden-PK next_row_id from the last data key.
  Also seed auto_inc_val for tables with AUTO_INCREMENT user-defined PKs
  so that get_auto_increment() can return O(1) instead of doing index_last()
  on every INSERT.
*/
void ha_tidesdb::recover_counters()
{
    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return;

    tidesdb_iter_t *iter = NULL;
    if (tdb_iter_new_blocking(ha_thd(), txn, share->cf, &iter) == TDB_SUCCESS)
    {
        tidesdb_iter_seek_to_last(iter);
        if (tidesdb_iter_valid(iter))
        {
            uint8_t *key = NULL;
            size_t key_size = 0;
            tdb_owned_buf key_g(key);
            if (tidesdb_iter_key(iter, &key, &key_size) == TDB_SUCCESS &&
                is_data_key(key, key_size))
            {
                if (!share->has_user_pk && key_size == KEY_NAMESPACE_LEN + HIDDEN_PK_SIZE)
                {
                    /* Hidden PK -- we decode the big-endian row-id */
                    uint64_t max_id = decode_be64(key + KEY_NAMESPACE_LEN);
                    share->next_row_id.store(max_id + 1, std::memory_order_relaxed);
                }

                /* Seeding auto_inc_val from the last row in primary-key order
                   is only correct when the AUTO_INCREMENT column is the
                   leftmost part of the primary key, since only then does the
                   PK-order maximum coincide with the auto-inc maximum.  When
                   the auto-inc column is the first part of some other index
                   instead, recover the maximum from that index's last entry
                   (recover_auto_inc_secondary below); otherwise the counter
                   would restart low after a restart and get_auto_increment
                   would hand out colliding ids. */
                bool auto_inc_is_pk_leftmost = false;
                if (share->has_user_pk && table->found_next_number_field)
                {
                    const KEY *pk = &table->key_info[share->pk_index];
                    if (pk->user_defined_key_parts > 0 &&
                        pk->key_part[0].field == table->found_next_number_field)
                        auto_inc_is_pk_leftmost = true;
                }
                if (auto_inc_is_pk_leftmost)
                {
                    /* User PK with AUTO_INCREMENT -- we read the last row to seed
                       the in-memory counter from the max PK value. */
                    uint8_t *val = NULL;
                    size_t val_size = 0;
                    tdb_owned_buf val_g(val);
                    if (tidesdb_iter_value(iter, &val, &val_size) == TDB_SUCCESS)
                    {
                        /* We just unpack the packed row into record[1] using the proper
                           deserialize path so field offsets are correct even when
                           variable-length fields (CHAR/VARCHAR) precede the
                           AUTO_INCREMENT column. */
                        if (share->has_blobs || share->encrypted)
                        {
                            std::string row_data((const char *)val, val_size);
                            deserialize_row(table->record[1], row_data);
                        }
                        else
                        {
                            deserialize_row(table->record[1], (const uchar *)val, val_size);
                        }
                        /* deserialize_row writes into table->record[1];
                           val_int_offset wants the byte offset from record[0]
                           to record[1].  table->s->rec_buff_length is
                           normally equal but the API does not guarantee it,
                           so use the explicit subtraction the deserialize
                           path already relies on. */
                        ulonglong max_val = table->found_next_number_field->val_int_offset(
                            (uint)(table->record[1] - table->record[0]));
                        share->auto_inc_val.store(max_val, std::memory_order_relaxed);
                    }
                }
            }
        }
        tidesdb_iter_free(iter);
    }

    if (!share->has_user_pk && share->next_row_id.load(std::memory_order_relaxed) == 0)
        share->next_row_id.store(HIDDEN_PK_FIRST_ROW_ID, std::memory_order_relaxed);

    /* Non-leftmost-PK auto-increment recovers its max from the column's own index first, so the
       persisted start value below is compared against the true row maximum. */
    recover_auto_inc_secondary();
    apply_auto_inc_start_meta(txn);

    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
}

void ha_tidesdb::apply_auto_inc_start_meta(tidesdb_txn_t *txn)
{
    if (!table->found_next_number_field) return;

    uint8_t *mv = NULL;
    size_t mvlen = 0;
    if (tidesdb_txn_get(txn, share->cf, AUTOINC_META_KEY, AUTOINC_META_KEY_LEN, &mv, &mvlen) !=
            TDB_SUCCESS ||
        mvlen != AUTOINC_META_VALUE_LEN)
        return;

    uint64_t start = decode_be64(mv);
    ulonglong cur = share->auto_inc_val.load(std::memory_order_relaxed);
    if (start > 1 && start - 1 > cur)
        share->auto_inc_val.store(start - 1, std::memory_order_relaxed);
}

void ha_tidesdb::write_auto_inc_meta(tidesdb_column_family_t *cf, ulonglong next_value)
{
    if (!cf || next_value <= 1) return;

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return;

    uint8_t mv[AUTOINC_META_VALUE_LEN];
    encode_be64((uint64_t)next_value, mv);
    if (tidesdb_txn_put(txn, cf, AUTOINC_META_KEY, AUTOINC_META_KEY_LEN, mv, AUTOINC_META_VALUE_LEN,
                        TIDESDB_TTL_NONE) == TDB_SUCCESS)
        tidesdb_txn_commit(txn);
    else
        tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
}

void ha_tidesdb::recover_auto_inc_secondary()
{
    Field *ai = table->found_next_number_field;
    if (!share->has_user_pk || !ai) return;
    if (share->auto_inc_val.load(std::memory_order_relaxed) != 0) return;

    /* The leftmost-PK case is already seeded from the last primary-key row. */
    const KEY *pk = &table->key_info[share->pk_index];
    if (pk->user_defined_key_parts > 0 && pk->key_part[0].field == ai) return;

    /* Find the index whose first part is the auto-increment column; its last entry is the maximum.
     */
    uint ai_key = MAX_KEY;
    for (uint i = 0; i < table->s->keys; i++)
    {
        const KEY *k = &table->key_info[i];
        if (i != share->pk_index && k->user_defined_key_parts > 0 && k->key_part[0].field == ai)
        {
            ai_key = i;
            break;
        }
    }
    if (ai_key == MAX_KEY || ai_key >= share->idx_cfs.size() || !share->idx_cfs[ai_key]) return;

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return;
    tidesdb_iter_t *iter = NULL;
    if (tdb_iter_new_blocking(ha_thd(), txn, share->idx_cfs[ai_key], &iter) == TDB_SUCCESS)
    {
        tidesdb_iter_seek_to_last(iter);
        if (tidesdb_iter_valid(iter))
        {
            uint8_t *ik = NULL;
            size_t iks = 0;
            tdb_owned_buf ik_g(ik);
            /* auto-increment columns are NOT NULL, so the comparable key starts with the value's
               sort key directly -- no null-indicator prefix byte to skip. */
            uint len = ai->pack_length();
            if (tidesdb_iter_key(iter, &ik, &iks) == TDB_SUCCESS && iks >= len &&
                decode_sort_key_part(ik, len, ai, table->record[1]))
            {
                ulonglong max_val = ai->val_int_offset((uint)(table->record[1] - table->record[0]));
                share->auto_inc_val.store(max_val, std::memory_order_relaxed);
            }
        }
        tidesdb_iter_free(iter);
    }
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
}

/* ******************** open / close / create ******************** */

int ha_tidesdb::open_init_share_columns(const char *name)
{
    share->cf_name = path_to_cf_name(name);
    share->cf = tidesdb_get_column_family(tdb_global, share->cf_name.c_str());
    if (!share->cf)
    {
        sql_print_error("[TIDESDB] CF '%s' not found for table '%s'", share->cf_name.c_str(), name);
        return HA_ERR_NO_SUCH_TABLE;
    }

    if (table->s->primary_key != MAX_KEY)
    {
        share->has_user_pk = true;
        share->pk_index = table->s->primary_key;
        share->pk_key_len = comparable_key_length(&table->key_info[share->pk_index]);
    }
    else
    {
        share->has_user_pk = false;
        share->pk_index = MAX_KEY;
        share->pk_key_len = HIDDEN_PK_SIZE;
    }

    if (TDB_TABLE_OPTIONS(table))
    {
        uint iso_idx = TDB_TABLE_OPTIONS(table)->isolation_level;
        if (iso_idx < array_elements(tdb_isolation_map))
            share->isolation_level = (tidesdb_isolation_level_t)tdb_isolation_map[iso_idx];
    }

    if (TDB_TABLE_OPTIONS(table)) share->default_ttl = TDB_TABLE_OPTIONS(table)->ttl;

    share->encrypted = false;
    share->encryption_key_id = TIDESDB_DEFAULT_ENCRYPTION_KEY_ID;
    share->encryption_key_version = 0;
    if (TDB_TABLE_OPTIONS(table) && TDB_TABLE_OPTIONS(table)->encrypted)
    {
        share->encrypted = true;
        share->encryption_key_id = (uint)TDB_TABLE_OPTIONS(table)->encryption_key_id;
        uint ver = encryption_key_get_latest_version(share->encryption_key_id);
        if (ver == ENCRYPTION_KEY_VERSION_INVALID)
        {
            sql_print_error("[TIDESDB] encryption key %u not available", share->encryption_key_id);
            return HA_ERR_NO_SUCH_TABLE;
        }
        share->encryption_key_version = ver;
    }

    share->ttl_field_idx = TIDESDB_TTL_FIELD_NONE;
    for (uint i = 0; i < table->s->fields; i++)
    {
        if (table->s->field[i]->option_struct && table->s->field[i]->option_struct->ttl)
        {
            share->ttl_field_idx = (int)i;
            break;
        }
    }

    /* We cache table shape flags for hot-path short-circuiting.  We also
       capture the BLOB field indices so serialize_row's size estimate can
       iterate that short list instead of every field on every INSERT. */
    share->has_blobs = false;
    share->blob_field_indices.clear();
    for (uint i = 0; i < table->s->fields; i++)
    {
        if (table->s->field[i]->flags & BLOB_FLAG)
        {
            share->has_blobs = true;
            share->blob_field_indices.push_back((uint16)i);
        }
    }
    share->has_ttl = (share->default_ttl > 0 || share->ttl_field_idx >= 0);

    /* Load this table's foreign keys from the engine catalog into the share once,
       resolving the referencing columns on the child side and the referenced
       columns on the parent side against this table definition. */
    fk_load();

    return 0;
}

void ha_tidesdb::open_build_field_plan()
{
    /* Per-field serialize/deserialize plan.  For each field cache its
       offset within record[0] and whether its pack format is a pure
       memcpy of pack_length() bytes -- if so the hot loops skip the
       Field::pack/unpack vtable dispatch entirely.

       The whitelist below covers the field types whose pack() output
       is byte-identical to memcpy(pack_length()) on a little-endian
       host.  CHAR / VARCHAR / BLOB / GEOMETRY / JSON / BIT / DECIMAL
       keep the slow path, their pack() trims trailing pad bytes,
       emits a length prefix, or layers a null-bit on top.

       Field_long / Field_longlong / Field_short etc. emit data in
       on-disk little-endian via the mi_int*store macros which equal
       memcpy on x86_64.  TIDESDB_FAST_SERDES_LE_ONLY guards the fast
       path so a big-endian build cleanly falls back to Field::pack. */
    share->field_plan.clear();
    share->field_plan.reserve(table->s->fields);
    share->null_bytes_cached = (uint8)table->s->null_bytes;
    share->fields_cached = (uint16)table->s->fields;
    share->has_no_nullable = (table->s->null_bytes == 0);
    for (uint i = 0; i < table->s->fields; i++)
    {
        /* We use the per-instance Field (table->field[i]), not the share
           prototype (table->s->field[i]).  maybe_null() / real_type()
           read through Field::table -- the share prototype is created
           with a null table pointer and crashes on those calls.  The
           per-instance Field has table = this handler's TABLE and is
           safe to query. */
        Field *f = table->field[i];
        TidesDB_share::field_plan_t fp;
        fp.src_off = (uint32)(f->ptr - table->record[0]);
        fp.pack_len = (uint16)f->pack_length();
        fp.maybe_null = f->maybe_null();
        fp.memcpy_ok = false;
#ifndef WORDS_BIGENDIAN
        switch (f->real_type())
        {
            case MYSQL_TYPE_TINY:
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_INT24:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_LONGLONG:
            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_DOUBLE:
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_TIME2:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_DATETIME2:
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_TIMESTAMP2:
            case MYSQL_TYPE_YEAR:
            case MYSQL_TYPE_NEWDECIMAL:
                fp.memcpy_ok = true;
                break;
            default:
                fp.memcpy_ok = false;
                break;
        }
        /* BLOB columns share MYSQL_TYPE_LONGLONG underneath in older
           codepaths; never fast-path anything carrying BLOB_FLAG. */
        if (f->flags & BLOB_FLAG) fp.memcpy_ok = false;
#endif
        share->field_plan.push_back(fp);
    }
}

void ha_tidesdb::open_build_index_meta(const char *name)
{
    /* We precompute comparable key lengths and index-type flags per index.
       Caching the type flags avoids a ki->algorithm dereference per row
       in write_row's dup-check loop and in update_row/delete_row. */
    for (uint i = 0; i < table->s->keys; i++)
    {
        share->idx_comp_key_len[i] = comparable_key_length(&table->key_info[i]);
        share->idx_is_fts[i] = is_fts_index(&table->key_info[i]);
        share->idx_is_spatial[i] = is_spatial_index(&table->key_info[i]);
    }

    /* Precompute per-index coverage bitmaps so try_keyread_from_index is
       O(set bits in read_set) instead of nested scans over key parts. */
    share->idx_cover.assign(table->s->keys, std::vector<bool>(table->s->fields, false));
    for (uint i = 0; i < table->s->keys; i++)
    {
        const KEY *ki = &table->key_info[i];
        for (uint p = 0; p < ki->user_defined_key_parts; p++)
        {
            uint fnr = ki->key_part[p].fieldnr;
            if (fnr > 0 && fnr - 1 < table->s->fields) share->idx_cover[i][fnr - 1] = true;
        }
        /* Secondary indexes also cover the PK columns appended to the key. */
        if (table->s->primary_key != MAX_KEY && i != table->s->primary_key)
        {
            const KEY *pk_key = &table->key_info[table->s->primary_key];
            for (uint p = 0; p < pk_key->user_defined_key_parts; p++)
            {
                uint fnr = pk_key->key_part[p].fieldnr;
                if (fnr > 0 && fnr - 1 < table->s->fields) share->idx_cover[i][fnr - 1] = true;
            }
        }
    }

    for (uint i = 0; i < table->s->keys; i++)
    {
        if (share->has_user_pk && i == share->pk_index)
        {
            share->idx_cfs.push_back(NULL);
            share->idx_cf_names.push_back("");
            continue;
        }
        std::string idx_name;
        tidesdb_column_family_t *icf =
            resolve_idx_cf(tdb_global, share->cf_name, table->key_info[i].name.str, idx_name);
        share->idx_cfs.push_back(icf);
        share->idx_cf_names.push_back(idx_name);
    }

    share->num_secondary_indexes = 0;
    for (uint i = 0; i < share->idx_cfs.size(); i++)
        if (share->idx_cfs[i]) share->num_secondary_indexes++;

    /* We recover the hidden-PK counter and seed auto_inc_val at open from the last
       stored row, or from the auto-increment column's own index when it is not the
       leftmost primary-key part. */
    recover_counters();

    char frm_path[FN_REFLEN];
    fn_format(frm_path, name, "", reg_ext, MY_UNPACK_FILENAME | MY_APPEND_EXT);
    MY_STAT st_buf;
    if (mysql_file_stat(0, frm_path, &st_buf, MYF(0))) share->create_time = st_buf.st_mtime;
}

int ha_tidesdb::open(const char *name, int mode, uint test_if_locked)
{
    DBUG_ENTER("ha_tidesdb::open");

    if (!(share = get_share())) DBUG_RETURN(1);

    /*
      We resolve CF pointers only once (first open).  Subsequent opens by
      other connections reuse the already-resolved share.  We hold
      lock_shared_ha_data() to prevent concurrent open() calls from
      racing on the shared vectors.
    */
    lock_shared_ha_data();
    if (!share->cf)
    {
        int rc = open_init_share_columns(name);
        if (rc)
        {
            unlock_shared_ha_data();
            DBUG_RETURN(rc);
        }
        open_build_field_plan();
        open_build_index_meta(name);
    }
    unlock_shared_ha_data();

    ref_length = share->pk_key_len;

    /* We mirror shape flags onto the handler so the row-fetch hot paths
       read from a local member instead of chasing `share` into shared
       memory on every row.  These mirror constants that never change for
       the open handler. */
    has_blobs_ = share->has_blobs;
    encrypted_ = share->encrypted;

    /* We precompute the record[1] pointer range so the BLOB path of
       fetch_row_by_pk/iter_read_current doesn't rebuild it per row. */
    if (table->record[1])
    {
        record1_lo_ = table->record[1];
        record1_hi_ = table->record[1] + table->s->reclength;
    }
    else
    {
        record1_lo_ = NULL;
        record1_hi_ = NULL;
    }

    /* Populate the row-count estimate and per-index rec_per_key now, at open, so the very first
       query planned against this table sees real selectivity.  The server does not call
       info(HA_STATUS_CONST) until later, a SHOW INDEX or ANALYZE, and until then rec_per_key sits
       at the default the optimizer reads as one row per key value, which mis-drives a join on a
       composite key whose leading part is low-cardinality into a full-scan BNL instead of an
       eq_ref. */
    info(HA_STATUS_VARIABLE | HA_STATUS_CONST);

    /* That priming pass stamped the refresh throttle with this open-time snapshot.  A table opened
       while still empty and then populated, as a freshly created table is, would otherwise keep
       serving the empty snapshot until the throttle window elapsed, so clear the stamp and let the
       first real query after rows land recompute against the actual contents. */
    share->stats_refresh_us.store(0, std::memory_order_relaxed);

    DBUG_RETURN(0);
}

int ha_tidesdb::close(void)
{
    DBUG_ENTER("ha_tidesdb::close");
    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_cf_ = NULL;
        scan_iter_txn_ = NULL;
    }
    /* stmt_txn is a borrowed pointer into the per-connection trx->txn.
       We do not free it here -- the txn is owned by the per-connection trx
       and will be freed in tidesdb_close_connection(). */
    stmt_txn = NULL;
    stmt_txn_dirty = false;
    DBUG_RETURN(0);
}

int ha_tidesdb::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
    DBUG_ENTER("ha_tidesdb::create");

    std::string cf_name = path_to_cf_name(name);

    ha_table_option_struct *opts = TDB_TABLE_OPTIONS(table_arg);
    DBUG_ASSERT(opts);

    tidesdb_column_family_config_t cfg = build_cf_config(opts);
    tidesdb_column_family_config_t data_cfg = data_cf_config(cfg, opts && opts->encrypted);

    /* We create main data CF (we simply skip if it already exists, e.g. crash recovery) */
    if (!tidesdb_get_column_family(tdb_global, cf_name.c_str()))
    {
        int rc = tidesdb_create_column_family(tdb_global, cf_name.c_str(), &data_cfg);
        if (rc != TDB_SUCCESS)
        {
            sql_print_error("[TIDESDB] Failed to create CF '%s' (err=%d)", cf_name.c_str(), rc);
            DBUG_RETURN(tdb_rc_to_ha(rc, "create main_cf"));
        }
    }

    /* Create a column family for each secondary index, sharing the table's config. */
    for (uint i = 0; i < table_arg->s->keys; i++)
    {
        if (table_arg->s->primary_key != MAX_KEY && i == table_arg->s->primary_key) continue;

        std::string idx_cf = cf_name + CF_INDEX_INFIX + table_arg->key_info[i].name.str;
        if (!tidesdb_get_column_family(tdb_global, idx_cf.c_str()))
        {
            tidesdb_column_family_config_t idx_cfg = cfg;

            int rc = tidesdb_create_column_family(tdb_global, idx_cf.c_str(), &idx_cfg);
            if (rc != TDB_SUCCESS)
            {
                sql_print_error("[TIDESDB] Failed to create index CF '%s' (err=%d)", idx_cf.c_str(),
                                rc);
                DBUG_RETURN(tdb_rc_to_ha(rc, "create idx_cf"));
            }
        }
    }

    /* Persist a CREATE TABLE ... AUTO_INCREMENT=N start value so it survives a restart when the
       table is opened empty (nothing to recover the counter from). */
    if (create_info && create_info->auto_increment_value > 1)
        write_auto_inc_meta(tidesdb_get_column_family(tdb_global, cf_name.c_str()),
                            create_info->auto_increment_value);

    /* Record any foreign keys declared on this table in the engine catalog so
       both sides load them at open and the row operations can enforce them. */
    if (int frc = fk_persist_defs(name, table_arg, create_info)) DBUG_RETURN(frc);

    DBUG_RETURN(0);
}
