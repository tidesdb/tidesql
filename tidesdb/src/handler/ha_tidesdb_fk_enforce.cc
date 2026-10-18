/*
  Foreign-key enforcement for the TidesDB storage engine.

  The catalog side, which persists a table's constraints at create time, loads them at open, and
  describes them to the server, lives in ha_tidesdb_fk.cc.  This file is the row-operation side.
  It refuses a child row whose referenced parent key is absent, and on a delete or update of a
  parent row it applies the constraint's action to the matching child rows, one of RESTRICT,
  NO ACTION, CASCADE, or SET NULL, through a handler it opens on the child table itself.
*/

#include "ha_tidesdb.h"

#include <cstring>
#include <string>
#include <vector>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/handler/ha_tidesdb_internal.h"

/* Whether any key of cf begins with prefix, read through txn.  Returns 1 when one does, 0 when
   none does, or the negative library code when the probe itself failed, which the caller must not
   read as "no row", since that would let an orphan in or a referenced parent go. */
static int fk_prefix_present(THD *thd, tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                             const uchar *prefix, uint prefix_len)
{
    std::string hi((const char *)prefix, prefix_len);
    hi.push_back((char)0xff);
    tidesdb_iter_t *it = NULL;
    int rc = tdb_iter_new_range_blocking(thd, txn, cf, prefix, prefix_len,
                                         (const uint8_t *)hi.data(), hi.size(), &it);
    if (rc != TDB_SUCCESS) return rc;
    if (!it) return TDB_ERR_MEMORY;
    rc = tdb_iter_seek_blocking(thd, it, prefix, prefix_len);
    int found = 0;
    while (!tdb_iter_failed(rc) && tidesdb_iter_valid(it))
    {
        uint8_t *k = NULL;
        size_t ks = 0;
        rc = tidesdb_iter_key(it, &k, &ks);
        if (rc != TDB_SUCCESS)
        {
            if (rc == TDB_ERR_NOT_FOUND) rc = TDB_ERR_IO;
            break;
        }
        const bool match = ks >= prefix_len && memcmp(k, prefix, prefix_len) == 0;
        tidesdb_free(k);
        if (match)
        {
            found = 1;
            break;
        }
        rc = tidesdb_iter_next(it);
    }
    tidesdb_iter_free(it);
    if (tdb_iter_failed(rc)) return rc;
    return found;
}

int ha_tidesdb::fk_check_child(const uchar *new_row)
{
    if (!share || fks().child.empty()) return 0;
    /* A cascade writing this row already holds a valid parent value, so skip the
       existence probe, which would otherwise race the parent's own row update. */
    if (fk_in_cascade_) return 0;
    THD *thd = cached_thd_ ? cached_thd_ : ha_thd();
    if (tdb_fk_checks_off(thd)) return 0;
    tidesdb_txn_t *txn = stmt_txn;
    if (!txn) return 0;

    for (const auto &d : fks().child)
    {
        if (d.child_key_no < 0) continue;
        KEY *ki = &table->key_info[d.child_key_no];
        uint nparts = (uint)d.child_fields.size();

        /* MATCH SIMPLE, skip the check when any referencing column is null */
        bool any_null = false;
        for (uint16 fi : d.child_fields)
            if (table->field[fi]->is_null_in_record(new_row))
            {
                any_null = true;
                break;
            }
        if (any_null) continue;

        /* Encode value-only so the probe matches the non-nullable parent key. */
        uchar comp[MAX_KEY_LENGTH];
        uint comp_len = make_comparable_key(ki, new_row, nparts, comp, true);

        /* hold the parent shared before reading it, so a concurrent delete of it is refused, and
           under galera certify the reference so the same race between nodes is refused too */
        if (int g = tdb_fk_guard_claim(cached_thd_, cached_trx_,
                                       tdb_fk_guard_key(d, comp, comp_len), false))
            return g;
#ifdef WITH_WSREP
        if (int w = wsrep_certify_parent_ref(d, comp, comp_len)) return w;
#endif

        bool present = false;
        if (d.parent_is_pk)
        {
            /* The parent stores its rows in the data family keyed by the data key
               of the comparable primary key, so a point probe answers existence. */
            tidesdb_column_family_t *pcf =
                tidesdb_get_column_family(tdb_global, d.parent_cf.c_str());
            if (!pcf) continue;
            uchar dk[DATA_KEY_BUF_LEN];
            uint dk_len = build_data_key(comp, comp_len, dk);
            int rc = tdb_txn_contains_blocking(thd, txn, pcf, dk, dk_len);
            if (rc == TDB_SUCCESS)
                present = true;
            else if (rc != TDB_ERR_NOT_FOUND)
                return tdb_rc_to_ha(rc, "fk_check_child");
        }
        else
        {
            /* The parent stores a unique index entry keyed by the comparable
               index columns followed by its primary key, so we prefix-scan the
               parent index family for an entry that begins with the value. */
            std::string picf = d.parent_cf + CF_INDEX_INFIX + d.parent_index_name;
            tidesdb_column_family_t *pcf = tidesdb_get_column_family(tdb_global, picf.c_str());
            if (!pcf) continue;
            int prc = fk_prefix_present(thd, txn, pcf, comp, comp_len);
            if (prc < 0) return tdb_rc_to_ha(prc, "fk_check_child");
            present = prc > 0;
        }

        if (!present)
        {
            my_error(ER_NO_REFERENCED_ROW_2, MYF(0), d.name.c_str());
            return HA_ERR_NO_REFERENCED_ROW;
        }
    }
    return 0;
}

/* Encode the referenced values from a parent row the way the child index stored
   them, so a range scan of that index finds the referencing children.  The child
   index carries a null indicator only for a nullable child column, so we prepend
   one exactly where child_nullable says to, then the value bytes.  Types match by
   the foreign-key definition, so the parent field's sort_string yields the same
   value bytes the child wrote.  Descending and binary-varstring key parts are not
   handled here yet, matching this increment's primary-key referenced scope. */
static uint fk_encode_child_prefix(TABLE *table, KEY *key, uint nparts, const uchar *row,
                                   const std::vector<uint8> &child_nullable, uchar *out)
{
    uint pos = 0;
    my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(row - table->record[0]);
    for (uint p = 0; p < nparts && p < key->user_defined_key_parts; p++)
    {
        KEY_PART_INFO *kp = &key->key_part[p];
        Field *field = kp->field;
        if (p < child_nullable.size() && child_nullable[p]) out[pos++] = SORT_KEY_NOT_NULL;
        field->move_field_offset(ptrdiff);
        field->sort_string(out + pos, kp->length);
        field->move_field_offset(-ptrdiff);
        pos += kp->length;
    }
    return pos;
}

/* Does any child row reference the key built from old_row for constraint d?
   Returns 1 referenced, 0 none, or a negative handler error. */
int ha_tidesdb::fk_child_ref_exists(const tdb_fk_def &d, const uchar *old_row)
{
    tidesdb_txn_t *txn = stmt_txn;
    if (!txn || d.parent_key_no < 0) return 0;
    KEY *ki = &table->key_info[d.parent_key_no];
    uint nparts = (uint)d.parent_fields.size();

    uchar comp[MAX_KEY_LENGTH];
    uint comp_len = fk_encode_child_prefix(table, ki, nparts, old_row, d.child_nullable, comp);

    std::string idx_cf_name = d.child_cf;
    idx_cf_name += CF_INDEX_INFIX;
    idx_cf_name += d.child_index_name;
    tidesdb_column_family_t *icf = tidesdb_get_column_family(tdb_global, idx_cf_name.c_str());
    if (!icf) return 0;

    THD *thd = cached_thd_ ? cached_thd_ : ha_thd();
    int prc = fk_prefix_present(thd, txn, icf, comp, comp_len);
    if (prc < 0) return -tdb_rc_to_ha(prc, "fk_child_ref_exists");
    return prc;
}

/* Find an already-open table by database and name in this statement's global
   table list, reached from the handler's own table.  Foreign-key prelocking
   opens the referencing children for a parent DML, appending them after the
   parent, so a forward walk from the parent finds them.  Returns NULL when the
   child is not open, and the caller then falls back to the restrict behaviour. */
static TABLE *fk_find_open_table(TABLE *self, const std::string &db, const std::string &tbl)
{
    if (!self || !self->pos_in_table_list) return NULL;
    for (TABLE_LIST *tl = self->pos_in_table_list; tl; tl = tl->next_global)
    {
        TABLE *t = tl->table;
        if (!t || !t->s) continue;
        if (t->s->db.str && t->s->table_name.str && db.size() == t->s->db.length &&
            tbl.size() == t->s->table_name.length &&
            memcmp(db.data(), t->s->db.str, db.size()) == 0 &&
            memcmp(tbl.data(), t->s->table_name.str, tbl.size()) == 0)
            return t;
    }
    return NULL;
}

handler *ha_tidesdb::fk_self_handler(THD *thd)
{
    if (!fk_self_h_)
    {
        if (!fk_self_root_inited_)
        {
            init_alloc_root(PSI_NOT_INSTRUMENTED, &fk_self_root_, 1024, 0, MYF(0));
            fk_self_root_inited_ = true;
        }
        fk_self_h_ = clone(table->s->normalized_path.str, &fk_self_root_);
        if (!fk_self_h_) return NULL;
    }
    if (!fk_self_locked_)
    {
        if (fk_self_h_->ha_external_lock(thd, F_WRLCK)) return NULL;
        fk_self_locked_ = true;
    }
    return fk_self_h_;
}

void ha_tidesdb::fk_self_handler_unlock(THD *thd)
{
    if (fk_self_h_ && fk_self_locked_)
    {
        fk_self_h_->ha_external_lock(thd, F_UNLCK);
        fk_self_locked_ = false;
    }
}

void ha_tidesdb::fk_self_handler_close()
{
    if (fk_self_h_)
    {
        fk_self_handler_unlock(ha_thd());
        fk_self_h_->ha_close();
        delete fk_self_h_;
        fk_self_h_ = NULL;
    }
    if (fk_self_root_inited_)
    {
        free_root(&fk_self_root_, MYF(0));
        fk_self_root_inited_ = false;
    }
}

int ha_tidesdb::fk_cascade_children(const tdb_fk_def &d, const uchar *old_row, const uchar *new_row)
{
    const bool is_update = (new_row != NULL);
    const enum_fk_option act = (enum_fk_option)(is_update ? d.on_update : d.on_delete);
    const bool set_null = (act == FK_OPTION_SET_NULL);

#ifdef WITH_WSREP
    /* A cascade's child rows depend on the parent row in ways their own keys do not order, so the
       write set is applied on its own rather than in parallel with others, as InnoDB marks it. */
    {
        THD *thd = cached_thd_ ? cached_thd_ : ha_thd();
        if (wsrep_on(thd) && wsrep_thd_is_local(thd))
        {
            /* the server's wsrep_thd_set_PA_unsafe macro names a service member that does not
               exist, so a dynamic plugin calls the member the service really has */
#ifdef MYSQL_DYNAMIC_PLUGIN
            wsrep_service->wsrep_thd_set_wsrep_PA_unsafe_func(thd);
#else
            wsrep_thd_set_PA_unsafe(thd);
#endif
        }
    }
#endif

    TABLE *ct = fk_find_open_table(table, d.child_db, d.child_table);
    if (!ct || !ct->file)
    {
        /* Cannot reach the child to cascade, so fall back to restrict rather than
           risk leaving an orphan.  This should not happen while prelocking is in
           effect, but it keeps the store consistent if it ever does. */
        int r = fk_child_ref_exists(d, old_row);
        if (r < 0) return -r;
        if (r > 0)
        {
            my_error(ER_ROW_IS_REFERENCED_2, MYF(0), d.name.c_str());
            return HA_ERR_ROW_IS_REFERENCED;
        }
        return 0;
    }

    /* Locate the child index that carries the foreign-key columns. */
    int cidx = -1;
    for (uint i = 0; i < ct->s->keys; i++)
        if (ct->key_info[i].name.str && d.child_index_name == ct->key_info[i].name.str)
        {
            cidx = (int)i;
            break;
        }
    if (cidx < 0) return 0;
    KEY *ckey = &ct->key_info[cidx];
    uint nparts = (uint)d.parent_fields.size();
    if (nparts > ckey->user_defined_key_parts) nparts = ckey->user_defined_key_parts;

    if (fk_cascade_depth_ >= TDB_FK_MAX_CASCADE_DEPTH)
    {
        my_error(ER_FK_DEPTH_EXCEEDED, MYF(0), (int)TDB_FK_MAX_CASCADE_DEPTH);
        return HA_ERR_FK_DEPTH_EXCEEDED;
    }

    /* A constraint on its own table finds this very table as the child.  Its rows are then driven
       through a second handler, since this one is part way through the parent row's own write,
       and the record buffers the cascade has to work in are this table's own, which hold the
       parent row the caller is about to finish writing.  They are saved here and put back after,
       and the parent's old and new images are read from the saved copies. */
    const bool self_ref = (ct == table);
    handler *ch = ct->file;
    std::string saved0, saved1;
    if (self_ref)
    {
        THD *thd = cached_thd_ ? cached_thd_ : ha_thd();
        ch = fk_self_handler(thd);
        if (!ch) return HA_ERR_INTERNAL_ERROR;
        const size_t rl = table->s->rec_buff_length;
        saved0.assign((const char *)table->record[0], rl);
        saved1.assign((const char *)table->record[1], rl);
        if (old_row == table->record[0])
            old_row = (const uchar *)saved0.data();
        else if (old_row == table->record[1])
            old_row = (const uchar *)saved1.data();
        if (new_row == table->record[0])
            new_row = (const uchar *)saved0.data();
        else if (new_row == table->record[1])
            new_row = (const uchar *)saved1.data();
    }
    auto restore_parent_records = [&]()
    {
        if (!self_ref) return;
        memcpy(table->record[0], saved0.data(), saved0.size());
        memcpy(table->record[1], saved1.data(), saved1.size());
    };

    /* We read and write every child column during the cascade, so widen the
       child's column maps for the duration and restore them after. */
    MY_BITMAP *old_r = tmp_use_all_columns(ct, &ct->read_set);
    MY_BITMAP *old_w = tmp_use_all_columns(ct, &ct->write_set);

    /* Set the child key columns from the parent's old referenced values so the
       index read lands on the referencing rows.  Types match by the constraint,
       so a raw copy of the field bytes reproduces the stored value. */
    my_ptrdiff_t opd = (my_ptrdiff_t)(old_row - table->record[0]);
    uint key_len = 0;
    for (uint i = 0; i < nparts; i++)
    {
        Field *pf = table->field[d.parent_fields[i]];
        Field *cf = ckey->key_part[i].field;
        pf->move_field_offset(opd);
        cf->set_notnull();
        field_conv(cf, pf);
        pf->move_field_offset(-opd);
        key_len += ckey->key_part[i].store_length;
    }

    uchar keybuf[MAX_KEY_LENGTH];
    key_copy(keybuf, ct->record[0], ckey, key_len);

    /* Collect the referencing children's positions first, then act on them, so a
       delete or update does not disturb the index walk mid-scan. */
    std::vector<std::string> refs;
    int rc = ch->ha_index_init((uint)cidx, true);
    if (rc == 0)
    {
        rc = ch->ha_index_read_map(ct->record[0], keybuf, make_prev_keypart_map(nparts),
                                   HA_READ_KEY_EXACT);
        while (rc == 0)
        {
            ch->position(ct->record[0]);
            refs.emplace_back((const char *)ch->ref, ch->ref_length);
            rc = ch->ha_index_next_same(ct->record[0], keybuf, key_len);
        }
        ch->ha_index_end();
    }
    if (rc != 0 && rc != HA_ERR_END_OF_FILE && rc != HA_ERR_KEY_NOT_FOUND)
    {
        tmp_restore_column_map(&ct->read_set, old_r);
        tmp_restore_column_map(&ct->write_set, old_w);
        restore_parent_records();
        return rc;
    }

    int result = 0;
    /* Suppress the child's parent-existence check while we rewrite its rows, and
       restore it after.  The cascade only ever writes a valid parent value. */
    ha_tidesdb *child_ha = (ch->ht == ht) ? static_cast<ha_tidesdb *>(ch) : NULL;
    bool prev_in_cascade = false;
    uint prev_depth = 0;
    if (child_ha)
    {
        prev_in_cascade = child_ha->fk_in_cascade_;
        prev_depth = child_ha->fk_cascade_depth_;
        child_ha->fk_in_cascade_ = true;
        child_ha->fk_cascade_depth_ = fk_cascade_depth_ + 1;
    }
    /* Every child the index walk found must be read back and rewritten, or the parent must not
       go.  A child already gone within this transaction, removed by an earlier cascade reaching it
       through another constraint, is the one read that may fail.  Any other failure, a conflict
       or an error from the store, is the statement's error, since skipping the child would leave
       it pointing at a parent that no longer exists. */
    if (!refs.empty() && (result = ch->ha_rnd_init(false)) == 0)
    {
        my_ptrdiff_t npd = is_update ? (my_ptrdiff_t)(new_row - table->record[0]) : 0;
        for (auto &r : refs)
        {
            int prc = ch->ha_rnd_pos(ct->record[0], (uchar *)r.data());
            if (prc == HA_ERR_KEY_NOT_FOUND || prc == HA_ERR_RECORD_DELETED) continue;
            if (prc)
            {
                result = prc;
                break;
            }

            if (!is_update && !set_null)
            {
                /* ON DELETE CASCADE removes the child, which recurses through the
                   child handler into its own foreign keys and indexes. */
                result = ch->ha_delete_row(ct->record[0]);
            }
            else
            {
                store_record(ct, record[1]);
                for (uint i = 0; i < nparts && !result; i++)
                {
                    Field *cf = ckey->key_part[i].field;
                    if (set_null)
                    {
                        /* A column that cannot hold NULL cannot be set to it.  Such a constraint
                           is refused when it is defined, so this only guards one defined before
                           that check, which must not leave the child pointing at a parent that
                           is gone. */
                        if (!cf->real_maybe_null())
                        {
                            my_error(ER_ROW_IS_REFERENCED_2, MYF(0), d.name.c_str());
                            result = HA_ERR_ROW_IS_REFERENCED;
                            break;
                        }
                        cf->set_null();
                    }
                    else
                    {
                        Field *pf = table->field[d.parent_fields[i]];
                        pf->move_field_offset(npd);
                        cf->set_notnull();
                        field_conv(cf, pf);
                        pf->move_field_offset(-npd);
                    }
                }
                if (!result) result = ch->ha_update_row(ct->record[1], ct->record[0]);
            }
            if (result) break;
        }
        ch->ha_rnd_end();
    }
    if (child_ha)
    {
        child_ha->fk_in_cascade_ = prev_in_cascade;
        child_ha->fk_cascade_depth_ = prev_depth;
    }

    tmp_restore_column_map(&ct->read_set, old_r);
    tmp_restore_column_map(&ct->write_set, old_w);
    restore_parent_records();
    return result;
}

int ha_tidesdb::fk_enforce_parent_delete(const uchar *old_row)
{
    if (!share || fks().parent.empty()) return 0;
    THD *thd = cached_thd_ ? cached_thd_ : ha_thd();
    if (tdb_fk_checks_off(thd)) return 0;
    if (!stmt_txn) return 0;

    for (const auto &d : fks().parent)
    {
        if (d.parent_key_no < 0) continue;
        if (int g = fk_guard_parent(d, old_row)) return g;
        enum_fk_option act = (enum_fk_option)d.on_delete;
        if (act == FK_OPTION_CASCADE || act == FK_OPTION_SET_NULL)
        {
            int rc = fk_cascade_children(d, old_row, NULL);
            if (rc) return rc;
        }
        else
        {
            int r = fk_child_ref_exists(d, old_row);
            if (r < 0) return -r;
            if (r > 0)
            {
                my_error(ER_ROW_IS_REFERENCED_2, MYF(0), d.name.c_str());
                return HA_ERR_ROW_IS_REFERENCED;
            }
        }
    }
    return 0;
}

int ha_tidesdb::fk_enforce_parent_update(const uchar *old_row, const uchar *new_row)
{
    if (!share || fks().parent.empty()) return 0;
    THD *thd = cached_thd_ ? cached_thd_ : ha_thd();
    if (tdb_fk_checks_off(thd)) return 0;
    if (!stmt_txn) return 0;

    /* Only the constraints whose referenced columns actually changed can be
       affected by an update, so we compare the old and new referenced prefixes
       and skip the constraint when they match. */
    for (const auto &d : fks().parent)
    {
        if (d.parent_key_no < 0) continue;
        KEY *ki = &table->key_info[d.parent_key_no];
        uint nparts = (uint)d.parent_fields.size();

        uchar oldp[MAX_KEY_LENGTH], newp[MAX_KEY_LENGTH];
        uint ol = fk_encode_child_prefix(table, ki, nparts, old_row, d.child_nullable, oldp);
        uint nl = fk_encode_child_prefix(table, ki, nparts, new_row, d.child_nullable, newp);
        if (ol == nl && memcmp(oldp, newp, ol) == 0) continue;
        if (int g = fk_guard_parent(d, old_row)) return g;

        enum_fk_option act = (enum_fk_option)d.on_update;
        if (act == FK_OPTION_CASCADE || act == FK_OPTION_SET_NULL)
        {
            int rc = fk_cascade_children(d, old_row, new_row);
            if (rc) return rc;
        }
        else
        {
            int r = fk_child_ref_exists(d, old_row);
            if (r < 0) return -r;
            if (r > 0)
            {
                my_error(ER_ROW_IS_REFERENCED_2, MYF(0), d.name.c_str());
                return HA_ERR_ROW_IS_REFERENCED;
            }
        }
    }
    return 0;
}
