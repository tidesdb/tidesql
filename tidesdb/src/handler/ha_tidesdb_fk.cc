/*
  Foreign-key support for the TidesDB storage engine.

  MariaDB parses a FOREIGN KEY clause, hands it to the engine once at create
  time, prelocks the child tables using the lists the engine reports back, and
  displays whatever the engine describes.  It never checks a constraint itself.
  So this engine keeps its own catalog of the constraints, loads it on both sides
  at open, and runs the referential checks inside its own row operations.

  It enforces the referential actions itself.  It rejects a child row whose referenced parent key is
  absent, and on a delete or update of a parent row it applies the constraint's action to the
  matching child rows, one of RESTRICT, NO ACTION, CASCADE, or SET NULL.  The referenced columns may
  be the parent primary key or a non-nullable unique key.
*/

#include "ha_tidesdb.h"

#include <cstring>
#include <string>
#include <vector>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "sql_table.h"
#include "src/handler/ha_tidesdb_internal.h"

/* The engine keeps every table's foreign keys in one internal column family.
   Each constraint is stored twice, once under a child-side key so the
   referencing table finds it at open, and once under a parent-side key so the
   referenced table finds it too. */
static constexpr const char FK_CATALOG_CF[] = "__tidesdb_fk_catalog";
static constexpr uint8_t FK_REC_CHILD = 'c';
static constexpr uint8_t FK_REC_PARENT = 'p';
static constexpr uint8_t FK_SER_VERSION = 1;

/* ---- small serialization helpers -------------------------------------- */

static void fk_put_str(std::string &out, const std::string &s)
{
    uint16 n = (uint16)s.size();
    out.push_back((char)(n & 0xff));
    out.push_back((char)((n >> 8) & 0xff));
    out.append(s);
}

static bool fk_get_str(const uint8_t *&p, const uint8_t *end, std::string &out)
{
    if (p + 2 > end) return false;
    uint16 n = (uint16)(p[0] | (p[1] << 8));
    p += 2;
    if (p + n > end) return false;
    out.assign((const char *)p, n);
    p += n;
    return true;
}

static void fk_put_names(std::string &out, const std::vector<std::string> &v)
{
    uint16 n = (uint16)v.size();
    out.push_back((char)(n & 0xff));
    out.push_back((char)((n >> 8) & 0xff));
    for (const auto &s : v) fk_put_str(out, s);
}

static bool fk_get_names(const uint8_t *&p, const uint8_t *end, std::vector<std::string> &v)
{
    if (p + 2 > end) return false;
    uint16 n = (uint16)(p[0] | (p[1] << 8));
    p += 2;
    for (uint16 i = 0; i < n; i++)
    {
        std::string s;
        if (!fk_get_str(p, end, s)) return false;
        v.push_back(std::move(s));
    }
    return true;
}

/* A constraint as it travels through the catalog, with the column names kept as
   text so each side can resolve them against its own table at open. */
struct fk_catalog_entry
{
    std::string name;
    std::string child_cf;
    std::string child_db;
    std::string child_table;
    std::vector<std::string> child_columns;
    std::vector<uint8> child_nullable;
    std::string ref_db;
    std::string ref_table;
    std::string parent_cf;
    std::vector<std::string> ref_columns;
    std::string child_index_name;
    std::string parent_index_name; /* parent key the fk references, resolved at create */
    uint8 parent_is_pk;            /* whether that key is the parent primary key      */
    uint8 on_delete;
    uint8 on_update;
};

static void fk_serialize(const fk_catalog_entry &e, std::string &out)
{
    out.clear();
    out.push_back((char)FK_SER_VERSION);
    fk_put_str(out, e.name);
    fk_put_str(out, e.child_cf);
    fk_put_str(out, e.child_db);
    fk_put_str(out, e.child_table);
    fk_put_names(out, e.child_columns);
    fk_put_str(out, e.ref_db);
    fk_put_str(out, e.ref_table);
    fk_put_str(out, e.parent_cf);
    fk_put_names(out, e.ref_columns);
    fk_put_str(out, e.child_index_name);
    fk_put_str(out, e.parent_index_name);
    out.push_back((char)e.parent_is_pk);
    out.push_back((char)e.on_delete);
    out.push_back((char)e.on_update);
    uint16 nn = (uint16)e.child_nullable.size();
    out.push_back((char)(nn & 0xff));
    out.push_back((char)((nn >> 8) & 0xff));
    for (uint8 b : e.child_nullable) out.push_back((char)b);
}

static bool fk_deserialize(const uint8_t *p, size_t len, fk_catalog_entry &e)
{
    const uint8_t *end = p + len;
    if (p >= end || *p != FK_SER_VERSION) return false;
    p++;
    if (!fk_get_str(p, end, e.name)) return false;
    if (!fk_get_str(p, end, e.child_cf)) return false;
    if (!fk_get_str(p, end, e.child_db)) return false;
    if (!fk_get_str(p, end, e.child_table)) return false;
    if (!fk_get_names(p, end, e.child_columns)) return false;
    if (!fk_get_str(p, end, e.ref_db)) return false;
    if (!fk_get_str(p, end, e.ref_table)) return false;
    if (!fk_get_str(p, end, e.parent_cf)) return false;
    if (!fk_get_names(p, end, e.ref_columns)) return false;
    if (!fk_get_str(p, end, e.child_index_name)) return false;
    if (!fk_get_str(p, end, e.parent_index_name)) return false;
    if (p + 3 > end) return false;
    e.parent_is_pk = *p++;
    e.on_delete = *p++;
    e.on_update = *p++;
    if (p + 2 > end) return false;
    uint16 nn = (uint16)(p[0] | (p[1] << 8));
    p += 2;
    if (p + nn > end) return false;
    for (uint16 i = 0; i < nn; i++) e.child_nullable.push_back(*p++);
    return true;
}

/* ---- catalog column family + record keys ------------------------------ */

static tidesdb_column_family_t *fk_catalog_cf()
{
    tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, FK_CATALOG_CF);
    if (cf) return cf;
    tidesdb_column_family_config_t cfg = tidesdb_default_column_family_config();
    if (tidesdb_create_column_family(tdb_global, FK_CATALOG_CF, &cfg) != TDB_SUCCESS)
        return tidesdb_get_column_family(tdb_global, FK_CATALOG_CF);
    return tidesdb_get_column_family(tdb_global, FK_CATALOG_CF);
}

/* record key "c" + child_cf + '\0' + name  (a table's outgoing constraints) */
static std::string fk_child_key(const std::string &child_cf, const std::string &name)
{
    std::string k;
    k.push_back((char)FK_REC_CHILD);
    k.append(child_cf);
    k.push_back('\0');
    k.append(name);
    return k;
}

/* record key "p" + parent_cf + '\0' + child_cf + '\0' + name (incoming ones) */
static std::string fk_parent_key(const std::string &parent_cf, const std::string &child_cf,
                                 const std::string &name)
{
    std::string k;
    k.push_back((char)FK_REC_PARENT);
    k.append(parent_cf);
    k.push_back('\0');
    k.append(child_cf);
    k.push_back('\0');
    k.append(name);
    return k;
}

/* ---- helpers over the server table definition ------------------------- */

/* the index whose leading parts are exactly these columns in order, or -1 */
static int fk_find_covering_index(TABLE *table, const std::vector<std::string> &cols)
{
    for (uint i = 0; i < table->s->keys; i++)
    {
        KEY *ki = &table->key_info[i];
        if (ki->user_defined_key_parts < cols.size()) continue;
        bool match = true;
        for (uint p = 0; p < cols.size(); p++)
        {
            const char *fn = ki->key_part[p].field->field_name.str;
            if (!fn || strcmp(fn, cols[p].c_str()) != 0)
            {
                match = false;
                break;
            }
        }
        if (match) return (int)i;
    }
    return -1;
}

static int fk_field_index(TABLE *table, const std::string &col)
{
    for (uint i = 0; i < table->s->fields; i++)
    {
        const char *fn = table->field[i]->field_name.str;
        if (fn && strcmp(fn, col.c_str()) == 0) return (int)i;
    }
    return -1;
}

/* Read the referenced parent table's frm off the table-definition cache and
   without any metadata lock, to learn which key the foreign key references,
   whether that key is the parent primary key, and whether its columns are
   nullable.  init_tmp_table_share plus open_table_def is the same lock-free frm
   peek the server uses to read a table's keys by name.  Returns true when the
   parent was read and a matching primary or unique key was found. */
static bool fk_checks_off(THD *thd);

enum fk_parent_resolution
{
    FK_PARENT_KEY_FOUND,
    FK_PARENT_TABLE_MISSING,
    FK_PARENT_KEY_MISSING
};

static fk_parent_resolution fk_resolve_parent_index(THD *thd, const std::string &ref_db,
                                                    const std::string &ref_table,
                                                    const std::vector<std::string> &ref_cols,
                                                    bool &out_is_pk, std::string &out_index_name,
                                                    bool &out_has_nullable)
{
    if (ref_db.empty() || ref_table.empty() || ref_cols.empty()) return FK_PARENT_KEY_MISSING;

    char path[FN_REFLEN + 1];
    build_table_filename(path, sizeof(path) - 1, ref_db.c_str(), ref_table.c_str(), "", 0);

    /* Look for the parent's definition before reading it, since reading a missing one raises an
       error the statement would then fail with, even when foreign key checks are off and a
       missing parent is allowed. */
    char frm[FN_REFLEN + 1];
    build_table_filename(frm, sizeof(frm) - 1, ref_db.c_str(), ref_table.c_str(), reg_ext, 0);
    if (my_access(frm, F_OK)) return FK_PARENT_TABLE_MISSING;

    TABLE_SHARE share;
    /* init_tmp_table_share gained a trailing thread_specific flag in MariaDB 11.7; pass it where
       the server accepts it, and fall back to the six-argument form on older releases. */
#if MYSQL_VERSION_ID >= 110700
    init_tmp_table_share(thd, &share, ref_db.c_str(), 0, ref_table.c_str(), path, 1);
#else
    init_tmp_table_share(thd, &share, ref_db.c_str(), 0, ref_table.c_str(), path);
#endif
    fk_parent_resolution res = FK_PARENT_TABLE_MISSING;
    if (open_table_def(thd, &share, GTS_TABLE | GTS_USE_DISCOVERY) == OPEN_FRM_OK)
    {
        res = FK_PARENT_KEY_MISSING;
        int best = -1;
        for (uint i = 0; i < share.keys; i++)
        {
            KEY *k = &share.key_info[i];
            if (!(k->flags & HA_NOSAME)) continue; /* only primary or unique keys qualify */
            /* The referenced columns have to be the whole key.  A leading part of a composite
               key is not unique, and the parent probe of a primary key builds the full data key
               from the referenced values, so a prefix would never match a row. */
            if (k->user_defined_key_parts != ref_cols.size()) continue;
            bool match = true;
            for (uint p = 0; p < ref_cols.size(); p++)
            {
                const char *fn = k->key_part[p].field->field_name.str;
                if (!fn || strcasecmp(fn, ref_cols[p].c_str()) != 0)
                {
                    match = false;
                    break;
                }
            }
            if (!match) continue;
            if (best < 0 || i == share.primary_key) best = (int)i; /* prefer the primary key */
        }
        if (best >= 0)
        {
            KEY *k = &share.key_info[best];
            out_is_pk = ((uint)best == share.primary_key);
            out_index_name.assign(k->name.str, k->name.length);
            out_has_nullable = false;
            for (uint p = 0; p < ref_cols.size(); p++)
                if (k->key_part[p].field->real_maybe_null()) out_has_nullable = true;
            res = FK_PARENT_KEY_FOUND;
        }
    }
    free_table_share(&share);
    return res;
}

/* ---- create-time persistence ------------------------------------------ */

int ha_tidesdb::fk_persist_defs(const char *path, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
    if (!create_info || !create_info->alter_info) return 0;

    std::string child_cf = path_to_cf_name(path);
    THD *thd = ha_thd();

    std::vector<std::pair<std::string, std::string>> records; /* key, value */

    List_iterator_fast<Key> kit(create_info->alter_info->key_list);
    Key *key;
    uint anon = 0;
    while ((key = kit++))
    {
        if (key->type != Key::FOREIGN_KEY) continue;
        Foreign_key *fk = static_cast<Foreign_key *>(key);

        fk_catalog_entry e;
        e.child_cf = child_cf;
        if (table_arg->s->db.str) e.child_db.assign(table_arg->s->db.str, table_arg->s->db.length);
        if (table_arg->s->table_name.str)
            e.child_table.assign(table_arg->s->table_name.str, table_arg->s->table_name.length);
        e.on_delete = (uint8)fk->delete_opt;
        e.on_update = (uint8)fk->update_opt;

        if (fk->constraint_name.str && fk->constraint_name.length)
            e.name.assign(fk->constraint_name.str, fk->constraint_name.length);
        else if (fk->name.str && fk->name.length)
            e.name.assign(fk->name.str, fk->name.length);
        else
        {
            char buf[64];
            snprintf(buf, sizeof(buf), "%s_ibfk_%u", table_arg->s->table_name.str, ++anon);
            e.name = buf;
        }

        List_iterator_fast<Key_part_spec> cc(fk->columns);
        Key_part_spec *kp;
        while ((kp = cc++))
            if (kp->field_name.str)
                e.child_columns.emplace_back(kp->field_name.str, kp->field_name.length);

        /* Record whether each referencing column is nullable so the parent side
           can rebuild the child index prefix, whose encoding carries a null
           indicator only for a nullable column. */
        bool any_not_null = false;
        for (const auto &c : e.child_columns)
        {
            int fi = fk_field_index(table_arg, c);
            const bool nullable = fi >= 0 && table_arg->field[fi]->real_maybe_null();
            e.child_nullable.push_back((uint8)nullable);
            if (!nullable) any_not_null = true;
        }

        /* SET NULL on a column that cannot hold NULL has nothing it can do, and enforcing it would
           leave the child pointing at a parent row that is gone.  InnoDB refuses it too. */
        if (any_not_null &&
            (fk->delete_opt == FK_OPTION_SET_NULL || fk->update_opt == FK_OPTION_SET_NULL))
        {
            my_printf_error(ER_CANT_CREATE_TABLE,
                            "TidesDB foreign key %s uses SET NULL on a column declared NOT NULL",
                            MYF(0), e.name.c_str());
            return HA_ERR_UNSUPPORTED;
        }

        List_iterator_fast<Key_part_spec> rc(fk->ref_columns);
        while ((kp = rc++))
            if (kp->field_name.str)
                e.ref_columns.emplace_back(kp->field_name.str, kp->field_name.length);

        if (fk->ref_db.str && fk->ref_db.length)
            e.ref_db.assign(fk->ref_db.str, fk->ref_db.length);
        else
            e.ref_db = table_arg->s->db.str
                           ? std::string(table_arg->s->db.str, table_arg->s->db.length)
                           : std::string();
        e.ref_table.assign(fk->ref_table.str ? fk->ref_table.str : "",
                           fk->ref_table.str ? fk->ref_table.length : 0);

        /* build the parent cf name the same way create() builds the child one */
        std::string parent_path = "./" + e.ref_db + "/" + e.ref_table;
        e.parent_cf = path_to_cf_name(parent_path.c_str());

        int cidx = fk_find_covering_index(table_arg, e.child_columns);
        if (cidx < 0)
        {
            my_printf_error(ER_CANT_CREATE_TABLE,
                            "TidesDB requires an index on the foreign key columns of %s", MYF(0),
                            e.name.c_str());
            return HA_ERR_UNSUPPORTED;
        }
        e.child_index_name = table_arg->key_info[cidx].name.str;

        /* The parent-side scan rebuilds the child index prefix with a plain
           forward sort key, so a descending foreign-key column would not line up.
           Reject it clearly rather than enforce it incorrectly. */
        for (uint kp = 0; kp < e.child_columns.size(); kp++)
            if (table_arg->key_info[cidx].key_part[kp].key_part_flag & HA_REVERSE_SORT)
            {
                my_printf_error(
                    ER_CANT_CREATE_TABLE,
                    "TidesDB does not support a descending column in the foreign key %s", MYF(0),
                    e.name.c_str());
                return HA_ERR_UNSUPPORTED;
            }

        /* Resolve which parent key the constraint references so the child probe
           targets the right place, the parent data family for a primary key or
           the parent index family for a unique key.  A constraint that names no
           primary or unique key of its parent is refused, since enforcing it
           against some other key would accept and reject the wrong rows.  A
           parent that does not exist yet is accepted only with foreign key
           checks off, as InnoDB does, and is taken to be its primary key. */
        e.parent_is_pk = 1;
        bool p_is_pk = true, p_has_nullable = false;
        std::string p_index;
        switch (fk_resolve_parent_index(thd, e.ref_db, e.ref_table, e.ref_columns, p_is_pk, p_index,
                                        p_has_nullable))
        {
            case FK_PARENT_KEY_FOUND:
                e.parent_is_pk = p_is_pk ? 1 : 0;
                e.parent_index_name = p_index;
                /* A referenced unique index whose columns are nullable stores a null
                   indicator the value-only child probe would not reproduce, so reject
                   that rare shape rather than enforce it incorrectly. */
                if (!p_is_pk && p_has_nullable)
                {
                    my_printf_error(
                        ER_CANT_CREATE_TABLE,
                        "TidesDB foreign key %s must reference a NOT NULL unique key or the "
                        "primary key",
                        MYF(0), e.name.c_str());
                    return HA_ERR_UNSUPPORTED;
                }
                break;
            case FK_PARENT_TABLE_MISSING:
                if (!fk_checks_off(thd))
                {
                    my_printf_error(ER_CANT_CREATE_TABLE,
                                    "TidesDB foreign key %s references %s.%s, which does not exist",
                                    MYF(0), e.name.c_str(), e.ref_db.c_str(), e.ref_table.c_str());
                    return HA_ERR_UNSUPPORTED;
                }
                break;
            case FK_PARENT_KEY_MISSING:
                my_printf_error(ER_CANT_CREATE_TABLE,
                                "TidesDB foreign key %s must reference every column of the "
                                "primary key or of a unique key of %s.%s",
                                MYF(0), e.name.c_str(), e.ref_db.c_str(), e.ref_table.c_str());
                return HA_ERR_UNSUPPORTED;
        }

        std::string val;
        fk_serialize(e, val);
        records.emplace_back(fk_child_key(e.child_cf, e.name), val);
        records.emplace_back(fk_parent_key(e.parent_cf, e.child_cf, e.name), val);
        (void)thd;
    }

    if (records.empty()) return 0;

    tidesdb_column_family_t *cf = fk_catalog_cf();
    if (!cf) return HA_ERR_GENERIC;

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return HA_ERR_GENERIC;
    for (auto &r : records)
    {
        if (tdb_txn_put_blocking(thd, txn, cf, (const uint8_t *)r.first.data(), r.first.size(),
                                 (const uint8_t *)r.second.data(), r.second.size(),
                                 TIDESDB_TTL_NONE) != TDB_SUCCESS)
        {
            tidesdb_txn_rollback(txn);
            tidesdb_txn_free(txn);
            return HA_ERR_GENERIC;
        }
    }
    int rc = tidesdb_txn_commit(txn);
    tidesdb_txn_free(txn);
    if (rc == TDB_SUCCESS) tdb_fk_catalog_gen.fetch_add(1, std::memory_order_release);
    return rc == TDB_SUCCESS ? 0 : HA_ERR_GENERIC;
}

/* ---- drop-time purge -------------------------------------------------- */

int ha_tidesdb::fk_purge_catalog(const char *child_cf_name)
{
    tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, FK_CATALOG_CF);
    if (!cf) return 0;
    std::string cfn(child_cf_name);

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return 0;

    /* Collect every record naming this cf on either side, then delete them.  A
       full scan is fine because the catalog holds one small record per
       constraint and this only runs on DROP TABLE. */
    std::vector<std::string> to_delete;
    THD *thd = current_thd;
    tidesdb_iter_t *it = NULL;
    int lrc = tdb_iter_new_blocking(thd, txn, cf, &it);
    if (lrc == TDB_SUCCESS && it)
    {
        const uint8_t lo0 = 0;
        lrc = tdb_iter_seek_blocking(thd, it, &lo0, 1);
        while (!tdb_iter_failed(lrc) && tidesdb_iter_valid(it))
        {
            uint8_t *k = NULL, *v = NULL;
            size_t ks = 0, vs = 0;
            lrc = tidesdb_iter_key(it, &k, &ks);
            if (lrc == TDB_SUCCESS) lrc = tidesdb_iter_value(it, &v, &vs);
            if (lrc != TDB_SUCCESS)
            {
                if (k) tidesdb_free(k);
                if (lrc == TDB_ERR_NOT_FOUND) lrc = TDB_ERR_IO;
                break;
            }
            fk_catalog_entry e;
            if (fk_deserialize(v, vs, e) && (e.child_cf == cfn || e.parent_cf == cfn))
                to_delete.emplace_back((const char *)k, ks);
            tidesdb_free(k);
            tidesdb_free(v);
            lrc = tidesdb_iter_next(it);
        }
        tidesdb_iter_free(it);
    }
    /* A catalog that could not be read whole is left as it is rather than purged in part. */
    if (tdb_iter_failed(lrc))
    {
        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
        sql_print_warning("[TIDESDB] could not read the foreign key catalog to purge '%s' (err=%d)",
                          child_cf_name, lrc);
        return 0;
    }

    int drc = TDB_SUCCESS;
    for (auto &k : to_delete)
    {
        drc = tdb_txn_delete_cf_blocking(thd, txn, cf, (const uint8_t *)k.data(), k.size(), false);
        if (drc != TDB_SUCCESS) break;
    }
    if (drc == TDB_SUCCESS) drc = tidesdb_txn_commit(txn);
    if (drc != TDB_SUCCESS)
    {
        tidesdb_txn_rollback(txn);
        tidesdb_txn_free(txn);
        sql_print_warning("[TIDESDB] could not purge the foreign key catalog of '%s' (err=%d)",
                          child_cf_name, drc);
        return 0;
    }
    tidesdb_txn_free(txn);
    if (!to_delete.empty()) tdb_fk_catalog_gen.fetch_add(1, std::memory_order_release);
    return 0;
}

/* ---- the per-table constraint set ------------------------------------- */

std::atomic<uint64_t> tdb_fk_catalog_gen{1};

std::shared_ptr<const tdb_fk_set> ha_tidesdb::fk_build(uint64_t gen)
{
    auto set = std::make_shared<tdb_fk_set>();
    set->gen = gen;

    tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, FK_CATALOG_CF);
    if (!cf) return set;

    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return nullptr;
    THD *thd = ha_thd();
    int lrc = TDB_SUCCESS;

    const std::string &self = share->cf_name;

    /* The catalog is small, one record per constraint per side, so we walk it
       whole and route each entry by the column families it names.  A record
       whose child cf is this table feeds the child list, one whose parent cf is
       this table feeds the parent list, and a self-referencing constraint feeds
       both. */
    tidesdb_iter_t *it = NULL;
    lrc = tdb_iter_new_blocking(thd, txn, cf, &it);
    if (lrc == TDB_SUCCESS && it)
    {
        /* a fresh iterator is unpositioned, seek to the low end to start */
        const uint8_t lo0 = 0;
        lrc = tdb_iter_seek_blocking(thd, it, &lo0, 1);
        while (!tdb_iter_failed(lrc) && tidesdb_iter_valid(it))
        {
            uint8_t *k = NULL, *v = NULL;
            size_t ks = 0, vs = 0;
            lrc = tidesdb_iter_key(it, &k, &ks);
            if (lrc == TDB_SUCCESS) lrc = tidesdb_iter_value(it, &v, &vs);
            if (lrc != TDB_SUCCESS)
            {
                if (k) tidesdb_free(k);
                if (lrc == TDB_ERR_NOT_FOUND) lrc = TDB_ERR_IO;
                break;
            }
            {
                /* only the child-side record is materialized, so each constraint
                   is considered once even though it is stored on both sides */
                fk_catalog_entry e;
                if (ks > 0 && k[0] == FK_REC_CHILD && fk_deserialize(v, vs, e))
                {
                    if (e.child_cf == self)
                    {
                        tdb_fk_def d;
                        d.name = e.name;
                        d.child_cf = e.child_cf;
                        d.child_db = e.child_db;
                        d.child_table = e.child_table;
                        d.ref_db = e.ref_db;
                        d.ref_table = e.ref_table;
                        d.parent_cf = e.parent_cf;
                        d.child_index_name = e.child_index_name;
                        d.parent_index_name = e.parent_index_name;
                        d.ref_column_names = e.ref_columns;
                        d.child_nullable = e.child_nullable;
                        d.on_delete = e.on_delete;
                        d.on_update = e.on_update;
                        for (auto &c : e.child_columns)
                        {
                            int fi = fk_field_index(table, c);
                            if (fi >= 0) d.child_fields.push_back((uint16)fi);
                        }
                        d.child_key_no = fk_find_covering_index(table, e.child_columns);
                        d.parent_is_pk = (e.parent_is_pk != 0);
                        set->child.push_back(std::move(d));
                    }
                    if (e.parent_cf == self)
                    {
                        tdb_fk_def d;
                        d.name = e.name;
                        d.child_cf = e.child_cf;
                        d.child_db = e.child_db;
                        d.child_table = e.child_table;
                        d.ref_db = e.ref_db;
                        d.ref_table = e.ref_table;
                        d.parent_cf = e.parent_cf;
                        d.child_index_name = e.child_index_name;
                        d.ref_column_names = e.ref_columns;
                        d.child_nullable = e.child_nullable;
                        d.on_delete = e.on_delete;
                        d.on_update = e.on_update;
                        for (auto &c : e.ref_columns)
                        {
                            int fi = fk_field_index(table, c);
                            if (fi >= 0) d.parent_fields.push_back((uint16)fi);
                        }
                        d.parent_key_no = fk_find_covering_index(table, e.ref_columns);
                        d.parent_is_pk = (table->s->primary_key != MAX_KEY &&
                                          d.parent_key_no == (int)table->s->primary_key);
                        set->parent.push_back(std::move(d));
                    }
                }
            }
            if (k) tidesdb_free(k);
            if (v) tidesdb_free(v);
            lrc = tidesdb_iter_next(it);
        }
        tidesdb_iter_free(it);
    }

    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    /* A catalog that could not be read whole yields no set, rather than one missing constraints
       that would then go unenforced. */
    if (tdb_iter_failed(lrc))
    {
        sql_print_error("[TIDESDB] could not read the foreign key catalog for '%s' (err=%d)",
                        share->cf_name.c_str(), lrc);
        return nullptr;
    }
    return set;
}

std::shared_ptr<const tdb_fk_set> ha_tidesdb::fk_current()
{
    if (!share) return nullptr;
    /* Read the generation before the catalog, so a set built while a change commits carries the
       older number and is rebuilt on the next look. */
    const uint64_t gen = tdb_fk_catalog_gen.load(std::memory_order_acquire);
    std::shared_ptr<const tdb_fk_set> cur = std::atomic_load(&share->fk);
    if (cur && cur->gen == gen) return cur;
    std::shared_ptr<const tdb_fk_set> fresh = fk_build(gen);
    /* A failed build is not cached, so the next statement tries again; this one gets no set. */
    if (fresh) std::atomic_store(&share->fk, fresh);
    return fresh;
}

const tdb_fk_set &ha_tidesdb::fks()
{
    static const tdb_fk_set empty;
    if (!fk_stmt_) fk_stmt_ = fk_current();
    return fk_stmt_ ? *fk_stmt_ : empty;
}

/* ---- server read-back methods ----------------------------------------- */

static enum_fk_option fk_opt(uint8 v)
{
    return (enum_fk_option)v;
}

/* A constraint declared without an explicit action carries FK_OPTION_UNDEF, which
   information_schema cannot render, so we report it as RESTRICT the way InnoDB
   does for the default. */
static enum_fk_option fk_display_opt(uint8 v)
{
    enum_fk_option o = (enum_fk_option)v;
    return (o == FK_OPTION_UNDEF) ? FK_OPTION_RESTRICT : o;
}

/* Describe one constraint for the server.  The referencing (foreign) table and
   the referenced table swap roles depending on which side is asking, since the
   same constraint is reported from the child by get_foreign_key_list and from
   the parent by get_parent_foreign_key_list.  Getting the foreign table right on
   the parent side is what lets prelocking open the child for a cascade. */
static void fk_fill_info(THD *thd, const tdb_fk_def &d, TABLE *table, FOREIGN_KEY_INFO *fki,
                         bool parent_side)
{
    fki->foreign_id = thd_make_lex_string(thd, NULL, d.name.c_str(), d.name.size(), 1);
    if (parent_side)
    {
        fki->foreign_db = thd_make_lex_string(thd, NULL, d.child_db.c_str(), d.child_db.size(), 1);
        fki->foreign_table =
            thd_make_lex_string(thd, NULL, d.child_table.c_str(), d.child_table.size(), 1);
        fki->referenced_db =
            thd_make_lex_string(thd, NULL, table->s->db.str, table->s->db.length, 1);
        fki->referenced_table = thd_make_lex_string(thd, NULL, table->s->table_name.str,
                                                    table->s->table_name.length, 1);
    }
    else
    {
        fki->foreign_db = thd_make_lex_string(thd, NULL, table->s->db.str, table->s->db.length, 1);
        fki->foreign_table = thd_make_lex_string(thd, NULL, table->s->table_name.str,
                                                 table->s->table_name.length, 1);
        fki->referenced_db = thd_make_lex_string(thd, NULL, d.ref_db.c_str(), d.ref_db.size(), 1);
        fki->referenced_table =
            thd_make_lex_string(thd, NULL, d.ref_table.c_str(), d.ref_table.size(), 1);
    }
    fki->referenced_key_name = NULL;
    fki->update_method = fk_display_opt(d.on_update);
    fki->delete_method = fk_display_opt(d.on_delete);

    /* Column pairs for information_schema.  On the child side we know both the
       referencing columns (our own fields) and the referenced column names.  On
       the parent side we know our referenced columns; the referencing column
       names live with the child, so we list what we hold. */
    if (!parent_side)
    {
        for (size_t i = 0; i < d.child_fields.size(); i++)
        {
            const char *cn = table->field[d.child_fields[i]]->field_name.str;
            fki->foreign_fields.push_back(thd_make_lex_string(thd, NULL, cn, strlen(cn), 1));
            if (i < d.ref_column_names.size())
                fki->referenced_fields.push_back(thd_make_lex_string(
                    thd, NULL, d.ref_column_names[i].c_str(), d.ref_column_names[i].size(), 1));
        }
    }
    else
    {
        for (uint16 fi : d.parent_fields)
        {
            const char *cn = table->field[fi]->field_name.str;
            fki->referenced_fields.push_back(thd_make_lex_string(thd, NULL, cn, strlen(cn), 1));
        }
    }
}

int ha_tidesdb::get_foreign_key_list(THD *thd, List<FOREIGN_KEY_INFO> *f_key_list)
{
    std::shared_ptr<const tdb_fk_set> fk = fk_current();
    if (!fk) return 0;
    for (const auto &d : fk->child)
    {
        FOREIGN_KEY_INFO fki;
        fk_fill_info(thd, d, table, &fki, false);
        FOREIGN_KEY_INFO *p = (FOREIGN_KEY_INFO *)thd_memdup(thd, &fki, sizeof(fki));
        if (p) f_key_list->push_back(p);
    }
    return 0;
}

int ha_tidesdb::get_parent_foreign_key_list(THD *thd, List<FOREIGN_KEY_INFO> *f_key_list)
{
    std::shared_ptr<const tdb_fk_set> fk = fk_current();
    if (!fk) return 0;
    for (const auto &d : fk->parent)
    {
        FOREIGN_KEY_INFO fki;
        fk_fill_info(thd, d, table, &fki, true);
        FOREIGN_KEY_INFO *p = (FOREIGN_KEY_INFO *)thd_memdup(thd, &fki, sizeof(fki));
        if (p) f_key_list->push_back(p);
    }
    return 0;
}

#if MYSQL_VERSION_ID >= 110400
bool ha_tidesdb::referenced_by_foreign_key() const noexcept
{
    /* The server asks this outside a statement too, so look at the current catalog, which only
       reads and swaps the shared set and leaves this handler unchanged. */
    std::shared_ptr<const tdb_fk_set> fk = const_cast<ha_tidesdb *>(this)->fk_current();
    return fk && !fk->parent.empty();
}
#else
uint ha_tidesdb::referenced_by_foreign_key()
{
    std::shared_ptr<const tdb_fk_set> fk = fk_current();
    return (fk && !fk->parent.empty()) ? 1 : 0;
}
#endif

bool ha_tidesdb::can_switch_engines()
{
    std::shared_ptr<const tdb_fk_set> fk = fk_current();
    if (!fk) return true;
    return fk->child.empty() && fk->parent.empty();
}

char *ha_tidesdb::get_foreign_key_create_info()
{
    std::shared_ptr<const tdb_fk_set> fk = fk_current();
    if (!fk || fk->child.empty()) return NULL;

    std::string s;
    for (const auto &d : fk->child)
    {
        s += ",\n  CONSTRAINT `";
        s += d.name;
        s += "` FOREIGN KEY (";
        for (size_t i = 0; i < d.child_fields.size(); i++)
        {
            if (i) s += ",";
            s += "`";
            s += table->field[d.child_fields[i]]->field_name.str;
            s += "`";
        }
        s += ") REFERENCES `";
        s += d.ref_table;
        s += "` (";
        for (size_t i = 0; i < d.ref_column_names.size(); i++)
        {
            if (i) s += ",";
            s += "`";
            s += d.ref_column_names[i];
            s += "`";
        }
        s += ")";
        if (fk_opt(d.on_delete) == FK_OPTION_CASCADE)
            s += " ON DELETE CASCADE";
        else if (fk_opt(d.on_delete) == FK_OPTION_SET_NULL)
            s += " ON DELETE SET NULL";
        if (fk_opt(d.on_update) == FK_OPTION_CASCADE)
            s += " ON UPDATE CASCADE";
        else if (fk_opt(d.on_update) == FK_OPTION_SET_NULL)
            s += " ON UPDATE SET NULL";
    }

    char *out = (char *)my_malloc(PSI_NOT_INSTRUMENTED, s.size() + 1, MYF(MY_WME));
    if (!out) return NULL;
    memcpy(out, s.data(), s.size());
    out[s.size()] = '\0';
    return out;
}

void ha_tidesdb::free_foreign_key_create_info(char *str)
{
    my_free(str);
}

/* ---- enforcement ------------------------------------------------------ */

static bool fk_checks_off(THD *thd)
{
    return thd && thd_test_options(thd, OPTION_NO_FOREIGN_KEY_CHECKS);
}

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
    if (fk_checks_off(thd)) return 0;
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
    if (!refs.empty() && ch->ha_rnd_init(false) == 0)
    {
        my_ptrdiff_t npd = is_update ? (my_ptrdiff_t)(new_row - table->record[0]) : 0;
        for (auto &r : refs)
        {
            if (ch->ha_rnd_pos(ct->record[0], (uchar *)r.data()) != 0) continue;

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
    if (fk_checks_off(thd)) return 0;
    if (!stmt_txn) return 0;

    for (const auto &d : fks().parent)
    {
        if (d.parent_key_no < 0) continue;
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
    if (fk_checks_off(thd)) return 0;
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
