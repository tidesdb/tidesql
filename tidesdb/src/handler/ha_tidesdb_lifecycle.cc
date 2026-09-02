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

/* table and database teardown: rename_table moves a table's column families to the new name,
   delete_table and the handlerton drop_table callback remove one table's families and directory,
   the drop_database callback sweeps every family under a dropped database, and delete_all_rows
   drops and recreates a table's families to serve TRUNCATE.  the shared helpers force a
   column-family directory removal and translate a server path into the base names the library keys
   its column families by. */

#include "ha_tidesdb.h"

#include <mysql/plugin.h>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"

#include <string>
#include <vector>

#include "src/engine/ha_tidesdb_config.h"
#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_lifecycle.h"
int ha_tidesdb::rename_table(const char *from, const char *to)
{
    DBUG_ENTER("ha_tidesdb::rename_table");

    std::string old_cf = path_to_cf_name(from);
    std::string new_cf = path_to_cf_name(to);

    /* If the destination CF already exists (stale from a previous ALTER),
       drop it first so the rename can proceed. */
    tidesdb_drop_column_family(tdb_global, new_cf.c_str());

    int rc = tidesdb_rename_column_family(tdb_global, old_cf.c_str(), new_cf.c_str());
    if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
    {
        sql_print_error("[TIDESDB] Failed to rename CF '%s' -> '%s' (err=%d)", old_cf.c_str(),
                        new_cf.c_str(), rc);
        DBUG_RETURN(tdb_rc_to_ha(rc, "rename_table"));
    }

    {
        std::string prefix = old_cf + CF_INDEX_INFIX;
        char **names = NULL;
        int count = 0;
        if (tidesdb_list_column_families(tdb_global, &names, &count) == TDB_SUCCESS && names)
        {
            for (int i = 0; i < count; i++)
            {
                if (!names[i]) continue;
                std::string cf_str(names[i]);
                if (cf_str.compare(0, prefix.size(), prefix) == 0)
                {
                    std::string suffix = cf_str.substr(prefix.size());
                    std::string new_idx = new_cf + CF_INDEX_INFIX + suffix;

                    tidesdb_drop_column_family(tdb_global, new_idx.c_str());
                    rc = tidesdb_rename_column_family(tdb_global, cf_str.c_str(), new_idx.c_str());
                    if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
                        sql_print_error("[TIDESDB] Failed to rename idx CF '%s' -> '%s' (err=%d)",
                                        cf_str.c_str(), new_idx.c_str(), rc);
                }
                tidesdb_free(names[i]);
            }
            tidesdb_free(names);
        }
    }

    DBUG_RETURN(0);
}

/* ******************** delete_table (DROP TABLE) ******************** */

/*
  Force-remove a directory tree from disk.  Used as a safety net after
  tidesdb_drop_column_family() because the library's internal
  remove_directory() can fail silently (e.g. open fds from block cache,
  mmap, or background workers).  If stale SSTables survive, the next
  CREATE TABLE with the same name inherits them -- catastrophic for
  performance (bloom filters pass on every SSTable since keys overlap).
*/
static void force_remove_cf_dir(const std::string &cf_name)
{
    char dir[FN_REFLEN];
    const char sep[] = {FN_LIBCHAR, 0};
    strxnmov(dir, sizeof(dir) - 1, tdb_path.c_str(), sep, cf_name.c_str(), NullS);

    MY_STAT st;
    if (!my_stat(dir, &st, MYF(0))) return; /* already gone */

    /* my_rmtree() is MariaDB's portable recursive directory removal
       (handles Windows, symlinks, read-only attrs, etc.). */
    if (my_rmtree(dir, MYF(0)) != 0)
        sql_print_warning("[TIDESDB] force_remove_cf_dir failed for %s", dir);
}

/*
  Shared drop logic used by both the handlerton callback (hton->drop_table)
  and the handler method (ha_tidesdb::delete_table).  Drops the main data CF
  and all secondary index CFs, then force-removes their directories.
  Returns 0 on success.
*/
static int tidesdb_drop_table_impl(const char *path)
{
    if (!tdb_global) return 0;

    std::string cf_name = ha_tidesdb::path_to_cf_name(path);

    /* Remove this table's foreign-key catalog rows, on both the child and parent
       side, so a dropped table leaves no dangling constraint metadata. */
    ha_tidesdb::fk_purge_catalog(cf_name.c_str());

    /* We collect secondary index CF names before dropping so we can
       force-remove their directories afterwards. */
    std::vector<std::string> idx_cf_names;
    {
        std::string prefix = cf_name + CF_INDEX_INFIX;
        char **names = NULL;
        int count = 0;
        if (tidesdb_list_column_families(tdb_global, &names, &count) == TDB_SUCCESS && names)
        {
            for (int i = 0; i < count; i++)
            {
                if (!names[i]) continue;
                if (strncmp(names[i], prefix.c_str(), prefix.size()) == 0)
                    idx_cf_names.push_back(names[i]);
                tidesdb_free(names[i]);
            }
            tidesdb_free(names);
        }
    }

    int rc = tidesdb_drop_column_family(tdb_global, cf_name.c_str());
    if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
    {
        sql_print_error("[TIDESDB] Failed to drop CF '%s' (err=%d)", cf_name.c_str(), rc);
        return rc;
    }

    for (const auto &idx_name : idx_cf_names)
        tidesdb_drop_column_family(tdb_global, idx_name.c_str());

    force_remove_cf_dir(cf_name);
    for (const auto &idx_name : idx_cf_names) force_remove_cf_dir(idx_name);

    return 0;
}

/*
  Handlerton-level drop_table callback.  MariaDB 12.x calls hton->drop_table
  instead of handler::delete_table.  Must return 0 on success, not -1.
*/
int tidesdb_hton_drop_table(handlerton *, const char *path)
{
    return tidesdb_drop_table_impl(path);
}

/*
  Extract the database name from a directory path handed to drop_database.
  The server passes something like "./test/" or "/var/lib/mysql/test/";
  we strip trailing separators and return the final path component.
*/
static std::string tidesdb_path_to_db_name(const char *path)
{
    if (!path) return std::string();
    std::string p(path);
    while (!p.empty() && (p.back() == FN_LIBCHAR || p.back() == '/')) p.pop_back();
    size_t slash = p.find_last_of("/\\");
    if (slash != std::string::npos) p = p.substr(slash + 1);
    return p;
}

/*
  Handlerton-level drop_database callback.  MariaDB calls this when the
  server-side DROP DATABASE has finished removing .frm files from the db
  directory.  Without this hook, TidesDB column families whose .frm was
  already unlinked would outlive the database and accumulate on disk.

  We enumerate every CF whose name starts with "<db_name>__" (the prefix
  path_to_cf_name builds for a table in that database -- which also
  captures all "db__tbl__idx_*" secondary-index CFs) and drop each.
*/
void tidesdb_hton_drop_database(handlerton *, char *path)
{
    if (!tdb_global || !path) return;

    std::string db = tidesdb_path_to_db_name(path);
    if (db.empty()) return;

    std::string prefix = db + CF_DB_TABLE_SEP;

    std::vector<std::string> to_drop;
    {
        char **names = NULL;
        int count = 0;
        if (tidesdb_list_column_families(tdb_global, &names, &count) == TDB_SUCCESS && names)
        {
            for (int i = 0; i < count; i++)
            {
                if (!names[i]) continue;
                if (strncmp(names[i], prefix.c_str(), prefix.size()) == 0)
                    to_drop.emplace_back(names[i]);
                tidesdb_free(names[i]);
            }
            tidesdb_free(names);
        }
    }

    for (const auto &cf_name : to_drop)
    {
        int rc = tidesdb_drop_column_family(tdb_global, cf_name.c_str());
        if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
            sql_print_warning("[TIDESDB] drop_database: failed to drop CF '%s' (err=%d)",
                              cf_name.c_str(), rc);
        force_remove_cf_dir(cf_name);
    }

    if (!to_drop.empty())
        sql_print_information("[TIDESDB] drop_database: removed %zu column famil%s for '%s'",
                              to_drop.size(), to_drop.size() == 1 ? "y" : "ies", db.c_str());
}

int ha_tidesdb::delete_table(const char *name)
{
    DBUG_ENTER("ha_tidesdb::delete_table");
    DBUG_RETURN(tidesdb_drop_table_impl(name));
}


/* ******************** delete_all_rows (TRUNCATE) ******************** */

int ha_tidesdb::delete_all_rows(void)
{
    DBUG_ENTER("ha_tidesdb::delete_all_rows");

    /* We free cached iterators before dropping/recreating CFs.
       The iterators hold refs to SSTables in the CFs being dropped. */
    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_cf_ = NULL;
        scan_iter_txn_ = NULL;
    }

    /* We discard the connection txn before drop/recreate.  The txn may have
       buffered INSERT/UPDATE ops from earlier statements; committing them
       after the CF is recreated would re-insert stale data. */
    {
        THD *thd = ha_thd();
        tidesdb_trx_t *trx = (tidesdb_trx_t *)thd_get_ha_data(thd, ht);
        if (trx && trx->txn)
        {
            tidesdb_txn_rollback(trx->txn);
            tidesdb_txn_free(trx->txn);
            trx->txn = NULL;
            trx->dirty = false;
            trx->fts_meta_pending.clear();
            trx->fts_meta_dirty = false;
        }
        stmt_txn = NULL;
        stmt_txn_dirty = false;
    }

    const ha_table_option_struct *t_opts = TDB_TABLE_OPTIONS(table);
    tidesdb_column_family_config_t cfg = build_cf_config(t_opts);
    tidesdb_column_family_config_t data_cfg = data_cf_config(cfg, t_opts && t_opts->encrypted);

    {
        std::string cf_name = share->cf_name;
        int rc = tidesdb_drop_column_family(tdb_global, cf_name.c_str());
        if (rc != TDB_SUCCESS && rc != TDB_ERR_NOT_FOUND)
        {
            sql_print_error("[TIDESDB] truncate: failed to drop CF '%s' (err=%d)", cf_name.c_str(),
                            rc);
            DBUG_RETURN(tdb_rc_to_ha(rc, "truncate drop_cf"));
        }

        rc = tidesdb_create_column_family(tdb_global, cf_name.c_str(), &data_cfg);
        if (rc != TDB_SUCCESS)
        {
            sql_print_error("[TIDESDB] truncate: failed to recreate CF '%s' (err=%d)",
                            cf_name.c_str(), rc);
            DBUG_RETURN(tdb_rc_to_ha(rc, "truncate create_cf"));
        }

        share->cf = tidesdb_get_column_family(tdb_global, cf_name.c_str());
        if (!share->cf)
        {
            sql_print_error("[TIDESDB] truncate: CF '%s' not found after recreate",
                            cf_name.c_str());
            DBUG_RETURN(HA_ERR_GENERIC);
        }
    }

    for (uint i = 0; i < share->idx_cfs.size(); i++)
    {
        if (!share->idx_cfs[i]) continue;

        const std::string &idx_name = share->idx_cf_names[i];
        tidesdb_drop_column_family(tdb_global, idx_name.c_str());

        tidesdb_column_family_config_t idx_cfg = cfg;

        int rc = tidesdb_create_column_family(tdb_global, idx_name.c_str(), &idx_cfg);
        if (rc != TDB_SUCCESS)
        {
            sql_print_warning("[TIDESDB] truncate: failed to recreate idx CF '%s' (err=%d)",
                              idx_name.c_str(), rc);
            share->idx_cfs[i] = NULL;
            continue;
        }

        share->idx_cfs[i] = tidesdb_get_column_family(tdb_global, idx_name.c_str());
    }

    share->next_row_id.store(HIDDEN_PK_FIRST_ROW_ID, std::memory_order_relaxed);

    DBUG_RETURN(0);
}
