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
  A Galera state transfer and the store identity.  The store lives outside the server's data
  directory, so a donor writes a consistent copy of it where the rsync transfer looks, and a joiner
  installs the copy before the store opens, while an identity kept in both the store and the data
  directory tells a transfer that did not carry the store apart from one that did.
*/

#include "ha_tidesdb.h"

#ifdef WITH_WSREP

#include <cstdio>
#include <cstring>
#include <random>
#include <string>

#include "sql_class.h"
#include "src/handler/ha_tidesdb_internal.h"
#include "src/handler/ha_tidesdb_wsrep_internal.h"

/* ---- state transfer ---------------------------------------------------- */

/* The store lives outside the server's data directory, where no state transfer method looks, so a
   joiner used to receive every table definition and none of its TidesDB rows.  While the server
   holds the donor's tables flushed and locked for the transfer it tells each engine to stop its
   own writes, and that is when a Galera node writes a consistent copy of the store into a
   directory inside the data directory.  The rsync method carries every directory there, so the
   copy reaches the joiner, which puts it in place as its store when it next starts, before the
   store opens.  The copy is removed again when the donor resumes.  The same holds for a backup
   tool that disables checkpoints on a Galera node, which then finds a consistent copy too. */
static std::string wsrep_sst_path(const char *leaf)
{
    std::string p(mysql_real_data_home);
    if (!p.empty() && p.back() != '/' && p.back() != FN_LIBCHAR) p.push_back(FN_LIBCHAR);
    p += TDB_WSREP_SST_DIR;
    if (leaf)
    {
        p.push_back(FN_LIBCHAR);
        p += leaf;
    }
    return p;
}

static bool wsrep_path_exists(const std::string &p)
{
    MY_STAT st;
    return my_stat(p.c_str(), &st, MYF(0)) != NULL;
}

/* The data directory, resolved, so a donor's own leftover copy is told apart from one received. */
static std::string wsrep_own_origin()
{
    char buf[FN_REFLEN];
    if (my_realpath(buf, mysql_real_data_home, MYF(0))) return std::string(mysql_real_data_home);
    return std::string(buf);
}

static bool wsrep_write_marker(const char *leaf, const std::string &text)
{
    FILE *f = fopen(wsrep_sst_path(leaf).c_str(), "w");
    if (!f) return false;
    const bool ok = fwrite(text.data(), 1, text.size(), f) == text.size();
    return fclose(f) == 0 && ok;
}

static std::string wsrep_read_marker(const char *leaf)
{
    std::string text;
    FILE *f = fopen(wsrep_sst_path(leaf).c_str(), "r");
    if (!f) return text;
    char buf[FN_REFLEN];
    size_t n = fread(buf, 1, sizeof(buf), f);
    text.assign(buf, n);
    fclose(f);
    return text;
}

void tidesdb_wsrep_disable_internal_writes(bool disable)
{
    if (!tdb_global || !WSREP_ON) return;
    const std::string dir = wsrep_sst_path(nullptr);
    if (wsrep_path_exists(dir) && my_rmtree(dir.c_str(), MYF(0)) != 0)
        sql_print_warning("[TIDESDB] Could not remove the old state transfer copy in '%s'",
                          dir.c_str());
    if (!disable) return;

    int rc = TDB_ERR_IO;
    if (my_mkdir(dir.c_str(), 0750, MYF(0)) == 0 &&
        wsrep_write_marker(TDB_WSREP_SST_ORIGIN, wsrep_own_origin()))
    {
        rc = tidesdb_checkpoint(tdb_global);
        if (rc == TDB_SUCCESS)
        {
            std::string store = wsrep_sst_path(TDB_WSREP_SST_STORE);
            rc = tidesdb_backup(tdb_global, const_cast<char *>(store.c_str()));
        }
    }
    if (rc == TDB_SUCCESS && wsrep_write_marker(TDB_WSREP_SST_OK, "ok"))
    {
        sql_print_information(
            "[TIDESDB] Wrote a consistent copy of the store to '%s' for a state "
            "transfer",
            dir.c_str());
        return;
    }
    /* a joiner that receives this refuses to start rather than run without its rows */
    (void)wsrep_write_marker(TDB_WSREP_SST_FAILED, std::to_string(rc));
    sql_print_error("[TIDESDB] Could not copy the store for a state transfer (err=%d)", rc);
}

/* ---- store identity ------------------------------------------------------
   A transfer method that replaces the data directory but does not carry the store, mariabackup
   among them, leaves a node whose table definitions came from the donor and whose store is its own
   old one, or an empty one.  It reports itself synced while its TidesDB tables are missing rows or
   hold stale ones.  So the store carries an identity, recorded both in the store and in a small
   file inside the data directory.  rsync carries both, the file with the data directory and the
   identity inside the store copy, an incremental transfer or a logical dump replaces neither, and
   a method that carries only the data directory brings the donor's file without the donor's store,
   which the two no longer agreeing gives away.  A store and data directory with no identity yet,
   a new node or one from before the identity existed, are given one. */

static std::string wsrep_store_id_file()
{
    std::string p(mysql_real_data_home);
    if (!p.empty() && p.back() != '/' && p.back() != FN_LIBCHAR) p.push_back(FN_LIBCHAR);
    p += "mysql";
    p.push_back(FN_LIBCHAR);
    p += TDB_WSREP_STORE_ID_FILE;
    return p;
}

static std::string wsrep_read_store_id_file()
{
    std::string id;
    FILE *f = fopen(wsrep_store_id_file().c_str(), "r");
    if (!f) return id;
    char buf[64];
    size_t n = fread(buf, 1, sizeof(buf), f);
    fclose(f);
    id.assign(buf, n);
    while (!id.empty() && (id.back() == '\n' || id.back() == '\r' || id.back() == ' '))
        id.pop_back();
    return id;
}

static std::string wsrep_read_store_id()
{
    std::string id;
    tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, WSREP_META_CF);
    if (!cf) return id;
    tidesdb_txn_t *txn = nullptr;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return id;
    uint8_t *val = nullptr;
    size_t val_len = 0;
    if (tdb_txn_get_notrack_blocking(current_thd, txn, cf, (const uint8_t *)WSREP_STORE_ID_KEY,
                                     sizeof(WSREP_STORE_ID_KEY) - 1, &val,
                                     &val_len) == TDB_SUCCESS &&
        val)
        id.assign((const char *)val, val_len);
    if (val) tidesdb_free(val);
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return id;
}

static bool wsrep_write_store_id(const std::string &id)
{
    tidesdb_column_family_t *cf = wsrep_meta_cf();
    if (!cf) return false;
    tidesdb_txn_t *txn = nullptr;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return false;
    int rc = tdb_txn_put_blocking(current_thd, txn, cf, (const uint8_t *)WSREP_STORE_ID_KEY,
                                  sizeof(WSREP_STORE_ID_KEY) - 1, (const uint8_t *)id.data(),
                                  id.size(), TIDESDB_TTL_NONE);
    if (rc == TDB_SUCCESS)
        rc = tidesdb_txn_commit(txn);
    else
        tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    if (rc != TDB_SUCCESS) return false;
    FILE *f = fopen(wsrep_store_id_file().c_str(), "w");
    if (!f) return false;
    const bool ok = fwrite(id.data(), 1, id.size(), f) == id.size();
    return fclose(f) == 0 && ok;
}

int tidesdb_wsrep_store_identity()
{
    if (!tdb_global || !WSREP_ON) return 0;
    const std::string in_store = wsrep_read_store_id();
    const std::string in_datadir = wsrep_read_store_id_file();
    if (!in_store.empty() && in_store == in_datadir) return 0;
    if (in_store.empty() && in_datadir.empty())
    {
        std::random_device rd;
        char id[33];
        snprintf(id, sizeof(id), "%08x%08x%08x%08x", rd(), rd(), rd(), rd());
        if (!wsrep_write_store_id(id))
            sql_print_warning("[TIDESDB] Could not record the store identity in '%s'",
                              wsrep_store_id_file().c_str());
        return 0;
    }
    sql_print_error(
        "[TIDESDB] The data directory and the TidesDB store at '%s' did not come from "
        "the same node, which a state transfer that does not carry the store leaves "
        "behind.  TidesDB will not start without its rows.  Only "
        "wsrep_sst_method=rsync carries the store, so use it and let the node request "
        "a new state transfer.",
        tdb_path.c_str());
    return 1;
}

int tidesdb_wsrep_sst_install()
{
    const std::string dir = wsrep_sst_path(nullptr);
    if (!wsrep_path_exists(dir)) return 0;

    /* a copy this node wrote as a donor was never meant for it, so it is dropped */
    if (wsrep_read_marker(TDB_WSREP_SST_ORIGIN) == wsrep_own_origin())
    {
        (void)my_rmtree(dir.c_str(), MYF(0));
        return 0;
    }
    if (wsrep_path_exists(wsrep_sst_path(TDB_WSREP_SST_FAILED)) ||
        !wsrep_path_exists(wsrep_sst_path(TDB_WSREP_SST_OK)))
    {
        sql_print_error(
            "[TIDESDB] The state transfer in '%s' brought no complete copy of the "
            "store, so TidesDB will not start without its rows.  Remove the directory "
            "and let the node request a new state transfer.",
            dir.c_str());
        return 1;
    }
    const std::string store = wsrep_sst_path(TDB_WSREP_SST_STORE);
    if (wsrep_path_exists(tdb_path) && my_rmtree(tdb_path.c_str(), MYF(0)) != 0)
    {
        sql_print_error(
            "[TIDESDB] Could not remove the old store '%s' to install the state "
            "transfer copy",
            tdb_path.c_str());
        return 1;
    }
    if (my_rename(store.c_str(), tdb_path.c_str(), MYF(0)) != 0)
    {
        sql_print_error(
            "[TIDESDB] Could not move the state transfer copy '%s' to '%s'. Both must "
            "be on one filesystem.",
            store.c_str(), tdb_path.c_str());
        return 1;
    }
    (void)my_rmtree(dir.c_str(), MYF(0));
    sql_print_information("[TIDESDB] Installed the store received by state transfer");
    return 0;
}

#endif /* WITH_WSREP */
