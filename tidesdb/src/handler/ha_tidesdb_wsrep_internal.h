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
  Shared by the Galera translation units, ha_tidesdb_wsrep.cc, ha_tidesdb_wsrep_reads.cc,
  ha_tidesdb_wsrep_pos.cc and ha_tidesdb_wsrep_sst.cc, and by nothing else.
*/

#ifndef HA_TIDESDB_WSREP_INTERNAL_H
#define HA_TIDESDB_WSREP_INTERNAL_H

#include "ha_tidesdb.h"

#ifdef WITH_WSREP

#include <string>

#include "key.h"
#include "sql_class.h"
#include "sql_show.h"
#include "src/handler/ha_tidesdb_internal.h"

/* WSREP_ON, the cheap global gate the wsrep_on() service macro folds in front of
   the provider call so a non-clustered server pays nothing per row. */
#include "wsrep_on.h"

/* The server's own position writer, which hands a position to every engine.  The gtid form marks
   the position undefined.  Both are declared here rather than through wsrep_xid.h, whose own
   declarations collide with the plugin service macros of the same names, and the XID form is
   defined in the server but left out of that header anyway. */
#include "wsrep_mysqld.h"
bool wsrep_set_SE_checkpoint(const wsrep::gtid &gtid, const wsrep_server_gtid_t &server_gtid);
bool wsrep_set_SE_checkpoint(XID &xid);

/* The meta column family, created on first use like the foreign key catalog. */
static inline tidesdb_column_family_t *wsrep_meta_cf()
{
    tidesdb_column_family_t *cf = tidesdb_get_column_family(tdb_global, WSREP_META_CF);
    if (cf) return cf;
    tidesdb_column_family_config_t cfg = tidesdb_default_column_family_config();
    if (tidesdb_create_column_family(tdb_global, WSREP_META_CF, &cfg) != TDB_SUCCESS)
        return tidesdb_get_column_family(tdb_global, WSREP_META_CF);
    return tidesdb_get_column_family(tdb_global, WSREP_META_CF);
}

/* Whether the transaction recorded with the counter life at generation gen is still that
   transaction.  The counter outlives the context through the record, so nothing is found through
   the server. */
static inline bool tdb_wsrep_trx_live(const std::shared_ptr<std::atomic<uint64_t>> &life,
                                      uint64_t gen)
{
    return life && life->load() == gen;
}

/* The handlerton hooks the registration in ha_tidesdb_wsrep.cc wires, each defined in the unit
   that owns what it does. */
int tidesdb_wsrep_set_checkpoint(handlerton *, const XID *xid);
int tidesdb_wsrep_get_checkpoint(handlerton *, XID *xid);
ulonglong tidesdb_wsrep_prepare_commit_versioned(THD *thd, ulonglong *);
void tidesdb_wsrep_disable_internal_writes(bool disable);

#endif /* WITH_WSREP */
#endif /* HA_TIDESDB_WSREP_INTERNAL_H */
