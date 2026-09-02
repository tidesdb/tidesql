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

/* the option-to-config layer: turns a table's CREATE TABLE options and the session isolation
   level into the library column-family configuration that create, open, truncate, and online DDL
   all build from.  keeping the mapping here means a new per-cf knob is wired in one place rather
   than duplicated across every path that opens a column family. */

#include "ha_tidesdb.h"

#include <mysql/plugin.h>

#include <string>

#include "sql_class.h"
#include "src/engine/ha_tidesdb_config.h"

/* enum-index to library-constant maps for the COMPRESSION, ISOLATION_LEVEL, and db-level sync-mode
   options; the option list stores the small enum index and these translate it at build time. */
const int tdb_compression_map[TDB_COMPRESSION_CHOICE_COUNT] = {
    TDB_COMPRESS_NONE, TDB_COMPRESS_SNAPPY, TDB_COMPRESS_LZ4, TDB_COMPRESS_ZSTD,
    TDB_COMPRESS_LZ4_FAST};

const int tdb_isolation_map[TDB_ISOLATION_CHOICE_COUNT] = {
    TDB_ISOLATION_READ_UNCOMMITTED, TDB_ISOLATION_READ_COMMITTED, TDB_ISOLATION_REPEATABLE_READ,
    TDB_ISOLATION_SNAPSHOT, TDB_ISOLATION_SERIALIZABLE};

const int tdb_sync_mode_map[TDB_SYNC_MODE_CHOICE_COUNT] = {TDB_SYNC_NONE, TDB_SYNC_INTERVAL,
                                                           TDB_SYNC_FULL};

/*
  Map the MariaDB session isolation level (from SET TRANSACTION ISOLATION
  LEVEL) to a TidesDB isolation level.  An explicitly chosen session level
  always wins.  When the session is left at the SQL default of REPEATABLE
  READ the table-level ISOLATION_LEVEL option decides, because that is the
  signal that the client expressed no preference of its own.

  The MariaDB enum_tx_isolation values are ISO_READ_UNCOMMITTED 0,
  ISO_READ_COMMITTED 1, ISO_REPEATABLE_READ 2 and ISO_SERIALIZABLE 3.

  TidesDB has a fifth level, SNAPSHOT, with no SQL equivalent.  A table
  that leaves ISOLATION_LEVEL at REPEATABLE READ resolves to SNAPSHOT for
  InnoDB parity, since TidesDB's strict REPEATABLE_READ tracks the read
  set and produces excessive TDB_ERR_CONFLICT under normal OLTP.  A table
  that sets SNAPSHOT, SERIALIZABLE, READ COMMITTED or READ UNCOMMITTED is
  honored as written.
*/
tidesdb_isolation_level_t resolve_effective_isolation(THD *thd, tidesdb_isolation_level_t table_iso)
{
    int session_iso = thd_tx_isolation(thd);

    switch (session_iso)
    {
        case ISO_READ_UNCOMMITTED:
            return TDB_ISOLATION_READ_UNCOMMITTED;
        case ISO_READ_COMMITTED:
            return TDB_ISOLATION_READ_COMMITTED;
        case ISO_REPEATABLE_READ:
            /* The session is at the SQL default, so the table-level
               ISOLATION_LEVEL option decides.  A table left at REPEATABLE
               READ maps to TidesDB SNAPSHOT for InnoDB parity, since
               TidesDB's strict REPEATABLE_READ tracks the read set and
               produces excessive TDB_ERR_CONFLICT under normal OLTP.  An
               explicit SNAPSHOT, SERIALIZABLE, READ COMMITTED or READ
               UNCOMMITTED table option is honored as written. */
            return table_iso == TDB_ISOLATION_REPEATABLE_READ ? TDB_ISOLATION_SNAPSHOT : table_iso;
        case ISO_SERIALIZABLE:
            return TDB_ISOLATION_SERIALIZABLE;
        default:
            return TDB_ISOLATION_READ_COMMITTED;
    }
}

/*
  Build a tidesdb_column_family_config_t from table options.
  Centralises the option-to-config mapping so create() and
  prepare_inplace_alter_table() stay in sync.
*/
tidesdb_column_family_config_t build_cf_config(const ha_table_option_struct *opts)
{
    tidesdb_column_family_config_t cfg = tidesdb_default_column_family_config();
    if (!opts) return cfg;

    cfg.enable_bloom_filter = opts->bloom_filter ? 1 : 0;
    cfg.bloom_fpr = (double)opts->bloom_fpr / TIDESDB_BLOOM_FPR_DIVISOR;
    cfg.keep_values_inline = opts->keep_values_inline ? 1 : 0;
    cfg.btree_klog_block_size = (size_t)opts->btree_klog_block_size;
    cfg.default_isolation_level =
        (tidesdb_isolation_level_t)tdb_isolation_map[opts->isolation_level];
    cfg.level_size_ratio = (size_t)opts->level_size_ratio;
    cfg.min_levels = (int)opts->min_levels;
    cfg.dividing_level_offset = (int)opts->dividing_level_offset;
    cfg.l1_file_count_trigger = (int)opts->l1_file_count_trigger;
    cfg.tombstone_density_trigger =
        (double)opts->tombstone_density_trigger / TIDESDB_TOMBSTONE_DENSITY_DIVISOR;
    cfg.tombstone_density_min_entries = (uint64_t)opts->tombstone_density_min_entries;

    /* compression is the first stage of the encoding pipeline in TidesDB 10; other
       per-cf knobs (write buffer, sync mode, skip list, l0 stall) moved to the db-level
       tidesdb_config_t and are applied at open. */
    int comp = tdb_compression_map[opts->compression];
    if (comp != TDB_COMPRESS_NONE)
    {
        cfg.encoding_pipeline[0] = (uint8_t)comp;
        cfg.encoding_count = 1;
    }
    else
    {
        cfg.encoding_count = 0;
    }
    return cfg;
}

/* Rows in a table's data column family are encrypted in serialize_row before
   they reach the library, and ciphertext does not compress, so running the
   block and value-log compressor over it on every flush and compaction spends
   CPU for no space saving.  Return a copy of the config with compression forced
   off when the table is encrypted, leaving every other setting untouched.  This
   is for the data CF only -- secondary-index CFs hold unencrypted comparable
   keys and keep whatever compression the table selected. */
tidesdb_column_family_config_t data_cf_config(const tidesdb_column_family_config_t &cfg,
                                              bool encrypted)
{
    tidesdb_column_family_config_t data = cfg;
    if (encrypted) data.encoding_count = 0; /* ciphertext does not compress */
    return data;
}

/*
  Resolve a secondary index CF by name.
  Returns the CF pointer (may be NULL if not found).
  Writes the CF name into out_name.
*/
tidesdb_column_family_t *resolve_idx_cf(tidesdb_t *db, const std::string &table_cf,
                                        const char *key_name, std::string &out_name)
{
    out_name = table_cf + CF_INDEX_INFIX + key_name;
    return tidesdb_get_column_family(db, out_name.c_str());
}
