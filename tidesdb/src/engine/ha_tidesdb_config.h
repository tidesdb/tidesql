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

#ifndef HA_TIDESDB_CONFIG_H
#define HA_TIDESDB_CONFIG_H

/* translation of CREATE TABLE options and the session isolation level into the
   library column-family configuration.  create, open, truncate, and online DDL
   all build their column families through these helpers so the option-to-config
   mapping stays in one place.  include after ha_tidesdb.h so the table option
   struct and library config types are already visible. */

/* number of choices in each option enum, and thus the length of each map below.  named here so the
   maps declare a complete bound and the root translation unit can bound-check a resolved index
   against the extern arrays. */
constexpr uint TDB_COMPRESSION_CHOICE_COUNT = 5;
constexpr uint TDB_ISOLATION_CHOICE_COUNT = 5;
constexpr uint TDB_SYNC_MODE_CHOICE_COUNT = 3;

/* enum-index to library-constant maps for the COMPRESSION, ISOLATION_LEVEL, and db-level sync-mode
   options.  defined in ha_tidesdb_config.cc; the root translation unit reads the isolation and
   sync-mode maps directly when resolving a share's level and the database sync mode. */
extern const int tdb_compression_map[TDB_COMPRESSION_CHOICE_COUNT];
extern const int tdb_isolation_map[TDB_ISOLATION_CHOICE_COUNT];
extern const int tdb_sync_mode_map[TDB_SYNC_MODE_CHOICE_COUNT];

/**
 * resolve_effective_isolation
 * pick the library isolation level for a statement from the session level and the table default
 * @param thd the current server session
 * @param table_iso the table-level ISOLATION_LEVEL option value
 * @return the resolved library isolation level
 */
tidesdb_isolation_level_t resolve_effective_isolation(THD *thd,
                                                      tidesdb_isolation_level_t table_iso);

/**
 * build_cf_config
 * build a library column-family config from a table's CREATE TABLE options
 * @param opts the table options, or NULL to take the library defaults
 * @return the populated column-family config
 */
tidesdb_column_family_config_t build_cf_config(const ha_table_option_struct *opts);

/**
 * data_cf_config
 * derive the data column family's config, forcing compression off when the table is encrypted
 * @param cfg the base config shared with the secondary-index families
 * @param encrypted true when the table stores rows encrypted
 * @return a copy of cfg with compression disabled for encrypted tables
 */
tidesdb_column_family_config_t data_cf_config(const tidesdb_column_family_config_t &cfg,
                                              bool encrypted);

/**
 * resolve_idx_cf
 * look up a secondary index column family by its derived name
 * @param db the open database handle
 * @param table_cf the table's base column-family name
 * @param key_name the index name
 * @param out_name receives the derived column-family name
 * @return the column family, or NULL when it does not exist
 */
tidesdb_column_family_t *resolve_idx_cf(tidesdb_t *db, const std::string &table_cf,
                                        const char *key_name, std::string &out_name);

#endif /* HA_TIDESDB_CONFIG_H */
