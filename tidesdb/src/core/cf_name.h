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

/* server-free core for turning a server table path into the engine's column-family name and
 * its schema-key. the path parsing is pure string work, shared here by both formatters so
 * the two can no longer drift. the prefix and marker conventions below match how mariadb
 * hands paths to a handler; the compat layer revisits them when the mysql target lands. */
#pragma once

#include <cstddef>
#include <string>

namespace tidesdb
{
namespace cf_name
{

/* a table path arrives relative, as "./db/table". */
static constexpr const char REL_PATH_PREFIX[] = "./";
static constexpr size_t REL_PATH_PREFIX_LEN = 2;

/* a column-family name joins the database and table with this infix. */
static constexpr const char DB_TABLE_SEP[] = "__";

/* an internal temp or exchange table path embeds this marker, which a column-family name
 * substitutes so the name stays a valid identifier. */
static constexpr char TEMP_NAME_MARKER = '#';
static constexpr char TEMP_NAME_REPLACEMENT = '_';

/**
 * table_path
 * the database and table parsed out of a server path
 * @param db the database component, empty when the path has no directory
 * @param table the table component, or the whole remainder when the path has no directory
 * @param has_dir whether the path carried a directory component, which the formatters use to
 *                reproduce their historical no-directory behaviour exactly
 */
struct table_path
{
    std::string db;
    std::string table;
    bool has_dir;
};

/**
 * split_table_path
 * parse a server table path into its database and table, stripping the relative prefix
 * @param path the server path, such as "./db/table"
 * @return the parsed components, with has_dir false and table set to the whole remainder when
 *         the path holds no slash
 */
table_path split_table_path(const char *path);

/**
 * to_cf_name
 * the column-family name for a table path, database and table joined by the infix with temp
 * markers substituted; a path with no directory is returned unchanged, as before
 * @param path the server table path
 * @return the column-family name
 */
std::string to_cf_name(const char *path);

}  // namespace cf_name
}  // namespace tidesdb
