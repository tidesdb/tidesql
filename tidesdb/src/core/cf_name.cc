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

#include "cf_name.h"

namespace tidesdb
{
namespace cf_name
{

table_path split_table_path(const char *path)
{
    std::string p(path);

    if (p.size() >= REL_PATH_PREFIX_LEN && p.compare(0, REL_PATH_PREFIX_LEN, REL_PATH_PREFIX) == 0)
        p = p.substr(REL_PATH_PREFIX_LEN);

    size_t last_slash = p.rfind('/');
    if (last_slash == std::string::npos) return {std::string(), p, false};

    std::string table = p.substr(last_slash + 1);

    /* the database is the component just before the table, whether the path is one deep
     * ("db/table") or carries a datadir prefix ("/var/lib/db/table"). */
    size_t prev_slash = (last_slash > 0) ? p.rfind('/', last_slash - 1) : std::string::npos;
    std::string db;
    if (prev_slash == std::string::npos)
        db = p.substr(0, last_slash);
    else
        db = p.substr(prev_slash + 1, last_slash - prev_slash - 1);

    return {db, table, true};
}

std::string to_cf_name(const char *path)
{
    table_path tp = split_table_path(path);
    /* a path with no directory is the degenerate case; return the remainder as it was. */
    if (!tp.has_dir) return tp.table;

    std::string result = tp.db + DB_TABLE_SEP + tp.table;
    for (size_t i = 0; i < result.size(); i++)
        if (result[i] == TEMP_NAME_MARKER) result[i] = TEMP_NAME_REPLACEMENT;
    return result;
}

}  // namespace cf_name
}  // namespace tidesdb
