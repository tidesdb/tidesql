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

/* server-coupled spatial adapters. each one bridges a server type onto the server-free
   tidesdb::spatial core so the curve, wkb walk, and predicates have a single implementation. */

#include "ha_tidesdb.h"

#include "src/handler/ha_tidesdb_spatial.h"

bool spatial_compute_mbr(const uchar *data, size_t len, double *xmin, double *ymin, double *xmax,
                         double *ymax)
{
    tidesdb::spatial::mbr_t m;
    if (!tidesdb::spatial::compute_mbr(data, len, &m)) return false;
    *xmin = m.xmin;
    *ymin = m.ymin;
    *xmax = m.xmax;
    *ymax = m.ymax;
    return true;
}

uint spatial_build_key(double cx, double cy, const uchar *pk, uint pk_len, uchar *out)
{
    return tidesdb::spatial::build_key(cx, cy, pk, pk_len, out);
}

void spatial_build_value(double xmin, double ymin, double xmax, double ymax, uchar *out)
{
    const tidesdb::spatial::mbr_t m{xmin, ymin, xmax, ymax};
    tidesdb::spatial::build_value(m, out);
}

void spatial_parse_query_mbr(const uchar *key, tdb_mbr_t *mbr)
{
    tidesdb::spatial::parse_query_mbr(key, mbr);
}

bool spatial_mbr_predicate(enum ha_rkey_function mode, const tdb_mbr_t *query,
                           const tdb_mbr_t *entry)
{
    tidesdb::spatial::predicate p;
    switch (mode)
    {
        case HA_READ_MBR_INTERSECT:
            p = tidesdb::spatial::predicate::intersect;
            break;
        case HA_READ_MBR_CONTAIN:
            p = tidesdb::spatial::predicate::contain;
            break;
        case HA_READ_MBR_WITHIN:
            p = tidesdb::spatial::predicate::within;
            break;
        case HA_READ_MBR_EQUAL:
            p = tidesdb::spatial::predicate::equal;
            break;
        case HA_READ_MBR_DISJOINT:
            p = tidesdb::spatial::predicate::disjoint;
            break;
        default:
            p = tidesdb::spatial::predicate::unsupported;
            break;
    }
    return tidesdb::spatial::mbr_predicate(p, *query, *entry);
}
