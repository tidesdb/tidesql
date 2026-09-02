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

#ifndef HA_TIDESDB_SPATIAL_H
#define HA_TIDESDB_SPATIAL_H

/* server-coupled spatial adapters for the tidesdb handler. the order-preserving hilbert curve,
   the wkb bounding-box walk, and the corner-order predicates all live in the server-free core
   module tidesdb::spatial; this header exposes just the thin adapters that touch server types
   (the KEY flags, the server's mbr key buffer, and ha_rkey_function) plus pass-throughs so the
   handler keeps calling one spatial surface. include after the server headers and ha_tidesdb.h. */

#include <vector>

#include "src/core/spatial.h"

/* mariadb renamed HA_SPATIAL to HA_SPATIAL_legacy after the 11.x series; the key-flag bit is
   unchanged, and spatial keys carry it on every version, whereas KEY::algorithm is only set to
   HA_KEY_ALG_RTREE on newer servers, so detection keys off the flag. */
#ifndef HA_SPATIAL
#define HA_SPATIAL HA_SPATIAL_legacy
#endif

/* the handler's rectangle is exactly the core rectangle, in the corner order the stored value
   uses, so a parsed row mbr and a parsed query mbr compare directly. */
using tdb_mbr_t = tidesdb::spatial::mbr_t;

/* on-disk spatial key and value widths, shared with the core contract. the mbr centroid divisor
   lives in ha_tidesdb.h alongside the other row-layout constants. */
static constexpr unsigned SPATIAL_HILBERT_KEY_LEN = tidesdb::spatial::HILBERT_KEY_LEN;
static constexpr unsigned SPATIAL_MBR_VALUE_LEN = tidesdb::spatial::MBR_VALUE_LEN;

/* whether a key is a spatial (rtree) index. */
static inline bool is_spatial_index(const KEY *ki)
{
    return (ki->flags & HA_SPATIAL) || ki->algorithm == HA_KEY_ALG_RTREE;
}

/* pass-throughs to the core curve so callers keep one spatial vocabulary; these carry no server
   type and add no logic, they only spare every call site an explicit namespace qualification. */
static inline uint32_t double_to_lex_uint32(double val)
{
    return tidesdb::spatial::double_to_lex_uint32(val);
}

static inline void encode_hilbert_be(uint64_t h, uchar *out)
{
    tidesdb::spatial::encode_hilbert_be(h, out);
}

static inline uint64_t decode_hilbert_be(const uchar *in)
{
    return tidesdb::spatial::decode_hilbert_be(in);
}

static inline void spatial_decompose_ranges(uint32_t qx_min, uint32_t qy_min, uint32_t qx_max,
                                            uint32_t qy_max,
                                            std::vector<std::pair<uint64_t, uint64_t>> &out)
{
    tidesdb::spatial::decompose_ranges(qx_min, qy_min, qx_max, qy_max, out);
}

/**
 * spatial_compute_mbr
 * derive the bounding rectangle of a geometry field value into four separate corners
 * @param data the raw field bytes, a 4-byte srid prefix followed by native-order wkb
 * @param len the length of data
 * @param xmin receives the low x edge on success
 * @param ymin receives the low y edge on success
 * @param xmax receives the high x edge on success
 * @param ymax receives the high y edge on success
 * @return true when the geometry parsed into a non-empty rectangle, false on malformed input
 */
bool spatial_compute_mbr(const uchar *data, size_t len, double *xmin, double *ymin, double *xmax,
                         double *ymax);

/**
 * spatial_build_key
 * assemble a spatial index key as the big-endian hilbert distance of the centroid then the pk
 * @param cx the centroid x
 * @param cy the centroid y
 * @param pk the primary key bytes to append
 * @param pk_len the primary key length
 * @param out a buffer of at least SPATIAL_HILBERT_KEY_LEN + pk_len bytes
 * @return the total key length written
 */
uint spatial_build_key(double cx, double cy, const uchar *pk, uint pk_len, uchar *out);

/**
 * spatial_build_value
 * write the four mbr corners as native doubles into the stored spatial value
 * @param xmin the low x edge
 * @param ymin the low y edge
 * @param xmax the high x edge
 * @param ymax the high y edge
 * @param out a buffer of at least SPATIAL_MBR_VALUE_LEN bytes
 */
void spatial_build_value(double xmin, double ymin, double xmax, double ymax, uchar *out);

/**
 * spatial_parse_query_mbr
 * read a query rectangle out of the server's spatial key buffer, normalising inverted corners
 * @param key a buffer of at least SPATIAL_MBR_VALUE_LEN bytes in the server's corner order
 * @param mbr receives the normalised rectangle
 */
void spatial_parse_query_mbr(const uchar *key, tdb_mbr_t *mbr);

/**
 * spatial_mbr_predicate
 * test a stored row rectangle against a query rectangle under a server spatial relation
 * @param mode the ha_rkey_function spatial relation to test; an unmapped mode fails
 * @param query the query rectangle
 * @param entry the stored row rectangle
 * @return true when entry relates to query as mode requires
 */
bool spatial_mbr_predicate(enum ha_rkey_function mode, const tdb_mbr_t *query,
                           const tdb_mbr_t *entry);

#endif /* HA_TIDESDB_SPATIAL_H */
