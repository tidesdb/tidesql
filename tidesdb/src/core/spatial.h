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

/* server-free core for the spatial index. everything here is plain geometry and
 * hilbert-curve math with no dependency on the mysql or mariadb server headers, so
 * it links into the plugin and into the standalone unit tests unchanged. the handler
 * translates server types (Field, KEY, ha_rkey_function) into the plain buffers and
 * the predicate enum below, then calls into this module. */
#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace tidesdb
{
namespace spatial
{

/* bits per axis fed to the hilbert transform, so the curve has 32-bit precision on
 * each of x and y and a 64-bit distance. hilbert_xy2d_64 and decompose_ranges are a
 * matched pair, the reflection chosen so a coarse cell corner maps to the base of its
 * contiguous hilbert interval, so this order and that transform change together. */
static constexpr unsigned HILBERT_ORDER = 32;
static constexpr unsigned HILBERT_DIM = 2;
static constexpr uint64_t HILBERT_N = (uint64_t)1 << HILBERT_ORDER;

/* the stored spatial key is the 8-byte big-endian hilbert distance followed by the
 * primary key bytes; the stored value is the four mbr corners as native doubles. */
static constexpr unsigned HILBERT_KEY_LEN = 8;
static constexpr unsigned MBR_VALUE_LEN = 32;

/* inclusive bounds of the whole 64-bit hilbert space, used when a query has no
 * decomposable cells and the scan has to walk the entire curve. */
static constexpr uint64_t RANGE_FULL_LO = 0;
static constexpr uint64_t RANGE_FULL_HI = UINT64_MAX;

/* ieee-754 double layout the lexicographic-orderable coordinate encoding relies on.
 * the sign bit is bit 63; the high 32 bits carry enough precision for the grid. */
static constexpr uint64_t IEEE754_DOUBLE_SIGN_MASK = (uint64_t)1 << 63;
static constexpr unsigned LEX_UINT32_HI_SHIFT = 32;
static constexpr unsigned BITS_PER_BYTE = 8;

/* ogc well-known-binary geometry type tags. */
static constexpr uint32_t WKB_POINT = 1;
static constexpr uint32_t WKB_LINESTRING = 2;
static constexpr uint32_t WKB_POLYGON = 3;
static constexpr uint32_t WKB_MULTIPOINT = 4;
static constexpr uint32_t WKB_MULTILINESTRING = 5;
static constexpr uint32_t WKB_MULTIPOLYGON = 6;
static constexpr uint32_t WKB_GEOMETRYCOLLECTION = 7;

/* sanity caps that reject malformed wkb before it drives a huge loop or allocation. */
static constexpr uint32_t WKB_MAX_POINTS = 1000000;
static constexpr uint32_t WKB_MAX_RINGS = 10000;
static constexpr uint32_t WKB_MAX_GEOMS = 100000;

/* deepest a geometrycollection or multi geometry may nest before the parser gives up,
 * bounding the recursive descent well above any geometry a server would accept. */
static constexpr int WKB_MAX_RECURSION_DEPTH = 32;

/* a geometry field's raw value is a 4-byte srid prefix followed by wkb whose header is
 * one byte-order marker plus a 4-byte type; a point body is two 8-byte doubles. */
static constexpr unsigned SRID_SIZE = 4;
static constexpr unsigned WKB_HEADER_SIZE = 5;
static constexpr unsigned WKB_COUNT_SIZE = sizeof(uint32_t);
static constexpr unsigned POINT_DATA_SIZE = 16;
static constexpr unsigned DOUBLE_SIZE = sizeof(double);

/* divisor turning an mbr edge pair into its centroid ((min + max) / 2); the centroid is
 * the point that feeds the hilbert key, while the corners live in the stored value. */
static constexpr double MBR_CENTROID_DIV = 2.0;

/* resolution of the query-box decomposition. at DECOMP_BITS per axis the space is a
 * 2^DECOMP_BITS grid; more bits mean tighter ranges but more of them. */
static constexpr unsigned DECOMP_BITS = 8;
static constexpr unsigned DECOMP_N = 1u << DECOMP_BITS;
static_assert(DECOMP_BITS < HILBERT_ORDER, "DECOMP_BITS must stay below HILBERT_ORDER");

/* above this many covered cells the decomposition gives up and asks for one full-curve
 * range instead, since the read-side mbr post-filter drops the extra rows anyway and a
 * whole-universe box would otherwise enumerate and sort tens of thousands of cells. */
static constexpr unsigned DECOMP_FULL_SCAN_THRESHOLD = DECOMP_N * 16;

/**
 * mbr_t
 * an axis-aligned minimum bounding rectangle in the same corner order the stored value
 * uses, so a parsed row mbr and a parsed query mbr compare directly
 * @param xmin the low x edge
 * @param ymin the low y edge
 * @param xmax the high x edge
 * @param ymax the high y edge
 */
struct mbr_t
{
    double xmin, ymin, xmax, ymax;
};

/**
 * predicate
 * the spatial match the caller wants, decoupled from the server's ha_rkey_function so
 * this module carries no server enum; the handler maps HA_READ_MBR_* onto these and
 * passes anything it cannot map as unsupported
 */
enum class predicate
{
    intersect,
    contain,
    within,
    equal,
    disjoint,
    unsupported
};

/**
 * double_to_lex_uint32
 * fold a double into a uint32 whose unsigned order matches the double's numeric order,
 * so quantized coordinates sort correctly and negatives sort below positives
 * @param val a finite coordinate; non-finite input yields an unspecified but total order
 * @return the high 32 bits of the order-preserving transform of val
 */
uint32_t double_to_lex_uint32(double val);

/**
 * hilbert_xy2d_64
 * map a quantized 2d point to its distance along the order-32 hilbert curve so nearby
 * points cluster under byte order; this mapping is a frozen on-disk contract
 * @param x the x coordinate on the full 32-bit grid
 * @param y the y coordinate on the full 32-bit grid
 * @return the 64-bit hilbert distance
 */
uint64_t hilbert_xy2d_64(uint32_t x, uint32_t y);

/**
 * encode_hilbert_be
 * write a hilbert distance as 8 big-endian bytes so memcmp on the key matches numeric order
 * @param h the distance to encode
 * @param out a buffer of at least HILBERT_KEY_LEN bytes
 */
void encode_hilbert_be(uint64_t h, uint8_t *out);

/**
 * decode_hilbert_be
 * read back an 8-byte big-endian hilbert distance
 * @param in a buffer of at least HILBERT_KEY_LEN bytes
 * @return the decoded distance
 */
uint64_t decode_hilbert_be(const uint8_t *in);

/**
 * compute_mbr
 * derive the bounding rectangle of a geometry field value, walking all seven ogc types
 * and ignoring non-finite coordinates
 * @param data the raw field bytes, a 4-byte srid prefix followed by native-order wkb
 * @param len the length of data
 * @param out receives the bounding rectangle on success
 * @return true when the geometry parsed and produced a non-empty rectangle, false on
 *         malformed input or a geometry with no finite coordinates
 */
bool compute_mbr(const uint8_t *data, size_t len, mbr_t *out);

/**
 * build_key
 * assemble a spatial index key as the big-endian hilbert distance of the centroid
 * followed by the row's primary key
 * @param cx the centroid x
 * @param cy the centroid y
 * @param pk the primary key bytes to append
 * @param pk_len the primary key length
 * @param out a buffer of at least HILBERT_KEY_LEN + pk_len bytes
 * @return the total key length written
 */
uint32_t build_key(double cx, double cy, const uint8_t *pk, uint32_t pk_len, uint8_t *out);

/**
 * build_value
 * write the four mbr corners as native doubles into the stored spatial value
 * @param m the rectangle to store
 * @param out a buffer of at least MBR_VALUE_LEN bytes
 */
void build_value(const mbr_t &m, uint8_t *out);

/**
 * parse_query_mbr
 * read a query rectangle out of the server's spatial key buffer, whose corner order is
 * xmin, xmax, ymin, ymax, normalising any inverted corner so callers see min <= max
 * @param key a buffer of at least MBR_VALUE_LEN bytes in the server's corner order
 * @param out receives the normalised rectangle
 */
void parse_query_mbr(const uint8_t *key, mbr_t *out);

/**
 * mbr_predicate
 * test a stored row rectangle against a query rectangle under the requested spatial match
 * @param mode the spatial relation to test; unsupported always fails
 * @param query the query rectangle
 * @param entry the stored row rectangle
 * @return true when entry relates to query as mode requires
 */
bool mbr_predicate(predicate mode, const mbr_t &query, const mbr_t &entry);

/**
 * decompose_ranges
 * cover a quantized query box with a small set of non-overlapping hilbert ranges by
 * enumerating the covered coarse cells, mapping each to its contiguous hilbert interval,
 * and merging touching intervals; falls back to one full-curve range for a wide box
 * @param qx_min the quantized low x of the query box
 * @param qy_min the quantized low y of the query box
 * @param qx_max the quantized high x of the query box, at least qx_min
 * @param qy_max the quantized high y of the query box, at least qy_min
 * @param out cleared then filled with inclusive [lo, hi] hilbert ranges to scan
 */
void decompose_ranges(uint32_t qx_min, uint32_t qy_min, uint32_t qx_max, uint32_t qy_max,
                      std::vector<std::pair<uint64_t, uint64_t>> &out);

}  // namespace spatial
}  // namespace tidesdb
