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

#include "spatial.h"

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstring>

namespace tidesdb
{
namespace spatial
{

/* read and write eight-byte doubles in native order. the server stores geometry
 * coordinates and mbr corners as native doubles, so a plain memcpy reproduces its
 * float8get and float8store on every platform the plugin targets. */
static inline double read_double(const uint8_t *p)
{
    double v;
    std::memcpy(&v, p, DOUBLE_SIZE);
    return v;
}

static inline void write_double(uint8_t *p, double v)
{
    std::memcpy(p, &v, DOUBLE_SIZE);
}

uint32_t double_to_lex_uint32(double val)
{
    uint64_t bits;
    std::memcpy(&bits, &val, sizeof(bits));
    if (bits & IEEE754_DOUBLE_SIGN_MASK)
        bits = ~bits; /* negative, flip every bit so it sorts below any positive */
    else
        bits ^= IEEE754_DOUBLE_SIGN_MASK; /* positive, flip only the sign bit */
    return (uint32_t)(bits >> LEX_UINT32_HI_SHIFT);
}

/* inner reflection of the iterative hilbert transform. rx and ry are the quadrant flags
 * for the current level and n is the sub-grid size the caller passes; the (n - 1)
 * complement reflects a coordinate about the centre of that sub-grid. this reflection is
 * paired with decompose_ranges below, which depends on a coarse cell corner mapping to
 * the base of its contiguous hilbert interval, so the two must change together. */
static inline void hilbert_rot(uint32_t n, uint32_t *x, uint32_t *y, uint32_t rx, uint32_t ry)
{
    if (ry == 0)
    {
        if (rx == 1)
        {
            *x = n - 1 - *x;
            *y = n - 1 - *y;
        }
        uint32_t t = *x;
        *x = *y;
        *y = t;
    }
}

uint64_t hilbert_xy2d_64(uint32_t x, uint32_t y)
{
    uint64_t d = 0;
    for (uint64_t s = HILBERT_N >> 1; s > 0; s >>= 1)
    {
        uint32_t rx = (x & s) > 0 ? 1 : 0;
        uint32_t ry = (y & s) > 0 ? 1 : 0;
        d += s * s * (uint64_t)((3 * rx) ^ ry);
        hilbert_rot((uint32_t)s << 1, &x, &y, rx, ry);
    }
    return d;
}

void encode_hilbert_be(uint64_t h, uint8_t *out)
{
    for (unsigned i = 0; i < HILBERT_KEY_LEN; i++)
        out[i] = (uint8_t)(h >> ((HILBERT_KEY_LEN - 1 - i) * BITS_PER_BYTE));
}

uint64_t decode_hilbert_be(const uint8_t *in)
{
    uint64_t h = 0;
    for (unsigned i = 0; i < HILBERT_KEY_LEN; i++) h = (h << BITS_PER_BYTE) | (uint64_t)in[i];
    return h;
}

/* read one coordinate pair and grow the running rectangle, skipping non-finite values,
 * and advance p past the pair. bounds are compared as byte counts so a short buffer never
 * forms a pointer beyond one-past-the-end. */
static inline bool wkb_read_point(const uint8_t *&p, const uint8_t *end, mbr_t *m)
{
    if ((size_t)(end - p) < POINT_DATA_SIZE) return false;
    double x = read_double(p);
    double y = read_double(p + DOUBLE_SIZE);
    p += POINT_DATA_SIZE;
    if (std::isfinite(x) && std::isfinite(y))
    {
        if (x < m->xmin) m->xmin = x;
        if (x > m->xmax) m->xmax = x;
        if (y < m->ymin) m->ymin = y;
        if (y > m->ymax) m->ymax = y;
    }
    return true;
}

/* read a counted point sequence, used by a linestring and by each polygon ring. */
static inline bool wkb_read_point_sequence(const uint8_t *&p, const uint8_t *end, mbr_t *m)
{
    if ((size_t)(end - p) < WKB_COUNT_SIZE) return false;
    uint32_t n_pts;
    std::memcpy(&n_pts, p, WKB_COUNT_SIZE);
    p += WKB_COUNT_SIZE;
    if (n_pts > WKB_MAX_POINTS) return false;
    for (uint32_t i = 0; i < n_pts; i++)
    {
        if (!wkb_read_point(p, end, m)) return false;
    }
    return true;
}

/* parse one geometry object, growing the rectangle to cover its coordinates and advancing
 * p past it. depth bounds the descent into nested collections. the leading byte-order
 * marker is skipped and native order is assumed, matching how the server stores wkb. */
static bool wkb_parse_geometry(const uint8_t *&p, const uint8_t *end, mbr_t *m, int depth)
{
    if (depth > WKB_MAX_RECURSION_DEPTH) return false;
    if ((size_t)(end - p) < WKB_HEADER_SIZE) return false;
    p++; /* skip the byte-order marker */
    uint32_t gt;
    std::memcpy(&gt, p, WKB_COUNT_SIZE);
    p += WKB_COUNT_SIZE;

    switch (gt)
    {
        case WKB_POINT:
            return wkb_read_point(p, end, m);

        case WKB_LINESTRING:
            return wkb_read_point_sequence(p, end, m);

        case WKB_POLYGON:
        {
            if ((size_t)(end - p) < WKB_COUNT_SIZE) return false;
            uint32_t n_rings;
            std::memcpy(&n_rings, p, WKB_COUNT_SIZE);
            p += WKB_COUNT_SIZE;
            if (n_rings > WKB_MAX_RINGS) return false;
            for (uint32_t r = 0; r < n_rings; r++)
            {
                if (!wkb_read_point_sequence(p, end, m)) return false;
            }
            return true;
        }

        case WKB_MULTIPOINT:
        case WKB_MULTILINESTRING:
        case WKB_MULTIPOLYGON:
        case WKB_GEOMETRYCOLLECTION:
        {
            if ((size_t)(end - p) < WKB_COUNT_SIZE) return false;
            uint32_t n_geoms;
            std::memcpy(&n_geoms, p, WKB_COUNT_SIZE);
            p += WKB_COUNT_SIZE;
            if (n_geoms > WKB_MAX_GEOMS) return false;
            for (uint32_t i = 0; i < n_geoms; i++)
            {
                if (!wkb_parse_geometry(p, end, m, depth + 1)) return false;
            }
            return true;
        }

        default:
            return false;
    }
}

bool compute_mbr(const uint8_t *data, size_t len, mbr_t *out)
{
    if (len < SRID_SIZE + WKB_HEADER_SIZE) return false;

    const uint8_t *p = data + SRID_SIZE;
    const uint8_t *end = data + len;

    out->xmin = out->ymin = DBL_MAX;
    out->xmax = out->ymax = -DBL_MAX;

    if (!wkb_parse_geometry(p, end, out, 0)) return false;

    return out->xmin <= out->xmax && out->ymin <= out->ymax;
}

uint32_t build_key(double cx, double cy, const uint8_t *pk, uint32_t pk_len, uint8_t *out)
{
    uint32_t qx = double_to_lex_uint32(cx);
    uint32_t qy = double_to_lex_uint32(cy);
    uint64_t h = hilbert_xy2d_64(qx, qy);
    encode_hilbert_be(h, out);
    std::memcpy(out + HILBERT_KEY_LEN, pk, pk_len);
    return HILBERT_KEY_LEN + pk_len;
}

void build_value(const mbr_t &m, uint8_t *out)
{
    write_double(out, m.xmin);
    write_double(out + DOUBLE_SIZE, m.ymin);
    write_double(out + 2 * DOUBLE_SIZE, m.xmax);
    write_double(out + 3 * DOUBLE_SIZE, m.ymax);
}

void parse_query_mbr(const uint8_t *key, mbr_t *out)
{
    /* the server hands the query rectangle in the order xmin, xmax, ymin, ymax. */
    out->xmin = read_double(key);
    out->xmax = read_double(key + DOUBLE_SIZE);
    out->ymin = read_double(key + 2 * DOUBLE_SIZE);
    out->ymax = read_double(key + 3 * DOUBLE_SIZE);
    if (out->xmin > out->xmax) std::swap(out->xmin, out->xmax);
    if (out->ymin > out->ymax) std::swap(out->ymin, out->ymax);
}

static inline bool mbr_intersects(const mbr_t &a, const mbr_t &b)
{
    return !(a.xmax < b.xmin || a.xmin > b.xmax || a.ymax < b.ymin || a.ymin > b.ymax);
}

static inline bool mbr_within(const mbr_t &a, const mbr_t &b)
{
    return a.xmin >= b.xmin && a.xmax <= b.xmax && a.ymin >= b.ymin && a.ymax <= b.ymax;
}

static inline bool mbr_equals(const mbr_t &a, const mbr_t &b)
{
    return a.xmin == b.xmin && a.xmax == b.xmax && a.ymin == b.ymin && a.ymax == b.ymax;
}

bool mbr_predicate(predicate mode, const mbr_t &query, const mbr_t &entry)
{
    /* contain and within both reduce to the row rectangle lying inside the query
     * rectangle once the server has normalised its argument order, so they share a
     * within test; intersect is symmetric and disjoint is its negation. */
    switch (mode)
    {
        case predicate::intersect:
            return mbr_intersects(entry, query);
        case predicate::contain:
            return mbr_within(entry, query);
        case predicate::within:
            return mbr_within(entry, query);
        case predicate::equal:
            return mbr_equals(entry, query);
        case predicate::disjoint:
            return !mbr_intersects(entry, query);
        case predicate::unsupported:
            return false;
    }
    return false;
}

void decompose_ranges(uint32_t qx_min, uint32_t qy_min, uint32_t qx_max, uint32_t qy_max,
                      std::vector<std::pair<uint64_t, uint64_t>> &out)
{
    out.clear();

    unsigned shift = HILBERT_ORDER - DECOMP_BITS;
    unsigned gx0 = qx_min >> shift;
    unsigned gy0 = qy_min >> shift;
    unsigned gx1 = qx_max >> shift;
    unsigned gy1 = qy_max >> shift;

    /* a 32-bit coordinate shifted right by HILBERT_ORDER - DECOMP_BITS lands in
     * [0, DECOMP_N), and callers pass min <= max, so the coarse cell window is always a
     * valid non-empty rectangle inside the grid. */
    assert(gx0 <= gx1 && gy0 <= gy1);
    assert(gx1 < DECOMP_N && gy1 < DECOMP_N);

    /* a box covering many cells is cheaper to scan as one full-curve range than to
     * enumerate and sort, since the read-side mbr filter drops the extra rows anyway. */
    const uint64_t cell_count = (uint64_t)(gx1 - gx0 + 1) * (uint64_t)(gy1 - gy0 + 1);
    if (cell_count > DECOMP_FULL_SCAN_THRESHOLD)
    {
        out.push_back({RANGE_FULL_LO, RANGE_FULL_HI});
        return;
    }

    /* one coarse cell covers a contiguous run of fine hilbert distances of this width. */
    const uint64_t cell_span = (uint64_t)1 << (HILBERT_DIM * shift);

    std::vector<uint64_t> cells;
    cells.reserve((size_t)cell_count);
    for (unsigned gx = gx0; gx <= gx1; gx++)
    {
        for (unsigned gy = gy0; gy <= gy1; gy++)
        {
            /* every hilbert distance splits into a coarse-cell index times cell_span plus a
             * within-cell offset below cell_span, because each of the top DECOMP_BITS
             * transform terms is a multiple of cell_span and each lower term is smaller than
             * it. the corner distance therefore is not the cell base unless its own offset is
             * zero, which this reflection does not guarantee, so snap it down to the cell base
             * that bounds every point in the cell. */
            uint64_t corner = hilbert_xy2d_64(gx << shift, gy << shift);
            cells.push_back(corner / cell_span * cell_span);
        }
    }

    std::sort(cells.begin(), cells.end());

    uint64_t range_lo = cells[0];
    uint64_t range_hi = cells[0] + cell_span - 1;

    for (size_t i = 1; i < cells.size(); i++)
    {
        uint64_t lo = cells[i];
        uint64_t hi = cells[i] + cell_span - 1;

        if (lo <= range_hi + 1)
        {
            if (hi > range_hi) range_hi = hi;
        }
        else
        {
            out.push_back({range_lo, range_hi});
            range_lo = lo;
            range_hi = hi;
        }
    }
    out.push_back({range_lo, range_hi});
}

}  // namespace spatial
}  // namespace tidesdb
