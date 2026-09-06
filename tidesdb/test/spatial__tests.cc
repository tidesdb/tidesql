/* Copyright (c) 2026 TidesDB Corp.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 */

/* unit tests for the server-free spatial core. the heavy tests prove the two properties
 * decompose_ranges leans on, that a coarse cell corner is the base of a contiguous
 * hilbert interval and that a decomposition never drops a point whose centroid sits
 * inside the query box. */
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "../src/core/spatial.h"
#include "test_utils.h"

using namespace tidesdb::spatial;

static int tests_passed = 0;
static int tests_failed = 0;

/* deterministic generator so the sampled tests reproduce exactly on every run. */
struct lcg
{
    uint64_t state;
    explicit lcg(uint64_t seed) : state(seed)
    {
    }
    uint64_t next()
    {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        return state >> 11;
    }
    uint32_t u32()
    {
        return (uint32_t)next();
    }
    /* a finite double in roughly [-1e6, 1e6] for coordinate sampling. */
    double coord()
    {
        double unit = (double)(next() & 0xFFFFFFFFULL) / (double)0xFFFFFFFFULL;
        return (unit - 0.5) * 2.0e6;
    }
};

/* build a minimal geometry field value, a 4-byte srid prefix then native-order wkb. */
static void put_u32(std::vector<uint8_t> &b, uint32_t v)
{
    uint8_t tmp[4];
    std::memcpy(tmp, &v, 4);
    b.insert(b.end(), tmp, tmp + 4);
}
static void put_double(std::vector<uint8_t> &b, double v)
{
    uint8_t tmp[8];
    std::memcpy(tmp, &v, 8);
    b.insert(b.end(), tmp, tmp + 8);
}
static void put_header(std::vector<uint8_t> &b, uint32_t type)
{
    const uint32_t probe = 1;
    b.push_back(*(const uint8_t *)&probe); /* native byte-order marker */
    put_u32(b, type);
}
static std::vector<uint8_t> geom_point(double x, double y)
{
    std::vector<uint8_t> b;
    put_u32(b, 0); /* srid */
    put_header(b, WKB_POINT);
    put_double(b, x);
    put_double(b, y);
    return b;
}
static std::vector<uint8_t> geom_linestring(const std::vector<std::pair<double, double>> &pts)
{
    std::vector<uint8_t> b;
    put_u32(b, 0);
    put_header(b, WKB_LINESTRING);
    put_u32(b, (uint32_t)pts.size());
    for (auto &p : pts)
    {
        put_double(b, p.first);
        put_double(b, p.second);
    }
    return b;
}

void test_double_to_lex_uint32_monotonic(void)
{
    const double vals[] = {-1e9, -1e6, -1.5, -1.0, -0.001, 0.0, 0.001, 1.0, 1.5, 1e6, 1e9};
    const int n = (int)(sizeof(vals) / sizeof(vals[0]));
    for (int i = 1; i < n; i++)
    {
        ASSERT_TRUE(double_to_lex_uint32(vals[i - 1]) <= double_to_lex_uint32(vals[i]));
    }
    /* strictly larger inputs that differ in the retained high bits stay strictly ordered. */
    ASSERT_TRUE(double_to_lex_uint32(-1.0) < double_to_lex_uint32(1.0));
    ASSERT_TRUE(double_to_lex_uint32(0.0) < double_to_lex_uint32(1e9));
}

void test_hilbert_be_roundtrip(void)
{
    lcg rng(1);
    for (int i = 0; i < 100000; i++)
    {
        uint64_t h = rng.next();
        uint8_t buf[HILBERT_KEY_LEN];
        encode_hilbert_be(h, buf);
        ASSERT_EQ(decode_hilbert_be(buf), h);
        /* big-endian means the most significant byte leads. */
        ASSERT_EQ(buf[0], (uint8_t)(h >> 56));
        ASSERT_EQ(buf[HILBERT_KEY_LEN - 1], (uint8_t)(h & 0xFF));
    }
}

/* the coarse-cell bases, each corner distance snapped down to its span multiple, must be
 * exactly the set {0, span, 2*span, ...}. that proves the coarse hilbert map is a
 * bijection and that snapping a corner down to its span multiple recovers a distinct cell
 * base for every cell, the invariant decompose_ranges relies on. */
void test_hilbert_coarse_cells_partition(void)
{
    const unsigned shift = HILBERT_ORDER - DECOMP_BITS;
    const uint64_t span = (uint64_t)1 << (HILBERT_DIM * shift);
    std::vector<uint64_t> bases;
    bases.reserve((size_t)DECOMP_N * DECOMP_N);
    for (unsigned gx = 0; gx < DECOMP_N; gx++)
        for (unsigned gy = 0; gy < DECOMP_N; gy++)
        {
            uint64_t corner = hilbert_xy2d_64(gx << shift, gy << shift);
            bases.push_back(corner / span * span);
        }
    std::sort(bases.begin(), bases.end());
    for (size_t i = 0; i < bases.size(); i++) ASSERT_EQ(bases[i], (uint64_t)i * span);
}

/* fine points inside a coarse cell must all fall inside that cell's contiguous interval
 * [base, base + span - 1], where base is the corner distance snapped down to a span
 * multiple. this is the property that makes decompose_ranges lossless. */
void test_hilbert_cell_contiguity(void)
{
    const unsigned shift = HILBERT_ORDER - DECOMP_BITS;
    const uint64_t span = (uint64_t)1 << (HILBERT_DIM * shift);
    lcg rng(7);
    for (int c = 0; c < 400; c++)
    {
        unsigned gx = rng.u32() % DECOMP_N;
        unsigned gy = rng.u32() % DECOMP_N;
        uint64_t base = hilbert_xy2d_64(gx << shift, gy << shift) / span * span;
        for (int s = 0; s < 500; s++)
        {
            uint32_t dx = rng.u32() & ((1u << shift) - 1);
            uint32_t dy = rng.u32() & ((1u << shift) - 1);
            uint64_t h = hilbert_xy2d_64((gx << shift) | dx, (gy << shift) | dy);
            ASSERT_TRUE(h >= base && h <= base + span - 1);
        }
    }
}

void test_compute_mbr_point(void)
{
    auto g = geom_point(3.0, -4.0);
    mbr_t m;
    ASSERT_TRUE(compute_mbr(g.data(), g.size(), &m));
    ASSERT_TRUE(m.xmin == 3.0 && m.xmax == 3.0 && m.ymin == -4.0 && m.ymax == -4.0);
}

void test_compute_mbr_linestring(void)
{
    auto g = geom_linestring({{1.0, 2.0}, {5.0, -1.0}, {-3.0, 4.0}});
    mbr_t m;
    ASSERT_TRUE(compute_mbr(g.data(), g.size(), &m));
    ASSERT_TRUE(m.xmin == -3.0 && m.xmax == 5.0 && m.ymin == -1.0 && m.ymax == 4.0);
}

/* a coordinate that is not finite is ignored, and a geometry with only such coordinates
 * has no rectangle and is rejected. */
void test_compute_mbr_skips_non_finite(void)
{
    double inf = HUGE_VAL;
    auto g = geom_linestring({{1.0, 1.0}, {inf, 2.0}, {3.0, 3.0}});
    mbr_t m;
    ASSERT_TRUE(compute_mbr(g.data(), g.size(), &m));
    ASSERT_TRUE(m.xmin == 1.0 && m.xmax == 3.0 && m.ymin == 1.0 && m.ymax == 3.0);

    auto bad = geom_point(inf, inf);
    ASSERT_FALSE(compute_mbr(bad.data(), bad.size(), &m));
}

/* truncated inputs of every length must be rejected without reading out of bounds, which
 * the sanitizer build enforces. */
void test_compute_mbr_rejects_truncated(void)
{
    auto g = geom_linestring({{1.0, 2.0}, {3.0, 4.0}});
    mbr_t m;
    for (size_t len = 0; len < g.size(); len++)
    {
        ASSERT_FALSE(compute_mbr(g.data(), len, &m));
    }
    ASSERT_TRUE(compute_mbr(g.data(), g.size(), &m));
}

/* a count field larger than the sanity cap is rejected rather than driving a huge loop. */
void test_compute_mbr_rejects_absurd_count(void)
{
    std::vector<uint8_t> b;
    put_u32(b, 0);
    put_header(b, WKB_LINESTRING);
    put_u32(b, WKB_MAX_POINTS + 1);
    put_double(b, 1.0);
    put_double(b, 2.0);
    mbr_t m;
    ASSERT_FALSE(compute_mbr(b.data(), b.size(), &m));
}

void test_build_value_roundtrip(void)
{
    mbr_t in = {-1.5, 2.5, 10.0, 20.0};
    uint8_t buf[MBR_VALUE_LEN];
    build_value(in, buf);
    double got[4];
    std::memcpy(got, buf, MBR_VALUE_LEN);
    ASSERT_TRUE(got[0] == in.xmin && got[1] == in.ymin && got[2] == in.xmax && got[3] == in.ymax);
}

/* the server hands corners as xmin, xmax, ymin, ymax and an inverted pair is normalised. */
void test_parse_query_mbr_normalizes(void)
{
    uint8_t buf[MBR_VALUE_LEN];
    double corners[4] = {10.0, -5.0, 8.0, 1.0}; /* xmin>xmax and ymin>ymax */
    std::memcpy(buf, corners, MBR_VALUE_LEN);
    mbr_t m;
    parse_query_mbr(buf, &m);
    ASSERT_TRUE(m.xmin == -5.0 && m.xmax == 10.0 && m.ymin == 1.0 && m.ymax == 8.0);
}

void test_mbr_predicate_matrix(void)
{
    mbr_t query = {0.0, 0.0, 10.0, 10.0};
    mbr_t inside = {2.0, 2.0, 3.0, 3.0};
    mbr_t overlap = {5.0, 5.0, 15.0, 15.0};
    mbr_t outside = {20.0, 20.0, 30.0, 30.0};
    mbr_t same = {0.0, 0.0, 10.0, 10.0};

    ASSERT_TRUE(mbr_predicate(predicate::intersect, query, inside));
    ASSERT_TRUE(mbr_predicate(predicate::intersect, query, overlap));
    ASSERT_FALSE(mbr_predicate(predicate::intersect, query, outside));

    ASSERT_TRUE(mbr_predicate(predicate::within, query, inside));
    ASSERT_FALSE(mbr_predicate(predicate::within, query, overlap));

    ASSERT_TRUE(mbr_predicate(predicate::contain, query, inside));

    ASSERT_TRUE(mbr_predicate(predicate::equal, query, same));
    ASSERT_FALSE(mbr_predicate(predicate::equal, query, inside));

    ASSERT_TRUE(mbr_predicate(predicate::disjoint, query, outside));
    ASSERT_FALSE(mbr_predicate(predicate::disjoint, query, inside));

    ASSERT_FALSE(mbr_predicate(predicate::unsupported, query, inside));
}

/* ranges come back sorted and non-overlapping so a scan can walk them in order. */
static void assert_sorted_disjoint(const std::vector<std::pair<uint64_t, uint64_t>> &r)
{
    for (size_t i = 0; i < r.size(); i++)
    {
        ASSERT_TRUE(r[i].first <= r[i].second);
        if (i > 0) ASSERT_TRUE(r[i - 1].second < r[i].first);
    }
}

void test_decompose_full_scan_fallback(void)
{
    std::vector<std::pair<uint64_t, uint64_t>> r;
    decompose_ranges(0, 0, UINT32_MAX, UINT32_MAX, r);
    ASSERT_EQ(r.size(), (size_t)1);
    ASSERT_EQ(r[0].first, RANGE_FULL_LO);
    ASSERT_EQ(r[0].second, RANGE_FULL_HI);
}

void test_decompose_single_cell(void)
{
    std::vector<std::pair<uint64_t, uint64_t>> r;
    decompose_ranges(0, 0, 1, 1, r);
    ASSERT_TRUE(!r.empty());
    assert_sorted_disjoint(r);
}

/* the property that matters, a decomposition must never drop a point whose centroid lies
 * inside the query box; extra ranges only cost reads, missing ranges lose rows. */
void test_decompose_no_false_negatives(void)
{
    lcg rng(42);
    for (int q = 0; q < 300; q++)
    {
        double x0 = rng.coord(), x1 = rng.coord();
        double y0 = rng.coord(), y1 = rng.coord();
        if (x0 > x1) std::swap(x0, x1);
        if (y0 > y1) std::swap(y0, y1);

        uint32_t qx_min = double_to_lex_uint32(x0);
        uint32_t qx_max = double_to_lex_uint32(x1);
        uint32_t qy_min = double_to_lex_uint32(y0);
        uint32_t qy_max = double_to_lex_uint32(y1);

        std::vector<std::pair<uint64_t, uint64_t>> ranges;
        decompose_ranges(qx_min, qy_min, qx_max, qy_max, ranges);
        assert_sorted_disjoint(ranges);

        for (int s = 0; s < 400; s++)
        {
            /* sample a centroid strictly inside the query box. */
            double ux = (double)(rng.next() & 0xFFFFFFFFULL) / (double)0xFFFFFFFFULL;
            double uy = (double)(rng.next() & 0xFFFFFFFFULL) / (double)0xFFFFFFFFULL;
            double px = x0 + ux * (x1 - x0);
            double py = y0 + uy * (y1 - y0);

            uint64_t h = hilbert_xy2d_64(double_to_lex_uint32(px), double_to_lex_uint32(py));
            bool covered = false;
            for (auto &rg : ranges)
                if (h >= rg.first && h <= rg.second)
                {
                    covered = true;
                    break;
                }
            ASSERT_TRUE(covered);
        }
    }
}

int main(int argc, char **argv)
{
    INIT_TEST_FILTER(argc, argv);

    RUN_TEST(test_double_to_lex_uint32_monotonic, tests_passed);
    RUN_TEST(test_hilbert_be_roundtrip, tests_passed);
    RUN_TEST(test_hilbert_coarse_cells_partition, tests_passed);
    RUN_TEST(test_hilbert_cell_contiguity, tests_passed);
    RUN_TEST(test_compute_mbr_point, tests_passed);
    RUN_TEST(test_compute_mbr_linestring, tests_passed);
    RUN_TEST(test_compute_mbr_skips_non_finite, tests_passed);
    RUN_TEST(test_compute_mbr_rejects_truncated, tests_passed);
    RUN_TEST(test_compute_mbr_rejects_absurd_count, tests_passed);
    RUN_TEST(test_build_value_roundtrip, tests_passed);
    RUN_TEST(test_parse_query_mbr_normalizes, tests_passed);
    RUN_TEST(test_mbr_predicate_matrix, tests_passed);
    RUN_TEST(test_decompose_full_scan_fallback, tests_passed);
    RUN_TEST(test_decompose_single_cell, tests_passed);
    RUN_TEST(test_decompose_no_false_negatives, tests_passed);

    PRINT_TEST_RESULTS(tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
