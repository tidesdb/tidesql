/* Copyright (c) 2026 TidesDB Corp.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 */

/* unit tests for auto-increment reservation on the server's offset and increment grid. */
#include <cstdint>

#include "../src/core/autoinc.h"
#include "test_utils.h"

using namespace tidesdb::autoinc;

static int tests_passed = 0;
static int tests_failed = 0;

/* the server's compute_next_insert_id, written out independently, to check against */
static uint64_t server_round(uint64_t nr, uint64_t offset, uint64_t increment)
{
    uint64_t save = nr;
    if (increment == 1)
        nr = nr + 1;
    else
        nr = ((nr + increment - offset) / increment) * increment + offset;
    return nr <= save ? UINT64_MAX : nr;
}

void test_default_spacing(void)
{
    ASSERT_TRUE(next_on_grid(0, 1, 1) == 1);
    ASSERT_TRUE(next_on_grid(41, 1, 1) == 42);
    ASSERT_TRUE(next_on_grid(5, 1, 0) == 6);
}

void test_values_land_on_grid(void)
{
    /* offset 2, increment 3 is the grid 2, 5, 8, ... */
    ASSERT_TRUE(next_on_grid(0, 2, 3) == 2);
    ASSERT_TRUE(next_on_grid(1, 2, 3) == 2);
    ASSERT_TRUE(next_on_grid(2, 2, 3) == 5);
    ASSERT_TRUE(next_on_grid(4, 2, 3) == 5);
    ASSERT_TRUE(next_on_grid(5, 2, 3) == 8);
}

void test_server_rounding_is_a_no_op(void)
{
    /* the engine answers exactly what the server computes, and the server rounds first - 1 again
     * after the engine answers, which must give first back.  the server's own formula only forms
     * a grid once after + increment reaches the offset, below that it wraps for the server too, so
     * the round trip is checked on the grid alone */
    for (uint64_t inc = 1; inc <= 7; inc++)
        for (uint64_t off = 1; off <= 9; off++)
            for (uint64_t after = 0; after < 60; after++)
            {
                uint64_t first = next_on_grid(after, off, inc);
                ASSERT_TRUE(first == server_round(after, off, inc));
                if (first != UINT64_MAX && after + inc >= off)
                    ASSERT_TRUE(server_round(first - 1, off, inc) == first);
            }
}

void test_consecutive_runs_never_overlap(void)
{
    /* each caller's run starts past the previous caller's last value */
    uint64_t counter = 0;
    uint64_t seen_last = 0;
    for (int caller = 0; caller < 100; caller++)
    {
        uint64_t first = next_on_grid(counter, 2, 3);
        uint64_t n = run_length(first, 1 + caller % 4, 3);
        uint64_t last = first + (n - 1) * 3;
        ASSERT_TRUE(caller == 0 || first > seen_last);
        ASSERT_TRUE((first - 2) % 3 == 0);
        seen_last = last;
        counter = last;
    }
}

void test_top_of_range(void)
{
    ASSERT_TRUE(next_on_grid(UINT64_MAX, 1, 1) == UINT64_MAX);
    ASSERT_TRUE(next_on_grid(UINT64_MAX - 1, 1, 3) == UINT64_MAX);
    ASSERT_TRUE(run_length(UINT64_MAX - 4, 10, 3) == 1);
    ASSERT_TRUE(run_length(10, 0, 3) == 1);
    ASSERT_TRUE(run_length(10, 5, 3) == 5);
}

int main(int argc, char **argv)
{
    INIT_TEST_FILTER(argc, argv);

    RUN_TEST(test_default_spacing, tests_passed);
    RUN_TEST(test_values_land_on_grid, tests_passed);
    RUN_TEST(test_server_rounding_is_a_no_op, tests_passed);
    RUN_TEST(test_consecutive_runs_never_overlap, tests_passed);
    RUN_TEST(test_top_of_range, tests_passed);

    PRINT_TEST_RESULTS(tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
