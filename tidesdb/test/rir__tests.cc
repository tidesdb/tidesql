/* Copyright (c) 2026 TidesDB Corp.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 */

/* unit tests for the server-free records-in-range estimator. */
#include <cstdint>

#include "../src/core/rir.h"
#include "test_utils.h"

using namespace tidesdb::rir;

static int tests_passed = 0;
static int tests_failed = 0;

void test_unknown_fallback(void)
{
    ASSERT_EQ(unknown_fallback(0), (uint64_t)1);    /* 0/4 + 1 */
    ASSERT_EQ(unknown_fallback(4), (uint64_t)2);    /* 1 + 1 */
    ASSERT_EQ(unknown_fallback(100), (uint64_t)26); /* 25 + 1 */
}

void test_point_estimate(void)
{
    ASSERT_EQ(point_estimate(0, 100), (uint64_t)1);     /* unknown rpk floors to 1 */
    ASSERT_EQ(point_estimate(5, 100), (uint64_t)5);     /* rec_per_key used directly */
    ASSERT_EQ(point_estimate(500, 100), (uint64_t)100); /* capped at total */
    ASSERT_EQ(point_estimate(1, 1), (uint64_t)1);
}

int main(int argc, char **argv)
{
    INIT_TEST_FILTER(argc, argv);

    RUN_TEST(test_unknown_fallback, tests_passed);
    RUN_TEST(test_point_estimate, tests_passed);

    PRINT_TEST_RESULTS(tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
