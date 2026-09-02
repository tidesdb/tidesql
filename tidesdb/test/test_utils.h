/* Copyright (c) 2026 TidesDB Corp.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 */

/* shared harness for the plugin core unit tests. these tests link only the server-free
 * core, so this header pulls in nothing from the mysql or mariadb server; it mirrors the
 * tidesdb library test harness so the two feel the same to a reader. */
#ifndef __TIDESQL_TEST_UTILS_H__
#define __TIDESQL_TEST_UTILS_H__

/* assertions must fire in release builds too, so drop any NDEBUG before <cassert>. */
#undef NDEBUG
#include <cassert>
#include <cstdio>
#include <cstring>

#include "test_macros.h"

/* optional substring filter taken from argv[1] so a single test binary can run one case. */
static const char *test_filter = nullptr;
static int tests_skipped = 0;

#define INIT_TEST_FILTER(argc, argv)             \
    do                                           \
    {                                            \
        if ((argc) > 1) test_filter = (argv)[1]; \
    } while (0)

#define ASSERT_EQ(a, b) assert((a) == (b))
#define ASSERT_NE(a, b) assert((a) != (b))
#define ASSERT_TRUE(a)  assert(a)
#define ASSERT_FALSE(a) assert(!(a))

#define RUN_TEST(test_func, tests_passed)                    \
    do                                                       \
    {                                                        \
        if (test_filter && !strstr(#test_func, test_filter)) \
        {                                                    \
            tests_skipped++;                                 \
            break;                                           \
        }                                                    \
        printf(YELLOW "Running: %s... " RESET, #test_func);  \
        fflush(stdout);                                      \
        test_func();                                         \
        printf(GREEN "PASSED\n" RESET);                      \
        (tests_passed)++;                                    \
    } while (0)

#define PRINT_TEST_RESULTS(tests_passed, tests_failed)                                      \
    do                                                                                      \
    {                                                                                       \
        printf("\n");                                                                       \
        printf("*=======================================*\n");                              \
        printf("Test Results\n");                                                           \
        printf("  " BOLDGREEN "PASSED %d" RESET "\n", (tests_passed));                      \
        printf("  " BOLDRED "FAILED %d" RESET "\n", (tests_failed));                        \
        if (tests_skipped > 0) printf("  " YELLOW "SKIPPED %d" RESET "\n", tests_skipped);  \
        printf("*=======================================*\n");                              \
    } while (0)

#endif /* __TIDESQL_TEST_UTILS_H__ */
