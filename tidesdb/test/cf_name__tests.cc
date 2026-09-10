/* Copyright (c) 2026 TidesDB Corp.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 */

/* unit tests for the server-free path parsing, the column-family name and schema key. */
#include <string>

#include "../src/core/cf_name.h"
#include "test_utils.h"

using namespace tidesdb::cf_name;

static int tests_passed = 0;
static int tests_failed = 0;

void test_split_relative_and_plain(void)
{
    table_path a = split_table_path("./test/foo");
    ASSERT_TRUE(a.has_dir && a.db == "test" && a.table == "foo");

    table_path b = split_table_path("test/foo"); /* no relative prefix */
    ASSERT_TRUE(b.has_dir && b.db == "test" && b.table == "foo");
}

void test_split_datadir_prefix_and_leading_slash(void)
{
    /* a deeper path keeps the component just before the table as the database. */
    table_path a = split_table_path("/var/lib/mysql/test/foo");
    ASSERT_TRUE(a.has_dir && a.db == "test" && a.table == "foo");

    /* a leading slash yields an empty database with a directory present. */
    table_path b = split_table_path("/foo");
    ASSERT_TRUE(b.has_dir && b.db.empty() && b.table == "foo");
}

void test_split_no_directory(void)
{
    table_path a = split_table_path("foo");
    ASSERT_TRUE(!a.has_dir && a.db.empty() && a.table == "foo");
}

void test_cf_name_basic_and_temp(void)
{
    ASSERT_TRUE(to_cf_name("./test/foo") == "test__foo");
    /* the temp marker is substituted so the name stays a valid identifier. */
    ASSERT_TRUE(to_cf_name("./test/#sql-1_2") == "test___sql-1_2");
    /* a no-directory path is returned unchanged, matching the historical behaviour. */
    ASSERT_TRUE(to_cf_name("foo") == "foo");
}

int main(int argc, char **argv)
{
    INIT_TEST_FILTER(argc, argv);

    RUN_TEST(test_split_relative_and_plain, tests_passed);
    RUN_TEST(test_split_datadir_prefix_and_leading_slash, tests_passed);
    RUN_TEST(test_split_no_directory, tests_passed);
    RUN_TEST(test_cf_name_basic_and_temp, tests_passed);

    PRINT_TEST_RESULTS(tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
