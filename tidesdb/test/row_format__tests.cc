/* Copyright (c) 2026 TidesDB Corp.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 */

/* unit tests for the server-free on-disk framing core, the row header, the big-endian
 * hidden primary key, and the key-namespace helpers. */
#include <cstdint>
#include <cstring>

#include "../src/core/row_format.h"
#include "test_utils.h"

using namespace tidesdb::row_format;

static int tests_passed = 0;
static int tests_failed = 0;

struct lcg
{
    uint64_t state;
    explicit lcg(uint64_t s) : state(s)
    {
    }
    uint64_t next()
    {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        return state >> 11;
    }
};

void test_row_header_roundtrip(void)
{
    uint8_t buf[ROW_HEADER_SIZE];
    encode_row_header(7, 300, buf);
    ASSERT_EQ(buf[0], ROW_HEADER_MAGIC);
    row_header h;
    ASSERT_TRUE(decode_row_header(buf, ROW_HEADER_SIZE, &h));
    ASSERT_EQ(h.null_bytes, (uint16_t)7);
    ASSERT_EQ(h.field_count, (uint16_t)300);
    /* little-endian field layout, independent of host order. */
    ASSERT_EQ(buf[1], (uint8_t)(7 & 0xFF));
    ASSERT_EQ(buf[3], (uint8_t)(300 & 0xFF));
    ASSERT_EQ(buf[4], (uint8_t)((300 >> 8) & 0xFF));
}

void test_row_header_rejects_short_and_bad_magic(void)
{
    uint8_t buf[ROW_HEADER_SIZE];
    encode_row_header(1, 1, buf);
    row_header h;
    for (size_t l = 0; l < ROW_HEADER_SIZE; l++) ASSERT_FALSE(decode_row_header(buf, l, &h));
    uint8_t bad[ROW_HEADER_SIZE];
    std::memcpy(bad, buf, ROW_HEADER_SIZE);
    bad[0] = 0x00; /* wrong magic */
    ASSERT_FALSE(decode_row_header(bad, ROW_HEADER_SIZE, &h));
}

void test_be64_roundtrip(void)
{
    lcg rng(5);
    for (int i = 0; i < 100000; i++)
    {
        uint64_t id = rng.next();
        uint8_t buf[HIDDEN_PK_SIZE];
        encode_be64(id, buf);
        ASSERT_EQ(decode_be64(buf), id);
        ASSERT_EQ(buf[0], (uint8_t)(id >> 56)); /* most significant byte leads */
        ASSERT_EQ(buf[HIDDEN_PK_SIZE - 1], (uint8_t)(id & 0xFF));
    }
}

/* the big-endian encoding must order under memcmp the way the ids order numerically, which
 * is what lets a hidden-pk scan walk rows in id order. */
void test_be64_order_preserving(void)
{
    lcg rng(9);
    for (int i = 0; i < 100000; i++)
    {
        uint64_t a = rng.next(), b = rng.next();
        uint8_t ea[HIDDEN_PK_SIZE], eb[HIDDEN_PK_SIZE];
        encode_be64(a, ea);
        encode_be64(b, eb);
        int cmp = std::memcmp(ea, eb, HIDDEN_PK_SIZE);
        if (a < b)
            ASSERT_TRUE(cmp < 0);
        else if (a > b)
            ASSERT_TRUE(cmp > 0);
        else
            ASSERT_TRUE(cmp == 0);
    }
}

void test_build_data_key_and_is_data_key(void)
{
    uint8_t pk[4] = {0xDE, 0xAD, 0xBE, 0xEF};
    uint8_t key[1 + 4];
    uint32_t n = build_data_key(pk, 4, key);
    ASSERT_EQ(n, (uint32_t)(KEY_NAMESPACE_LEN + 4));
    ASSERT_EQ(key[0], KEY_NS_DATA);
    ASSERT_TRUE(std::memcmp(key + KEY_NAMESPACE_LEN, pk, 4) == 0);

    ASSERT_TRUE(is_data_key(key, n));

    uint8_t meta[3] = {KEY_NS_META, 1, 2};
    ASSERT_FALSE(is_data_key(meta, 3));
    ASSERT_FALSE(is_data_key(key, 0)); /* empty key is not a data key */
}

int main(int argc, char **argv)
{
    INIT_TEST_FILTER(argc, argv);

    RUN_TEST(test_row_header_roundtrip, tests_passed);
    RUN_TEST(test_row_header_rejects_short_and_bad_magic, tests_passed);
    RUN_TEST(test_be64_roundtrip, tests_passed);
    RUN_TEST(test_be64_order_preserving, tests_passed);
    RUN_TEST(test_build_data_key_and_is_data_key, tests_passed);

    PRINT_TEST_RESULTS(tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
