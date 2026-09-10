/* Copyright (c) 2026 TidesDB Corp.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 */

/* unit tests for the server-free sort-key decoder. the tests carry a local encoder for the
 * memory-comparable format that the server's Field::sort_string produces, then check that
 * decode inverts it and that the format orders correctly under memcmp. */
#include <cstdint>
#include <cstring>

#include "../src/core/sort_key.h"
#include "test_utils.h"

using namespace tidesdb::sort_key;

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

/* native little-endian bytes of a value's low `w` bytes. */
static void native_bytes(uint64_t v, unsigned w, uint8_t *nat)
{
    for (unsigned i = 0; i < w; i++) nat[i] = (uint8_t)(v >> (8 * i));
}

/* the memory-comparable sort key of an integer, big-endian with the sign bit flipped for a
 * signed value, mirroring what Field::sort_string writes. */
static void encode_sortkey(uint64_t v, unsigned w, bool is_signed, uint8_t *sk)
{
    uint8_t nat[8];
    native_bytes(v, w, nat);
    for (unsigned i = 0; i < w; i++) sk[i] = nat[w - 1 - i];
    if (is_signed) sk[0] ^= 0x80;
}

void test_decode_int_roundtrip(void)
{
    const unsigned widths[] = {1, 2, 3, 4, 8};
    lcg rng(3);
    for (unsigned wi = 0; wi < 5; wi++)
    {
        unsigned w = widths[wi];
        for (int sgn = 0; sgn < 2; sgn++)
        {
            bool is_signed = sgn == 1;
            for (int s = 0; s < 5000; s++)
            {
                uint64_t v = rng.next();
                uint8_t nat[8], sk[8], out[8];
                native_bytes(v, w, nat);
                encode_sortkey(v, w, is_signed, sk);
                ASSERT_TRUE(decode_int(sk, w, is_signed, out));
                ASSERT_TRUE(std::memcmp(out, nat, w) == 0);
            }
        }
    }
}

/* the encoded keys must order under memcmp the way the values order, signed values by their
 * signed magnitude and unsigned values by their unsigned magnitude. */
void test_decode_int_order(void)
{
    const unsigned w = 4;
    const int64_t signed_vals[] = {-2000000000, -1000000, -1, 0, 1, 1000000, 2000000000};
    for (unsigned i = 1; i < sizeof(signed_vals) / sizeof(signed_vals[0]); i++)
    {
        uint8_t a[8], b[8];
        encode_sortkey((uint64_t)signed_vals[i - 1], w, true, a);
        encode_sortkey((uint64_t)signed_vals[i], w, true, b);
        ASSERT_TRUE(std::memcmp(a, b, w) < 0);
    }
    const uint64_t unsigned_vals[] = {0, 1, 255, 65535, 1u << 24, 0xFFFFFFFFu};
    for (unsigned i = 1; i < sizeof(unsigned_vals) / sizeof(unsigned_vals[0]); i++)
    {
        uint8_t a[8], b[8];
        encode_sortkey(unsigned_vals[i - 1], w, false, a);
        encode_sortkey(unsigned_vals[i], w, false, b);
        ASSERT_TRUE(std::memcmp(a, b, w) < 0);
    }
}

void test_decode_int_rejects_bad_width(void)
{
    uint8_t src[8] = {0}, out[8];
    const unsigned bad[] = {0, 5, 6, 7, 9, 16};
    for (unsigned i = 0; i < sizeof(bad) / sizeof(bad[0]); i++)
        ASSERT_FALSE(decode_int(src, bad[i], false, out));
}

void test_decode_year(void)
{
    field_sort_desc d;
    d.k = kind::year;
    uint8_t src[1] = {123}, out[1] = {0};
    ASSERT_TRUE(decode_part(src, 1, d, out));
    ASSERT_EQ(out[0], (uint8_t)123);
    ASSERT_FALSE(decode_part(src, 0, d, out));
}

void test_decode_date(void)
{
    field_sort_desc d;
    d.k = kind::date;
    uint8_t src[3] = {0x11, 0x22, 0x33}, out[3] = {0};
    ASSERT_TRUE(decode_part(src, 3, d, out));
    ASSERT_TRUE(out[0] == 0x33 && out[1] == 0x22 && out[2] == 0x11);
    ASSERT_FALSE(decode_part(src, 2, d, out));
    ASSERT_FALSE(decode_part(src, 4, d, out));
}

void test_decode_datetime(void)
{
    field_sort_desc d;
    d.k = kind::datetime;
    uint8_t src[8] = {1, 2, 3, 4, 5, 6, 7, 8}, out[8] = {0};
    ASSERT_TRUE(decode_part(src, 5, d, out));
    for (unsigned i = 0; i < 5; i++) ASSERT_EQ(out[i], src[4 - i]);
    ASSERT_FALSE(decode_part(src, 0, d, out));
    ASSERT_FALSE(decode_part(src, 9, d, out));
}

void test_decode_char_fixed_latin1_pads_space(void)
{
    field_sort_desc d;
    d.k = kind::char_fixed;
    d.pack_length = 8;
    d.pad_byte = ' ';
    uint8_t src[3] = {'a', 'b', 'c'};
    uint8_t out[8];
    std::memset(out, 0xEE, sizeof(out));
    ASSERT_TRUE(decode_part(src, 3, d, out));
    ASSERT_TRUE(std::memcmp(out, "abc     ", 8) == 0); /* space padded to width 8 */
}

/* the fix: a binary column pads with nul, not space. the pre-split code padded both with a
 * space, corrupting a reconstructed binary prefix value. */
void test_decode_char_fixed_binary_pads_nul(void)
{
    field_sort_desc d;
    d.k = kind::char_fixed;
    d.pack_length = 6;
    d.pad_byte = 0x00;
    uint8_t src[2] = {0x01, 0x02};
    uint8_t out[6];
    std::memset(out, 0xEE, sizeof(out));
    ASSERT_TRUE(decode_part(src, 2, d, out));
    uint8_t want[6] = {0x01, 0x02, 0x00, 0x00, 0x00, 0x00};
    ASSERT_TRUE(std::memcmp(out, want, 6) == 0);
}

void test_decode_undecodable(void)
{
    field_sort_desc d; /* defaults to kind::undecodable */
    uint8_t src[4] = {0}, out[4];
    ASSERT_FALSE(decode_part(src, 4, d, out));
}

int main(int argc, char **argv)
{
    INIT_TEST_FILTER(argc, argv);

    RUN_TEST(test_decode_int_roundtrip, tests_passed);
    RUN_TEST(test_decode_int_order, tests_passed);
    RUN_TEST(test_decode_int_rejects_bad_width, tests_passed);
    RUN_TEST(test_decode_year, tests_passed);
    RUN_TEST(test_decode_date, tests_passed);
    RUN_TEST(test_decode_datetime, tests_passed);
    RUN_TEST(test_decode_char_fixed_latin1_pads_space, tests_passed);
    RUN_TEST(test_decode_char_fixed_binary_pads_nul, tests_passed);
    RUN_TEST(test_decode_undecodable, tests_passed);

    PRINT_TEST_RESULTS(tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
