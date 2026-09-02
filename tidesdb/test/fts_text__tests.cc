/* Copyright (c) 2026 TidesDB Corp.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 */

/* unit tests for the server-free fts text core. the tests bring their own tiny charsets in
 * place of CHARSET_INFO, an ascii one and a minimal utf-8 one, which is enough to drive the
 * tokenizer's multibyte path and the boolean parser. */
#include <cctype>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

#include "../src/core/fts_text.h"
#include "test_utils.h"

using namespace tidesdb::fts;

static int tests_passed = 0;
static int tests_failed = 0;

/* ascii charset, every byte single width. */
struct ascii_charset : charset
{
    unsigned mbchar_len(const char *, const char *) const override
    {
        return 0;
    }
    bool is_alnum(unsigned char c) const override
    {
        return std::isalnum(c) != 0;
    }
    std::string casedn(const char *s, size_t len) const override
    {
        std::string r(s, len);
        for (auto &ch : r) ch = (char)std::tolower((unsigned char)ch);
        return r;
    }
};

/* minimal utf-8 charset, multibyte length taken from the lead byte with a bounds and
 * continuation-byte check so it never over-reads, ascii-only classification and folding. */
struct utf8_charset : charset
{
    unsigned mbchar_len(const char *p, const char *end) const override
    {
        unsigned char c = (unsigned char)*p;
        unsigned n = 0;
        if (c >= 0xF0)
            n = 4;
        else if (c >= 0xE0)
            n = 3;
        else if (c >= 0xC0)
            n = 2;
        else
            return 0; /* ascii or a stray continuation byte */
        if (p + n > end) return 0;
        for (unsigned i = 1; i < n; i++)
            if (((unsigned char)p[i] & 0xC0) != 0x80) return 0;
        return n;
    }
    bool is_alnum(unsigned char c) const override
    {
        return c < 0x80 && std::isalnum(c) != 0;
    }
    std::string casedn(const char *s, size_t len) const override
    {
        std::string r(s, len);
        for (auto &ch : r)
            if ((unsigned char)ch < 0x80) ch = (char)std::tolower((unsigned char)ch);
        return r;
    }
};

static bool contains(const std::vector<std::string> &v, const std::string &w)
{
    for (auto &s : v)
        if (s == w) return true;
    return false;
}

/* local little-endian read so the test does not reach into the module's internals. */
static uint16_t load_test_u16(const uint8_t *p)
{
    return (uint16_t)(p[0] | (p[1] << 8));
}

void test_entry_value_roundtrip(void)
{
    uint8_t buf[VALUE_LEN];
    encode_entry_value(1234, 987654, buf);
    uint16_t tf = 0;
    uint32_t dl = 0;
    ASSERT_TRUE(decode_entry_value(buf, VALUE_LEN, &tf, &dl));
    ASSERT_EQ(tf, (uint16_t)1234);
    ASSERT_EQ(dl, (uint32_t)987654);
    /* the layout is little-endian regardless of host order. */
    ASSERT_EQ(buf[0], (uint8_t)(1234 & 0xFF));
    ASSERT_EQ(buf[1], (uint8_t)((1234 >> 8) & 0xFF));
    /* a short buffer is rejected rather than read out of bounds. */
    for (size_t l = 0; l < VALUE_LEN; l++) ASSERT_FALSE(decode_entry_value(buf, l, &tf, &dl));
}

void test_meta_value_roundtrip(void)
{
    uint8_t buf[META_VALUE_LEN];
    encode_meta_value(9000000000LL, 42, buf);
    int64_t docs = 0, words = 0;
    ASSERT_TRUE(decode_meta_value(buf, META_VALUE_LEN, &docs, &words));
    ASSERT_EQ(docs, 9000000000LL);
    ASSERT_EQ(words, 42LL);
    for (size_t l = 0; l < META_VALUE_LEN; l++)
        ASSERT_FALSE(decode_meta_value(buf, l, &docs, &words));
}

void test_build_key_layout_and_truncation(void)
{
    uint8_t pk[3] = {0xAA, 0xBB, 0xCC};
    uint8_t buf[TERM_LEN_PREFIX + MAX_TERM_BYTES + 3];

    uint32_t n = build_key("cat", 3, pk, 3, buf);
    ASSERT_EQ(n, (uint32_t)(TERM_LEN_PREFIX + 3 + 3));
    ASSERT_EQ(buf[0], (uint8_t)3);
    ASSERT_EQ(buf[1], (uint8_t)0);
    ASSERT_TRUE(std::memcmp(buf + TERM_LEN_PREFIX, "cat", 3) == 0);
    ASSERT_TRUE(std::memcmp(buf + TERM_LEN_PREFIX + 3, pk, 3) == 0);

    /* an over-long term is truncated to MAX_TERM_BYTES. */
    std::string big(MAX_TERM_BYTES + 100, 'x');
    uint32_t n2 = build_key(big.data(), (unsigned)big.size(), pk, 3, buf);
    ASSERT_EQ(n2, (uint32_t)(TERM_LEN_PREFIX + MAX_TERM_BYTES + 3));
    ASSERT_EQ(load_test_u16(buf), (uint16_t)MAX_TERM_BYTES);
}

void test_build_blend_map(void)
{
    bool map[BLEND_MAP_SIZE];
    build_blend_map("'-", map);
    ASSERT_TRUE(map[(unsigned char)'\'']);
    ASSERT_TRUE(map[(unsigned char)'-']);
    ASSERT_FALSE(map[(unsigned char)'a']);
    build_blend_map(nullptr, map);
    for (unsigned i = 0; i < BLEND_MAP_SIZE; i++) ASSERT_FALSE(map[i]);
}

void test_phrase_in_tokens(void)
{
    std::vector<std::string> doc = {"the", "quick", "brown", "fox", "jumps"};
    ASSERT_TRUE(phrase_in_tokens(doc, {}));                  /* empty phrase */
    ASSERT_TRUE(phrase_in_tokens(doc, {"quick", "brown"}));  /* middle */
    ASSERT_TRUE(phrase_in_tokens(doc, {"the", "quick"}));    /* start */
    ASSERT_TRUE(phrase_in_tokens(doc, {"fox", "jumps"}));    /* end */
    ASSERT_FALSE(phrase_in_tokens(doc, {"brown", "quick"})); /* wrong order */
    ASSERT_FALSE(phrase_in_tokens(doc, {"quick", "fox"}));   /* not consecutive */
    ASSERT_FALSE(phrase_in_tokens({"a"}, {"a", "b"}));       /* phrase longer than doc */
}

void test_tokenize_basic(void)
{
    ascii_charset cs;
    tokenize_opts opts; /* min 3, max 84 */
    std::vector<std::string> out;
    tokenize("The Quick, BROWN fox!", std::strlen("The Quick, BROWN fox!"), cs, opts, out);
    /* lowercased, punctuation split, "The" and "fox" kept (len 3). */
    ASSERT_TRUE(contains(out, "the"));
    ASSERT_TRUE(contains(out, "quick"));
    ASSERT_TRUE(contains(out, "brown"));
    ASSERT_TRUE(contains(out, "fox"));
}

void test_tokenize_length_and_stopwords(void)
{
    ascii_charset cs;
    std::unordered_set<std::string> stop = {"the", "and"};
    tokenize_opts opts;
    opts.stopwords = &stop;
    std::vector<std::string> out;
    tokenize("the ox and a cat", std::strlen("the ox and a cat"), cs, opts, out);
    /* "the" and "and" dropped as stop words; "ox" and "a" dropped under min length 3. */
    ASSERT_FALSE(contains(out, "the"));
    ASSERT_FALSE(contains(out, "and"));
    ASSERT_FALSE(contains(out, "ox"));
    ASSERT_FALSE(contains(out, "a"));
    ASSERT_TRUE(contains(out, "cat"));
}

void test_tokenize_blend_ascii(void)
{
    ascii_charset cs;
    bool map[BLEND_MAP_SIZE];
    build_blend_map("'", map);
    tokenize_opts opts;
    opts.blend_map = map;
    std::vector<std::string> out;
    tokenize("l'aria", 6, cs, opts, out);
    /* full blended token and the long part are kept; the one-char "l" is under min length. */
    ASSERT_TRUE(contains(out, "l'aria"));
    ASSERT_TRUE(contains(out, "aria"));
    ASSERT_FALSE(contains(out, "l"));
}

/* the multibyte blend sub-part must be measured in characters. "a" plus the two-byte "é"
 * form a two-character sub-part, which is under min length 3 and must be dropped; the old
 * per-byte count measured it as three bytes and wrongly kept it. */
void test_tokenize_blend_multibyte_char_count(void)
{
    utf8_charset cs;
    bool map[BLEND_MAP_SIZE];
    build_blend_map("'", map);
    tokenize_opts opts;
    opts.blend_map = map;
    std::vector<std::string> out;
    const char *w = "a\xC3\xA9'xyz"; /* aé'xyz */
    tokenize(w, std::strlen(w), cs, opts, out);
    ASSERT_TRUE(contains(out, "a\xC3\xA9'xyz")); /* full word, 6 chars */
    ASSERT_TRUE(contains(out, "xyz"));           /* 3 chars, kept */
    ASSERT_FALSE(contains(out, "a\xC3\xA9"));    /* 2 chars, dropped */
}

void test_parse_boolean_operators(void)
{
    ascii_charset cs;
    tokenize_opts opts;
    std::vector<query_term> out;
    const char *q = "+cat -dog fish*";
    parse_boolean(q, std::strlen(q), cs, opts, out);
    ASSERT_EQ(out.size(), (size_t)3);
    ASSERT_TRUE(out[0].term == "cat" && out[0].req == requirement::required && !out[0].trunc);
    ASSERT_TRUE(out[1].term == "dog" && out[1].req == requirement::excluded);
    ASSERT_TRUE(out[2].term == "fish" && out[2].req == requirement::neutral && out[2].trunc);
}

void test_parse_boolean_phrase(void)
{
    ascii_charset cs;
    tokenize_opts opts;
    std::vector<query_term> out;
    const char *q = "+\"quick brown\" fox";
    parse_boolean(q, std::strlen(q), cs, opts, out);
    /* a phrase term carrying both words, then each word as a required term, then fox. */
    ASSERT_EQ(out.size(), (size_t)3);
    ASSERT_TRUE(out[0].is_phrase);
    ASSERT_TRUE(out[0].req == requirement::required);
    ASSERT_EQ(out[0].phrase_words.size(), (size_t)2);
    ASSERT_TRUE(out[0].phrase_words[0] == "quick" && out[0].phrase_words[1] == "brown");
    ASSERT_TRUE(out[1].term == "brown" && out[1].req == requirement::required && !out[1].is_phrase);
    ASSERT_TRUE(out[2].term == "fox" && out[2].req == requirement::neutral);
}

int main(int argc, char **argv)
{
    INIT_TEST_FILTER(argc, argv);

    RUN_TEST(test_entry_value_roundtrip, tests_passed);
    RUN_TEST(test_meta_value_roundtrip, tests_passed);
    RUN_TEST(test_build_key_layout_and_truncation, tests_passed);
    RUN_TEST(test_build_blend_map, tests_passed);
    RUN_TEST(test_phrase_in_tokens, tests_passed);
    RUN_TEST(test_tokenize_basic, tests_passed);
    RUN_TEST(test_tokenize_length_and_stopwords, tests_passed);
    RUN_TEST(test_tokenize_blend_ascii, tests_passed);
    RUN_TEST(test_tokenize_blend_multibyte_char_count, tests_passed);
    RUN_TEST(test_parse_boolean_operators, tests_passed);
    RUN_TEST(test_parse_boolean_phrase, tests_passed);

    PRINT_TEST_RESULTS(tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
