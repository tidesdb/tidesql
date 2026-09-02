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

#include "fts_text.h"

#include <cstring>

namespace tidesdb
{
namespace fts
{

/* little-endian integer store and load, matching the server's int2store family so the
 * inverted index reads the same bytes regardless of host byte order. */
static inline void store_u16le(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
}
static inline void store_u32le(uint8_t *p, uint32_t v)
{
    for (unsigned i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (i * 8));
}
static inline void store_u64le(uint8_t *p, uint64_t v)
{
    for (unsigned i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (i * 8));
}
static inline uint16_t load_u16le(const uint8_t *p)
{
    return (uint16_t)(p[0] | (p[1] << 8));
}
static inline uint32_t load_u32le(const uint8_t *p)
{
    uint32_t v = 0;
    for (unsigned i = 0; i < 4; i++) v |= (uint32_t)p[i] << (i * 8);
    return v;
}
static inline uint64_t load_u64le(const uint8_t *p)
{
    uint64_t v = 0;
    for (unsigned i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

void encode_entry_value(uint16_t tf, uint32_t doc_len, uint8_t *out)
{
    store_u16le(out, tf);
    store_u32le(out + VALUE_TF_LEN, doc_len);
}

bool decode_entry_value(const uint8_t *in, size_t len, uint16_t *tf, uint32_t *doc_len)
{
    if (len < VALUE_LEN) return false;
    *tf = load_u16le(in);
    *doc_len = load_u32le(in + VALUE_TF_LEN);
    return true;
}

void encode_meta_value(int64_t total_docs, int64_t total_words, uint8_t *out)
{
    store_u64le(out, (uint64_t)total_docs);
    store_u64le(out + META_VALUE_DOCS_LEN, (uint64_t)total_words);
}

bool decode_meta_value(const uint8_t *in, size_t len, int64_t *total_docs, int64_t *total_words)
{
    if (len < META_VALUE_LEN) return false;
    *total_docs = (int64_t)load_u64le(in);
    *total_words = (int64_t)load_u64le(in + META_VALUE_DOCS_LEN);
    return true;
}

uint32_t build_key(const char *term, unsigned term_len, const uint8_t *pk, unsigned pk_len,
                   uint8_t *out)
{
    if (term_len > MAX_TERM_BYTES) term_len = MAX_TERM_BYTES;
    store_u16le(out, (uint16_t)term_len);
    uint32_t pos = TERM_LEN_PREFIX;
    std::memcpy(out + pos, term, term_len);
    pos += term_len;
    std::memcpy(out + pos, pk, pk_len);
    pos += pk_len;
    return pos;
}

void build_blend_map(const char *chars, bool out[BLEND_MAP_SIZE])
{
    std::memset(out, 0, BLEND_MAP_SIZE * sizeof(bool));
    if (!chars) return;
    for (const char *p = chars; *p; p++) out[(unsigned char)*p] = true;
}

/* count the characters in a byte range using the charset, so a multibyte word is measured
 * in characters and not in bytes; the byte scan below never straddles a character because a
 * blend character is single-byte ascii and cannot appear inside a multibyte sequence. */
static unsigned char_count(const charset &cs, const char *start, size_t byte_len)
{
    unsigned chars = 0;
    const char *p = start;
    const char *end = start + byte_len;
    while (p < end)
    {
        unsigned mb = cs.mbchar_len(p, end);
        p += mb ? mb : 1;
        chars++;
    }
    return chars;
}

/* lowercase, apply the length bounds in characters, drop stop words, then emit one token. */
static void emit_token(const charset &cs, const tokenize_opts &opts, const char *start,
                       size_t byte_len, std::vector<std::string> &out)
{
    if (byte_len == 0) return;
    unsigned chars = char_count(cs, start, byte_len);
    if (chars < opts.min_word_len || chars > opts.max_word_len) return;

    std::string word = cs.casedn(start, byte_len);
    if (opts.stopwords && opts.stopwords->count(word) > 0) return;
    out.push_back(std::move(word));
}

void tokenize(const char *text, size_t len, const charset &cs, const tokenize_opts &opts,
              std::vector<std::string> &out)
{
    const char *p = text;
    const char *end = text + len;
    const bool *blend = opts.blend_map;

    while (p < end)
    {
        /* skip separators until a word character, which is a multibyte char, an alphanumeric
         * byte, or a blend character. */
        while (p < end)
        {
            if (cs.mbchar_len(p, end)) break;
            if (cs.is_alnum((unsigned char)*p)) break;
            if (blend && blend[(unsigned char)*p]) break;
            p++;
        }
        if (p >= end) break;

        const char *word_start = p;
        bool contains_blend = false;
        while (p < end)
        {
            unsigned mb = cs.mbchar_len(p, end);
            if (mb)
            {
                p += mb;
                continue;
            }
            if (cs.is_alnum((unsigned char)*p))
            {
                p++;
                continue;
            }
            if (blend && blend[(unsigned char)*p])
            {
                contains_blend = true;
                p++;
                continue;
            }
            break;
        }
        size_t byte_len = (size_t)(p - word_start);

        /* always emit the whole word; a blended word also yields each of its parts. */
        emit_token(cs, opts, word_start, byte_len, out);
        if (contains_blend)
        {
            const char *sub_start = word_start;
            for (const char *s = word_start; s < word_start + byte_len; s++)
            {
                if (blend[(unsigned char)*s])
                {
                    emit_token(cs, opts, sub_start, (size_t)(s - sub_start), out);
                    sub_start = s + 1;
                }
            }
            emit_token(cs, opts, sub_start, (size_t)((word_start + byte_len) - sub_start), out);
        }
    }
}

void parse_boolean(const char *query, size_t len, const charset &cs, const tokenize_opts &opts,
                   std::vector<query_term> &out)
{
    const char *p = query;
    const char *end = query + len;

    while (p < end)
    {
        while (p < end && *p == ' ') p++;
        if (p >= end) break;

        requirement req = requirement::neutral;
        if (*p == OP_REQUIRED)
        {
            req = requirement::required;
            p++;
        }
        else if (*p == OP_EXCLUDED)
        {
            req = requirement::excluded;
            p++;
        }

        while (p < end && *p == ' ') p++;
        if (p >= end) break;

        if (*p == OP_PHRASE)
        {
            p++;
            const char *phrase_start = p;
            while (p < end && *p != OP_PHRASE) p++;
            size_t phrase_len = (size_t)(p - phrase_start);
            if (p < end) p++; /* consume the closing quote */

            if (phrase_len == 0) continue;

            std::vector<std::string> words;
            tokenize(phrase_start, phrase_len, cs, opts, words);
            if (words.empty()) continue;

            /* the phrase becomes one phrase term carrying every word, and each word is also
             * a required term so the candidate set narrows before phrase verification. */
            query_term qt;
            qt.term = words[0];
            qt.req = (req == requirement::neutral) ? requirement::required : req;
            qt.is_phrase = true;
            qt.phrase_words = words;
            out.push_back(std::move(qt));

            for (size_t i = 1; i < words.size(); i++)
            {
                query_term wt;
                wt.term = words[i];
                wt.req = requirement::required;
                out.push_back(std::move(wt));
            }
            continue;
        }

        /* skip anything that cannot start a plain term. */
        while (p < end && !cs.is_alnum((unsigned char)*p) && !cs.mbchar_len(p, end) &&
               *p != OP_TRUNC)
            p++;
        if (p >= end) break;

        const char *word_start = p;
        while (p < end)
        {
            unsigned mb = cs.mbchar_len(p, end);
            if (mb)
            {
                p += mb;
                continue;
            }
            if (cs.is_alnum((unsigned char)*p) || *p == OP_TRUNC)
            {
                p++;
                continue;
            }
            break;
        }
        size_t wlen = (size_t)(p - word_start);
        if (wlen == 0) continue;

        bool trunc = false;
        if (word_start[wlen - 1] == OP_TRUNC)
        {
            trunc = true;
            wlen--;
        }
        if (wlen == 0) continue;

        query_term qt;
        qt.term = cs.casedn(word_start, wlen);
        qt.req = req;
        qt.trunc = trunc;
        out.push_back(std::move(qt));
    }
}

bool phrase_in_tokens(const std::vector<std::string> &doc_tokens,
                      const std::vector<std::string> &phrase_words)
{
    if (phrase_words.empty()) return true;
    if (doc_tokens.size() < phrase_words.size()) return false;

    size_t limit = doc_tokens.size() - phrase_words.size();
    for (size_t i = 0; i <= limit; i++)
    {
        bool match = true;
        for (size_t j = 0; j < phrase_words.size(); j++)
        {
            if (doc_tokens[i + j] != phrase_words[j])
            {
                match = false;
                break;
            }
        }
        if (match) return true;
    }
    return false;
}

}  // namespace fts
}  // namespace tidesdb
