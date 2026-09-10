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

/* server-free core for full-text search text handling. this is the tokenizer, the boolean
 * query parser, the phrase matcher, and the inverted-index byte codecs, with no dependency
 * on the mysql or mariadb server. the one thing the algorithms need from the server is
 * charset behaviour, which the handler supplies by adapting CHARSET_INFO to the small
 * charset interface below, and which the tests supply with their own tiny charsets. the
 * caller also snapshots the tunable dictionaries (stop words, blend characters) and passes
 * them in, so no global lock reaches into this module. */
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

namespace tidesdb
{
namespace fts
{

/* terms longer than this are truncated on both the index and the query side, so the two
 * still agree; the cap accommodates long cjk compound words. */
static constexpr unsigned MAX_TERM_BYTES = 512;

/* an inverted-index entry key leads with a 2-byte little-endian term length; an entry
 * value is a 2-byte term frequency then a 4-byte document length. */
static constexpr unsigned TERM_LEN_PREFIX = 2;
static constexpr unsigned VALUE_TF_LEN = 2;
static constexpr unsigned VALUE_DOC_LEN_LEN = 4;
static constexpr unsigned VALUE_LEN = VALUE_TF_LEN + VALUE_DOC_LEN_LEN;

/* a per-index meta value is an 8-byte total document count then an 8-byte total word count,
 * both little-endian and signed, feeding bm25 inverse document frequency. */
static constexpr unsigned META_VALUE_DOCS_LEN = 8;
static constexpr unsigned META_VALUE_WORDS_LEN = 8;
static constexpr unsigned META_VALUE_LEN = META_VALUE_DOCS_LEN + META_VALUE_WORDS_LEN;

/* default indexable word length bounds in characters, mirroring innodb's token size
 * defaults; the handler overrides them from its session variables. */
static constexpr unsigned DEFAULT_MIN_WORD_LEN = 3;
static constexpr unsigned DEFAULT_MAX_WORD_LEN = 84;

/* operator characters of the boolean-mode query language. */
static constexpr char OP_REQUIRED = '+';
static constexpr char OP_EXCLUDED = '-';
static constexpr char OP_PHRASE = '"';
static constexpr char OP_TRUNC = '*';

/* a blend map is a 256-entry byte-indexed table marking characters that are both word
 * characters and separators, so a blended token also yields its parts. */
static constexpr unsigned BLEND_MAP_SIZE = 256;

/**
 * requirement
 * whether a boolean-mode term must appear, must not appear, or merely contributes to score
 */
enum class requirement
{
    neutral,
    required,
    excluded
};

/**
 * charset
 * the sliver of charset behaviour the tokenizer and parser need, adapted by the handler
 * from the server CHARSET_INFO and by the tests from a small local charset
 */
struct charset
{
    virtual ~charset() = default;

    /**
     * mbchar_len
     * length in bytes of the multibyte character beginning at p, or zero when p begins a
     * single-byte character; must not read at or beyond end
     * @param p the first byte of the character
     * @param end one past the last readable byte
     * @return the multibyte length, or 0 for a single-byte character
     */
    virtual unsigned mbchar_len(const char *p, const char *end) const = 0;

    /**
     * is_alnum
     * whether a single byte is an alphanumeric word character in this charset
     * @param c the byte to classify
     * @return true when c is a word character
     */
    virtual bool is_alnum(unsigned char c) const = 0;

    /**
     * casedn
     * lowercased copy of the byte range, following this charset's case folding
     * @param s the first byte
     * @param len the byte length
     * @return the lowercased string, which may differ in length from the input
     */
    virtual std::string casedn(const char *s, size_t len) const = 0;
};

/**
 * tokenize_opts
 * the tunables and dictionaries a tokenize or parse pass reads, snapshotted by the caller
 * so this module never touches a global or a lock
 * @param min_word_len shortest indexable word in characters
 * @param max_word_len longest indexable word in characters
 * @param blend_map an optional BLEND_MAP_SIZE table of blend characters, or null for none
 * @param stopwords an optional set of lowercased stop words to drop, or null for none
 */
struct tokenize_opts
{
    unsigned min_word_len = DEFAULT_MIN_WORD_LEN;
    unsigned max_word_len = DEFAULT_MAX_WORD_LEN;
    const bool *blend_map = nullptr;
    const std::unordered_set<std::string> *stopwords = nullptr;
};

/**
 * query_term
 * one parsed boolean-mode term, either a plain or truncated word or a phrase whose words
 * are also emitted as required terms to narrow the candidate set before verification
 * @param term the lowercased word, or the first word of a phrase
 * @param req whether the term is required, excluded, or neutral
 * @param trunc whether the term is a prefix match
 * @param is_phrase whether this term carries a phrase to verify
 * @param phrase_words the phrase's words in order, set only when is_phrase is true
 */
struct query_term
{
    std::string term;
    requirement req = requirement::neutral;
    bool trunc = false;
    bool is_phrase = false;
    std::vector<std::string> phrase_words;
};

/**
 * encode_entry_value
 * write an inverted-index entry value, a little-endian term frequency then document length
 * @param tf the term frequency
 * @param doc_len the document length in words
 * @param out a buffer of at least VALUE_LEN bytes
 */
void encode_entry_value(uint16_t tf, uint32_t doc_len, uint8_t *out);

/**
 * decode_entry_value
 * read an inverted-index entry value back
 * @param in the encoded bytes
 * @param len the length of in
 * @param tf receives the term frequency
 * @param doc_len receives the document length
 * @return true when in held at least VALUE_LEN bytes
 */
bool decode_entry_value(const uint8_t *in, size_t len, uint16_t *tf, uint32_t *doc_len);

/**
 * encode_meta_value
 * write a per-index meta value, a little-endian signed total document then total word count
 * @param total_docs the total indexed documents
 * @param total_words the total indexed words
 * @param out a buffer of at least META_VALUE_LEN bytes
 */
void encode_meta_value(int64_t total_docs, int64_t total_words, uint8_t *out);

/**
 * decode_meta_value
 * read a per-index meta value back
 * @param in the encoded bytes
 * @param len the length of in
 * @param total_docs receives the total documents
 * @param total_words receives the total words
 * @return true when in held at least META_VALUE_LEN bytes
 */
bool decode_meta_value(const uint8_t *in, size_t len, int64_t *total_docs, int64_t *total_words);

/**
 * build_key
 * assemble an inverted-index entry key, a little-endian term length then the term bytes then
 * the row's comparable primary key; the term is truncated to MAX_TERM_BYTES
 * @param term the term bytes
 * @param term_len the term length
 * @param pk the comparable primary key bytes
 * @param pk_len the primary key length
 * @param out a buffer of at least TERM_LEN_PREFIX + min(term_len, MAX_TERM_BYTES) + pk_len
 * @return the total key length written
 */
uint32_t build_key(const char *term, unsigned term_len, const uint8_t *pk, unsigned pk_len,
                   uint8_t *out);

/**
 * build_blend_map
 * fill a blend table from a null-terminated set of blend characters
 * @param chars the blend characters, or null to clear the table
 * @param out a BLEND_MAP_SIZE table set true at each blend character's byte value
 */
void build_blend_map(const char *chars, bool out[BLEND_MAP_SIZE]);

/**
 * tokenize
 * split text into lowercased index tokens, honouring word length bounds, stop words, and
 * blend characters, where a blended word also yields each of its parts
 * @param text the document bytes
 * @param len the document length
 * @param cs the charset driving multibyte scanning, classification, and case folding
 * @param opts the length bounds and dictionaries to apply
 * @param out appended with the tokens in document order
 */
void tokenize(const char *text, size_t len, const charset &cs, const tokenize_opts &opts,
              std::vector<std::string> &out);

/**
 * parse_boolean
 * parse a boolean-mode query into terms, expanding a quoted phrase into a phrase term plus
 * its words as required terms; plain terms are lowercased but not stop-word filtered
 * @param query the query bytes
 * @param len the query length
 * @param cs the charset driving scanning and case folding
 * @param opts the options used when tokenizing a phrase's interior
 * @param out appended with the parsed terms in query order
 */
void parse_boolean(const char *query, size_t len, const charset &cs, const tokenize_opts &opts,
                   std::vector<query_term> &out);

/**
 * phrase_in_tokens
 * whether a phrase appears as a consecutive run within an already-tokenized document
 * @param doc_tokens the document's tokens in order
 * @param phrase_words the phrase's words in order; an empty phrase always matches
 * @return true when the phrase occurs consecutively in the document
 */
bool phrase_in_tokens(const std::vector<std::string> &doc_tokens,
                      const std::vector<std::string> &phrase_words);

}  // namespace fts
}  // namespace tidesdb
