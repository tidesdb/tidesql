/*
   Copyright (c) 2026 TidesDB Corp.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/

#ifndef HA_TIDESDB_FTS_H
#define HA_TIDESDB_FTS_H

/* the public surface of the full-text module. the tokenizer, boolean-query parser, bm25 scorer,
   inverted-index maintenance, and the FT_INFO search context all live in ha_tidesdb_fts.cc; this
   header exposes only what the dml, commit, sysvar, and open paths in ha_tidesdb.cc still touch, so
   the index layout stays a shared contract while the machinery stays private to the module. include
   after ha_tidesdb.h so the server types and the key-namespace constants are already visible. */

#include <string>
#include <vector>

/* mariadb renamed HA_FULLTEXT to HA_FULLTEXT_legacy after the 11.x series (flag bit unchanged);
   detection keys off the flag because KEY::algorithm is only set to HA_KEY_ALG_FULLTEXT on newer
   servers and notably not in the alter key_info_buffer on 11.4. */
#ifndef HA_FULLTEXT
#define HA_FULLTEXT HA_FULLTEXT_legacy
#endif

/* longest term byte length the inverted index stores; longer terms are truncated. 512 bytes holds
   even long cjk compound words (170+ 3-byte utf-8 characters). */
static constexpr uint FTS_MAX_TERM_BYTES = 512;

/* the leading 2-byte little-endian term-length field on every inverted-index entry key. */
static constexpr uint FTS_TERM_LEN_PREFIX = 2;

/* worst-case inverted-index entry key buffer, [2B term_len][term bytes][pk]. */
static constexpr uint FTS_KEY_BUF_LEN = FTS_TERM_LEN_PREFIX + FTS_MAX_TERM_BYTES + MAX_KEY_LENGTH;

/* inverted-index entry value layout, [2B tf le][4B doc_len le] = 6 bytes. */
static constexpr uint FTS_VALUE_TF_LEN = 2;
static constexpr uint FTS_VALUE_DOC_LEN_OFFSET = FTS_VALUE_TF_LEN;
static constexpr uint FTS_VALUE_DOC_LEN_LEN = 4;
static constexpr uint FTS_VALUE_LEN = FTS_VALUE_TF_LEN + FTS_VALUE_DOC_LEN_LEN;

/* per-index meta key layout, [KEY_NS_META(1B)][FTS tag(4B incl nul)][keynr(1B)] = 6 bytes, and meta
   value layout, [8B total_docs][8B total_words] = 16 bytes. */
static constexpr const char FTS_META_KEY_TAG[] = "FTS\x00";
static constexpr uint FTS_META_KEY_TAG_LEN = 4; /* 3 letters plus the trailing nul */
static constexpr uint FTS_META_KEY_TAG_OFFSET = KEY_NAMESPACE_LEN;
static constexpr uint FTS_META_KEY_KEYNR_OFFSET = FTS_META_KEY_TAG_OFFSET + FTS_META_KEY_TAG_LEN;
static constexpr uint FTS_META_KEY_LEN = FTS_META_KEY_KEYNR_OFFSET + 1;
static constexpr uint FTS_META_VALUE_DOCS_LEN = 8;
static constexpr uint FTS_META_VALUE_WORDS_OFFSET = FTS_META_VALUE_DOCS_LEN;
static constexpr uint FTS_META_VALUE_WORDS_LEN = 8;
static constexpr uint FTS_META_VALUE_LEN = FTS_META_VALUE_DOCS_LEN + FTS_META_VALUE_WORDS_LEN;

/* whether a key is a full-text index. */
static inline bool is_fts_index(const KEY *ki)
{
    return (ki->flags & HA_FULLTEXT) || ki->algorithm == HA_KEY_ALG_FULLTEXT;
}

/* sysvar backing variables, defined in the module and registered by the sysvar block. */
extern ulong srv_fts_min_word_len;
extern ulong srv_fts_max_word_len;
extern char *srv_fts_blend_chars;
extern char *srv_ft_stopword_table;
extern double srv_fts_bm25_k1;
extern double srv_fts_bm25_b;

/**
 * fts_build_key
 * assemble an inverted-index key, [2B term_len le][lowercased term][comparable pk], truncating an
 * over-long term to FTS_MAX_TERM_BYTES
 * @param term the lowercased term bytes
 * @param term_len the term length
 * @param pk the comparable primary key bytes to append
 * @param pk_len the primary key length
 * @param out a buffer of at least FTS_KEY_BUF_LEN bytes
 * @return the total key length written
 */
uint fts_build_key(const char *term, uint term_len, const uchar *pk, uint pk_len, uchar *out);

/**
 * fts_build_value
 * write the entry value, [2B tf le][4B doc_len le]
 * @param tf the term frequency in the document
 * @param doc_len the document length in tokens
 * @param out a buffer of at least FTS_VALUE_LEN bytes
 * @return FTS_VALUE_LEN
 */
uint fts_build_value(uint16 tf, uint32 doc_len, uchar *out);

/**
 * fts_extract_and_tokenize
 * pull every full-text key_part field out of a record and tokenize the concatenation
 * @param table the table whose fields back the document
 * @param key_info the full-text key describing the indexed columns
 * @param record the packed row the fields read from
 * @param cs the charset the tokenizer folds and length-filters with
 * @param out_tokens appended with the document's lowercased tokens
 */
void fts_extract_and_tokenize(TABLE *table, const KEY *key_info, const uchar *record,
                              CHARSET_INFO *cs, std::vector<std::string> &out_tokens);

/**
 * trx_fts_meta_accumulate
 * fold a per-row meta delta into the transaction-level accumulator, combining with the matching
 * (data_cf, keynr) entry or appending a new one
 * @param trx the transaction whose accumulator to update, may be NULL
 * @param cf the data column family the meta key lives in
 * @param keynr the full-text index number
 * @param doc_delta the change in indexed document count
 * @param word_delta the change in total indexed word count
 */
void trx_fts_meta_accumulate(tidesdb_trx_t *trx, tidesdb_column_family_t *cf, uint keynr,
                             int64_t doc_delta, int64_t word_delta);

/**
 * flush_trx_fts_meta_pending
 * apply every accumulated meta delta to its index meta key inside the current transaction, then
 * clear the accumulator whether or not a delta failed since the transaction is about to resolve
 * @param thd the session, forwarded to the library put
 * @param trx the transaction holding the accumulator, may be NULL
 * @return TDB_SUCCESS, or the first library error encountered
 */
int flush_trx_fts_meta_pending(THD *thd, tidesdb_trx_t *trx);

/**
 * fts_init
 * initialise the module at plugin start, arming the stop-word and blend-character locks, seeding
 * the default stop-word set, and building the blend-character lookup from tidesdb_fts_blend_chars
 */
void fts_init();

/**
 * fts_deinit
 * tear the module down at plugin stop, destroying its locks and clearing the stop-word set
 */
void fts_deinit();

/**
 * tdb_fts_blend_chars_update
 * sysvar update callback that rebuilds the blend-character lookup after tidesdb_fts_blend_chars set
 */
void tdb_fts_blend_chars_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr,
                                const void *save);

/**
 * tdb_ft_stopword_table_update
 * sysvar update callback that reloads the stop-word set after tidesdb_ft_stopword_table is set
 */
void tdb_ft_stopword_table_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr,
                                  const void *save);

#endif /* HA_TIDESDB_FTS_H */
