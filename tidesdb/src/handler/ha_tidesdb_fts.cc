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

/* full-text search for the tidesdb handler: the charset-aware tokenizer, the boolean-query parser,
   okapi bm25 scoring over the inverted index, per-index meta maintenance, and the FT_INFO search
   context. the pure tokenizer and scorer math live in tidesdb::fts and tidesdb::fts_score; this
   unit is the server-coupled glue and the index/query paths. */

#include "src/handler/ha_tidesdb_fts.h"

#include <ft_global.h>
#include <mysql/plugin.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

#include "ha_tidesdb.h"
#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/core/fts_score.h"
#include "src/core/fts_text.h"
#include "src/handler/ha_tidesdb_fts_internal.h"
#include "src/handler/ha_tidesdb_internal.h"

/* ******************** Full-Text Search helpers ******************** */

/* assemble the inverted-index entry key, the little-endian term length then the term bytes then the
   comparable primary key, by delegating the framing to the server-free core so the on-disk layout
   has one authority; the term is truncated there to the shared maximum. */
uint fts_build_key(const char *term, uint term_len, const uchar *pk, uint pk_len, uchar *out)
{
    return tidesdb::fts::build_key(term, term_len, (const uint8_t *)pk, pk_len, (uint8_t *)out);
}

/* write the entry value, the little-endian term frequency then document length, through the same
   core codec the reader agrees with, and report the fixed value length. */
uint fts_build_value(uint16 tf, uint32 doc_len, uchar *out)
{
    tidesdb::fts::encode_entry_value(tf, doc_len, (uint8_t *)out);
    return FTS_VALUE_LEN;
}

/* Read or initialize FTS metadata counters from the data CF.
   Key format-- [KEY_NS_META][FTS tag][keynr].
   Returns TDB_SUCCESS on a found row, TDB_ERR_NOT_FOUND for a fresh index
   (totals zeroed), or the library's error code otherwise.  Callers must
   not write back a zeroed total derived from a transient read failure --
   that would clobber the real counters and degrade BM25 IDF. */
int fts_load_meta(tidesdb_txn_t *txn, tidesdb_column_family_t *data_cf, uint keynr,
                  int64_t *total_docs, int64_t *total_words)
{
    uchar mk[FTS_META_KEY_LEN];
    mk[0] = KEY_NS_META;
    memcpy(mk + FTS_META_KEY_TAG_OFFSET, FTS_META_KEY_TAG, FTS_META_KEY_TAG_LEN);
    mk[FTS_META_KEY_KEYNR_OFFSET] = (uchar)keynr;

    uint8_t *val = NULL;
    size_t vlen = 0;
    *total_docs = 0;
    *total_words = 0;

    int rc = tidesdb_txn_get(txn, data_cf, mk, FTS_META_KEY_LEN, &val, &vlen);
    if (rc == TDB_SUCCESS && tidesdb::fts::decode_meta_value(val, vlen, total_docs, total_words))
    {
        tidesdb_free(val);
        return TDB_SUCCESS;
    }
    if (val) tidesdb_free(val);
    /* TDB_ERR_NOT_FOUND is the legitimate empty-index case. */
    return rc;
}

/* Update FTS metadata counters atomically within the current transaction.
   thd may be NULL for paths where no session is available such as recovery. */
static int fts_update_meta(THD *thd, tidesdb_txn_t *txn, tidesdb_column_family_t *data_cf,
                           uint keynr, int64_t delta_docs, int64_t delta_words)
{
    int64_t total_docs = 0, total_words = 0;
    /* A transient read failure must not be turned into a zero-based
       write-back, zero - delta clamped at 0 would persist garbage and
       only manual rebuild would fix BM25.  Fresh index (NOT_FOUND) is
       the only "no prior value" case we treat as zero. */
    int rrc = fts_load_meta(txn, data_cf, keynr, &total_docs, &total_words);
    if (rrc != TDB_SUCCESS && rrc != TDB_ERR_NOT_FOUND)
    {
        sql_print_error(
            "[TIDESDB] fts_update_meta: skipping meta write for keynr=%u "
            "because fts_load_meta failed (rc=%d); BM25 totals are unchanged",
            keynr, rrc);
        return rrc;
    }

    total_docs += delta_docs;
    total_words += delta_words;
    if (total_docs < 0) total_docs = 0;
    if (total_words < 0) total_words = 0;

    uchar mk[FTS_META_KEY_LEN];
    mk[0] = KEY_NS_META;
    memcpy(mk + FTS_META_KEY_TAG_OFFSET, FTS_META_KEY_TAG, FTS_META_KEY_TAG_LEN);
    mk[FTS_META_KEY_KEYNR_OFFSET] = (uchar)keynr;

    uchar mv[FTS_META_VALUE_LEN];
    tidesdb::fts::encode_meta_value(total_docs, total_words, (uint8_t *)mv);
    return tdb_txn_put_blocking(thd, txn, data_cf, mk, FTS_META_KEY_LEN, mv, FTS_META_VALUE_LEN,
                                TIDESDB_TTL_NONE);
}

/* Fold a per-row FTS meta delta into the txn-level accumulator.  Find the
   matching (data_cf, keynr) entry and combine, or append a new one.  The
   list is typically tiny (one or two FTS indexes per touched table), so
   linear scan beats a hash. */
void trx_fts_meta_accumulate(tidesdb_trx_t *trx, tidesdb_column_family_t *cf, uint keynr,
                             int64_t doc_delta, int64_t word_delta)
{
    if (!trx) return;
    for (auto &e : trx->fts_meta_pending)
    {
        if (e.data_cf == cf && e.keynr == keynr)
        {
            e.doc_delta += doc_delta;
            e.word_delta += word_delta;
            trx->fts_meta_dirty = true;
            return;
        }
    }
    trx->fts_meta_pending.push_back({cf, keynr, doc_delta, word_delta});
    trx->fts_meta_dirty = true;
}

/* Apply every accumulated FTS meta delta to its index's meta key inside
   the current txn.  Called before tidesdb_commit hands the txn to the
   library and before maybe_bulk_commit's mid-statement commit so the meta
   update is part of the same commit as the row puts that produced it.
   Returns a TDB_* error code on the first failure; the accumulator is
   cleared in every case since the txn it tracks is about to commit or be
   rolled back. */
int flush_trx_fts_meta_pending(THD *thd, tidesdb_trx_t *trx)
{
    if (!trx) return TDB_SUCCESS;
    if (!trx->fts_meta_dirty || trx->fts_meta_pending.empty() || !trx->txn)
    {
        trx->fts_meta_pending.clear();
        trx->fts_meta_dirty = false;
        return TDB_SUCCESS;
    }
    int rc = TDB_SUCCESS;
    for (const auto &e : trx->fts_meta_pending)
    {
        rc = fts_update_meta(thd, trx->txn, e.data_cf, e.keynr, e.doc_delta, e.word_delta);
        if (rc != TDB_SUCCESS) break;
    }
    trx->fts_meta_pending.clear();
    trx->fts_meta_dirty = false;
    return rc;
}

/* Minimum and maximum word length for FTS indexing (in characters).
   These mirror InnoDB's innodb_ft_min_token_size / innodb_ft_max_token_size
   defaults.  Exposed as session variables below for tuning. */
ulong srv_fts_min_word_len = 3;
ulong srv_fts_max_word_len = 84;

/* Blend characters -- characters that are indexed as both separators and valid
   word characters.  When a blend char appears inside a token, the tokenizer
   emits the full blended form plus each sub-part between blend characters.
   For example, with blend_chars="'" and input "l'aria":
     -- "l'aria" (full blended token)
     -- "l"      (left part, may be filtered by min_word_len)
     -- "aria"   (right part)
   This allows Italian/French elision (dell'aria, l'homme) and Irish/Scottish
   names (O'Malley) to be searchable by any component or the full form.
   Default is empty (no blend characters). Set to "'" for Romance languages. */
char *srv_fts_blend_chars = NULL;

/* Fast lookup table for blend characters, indexed by raw byte value
   (covers the full 8-bit range).  Rebuilt when the sysvar changes. */
static constexpr uint TDB_BLEND_MAP_SIZE = 256;
static bool tdb_blend_char_map[TDB_BLEND_MAP_SIZE] = {false};
static mysql_rwlock_t tdb_blend_lock;
static PSI_rwlock_key tdb_blend_lock_key;

static void tdb_rebuild_blend_map(const char *chars)
{
    memset(tdb_blend_char_map, 0, sizeof(tdb_blend_char_map));
    if (!chars) return;
    for (const char *p = chars; *p; p++) tdb_blend_char_map[(unsigned char)*p] = true;
}

void tdb_fts_blend_chars_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr,
                                const void *save)
{
    const char *new_val = *static_cast<const char *const *>(save);
    mysql_rwlock_wrlock(&tdb_blend_lock);
    tdb_rebuild_blend_map(new_val);
    mysql_rwlock_unlock(&tdb_blend_lock);
    *static_cast<const char **>(var_ptr) = new_val;
    if (new_val && new_val[0])
        sql_print_information("[TIDESDB] FTS blend_chars set to '%s'", new_val);
    else
        sql_print_information("[TIDESDB] FTS blend_chars cleared");
}

/* Stop word support
   Mirrors InnoDB's innodb_ft_server_stopword_table.  When NULL, we use the
   default list from information_schema.INNODB_FT_DEFAULT_STOPWORD.
   When set to "db/table", we read the 'value' column at next FTS rebuild.
   The stop word set is stored in a global unordered_set protected by a
   read-mostly rwlock (writes are rare -- only on SET GLOBAL or plugin init). */
char *srv_ft_stopword_table = NULL; /* db/table or NULL for defaults */

/* InnoDB's default stop words, copied verbatim from INNODB_FT_DEFAULT_STOPWORD.
   The array holds 36 entries but InnoDB itself lists "the" twice, so 35 distinct
   words load into the set below.  Kept identical to InnoDB's list on purpose. */
static const char *tdb_default_stopwords[] = {
    "a",   "about", "an",   "are", "as",   "at",  "be",  "by",   "com",  "de",
    "en",  "for",   "from", "how", "i",    "in",  "is",  "it",   "la",   "of",
    "on",  "or",    "that", "the", "this", "to",  "was", "what", "when", "where",
    "who", "will",  "with", "und", "the",  "www", NULL};

static std::unordered_set<std::string> tdb_stopwords;
static mysql_rwlock_t tdb_stopword_lock;
static PSI_rwlock_key tdb_stopword_lock_key;

/* Load stop words from the default list */
static void tdb_load_default_stopwords()
{
    tdb_stopwords.clear();
    for (const char **w = tdb_default_stopwords; *w; w++) tdb_stopwords.insert(*w);
}

/* Check if a lowercased token is a stop word.
   PRECONDITION caller holds tdb_stopword_lock for reading (taken once per
   fts_tokenize call to avoid N lock pairs per document). */
static inline bool tdb_is_stopword_locked(const std::string &word)
{
    return tdb_stopwords.count(word) > 0;
}

/* Load stop words from a user table specified as "db_name/table_name".
   Must be called with tdb_stopword_lock held for writing.
   Uses TidesDB's own CF to read the table if it's a TidesDB table,
   or falls back to an empty set with a warning for other engines.
   For simplicity, the table must store one word per row in a column named 'value'
   and be accessible as a TidesDB CF named "db_name__table_name". */
/* parse a single stop-word row (self-describing header + one packed VARCHAR column) out of a raw
   value buffer and, when valid, insert the lowercased word into the global stop-word set. */
static void tdb_extract_stopword_from_row(const uint8_t *val, size_t val_size)
{
    if (!val || val_size <= ROW_HEADER_SIZE || val[0] != ROW_HEADER_MAGIC) return;

    /* The row carries the self-describing header written by serialize_row, so the null bitmap
       width is read from the header rather than assumed.  After the header and the bitmap a
       single-column table holds just the one packed VARCHAR. */
    uint stored_null_bytes = uint2korr(val + 1);
    size_t off = (size_t)ROW_HEADER_SIZE + stored_null_bytes;
    if (off >= val_size) return;

    const uint8_t *data = val + off;
    size_t data_len = val_size - off;

    /* Field::pack stores a VARCHAR with a one-byte length prefix when the column is at most 255
       chars wide and a two-byte prefix otherwise.  The packed field of a single-column row spans
       the whole remaining buffer, so the prefix width is the one whose recorded length consumes
       exactly the rest. */
    uint prefix = 0;
    size_t str_len = 0;
    if (data_len >= 1 && (size_t)data[0] + 1 == data_len)
    {
        prefix = 1;
        str_len = data[0];
    }
    else if (data_len >= FIELD_VARCHAR_LEN_PREFIX &&
             (size_t)uint2korr(data) + FIELD_VARCHAR_LEN_PREFIX == data_len)
    {
        prefix = FIELD_VARCHAR_LEN_PREFIX;
        str_len = uint2korr(data);
    }
    if (prefix && str_len > 0)
    {
        std::string word((const char *)(data + prefix), str_len);
        std::transform(word.begin(), word.end(), word.begin(), ::tolower);
        tdb_stopwords.insert(std::move(word));
    }
}

static bool tdb_load_stopwords_from_table_spec(const char *table_spec)
{
    if (!table_spec || !table_spec[0]) return false;

    const char *slash = strchr(table_spec, '/');
    if (!slash)
    {
        sql_print_warning(
            "[TIDESDB] ft_stopword_table format must be 'db_name/table_name', got '%s'",
            table_spec);
        return false;
    }

    std::string db_name(table_spec, slash - table_spec);
    std::string tbl_name(slash + 1);

    /* CF names join the database and table with CF_DB_TABLE_SEP, the same
       way path_to_cf_name builds them, so the lookup has to use that
       separator rather than the slash from the user-facing spec. */
    std::string cf_name = db_name + CF_DB_TABLE_SEP + tbl_name;
    tidesdb_column_family_t *sw_cf =
        tdb_global ? tidesdb_get_column_family(tdb_global, cf_name.c_str()) : NULL;

    if (!sw_cf)
    {
        sql_print_warning(
            "[TIDESDB] Stop word table '%s' not found as TidesDB CF '%s'. "
            "The table must be a TidesDB ENGINE table. Keeping current stop words.",
            table_spec, cf_name.c_str());
        return false;
    }

    /* We scan the CF for all keys with DATA namespace prefix.
       Each row should have a 'value' field which we extract via full table scan. */
    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin(tdb_global, &txn) != TDB_SUCCESS) return false;

    tidesdb_iter_t *iter = NULL;
    if (tdb_iter_new_blocking(current_thd, txn, sw_cf, &iter) != TDB_SUCCESS)
    {
        tidesdb_txn_free(txn);
        return false;
    }

    tidesdb_iter_seek_to_first(iter);
    tdb_stopwords.clear();

    while (tidesdb_iter_valid(iter))
    {
        uint8_t *val = NULL;
        size_t val_size = 0;
        tdb_owned_buf val_g(val);
        if (tidesdb_iter_value(iter, &val, &val_size) == TDB_SUCCESS)
            tdb_extract_stopword_from_row(val, val_size);
        tidesdb_iter_next(iter);
    }

    tidesdb_iter_free(iter);
    tidesdb_txn_free(txn);

    sql_print_information("[TIDESDB] Loaded %zu stop words from table '%s'", tdb_stopwords.size(),
                          table_spec);
    return true;
}

/* Sysvar update callback for tidesdb_ft_stopword_table */
void tdb_ft_stopword_table_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr,
                                  const void *save)
{
    const char *new_val = *static_cast<const char *const *>(save);
    mysql_rwlock_wrlock(&tdb_stopword_lock);

    if (!new_val || !new_val[0])
    {
        /* NULL or empty string -- we reset to defaults */
        tdb_load_default_stopwords();
        sql_print_information("[TIDESDB] Stop words reset to defaults (%zu words)",
                              tdb_stopwords.size());
    }
    else
    {
        if (!tdb_load_stopwords_from_table_spec(new_val))
        {
            sql_print_warning("[TIDESDB] Failed to load stop words from '%s', keeping current set",
                              new_val);
        }
    }

    *static_cast<const char **>(var_ptr) = new_val;
    mysql_rwlock_unlock(&tdb_stopword_lock);
}

/* BM25 tuning parameters.  k1 controls term-frequency saturation
   (higher = more weight to repeated terms).  b controls document-length
   normalization (0 = no normalization, 1 = full normalization). */
double srv_fts_bm25_k1 = 1.2;
double srv_fts_bm25_b = 0.75;

/* Helper to lowercase, check stop words, length filter, and emit a token */
/* adapter exposing MariaDB's charset behaviour to the server-free fts core. */
struct fts_mariadb_charset : tidesdb::fts::charset
{
    CHARSET_INFO *cs;
    explicit fts_mariadb_charset(CHARSET_INFO *c) : cs(c)
    {
    }
    unsigned mbchar_len(const char *p, const char *end) const override
    {
        return my_ismbchar(cs, p, end);
    }
    bool is_alnum(unsigned char c) const override
    {
        return my_isalnum(cs, c) != 0;
    }
    std::string casedn(const char *s, size_t len) const override
    {
        std::string r(s, len);
        if (!r.empty())
        {
            size_t n = cs->cset->casedn(cs, &r[0], r.size(), &r[0], r.size());
            r.resize(n);
        }
        return r;
    }
};

/* snapshot the tunable length bounds, blend map, and stop-word set into a core tokenize_opts,
   taking each read lock once so the server-free core never reaches into a global or a lock. the
   blend-map and stop-word snapshots live in caller storage so they outlast the returned opts. */
static void fts_snapshot_opts(tidesdb::fts::tokenize_opts &opts,
                              bool (&blend_snap)[tidesdb::fts::BLEND_MAP_SIZE],
                              std::unordered_set<std::string> &stop_snap)
{
    opts.min_word_len = (unsigned)srv_fts_min_word_len;
    opts.max_word_len = (unsigned)srv_fts_max_word_len;

    mysql_rwlock_rdlock(&tdb_blend_lock);
    memcpy(blend_snap, tdb_blend_char_map, sizeof(blend_snap));
    mysql_rwlock_unlock(&tdb_blend_lock);
    opts.blend_map = blend_snap;

    mysql_rwlock_rdlock(&tdb_stopword_lock);
    stop_snap = tdb_stopwords;
    mysql_rwlock_unlock(&tdb_stopword_lock);
    opts.stopwords = &stop_snap;
}

/* charset-aware tokenizer that snapshots the tunable dictionaries and delegates splitting, case
   folding, and blend handling to the server-free fts core, which measures blend sub-part length in
   characters rather than bytes. */
void fts_tokenize(const char *text, size_t text_len, CHARSET_INFO *cs,
                  std::vector<std::string> &out)
{
    fts_mariadb_charset mcs(cs);
    tidesdb::fts::tokenize_opts opts;
    bool blend_snap[tidesdb::fts::BLEND_MAP_SIZE];
    std::unordered_set<std::string> stop_snap;
    fts_snapshot_opts(opts, blend_snap, stop_snap);

    tidesdb::fts::tokenize(text, text_len, mcs, opts, out);
}

/* Extract and tokenize the document from all FULLTEXT key_part fields.
   Returns the token list and word count. */
void fts_extract_and_tokenize(TABLE *table, const KEY *key_info, const uchar *record,
                              CHARSET_INFO *cs, std::vector<std::string> &out_tokens)
{
    std::string doc;
    my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(record - table->record[0]);

    for (uint p = 0; p < key_info->user_defined_key_parts; p++)
    {
        Field *f = key_info->key_part[p].field;
        if (ptrdiff) f->move_field_offset(ptrdiff);
        if (!f->is_null())
        {
            String val;
            f->val_str(&val);
            if (!doc.empty()) doc += ' ';
            doc.append(val.ptr(), val.length());
        }
        if (ptrdiff) f->move_field_offset(-ptrdiff);
    }

    fts_tokenize(doc.data(), doc.size(), cs, out_tokens);
}

/* boolean-mode query parser that snapshots the tunable dictionaries and delegates the whole
   grammar, the required and excluded operators, truncation, quoted phrases, and plain terms, to the
   server-free core parser, which expands a phrase into a phrase term plus its words as required
   terms and folds case through the charset adapter. */
void fts_parse_boolean(const char *query, size_t len, CHARSET_INFO *cs,
                       std::vector<tidesdb::fts::query_term> &out)
{
    fts_mariadb_charset mcs(cs);
    tidesdb::fts::tokenize_opts opts;
    bool blend_snap[tidesdb::fts::BLEND_MAP_SIZE];
    std::unordered_set<std::string> stop_snap;
    fts_snapshot_opts(opts, blend_snap, stop_snap);

    tidesdb::fts::parse_boolean(query, len, mcs, opts, out);
}

void fts_init()
{
    mysql_rwlock_init(tdb_stopword_lock_key, &tdb_stopword_lock);
    tdb_load_default_stopwords();
    sql_print_information("[TIDESDB] Loaded %zu default stop words", tdb_stopwords.size());

    mysql_rwlock_init(tdb_blend_lock_key, &tdb_blend_lock);
    tdb_rebuild_blend_map(srv_fts_blend_chars);
}

void fts_deinit()
{
    mysql_rwlock_destroy(&tdb_stopword_lock);
    mysql_rwlock_destroy(&tdb_blend_lock);
    tdb_stopwords.clear();
}
