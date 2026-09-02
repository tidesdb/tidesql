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

/* the full-text search side: the FT_INFO context and its vtables, and the ha_tidesdb ft_* methods
   that plan a MATCH ... AGAINST, score its postings with okapi bm25, and stream ranked rows. the
   tokenizer, boolean parser, and inverted-index maintenance it leans on live in ha_tidesdb_fts.cc.
 */

#include "ha_tidesdb.h"

#include <ft_global.h>
#include <mysql/plugin.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/core/fts_score.h"
#include "src/core/fts_text.h"
#include "src/handler/ha_tidesdb_fts.h"
#include "src/handler/ha_tidesdb_fts_internal.h"
#include "src/handler/ha_tidesdb_internal.h"

/* ******************** Full-Text Search context and vtables ******************** */

/* FTS result entry -- one per matching document */
struct tdb_fts_result_t
{
    uchar *pk; /* heap-allocated comparable PK bytes */
    uint pk_len;
    float rank; /* BM25 score */
};

/* FTS search context returned by ft_init_ext as FT_INFO* */
struct tdb_ft_info_t
{
    struct _ft_vft *please;                /* required by MariaDB FT_INFO layout */
    struct _ft_vft_ext *could_you;         /* extended FT API (HA_CAN_FULLTEXT_EXT) */
    ha_tidesdb *handler;                   /* back-pointer for row fetching */
    uint keynr;                            /* which FTS index */
    std::vector<tdb_fts_result_t> results; /* sorted by rank descending */
    size_t current_idx;                    /* iteration position */
    float current_rank;                    /* rank of last-returned row */
    ulonglong match_count;                 /* total matches for count_matches() */
};

/* Forward declarations of FT_INFO vtable callbacks */
static int tdb_fts_read_next(FT_INFO *, char *);
static float tdb_fts_find_relevance(FT_INFO *, uchar *, uint);
static void tdb_fts_close_search(FT_INFO *);
static float tdb_fts_get_relevance(FT_INFO *);
static void tdb_fts_reinit_search(FT_INFO *);

static const struct _ft_vft tdb_ft_vft = {tdb_fts_read_next, tdb_fts_find_relevance,
                                          tdb_fts_close_search, tdb_fts_get_relevance,
                                          tdb_fts_reinit_search};

/* Extended FT API callbacks for HA_CAN_FULLTEXT_EXT */
static uint tdb_fts_get_version()
{
    return 2;
}

static ulonglong tdb_fts_get_flags()
{
    return FTS_ORDERED_RESULT;
}

static ulonglong tdb_fts_get_docid(FT_INFO_EXT *fts)
{
    tdb_ft_info_t *info = reinterpret_cast<tdb_ft_info_t *>(fts);
    if (info->current_idx > 0 && info->current_idx <= info->results.size())
        return (ulonglong)(info->current_idx); /* 1-based doc ID */
    return 0;
}

static ulonglong tdb_fts_count_matches(FT_INFO_EXT *fts)
{
    tdb_ft_info_t *info = reinterpret_cast<tdb_ft_info_t *>(fts);
    return info->match_count;
}

static struct _ft_vft_ext tdb_ft_vft_ext = {tdb_fts_get_version, tdb_fts_get_flags,
                                            tdb_fts_get_docid, tdb_fts_count_matches};

/* FT_INFO vtable callback implementations */
static int tdb_fts_read_next(FT_INFO *, char *)
{
    return HA_ERR_END_OF_FILE; /* not used -- ft_read() is the entry point */
}

static float tdb_fts_find_relevance(FT_INFO *fts, uchar *, uint)
{
    tdb_ft_info_t *info = reinterpret_cast<tdb_ft_info_t *>(fts);
    return info->current_rank;
}

static float tdb_fts_get_relevance(FT_INFO *fts)
{
    tdb_ft_info_t *info = reinterpret_cast<tdb_ft_info_t *>(fts);
    return info->current_rank;
}

static void tdb_fts_close_search(FT_INFO *fts)
{
    tdb_ft_info_t *info = reinterpret_cast<tdb_ft_info_t *>(fts);
    for (auto &r : info->results) my_free(r.pk);
    delete info;
}

static void tdb_fts_reinit_search(FT_INFO *fts)
{
    tdb_ft_info_t *info = reinterpret_cast<tdb_ft_info_t *>(fts);
    info->current_idx = 0;
}

/* ******************** Full-Text Search methods ******************** */

int ha_tidesdb::ft_init()
{
    DBUG_ENTER("ha_tidesdb::ft_init");
    if (ft_handler)
    {
        tdb_ft_info_t *info = reinterpret_cast<tdb_ft_info_t *>(ft_handler);
        info->current_idx = 0;
    }
    DBUG_RETURN(0);
}

void ha_tidesdb::ft_end()
{
    DBUG_ENTER("ha_tidesdb::ft_end");
    DBUG_VOID_RETURN;
}

/* one FTS posting read from the index column family: the matched row's primary key plus the term
   frequency and document length the BM25 score needs. */
struct fts_posting_t
{
    std::string pk;
    uint16 tf;
    uint32 doc_len;
};

/* parse a MATCH..AGAINST search string into query terms, boolean-mode when FT_BOOL is set and a
   flat neutral token list otherwise. */
static void ft_parse_query(uint flags, const char *keyp, size_t keylen, CHARSET_INFO *cs,
                           std::vector<tidesdb::fts::query_term> &terms)
{
    if (flags & FT_BOOL)
    {
        fts_parse_boolean(keyp, keylen, cs, terms);
        return;
    }

    /* natural-language mode treats every token as a neutral scoring term, which is the default
       requirement on a freshly constructed query_term. */
    std::vector<std::string> tokens;
    fts_tokenize(keyp, keylen, cs, tokens);
    for (auto &tok : tokens)
    {
        tidesdb::fts::query_term qt;
        qt.term = std::move(tok);
        terms.push_back(std::move(qt));
    }
}

/* allocate and initialize an empty FTS result handle bound to handler h and index inx. */
static tdb_ft_info_t *ft_new_info(ha_tidesdb *h, uint inx)
{
    tdb_ft_info_t *info = new tdb_ft_info_t();
    info->please = const_cast<_ft_vft *>(&tdb_ft_vft);
    info->could_you = &tdb_ft_vft_ext;
    info->handler = h;
    info->keynr = inx;
    info->current_idx = 0;
    info->current_rank = 0.0f;
    info->match_count = 0;
    return info;
}

/* gather every posting for one query term from the FTS iterator, following the wildcard length
   buckets for a truncated term or a single prefix seek for an exact term. */
static void ft_gather_postings(tidesdb_iter_t *it, const tidesdb::fts::query_term &qt, size_t qlen,
                               std::vector<fts_posting_t> &postings)
{
    uchar prefix[FTS_TERM_LEN_PREFIX + FTS_MAX_TERM_BYTES];
    uint prefix_len = 0;
    int2store(prefix, (uint16)qlen);
    prefix_len += FTS_TERM_LEN_PREFIX;
    memcpy(prefix + prefix_len, qt.term.data(), qlen);
    prefix_len += (uint)qlen;

    if (qt.trunc)
    {
        /* Wildcard search keys are sorted by [2B term_len][term][pk],
           so terms of different lengths are in different regions.
           We iterate over each possible term length from the prefix
           length up to max_word_len, seeking directly to [len][prefix]
           for each bucket.  This is O(max_word_len) seeks, each precise. */
        uint min_len = (uint)qlen;
        uint max_len = (uint)srv_fts_max_word_len;
        if (max_len > FTS_MAX_TERM_BYTES) max_len = FTS_MAX_TERM_BYTES;

        for (uint tlen = min_len; tlen <= max_len; tlen++)
        {
            uchar seek[FTS_TERM_LEN_PREFIX + FTS_MAX_TERM_BYTES];
            int2store(seek, (uint16)tlen);
            memcpy(seek + FTS_TERM_LEN_PREFIX, qt.term.data(), qlen);
            uint seek_len = FTS_TERM_LEN_PREFIX + (uint)qlen;

            tidesdb_iter_seek(it, seek, seek_len);
            while (tidesdb_iter_valid(it))
            {
                uint8_t *ik = NULL;
                size_t iks = 0;
                uint8_t *iv = NULL;
                size_t ivs = 0;
                if (tidesdb_iter_key_value(it, &ik, &iks, &iv, &ivs) != TDB_SUCCESS) break;
                tdb_owned_buf ik_g(ik), iv_g(iv);

                if (iks < FTS_TERM_LEN_PREFIX) break;
                uint16 stored_len = uint2korr(ik);
                if (stored_len != tlen) break;

                if (iks < (size_t)(FTS_TERM_LEN_PREFIX + stored_len)) break;
                if (memcmp(ik + FTS_TERM_LEN_PREFIX, qt.term.data(), qlen) != 0) break;

                uint pk_off = FTS_TERM_LEN_PREFIX + stored_len;
                if (iks <= pk_off)
                {
                    tidesdb_iter_next(it);
                    continue;
                }
                std::string pk((char *)(ik + pk_off), iks - pk_off);

                if (ivs >= FTS_VALUE_LEN)
                    postings.push_back({pk, (uint16)uint2korr(iv),
                                        (uint32)uint4korr(iv + FTS_VALUE_DOC_LEN_OFFSET)});

                tidesdb_iter_next(it);
            }
        }
        return;
    }

    tidesdb_iter_seek(it, prefix, prefix_len);
    /* exact-match path (non-truncated) */
    while (tidesdb_iter_valid(it))
    {
        uint8_t *ik = NULL;
        size_t iks = 0;
        uint8_t *iv = NULL;
        size_t ivs = 0;
        if (tidesdb_iter_key_value(it, &ik, &iks, &iv, &ivs) != TDB_SUCCESS) break;
        tdb_owned_buf ik_g(ik), iv_g(iv);

        if (iks < prefix_len || memcmp(ik, prefix, prefix_len) != 0) break;
        std::string pk((char *)(ik + prefix_len), iks - prefix_len);

        if (ivs >= FTS_VALUE_LEN)
            postings.push_back(
                {pk, (uint16)uint2korr(iv), (uint32)uint4korr(iv + FTS_VALUE_DOC_LEN_OFFSET)});
        tidesdb_iter_next(it);
    }
}

/* score every query term against the FTS index, accumulating per-document BM25 scores in
   doc_scores and, for boolean required terms, dropping documents that miss any of them.  reports
   how many terms were required through num_required. */
static void ft_score_terms(THD *thd, tidesdb_txn_t *txn, tidesdb_column_family_t *fts_cf,
                           const std::vector<tidesdb::fts::query_term> &terms, int64_t total_docs,
                           double inv_avgdl, std::unordered_map<std::string, double> &doc_scores,
                           uint &num_required)
{
    std::unordered_map<std::string, uint> doc_required_hits;
    std::unordered_set<std::string> excluded_pks;
    num_required = 0;

    /* Open one iterator over the FTS CF and reuse it across every query
       term -- iterator construction does an O(num_sstables) merge-heap
       build, so doing it per term made the heap work scale linearly with
       term count.  All terms in this MATCH AGAINST live in the same
       index, so a single iterator is reseeked for each term. */
    tidesdb_iter_t *shared_it = NULL;
    {
        int sirc = tdb_iter_new_blocking(thd, txn, fts_cf, &shared_it);
        if (sirc != TDB_SUCCESS) shared_it = NULL;
    }

    for (auto &qt : terms)
    {
        if (qt.req == tidesdb::fts::requirement::required) num_required++;

        /* fts_build_key truncates inserted terms to FTS_MAX_TERM_BYTES, so on-disk
           keys never carry more than that.  The query term lives in a std::string
           that has no such cap, so cap the copy length to the on-disk maximum. */
        size_t qlen = qt.term.size();
        if (qlen > FTS_MAX_TERM_BYTES) qlen = FTS_MAX_TERM_BYTES;

        if (!shared_it) continue;
        std::vector<fts_posting_t> postings;
        ft_gather_postings(shared_it, qt, qlen, postings);

        uint32 df = (uint32)postings.size();
        const double k1 = srv_fts_bm25_k1, b = srv_fts_bm25_b;

        /* fold the term's idf and the bm25 tunables into per-posting weights once. */
        tidesdb::fts_score::term_weights w = tidesdb::fts_score::make_term_weights(
            tidesdb::fts_score::idf(total_docs, df), k1, b, inv_avgdl);

        /* Reserve approximate growth so the per-posting unordered_map
           insert doesn't rehash; modest win on terms with many matches. */
        if (qt.req != tidesdb::fts::requirement::excluded)
        {
            doc_scores.reserve(doc_scores.size() + postings.size());
            if (qt.req == tidesdb::fts::requirement::required)
                doc_required_hits.reserve(doc_required_hits.size() + postings.size());
        }

        for (auto &p : postings)
        {
            double score = tidesdb::fts_score::posting_score(w, p.tf, p.doc_len);

            if (qt.req == tidesdb::fts::requirement::excluded)
            {
                /* excluded term.  We record the pk and subtract it after the
                   positive pass, so a negative term parsed before the positive
                   term that adds the same document still excludes it. */
                excluded_pks.insert(p.pk);
            }
            else
            {
                doc_scores[p.pk] += score;
                if (qt.req == tidesdb::fts::requirement::required) doc_required_hits[p.pk]++;
            }
        }
    }

    if (shared_it) tidesdb_iter_free(shared_it);

    /* Apply exclusions after the positive pass so the order of terms in the
       query does not matter.  Each excluded document leaves both the score and
       the required-hit maps. */
    for (const auto &pk : excluded_pks)
    {
        doc_scores.erase(pk);
        doc_required_hits.erase(pk);
    }

    /* bool mode -- we filter docs that don't match all required terms */
    if (num_required > 0)
    {
        for (auto it = doc_scores.begin(); it != doc_scores.end();)
        {
            auto rh = doc_required_hits.find(it->first);
            if (rh == doc_required_hits.end() || rh->second < num_required)
                it = doc_scores.erase(it);
            else
                ++it;
        }
    }
}

/* copy the scored documents into the result handle, allocating a server-owned copy of each pk. */
static void ft_fill_results(tdb_ft_info_t *info,
                            std::unordered_map<std::string, double> &doc_scores)
{
    for (auto &kv : doc_scores)
    {
        const auto &pk_str = kv.first;
        auto &score = kv.second;
        tdb_fts_result_t r;
        r.pk_len = (uint)pk_str.size();
        r.pk = (uchar *)my_malloc(PSI_NOT_INSTRUMENTED, r.pk_len, MYF(0));
        if (!r.pk) continue;
        memcpy(r.pk, pk_str.data(), r.pk_len);
        r.rank = (float)score;
        info->results.push_back(r);
    }
}

/* re-tokenize the row currently in record[0] and test that every phrase term appears in it as a
   consecutive word sequence. */
static bool ft_doc_matches_phrases(TABLE *table, KEY *ki, CHARSET_INFO *vcs,
                                   const std::vector<const tidesdb::fts::query_term *> &phrases,
                                   std::vector<std::string> &doc_tokens)
{
    doc_tokens.clear();
    fts_extract_and_tokenize(table, ki, table->record[0], vcs, doc_tokens);
    for (auto *ph : phrases)
        if (!tidesdb::fts::phrase_in_tokens(doc_tokens, ph->phrase_words)) return false;
    return true;
}

FT_INFO *ha_tidesdb::ft_init_ext(uint flags, uint inx, String *key)
{
    DBUG_ENTER("ha_tidesdb::ft_init_ext");

    if (!share || inx >= share->idx_cfs.size() || !share->idx_cfs[inx] ||
        !is_fts_index(&table->key_info[inx]))
        DBUG_RETURN(NULL);

    {
        int erc = ensure_stmt_txn();
        if (erc) DBUG_RETURN(NULL);
    }

    CHARSET_INFO *cs = table->key_info[inx].key_part[0].field->charset();

    std::vector<tidesdb::fts::query_term> query_terms;
    ft_parse_query(flags, key->ptr(), key->length(), cs, query_terms);

    /* A query that tokenises down to nothing (e.g. all stop words, all
       characters below the min-word-len threshold) still has to return a
       usable FT_INFO, not NULL.  The server's execution path assumes an
       FT_INFO was handed back and leaves the diagnostics area in an
       inconsistent state when ft_init_ext yields NULL for reasons other
       than an outright error; debug builds trip Protocol::end_statement's
       DBUG_ASSERT(0).  We return an empty result set instead, which the
       optimizer folds into zero matched rows. */
    if (query_terms.empty()) DBUG_RETURN(reinterpret_cast<FT_INFO *>(ft_new_info(this, inx)));

    int64_t total_docs = 0, total_words = 0;
    fts_load_meta(stmt_txn, share->cf, inx, &total_docs, &total_words);
    double avgdl = tidesdb::fts_score::avgdl(total_words, total_docs);
    if (total_docs == 0) total_docs = BM25_MIN_TOTAL_DOCS; /* avoid division by zero */
    /* We precompute 1/avgdl so the per-posting BM25 loop multiplies instead of
       dividing.  Divisions are expensive on modern CPUs (~20 cycles vs ~5
       for a multiply) and this runs per term per matched document. */
    const double inv_avgdl = 1.0 / avgdl;

    std::unordered_map<std::string, double> doc_scores;
    uint num_required = 0;
    ft_score_terms(ha_thd(), stmt_txn, share->idx_cfs[inx], query_terms, total_docs, inv_avgdl,
                   doc_scores, num_required);

    tdb_ft_info_t *info = ft_new_info(this, inx);
    ft_fill_results(info, doc_scores);

    /* For any phrase terms in the query, we fetch each
       candidate row, re-tokenize the document, and check for the exact
       phrase as a consecutive word sequence.  We remove non-matching candidates. */
    std::vector<const tidesdb::fts::query_term *> phrases;
    for (auto &qt : query_terms)
        if (qt.is_phrase) phrases.push_back(&qt);

    if (!phrases.empty() && !info->results.empty())
    {
        CHARSET_INFO *vcs = table->key_info[inx].key_part[0].field->charset();
        std::vector<tdb_fts_result_t> verified;
        std::vector<std::string> doc_tokens;
        for (auto &r : info->results)
        {
            if (fetch_row_by_pk(stmt_txn, r.pk, r.pk_len, table->record[0])) continue;

            if (ft_doc_matches_phrases(table, &table->key_info[inx], vcs, phrases, doc_tokens))
                verified.push_back(r);
            else
                my_free(r.pk); /* free PK of non-matching result */
        }
        info->results = std::move(verified);
    }

    std::sort(info->results.begin(), info->results.end(),
              [](const tdb_fts_result_t &a, const tdb_fts_result_t &b) { return a.rank > b.rank; });

    info->match_count = (ulonglong)info->results.size();

    DBUG_RETURN(reinterpret_cast<FT_INFO *>(info));
}

int ha_tidesdb::ft_read(uchar *buf)
{
    DBUG_ENTER("ha_tidesdb::ft_read");

    if (cached_thd_ && thd_killed(cached_thd_)) DBUG_RETURN(HA_ERR_ABORTED_BY_USER);

    tdb_ft_info_t *info = reinterpret_cast<tdb_ft_info_t *>(ft_handler);
    if (!info)
    {
        table->status = STATUS_NOT_FOUND;
        DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    {
        int erc = ensure_stmt_txn();
        if (erc) DBUG_RETURN(erc);
    }

    while (info->current_idx < info->results.size())
    {
        tdb_fts_result_t &r = info->results[info->current_idx];
        info->current_rank = r.rank;

        int err = fetch_row_by_pk(stmt_txn, r.pk, r.pk_len, buf);
        if (err == HA_ERR_KEY_NOT_FOUND)
        {
            info->current_idx++;
            continue; /* skip stale entry */
        }
        if (err)
        {
            table->status = STATUS_NOT_FOUND;
            DBUG_RETURN(err);
        }

        info->current_idx++;
        table->status = 0;
        DBUG_RETURN(0);
    }

    table->status = STATUS_NOT_FOUND;
    DBUG_RETURN(HA_ERR_END_OF_FILE);
}
