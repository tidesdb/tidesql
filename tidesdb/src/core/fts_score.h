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

/* server-free core for full-text relevance scoring, the okapi bm25 formula. the handler
 * gathers postings for a term from the index and reads the corpus totals from the fts meta
 * row, then this module turns a term's document frequency and each posting's term frequency
 * and document length into a score. the math is split the way the runtime uses it, one set of
 * per-term weights folded once, then a cheap per-posting score, so the scoring stays testable
 * without an index or a server. */
#pragma once

#include <cstdint>

namespace tidesdb
{
namespace fts_score
{

/* smoothing added to both sides of the idf ratio, the lucene form. */
static constexpr double IDF_EPSILON = 0.5;

/* shift inside the idf logarithm that keeps idf non-negative for a term present in no more
 * documents than the corpus holds. */
static constexpr double IDF_NONNEG_SHIFT = 1.0;

/* the classic bm25 term-frequency saturation and length-normalisation bases. */
static constexpr double TF_SATURATION_BOOST = 1.0;
static constexpr double LENGTH_NORM_BASE = 1.0;

/* average document length used when the corpus is empty, collapsing length normalisation. */
static constexpr double DEFAULT_AVGDL = 1.0;

/* floor for the corpus document count so the idf ratio never divides by zero. */
static constexpr int64_t MIN_TOTAL_DOCS = 1;

/**
 * avgdl
 * the average document length in words, or a neutral fallback when nothing is indexed
 * @param total_words the corpus word total
 * @param total_docs the corpus document total
 * @return total_words divided by total_docs, or DEFAULT_AVGDL when total_docs is zero
 */
double avgdl(int64_t total_words, int64_t total_docs);

/**
 * idf
 * the inverse document frequency of a term, in the lucene smoothed non-negative form; the
 * corpus count is floored so the ratio is well defined, and the result is floored at zero so
 * a stale meta count that reports fewer documents than the term's own postings can never
 * produce a negative weight
 * @param total_docs the corpus document total
 * @param df the number of documents containing the term
 * @return the non-negative inverse document frequency
 */
double idf(int64_t total_docs, int64_t df);

/**
 * term_weights
 * the per-term constants folded once so each posting score is a single expression
 * @param idf_x_k1_plus_1 idf times (k1 + saturation boost)
 * @param k1_one_minus_b k1 times (length base - b)
 * @param k1_b_inv_avgdl k1 times b divided by the average document length
 */
struct term_weights
{
    double idf_x_k1_plus_1;
    double k1_one_minus_b;
    double k1_b_inv_avgdl;
};

/**
 * make_term_weights
 * fold a term's idf and the bm25 tunables into the reusable per-term weights
 * @param term_idf the term's inverse document frequency
 * @param k1 the term-frequency saturation parameter
 * @param b the length-normalisation parameter
 * @param inv_avgdl one divided by the average document length
 * @return the folded weights
 */
term_weights make_term_weights(double term_idf, double k1, double b, double inv_avgdl);

/**
 * posting_score
 * the bm25 score contribution of one posting for a term
 * @param w the term's folded weights
 * @param tf the posting's term frequency
 * @param doc_len the posting document's length in words
 * @return the score contribution, summed by the caller across a document's matching terms
 */
double posting_score(const term_weights &w, uint32_t tf, uint32_t doc_len);

}  // namespace fts_score
}  // namespace tidesdb
