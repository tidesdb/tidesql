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

#include "fts_score.h"

#include <cmath>

namespace tidesdb
{
namespace fts_score
{

double avgdl(int64_t total_words, int64_t total_docs)
{
    return total_docs > 0 ? (double)total_words / (double)total_docs : DEFAULT_AVGDL;
}

double idf(int64_t total_docs, int64_t df)
{
    int64_t docs = total_docs < MIN_TOTAL_DOCS ? MIN_TOTAL_DOCS : total_docs;
    double value = std::log(((double)docs - (double)df + IDF_EPSILON) / ((double)df + IDF_EPSILON) +
                            IDF_NONNEG_SHIFT);
    /* the shift keeps this non-negative whenever df does not exceed docs; a stale meta count
     * can report fewer documents than the term's own postings, so floor it here to hold the
     * documented non-negative invariant. */
    return value < 0.0 ? 0.0 : value;
}

term_weights make_term_weights(double term_idf, double k1, double b, double inv_avgdl)
{
    term_weights w;
    w.idf_x_k1_plus_1 = term_idf * (k1 + TF_SATURATION_BOOST);
    w.k1_one_minus_b = k1 * (LENGTH_NORM_BASE - b);
    w.k1_b_inv_avgdl = k1 * b * inv_avgdl;
    return w;
}

double posting_score(const term_weights &w, uint32_t tf, uint32_t doc_len)
{
    double denom = (double)tf + w.k1_one_minus_b + w.k1_b_inv_avgdl * (double)doc_len;
    return ((double)tf * w.idf_x_k1_plus_1) / denom;
}

}  // namespace fts_score
}  // namespace tidesdb
