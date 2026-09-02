/* Copyright (c) 2026 TidesDB Corp.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 */

/* unit tests for the server-free bm25 scoring core. */
#include "../src/core/fts_score.h"

#include <cmath>
#include <cstdint>

#include "test_utils.h"

using namespace tidesdb::fts_score;

static int tests_passed = 0;
static int tests_failed = 0;

static bool approx(double a, double b) { return std::fabs(a - b) < 1e-9; }

void test_avgdl(void)
{
    ASSERT_TRUE(approx(avgdl(1000, 100), 10.0));
    ASSERT_TRUE(approx(avgdl(0, 0), DEFAULT_AVGDL)); /* empty corpus falls back */
    ASSERT_TRUE(approx(avgdl(50, 0), DEFAULT_AVGDL));
}

void test_idf_non_negative_and_monotone(void)
{
    /* idf is positive for a term in fewer documents than the corpus, and decreases as the
     * term spreads across more documents. */
    double rare = idf(1000, 1);
    double common = idf(1000, 500);
    ASSERT_TRUE(rare > 0.0);
    ASSERT_TRUE(common > 0.0);
    ASSERT_TRUE(rare > common);
    /* matches the hand-computed lucene form for a rare term. */
    double expect = std::log((1000.0 - 1.0 + 0.5) / (1.0 + 0.5) + 1.0);
    ASSERT_TRUE(approx(rare, expect));
}

/* the fix: a stale meta count reporting fewer documents than the term's postings would give
 * a negative weight without the floor; the core floors it at zero. */
void test_idf_floored_on_stale_meta(void)
{
    ASSERT_TRUE(approx(idf(1, 100), 0.0));
    ASSERT_TRUE(idf(10, 1000) == 0.0);
    /* a zero corpus count is floored to one document rather than dividing by zero. */
    ASSERT_TRUE(std::isfinite(idf(0, 0)));
}

void test_posting_score_matches_formula(void)
{
    const double k1 = 1.2, b = 0.75;
    double dl_avg = avgdl(1000, 100); /* 10 */
    double inv = 1.0 / dl_avg;
    double term_idf = idf(100, 10);
    term_weights w = make_term_weights(term_idf, k1, b, inv);

    uint32_t tf = 3, doc_len = 12;
    double got = posting_score(w, tf, doc_len);

    double denom = (double)tf + k1 * (1.0 - b) + k1 * b * inv * (double)doc_len;
    double expect = ((double)tf * term_idf * (k1 + 1.0)) / denom;
    ASSERT_TRUE(approx(got, expect));
}

void test_posting_score_tf_saturation_and_length_norm(void)
{
    const double k1 = 1.2, b = 0.75;
    double inv = 1.0 / 10.0;
    term_weights w = make_term_weights(idf(1000, 20), k1, b, inv);

    /* more occurrences of the term raise the score. */
    ASSERT_TRUE(posting_score(w, 5, 10) > posting_score(w, 1, 10));
    /* a longer document lowers the score for the same term frequency. */
    ASSERT_TRUE(posting_score(w, 3, 40) < posting_score(w, 3, 5));
    /* the increase from tf saturates, so the step from 1 to 2 exceeds the step from 10 to 11. */
    double d_low = posting_score(w, 2, 10) - posting_score(w, 1, 10);
    double d_high = posting_score(w, 11, 10) - posting_score(w, 10, 10);
    ASSERT_TRUE(d_low > d_high);
}

int main(int argc, char **argv)
{
    INIT_TEST_FILTER(argc, argv);

    RUN_TEST(test_avgdl, tests_passed);
    RUN_TEST(test_idf_non_negative_and_monotone, tests_passed);
    RUN_TEST(test_idf_floored_on_stale_meta, tests_passed);
    RUN_TEST(test_posting_score_matches_formula, tests_passed);
    RUN_TEST(test_posting_score_tf_saturation_and_length_norm, tests_passed);

    PRINT_TEST_RESULTS(tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
