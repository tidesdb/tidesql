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

/* server-free core for the records-in-range row estimate. the library supplies the range's live
 * key count directly, so this module holds only the two estimates that do not come from a range
 * count, the fallback used when the library call fails and the point estimate for an equality
 * taken from rec_per_key, along with the never-return-zero floor. keeping the numeric policy here
 * lets it be exercised without a server or a live column family. */
#pragma once

#include <cstdint>

namespace tidesdb
{
namespace rir
{

/* estimate returned when no table share is available yet. */
static constexpr uint64_t DEFAULT_EST = 10;

/* floor for a stored row count so the optimizer never sees zero rows. */
static constexpr uint64_t MIN_STATS_RECORDS = 2;

/* when no useful cost is available the estimate is this fraction of the table plus the floor. */
static constexpr uint64_t UNKNOWN_DENOM = 4;

/* smallest estimate ever returned, since the optimizer reads zero as an empty relation. */
static constexpr uint64_t REC_PER_KEY_FLOOR = 1;

/**
 * unknown_fallback
 * the estimate used when no useful range cost is available, a fixed fraction of the table
 * plus the floor
 * @param total the table's row count, expected to be at least one
 * @return total divided by the unknown denominator, plus the floor
 */
uint64_t unknown_fallback(uint64_t total);

/**
 * point_estimate
 * the estimate for an exact key match, taken from rec_per_key and capped at the table size
 * @param rpk the index rec_per_key for the matched prefix, or zero when unknown
 * @param total the table's row count, expected to be at least one
 * @return rec_per_key when known else the floor, never exceeding total
 */
uint64_t point_estimate(uint64_t rpk, uint64_t total);

}  // namespace rir
}  // namespace tidesdb
