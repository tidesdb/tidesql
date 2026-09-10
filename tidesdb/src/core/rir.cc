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

#include "rir.h"

namespace tidesdb
{
namespace rir
{

uint64_t unknown_fallback(uint64_t total)
{
    return (total / UNKNOWN_DENOM) + REC_PER_KEY_FLOOR;
}

uint64_t point_estimate(uint64_t rpk, uint64_t total)
{
    uint64_t est = rpk > 0 ? rpk : REC_PER_KEY_FLOOR;
    if (est > total) est = total;
    return est;
}

}  // namespace rir
}  // namespace tidesdb
