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

#include "autoinc.h"

namespace tidesdb
{
namespace autoinc
{

uint64_t next_on_grid(uint64_t after, uint64_t offset, uint64_t increment)
{
    uint64_t next;
    if (increment <= 1)
        next = after + 1;
    else
    {
        /* the server's arithmetic, wrap included, so its rounding of this value is a no-op */
        next = (after + increment - offset) / increment;
        next = next * increment + offset;
    }
    return next <= after ? UINT64_MAX : next;
}

uint64_t run_length(uint64_t first, uint64_t wanted, uint64_t increment)
{
    if (increment == 0) increment = 1;
    if (wanted == 0) return 1;
    if (wanted - 1 > (UINT64_MAX - first) / increment) return 1;
    return wanted;
}

}  // namespace autoinc
}  // namespace tidesdb
