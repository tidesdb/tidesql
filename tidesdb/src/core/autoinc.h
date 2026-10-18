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

/* server-free core for auto-increment reservation. the server places a generated value on the
 * grid that auto_increment_offset and auto_increment_increment describe and spaces a reserved run
 * by the increment, so the engine reserves on that same grid, with the server's own formula, or
 * two sessions can round onto one value. */
#pragma once

#include <cstdint>

namespace tidesdb
{
namespace autoinc
{

/**
 * next_on_grid
 * the first grid value above after, by the same formula the server's compute_next_insert_id uses,
 * so a value reserved here is one the server's rounding leaves where it is
 * @param after the last value already handed out
 * @param offset auto_increment_offset
 * @param increment auto_increment_increment, treated as 1 when 0
 * @return the next grid value, or UINT64_MAX when none is left above after
 */
uint64_t next_on_grid(uint64_t after, uint64_t offset, uint64_t increment);

/**
 * run_length
 * how many grid values starting at first fit below the top of the range, capped at wanted
 * @param first the first value of the run
 * @param wanted the values the server asked for, treated as 1 when 0
 * @param increment the spacing between values, treated as 1 when 0
 * @return wanted, or 1 when a run that long would pass the top of the range
 */
uint64_t run_length(uint64_t first, uint64_t wanted, uint64_t increment);

}  // namespace autoinc
}  // namespace tidesdb
