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

#include <cstring>

#include "sort_key.h"

namespace tidesdb
{
namespace sort_key
{

bool decode_int(const uint8_t *src, unsigned sort_len, bool is_signed, uint8_t *to)
{
    /* server integer widths are tiny 1, short 2, int24 3, long 4, and longlong 8. */
    if (sort_len == 0 || (sort_len > 4 && sort_len != 8)) return false;

    for (unsigned i = 0; i < sort_len; i++) to[i] = src[sort_len - 1 - i];
    if (is_signed) to[sort_len - 1] ^= INT_SIGN_FLIP_MASK;
    return true;
}

/* reverse a big-endian unsigned sort key of a fixed width to native little-endian. */
static bool decode_reverse(const uint8_t *src, unsigned sort_len, uint8_t *to)
{
    for (unsigned i = 0; i < sort_len; i++) to[i] = src[sort_len - 1 - i];
    return true;
}

bool decode_part(const uint8_t *src, unsigned sort_len, const field_sort_desc &d, uint8_t *to)
{
    switch (d.k)
    {
        case kind::integer:
            return decode_int(src, sort_len, d.is_signed, to);

        case kind::year:
            /* a one-byte unsigned value whose sort key is the value itself. */
            if (sort_len < 1) return false;
            to[0] = src[0];
            return true;

        case kind::date:
            if (sort_len != DATE_PACK_LEN) return false;
            return decode_reverse(src, sort_len, to);

        case kind::datetime:
            if (sort_len == 0 || sort_len > DATETIME_MAX_PACK_LEN) return false;
            return decode_reverse(src, sort_len, to);

        case kind::char_fixed:
        {
            /* the sort key of a fixed char or binary value is its stored bytes; a part
             * shorter than the field width is a prefix key, so pad the tail back to the
             * native width with the field's own pad byte, a space for a latin1 char and a
             * nul for a binary column. padding with a space for both, as the pre-split code
             * did, corrupted a reconstructed binary value. */
            unsigned copy = sort_len < d.pack_length ? sort_len : d.pack_length;
            std::memcpy(to, src, copy);
            if (copy < d.pack_length) std::memset(to + copy, d.pad_byte, d.pack_length - copy);
            return true;
        }

        case kind::undecodable:
            return false;
    }
    return false;
}

}  // namespace sort_key
}  // namespace tidesdb
