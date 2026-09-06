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

/* server-free core for decoding a memory-comparable sort key back to a native field value.
 * the encoding side stays in the handler because it delegates to the server's
 * Field::sort_string, which differs between mysql and mariadb; this module is the engine's
 * own inverse of that encoding for the types whose sort key is losslessly invertible. the
 * handler classifies a field into the descriptor below and computes the destination pointer
 * inside the record buffer, then this module writes the reconstructed bytes there. */
#pragma once

#include <cstddef>
#include <cstdint>

namespace tidesdb
{
namespace sort_key
{

/* the most significant byte of a signed integer sort key has this bit flipped so negatives
 * sort below positives; flipping it again on decode recovers the native value. */
static constexpr uint8_t INT_SIGN_FLIP_MASK = 0x80;

/* server packed widths the date and datetime decoders accept. */
static constexpr unsigned DATE_PACK_LEN = 3;
static constexpr unsigned DATETIME_MAX_PACK_LEN = 8;

/**
 * kind
 * the shape of a field's sort key, which decides how decode_part reverses it; undecodable
 * marks a type whose sort key is lossy, such as varchar, decimal, floating point, or a
 * multibyte char, for which a keyread must fall back to the primary-key row fetch
 */
enum class kind
{
    undecodable,
    integer,
    year,
    date,
    datetime,
    char_fixed
};

/**
 * field_sort_desc
 * the field properties decode_part needs, built by the handler from a server Field so this
 * module carries no server type
 * @param k the sort-key shape
 * @param is_signed whether an integer field is signed, driving the sign-bit flip
 * @param pack_length the native width a fixed char or binary value reconstructs to
 * @param pad_byte the byte a fixed char or binary value pads with, a space for a latin1
 *                 char and a nul for a binary column
 */
struct field_sort_desc
{
    kind k = kind::undecodable;
    bool is_signed = false;
    unsigned pack_length = 0;
    uint8_t pad_byte = ' ';
};

/**
 * decode_int
 * reverse a big-endian sign-flipped integer sort key back to a native little-endian value
 * @param src the sort-key bytes
 * @param sort_len the width, which must be a server integer width of 1, 2, 3, 4, or 8
 * @param is_signed whether to undo the sign-bit flip on the most significant byte
 * @param to a buffer of at least sort_len bytes receiving the native value
 * @return true on a valid width, false otherwise with to left untouched
 */
bool decode_int(const uint8_t *src, unsigned sort_len, bool is_signed, uint8_t *to);

/**
 * decode_part
 * reconstruct one field value from its sort key according to the descriptor
 * @param src the sort-key bytes for this part
 * @param sort_len the sort-key length for this part
 * @param d the field descriptor
 * @param to the destination inside the record buffer, sized for the field's native value
 * @return true when the value was reconstructed, false for an undecodable type or a length
 *         that does not fit the type
 */
bool decode_part(const uint8_t *src, unsigned sort_len, const field_sort_desc &d, uint8_t *to);

}  // namespace sort_key
}  // namespace tidesdb
