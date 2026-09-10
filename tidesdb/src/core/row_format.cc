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

#include "row_format.h"

namespace tidesdb
{
namespace row_format
{

static constexpr unsigned BITS_PER_BYTE = 8;

static inline void store_u16le(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
}

static inline uint16_t load_u16le(const uint8_t *p)
{
    return (uint16_t)(p[0] | (p[1] << 8));
}

void encode_row_header(uint16_t null_bytes, uint16_t field_count, uint8_t *out)
{
    out[0] = ROW_HEADER_MAGIC;
    store_u16le(out + 1, null_bytes);
    store_u16le(out + 1 + sizeof(uint16_t), field_count);
}

bool decode_row_header(const uint8_t *data, size_t len, row_header *out)
{
    if (len < ROW_HEADER_SIZE || data[0] != ROW_HEADER_MAGIC) return false;
    out->null_bytes = load_u16le(data + 1);
    out->field_count = load_u16le(data + 1 + sizeof(uint16_t));
    return true;
}

void encode_be64(uint64_t id, uint8_t *out)
{
    for (unsigned i = 0; i < HIDDEN_PK_SIZE; i++)
        out[i] = (uint8_t)(id >> ((HIDDEN_PK_SIZE - 1 - i) * BITS_PER_BYTE));
}

uint64_t decode_be64(const uint8_t *in)
{
    uint64_t id = 0;
    for (unsigned i = 0; i < HIDDEN_PK_SIZE; i++) id = (id << BITS_PER_BYTE) | (uint64_t)in[i];
    return id;
}

uint32_t build_data_key(const uint8_t *pk, uint32_t pk_len, uint8_t *out)
{
    out[0] = KEY_NS_DATA;
    std::memcpy(out + KEY_NAMESPACE_LEN, pk, pk_len);
    return pk_len + KEY_NAMESPACE_LEN;
}

bool is_data_key(const uint8_t *key, size_t key_size)
{
    return key_size > 0 && key[0] == KEY_NS_DATA;
}

}  // namespace row_format
}  // namespace tidesdb
