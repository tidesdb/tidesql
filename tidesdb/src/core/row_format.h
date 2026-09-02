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

/* server-free core for the engine's on-disk framing, the parts of the key and row layout
 * that are pure byte manipulation with no server type involved. the field packing inside a
 * row still goes through the server's Field::pack, and the null-bitmap and default-value
 * reconciliation still reads server record buffers, so those stay in the handler; what lives
 * here is the row header that carries the null-byte count and field count for instant add
 * and drop column, the big-endian hidden primary key, and the key-namespace prefix. */
#pragma once

#include <cstddef>
#include <cstdint>

namespace tidesdb
{
namespace row_format
{

/* first byte of every key, separating the meta keyspace from the data keyspace. */
static constexpr uint8_t KEY_NS_META = 0x00;
static constexpr uint8_t KEY_NS_DATA = 0x01;
static constexpr unsigned KEY_NAMESPACE_LEN = 1;

/* a hidden primary key is an 8-byte big-endian row id, so byte order matches numeric order
 * under memcmp. */
static constexpr unsigned HIDDEN_PK_SIZE = 8;

/* every serialized row begins with a magic byte, a 2-byte little-endian null-byte count, and
 * a 2-byte little-endian field count, recorded at write time so a later add or drop column
 * can reconcile against the row's original shape. */
static constexpr uint8_t ROW_HEADER_MAGIC = 0xFE;
static constexpr unsigned ROW_HEADER_SIZE = 5;

/**
 * row_header
 * the shape a row was written with, read back from its header
 * @param null_bytes the width of the null bitmap that follows the header
 * @param field_count the number of packed fields that follow the null bitmap
 */
struct row_header
{
    uint16_t null_bytes;
    uint16_t field_count;
};

/**
 * encode_row_header
 * write the fixed row header
 * @param null_bytes the null bitmap width to record
 * @param field_count the field count to record
 * @param out a buffer of at least ROW_HEADER_SIZE bytes
 */
void encode_row_header(uint16_t null_bytes, uint16_t field_count, uint8_t *out);

/**
 * decode_row_header
 * validate and read a row header
 * @param data the row bytes
 * @param len the row length
 * @param out receives the recorded shape on success
 * @return true when data held at least ROW_HEADER_SIZE bytes and began with the magic byte
 */
bool decode_row_header(const uint8_t *data, size_t len, row_header *out);

/**
 * encode_be64
 * write a 64-bit id as 8 big-endian bytes so memcmp on the key matches numeric order
 * @param id the row id
 * @param out a buffer of at least HIDDEN_PK_SIZE bytes
 */
void encode_be64(uint64_t id, uint8_t *out);

/**
 * decode_be64
 * read back an 8-byte big-endian id
 * @param in a buffer of at least HIDDEN_PK_SIZE bytes
 * @return the decoded id
 */
uint64_t decode_be64(const uint8_t *in);

/**
 * build_data_key
 * prefix a comparable primary key with the data namespace byte to form a data-keyspace key
 * @param pk the comparable primary key bytes
 * @param pk_len the primary key length
 * @param out a buffer of at least KEY_NAMESPACE_LEN + pk_len bytes
 * @return the total key length written
 */
uint32_t build_data_key(const uint8_t *pk, uint32_t pk_len, uint8_t *out);

/**
 * is_data_key
 * whether a key lives in the data namespace rather than the meta namespace
 * @param key the key bytes
 * @param key_size the key length
 * @return true when the key is non-empty and begins with the data namespace byte
 */
bool is_data_key(const uint8_t *key, size_t key_size);

}  // namespace row_format
}  // namespace tidesdb
