/*
   Copyright (c) 2026 TidesDB Corp.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/

#ifndef HA_TIDESDB_KEYCODEC_H
#define HA_TIDESDB_KEYCODEC_H

/* the two free helpers of the key-codec module (ha_tidesdb_keycodec.cc) that the open, index-flag
   and inplace-alter paths in ha_tidesdb.cc need.  the comparable-key builders and sort-key decoders
   are ha_tidesdb methods declared in ha_tidesdb.h; only these two stand alone.  include after
   ha_tidesdb.h so KEY and TABLE_SHARE are visible. */

/**
 * comparable_key_length
 * the worst-case length in bytes of the memcmp-comparable key this engine builds for a KEY,
 * summing each part's sort-key width plus its null-indicator byte
 * @param ki the key to size
 * @return the comparable key length in bytes
 */
uint comparable_key_length(const KEY *ki);

/* invert every byte of a comparable-key part so memcmp orders it in reverse.  A descending
   (HA_REVERSE_SORT) key part stores its null-indicator and value bytes inverted, so a forward index
   scan yields descending order and NULLs sort last; the decode path inverts the bytes back.  This
   is an involution, so the same call encodes on write and un-encodes on read. */
static inline void invert_key_part(uchar *p, uint n)
{
    for (uint i = 0; i < n; i++) p[i] = (uchar)~p[i];
}

/**
 * index_part_is_decodable
 * whether a covering read can reconstruct a key part's column straight from the index bytes,
 * so the handler can advertise HA_KEYREAD_ONLY for that part
 * @param ts the table share the key belongs to
 * @param keyno the key number
 * @param part the key part index
 * @return true when the part's type round-trips from its sort-key encoding
 */
bool index_part_is_decodable(const TABLE_SHARE *ts, uint keyno, uint part);

#endif /* HA_TIDESDB_KEYCODEC_H */
