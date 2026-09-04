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

/* the key codec: memcmp-comparable key bytes built from record fields (via
   Field::make_sort_key_part so numeric types sort correctly under memcmp), and the inverse sort-key
   decoders that let a covering index read reconstruct columns straight from the index bytes. the
   per-part decode math lives in the server-free tidesdb::sort_key core; this unit is the
   server-coupled glue plus the pk/secondary key builders and the index-condition-pushdown check. */

#include "ha_tidesdb.h"

#include <cstring>
#include <string>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/core/sort_key.h"
#include "src/handler/ha_tidesdb_keycodec.h"
#include "src/handler/ha_tidesdb_spatial.h"

/* ******************** PK / Index key helpers ******************** */

/*
  We build memcmp-comparable key bytes from record fields for a given KEY.
  Uses Field::make_sort_key_part() so that big-endian, sign-bit-flipped encoding
  is produced for numeric types -- which sorts correctly under memcmp.

  The record may point to record[0] or record[1]; we adjust field pointers
  via move_field_offset to read from the correct buffer.
*/
uint ha_tidesdb::make_comparable_key(KEY *key_info, const uchar *record, uint num_parts, uchar *out,
                                     bool for_fk_ref)
{
    uint pos = 0;
    my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(record - table->record[0]);

    for (uint p = 0; p < num_parts && p < key_info->user_defined_key_parts; p++)
    {
        KEY_PART_INFO *kp = &key_info->key_part[p];
        Field *field = kp->field;

        /* A descending key part stores its whole span (null indicator plus value) byte-inverted so
           memcmp orders it in reverse; record where the part starts so it can be inverted once it
           is fully written. */
        const uint part_start = pos;
        const bool desc = (kp->key_part_flag & HA_REVERSE_SORT) != 0;

        /* We handle the null indicator ourselves using real_maybe_null()
           (which checks field-level nullability only) instead of relying on
           make_sort_key_part() which uses maybe_null() (includes
           table->maybe_null).  For inner tables of outer joins,
           table->maybe_null is true, causing make_sort_key_part to write
           a spurious null indicator byte even for NOT NULL PK fields.
           Handling the null indicator ourselves and calling sort_string()
           directly avoids this mismatch. */
        field->move_field_offset(ptrdiff);
        /* A foreign-key reference key omits the null indicator so it matches the
           non-nullable parent primary key it probes.  The caller guarantees the
           referencing columns are non-null before asking for such a key. */
        if (field->real_maybe_null() && !for_fk_ref)
        {
            if (field->is_null())
            {
                out[pos++] = SORT_KEY_NULL;
                bzero(out + pos, kp->length);
                pos += kp->length;
                field->move_field_offset(-ptrdiff);
                if (desc) invert_key_part(out + part_start, pos - part_start);
                continue;
            }
            out[pos++] = SORT_KEY_NOT_NULL;
        }
        /* For VARBINARY (binary charset variable-length fields), sort_string()
           stores the value length in the last length_bytes of the output,
           truncating trailing data bytes when the value fills the field.
           This causes false duplicate detection on UNIQUE indexes because
           different values produce identical sort keys.
           Thus for binary charset varstrings, write all data bytes zero-padded
           followed by the length, so the full value is preserved. */
        if (field->type() == MYSQL_TYPE_VARCHAR && field->charset() == &my_charset_bin)
        {
            Field_varstring *fvs = static_cast<Field_varstring *>(field);
            String buf;
            fvs->val_str(&buf, &buf);
            uint data_len = (uint)buf.length();
            uint len_bytes = fvs->length_bytes;
            uint data_space = kp->length - len_bytes;

            uint copy_len = MY_MIN(data_len, data_space);
            memcpy(out + pos, buf.ptr(), copy_len);
            if (copy_len < data_space) bzero(out + pos + copy_len, data_space - copy_len);
            pos += data_space;

            /* For values that overflow data_space (value is exactly field_length
               bytes), write the overflow bytes into the length area first */
            if (data_len > data_space)
            {
                uint overflow = MY_MIN(data_len - data_space, len_bytes);
                memcpy(out + pos, buf.ptr() + data_space, overflow);
                pos += len_bytes;
            }
            else
            {
                /* Length suffix in high-byte order (preserves sort order) */
                if (len_bytes == 1)
                    out[pos] = (uchar)data_len;
                else
                    mi_int2store(out + pos, data_len);
                pos += len_bytes;
            }

            field->move_field_offset(-ptrdiff);
            if (desc) invert_key_part(out + part_start, pos - part_start);
            continue;
        }

        field->sort_string(out + pos, kp->length);
        field->move_field_offset(-ptrdiff);
        pos += kp->length;
        if (desc) invert_key_part(out + part_start, pos - part_start);
    }

    return pos;
}

/*
  Convert a key_copy-format search key (as passed to index_read_map)
  into the comparable format that we store in TidesDB.
  Uses key_restore to unpack into record[1], then make_comparable_key.
*/
uint ha_tidesdb::key_copy_to_comparable(KEY *key_info, const uchar *key_buf, uint key_len,
                                        uchar *out)
{
    key_restore(table->record[1], key_buf, key_info, key_len);

    uint parts = 0;
    uint len = 0;
    for (parts = 0; parts < key_info->user_defined_key_parts; parts++)
    {
        uint part_len = key_info->key_part[parts].store_length;
        if (len + part_len > key_len) break;
        len += part_len;
    }
    if (parts == 0) parts = 1;

    return make_comparable_key(key_info, table->record[1], parts, out);
}

/*
  Build PK bytes from a record.
  -- With user PK     use make_comparable_key for memcmp-correct ordering.
  -- Without PK       not applicable for NEW rows (caller generates hidden id);
                      for EXISTING rows current_pk already holds the key.
*/
uint ha_tidesdb::pk_from_record(const uchar *record, uchar *out)
{
    if (share->has_user_pk)
    {
        return make_comparable_key(&table->key_info[share->pk_index], record,
                                   table->key_info[share->pk_index].user_defined_key_parts, out);
    }
    else
    {
        /* Hidden PK -- we copy current_pk (must have been set by a prior read) */
        memcpy(out, current_pk_buf_, current_pk_len_);
        return current_pk_len_;
    }
}

/*
  Compute the comparable key byte length for a KEY.
  Matches what make_comparable_key() actually produces:
    sum of (nullable ? 1 : 0) + kp->length for each key part.

  NOTE -- ki->key_length includes store_length overhead (e.g. 2 bytes
  per VARCHAR part for length prefix in key_copy format) which is
  not present in the comparable key output.
*/
uint comparable_key_length(const KEY *ki)
{
    /* Spatial indexes use a fixed 8-byte Hilbert value as the comparable key. */
    if (is_spatial_index(ki)) return SPATIAL_HILBERT_KEY_LEN;

    uint len = 0;
    for (uint p = 0; p < ki->user_defined_key_parts; p++)
    {
        if (ki->key_part[p].field->real_maybe_null()) len++;
        len += ki->key_part[p].length;
    }
    return len;
}

/*
  Build a secondary index CF entry key:
    [comparable index-column bytes] + [comparable PK bytes]
*/
uint ha_tidesdb::sec_idx_key(uint idx, const uchar *record, uchar *out)
{
    KEY *key_info = &table->key_info[idx];
    uint pos = make_comparable_key(key_info, record, key_info->user_defined_key_parts, out);
    pos += pk_from_record(record, out + pos);
    return pos;
}

#ifdef WITH_WSREP
/* whether secondary index i pins this row by a unique value, so its comparable value certifies as a
   cross-node conflict key in the galera write-intent map.  a non-unique, fulltext, or spatial index
   carries no such key, and a unique key with a null part does not constrain the row. */
bool ha_tidesdb::sec_idx_value_keyed(uint i, const uchar *record)
{
    if (i >= table->s->keys) return false;
    if (share->idx_is_fts[i] || share->idx_is_spatial[i]) return false;
    KEY *ki = &table->key_info[i];
    if (!(ki->flags & HA_NOSAME)) return false;
    for (uint j = 0; j < ki->user_defined_key_parts; j++)
        if (ki->key_part[j].field->is_null_in_record(record)) return false;
    return true;
}
#endif

/*
  Try to fill record buf with column values decoded from the secondary
  index key, avoiding the expensive PK point-lookup.  Used when
  keyread_only_ is true (covering index scan).

  The secondary index key layout is:
    [comparable_idx_cols | comparable_pk]

  Uses decode_sort_key_part() which supports integers, DATE, DATETIME,
  TIMESTAMP, YEAR, and fixed-length CHAR/BINARY (binary/latin1).
  Returns true on success.
*/
bool ha_tidesdb::try_keyread_from_index(const uint8_t *ik, size_t iks, uint idx, uchar *buf)
{
    if (!share->has_user_pk) return false;

    KEY *pk_key = &table->key_info[share->pk_index];
    KEY *idx_key = &table->key_info[idx];
    uint idx_col_len = share->idx_comp_key_len[idx];

    /* We check every column in read_set against the precomputed coverage
       bitmap for this index.  O(read_set set-bits) instead of the prior
       O(set-bits * (pk_parts + idx_parts)) nested scan. */
    if (idx < share->idx_cover.size())
    {
        const std::vector<bool> &cover = share->idx_cover[idx];
        for (uint c = bitmap_get_first_set(table->read_set); c != MY_BIT_NONE;
             c = bitmap_get_next_set(table->read_set, c))
        {
            if (c >= cover.size() || !cover[c]) return false;
        }
    }
    else
    {
        /* Share not populated for this index (shouldn't happen). */
        return false;
    }

    /* decode_sort_key_part writes each value into buf at the field's offset, but Field::set_null /
       set_notnull default to the field's own record[0] null bit.  When the caller reads into a
       buffer other than record[0] (the partition wrapper does this for ordered index scans), the
       null bit must be flipped in that same buffer, so pass the buf-relative offset. */
    const my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(buf - table->record[0]);

    const uint8_t *end = ik + iks;
    const uint8_t *pos = ik;
    if (!keyread_decode_key_parts(idx_key, pos, end, buf, ptrdiff)) return false;

    const uint8_t *pk_start = ik + idx_col_len;
    pos = pk_start;
    if (!keyread_decode_key_parts(pk_key, pos, end, buf, ptrdiff)) return false;

    uint pk_bytes = (uint)(iks - idx_col_len);
    memcpy(current_pk_buf_, pk_start, pk_bytes);
    current_pk_len_ = pk_bytes;

    return true;
}

bool ha_tidesdb::keyread_decode_key_parts(KEY *key, const uint8_t *&pos, const uint8_t *end,
                                          uchar *buf, my_ptrdiff_t ptrdiff)
{
    for (uint p = 0; p < key->user_defined_key_parts; p++)
    {
        KEY_PART_INFO *kp = &key->key_part[p];
        Field *f = kp->field;
        const bool desc = (kp->key_part_flag & HA_REVERSE_SORT) != 0;
        if (f->real_maybe_null())
        {
            if (pos >= end) return false;
            /* A descending part's null indicator is stored inverted. */
            uchar nb = desc ? (uchar) ~(*pos) : *pos;
            if (nb == 0)
            {
                f->set_null(ptrdiff);
                pos++;
                /* make_comparable_key writes the null indicator followed by kp->length zeroed
                   value bytes, so skip that full width; advancing by the indicator alone decodes
                   every later key part one part-width early and silently loses its value. */
                if (pos + kp->length > end) return false;
                pos += kp->length;
                continue;
            }
            f->set_notnull(ptrdiff);
            pos++;
        }
        if (pos + kp->length > end) return false;
        if (bitmap_is_set(table->read_set, kp->fieldnr - 1))
        {
            const uint8_t *dp = pos;
            uchar tmp[MAX_KEY_LENGTH];
            if (desc)
            {
                memcpy(tmp, pos, kp->length);
                invert_key_part(tmp, kp->length);
                dp = tmp;
            }
            if (!decode_sort_key_part(dp, kp->length, f, buf)) return false;
        }
        pos += kp->length;
    }
    return true;
}

/* ******************** ICP (Index Condition Pushdown) helpers ******************** */

/*
  Extended sort-key decoder -- handles integers, DATE (3 bytes big-endian),
  DATETIME/TIMESTAMP (4-8 bytes big-endian), YEAR (1 byte), and fixed-length
  CHAR/BINARY (direct memcpy of sort key).

  The field is classified into a server-free field_sort_desc and the actual
  byte reversal, sign flip, and padding happen in tidesdb::sort_key::decode_part.
  For integer types that means a byte-reverse with the most-significant byte
  XORed by the sign-flip mask for signed columns so the native value is recovered.

  For DATE/DATETIME/TIMESTAMP/YEAR, the sort key is big-endian unsigned;
  we reverse the byte order to native little-endian without sign-flip
  (these types are always unsigned internally).

  For CHAR/BINARY (MYSQL_TYPE_STRING), the sort key produced by
  Field_string::sort_string is the charset's sort weight sequence.
  For binary/latin1 charsets this is identical to the field content
  (padded with spaces to kp->length).  We copy it directly.
  For multi-byte charsets (utf8) the sort weights differ from the
  stored bytes, so we cannot reverse -- return false.

  Returns true on success, false for unsupported types.
*/

/* Whether decode_sort_key_part can reconstruct this field's value from its
   comparable sort key.  Depends only on the field type and charset, not on any
   stored bytes, so index_flags can use it to decide index-only capability
   without touching data.  This is the single source of truth for the set of
   decodable types -- decode_sort_key_part checks it first, so the two never
   drift.  The mem-comparable sort key is only invertible for these types; for
   VARCHAR, DECIMAL, floating point, multi-byte CHAR, BLOB, ENUM/SET and the
   like the sort weights are lossy, so a keyread of such a column must fall back
   to the primary-key row fetch. */
static bool is_keyread_decodable_type(const Field *f)
{
    switch (f->real_type())
    {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
        case MYSQL_TYPE_YEAR:
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            return true;
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
            /* Only the whole-second forms reverse cleanly from the sort key.  A fractional-second
               column packs extra sub-second bytes the decoder does not reconstruct, so leave it
               undecodable and let the covering read fall back to a primary-key row fetch. */
            return f->decimals() == 0;
        case MYSQL_TYPE_STRING:
            /* Fixed CHAR/BINARY only for charsets whose sort weights equal the
               stored bytes. */
            return f->charset() == &my_charset_bin || f->charset() == &my_charset_latin1;
        default:
            return false;
    }
}

/* Whether the field at key part `part` of index `keyno` reconstructs from its
   sort key.  Resolved through key_part.fieldnr and table_share->field rather
   than key_part.field, because index_flags builds field->part_of_key on a
   fresh, unopened handler at frm-parse time where key_part.field and the plugin
   share are not yet available.  An unresolvable field is treated conservatively
   as undecodable. */
bool index_part_is_decodable(const TABLE_SHARE *ts, uint keyno, uint part)
{
    if (!ts || !ts->field || keyno >= ts->keys) return false;
    const KEY *k = &ts->key_info[keyno];
    if (part >= k->user_defined_key_parts) return false;
    uint fnr = k->key_part[part].fieldnr;
    if (fnr == 0 || fnr - 1 >= ts->fields) return false;
    return is_keyread_decodable_type(ts->field[fnr - 1]);
}

bool ha_tidesdb::decode_sort_key_part(const uint8_t *src, uint sort_len, Field *f, uchar *buf)
{
    /* Classify the field into the server-free descriptor, staying in lockstep with
       is_keyread_decodable_type (which index_flags relies on), then let the core do the
       byte reversal, sign flip, and padding. */
    tidesdb::sort_key::field_sort_desc d;
    switch (f->real_type())
    {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
            d.k = tidesdb::sort_key::kind::integer;
            d.is_signed = !f->is_unsigned();
            break;

        case MYSQL_TYPE_YEAR:
            d.k = tidesdb::sort_key::kind::year;
            break;

        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            d.k = tidesdb::sort_key::kind::date;
            break;

        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
            d.k = tidesdb::sort_key::kind::datetime;
            break;

        case MYSQL_TYPE_STRING:
            /* Fixed CHAR/BINARY only for charsets whose sort weights equal the stored bytes.
               A binary column pads with a nul, a latin1 char with a space. */
            if (f->charset() == &my_charset_bin)
            {
                d.k = tidesdb::sort_key::kind::char_fixed;
                d.pack_length = f->pack_length();
                d.pad_byte = 0x00;
            }
            else if (f->charset() == &my_charset_latin1)
            {
                d.k = tidesdb::sort_key::kind::char_fixed;
                d.pack_length = f->pack_length();
                d.pad_byte = ' ';
            }
            else
                return false;
            break;

        default:
            return false;
    }

    uchar *to = buf + (uintptr_t)(f->ptr - f->table->record[0]);
    return tidesdb::sort_key::decode_part(src, sort_len, d, to);
}

/*
  Evaluate pushed index condition on a secondary-index entry before
  the expensive PK point-lookup (InnoDB pattern).

  Decodes the index key column values and PK column values from the
  comparable-format index key into the record buffer, then calls
  handler_index_cond_check() which evaluates the pushed condition,
  checks end_range, and handles THD kill signals.

  Supports integer types, DATE, DATETIME, TIMESTAMP, YEAR, and
  fixed-length CHAR/BINARY (binary/latin1 charset) via
  decode_sort_key_part().  For unsupported types, ICP is skipped and
  CHECK_POS is returned so the caller falls through to the PK lookup.
*/
bool ha_tidesdb::icp_decode_key_parts(KEY *key, const uint8_t *&pos, const uint8_t *end, uchar *buf,
                                      my_ptrdiff_t ptrdiff)
{
    for (uint p = 0; p < key->user_defined_key_parts; p++)
    {
        KEY_PART_INFO *kp = &key->key_part[p];
        Field *f = kp->field;
        const bool desc = (kp->key_part_flag & HA_REVERSE_SORT) != 0;

        if (f->real_maybe_null())
        {
            if (pos >= end) return false;
            uchar nb = desc ? (uchar) ~(*pos) : *pos;
            if (nb == 0)
            {
                /* decode_sort_key_part writes the value into buf at the field's
                   offset, so the null bit must land in that same buffer too when
                   buf is not record[0], matching keyread_decode_key_parts. */
                f->set_null(ptrdiff);
                pos++;
                /* skip the kp->length zeroed value bytes the null part occupies so the pushed
                   condition reads the following part's bytes, not this part's padding. */
                if (pos + kp->length > end) return false;
                pos += kp->length;
                continue;
            }
            f->set_notnull(ptrdiff);
            pos++;
        }
        if (pos + kp->length > end) return false;
        const uint8_t *dp = pos;
        uchar tmp[MAX_KEY_LENGTH];
        if (desc)
        {
            memcpy(tmp, pos, kp->length);
            invert_key_part(tmp, kp->length);
            dp = tmp;
        }
        if (!decode_sort_key_part(dp, kp->length, f, buf)) return false;
        pos += kp->length;
    }
    return true;
}

check_result_t ha_tidesdb::icp_check_secondary(const uint8_t *ik, size_t iks, uint idx, uchar *buf)
{
    if (!pushed_idx_cond || pushed_idx_cond_keyno != idx) return CHECK_POS;

    KEY *idx_key = &table->key_info[idx];
    uint idx_col_len = share->idx_comp_key_len[idx];
    const uint8_t *end = ik + iks;

    /* Null bits, like the decoded values, must land in buf rather than record[0]
       when the caller reads into some other buffer. */
    const my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(buf - table->record[0]);

    /* Decode index column parts from the comparable-format key.
       If any part can't be decoded (DECIMAL, VARCHAR, etc.), we fall
       back to a full PK row fetch so the condition evaluates correctly. */
    const uint8_t *pos = ik;
    bool decode_ok = icp_decode_key_parts(idx_key, pos, end, buf, ptrdiff);

    /* Decode PK parts from the tail (pushed condition may reference PK columns). */
    if (decode_ok && share->has_user_pk)
    {
        pos = ik + idx_col_len;
        decode_ok = icp_decode_key_parts(&table->key_info[share->pk_index], pos, end, buf, ptrdiff);
    }

    if (!decode_ok)
    {
        /* Could not decode all key parts from the sort key (unsupported type
           like DECIMAL, VARCHAR, multi-byte CHAR).  Fall back to a full PK
           row fetch so ALL columns are available for condition evaluation.
           This is more expensive than pure ICP (still does the PK lookup)
           but is correct, the server won't re-evaluate pushed conditions. */
        if (iks > idx_col_len)
        {
            const uchar *pk = ik + idx_col_len;
            uint pk_len = (uint)(iks - idx_col_len);
            if (fetch_row_by_pk(scan_txn, pk, pk_len, buf) != 0)
                return CHECK_POS; /* PK lookup failed -- we accept row, let caller handle */
        }
        else
        {
            return CHECK_POS; /* malformed key -- we accept */
        }
    }

    /* Delegate to MariaDB's ICP evaluator which checks kill state,
       end_range, and pushed_idx_cond->val_bool(). */
    return handler_index_cond_check(this);
}
