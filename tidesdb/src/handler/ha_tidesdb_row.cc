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

/* the row wire format: the self-describing on-disk record (a magic byte, the stored null-bitmap
   width and field count, then the packed fields, enabling instant add/drop column) and the
   data-at-rest encryption envelope wrapped around it. serialize_row builds it from a server record,
   deserialize_row rebuilds a record from any prior schema's bytes. */

#include "ha_tidesdb.h"

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"

#include <cstring>
#include <string>

/* ******************** Data-at-rest encryption helpers ******************** */

/*
  Encrypt plaintext into out.  The on-disk blob is the 4-byte little-endian
  key version, then the 16-byte IV, then the ciphertext.  Storing the key
  version lets tidesdb_decrypt_row recover the exact key a row was written
  under, so encrypted rows remain readable across a key rotation.
*/
static bool tidesdb_encrypt_row_into(const std::string &plain, uint key_id, uint key_version,
                                     std::string &out)
{
    unsigned char key[TIDESDB_ENC_KEY_LEN];
    unsigned int klen = sizeof(key);
    /* Fail closed if the keyring cannot satisfy the request (missing version,
       buffer too small, plugin not loaded).  Without this check the local key
       buffer holds uninitialized stack bytes and encryption_crypt would
       proceed as if the request had succeeded, producing rows nobody can
       decrypt. */
    if (encryption_key_get(key_id, key_version, key, &klen) != 0)
    {
        sql_print_error("[TIDESDB] encryption_key_get failed for key_id=%u version=%u", key_id,
                        key_version);
        out.clear();
        return false;
    }

    unsigned char iv[TIDESDB_ENC_IV_LEN];
    my_random_bytes(iv, TIDESDB_ENC_IV_LEN);

    unsigned int slen = (unsigned int)plain.size();
    unsigned int enc_len = encryption_encrypted_length(slen, key_id, key_version);
    out.resize(TIDESDB_ENC_VERSION_LEN + TIDESDB_ENC_IV_LEN + enc_len);

    int4store(&out[0], (uint32)key_version);
    memcpy(&out[TIDESDB_ENC_VERSION_LEN], iv, TIDESDB_ENC_IV_LEN);

    unsigned int dlen = enc_len;
    int rc = encryption_crypt((const unsigned char *)plain.data(), slen,
                              (unsigned char *)&out[TIDESDB_ENC_VERSION_LEN + TIDESDB_ENC_IV_LEN],
                              &dlen, key, klen, iv, TIDESDB_ENC_IV_LEN, ENCRYPTION_FLAG_ENCRYPT,
                              key_id, key_version);
    if (rc != 0)
    {
        sql_print_error("[TIDESDB] encryption_crypt(encrypt) failed rc=%d", rc);
        out.clear();
        return false;
    }
    out.resize(TIDESDB_ENC_VERSION_LEN + TIDESDB_ENC_IV_LEN + dlen);
    return true;
}

/*
  Decrypt a row stored as [key version (4)] [IV (16)] [ciphertext].  The key
  version is read back from the blob so a row encrypted before a key rotation
  is decrypted with the key it was actually written under, not the latest.
*/
static std::string tidesdb_decrypt_row(const char *data, size_t len, uint key_id)
{
    if (len <= TIDESDB_ENC_VERSION_LEN + TIDESDB_ENC_IV_LEN)
    {
        sql_print_error("[TIDESDB] encrypted row too short (%zu bytes)", len);
        return std::string(); /* signal failure */
    }

    uint key_version = (uint)uint4korr(data);

    unsigned char key[TIDESDB_ENC_KEY_LEN];
    unsigned int klen = sizeof(key);
    /* Fail closed if the keyring cannot return the version this row was
       written under (rotated-out key, plugin not loaded, version never
       existed).  Falling through with an uninitialized key buffer would
       feed garbage into encryption_crypt and silently corrupt the
       deserialize path. */
    if (encryption_key_get(key_id, key_version, key, &klen) != 0)
    {
        sql_print_error("[TIDESDB] encryption_key_get failed for key_id=%u version=%u", key_id,
                        key_version);
        return std::string(); /* signal failure to caller */
    }

    const unsigned char *iv = (const unsigned char *)data + TIDESDB_ENC_VERSION_LEN;
    const unsigned char *src =
        (const unsigned char *)data + TIDESDB_ENC_VERSION_LEN + TIDESDB_ENC_IV_LEN;
    unsigned int slen = (unsigned int)(len - TIDESDB_ENC_VERSION_LEN - TIDESDB_ENC_IV_LEN);

    std::string out;
    unsigned int dlen = slen + TIDESDB_ENC_KEY_LEN; /* padding slack */
    out.resize(dlen);

    int rc = encryption_crypt(src, slen, (unsigned char *)&out[0], &dlen, key, klen, iv,
                              TIDESDB_ENC_IV_LEN, ENCRYPTION_FLAG_DECRYPT, key_id, key_version);
    if (rc != 0)
    {
        sql_print_error("[TIDESDB] encryption_crypt(decrypt) failed rc=%d", rc);
        return std::string(); /* signal failure */
    }
    out.resize(dlen);
    return out;
}

/* ******************** serialize / deserialize (BLOB deep-copy) ******************** */

/* Row format header constants live in ha_tidesdb.h so the stop-word
   loader and other callers can reference them without forward decls.
   Layout is [ROW_HEADER_MAGIC] [null_bytes_stored (2 LE)] [field_count (2 LE)]
   for ROW_HEADER_SIZE bytes total.  Enables instant ADD/DROP COLUMN. */

size_t ha_tidesdb::serialize_estimate_size(const uchar *buf, my_ptrdiff_t ptrdiff)
{
    /* Upper-bound packed size.  For non-BLOB tables the estimate is constant
       (header + null_bytes + reclength + 2 bytes per field for length-prefix
       overhead from Field_string::pack).  Cache it to avoid recomputing on
       every row.  For BLOB tables we must add the actual blob data sizes. */
    size_t est = share->cached_row_est;
    if (unlikely(est == 0))
    {
        est = ROW_HEADER_SIZE + table->s->null_bytes + table->s->reclength +
              FIELD_VARCHAR_LEN_PREFIX * table->s->fields;
        if (!share->has_blobs)
            share->cached_row_est = est; /* safe to cache -- constant for non-BLOB tables */
    }
    if (share->has_blobs)
    {
        /* Walk only the precomputed BLOB field list instead of every field. */
        for (uint16 idx : share->blob_field_indices)
        {
            Field *f = table->field[idx];
            if (f->is_real_null(ptrdiff)) continue;
            Field_blob *blob = (Field_blob *)f;
            est += blob->get_length(buf + (uintptr_t)(f->ptr - table->record[0]));
        }
    }
    return est;
}

const std::string &ha_tidesdb::serialize_encrypt_row()
{
    /* We cache the encryption key version per-statement to avoid the
       expensive encryption_key_get_latest_version() syscall on every
       single row.  The cache is invalidated at statement start
       (enc_key_ver_valid_ = false in external_lock). */
    if (!enc_key_ver_valid_)
    {
        uint cur_ver = encryption_key_get_latest_version(share->encryption_key_id);
        if (cur_ver != ENCRYPTION_KEY_VERSION_INVALID)
        {
            share->encryption_key_version = cur_ver;
            cached_enc_key_ver_ = cur_ver;
        }
        else
        {
            cached_enc_key_ver_ = share->encryption_key_version;
        }
        enc_key_ver_valid_ = true;
    }
    /* We encrypt into enc_buf_ instead of replacing row_buf_, so that
       row_buf_'s heap capacity is preserved across calls.
       Writing directly into enc_buf_ reuses its heap capacity across rows,
       avoiding a per-row allocation when the encrypted size is stable. */
    if (!tidesdb_encrypt_row_into(row_buf_, share->encryption_key_id, cached_enc_key_ver_, enc_buf_))
    {
        enc_buf_.clear(); /* signal failure */
    }
    return enc_buf_;
}

const std::string &ha_tidesdb::serialize_row(const uchar *buf)
{
    my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(buf - table->record[0]);

    size_t est = serialize_estimate_size(buf, ptrdiff);

    row_buf_.resize(est);
    uchar *start = (uchar *)&row_buf_[0];
    uchar *pos = start;

    /* Row header -- enables instant ADD/DROP COLUMN by recording the
       null bitmap size and field count at write time. */
    *pos++ = ROW_HEADER_MAGIC;
    const uint nb = share->null_bytes_cached;
    const uint nf = share->fields_cached;
    int2store(pos, (uint16)nb);
    pos += sizeof(uint16);
    int2store(pos, (uint16)nf);
    pos += sizeof(uint16);

    /* Null bitmap */
    if (nb) memcpy(pos, buf, nb);
    pos += nb;

    /* We pack each non-null field.  We use a precomputed per-field plan
       (built once at open()) so the hot path skips the Field::pack vtable
       dispatch for fields whose pack format is a pure memcpy of
       pack_length() bytes -- integers, fixed-precision datetimes,
       NEWDECIMAL, FLOAT, DOUBLE.  CHAR / VARCHAR / BLOB still go through
       Field::pack because their format trims pad bytes or emits a length
       prefix.  The plan also caches `f->ptr - record[0]` so that
       subtraction does not run per row.

       When the table has no nullable fields (share->has_no_nullable),
       skip the per-field real_maybe_null branch entirely. */
    const TidesDB_share::field_plan_t *plan = share->field_plan.data();
    const bool all_not_null = share->has_no_nullable;
    for (uint i = 0; i < nf; i++)
    {
        const TidesDB_share::field_plan_t &fp = plan[i];
        if (!all_not_null && fp.maybe_null)
        {
            if (table->field[i]->is_real_null(ptrdiff)) continue;
        }
        const uchar *src = buf + fp.src_off;
        if (fp.memcpy_ok)
        {
            memcpy(pos, src, fp.pack_len);
            pos += fp.pack_len;
        }
        else
        {
            pos = table->field[i]->pack(pos, src);
        }
    }

    row_buf_.resize((size_t)(pos - start));

    if (share->encrypted) return serialize_encrypt_row();

    return row_buf_;
}

void ha_tidesdb::deserialize_row(uchar *buf, const uchar *data, size_t len)
{
    const uchar *from = data;
    const uchar *from_end = data + len;

    /* All rows have the header([0xFE] [null_bytes(2)] [field_count(2)]) */
    if (unlikely(len < ROW_HEADER_SIZE || data[0] != ROW_HEADER_MAGIC))
    {
        /* Corrupted or truncated row, we zero the record to avoid garbage */
        memset(buf, 0, table->s->reclength);
        return;
    }

    from++;
    uint stored_null_bytes = uint2korr(from);
    from += sizeof(uint16);
    uint stored_fields = uint2korr(from);
    from += sizeof(uint16);

    /* Null bitmap -- we copy the smaller of stored vs current.
       When columns were added (stored_null_bytes < table->s->null_bytes),
       fill the extra null bitmap bytes from the table's default record
       so that new columns inherit their correct DEFAULT / NOT NULL state
       rather than blindly marking them NULL. */
    if ((size_t)(from_end - from) < stored_null_bytes)
    {
        /* Truncated row -- zero the record like the bad-header path above, rather
           than leaving the reused buffer's stale bytes from a prior row. */
        memset(buf, 0, table->s->reclength);
        return;
    }
    const uint cur_nb = share->null_bytes_cached;
    uint copy_nb = MY_MIN(stored_null_bytes, cur_nb);
    if (copy_nb) memcpy(buf, from, copy_nb);
    if (copy_nb < cur_nb)
        memcpy(buf + copy_nb, table->s->default_values + copy_nb, cur_nb - copy_nb);
    from += stored_null_bytes;

    /* We unpack.  Only unpack up to MIN(stored_fields, current_fields).
       If the row has more fields than the current schema (DROP COLUMN),
       the extra packed data is simply skipped.
       If the row has fewer fields (ADD COLUMN), fill the missing fields
       from the table's default record so they get their DEFAULT value. */
    const uint cur_nf = share->fields_cached;
    uint unpack_count = MY_MIN(stored_fields, cur_nf);

    /* Pre-fill default values for columns added after this row was written.
       Copy each new field's bytes from default_values into buf so that
       they have the correct DEFAULT even when the field is NOT NULL. */
    if (stored_fields < cur_nf)
    {
        const TidesDB_share::field_plan_t *plan_d = share->field_plan.data();
        for (uint i = stored_fields; i < cur_nf; i++)
        {
            const TidesDB_share::field_plan_t &fp = plan_d[i];
            memcpy(buf + fp.src_off, table->s->default_values + fp.src_off, fp.pack_len);
        }
    }

    deserialize_unpack_fields(buf, from, from_end, unpack_count);
}

void ha_tidesdb::deserialize_unpack_fields(uchar *buf, const uchar *from, const uchar *from_end,
                                           uint unpack_count)
{
    /* memcpy_ok fields write directly to `to` via memcpy, so they never
       need move_field_offset.  The slow-path branch covers CHAR / VARCHAR
       / BLOB; only Field_blob::unpack writes through field->ptr (via
       set_ptr), so we only pay the virtual move_field_offset pair when
       the destination buffer is not record[0] AND the field needs the
       slow path.  buf == record[0] (ptrdiff == 0) is the common case
       for index scans and PK reads, so the loop avoids the vcall pair
       entirely there. */
    const my_ptrdiff_t ptrdiff = (my_ptrdiff_t)(buf - table->record[0]);
    const TidesDB_share::field_plan_t *plan = share->field_plan.data();
    const bool all_not_null = share->has_no_nullable;
    for (uint i = 0; i < unpack_count; i++)
    {
        const TidesDB_share::field_plan_t &fp = plan[i];
        if (!all_not_null && fp.maybe_null)
        {
            if (table->field[i]->is_real_null(ptrdiff)) continue;
        }
        if (from >= from_end) break;
        uchar *to = buf + fp.src_off;
        if (fp.memcpy_ok)
        {
            if (from + fp.pack_len > from_end) break;
            memcpy(to, from, fp.pack_len);
            from += fp.pack_len;
        }
        else
        {
            Field *f = table->field[i];
            const uchar *next;
            if (ptrdiff != 0)
            {
                f->move_field_offset(ptrdiff);
                next = f->unpack(to, from, from_end);
                f->move_field_offset(-ptrdiff);
            }
            else
            {
                next = f->unpack(to, from, from_end);
            }
            if (!next) break;
            from = next;
        }
    }
}

void ha_tidesdb::deserialize_row(uchar *buf, const std::string &row)
{
    const std::string *plain = &row;
    std::string decrypted;

    if (share->encrypted)
    {
        decrypted = tidesdb_decrypt_row(row.data(), row.size(), share->encryption_key_id);
        if (decrypted.empty())
        {
            /* Decryption failed! we zero record to avoid returning garbage */
            memset(buf, 0, table->s->reclength);
            return;
        }
        last_row = std::move(decrypted);
        plain = &last_row;
    }

    deserialize_row(buf, (const uchar *)plain->data(), plain->size());
}
