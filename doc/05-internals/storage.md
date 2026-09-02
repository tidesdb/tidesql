---
title: How Data Is Stored
description: The physical layout of rows, keys, and secondary index entries inside a column family.
---

# How Data Is Stored

Understanding the physical layout helps when reading `ANALYZE TABLE` output or diagnosing
performance. The storage mechanics below the column family, SSTables, compaction, and recovery,
belong to the library and are covered in the [TidesDB library manual](/internals/architecture).

Each table's data lives in a column family, an independent LSM-tree. Writes go to a skip-list
memtable. When the memtable fills it becomes immutable and is flushed as a sorted SSTable, and
compaction merges overlapping SSTables into higher levels while keeping the sorted invariant.

## Keys

Row keys inside a column family carry a namespace prefix byte, `0x01` for data rows and `0x00` for
metadata. The metadata namespace holds the auto-increment counter, under an `AINC` key as an 8-byte
big-endian value, and the FTS aggregate counters (document count and word count for BM25 scoring),
so a data scan that seeks to `0x01` naturally skips them.

Primary-key bytes are encoded in a memcmp-comparable form. For a signed 32-bit integer the encoding
flips the sign bit and stores the result big-endian, so `-1` sorts before `0` and `0` before `1`
under a plain byte comparison. The same principle covers the other numeric types and string
collations. A table without an explicit primary key gets a hidden 8-byte big-endian row id from an
atomic counter, recovered at open by seeking the last key in the column family.

## Row values

Row values are a packed binary format. Each row begins with a 5-byte header, a magic byte `0xFE`
followed by the null bitmap size (2 bytes little-endian) and the field count (2 bytes little-endian)
as of the write. That header is what makes `ADD COLUMN` and `DROP COLUMN` instant, because the
deserializer can adapt to rows written under any prior schema. After the header comes the null
bitmap, then each non-null field serialized with `Field::pack()`. On read, `Field::unpack()`
restores the fields. A row written with fewer fields than the current schema, from before a column
was added, fills the missing fields with their `DEFAULT`, and a row written with more fields, from
before a column was dropped, has the extra data skipped. This is more compact than the raw record
buffer, especially for `VARCHAR` and `CHAR` columns.

## Secondary index entries

A secondary index entry lives in its own column family. The key concatenates the comparable
index-column bytes with the comparable primary-key bytes, and the value is a single zero byte, so all
the information is in the key. To resolve a lookup the engine seeks into the index CF, reads the key,
splits off the trailing PK bytes, and does a point-get into the data CF. When the query needs only
indexed columns and each is of a reconstructable type, integers, temporal types, or fixed
`CHAR`/`BINARY` in binary or latin1, the row is decoded straight from the index key bytes and the
data-CF point-get is skipped.
