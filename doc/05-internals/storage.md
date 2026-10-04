---
title: How Data Is Stored
description: The physical layout of rows, keys, and secondary index entries inside a column family.
---

# How Data Is Stored

Understanding the physical layout helps when reading `ANALYZE TABLE` output or diagnosing
performance. The storage mechanics below the column family, SSTables, compaction, and recovery,
belong to the library and are covered in the [TidesDB library manual](/internals/architecture).

Each table's data lives in a column family, an LSM-tree with its own SSTables and levels. Writes go
to a skip-list memtable, which in TidesDB 10 is database-level and shared by every column family,
as is the write-ahead log. When the memtable fills it becomes immutable and is flushed to sorted
SSTables in each column family it holds keys for, and compaction merges overlapping SSTables into
higher levels while keeping the sorted invariant.

## Keys

Row keys inside a column family carry a namespace prefix byte, `0x01` for data rows and `0x00` for
metadata. The metadata namespace holds the auto-increment start value set by `AUTO_INCREMENT=N`,
under an `AINC` key as an 8-byte big-endian value so it survives a restart, and the FTS aggregate counters (document count and word count for BM25 scoring),
so a data scan that seeks to `0x01` naturally skips them.

Primary-key bytes are encoded in a memcmp-comparable form. For a signed 32-bit integer the encoding
flips the sign bit and stores the result big-endian, so `-1` sorts before `0` and `0` before `1`
under a plain byte comparison. The same principle covers the other numeric types and string
collations. A table without an explicit primary key gets a hidden 8-byte big-endian row id from an
atomic counter, recovered at open by seeking the last key in the column family.

## Row values

Row values are a packed binary format. Each row begins with a 5-byte header, a magic byte `0xFE`
followed by the null bitmap size (2 bytes little-endian) and the field count (2 bytes little-endian)
as of the write. After the header comes the null bitmap, then each non-null field serialized with
`Field::pack()`, in table order. On read, `Field::unpack()` restores the fields. Nothing in the row
names its columns, so an old row decodes by position. A row written with fewer fields than the
current schema, from before columns were appended, fills the missing fields and their null bits
from their `DEFAULT`, which is what makes an appended `ADD COLUMN` instant. A change that moves an
existing column or its null bit, such as `DROP COLUMN` or adding a column before others, rewrites
the table instead (see [Online DDL](/administration/online-ddl)). This is more compact than the raw
record buffer, especially for `VARCHAR` and `CHAR` columns. On an `ENCRYPTED` table the whole packed row
is then wrapped in an encryption envelope before it is stored.

## Secondary index entries

A regular secondary index entry lives in its own column family. The key concatenates the comparable
index-column bytes with the comparable primary-key bytes, and the value is a single zero byte, so all
the information is in the key. To resolve a lookup the engine seeks into the index CF, reads the key,
splits off the trailing PK bytes, and does a point-get into the data CF. When the query needs only
indexed columns and each is of a reconstructable type, integers, `YEAR`, `DATE`, `DATETIME` or
`TIMESTAMP` without fractional seconds, or fixed
`CHAR`/`BINARY` in binary or latin1, the row is decoded straight from the index key bytes and the
data-CF point-get is skipped. This covering read applies to tables with an explicit primary key.

Because the key carries the primary key as its suffix, two rows with the same value in a `UNIQUE`
secondary index store two different keys. So every write that creates a non-NULL value in a
`UNIQUE` secondary index also writes a sentinel key, the table's data CF name, a NUL byte, one byte
of index number, and the comparable value, into a reserved column family named `__tidesdb_uniq`
that every table shares. Two transactions writing the same value then collide on that one key, and
at SNAPSHOT isolation or higher the commit-time conflict check lets only the first committer
through. The sentinel is skipped along with the uniqueness check when the session sets
`tidesdb_skip_unique_check`. The sentinel is never read for the uniqueness check itself, which stays
with the index probe, and a table's sentinels are purged by prefix when it is dropped, truncated, or
renamed. Internal families whose names begin with `__tidesdb` are left out of the per-family listing
in `SHOW ENGINE TIDESDB STATUS`.
