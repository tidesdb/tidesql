---
title: Data-at-Rest Encryption
description: Per-row encryption through MariaDB key management, key versions, and how it interacts with compression and indexes.
---

# Data-at-Rest Encryption

TidesDB can encrypt row data before it is written to the column family. Encryption uses MariaDB's
key management infrastructure, so it works with any configured key management plugin such as
`file_key_management`.

```sql
CREATE TABLE secrets (
  id INT NOT NULL PRIMARY KEY, val VARCHAR(100)
) ENGINE=TIDESDB `ENCRYPTED`=YES;
```

Each row is encrypted individually. The on-disk record is a 4-byte little-endian key version, then
a 16-byte random IV, then the ciphertext. The key-version prefix lets the engine decrypt rows that
were written under an older key after a rotation, because `encryption_key_get(key_id, key_version)`
resolves the exact key bytes used at write time. On read the engine decrypts transparently. You can
choose the key id:

```sql
CREATE TABLE classified (
  id INT NOT NULL PRIMARY KEY, data TEXT
) ENGINE=TIDESDB `ENCRYPTED`=YES `ENCRYPTION_KEY_ID`=2;
```

`ENCRYPTION_KEY_ID` defaults to 1 and ranges from 1 to 255.

Encryption can be turned on for an existing table. The change rewrites the table through a copy so
the current rows are stored as ciphertext:

```sql
ALTER TABLE existing_table `ENCRYPTED`=YES;
```

Encryption composes with everything else, including secondary indexes, BLOB columns, and TTL. The
secondary-index keys are not encrypted, because they must stay comparable for seeking, but the row
data those keys point at is encrypted in the data column family. Because ciphertext does not
compress, the engine forces the data column family's compression to `NONE` regardless of the table's
`COMPRESSION` option, and the index CFs are unaffected. See [Table Options](/reference/table-options)
for the compression interaction.

If `encryption_key_get()` cannot return the requested key, a rotation hole, a keyring plugin that
is not loaded, or a version that never existed, the encrypt or decrypt call fails closed. The engine
logs the failure and returns an error to the SQL layer rather than feeding uninitialized bytes into
the cipher. A row written with a key version no longer in the keyring is unreadable until the key is
restored, and the engine never silently mis-encrypts or returns zeroed plaintext.
