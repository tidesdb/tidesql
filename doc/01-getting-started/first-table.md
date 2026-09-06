---
title: Your First Table
description: Create a table, insert rows, and see how a TideSQL table maps onto TidesDB column families.
---

# Your First Table

Creating a TidesDB table is a matter of the `ENGINE` clause:

```sql
CREATE TABLE events (
  id    INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ts    DATETIME NOT NULL,
  kind  VARCHAR(50),
  data  TEXT
) ENGINE=TIDESDB;
```

This creates one TidesDB column family to hold the table's rows. If you later drop the table, that
column family and all of its SSTables are removed with it.

```sql
INSERT INTO events (ts, kind, data) VALUES (NOW(), 'signup', 'alice');
INSERT INTO events (ts, kind, data) VALUES (NOW(), 'login',  'alice');

SELECT * FROM events ORDER BY id;
```

Everything you would expect from a SQL table works from here. The rest of this manual is about the
parts that are specific to TidesDB.

## The one idea to carry forward

A TideSQL table is one column family for its rows, plus one more column family for each secondary
index. Each column family is an independent LSM-tree with its own memtable presence, its own
SSTables, and its own compaction schedule. That mapping is what the [Data Model](/concepts/data-model)
chapter builds on, and it is worth keeping in mind because it explains where storage, statistics,
and maintenance operations act.

## Where to go next

- [Data Model](/concepts/data-model) for how rows, keys, and indexes are laid out.
- [Transactions and Isolation](/concepts/transactions) before running anything with write
  contention, because the concurrency model differs from InnoDB.
- [Durability and Sync Modes](/concepts/durability) to decide what a committed write guarantees.
- [Table Options](/reference/table-options) for the per-table knobs available at `CREATE TABLE`.
