---
title: Backup and Checkpoint
description: Online backup, durable checkpoints that flush and copy the data directory, and why FLUSH TABLES FOR EXPORT is not a backup.
---

# Backup and Checkpoint

## Online backup

Setting `tidesdb_backup_dir` to a directory path triggers a consistent backup of the whole TidesDB
data directory:

```sql
SET GLOBAL tidesdb_backup_dir = '/path/to/backup';
```

The backup flushes the memtable, holds off compaction and value-log reclaim while it copies the
manifest, the value log, and every SSTable the manifest references, and syncs those files first. It
takes no lock that writers wait on, so reads and writes continue while it runs. The directory is
created if it does not exist, though only the last path component is created, so its parent must
already exist. A backup into a directory that already holds an earlier one succeeds, but files the
earlier run left there are not removed, so use a new or empty directory each time. The same applies
to `tidesdb_checkpoint_dir` below. The engine rolls back and frees the calling connection's own open
transaction before starting, so run the backup from a connection with no uncommitted work. After it
completes, the variable reflects the path of the last successful backup. If the memtable cannot
drain, or a compaction or value-log reclaim does not release in time, the statement fails with an
error and the variable keeps its previous value. Clear it with an empty string:

```sql
SET GLOBAL tidesdb_backup_dir = '';
```

## Checkpoint

Setting `tidesdb_checkpoint_dir` takes a checkpoint, which forces a durability barrier on the live
database and then writes a consistent copy of it to the given path:

```sql
SET GLOBAL tidesdb_checkpoint_dir = '/path/to/checkpoint';
```

The durability barrier flushes the memtable, then forces every SSTable, the write-ahead log, the
value log, and the manifest to disk regardless of the sync mode, so the live database is fully
durable to this point. The engine then runs the same copy as `tidesdb_backup_dir` above, so the copy
left at the path is a consistent, directly-openable database. The difference from a backup is the
barrier on the live database, most notably the forced sync of the active write-ahead log, which a
backup does not do because it copies no WAL. Use a checkpoint when you want the running database
made durable as part of the snapshot, and a backup when you only need the copy.

## FLUSH TABLES FOR EXPORT

The engine accepts `FLUSH TABLES ... FOR EXPORT`, which takes a server-level table lock that blocks
writes to the table until `UNLOCK TABLES`. The engine does nothing further for it. It does not flush
the memtable, and the library's background flush and compaction keep rewriting files under the data
directory while the lock is held, so copying column family directories under this lock does not
produce a consistent copy. Use `tidesdb_backup_dir` or `tidesdb_checkpoint_dir` for that.
