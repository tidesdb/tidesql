---
title: Glossary
description: Terms specific to how TideSQL maps SQL onto TidesDB.
---

# Glossary

This glossary covers terms as TideSQL uses them. For the library's own vocabulary, SSTable
internals, compaction, recovery, see the [TidesDB library glossary](/appendix/glossary).

**Column family** - an independent ordered keyspace inside TidesDB, its own LSM-tree. A TideSQL
table is one column family for its rows plus one more per secondary index.

**Comparable key** - the memcmp-comparable byte encoding TideSQL uses for keys, so byte order equals
logical order. Integers are big-endian with the sign bit flipped, strings use the collation sort key.

**Conflict footprint** - the set of reads and writes a transaction records so the library can run its
first-committer-wins check at commit. A concurrent write to a footprinted row makes the losing
transaction fail its commit.

**Covering read** - a query whose columns are all carried by the index key, so the engine
materializes the row from the index bytes without a point-get into the data CF.

**Data CF** - the column family that holds a table's row data, as opposed to the secondary-index CFs.

**Dividing level** - the LSM level the library uses as its primary compaction target, computed from
`MIN_LEVELS`, `LEVEL_SIZE_RATIO`, and `DIVIDING_LEVEL_OFFSET`.

**First-committer-wins** - the write-write conflict rule, where the first transaction to commit a
change to a row succeeds and a second transaction that changed the same row fails at commit. It
applies from `SNAPSHOT` isolation upward, and `SERIALIZABLE` keeps it while adding read-set tracking
for full serializable isolation.

**Foreign key** - a referential constraint TideSQL enforces in the engine, checked on every write
and persisted in a reserved catalog column family, with `CASCADE`, `SET NULL`, and `RESTRICT`
referential actions. See [Foreign Keys](/reference/foreign-keys).

**Klog and vlog** - the key log and value log of an SSTable. Values smaller than the database
`value_separation_threshold` are stored inline in the klog, larger ones go to the vlog with a
pointer in the klog. A table set `KEEP_VALUES_INLINE` keeps every value in the klog whatever its
size.

**Memtable** - the in-memory skip list that absorbs writes before they flush to an SSTable. It is
shared across the database in TidesDB 10, so its sizing and WAL durability are set by the
`tidesdb_memtable_*` system variables rather than per table.

**Optimistic MVCC** - the concurrency model, where readers and writers never block each other and a
write-write conflict is detected at commit rather than prevented by a lock.

**Prepared transaction** - a transaction durably logged under an XID by the first phase of two-phase
commit, awaiting a commit or rollback that can arrive from another connection or after a restart
through XA recovery.

**Read amplification** - the number of SSTables a worst-case point lookup may probe, reported by the
library and used by the cost model.

**Single-delete** - a delete primitive that lets compaction cancel a put and its matching tombstone
together the first time they meet, used automatically for secondary-index entries and opt-in for the
primary CF.

**Tombstone** - the marker a delete writes to hide an older value until compaction reclaims it. A
high tombstone density slows range scans, which the tombstone-density trigger acts on.

**Write-intent map** - the process-wide record of each uncommitted write's key that lets a Galera
applier find and brute-force abort a local transaction it conflicts with, the lock-free analogue of
a row lock. See [Replication and High Availability](/administration/replication-ha).
