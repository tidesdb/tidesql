---
title: Write-Path Optimizations
description: How the engine keeps delete-heavy and write-heavy workloads from degrading, through single-delete, the tombstone-density trigger, range-delete compaction, and backpressure absorption.
---

# Write-Path Optimizations

## Single-delete

A `DELETE` writes a tombstone into every column family the row touches, the primary row CF plus one
CF per secondary, full-text, or spatial index. A regular tombstone must be carried through every
compaction until it reaches the largest active level, because a lower level could still hold an older
put of the same key that the tombstone masks. Insert-then-delete workloads pile these at the low end
of the key space where DELETE range scans start, and scan cost climbs with the backlog until
compaction catches up.

The library's single-delete primitive lets compaction drop a put and its matching tombstone together
the first time both appear in one merge input, regardless of level. Its contract is at most one put
between single-deletes on the same key. For reads a single-delete behaves like a regular tombstone.

The engine applies this in two ways.

### Secondary-index single-delete, automatic

Every secondary index entry, `(col_values, pk)` for a regular index, `(term, pk)` for FULLTEXT,
`(hilbert, pk)` for SPATIAL, is written exactly once per row lifetime and deleted exactly once,
across INSERT, UPDATE, DELETE, `REPLACE INTO`, and `INSERT ... ON DUPLICATE KEY UPDATE`. The same
composite bytes never see a second put without an intervening delete, so the single-delete contract
holds by construction. The engine therefore uses single-delete for every secondary-index delete with
no configuration, which covers three of the four tombstones per deleted row on a table with three
secondary indexes.

### Primary-CF single-delete, opt-in

The primary row CF is different. `UPDATE ... SET non_pk_col` writes a fresh row at the same key, a
put over a put, and `REPLACE INTO` on a table without secondary indexes overwrites silently for the
same reason. Under either pattern, dropping a primary-CF put and its later single-delete together can
re-expose an older put, which the engine cannot detect from outside. So primary-CF single-delete is
behind the session variable `tidesdb_single_delete_primary`, default OFF. Enabling it is a promise
that the session performs no UPDATE on non-PK columns, no `REPLACE INTO` or
`INSERT ... ON DUPLICATE KEY UPDATE` on the silent-overwrite path, and that a new row for a given PK
is always preceded by a DELETE of that PK.

```sql
SET SESSION tidesdb_single_delete_primary = 1;
INSERT INTO events (...) VALUES ...;   -- monotonic PK
DELETE FROM events WHERE ts < NOW() - INTERVAL 1 HOUR;
```

Leave it OFF for any session that may issue those statements, because setting it ON there can leak
older row versions through reads after a compaction.

## Tombstone-density trigger

Single-delete handles the insert-then-delete shape where the contract holds. For everything else,
UPDATEs of indexed columns, `REPLACE INTO` on tables without secondary indexes, mixed OLTP,
tombstones accumulate inside SSTables until a compaction at the largest level reclaims them, and a
read over a deleted region pays for every tombstone the merge iterator skips.

The tombstone-density trigger lets the engine act on this without waiting for a capacity or
file-count trigger. After each flush it inspects level-1 SSTables and asks whether any one SSTable's
tombstone count divided by entry count exceeds a ratio while holding at least a minimum entry count.
A single witness escalates compaction. The `TOMBSTONE_DENSITY_TRIGGER` and
`TOMBSTONE_DENSITY_MIN_ENTRIES` [table options](/reference/table-options) arm it, and the aggregates
show up in the `Tidesdb_total_tombstones`, `Tidesdb_tombstone_ratio`,
`Tidesdb_max_sst_tombstone_density`, and `Tidesdb_max_sst_tombstone_density_level`
[status variables](/reference/status-variables) and in the Tombstones block of
`SHOW ENGINE TIDESDB STATUS`.

## Compact after a range delete

Sliding-window expiry, tenant eviction, and time-bucketed log rotation share a shape, a multi-row
DELETE over a known primary-key range followed by a wait for compaction to reclaim the space. A
caller that already knows the range can skip the wait with
`tidesdb_compact_after_range_delete_min_rows`:

```sql
SET SESSION tidesdb_compact_after_range_delete_min_rows = 100000;
DELETE FROM events WHERE ts < NOW() - INTERVAL 30 DAY;
```

The default `0` disables it. A non-zero value is both an opt-in and a row-count threshold, so only a
DELETE touching at least that many rows triggers the synchronous compaction and a one-row DELETE
never pays for it. The engine tracks the comparable minimum and maximum primary-key bytes seen during
the statement, two string swaps per `delete_row` with no extra scan or locking, and on
`end_bulk_delete` compacts the observed range on the primary CF. Secondary-index tombstones are not
compacted this way, because a PK range does not bound a secondary-index range, and are left to the
tombstone-density trigger. The compaction runs on the caller's thread, so the DELETE returns only
after it commits, and the threshold should be high enough that the compaction time is small relative
to the DELETE that triggered it.

## Range tombstone for a whole-range delete

When a multi-row DELETE removes every row across a contiguous primary-key span, the engine writes one
range tombstone over the span instead of a per-row tombstone for each key. One interval on the WAL
and in the memtable replaces thousands, and compaction drops the whole span in a single step rather
than carrying each tombstone through the levels.

The engine cannot know upfront that a DELETE is a clean range, so it works it out as it goes.
Through the statement it buffers each deleted primary-row key and holds its tombstone back rather
than writing it. With the tombstones deferred, every row the statement deleted is still live, so on
`end_bulk_delete` the count of live rows in the touched `[min, max]` span equals the number of
buffered keys exactly when the statement removed the whole span. In that case one
`tidesdb_txn_delete_range` replaces the run. A survivor, from a residual `WHERE` condition, an
`IN`-list gap, an index-condition-pushdown rejection, or an `ORDER BY ... LIMIT` that picks
scattered rows, leaves more live rows than buffered keys, and the buffer falls back to per-row
tombstones with no change in result. The whole thing lives in the transaction, so a `ROLLBACK`
restores the rows like any other write.

Deferral is skipped for a table with a delete trigger, since a deferred tombstone must never hide a
row from a trigger reading the table mid-statement, and under Galera, where a deferred range
tombstone would not line up with the per-row certification the write path already issues.
Secondary-index entries are still deleted per row, because a primary-key range does not bound a
secondary-index range, so the range tombstone covers the primary row CF alone. A delete larger than
an internal cap flushes its buffer to per-row tombstones and finishes on the ordinary path, which
keeps the buffered keys bounded to a few megabytes. When the range tombstone is written it already
reclaims the span, so the compact-after-range-delete pass above is skipped for that statement.

## Backpressure absorption

When ingest outruns flush, the library applies backpressure at its L0 admission policy, making a
commit dwell or wait for the flush queue to drain before it is admitted. This happens inside the
library, so a writer is slowed rather than failed, and the effect is visible in the write-stall
[status variables](/reference/status-variables), `Tidesdb_writes_throttled`, `Tidesdb_writes_blocked`,
`Tidesdb_write_stall_us`, and `Tidesdb_write_stall_ceiling_hits`. A sustained rise in the
ceiling-hits counter is the sign that flush is not keeping up with ingest.

Only when the library's own no-progress budget is spent, with the memtable, flush queue, or L0
backlog still at its cap, does it return `TDB_ERR_MEMORY_LIMIT` to the plugin. The engine maps that
to `HA_ERR_LOCK_WAIT_TIMEOUT`, which is the accurate name because nothing is locked, and the
statement can be retried once flush has caught up.
