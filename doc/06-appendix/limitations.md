---
title: Limitations
description: What TideSQL does not do, and the behaviors to plan around.
---

# Limitations

**Foreign keys carry a few shape restrictions.** TideSQL enforces foreign keys inside the engine,
including `ON DELETE` and `ON UPDATE` with `CASCADE`, `SET NULL`, and `RESTRICT`, references to a
primary key or to a non-nullable unique key, and self-references. A foreign key column declared with
descending order is rejected at `CREATE TABLE` and `ALTER TABLE`, because the engine matches child
rows against a forward sort key, and so is a reference to a nullable unique key, because the
value-only child probe cannot reproduce that key's null indicator. A reference to parent columns
with only a plain index is also rejected, where InnoDB accepts it. One difference goes the other
way, since a self-referencing `ON UPDATE CASCADE` is applied, where InnoDB treats it as `RESTRICT`.
Everything else behaves as in InnoDB. See [Foreign Keys](/reference/foreign-keys) for the full
description.

**Only appending a column is instant.** Rows store their fields by position, so `DROP COLUMN`,
`ADD COLUMN ... FIRST` or `AFTER`, and any column reorder rebuild the table by copy, as do a change to
the primary key or to a column type such as `INT` to `BIGINT`. Appending a `BIT` column or a stored
generated column copies too. Adding a regular secondary index runs inplace but blocks writes until
the build finishes, so `LOCK=NONE` is refused, and a `FULLTEXT` or `SPATIAL` index is added by copy.
See
[Online DDL](/administration/online-ddl).

**Statistics are cached for up to two seconds.** Right after a bulk load the optimizer may briefly
see stale row counts. `ANALYZE TABLE` forces an immediate refresh.

**Write conflicts surface at commit and are the application's to retry.** Concurrency is optimistic
MVCC, so a multi-statement transaction at `REPEATABLE READ` or higher can fail at commit with a
first-committer-wins conflict, which reaches the client as `ER_ERROR_DURING_COMMIT` (ERROR 1180)
wrapping handler error 149, or 146 for transient contention. An error raised inside a statement
instead arrives as `ER_LOCK_DEADLOCK` (1213) or `ER_LOCK_WAIT_TIMEOUT` (1205). An application that
uses explicit `BEGIN ... COMMIT` blocks at those levels should retry on these errors. Autocommit
statements run at `READ COMMITTED`, where the library does no write-write checking, and a
transaction that wrote nothing never conflicts. There are no pessimistic row locks, and
`SELECT ... FOR UPDATE` takes none, so there are no lock waits and no lock-wait deadlocks to tune.
See [Transactions and Isolation](/concepts/transactions).
