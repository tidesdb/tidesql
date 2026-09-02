---
title: Preface
description: What TideSQL is, how it relates to the TidesDB library, who this manual is for, and the conventions it uses.
---

# Preface

TideSQL is a pluggable storage engine for [MariaDB](https://mariadb.org/), built on the TidesDB
library. It lets a MariaDB table keep its data in a TidesDB log-structured merge tree instead of
InnoDB, reachable through ordinary SQL. Switching a table from InnoDB to TidesDB is a change to
the `ENGINE` clause and nothing more.

This is the TideSQL 5.x manual, the storage engine for TidesDB v10.0.0, and the companion to the
[TidesDB library manual](/preface). It tracks the 5.x line as a whole, so a minor or patch release
extends this manual rather than starting a separate one. Where this book says "the library" it means TidesDB, and
where it needs to explain a storage behavior in depth it points at the library manual rather than
repeating it.

## What it is

- **Transactional**, through the library's multi-version concurrency control, so readers proceed
  without blocking writers.
- **A full SQL engine surface**, with primary keys, secondary indexes, foreign keys, auto-increment,
  virtual and stored generated columns, savepoints, TTL expiration, data-at-rest encryption,
  full-text, vector and spatial indexes, online DDL, partitioning, and online backup, all reached
  through standard SQL.
- **Log-structured**, so it favors write throughput, with compaction keeping read cost bounded.

## What it is not

- **Not a fork of InnoDB.** The concurrency model is optimistic MVCC from the library rather than
  InnoDB's pessimistic row locks, which changes how write contention behaves. The
  [Transactions](/concepts/transactions) chapter is the one to read before porting a
  contention-heavy workload.
- **Not a distributed system on its own.** The engine stores data on one node, and clustering comes
  from the layers above it. Within those layers TideSQL is a first-class participant in both MariaDB
  replication and Galera, with engine-level write-set certification and cross-node conflict
  resolution rather than a generic pass-through. See
  [Replication and High Availability](/administration/replication-ha).
- **Not a place to relearn the library.** The on-disk format, compaction, and recovery all belong
  to TidesDB and are documented in its manual.

## Who this manual is for

**If you are putting tables on TideSQL**, read [Getting Started](/getting-started/install), then
[Concepts](/concepts/data-model), and keep the [SQL Reference](/reference/table-options) nearby.

**If you are operating a server that uses it**, [Administration](/administration/monitoring)
covers monitoring, backup, maintenance, online DDL, and partitioning.

**If you want to know how a row becomes bytes on disk**, [Internals](/internals/storage) covers
the storage layout, the optimizer integration, and the write-path optimizations the engine layers
over the library.

## Where the engine keeps its data

TidesDB data files live in a sibling directory of the MariaDB data directory named
`tidesdb_data`, and the engine manages that layout itself. The location can be overridden with the
read-only `tidesdb_data_home_dir` system variable.
