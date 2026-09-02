---
title: MariaDB Compatibility
description: Which MariaDB versions TideSQL is tested against, and the current release.
---

# MariaDB Compatibility

TideSQL is built against a MariaDB server and tracks the server's storage-engine interface, so a
given TideSQL release targets specific MariaDB versions. The table records which have been tested
and confirmed working. Full support means the engine is tested against all known functionality on
that server version.

| MariaDB version | Minimum TideSQL version | Full support |
|-----------------|-------------------------|:------------:|
| 10.x.x  | -        | No  |
| 11.4.10 | 3.4.0    | Yes |
| 11.8.6  | 4.0.0    | Yes |
| 12.2.2  | 1.0.0    | Yes |
| 12.3.1  | 4.2.6    | Yes |
| 13.0.2  | 4.5.4    | Yes |

This is the TideSQL 5.x manual and it pairs with TidesDB v10. The current pinned release is 5.0.0
(hex `0x50000`), linking TidesDB v10.0.0. A 5.x minor or patch links a TidesDB v10 release and
extends this same manual, so only a new major opens a new manual. See
[Versioning](https://github.com/tidesdb/tidesql/blob/master/VERSIONING.md) for how the plugin and
the library versions relate.

As versions are tested and confirmed working this table is updated.

Beyond standalone and replicated servers, TideSQL participates in Galera clustering through the
server's wsrep interface, with engine-level write-set certification and cross-node conflict
resolution. See [Replication and High Availability](/administration/replication-ha) for what the
engine does in a cluster.
