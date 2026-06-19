# TideSQL HA replication / failover tests (SQL level)

This is the SQL-level analogue of the TidesDB library's `test/failover/` suite.
Where the library drives raw `tidesdb_node` processes, this drives real
`mariadbd` nodes entirely over SQL, sharing one directory as the object-store
bucket via the **FS backend** -- so it needs no S3 or MinIO and runs anywhere.

It exists to cover the part the library tests cannot: the **plugin's** behaviour
through promotion and fencing (schema discovery, runtime-role gating of DDL/DROP,
the `Tidesdb_*` status vars, the `tidesdb_promote_primary` trigger).

## How it maps to the library suite

| library `tidesdb_node` | SQL here |
|---|---|
| `PUT k v` | `INSERT` |
| `PROMOTE` | `SET GLOBAL tidesdb_promote_primary=ON` |
| `FLUSH` | `OPTIMIZE TABLE` |
| `stat primary_epoch` / `replica_mode` | `SHOW GLOBAL STATUS LIKE 'Tidesdb_primary_epoch'` / `'Tidesdb_replica_mode_active'` |
| verify range | `SELECT COUNT(*)` |

## Topology

N `mariadbd` nodes, each with its own datadir + local TidesDB cache, all pointing
at one shared directory as the bucket:

```
tidesdb_object_store_backend = FS
tidesdb_objstore_fs_path      = <shared bucket dir>
tidesdb_replica_mode          = ON     # every node boots read-only
```

All nodes start as replicas; one is promoted to primary
(`SET GLOBAL tidesdb_promote_primary=ON`). The FS connector serialises its
conditional writes with `flock`, so the single-writer epoch fence holds across
processes exactly as it does over S3.

## Scenarios

- **`failover_catchup`** -- node 0 is promoted and writes 400 rows; a lagging
  replica is then promoted and must (a) catch up to all rows and (b) be able to
  `CREATE TABLE` + `INSERT` as the new primary. (b) is the regression test for the
  runtime-role gating fix -- a node promoted from a replica must persist schema and
  accept writes, which it could not when those paths gated on the static startup
  sysvar.
- **`zombie_fence`** -- a replica is promoted while the old primary stays alive;
  the old primary keeps writing and publishes, and must be fenced
  (`Tidesdb_replica_mode_active` flips to 1, its writes never appear on the new
  primary). This is the split-brain case at the SQL layer.

## Run locally

```bash
TIDESQL_PREFIX=/path/to/mariadb/install ./run_local.sh
# keep the scratch dir + node logs for inspection:
TDB_HA_KEEP=1 TIDESQL_PREFIX=... ./run_local.sh
```

Requires a MariaDB install whose plugin (`ha_tidesdb.so`) is built against a
libtidesdb that includes the FS object-store backend (the default build does).

## CI

`.github/workflows/ha_replication.yml` builds the library + MariaDB + plugin and
runs this on a nightly + manual + plugin-touching-PR cadence (the MariaDB build is
heavy). A real-MinIO S3 tier -- the analogue of the library's `failover_k8s` -- is a
planned follow-up; the scenarios are written so the same script can target S3 by
swapping the backend config.
