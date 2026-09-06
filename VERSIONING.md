# Versioning

TideSQL is the MariaDB storage engine that embeds the TidesDB library. It
follows [SemVer](https://semver.org/) (MAJOR.MINOR.PATCH). Two contracts version
here and they move independently. One is the SQL surface the plugin presents to
MariaDB. The other is the on-disk format, which TideSQL does not define itself
but inherits whole from the TidesDB release it links.

**Public surface** is what a SQL user or an operator can depend on - the
`TIDESDB` engine name, the `CREATE TABLE` options, the system and status
variables, and the observable SQL behavior of the engine. Anything reachable
only by editing plugin source carries no compatibility guarantee.

## TideSQL 5.0.0 pairs with TidesDB v10.0.0

Each TideSQL release links exactly one TidesDB release and stores data in that
library's on-disk format. TideSQL 5.0.0 links TidesDB v10.0.0 and writes the
v10 format line. The plugin version and the library version keep their own
cadence, so the pairing is recorded here and surfaced at runtime through the
`tidesdb_version` status variable for the plugin and `tidesdb_library_version`
for the linked library.

## Major
- The linked TidesDB major changes, which opens a new on-disk format line.
- A `CREATE TABLE` option, system variable, or status variable is removed or
  changes meaning.
- Observable SQL behavior changes in a way that can affect results or query
  plans.
- A database created by the prior major needs a dump and reload rather than an
  in-place upgrade.

## Minor
- Backward-compatible additions to the SQL surface, such as a new table option,
  a new system or status variable, or a new engine capability.
- Reads every database written by prior minors of the same major.
- Any new on-disk behavior from the linked library is opt-in and default-off, so
  a downgrade stays possible.

## Patch
- Bug and security fixes in the plugin.
- A patch bump of the linked TidesDB library that does not change the on-disk
  format or observable behavior.
- Must not change the SQL surface or the on-disk format, so it is safe to apply
  without reading release notes.

## Compatibility matrix

<!-- Records the durability guarantees operators check before touching production. -->

The on-disk format belongs to the linked TidesDB library. TidesDB stamps one
format number into every file it writes and checks it for **exact equality** on
read, rejecting any other version outright. The rollback boundary for a TideSQL
release is therefore the set of releases that link a library writing the same
format line.

| TideSQL | TidesDB library | On-disk format | Rollback boundary            |
|---------|-----------------|----------------|------------------------------|
| 5.0.0   | 10.0.0          | 10             | any TideSQL linking format 10 |

- **TideSQL 5.0.0 opens the v10 format line** by linking TidesDB v10.0.0. It
  reads only the v10 format, so a database created by an earlier release line
  does not open in place and no in-place migration ships for that step. A
  database from an earlier release moves across by dumping with `mysqldump` and
  reloading through SQL.
- **Rollback across TideSQL 5.x is unrestricted** while every 5.x minor links a
  TidesDB release that writes format 10. A minor that moves to a new format must
  make it opt-in and default-off, which is what keeps that column true, and the
  release that changes it updates this table in the same commit.

Add a row per release. A release that moves none of the columns still gets a
row, because "unchanged" is the answer an operator is looking for.
