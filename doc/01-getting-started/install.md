---
title: Installing TideSQL
description: Loading the plugin, the install.sh builder, and the one linkage detail that decides whether the plugin loads at all.
---

# Installing TideSQL

TideSQL is a shared-object plugin. Once a MariaDB server is built with the plugin, load it at
startup from `my.cnf`:

```ini
[mysqld]
plugin-load-add=ha_tidesdb.so
plugin-maturity=beta
```

or dynamically in a running server:

```sql
INSTALL SONAME 'ha_tidesdb';
```

The plugin ships at Beta maturity, so the server's `plugin_maturity` threshold has to be `beta` or lower or the load is refused. A stable server defaults that threshold to `gamma`, which is stricter than `beta`, so the `plugin-maturity=beta` line above is needed, and `install.sh` writes it into the `my.cnf` it generates.

Once loaded it appears in `SHOW ENGINES`:

```
Engine: TidesDB
Support: YES
Comment: LSM B-tree engine with ACID transactions, MVCC concurrency, replication, and secondary, spatial, full-text and vector indexes
Transactions: YES
     XA: YES
Savepoints: YES
```

## Building with install.sh

The repository ships `install.sh`, which clones MariaDB, builds it with the plugin against a
matching TidesDB library, and writes a ready-to-run `my.cnf`. It resolves system dependencies,
submodules, and configuration.

```bash
git clone https://github.com/tidesdb/tidesql.git
cd tidesql
./install.sh --mariadb-prefix ~/mariadb-tidesdb
```

The options it accepts:

| Option | Description |
|--------|-------------|
| `--mariadb-prefix <path>` | MariaDB install directory |
| `--tidesdb-prefix <path>` | TidesDB library install directory (default `/usr/local`) |
| `--mariadb-version <tag>` | MariaDB branch or tag to build |
| `--tidesdb-version <tag>` | TidesDB library release tag |
| `--build-dir <path>` | Build directory |
| `--jobs <n>` | Parallel build jobs |
| `--skip-deps` | Skip system dependency installation |
| `--skip-tidesdb` | Skip building the library (use an already-installed one) |
| `--rebuild-plugin` | Rebuild and reinstall only `ha_tidesdb.so` against an existing MariaDB install |
| `--skip-engines <list>` | Comma-separated storage engines to exclude from the build |
| `--list-engines` | List available storage engines and exit |
| `--pgo` | Profile-guided optimization, a longer build for faster binaries |
| `--s3` | Build the library with its S3 connector compiled in, which requires libcurl |
| `--allocator <name>` | Link the library against `system` (default), `jemalloc`, `mimalloc`, or `tcmalloc` |

After it finishes, start the server and connect over the socket:

```bash
~/mariadb-tidesdb/bin/mariadbd --defaults-file=~/mariadb-tidesdb/my.cnf &
~/mariadb-tidesdb/bin/mariadb -S /tmp/mariadb.sock
```

## The allocator and why the plugin may fail to load

`--allocator` changes only the allocator inside `libtidesdb.so`. It does not touch mariadbd's own
allocator. It affects one operational detail that matters a great deal.

`jemalloc`, `mimalloc`, and `tcmalloc` place their thread-local state in the initial-exec TLS
model. That model needs its space reserved when the program starts. The plugin is loaded late, with
`dlopen`, well after startup, so when `libtidesdb.so` was linked against one of those allocators the
loader cannot find room for its TLS and the plugin fails to load with an error like:

```
Can't open shared library 'ha_tidesdb.so' (errno: 2, libjemalloc.so.2: cannot allocate memory in static TLS block)
```

The fix is to put the allocator in the process image at startup so its TLS is reserved up front.
Use MariaDB's `--malloc-lib`, or preload it directly:

```bash
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 \
  ~/mariadb-tidesdb/bin/mariadbd --defaults-file=~/mariadb-tidesdb/my.cnf &
```

A library built with the default `system` allocator has no such requirement and loads with no
preload. Check what a build linked against with:

```bash
ldd /usr/local/lib/libtidesdb.so | grep -E 'jemalloc|mimalloc|tcmalloc'
```

Because `--rebuild-plugin` does not rebuild the library, changing `--allocator` needs a full
install run to take effect.
