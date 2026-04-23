# TIDESQL Docker Images

This directory contains Dockerfiles for running TideSQL (MariaDB + TidesDB
storage engine) in a container.


## Prerequisites

  - Docker OR Podman (tested with Docker 28.2.2 and Podman 5.8)
  - An internet connection at build time to clone MariaDB and TidesDB sources
    from GitHub


## Building Images

To make building less error-prone, the following scripts are available:
- `docker/cleanup.sh` removes the existing image, any container
  created from it, and associated volumes.
- `docker/setup.sh` builds the image. This includes compiling MariaDB and
  TidesDB so, depending on your hardware, can take 20-40 minutes. It also
  creates a new container so that the image can be tested immediately.
- `docker/rebuild.sh` calls both, passing all necessary environment
  variables.

These scripts can be called from any path.

Environment variables accepted by the scripts:

- `IMAGE_NAME` Name of the newly built image, default: tidesql
- `TAG` Image tag, default: latest
- `CONTAINER_NAME` Name of the container to be created, default: tidesql
- `MARIADB_VERSION` MariaDB version to build, default: 11.8
- `TIDESDB_VERSION` TidesDB release tag to build, default: latest from GitHub
- `EXCLUDE_ENGINES`  Comma-separated list of optional engines to exclude from the
  build.  Engine names are case-insensitive.  Use `ALL` to include all optional
  engines.
- `INCLUDE_ENGINES` Comma-separated list of optional engines to include.
  All other optional engines are excluded. Engine names are case-insensitive.

`EXCLUDE_ENGINES` and `INCLUDE_ENGINES` cannot be set at the same time.

Optional engines that can be excluded or selectively included:

- ARCHIVE
- BLACKHOLE
- CONNECT
- EXAMPLE
- FEDERATED
- FEDERATEDX
- MROONGA
- ROCKSDB
- S3
- SPHINX
- SPIDER

TidesDB and some other engines are always compiled in and cannot be excluded.

Specifying an engine name outside the optional list in either variable, or
specifying an always-included engine in `EXCLUDE_ENGINES`, will cause the
script to exit with an error.

Example - build without Mroonga and RocksDB:

```
EXCLUDE_ENGINES=Mroonga,RocksDB bash docker/rebuild.sh
```

Example - build with only Blackhole and RocksDB (all others excluded):

```
INCLUDE_ENGINES=BLACKHOLE,ROCKSDB bash docker/rebuild.sh
```

## Build Arguments

The following build arguments are understood by the Dockerfile.

Some of them currently cannot be changed by using the scripts.

```
  MARIADB_VERSION   MariaDB branch or tag to build    REQUIRED
  TIDESDB_VERSION   TidesDB release tag to build      REQUIRED
  TIDESDB_PREFIX    TidesDB install prefix            Default: /usr/local
  MARIADB_PREFIX    MariaDB install prefix            Default: /usr/local/mariadb
  WITH_TESTS        Include MTR in the image. 1=yes, 0=no. Default: 0
  WITH_S3           Build with S3 object store support. 1=yes, 0=no. Default: 0
                    Requires libcurl and OpenSSL (already in the base image).
  ALLOCATOR         Memory allocator to link libtidesdb.so against.
                    One of: system (default), jemalloc, mimalloc, tcmalloc.
                    Only affects TidesDB's internal allocations; mariadbd's
                    allocator is unchanged.  Combine with LD_PRELOAD at
                    container startup for a process-wide swap.
  DISABLED_ENGINES  Normally set indirectly via EXCLUDE_ENGINES /
                    INCLUDE_ENGINES in rebuild.sh (see REBUILD SCRIPT above).
```

`MARIADB_VERSION` and `TIDESDB_VERSION` have no default in the Dockerfile and
must always be supplied.  The scripts (`rebuild.sh`, `setup.sh`) default them
to `11.8` and the latest TidesDB release from GitHub respectively, so a bare
`bash docker/rebuild.sh` works without any extra configuration.

Example - include the test suite:

```
  docker build \
      -f docker/ubuntu/Dockerfile \
      --build-arg MARIADB_VERSION=12.2.3 \
      --build-arg TIDESDB_VERSION=v9.0.9 \
      --build-arg WITH_TESTS=1 \
      -t tidesql:12.2.3-ubuntu-tests \
      .
```

Example - build with S3 object store support:

```
  docker build \
      -f docker/ubuntu/Dockerfile \
      --build-arg MARIADB_VERSION=12.2.3 \
      --build-arg TIDESDB_VERSION=v9.0.9 \
      --build-arg WITH_S3=1 \
      -t tidesql:12.2.3-s3 \
      .
```

Example - link libtidesdb against jemalloc:

```
  docker build \
      -f docker/ubuntu/Dockerfile \
      --build-arg MARIADB_VERSION=12.2.3 \
      --build-arg TIDESDB_VERSION=v9.0.9 \
      --build-arg ALLOCATOR=jemalloc \
      -t tidesql:12.2.3-jemalloc \
      .

  # Or via setup.sh / rebuild.sh:
  ALLOCATOR=jemalloc bash docker/rebuild.sh
```

For a process-wide allocator swap (covers mariadbd too), start the
container with an LD_PRELOAD pointing at the same library:

```
  docker run -d --name tidesql \
      -e LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 \
      -p 3306:3306 tidesql:12.2.3-jemalloc
```


## Running a Container

Start a container with named volumes so data and configuration persist across
restarts:

```
  docker run -d \
      --name tidesql \
      -p 3306:3306 \
      -v tidesql-conf:/etc/mysql \
      -v tidesql-data:/usr/local/mariadb/data \
      -v tidesql-log:/usr/local/mariadb/log \
      tidesql:latest
```

On the first start the entrypoint initialises the data directory
(mariadb-install-db) before launching the server. Subsequent starts reuse
the existing data directory.

The conf volume is initialised from the my.cnf baked into the image.  Mount a
host directory there to supply your own configuration:

Connect for the first time via the command line:

```
docker exec -ti tidesql mariadb
```

Then, you can create users to connect from the outside.


## Quick Test

Verify the TidesDB plugin is loaded:

```
SHOW ENGINE TIDESDB STATUS;
```

And now, you can play with it!

```
CREATE SCHEMA IF NOT EXISTS test;
CREATE TABLE person (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, full_name VARCHAR(100)) ENGINE=TidesDB;
START TRANSACTION;
INSERT INTO person (id, full_name) VALUES (DEFAULT, 'Invisible Man');
ROLLBACK;
SELECT * FROM person;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;
INSERT INTO person (id, full_name) VALUES
    (DEFAULT, 'Tom Baker'),
    (DEFAULT, 'Leonard Nimoy');
SELECT * FROM person;
COMMIT;
SELECT * FROM person;
```

## Customising the Configuration

Files placed in the `conf/custom` directory are loaded after the main
`my.cnf`, so any settings they contain override the defaults. This directory
is mounted as part of the `/etc/mysql` volume.

See `conf/custom/00-example.cnf` for an example that shows how to override
`sql_mode` and `old_mode`. Copy it to a new file (any name ending in `.cnf`)
in the same directory and edit as needed.

Files added to `conf/custom` (other than `00-example.cnf`) are not versioned
and will not appear in git.


## Initialising Databases and Creating Users

By default, the `root` user needs to authenticate locally via the `UNIX_SOCKET`
plugin and only the `test` database exists. In practice, you might want to
include more databases in your image, more users, or drop the `test` database.

To do so, you can use the `/docker-entrypoint-initdb.d` directory, is a volume
that can be mapped to a local directory. Any `.sql` and `.sh` files placed there
are executed automatically, in alphabetical order, every time a container starts -
right after MariaDB is up and ready to accept connections.

This is useful for:

- Creating more users, or being more flexible about how `root` can connect.
- Creating databases needed by your application.
- Restoring a backup, you can place a dump file (e.g. `01-restore.sql`) in the
  directory so that it is imported when a newly created container starts.
  Remove the file (or unmap the volume) after the first start to avoid
  re-running it on subsequent restarts.

See `initdb.d/00-example.sql` for an annotated example. Copy it to a new file
in the same directory and edit as needed.

Files added to `initdb.d` (other than `00-example.sql`, which does nothinbg) are
ignored by git.

To map the directory to a local path when running a container:

```
docker run -d \
    --name tidesql \
    -p 3306:3306 \
    -v tidesql-conf:/etc/mysql \
    -v tidesql-data:/usr/local/mariadb/data \
    -v tidesql-log:/usr/local/mariadb/log \
    -v /path/to/your/initdb.d:/docker-entrypoint-initdb.d \
    tidesql:latest
```


## Stopping and Removing the Container

```
docker stop tidesql
docker rm   tidesql
docker volume rm tidesql-conf tidesql-data tidesql-log
```
