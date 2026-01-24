# tidesql 
A space and write optimized MySQL/MariaDB pluggable storage engine that uses [TidesDB](https://github.com/tidesdb/tidesdb).

## Features

LSM-tree based storage with:
  - ACID transactions (MySQL-level BEGIN/COMMIT/ROLLBACK)
  - Compression (LZ4, Zstd, Snappy)
  - Bloom filters for fast lookups
  - Column families (rows, index, meta, fulltext)
  - 5 isolation levels
  - TTL support

- Full support for AUTO_INCREMENT columns with persistence across restarts

- Secondary Indexes (BTREE-style)

- Fulltext Search with BM25 ranking - `MATCH...AGAINST` queries with Okapi BM25 scoring

- Foreign key support

- Table RENAME support

- Dynamic configuration updates (some settings can be changed at runtime)

## Prerequisites

### Install TidesDB

```bash
# Clone and build TidesDB
git clone https://github.com/tidesdb/tidesdb.git
cd tidesdb
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
sudo cmake --install build
sudo ldconfig
```

### Install Dependencies

```bash
# Debian/Ubuntu
sudo apt install -y cmake build-essential \
    libzstd-dev liblz4-dev libsnappy-dev \
    libmysqlclient-dev

# Or for MariaDB
sudo apt install -y libmariadb-dev
```

### MySQL/MariaDB Source (for development)

Building a storage engine requires MySQL/MariaDB source code headers:

```bash
# Download MySQL source
wget https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.36.tar.gz
tar xzf mysql-8.0.36.tar.gz
export MYSQL_SOURCE=/path/to/mysql-8.0.36

# Or MariaDB source
git clone https://github.com/MariaDB/server.git mariadb-server
export MARIADB_SOURCE=/path/to/mariadb-server
```

## Building

```bash
# Configure
cmake -S . -B build \
    -DMYSQL_SOURCE=/path/to/mysql-source \
    -DTIDESDB_DIR=/usr/local

# Build
cmake --build build

# Install plugin
sudo cmake --install build
```

## Installation in MySQL/MariaDB

```sql
-- Install the plugin
INSTALL PLUGIN tidesql SONAME 'ha_tidesql.so';

-- Verify installation
SHOW PLUGINS;
SHOW ENGINES;
```

## Usage

```sql
-- Create a table using TideSQL engine
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
) ENGINE=TIDESQL;

-- Standard SQL operations work as expected
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
SELECT * FROM users WHERE id = 1;
UPDATE users SET name = 'Alice Smith' WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

## Configuration

TideSQL is fully configurable through MySQL system variables. All settings can be configured in `my.cnf` or at runtime.

### System Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `tidesql_data_dir` | `./tidesql_data` | Base directory for TidesDB data files |
| `tidesql_compression` | `lz4` | Compression algorithm: `none`, `lz4`, `zstd`, `snappy` |
| `tidesql_write_buffer_size` | `67108864` (64MB) | Memtable size before flush to disk |
| `tidesql_block_cache_size` | `134217728` (128MB) | Block cache size (0 to disable) |
| `tidesql_flush_threads` | `2` | Number of background flush threads |
| `tidesql_compaction_threads` | `2` | Number of background compaction threads |
| `tidesql_sync_mode` | `1` | Sync mode: 0=none, 1=interval, 2=full |
| `tidesql_sync_interval` | `128000` | Sync interval in microseconds (for mode=1) |
| `tidesql_enable_bloom_filter` | `ON` | Enable bloom filters for faster lookups |
| `tidesql_bloom_fpr` | `0.01` | Bloom filter false positive rate (0.001-0.1) |
| `tidesql_enable_block_indexes` | `ON` | Enable block indexes for faster seeks |
| `tidesql_max_open_sstables` | `256` | Maximum open SSTable file handles |
| `tidesql_log_level` | `1` | Log level: 0=debug, 1=info, 2=warn, 3=error, 4=fatal, 5=none |
| `tidesql_txn_isolation` | `1` | Transaction isolation: 0=READ_UNCOMMITTED, 1=READ_COMMITTED, 2=REPEATABLE_READ, 3=SNAPSHOT, 4=SERIALIZABLE |

### Transaction Isolation Levels

TideSQL supports 5 isolation levels via the `tidesql_txn_isolation` setting:

| Level | Value | Description |
|-------|-------|-------------|
| READ_UNCOMMITTED | 0 | Transactions can see uncommitted changes from other transactions (dirty reads) |
| READ_COMMITTED | 1 | Transactions only see committed changes (default) |
| REPEATABLE_READ | 2 | Consistent reads within a transaction; same query returns same results |
| SNAPSHOT | 3 | Transaction sees a consistent snapshot from its start time |
| SERIALIZABLE | 4 | Full isolation; transactions execute as if serial |

**Using Standard MySQL Syntax:**

TideSQL supports MySQL's standard transaction isolation level commands:

```sql
-- Set isolation level for the next transaction
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
-- ... operations use REPEATABLE READ ...
COMMIT;

-- Set isolation level for the session
SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- Start transaction with consistent snapshot (uses SNAPSHOT isolation)
START TRANSACTION WITH CONSISTENT SNAPSHOT;
```

**Mapping to TidesDB Isolation Levels:**

| MySQL Level | TidesDB Level | Description |
|-------------|---------------|-------------|
| READ UNCOMMITTED | READ_UNCOMMITTED | Dirty reads allowed |
| READ COMMITTED | READ_COMMITTED | Only see committed data |
| REPEATABLE READ | REPEATABLE_READ | Consistent reads within transaction |
| SERIALIZABLE | SERIALIZABLE | Full isolation, may conflict |
| WITH CONSISTENT SNAPSHOT | SNAPSHOT | Point-in-time snapshot |

**Engine-Level Default (my.cnf):**

```ini
[mysqld]
# Default isolation when MySQL's tx_isolation is not explicitly set
tidesql_txn_isolation = 1  # 0=READ_UNCOMMITTED, 1=READ_COMMITTED, 2=REPEATABLE_READ, 3=SNAPSHOT, 4=SERIALIZABLE
```

**Use Cases:**

- **READ UNCOMMITTED** - Maximum throughput for analytics where dirty reads are acceptable
- **READ COMMITTED** - Default; good balance for most OLTP workloads
- **REPEATABLE READ** - Consistent reads within a transaction (e.g., generating reports)
- **SERIALIZABLE** - When correctness is critical; may cause conflicts on concurrent modifications
- **WITH CONSISTENT SNAPSHOT** - Point-in-time consistency for backups or consistent multi-table reads

### Example Configuration (my.cnf)

```ini
[mysqld]
# TideSQL Storage Engine Configuration
tidesql_data_dir = /var/lib/mysql/tidesql_data
tidesql_compression = lz4
tidesql_write_buffer_size = 134217728    # 128MB
tidesql_block_cache_size = 268435456     # 256MB
tidesql_flush_threads = 4
tidesql_compaction_threads = 4
tidesql_sync_mode = 1                    # interval sync
tidesql_sync_interval = 100000           # 100ms
tidesql_enable_bloom_filter = ON
tidesql_bloom_fpr = 0.01
tidesql_max_open_sstables = 512
tidesql_log_level = 2                    # warn
```

### Runtime Configuration

```sql
-- View current settings
SHOW VARIABLES LIKE 'tidesql%';

-- Change settings at runtime (session or global)
SET GLOBAL tidesql_write_buffer_size = 134217728;
SET GLOBAL tidesql_compression = 'zstd';
```

### Status Variables

Monitor TideSQL performance with status variables:

```sql
SHOW STATUS LIKE 'tidesql%';
```

| Status Variable | Description |
|-----------------|-------------|
| `tidesql_rows_read` | Total rows read |
| `tidesql_rows_written` | Total rows inserted |
| `tidesql_rows_updated` | Total rows updated |
| `tidesql_rows_deleted` | Total rows deleted |
| `tidesql_tables_opened` | Total tables opened |
| `tidesql_tables_created` | Total tables created |

### Sync Modes Explained

- Mode 0 (none) - Fastest, relies on OS page cache. Risk of data loss on crash.
- Mode 1 (interval) - Balanced. Syncs every `sync_interval` microseconds. Default.
- Mode 2 (full) - Safest. Fsync on every write. Slowest but most durable.

### Compression Algorithms

- none - No compression. Best for pre-compressed data or CPU-constrained systems.
- lz4 - Fast compression with good ratios. Default and recommended.
- zstd - Best compression ratio. Good for storage-constrained or read-heavy workloads.
- snappy - Fast compression. Good balance of speed and ratio.

### Column Families (always-on)

TideSQL uses TidesDB column families to separate data by access pattern:

- rows - row storage (`r:<rowid>` -> packed row)
- index - secondary indexes (`i:<indexno>:<keybytes>:<rowid>` -> `r:<rowid>`)
- meta - engine metadata (`m:row_id_counter`, `m:auto_inc_value`)
- fulltext - BM25 inverted index (`ft:df:<term>`, `ft:tf:<term>:<rowid>`, `ft:dl:<rowid>`)

### Fulltext Search (BM25)

TideSQL implements Okapi BM25 ranking for fulltext search:

```sql
-- Create table with fulltext index
CREATE TABLE articles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    content TEXT,
    FULLTEXT(title, content)
) ENGINE=TIDESQL;

-- Search with relevance ranking
SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('database performance');
```

BM25 parameters: k1=1.2, b=0.75 (standard values)

### Runtime Configuration Updates

Some settings can be updated at runtime using `tidesdb_cf_update_runtime_config`:
- `write_buffer_size` - Memtable flush threshold
- `bloom_fpr` - Bloom filter false positive rate (for new SSTables)
- `sync_mode` - Durability mode
- `sync_interval_us` - Sync interval

Non-updatable settings (would corrupt existing data):
- `compression_algorithm`, `enable_bloom_filter`, `enable_block_indexes`, `comparator_name`

## Current Limitations

- Some settings cannot be changed for existing SSTables (compression, bloom filter presence)

## License

GPL v2 (compatible with MySQL/MariaDB)
