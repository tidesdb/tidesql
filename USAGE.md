# TideSQL Usage Guide

TideSQL is MySQL 5.1 with TidesDB as the default storage engine. This guide covers setup, usage, and TidesDB-specific features.

## Quick Start

### 1. Initialize the Data Directory

Before first use, initialize the MySQL data directory:

```bash
cd /path/to/tidesql

# Create data directory
mkdir -p /tmp/tidesql-test/data

# Initialize system tables
perl scripts/mysql_install_db --no-defaults --srcdir=$(pwd) --datadir=/tmp/tidesql-test/data --force
```

### 2. Start the Server

```bash
# Start mysqld with TidesDB plugin directory
LD_LIBRARY_PATH=/usr/local/lib ./sql/mysqld --no-defaults \
    --datadir=/tmp/tidesql-test/data \
    --basedir=$(pwd) \
    --language=$(pwd)/sql/share/english \
    --socket=/tmp/tidesql-test.sock \
    --port=3307 \
    --plugin-dir=$(pwd)/storage/tidesdb/.libs &
```

### 3. Install the TidesDB Plugin

On first run, install the TidesDB storage engine plugin:

```bash
./client/mysql --socket=/tmp/tidesql-test.sock -u root \
    -e "INSTALL PLUGIN tidesdb SONAME 'ha_tidesdb.so';"
```

### 4. Verify TidesDB is Available

```bash
./client/mysql --socket=/tmp/tidesql-test.sock -u root -e "SHOW ENGINES;"
```

Expected output:
```
Engine     Support  Comment                                                  Transactions  XA  Savepoints
TidesDB    DEFAULT  TidesDB LSM-based storage engine with ACID transactions  YES           NO  YES
MRG_MYISAM YES      Collection of identical MyISAM tables                    NO            NO  NO
MEMORY     YES      Hash based, stored in memory, useful for temporary...    NO            NO  NO
MyISAM     YES      Default engine as of MySQL 3.23 with great performance   NO            NO  NO
CSV        YES      CSV storage engine                                       NO            NO  NO
```

## Connecting to TideSQL

```bash
# Connect via socket
./client/mysql --socket=/tmp/tidesql-test.sock -u root

# Connect via TCP
./client/mysql -h 127.0.0.1 -P 3307 -u root
```

## Creating Tables with TidesDB

```sql
-- Explicitly specify TidesDB engine
CREATE TABLE users (
    id INT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL
) ENGINE=TidesDB;

-- Create another table
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    stock INT
) ENGINE=TidesDB;
```

### TidesDB is the Default Engine

TidesDB is the default storage engine. Tables created without specifying `ENGINE=` will use TidesDB:

```sql
-- This table will use TidesDB (the default)
CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT,
    total DECIMAL(10,2)
);
```

## TTL (Time-to-Live) Support

TidesDB supports automatic row expiration using a `TTL` column.

### Creating a Table with TTL

```sql
-- Add a TTL column (INT type) to enable per-row TTL
CREATE TABLE sessions (
  id INT PRIMARY KEY,
  user_id INT,
  session_data TEXT,
  TTL INT DEFAULT 0  -- TTL in seconds (0 = no expiration)
) ENGINE=TidesDB;
```

### Inserting Rows with TTL

```sql
-- Row expires in 60 seconds
INSERT INTO sessions (id, user_id, session_data, TTL) 
VALUES (1, 100, 'session data', 60);

-- Row expires in 1 hour (3600 seconds)
INSERT INTO sessions (id, user_id, session_data, TTL) 
VALUES (2, 101, 'session data', 3600);

-- Row never expires (TTL = 0 or NULL)
INSERT INTO sessions (id, user_id, session_data, TTL) 
VALUES (3, 102, 'permanent data', 0);
```

### TTL Column Rules

| Value | Behavior |
|-------|----------|
| `> 0` | Row expires after N seconds from insert/update time |
| `0` | Row never expires |
| `NULL` | Row never expires |

**Column naming:** Use `TTL` (or `_ttl` for backwards compatibility)

**Column type:** Must be an integer type (INT, BIGINT, SMALLINT, TINYINT)

### Updating TTL

```sql
-- Extend session TTL by updating the TTL column
UPDATE sessions SET TTL = 7200 WHERE id = 1;  -- Now expires in 2 hours
```

### Global Default TTL

For tables without a `TTL` column, you can set a global default:

```sql
SET GLOBAL tidesdb_default_ttl = 3600;  -- 1 hour default for all new rows
```

## Basic CRUD Operations

### INSERT

```sql
-- Single row insert
INSERT INTO users (username, email) VALUES ('alice', 'alice@example.com');

-- Multiple rows
INSERT INTO users (username, email) VALUES 
    ('bob', 'bob@example.com'),
    ('charlie', 'charlie@example.com'),
    ('diana', 'diana@example.com');

-- Insert with specific ID
INSERT INTO products (id, name, price, stock) VALUES 
    (1, 'Widget', 19.99, 100),
    (2, 'Gadget', 29.99, 50),
    (3, 'Gizmo', 9.99, 200);
```

### SELECT

```sql
-- Select all
SELECT * FROM users;

-- Select with conditions
SELECT * FROM users WHERE username = 'alice';

-- Select with ordering
SELECT * FROM products ORDER BY price DESC;

-- Select with limit
SELECT * FROM users LIMIT 10;

-- Join queries
SELECT u.username, o.product_id, o.quantity
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.id = 1;
```

### UPDATE

```sql
-- Update single row
UPDATE users SET email = 'alice.new@example.com' WHERE id = 1;

-- Update multiple rows
UPDATE products SET stock = stock - 1 WHERE id IN (1, 2, 3);

-- Update with conditions
UPDATE products SET price = price * 0.9 WHERE stock > 100;
```

### DELETE

```sql
-- Delete single row
DELETE FROM users WHERE id = 1;

-- Delete with conditions
DELETE FROM products WHERE stock = 0;

-- Delete all rows (TRUNCATE is faster)
TRUNCATE TABLE users;
```

## Server Management

### Starting the Server

```bash
# Development/testing (from source directory)
LD_LIBRARY_PATH=/usr/local/lib ./sql/mysqld --no-defaults \
    --datadir=/tmp/tidesql-test/data \
    --basedir=$(pwd) \
    --language=$(pwd)/sql/share/english \
    --socket=/tmp/tidesql-test.sock \
    --port=3307 \
    --plugin-dir=$(pwd)/storage/tidesdb/.libs &

# Production (after make install)
/usr/local/tidesql/bin/mysqld_safe --defaults-file=/usr/local/tidesql/my.cnf &
```

### Stopping the Server

```bash
# Graceful shutdown
./client/mysqladmin --socket=/tmp/tidesql-test.sock -u root shutdown

# Or
killall mysqld
```

### Sample my.cnf Configuration

```ini
[mysqld]
basedir = /usr/local/tidesql
datadir = /usr/local/tidesql/data
socket = /tmp/tidesql.sock
port = 3307
language = /usr/local/tidesql/share/english
plugin-dir = /usr/local/tidesql/lib/plugin

# Load TidesDB plugin at startup
plugin-load = tidesdb=ha_tidesdb.so

# Optional: Make TidesDB the default engine
# default-storage-engine = TidesDB

[client]
socket = /tmp/tidesql.sock
port = 3307
```

## Working with Indexes

TidesDB uses LSM-tree based indexes:

```sql
-- Primary key (automatically indexed)
CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    product_id INT,
    quantity INT,
    order_date DATE
);

-- Secondary indexes are stored as separate column families
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    INDEX idx_category (category)
);
```

## Fulltext Search

TidesDB supports fulltext search using an inverted index structure.

### Creating a Fulltext Index

```sql
CREATE TABLE articles (
    id INT PRIMARY KEY,
    title VARCHAR(200),
    body TEXT,
    FULLTEXT INDEX ft_body (body)
) ENGINE=TidesDB;

-- Multiple columns in fulltext index
CREATE TABLE documents (
    id INT PRIMARY KEY,
    title VARCHAR(200),
    content TEXT,
    FULLTEXT INDEX ft_content (title, content)
) ENGINE=TidesDB;
```

### Fulltext Search Queries

```sql
-- Insert sample data
INSERT INTO articles VALUES 
    (1, 'MySQL Tutorial', 'This is a tutorial about MySQL database'),
    (2, 'TidesDB Guide', 'TidesDB is a fast storage engine'),
    (3, 'Database Basics', 'Learn about database fundamentals');

-- Search for rows containing 'database'
SELECT * FROM articles WHERE MATCH(body) AGAINST('database');
-- Returns rows 1 and 3

-- Search for 'storage'
SELECT * FROM articles WHERE MATCH(body) AGAINST('storage');
-- Returns row 2
```

### Fulltext Search Notes

- Words are tokenized and converted to lowercase
- Minimum word length follows MySQL's `ft_min_word_len` setting (default: 4)
- Maximum word length follows MySQL's `ft_max_word_len` setting (default: 84)
- The inverted index is stored in a separate column family per fulltext index
- Updates and deletes automatically maintain the fulltext index

## Transactions and ACID

TidesDB provides full ACID transaction support with 5 isolation levels.

### ACID Properties

| Property | TidesDB Implementation |
|----------|------------------------|
| **Atomicity** | All operations in a transaction succeed or fail together |
| **Consistency** | Data integrity maintained through transactions |
| **Isolation** | 5 isolation levels from READ UNCOMMITTED to SERIALIZABLE |
| **Durability** | Configurable sync modes (none, interval, full) |

### Basic Transactions

```sql
-- Start transaction
START TRANSACTION;

-- Perform operations
INSERT INTO orders (id, user_id, product_id, quantity) VALUES (1, 1, 1, 5);
UPDATE products SET stock = stock - 5 WHERE id = 1;

-- Commit
COMMIT;

-- Or rollback
ROLLBACK;
```

### Isolation Levels

TidesDB supports 5 isolation levels via the `tidesdb_default_isolation` system variable:

| Level | Description | Use Case |
|-------|-------------|----------|
| `read_uncommitted` | Sees uncommitted changes | Maximum concurrency, minimal consistency |
| `read_committed` | Sees only committed data (default) | Balanced for OLTP workloads |
| `repeatable_read` | Consistent snapshot, phantom reads possible | Strong point read consistency |
| `snapshot` | Write-write conflict detection | Prevents lost updates |
| `serializable` | Full SSI (Serializable Snapshot Isolation) | Strongest guarantees, higher abort rates |

```sql
-- Standard MySQL syntax (recommended)
SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- Check current isolation level
SELECT @@tx_isolation;

-- Or use TidesDB-specific variable for SNAPSHOT isolation
SET GLOBAL tidesdb_default_isolation = 'snapshot';
```

TidesDB's SNAPSHOT isolation (write-write conflict detection) is only available via the `tidesdb_default_isolation` variable since MySQL doesn't have a SNAPSHOT level.

### Durability (Sync Modes)

Control the durability vs performance tradeoff:

```sql
-- Maximum performance, least durable (OS handles flushing)
SET GLOBAL tidesdb_sync_mode = 'none';

-- Balanced: periodic background syncing (default)
SET GLOBAL tidesdb_sync_mode = 'interval';
SET GLOBAL tidesdb_sync_interval_us = 128000;  -- 128ms

-- Maximum durability: fsync on every write
SET GLOBAL tidesdb_sync_mode = 'full';
```

## Table Maintenance

### Check Table Status

```sql
SHOW TABLE STATUS LIKE 'users';
```

### Analyze Table

```sql
ANALYZE TABLE users;
```

### Optimize Table

```sql
-- Triggers compaction in TidesDB
OPTIMIZE TABLE users;
```

## Migrating from Other Engines

### Convert MyISAM Table to TidesDB

```sql
ALTER TABLE old_table ENGINE=TidesDB;
```

### Copy Data Between Engines

```sql
-- Create TidesDB table with same structure
CREATE TABLE new_table LIKE old_table;
ALTER TABLE new_table ENGINE=TidesDB;

-- Copy data
INSERT INTO new_table SELECT * FROM old_table;
```

## Best Practices

### 1. Primary Key Design

TidesDB performs best with well-designed primary keys:

```sql
-- Good: Sequential or time-based keys
CREATE TABLE events (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_type VARCHAR(50),
    event_data TEXT,
    created_at TIMESTAMP
);

-- Good: Composite key for time-series data
CREATE TABLE metrics (
    sensor_id INT,
    timestamp BIGINT,
    value DOUBLE,
    PRIMARY KEY (sensor_id, timestamp)
);
```

### 2. Batch Inserts

For bulk loading, use batch inserts:

```sql
-- Efficient: Single statement with multiple values
INSERT INTO logs (message, level, timestamp) VALUES
    ('msg1', 'INFO', NOW()),
    ('msg2', 'WARN', NOW()),
    ('msg3', 'ERROR', NOW());

-- Or use LOAD DATA for large imports
LOAD DATA INFILE '/path/to/data.csv' INTO TABLE logs;
```

### 3. Memory Configuration

For write-heavy workloads, increase write buffer:

```ini
tidesdb_write_buffer_size = 268435456  # 256MB
```

For read-heavy workloads, increase block cache:

```ini
tidesdb_block_cache_size = 536870912   # 512MB
```

### 4. Compression

LZ4 compression is enabled by default. For storage-constrained environments, this provides good compression with minimal CPU overhead.

## TidesDB System Variables

TidesDB provides several system variables for tuning performance and behavior:

### Storage Configuration

```sql
-- Data directory (read-only, set at startup)
SHOW VARIABLES LIKE 'tidesdb_data_dir';

-- Write buffer size (memtable flush threshold)
SET GLOBAL tidesdb_write_buffer_size = 134217728;  -- 128MB

-- Block cache size for reads
SET GLOBAL tidesdb_block_cache_size = 268435456;   -- 256MB
```

### Compression

```sql
-- Enable/disable compression (default: enabled)
SET GLOBAL tidesdb_enable_compression = ON;

-- Compression algorithm: 0=none, 1=snappy, 2=lz4, 3=zstd
SET GLOBAL tidesdb_compression_algo = 2;  -- LZ4 (default)
```

### Bloom Filters

```sql
-- Enable bloom filters for faster lookups
SET GLOBAL tidesdb_enable_bloom_filter = ON;

-- Bloom filter false positive rate (lower = more accurate, more memory)
SET GLOBAL tidesdb_bloom_fpr = 0.01;  -- 1% false positive rate
```

### Background Threads

```sql
-- Number of flush threads
SET GLOBAL tidesdb_flush_threads = 2;

-- Number of compaction threads
SET GLOBAL tidesdb_compaction_threads = 2;
```

### TTL and Durability

```sql
-- Default TTL for rows without _ttl column (0 = no expiration)
SET GLOBAL tidesdb_default_ttl = 0;

-- Sync mode: 0=none, 1=interval, 2=full
SET GLOBAL tidesdb_sync_mode = 1;  -- interval (default)

-- Sync interval in microseconds (for interval mode)
SET GLOBAL tidesdb_sync_interval_us = 128000;  -- 128ms
```

### View All TidesDB Variables

```sql
SHOW VARIABLES LIKE 'tidesdb%';
```

## Monitoring

### Check Engine Status

```sql
SHOW ENGINE TIDESDB STATUS;
```

### View Active Connections

```sql
SHOW PROCESSLIST;
```

### Check Table Size

```sql
SELECT 
    table_name,
    table_rows,
    data_length,
    index_length
FROM information_schema.tables 
WHERE table_schema = 'your_database' 
AND engine = 'TidesDB';
```

## Backup and Recovery

### Backup

```bash
# Use mysqldump for logical backup
mysqldump -S /tmp/tidesql.sock -u root -p --all-databases > backup.sql

# Or backup specific database
mysqldump -S /tmp/tidesql.sock -u root -p mydb > mydb_backup.sql
```

### Restore

```bash
mysql -S /tmp/tidesql.sock -u root -p < backup.sql
```

### Physical Backup

For physical backups, stop the server and copy the data directory:

```bash
# Stop server
mysqladmin -S /tmp/tidesql.sock -u root -p shutdown

# Copy data
cp -r /usr/local/tidesql/data /backup/tidesql-$(date +%Y%m%d)

# Restart server
/usr/local/tidesql/bin/mysqld_safe --defaults-file=/usr/local/tidesql/my.cnf &
```

### Online Backup (BACKUP TABLE)

TidesDB supports online backup using the `BACKUP TABLE` command:

```sql
-- Backup a table (creates snapshot in /tmp/tidesdb_backup_TIMESTAMP)
BACKUP TABLE mytable TO '/tmp/backup';
```

This flushes the memtable and copies the data directory without blocking reads/writes.

## Troubleshooting

### TidesDB Plugin Won't Load

**Error:** `undefined symbol: tidesdb_*`

The plugin needs libtidesdb. Ensure LD_LIBRARY_PATH includes `/usr/local/lib`:

```bash
LD_LIBRARY_PATH=/usr/local/lib ./sql/mysqld ...
```

Or add to system library path:

```bash
echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/tidesdb.conf
sudo ldconfig
```

### Error Message File Warning

If you see:
```
[ERROR] Error message file had only 641 error messages, but it should contain at least 645
```

Regenerate the error message files:

```bash
cd extra
./comp_err --charset=../sql/share/charsets \
    --in_file=../sql/share/errmsg.txt \
    --out_dir=../sql/share/ \
    --header_file=../include/mysqld_error.h \
    --name_file=../include/mysqld_ername.h \
    --state_file=../include/sql_state.h
```

### Table Not Found

```sql
-- Check if table exists
SHOW TABLES LIKE 'tablename';

-- Recreate if needed
DROP TABLE IF EXISTS tablename;
CREATE TABLE tablename (...) ENGINE=TidesDB;
```

### Connection Issues

```bash
# Check if server is running
ps aux | grep mysqld

# Check socket file
ls -la /tmp/tidesql-test.sock

# View server output
tail -f /tmp/tidesql-test/mysqld.log
```
