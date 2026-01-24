# TideSQL Usage Guide

TideSQL is MySQL 5.1 with TidesDB as an available storage engine. This guide covers setup, usage, and TidesDB-specific features.

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
Engine     Support  Comment
TidesDB    YES      TidesDB LSM-based storage engine with ACID transactions
MyISAM     DEFAULT  Default engine as of MySQL 3.23 with great performance
MEMORY     YES      Hash based, stored in memory
CSV        YES      CSV storage engine
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

### Using TidesDB as Default Engine

To make TidesDB the default for new tables, start the server with:

```bash
./sql/mysqld --no-defaults \
    --default-storage-engine=TidesDB \
    --plugin-load=tidesdb=ha_tidesdb.so \
    ... other options ...
```

## TTL (Time-to-Live) Support

TidesDB supports automatic row expiration using a special `_ttl` column.

### Creating a Table with TTL

```sql
-- Add a _ttl column (INT type) to enable per-row TTL
CREATE TABLE sessions (
  id INT PRIMARY KEY,
  user_id INT,
  session_data TEXT,
  _ttl INT DEFAULT 0  -- TTL in seconds (0 = no expiration)
) ENGINE=TidesDB;
```

### Inserting Rows with TTL

```sql
-- Row expires in 60 seconds
INSERT INTO sessions (id, user_id, session_data, _ttl) 
VALUES (1, 100, 'session data', 60);

-- Row expires in 1 hour (3600 seconds)
INSERT INTO sessions (id, user_id, session_data, _ttl) 
VALUES (2, 101, 'session data', 3600);

-- Row never expires (TTL = 0 or NULL)
INSERT INTO sessions (id, user_id, session_data, _ttl) 
VALUES (3, 102, 'permanent data', 0);
```

### TTL Column Rules

| Value | Behavior |
|-------|----------|
| `> 0` | Row expires after N seconds from insert/update time |
| `0` | Row never expires |
| `NULL` | Row never expires |

**Column naming:** Use `_ttl` or `_tidesdb_ttl`

**Column type:** Must be an integer type (INT, BIGINT, SMALLINT, TINYINT)

### Updating TTL

```sql
-- Extend session TTL by updating the _ttl column
UPDATE sessions SET _ttl = 7200 WHERE id = 1;  -- Now expires in 2 hours
```

### Global Default TTL

For tables without a `_ttl` column, you can set a global default:

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

-- Note: Secondary indexes are stored as separate column families
-- For best performance, design tables with good primary keys
```

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

**Note:** TidesDB's SNAPSHOT isolation (write-write conflict detection) is only available via the `tidesdb_default_isolation` variable since MySQL doesn't have a SNAPSHOT level.

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

### Server Aborts During Bootstrap

Ensure you're using MyISAM as the default engine during bootstrap (handled automatically by `mysql_install_db`). TidesDB is a plugin that loads after the server starts.
