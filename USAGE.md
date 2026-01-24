# TideSQL Usage Guide

TideSQL is MySQL with TidesDB as the default storage engine. This guide covers basic usage and TidesDB-specific features.

## Connecting to TideSQL

```bash
# Connect via socket
mysql -S /tmp/tidesql.sock -u root -p

# Connect via TCP
mysql -h 127.0.0.1 -P 3307 -u root -p
```

## Creating Tables

Tables are created with TidesDB by default:

```sql
-- TidesDB is used automatically (default engine)
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Explicitly specify TidesDB
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    stock INT
) ENGINE=TidesDB;
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

## TidesDB Configuration

### View TidesDB Settings

```sql
SHOW VARIABLES LIKE 'tidesdb%';
```

| Variable | Default | Description |
|----------|---------|-------------|
| `tidesdb_data_dir` | `$datadir/tidesdb` | TidesDB data directory |
| `tidesdb_flush_threads` | 2 | Number of flush threads |
| `tidesdb_compaction_threads` | 2 | Number of compaction threads |
| `tidesdb_block_cache_size` | 64MB | Block cache size |
| `tidesdb_write_buffer_size` | 64MB | Memtable size before flush |
| `tidesdb_enable_compression` | ON | Enable LZ4 compression |
| `tidesdb_enable_bloom_filter` | ON | Enable bloom filters |

### Configuration in my.cnf

```ini
[mysqld]
# TidesDB settings
tidesdb_data_dir = /var/lib/tidesql/tidesdb
tidesdb_flush_threads = 4
tidesdb_compaction_threads = 4
tidesdb_block_cache_size = 134217728    # 128MB
tidesdb_write_buffer_size = 134217728   # 128MB
tidesdb_enable_compression = ON
tidesdb_enable_bloom_filter = ON
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

## Transactions

TidesDB supports ACID transactions:

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

TidesDB supports multiple isolation levels:

```sql
-- Set isolation level for session
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- Check current level
SELECT @@tx_isolation;
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

### Table Not Found

Ensure the column family exists in TidesDB:

```sql
-- Check if table exists
SHOW TABLES LIKE 'tablename';

-- Recreate if needed
DROP TABLE IF EXISTS tablename;
CREATE TABLE tablename (...) ENGINE=TidesDB;
```

### Slow Queries

1. Check if bloom filters are enabled
2. Ensure adequate block cache size
3. Consider table design and primary key choice

### Connection Issues

```bash
# Check if server is running
ps aux | grep mysqld

# Check socket file
ls -la /tmp/tidesql.sock

# Check error log
tail -f /usr/local/tidesql/data/*.err
```
