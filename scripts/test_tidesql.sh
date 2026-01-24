#!/bin/bash
#
# TideSQL Storage Engine Test Script
# Tests the tidesql storage engine functionality with MariaDB/MySQL
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
TEST_DB="tidesql_test"
PLUGIN_PATH="${PLUGIN_PATH:-/usr/lib/mysql/plugin/ha_tidesql.so}"

# Build mysql command
if [ -n "$MYSQL_PASSWORD" ]; then
    MYSQL_CMD="mysql -u${MYSQL_USER} -p${MYSQL_PASSWORD} -h${MYSQL_HOST} -P${MYSQL_PORT}"
else
    MYSQL_CMD="mysql -u${MYSQL_USER} -h${MYSQL_HOST} -P${MYSQL_PORT}"
fi

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

run_sql() {
    echo "$1" | $MYSQL_CMD 2>&1
}

run_sql_db() {
    echo "$1" | $MYSQL_CMD "$TEST_DB" 2>&1
}

test_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((TESTS_PASSED++))
}

test_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((TESTS_FAILED++))
}

test_plugin_install() {
    log_info "Testing plugin installation..."
    
    # Check if plugin file exists
    if [ ! -f "$PLUGIN_PATH" ]; then
        test_fail "Plugin file not found at $PLUGIN_PATH"
        return 1
    fi
    
    # Try to install the plugin
    result=$(run_sql "INSTALL PLUGIN tidesql SONAME 'ha_tidesql.so';" 2>&1) || true
    
    # Check if already installed or successful
    if echo "$result" | grep -q "already exists"; then
        log_info "Plugin already installed"
    elif echo "$result" | grep -q "ERROR"; then
        test_fail "Plugin installation failed: $result"
        return 1
    fi
    
    # Verify plugin is active
    result=$(run_sql "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='TIDESQL';")
    if echo "$result" | grep -q "ACTIVE"; then
        test_pass "Plugin installed and active"
        return 0
    else
        test_fail "Plugin not active"
        return 1
    fi
}

test_create_database() {
    log_info "Creating test database..."
    
    run_sql "DROP DATABASE IF EXISTS ${TEST_DB};"
    result=$(run_sql "CREATE DATABASE ${TEST_DB};")
    
    if [ $? -eq 0 ]; then
        test_pass "Test database created"
        return 0
    else
        test_fail "Failed to create test database: $result"
        return 1
    fi
}

test_basic_table() {
    log_info "Testing basic table operations..."
    
    # Create table
    result=$(run_sql_db "CREATE TABLE test_basic (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100),
        value INT
    ) ENGINE=TIDESQL;")
    
    if echo "$result" | grep -q "ERROR"; then
        test_fail "Failed to create table: $result"
        return 1
    fi
    test_pass "CREATE TABLE with AUTO_INCREMENT"
    
    # Insert data
    result=$(run_sql_db "INSERT INTO test_basic (name, value) VALUES ('test1', 100), ('test2', 200), ('test3', 300);")
    if echo "$result" | grep -q "ERROR"; then
        test_fail "Failed to insert data: $result"
        return 1
    fi
    test_pass "INSERT multiple rows"
    
    # Select data
    result=$(run_sql_db "SELECT COUNT(*) as cnt FROM test_basic;")
    if echo "$result" | grep -q "3"; then
        test_pass "SELECT COUNT returns correct value"
    else
        test_fail "SELECT COUNT incorrect: $result"
    fi
    
    # Update data
    result=$(run_sql_db "UPDATE test_basic SET value = 150 WHERE name = 'test1';")
    if echo "$result" | grep -q "ERROR"; then
        test_fail "Failed to update data: $result"
        return 1
    fi
    test_pass "UPDATE row"
    
    # Verify update
    result=$(run_sql_db "SELECT value FROM test_basic WHERE name = 'test1';")
    if echo "$result" | grep -q "150"; then
        test_pass "UPDATE verified"
    else
        test_fail "UPDATE verification failed: $result"
    fi
    
    # Delete data
    result=$(run_sql_db "DELETE FROM test_basic WHERE name = 'test2';")
    if echo "$result" | grep -q "ERROR"; then
        test_fail "Failed to delete data: $result"
        return 1
    fi
    test_pass "DELETE row"
    
    # Verify delete
    result=$(run_sql_db "SELECT COUNT(*) as cnt FROM test_basic;")
    if echo "$result" | grep -q "2"; then
        test_pass "DELETE verified"
    else
        test_fail "DELETE verification failed: $result"
    fi
    
    return 0
}

test_auto_increment() {
    log_info "Testing AUTO_INCREMENT functionality..."
    
    run_sql_db "DROP TABLE IF EXISTS test_autoinc;"
    run_sql_db "CREATE TABLE test_autoinc (
        id INT AUTO_INCREMENT PRIMARY KEY,
        data VARCHAR(50)
    ) ENGINE=TIDESQL;"
    
    # Insert without specifying ID
    run_sql_db "INSERT INTO test_autoinc (data) VALUES ('row1'), ('row2'), ('row3');"
    
    # Check IDs are sequential
    result=$(run_sql_db "SELECT id FROM test_autoinc ORDER BY id;")
    if echo "$result" | grep -q "1" && echo "$result" | grep -q "2" && echo "$result" | grep -q "3"; then
        test_pass "AUTO_INCREMENT generates sequential IDs"
    else
        test_fail "AUTO_INCREMENT IDs not sequential: $result"
    fi
    
    # Insert more and check continuation
    run_sql_db "INSERT INTO test_autoinc (data) VALUES ('row4');"
    result=$(run_sql_db "SELECT MAX(id) FROM test_autoinc;")
    if echo "$result" | grep -q "4"; then
        test_pass "AUTO_INCREMENT continues correctly"
    else
        test_fail "AUTO_INCREMENT continuation failed: $result"
    fi
    
    return 0
}

test_secondary_index() {
    log_info "Testing secondary index operations..."
    
    run_sql_db "DROP TABLE IF EXISTS test_index;"
    run_sql_db "CREATE TABLE test_index (
        id INT AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(100),
        status INT,
        INDEX idx_email (email),
        INDEX idx_status (status)
    ) ENGINE=TIDESQL;"
    
    # Insert test data
    run_sql_db "INSERT INTO test_index (email, status) VALUES 
        ('alice@test.com', 1),
        ('bob@test.com', 2),
        ('charlie@test.com', 1),
        ('david@test.com', 3);"
    
    # Query using index
    result=$(run_sql_db "SELECT email FROM test_index WHERE status = 1;")
    if echo "$result" | grep -q "alice" && echo "$result" | grep -q "charlie"; then
        test_pass "Secondary index query works"
    else
        test_fail "Secondary index query failed: $result"
    fi
    
    # Range query
    result=$(run_sql_db "SELECT COUNT(*) FROM test_index WHERE status >= 2;")
    if echo "$result" | grep -q "2"; then
        test_pass "Index range query works"
    else
        test_fail "Index range query failed: $result"
    fi
    
    return 0
}

test_transactions() {
    log_info "Testing transaction support..."
    
    run_sql_db "DROP TABLE IF EXISTS test_txn;"
    run_sql_db "CREATE TABLE test_txn (
        id INT PRIMARY KEY,
        balance INT
    ) ENGINE=TIDESQL;"
    
    run_sql_db "INSERT INTO test_txn VALUES (1, 1000), (2, 500);"
    
    # Test COMMIT
    run_sql_db "BEGIN;
        UPDATE test_txn SET balance = balance - 100 WHERE id = 1;
        UPDATE test_txn SET balance = balance + 100 WHERE id = 2;
        COMMIT;"
    
    result=$(run_sql_db "SELECT balance FROM test_txn WHERE id = 1;")
    if echo "$result" | grep -q "900"; then
        test_pass "Transaction COMMIT works"
    else
        test_fail "Transaction COMMIT failed: $result"
    fi
    
    # Test ROLLBACK
    run_sql_db "BEGIN;
        UPDATE test_txn SET balance = 0 WHERE id = 1;
        ROLLBACK;"
    
    result=$(run_sql_db "SELECT balance FROM test_txn WHERE id = 1;")
    if echo "$result" | grep -q "900"; then
        test_pass "Transaction ROLLBACK works"
    else
        test_fail "Transaction ROLLBACK failed: $result"
    fi
    
    return 0
}

test_fulltext_search() {
    log_info "Testing fulltext search (BM25)..."
    
    run_sql_db "DROP TABLE IF EXISTS test_fulltext;"
    run_sql_db "CREATE TABLE test_fulltext (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(200),
        content TEXT,
        FULLTEXT(title, content)
    ) ENGINE=TIDESQL;"
    
    # Insert test documents
    run_sql_db "INSERT INTO test_fulltext (title, content) VALUES 
        ('Database Performance', 'TidesDB is a high performance LSM-tree database engine'),
        ('MySQL Storage', 'MySQL supports pluggable storage engines for different workloads'),
        ('Web Development', 'Building modern web applications with JavaScript and React'),
        ('Database Indexing', 'Proper indexing is crucial for database query performance');"
    
    # Search for 'database'
    result=$(run_sql_db "SELECT title FROM test_fulltext WHERE MATCH(title, content) AGAINST('database');")
    if echo "$result" | grep -q "Database"; then
        test_pass "Fulltext search returns results"
    else
        test_fail "Fulltext search failed: $result"
    fi
    
    # Search for 'performance'
    result=$(run_sql_db "SELECT COUNT(*) FROM test_fulltext WHERE MATCH(title, content) AGAINST('performance');")
    if echo "$result" | grep -q "2"; then
        test_pass "Fulltext search returns correct count"
    else
        test_fail "Fulltext search count incorrect: $result"
    fi
    
    return 0
}

test_table_rename() {
    log_info "Testing table RENAME..."
    
    run_sql_db "DROP TABLE IF EXISTS test_rename_old;"
    run_sql_db "DROP TABLE IF EXISTS test_rename_new;"
    run_sql_db "CREATE TABLE test_rename_old (id INT PRIMARY KEY, data VARCHAR(50)) ENGINE=TIDESQL;"
    run_sql_db "INSERT INTO test_rename_old VALUES (1, 'test data');"
    
    # Rename table
    result=$(run_sql_db "RENAME TABLE test_rename_old TO test_rename_new;")
    if echo "$result" | grep -q "ERROR"; then
        test_fail "Table RENAME failed: $result"
        return 1
    fi
    
    # Verify data in renamed table
    result=$(run_sql_db "SELECT data FROM test_rename_new WHERE id = 1;")
    if echo "$result" | grep -q "test data"; then
        test_pass "Table RENAME preserves data"
    else
        test_fail "Table RENAME data lost: $result"
    fi
    
    return 0
}

test_truncate() {
    log_info "Testing TRUNCATE TABLE..."
    
    run_sql_db "DROP TABLE IF EXISTS test_truncate;"
    run_sql_db "CREATE TABLE test_truncate (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(50)) ENGINE=TIDESQL;"
    run_sql_db "INSERT INTO test_truncate (data) VALUES ('row1'), ('row2'), ('row3');"
    
    # Truncate
    result=$(run_sql_db "TRUNCATE TABLE test_truncate;")
    if echo "$result" | grep -q "ERROR"; then
        test_fail "TRUNCATE failed: $result"
        return 1
    fi
    
    # Verify empty
    result=$(run_sql_db "SELECT COUNT(*) FROM test_truncate;")
    if echo "$result" | grep -q "0"; then
        test_pass "TRUNCATE TABLE works"
    else
        test_fail "TRUNCATE did not empty table: $result"
    fi
    
    return 0
}

test_data_types() {
    log_info "Testing various data types..."
    
    run_sql_db "DROP TABLE IF EXISTS test_types;"
    run_sql_db "CREATE TABLE test_types (
        id INT AUTO_INCREMENT PRIMARY KEY,
        col_int INT,
        col_bigint BIGINT,
        col_float FLOAT,
        col_double DOUBLE,
        col_varchar VARCHAR(255),
        col_text TEXT,
        col_blob BLOB,
        col_date DATE,
        col_datetime DATETIME,
        col_timestamp TIMESTAMP
    ) ENGINE=TIDESQL;"
    
    # Insert various types
    run_sql_db "INSERT INTO test_types (col_int, col_bigint, col_float, col_double, col_varchar, col_text, col_blob, col_date, col_datetime, col_timestamp)
        VALUES (42, 9223372036854775807, 3.14, 2.718281828, 'hello world', 'long text content', 'binary data', '2024-01-15', '2024-01-15 10:30:00', NOW());"
    
    # Verify retrieval
    result=$(run_sql_db "SELECT col_int, col_bigint, col_varchar FROM test_types WHERE id = 1;")
    if echo "$result" | grep -q "42" && echo "$result" | grep -q "9223372036854775807" && echo "$result" | grep -q "hello world"; then
        test_pass "Various data types stored and retrieved correctly"
    else
        test_fail "Data type test failed: $result"
    fi
    
    return 0
}

test_null_handling() {
    log_info "Testing NULL handling..."
    
    run_sql_db "DROP TABLE IF EXISTS test_null;"
    run_sql_db "CREATE TABLE test_null (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nullable_col VARCHAR(50),
        INDEX idx_nullable (nullable_col)
    ) ENGINE=TIDESQL;"
    
    # Insert with NULL
    run_sql_db "INSERT INTO test_null (nullable_col) VALUES ('value1'), (NULL), ('value2'), (NULL);"
    
    # Query NULL values
    result=$(run_sql_db "SELECT COUNT(*) FROM test_null WHERE nullable_col IS NULL;")
    if echo "$result" | grep -q "2"; then
        test_pass "NULL values handled correctly"
    else
        test_fail "NULL handling failed: $result"
    fi
    
    # Query NOT NULL values
    result=$(run_sql_db "SELECT COUNT(*) FROM test_null WHERE nullable_col IS NOT NULL;")
    if echo "$result" | grep -q "2"; then
        test_pass "NOT NULL query works"
    else
        test_fail "NOT NULL query failed: $result"
    fi
    
    return 0
}

test_isolation_levels() {
    log_info "Testing transaction isolation levels..."
    
    run_sql_db "DROP TABLE IF EXISTS test_isolation;"
    run_sql_db "CREATE TABLE test_isolation (
        id INT PRIMARY KEY,
        value INT
    ) ENGINE=TIDESQL;"
    run_sql_db "INSERT INTO test_isolation VALUES (1, 100);"
    
    # Test SET TRANSACTION ISOLATION LEVEL syntax
    result=$(run_sql_db "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
    if echo "$result" | grep -q "ERROR"; then
        test_fail "SET TRANSACTION ISOLATION LEVEL READ COMMITTED failed: $result"
    else
        test_pass "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    fi
    
    result=$(run_sql_db "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
    if echo "$result" | grep -q "ERROR"; then
        test_fail "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ failed: $result"
    else
        test_pass "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    fi
    
    result=$(run_sql_db "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
    if echo "$result" | grep -q "ERROR"; then
        test_fail "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE failed: $result"
    else
        test_pass "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
    fi
    
    result=$(run_sql_db "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
    if echo "$result" | grep -q "ERROR"; then
        test_fail "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED failed: $result"
    else
        test_pass "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
    fi
    
    # Test SET SESSION TRANSACTION ISOLATION LEVEL
    result=$(run_sql_db "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
    if echo "$result" | grep -q "ERROR"; then
        test_fail "SET SESSION TRANSACTION ISOLATION LEVEL failed: $result"
    else
        test_pass "SET SESSION TRANSACTION ISOLATION LEVEL"
    fi
    
    # Test START TRANSACTION WITH CONSISTENT SNAPSHOT
    result=$(run_sql_db "START TRANSACTION WITH CONSISTENT SNAPSHOT;
        SELECT value FROM test_isolation WHERE id = 1;
        COMMIT;")
    if echo "$result" | grep -q "100"; then
        test_pass "START TRANSACTION WITH CONSISTENT SNAPSHOT"
    else
        test_fail "START TRANSACTION WITH CONSISTENT SNAPSHOT failed: $result"
    fi
    
    # Test transaction with explicit isolation level
    result=$(run_sql_db "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
        BEGIN;
        SELECT value FROM test_isolation WHERE id = 1;
        COMMIT;")
    if echo "$result" | grep -q "100"; then
        test_pass "Transaction with REPEATABLE READ isolation"
    else
        test_fail "Transaction with REPEATABLE READ failed: $result"
    fi
    
    # Test viewing current isolation level
    result=$(run_sql_db "SELECT @@tx_isolation;")
    if echo "$result" | grep -qi "READ\|REPEATABLE\|SERIALIZABLE"; then
        test_pass "Can query @@tx_isolation"
    else
        # MariaDB 10.5+ uses transaction_isolation
        result=$(run_sql_db "SELECT @@transaction_isolation;")
        if echo "$result" | grep -qi "READ\|REPEATABLE\|SERIALIZABLE"; then
            test_pass "Can query @@transaction_isolation"
        else
            test_fail "Cannot query isolation level: $result"
        fi
    fi
    
    return 0
}

cleanup() {
    log_info "Cleaning up..."
    run_sql "DROP DATABASE IF EXISTS ${TEST_DB};" || true
}

main() {
    echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
    echo "░░  TideSQL Storage Engine Test Suite   ░░"
    echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
    echo ""
    
    # Run tests
    test_plugin_install || exit 1
    test_create_database || exit 1
    
    test_basic_table
    test_auto_increment
    test_secondary_index
    test_transactions
    test_isolation_levels
    test_fulltext_search
    test_table_rename
    test_truncate
    test_data_types
    test_null_handling
    
    # Cleanup
    cleanup
    
    # Summary
    echo ""
    echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
    echo "░░  Test Summary                      ░░"
    echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
    echo -e "  ${GREEN}Passed:${NC} $TESTS_PASSED"
    echo -e "  ${RED}Failed:${NC} $TESTS_FAILED"
    echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
    
    if [ $TESTS_FAILED -gt 0 ]; then
        exit 1
    fi
    exit 0
}

# Handle cleanup on exit
trap cleanup EXIT

main "$@"
