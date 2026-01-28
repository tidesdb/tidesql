#!/bin/bash

MYSQL_BIN="${MYSQL_BIN:-/home/agpmastersystem/server-mariadb-12.1.2/build/client/mariadb}"
SOCKET="${SOCKET:-/home/agpmastersystem/server-mariadb-12.1.2/build/mysql-test/var/tmp/mysqld.1.sock}"
MYSQL_USER="${MYSQL_USER:-root}"
ROW_COUNT="${ROW_COUNT:-10000}"
OUTPUT_FILE="${OUTPUT_FILE:-benchmark_results.csv}"

if ! $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" -e "SELECT 1" &>/dev/null; then
    echo "Error: MariaDB server not running. Start it first with:"
    echo "  cd build && ./mysql-test/mtr --start-and-exit --mysqld=--plugin-load-add=ha_tidesdb.so --mysqld=--innodb=ON"
    exit 1
fi

run_sql() {
    $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" -N -e "$1" test 2>/dev/null
}

# Generate SQL file for sequential operations (one statement per line)
generate_sequential_inserts() {
    local table=$1
    local count=$2
    local file=$3
    > "$file"
    for ((i=1; i<=count; i++)); do
        echo "INSERT INTO $table VALUES ($i, 'value_$i', $((i % 1000)), NOW());" >> "$file"
    done
}

generate_sequential_selects() {
    local table=$1
    local count=$2
    local file=$3
    > "$file"
    for ((i=1; i<=count; i++)); do
        echo "SELECT * FROM $table WHERE id = $i;" >> "$file"
    done
}

generate_sequential_updates() {
    local table=$1
    local count=$2
    local file=$3
    > "$file"
    for ((i=1; i<=count; i++)); do
        echo "UPDATE $table SET val1 = 'updated_$i' WHERE id = $i;" >> "$file"
    done
}

generate_sequential_deletes() {
    local table=$1
    local count=$2
    local file=$3
    > "$file"
    for ((i=1; i<=count; i++)); do
        echo "DELETE FROM $table WHERE id = $i;" >> "$file"
    done
}

# Run SQL file with timing
run_sql_file() {
    $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" test < "$1" 2>/dev/null
}

echo "TidesDB vs InnoDB Benchmark v2"
echo "Row count: $ROW_COUNT"
echo "Output: $OUTPUT_FILE"
echo ""

# CSV Header
echo "engine,operation,row_count,duration_ms,ops_per_sec" > "$OUTPUT_FILE"

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo "Running InnoDB benchmarks..."
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"

# Setup
run_sql "DROP TABLE IF EXISTS bench_innodb"
run_sql "CREATE TABLE bench_innodb (id INT PRIMARY KEY, val1 VARCHAR(100), val2 INT, val3 DATETIME, INDEX idx_val2 (val2)) ENGINE=InnoDB"

# Sequential INSERT (autocommit - each is its own transaction)
echo "  InnoDB: Sequential INSERT (autocommit)..."
generate_sequential_inserts "bench_innodb" "$ROW_COUNT" "$TMPDIR/inserts.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/inserts.sql"
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
echo "InnoDB,Sequential INSERT,$ROW_COUNT,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

# Bulk INSERT (single transaction)
run_sql "DROP TABLE IF EXISTS bench_innodb"
run_sql "CREATE TABLE bench_innodb (id INT PRIMARY KEY, val1 VARCHAR(100), val2 INT, val3 DATETIME, INDEX idx_val2 (val2)) ENGINE=InnoDB"
echo "  InnoDB: Bulk INSERT (single transaction)..."
{
    echo "START TRANSACTION;"
    for ((i=1; i<=ROW_COUNT; i++)); do
        echo "INSERT INTO bench_innodb VALUES ($i, 'value_$i', $((i % 1000)), NOW());"
    done
    echo "COMMIT;"
} > "$TMPDIR/bulk_inserts.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/bulk_inserts.sql"
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
echo "InnoDB,Bulk INSERT (txn),$ROW_COUNT,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

# Point SELECT
echo "  InnoDB: Point SELECT..."
generate_sequential_selects "bench_innodb" "$ROW_COUNT" "$TMPDIR/selects.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/selects.sql" > /dev/null
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
echo "InnoDB,Point SELECT,$ROW_COUNT,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

# Full Table Scan
echo "  InnoDB: Full Table Scan..."
{
    for ((i=1; i<=100; i++)); do
        echo "SELECT COUNT(*) FROM bench_innodb;"
    done
} > "$TMPDIR/scans.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/scans.sql" > /dev/null
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; 100 / (($end_time - $start_time) / 1000000000)" | bc)
echo "InnoDB,Full Table Scan (x100),100,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

# UPDATE
echo "  InnoDB: UPDATE..."
generate_sequential_updates "bench_innodb" "$ROW_COUNT" "$TMPDIR/updates.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/updates.sql"
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
echo "InnoDB,UPDATE,$ROW_COUNT,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

# DELETE
echo "  InnoDB: DELETE..."
generate_sequential_deletes "bench_innodb" "$ROW_COUNT" "$TMPDIR/deletes.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/deletes.sql"
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
echo "InnoDB,DELETE,$ROW_COUNT,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

run_sql "DROP TABLE IF EXISTS bench_innodb"

# ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
# TidesDB Benchmarks
# ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
echo ""
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo "Running TidesDB benchmarks..."
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"

# Setup
run_sql "DROP TABLE IF EXISTS bench_tidesdb"
run_sql "CREATE TABLE bench_tidesdb (id INT PRIMARY KEY, val1 VARCHAR(100), val2 INT, val3 DATETIME, INDEX idx_val2 (val2)) ENGINE=TidesDB"

# Sequential INSERT
echo "  TidesDB: Sequential INSERT (autocommit)..."
generate_sequential_inserts "bench_tidesdb" "$ROW_COUNT" "$TMPDIR/inserts.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/inserts.sql"
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
echo "TidesDB,Sequential INSERT,$ROW_COUNT,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

# Bulk INSERT (single transaction)
run_sql "DROP TABLE IF EXISTS bench_tidesdb"
run_sql "CREATE TABLE bench_tidesdb (id INT PRIMARY KEY, val1 VARCHAR(100), val2 INT, val3 DATETIME, INDEX idx_val2 (val2)) ENGINE=TidesDB"
echo "  TidesDB: Bulk INSERT (single transaction)..."
{
    echo "START TRANSACTION;"
    for ((i=1; i<=ROW_COUNT; i++)); do
        echo "INSERT INTO bench_tidesdb VALUES ($i, 'value_$i', $((i % 1000)), NOW());"
    done
    echo "COMMIT;"
} > "$TMPDIR/bulk_inserts.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/bulk_inserts.sql"
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
echo "TidesDB,Bulk INSERT (txn),$ROW_COUNT,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

# Point SELECT
echo "  TidesDB: Point SELECT..."
generate_sequential_selects "bench_tidesdb" "$ROW_COUNT" "$TMPDIR/selects.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/selects.sql" > /dev/null
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
echo "TidesDB,Point SELECT,$ROW_COUNT,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

# Full Table Scan
echo "  TidesDB: Full Table Scan..."
{
    for ((i=1; i<=100; i++)); do
        echo "SELECT COUNT(*) FROM bench_tidesdb;"
    done
} > "$TMPDIR/scans.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/scans.sql" > /dev/null
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; 100 / (($end_time - $start_time) / 1000000000)" | bc)
echo "TidesDB,Full Table Scan (x100),100,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

# UPDATE
echo "  TidesDB: UPDATE..."
generate_sequential_updates "bench_tidesdb" "$ROW_COUNT" "$TMPDIR/updates.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/updates.sql"
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
echo "TidesDB,UPDATE,$ROW_COUNT,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

# DELETE
echo "  TidesDB: DELETE..."
generate_sequential_deletes "bench_tidesdb" "$ROW_COUNT" "$TMPDIR/deletes.sql"
start_time=$(date +%s%N)
run_sql_file "$TMPDIR/deletes.sql"
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
echo "TidesDB,DELETE,$ROW_COUNT,$duration_ms,$ops_per_sec" >> "$OUTPUT_FILE"
echo "    $ops_per_sec ops/sec"

run_sql "DROP TABLE IF EXISTS bench_tidesdb"

echo ""
echo "Benchmark complete! Results saved to $OUTPUT_FILE"
echo ""
echo "Results:"
cat "$OUTPUT_FILE"
