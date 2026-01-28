#!/bin/bash
# TidesDB and InnoDB Concurrent Benchmark Script
# Tests concurrent read/write workloads with multiple parallel connections

echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo "TidesDB Concurrent Benchmark Configuration"
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo ""

# Prompt for MySQL binary path
echo "Enter path to MySQL/MariaDB client binary"
echo "  (e.g., /usr/bin/mysql, /usr/local/mysql/bin/mysql, or for MariaDB build: /path/to/server-mariadb/build/client/mariadb)"
read -p "MYSQL_BIN: " MYSQL_BIN
if [ -z "$MYSQL_BIN" ]; then
    echo "Error: MYSQL_BIN is required"
    exit 1
fi

# Prompt for socket path
echo ""
echo "Enter path to MySQL/MariaDB socket"
echo "  (e.g., /var/run/mysqld/mysqld.sock, /tmp/mysql.sock, or for MTR: /path/to/build/mysql-test/var/tmp/mysqld.1.sock)"
read -p "SOCKET: " SOCKET
if [ -z "$SOCKET" ]; then
    echo "Error: SOCKET is required"
    exit 1
fi

# Prompt for MySQL user
echo ""
echo "Enter MySQL/MariaDB user"
echo "  (e.g., root, mysql, or your database user)"
read -p "MYSQL_USER: " MYSQL_USER
if [ -z "$MYSQL_USER" ]; then
    echo "Error: MYSQL_USER is required"
    exit 1
fi

echo ""
ROW_COUNT="${ROW_COUNT:-10000}"
THREADS="${THREADS:-4}"
OUTPUT_FILE="${OUTPUT_FILE:-benchmark_concurrent_results.csv}"

# Check if server is running
if ! $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" -e "SELECT 1" &>/dev/null; then
    echo "Error: MariaDB server not running. Start it first with:"
    echo "  cd build && ./mysql-test/mtr --start-and-exit --mysqld=--plugin-load-add=ha_tidesdb.so --mysqld=--innodb=ON"
    exit 1
fi

run_sql() {
    $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" -N -e "$1" test 2>/dev/null
}

run_sql_file() {
    $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" test < "$1" 2>/dev/null
}

# Generate SQL files for a specific thread range
generate_inserts_range() {
    local table=$1
    local start=$2
    local end=$3
    local file=$4
    > "$file"
    for ((i=start; i<=end; i++)); do
        echo "INSERT INTO $table VALUES ($i, 'value_$i', $((i % 1000)), NOW());" >> "$file"
    done
}

generate_selects_range() {
    local table=$1
    local start=$2
    local end=$3
    local file=$4
    > "$file"
    for ((i=start; i<=end; i++)); do
        echo "SELECT * FROM $table WHERE id = $i;" >> "$file"
    done
}

generate_updates_range() {
    local table=$1
    local start=$2
    local end=$3
    local file=$4
    > "$file"
    for ((i=start; i<=end; i++)); do
        echo "UPDATE $table SET val1 = 'updated_$i', val2 = $((i % 500)) WHERE id = $i;" >> "$file"
    done
}

generate_mixed_workload() {
    local table=$1
    local start=$2
    local end=$3
    local file=$4
    > "$file"
    for ((i=start; i<=end; i++)); do
        # 70% reads, 20% updates, 10% inserts (to new range)
        local r=$((RANDOM % 100))
        if [ $r -lt 70 ]; then
            echo "SELECT * FROM $table WHERE id = $i;" >> "$file"
        elif [ $r -lt 90 ]; then
            echo "UPDATE $table SET val2 = $((RANDOM % 1000)) WHERE id = $i;" >> "$file"
        else
            local new_id=$((i + ROW_COUNT * 10))
            echo "INSERT IGNORE INTO $table VALUES ($new_id, 'new_$new_id', $((RANDOM % 1000)), NOW());" >> "$file"
        fi
    done
}

echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo "TidesDB vs InnoDB Concurrent Benchmark"
echo "Row count: $ROW_COUNT"
echo "Threads: $THREADS"
echo "Output: $OUTPUT_FILE"
echo ""
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"


# CSV Header
echo "engine,operation,row_count,threads,duration_ms,total_ops_per_sec,ops_per_thread" > "$OUTPUT_FILE"

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

ROWS_PER_THREAD=$((ROW_COUNT / THREADS))

echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo "Running InnoDB concurrent benchmarks..."
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"

# Setup - pre-populate table
run_sql "DROP TABLE IF EXISTS bench_innodb"
run_sql "CREATE TABLE bench_innodb (id INT PRIMARY KEY, val1 VARCHAR(100), val2 INT, val3 DATETIME, INDEX idx_val2 (val2)) ENGINE=InnoDB"

echo "  Populating InnoDB table with $ROW_COUNT rows..."
{
    echo "START TRANSACTION;"
    for ((i=1; i<=ROW_COUNT; i++)); do
        echo "INSERT INTO bench_innodb VALUES ($i, 'value_$i', $((i % 1000)), NOW());"
    done
    echo "COMMIT;"
} > "$TMPDIR/populate.sql"
run_sql_file "$TMPDIR/populate.sql"

# Concurrent Point SELECTs
echo "  InnoDB: Concurrent Point SELECT ($THREADS threads)..."
for ((t=0; t<THREADS; t++)); do
    start_row=$((t * ROWS_PER_THREAD + 1))
    end_row=$(((t + 1) * ROWS_PER_THREAD))
    generate_selects_range "bench_innodb" "$start_row" "$end_row" "$TMPDIR/select_$t.sql"
done

start_time=$(date +%s%N)
for ((t=0; t<THREADS; t++)); do
    run_sql_file "$TMPDIR/select_$t.sql" > /dev/null &
done
wait
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
total_ops=$ROW_COUNT
ops_per_sec=$(echo "scale=2; $total_ops / (($end_time - $start_time) / 1000000000)" | bc)
ops_per_thread=$(echo "scale=2; $ops_per_sec / $THREADS" | bc)
echo "InnoDB,Concurrent SELECT,$ROW_COUNT,$THREADS,$duration_ms,$ops_per_sec,$ops_per_thread" >> "$OUTPUT_FILE"
echo "    $ops_per_sec total ops/sec ($ops_per_thread per thread)"

# Concurrent UPDATEs (non-overlapping ranges)
echo "  InnoDB: Concurrent UPDATE ($THREADS threads, non-overlapping)..."
for ((t=0; t<THREADS; t++)); do
    start_row=$((t * ROWS_PER_THREAD + 1))
    end_row=$(((t + 1) * ROWS_PER_THREAD))
    generate_updates_range "bench_innodb" "$start_row" "$end_row" "$TMPDIR/update_$t.sql"
done

start_time=$(date +%s%N)
for ((t=0; t<THREADS; t++)); do
    run_sql_file "$TMPDIR/update_$t.sql" &
done
wait
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
ops_per_thread=$(echo "scale=2; $ops_per_sec / $THREADS" | bc)
echo "InnoDB,Concurrent UPDATE,$ROW_COUNT,$THREADS,$duration_ms,$ops_per_sec,$ops_per_thread" >> "$OUTPUT_FILE"
echo "    $ops_per_sec total ops/sec ($ops_per_thread per thread)"

# Concurrent INSERTs (non-overlapping ranges)
run_sql "DROP TABLE IF EXISTS bench_innodb"
run_sql "CREATE TABLE bench_innodb (id INT PRIMARY KEY, val1 VARCHAR(100), val2 INT, val3 DATETIME, INDEX idx_val2 (val2)) ENGINE=InnoDB"

echo "  InnoDB: Concurrent INSERT ($THREADS threads, non-overlapping)..."
for ((t=0; t<THREADS; t++)); do
    start_row=$((t * ROWS_PER_THREAD + 1))
    end_row=$(((t + 1) * ROWS_PER_THREAD))
    generate_inserts_range "bench_innodb" "$start_row" "$end_row" "$TMPDIR/insert_$t.sql"
done

start_time=$(date +%s%N)
for ((t=0; t<THREADS; t++)); do
    run_sql_file "$TMPDIR/insert_$t.sql" &
done
wait
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
ops_per_thread=$(echo "scale=2; $ops_per_sec / $THREADS" | bc)
echo "InnoDB,Concurrent INSERT,$ROW_COUNT,$THREADS,$duration_ms,$ops_per_sec,$ops_per_thread" >> "$OUTPUT_FILE"
echo "    $ops_per_sec total ops/sec ($ops_per_thread per thread)"

# Mixed workload (read-heavy OLTP simulation)
echo "  InnoDB: Mixed Workload (70% read, 20% update, 10% insert)..."
for ((t=0; t<THREADS; t++)); do
    start_row=$((t * ROWS_PER_THREAD + 1))
    end_row=$(((t + 1) * ROWS_PER_THREAD))
    generate_mixed_workload "bench_innodb" "$start_row" "$end_row" "$TMPDIR/mixed_$t.sql"
done

start_time=$(date +%s%N)
for ((t=0; t<THREADS; t++)); do
    run_sql_file "$TMPDIR/mixed_$t.sql" > /dev/null &
done
wait
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
ops_per_thread=$(echo "scale=2; $ops_per_sec / $THREADS" | bc)
echo "InnoDB,Mixed Workload,$ROW_COUNT,$THREADS,$duration_ms,$ops_per_sec,$ops_per_thread" >> "$OUTPUT_FILE"
echo "    $ops_per_sec total ops/sec ($ops_per_thread per thread)"

run_sql "DROP TABLE IF EXISTS bench_innodb"

echo ""
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo "Running TidesDB concurrent benchmarks..."
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"

# Setup - pre-populate table
run_sql "DROP TABLE IF EXISTS bench_tidesdb"
run_sql "CREATE TABLE bench_tidesdb (id INT PRIMARY KEY, val1 VARCHAR(100), val2 INT, val3 DATETIME, INDEX idx_val2 (val2)) ENGINE=TidesDB"

echo "  Populating TidesDB table with $ROW_COUNT rows..."
{
    echo "START TRANSACTION;"
    for ((i=1; i<=ROW_COUNT; i++)); do
        echo "INSERT INTO bench_tidesdb VALUES ($i, 'value_$i', $((i % 1000)), NOW());"
    done
    echo "COMMIT;"
} > "$TMPDIR/populate.sql"
run_sql_file "$TMPDIR/populate.sql"

# Concurrent Point SELECTs
echo "  TidesDB: Concurrent Point SELECT ($THREADS threads)..."
for ((t=0; t<THREADS; t++)); do
    start_row=$((t * ROWS_PER_THREAD + 1))
    end_row=$(((t + 1) * ROWS_PER_THREAD))
    generate_selects_range "bench_tidesdb" "$start_row" "$end_row" "$TMPDIR/select_$t.sql"
done

start_time=$(date +%s%N)
for ((t=0; t<THREADS; t++)); do
    run_sql_file "$TMPDIR/select_$t.sql" > /dev/null &
done
wait
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
ops_per_thread=$(echo "scale=2; $ops_per_sec / $THREADS" | bc)
echo "TidesDB,Concurrent SELECT,$ROW_COUNT,$THREADS,$duration_ms,$ops_per_sec,$ops_per_thread" >> "$OUTPUT_FILE"
echo "    $ops_per_sec total ops/sec ($ops_per_thread per thread)"

# Concurrent UPDATEs (non-overlapping ranges)
echo "  TidesDB: Concurrent UPDATE ($THREADS threads, non-overlapping)..."
for ((t=0; t<THREADS; t++)); do
    start_row=$((t * ROWS_PER_THREAD + 1))
    end_row=$(((t + 1) * ROWS_PER_THREAD))
    generate_updates_range "bench_tidesdb" "$start_row" "$end_row" "$TMPDIR/update_$t.sql"
done

start_time=$(date +%s%N)
for ((t=0; t<THREADS; t++)); do
    run_sql_file "$TMPDIR/update_$t.sql" &
done
wait
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
ops_per_thread=$(echo "scale=2; $ops_per_sec / $THREADS" | bc)
echo "TidesDB,Concurrent UPDATE,$ROW_COUNT,$THREADS,$duration_ms,$ops_per_sec,$ops_per_thread" >> "$OUTPUT_FILE"
echo "    $ops_per_sec total ops/sec ($ops_per_thread per thread)"

# Concurrent INSERTs (non-overlapping ranges)
run_sql "DROP TABLE IF EXISTS bench_tidesdb"
run_sql "CREATE TABLE bench_tidesdb (id INT PRIMARY KEY, val1 VARCHAR(100), val2 INT, val3 DATETIME, INDEX idx_val2 (val2)) ENGINE=TidesDB"

echo "  TidesDB: Concurrent INSERT ($THREADS threads, non-overlapping)..."
for ((t=0; t<THREADS; t++)); do
    start_row=$((t * ROWS_PER_THREAD + 1))
    end_row=$(((t + 1) * ROWS_PER_THREAD))
    generate_inserts_range "bench_tidesdb" "$start_row" "$end_row" "$TMPDIR/insert_$t.sql"
done

start_time=$(date +%s%N)
for ((t=0; t<THREADS; t++)); do
    run_sql_file "$TMPDIR/insert_$t.sql" &
done
wait
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
ops_per_thread=$(echo "scale=2; $ops_per_sec / $THREADS" | bc)
echo "TidesDB,Concurrent INSERT,$ROW_COUNT,$THREADS,$duration_ms,$ops_per_sec,$ops_per_thread" >> "$OUTPUT_FILE"
echo "    $ops_per_sec total ops/sec ($ops_per_thread per thread)"

# Mixed workload (read-heavy OLTP simulation)
echo "  TidesDB: Mixed Workload (70% read, 20% update, 10% insert)..."
for ((t=0; t<THREADS; t++)); do
    start_row=$((t * ROWS_PER_THREAD + 1))
    end_row=$(((t + 1) * ROWS_PER_THREAD))
    generate_mixed_workload "bench_tidesdb" "$start_row" "$end_row" "$TMPDIR/mixed_$t.sql"
done

start_time=$(date +%s%N)
for ((t=0; t<THREADS; t++)); do
    run_sql_file "$TMPDIR/mixed_$t.sql" > /dev/null &
done
wait
end_time=$(date +%s%N)
duration_ms=$(echo "scale=2; ($end_time - $start_time) / 1000000" | bc)
ops_per_sec=$(echo "scale=2; $ROW_COUNT / (($end_time - $start_time) / 1000000000)" | bc)
ops_per_thread=$(echo "scale=2; $ops_per_sec / $THREADS" | bc)
echo "TidesDB,Mixed Workload,$ROW_COUNT,$THREADS,$duration_ms,$ops_per_sec,$ops_per_thread" >> "$OUTPUT_FILE"
echo "    $ops_per_sec total ops/sec ($ops_per_thread per thread)"

run_sql "DROP TABLE IF EXISTS bench_tidesdb"

echo ""
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo "Concurrent benchmark complete! Results saved to $OUTPUT_FILE"
echo ""
echo "Results:"
cat "$OUTPUT_FILE"
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
