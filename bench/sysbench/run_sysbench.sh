#!/bin/bash
#
# Sysbench benchmark wrapper for TidesDB vs InnoDB
# Comprehensive OLTP benchmark with multiple workloads, table sizes, and thread counts
# Outputs results to CSV for easy comparison and analysis
#
# Usage:
#   # Quick test
#   ./run_sysbench.sh
#
#   # Full benchmark
#   TABLE_SIZES="100000 1000000" \
#   THREAD_COUNTS="1 4 8 16" \
#   TIME=300 \
#   WARMUP=30 \
#   ./run_sysbench.sh
#
# Environment variables:
#   DATA_DIR              -- Base data directory for MariaDB (will start server automatically)
#   INNODB_DATA_DIR       -- Custom InnoDB data/tablespace directory (for fast disk)
#   TIDESDB_DATA_DIR      -- Custom TidesDB data directory (for fast disk)
#   TIDESDB_USE_BTREE     -- Use B+tree SSTable format (default: ON, set OFF for block layout)
#   SOCKET                -- MySQL socket path (default: MTR socket or DATA_DIR/mysqld.sock)
#   TABLE_SIZES           -- Space-separated list of table sizes (default: "10000")
#   THREAD_COUNTS         -- Space-separated list of thread counts (default: "1")
#   TIME                  -- Benchmark duration in seconds (default: 60)
#   WARMUP                -- Warmup duration in seconds before measurement (default: 10)
#   ENGINES               -- Space-separated list of engines to test (default: "InnoDB TidesDB")
#   WORKLOADS             -- Space-separated list of workloads (default: all OLTP workloads)
#
# Available workloads:
#   oltp_read_only        -- Read-only transactions (point selects + range scans)
#   oltp_write_only       -- Write-only transactions (inserts, updates, deletes)
#   oltp_read_write       -- Mixed read-write transactions
#   oltp_point_select     -- Pure point lookups (tests bloom filters)
#   oltp_insert           -- Pure inserts (LSM-tree strength)
#   oltp_update_index     -- Updates on indexed columns
#   oltp_update_non_index -- Updates on non-indexed columns
#   oltp_delete           -- Delete operations
#   select_random_ranges  -- Range scans (tests LSM merge overhead)
#
# Example with custom I/O directories on fast NVMe:
#   INNODB_DATA_DIR=/mnt/nvme/innodb TIDESDB_DATA_DIR=/mnt/nvme/tidesdb ./run_sysbench.sh
#

# Don't exit on error - handle errors explicitly
set +e

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-/home/agpmastersystem/server-mariadb-12.1.2/build}"

# MariaDB client path
MYSQL_BIN="${MYSQL_BIN:-${BUILD_DIR}/client/mariadb}"

# Configuration
DATA_DIR="${DATA_DIR:-}"
if [ -n "$DATA_DIR" ]; then
    SOCKET="${SOCKET:-${DATA_DIR}/mysqld.sock}"
else
    SOCKET="${SOCKET:-${BUILD_DIR}/mysql-test/var/tmp/mysqld.1.sock}"
fi
USER="${MYSQL_USER:-root}"
DB="${MYSQL_DB:-test}"

# Support multiple table sizes and thread counts
TABLE_SIZES="${TABLE_SIZES:-10000}"
THREAD_COUNTS="${THREAD_COUNTS:-1}"
TIME="${TIME:-60}"
WARMUP="${WARMUP:-10}"
REPORT_INTERVAL="${REPORT_INTERVAL:-10}"
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/sysbench_results}"

# Default workloads - comprehensive OLTP coverage
DEFAULT_WORKLOADS="oltp_point_select oltp_read_only oltp_write_only oltp_read_write oltp_insert oltp_update_index oltp_update_non_index oltp_delete"
WORKLOADS="${WORKLOADS:-$DEFAULT_WORKLOADS}"

mkdir -p "$OUTPUT_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY_CSV="$OUTPUT_DIR/summary_${TIMESTAMP}.csv"
DETAIL_CSV="$OUTPUT_DIR/detail_${TIMESTAMP}.csv"
LATENCY_CSV="$OUTPUT_DIR/latency_${TIMESTAMP}.csv"

# Extended CSV headers with more metrics
echo "engine,workload,threads,table_size,tps,qps,reads_per_sec,writes_per_sec,latency_avg_ms,latency_min_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,latency_max_ms,errors,reconnects,total_time_s,warmup_s" > "$SUMMARY_CSV"
echo "engine,workload,threads,table_size,time_s,tps,qps,latency_avg_ms,latency_p95_ms" > "$DETAIL_CSV"
echo "engine,workload,threads,table_size,percentile,latency_ms" > "$LATENCY_CSV"

run_sysbench_test() {
    local engine=$1
    local test=$2
    local threads=$3
    local table_size=$4

    # Extract just the test name for display and filenames
    local test_name=$(basename "$test" .lua)

    echo ""
    echo "═══════════════════════════════════════════════════════════════════"
    echo "  $engine - $test_name (threads=$threads, table_size=$table_size)"
    echo "═══════════════════════════════════════════════════════════════════"

    # Cleanup
    $MYSQL_BIN -S "$SOCKET" -u "$USER" -e "DROP TABLE IF EXISTS sbtest1" "$DB" 2>/dev/null || true

    # Prepare
    echo "▶ Preparing table with $table_size rows..."
    if ! sysbench "$test" \
        --mysql-socket="$SOCKET" \
        --mysql-user="$USER" \
        --mysql-db="$DB" \
        --tables=1 \
        --table-size="$table_size" \
        --threads=1 \
        --mysql-storage-engine="$engine" \
        prepare 2>&1; then
        echo "  ✗ Prepare failed for $engine $test_name"
        return 1
    fi

    # Warmup phase (if configured)
    if [ "$WARMUP" -gt 0 ]; then
        echo "▶ Warming up for ${WARMUP}s..."
        sysbench "$test" \
            --mysql-socket="$SOCKET" \
            --mysql-user="$USER" \
            --mysql-db="$DB" \
            --tables=1 \
            --table-size="$table_size" \
            --threads="$threads" \
            --time="$WARMUP" \
            --mysql-storage-engine="$engine" \
            --mysql-ignore-errors=1213,1020,1205,1180 \
            run > /dev/null 2>&1 || true
    fi

    # Run and capture output
    echo "▶ Running benchmark for ${TIME}s with $threads threads..."
    local output_file="$OUTPUT_DIR/${engine}_${test_name}_t${threads}_s${table_size}_${TIMESTAMP}.txt"

    sysbench "$test" \
        --mysql-socket="$SOCKET" \
        --mysql-user="$USER" \
        --mysql-db="$DB" \
        --tables=1 \
        --table-size="$table_size" \
        --threads="$threads" \
        --time="$TIME" \
        --mysql-storage-engine="$engine" \
        --report-interval="$REPORT_INTERVAL" \
        --histogram=on \
        --percentile=99 \
        --mysql-ignore-errors=1213,1020,1205,1180 \
        run 2>&1 | tee "$output_file"

    # Parse results - extended metrics
    local tps=$(grep "transactions:" "$output_file" | awk '{print $2}' | sed 's/(//g')
    local qps=$(grep "queries:" "$output_file" | head -1 | awk '{print $2}' | sed 's/(//g')
    local reads=$(grep "read:" "$output_file" | head -1 | awk '{print $2}')
    local writes=$(grep "write:" "$output_file" | head -1 | awk '{print $2}')
    local lat_avg=$(grep "avg:" "$output_file" | tail -1 | awk '{print $2}')
    local lat_min=$(grep "min:" "$output_file" | tail -1 | awk '{print $2}')
    local lat_p50=$(grep "50th percentile:" "$output_file" | awk '{print $3}' || echo "0")
    local lat_p95=$(grep "95th percentile:" "$output_file" | awk '{print $3}')
    local lat_p99=$(grep "99th percentile:" "$output_file" | awk '{print $3}' || echo "0")
    local lat_max=$(grep "max:" "$output_file" | tail -1 | awk '{print $2}')
    local errors=$(grep "errors:" "$output_file" | head -1 | awk '{print $2}' || echo "0")
    local reconnects=$(grep "reconnects:" "$output_file" | head -1 | awk '{print $2}' || echo "0")
    local total_time=$(grep "total time:" "$output_file" | awk '{print $3}' | sed 's/s//g')

    # Calculate reads/writes per second
    local reads_per_sec=$(echo "scale=2; ${reads:-0} / ${total_time:-1}" | bc 2>/dev/null || echo "0")
    local writes_per_sec=$(echo "scale=2; ${writes:-0} / ${total_time:-1}" | bc 2>/dev/null || echo "0")

    # Write to summary CSV with extended metrics
    echo "$engine,$test_name,$threads,$table_size,$tps,$qps,$reads_per_sec,$writes_per_sec,$lat_avg,$lat_min,$lat_p50,$lat_p95,$lat_p99,$lat_max,$errors,$reconnects,$total_time,$WARMUP" >> "$SUMMARY_CSV"

    # Parse interval reports for detail CSV
    grep "thds:" "$output_file" | while read line; do
        local time_s=$(echo "$line" | awk -F'[\\[\\]]' '{print $2}' | sed 's/s//g')
        local int_tps=$(echo "$line" | awk '{print $5}')
        local int_qps=$(echo "$line" | awk '{print $7}')
        local int_lat=$(echo "$line" | awk '{print $9}')
        local int_p95=$(echo "$line" | awk '{print $11}' 2>/dev/null || echo "0")
        echo "$engine,$test_name,$threads,$table_size,$time_s,$int_tps,$int_qps,$int_lat,$int_p95" >> "$DETAIL_CSV"
    done

    # Parse histogram for latency distribution CSV
    if grep -q "Latency histogram" "$output_file"; then
        grep -A 100 "Latency histogram" "$output_file" | grep "ms$" | while read line; do
            local pct=$(echo "$line" | awk '{print $1}')
            local lat=$(echo "$line" | awk '{print $2}' | sed 's/ms//g')
            echo "$engine,$test_name,$threads,$table_size,$pct,$lat" >> "$LATENCY_CSV"
        done
    fi

    echo ""
    echo "✓ $engine $test_name: TPS=$tps, QPS=$qps, Latency avg=${lat_avg}ms p95=${lat_p95}ms p99=${lat_p99}ms"

    # Cleanup
    sysbench "$test" \
        --mysql-socket="$SOCKET" \
        --mysql-user="$USER" \
        --mysql-db="$DB" \
        --tables=1 \
        cleanup > /dev/null 2>&1 || true
}

# If DATA_DIR is specified, start the server automatically
SERVER_STARTED=0
if [ -n "$DATA_DIR" ]; then
    # Custom I/O directories for each engine (allows placing on different disks)
    # TIDESDB_DATA_DIR: Where TidesDB stores its LSM-tree data (SSTables, WAL, etc.)
    # INNODB_DATA_DIR: Where InnoDB stores its tablespace files
    TIDESDB_DIR="${TIDESDB_DATA_DIR:-${DATA_DIR}/tidesdb}"
    INNODB_DIR="${INNODB_DATA_DIR:-${DATA_DIR}}"

    PID_FILE="${DATA_DIR}/mysqld.pid"
    ERROR_LOG="${DATA_DIR}/mysqld.err"
    MYSQLD="${BUILD_DIR}/sql/mariadbd"

    # Check if server is already running
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "Server already running (PID: $(cat $PID_FILE))"
    else
        # Create directories
        mkdir -p "$DATA_DIR" "$TIDESDB_DIR" "$INNODB_DIR"

        # Initialize data directory if needed
        if [ ! -d "$DATA_DIR/mysql" ]; then
            echo "Initializing data directory: $DATA_DIR"
            "${BUILD_DIR}/scripts/mariadb-install-db" \
                --basedir="${BUILD_DIR}" \
                --datadir="$DATA_DIR" \
                --user=$(whoami) 2>&1 | tail -3
        fi

        echo "Starting MariaDB server with custom data directories..."
        echo "  MariaDB data:  $DATA_DIR"
        echo "  InnoDB data:   $INNODB_DIR"
        echo "  TidesDB data:  $TIDESDB_DIR"

        "$MYSQLD" \
            --no-defaults \
            --basedir="${BUILD_DIR}" \
            --datadir="$DATA_DIR" \
            --socket="$SOCKET" \
            --pid-file="$PID_FILE" \
            --log-error="$ERROR_LOG" \
            --plugin-dir="${BUILD_DIR}/storage/tidesdb" \
            --plugin-maturity=alpha \
            --plugin-load-add=ha_tidesdb.so \
            --loose-tidesdb_data_dir="$TIDESDB_DIR" \
            --loose-tidesdb_use_btree="${TIDESDB_USE_BTREE:-ON}" \
            --innodb=ON \
            --innodb-data-home-dir="$INNODB_DIR" \
            --innodb-log-group-home-dir="$INNODB_DIR" \
            --innodb-buffer-pool-size=256M \
            --innodb-log-file-size=64M \
            --innodb-flush-log-at-trx-commit=1 \
            --skip-grant-tables \
            --skip-networking \
            &

        # Wait for server to start
        echo -n "Waiting for server"
        for i in {1..30}; do
            if "$MYSQL_BIN" -S "$SOCKET" -u root -e "SELECT 1" >/dev/null 2>&1; then
                echo " OK"
                SERVER_STARTED=1
                break
            fi
            echo -n "."
            sleep 1
        done

        if [ $SERVER_STARTED -eq 0 ]; then
            echo " FAILED"
            echo "Check error log: $ERROR_LOG"
            tail -20 "$ERROR_LOG"
            exit 1
        fi

        # Create test database if it doesn't exist
        echo "Creating test database..."
        "$MYSQL_BIN" -S "$SOCKET" -u root -e "CREATE DATABASE IF NOT EXISTS $DB" 2>/dev/null || true
    fi
fi

# Ensure test database exists (even if server was already running)
"$MYSQL_BIN" -S "$SOCKET" -u root -e "CREATE DATABASE IF NOT EXISTS $DB" 2>/dev/null || true

# Count total tests for progress
ENGINES="${ENGINES:-InnoDB TidesDB}"
num_engines=$(echo $ENGINES | wc -w)
num_sizes=$(echo $TABLE_SIZES | wc -w)
num_threads=$(echo $THREAD_COUNTS | wc -w)
num_workloads=$(echo $WORKLOADS | wc -w)
total_tests=$((num_engines * num_sizes * num_threads * num_workloads))

echo ""
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo "  Sysbench TidesDB vs InnoDB Comprehensive OLTP Benchmark"
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo ""
echo "Configuration:"
echo "  Socket:           $SOCKET"
echo "  Data directory:   ${DATA_DIR:-default (MTR)}"
if [ -n "$DATA_DIR" ]; then
echo "  InnoDB I/O dir:   ${INNODB_DIR:-$DATA_DIR}"
echo "  TidesDB I/O dir:  ${TIDESDB_DIR:-$DATA_DIR/tidesdb}"
fi
echo "  Table sizes:      $TABLE_SIZES"
echo "  Thread counts:    $THREAD_COUNTS"
echo "  Duration:         ${TIME}s per test"
echo "  Warmup:           ${WARMUP}s per test"
echo "  Report interval:  ${REPORT_INTERVAL}s"
echo "  Engines:          $ENGINES"
echo "  Workloads:        $WORKLOADS"
echo "  Total tests:      $total_tests"
echo "  Output directory: $OUTPUT_DIR"
echo ""

# Run tests for all combinations
current_test=0
for table_size in $TABLE_SIZES; do
    for threads in $THREAD_COUNTS; do
        for engine in $ENGINES; do
            for workload in $WORKLOADS; do
                current_test=$((current_test + 1))
                echo ""
                echo "▓▓▓ Test $current_test of $total_tests ▓▓▓"

                # Check for batched version first (reduces conflicts for MVCC)
                batched_script="$SCRIPT_DIR/${workload}_batched.lua"
                if [ -f "$batched_script" ]; then
                    run_sysbench_test "$engine" "$batched_script" "$threads" "$table_size" || echo "  (skipping due to error)"
                else
                    run_sysbench_test "$engine" "$workload" "$threads" "$table_size" || echo "  (skipping due to error)"
                fi
            done
        done
    done
done

echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo "  Benchmark Complete - Summary"
echo "═══════════════════════════════════════════════════════════════════════"
echo ""
echo "Tests completed: $current_test of $total_tests"
echo ""

# Show summary table (first 20 lines to avoid overwhelming output)
echo "Top results (sorted by TPS):"
head -1 "$SUMMARY_CSV"
tail -n +2 "$SUMMARY_CSV" | sort -t',' -k5 -rn | head -20
echo ""

echo "Results saved to:"
echo "  Summary:     $SUMMARY_CSV"
echo "  Detail:      $DETAIL_CSV"
echo "  Latency:     $LATENCY_CSV"
echo "  Raw output:  $OUTPUT_DIR/*.txt"
echo ""
echo "To analyze results:"
echo "  # Compare engines by workload"
echo "  cat $SUMMARY_CSV | column -t -s','"
echo ""
echo "  # Plot with gnuplot or import to spreadsheet for charts"
