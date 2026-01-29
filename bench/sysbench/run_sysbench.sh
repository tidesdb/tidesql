#!/bin/bash
#
# Sysbench benchmark wrapper for benchmarking TidesDB against InnoDB
# Outputs results to CSV for easy comparison
#
# Usage:
#   TABLE_SIZE=100000 THREADS=4 TIME=60 ./run_sysbench.sh
#
# Environment variables:
#   DATA_DIR    - Custom data directory (will start server automatically)
#   SOCKET      - MySQL socket path (default: MTR socket or DATA_DIR/mysqld.sock)
#   TABLE_SIZE  - Number of rows per table (default: 10000)
#   THREADS     - Number of concurrent threads (default: 1)
#   TIME        - Benchmark duration in seconds (default: 60)
#   ENGINES     - Space-separated list of engines to test (default: "InnoDB TidesDB")
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
TABLE_SIZE="${TABLE_SIZE:-10000}"
THREADS="${THREADS:-1}"
TIME="${TIME:-60}"
REPORT_INTERVAL="${REPORT_INTERVAL:-10}"
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/sysbench_results}"

mkdir -p "$OUTPUT_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY_CSV="$OUTPUT_DIR/summary_${TIMESTAMP}.csv"
DETAIL_CSV="$OUTPUT_DIR/detail_${TIMESTAMP}.csv"

echo "engine,test,threads,table_size,tps,qps,latency_avg_ms,latency_p95_ms,latency_max_ms,total_time_s" > "$SUMMARY_CSV"
echo "engine,test,threads,time_s,tps,qps,latency_avg_ms" > "$DETAIL_CSV"

run_sysbench_test() {
    local engine=$1
    local test=$2
    
    # Extract just the test name for display and filenames
    local test_name=$(basename "$test" .lua)
    
    echo ""
    echo "═══════════════════════════════════════════════════════════════════"
    echo "  $engine - $test_name (threads=$THREADS, table_size=$TABLE_SIZE)"
    echo "═══════════════════════════════════════════════════════════════════"
    
    # Cleanup
    $MYSQL_BIN -S "$SOCKET" -u "$USER" -e "DROP TABLE IF EXISTS sbtest1" "$DB" 2>/dev/null || true
    
    # Prepare
    echo "▶ Preparing..."
    if ! sysbench "$test" \
        --mysql-socket="$SOCKET" \
        --mysql-user="$USER" \
        --mysql-db="$DB" \
        --tables=1 \
        --table-size="$TABLE_SIZE" \
        --threads=1 \
        --mysql-storage-engine="$engine" \
        prepare 2>&1; then
        echo "  ✗ Prepare failed for $engine $test_name"
        return 1
    fi
    
    # Run and capture output
    echo "▶ Running benchmark for ${TIME}s..."
    local output_file="$OUTPUT_DIR/${engine}_${test_name}_${TIMESTAMP}.txt"
    
    sysbench "$test" \
        --mysql-socket="$SOCKET" \
        --mysql-user="$USER" \
        --mysql-db="$DB" \
        --tables=1 \
        --table-size="$TABLE_SIZE" \
        --threads="$THREADS" \
        --time="$TIME" \
        --mysql-storage-engine="$engine" \
        --report-interval="$REPORT_INTERVAL" \
        --histogram=on \
        --mysql-ignore-errors=1213,1020,1205,1180 \
        run 2>&1 | tee "$output_file"
    
    # Parse results
    local tps=$(grep "transactions:" "$output_file" | awk '{print $2}' | sed 's/(//g')
    local qps=$(grep "queries:" "$output_file" | head -1 | awk '{print $2}' | sed 's/(//g')
    local lat_avg=$(grep "avg:" "$output_file" | tail -1 | awk '{print $2}')
    local lat_p95=$(grep "95th percentile:" "$output_file" | awk '{print $3}')
    local lat_max=$(grep "max:" "$output_file" | tail -1 | awk '{print $2}')
    local total_time=$(grep "total time:" "$output_file" | awk '{print $3}' | sed 's/s//g')
    
    # Write to summary CSV
    echo "$engine,$test_name,$THREADS,$TABLE_SIZE,$tps,$qps,$lat_avg,$lat_p95,$lat_max,$total_time" >> "$SUMMARY_CSV"
    
    # Parse interval reports for detail CSV
    grep "thds:" "$output_file" | while read line; do
        local time_s=$(echo "$line" | awk -F'[\\[\\]]' '{print $2}' | sed 's/s//g')
        local int_tps=$(echo "$line" | awk '{print $5}')
        local int_qps=$(echo "$line" | awk '{print $7}')
        local int_lat=$(echo "$line" | awk '{print $9}')
        echo "$engine,$test_name,$THREADS,$time_s,$int_tps,$int_qps,$int_lat" >> "$DETAIL_CSV"
    done
    
    echo ""
    echo "✓ $engine $test_name: TPS=$tps, QPS=$qps, Latency avg=${lat_avg}ms p95=${lat_p95}ms"
    
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
    TIDESDB_DIR="${TIDESDB_DIR:-${DATA_DIR}/tidesdb}"
    PID_FILE="${DATA_DIR}/mysqld.pid"
    ERROR_LOG="${DATA_DIR}/mysqld.err"
    MYSQLD="${BUILD_DIR}/sql/mariadbd"
    
    # Check if server is already running
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "Server already running (PID: $(cat $PID_FILE))"
    else
        # Create directories
        mkdir -p "$DATA_DIR" "$TIDESDB_DIR"
        
        # Initialize data directory if needed
        if [ ! -d "$DATA_DIR/mysql" ]; then
            echo "Initializing data directory: $DATA_DIR"
            "${BUILD_DIR}/scripts/mariadb-install-db" \
                --basedir="${BUILD_DIR}" \
                --datadir="$DATA_DIR" \
                --user=$(whoami) 2>&1 | tail -3
        fi
        
        echo "Starting MariaDB server with custom data directory..."
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
            --innodb=ON \
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

echo ""
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo "  Sysbench TidesDB vs InnoDB Benchmark"
echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
echo ""
echo "Configuration:"
echo "  Socket:           $SOCKET"
echo "  Data directory:   ${DATA_DIR:-default (MTR)}"
echo "  Table size:       $TABLE_SIZE rows"
echo "  Threads:          $THREADS"
echo "  Duration:         ${TIME}s"
echo "  Report interval:  ${REPORT_INTERVAL}s"
echo "  Output directory: $OUTPUT_DIR"

# Run tests for specified engines (default: both InnoDB and TidesDB)
# Both engines use the same batched Lua scripts for fair comparison
# (batched scripts use per-thread key partitioning to reduce conflicts)
ENGINES="${ENGINES:-InnoDB TidesDB}"
for engine in $ENGINES; do
    for test in oltp_read_only oltp_write_only oltp_read_write; do
        batched_script="$SCRIPT_DIR/${test}_batched.lua"
        if [ -f "$batched_script" ]; then
            run_sysbench_test "$engine" "$batched_script" || echo "  (skipping due to error)"
        else
            # Fall back to standard test if batched version doesn't exist
            run_sysbench_test "$engine" "$test" || echo "  (skipping due to error)"
        fi
    done
done

echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo "  Summary"
echo "═══════════════════════════════════════════════════════════════════════"
echo ""
cat "$SUMMARY_CSV" | column -t -s','
echo ""
echo "Results saved to:"
echo "  Summary: $SUMMARY_CSV"
echo "  Detail:  $DETAIL_CSV"
echo "  Raw:     $OUTPUT_DIR/*.txt"
