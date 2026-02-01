#!/bin/bash
# TidesDB vs InnoDB Key Pattern Benchmark (Pure SQL Stored Procedures)
# This script runs the benchmark_keypatterns.sql and exports results to CSV
#
# Usage: ROW_COUNT=100000 ./run_benchmark_procedures.sh

MYSQL_BIN="${MYSQL_BIN:-/home/agpmastersystem/server-mariadb-12.1.2/build/client/mariadb}"
SOCKET="${SOCKET:-/home/agpmastersystem/server-mariadb-12.1.2/build/mysql-test/var/tmp/mysqld.1.sock}"
MYSQL_USER="${MYSQL_USER:-root}"
ROW_COUNT="${ROW_COUNT:-50000}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
OUTPUT_DIR="${OUTPUT_DIR:-benchmark_results}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${CYAN}════════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════════════════════════${NC}"
}

print_info() {
    echo -e "  ${BLUE}ℹ${NC} $1"
}

print_result() {
    echo -e "  ${GREEN}✓${NC} $1"
}

# Check server
if ! $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" -e "SELECT 1" &>/dev/null; then
    echo -e "${RED}Error: MariaDB server not running. Start with:${NC}"
    echo "  cd build && ./mysql-test/mtr --start-and-exit --mysqld=--plugin-load-add=ha_tidesdb.so --mysqld=--innodb=ON"
    exit 1
fi

run_sql() {
    $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" -N -e "$1" test 2>/dev/null
}

mkdir -p "$OUTPUT_DIR"

print_header "TidesDB vs InnoDB Key Pattern Benchmark (Stored Procedures)"
echo "  Row Count: $ROW_COUNT"
echo "  Batch Size: $BATCH_SIZE"
echo "  Output Dir: $OUTPUT_DIR"

# Create the benchmark SQL dynamically with the row count
BENCHMARK_SQL=$(cat << 'EOSQL'
-- TidesDB vs InnoDB Key Pattern Benchmark (Pure SQL)
SET @row_count = ROW_COUNT_PLACEHOLDER;
SET @batch_size = BATCH_SIZE_PLACEHOLDER;

-- Results table
DROP TABLE IF EXISTS bench_results;
CREATE TABLE bench_results (
    engine VARCHAR(20),
    pattern VARCHAR(20),
    operation VARCHAR(20),
    row_count INT,
    duration_ms DECIMAL(12,2),
    ops_per_sec DECIMAL(12,2)
) ENGINE=MEMORY;

DELIMITER //

DROP PROCEDURE IF EXISTS record_result //
CREATE PROCEDURE record_result(
    p_engine VARCHAR(20),
    p_pattern VARCHAR(20),
    p_operation VARCHAR(20),
    p_count INT,
    p_start DATETIME(6),
    p_end DATETIME(6)
)
BEGIN
    DECLARE v_ms DECIMAL(12,2);
    DECLARE v_ops DECIMAL(12,2);
    SET v_ms = TIMESTAMPDIFF(MICROSECOND, p_start, p_end) / 1000.0;
    SET v_ops = IF(v_ms > 0, p_count / (v_ms / 1000.0), 0);
    INSERT INTO bench_results VALUES (p_engine, p_pattern, p_operation, p_count, v_ms, v_ops);
END //

-- Sequential INSERT
DROP PROCEDURE IF EXISTS bench_seq_insert //
CREATE PROCEDURE bench_seq_insert(p_engine VARCHAR(20), p_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_seq_', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    
    SET @sql = CONCAT('CREATE TABLE bench_seq_', p_engine, 
                      ' (id BIGINT PRIMARY KEY, val VARCHAR(100), num INT) ENGINE=', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET @sql = CONCAT('INSERT INTO bench_seq_', p_engine, ' VALUES (', i, ', ''v', i, ''', ', i MOD 1000, ')');
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'sequential', 'INSERT', p_count, t1, t2);
END //

-- Sequential READ
DROP PROCEDURE IF EXISTS bench_seq_read //
CREATE PROCEDURE bench_seq_read(p_engine VARCHAR(20), p_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    
    SET t1 = NOW(6);
    WHILE i <= p_count DO
        SET @sql = CONCAT('SELECT val INTO @d FROM bench_seq_', p_engine, ' WHERE id = ', i);
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'sequential', 'READ', p_count, t1, t2);
END //

-- Sequential UPDATE
DROP PROCEDURE IF EXISTS bench_seq_update //
CREATE PROCEDURE bench_seq_update(p_engine VARCHAR(20), p_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    
    SET t1 = NOW(6);
    WHILE i <= p_count DO
        SET @sql = CONCAT('UPDATE bench_seq_', p_engine, ' SET val = ''u', i, ''' WHERE id = ', i);
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'sequential', 'UPDATE', p_count, t1, t2);
    
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_seq_', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
END //

-- Random READ
DROP PROCEDURE IF EXISTS bench_random_read //
CREATE PROCEDURE bench_random_read(p_engine VARCHAR(20), p_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    DECLARE rkey BIGINT;
    
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_rnd_', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    
    SET @sql = CONCAT('CREATE TABLE bench_rnd_', p_engine, 
                      ' (id BIGINT PRIMARY KEY, val VARCHAR(100)) ENGINE=', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    
    -- Populate
    SET i = 1;
    WHILE i <= p_count DO
        SET @sql = CONCAT('INSERT INTO bench_rnd_', p_engine, ' VALUES (', i, ', ''v', i, ''')');
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    
    -- Random reads
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET rkey = FLOOR(1 + RAND() * p_count);
        SET @sql = CONCAT('SELECT val INTO @d FROM bench_rnd_', p_engine, ' WHERE id = ', rkey);
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'random', 'READ', p_count, t1, t2);
END //

-- Random UPDATE
DROP PROCEDURE IF EXISTS bench_random_update //
CREATE PROCEDURE bench_random_update(p_engine VARCHAR(20), p_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    DECLARE rkey BIGINT;
    
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET rkey = FLOOR(1 + RAND() * p_count);
        SET @sql = CONCAT('UPDATE bench_rnd_', p_engine, ' SET val = ''u', i, ''' WHERE id = ', rkey);
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'random', 'UPDATE', p_count, t1, t2);
    
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_rnd_', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
END //

-- Zipfian READ
DROP PROCEDURE IF EXISTS bench_zipf_read //
CREATE PROCEDURE bench_zipf_read(p_engine VARCHAR(20), p_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    DECLARE zkey BIGINT;
    
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_zipf_', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    
    SET @sql = CONCAT('CREATE TABLE bench_zipf_', p_engine, 
                      ' (id BIGINT PRIMARY KEY, val VARCHAR(100), cnt INT DEFAULT 0) ENGINE=', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    
    -- Populate
    SET i = 1;
    WHILE i <= p_count DO
        SET @sql = CONCAT('INSERT INTO bench_zipf_', p_engine, ' (id, val) VALUES (', i, ', ''v', i, ''')');
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    
    -- Zipfian reads
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET zkey = GREATEST(1, FLOOR(POW(RAND(), 2) * p_count));
        SET @sql = CONCAT('SELECT val INTO @d FROM bench_zipf_', p_engine, ' WHERE id = ', zkey);
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'zipfian', 'READ', p_count, t1, t2);
END //

-- Zipfian UPDATE
DROP PROCEDURE IF EXISTS bench_zipf_update //
CREATE PROCEDURE bench_zipf_update(p_engine VARCHAR(20), p_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    DECLARE zkey BIGINT;
    
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET zkey = GREATEST(1, FLOOR(POW(RAND(), 2) * p_count));
        SET @sql = CONCAT('UPDATE bench_zipf_', p_engine, ' SET cnt = cnt + 1 WHERE id = ', zkey);
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'zipfian', 'UPDATE', p_count, t1, t2);
    
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_zipf_', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
END //

-- Batch INSERT
DROP PROCEDURE IF EXISTS bench_batch //
CREATE PROCEDURE bench_batch(p_engine VARCHAR(20), p_count INT, p_batch INT)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE j INT;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    DECLARE batches INT;
    DECLARE vals TEXT;
    DECLARE rid INT;
    
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_batch_', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    
    SET @sql = CONCAT('CREATE TABLE bench_batch_', p_engine, 
                      ' (id BIGINT PRIMARY KEY, val VARCHAR(100)) ENGINE=', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    
    SET batches = p_count DIV p_batch;
    
    SET t1 = NOW(6);
    WHILE i < batches DO
        SET vals = '';
        SET j = 1;
        WHILE j <= p_batch DO
            SET rid = i * p_batch + j;
            IF vals != '' THEN SET vals = CONCAT(vals, ','); END IF;
            SET vals = CONCAT(vals, '(', rid, ',''v', rid, ''')');
            SET j = j + 1;
        END WHILE;
        
        SET @sql = CONCAT('INSERT INTO bench_batch_', p_engine, ' VALUES ', vals);
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'batch', 'INSERT', p_count, t1, t2);
    
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_batch_', p_engine);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
END //

DELIMITER ;

-- Run benchmarks
SELECT 'Running InnoDB Sequential...' AS status;
CALL bench_seq_insert('InnoDB', @row_count);
CALL bench_seq_read('InnoDB', @row_count);
CALL bench_seq_update('InnoDB', @row_count);

SELECT 'Running TidesDB Sequential...' AS status;
CALL bench_seq_insert('TidesDB', @row_count);
CALL bench_seq_read('TidesDB', @row_count);
CALL bench_seq_update('TidesDB', @row_count);

SELECT 'Running InnoDB Random...' AS status;
CALL bench_random_read('InnoDB', @row_count);
CALL bench_random_update('InnoDB', @row_count);

SELECT 'Running TidesDB Random...' AS status;
CALL bench_random_read('TidesDB', @row_count);
CALL bench_random_update('TidesDB', @row_count);

SELECT 'Running InnoDB Zipfian...' AS status;
CALL bench_zipf_read('InnoDB', @row_count);
CALL bench_zipf_update('InnoDB', @row_count);

SELECT 'Running TidesDB Zipfian...' AS status;
CALL bench_zipf_read('TidesDB', @row_count);
CALL bench_zipf_update('TidesDB', @row_count);

SELECT 'Running InnoDB Batch...' AS status;
CALL bench_batch('InnoDB', @row_count, @batch_size);

SELECT 'Running TidesDB Batch...' AS status;
CALL bench_batch('TidesDB', @row_count, @batch_size);

-- Output results
SELECT 'RESULTS_START' AS marker;
SELECT CONCAT_WS(',', engine, pattern, operation, row_count, duration_ms, ops_per_sec) AS csv_row
FROM bench_results ORDER BY pattern, operation, engine;
SELECT 'RESULTS_END' AS marker;

-- Cleanup
DROP PROCEDURE IF EXISTS record_result;
DROP PROCEDURE IF EXISTS bench_seq_insert;
DROP PROCEDURE IF EXISTS bench_seq_read;
DROP PROCEDURE IF EXISTS bench_seq_update;
DROP PROCEDURE IF EXISTS bench_random_read;
DROP PROCEDURE IF EXISTS bench_random_update;
DROP PROCEDURE IF EXISTS bench_zipf_read;
DROP PROCEDURE IF EXISTS bench_zipf_update;
DROP PROCEDURE IF EXISTS bench_batch;
DROP TABLE IF EXISTS bench_results;
EOSQL
)

# Replace placeholders
BENCHMARK_SQL="${BENCHMARK_SQL//ROW_COUNT_PLACEHOLDER/$ROW_COUNT}"
BENCHMARK_SQL="${BENCHMARK_SQL//BATCH_SIZE_PLACEHOLDER/$BATCH_SIZE}"

print_header "Running Benchmark"

# Run benchmark and capture output
print_info "Executing stored procedures benchmark..."
OUTPUT=$(echo "$BENCHMARK_SQL" | $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" -N test 2>&1)

# Extract CSV data
RESULTS_CSV="$OUTPUT_DIR/procedures_results.csv"
echo "engine,pattern,operation,row_count,duration_ms,ops_per_sec" > "$RESULTS_CSV"

echo "$OUTPUT" | sed -n '/RESULTS_START/,/RESULTS_END/p' | grep -v "RESULTS_" | grep -v "^$" >> "$RESULTS_CSV"

print_result "Results saved to: $RESULTS_CSV"

# Create comparison CSV
COMPARE_CSV="$OUTPUT_DIR/procedures_comparison.csv"
echo "pattern,operation,innodb_ops,tidesdb_ops,ratio,winner" > "$COMPARE_CSV"

print_header "Results Summary"

echo ""
printf "%-12s %-10s %12s %12s %8s %10s\n" "Pattern" "Operation" "InnoDB" "TidesDB" "Ratio" "Winner"
echo "─────────────────────────────────────────────────────────────────────────"

# Process results
tail -n +2 "$RESULTS_CSV" | while IFS=, read -r engine pattern op rows dur ops; do
    if [ "$engine" = "InnoDB" ]; then
        INNODB_OPS="$ops"
        # Find matching TidesDB result
        TIDESDB_LINE=$(grep "^TidesDB,$pattern,$op," "$RESULTS_CSV")
        if [ -n "$TIDESDB_LINE" ]; then
            TIDESDB_OPS=$(echo "$TIDESDB_LINE" | cut -d, -f6)
            if [ -n "$INNODB_OPS" ] && [ -n "$TIDESDB_OPS" ] && [ "$INNODB_OPS" != "0" ]; then
                RATIO=$(echo "scale=2; $TIDESDB_OPS / $INNODB_OPS" | bc 2>/dev/null || echo "0")
                if (( $(echo "$RATIO > 1" | bc -l 2>/dev/null || echo 0) )); then
                    WINNER="TidesDB"
                else
                    WINNER="InnoDB"
                fi
                printf "%-12s %-10s %12.0f %12.0f %7.2fx %10s\n" "$pattern" "$op" "$INNODB_OPS" "$TIDESDB_OPS" "$RATIO" "$WINNER"
                echo "$pattern,$op,$INNODB_OPS,$TIDESDB_OPS,$RATIO,$WINNER" >> "$COMPARE_CSV"
            fi
        fi
    fi
done

echo ""
print_result "Comparison saved to: $COMPARE_CSV"

print_header "Benchmark Complete"
echo ""
echo "Files generated:"
echo "  - $RESULTS_CSV (raw results)"
echo "  - $COMPARE_CSV (comparison)"
echo ""
