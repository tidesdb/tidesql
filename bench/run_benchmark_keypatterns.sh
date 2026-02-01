#!/bin/bash
# TidesDB vs InnoDB Key Pattern Benchmark Script
# Tests different access patterns:
#   - Sequential keys (best case for LSM-tree writes, B-tree reads)
#   - Random keys (stress test for both)
#   - Zipfian distribution (hot spots - realistic workload)
#   - Batch operations (bulk insert performance)

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

print_section() {
    echo ""
    echo -e "${YELLOW}▶ $1${NC}"
}

print_result() {
    echo -e "  ${GREEN}✓${NC} $1"
}

print_info() {
    echo -e "  ${BLUE}ℹ${NC} $1"
}

# Check server
if ! $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" -e "SELECT 1" &>/dev/null; then
    echo -e "${RED}Error: MariaDB server not running${NC}"
    exit 1
fi

run_sql() {
    $MYSQL_BIN -S "$SOCKET" -u "$MYSQL_USER" -N -e "$1" test 2>/dev/null
}

mkdir -p "$OUTPUT_DIR"

# Results file
RESULTS_FILE="$OUTPUT_DIR/keypattern_results.csv"
echo "engine,pattern,operation,row_count,duration_ms,ops_per_sec" > "$RESULTS_FILE"

print_header "TidesDB vs InnoDB Key Pattern Benchmark"
echo "Row Count: $ROW_COUNT"
echo "Batch Size: $BATCH_SIZE"

#═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: Sequential Key Pattern
#═══════════════════════════════════════════════════════════════════════════════
print_header "SECTION 1: Sequential Key Pattern (Best for LSM writes)"

for ENGINE in InnoDB TidesDB; do
    print_section "$ENGINE Sequential Operations"
    
    # Create table
    run_sql "DROP TABLE IF EXISTS bench_seq"
    run_sql "CREATE TABLE bench_seq (id BIGINT PRIMARY KEY, val VARCHAR(100), num INT) ENGINE=$ENGINE"
    
    # Sequential INSERT
    print_info "Sequential INSERT ($ROW_COUNT rows)..."
    START=$(date +%s%3N)
    run_sql "
    SET @i = 1;
    WHILE @i <= $ROW_COUNT DO
        INSERT INTO bench_seq VALUES (@i, CONCAT('value_', @i), @i % 1000);
        SET @i = @i + 1;
    END WHILE;
    "
    END=$(date +%s%3N)
    DURATION=$((END - START))
    OPS=$(echo "scale=2; $ROW_COUNT / ($DURATION / 1000)" | bc)
    print_result "INSERT: ${DURATION}ms (${OPS} ops/sec)"
    echo "$ENGINE,sequential,INSERT,$ROW_COUNT,$DURATION,$OPS" >> "$RESULTS_FILE"
    
    # Sequential READ (forward scan)
    print_info "Sequential READ (forward scan)..."
    START=$(date +%s%3N)
    run_sql "SELECT SQL_NO_CACHE COUNT(*) FROM bench_seq WHERE id BETWEEN 1 AND $ROW_COUNT" > /dev/null
    END=$(date +%s%3N)
    DURATION=$((END - START))
    OPS=$(echo "scale=2; $ROW_COUNT / ($DURATION / 1000)" | bc)
    print_result "READ (range): ${DURATION}ms (${OPS} ops/sec)"
    echo "$ENGINE,sequential,READ_RANGE,$ROW_COUNT,$DURATION,$OPS" >> "$RESULTS_FILE"
    
    # Sequential point lookups
    print_info "Sequential point lookups..."
    START=$(date +%s%3N)
    run_sql "
    SET @i = 1;
    WHILE @i <= $ROW_COUNT DO
        SELECT val INTO @dummy FROM bench_seq WHERE id = @i;
        SET @i = @i + 1;
    END WHILE;
    "
    END=$(date +%s%3N)
    DURATION=$((END - START))
    OPS=$(echo "scale=2; $ROW_COUNT / ($DURATION / 1000)" | bc)
    print_result "READ (point): ${DURATION}ms (${OPS} ops/sec)"
    echo "$ENGINE,sequential,READ_POINT,$ROW_COUNT,$DURATION,$OPS" >> "$RESULTS_FILE"
    
    run_sql "DROP TABLE IF EXISTS bench_seq"
done

#═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: Random Key Pattern
#═══════════════════════════════════════════════════════════════════════════════
print_header "SECTION 2: Random Key Pattern (Stress Test)"

for ENGINE in InnoDB TidesDB; do
    print_section "$ENGINE Random Operations"
    
    # Create table with pre-populated data for random reads
    run_sql "DROP TABLE IF EXISTS bench_rand"
    run_sql "CREATE TABLE bench_rand (id BIGINT PRIMARY KEY, val VARCHAR(100), num INT) ENGINE=$ENGINE"
    
    # Populate with sequential data first (for random read test)
    print_info "Populating table..."
    run_sql "
    SET @i = 1;
    WHILE @i <= $ROW_COUNT DO
        INSERT INTO bench_rand VALUES (@i, CONCAT('value_', @i), @i % 1000);
        SET @i = @i + 1;
    END WHILE;
    "
    
    # Random READ (using RAND() to generate random keys)
    print_info "Random point lookups ($ROW_COUNT lookups)..."
    START=$(date +%s%3N)
    run_sql "
    SET @i = 1;
    WHILE @i <= $ROW_COUNT DO
        SET @key = FLOOR(1 + RAND() * $ROW_COUNT);
        SELECT val INTO @dummy FROM bench_rand WHERE id = @key;
        SET @i = @i + 1;
    END WHILE;
    "
    END=$(date +%s%3N)
    DURATION=$((END - START))
    OPS=$(echo "scale=2; $ROW_COUNT / ($DURATION / 1000)" | bc)
    print_result "READ (random): ${DURATION}ms (${OPS} ops/sec)"
    echo "$ENGINE,random,READ_POINT,$ROW_COUNT,$DURATION,$OPS" >> "$RESULTS_FILE"
    
    # Random UPDATE
    print_info "Random updates ($ROW_COUNT updates)..."
    START=$(date +%s%3N)
    run_sql "
    SET @i = 1;
    WHILE @i <= $ROW_COUNT DO
        SET @key = FLOOR(1 + RAND() * $ROW_COUNT);
        UPDATE bench_rand SET val = CONCAT('updated_', @i) WHERE id = @key;
        SET @i = @i + 1;
    END WHILE;
    "
    END=$(date +%s%3N)
    DURATION=$((END - START))
    OPS=$(echo "scale=2; $ROW_COUNT / ($DURATION / 1000)" | bc)
    print_result "UPDATE (random): ${DURATION}ms (${OPS} ops/sec)"
    echo "$ENGINE,random,UPDATE,$ROW_COUNT,$DURATION,$OPS" >> "$RESULTS_FILE"
    
    run_sql "DROP TABLE IF EXISTS bench_rand"
done

#═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: Zipfian Distribution (Hot Spots)
#═══════════════════════════════════════════════════════════════════════════════
print_header "SECTION 3: Zipfian Distribution (Realistic Hot Spots)"

# Zipfian: 80% of accesses go to 20% of keys
# We simulate this by having most accesses hit low key values

for ENGINE in InnoDB TidesDB; do
    print_section "$ENGINE Zipfian Operations"
    
    run_sql "DROP TABLE IF EXISTS bench_zipf"
    run_sql "CREATE TABLE bench_zipf (id BIGINT PRIMARY KEY, val VARCHAR(100), num INT, access_count INT DEFAULT 0) ENGINE=$ENGINE"
    
    # Populate
    print_info "Populating table..."
    run_sql "
    SET @i = 1;
    WHILE @i <= $ROW_COUNT DO
        INSERT INTO bench_zipf (id, val, num) VALUES (@i, CONCAT('value_', @i), @i % 1000);
        SET @i = @i + 1;
    END WHILE;
    "
    
    # Zipfian READ - use power law distribution
    # FLOOR(POW(RAND(), 2) * N) gives zipfian-like distribution
    print_info "Zipfian point lookups ($ROW_COUNT lookups)..."
    START=$(date +%s%3N)
    run_sql "
    SET @i = 1;
    WHILE @i <= $ROW_COUNT DO
        SET @key = GREATEST(1, FLOOR(POW(RAND(), 2) * $ROW_COUNT));
        SELECT val INTO @dummy FROM bench_zipf WHERE id = @key;
        SET @i = @i + 1;
    END WHILE;
    "
    END=$(date +%s%3N)
    DURATION=$((END - START))
    OPS=$(echo "scale=2; $ROW_COUNT / ($DURATION / 1000)" | bc)
    print_result "READ (zipfian): ${DURATION}ms (${OPS} ops/sec)"
    echo "$ENGINE,zipfian,READ_POINT,$ROW_COUNT,$DURATION,$OPS" >> "$RESULTS_FILE"
    
    # Zipfian UPDATE
    print_info "Zipfian updates ($ROW_COUNT updates)..."
    START=$(date +%s%3N)
    run_sql "
    SET @i = 1;
    WHILE @i <= $ROW_COUNT DO
        SET @key = GREATEST(1, FLOOR(POW(RAND(), 2) * $ROW_COUNT));
        UPDATE bench_zipf SET access_count = access_count + 1 WHERE id = @key;
        SET @i = @i + 1;
    END WHILE;
    "
    END=$(date +%s%3N)
    DURATION=$((END - START))
    OPS=$(echo "scale=2; $ROW_COUNT / ($DURATION / 1000)" | bc)
    print_result "UPDATE (zipfian): ${DURATION}ms (${OPS} ops/sec)"
    echo "$ENGINE,zipfian,UPDATE,$ROW_COUNT,$DURATION,$OPS" >> "$RESULTS_FILE"
    
    run_sql "DROP TABLE IF EXISTS bench_zipf"
done

#═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: Batch Operations (Bulk Performance)
#═══════════════════════════════════════════════════════════════════════════════
print_header "SECTION 4: Batch Operations (Bulk Insert Performance)"

for ENGINE in InnoDB TidesDB; do
    print_section "$ENGINE Batch Operations"
    
    run_sql "DROP TABLE IF EXISTS bench_batch"
    run_sql "CREATE TABLE bench_batch (id BIGINT PRIMARY KEY, val VARCHAR(100), num INT) ENGINE=$ENGINE"
    
    # Batch INSERT using multi-row INSERT
    print_info "Batch INSERT (batches of $BATCH_SIZE)..."
    START=$(date +%s%3N)
    
    # Generate batch insert SQL
    BATCH_COUNT=$((ROW_COUNT / BATCH_SIZE))
    for ((b=0; b<BATCH_COUNT; b++)); do
        OFFSET=$((b * BATCH_SIZE))
        VALUES=""
        for ((i=1; i<=BATCH_SIZE; i++)); do
            ID=$((OFFSET + i))
            if [ -n "$VALUES" ]; then
                VALUES="$VALUES,"
            fi
            VALUES="$VALUES($ID,'value_$ID',$((ID % 1000)))"
        done
        run_sql "INSERT INTO bench_batch VALUES $VALUES"
    done
    
    END=$(date +%s%3N)
    DURATION=$((END - START))
    OPS=$(echo "scale=2; $ROW_COUNT / ($DURATION / 1000)" | bc)
    print_result "BATCH INSERT: ${DURATION}ms (${OPS} ops/sec)"
    echo "$ENGINE,batch,INSERT,$ROW_COUNT,$DURATION,$OPS" >> "$RESULTS_FILE"
    
    run_sql "DROP TABLE IF EXISTS bench_batch"
done

#═══════════════════════════════════════════════════════════════════════════════
# SECTION 5: Mixed Workload (OLTP-like)
#═══════════════════════════════════════════════════════════════════════════════
print_header "SECTION 5: Mixed Workload (80% Read, 20% Write)"

MIXED_OPS=$((ROW_COUNT / 2))  # Fewer ops for mixed workload

for ENGINE in InnoDB TidesDB; do
    print_section "$ENGINE Mixed Workload"
    
    run_sql "DROP TABLE IF EXISTS bench_mixed"
    run_sql "CREATE TABLE bench_mixed (id BIGINT PRIMARY KEY, val VARCHAR(100), counter INT DEFAULT 0) ENGINE=$ENGINE"
    
    # Pre-populate
    print_info "Populating table..."
    run_sql "
    SET @i = 1;
    WHILE @i <= $ROW_COUNT DO
        INSERT INTO bench_mixed (id, val) VALUES (@i, CONCAT('value_', @i));
        SET @i = @i + 1;
    END WHILE;
    "
    
    # Mixed workload: 80% reads, 20% writes (updates)
    print_info "Mixed workload ($MIXED_OPS operations, 80% read / 20% write)..."
    START=$(date +%s%3N)
    run_sql "
    SET @i = 1;
    WHILE @i <= $MIXED_OPS DO
        SET @key = GREATEST(1, FLOOR(POW(RAND(), 2) * $ROW_COUNT));
        IF RAND() < 0.8 THEN
            SELECT val INTO @dummy FROM bench_mixed WHERE id = @key;
        ELSE
            UPDATE bench_mixed SET counter = counter + 1 WHERE id = @key;
        END IF;
        SET @i = @i + 1;
    END WHILE;
    "
    END=$(date +%s%3N)
    DURATION=$((END - START))
    OPS=$(echo "scale=2; $MIXED_OPS / ($DURATION / 1000)" | bc)
    print_result "MIXED (80/20): ${DURATION}ms (${OPS} ops/sec)"
    echo "$ENGINE,mixed_80_20,MIXED,$MIXED_OPS,$DURATION,$OPS" >> "$RESULTS_FILE"
    
    run_sql "DROP TABLE IF EXISTS bench_mixed"
done

#═══════════════════════════════════════════════════════════════════════════════
# SECTION 6: Summary Report
#═══════════════════════════════════════════════════════════════════════════════
print_header "SECTION 6: Summary Report"

echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "                    Key Pattern Benchmark Results"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Results saved to: $RESULTS_FILE"
echo ""

# Print formatted results
echo "───────────────────────────────────────────────────────────────────────────────"
printf "%-10s %-12s %-12s %10s %12s %12s\n" "Engine" "Pattern" "Operation" "Rows" "Duration(ms)" "Ops/sec"
echo "───────────────────────────────────────────────────────────────────────────────"
tail -n +2 "$RESULTS_FILE" | while IFS=, read -r engine pattern op rows dur ops; do
    printf "%-10s %-12s %-12s %10s %12s %12s\n" "$engine" "$pattern" "$op" "$rows" "$dur" "$ops"
done
echo "───────────────────────────────────────────────────────────────────────────────"

# Calculate and show comparison
echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "                    TidesDB vs InnoDB Comparison"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""

# Create comparison file
COMPARE_FILE="$OUTPUT_DIR/keypattern_comparison.csv"
echo "pattern,operation,innodb_ops,tidesdb_ops,ratio,winner" > "$COMPARE_FILE"

for PATTERN in sequential random zipfian batch mixed_80_20; do
    for OP in INSERT READ_POINT READ_RANGE UPDATE MIXED; do
        INNODB_OPS=$(grep "^InnoDB,$PATTERN,$OP," "$RESULTS_FILE" 2>/dev/null | cut -d, -f6)
        TIDESDB_OPS=$(grep "^TidesDB,$PATTERN,$OP," "$RESULTS_FILE" 2>/dev/null | cut -d, -f6)
        
        if [ -n "$INNODB_OPS" ] && [ -n "$TIDESDB_OPS" ]; then
            RATIO=$(echo "scale=2; $TIDESDB_OPS / $INNODB_OPS" | bc)
            if (( $(echo "$RATIO > 1" | bc -l) )); then
                WINNER="TidesDB"
            else
                WINNER="InnoDB"
            fi
            echo "$PATTERN,$OP,$INNODB_OPS,$TIDESDB_OPS,$RATIO,$WINNER" >> "$COMPARE_FILE"
            printf "%-12s %-12s: InnoDB=%10s  TidesDB=%10s  Ratio=%.2fx  Winner=%s\n" \
                "$PATTERN" "$OP" "$INNODB_OPS" "$TIDESDB_OPS" "$RATIO" "$WINNER"
        fi
    done
done

echo ""
echo "Comparison saved to: $COMPARE_FILE"
echo ""
echo "Benchmark complete!"
