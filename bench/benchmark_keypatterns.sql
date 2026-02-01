-- TidesDB vs InnoDB Key Pattern Benchmark (Pure SQL)
-- Tests: Sequential, Random, Zipfian access patterns
-- Uses stored procedures for accurate timing without shell overhead

SET @row_count = 50000;

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

-- ═══════════════════════════════════════════════════════════════════════════════
-- HELPER: Record result
-- ═══════════════════════════════════════════════════════════════════════════════
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
    SELECT CONCAT('  ', p_engine, ' ', p_pattern, ' ', p_operation, ': ', 
                  ROUND(v_ms, 2), 'ms (', FORMAT(v_ops, 0), ' ops/sec)') AS result;
END //

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEQUENTIAL KEY PATTERN
-- ═══════════════════════════════════════════════════════════════════════════════

DROP PROCEDURE IF EXISTS bench_sequential //
CREATE PROCEDURE bench_sequential(p_engine VARCHAR(20), p_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    DECLARE dummy VARCHAR(100);
    DECLARE dummy_count INT;
    
    -- Create table
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_seq_', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET @sql = CONCAT('CREATE TABLE bench_seq_', p_engine, 
                      ' (id BIGINT PRIMARY KEY, val VARCHAR(100), num INT) ENGINE=', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Sequential INSERT
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET @sql = CONCAT('INSERT INTO bench_seq_', p_engine, ' VALUES (', i, ', ''value_', i, ''', ', i MOD 1000, ')');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'sequential', 'INSERT', p_count, t1, t2);
    
    -- Sequential READ (point lookups)
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET @sql = CONCAT('SELECT val INTO @dummy FROM bench_seq_', p_engine, ' WHERE id = ', i);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'sequential', 'READ_POINT', p_count, t1, t2);
    
    -- Sequential UPDATE
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET @sql = CONCAT('UPDATE bench_seq_', p_engine, ' SET val = ''updated_', i, ''' WHERE id = ', i);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'sequential', 'UPDATE', p_count, t1, t2);
    
    -- Cleanup
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_seq_', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

-- ═══════════════════════════════════════════════════════════════════════════════
-- RANDOM KEY PATTERN
-- ═══════════════════════════════════════════════════════════════════════════════

DROP PROCEDURE IF EXISTS bench_random //
CREATE PROCEDURE bench_random(p_engine VARCHAR(20), p_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    DECLARE rnd_key BIGINT;
    
    -- Create and populate table
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_rnd_', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET @sql = CONCAT('CREATE TABLE bench_rnd_', p_engine, 
                      ' (id BIGINT PRIMARY KEY, val VARCHAR(100), num INT) ENGINE=', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Populate sequentially first
    SET i = 1;
    WHILE i <= p_count DO
        SET @sql = CONCAT('INSERT INTO bench_rnd_', p_engine, ' VALUES (', i, ', ''value_', i, ''', ', i MOD 1000, ')');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    
    -- Random READ
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET rnd_key = FLOOR(1 + RAND() * p_count);
        SET @sql = CONCAT('SELECT val INTO @dummy FROM bench_rnd_', p_engine, ' WHERE id = ', rnd_key);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'random', 'READ_POINT', p_count, t1, t2);
    
    -- Random UPDATE
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET rnd_key = FLOOR(1 + RAND() * p_count);
        SET @sql = CONCAT('UPDATE bench_rnd_', p_engine, ' SET val = ''updated_', i, ''' WHERE id = ', rnd_key);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'random', 'UPDATE', p_count, t1, t2);
    
    -- Cleanup
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_rnd_', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

-- ═══════════════════════════════════════════════════════════════════════════════
-- ZIPFIAN KEY PATTERN (Hot Spots)
-- ═══════════════════════════════════════════════════════════════════════════════

DROP PROCEDURE IF EXISTS bench_zipfian //
CREATE PROCEDURE bench_zipfian(p_engine VARCHAR(20), p_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    DECLARE zipf_key BIGINT;
    
    -- Create and populate table
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_zipf_', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET @sql = CONCAT('CREATE TABLE bench_zipf_', p_engine, 
                      ' (id BIGINT PRIMARY KEY, val VARCHAR(100), access_count INT DEFAULT 0) ENGINE=', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- Populate
    SET i = 1;
    WHILE i <= p_count DO
        SET @sql = CONCAT('INSERT INTO bench_zipf_', p_engine, ' (id, val) VALUES (', i, ', ''value_', i, ''')');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    
    -- Zipfian READ (POW(RAND(), 2) gives zipfian-like distribution)
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET zipf_key = GREATEST(1, FLOOR(POW(RAND(), 2) * p_count));
        SET @sql = CONCAT('SELECT val INTO @dummy FROM bench_zipf_', p_engine, ' WHERE id = ', zipf_key);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'zipfian', 'READ_POINT', p_count, t1, t2);
    
    -- Zipfian UPDATE
    SET t1 = NOW(6);
    SET i = 1;
    WHILE i <= p_count DO
        SET zipf_key = GREATEST(1, FLOOR(POW(RAND(), 2) * p_count));
        SET @sql = CONCAT('UPDATE bench_zipf_', p_engine, ' SET access_count = access_count + 1 WHERE id = ', zipf_key);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'zipfian', 'UPDATE', p_count, t1, t2);
    
    -- Cleanup
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_zipf_', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

-- ═══════════════════════════════════════════════════════════════════════════════
-- BATCH INSERT (Multi-row INSERT)
-- ═══════════════════════════════════════════════════════════════════════════════

DROP PROCEDURE IF EXISTS bench_batch_insert //
CREATE PROCEDURE bench_batch_insert(p_engine VARCHAR(20), p_count INT, p_batch_size INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE j INT;
    DECLARE t1 DATETIME(6);
    DECLARE t2 DATETIME(6);
    DECLARE batch_count INT;
    DECLARE vals TEXT;
    DECLARE row_id INT;
    
    -- Create table
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_batch_', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET @sql = CONCAT('CREATE TABLE bench_batch_', p_engine, 
                      ' (id BIGINT PRIMARY KEY, val VARCHAR(100), num INT) ENGINE=', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET batch_count = p_count DIV p_batch_size;
    
    SET t1 = NOW(6);
    SET i = 0;
    WHILE i < batch_count DO
        SET vals = '';
        SET j = 1;
        WHILE j <= p_batch_size DO
            SET row_id = i * p_batch_size + j;
            IF vals != '' THEN
                SET vals = CONCAT(vals, ',');
            END IF;
            SET vals = CONCAT(vals, '(', row_id, ',''value_', row_id, ''',', row_id MOD 1000, ')');
            SET j = j + 1;
        END WHILE;
        
        SET @sql = CONCAT('INSERT INTO bench_batch_', p_engine, ' VALUES ', vals);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SET i = i + 1;
    END WHILE;
    SET t2 = NOW(6);
    CALL record_result(p_engine, 'batch', 'INSERT', p_count, t1, t2);
    
    -- Cleanup
    SET @sql = CONCAT('DROP TABLE IF EXISTS bench_batch_', p_engine);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;

-- ═══════════════════════════════════════════════════════════════════════════════
-- RUN BENCHMARKS
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════════════════════' AS '';
SELECT '                    TidesDB vs InnoDB Key Pattern Benchmark' AS '';
SELECT '═══════════════════════════════════════════════════════════════════════════════' AS '';
SELECT CONCAT('Row Count: ', @row_count) AS '';

SELECT '' AS '';
SELECT '▶ Sequential Pattern (InnoDB)' AS '';
CALL bench_sequential('InnoDB', @row_count);

SELECT '' AS '';
SELECT '▶ Sequential Pattern (TidesDB)' AS '';
CALL bench_sequential('TidesDB', @row_count);

SELECT '' AS '';
SELECT '▶ Random Pattern (InnoDB)' AS '';
CALL bench_random('InnoDB', @row_count);

SELECT '' AS '';
SELECT '▶ Random Pattern (TidesDB)' AS '';
CALL bench_random('TidesDB', @row_count);

SELECT '' AS '';
SELECT '▶ Zipfian Pattern (InnoDB)' AS '';
CALL bench_zipfian('InnoDB', @row_count);

SELECT '' AS '';
SELECT '▶ Zipfian Pattern (TidesDB)' AS '';
CALL bench_zipfian('TidesDB', @row_count);

SELECT '' AS '';
SELECT '▶ Batch Insert (InnoDB) - 1000 rows per batch' AS '';
CALL bench_batch_insert('InnoDB', @row_count, 1000);

SELECT '' AS '';
SELECT '▶ Batch Insert (TidesDB) - 1000 rows per batch' AS '';
CALL bench_batch_insert('TidesDB', @row_count, 1000);

-- ═══════════════════════════════════════════════════════════════════════════════
-- RESULTS SUMMARY
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '' AS '';
SELECT '═══════════════════════════════════════════════════════════════════════════════' AS '';
SELECT '                              RESULTS SUMMARY' AS '';
SELECT '═══════════════════════════════════════════════════════════════════════════════' AS '';
SELECT '' AS '';

SELECT 
    LPAD(engine, 8, ' ') AS engine,
    LPAD(pattern, 12, ' ') AS pattern,
    LPAD(operation, 12, ' ') AS operation,
    LPAD(FORMAT(duration_ms, 2), 12, ' ') AS 'duration_ms',
    LPAD(FORMAT(ops_per_sec, 0), 15, ' ') AS 'ops_per_sec'
FROM bench_results
ORDER BY pattern, operation, engine;

SELECT '' AS '';
SELECT '═══════════════════════════════════════════════════════════════════════════════' AS '';
SELECT '                           COMPARISON (TidesDB / InnoDB)' AS '';
SELECT '═══════════════════════════════════════════════════════════════════════════════' AS '';

SELECT 
    i.pattern,
    i.operation,
    FORMAT(i.ops_per_sec, 0) AS innodb_ops,
    FORMAT(t.ops_per_sec, 0) AS tidesdb_ops,
    CONCAT(FORMAT(t.ops_per_sec / i.ops_per_sec, 2), 'x') AS ratio,
    IF(t.ops_per_sec > i.ops_per_sec, 'TidesDB', 'InnoDB') AS winner
FROM bench_results i
JOIN bench_results t ON i.pattern = t.pattern AND i.operation = t.operation
WHERE i.engine = 'InnoDB' AND t.engine = 'TidesDB'
ORDER BY i.pattern, i.operation;

-- Cleanup
DROP PROCEDURE IF EXISTS record_result;
DROP PROCEDURE IF EXISTS bench_sequential;
DROP PROCEDURE IF EXISTS bench_random;
DROP PROCEDURE IF EXISTS bench_zipfian;
DROP PROCEDURE IF EXISTS bench_batch_insert;
DROP TABLE IF EXISTS bench_results;
