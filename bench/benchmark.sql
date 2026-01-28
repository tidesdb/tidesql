-- TidesDB and InnoDB Benchmark Script

-- Configuration
SET @row_count = 10000;
SET @batch_size = 1000;

-- Create output table for results
DROP TABLE IF EXISTS benchmark_results;
CREATE TABLE benchmark_results (
  id INT AUTO_INCREMENT PRIMARY KEY,
  engine VARCHAR(20),
  operation VARCHAR(50),
  row_count INT,
  duration_ms DECIMAL(10,2),
  ops_per_sec DECIMAL(10,2)
) ENGINE=MEMORY;

-- Helper procedure to record results
DELIMITER //
CREATE OR REPLACE PROCEDURE record_result(
  IN p_engine VARCHAR(20),
  IN p_operation VARCHAR(50),
  IN p_row_count INT,
  IN p_start_time DATETIME(6),
  IN p_end_time DATETIME(6)
)
BEGIN
  DECLARE v_duration_ms DECIMAL(10,2);
  DECLARE v_ops_per_sec DECIMAL(10,2);
  
  SET v_duration_ms = TIMESTAMPDIFF(MICROSECOND, p_start_time, p_end_time) / 1000.0;
  SET v_ops_per_sec = IF(v_duration_ms > 0, p_row_count / (v_duration_ms / 1000.0), 0);
  
  INSERT INTO benchmark_results (engine, operation, row_count, duration_ms, ops_per_sec)
  VALUES (p_engine, p_operation, p_row_count, v_duration_ms, v_ops_per_sec);
END //
DELIMITER ;

SELECT '=== InnoDB Benchmarks ===' AS status;

-- Create InnoDB test table
DROP TABLE IF EXISTS bench_innodb;
CREATE TABLE bench_innodb (
  id INT PRIMARY KEY,
  val1 VARCHAR(100),
  val2 INT,
  val3 DATETIME,
  INDEX idx_val2 (val2)
) ENGINE=InnoDB;

-- Sequential INSERT
SET @start_time = NOW(6);
SET @i = 1;
WHILE @i <= @row_count DO
  INSERT INTO bench_innodb VALUES (@i, CONCAT('value_', @i), @i % 1000, NOW());
  SET @i = @i + 1;
END WHILE;
SET @end_time = NOW(6);
CALL record_result('InnoDB', 'Sequential INSERT', @row_count, @start_time, @end_time);

-- Point SELECT (PK lookup)
SET @start_time = NOW(6);
SET @i = 1;
WHILE @i <= @row_count DO
  SELECT * FROM bench_innodb WHERE id = @i INTO @dummy1, @dummy2, @dummy3, @dummy4;
  SET @i = @i + 1;
END WHILE;
SET @end_time = NOW(6);
CALL record_result('InnoDB', 'Point SELECT (PK)', @row_count, @start_time, @end_time);

-- Range SELECT
SET @start_time = NOW(6);
SET @i = 1;
WHILE @i <= 100 DO
  SELECT COUNT(*) FROM bench_innodb WHERE val2 BETWEEN @i * 10 AND @i * 10 + 100 INTO @dummy1;
  SET @i = @i + 1;
END WHILE;
SET @end_time = NOW(6);
CALL record_result('InnoDB', 'Range SELECT (100 queries)', 100, @start_time, @end_time);

-- UPDATE
SET @start_time = NOW(6);
SET @i = 1;
WHILE @i <= @row_count DO
  UPDATE bench_innodb SET val1 = CONCAT('updated_', @i) WHERE id = @i;
  SET @i = @i + 1;
END WHILE;
SET @end_time = NOW(6);
CALL record_result('InnoDB', 'UPDATE (PK)', @row_count, @start_time, @end_time);

-- DELETE
SET @start_time = NOW(6);
SET @i = 1;
WHILE @i <= @row_count DO
  DELETE FROM bench_innodb WHERE id = @i;
  SET @i = @i + 1;
END WHILE;
SET @end_time = NOW(6);
CALL record_result('InnoDB', 'DELETE (PK)', @row_count, @start_time, @end_time);

DROP TABLE bench_innodb;

SELECT '=== TidesDB Benchmarks ===' AS status;

-- Create TidesDB test table
DROP TABLE IF EXISTS bench_tidesdb;
CREATE TABLE bench_tidesdb (
  id INT PRIMARY KEY,
  val1 VARCHAR(100),
  val2 INT,
  val3 DATETIME,
  INDEX idx_val2 (val2)
) ENGINE=TidesDB;

-- Sequential INSERT
SET @start_time = NOW(6);
SET @i = 1;
WHILE @i <= @row_count DO
  INSERT INTO bench_tidesdb VALUES (@i, CONCAT('value_', @i), @i % 1000, NOW());
  SET @i = @i + 1;
END WHILE;
SET @end_time = NOW(6);
CALL record_result('TidesDB', 'Sequential INSERT', @row_count, @start_time, @end_time);

-- Point SELECT (PK lookup)
SET @start_time = NOW(6);
SET @i = 1;
WHILE @i <= @row_count DO
  SELECT * FROM bench_tidesdb WHERE id = @i INTO @dummy1, @dummy2, @dummy3, @dummy4;
  SET @i = @i + 1;
END WHILE;
SET @end_time = NOW(6);
CALL record_result('TidesDB', 'Point SELECT (PK)', @row_count, @start_time, @end_time);

-- Range SELECT
SET @start_time = NOW(6);
SET @i = 1;
WHILE @i <= 100 DO
  SELECT COUNT(*) FROM bench_tidesdb WHERE val2 BETWEEN @i * 10 AND @i * 10 + 100 INTO @dummy1;
  SET @i = @i + 1;
END WHILE;
SET @end_time = NOW(6);
CALL record_result('TidesDB', 'Range SELECT (100 queries)', 100, @start_time, @end_time);

-- UPDATE
SET @start_time = NOW(6);
SET @i = 1;
WHILE @i <= @row_count DO
  UPDATE bench_tidesdb SET val1 = CONCAT('updated_', @i) WHERE id = @i;
  SET @i = @i + 1;
END WHILE;
SET @end_time = NOW(6);
CALL record_result('TidesDB', 'UPDATE (PK)', @row_count, @start_time, @end_time);

-- DELETE
SET @start_time = NOW(6);
SET @i = 1;
WHILE @i <= @row_count DO
  DELETE FROM bench_tidesdb WHERE id = @i;
  SET @i = @i + 1;
END WHILE;
SET @end_time = NOW(6);
CALL record_result('TidesDB', 'DELETE (PK)', @row_count, @start_time, @end_time);

DROP TABLE bench_tidesdb;

SELECT 'engine,operation,row_count,duration_ms,ops_per_sec' AS '';
SELECT CONCAT(engine, ',', operation, ',', row_count, ',', duration_ms, ',', ops_per_sec)
FROM benchmark_results
ORDER BY operation, engine;

-- Cleanup
DROP PROCEDURE IF EXISTS record_result;
DROP TABLE IF EXISTS benchmark_results;
