disable_query_log;
--error 0,1286
eval SET @@default_storage_engine = TidesDB;
ALTER DATABASE test DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
enable_query_log;
