--require r/have_tidesdb.require
disable_query_log;
--error 0,1286
eval SET @@default_storage_engine = TidesDB;
enable_query_log;
