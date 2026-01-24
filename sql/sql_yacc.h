/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_MYSQL_SQL_YACC_H_INCLUDED
# define YY_MYSQL_SQL_YACC_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int MYSQLdebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    ABORT_SYM = 258,               /* ABORT_SYM  */
    ACCESSIBLE_SYM = 259,          /* ACCESSIBLE_SYM  */
    ACTION = 260,                  /* ACTION  */
    ADD = 261,                     /* ADD  */
    ADDDATE_SYM = 262,             /* ADDDATE_SYM  */
    AFTER_SYM = 263,               /* AFTER_SYM  */
    AGAINST = 264,                 /* AGAINST  */
    AGGREGATE_SYM = 265,           /* AGGREGATE_SYM  */
    ALGORITHM_SYM = 266,           /* ALGORITHM_SYM  */
    ALL = 267,                     /* ALL  */
    ALTER = 268,                   /* ALTER  */
    ANALYZE_SYM = 269,             /* ANALYZE_SYM  */
    AND_AND_SYM = 270,             /* AND_AND_SYM  */
    AND_SYM = 271,                 /* AND_SYM  */
    ANY_SYM = 272,                 /* ANY_SYM  */
    AS = 273,                      /* AS  */
    ASC = 274,                     /* ASC  */
    ASCII_SYM = 275,               /* ASCII_SYM  */
    ASENSITIVE_SYM = 276,          /* ASENSITIVE_SYM  */
    ASYNC_COMMIT_SYM = 277,        /* ASYNC_COMMIT_SYM  */
    AT_SYM = 278,                  /* AT_SYM  */
    AUTHORS_SYM = 279,             /* AUTHORS_SYM  */
    AUTOEXTEND_SIZE_SYM = 280,     /* AUTOEXTEND_SIZE_SYM  */
    AUTO_INC = 281,                /* AUTO_INC  */
    AVG_ROW_LENGTH = 282,          /* AVG_ROW_LENGTH  */
    AVG_SYM = 283,                 /* AVG_SYM  */
    BACKUP_SYM = 284,              /* BACKUP_SYM  */
    BEFORE_SYM = 285,              /* BEFORE_SYM  */
    BEGIN_SYM = 286,               /* BEGIN_SYM  */
    BETWEEN_SYM = 287,             /* BETWEEN_SYM  */
    BIGINT = 288,                  /* BIGINT  */
    BINARY = 289,                  /* BINARY  */
    BINLOG_SYM = 290,              /* BINLOG_SYM  */
    BIN_NUM = 291,                 /* BIN_NUM  */
    BIT_AND = 292,                 /* BIT_AND  */
    BIT_OR = 293,                  /* BIT_OR  */
    BIT_SYM = 294,                 /* BIT_SYM  */
    BIT_XOR = 295,                 /* BIT_XOR  */
    BLOB_SYM = 296,                /* BLOB_SYM  */
    BLOCK_SYM = 297,               /* BLOCK_SYM  */
    BOOLEAN_SYM = 298,             /* BOOLEAN_SYM  */
    BOOL_SYM = 299,                /* BOOL_SYM  */
    BOTH = 300,                    /* BOTH  */
    BTREE_SYM = 301,               /* BTREE_SYM  */
    BY = 302,                      /* BY  */
    BYTE_SYM = 303,                /* BYTE_SYM  */
    CACHE_SYM = 304,               /* CACHE_SYM  */
    CALL_SYM = 305,                /* CALL_SYM  */
    CASCADE = 306,                 /* CASCADE  */
    CASCADED = 307,                /* CASCADED  */
    CASE_SYM = 308,                /* CASE_SYM  */
    CAST_SYM = 309,                /* CAST_SYM  */
    CHAIN_SYM = 310,               /* CHAIN_SYM  */
    CHANGE = 311,                  /* CHANGE  */
    CHANGED = 312,                 /* CHANGED  */
    CHARSET = 313,                 /* CHARSET  */
    CHAR_SYM = 314,                /* CHAR_SYM  */
    CHECKSUM_SYM = 315,            /* CHECKSUM_SYM  */
    CHECK_SYM = 316,               /* CHECK_SYM  */
    CIPHER_SYM = 317,              /* CIPHER_SYM  */
    CLIENT_SYM = 318,              /* CLIENT_SYM  */
    CLOSE_SYM = 319,               /* CLOSE_SYM  */
    COALESCE = 320,                /* COALESCE  */
    CODE_SYM = 321,                /* CODE_SYM  */
    COLLATE_SYM = 322,             /* COLLATE_SYM  */
    COLLATION_SYM = 323,           /* COLLATION_SYM  */
    COLUMNS = 324,                 /* COLUMNS  */
    COLUMN_SYM = 325,              /* COLUMN_SYM  */
    COMMENT_SYM = 326,             /* COMMENT_SYM  */
    COMMITTED_SYM = 327,           /* COMMITTED_SYM  */
    COMMIT_SYM = 328,              /* COMMIT_SYM  */
    COMPACT_SYM = 329,             /* COMPACT_SYM  */
    COMPLETION_SYM = 330,          /* COMPLETION_SYM  */
    COMPRESSED_SYM = 331,          /* COMPRESSED_SYM  */
    CONCURRENT = 332,              /* CONCURRENT  */
    CONDITION_SYM = 333,           /* CONDITION_SYM  */
    CONNECTION_SYM = 334,          /* CONNECTION_SYM  */
    CONSISTENT_SYM = 335,          /* CONSISTENT_SYM  */
    CONSTRAINT = 336,              /* CONSTRAINT  */
    CONTAINS_SYM = 337,            /* CONTAINS_SYM  */
    CONTEXT_SYM = 338,             /* CONTEXT_SYM  */
    CONTINUE_SYM = 339,            /* CONTINUE_SYM  */
    CONTRIBUTORS_SYM = 340,        /* CONTRIBUTORS_SYM  */
    CONVERT_SYM = 341,             /* CONVERT_SYM  */
    COUNT_SYM = 342,               /* COUNT_SYM  */
    CPU_SYM = 343,                 /* CPU_SYM  */
    CREATE = 344,                  /* CREATE  */
    CROSS = 345,                   /* CROSS  */
    CUBE_SYM = 346,                /* CUBE_SYM  */
    CURDATE = 347,                 /* CURDATE  */
    CURRENT_USER = 348,            /* CURRENT_USER  */
    CURSOR_SYM = 349,              /* CURSOR_SYM  */
    CURTIME = 350,                 /* CURTIME  */
    DATABASE = 351,                /* DATABASE  */
    DATABASES = 352,               /* DATABASES  */
    DATAFILE_SYM = 353,            /* DATAFILE_SYM  */
    DATA_SYM = 354,                /* DATA_SYM  */
    DATETIME = 355,                /* DATETIME  */
    DATE_ADD_INTERVAL = 356,       /* DATE_ADD_INTERVAL  */
    DATE_SUB_INTERVAL = 357,       /* DATE_SUB_INTERVAL  */
    DATE_SYM = 358,                /* DATE_SYM  */
    DAY_HOUR_SYM = 359,            /* DAY_HOUR_SYM  */
    DAY_MICROSECOND_SYM = 360,     /* DAY_MICROSECOND_SYM  */
    DAY_MINUTE_SYM = 361,          /* DAY_MINUTE_SYM  */
    DAY_SECOND_SYM = 362,          /* DAY_SECOND_SYM  */
    DAY_SYM = 363,                 /* DAY_SYM  */
    DEALLOCATE_SYM = 364,          /* DEALLOCATE_SYM  */
    DECIMAL_NUM = 365,             /* DECIMAL_NUM  */
    DECIMAL_SYM = 366,             /* DECIMAL_SYM  */
    DECLARE_SYM = 367,             /* DECLARE_SYM  */
    DEFAULT = 368,                 /* DEFAULT  */
    DEFINER_SYM = 369,             /* DEFINER_SYM  */
    DELAYED_SYM = 370,             /* DELAYED_SYM  */
    DELAY_KEY_WRITE_SYM = 371,     /* DELAY_KEY_WRITE_SYM  */
    DELETE_SYM = 372,              /* DELETE_SYM  */
    DESC = 373,                    /* DESC  */
    DESCRIBE = 374,                /* DESCRIBE  */
    DES_KEY_FILE = 375,            /* DES_KEY_FILE  */
    DETERMINISTIC_SYM = 376,       /* DETERMINISTIC_SYM  */
    DIRECTORY_SYM = 377,           /* DIRECTORY_SYM  */
    DISABLE_SYM = 378,             /* DISABLE_SYM  */
    DISCARD = 379,                 /* DISCARD  */
    DISK_SYM = 380,                /* DISK_SYM  */
    DISTINCT = 381,                /* DISTINCT  */
    DIV_SYM = 382,                 /* DIV_SYM  */
    DOUBLE_SYM = 383,              /* DOUBLE_SYM  */
    DO_SYM = 384,                  /* DO_SYM  */
    DROP = 385,                    /* DROP  */
    DUAL_SYM = 386,                /* DUAL_SYM  */
    DUMPFILE = 387,                /* DUMPFILE  */
    DUPLICATE_SYM = 388,           /* DUPLICATE_SYM  */
    DYNAMIC_SYM = 389,             /* DYNAMIC_SYM  */
    EACH_SYM = 390,                /* EACH_SYM  */
    ELSE = 391,                    /* ELSE  */
    ELSEIF_SYM = 392,              /* ELSEIF_SYM  */
    ENABLE_SYM = 393,              /* ENABLE_SYM  */
    ENCLOSED = 394,                /* ENCLOSED  */
    END = 395,                     /* END  */
    ENDS_SYM = 396,                /* ENDS_SYM  */
    END_OF_INPUT = 397,            /* END_OF_INPUT  */
    ENGINES_SYM = 398,             /* ENGINES_SYM  */
    ENGINE_SYM = 399,              /* ENGINE_SYM  */
    ENUM = 400,                    /* ENUM  */
    EQ = 401,                      /* EQ  */
    EQUAL_SYM = 402,               /* EQUAL_SYM  */
    ERRORS = 403,                  /* ERRORS  */
    ESCAPED = 404,                 /* ESCAPED  */
    ESCAPE_SYM = 405,              /* ESCAPE_SYM  */
    EVENTS_SYM = 406,              /* EVENTS_SYM  */
    EVENT_SYM = 407,               /* EVENT_SYM  */
    EVERY_SYM = 408,               /* EVERY_SYM  */
    EXECUTE_SYM = 409,             /* EXECUTE_SYM  */
    EXISTS = 410,                  /* EXISTS  */
    EXIT_SYM = 411,                /* EXIT_SYM  */
    EXPANSION_SYM = 412,           /* EXPANSION_SYM  */
    EXTENDED_SYM = 413,            /* EXTENDED_SYM  */
    EXTENT_SIZE_SYM = 414,         /* EXTENT_SIZE_SYM  */
    EXTRACT_SYM = 415,             /* EXTRACT_SYM  */
    FALSE_SYM = 416,               /* FALSE_SYM  */
    FAST_SYM = 417,                /* FAST_SYM  */
    FAULTS_SYM = 418,              /* FAULTS_SYM  */
    FETCH_SYM = 419,               /* FETCH_SYM  */
    FILE_SYM = 420,                /* FILE_SYM  */
    FIRST_SYM = 421,               /* FIRST_SYM  */
    FIXED_SYM = 422,               /* FIXED_SYM  */
    FLOAT_NUM = 423,               /* FLOAT_NUM  */
    FLOAT_SYM = 424,               /* FLOAT_SYM  */
    FLUSH_SYM = 425,               /* FLUSH_SYM  */
    FORCE_SYM = 426,               /* FORCE_SYM  */
    FOREIGN = 427,                 /* FOREIGN  */
    FOR_SYM = 428,                 /* FOR_SYM  */
    FOUND_SYM = 429,               /* FOUND_SYM  */
    FRAC_SECOND_SYM = 430,         /* FRAC_SECOND_SYM  */
    FROM = 431,                    /* FROM  */
    FULL = 432,                    /* FULL  */
    FULLTEXT_SYM = 433,            /* FULLTEXT_SYM  */
    FUNCTION_SYM = 434,            /* FUNCTION_SYM  */
    GE = 435,                      /* GE  */
    GEOMETRYCOLLECTION = 436,      /* GEOMETRYCOLLECTION  */
    GEOMETRY_SYM = 437,            /* GEOMETRY_SYM  */
    GET_FORMAT = 438,              /* GET_FORMAT  */
    GLOBAL_SYM = 439,              /* GLOBAL_SYM  */
    GRANT = 440,                   /* GRANT  */
    GRANTS = 441,                  /* GRANTS  */
    GROUP_SYM = 442,               /* GROUP_SYM  */
    GROUP_CONCAT_SYM = 443,        /* GROUP_CONCAT_SYM  */
    GT_SYM = 444,                  /* GT_SYM  */
    HANDLER_SYM = 445,             /* HANDLER_SYM  */
    HASH_SYM = 446,                /* HASH_SYM  */
    HAVING = 447,                  /* HAVING  */
    HELP_SYM = 448,                /* HELP_SYM  */
    HEX_NUM = 449,                 /* HEX_NUM  */
    HIGH_PRIORITY = 450,           /* HIGH_PRIORITY  */
    HOST_SYM = 451,                /* HOST_SYM  */
    HOSTS_SYM = 452,               /* HOSTS_SYM  */
    HOUR_MICROSECOND_SYM = 453,    /* HOUR_MICROSECOND_SYM  */
    HOUR_MINUTE_SYM = 454,         /* HOUR_MINUTE_SYM  */
    HOUR_SECOND_SYM = 455,         /* HOUR_SECOND_SYM  */
    HOUR_SYM = 456,                /* HOUR_SYM  */
    IDENT = 457,                   /* IDENT  */
    IDENTIFIED_SYM = 458,          /* IDENTIFIED_SYM  */
    IDENT_QUOTED = 459,            /* IDENT_QUOTED  */
    IF = 460,                      /* IF  */
    IGNORE_SYM = 461,              /* IGNORE_SYM  */
    IMPORT = 462,                  /* IMPORT  */
    INDEXES = 463,                 /* INDEXES  */
    INDEX_SYM = 464,               /* INDEX_SYM  */
    INFILE = 465,                  /* INFILE  */
    INITIAL_SIZE_SYM = 466,        /* INITIAL_SIZE_SYM  */
    INNER_SYM = 467,               /* INNER_SYM  */
    INNOBASE_SYM = 468,            /* INNOBASE_SYM  */
    INOUT_SYM = 469,               /* INOUT_SYM  */
    INSENSITIVE_SYM = 470,         /* INSENSITIVE_SYM  */
    INSERT = 471,                  /* INSERT  */
    INSERT_METHOD = 472,           /* INSERT_METHOD  */
    INSTALL_SYM = 473,             /* INSTALL_SYM  */
    INTERVAL_SYM = 474,            /* INTERVAL_SYM  */
    INTO = 475,                    /* INTO  */
    INT_SYM = 476,                 /* INT_SYM  */
    INVOKER_SYM = 477,             /* INVOKER_SYM  */
    IN_SYM = 478,                  /* IN_SYM  */
    IO_SYM = 479,                  /* IO_SYM  */
    IPC_SYM = 480,                 /* IPC_SYM  */
    IS = 481,                      /* IS  */
    ISOLATION = 482,               /* ISOLATION  */
    ISSUER_SYM = 483,              /* ISSUER_SYM  */
    ITERATE_SYM = 484,             /* ITERATE_SYM  */
    JOIN_SYM = 485,                /* JOIN_SYM  */
    KEYS = 486,                    /* KEYS  */
    KEY_BLOCK_SIZE = 487,          /* KEY_BLOCK_SIZE  */
    KEY_SYM = 488,                 /* KEY_SYM  */
    KILL_SYM = 489,                /* KILL_SYM  */
    LANGUAGE_SYM = 490,            /* LANGUAGE_SYM  */
    LAST_SYM = 491,                /* LAST_SYM  */
    LE = 492,                      /* LE  */
    LEADING = 493,                 /* LEADING  */
    LEAVES = 494,                  /* LEAVES  */
    LEAVE_SYM = 495,               /* LEAVE_SYM  */
    LEFT = 496,                    /* LEFT  */
    LESS_SYM = 497,                /* LESS_SYM  */
    LEVEL_SYM = 498,               /* LEVEL_SYM  */
    LEX_HOSTNAME = 499,            /* LEX_HOSTNAME  */
    LIKE = 500,                    /* LIKE  */
    LIMIT = 501,                   /* LIMIT  */
    LINEAR_SYM = 502,              /* LINEAR_SYM  */
    LINES = 503,                   /* LINES  */
    LINESTRING = 504,              /* LINESTRING  */
    LIST_SYM = 505,                /* LIST_SYM  */
    LOAD = 506,                    /* LOAD  */
    LOCAL_SYM = 507,               /* LOCAL_SYM  */
    LOCATOR_SYM = 508,             /* LOCATOR_SYM  */
    LOCKS_SYM = 509,               /* LOCKS_SYM  */
    LOCK_SYM = 510,                /* LOCK_SYM  */
    LOGFILE_SYM = 511,             /* LOGFILE_SYM  */
    LOGS_SYM = 512,                /* LOGS_SYM  */
    LONGBLOB = 513,                /* LONGBLOB  */
    LONGTEXT = 514,                /* LONGTEXT  */
    LONG_NUM = 515,                /* LONG_NUM  */
    LONG_SYM = 516,                /* LONG_SYM  */
    LOOP_SYM = 517,                /* LOOP_SYM  */
    LOW_PRIORITY = 518,            /* LOW_PRIORITY  */
    LT = 519,                      /* LT  */
    MASTER_CONNECT_RETRY_SYM = 520, /* MASTER_CONNECT_RETRY_SYM  */
    MASTER_HOST_SYM = 521,         /* MASTER_HOST_SYM  */
    MASTER_LOG_FILE_SYM = 522,     /* MASTER_LOG_FILE_SYM  */
    MASTER_LOG_POS_SYM = 523,      /* MASTER_LOG_POS_SYM  */
    MASTER_PASSWORD_SYM = 524,     /* MASTER_PASSWORD_SYM  */
    MASTER_PORT_SYM = 525,         /* MASTER_PORT_SYM  */
    MASTER_SERVER_ID_SYM = 526,    /* MASTER_SERVER_ID_SYM  */
    MASTER_SSL_CAPATH_SYM = 527,   /* MASTER_SSL_CAPATH_SYM  */
    MASTER_SSL_CA_SYM = 528,       /* MASTER_SSL_CA_SYM  */
    MASTER_SSL_CERT_SYM = 529,     /* MASTER_SSL_CERT_SYM  */
    MASTER_SSL_CIPHER_SYM = 530,   /* MASTER_SSL_CIPHER_SYM  */
    MASTER_SSL_KEY_SYM = 531,      /* MASTER_SSL_KEY_SYM  */
    MASTER_SSL_SYM = 532,          /* MASTER_SSL_SYM  */
    MASTER_SSL_VERIFY_SERVER_CERT_SYM = 533, /* MASTER_SSL_VERIFY_SERVER_CERT_SYM  */
    MASTER_SYM = 534,              /* MASTER_SYM  */
    MASTER_USER_SYM = 535,         /* MASTER_USER_SYM  */
    MATCH = 536,                   /* MATCH  */
    MAX_CONCURRENT_QUERIES = 537,  /* MAX_CONCURRENT_QUERIES  */
    MAX_CONCURRENT_TRANSACTIONS = 538, /* MAX_CONCURRENT_TRANSACTIONS  */
    MAX_CONNECTIONS_PER_HOUR = 539, /* MAX_CONNECTIONS_PER_HOUR  */
    MAX_QUERIES_PER_HOUR = 540,    /* MAX_QUERIES_PER_HOUR  */
    MAX_ROWS = 541,                /* MAX_ROWS  */
    MAX_SIZE_SYM = 542,            /* MAX_SIZE_SYM  */
    MAX_SYM = 543,                 /* MAX_SYM  */
    MAX_UPDATES_PER_HOUR = 544,    /* MAX_UPDATES_PER_HOUR  */
    MAX_USER_CONNECTIONS_SYM = 545, /* MAX_USER_CONNECTIONS_SYM  */
    MAX_VALUE_SYM = 546,           /* MAX_VALUE_SYM  */
    MEDIUMBLOB = 547,              /* MEDIUMBLOB  */
    MEDIUMINT = 548,               /* MEDIUMINT  */
    MEDIUMTEXT = 549,              /* MEDIUMTEXT  */
    MEDIUM_SYM = 550,              /* MEDIUM_SYM  */
    MEMCACHE_DIRTY = 551,          /* MEMCACHE_DIRTY  */
    MEMORY_SYM = 552,              /* MEMORY_SYM  */
    MERGE_SYM = 553,               /* MERGE_SYM  */
    MICROSECOND_SYM = 554,         /* MICROSECOND_SYM  */
    MIGRATE_SYM = 555,             /* MIGRATE_SYM  */
    MINUTE_MICROSECOND_SYM = 556,  /* MINUTE_MICROSECOND_SYM  */
    MINUTE_SECOND_SYM = 557,       /* MINUTE_SECOND_SYM  */
    MINUTE_SYM = 558,              /* MINUTE_SYM  */
    MIN_ROWS = 559,                /* MIN_ROWS  */
    MIN_SYM = 560,                 /* MIN_SYM  */
    MODE_SYM = 561,                /* MODE_SYM  */
    MODIFIES_SYM = 562,            /* MODIFIES_SYM  */
    MODIFY_SYM = 563,              /* MODIFY_SYM  */
    MOD_SYM = 564,                 /* MOD_SYM  */
    MONTH_SYM = 565,               /* MONTH_SYM  */
    MULTILINESTRING = 566,         /* MULTILINESTRING  */
    MULTIPOINT = 567,              /* MULTIPOINT  */
    MULTIPOLYGON = 568,            /* MULTIPOLYGON  */
    MUTEX_SYM = 569,               /* MUTEX_SYM  */
    NAMES_SYM = 570,               /* NAMES_SYM  */
    NAME_SYM = 571,                /* NAME_SYM  */
    NATIONAL_SYM = 572,            /* NATIONAL_SYM  */
    NATURAL = 573,                 /* NATURAL  */
    NCHAR_STRING = 574,            /* NCHAR_STRING  */
    NCHAR_SYM = 575,               /* NCHAR_SYM  */
    NDBCLUSTER_SYM = 576,          /* NDBCLUSTER_SYM  */
    NE = 577,                      /* NE  */
    NEG = 578,                     /* NEG  */
    NEW_SYM = 579,                 /* NEW_SYM  */
    NEXT_SYM = 580,                /* NEXT_SYM  */
    NODEGROUP_SYM = 581,           /* NODEGROUP_SYM  */
    NONE_SYM = 582,                /* NONE_SYM  */
    NOT2_SYM = 583,                /* NOT2_SYM  */
    NOT_SYM = 584,                 /* NOT_SYM  */
    NOW_SYM = 585,                 /* NOW_SYM  */
    NO_SLAVE_EXEC_SYM = 586,       /* NO_SLAVE_EXEC_SYM  */
    NO_SYM = 587,                  /* NO_SYM  */
    NO_WAIT_SYM = 588,             /* NO_WAIT_SYM  */
    NO_WRITE_TO_BINLOG = 589,      /* NO_WRITE_TO_BINLOG  */
    NULL_SYM = 590,                /* NULL_SYM  */
    NUM = 591,                     /* NUM  */
    NUMERIC_SYM = 592,             /* NUMERIC_SYM  */
    NVARCHAR_SYM = 593,            /* NVARCHAR_SYM  */
    OFFSET_SYM = 594,              /* OFFSET_SYM  */
    OLD_PASSWORD = 595,            /* OLD_PASSWORD  */
    ON = 596,                      /* ON  */
    ONE_SHOT_SYM = 597,            /* ONE_SHOT_SYM  */
    ONE_SYM = 598,                 /* ONE_SYM  */
    OPEN_SYM = 599,                /* OPEN_SYM  */
    OPEN_READ_CLOSE_SYM = 600,     /* OPEN_READ_CLOSE_SYM  */
    OPTIMIZE = 601,                /* OPTIMIZE  */
    OPTIONS_SYM = 602,             /* OPTIONS_SYM  */
    OPTION = 603,                  /* OPTION  */
    OPTIONALLY = 604,              /* OPTIONALLY  */
    OR2_SYM = 605,                 /* OR2_SYM  */
    ORDER_SYM = 606,               /* ORDER_SYM  */
    OR_OR_SYM = 607,               /* OR_OR_SYM  */
    OR_SYM = 608,                  /* OR_SYM  */
    OUTER = 609,                   /* OUTER  */
    OUTFILE = 610,                 /* OUTFILE  */
    OUT_SYM = 611,                 /* OUT_SYM  */
    OWNER_SYM = 612,               /* OWNER_SYM  */
    PACK_KEYS_SYM = 613,           /* PACK_KEYS_SYM  */
    PAGE_SYM = 614,                /* PAGE_SYM  */
    PARAM_MARKER = 615,            /* PARAM_MARKER  */
    PARSER_SYM = 616,              /* PARSER_SYM  */
    PARTIAL = 617,                 /* PARTIAL  */
    PARTITIONING_SYM = 618,        /* PARTITIONING_SYM  */
    PARTITIONS_SYM = 619,          /* PARTITIONS_SYM  */
    PARTITION_SYM = 620,           /* PARTITION_SYM  */
    PASSWORD = 621,                /* PASSWORD  */
    PHASE_SYM = 622,               /* PHASE_SYM  */
    PLUGINS_SYM = 623,             /* PLUGINS_SYM  */
    PLUGIN_SYM = 624,              /* PLUGIN_SYM  */
    POINT_SYM = 625,               /* POINT_SYM  */
    POLYGON = 626,                 /* POLYGON  */
    PORT_SYM = 627,                /* PORT_SYM  */
    POSITION_SYM = 628,            /* POSITION_SYM  */
    PRECISION = 629,               /* PRECISION  */
    PREPARE_SYM = 630,             /* PREPARE_SYM  */
    PRESERVE_SYM = 631,            /* PRESERVE_SYM  */
    PREV_SYM = 632,                /* PREV_SYM  */
    PRIMARY_SYM = 633,             /* PRIMARY_SYM  */
    PRIVILEGES = 634,              /* PRIVILEGES  */
    PROCEDURE = 635,               /* PROCEDURE  */
    PROCESS = 636,                 /* PROCESS  */
    PROCESSLIST_SYM = 637,         /* PROCESSLIST_SYM  */
    PROFILE_SYM = 638,             /* PROFILE_SYM  */
    PROFILES_SYM = 639,            /* PROFILES_SYM  */
    PURGE = 640,                   /* PURGE  */
    QUARTER_SYM = 641,             /* QUARTER_SYM  */
    QUERY_SYM = 642,               /* QUERY_SYM  */
    QUICK = 643,                   /* QUICK  */
    RANGE_SYM = 644,               /* RANGE_SYM  */
    READS_SYM = 645,               /* READS_SYM  */
    READ_ONLY_SYM = 646,           /* READ_ONLY_SYM  */
    READ_SYM = 647,                /* READ_SYM  */
    READ_WRITE_SYM = 648,          /* READ_WRITE_SYM  */
    REAL = 649,                    /* REAL  */
    REBUILD_SYM = 650,             /* REBUILD_SYM  */
    RECOVER_SYM = 651,             /* RECOVER_SYM  */
    REDOFILE_SYM = 652,            /* REDOFILE_SYM  */
    REDO_BUFFER_SIZE_SYM = 653,    /* REDO_BUFFER_SIZE_SYM  */
    REDUNDANT_SYM = 654,           /* REDUNDANT_SYM  */
    REFERENCES = 655,              /* REFERENCES  */
    REGEXP = 656,                  /* REGEXP  */
    RELAY_LOG_FILE_SYM = 657,      /* RELAY_LOG_FILE_SYM  */
    RELAY_LOG_POS_SYM = 658,       /* RELAY_LOG_POS_SYM  */
    RELAY_THREAD = 659,            /* RELAY_THREAD  */
    RELEASE_SYM = 660,             /* RELEASE_SYM  */
    RELOAD = 661,                  /* RELOAD  */
    REMOVE_SYM = 662,              /* REMOVE_SYM  */
    RENAME = 663,                  /* RENAME  */
    REORGANIZE_SYM = 664,          /* REORGANIZE_SYM  */
    REPAIR = 665,                  /* REPAIR  */
    REPEATABLE_SYM = 666,          /* REPEATABLE_SYM  */
    REPEAT_SYM = 667,              /* REPEAT_SYM  */
    REPLACE = 668,                 /* REPLACE  */
    REPLICATION = 669,             /* REPLICATION  */
    REQUIRE_SYM = 670,             /* REQUIRE_SYM  */
    RESET_SYM = 671,               /* RESET_SYM  */
    RESOURCES = 672,               /* RESOURCES  */
    RESTORE_SYM = 673,             /* RESTORE_SYM  */
    RESTRICT = 674,                /* RESTRICT  */
    RESUME_SYM = 675,              /* RESUME_SYM  */
    RETURNS_SYM = 676,             /* RETURNS_SYM  */
    RETURN_SYM = 677,              /* RETURN_SYM  */
    REVOKE = 678,                  /* REVOKE  */
    RIGHT = 679,                   /* RIGHT  */
    ROLLBACK_SYM = 680,            /* ROLLBACK_SYM  */
    ROLLUP_SYM = 681,              /* ROLLUP_SYM  */
    ROUTINE_SYM = 682,             /* ROUTINE_SYM  */
    ROWS_SYM = 683,                /* ROWS_SYM  */
    ROW_FORMAT_SYM = 684,          /* ROW_FORMAT_SYM  */
    ROW_SYM = 685,                 /* ROW_SYM  */
    RTREE_SYM = 686,               /* RTREE_SYM  */
    SAVEPOINT_SYM = 687,           /* SAVEPOINT_SYM  */
    SCHEDULE_SYM = 688,            /* SCHEDULE_SYM  */
    SECOND_MICROSECOND_SYM = 689,  /* SECOND_MICROSECOND_SYM  */
    SECOND_SYM = 690,              /* SECOND_SYM  */
    SECURITY_SYM = 691,            /* SECURITY_SYM  */
    SELECT_SYM = 692,              /* SELECT_SYM  */
    SENSITIVE_SYM = 693,           /* SENSITIVE_SYM  */
    SEPARATOR_SYM = 694,           /* SEPARATOR_SYM  */
    SERIALIZABLE_SYM = 695,        /* SERIALIZABLE_SYM  */
    SERIAL_SYM = 696,              /* SERIAL_SYM  */
    SESSION_SYM = 697,             /* SESSION_SYM  */
    SERVER_SYM = 698,              /* SERVER_SYM  */
    SERVER_OPTIONS = 699,          /* SERVER_OPTIONS  */
    SET = 700,                     /* SET  */
    SET_VAR = 701,                 /* SET_VAR  */
    SHARE_SYM = 702,               /* SHARE_SYM  */
    SHIFT_LEFT = 703,              /* SHIFT_LEFT  */
    SHIFT_RIGHT = 704,             /* SHIFT_RIGHT  */
    SHOW = 705,                    /* SHOW  */
    SHUTDOWN = 706,                /* SHUTDOWN  */
    SIGNED_SYM = 707,              /* SIGNED_SYM  */
    SIMPLE_SYM = 708,              /* SIMPLE_SYM  */
    SLAVE = 709,                   /* SLAVE  */
    SMALLINT = 710,                /* SMALLINT  */
    SNAPSHOT_SYM = 711,            /* SNAPSHOT_SYM  */
    SOCKET_SYM = 712,              /* SOCKET_SYM  */
    SONAME_SYM = 713,              /* SONAME_SYM  */
    SOUNDS_SYM = 714,              /* SOUNDS_SYM  */
    SOURCE_SYM = 715,              /* SOURCE_SYM  */
    SPATIAL_SYM = 716,             /* SPATIAL_SYM  */
    SPECIFIC_SYM = 717,            /* SPECIFIC_SYM  */
    SQLEXCEPTION_SYM = 718,        /* SQLEXCEPTION_SYM  */
    SQLSTATE_SYM = 719,            /* SQLSTATE_SYM  */
    SQLWARNING_SYM = 720,          /* SQLWARNING_SYM  */
    SQL_BIG_RESULT = 721,          /* SQL_BIG_RESULT  */
    SQL_BUFFER_RESULT = 722,       /* SQL_BUFFER_RESULT  */
    SQL_CACHE_SYM = 723,           /* SQL_CACHE_SYM  */
    SQL_CALC_FOUND_ROWS = 724,     /* SQL_CALC_FOUND_ROWS  */
    SQL_NO_CACHE_SYM = 725,        /* SQL_NO_CACHE_SYM  */
    SQL_NO_FCACHE_SYM = 726,       /* SQL_NO_FCACHE_SYM  */
    SQL_SMALL_RESULT = 727,        /* SQL_SMALL_RESULT  */
    SQL_SYM = 728,                 /* SQL_SYM  */
    SQL_THREAD = 729,              /* SQL_THREAD  */
    SSL_SYM = 730,                 /* SSL_SYM  */
    STARTING = 731,                /* STARTING  */
    STARTS_SYM = 732,              /* STARTS_SYM  */
    START_SYM = 733,               /* START_SYM  */
    STATUS_SYM = 734,              /* STATUS_SYM  */
    STDDEV_SAMP_SYM = 735,         /* STDDEV_SAMP_SYM  */
    STD_SYM = 736,                 /* STD_SYM  */
    STOP_SYM = 737,                /* STOP_SYM  */
    STORAGE_SYM = 738,             /* STORAGE_SYM  */
    STRAIGHT_JOIN = 739,           /* STRAIGHT_JOIN  */
    STRING_SYM = 740,              /* STRING_SYM  */
    SUBDATE_SYM = 741,             /* SUBDATE_SYM  */
    SUBJECT_SYM = 742,             /* SUBJECT_SYM  */
    SUBPARTITIONS_SYM = 743,       /* SUBPARTITIONS_SYM  */
    SUBPARTITION_SYM = 744,        /* SUBPARTITION_SYM  */
    SUBSTRING = 745,               /* SUBSTRING  */
    SUM_SYM = 746,                 /* SUM_SYM  */
    SUPER_SYM = 747,               /* SUPER_SYM  */
    SUSPEND_SYM = 748,             /* SUSPEND_SYM  */
    SWAPS_SYM = 749,               /* SWAPS_SYM  */
    SWITCHES_SYM = 750,            /* SWITCHES_SYM  */
    SYSDATE = 751,                 /* SYSDATE  */
    TABLES = 752,                  /* TABLES  */
    TABLESPACE = 753,              /* TABLESPACE  */
    TABLE_REF_PRIORITY = 754,      /* TABLE_REF_PRIORITY  */
    TABLE_SYM = 755,               /* TABLE_SYM  */
    STATISTICS_SYM = 756,          /* STATISTICS_SYM  */
    TABLE_CHECKSUM_SYM = 757,      /* TABLE_CHECKSUM_SYM  */
    TEMPORARY = 758,               /* TEMPORARY  */
    TEMPTABLE_SYM = 759,           /* TEMPTABLE_SYM  */
    TERMINATED = 760,              /* TERMINATED  */
    TEXT_STRING = 761,             /* TEXT_STRING  */
    TEXT_SYM = 762,                /* TEXT_SYM  */
    THAN_SYM = 763,                /* THAN_SYM  */
    THEN_SYM = 764,                /* THEN_SYM  */
    TIMESTAMP = 765,               /* TIMESTAMP  */
    TIMESTAMP_ADD = 766,           /* TIMESTAMP_ADD  */
    TIMESTAMP_DIFF = 767,          /* TIMESTAMP_DIFF  */
    TIME_SYM = 768,                /* TIME_SYM  */
    TINYBLOB = 769,                /* TINYBLOB  */
    TINYINT = 770,                 /* TINYINT  */
    TINYTEXT = 771,                /* TINYTEXT  */
    TO_SYM = 772,                  /* TO_SYM  */
    TRAILING = 773,                /* TRAILING  */
    TRANSACTION_SYM = 774,         /* TRANSACTION_SYM  */
    TRIGGERS_SYM = 775,            /* TRIGGERS_SYM  */
    TRIGGER_SYM = 776,             /* TRIGGER_SYM  */
    TRIM = 777,                    /* TRIM  */
    TRUE_SYM = 778,                /* TRUE_SYM  */
    TRUNCATE_SYM = 779,            /* TRUNCATE_SYM  */
    TYPES_SYM = 780,               /* TYPES_SYM  */
    TYPE_SYM = 781,                /* TYPE_SYM  */
    UDF_RETURNS_SYM = 782,         /* UDF_RETURNS_SYM  */
    ULONGLONG_NUM = 783,           /* ULONGLONG_NUM  */
    UNCOMMITTED_SYM = 784,         /* UNCOMMITTED_SYM  */
    UNDEFINED_SYM = 785,           /* UNDEFINED_SYM  */
    UNDERSCORE_CHARSET = 786,      /* UNDERSCORE_CHARSET  */
    UNDOFILE_SYM = 787,            /* UNDOFILE_SYM  */
    UNDO_BUFFER_SIZE_SYM = 788,    /* UNDO_BUFFER_SIZE_SYM  */
    UNDO_SYM = 789,                /* UNDO_SYM  */
    UNICODE_SYM = 790,             /* UNICODE_SYM  */
    UNINSTALL_SYM = 791,           /* UNINSTALL_SYM  */
    UNION_SYM = 792,               /* UNION_SYM  */
    UNIQUE_SYM = 793,              /* UNIQUE_SYM  */
    UNKNOWN_SYM = 794,             /* UNKNOWN_SYM  */
    UNLOCK_SYM = 795,              /* UNLOCK_SYM  */
    UNSIGNED = 796,                /* UNSIGNED  */
    UNTIL_SYM = 797,               /* UNTIL_SYM  */
    UPDATE_SYM = 798,              /* UPDATE_SYM  */
    UPGRADE_SYM = 799,             /* UPGRADE_SYM  */
    USAGE = 800,                   /* USAGE  */
    USER = 801,                    /* USER  */
    USE_FRM = 802,                 /* USE_FRM  */
    USE_SYM = 803,                 /* USE_SYM  */
    USING = 804,                   /* USING  */
    UTC_DATE_SYM = 805,            /* UTC_DATE_SYM  */
    UTC_TIMESTAMP_SYM = 806,       /* UTC_TIMESTAMP_SYM  */
    UTC_TIME_SYM = 807,            /* UTC_TIME_SYM  */
    VALUES = 808,                  /* VALUES  */
    VALUE_SYM = 809,               /* VALUE_SYM  */
    VARBINARY = 810,               /* VARBINARY  */
    VARCHAR = 811,                 /* VARCHAR  */
    VARIABLES = 812,               /* VARIABLES  */
    VARIANCE_SYM = 813,            /* VARIANCE_SYM  */
    VARYING = 814,                 /* VARYING  */
    VAR_SAMP_SYM = 815,            /* VAR_SAMP_SYM  */
    VIEW_SYM = 816,                /* VIEW_SYM  */
    WAIT_SYM = 817,                /* WAIT_SYM  */
    WARNINGS = 818,                /* WARNINGS  */
    WEEK_SYM = 819,                /* WEEK_SYM  */
    WHEN_SYM = 820,                /* WHEN_SYM  */
    WHERE = 821,                   /* WHERE  */
    WHILE_SYM = 822,               /* WHILE_SYM  */
    WITH = 823,                    /* WITH  */
    WORK_SYM = 824,                /* WORK_SYM  */
    WRAPPER_SYM = 825,             /* WRAPPER_SYM  */
    WRITE_SYM = 826,               /* WRITE_SYM  */
    X509_SYM = 827,                /* X509_SYM  */
    XA_SYM = 828,                  /* XA_SYM  */
    XOR = 829,                     /* XOR  */
    YEAR_MONTH_SYM = 830,          /* YEAR_MONTH_SYM  */
    YEAR_SYM = 831,                /* YEAR_SYM  */
    ZEROFILL = 832                 /* ZEROFILL  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif
/* Token kinds.  */
#define YYEMPTY -2
#define YYEOF 0
#define YYerror 256
#define YYUNDEF 257
#define ABORT_SYM 258
#define ACCESSIBLE_SYM 259
#define ACTION 260
#define ADD 261
#define ADDDATE_SYM 262
#define AFTER_SYM 263
#define AGAINST 264
#define AGGREGATE_SYM 265
#define ALGORITHM_SYM 266
#define ALL 267
#define ALTER 268
#define ANALYZE_SYM 269
#define AND_AND_SYM 270
#define AND_SYM 271
#define ANY_SYM 272
#define AS 273
#define ASC 274
#define ASCII_SYM 275
#define ASENSITIVE_SYM 276
#define ASYNC_COMMIT_SYM 277
#define AT_SYM 278
#define AUTHORS_SYM 279
#define AUTOEXTEND_SIZE_SYM 280
#define AUTO_INC 281
#define AVG_ROW_LENGTH 282
#define AVG_SYM 283
#define BACKUP_SYM 284
#define BEFORE_SYM 285
#define BEGIN_SYM 286
#define BETWEEN_SYM 287
#define BIGINT 288
#define BINARY 289
#define BINLOG_SYM 290
#define BIN_NUM 291
#define BIT_AND 292
#define BIT_OR 293
#define BIT_SYM 294
#define BIT_XOR 295
#define BLOB_SYM 296
#define BLOCK_SYM 297
#define BOOLEAN_SYM 298
#define BOOL_SYM 299
#define BOTH 300
#define BTREE_SYM 301
#define BY 302
#define BYTE_SYM 303
#define CACHE_SYM 304
#define CALL_SYM 305
#define CASCADE 306
#define CASCADED 307
#define CASE_SYM 308
#define CAST_SYM 309
#define CHAIN_SYM 310
#define CHANGE 311
#define CHANGED 312
#define CHARSET 313
#define CHAR_SYM 314
#define CHECKSUM_SYM 315
#define CHECK_SYM 316
#define CIPHER_SYM 317
#define CLIENT_SYM 318
#define CLOSE_SYM 319
#define COALESCE 320
#define CODE_SYM 321
#define COLLATE_SYM 322
#define COLLATION_SYM 323
#define COLUMNS 324
#define COLUMN_SYM 325
#define COMMENT_SYM 326
#define COMMITTED_SYM 327
#define COMMIT_SYM 328
#define COMPACT_SYM 329
#define COMPLETION_SYM 330
#define COMPRESSED_SYM 331
#define CONCURRENT 332
#define CONDITION_SYM 333
#define CONNECTION_SYM 334
#define CONSISTENT_SYM 335
#define CONSTRAINT 336
#define CONTAINS_SYM 337
#define CONTEXT_SYM 338
#define CONTINUE_SYM 339
#define CONTRIBUTORS_SYM 340
#define CONVERT_SYM 341
#define COUNT_SYM 342
#define CPU_SYM 343
#define CREATE 344
#define CROSS 345
#define CUBE_SYM 346
#define CURDATE 347
#define CURRENT_USER 348
#define CURSOR_SYM 349
#define CURTIME 350
#define DATABASE 351
#define DATABASES 352
#define DATAFILE_SYM 353
#define DATA_SYM 354
#define DATETIME 355
#define DATE_ADD_INTERVAL 356
#define DATE_SUB_INTERVAL 357
#define DATE_SYM 358
#define DAY_HOUR_SYM 359
#define DAY_MICROSECOND_SYM 360
#define DAY_MINUTE_SYM 361
#define DAY_SECOND_SYM 362
#define DAY_SYM 363
#define DEALLOCATE_SYM 364
#define DECIMAL_NUM 365
#define DECIMAL_SYM 366
#define DECLARE_SYM 367
#define DEFAULT 368
#define DEFINER_SYM 369
#define DELAYED_SYM 370
#define DELAY_KEY_WRITE_SYM 371
#define DELETE_SYM 372
#define DESC 373
#define DESCRIBE 374
#define DES_KEY_FILE 375
#define DETERMINISTIC_SYM 376
#define DIRECTORY_SYM 377
#define DISABLE_SYM 378
#define DISCARD 379
#define DISK_SYM 380
#define DISTINCT 381
#define DIV_SYM 382
#define DOUBLE_SYM 383
#define DO_SYM 384
#define DROP 385
#define DUAL_SYM 386
#define DUMPFILE 387
#define DUPLICATE_SYM 388
#define DYNAMIC_SYM 389
#define EACH_SYM 390
#define ELSE 391
#define ELSEIF_SYM 392
#define ENABLE_SYM 393
#define ENCLOSED 394
#define END 395
#define ENDS_SYM 396
#define END_OF_INPUT 397
#define ENGINES_SYM 398
#define ENGINE_SYM 399
#define ENUM 400
#define EQ 401
#define EQUAL_SYM 402
#define ERRORS 403
#define ESCAPED 404
#define ESCAPE_SYM 405
#define EVENTS_SYM 406
#define EVENT_SYM 407
#define EVERY_SYM 408
#define EXECUTE_SYM 409
#define EXISTS 410
#define EXIT_SYM 411
#define EXPANSION_SYM 412
#define EXTENDED_SYM 413
#define EXTENT_SIZE_SYM 414
#define EXTRACT_SYM 415
#define FALSE_SYM 416
#define FAST_SYM 417
#define FAULTS_SYM 418
#define FETCH_SYM 419
#define FILE_SYM 420
#define FIRST_SYM 421
#define FIXED_SYM 422
#define FLOAT_NUM 423
#define FLOAT_SYM 424
#define FLUSH_SYM 425
#define FORCE_SYM 426
#define FOREIGN 427
#define FOR_SYM 428
#define FOUND_SYM 429
#define FRAC_SECOND_SYM 430
#define FROM 431
#define FULL 432
#define FULLTEXT_SYM 433
#define FUNCTION_SYM 434
#define GE 435
#define GEOMETRYCOLLECTION 436
#define GEOMETRY_SYM 437
#define GET_FORMAT 438
#define GLOBAL_SYM 439
#define GRANT 440
#define GRANTS 441
#define GROUP_SYM 442
#define GROUP_CONCAT_SYM 443
#define GT_SYM 444
#define HANDLER_SYM 445
#define HASH_SYM 446
#define HAVING 447
#define HELP_SYM 448
#define HEX_NUM 449
#define HIGH_PRIORITY 450
#define HOST_SYM 451
#define HOSTS_SYM 452
#define HOUR_MICROSECOND_SYM 453
#define HOUR_MINUTE_SYM 454
#define HOUR_SECOND_SYM 455
#define HOUR_SYM 456
#define IDENT 457
#define IDENTIFIED_SYM 458
#define IDENT_QUOTED 459
#define IF 460
#define IGNORE_SYM 461
#define IMPORT 462
#define INDEXES 463
#define INDEX_SYM 464
#define INFILE 465
#define INITIAL_SIZE_SYM 466
#define INNER_SYM 467
#define INNOBASE_SYM 468
#define INOUT_SYM 469
#define INSENSITIVE_SYM 470
#define INSERT 471
#define INSERT_METHOD 472
#define INSTALL_SYM 473
#define INTERVAL_SYM 474
#define INTO 475
#define INT_SYM 476
#define INVOKER_SYM 477
#define IN_SYM 478
#define IO_SYM 479
#define IPC_SYM 480
#define IS 481
#define ISOLATION 482
#define ISSUER_SYM 483
#define ITERATE_SYM 484
#define JOIN_SYM 485
#define KEYS 486
#define KEY_BLOCK_SIZE 487
#define KEY_SYM 488
#define KILL_SYM 489
#define LANGUAGE_SYM 490
#define LAST_SYM 491
#define LE 492
#define LEADING 493
#define LEAVES 494
#define LEAVE_SYM 495
#define LEFT 496
#define LESS_SYM 497
#define LEVEL_SYM 498
#define LEX_HOSTNAME 499
#define LIKE 500
#define LIMIT 501
#define LINEAR_SYM 502
#define LINES 503
#define LINESTRING 504
#define LIST_SYM 505
#define LOAD 506
#define LOCAL_SYM 507
#define LOCATOR_SYM 508
#define LOCKS_SYM 509
#define LOCK_SYM 510
#define LOGFILE_SYM 511
#define LOGS_SYM 512
#define LONGBLOB 513
#define LONGTEXT 514
#define LONG_NUM 515
#define LONG_SYM 516
#define LOOP_SYM 517
#define LOW_PRIORITY 518
#define LT 519
#define MASTER_CONNECT_RETRY_SYM 520
#define MASTER_HOST_SYM 521
#define MASTER_LOG_FILE_SYM 522
#define MASTER_LOG_POS_SYM 523
#define MASTER_PASSWORD_SYM 524
#define MASTER_PORT_SYM 525
#define MASTER_SERVER_ID_SYM 526
#define MASTER_SSL_CAPATH_SYM 527
#define MASTER_SSL_CA_SYM 528
#define MASTER_SSL_CERT_SYM 529
#define MASTER_SSL_CIPHER_SYM 530
#define MASTER_SSL_KEY_SYM 531
#define MASTER_SSL_SYM 532
#define MASTER_SSL_VERIFY_SERVER_CERT_SYM 533
#define MASTER_SYM 534
#define MASTER_USER_SYM 535
#define MATCH 536
#define MAX_CONCURRENT_QUERIES 537
#define MAX_CONCURRENT_TRANSACTIONS 538
#define MAX_CONNECTIONS_PER_HOUR 539
#define MAX_QUERIES_PER_HOUR 540
#define MAX_ROWS 541
#define MAX_SIZE_SYM 542
#define MAX_SYM 543
#define MAX_UPDATES_PER_HOUR 544
#define MAX_USER_CONNECTIONS_SYM 545
#define MAX_VALUE_SYM 546
#define MEDIUMBLOB 547
#define MEDIUMINT 548
#define MEDIUMTEXT 549
#define MEDIUM_SYM 550
#define MEMCACHE_DIRTY 551
#define MEMORY_SYM 552
#define MERGE_SYM 553
#define MICROSECOND_SYM 554
#define MIGRATE_SYM 555
#define MINUTE_MICROSECOND_SYM 556
#define MINUTE_SECOND_SYM 557
#define MINUTE_SYM 558
#define MIN_ROWS 559
#define MIN_SYM 560
#define MODE_SYM 561
#define MODIFIES_SYM 562
#define MODIFY_SYM 563
#define MOD_SYM 564
#define MONTH_SYM 565
#define MULTILINESTRING 566
#define MULTIPOINT 567
#define MULTIPOLYGON 568
#define MUTEX_SYM 569
#define NAMES_SYM 570
#define NAME_SYM 571
#define NATIONAL_SYM 572
#define NATURAL 573
#define NCHAR_STRING 574
#define NCHAR_SYM 575
#define NDBCLUSTER_SYM 576
#define NE 577
#define NEG 578
#define NEW_SYM 579
#define NEXT_SYM 580
#define NODEGROUP_SYM 581
#define NONE_SYM 582
#define NOT2_SYM 583
#define NOT_SYM 584
#define NOW_SYM 585
#define NO_SLAVE_EXEC_SYM 586
#define NO_SYM 587
#define NO_WAIT_SYM 588
#define NO_WRITE_TO_BINLOG 589
#define NULL_SYM 590
#define NUM 591
#define NUMERIC_SYM 592
#define NVARCHAR_SYM 593
#define OFFSET_SYM 594
#define OLD_PASSWORD 595
#define ON 596
#define ONE_SHOT_SYM 597
#define ONE_SYM 598
#define OPEN_SYM 599
#define OPEN_READ_CLOSE_SYM 600
#define OPTIMIZE 601
#define OPTIONS_SYM 602
#define OPTION 603
#define OPTIONALLY 604
#define OR2_SYM 605
#define ORDER_SYM 606
#define OR_OR_SYM 607
#define OR_SYM 608
#define OUTER 609
#define OUTFILE 610
#define OUT_SYM 611
#define OWNER_SYM 612
#define PACK_KEYS_SYM 613
#define PAGE_SYM 614
#define PARAM_MARKER 615
#define PARSER_SYM 616
#define PARTIAL 617
#define PARTITIONING_SYM 618
#define PARTITIONS_SYM 619
#define PARTITION_SYM 620
#define PASSWORD 621
#define PHASE_SYM 622
#define PLUGINS_SYM 623
#define PLUGIN_SYM 624
#define POINT_SYM 625
#define POLYGON 626
#define PORT_SYM 627
#define POSITION_SYM 628
#define PRECISION 629
#define PREPARE_SYM 630
#define PRESERVE_SYM 631
#define PREV_SYM 632
#define PRIMARY_SYM 633
#define PRIVILEGES 634
#define PROCEDURE 635
#define PROCESS 636
#define PROCESSLIST_SYM 637
#define PROFILE_SYM 638
#define PROFILES_SYM 639
#define PURGE 640
#define QUARTER_SYM 641
#define QUERY_SYM 642
#define QUICK 643
#define RANGE_SYM 644
#define READS_SYM 645
#define READ_ONLY_SYM 646
#define READ_SYM 647
#define READ_WRITE_SYM 648
#define REAL 649
#define REBUILD_SYM 650
#define RECOVER_SYM 651
#define REDOFILE_SYM 652
#define REDO_BUFFER_SIZE_SYM 653
#define REDUNDANT_SYM 654
#define REFERENCES 655
#define REGEXP 656
#define RELAY_LOG_FILE_SYM 657
#define RELAY_LOG_POS_SYM 658
#define RELAY_THREAD 659
#define RELEASE_SYM 660
#define RELOAD 661
#define REMOVE_SYM 662
#define RENAME 663
#define REORGANIZE_SYM 664
#define REPAIR 665
#define REPEATABLE_SYM 666
#define REPEAT_SYM 667
#define REPLACE 668
#define REPLICATION 669
#define REQUIRE_SYM 670
#define RESET_SYM 671
#define RESOURCES 672
#define RESTORE_SYM 673
#define RESTRICT 674
#define RESUME_SYM 675
#define RETURNS_SYM 676
#define RETURN_SYM 677
#define REVOKE 678
#define RIGHT 679
#define ROLLBACK_SYM 680
#define ROLLUP_SYM 681
#define ROUTINE_SYM 682
#define ROWS_SYM 683
#define ROW_FORMAT_SYM 684
#define ROW_SYM 685
#define RTREE_SYM 686
#define SAVEPOINT_SYM 687
#define SCHEDULE_SYM 688
#define SECOND_MICROSECOND_SYM 689
#define SECOND_SYM 690
#define SECURITY_SYM 691
#define SELECT_SYM 692
#define SENSITIVE_SYM 693
#define SEPARATOR_SYM 694
#define SERIALIZABLE_SYM 695
#define SERIAL_SYM 696
#define SESSION_SYM 697
#define SERVER_SYM 698
#define SERVER_OPTIONS 699
#define SET 700
#define SET_VAR 701
#define SHARE_SYM 702
#define SHIFT_LEFT 703
#define SHIFT_RIGHT 704
#define SHOW 705
#define SHUTDOWN 706
#define SIGNED_SYM 707
#define SIMPLE_SYM 708
#define SLAVE 709
#define SMALLINT 710
#define SNAPSHOT_SYM 711
#define SOCKET_SYM 712
#define SONAME_SYM 713
#define SOUNDS_SYM 714
#define SOURCE_SYM 715
#define SPATIAL_SYM 716
#define SPECIFIC_SYM 717
#define SQLEXCEPTION_SYM 718
#define SQLSTATE_SYM 719
#define SQLWARNING_SYM 720
#define SQL_BIG_RESULT 721
#define SQL_BUFFER_RESULT 722
#define SQL_CACHE_SYM 723
#define SQL_CALC_FOUND_ROWS 724
#define SQL_NO_CACHE_SYM 725
#define SQL_NO_FCACHE_SYM 726
#define SQL_SMALL_RESULT 727
#define SQL_SYM 728
#define SQL_THREAD 729
#define SSL_SYM 730
#define STARTING 731
#define STARTS_SYM 732
#define START_SYM 733
#define STATUS_SYM 734
#define STDDEV_SAMP_SYM 735
#define STD_SYM 736
#define STOP_SYM 737
#define STORAGE_SYM 738
#define STRAIGHT_JOIN 739
#define STRING_SYM 740
#define SUBDATE_SYM 741
#define SUBJECT_SYM 742
#define SUBPARTITIONS_SYM 743
#define SUBPARTITION_SYM 744
#define SUBSTRING 745
#define SUM_SYM 746
#define SUPER_SYM 747
#define SUSPEND_SYM 748
#define SWAPS_SYM 749
#define SWITCHES_SYM 750
#define SYSDATE 751
#define TABLES 752
#define TABLESPACE 753
#define TABLE_REF_PRIORITY 754
#define TABLE_SYM 755
#define STATISTICS_SYM 756
#define TABLE_CHECKSUM_SYM 757
#define TEMPORARY 758
#define TEMPTABLE_SYM 759
#define TERMINATED 760
#define TEXT_STRING 761
#define TEXT_SYM 762
#define THAN_SYM 763
#define THEN_SYM 764
#define TIMESTAMP 765
#define TIMESTAMP_ADD 766
#define TIMESTAMP_DIFF 767
#define TIME_SYM 768
#define TINYBLOB 769
#define TINYINT 770
#define TINYTEXT 771
#define TO_SYM 772
#define TRAILING 773
#define TRANSACTION_SYM 774
#define TRIGGERS_SYM 775
#define TRIGGER_SYM 776
#define TRIM 777
#define TRUE_SYM 778
#define TRUNCATE_SYM 779
#define TYPES_SYM 780
#define TYPE_SYM 781
#define UDF_RETURNS_SYM 782
#define ULONGLONG_NUM 783
#define UNCOMMITTED_SYM 784
#define UNDEFINED_SYM 785
#define UNDERSCORE_CHARSET 786
#define UNDOFILE_SYM 787
#define UNDO_BUFFER_SIZE_SYM 788
#define UNDO_SYM 789
#define UNICODE_SYM 790
#define UNINSTALL_SYM 791
#define UNION_SYM 792
#define UNIQUE_SYM 793
#define UNKNOWN_SYM 794
#define UNLOCK_SYM 795
#define UNSIGNED 796
#define UNTIL_SYM 797
#define UPDATE_SYM 798
#define UPGRADE_SYM 799
#define USAGE 800
#define USER 801
#define USE_FRM 802
#define USE_SYM 803
#define USING 804
#define UTC_DATE_SYM 805
#define UTC_TIMESTAMP_SYM 806
#define UTC_TIME_SYM 807
#define VALUES 808
#define VALUE_SYM 809
#define VARBINARY 810
#define VARCHAR 811
#define VARIABLES 812
#define VARIANCE_SYM 813
#define VARYING 814
#define VAR_SAMP_SYM 815
#define VIEW_SYM 816
#define WAIT_SYM 817
#define WARNINGS 818
#define WEEK_SYM 819
#define WHEN_SYM 820
#define WHERE 821
#define WHILE_SYM 822
#define WITH 823
#define WORK_SYM 824
#define WRAPPER_SYM 825
#define WRITE_SYM 826
#define X509_SYM 827
#define XA_SYM 828
#define XOR 829
#define YEAR_MONTH_SYM 830
#define YEAR_SYM 831
#define ZEROFILL 832

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 637 "sql_yacc.yy"

  int  num;
  ulong ulong_num;
  ulonglong ulonglong_number;
  longlong longlong_number;
  LEX_STRING lex_str;
  LEX_STRING *lex_str_ptr;
  LEX_SYMBOL symbol;
  Table_ident *table;
  char *simple_string;
  Item *item;
  Item_num *item_num;
  List<Item> *item_list;
  List<String> *string_list;
  String *string;
  Key_part_spec *key_part;
  TABLE_LIST *table_list;
  udf_func *udf;
  LEX_USER *lex_user;
  struct sys_var_with_base variable;
  enum enum_var_type var_type;
  Key::Keytype key_type;
  enum ha_key_alg key_alg;
  handlerton *db_type;
  enum row_type row_type;
  enum ha_rkey_function ha_rkey_mode;
  enum enum_tx_isolation tx_isolation;
  enum Cast_target cast_type;
  enum Item_udftype udf_type;
  CHARSET_INFO *charset;
  thr_lock_type lock_type;
  interval_type interval, interval_time_st;
  timestamp_type date_time_type;
  st_select_lex *select_lex;
  chooser_compare_func_creator boolfunc2creator;
  struct sp_cond_type *spcondtype;
  struct { int vars, conds, hndlrs, curs; } spblock;
  sp_name *spname;
  struct st_lex *lex;
  sp_head *sphead;
  struct p_elem_val *p_elem_value;
  enum index_hint_type index_hint;

#line 1265 "sql_yacc.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif




int MYSQLparse (void *yythd);

/* "%code provides" blocks.  */
#line 28 "sql_yacc.yy"

  /* Bison 3.x generates yyerror with parse-param, provide wrapper */
  #define MYSQLerror(ctx, msg) MYSQLerror_impl(msg)

#line 1284 "sql_yacc.h"

#endif /* !YY_MYSQL_SQL_YACC_H_INCLUDED  */
