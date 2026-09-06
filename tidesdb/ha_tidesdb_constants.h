/*
  Copyright (c) 2026 TidesDB Corp.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef HA_TIDESDB_CONSTANTS_H
#define HA_TIDESDB_CONSTANTS_H

/* compile-time constants shared across the plugin: key-format and row-format layout, spatial and
   full-text encodings, cost-model and statistics tunables, and the library default mirrors.  this
   header is included by ha_tidesdb.h immediately after the server base headers, so it relies on the
   server types (uchar, uint, ha_rows, MAX_KEY, MAX_KEY_LENGTH, time_t) already being visible. */

#include <cstdint>

/* Mirror constants for the library's TDB_DEFAULT_* values defined in
   <tidesdb/tidesdb.h>.  We don't include that header directly because it
   leaks a `realloc` macro that conflicts with MariaDB's String::realloc()
   method.  Only the defaults the plugin's per-CF sysvars actually consume
   are mirrored here; keep them in sync with src/tidesdb.h on every library
   bump so drift shows up at this one spot rather than scattered across the
   sysvar declarations. */
static constexpr unsigned long long TIDESQL_DEFAULT_LEVEL_SIZE_RATIO = 10;
static constexpr unsigned long long TIDESQL_DEFAULT_MIN_LEVELS = 1;
static constexpr unsigned long long TIDESQL_DEFAULT_DIVIDING_LEVEL_OFFSET = 1;
static constexpr unsigned long long TIDESQL_DEFAULT_BTREE_KLOG_BLOCK_SIZE = 4096;

/* Key namespace prefixes (first byte of every TidesDB key) */
static constexpr uint8_t KEY_NS_META = 0x00;

/* Auto-increment start value persisted in the data column family under the meta namespace so a
   CREATE/ALTER ... AUTO_INCREMENT=N survives a restart even when the table has no rows to recover
   the counter from.  Key is [KEY_NS_META]["AINC"]; value is the next value as an 8-byte big-endian
   unsigned integer.  Sorts before every data key (KEY_NS_DATA), so it never disturbs a last-row
   seek. */
static constexpr uint8_t AUTOINC_META_KEY[] = {KEY_NS_META, 'A', 'I', 'N', 'C'};
static constexpr uint AUTOINC_META_KEY_LEN = 5;
static constexpr uint AUTOINC_META_VALUE_LEN = 8;
static constexpr uint8_t KEY_NS_DATA = 0x01;

/* Size of the namespace prefix that every TidesDB key starts with. */
static constexpr uint KEY_NAMESPACE_LEN = 1;

/* Buffer size for a data CF key, namespace byte + comparable PK + 1 byte slack.
   Used by every site that builds KEY_NS_DATA + pk via build_data_key. */
static constexpr uint DATA_KEY_BUF_LEN = KEY_NAMESPACE_LEN + MAX_KEY_LENGTH + 1;

/* Buffer size for a secondary-index CF entry key, comparable index-column
   bytes (up to MAX_KEY_LENGTH) + appended PK bytes (up to MAX_KEY_LENGTH)
   + 2 bytes of slack that covers VARBINARY length-byte overflow emitted
   by make_comparable_key. */
static constexpr uint SEC_IDX_KEY_BUF_LEN = (MAX_KEY_LENGTH * 2) + 2;

/* Number of doubles in a 2-D minimum bounding rectangle.  Always four
   (xmin, ymin, xmax, ymax); used for the on-disk spatial value layout
   and the in-memory query-MBR cache on the handler. */
static constexpr uint SPATIAL_MBR_DIMS = 4;

/* CF naming */
static constexpr const char CF_INDEX_INFIX[] = "__idx_";

/* Hidden primary key size (tables without explicit PK) */
static constexpr size_t HIDDEN_PK_SIZE = sizeof(uint64_t);

/* Maximum number of secondary indexes we support */
static constexpr uint MAX_TIDESDB_KEYS = MAX_KEY;

/* Cost model constants for the optimizer */
static constexpr double TIDESDB_COST_SEQ_READ = 0.00005;
static constexpr double TIDESDB_COST_KEY_READ = 0.00003;
static constexpr double TIDESDB_COST_RANGE_SETUP = 0.0001;
static constexpr double TIDESDB_DEFAULT_READ_AMP = 1.0;

/* Stats cache refresh interval (microseconds) */
static constexpr long long TIDESDB_STATS_REFRESH_US = 2000000LL; /* 2 seconds */

/* Minimum stats.records to avoid optimizer edge cases with 0 rows */
static constexpr ha_rows TIDESDB_MIN_STATS_RECORDS = 2;

/* scan_time() -- split the sorted-run overlap count reported by
   tidesdb_range_stats between MariaDB's I/O and CPU cost buckets.  LSM scans
   are mostly block-read bound, so the weighting leans heavily to I/O. */
static constexpr double TIDESDB_SCAN_IO_WEIGHT = 0.9;
static constexpr double TIDESDB_SCAN_CPU_WEIGHT = 0.1;

/* records_in_range() fallback estimate when no table share is available yet. */
static constexpr ha_rows TIDESDB_RIR_DEFAULT_EST = 10;

/* Sentinel bytes for building full-range bounds that pass through
   tidesdb_range_stats or seek primitives.  KEY_INF_HI_BYTE fills upper
   bound buffers with 0xFF.  KEY_INF_LO_BYTE seeds the smallest possible
   first byte for secondary-index lower bounds (primary uses KEY_NS_DATA). */
static constexpr uint8_t KEY_INF_HI_BYTE = 0xFF;
static constexpr uint8_t KEY_INF_LO_BYTE = 0x00;

/* Row format constants.  Every row written by serialize_row carries the
   header [ROW_HEADER_MAGIC][null_bytes(2 LE)][field_count(2 LE)] for a
   total of ROW_HEADER_SIZE bytes; deserialize_row reads them back to
   support instant ADD/DROP COLUMN. */
static constexpr uchar ROW_HEADER_MAGIC = 0xFE;
static constexpr uint ROW_HEADER_SIZE = 5;

/* Length prefix Field::pack writes ahead of a wide VARCHAR payload.
   Two bytes covers VARCHAR above 255 chars; narrower columns use a
   single-byte prefix. */
static constexpr uint FIELD_VARCHAR_LEN_PREFIX = 2;

/* Sign-bit XOR mask used to translate a signed integer's MSB into
   sortable form (and back).  Big-endian sort keys flip this bit so
   negative values sort below positive ones lexicographically. */
static constexpr uint8_t INT_SORT_SIGN_FLIP_MASK = 0x80;

/* MariaDB packed-field widths used by sort-key decoders. */
static constexpr uint DATE_PACK_LEN = 3;
static constexpr uint DATETIME_MAX_PACK_LEN = 8;

/* Separator that joins db and table names when forming a TidesDB CF name
   from a MariaDB path (e.g. "test/foo" -> "test__foo"). */
static constexpr const char CF_DB_TABLE_SEP[] = "__";

/* MariaDB temp-table marker character.  Internal temp/exchange tables
   carry one or more '#' in their on-disk name (e.g. "#sql-..."); we
   substitute '_' so the resulting CF name remains valid. */
static constexpr char MARIADB_TEMP_NAME_MARKER = '#';
static constexpr char MARIADB_TEMP_NAME_REPLACEMENT = '_';

/* Relative-path prefix that MariaDB prepends to table paths handed
   to handler callbacks ("./db/table").  path_to_cf_name strips it
   before extracting db/table components. */
static constexpr const char MARIADB_REL_PATH_PREFIX[] = "./";
static constexpr size_t MARIADB_REL_PATH_PREFIX_LEN = 2;

/* MariaDB sort-key null-indicator bytes prepended to nullable key parts
   in make_comparable_key.  Convention 0 sorts NULLs first under memcmp,
   1 marks a present value. */
static constexpr uchar SORT_KEY_NULL = 0;
static constexpr uchar SORT_KEY_NOT_NULL = 1;

/* Slot indices into the 4-double MBR layout used by spatial_qmbr_ and
   tdb_mbr_t-shaped buffers.  Order matches the on-disk SPATIAL_MBR_VALUE_LEN
   layout [xmin, ymin, xmax, ymax]. */
static constexpr uint MBR_XMIN_IDX = 0;
static constexpr uint MBR_YMIN_IDX = 1;
static constexpr uint MBR_XMAX_IDX = 2;
static constexpr uint MBR_YMAX_IDX = 3;

/* Inclusive bounds of the full 64-bit Hilbert value space.  Used when a
   spatial query has no decomposable cells (e.g. HA_READ_MBR_DISJOINT) and
   we have to scan the entire curve. */
static constexpr uint64_t HILBERT_RANGE_FULL_LO = 0;
static constexpr uint64_t HILBERT_RANGE_FULL_HI = UINT64_MAX;

/* Minimum number of point ranges in a multi-range request before our
   custom MRR path takes over from MariaDB's default implementation.
   Single-range plans bypass MRR and stay on the index_read_map fast path. */
static constexpr uint MRR_ACCEPT_MIN_RANGES = 2;

/* Selectivity values used in info() / analyze() for index rec_per_key.
   UNIQUE exactly one row per distinct value.  FLOOR smallest plausible
   estimate so the optimizer never sees rec_per_key=0 (treated as "unknown"). */
static constexpr ulong REC_PER_KEY_UNIQUE = 1;
static constexpr ulong REC_PER_KEY_FLOOR = 1;

/* Divisor used to compute the centroid of an MBR ((min + max) / 2) when
   building a Hilbert spatial index key.  The centroid is the point that
   feeds hilbert_xy2d_64 -- the MBR corners themselves are stored in the
   value, not the key. */
static constexpr double MBR_CENTROID_DIV = 2.0;

/* Multiplier used to convert a 0..1 ratio (cache hit rate, etc.) into
   a percentage for human-readable status output. */
static constexpr double PERCENT_SCALE = 100.0;

/* First row id assigned to a freshly created (or fully truncated)
   hidden-PK table.  Row ids are one-based so that "0" remains a clean
   sentinel for "no row id yet" / "uninitialized". */
static constexpr uint64_t HIDDEN_PK_FIRST_ROW_ID = 1;

/* Read-amplification value reported when TidesDB has not yet collected
   enough statistics to compute a real read_amp.  1.0 means "one disk op
   per logical op" -- the optimistic baseline that won't penalize plans. */
static constexpr double READ_AMP_NONE = 1.0;

/* Per-document delta values for fts_update_meta when maintaining the
   FTS metadata row alongside DML.  ADD/DEL track whether a document
   was inserted or removed; word-count deltas use the matching sign. */
static constexpr int FTS_DOC_DELTA_ADD = 1;
static constexpr int FTS_DOC_DELTA_DEL = -1;

/* Default ENCRYPTION_KEY_ID applied when a table is opened with
   encryption enabled but no explicit key id is provided.  Mirrors the
   default in the ENCRYPTION_KEY_ID HA_TOPTION_NUMBER declaration. */
static constexpr uint TIDESDB_DEFAULT_ENCRYPTION_KEY_ID = 1;

/* Sentinel value stored in TidesDB_share::ttl_field_idx when no TTL
   column is configured for the table.  Valid TTL field indexes are
   non-negative; >= 0 implies a TTL_COL column is present. */
static constexpr int TIDESDB_TTL_FIELD_NONE = -1;

/* Fallback divisor when rec_per_key is unset for a non-unique secondary
   index in info().  Estimate is total_records / N, biasing toward more
   selective lookups (10 ~= one decimal order of magnitude). */
static constexpr ha_rows STATS_REC_PER_KEY_FALLBACK_DIVISOR = 10;

/* Auto-sample secondary-index cardinality once a table crosses this row count.
   Below it the fallback divisor is close enough and any full scan it mis-drives
   is cheap, so the one-time sampling scan is not worth paying.  Above it the
   fallback is systematically wrong for a high-cardinality secondary index and
   the optimizer full-scans IN-lists and ranges, so we sample for real. */
static constexpr ha_rows STATS_AUTO_SAMPLE_MIN_ROWS = 1024;

/* Number of bits per byte for shift-based byte (de)serialization in the
   spatial encoder/decoder loops.  Equivalent to CHAR_BIT on POSIX. */
static constexpr uint BITS_PER_BYTE = 8;

/* Floor for total_docs in the IDF denominator.  Guards std::log from a
   divide-by-zero when no documents have been indexed yet.  The rest of the BM25
   scoring constants live with the scorer in src/core/fts_score. */
static constexpr int64_t BM25_MIN_TOTAL_DOCS = 1;

/* Inplace index builds rows between mid-txn commits and between
   thd_killed polls. */
static constexpr ha_rows TIDESDB_INDEX_BUILD_BATCH = 100;

/* Bulk DML ops between mid-txn commits during start_bulk_insert /
   start_bulk_update / start_bulk_delete.  Counts both the primary put
   and each secondary-index put. */
static constexpr ha_rows TIDESDB_BULK_INSERT_BATCH_OPS = 500;

/* Deferred data-key cap for a range-tombstone bulk DELETE.  While a bulk delete defers its
   primary-row tombstones to coalesce them into one range tombstone, the keys buffer in memory and
   the mid-txn commit is held back, so a delete larger than this flushes the buffer as per-row
   tombstones and finishes on the ordinary path.  Sized so the buffered keys stay a few megabytes.
 */
static constexpr size_t TIDESDB_BULK_DELETE_DEFER_CAP = 262144;

/* Fewest buffered rows worth verifying for a range tombstone.  Below this a bulk delete just writes
   the buffered keys as per-row tombstones, since the completeness scan would cost more than the few
   tombstones it might save. */
static constexpr size_t TIDESDB_BULK_DELETE_MIN_RANGE = 64;

/* Encryption */
static constexpr uint TIDESDB_ENC_IV_LEN = 16;
static constexpr uint TIDESDB_ENC_KEY_LEN = 32;

/* Bytes of key-version prefix on every encrypted row blob.  The on-disk
   layout is the 4-byte little-endian key version, then the IV, then the
   ciphertext, so a row always decrypts under the exact key version it was
   written with and survives an encryption key rotation. */
static constexpr uint TIDESDB_ENC_VERSION_LEN = 4;

/* Bloom filter FPR conversion (table option stores parts per 10000) */
static constexpr double TIDESDB_BLOOM_FPR_DIVISOR = 10000.0;

/* Tombstone density trigger conversion (table option stores parts per
   10000; library config is a 0.0..1.0 ratio). */
static constexpr double TIDESDB_TOMBSTONE_DENSITY_DIVISOR = 10000.0;

/* TTL sentinel value meaning no expiration */
static constexpr time_t TIDESDB_TTL_NONE = (time_t)-1;

/* Default block cache size (bytes) */
static constexpr ulonglong TIDESDB_DEFAULT_BLOCK_CACHE = 256ULL * 1024 * 1024; /* 256M */

#endif /* HA_TIDESDB_CONSTANTS_H */
