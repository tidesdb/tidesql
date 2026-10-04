---
title: Statistics and the Optimizer
description: The cached statistics the engine keeps, how they feed the cost model, and how range cardinality is estimated.
---

# Statistics and the Optimizer

The engine keeps cached statistics refreshed at most every two seconds, the total key count, the
total data size of the data and index column families, the mean record length from the average key
and value sizes, and the LSM-tree read amplification factor, alongside a sampled per-index
`rec_per_key` selectivity for each secondary index. They are atomic variables on the shared
table descriptor and feed MariaDB's cost-based optimizer through `info()`, `scan_time()`,
`keyread_time()`, `rnd_pos_time()`, and `records_in_range()`.

Secondary-index `rec_per_key` is measured automatically. The first time a populated table, one that
has reached 1,024 rows, is asked for constant statistics, the engine samples the distinct index
prefixes of each non-unique secondary index, reading up to 100,000 entries per index on its own
short READ COMMITTED transaction, and caches the result, once per table share. A workload never has
to run `ANALYZE TABLE` to get real selectivity; until the sample lands a high-cardinality index
falls back to a coarse fraction of the row count, which is what would otherwise steer the optimizer
into scanning an IN-list or a range rather than using the index. An explicit `ANALYZE TABLE` still
refreshes the same figures.

The cost model accounts for an LSM read consulting several levels. The read amplification factor,
from the library's statistics, scales the cost of point lookups and random-position reads. A higher
read amplification nudges the optimizer toward sequential scans, and when the data is well compacted
and the amplification is low, index lookups are cheap.

## Cost methods

- `scan_time()` starts from the base `handler::scan_time()` row-count cost and adds an LSM surcharge
  for the SSTable overlap the scan has to merge across. The surcharge is the count of overlapping
  SSTables weighted 90% as I/O and 10% as CPU, so the same row count costs more to scan when it is
  spread across many overlapping SSTables than when it sits in a compacted shape. The surcharge
  reflects overlap only, not compression or merge policy.
- `keyread_time()` models index reads as `rows * 0.00003 * read_amp + ranges * 0.0001`, since each
  point lookup touches `read_amp` levels and a range scan amortizes the merge-heap setup across rows.
- `rnd_pos_time()` models random-position lookups as `rows * 0.00005 * read_amp`, reflecting that
  each random fetch is a point-get through the full LSM stack.

`keyread_time()` and `rnd_pos_time()` charge everything to CPU and report no I/O cost. At
execution a range read opens a range-bounded iterator that only visits the SSTables overlapping the
range. An equality range, such as each value of an `IN` list, instead reuses one unbounded iterator
for the statement and seeks it, as long as the handler has not written in that statement, since an
iterator does not see writes made after it was opened. The scan iterator is freed at the end of
every statement.

## Range-aware cardinality

`records_in_range()` takes one of two paths.

For a point equality, where both bounds convert to identical comparable bytes such as `WHERE k = 5`,
a unique or primary key carries its `rec_per_key` for the key parts given, one row for the full key,
and a non-unique index that ANALYZE or the open-time pass
has already sampled carries a trustworthy `rec_per_key`, so both read that cached estimate directly.
Only a non-unique index with no sample yet needs more. There the value bytes encode the index value
without its primary-key suffix, so every matching row stores a key with that value as a prefix, and
the matching rows are exactly the half-open range from the value to its successor. The engine probes
that span with `tidesdb_range_stats`, which counts the rows carrying that one value and is right for
a low- or high-cardinality index alike, so a never-analyzed table gets a real estimate, clamped
between one and the table's row count, rather than the `records / 10` fallback that overestimates a
high-cardinality index and drives a full scan. The successor is the value with its last byte below
`0xFF` incremented and the trailing bytes dropped, and an all-`0xFF` value that has no finite
successor falls back to the cached estimate.

For a range predicate the engine asks the library for the range's live key count through
`tidesdb_range_stats` and returns it as an absolute cardinality. The count is memtable-aware, so a
range whose data has not been flushed yet reports its real cardinality. A range small enough to walk
is counted exactly, and a wider one is estimated from SSTable metadata without walking, taking each
SSTable's keys in proportion to the share of its key span the range covers. The estimate is then
clamped to the table's cached row count so the optimizer never prices a range wider than the whole
table. When the library cannot produce an estimate the engine falls back to a quarter of the table's
rows plus one. A narrow range returns a small estimate and a wide one a proportionally larger
estimate, which lets the optimizer choose indexes and join order sensibly.
