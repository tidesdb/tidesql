---
title: Vector Search
description: Approximate nearest-neighbor search through MariaDB's MHNSW index, stored by TidesDB.
---

# Vector Search

TidesDB supports approximate nearest-neighbor search through MariaDB's built-in MHNSW vector index.
The server handles graph construction and search, and TidesDB provides the storage for both the
table data and the hidden MHNSW graph.

```sql
CREATE TABLE embeddings (
  id INT NOT NULL PRIMARY KEY, title VARCHAR(200),
  v VECTOR(384) NOT NULL, VECTOR INDEX (v)
) ENGINE=TIDESDB;

INSERT INTO embeddings VALUES (1, 'cat picture', Vec_FromText('[0.1, 0.9, ...]'));
```

## Searching

Vector search uses `ORDER BY VEC_DISTANCE_EUCLIDEAN()` or `VEC_DISTANCE_COSINE()` with a `LIMIT`.
The MHNSW index returns approximate neighbors without scanning the whole table:

```sql
SELECT id, title, VEC_DISTANCE_EUCLIDEAN(v, Vec_FromText('[0.15, 0.85, ...]')) AS dist
FROM embeddings ORDER BY dist LIMIT 5;

SELECT id, title, VEC_DISTANCE_COSINE(v, Vec_FromText('[0.15, 0.85, ...]')) AS dist
FROM embeddings ORDER BY dist LIMIT 5;
```

## Index options

The MHNSW index accepts two optional parameters, both handled by the server:

```sql
CREATE TABLE docs (
  id INT PRIMARY KEY, v VECTOR(128) NOT NULL,
  VECTOR INDEX (v) M=12 DISTANCE='cosine'
) ENGINE=TIDESDB;
```

`M` is the number of neighbors per graph node, default 6, range 3 to 200. Higher values improve
recall at the cost of slower inserts and more memory. `DISTANCE` selects the metric, `euclidean`
(default) or `cosine`.

## DML and limitations

All DML works on vector-indexed tables. INSERT adds the vector to the graph, DELETE removes it, and
UPDATE on the vector column invalidates the old graph node and inserts a new one. The engine handles
the interleaved `record[0]`/`record[1]` access pattern that the MHNSW maintenance uses for
BLOB-backed vector data.

A partitioned table cannot carry a vector index, and MariaDB's MHNSW implementation supports one
vector index per table.
