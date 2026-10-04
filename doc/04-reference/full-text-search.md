---
title: Full-Text Search
description: FULLTEXT indexes with natural-language and boolean modes, BM25 ranking, tokenization, stop words, and blend characters.
---

# Full-Text Search

TidesDB supports `FULLTEXT` indexes for natural-language and boolean full-text search with BM25
relevance ranking. Each `FULLTEXT` index is a dedicated column family holding an inverted index,
where each entry maps a term-document pair to its term frequency and document length.

```sql
CREATE TABLE articles (
  id INT NOT NULL PRIMARY KEY, title VARCHAR(200), body TEXT,
  FULLTEXT ft_content (title, body)
) ENGINE=TIDESDB;
```

## Natural-language mode

The default mode tokenizes the query, scans the inverted index for each term, computes BM25 scores,
and returns results ordered by score:

```sql
SELECT id, title, MATCH(title, body) AGAINST('tutorial') AS score
FROM articles WHERE MATCH(title, body) AGAINST('tutorial')
ORDER BY score DESC;
```

Multi-term queries accumulate BM25 scores across terms, so documents matching more terms with
higher frequencies rank higher, normalized by document length.

## Boolean mode

Boolean mode supports required `+`, excluded `-`, prefix wildcard `*`, and exact phrase `"..."`:

```sql
SELECT * FROM articles WHERE MATCH(title, body) AGAINST('+mysql +tutorial' IN BOOLEAN MODE);
SELECT * FROM articles WHERE MATCH(title, body) AGAINST('+mysql -tutorial' IN BOOLEAN MODE);
SELECT * FROM articles WHERE MATCH(title, body) AGAINST('optim*' IN BOOLEAN MODE);
SELECT * FROM articles WHERE MATCH(title, body) AGAINST('"database management system"' IN BOOLEAN MODE);
SELECT * FROM articles WHERE MATCH(title, body) AGAINST('+"management system" -tutorial' IN BOOLEAN MODE);
```

A phrase query uses the inverted index to find candidate documents containing all phrase words, then
verifies the exact sequence by re-tokenizing the document.

## BM25 ranking

Scores use the Okapi BM25 algorithm:

```
IDF(t) = ln(1 + (N - df + 0.5) / (df + 0.5))
score  = IDF * (tf * (k1+1)) / (tf + k1 * (1 - b + b * |d| / avgdl))
```

`N` is the document count, `df` the document frequency of the term, `tf` its frequency in the
document, `|d|` the document length in tokens, and `avgdl` the average document length. The `+1`
inside the logarithm keeps every term's IDF non-negative, matching the Lucene variant. `k1` and `b`
are set by `tidesdb_fts_bm25_k1` (default 1.2) and `tidesdb_fts_bm25_b` (default 0.75).

## Tokenization

The tokenizer is charset-aware. A word is a run of characters the column's charset classifies as
alphanumeric, and every multi-byte character counts as a word character, so a script written without
spaces, such as CJK, indexes one token per unbroken run. Tokens are lowercased with the charset's
case-folding rules. Words shorter than `tidesdb_fts_min_word_len` (default 3) or longer than
`tidesdb_fts_max_word_len` (default 84) characters are excluded from the index, from
natural-language queries, and from phrases. A plain boolean-mode term is looked up as typed, so a
required term outside those bounds, or on the stop-word list, matches no row.

## Stop words

Common words are excluded from the index. By default TidesDB uses the same default list as InnoDB
(`information_schema.INNODB_FT_DEFAULT_STOPWORD`). Stop words are filtered during tokenization, so
they are never stored and never match. The list is customizable with `tidesdb_ft_stopword_table`,
which names a `db_name/table_name` table whose only column is a `value` VARCHAR holding one word
per row, since the loader reads each stored row as that single packed column.
The stop-word table must itself be a TidesDB table, since the loader resolves it as a TidesDB column
family. A `SET GLOBAL` naming a missing table, a table on another engine, or a value not in
`db_name/table_name` form fails with `ER_WRONG_VALUE_FOR_VAR` and the variable keeps its old value.
A TidesDB table of another shape is accepted, and any row that does not hold a single VARCHAR is
skipped. Each word is lowercased with utf8mb4 case rules, so an uppercase word outside ASCII still
matches the lowercased text. A value set in my.cnf is loaded at startup, and if that table cannot be
read the server logs a warning and keeps the default list.
Setting it to NULL or empty restores the default:

```sql
CREATE TABLE mydb.my_stopwords (value VARCHAR(50)) ENGINE=TidesDB;
INSERT INTO mydb.my_stopwords (value) VALUES ('custom'), ('words'), ('here');
SET GLOBAL tidesdb_ft_stopword_table = 'mydb/my_stopwords';
```

After changing the stop-word table, rebuild existing FULLTEXT indexes with
`ALTER TABLE ... DROP INDEX ..., ADD FULLTEXT INDEX ...` so they reflect the new list.

## Blend characters

A blend character is treated as both a separator and a valid word character. When one appears inside
a token the tokenizer emits both the full blended form and the sub-parts, which makes Romance-language
elision and apostrophe names searchable by any component or the whole form. With
`tidesdb_fts_blend_chars = "'"`:

| Input | Indexed tokens |
|-------|----------------|
| `L'aria` | `l'aria`, `aria` |
| `Dell'aria` | `dell'aria`, `dell`, `aria` |
| `O'Malley` | `o'malley`, `malley` |

```sql
SET GLOBAL tidesdb_fts_blend_chars = "'";
```

The default is empty. The setting is global and applies to subsequent indexing and queries, so
rebuild existing indexes after changing it.

## Multi-column indexes and maintenance

A single `FULLTEXT` index can span several columns, and the engine concatenates their text into one
document for tokenization and scoring:

```sql
CREATE TABLE docs (
  id INT NOT NULL PRIMARY KEY, title VARCHAR(200), summary TEXT, body TEXT,
  FULLTEXT (title, summary, body)
) ENGINE=TIDESDB;
```

Index entries are maintained inside the same transaction as the row change. An insert tokenizes the
document and writes one entry per unique term, a delete removes the entries, and an update that
changes the indexed columns or the primary key removes the old entries and writes new ones. An
update on a table with a TTL also rewrites the row's entries with its new expiry, or with none. The
document count and total word count behind BM25's average document length are maintained atomically
in the data column family.
