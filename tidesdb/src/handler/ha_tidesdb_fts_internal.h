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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/

#ifndef HA_TIDESDB_FTS_INTERNAL_H
#define HA_TIDESDB_FTS_INTERNAL_H

/* module-private surface shared between the two full-text translation units, the index and
   maintenance side (ha_tidesdb_fts.cc) and the search side (ha_tidesdb_fts_search.cc). these are
   not part of the handler's public full-text api; nothing outside the module includes this header.
   include after ha_tidesdb.h so the server types and the full-text core types are visible. */

#include <string>
#include <vector>

#include "src/core/fts_text.h"
#include "src/handler/ha_tidesdb_fts.h"

/* the module leans on the server-free core for the parsed-term type, the tokenizer, the boolean
   parser, and the phrase matcher, so a boolean query term is tidesdb::fts::query_term and a token
   is a plain lowercased std::string, the same representation the core tests exercise. */

/**
 * fts_load_meta
 * read the per-index document and word totals from the data column family
 * @param txn the reading transaction
 * @param data_cf the data column family the meta key lives in
 * @param keynr the full-text index number
 * @param total_docs receives the indexed document count, zeroed on a fresh index
 * @param total_words receives the total indexed word count, zeroed on a fresh index
 * @return TDB_SUCCESS on a found row, TDB_ERR_NOT_FOUND for a fresh index, else the library error
 */
int fts_load_meta(tidesdb_txn_t *txn, tidesdb_column_family_t *data_cf, uint keynr,
                  int64_t *total_docs, int64_t *total_words);

/**
 * fts_tokenize
 * fold, length-filter, and stop-word-filter a text run into lowercased index tokens by snapshotting
 * the tunable dictionaries and delegating to the server-free core tokenizer
 * @param text the text bytes
 * @param text_len the text length
 * @param cs the charset driving multi-byte folding and word boundaries
 * @param out appended with the surviving tokens in document order
 */
void fts_tokenize(const char *text, size_t text_len, CHARSET_INFO *cs,
                  std::vector<std::string> &out);

/**
 * fts_parse_boolean
 * parse a boolean-mode query into its required, excluded, wildcard, phrase, and plain terms by
 * snapshotting the tunable dictionaries and delegating to the server-free core parser
 * @param query the query bytes
 * @param len the query length
 * @param cs the charset driving multi-byte word scanning
 * @param out appended with the parsed terms in query order
 */
void fts_parse_boolean(const char *query, size_t len, CHARSET_INFO *cs,
                       std::vector<tidesdb::fts::query_term> &out);

#endif /* HA_TIDESDB_FTS_INTERNAL_H */
