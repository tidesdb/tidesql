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

/* the bulk DML lifecycle, the start and end hooks of a large INSERT, UPDATE or DELETE and the
   mid-statement commit that flushes its writes in bounded batches, together with the deferred
   bulk delete that collapses a whole primary key span into one range tombstone.  the row paths
   themselves are in ha_tidesdb_dml.cc and ha_tidesdb_dml_update.cc. */

#include "ha_tidesdb.h"

#include <mysql/plugin.h>

#include <cstring>
#include <string>
#include <vector>

#include "key.h"
#include "sql_class.h"
#include "sql_priv.h"
#include "src/handler/ha_tidesdb_fts.h"
#include "src/handler/ha_tidesdb_internal.h"

/* ******************** Bulk DML ******************** */

/*
  Commit the current txn mid-statement and reset it with READ_COMMITTED so
  the next batch starts fresh.  Shared by bulk INSERT/UPDATE/DELETE once
  buffered ops cross TIDESDB_BULK_INSERT_BATCH_OPS, which bounds the memory the
  txn holds before commit since the whole write set lives in the ops array
  until then.  The reset drops to READ_COMMITTED so the read set does not grow
  across batches the way a higher isolation level would.

  Any cached iterators and dup-check iterators are invalidated, they hold
  references to MERGE_SOURCE_TXN_OPS that txn_reset clears.

  If the inner commit fails (e.g. transient TDB_ERR_UNKNOWN from a memtable
  rotation race) we must surface that to the SQL layer.  Returning
  0 here while the buffered ops are gone causes silent data loss -- the
  caller (write_row / update_row / delete_row) reports success even though
  up to TIDESDB_BULK_INSERT_BATCH_OPS rows were dropped on the floor.
  Instead, rollback to release the txn's state, swap in a fresh txn so the
  connection is left in a valid state for any retry, and propagate the
  error code so MariaDB rolls the statement back and surfaces it to the
  client (typically as ER_ERROR_DURING_COMMIT).
*/
int ha_tidesdb::maybe_bulk_commit(tidesdb_trx_t *trx)
{
    if (!trx || !trx->txn) return 0;

    /* Folded FTS meta deltas have to land in the same txn as the row puts
       they account for, so flush them before the mid-statement commit. */
    int frc = flush_trx_fts_meta_pending(cached_thd_, trx);
    if (frc != TDB_SUCCESS) return tdb_rc_to_ha(frc, "bulk_commit fts_meta_flush");

    int crc = tdb_txn_commit_blocking(cached_thd_, trx->txn);
    if (crc != TDB_SUCCESS)
    {
        sql_print_error(
            "[TIDESDB] bulk mid-commit failed rc=%d -- aborting statement to "
            "avoid silent row loss",
            crc);
        /* Release the txn's buffered state.  Even if rollback itself fails
           we still free+begin below so the connection is usable. */
        (void)tidesdb_txn_rollback(trx->txn);
        tidesdb_txn_free(trx->txn);
        trx->txn = NULL;
        int brc =
            tidesdb_txn_begin_with_isolation(tdb_global, TDB_ISOLATION_READ_COMMITTED, &trx->txn);
        if (brc != TDB_SUCCESS) return tdb_rc_to_ha(brc, "bulk_commit txn_begin(after_fail)");
        trx->txn_generation++;
        stmt_txn = trx->txn;
        scan_txn = trx->txn;
        if (scan_iter)
        {
            tidesdb_iter_free(scan_iter);
            scan_iter = NULL;
            scan_iter_cf_ = NULL;
            scan_iter_txn_ = NULL;
        }
        /* The txn was torn down, so the armed statement savepoint is void;
           disarm so a later statement rollback falls back to a full rollback
           rather than a stale partial one. */
        trx->stmt_savepoint_active = false;
        trx->stmt_fts_snapshot.clear();
        return tdb_rc_to_ha(crc, "bulk_commit");
    }

    /* Carry on in a fresh transaction rather than resetting this one.  The source of an
       INSERT ... SELECT or an ALTER copy is scanned by another handler through an iterator opened
       under this same transaction, and a reset detaches it, which ends that scan early with no
       error and silently drops every row it had not reached. */
    int rrc = tdb_trx_retire_and_begin(trx, TDB_ISOLATION_READ_COMMITTED);
    if (rrc != TDB_SUCCESS) return tdb_rc_to_ha(rrc, "bulk_commit txn_begin");
    trx->isolation_level = TDB_ISOLATION_READ_COMMITTED;

    stmt_txn = trx->txn;
    trx->txn_generation++;

    /* This handler's own scan, a DELETE walking the rows it removes, keeps reading through its
       iterator under the retired transaction, which stays valid until the statement's real commit.
       Freeing it here ended that scan at the first batch, and the DELETE then reported success
       having removed only that batch.  The next statement sees the generation change and opens a
       fresh iterator under the new transaction. */
    /* The mid-statement commit reset the txn, so the armed statement savepoint
       no longer means anything.  Disarm it -- the remainder of this statement
       can no longer be rolled back atomically (the committed rows are durable),
       matching prior behavior. */
    trx->stmt_savepoint_active = false;
    trx->stmt_fts_snapshot.clear();
    return 0;
}

void ha_tidesdb::start_bulk_insert(ha_rows rows, uint flags)
{
    in_bulk_insert_ = true;
    bulk_insert_ops_ = 0;
    bulk_may_commit_ = bulk_statement_may_commit();

    /* A bulk statement flushes its writes in bounded batches through
       maybe_bulk_commit, and after the first batch it resets the transaction
       to READ_COMMITTED so the later ones take no commit-time reservation.
       The first batch, though, still commits at whatever level the session
       opened the transaction with, SNAPSHOT for a BEGIN block, and that
       reservation aborts conservatively whenever another committer's
       in-flight key happens to share a slot in the reservation table.  Two
       loaders writing disjoint keys therefore collide on their first batch,
       and since a real commit hands the transaction back at the session
       level, the exposure comes back after every COMMIT the loader issues.
       Bulk runs at READ_COMMITTED by design, so start it there when the
       transaction carries no writes yet.  One that already holds writes is
       left alone, because a reset would discard them.

       Only a statement that will actually reach the batch threshold and
       mid-commit is exposed, and the reset costs the transaction its read
       snapshot, which a small multi-row INSERT inside a BEGIN block relies on
       keeping.  So a statement that stays under the threshold is left to
       commit with its transaction as before, and only one that would have
       dropped to READ_COMMITTED after its first batch anyway is moved there
       up front.  An unknown row count, which LOAD DATA passes as zero, is
       left alone rather than guessed at. */
    if (!bulk_may_commit_ || rows == 0 ||
        rows * (1 + share->num_secondary_indexes) < TIDESDB_BULK_INSERT_BATCH_OPS)
        return;
    tidesdb_trx_t *trx = cached_trx_;
    if (!trx || !trx->txn || trx->dirty || trx->isolation_level <= TDB_ISOLATION_READ_COMMITTED)
        return;

    /* Switch to a fresh transaction rather than resetting this one, since the statement's source
       table may already have a scan open under it, see maybe_bulk_commit. */
    int rrc = tdb_trx_retire_and_begin(trx, TDB_ISOLATION_READ_COMMITTED);
    if (rrc != TDB_SUCCESS)
    {
        sql_print_warning(
            "[TIDESDB] bulk start could not begin a read committed transaction (rc=%d), "
            "leaving the transaction at its session level",
            rrc);
        return;
    }
    trx->isolation_level = TDB_ISOLATION_READ_COMMITTED;
    trx->txn_generation++;
    stmt_txn = trx->txn;
    if (scan_iter)
    {
        tidesdb_iter_free(scan_iter);
        scan_iter = NULL;
        scan_iter_cf_ = NULL;
        scan_iter_txn_ = NULL;
    }
    scan_txn = trx->txn;
    /* The reset voids the statement savepoint armed in external_lock.  With
       no writes in the transaction a statement rollback and a full rollback
       come to the same thing, so disarming it changes nothing. */
    trx->stmt_savepoint_active = false;
    trx->stmt_fts_snapshot.clear();
}

int ha_tidesdb::end_bulk_insert()
{
    in_bulk_insert_ = false;
    return 0;
}

/*
  start_bulk_update returns 0 when the engine will handle bulk batching.
  We then flip the flag that update_row checks at its tail so every row
  contributes to the shared ops counter.
*/
bool ha_tidesdb::start_bulk_update()
{
    /* The server calls this only for an engine that advertises HA_CAN_FORCE_BULK_UPDATE, which
       TidesDB does not, so an UPDATE never batches today.  It is kept consistent with the insert
       and delete paths should that flag be advertised. */
    in_bulk_update_ = true;
    bulk_insert_ops_ = 0;
    bulk_may_commit_ = bulk_statement_may_commit();
    return 0;
}

int ha_tidesdb::end_bulk_update()
{
    in_bulk_update_ = false;
    return 0;
}

/*
  MariaDB calls bulk_update_row instead of update_row when start_bulk_update
  returned 0.  We don't actually buffer rows (TidesDB's txn is the buffer);
  we just delegate so the standard update_row path runs and its tail-side
  mid-commit block batches.  dup_key_found tracks duplicate-key collisions
  found in buffered-but-not-yet-applied rows -- since we apply immediately,
  it's always zero.
*/
int ha_tidesdb::bulk_update_row(const uchar *old_data, const uchar *new_data,
                                ha_rows *dup_key_found)
{
    DBUG_ENTER("ha_tidesdb::bulk_update_row");
    if (dup_key_found) *dup_key_found = 0;
    DBUG_RETURN(update_row(old_data, new_data));
}

/* build the smallest key strictly greater than every key beginning with the given data key, so a
   half-open range from the low bound to this successor covers an inclusive [min, max] span.  the
   last byte below 0xFF is incremented and the rest dropped; an all-0xFF key has no finite successor
   and returns false. */
static bool data_key_successor(const uchar *in, uint in_len, uchar *out, uint &out_len)
{
    if (!in || in_len == 0) return false;
    memcpy(out, in, in_len);
    for (uint i = in_len; i > 0; i--)
    {
        if ((uint8_t)out[i - 1] != KEY_INF_HI_BYTE)
        {
            out[i - 1]++;
            out_len = i;
            return true;
        }
    }
    return false;
}

/* count the live data-key rows in the half-open span [lo, hi) of a column family, used to confirm a
   deferred bulk delete removed every row the span holds before it collapses to a range tombstone.
 */
static int count_live_data_keys(THD *thd, tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                                const uchar *lo, uint lo_len, const uchar *hi, uint hi_len,
                                uint64_t *out_count)
{
    *out_count = 0;
    tidesdb_iter_t *it = NULL;
    int rc = tdb_iter_new_range_blocking(thd, txn, cf, lo, lo_len, hi, hi_len, &it);
    if (rc != TDB_SUCCESS || !it) return rc != TDB_SUCCESS ? rc : TDB_ERR_IO;
    /* The count must be exact.  One cut short by a failed read could match the deleted count while
       rows the DELETE kept sit uncounted in the span, and the range tombstone would then remove
       them, so any failure is returned and the caller falls back to per-row tombstones. */
    rc = tdb_iter_seek_to_first_blocking(thd, it);
    while (!tdb_iter_failed(rc) && tidesdb_iter_valid(it))
    {
        uint8_t *k = NULL;
        size_t ks = 0;
        rc = tidesdb_iter_key(it, &k, &ks);
        if (rc != TDB_SUCCESS)
        {
            if (rc == TDB_ERR_NOT_FOUND) rc = TDB_ERR_IO;
            break;
        }
        tdb_owned_buf kg(k);
        if (is_data_key(k, ks)) (*out_count)++;
        rc = tidesdb_iter_next(it);
    }
    tidesdb_iter_free(it);
    return tdb_iter_failed(rc) ? rc : TDB_SUCCESS;
}

int ha_tidesdb::bulk_delete_flush_buffered(tidesdb_txn_t *txn)
{
    for (const std::string &k : bulk_delete_keys_)
    {
        int rc = tdb_txn_delete_cf_blocking(cached_thd_, txn, share->cf, (const uchar *)k.data(),
                                            (uint)k.size(), cached_single_delete_primary_);
        if (rc != TDB_SUCCESS) return tdb_rc_to_ha(rc, "bulk delete flush");
    }
    bulk_delete_keys_.clear();
    return 0;
}

bool ha_tidesdb::start_bulk_delete()
{
    in_bulk_delete_ = true;
    bulk_insert_ops_ = 0;
    bulk_may_commit_ = bulk_statement_may_commit();
    bulk_delete_rows_ = 0;
    bulk_delete_min_pk_.clear();
    bulk_delete_max_pk_.clear();
    bulk_delete_keys_.clear();
    /* defer primary-row tombstones into one range tombstone only for a plain user-pk table with no
       triggers of any kind, since deferral must not hide a row from a trigger reading the table
       mid-statement, and never under galera, where a deferred range tombstone would not line up
       with the per-row certification the write path already issued. */
    bulk_delete_defer_ = share && share->has_user_pk && share->cf && table->triggers == nullptr;
#ifdef WITH_WSREP
    if (wsrep_on(cached_thd_ ? cached_thd_ : ha_thd())) bulk_delete_defer_ = false;
#endif
    return 0;
}

int ha_tidesdb::end_bulk_delete()
{
    in_bulk_delete_ = false;
    bool did_range_tombstone = false;

    /* Resolve a deferred bulk delete.  Every buffered key is a row this statement deleted, and with
       the tombstones deferred those rows are still live, so the live rows in [min, max] equal the
       buffered count exactly when the statement removed the whole span.  In that case one range
       tombstone replaces the run; a survivor (a residual filter, an IN-list gap, an ICP reject)
       leaves more live rows than buffered keys, so the buffer falls back to per-row tombstones. */
    if (bulk_delete_defer_ && !bulk_delete_keys_.empty())
    {
        if (int erc = ensure_stmt_txn()) return erc;
        tidesdb_txn_t *txn = stmt_txn;

        /* Only a run large enough to repay the completeness scan attempts a range tombstone.  When
           the live rows in [min, max] equal the buffered count, one range tombstone replaces the
           whole run; otherwise the buffer falls back to per-row tombstones. */
        bool ranged = false;
        if (bulk_delete_keys_.size() >= TIDESDB_BULK_DELETE_MIN_RANGE && share && share->cf &&
            !bulk_delete_min_pk_.empty() && !bulk_delete_max_pk_.empty())
        {
            uchar hi[DATA_KEY_BUF_LEN];
            uint hi_len = 0;
            uint64_t range_live = 0;
            if (data_key_successor((const uchar *)bulk_delete_max_pk_.data(),
                                   (uint)bulk_delete_max_pk_.size(), hi, hi_len) &&
                count_live_data_keys(
                    cached_thd_, txn, share->cf, (const uchar *)bulk_delete_min_pk_.data(),
                    (uint)bulk_delete_min_pk_.size(), hi, hi_len, &range_live) == TDB_SUCCESS &&
                range_live == bulk_delete_keys_.size() &&
                tdb_retry_locked(cached_thd_,
                                 [&]
                                 {
                                     return tidesdb_txn_delete_range(
                                         txn, share->cf,
                                         (const uint8_t *)bulk_delete_min_pk_.data(),
                                         bulk_delete_min_pk_.size(), hi, hi_len);
                                 }) == TDB_SUCCESS)
            {
                did_range_tombstone = true;
                ranged = true;
                stmt_txn_dirty = true;
                if (cached_trx_) cached_trx_->dirty = true;
                bulk_delete_keys_.clear();
            }
        }
        if (!ranged)
            if (int frc = bulk_delete_flush_buffered(txn)) return frc;
    }
    bulk_delete_keys_.clear();
    bulk_delete_defer_ = false;

    /* Auto compact-after-range-delete.  Threshold zero (default) keeps the previous behavior, no
       compaction after the statement.  When the threshold is met the observed [min_pk, max_pk]
       range on the primary CF is queued on the transaction and compacted after it commits, when
       the tombstones have left the transaction, unless the run already became a range tombstone,
       which reclaims the span on its own.  The library range is half-open, so the end is the key
       just past max_pk.  Secondary index tombstones are reclaimed by the per-CF
       tombstone_density_trigger. */
    if (!did_range_tombstone && cached_compact_after_range_delete_min_rows_ > 0 &&
        bulk_delete_rows_ >= cached_compact_after_range_delete_min_rows_ && share && share->cf &&
        cached_trx_ && !bulk_delete_min_pk_.empty() && !bulk_delete_max_pk_.empty())
    {
        tidesdb_trx_t::pending_compact pc;
        pc.cf_name = share->cf_name;
        pc.lo = bulk_delete_min_pk_;
        uchar hi[DATA_KEY_BUF_LEN];
        uint hi_len = 0;
        if (data_key_successor((const uchar *)bulk_delete_max_pk_.data(),
                               (uint)bulk_delete_max_pk_.size(), hi, hi_len))
            pc.hi.assign((const char *)hi, hi_len);
        cached_trx_->pending_compacts.push_back(std::move(pc));
    }

    bulk_delete_rows_ = 0;
    bulk_delete_min_pk_.clear();
    bulk_delete_max_pk_.clear();
    return 0;
}
