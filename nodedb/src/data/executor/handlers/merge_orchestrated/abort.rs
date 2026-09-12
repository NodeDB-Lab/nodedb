// SPDX-License-Identifier: BUSL-1.1

//! Unwinding a partially-applied MERGE.
//!
//! Separate from the forward apply because reversal is governed by a different
//! rule: the forward path may fail anywhere, but the unwind has to put back
//! exactly the state that lives OUTSIDE the redb write transaction — the HNSW
//! and R-tree deltas and the read-through document cache — since dropping the
//! transaction already reverses everything inside it. Collecting that in one
//! place is what keeps every abort site in the apply pass identical, and makes
//! "did we reverse all of it?" a question about one short file.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;

/// Everything [`CoreLoop::abort_merge_apply`] needs to unwind a partially
/// applied MERGE and surface the terminating error.
pub(super) struct MergeAbort<'a> {
    pub(super) task: &'a ExecutionTask,
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: &'a str,
    pub(super) applied_keys: &'a [String],
    pub(super) undo_log: Vec<UndoEntry>,
    pub(super) err: ErrorCode,
}

impl CoreLoop {
    /// Evict cached document copies for rows written into a rolled-back apply
    /// transaction. `apply_point_put` populates the document cache BEFORE its
    /// UNIQUE check, so a row that fails the check — and every row rolled back
    /// when the shared txn is dropped — leaves a stale cache entry that a later
    /// point lookup would resurrect. Eviction is always safe: the worst case is
    /// a cache miss that falls through to the (correctly rolled-back) store.
    /// Mirrors the transaction-undo path's cache eviction.
    fn rollback_merge_cache(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        keys: &[String],
    ) {
        for key in keys {
            // `key` is a bare string accumulated several calls removed from
            // any typed scan. Eviction is always safe to skip: a shape that
            // fails to parse as a storage key can hold no live cache entry
            // either way.
            if let Some(key) = crate::engine::document::store::StorageKey::parse(key) {
                self.doc_cache
                    .invalidate(database_id, tid, collection, &key);
            }
        }
    }

    /// Abort the apply pass: reverse the in-memory vector/spatial index deltas
    /// applied so far, evict the stale document-cache entries, and surface the
    /// error. The shared redb write transaction (dropped uncommitted once this
    /// returns) reverses the document store, secondary btree, FTS, and column
    /// stats; the HNSW and R-tree live outside it and are reversed here via the
    /// canonical undo driver. An undo failure leaves shard state unknown, so it
    /// escalates to `RollbackFailed` rather than the original error.
    pub(super) fn abort_merge_apply(&mut self, p: MergeAbort<'_>) -> Response {
        let MergeAbort {
            task,
            database_id,
            tid,
            collection,
            applied_keys,
            undo_log,
            err,
        } = p;
        let final_err = match self.rollback_undo_log(database_id, tid, undo_log) {
            Ok(()) => err,
            Err((entry_index, detail)) => ErrorCode::RollbackFailed {
                entry_index,
                detail,
            },
        };
        self.rollback_merge_cache(database_id, tid, collection, applied_keys);
        self.response_error(task, final_err)
    }
}
