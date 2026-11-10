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
    pub(super) applied_keys: &'a [(String, crate::engine::document::store::StorageKey)],
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
        keys: &[(String, crate::engine::document::store::StorageKey)],
    ) {
        for (collection, key) in keys {
            self.doc_cache.invalidate(database_id, tid, collection, key);
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
            applied_keys,
            undo_log,
            err,
        } = p;
        // The caller's transaction owns durable heads. Restore only their cache
        // before reversing memory indexes, without opening another sparse writer.
        let mut memory_undo = Vec::with_capacity(undo_log.len());
        for entry in undo_log.into_iter().rev() {
            match entry {
                UndoEntry::ChainHead { collection, prior } => {
                    let key = (
                        crate::types::DatabaseId::new(database_id),
                        crate::types::TenantId::new(tid),
                        collection,
                    );
                    match prior {
                        Some(head) => {
                            self.chain.heads.insert(key, head);
                        }
                        None => {
                            self.chain.heads.remove(&key);
                        }
                    }
                }
                memory => memory_undo.push(memory),
            }
        }
        memory_undo.reverse();
        let final_err = match self.rollback_undo_log(database_id, tid, memory_undo) {
            Ok(()) => err,
            Err(undo_error) => ErrorCode::from(undo_error),
        };
        self.rollback_merge_cache(database_id, tid, applied_keys);
        self.response_error(task, final_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::engine::document::store::StorageKey;
    use crate::types::hash_chain::ChainHead;
    use crate::types::{DatabaseId, TenantId};
    use nodedb_types::Surrogate;

    #[test]
    fn merge_abort_restores_chain_cache_inside_the_source_transaction() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let prior = ChainHead {
            seq: 1,
            hash: "prior".into(),
        };
        let pending = ChainHead {
            seq: 2,
            hash: "pending".into(),
        };
        core.sparse
            .put_chain_head(0, 1, "ledger", &prior)
            .expect("prior head");
        let txn = core.sparse.begin_write().expect("source transaction");
        core.sparse
            .put_chain_head_in_txn(&txn, 0, 1, "ledger", &pending)
            .expect("pending head");
        let key = (DatabaseId::DEFAULT, TenantId::new(1), "ledger".to_owned());
        core.chain.heads.insert(key.clone(), pending.clone());
        let task = make_default_task();
        core.abort_merge_apply(MergeAbort {
            task: &task,
            database_id: 0,
            tid: 1,
            applied_keys: &[],
            undo_log: vec![UndoEntry::ChainHead {
                collection: "ledger".into(),
                prior: Some(prior.clone()),
            }],
            err: ErrorCode::Internal {
                detail: "declined statement".into(),
            },
        });
        assert_eq!(core.chain.heads.get(&key), Some(&prior));
        assert_eq!(
            core.sparse
                .get_chain_head_in_txn(&txn, 0, 1, "ledger")
                .expect("pending read"),
            Some(pending)
        );
        assert_eq!(
            core.sparse
                .get_chain_head(0, 1, "ledger")
                .expect("committed read"),
            Some(prior)
        );
        txn.abort().expect("abort source");
    }

    #[test]
    fn merge_abort_invalidates_source_and_target_cache_identities() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let key = StorageKey::for_surrogate(Surrogate::new(9));
        for collection in ["source", "target", "other"] {
            core.doc_cache.put(0, 1, collection, &key, 1, b"body");
        }
        let task = make_default_task();
        core.abort_merge_apply(MergeAbort {
            task: &task,
            database_id: 0,
            tid: 1,
            applied_keys: &[("source".into(), key), ("target".into(), key)],
            undo_log: Vec::new(),
            err: ErrorCode::Internal {
                detail: "declined statement".into(),
            },
        });
        assert!(core.doc_cache.get(0, 1, "source", &key).is_none());
        assert!(core.doc_cache.get(0, 1, "target", &key).is_none());
        assert_eq!(
            core.doc_cache.get(0, 1, "other", &key),
            Some(b"body".as_slice())
        );
    }
}
