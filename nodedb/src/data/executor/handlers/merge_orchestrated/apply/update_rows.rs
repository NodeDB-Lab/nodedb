// SPDX-License-Identifier: BUSL-1.1

//! The MERGE UPDATE arm (matched + not-matched-by-source), applied inside the
//! phase-A transaction shared with the INSERT arm.

use redb::WriteTransaction;

use crate::bridge::envelope::{Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::balanced::BalancedEntry;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;

use super::super::abort::MergeAbort;
use super::super::apply_support::{MergePutEvent, record_put_index_undo, returning_doc};
use super::super::plan::MergeUpdate;

/// Read-only context the UPDATE arm needs from the shared apply pass.
pub(super) struct UpdateRowsCtx<'a> {
    pub(super) task: &'a ExecutionTask,
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: &'a str,
    /// Whether the target maintains a secondary vector index. Gated once by
    /// the caller and threaded into the per-row re-index below.
    pub(super) has_vectors: bool,
    /// Whether the statement carries a `RETURNING` projection.
    pub(super) returning: bool,
    pub(super) resolved_sum_targets: &'a [nodedb_physical::physical_plan::ResolvedSumTarget],
}

/// Mutable accumulators the UPDATE arm folds into. Owned by the caller for
/// the whole apply pass and borrowed here rather than cloned.
pub(super) struct UpdateRowsTally<'a, 'p> {
    pub(super) affected: &'a mut u64,
    pub(super) applied_keys: &'a mut Vec<String>,
    pub(super) undo_log: &'a mut Vec<UndoEntry>,
    pub(super) put_events: &'a mut Vec<MergePutEvent<'p>>,
    pub(super) write_set: &'a mut Vec<WriteSetEntry>,
    pub(super) balanced_entries: &'a mut Vec<BalancedEntry>,
    pub(super) returned_docs: &'a mut Vec<serde_json::Value>,
}

impl CoreLoop {
    /// Apply every UPDATE arm (matched + not-matched-by-source) inside the
    /// caller's shared write transaction. `Err(response)` is the terminating
    /// error response the caller must return as-is — the transaction is left
    /// uncommitted for the caller to abort.
    pub(super) fn apply_merge_update_arm<'p>(
        &mut self,
        txn: &WriteTransaction,
        updates: &'p [MergeUpdate],
        ctx: UpdateRowsCtx<'_>,
        tally: UpdateRowsTally<'_, 'p>,
    ) -> Result<(), Response> {
        let UpdateRowsCtx {
            task,
            database_id,
            tid,
            collection,
            has_vectors,
            returning,
            resolved_sum_targets,
        } = ctx;
        let UpdateRowsTally {
            affected,
            applied_keys,
            undo_log,
            put_events,
            write_set,
            balanced_entries,
            returned_docs,
        } = tally;

        for upd in updates {
            match upd.surrogate {
                Some(surrogate) => {
                    let row_key = surrogate_to_doc_id(surrogate);
                    applied_keys.push(row_key.clone());
                    // `apply_point_put`'s vector step APPENDS (it never replaces),
                    // so an in-place UPDATE must first soft-delete the surrogate's
                    // prior embedding or the stale vector keeps scoring in KNN
                    // search. Push each removal as a `DeleteVector` undo BEFORE the
                    // put's `InsertVector` undos so an abort undeletes the old
                    // vector after removing the new one (reverse order).
                    if has_vectors {
                        for d in self.remove_document_vector_indexes(
                            database_id,
                            tid,
                            collection,
                            &row_key,
                        ) {
                            undo_log.push(UndoEntry::DeleteVector {
                                index_key: d.index_key,
                                vector_id: d.vector_id,
                                collection: d.collection,
                                field: d.field,
                                doc_id: d.doc_id,
                            });
                        }
                    }
                    match self.apply_point_put(
                        txn,
                        PointPutParams {
                            database_id,
                            tid,
                            collection,
                            document_id: &row_key,
                            surrogate,
                            value: &upd.body,
                            index_text: true,
                            user_roles: &task.request.user_roles,
                            enforce: true,
                            wal_lsn: task.wal_lsn(),
                            resolved_targets: resolved_sum_targets,
                        },
                    ) {
                        Ok(mut outcome) => {
                            record_put_index_undo(undo_log, &mut outcome);
                            // The arm's materialized-sum delta is folded inside
                            // the SAME transaction the arm's row lands in, so a
                            // moved total rolls back with the row that moved it.
                            // Both images come from the plan: the classifier held
                            // the pre-image already, so nothing is re-read.
                            match write_hook::run(
                                self,
                                txn,
                                &write_hook::HookCtx {
                                    database_id,
                                    tid,
                                    collection,
                                    resolved_targets: resolved_sum_targets,
                                    deferred_sum_targets: &[],
                                    wal_lsn: task.wal_lsn(),
                                },
                                write_hook::WriteImages::Update {
                                    old: write_hook::ImageBody::Submitted(&upd.old_body),
                                    new: write_hook::ImageBody::Submitted(&upd.body),
                                },
                            ) {
                                Ok(enforcement) => {
                                    write_set.extend(write_hook::target_write_set(
                                        &enforcement.target_writes,
                                    ));
                                    balanced_entries.extend(enforcement.balanced_entries);
                                }
                                Err(e) => {
                                    return Err(self.abort_merge_apply(MergeAbort {
                                        task,
                                        database_id,
                                        tid,
                                        collection,
                                        applied_keys: applied_keys.as_slice(),
                                        undo_log: std::mem::take(undo_log),
                                        err: e.into(),
                                    }));
                                }
                            }
                            if has_vectors {
                                write_set.push(WriteSetEntry {
                                    surrogate: surrogate.as_u32(),
                                    is_delete: false,
                                    value: upd.body.clone(),
                                    collection: None,
                                });
                            }
                            if returning {
                                match returning_doc(&upd.body, &row_key) {
                                    Ok(doc) => returned_docs.push(doc),
                                    Err(e) => {
                                        return Err(self.abort_merge_apply(MergeAbort {
                                            task,
                                            database_id,
                                            tid,
                                            collection,
                                            applied_keys: applied_keys.as_slice(),
                                            undo_log: std::mem::take(undo_log),
                                            err: e.into(),
                                        }));
                                    }
                                }
                            }
                            put_events.push((row_key, upd.body.as_slice(), outcome.prior_value));
                            *affected += 1;
                        }
                        Err(e) => {
                            return Err(self.abort_merge_apply(MergeAbort {
                                task,
                                database_id,
                                tid,
                                collection,
                                applied_keys: applied_keys.as_slice(),
                                undo_log: std::mem::take(undo_log),
                                err: e.into(),
                            }));
                        }
                    }
                }
                None => {
                    // A target row whose `doc_id` does not parse as a storage
                    // key: `put_in_txn` addresses DOCUMENTS rows by
                    // `StorageKey` only, and the workspace carries no
                    // on-disk-format compatibility burden for a row shape
                    // that predates surrogate keying, so this arm is refused
                    // rather than written through a raw string key.
                    return Err(self.abort_merge_apply(MergeAbort {
                        task,
                        database_id,
                        tid,
                        collection,
                        applied_keys: applied_keys.as_slice(),
                        undo_log: std::mem::take(undo_log),
                        err: crate::Error::Storage {
                            engine: "document".into(),
                            detail: format!(
                                "MERGE UPDATE target row '{}' in '{collection}' has no \
                                 surrogate storage key",
                                upd.doc_id
                            ),
                        }
                        .into(),
                    }));
                }
            }
        }
        Ok(())
    }
}
