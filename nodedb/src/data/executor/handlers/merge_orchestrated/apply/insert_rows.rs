// SPDX-License-Identifier: BUSL-1.1

//! The MERGE NOT-MATCHED INSERT arm, applied inside the phase-A transaction
//! shared with the UPDATE arm.

use std::collections::HashMap;

use redb::WriteTransaction;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::balanced::BalancedEntry;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_types::Surrogate;

use super::super::abort::MergeAbort;
use super::super::apply_support::{MergePutEvent, record_put_index_undo, returning_doc};
use super::super::plan::MergeInsert;

/// Read-only context the INSERT arm needs from the shared apply pass.
pub(super) struct InsertRowsCtx<'a> {
    pub(super) task: &'a ExecutionTask,
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: &'a str,
    /// Whether the target maintains a secondary vector index.
    pub(super) has_vectors: bool,
    /// Whether the statement carries a `RETURNING` projection.
    pub(super) returning: bool,
    pub(super) resolved_sum_targets: &'a [nodedb_physical::physical_plan::ResolvedSumTarget],
    /// Source join value → Control-Plane-pre-assigned surrogate, verified
    /// against `inserts` by the caller before this arm runs.
    pub(super) surrogate_for: &'a HashMap<&'a str, u32>,
}

/// Mutable accumulators the INSERT arm folds into. Owned by the caller for
/// the whole apply pass and borrowed here rather than cloned.
pub(super) struct InsertRowsTally<'a, 'p> {
    pub(super) affected: &'a mut u64,
    pub(super) applied_keys: &'a mut Vec<String>,
    pub(super) undo_log: &'a mut Vec<UndoEntry>,
    pub(super) put_events: &'a mut Vec<MergePutEvent<'p>>,
    pub(super) write_set: &'a mut Vec<WriteSetEntry>,
    pub(super) balanced_entries: &'a mut Vec<BalancedEntry>,
    pub(super) returned_docs: &'a mut Vec<serde_json::Value>,
}

impl CoreLoop {
    /// Apply every NOT-MATCHED INSERT arm inside the caller's shared write
    /// transaction. `Err(response)` is the terminating error response the
    /// caller must return as-is — the transaction is left uncommitted for the
    /// caller to abort.
    pub(super) fn apply_merge_insert_arm<'p>(
        &mut self,
        txn: &WriteTransaction,
        inserts: &'p [MergeInsert],
        ctx: InsertRowsCtx<'_>,
        tally: InsertRowsTally<'_, 'p>,
    ) -> Result<(), Response> {
        let InsertRowsCtx {
            task,
            database_id,
            tid,
            collection,
            has_vectors,
            returning,
            resolved_sum_targets,
            surrogate_for,
        } = ctx;
        let InsertRowsTally {
            affected,
            applied_keys,
            undo_log,
            put_events,
            write_set,
            balanced_entries,
            returned_docs,
        } = tally;

        for ins in inserts {
            // The verify above proved every insert key has a pre-assigned
            // surrogate; the lookup cannot miss, but a missing entry is treated
            // as drift rather than unwrapped.
            let surrogate = match surrogate_for.get(ins.join_key.as_str()) {
                Some(s) => Surrogate(*s),
                None => {
                    return Err(self.abort_merge_apply(MergeAbort {
                        task,
                        database_id,
                        tid,
                        collection,
                        applied_keys: applied_keys.as_slice(),
                        undo_log: std::mem::take(undo_log),
                        err: ErrorCode::OllpRetryRequired,
                    }));
                }
            };
            let row_key = surrogate_to_doc_id(surrogate);
            applied_keys.push(row_key.clone());
            match self.apply_point_put(
                txn,
                PointPutParams {
                    database_id,
                    tid,
                    collection,
                    document_id: &row_key,
                    surrogate,
                    value: &ins.body,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    wal_lsn: task.wal_lsn(),
                    resolved_targets: resolved_sum_targets,
                },
            ) {
                Ok(mut outcome) => {
                    record_put_index_undo(undo_log, &mut outcome);
                    // A NOT-MATCHED INSERT arm credits its target with the whole
                    // new row — post-image only, which is exactly what
                    // `RowImages::Insert` expresses.
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
                        write_hook::WriteImages::Insert {
                            new: write_hook::ImageBody::Submitted(&ins.body),
                        },
                    ) {
                        Ok(enforcement) => {
                            write_set
                                .extend(write_hook::target_write_set(&enforcement.target_writes));
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
                            value: ins.body.clone(),
                            collection: None,
                        });
                    }
                    if returning {
                        match returning_doc(&ins.body, &row_key) {
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
                    put_events.push((row_key, ins.body.as_slice(), None));
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
        Ok(())
    }
}
