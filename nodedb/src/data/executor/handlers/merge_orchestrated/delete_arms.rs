// SPDX-License-Identifier: BUSL-1.1

//! The MERGE DELETE arms, applied after the put transaction has committed.
//!
//! Its own file because it obeys a different transaction rule to the
//! UPDATE/INSERT arms it follows. Those share one redb write transaction and
//! are all-or-nothing across the whole set; each DELETE arm instead takes its
//! own transaction and commits on its own, which is what lets the arm's
//! post-commit bookkeeping (affected count, RETURNING pre-image, redo
//! write-set entry, delete event) observe a removal that is already durable.
//! Keeping the two phases in separate files stops a later edit from folding a
//! delete into the put phase's transaction, which would change when each
//! removal becomes durable relative to the event it emits.

use crate::bridge::envelope::{Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::RowIdentity;

use super::apply_support::returning_doc;
use super::plan::MergeDelete;

/// The DELETE arms to apply and the context they apply against.
pub(super) struct MergeDeleteArms<'a> {
    pub(super) task: &'a ExecutionTask,
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: &'a str,
    pub(super) deletes: &'a [MergeDelete],
    /// Whether the target maintains a secondary vector index — gates the
    /// post-apply redo write-set entries.
    pub(super) has_vectors: bool,
    /// Whether the statement carries a `RETURNING` projection.
    pub(super) returning: bool,
    /// Join-key VALUE → target row surrogate, resolved on the Control Plane.
    pub(super) resolved_targets: &'a [nodedb_physical::physical_plan::ResolvedSumTarget],
    /// The target's declared `PRIMARY KEY` column, when it has one. Names
    /// each removed row in its event and redo entry.
    pub(super) declared_primary_key: Option<&'a str>,
}

/// The statement-wide accumulators these arms contribute to, shared with the
/// UPDATE/INSERT phase that ran before them.
pub(super) struct MergeDeleteTally<'a> {
    pub(super) affected: &'a mut u64,
    pub(super) write_set: &'a mut Vec<WriteSetEntry>,
    pub(super) returned_docs: &'a mut Vec<serde_json::Value>,
}

impl CoreLoop {
    /// Apply every DELETE arm. `Err(response)` is the terminating error response
    /// the caller must return as-is; the put phase has already committed by this
    /// point, so there is nothing left to unwind.
    pub(super) fn apply_merge_delete_arms(
        &mut self,
        arms: MergeDeleteArms<'_>,
        tally: MergeDeleteTally<'_>,
    ) -> Result<(), Response> {
        let MergeDeleteArms {
            task,
            database_id,
            tid,
            collection,
            deletes,
            has_vectors,
            returning,
            resolved_targets,
            declared_primary_key,
        } = arms;
        let MergeDeleteTally {
            affected,
            write_set,
            returned_docs,
        } = tally;

        for del in deletes {
            let surrogate = del.key.surrogate();
            let row_key = del.key.to_string();
            // The identity INSERT minted for this row, from the plan's
            // captured MessagePack body: the declared primary key, else the
            // decimal surrogate.
            let row_identity = RowIdentity::of_stored_row(&del.body, declared_primary_key, del.key);
            // One write txn per arm: the removal and its index cascades
            // commit together, and a failing arm drops the txn
            // un-committed so it leaves nothing behind.
            let txn = match self.sparse.begin_write() {
                Ok(txn) => txn,
                Err(e) => return Err(self.response_error(task, e)),
            };
            match self.apply_point_delete(
                &txn,
                PointDeleteParams {
                    database_id,
                    tid,
                    collection,
                    document_id: &row_key,
                    surrogate,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    resolved_targets,
                },
            ) {
                Ok(outcome) => {
                    // A DELETE arm takes the removed row's contribution
                    // back off its target, folded inside THIS arm's
                    // transaction so the debit and the removal commit
                    // together. The pre-image is the plan's captured
                    // body — the only image a delete has.
                    match write_hook::run(
                        self,
                        &txn,
                        &write_hook::HookCtx {
                            database_id,
                            tid,
                            collection,
                            resolved_targets,
                            deferred_sum_targets: &[],
                            wal_lsn: task.wal_lsn(),
                        },
                        write_hook::WriteImages::Delete {
                            old: write_hook::ImageBody::Submitted(&del.body),
                        },
                    ) {
                        // The arm's BALANCED contribution is NOT settled
                        // here: these arms commit one transaction each,
                        // after the caller's phase-A commit, so a
                        // violation found here could no longer be
                        // undone. The caller accounts every delete
                        // arm's pre-image before phase A runs and
                        // judges the whole MERGE there.
                        Ok(enforcement) => write_set
                            .extend(write_hook::target_write_set(&enforcement.target_writes)),
                        // Dropping `txn` un-committed reverses the
                        // removal and every target it had debited.
                        Err(e) => return Err(self.response_error(task, e)),
                    }
                    if let Err(e) = txn.commit() {
                        return Err(self.response_error(
                            task,
                            crate::Error::Storage {
                                engine: "sparse".into(),
                                detail: format!("merge delete commit: {e}"),
                            },
                        ));
                    }
                    if outcome.prior_value.is_some() {
                        *affected += 1;
                        // A DELETE arm returns the PRE-image — the row
                        // as it was classified, since nothing survives
                        // the delete to project. Taken from the plan's
                        // captured body rather than `prior_value`, which
                        // is the raw stored form (Binary Tuple on a
                        // strict target) and would need re-decoding.
                        if returning {
                            match returning_doc(&del.body, &del.key) {
                                Ok(doc) => returned_docs.push(doc),
                                Err(e) => return Err(self.response_error(task, e)),
                            }
                        }
                        if has_vectors {
                            write_set.push(WriteSetEntry {
                                surrogate: surrogate.as_u32(),
                                identity: row_identity.clone(),
                                is_delete: true,
                                value: Vec::new(),
                                collection: None,
                            });
                        }
                    }
                    self.emit_document_delete_event(
                        task,
                        collection,
                        row_identity,
                        outcome.prior_value.as_deref(),
                    );
                }
                Err(e) => return Err(self.response_error(task, e)),
            }
        }
        Ok(())
    }
}
