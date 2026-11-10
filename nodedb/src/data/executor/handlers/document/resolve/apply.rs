// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `DocumentOp::ResolvedWrite`. Runs on every replica;
//! the plan carries the decided verdict and mutations, nothing recomputed.
//!
//! Drift check: every mutation's `precondition` (exact stored bytes resolve
//! read) is compared `==` before the first mutation runs, all-or-nothing —
//! a surrogate-existence check alone would miss a concurrent content change
//! (a lost update).

use nodedb_physical::physical_plan::DocumentResolvedMutation;
use tracing::debug;

use super::apply_row::{ApplyResolvedDelete, ApplyResolvedPut};
use super::apply_state::ResolvedApplyState;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::StorageKey;

impl CoreLoop {
    /// Handle `DocumentOp::ResolvedWrite`: check every precondition, apply every
    /// mutation, and return the shipped payload verbatim.
    pub(in crate::data::executor) fn execute_document_resolved_write(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        mutations: &[DocumentResolvedMutation],
        response_payload: &[u8],
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> Response {
        debug!(
            core = self.core_id,
            count = mutations.len(),
            "document resolved write"
        );
        if let Err(code) = self.check_resolved_document_preconditions(task, tid, mutations) {
            return self.response_error(task, code);
        }
        let database_id = task.request.database_id.as_u64();
        let txn = match self.sparse.begin_write() {
            Ok(txn) => txn,
            Err(error) => return self.response_error(task, error),
        };
        let mut state = ResolvedApplyState::default();
        for mutation in mutations {
            let applied = match mutation {
                DocumentResolvedMutation::Put {
                    collection,
                    surrogate,
                    value,
                    precondition,
                    resolved_sum_targets,
                    document_id,
                    pk_bytes: _,
                } => self.apply_resolved_document_put(
                    task,
                    &txn,
                    &mut state,
                    ApplyResolvedPut {
                        tid,
                        collection: collection.as_str(),
                        document_id,
                        surrogate: *surrogate,
                        value,
                        precondition: precondition.as_deref(),
                        resolved_sum_targets,
                        rls_write_check,
                    },
                ),
                DocumentResolvedMutation::Delete {
                    collection,
                    document_id,
                    surrogate,
                    resolved_sum_targets,
                    pk_bytes: _,
                    precondition: _,
                } => self.apply_resolved_document_delete(
                    task,
                    &txn,
                    &mut state,
                    ApplyResolvedDelete {
                        tid,
                        collection: collection.as_str(),
                        document_id,
                        surrogate: *surrogate,
                        resolved_sum_targets,
                        rls_write_check,
                    },
                ),
            };
            if let Err(error) = applied {
                let error = state.abort(self, database_id, tid, error);
                return self.response_error(task, error);
            }
        }

        let prepared = (|| -> crate::Result<_> {
            self.check_written_unit_unique(&txn, database_id, tid, &state.touched)?;
            for (collection, entries) in std::mem::take(&mut state.balanced) {
                self.settle_balanced_entries(database_id, tid, &collection, entries)?;
            }
            let mut notes = Vec::with_capacity(state.write_set.len());
            for entry in &state.write_set {
                let ordinal = match &entry.effect {
                    crate::bridge::envelope::RowEffect::InstalledPut { image, .. } => {
                        image.version.sys_from
                    }
                    crate::bridge::envelope::RowEffect::Delete { system_from } => *system_from,
                    _ => {
                        return Err(crate::Error::Internal {
                            detail: "resolved document capture contains a non-document effect"
                                .into(),
                        });
                    }
                };
                let collection =
                    entry
                        .collection
                        .as_deref()
                        .ok_or_else(|| crate::Error::Internal {
                            detail: "resolved document capture has no collection".into(),
                        })?;
                notes.push((
                    self.prepare_surrogate_write(task, tid, collection, entry.surrogate)?,
                    ordinal,
                ));
            }
            Ok((response_payload.to_vec(), notes))
        })();
        let (payload, notes) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let error = state.abort(self, database_id, tid, error);
                return self.response_error(task, error);
            }
        };
        if let Err(error) = txn.commit() {
            let error = crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("resolved document commit outcome is unknown: {error}"),
            };
            self.fail_journalled(&error);
            let error = state.abort(self, database_id, tid, error);
            return self.response_error(task, error);
        }
        for (note, ordinal) in notes {
            self.publish_surrogate_write(note, ordinal);
        }
        self.checkpoint_coordinator
            .mark_dirty("sparse", state.write_set.len());
        for event in state.events {
            event.publish(self, task);
        }
        let mut response = self.response_with_payload(task, payload);
        response.write_set = state.write_set;
        response
    }

    /// Confirm every mutation still describes the row it was resolved against.
    /// Runs to completion before the first mutation applies, all-or-nothing —
    /// same contract `KvOp::ResolvedWrite` holds.
    fn check_resolved_document_preconditions(
        &self,
        task: &ExecutionTask,
        tid: u64,
        mutations: &[DocumentResolvedMutation],
    ) -> Result<(), ErrorCode> {
        let database_id = task.request.database_id.as_u64();
        for mutation in mutations {
            let row_key = StorageKey::for_surrogate(mutation.surrogate());
            let current = self
                .current_row_body(database_id, tid, mutation.collection().as_str(), &row_key)
                .map_err(ErrorCode::from)?;
            if current.as_deref() != mutation.precondition() {
                return Err(ErrorCode::OllpRetryRequired);
            }
        }
        Ok(())
    }
}
