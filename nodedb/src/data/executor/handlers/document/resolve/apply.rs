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
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::unique::SubmittedWrite;
use crate::data::executor::handlers::rls_write_gate;
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
        // Each mutation below commits on its own, so UNIQUE is judged on the
        // statement's post-state before the first one lands.
        if let Err(e) = self.check_resolved_document_unique(task, tid, mutations) {
            return self.response_error(task, e);
        }
        // The gate stays on every write path even though
        // `DecidedEarlierInRequest` makes this a no-op — a single path that
        // skips it entirely is a hole future callers can fall into.
        for mutation in mutations {
            if let DocumentResolvedMutation::Put {
                collection,
                document_id,
                value,
                ..
            } = mutation
                && let Err(e) = rls_write_gate::admit_stored_row(
                    rls_write_check,
                    rls_write_gate::GatedRow {
                        body: value,
                        identity: &crate::engine::document::store::RowIdentity::from_user_key(
                            document_id.as_str(),
                        ),
                        strict_schema: None,
                        identity_column: &self.identity_column(
                            task.request.database_id.as_u64(),
                            tid,
                            collection.as_str(),
                        ),
                    },
                    tid,
                    collection.as_str(),
                    &self.crs_context(),
                )
            {
                return self.response_error(task, e);
            }
        }

        // Each mutation commits on its own, so a refusal after one landed
        // carries the entries of every landed row.
        let mut write_set = Vec::new();
        let mut landed = 0u64;
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
                    ApplyResolvedPut {
                        tid,
                        collection: collection.as_str(),
                        document_id,
                        surrogate: *surrogate,
                        value,
                        precondition: precondition.as_deref(),
                        resolved_sum_targets,
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
                    ApplyResolvedDelete {
                        tid,
                        collection: collection.as_str(),
                        document_id,
                        surrogate: *surrogate,
                        resolved_sum_targets,
                    },
                ),
            };
            match applied {
                Ok(entries) => {
                    write_set.extend(entries);
                    landed += 1;
                }
                Err(code) => {
                    let code = crate::data::executor::handlers::partial_refusal::refusal_after_rows(
                        landed, code,
                    );
                    return self.refusal_with_landed_rows(task, code, write_set);
                }
            }
        }

        let mut response = self.response_with_payload(task, response_payload.to_vec());
        response.write_set = write_set;
        response
    }

    /// Refuse the statement when its post-state gives one unique value two
    /// owners. A value one mutation releases is free for another.
    fn check_resolved_document_unique(
        &self,
        task: &ExecutionTask,
        tid: u64,
        mutations: &[DocumentResolvedMutation],
    ) -> crate::Result<()> {
        let writes: Vec<SubmittedWrite<'_>> = mutations
            .iter()
            .map(|mutation| SubmittedWrite {
                collection: mutation.collection().as_str(),
                surrogate: mutation.surrogate().as_u32(),
                body: match mutation {
                    DocumentResolvedMutation::Put { value, .. } => Some(value.as_slice()),
                    DocumentResolvedMutation::Delete { .. } => None,
                },
                judged: true,
            })
            .collect();
        self.check_submitted_unit_unique(task.request.database_id.as_u64(), tid, &writes)
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
