// SPDX-License-Identifier: BUSL-1.1

//! The non-sync apply path: SQL and native-client writes, no idempotency gate.
//!
//! There is no waiting sender to answer with a disposition here, so a refusal
//! is returned as a typed error. It still has to say *which kind* of refusal it
//! is: a caller that later grows a retry channel must be able to tell a
//! transient refusal from a permanent one without parsing the message.

use tracing::warn;

use nodedb_types::Surrogate;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::crdt::tenant_state::ValidatedApplyOutcome;

use super::params::{CRDT_PENDING_DEPENDENCIES, CRDT_SINGLE_DOCUMENT_DELTA, CrdtApplyParams};

/// Why a local apply produced no materializable row.
enum LocalRefusal {
    /// Permanent: the same bytes fail identically on a retry.
    Terminal {
        constraint: &'static str,
        detail: String,
    },
    /// Nothing applied, but the identical bytes apply once the missing causal
    /// history arrives.
    Retryable { detail: String },
}

impl CoreLoop {
    pub(super) fn apply_crdt_local(
        &mut self,
        task: &ExecutionTask,
        params: CrdtApplyParams<'_>,
    ) -> Response {
        let CrdtApplyParams {
            collection,
            document_id,
            delta,
            surrogate,
            peer_id,
            expected_frontier_digest,
            ..
        } = params;
        let tenant_id = task.request.tenant_id;

        if let Some(expected) = expected_frontier_digest {
            let actual =
                self.current_crdt_frontier_digest(task.request.database_id, tenant_id, collection);
            if expected != actual {
                return self
                    .response_error(task, ErrorCode::CrdtFrontierMismatch { expected, actual });
            }
        }

        // Borrow the engine in a nested block so the &mut borrow is dropped
        // before the sparse write below takes &self. On a Clean apply we read
        // the merged row back and encode it while the borrow is live, carrying
        // the materialized bytes out.
        let mut imported_authoritative = false;
        let materialized = {
            let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
                Ok(e) => e,
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            };
            let outcome = engine.apply_committed_delta_validated(
                collection,
                delta,
                surrogate,
                document_id,
                peer_id,
            );
            match outcome {
                ValidatedApplyOutcome::Clean { write_set, .. } => {
                    imported_authoritative = true;
                    // Enforce the one-document-per-delta contract before
                    // materializing: a delta that wrote rows other than the
                    // frame target has no surrogate for those rows, so
                    // materializing only `document_id` would silently drop the
                    // rest.
                    match Self::single_document_write_set(collection, document_id, &write_set) {
                        Ok(()) => {
                            if surrogate != Surrogate::ZERO {
                                Ok(Self::encode_crdt_row(engine, collection, document_id))
                            } else {
                                Ok(None)
                            }
                        }
                        Err(detail) => Err(LocalRefusal::Terminal {
                            constraint: CRDT_SINGLE_DOCUMENT_DELTA,
                            detail,
                        }),
                    }
                }
                ValidatedApplyOutcome::Rejected(vt) => {
                    imported_authoritative = true;
                    // There is no client to answer here, so the validated
                    // outcome is observed only for its DLQ side effect.
                    tracing::debug!(core = self.core_id, %collection, reason = %vt, "crdt apply violated constraint (DLQ)");
                    Ok(None)
                }
                ValidatedApplyOutcome::Malformed => {
                    warn!(core = self.core_id, %collection, "crdt apply skipped malformed delta");
                    Ok(None)
                }
                ValidatedApplyOutcome::PendingDependencies => {
                    // Nothing was imported: the operations are buffered awaiting
                    // predecessors this collection's document has never seen.
                    // Refuse loudly rather than report a write that did not
                    // happen — and refuse *retryably*, because the identical
                    // bytes land the moment the missing history arrives.
                    Err(LocalRefusal::Retryable {
                        detail: format!(
                            "delta for {collection}/{document_id} depends on operations \
                             absent from this collection's document; nothing was applied"
                        ),
                    })
                }
            }
        };
        // Engine borrow dropped here. A clean or constraint-rejected Loro
        // import changed authoritative state; malformed bytes did not.
        if imported_authoritative {
            self.checkpoint_coordinator.mark_dirty("crdt", 1);
        }
        match materialized {
            Ok(Some(bytes)) => {
                self.materialize_synced_document(
                    task,
                    tenant_id.as_u64(),
                    collection,
                    document_id,
                    surrogate,
                    &bytes,
                );
                if imported_authoritative {
                    self.note_collection_write_lsn(task, collection);
                }
            }
            Ok(None) if imported_authoritative => {
                // Headless and constraint-rejected imports have no sparse
                // projection, but still changed authoritative Loro state.
                self.note_collection_write_lsn(task, collection);
            }
            Ok(None) => {}
            Err(refusal) => {
                if imported_authoritative {
                    self.note_collection_write_lsn(task, collection);
                }
                let code = match refusal {
                    LocalRefusal::Terminal { constraint, detail } => {
                        warn!(
                            core = self.core_id,
                            %collection,
                            %document_id,
                            constraint,
                            detail = %detail,
                            "crdt apply rejected: delta could not be materialized"
                        );
                        ErrorCode::RejectedConstraint {
                            constraint: constraint.to_string(),
                            detail,
                        }
                    }
                    LocalRefusal::Retryable { detail } => {
                        warn!(
                            core = self.core_id,
                            %collection,
                            %document_id,
                            constraint = CRDT_PENDING_DEPENDENCIES,
                            detail = %detail,
                            "crdt apply refused retryably: delta depends on absent operations"
                        );
                        ErrorCode::RetryableRefusal { reason: detail }
                    }
                };
                return self.response_error(task, code);
            }
        }
        self.response_ok(task)
    }
}
