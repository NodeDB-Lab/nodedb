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
    /// Permanent: the delta fails to decode, fails its signature
    /// check, or writes rows outside its frame target. Nothing is imported.
    Malformed,
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

        // The engine accepts a delta only when every row it writes is the
        // frame target. An empty target admits any row, and the import is
        // installed before its write set is known. Refusing it here keeps
        // the refusal ahead of every state change.
        if document_id.is_empty() {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    constraint: CRDT_SINGLE_DOCUMENT_DELTA.to_string(),
                    detail: format!(
                        "a CRDT apply into {collection} must name the one document its delta \
                         writes; nothing was applied"
                    ),
                },
            );
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
                ValidatedApplyOutcome::Clean { .. } => {
                    imported_authoritative = true;
                    if surrogate != Surrogate::ZERO {
                        Ok(Self::encode_crdt_row(engine, collection, document_id))
                    } else {
                        Ok(None)
                    }
                }
                ValidatedApplyOutcome::Rejected(vt) => {
                    imported_authoritative = true;
                    // There is no client to answer here, so the validated
                    // outcome is observed only for its DLQ side effect.
                    tracing::debug!(core = self.core_id, %collection, reason = %vt, "crdt apply violated constraint (DLQ)");
                    Ok(None)
                }
                ValidatedApplyOutcome::Malformed => Err(LocalRefusal::Malformed),
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
                let code = match refusal {
                    LocalRefusal::Malformed => {
                        warn!(
                            core = self.core_id,
                            %collection,
                            %document_id,
                            "crdt apply refused a malformed delta"
                        );
                        ErrorCode::RejectedPrevalidation {
                            reason: format!(
                                "delta for {collection}/{document_id} could not be decoded, \
                                 failed its signature check, or wrote rows outside \
                                 {document_id}; nothing was applied"
                            ),
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

#[cfg(test)]
mod tests {
    use loro::LoroValue;
    use nodedb_types::Surrogate;

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};

    fn params<'a>(document_id: &'a str, delta: &'a [u8]) -> CrdtApplyParams<'a> {
        CrdtApplyParams {
            collection: "docs",
            document_id,
            delta,
            surrogate: Surrogate::ZERO,
            peer_id: 7,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
            auth_user_id: 0,
            auth_device_id: 0,
            auth_seq_no: 0,
            delta_signature: [0; 32],
            signing_required: false,
        }
    }

    fn two_row_delta() -> Vec<u8> {
        let source = nodedb_crdt::CrdtState::new(42).expect("source state");
        for row in ["one", "two"] {
            source
                .upsert("docs", row, &[("value", LoroValue::String(row.into()))])
                .expect("source write");
        }
        source.export_snapshot().expect("source snapshot")
    }

    /// A delta with no named target is refused before the import. The
    /// refusal code claims nothing applied, so no row can exist afterwards.
    #[test]
    fn a_delta_without_a_target_document_imports_no_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let delta = two_row_delta();

        let response = core.apply_crdt_local(&task, params("", &delta));

        assert_eq!(response.status, Status::Error);
        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RejectedConstraint { .. })
            ),
            "got {:?}",
            response.error_code
        );
        let key = (task.request.database_id, task.request.tenant_id);
        let imported = core.crdt_engines.get(&key).is_some_and(|engine| {
            engine.row_exists("docs", "one") || engine.row_exists("docs", "two")
        });
        assert!(!imported, "a refused delta must not reach the CRDT state");
    }

    /// A delta that writes rows beyond its named target is refused as
    /// malformed, and neither row is imported.
    #[test]
    fn a_delta_writing_a_foreign_row_imports_no_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let delta = two_row_delta();

        let response = core.apply_crdt_local(&task, params("one", &delta));

        assert_eq!(response.status, Status::Error);
        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RejectedPrevalidation { .. })
            ),
            "got {:?}",
            response.error_code
        );
        let key = (task.request.database_id, task.request.tenant_id);
        let imported = core.crdt_engines.get(&key).is_some_and(|engine| {
            engine.row_exists("docs", "one") || engine.row_exists("docs", "two")
        });
        assert!(!imported, "a refused delta must not reach the CRDT state");
    }
}
