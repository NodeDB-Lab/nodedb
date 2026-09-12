// SPDX-License-Identifier: BUSL-1.1

//! The sync-gated apply path: a peer delta arriving with provenance.
//!
//! Every exit answers a sender that is holding the write open, so each one has
//! to name a [`GateDisposition`] — apply, retry, or compensate. Which one it is
//! decides two things together, and they must never disagree:
//!
//! * the client-visible frame the Control Plane builds, and
//! * whether the producer high-water-mark advances.
//!
//! A retryable refusal holds the mark so the re-push at the same seq is
//! admitted rather than deduplicated to `Duplicate`; a terminal one advances it
//! so a dead frame cannot wedge the stream. Deriving both from one value is
//! what keeps them consistent — a refusal that reported "retry" to the sender
//! while advancing the mark, or held the mark while telling the sender to give
//! up, loses the write either way.

use tracing::{debug, warn};

use nodedb_types::Surrogate;
use nodedb_types::sync::violation::ViolationType;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::sync_gate::SyncAdmit;
use crate::data::executor::task::ExecutionTask;
use crate::engine::crdt::tenant_state::{DeltaSigningAdmission, ValidatedApplyOutcome};

use super::params::CrdtApplyParams;

/// What the gate decided about one peer delta.
enum GateDisposition {
    /// The delta is authoritative state now.
    Applied,
    /// Every operation the delta carried was already in this document, so the
    /// applied state did not move. The sender may retire the write — the
    /// operations are present — but calling it `Applied` would claim this
    /// delta put them there, which is the one thing that makes a peer-id
    /// collision indistinguishable from an idempotent replay.
    Deduplicated,
    /// Nothing was applied and the identical delta at the same seq is expected
    /// to succeed later. The high-water-mark is held back so the re-push is
    /// admitted rather than deduplicated.
    Retryable,
    /// The delta will never apply. The sender must compensate, and the
    /// high-water-mark advances so it cannot wedge the stream.
    Terminal(ViolationType),
}

/// Whether the constraint fence let the delta through to validation.
enum GateOutcome {
    Pending { installed: u64 },
    Applied(ValidatedApplyOutcome),
}

/// The ack status an admission decision reports directly, or `None` when the
/// decision is [`SyncAdmit::Apply`] and the real status depends on what the
/// apply produces.
fn ack_status_for(admit: &SyncAdmit) -> Option<AckStatus> {
    match admit {
        SyncAdmit::Apply => None,
        SyncAdmit::Duplicate => Some(AckStatus::Duplicate),
        SyncAdmit::Fenced => Some(AckStatus::Fenced),
        SyncAdmit::Gap { expected } => Some(AckStatus::Gap {
            expected: *expected,
        }),
    }
}

impl CoreLoop {
    pub(super) fn apply_crdt_sync_gated(
        &mut self,
        task: &ExecutionTask,
        params: CrdtApplyParams<'_>,
        prov: &SyncProvenance,
    ) -> Response {
        let CrdtApplyParams {
            collection,
            document_id,
            delta,
            surrogate,
            peer_id,
            constraint_version_required,
            expected_frontier_digest,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
            provenance: _,
        } = params;
        let tenant_id = task.request.tenant_id;

        // A frontier fence is an admission precondition, not an apply-time
        // validation. Classify first without consuming a newer producer epoch;
        // for every non-Apply result retain the established mutating admission
        // path and its epoch-floor semantics.
        let classified = self.sync_classify(prov);
        if !matches!(classified, SyncAdmit::Apply) {
            let admit = self.sync_admit(prov);
            let current_hwm = self.sync_hwm_value(prov.producer_id, prov.stream_id);
            let Some(status) = ack_status_for(&admit) else {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "sync admission classification changed unexpectedly".into(),
                    },
                );
            };
            return self.sync_ack_response(task, status, current_hwm);
        }

        // Verify a fenced write against immutable state before creating an
        // engine, observing constraints, advancing an epoch floor/HWM, or
        // touching checkpoint and sparse state.
        if let Some(expected) = expected_frontier_digest {
            let actual =
                self.current_crdt_frontier_digest(task.request.database_id, tenant_id, collection);
            if expected != actual {
                return self
                    .response_error(task, ErrorCode::CrdtFrontierMismatch { expected, actual });
            }
        }

        // The fence passed, so now perform normal mutating admission. This is
        // intentionally after the frontier check: a stale higher-epoch frame
        // must leave the producer floor unchanged.
        let admit = self.sync_admit(prov);
        let current_hwm = self.sync_hwm_value(prov.producer_id, prov.stream_id);
        if let Some(status) = ack_status_for(&admit) {
            return self.sync_ack_response(task, status, current_hwm);
        }

        // Borrow the engine in a nested block so the &mut borrow is dropped
        // before sync_commit takes &mut self for sync_hwm.
        //
        // Before validating, fence the delta against the constraint version it
        // was admitted against. `SetConstraints` rides the same per-vshard data
        // Raft log as this `CrdtApply`, so at this log index every replica has
        // applied the identical log prefix and therefore has the identical
        // installed `constraint_versions[collection]` — the gate decision is
        // deterministic across replicas, no divergence.
        let (outcome, materialized, declared_row_present) = {
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
            let installed = engine.installed_constraint_version(collection);
            if constraint_version_required > installed {
                (GateOutcome::Pending { installed }, None, false)
            } else {
                let applied = engine.apply_committed_delta_authenticated(
                    collection,
                    delta,
                    surrogate,
                    document_id,
                    peer_id,
                    DeltaSigningAdmission {
                        auth: nodedb_crdt::CrdtAuthContext {
                            user_id: auth_user_id,
                            tenant_id: tenant_id.as_u64(),
                            auth_expires_at: 0,
                            delta_signature,
                            device_id: auth_device_id,
                            seq_no: auth_seq_no,
                        },
                        required: signing_required,
                        preverified: true,
                    },
                );
                // On a Clean apply that actually imported something, read the
                // merged row back and encode it while the engine borrow is
                // still live so the bytes can be materialized into the sparse
                // store below. A delta that imported nothing has nothing new to
                // materialize.
                let advanced = matches!(
                    applied,
                    ValidatedApplyOutcome::Clean {
                        imported_ops: 1..,
                        ..
                    }
                );
                let mat = if advanced && surrogate != Surrogate::ZERO {
                    Self::encode_crdt_row(engine, collection, document_id)
                } else {
                    None
                };
                // Whether the row this frame declared is readable at all. Only
                // consulted for a delta that imported nothing, where it is the
                // difference between a replay of a write that is present and a
                // write that was discarded before it ever landed.
                let declared_row_present = engine.row_exists(collection, document_id);
                (GateOutcome::Applied(applied), mat, declared_row_present)
            }
        };
        // engine borrow is dropped here; mark_dirty / sync_commit take
        // &mut self, and the sparse materialize takes &self.
        let mut imported_authoritative = false;
        let disposition = match outcome {
            GateOutcome::Pending { installed } => {
                // Create-race: the constraints this delta was admitted against
                // are not yet installed on THIS replica (the reconcile loop
                // delivers SetConstraints asynchronously). Do NOT import an
                // unvalidated delta — that is exactly the hole this fence
                // closes. The client re-pushes once the install catches up, so
                // this is retryable and not a dead letter.
                debug!(
                    core = self.core_id,
                    %collection,
                    required = constraint_version_required,
                    installed,
                    "crdt apply fenced: constraint version pending (retryable)"
                );
                GateDisposition::Retryable
            }
            GateOutcome::Applied(ValidatedApplyOutcome::Clean {
                write_set,
                imported_ops,
            }) => {
                // Enforce the one-document-per-delta sync contract. A delta
                // that coalesced multiple documents (or targeted a synthetic
                // frame id that matches no written row) cannot be materialized
                // past its single surrogate; reject it loudly so the client
                // re-pushes one delta per document instead of silently losing
                // rows.
                match Self::single_document_write_set(collection, document_id, &write_set) {
                    Err(detail) => {
                        imported_authoritative = true;
                        self.checkpoint_coordinator.mark_dirty("crdt", 1);
                        warn!(
                            core = self.core_id,
                            %collection,
                            %document_id,
                            detail = %detail,
                            "crdt sync apply rejected: multi-document delta violates one-document-per-delta contract"
                        );
                        GateDisposition::Terminal(ViolationType::ConstraintViolation { detail })
                    }
                    // The delta contributed no operations: every one it carried
                    // was already in this document, so nothing was written and
                    // nothing is dirty. Reporting `Applied` here is what let a
                    // peer-id collision retire a write that was discarded.
                    Ok(()) if imported_ops == 0 => {
                        if !declared_row_present {
                            // The delta imported nothing AND the row it declared
                            // does not exist. A replayed delete looks like this
                            // and is harmless; so does a delta whose operations
                            // were consumed by another replica claiming the same
                            // peer id, and that one lost a write. The peer-id
                            // binding at the session boundary is what keeps the
                            // second case from reaching here — if this fires,
                            // that binding was unavailable for this producer.
                            warn!(
                                core = self.core_id,
                                %collection,
                                %document_id,
                                peer_id,
                                "crdt sync apply imported no operations and its declared row is \
                                 absent: a replayed delete, or a peer id shared with another \
                                 replica whose writes consumed this counter range"
                            );
                        }
                        GateDisposition::Deduplicated
                    }
                    Ok(()) => {
                        imported_authoritative = true;
                        self.checkpoint_coordinator.mark_dirty("crdt", 1);
                        GateDisposition::Applied
                    }
                }
            }
            GateOutcome::Applied(ValidatedApplyOutcome::Rejected(vt)) => {
                imported_authoritative = true;
                self.checkpoint_coordinator.mark_dirty("crdt", 1);
                GateDisposition::Terminal(vt)
            }
            GateOutcome::Applied(ValidatedApplyOutcome::Malformed) => {
                // The bytes did not decode, so nothing was imported. Acking
                // this as `Applied` would retire a write on the sender that
                // never landed anywhere — the corrupt frame has to come back as
                // a refusal. It is terminal: the same bytes decode the same way
                // forever, so a retry would spin.
                warn!(
                    core = self.core_id,
                    %collection,
                    %document_id,
                    "crdt sync apply refused: delta bytes are malformed; nothing was applied"
                );
                GateDisposition::Terminal(ViolationType::MalformedDelta {
                    detail: format!(
                        "delta for {collection}/{document_id} could not be decoded; \
                         nothing was applied"
                    ),
                })
            }
            GateOutcome::Applied(ValidatedApplyOutcome::PendingDependencies) => {
                // Well-formed operations that arrived without their causal
                // history: Loro buffered them and the applied state did not
                // move. Retryable — the identical bytes land once the missing
                // predecessors arrive, so the HWM must stay put.
                warn!(
                    core = self.core_id,
                    %collection,
                    %document_id,
                    constraint = super::CRDT_PENDING_DEPENDENCIES,
                    "crdt sync apply refused: delta depends on operations absent from \
                     this collection's document; nothing applied, high-water-mark held"
                );
                GateDisposition::Retryable
            }
        };

        // Materialize the merged document into the sparse store so
        // DocumentScan / ShapeSnapshot see the synced write. `materialized` is
        // Some only on a Clean apply with an assigned surrogate, and a refused
        // delta must not surface a partial row.
        if matches!(disposition, GateDisposition::Applied)
            && let Some(bytes) = materialized
        {
            self.materialize_synced_document(
                task,
                tenant_id.as_u64(),
                collection,
                document_id,
                surrogate,
                &bytes,
            );
        }
        if imported_authoritative {
            self.note_collection_write_lsn(task, collection);
        }

        // The high-water-mark and the client-visible frame are decided
        // together, from the one disposition, so they cannot contradict.
        match disposition {
            GateDisposition::Retryable => {
                // Nothing applied and the client will re-push at this seq.
                // Committing here would turn that re-push into a `Duplicate`
                // and drop the write — the silent-loss pattern this guard
                // exists to prevent. Report the unchanged mark instead.
                self.sync_ack_response(task, AckStatus::Gap { expected: prov.seq }, current_hwm)
            }
            GateDisposition::Terminal(violation) => {
                // Permanently refused: it will never succeed on a re-push, so
                // holding the stream for it buys nothing.
                self.sync_commit(prov);
                self.sync_reject_response(task, violation, prov.seq)
            }
            GateDisposition::Deduplicated => {
                // The operations are in the document, so the sender is free to
                // retire the write and a re-push must dedup rather than be
                // admitted again: the mark advances exactly as it does for an
                // apply. Only the reported status differs, and it has to.
                self.sync_commit(prov);
                self.sync_ack_response(task, AckStatus::Duplicate, prov.seq)
            }
            GateDisposition::Applied => {
                self.sync_commit(prov);
                self.sync_ack_response(task, AckStatus::Applied, prov.seq)
            }
        }
    }
}
