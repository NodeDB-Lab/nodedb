// SPDX-License-Identifier: BUSL-1.1

//! The single place a CRDT delta's fate becomes a client frame.
//!
//! A delta can end three ways, and exactly one of them is terminal:
//!
//! 1. **Applied / admitted / retryably refused** — the Data Plane returns a
//!    [`SyncAckResult`] whose outcome is an [`AckStatus`]. Every such status is
//!    survivable for the sender, including [`AckStatus::Gap`], which means
//!    nothing applied and the identical delta at the same seq should be
//!    re-pushed. All of them ride a `DeltaAck`.
//! 2. **Terminally refused** — the validator produced a deterministic
//!    [`ViolationType`]. The sender must compensate, not retry: `DeltaReject`.
//! 3. **Never reached the apply** — quota, surrogate assignment, timeout, or a
//!    transport error. Whether that is terminal depends on the typed error, and
//!    it is read from the error, never guessed from its message.
//!
//! Routing all three through this one function is the point. The bug this
//! replaces came from deciding terminality by *which channel* an outcome
//! arrived on: a refusal carrying a reason was assumed terminal, so the
//! retryable ones were rewritten into permanent rejections. The sender rolled
//! its write back while the server held its stream position waiting for a
//! re-push that would never come — a write lost with every server counter
//! green. Terminality is now a property of the value, read in one place.

use tracing::warn;

use nodedb_types::sync::violation::ViolationType;
use nodedb_types::sync::wire::{AckStatus, SyncAckResult, SyncOutcome};

use super::super::super::refusal::retryable_refusal_reason;
use super::super::super::wire::{
    CompensationHint, DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType,
};

/// Build the client frame for a completed dispatch.
///
/// `provisional_ack` is the frame the in-memory session already produced; its
/// `mutation_id` and `clock_skew_warning_ms` are preserved while the durable
/// outcome overwrites `applied_seq` and `status`.
pub(super) fn frame_for_dispatch(
    delta_msg: &DeltaPushMsg,
    provisional_ack: &SyncFrame,
    dispatch_result: crate::Result<Vec<u8>>,
) -> Option<SyncFrame> {
    let payload = match dispatch_result {
        Ok(payload) => payload,
        Err(error) => return frame_for_dispatch_error(delta_msg, &error),
    };

    let gate_result = match zerompk::from_msgpack::<SyncAckResult>(&payload) {
        Ok(result) => result,
        Err(err) => {
            // The Data Plane's outcome is unreadable, so whether the delta
            // applied is unknown. Acking would assert success we cannot verify
            // and let the sender retire the write; rejecting would make it roll
            // back a write that may well have landed. Neither is honest — but a
            // re-push is idempotent (the gate dedups by seq), so refusing
            // retryably is the only answer that cannot lose data.
            warn!(
                collection = %delta_msg.collection,
                doc = %delta_msg.document_id,
                error = %err,
                "sync: failed to decode SyncAckResult from Data Plane; refusing retryably \
                 rather than acking an unverified apply"
            );
            return retryable_gap_ack(delta_msg, provisional_ack);
        }
    };

    match gate_result.outcome {
        SyncOutcome::Ack(status) => {
            ack_frame(delta_msg, provisional_ack, status, gate_result.applied_seq)
        }
        SyncOutcome::Rejected(violation) => reject_frame(delta_msg, &violation),
    }
}

/// Rebuild the provisional ack with the durable status and applied sequence.
fn ack_frame(
    delta_msg: &DeltaPushMsg,
    provisional_ack: &SyncFrame,
    status: AckStatus,
    applied_seq: u64,
) -> Option<SyncFrame> {
    if let AckStatus::Gap { expected } = &status {
        warn!(
            collection = %delta_msg.collection,
            doc = %delta_msg.document_id,
            expected,
            "sync: delta refused retryably; nothing applied, client should re-push at this seq"
        );
    }
    let (mutation_id, clock_skew_warning_ms) = match provisional_ack.decode_body::<DeltaAckMsg>() {
        Some(existing) => (existing.mutation_id, existing.clock_skew_warning_ms),
        None => (delta_msg.mutation_id, None),
    };
    let ack = DeltaAckMsg {
        mutation_id,
        lsn: 0, // WAL LSN is not surfaced by dispatch_system_with_source; left as 0.
        clock_skew_warning_ms,
        applied_seq,
        status,
    };
    SyncFrame::try_encode(SyncMessageType::DeltaAck, &ack)
}

/// Refuse retryably at the delta's own sequence, holding nothing against it.
///
/// Used when the outcome is unknown rather than refused: the sender re-pushes
/// the identical frame, which the gate deduplicates if it did in fact apply.
fn retryable_gap_ack(delta_msg: &DeltaPushMsg, provisional_ack: &SyncFrame) -> Option<SyncFrame> {
    ack_frame(
        delta_msg,
        provisional_ack,
        AckStatus::Gap {
            expected: delta_msg.seq,
        },
        // The mark is whatever the Data Plane last committed; reporting the
        // delta's own seq would claim an apply that may not have happened.
        delta_msg.seq.saturating_sub(1),
    )
}

/// Terminal refusal: carry the precise, structured hint the validator produced.
fn reject_frame(delta_msg: &DeltaPushMsg, violation: &ViolationType) -> Option<SyncFrame> {
    warn!(
        collection = %delta_msg.collection,
        doc = %delta_msg.document_id,
        violation = %violation,
        "sync: delta terminally rejected by the CRDT validator"
    );
    let reject = DeltaRejectMsg {
        mutation_id: delta_msg.mutation_id,
        reason: violation.to_string(),
        compensation: Some(violation.to_compensation_hint()),
    };
    SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject)
}

/// The apply never reached (or never left) the Data Plane.
///
/// A retryable refusal keeps its retryable shape here too: the Data Plane
/// classified it as one, and re-wrapping it as a rejection would reintroduce
/// exactly the loss this module exists to prevent.
fn frame_for_dispatch_error(delta_msg: &DeltaPushMsg, error: &crate::Error) -> Option<SyncFrame> {
    // The validator's terminal verdict on the frame keeps its structured
    // violation and compensation hint.
    if let crate::Error::DataPlane(crate::bridge::envelope::ErrorCode::SyncRejected {
        violation,
        ..
    }) = error
    {
        return reject_frame(delta_msg, violation);
    }
    if let Some(reason) = retryable_refusal_reason(error) {
        warn!(
            collection = %delta_msg.collection,
            doc = %delta_msg.document_id,
            reason,
            "sync: delta refused retryably before apply; client should re-push at this seq"
        );
        let ack = DeltaAckMsg {
            mutation_id: delta_msg.mutation_id,
            lsn: 0,
            clock_skew_warning_ms: None,
            applied_seq: delta_msg.seq.saturating_sub(1),
            status: AckStatus::Gap {
                expected: delta_msg.seq,
            },
        };
        return SyncFrame::try_encode(SyncMessageType::DeltaAck, &ack);
    }

    let hint = compensation_hint_for_dispatch_error(error);
    warn!(
        collection = %delta_msg.collection,
        doc = %delta_msg.document_id,
        hint = hint.code(),
        error = %error,
        "sync: delta rejected by Data Plane"
    );
    let reject = DeltaRejectMsg {
        mutation_id: delta_msg.mutation_id,
        reason: error.to_string(),
        compensation: Some(hint),
    };
    SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject)
}

/// Classify a dispatch failure into the hint the edge compensates against.
pub(super) fn compensation_hint_for_dispatch_error(e: &crate::Error) -> CompensationHint {
    use crate::bridge::envelope::ErrorCode;

    match e {
        crate::Error::DataPlane(code) => match code {
            ErrorCode::RejectedConstraint { constraint, detail } => CompensationHint::Custom {
                constraint: constraint.clone(),
                detail: detail.clone(),
            },
            ErrorCode::RejectedPrevalidation { reason } => CompensationHint::Custom {
                constraint: "prevalidation".into(),
                detail: reason.clone(),
            },
            ErrorCode::RejectedAuthz { .. } => CompensationHint::PermissionDenied,
            ErrorCode::RateExceeded { retry_after_ms, .. } => CompensationHint::RateLimited {
                retry_after_ms: *retry_after_ms,
            },
            other => CompensationHint::Custom {
                constraint: "apply_failed".into(),
                detail: format!("{other:?}"),
            },
        },
        crate::Error::RejectedConstraint {
            constraint, detail, ..
        } => CompensationHint::Custom {
            constraint: constraint.clone(),
            detail: detail.clone(),
        },
        crate::Error::RejectedPrevalidation { constraint, reason } => CompensationHint::Custom {
            constraint: constraint.clone(),
            detail: reason.clone(),
        },
        crate::Error::RejectedAuthz { .. } => CompensationHint::PermissionDenied,
        crate::Error::RateExceeded { retry_after_ms, .. } => CompensationHint::RateLimited {
            retry_after_ms: *retry_after_ms,
        },
        other => CompensationHint::Custom {
            constraint: "apply_failed".into(),
            detail: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ErrorCode;
    use crate::types::TenantId;

    fn delta() -> DeltaPushMsg {
        DeltaPushMsg {
            collection: "orders".into(),
            document_id: "order-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 42,
            device_id: 0,
            delta_signature: [0; 32],
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 5,
        }
    }

    fn provisional() -> SyncFrame {
        SyncFrame::try_encode(
            SyncMessageType::DeltaAck,
            &DeltaAckMsg {
                mutation_id: 42,
                lsn: 0,
                clock_skew_warning_ms: Some(7),
                applied_seq: 0,
                status: AckStatus::Accepted,
            },
        )
        .expect("provisional ack encodes")
    }

    fn decode_ack(frame: &SyncFrame) -> DeltaAckMsg {
        assert_eq!(frame.msg_type, SyncMessageType::DeltaAck);
        frame.decode_body().expect("ack decodes")
    }

    #[test]
    fn a_gap_outcome_reaches_the_client_as_a_retryable_ack() {
        // The regression this module exists for: a refusal the Data Plane
        // marked retryable must not be rewritten into a DeltaReject.
        let payload =
            zerompk::to_msgpack_vec(&SyncAckResult::acked(AckStatus::Gap { expected: 5 }, 4))
                .expect("encode gate result");
        let frame = frame_for_dispatch(&delta(), &provisional(), Ok(payload)).expect("frame");
        let ack = decode_ack(&frame);
        assert_eq!(ack.status, AckStatus::Gap { expected: 5 });
        assert_eq!(ack.applied_seq, 4);
        // The session's provisional fields survive the rebuild.
        assert_eq!(ack.mutation_id, 42);
        assert_eq!(ack.clock_skew_warning_ms, Some(7));
    }

    #[test]
    fn a_pre_apply_retryable_refusal_reaches_the_client_as_a_retryable_ack() {
        // Pending causal dependencies are caught by admission prevalidation,
        // before the apply runs, and come back on the error channel. That is a
        // different route to the same disposition and must not diverge.
        let error = crate::Error::DataPlane(ErrorCode::RetryableRefusal {
            reason: "delta depends on operations absent from this document".into(),
        });
        let frame = frame_for_dispatch(&delta(), &provisional(), Err(error)).expect("frame");
        let ack = decode_ack(&frame);
        assert_eq!(ack.status, AckStatus::Gap { expected: 5 });
    }

    #[test]
    fn a_terminal_violation_reaches_the_client_as_a_rejection() {
        let payload = zerompk::to_msgpack_vec(&SyncAckResult::rejected(
            ViolationType::UniqueViolation {
                field: "email".into(),
                value: "a@b.com".into(),
            },
            5,
        ))
        .expect("encode gate result");
        let frame = frame_for_dispatch(&delta(), &provisional(), Ok(payload)).expect("frame");
        assert_eq!(frame.msg_type, SyncMessageType::DeltaReject);
        let reject: DeltaRejectMsg = frame.decode_body().expect("reject decodes");
        assert_eq!(
            reject.compensation,
            Some(CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "a@b.com".into(),
            })
        );
    }

    /// A frame the gate refused for good arrives on the error channel, since
    /// its record is cancelled. It keeps the validator's structured hint.
    #[test]
    fn a_terminal_refusal_on_the_error_channel_reaches_the_client_as_a_rejection() {
        let error = crate::Error::DataPlane(ErrorCode::SyncRejected {
            violation: ViolationType::UniqueViolation {
                field: "email".into(),
                value: "a@b.com".into(),
            },
            applied_seq: 5,
            provenance: nodedb_types::sync::wire::SyncProvenance {
                producer_id: 1,
                epoch: 1,
                stream_id: 1,
                seq: 5,
            },
        });
        let frame = frame_for_dispatch(&delta(), &provisional(), Err(error)).expect("frame");
        assert_eq!(frame.msg_type, SyncMessageType::DeltaReject);
        let reject: DeltaRejectMsg = frame.decode_body().expect("reject decodes");
        assert_eq!(
            reject.compensation,
            Some(CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "a@b.com".into(),
            })
        );
    }

    #[test]
    fn an_unreadable_outcome_is_refused_retryably_not_acked_or_rejected() {
        // Neither "it applied" nor "it never will" is knowable here. Only the
        // retryable answer cannot lose the write.
        let frame = frame_for_dispatch(&delta(), &provisional(), Ok(b"not-msgpack".to_vec()))
            .expect("frame");
        let ack = decode_ack(&frame);
        assert_eq!(ack.status, AckStatus::Gap { expected: 5 });
    }

    #[test]
    fn a_terminal_dispatch_error_still_rejects() {
        let error = crate::Error::DataPlane(ErrorCode::RejectedPrevalidation {
            reason: "malformed blob".into(),
        });
        let frame = frame_for_dispatch(&delta(), &provisional(), Err(error)).expect("frame");
        assert_eq!(frame.msg_type, SyncMessageType::DeltaReject);
    }

    #[test]
    fn preserved_data_plane_constraint_maps_to_custom_with_real_name() {
        // A Data-Plane RejectedConstraint carries the constraint name + detail,
        // but not the offending field/value — so the honest hint is Custom with
        // the real name, never a fabricated UniqueViolation.
        let e = crate::Error::DataPlane(ErrorCode::RejectedConstraint {
            constraint: "users_email_unique".into(),
            detail: "value 'a@b.com' already exists".into(),
        });
        match compensation_hint_for_dispatch_error(&e) {
            CompensationHint::Custom { constraint, detail } => {
                assert_eq!(constraint, "users_email_unique");
                assert!(detail.contains("a@b.com"));
            }
            other => panic!("expected Custom, got {other:?}"),
        }
    }

    #[test]
    fn data_plane_authz_maps_to_permission_denied() {
        let e = crate::Error::DataPlane(ErrorCode::RejectedAuthz {
            resource: "RLS write policy on 'orders' rejected the row".into(),
        });
        assert_eq!(
            compensation_hint_for_dispatch_error(&e),
            CompensationHint::PermissionDenied
        );
    }

    #[test]
    fn data_plane_rate_exceeded_preserves_retry_after() {
        let e = crate::Error::DataPlane(ErrorCode::RateExceeded {
            gate: "writes".into(),
            retry_after_ms: 1500,
        });
        assert_eq!(
            compensation_hint_for_dispatch_error(&e),
            CompensationHint::RateLimited {
                retry_after_ms: 1500
            }
        );
    }

    #[test]
    fn import_failure_maps_to_apply_failed_not_fabricated_constraint() {
        // The realistic CRDT-apply failure is a Loro import error, surfaced as
        // ErrorCode::Internal. It must NOT be guessed into a UNIQUE/FK hint.
        let e = crate::Error::DataPlane(ErrorCode::Internal {
            detail: "loro import failed".into(),
        });
        match compensation_hint_for_dispatch_error(&e) {
            CompensationHint::Custom { constraint, .. } => assert_eq!(constraint, "apply_failed"),
            other => panic!("expected Custom apply_failed, got {other:?}"),
        }
    }

    #[test]
    fn typed_authz_error_also_maps_to_permission_denied() {
        // Errors that arrive already typed (e.g. via the Raft path) classify too.
        let e = crate::Error::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: "users".into(),
        };
        assert_eq!(
            compensation_hint_for_dispatch_error(&e),
            CompensationHint::PermissionDenied
        );
    }
}
