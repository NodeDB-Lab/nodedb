// SPDX-License-Identifier: BUSL-1.1

//! Shared decode for the Data Plane's [`SyncAckResult`] reply.
//!
//! Typed core outcomes preserve their exact frontier. Unreadable outcomes close the session.

use nodedb_types::sync::violation::ViolationType;
use nodedb_types::sync::wire::{AckStatus, SyncAckResult, SyncOutcome};

/// What a per-engine session should tell its client.
pub(super) enum EngineAck {
    /// Report this status at this applied sequence.
    Status { status: AckStatus, applied_seq: u64 },
    /// The ingest will never apply, and the deterministic reason why. The
    /// session must surface a refusal rather than any status — reporting a
    /// status here would tell the sender its write landed, or is still coming,
    /// when neither is true.
    Rejected {
        violation: ViolationType,
        applied_seq: u64,
    },
}

/// The four fields every per-engine wire ack carries to describe an outcome.
///
/// Built only through [`EngineAck::into_wire`] so `accepted` / `reject_reason`
/// are always derived from the same value as `status`. Setting them
/// independently is how a refused ingest came to be sent with
/// `status: Applied`, which the edge then read as success and retired.
pub(super) struct EngineAckWire {
    pub accepted: bool,
    pub reject_reason: Option<String>,
    pub applied_seq: u64,
    pub status: AckStatus,
}

impl EngineAck {
    pub(super) fn from_result(result: SyncAckResult) -> Self {
        match result.outcome {
            SyncOutcome::Ack(status) => Self::Status {
                status,
                applied_seq: result.applied_seq,
            },
            SyncOutcome::Rejected(violation) => Self::Rejected {
                violation,
                applied_seq: result.applied_seq,
            },
        }
    }

    pub(super) fn into_wire(self) -> EngineAckWire {
        match self {
            Self::Status {
                status,
                applied_seq,
            } => EngineAckWire {
                accepted: true,
                reject_reason: None,
                applied_seq,
                status,
            },
            Self::Rejected {
                violation,
                applied_seq,
            } => {
                let reason = violation.to_string();
                EngineAckWire {
                    accepted: false,
                    reject_reason: Some(reason.clone()),
                    // A deterministic refusal consumes its sequence.
                    applied_seq,
                    status: AckStatus::Rejected { reason },
                }
            }
        }
    }
}

/// Only a typed core verdict supplies a stream frontier.
pub(super) fn decode_core_ack(payload: &[u8]) -> Option<EngineAckWire> {
    let result: SyncAckResult = zerompk::from_msgpack(payload).ok()?;
    Some(EngineAck::from_result(result).into_wire())
}

/// Unknown outcomes require reconnecting with the original sequence.
pub(super) fn decode_core_error(error: &crate::Error) -> Option<EngineAckWire> {
    use crate::bridge::envelope::ErrorCode;
    let crate::Error::DataPlane(code) = error else {
        return None;
    };
    let ack = match code {
        ErrorCode::SyncRejected {
            violation,
            applied_seq,
            ..
        } => EngineAck::Rejected {
            violation: violation.clone(),
            applied_seq: *applied_seq,
        },
        ErrorCode::SyncNotApplied { hold, applied_seq } => EngineAck::Status {
            status: hold.ack_status(),
            applied_seq: *applied_seq,
        },
        _ => return None,
    };
    Some(ack.into_wire())
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    #[test]
    fn a_rejected_wire_ack_preserves_its_consumed_frontier() {
        let bytes =
            zerompk::to_msgpack_vec(&SyncAckResult::rejected(ViolationType::PermissionDenied, 9))
                .unwrap();
        let ack = decode_core_ack(&bytes).expect("typed ACK");
        assert!(!ack.accepted);
        assert_eq!(ack.applied_seq, 9);
        assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    }

    #[test]
    fn strict_core_decoding_never_invents_a_frontier() {
        assert!(decode_core_ack(b"invalid msgpack").is_none());
        assert!(
            decode_core_error(&crate::Error::Internal {
                detail: "storage unavailable".into()
            })
            .is_none()
        );
        assert!(
            decode_core_error(&crate::Error::DataPlane(
                crate::bridge::envelope::ErrorCode::RejectedPrevalidation {
                    reason: "ordinary validation error".into()
                }
            ))
            .is_none()
        );
    }

    #[test]
    fn strict_core_errors_preserve_consumed_and_held_frontiers() {
        use crate::bridge::envelope::{ErrorCode, SyncHold};
        let error = crate::Error::DataPlane(ErrorCode::SyncRejected {
            violation: ViolationType::PermissionDenied,
            applied_seq: 7,
            provenance: nodedb_types::sync::wire::SyncProvenance {
                producer_id: 2,
                epoch: 1,
                stream_id: 3,
                seq: 7,
            },
        });
        let ack = decode_core_error(&error).unwrap();
        assert_eq!(ack.applied_seq, 7);
        assert!(!ack.accepted);
        assert!(matches!(ack.status, AckStatus::Rejected { .. }));
        let error = crate::Error::DataPlane(ErrorCode::SyncNotApplied {
            hold: SyncHold::Gap { expected: 5 },
            applied_seq: 4,
        });
        let ack = decode_core_error(&error).unwrap();
        assert_eq!(ack.applied_seq, 4);
        assert_eq!(ack.status, AckStatus::Gap { expected: 5 });
    }
    #[test]
    fn only_a_consumed_refusal_carries_a_reject_reason() {
        for status in [AckStatus::Applied, AckStatus::Gap { expected: 2 }] {
            let payload =
                zerompk::to_msgpack_vec(&SyncAckResult::acked(status, 1)).expect("encode ACK");
            assert!(
                decode_core_ack(&payload)
                    .expect("typed ACK")
                    .reject_reason
                    .is_none()
            );
        }
        let payload = zerompk::to_msgpack_vec(&SyncAckResult::rejected(
            ViolationType::ConstraintViolation {
                detail: "schema mismatch".into(),
            },
            2,
        ))
        .expect("encode refusal");
        let ack = decode_core_ack(&payload).expect("typed ACK");
        assert_eq!(
            ack.reject_reason.as_deref(),
            Some("constraint:schema mismatch")
        );
    }
}
