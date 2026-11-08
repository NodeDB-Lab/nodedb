// SPDX-License-Identifier: BUSL-1.1

//! Shared decode for the Data Plane's [`SyncAckResult`] reply.
//!
//! Typed outcomes preserve their applied sequence. Legacy sessions use
//! [`decode_sync_ack`] for malformed payloads. Vector sessions close on
//! unreadable replies before calling [`EngineAck::from_result`].

use tracing::warn;

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

/// Decode the [`SyncAckResult`] returned by the Data Plane in a sync ingest
/// response payload.
///
/// A decode failure is not evidence of an apply. Synthesising `Applied` would
/// retire a write on the sender that may never have landed — the same silent
/// loss as reporting a retryable refusal as terminal, just in the other
/// direction. Because a re-push is idempotent (the gate deduplicates by seq)
/// while a wrongly-acked write is gone, an unreadable envelope is reported as a
/// retryable [`AckStatus::Gap`] at `fallback_seq`: the sender re-sends, and the
/// gate answers `Duplicate` if the write did in fact apply.
pub(super) fn decode_sync_ack(
    payload_bytes: &[u8],
    op: &str,
    session_id: &str,
    collection: &str,
    fallback_seq: u64,
) -> EngineAck {
    let result = match zerompk::from_msgpack::<SyncAckResult>(payload_bytes) {
        Ok(result) => result,
        Err(e) => {
            warn!(
                session = %session_id,
                %collection,
                op,
                error = %e,
                "sync: failed to decode SyncAckResult from Data Plane; refusing retryably \
                 rather than acking an unverified apply"
            );
            return EngineAck::Status {
                status: AckStatus::Gap {
                    expected: fallback_seq,
                },
                applied_seq: fallback_seq.saturating_sub(1),
            };
        }
    };
    if let SyncOutcome::Rejected(violation) = &result.outcome {
        warn!(
            session = %session_id,
            %collection,
            op,
            violation = %violation,
            "sync: ingest terminally refused by the Data Plane"
        );
    }
    EngineAck::from_result(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unreadable_envelope_is_retryable_not_a_fabricated_apply() {
        // Synthesising `Applied` from an unreadable envelope tells the sender a
        // write landed that nobody verified; the sender then drops it.
        match decode_sync_ack(b"not-msgpack", "insert", "s1", "docs", 9) {
            EngineAck::Status { status, .. } => {
                assert_eq!(status, AckStatus::Gap { expected: 9 });
            }
            EngineAck::Rejected { violation: v, .. } => panic!("expected a retryable gap, got {v}"),
        }
    }

    #[test]
    fn a_clean_ack_passes_its_status_and_sequence_through() {
        let payload = zerompk::to_msgpack_vec(&SyncAckResult::acked(AckStatus::Duplicate, 4))
            .expect("encode");
        match decode_sync_ack(&payload, "insert", "s1", "docs", 9) {
            EngineAck::Status {
                status,
                applied_seq,
            } => {
                assert_eq!(status, AckStatus::Duplicate);
                assert_eq!(applied_seq, 4);
            }
            EngineAck::Rejected { violation: v, .. } => panic!("expected a status, got {v}"),
        }
    }

    #[test]
    fn a_terminal_refusal_is_surfaced_rather_than_dropped() {
        // Previously every per-engine session read only `status` and discarded
        // the refusal, so a terminally refused ingest was acked as applied.
        let payload = zerompk::to_msgpack_vec(&SyncAckResult::rejected(
            ViolationType::ConstraintViolation {
                detail: "bad row".into(),
            },
            4,
        ))
        .expect("encode");
        match decode_sync_ack(&payload, "insert", "s1", "docs", 9) {
            EngineAck::Rejected {
                violation: ViolationType::ConstraintViolation { detail },
                applied_seq,
            } => {
                assert_eq!(applied_seq, 4);
                assert_eq!(detail, "bad row");
            }
            EngineAck::Rejected {
                violation: other, ..
            } => panic!("wrong violation: {other}"),
            EngineAck::Status { status, .. } => {
                panic!("a terminal refusal was reported as status {status:?}")
            }
        }
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    #[test]
    fn a_rejected_wire_ack_preserves_its_consumed_frontier() {
        let bytes =
            zerompk::to_msgpack_vec(&SyncAckResult::rejected(ViolationType::PermissionDenied, 9))
                .unwrap();
        let ack = decode_sync_ack(&bytes, "insert", "s1", "docs", 9).into_wire();
        assert!(!ack.accepted);
        assert_eq!(ack.applied_seq, 9);
        assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    }
}
