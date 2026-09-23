// SPDX-License-Identifier: BUSL-1.1

//! Whether a failed sync dispatch is retryable or terminal.
//!
//! Every engine ack path faces the same question and must answer it the same
//! way: the sender retires its durable entry on a terminal refusal and re-sends
//! on a retryable one, so getting this backwards either loses the write or
//! spins forever re-sending one that will never land.
//!
//! The judgment lives here, in one place, for the same reason the CRDT delta
//! path routes all three of its outcomes through a single function: the bug
//! both guard against came from deciding terminality per-channel, letting two
//! paths disagree about the same error. A shared classifier cannot disagree
//! with itself.

use nodedb_types::sync::wire::AckStatus;

/// The reason text when `error` means "nothing applied, re-send the same frame".
///
/// Matched on the typed error only — never by substring-matching the human
/// message, which is how a rewording silently turns a retry into a loss.
pub(super) fn retryable_refusal_reason(error: &crate::Error) -> Option<&str> {
    use crate::bridge::envelope::ErrorCode;
    match error {
        crate::Error::RetryableRefusal { reason } => Some(reason),
        crate::Error::DataPlane(ErrorCode::RetryableRefusal { reason }) => Some(reason),
        _ => None,
    }
}

/// Whether `error` means the write never got a verdict, as opposed to being
/// refused on its merits.
///
/// These are the failures where the cluster never judged the write at all — it
/// timed out, the leader moved, the sequencer was absent, memory pressure
/// shed it, or the dispatcher refused it at capacity. Nothing about the write
/// itself is wrong, so the same bytes are expected to land once the condition
/// clears.
fn is_indeterminate(error: &crate::Error) -> bool {
    use crate::bridge::envelope::ErrorCode;
    matches!(
        error,
        crate::Error::DeadlineExceeded { .. }
            | crate::Error::CrdtAdmissionTimeout { .. }
            | crate::Error::NoLeader { .. }
            | crate::Error::NotLeader { .. }
            | crate::Error::StaleReadNotLeader { .. }
            | crate::Error::SequencerUnavailable
            | crate::Error::Backpressure { .. }
            | crate::Error::DispatchCapacity { .. }
            | crate::Error::ConflictRetry { .. }
            | crate::Error::DataPlane(
                ErrorCode::DeadlineExceeded
                    | ErrorCode::ResourcesExhausted
                    | ErrorCode::DispatchCapacity { .. }
                    | ErrorCode::ConflictRetry
            )
    )
}

/// The [`AckStatus`] an engine ack must carry when its dispatch failed.
///
/// A dispatch can fail because the write was refused on its merits (terminal —
/// the sender must compensate) or because it never got a verdict at all: a
/// timeout, a moved leader, shed load. The second kind is retryable, and
/// reporting it as [`AckStatus::Rejected`] tells the sender to permanently drop
/// a write the cluster never actually refused — a silent loss on every
/// transient failure, which are precisely the failures that do occur in normal
/// operation.
///
/// `next_seq` is the sequence the sender should resume from — its own seq for
/// this batch, since nothing applied.
pub(super) fn ack_status_for_dispatch_error(error: &crate::Error, next_seq: u64) -> AckStatus {
    if retryable_refusal_reason(error).is_some() || is_indeterminate(error) {
        return AckStatus::Gap { expected: next_seq };
    }
    AckStatus::Rejected {
        reason: error.to_string(),
    }
}

/// The `reject_reason` field that belongs beside `status` on an engine ack.
///
/// Derived from the status rather than passed alongside it, so the two cannot
/// disagree: only a terminal refusal carries a reason, because only a terminal
/// refusal asks the sender to compensate. A reason attached to a retryable
/// status reads as "give up" to any receiver that checks the field first.
pub(super) fn reject_reason_for(status: &AckStatus) -> Option<String> {
    match status {
        AckStatus::Rejected { reason } => Some(reason.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ErrorCode;

    #[test]
    fn a_retryable_refusal_becomes_a_gap_at_the_senders_own_seq() {
        // Nothing applied, so the sender resumes at the seq it just sent —
        // not one past it, which would skip the batch entirely.
        let error = crate::Error::DataPlane(ErrorCode::RetryableRefusal {
            reason: "shard is rebalancing".into(),
        });
        assert_eq!(
            ack_status_for_dispatch_error(&error, 9),
            AckStatus::Gap { expected: 9 }
        );
    }

    #[test]
    fn a_refusal_typed_at_the_control_plane_is_retryable_too() {
        // The same refusal reaches this code already typed on some paths and
        // wrapped in DataPlane on others; both must classify identically.
        let error = crate::Error::RetryableRefusal {
            reason: "shard is rebalancing".into(),
        };
        assert_eq!(
            ack_status_for_dispatch_error(&error, 3),
            AckStatus::Gap { expected: 3 }
        );
    }

    #[test]
    fn a_genuine_refusal_stays_terminal_and_carries_its_reason() {
        let error = crate::Error::DataPlane(ErrorCode::RejectedAuthz {
            resource: "RLS write policy on 'orders' rejected the row".into(),
        });
        match ack_status_for_dispatch_error(&error, 4) {
            AckStatus::Rejected { reason } => assert!(!reason.is_empty()),
            other => panic!("expected a terminal rejection, got {other:?}"),
        }
    }

    #[test]
    fn a_timeout_is_retryable_because_it_refused_nothing() {
        // The reachable case: a dispatch that timed out never judged the write.
        // Reporting it terminal makes the sender drop a batch on every blip.
        let error = crate::Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(1),
        };
        assert_eq!(
            ack_status_for_dispatch_error(&error, 6),
            AckStatus::Gap { expected: 6 }
        );
    }

    #[test]
    fn a_moved_leader_is_retryable() {
        let error = crate::Error::NotLeader {
            vshard_id: crate::types::VShardId::new(0),
            leader_node: 2,
            leader_addr: "10.0.0.2:9000".into(),
        };
        assert_eq!(
            ack_status_for_dispatch_error(&error, 2),
            AckStatus::Gap { expected: 2 }
        );
    }

    #[test]
    fn shed_load_is_retryable_not_a_refusal_of_the_write() {
        let error = crate::Error::Backpressure {
            engine: nodedb_mem::EngineId::Timeseries,
        };
        assert_eq!(
            ack_status_for_dispatch_error(&error, 4),
            AckStatus::Gap { expected: 4 }
        );
    }

    #[test]
    fn a_dispatch_refused_at_capacity_is_retryable_not_a_refusal_of_the_write() {
        let error = crate::Error::DispatchCapacity {
            scope: crate::DispatchCapacityScope::TenantInflight {
                tenant_id: crate::types::TenantId::new(1),
                inflight: 64,
                cap: 64,
            },
        };
        assert_eq!(
            ack_status_for_dispatch_error(&error, 4),
            AckStatus::Gap { expected: 4 }
        );
    }

    #[test]
    fn a_retryable_status_carries_no_reject_reason() {
        // A reason beside a retryable status reads as "give up" to a receiver
        // that checks the field before the status.
        assert_eq!(reject_reason_for(&AckStatus::Gap { expected: 2 }), None);
        assert_eq!(reject_reason_for(&AckStatus::Applied), None);
        assert_eq!(
            reject_reason_for(&AckStatus::Rejected {
                reason: "schema mismatch".into()
            }),
            Some("schema mismatch".to_string())
        );
    }

    #[test]
    fn an_unclassified_error_is_never_reported_as_applied() {
        // The failure mode this replaces: a dispatch error acked as `Applied`,
        // which retires a write that never landed.
        let error = crate::Error::Internal {
            detail: "bridge closed".into(),
        };
        assert_ne!(
            ack_status_for_dispatch_error(&error, 1),
            AckStatus::Applied,
            "a failed dispatch must never be reported as applied"
        );
    }
}
