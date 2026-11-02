// SPDX-License-Identifier: Apache-2.0

//! `SyncAckResult` — the bridge response payload for an idempotent ingest.
//!
//! The Data-Plane handler serializes this into `Response.payload` after
//! running the idempotency check; the Control-Plane handler decodes it to
//! build the per-engine wire ack (e.g. `FtsIndexAckMsg`).

use serde::{Deserialize, Serialize};

use crate::sync::violation::ViolationType;
use crate::sync::wire::ack_status::AckStatus;

/// What the Data Plane decided about one sync frame.
///
/// The two dispositions demand opposite sender behaviour, so they are variants
/// of one enum rather than two independent fields:
///
/// * [`Self::Ack`] — the frame reached a client-visible status. Every variant
///   of [`AckStatus`] is non-terminal or successful; in particular
///   [`AckStatus::Gap`] is a **retryable refusal**, meaning nothing applied and
///   the identical frame at the same sequence is expected to succeed once a
///   transient precondition resolves. The high-water-mark is deliberately held
///   so that re-push is admitted rather than deduplicated to `Duplicate`.
/// * [`Self::Rejected`] — the frame will never apply. The sender must
///   compensate rather than retry, and the high-water-mark advances so a dead
///   frame cannot wedge the stream.
///
/// Carrying a status *and* an independent rejection reason is what let a
/// retryable refusal be read as a permanent one: a producer with a reason to
/// report had to smuggle it through the terminal channel, and a consumer that
/// checked the terminal channel first never saw the retryable status behind it.
/// One field makes that state unrepresentable.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum SyncOutcome {
    /// The frame reached a client-visible ack status; nothing is terminal.
    Ack(AckStatus),
    /// The frame will never apply: the deterministic reason the sender must
    /// compensate against. Only the deterministic [`ViolationType`] is carried
    /// — never any node-local DLQ id or timestamp.
    Rejected(ViolationType),
}

impl Default for SyncOutcome {
    fn default() -> Self {
        Self::Ack(AckStatus::default())
    }
}

/// Outcome of one idempotent ingest operation returned from the Data Plane.
///
/// Serialized via zerompk into `Response.payload`; the Control Plane decodes
/// this to populate the engine-specific wire ack message.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct SyncAckResult {
    /// What the Data Plane decided about this frame. A retryable refusal is
    /// reported here as [`SyncOutcome::Ack`] carrying [`AckStatus::Gap`],
    /// never as [`SyncOutcome::Rejected`].
    pub outcome: SyncOutcome,
    /// Highest sequence number from this producer that has been durably applied
    /// on this stream, after processing the current message.
    pub applied_seq: u64,
}

impl SyncAckResult {
    /// A clean, non-terminal outcome at `applied_seq`.
    pub fn acked(status: AckStatus, applied_seq: u64) -> Self {
        Self {
            outcome: SyncOutcome::Ack(status),
            applied_seq,
        }
    }

    /// A terminal refusal: the frame will never apply.
    pub fn rejected(violation: ViolationType, applied_seq: u64) -> Self {
        Self {
            outcome: SyncOutcome::Rejected(violation),
            applied_seq,
        }
    }

    /// The client-visible status, or `None` when the outcome is terminal.
    ///
    /// Callers that build an ack frame must handle the `None` case by emitting
    /// a rejection instead — there is no defensible status to report for a
    /// frame that will never apply.
    pub fn status(&self) -> Option<AckStatus> {
        match &self.outcome {
            SyncOutcome::Ack(status) => Some(status.clone()),
            SyncOutcome::Rejected(_) => None,
        }
    }

    /// The terminal refusal reason, or `None` when the frame was not refused
    /// terminally. A retryable refusal never answers `Some` here.
    pub fn terminal_violation(&self) -> Option<&ViolationType> {
        match &self.outcome {
            SyncOutcome::Rejected(violation) => Some(violation),
            SyncOutcome::Ack(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_terminal_rejection() {
        let ack = SyncAckResult::rejected(
            ViolationType::UniqueViolation {
                field: "email".into(),
                value: "x@y.com".into(),
            },
            7,
        );
        let bytes = zerompk::to_msgpack_vec(&ack).unwrap();
        let decoded: SyncAckResult = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded, ack);
    }

    #[test]
    fn round_trips_a_clean_apply() {
        let ack = SyncAckResult::acked(AckStatus::Applied, 3);
        let bytes = zerompk::to_msgpack_vec(&ack).unwrap();
        let decoded: SyncAckResult = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded, ack);
        assert_eq!(decoded.terminal_violation(), None);
    }

    #[test]
    fn round_trips_a_retryable_gap() {
        let ack = SyncAckResult::acked(AckStatus::Gap { expected: 5 }, 4);
        let bytes = zerompk::to_msgpack_vec(&ack).unwrap();
        let decoded: SyncAckResult = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded, ack);
    }

    #[test]
    fn a_retryable_gap_is_never_a_terminal_violation() {
        // The bug this type shape exists to prevent: a retryable refusal that
        // also carried a violation was read as a permanent rejection, so the
        // edge abandoned a write the server was still holding the stream open
        // for. A `Gap` outcome cannot carry one.
        let gap = SyncAckResult::acked(AckStatus::Gap { expected: 1 }, 0);
        assert_eq!(gap.terminal_violation(), None);
        assert_eq!(gap.status(), Some(AckStatus::Gap { expected: 1 }));
    }

    #[test]
    fn a_terminal_rejection_offers_no_ack_status() {
        let rejected = SyncAckResult::rejected(ViolationType::PermissionDenied, 9);
        assert_eq!(rejected.status(), None);
        assert_eq!(
            rejected.terminal_violation(),
            Some(&ViolationType::PermissionDenied)
        );
    }
}
