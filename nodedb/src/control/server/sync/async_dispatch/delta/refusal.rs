// SPDX-License-Identifier: BUSL-1.1

//! The frames a delta gets when it is refused before its apply.

use super::super::super::wire::{
    CompensationHint, DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType,
};
use super::apply::DeltaDispatchOutcome;

/// Refuse retryably: nothing was applied and the identical delta at the same
/// sequence must be re-pushed once the binding can be established.
pub(super) fn retryable_binding_refusal(delta_msg: &DeltaPushMsg) -> Option<SyncFrame> {
    use nodedb_types::sync::wire::AckStatus;

    use super::super::super::wire::DeltaAckMsg;

    let ack = DeltaAckMsg {
        mutation_id: delta_msg.mutation_id,
        lsn: 0,
        clock_skew_warning_ms: None,
        applied_seq: delta_msg.seq.saturating_sub(1),
        status: AckStatus::Gap {
            expected: delta_msg.seq,
        },
    };
    SyncFrame::try_encode(SyncMessageType::DeltaAck, &ack)
}

/// A refusal decided in the Control Plane before the apply was ever attempted.
///
/// Every one of these is permanent for the frame as sent — a missing
/// collection, a revoked grant, an invalid signature, an exhausted quota —
/// so the sender must compensate rather than re-push identical bytes.
pub(super) fn terminal_reject(
    delta_msg: &DeltaPushMsg,
    reason: impl Into<String>,
    compensation: CompensationHint,
) -> DeltaDispatchOutcome {
    let reject = DeltaRejectMsg {
        mutation_id: delta_msg.mutation_id,
        reason: reason.into(),
        compensation: Some(compensation),
    };
    DeltaDispatchOutcome::refused(SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject))
}
