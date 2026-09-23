// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for Data-Plane responses that never reached their caller.

use std::sync::atomic::{AtomicU64, Ordering};

use faultbox::{Capture, EventKind, error_chain_of};

use crate::diag::context;

/// Count of finished Data-Plane writes whose response the bounded response
/// ring refused, leaving the caller with nothing but a deadline. Makes the
/// failure visible to the metrics exporter even with no recorder configured.
static DATA_PLANE_RESPONSES_LOST: AtomicU64 = AtomicU64::new(0);

/// Read the count of Data-Plane responses lost to a full response ring.
/// Exposed for the metrics exporter and tests.
pub fn data_plane_responses_lost() -> u64 {
    DATA_PLANE_RESPONSES_LOST.load(Ordering::Relaxed)
}

/// Report a completed Data-Plane write whose response the bounded response
/// ring refused, so the caller can only learn a deadline. Called from the
/// response-push helper every core-loop completion path funnels through.
pub fn data_plane_response_lost(core_id: usize, write: context::LostResponseWrite) {
    DATA_PLANE_RESPONSES_LOST.fetch_add(1, Ordering::Relaxed);
    let ctx = context::DataPlaneResponseLost { core_id, write };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "Data-Plane response dropped: the caller can never learn this write's outcome",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a Calvin cross-shard transaction whose completion wait timed out.
/// Called from the completion-timeout arm of
/// `submit_and_await_calvin_with_timeout`; the sibling "channel closed" arm
/// is a different root cause and is not reported here.
pub fn calvin_completion_timeout(
    err: &crate::Error,
    epoch: u64,
    position: u32,
    participants: usize,
    timeout_secs: u64,
) {
    let ctx = context::CalvinCompletionTimeout {
        epoch,
        position,
        participants,
        timeout_secs,
    };
    let _ = Capture::new(
        EventKind::Error,
        "Calvin transaction completion wait timed out",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a Calvin scheduler that held a sequenced txn unapplied and halted
/// its vShard. Called once per scheduler, from its halt latch on the first
/// cause.
pub fn calvin_apply_halted(
    vshard_id: u32,
    epoch: u64,
    position: u32,
    reason: &str,
    step: &str,
    error: &str,
) {
    let ctx = context::CalvinApplyHalted {
        vshard_id,
        epoch,
        position,
        reason,
        step,
        error,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "Calvin scheduler halted: a sequenced txn could not be applied on this replica",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
