// SPDX-License-Identifier: BUSL-1.1

//! The recording implementation of the Calvin sequencer's report sites.
//!
//! Both functions are called from paths that already log and count the drop
//! today; this only adds a structured report alongside that, at the same
//! site, without changing what either caller returns or does next. None of
//! them can fail: `Capture::emit` returns `None` when the host never
//! initialized the recorder and is documented never to panic, so the result
//! is deliberately discarded — a failure to record must never be worse than
//! the failure being recorded.

use faultbox::{Capture, EventKind};

use super::context::{self, DroppedTxn};

/// Report a break in the sequencer's epoch sequence.
///
/// Called from the one site that detects this: `apply`'s epoch-ordering check.
/// `direction` is the classification that site made (`"ahead"` — entries
/// missing on this replica; `"behind"` — an already-consumed epoch proposed
/// again), which decides both the report's wording and its grouping. There is
/// no live error object at this point — the break is a state-machine invariant
/// check, not a propagated failure — so no `error_chain` is attached.
pub fn sequencer_epoch_gap(
    epoch_expected: u64,
    epoch_found: u64,
    direction: &'static str,
    txns_in_batch: usize,
    raft_index: u64,
) {
    // Absolute distance: `found` is below `expected` in the "behind" case, and
    // a saturating subtraction one way round would report every regression as
    // a zero-width break.
    let gap = epoch_found.abs_diff(epoch_expected);
    let ctx = context::SequencerEpochGap {
        epoch_expected,
        epoch_found,
        gap,
        direction,
        txns_in_batch,
        raft_index,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "Calvin sequencer: epoch sequence break detected",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report the transactions dropped from a single `apply` call's fan-out loop
/// because a destination vshard channel was full or closed.
///
/// Called once after the fan-out loop completes, not per dropped
/// transaction — per-txn emission would report-storm under sustained
/// backpressure, since the loop can drop many positions in one call. Only
/// called when at least one transaction was dropped.
pub fn sequencer_backpressure_drop(epoch: u64, dropped_count: u64, drops: &[(u32, &'static str)]) {
    let drops: Vec<DroppedTxn> = drops
        .iter()
        .map(|&(vshard, cause)| DroppedTxn { vshard, cause })
        .collect();
    let ctx = context::SequencerBackpressureDrop {
        epoch,
        dropped_count,
        drops: &drops,
    };
    let _ = Capture::new(
        EventKind::Error,
        "Calvin sequencer: transactions dropped from epoch fan-out under backpressure",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report an epoch batch skipped because one of its transactions has an
/// underivable participant set.
///
/// Called from the one site that detects it: `apply`'s `restore_derived`
/// pass over a decoded epoch batch.
pub fn sequencer_participants_underivable(epoch: u64, raft_index: u64, detail: &str) {
    let ctx = context::SequencerParticipantsUnderivable {
        epoch,
        raft_index,
        detail,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "Calvin sequencer: epoch batch carries a transaction with underivable participants",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a Raft group's disk write that failed with an I/O error and is
/// retried. Called from the writer's retry loop on each failed attempt. The
/// grouping key holds no attempt, so the loop files one growing report.
pub fn raft_disk_write_retried(group_id: u64, attempt: u32, ops: &[&'static str], error: &str) {
    let ctx = context::RaftDiskWriteFailure {
        cause: "io_error_retried",
        group_id,
        attempt,
        ops,
        error,
    };
    let _ = Capture::new(
        EventKind::Error,
        "Raft group disk: a write failed with an I/O error and is retried",
    )
    .domain(&ctx)
    .emit();
}

/// Report a Raft group's disk that stopped on a write error no retry clears.
/// Called from the writer's retry loop, the one site that classifies it.
pub fn raft_disk_fail_stopped(group_id: u64, ops: &[&'static str], error: &str) {
    let ctx = context::RaftDiskWriteFailure {
        cause: "fail_stopped",
        group_id,
        attempt: 1,
        ops,
        error,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "Raft group disk: a write failed with an error no retry clears; the replica stopped",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a Raft group's disk write abandoned because the disk closed while
/// the write failed. Called from the writer's retry loop.
pub fn raft_disk_write_abandoned(group_id: u64, ops: &[&'static str], error: &str) {
    let ctx = context::RaftDiskWriteFailure {
        cause: "abandoned_on_close",
        group_id,
        attempt: 1,
        ops,
        error,
    };
    let _ = Capture::new(
        EventKind::Error,
        "Raft group disk: a write failed while the disk closed; it was abandoned",
    )
    .domain(&ctx)
    .emit();
}

/// Report a Calvin verdict that differs from the verdict stored for the same
/// transaction. Called from `note_verdict`, which keeps the stored one.
pub fn calvin_verdict_conflict(
    epoch: u64,
    position: u32,
    difference: &'static str,
    kept: &str,
    refused: &str,
) {
    let ctx = context::CalvinVerdictConflict {
        epoch,
        position,
        difference,
        kept,
        refused,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "Calvin completion: a verdict differs from the stored verdict of its transaction",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
