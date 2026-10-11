// SPDX-License-Identifier: BUSL-1.1

//! A backup cut's barrier entry: the group's floor above the cut, the
//! restore point's place, and a database backup's capture.
//!
//! The apply notes each barrier in the node's cut registry in log order, at
//! the entry's start. A cut waiting on this node reads where the group's
//! barrier sits, and a leader's schedulers release the Calvin redo they held
//! for the cut.

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::backup::cut_order::OrderedCut;
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};

use super::context::{ApplyContext, FinishedApply};
use super::lane::QueuedEntry;
use super::proposal_gate::EntryOutcome;
use super::start::Prepared;

/// Where a cut barrier cuts its group.
#[derive(Clone, Copy)]
struct CutBarrierPoint {
    hlc: u64,
    restore_point: u64,
    log_term: u64,
}

/// Prepare the barrier of `cut` at `pos`, whose entry has term `log_term`.
pub(super) fn prepare_cut_barrier<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    log_term: u64,
    cut: OrderedCut,
) -> Prepared<'a> {
    ctx.state
        .calvin
        .cut_barriers
        .note_applied(&cut, pos.group_id, pos.log_index);
    let barrier = CutBarrierPoint {
        hlc: cut.hlc,
        restore_point: cut.restore_point,
        log_term,
    };
    match cut.capture {
        Some(request) => prepare_capture_barrier(ctx, pos, barrier, request),
        None => prepare_plain_barrier(ctx, pos, barrier),
    }
}

/// Note a cut barrier an installed snapshot covers. The entry is committed,
/// and the snapshot holds every entry before it, so a cut waiting here and a
/// leader's held redo read the barrier as applied.
pub(super) fn note_covered_barrier(state: &SharedState, group_id: u64, queued: &QueuedEntry) {
    if let Some(ReplicatedEntry {
        write:
            ReplicatedWrite::CutBarrier {
                hlc,
                restore_point,
                capture,
            },
        ..
    }) = &queued.decoded
    {
        let cut = OrderedCut {
            hlc: *hlc,
            restore_point: *restore_point,
            capture: capture.clone(),
        };
        state
            .calvin
            .cut_barriers
            .note_applied(&cut, group_id, queued.entry.index);
    }
}

/// Prepare a backup capture's cut barrier. The lane starts a barrier with
/// nothing else of its group in flight, and starts no later entry until it
/// finishes: the floor is durable, and the capture holds every entry at or
/// below it and none above.
fn prepare_capture_barrier<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    barrier: CutBarrierPoint,
    request: nodedb_physical::physical_plan::CutCaptureRequest,
) -> Prepared<'a> {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
        ..
    } = pos;
    let CutBarrierPoint {
        hlc,
        restore_point,
        log_term,
    } = barrier;
    Prepared::Exclusive(Box::pin(async move {
        persist_floor_durably(ctx, group_id, log_index, hlc).await;
        record_restore_point(ctx, group_id, restore_point, hlc, log_index, log_term);
        crate::control::backup::cut_capture::apply::capture_at_barrier(
            ctx.state, group_id, &request,
        )
        .await;
        finish_barrier(ctx, group_id, log_index, applied_key)
    }))
}

/// Prepare a cut barrier with no capture. A failed floor write holds the
/// group until the floor is durable.
fn prepare_plain_barrier<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    barrier: CutBarrierPoint,
) -> Prepared<'a> {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
        ..
    } = pos;
    let CutBarrierPoint {
        hlc,
        restore_point,
        log_term,
    } = barrier;
    let persisted =
        crate::control::pitr::restore_point::persist_cut_floor(ctx.state, group_id, log_index, hlc);
    if persisted.is_ok() {
        record_restore_point(ctx, group_id, restore_point, hlc, log_index, log_term);
        return Prepared::Barrier;
    }
    Prepared::Exclusive(Box::pin(async move {
        persist_floor_durably(ctx, group_id, log_index, hlc).await;
        record_restore_point(ctx, group_id, restore_point, hlc, log_index, log_term);
        finish_barrier(ctx, group_id, log_index, applied_key)
    }))
}

/// Record the group's place at the cluster restore point `restore_point` a
/// cut barrier takes. `0` takes none.
fn record_restore_point(
    ctx: ApplyContext<'_>,
    group_id: u64,
    restore_point: u64,
    hlc: u64,
    log_index: u64,
    log_term: u64,
) {
    if restore_point == 0 {
        return;
    }
    crate::control::pitr::restore_point::record_group_point(
        ctx.state,
        nodedb_wal::record::RestorePointPayload {
            id: restore_point,
            hlc,
            group_id,
            applied_index: log_index,
            term: log_term,
            next_epoch: 0,
            epoch_system_ms: 0,
            vshards: Vec::new(),
        },
    );
}

/// Persist the floor of the barrier at `log_index`, retrying until it holds.
/// A floor write that keeps failing wedges the node until it succeeds.
async fn persist_floor_durably(ctx: ApplyContext<'_>, group_id: u64, log_index: u64, hlc: u64) {
    crate::control::pitr::restore_point::persist_until_durable(
        &ctx.state.metadata_apply_wedge,
        group_id,
        log_index,
        || {
            crate::control::pitr::restore_point::persist_cut_floor(
                ctx.state, group_id, log_index, hlc,
            )
        },
    )
    .await;
}

/// Resolve a barrier's waiter once every step of the barrier is durable.
fn finish_barrier(
    ctx: ApplyContext<'_>,
    group_id: u64,
    log_index: u64,
    applied_key: u64,
) -> FinishedApply {
    ctx.tracker.complete(
        group_id,
        log_index,
        applied_key,
        Ok(crate::control::distributed_applier::AppliedWrite::unversioned(Vec::new())),
    );
    FinishedApply {
        group_id,
        log_index,
        outcome: EntryOutcome::Skipped,
    }
}
