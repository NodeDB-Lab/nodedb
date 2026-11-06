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
    if cut.mirror.is_some() {
        return Prepared::Concluded(conclude_mirror_barrier(ctx, pos));
    }
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

/// Conclude a mirror cut's barrier. A mirror reads its place in the log, and no
/// restore names it, so it persists no floor and its proposer waits for nothing
/// behind it.
///
/// It concludes durable: it holds no state a restart needs, so the group's
/// durable floor can cross it. A neutral barrier leaves the floor at the group's
/// last write. An idle group then runs no compaction, retains one barrier per
/// cut, and never releases what a mirror's ack freed.
fn conclude_mirror_barrier(ctx: ApplyContext<'_>, pos: AppliedPosition) -> EntryOutcome {
    ctx.tracker.complete(
        pos.group_id,
        pos.log_index,
        pos.applied_key,
        Ok(crate::control::distributed_applier::AppliedWrite::new(
            Vec::new(),
        )),
    );
    EntryOutcome::Applied {
        durable: true,
        result: None,
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
                mirror,
            },
        ..
    }) = &queued.decoded
    {
        let cut = OrderedCut {
            hlc: *hlc,
            restore_point: *restore_point,
            capture: capture.clone(),
            mirror: mirror.as_ref().map(|barrier| barrier.mark),
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
        Ok(crate::control::distributed_applier::AppliedWrite::new(
            Vec::new(),
        )),
    );
    FinishedApply {
        group_id,
        log_index,
        outcome: EntryOutcome::Skipped,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use nodedb_raft::message::LogEntry;
    use nodedb_types::MirrorCutMark;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::distributed_applier::applier::ApplyBatch;
    use crate::control::distributed_applier::proposal_ledger::{
        PROPOSAL_LEDGER_CAPACITY, ProposalLedger,
    };
    use crate::control::distributed_applier::propose_tracker::ProposeTracker;
    use crate::control::wal_replication::types::MirrorBarrier;
    use crate::control::wal_replication::{RaftAppliedIndexSink, RaftCompactor};
    use crate::wal::WalManager;

    use super::super::pipeline::Pipeline;

    const GROUP: u64 = 4;

    /// With no write in the group, a mirror cut's barrier alone moves the
    /// durable floor and fires the compaction trigger. A cut per second would
    /// otherwise pile up behind a floor that never moves.
    #[tokio::test]
    async fn a_mirror_barrier_moves_the_durable_floor_and_runs_compaction() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");

        let saved = Arc::new(Mutex::new(Vec::new()));
        let saved_by_sink = Arc::clone(&saved);
        let sink: Arc<RaftAppliedIndexSink> = Arc::new(move |group, index| {
            saved_by_sink.lock().expect("saved").push((group, index));
            Ok(())
        });
        assert!(state.raft_applied_index_sink.set(sink).is_ok());
        let compacted = Arc::new(Mutex::new(Vec::new()));
        let compacted_by_compactor = Arc::clone(&compacted);
        let compactor: Arc<RaftCompactor> = Arc::new(move |group, index| {
            compacted_by_compactor
                .lock()
                .expect("compacted")
                .push((group, index));
            Ok(false)
        });
        assert!(state.raft_compactor.set(compactor).is_ok());

        let barrier = ReplicatedEntry::new(
            0,
            0,
            2,
            ReplicatedWrite::CutBarrier {
                hlc: 5,
                restore_point: 0,
                capture: None,
                mirror: Some(MirrorBarrier {
                    mark: MirrorCutMark {
                        seq: 7,
                        cap_ordinal: 6,
                        metadata_index: 0,
                    },
                    calvin_floors: Vec::new(),
                }),
            },
        );
        let tracker = Arc::new(ProposeTracker::new());
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };
        let mut pipeline = Pipeline::new(ctx, ProposalLedger::new(PROPOSAL_LEDGER_CAPACITY));
        pipeline.accept(ApplyBatch {
            group_id: GROUP,
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                data: barrier.encode().expect("encode"),
            }],
        });
        pipeline.pump();
        pipeline.settle();

        assert_eq!(*saved.lock().expect("saved"), vec![(GROUP, 1)]);
        assert_eq!(*compacted.lock().expect("compacted"), vec![(GROUP, 1)]);
    }
}
