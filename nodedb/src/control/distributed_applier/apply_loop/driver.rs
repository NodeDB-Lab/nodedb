// SPDX-License-Identifier: BUSL-1.1

//! Loop driver: takes batches off the apply channel into the pipeline, and
//! collects the enqueues and applies that finish, until the channel closes
//! and every started entry concluded.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

use crate::control::distributed_applier::applier::ApplyBatch;
use crate::control::distributed_applier::proposal_ledger::{
    PROPOSAL_LEDGER_CAPACITY, ProposalLedger,
};
use crate::control::distributed_applier::propose_tracker::ProposeTracker;
use crate::control::state::SharedState;

use super::context::ApplyContext;
use super::pipeline::Pipeline;

/// Run the background loop that applies committed Raft entries to the local Data Plane.
///
/// This task reads from the apply channel, deserializes each entry, dispatches
/// the write to the Data Plane via SPSC, and notifies proposers.
pub async fn run_apply_loop(
    mut apply_rx: mpsc::Receiver<ApplyBatch>,
    state: Arc<SharedState>,
    tracker: Arc<ProposeTracker>,
) {
    let Some(ledger) = recover_ledger(&state.startup, &state.wal).await else {
        return;
    };
    // Change events of this node's writes take Raft log positions from here on.
    state.cdc_router.positions().mark_replicated();

    let ctx = ApplyContext {
        state: &state,
        tracker: &tracker,
    };
    let mut pipeline = Pipeline::new(ctx, ledger);
    drive(&mut pipeline, &mut apply_rx, &state).await;
}

/// The proposals this node already applied, recovered from its WAL before
/// any entry is delivered: every record an entry's apply appended carries
/// the entry's idempotency key in its header. Boot cancels each committed
/// record its replay refused before the data plane replay phase ends, so
/// the WAL is read after that phase: a cancelled record never counts as
/// applied. `None` refuses to apply anything, and the loop stops.
async fn recover_ledger(
    startup: &crate::control::startup::StartupGate,
    wal: &crate::wal::WalManager,
) -> Option<ProposalLedger> {
    if let Err(error) = startup
        .await_phase(crate::control::startup::StartupPhase::DataPlaneReplay)
        .await
    {
        tracing::error!(
            %error,
            "data-group apply loop: boot stopped before the data plane replay settled its \
             records; refusing to apply committed entries"
        );
        return None;
    }
    match wal.replay() {
        Ok(records) => Some(ProposalLedger::from_records(
            &records,
            PROPOSAL_LEDGER_CAPACITY,
        )),
        Err(error) => {
            // Without the keys an entry re-delivered above the durable floor,
            // or a second committed copy of a proposal, applies a second
            // time. Refuse to apply anything rather than risk it: the loop
            // stops, and every propose waiter surfaces the stall.
            tracing::error!(
                %error,
                "data-group apply loop cannot read its WAL to recover applied proposals; \
                 refusing to apply committed entries"
            );
            None
        }
    }
}

/// Feed `pipeline` from `apply_rx` and its wakes until the channel closes and
/// every started entry concluded.
pub(super) async fn drive(
    pipeline: &mut Pipeline<'_>,
    apply_rx: &mut mpsc::Receiver<ApplyBatch>,
    state: &SharedState,
) {
    let mut install_released = state.raft_apply_gates.get().map(|g| g.subscribe_released());
    let mut mirror_moved = state.mirror.cuts.subscribe();
    // A held group whose snapshot debt did not persist gets no batches. The
    // loop wakes on each Raft tick to write the debt again. A mirror entry
    // behind its metadata floor wakes the same way: no cut watch moves for it.
    let mut debt_retry = tokio::time::interval(Duration::from_millis(
        state.tuning.cluster_transport.raft_tick_interval_ms,
    ));
    debt_retry.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut accepting = true;
    loop {
        if !accepting && !pipeline.has_running() {
            // The channel closed and every started entry concluded. An entry
            // still queued waits on a snapshot install that shutdown ends.
            return;
        }
        let running = pipeline.has_running();
        let blocked = pipeline.awaits_install();
        // A change journal write that failed retries with the next pass:
        // the timer runs one while no batch arrives.
        let retry_debt = pipeline.retries_debt() || state.change_stream.journal_owed();
        let awaits_mirror = pipeline.awaits_mirror();
        tokio::select! {
            biased;
            Some(event) = pipeline.next_event(), if running => {
                pipeline.handle(event);
            }
            () = wait_install_released(&mut install_released), if blocked => {}
            // The sender lives as long as `state`, so the wait never ends early.
            _ = mirror_moved.changed(), if awaits_mirror => {}
            _ = debt_retry.tick(), if retry_debt || awaits_mirror => {}
            batch = apply_rx.recv(), if accepting => match batch {
                Some(batch) => {
                    pipeline.accept(batch);
                    // Take every batch already queued, so one pass starts
                    // them all and one floor save covers them.
                    while let Ok(batch) = apply_rx.try_recv() {
                        pipeline.accept(batch);
                    }
                }
                None => accepting = false,
            },
        }
        pipeline.pump();
        pipeline.settle().await;
    }
}

/// Resolve once a snapshot install releases a group's apply gate. Never
/// resolves before `start_raft` installs the gates, when no install runs.
async fn wait_install_released(released: &mut Option<watch::Receiver<u64>>) {
    match released {
        Some(rx) => {
            if rx.changed().await.is_err() {
                // The gates are gone with the Raft loop: no install follows.
                std::future::pending::<()>().await;
            }
        }
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod ledger_tests {
    use super::*;
    use crate::control::startup::{StartupPhase, StartupSequencer};
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use crate::wal::WalManager;
    use crate::wal::manager::NO_APPLY_KEY;

    /// The ledger is read only once the data plane replay phase is reached.
    /// A record boot cancels before then never counts as applied, so its
    /// entry applies again when it is delivered.
    #[tokio::test]
    async fn the_ledger_waits_for_the_boot_cancels() {
        const KEY: u64 = 0xCA9C;
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal"));
        let (t, v, d) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        let lsn = wal
            .appender(KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(t, v, d, b"row")
            .expect("append");
        wal.sync().expect("sync");

        let (sequencer, startup) = StartupSequencer::new();
        let replay = sequencer.register_gate(StartupPhase::DataPlaneReplay, "data-plane-replay");
        let _hold = sequencer.register_gate(StartupPhase::RaftMetadataReplay, "hold");
        let reader = {
            let (startup, wal) = (Arc::clone(&startup), Arc::clone(&wal));
            tokio::spawn(async move { recover_ledger(&startup, &wal).await })
        };
        tokio::task::yield_now().await;
        assert!(
            !reader.is_finished(),
            "the ledger waits for the replay phase"
        );

        // Boot cancels the record its replay refused, with no proposal key,
        // then ends the replay phase.
        wal.appender(NO_APPLY_KEY)
            .append_write_aborted(t, v, d, Lsn::new(lsn.as_u64()))
            .expect("cancel");
        wal.sync().expect("sync");
        replay.fire();

        let ledger = reader.await.expect("reader").expect("ledger");
        assert!(
            ledger.prior(KEY).is_none(),
            "a cancelled record is not applied"
        );
    }
}
