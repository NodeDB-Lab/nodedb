// SPDX-License-Identifier: BUSL-1.1

//! Settle every group's concluded entries and save the floors that moved.

use std::collections::HashMap;
use std::sync::Arc;

use crate::control::change_stream::stream::Settled;
use crate::control::cluster::calvin::scheduler::barrier_store;
use crate::control::server::dispatch_utils::publish_settled_changes;
use crate::control::state::SharedState;

use super::bookkeeping::record_durable_apply;
use super::context::ApplyContext;
use super::lane::Lane;

/// Settle every group's concluded entries in log order, release their
/// window, and save each durable floor that moved.
///
/// The tenant write marks of the settled entries are persisted before any
/// floor that covers them. An entry above the saved floor is delivered
/// again after a restart and records its mark again, so every committed
/// write keeps a durable mark.
///
/// The dependent-read barrier rows saved before the pass persist before any
/// floor too: a floor covers a barrier entry only once its row is durable.
///
/// An applied entry above the saved floor holds its records in the WAL. A
/// restart delivers it again and stages its change events from them. The
/// floor that covers it journals its events first, then releases the hold.
///
/// A group whose change journal writes keep failing settles no more entries
/// until a write succeeds (see `change_stream::stream::journal_debt`). Its
/// floor save still retries every pass.
///
/// Every durable write of the pass runs on the blocking pool, as does a
/// settle that reads spilled pages back from the change journal.
pub(super) async fn settle_lanes(ctx: ApplyContext<'_>, lanes: &mut HashMap<u64, Lane>) {
    let state = ctx.state;
    let tracker = ctx.tracker;
    let wedge = &state.metadata_apply_wedge;
    state.change_stream.clear_journal_halts(wedge);
    let mut floors: Vec<(u64, u64)> = Vec::new();
    for (group_id, lane) in lanes.iter_mut() {
        if !state.change_stream.journal_halts(*group_id, wedge) {
            let first = lane.front_index();
            let settled = lane.settle(tracker, &state.tenant_marks);
            if settled > 0 {
                tracker.window().release(*group_id, settled);
                // The entries' change events publish in log order, at their
                // log positions, on every replica.
                if let (Some(first), Some(last)) = (first, lane.last_settled()) {
                    let published = publish_settled_changes(state, *group_id, first, last).await;
                    pause_before_floor_save(state, &published);
                }
            }
        }
        // One save per pass that moved the floor, never one per entry: each
        // save is an fsync, and the pass coalesces every apply that finished
        // before it.
        if let Some(floor) = lane.floor_to_save() {
            floors.push((*group_id, floor));
        }
    }
    if floors.is_empty() {
        return;
    }
    let shared = Arc::clone(state);
    let saved = match tokio::task::spawn_blocking(move || save_floors(&shared, floors)).await {
        Ok(saved) => saved,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // Cancelled at runtime shutdown: no floor moved, and a restart
        // delivers every entry above the saved floors again.
        Err(error) => {
            tracing::error!(%error, "apply loop: the floor save did not finish");
            return;
        }
    };
    for group_id in saved {
        if let Some(lane) = lanes.get_mut(&group_id) {
            lane.take_floor_to_save();
        }
    }
}

/// Save each `(group, floor)` of `floors`, on the blocking pool. Returns
/// the groups whose floor the pass took: the change journal holds every
/// event their floor covers. A floor not taken waits for the next pass.
fn save_floors(state: &Arc<SharedState>, floors: Vec<(u64, u64)>) -> Vec<u64> {
    if !persist_barrier_rows(state) {
        // Every floor waits: one may cover a barrier entry whose row a
        // crash loses. The next pass persists again.
        return Vec::new();
    }
    if let Err(error) = state.tenant_marks.persist(state.credentials.catalog()) {
        // The floors stay where they are, so every entry above them keeps
        // its place in the log. The next pass persists and saves again.
        tracing::error!(
            %error,
            "apply loop: tenant write marks did not persist; the applied floors wait"
        );
        return Vec::new();
    }
    let mut taken = Vec::with_capacity(floors.len());
    for (group_id, floor) in floors {
        // Every entry the floor covers keeps its changes on disk first. A
        // journal that did not take them keeps the floor where it is, with
        // every entry above it held in the WAL: the next pass journals and
        // saves again.
        if let Err(error) = state.change_stream.journal_group(group_id, floor) {
            tracing::error!(
                group_id,
                %error,
                "apply loop: the change journal did not take the events the floor covers; the \
                 applied floor waits"
            );
            crate::diag::change_journal_failed(group_id, "floor batch write", &error);
            continue;
        }
        taken.push(group_id);
        // The journal holds the events of every entry the floor covers,
        // and a restart delivers none of them again: their records leave
        // the WAL's hold. A floor that did not save keeps them held, so
        // the restart that delivers them again finds their records.
        if record_durable_apply(state, group_id, floor) {
            state
                .change_stream
                .unjournaled()
                .release_through(group_id, floor);
        }
    }
    taken
}

/// A crash test's stop: the pass published new events of a collection, and
/// no floor covers their entries yet. The point is
/// `apply_loop::before_floor_save::<collection>`. It is evaluated only once
/// boot's data-group replay finished, and only for events the ring did not
/// hold: an entry boot delivers again, or one that publishes nothing, never
/// reaches it.
fn pause_before_floor_save(state: &SharedState, published: &Settled) {
    #[cfg(feature = "failpoints")]
    {
        if !state
            .data_groups_replayed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        for collection in &published.collections {
            crate::fail_point!(
                crate::fail_point::FailScope::Node(state.node_id),
                &format!("apply_loop::before_floor_save::{collection}")
            );
        }
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = (state, published);
}

/// Persist the barrier rows saved since the last persist. Returns whether
/// they are durable.
fn persist_barrier_rows(state: &SharedState) -> bool {
    match barrier_store::persist_pending(state.credentials.catalog()) {
        Ok(()) => true,
        Err(error) => {
            tracing::error!(
                %error,
                "apply loop: the dependent-read barrier rows did not persist; every applied \
                 floor waits"
            );
            crate::diag::calvin_barrier_log_store_failed(None, None, "persist", &error);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use nodedb_cluster::GroupAppliedWatchers;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::cluster::calvin::BarrierEvent;
    use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
    use crate::control::distributed_applier::propose_tracker::ProposeTracker;
    use crate::wal::WalManager;

    use super::super::lane::{Slot, SlotState};
    use super::super::proposal_gate::PrefixStep;

    const GROUP: u64 = 4;

    /// A concluded barrier entry at `log_index` whose row holds its event.
    fn concluded(log_index: u64) -> Slot {
        Slot {
            log_index,
            proposal_key: 0,
            collection: None,
            write_mark: None,
            user_write: false,
            state: SlotState::Concluded(PrefixStep::Record(true)),
        }
    }

    /// A barrier row persist fails: the pass saves no floor, so the floor
    /// never passes the barrier entry. The next pass persists and saves it.
    #[tokio::test]
    async fn the_floor_waits_for_the_barrier_rows_to_persist() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let saved: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink_saved = Arc::clone(&saved);
        let sink: Arc<crate::control::wal_replication::RaftAppliedIndexSink> =
            Arc::new(move |group_id, applied_index| {
                sink_saved
                    .lock()
                    .expect("saved floors")
                    .push((group_id, applied_index));
                Ok::<(), crate::Error>(())
            });
        assert!(state.raft_applied_index_sink.set(sink).is_ok());
        let watchers = Arc::new(GroupAppliedWatchers::new());
        let tracker = Arc::new(ProposeTracker::new().with_group_watchers(watchers));
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };

        let catalog = state.credentials.catalog();
        barrier_store::save_event(catalog, 7, TxnId::new(3, 0), &BarrierEvent::Timeout)
            .expect("save the row");
        let mut lane = Lane::new(GROUP);
        lane.push(concluded(5));
        let mut lanes = HashMap::from([(GROUP, lane)]);

        catalog.fail_next_calvin_barrier_persist_for_test();
        settle_lanes(ctx, &mut lanes).await;
        assert!(
            saved.lock().expect("saved floors").is_empty(),
            "no floor passes a row that is not durable"
        );
        assert!(lanes[&GROUP].floor_to_save().is_some());

        settle_lanes(ctx, &mut lanes).await;
        assert_eq!(*saved.lock().expect("saved floors"), vec![(GROUP, 5)]);
        assert!(lanes[&GROUP].floor_to_save().is_none());
    }
}
