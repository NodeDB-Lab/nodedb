// SPDX-License-Identifier: BUSL-1.1

//! Settle every group's concluded entries and save the floors that moved.

use std::collections::HashMap;

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
pub(super) fn settle_lanes(ctx: ApplyContext<'_>, lanes: &mut HashMap<u64, Lane>) {
    let state = ctx.state;
    let tracker = ctx.tracker;
    // Persisted once per pass, at the first floor that moved.
    let mut barrier_rows_durable: Option<bool> = None;
    for (group_id, lane) in lanes {
        let first = lane.front_index();
        let settled = lane.settle(tracker, &state.tenant_marks);
        if settled > 0 {
            tracker.window().release(*group_id, settled);
            // The entries' change events publish in log order, at their
            // log positions, on every replica.
            if let (Some(first), Some(last)) = (first, lane.last_settled()) {
                publish_settled_changes(state, *group_id, first, last);
            }
        }
        if !lane.floor_pending() {
            continue;
        }
        if !*barrier_rows_durable.get_or_insert_with(|| persist_barrier_rows(state)) {
            // Every floor waits: one may cover a barrier entry whose row a
            // crash loses. The next pass persists again.
            continue;
        }
        // One save per pass that moved the floor, never one per entry:
        // each save is an fsync, and the pass coalesces every apply that
        // finished before it.
        if let Err(error) = state.tenant_marks.persist(state.credentials.catalog()) {
            // The floor stays where it is, so every entry above it keeps
            // its place in the log. The next pass persists and saves again.
            tracing::error!(
                group_id = *group_id,
                %error,
                "apply loop: tenant write marks did not persist; the applied floor waits"
            );
            continue;
        }
        if let Some(floor) = lane.take_floor_to_save() {
            // Every entry the floor covers keeps its changes on disk.
            state.change_stream.journal_group(*group_id, floor);
            record_durable_apply(state, *group_id, floor);
            // The journal holds the events of every entry the floor covers,
            // so no restart stages them again from a marker's chunks.
            state.redo_chunks.release_unstaged_through(*group_id, floor);
        }
    }
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
    #[test]
    fn the_floor_waits_for_the_barrier_rows_to_persist() {
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
        settle_lanes(ctx, &mut lanes);
        assert!(
            saved.lock().expect("saved floors").is_empty(),
            "no floor passes a row that is not durable"
        );
        assert!(lanes[&GROUP].floor_pending());

        settle_lanes(ctx, &mut lanes);
        assert_eq!(*saved.lock().expect("saved floors"), vec![(GROUP, 5)]);
        assert!(!lanes[&GROUP].floor_pending());
    }
}
