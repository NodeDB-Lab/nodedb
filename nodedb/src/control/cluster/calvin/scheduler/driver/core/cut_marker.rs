// SPDX-License-Identifier: BUSL-1.1

//! A backup's cut marker on this vShard's scheduler.
//!
//! The scheduler reports a marker to the node's cut registry once every
//! transaction delivered before it finished: installed from its stamped
//! redo, or ended with no log entry. The backup waiting on the marker then
//! snapshots a vShard that holds every one of them. Every transaction
//! delivered after the marker records a commit HLC above the marker's
//! watermark, so a restore of that backup refuses it. The data-group apply
//! raises the tenant's write mark from the redo entry's commit HLC.
//!
//! A marker that carries a barrier orders the data groups too (see
//! [`crate::control::backup::cut_order`]). The scheduler holds the redo of
//! every transaction delivered after it until this node applied the cut's
//! barrier in the vShard's group, or the cut's window closed. It starts the
//! node's driver of the cut, which places the barrier while this node leads
//! the group.

use std::sync::Arc;

use nodedb_cluster::calvin::CutBarrierWire;

use super::scheduler::Scheduler;
use crate::control::backup::cut_order::driver::ensure_driver;
use crate::control::backup::cut_order::{CutWindow, OrderedCut};
use crate::control::state::SharedState;

impl Scheduler {
    /// Receive a backup's cut marker carrying `hlc`, in input order. A
    /// `barrier` orders the cut's barriers with the marker.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn receive_cut_marker(
        &mut self,
        hlc: u64,
        restore_point: u64,
        barrier: Option<Arc<CutBarrierWire>>,
    ) {
        let highest_seen = self.applied.highest_seen_epoch();
        if let Some(barrier) = barrier {
            self.hold_for_cut(
                OrderedCut::from_marker(hlc, restore_point, &barrier),
                highest_seen,
            );
        }
        if self.cut_floors.receive(hlc, highest_seen) {
            self.shared.calvin.cuts.note_passed(self.vshard_id, hlc);
        }
        self.report_passed_cuts();
    }

    /// Report every marker whose earlier transactions all finished, and fold
    /// the markers every later epoch is above.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn report_passed_cuts(
        &mut self,
    ) {
        let applied = &self.applied;
        let passed = self
            .cut_floors
            .take_passed(|through| applied.is_fully_applied_through(through));
        for hlc in passed {
            self.shared.calvin.cuts.note_passed(self.vshard_id, hlc);
        }
        self.cut_floors.fold(self.applied.fully_applied_epoch());
    }

    /// Hold the redo of every transaction after the marker of `cut`, which
    /// arrived after epochs up to `through`, and start the cut's driver.
    fn hold_for_cut(&mut self, cut: OrderedCut, through: Option<u64>) {
        if window_closed(&self.shared, &cut) {
            // A marker this scheduler receives again after a restart, long
            // after its cut ended. Its cut placed every barrier it ever will.
            return;
        }
        if self.cut_holds.receive(cut.clone(), through) {
            ensure_driver(&self.shared, &cut);
        }
    }

    /// Whether the redo of a transaction of `epoch` waits for an ordered
    /// cut's barrier.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn held_for_cut(
        &self,
        epoch: u64,
    ) -> bool {
        self.cut_holds.holds(epoch)
    }

    /// End the hold of every cut whose barrier this node applied in the
    /// vShard's group, or whose window closed. Start the driver again of a
    /// cut still held whose driver stopped. Returns whether a hold ended: the
    /// held redo can then be proposed.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn release_cut_holds(
        &mut self,
    ) -> bool {
        if self.cut_holds.is_empty() {
            return false;
        }
        let group_id = self.data_group();
        let shared = Arc::clone(&self.shared);
        let vshard_id = self.vshard_id;
        let ended = self.cut_holds.end(|cut| {
            let applied = group_id.is_some_and(|group_id| {
                shared
                    .calvin
                    .cut_barriers
                    .applied(cut.key(), group_id)
                    .is_some()
            });
            if applied {
                return true;
            }
            let closed = window_closed(&shared, cut);
            if closed {
                tracing::warn!(
                    vshard_id,
                    ?group_id,
                    hlc = cut.hlc,
                    "calvin scheduler: a cut's window closed before its barrier applied here; \
                     releasing the redo held for it"
                );
            }
            closed
        });
        for cut in self.cut_holds.cuts() {
            // no-determinism: the window bounds only when this node proposes.
            if shared.hlc_clock.now().wall_ns <= CutWindow::on(&shared, cut.hlc).propose_until {
                ensure_driver(&shared, cut);
            }
        }
        ended
    }

    /// The data group that homes this vShard.
    fn data_group(&self) -> Option<u64> {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.routing()
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_for_vshard(self.vshard_id)
            .ok()
    }
}

/// Whether `cut`'s hold window closed on `state`'s clock.
///
/// no-determinism: the window bounds only how long this node holds redo.
/// Every replica installs the redo at its place in the log.
fn window_closed(state: &SharedState, cut: &OrderedCut) -> bool {
    state.hlc_clock.now().wall_ns > CutWindow::on(state, cut.hlc).hold_until
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::SchedulerInput;

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler, make_validate_only_txn, test_coll_vshard,
    };

    /// A cut whose watermark is now, so its window is open.
    fn fresh_cut(scheduler: &Scheduler) -> OrderedCut {
        OrderedCut {
            hlc: scheduler.shared.hlc_clock.now().wall_ns,
            restore_point: 0,
            capture: None,
        }
    }

    fn marker(cut: &OrderedCut) -> SchedulerInput {
        SchedulerInput::CutMarker {
            hlc: cut.hlc,
            restore_point: cut.restore_point,
            barrier: Some(Arc::new(cut.to_wire())),
        }
    }

    /// Only a transaction delivered after a barrier-carrying marker waits,
    /// and the marker still passes once the earlier ones finished.
    #[tokio::test]
    async fn a_marker_with_a_barrier_holds_only_later_redo() {
        let (mut scheduler, _dir) = build_test_scheduler(test_coll_vshard());
        scheduler
            .process_scheduler_input(SchedulerInput::Txn(Box::new(make_validate_only_txn(4, 0))));
        let cut = fresh_cut(&scheduler);
        scheduler.process_scheduler_input(marker(&cut));

        assert!(!scheduler.held_for_cut(4), "epoch 4 came before the marker");
        assert!(scheduler.held_for_cut(5), "epoch 5 came after it");
        assert!(
            scheduler
                .shared
                .calvin
                .cut_barriers
                .cut(cut.key())
                .is_some(),
            "the cut's driver tracks it"
        );
    }

    /// The hold ends once this node applied the cut's barrier in the
    /// vShard's group, and not before. A barrier of another group ends
    /// nothing.
    #[tokio::test]
    async fn the_hold_ends_when_the_group_applied_the_barrier() {
        let (mut scheduler, _dir) = build_test_scheduler(test_coll_vshard());
        let cut = fresh_cut(&scheduler);
        scheduler.process_scheduler_input(marker(&cut));
        assert!(
            scheduler.held_for_cut(0),
            "a marker before any epoch holds all"
        );
        let group_id = scheduler
            .data_group()
            .expect("the fixture routes every vShard");

        assert!(!scheduler.release_cut_holds());
        scheduler
            .shared
            .calvin
            .cut_barriers
            .note_applied(&cut, group_id.wrapping_add(1), 9);
        assert!(!scheduler.release_cut_holds(), "another group's barrier");
        assert!(scheduler.held_for_cut(0));

        scheduler
            .shared
            .calvin
            .cut_barriers
            .note_applied(&cut, group_id, 12);
        assert!(scheduler.release_cut_holds());
        assert!(!scheduler.held_for_cut(0));
        assert!(!scheduler.release_cut_holds(), "nothing left to end");
    }

    /// A second copy of the marker keeps the first copy's place, and a copy
    /// of a cut whose barrier applied holds nothing for long.
    #[tokio::test]
    async fn a_marker_copy_keeps_the_first_place() {
        let (mut scheduler, _dir) = build_test_scheduler(test_coll_vshard());
        scheduler
            .process_scheduler_input(SchedulerInput::Txn(Box::new(make_validate_only_txn(4, 0))));
        let cut = fresh_cut(&scheduler);
        scheduler.process_scheduler_input(marker(&cut));
        scheduler
            .process_scheduler_input(SchedulerInput::Txn(Box::new(make_validate_only_txn(6, 0))));
        scheduler.process_scheduler_input(marker(&cut));
        assert!(
            scheduler.held_for_cut(5),
            "epoch 5 came after the first copy, so it waits"
        );
        assert_eq!(scheduler.cut_holds.cuts().count(), 1);

        let group_id = scheduler
            .data_group()
            .expect("the fixture routes every vShard");
        scheduler
            .shared
            .calvin
            .cut_barriers
            .note_applied(&cut, group_id, 30);
        assert!(scheduler.release_cut_holds());
        scheduler.process_scheduler_input(marker(&cut));
        assert!(
            scheduler.release_cut_holds(),
            "a late copy finds the barrier applied"
        );
        assert!(!scheduler.held_for_cut(7));
    }

    /// A marker received long after its cut's window closed holds nothing:
    /// a replay after a restart never blocks redo on a cut that ended.
    #[tokio::test]
    async fn a_stale_marker_holds_nothing() {
        let (mut scheduler, _dir) = build_test_scheduler(test_coll_vshard());
        let stale = OrderedCut {
            hlc: 1,
            restore_point: 0,
            capture: None,
        };
        scheduler.process_scheduler_input(marker(&stale));
        assert!(!scheduler.held_for_cut(0));
        assert!(scheduler.cut_holds.is_empty());
    }

    /// A cut whose window closes with no barrier ends its hold.
    #[tokio::test]
    async fn a_closed_window_ends_the_hold() {
        let (mut scheduler, _dir) = build_test_scheduler(test_coll_vshard());
        let cut = fresh_cut(&scheduler);
        scheduler.process_scheduler_input(marker(&cut));
        assert!(scheduler.held_for_cut(1));
        let past_window = CutWindow::on(&scheduler.shared, cut.hlc)
            .hold_until
            .saturating_add(1);
        scheduler
            .shared
            .hlc_clock
            .update(nodedb_types::Hlc::new(past_window, 0));
        assert!(scheduler.release_cut_holds());
        assert!(!scheduler.held_for_cut(1));
    }
}
