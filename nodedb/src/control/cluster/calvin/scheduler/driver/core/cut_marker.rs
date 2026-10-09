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

use super::scheduler::Scheduler;

impl Scheduler {
    /// Receive a backup's cut marker carrying `hlc`, in input order.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn receive_cut_marker(
        &mut self,
        hlc: u64,
    ) {
        if self
            .cut_floors
            .receive(hlc, self.applied.highest_seen_epoch())
        {
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
}
