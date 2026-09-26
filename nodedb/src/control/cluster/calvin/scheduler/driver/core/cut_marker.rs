// SPDX-License-Identifier: BUSL-1.1

//! A backup's cut marker on this vShard's scheduler, and the tenant write
//! mark a committed Calvin transaction records.
//!
//! The scheduler reports a marker to the node's cut registry once every
//! transaction delivered before it finished: installed by its flush, or
//! dropped under an abort verdict. The backup waiting on the marker then
//! snapshots a vShard that holds every one of them. Every transaction
//! delivered after the marker records a commit HLC above the marker's
//! watermark, so a restore of that backup refuses it.

use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::security::auth_fence::cluster::group_of_vshard;
use crate::control::state::tenant_marks::MarkSite;

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

    /// Record the commit HLC of the committed transaction `txn_id` on its
    /// tenant's observed write high-water, before its `CompletionAck` lets
    /// the coordinator acknowledge the COMMIT.
    ///
    /// Only a slice that carries a primary user data write records it. The
    /// implicit edge cleanup that dual-homes alongside one writes no user row
    /// of its own.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn record_calvin_write_mark(
        &self,
        txn_id: TxnId,
    ) {
        let Some(pending) = self.pending.get(&txn_id) else {
            return;
        };
        if !pending.has_primary_write {
            return;
        }
        let tx_class = &pending.txn.tx_class;
        let tenant_id = tx_class.tenant_id.as_u64();
        let collection = tx_class.write_set.0.first().map(|set| set.collection());
        let commit_hlc = self
            .cut_floors
            .commit_hlc(pending.txn.epoch, pending.txn.epoch_system_ms);
        self.shared
            .advance_tenant_write_hlc(tenant_id, commit_hlc, "calvin flush", collection);

        // The durable mark, in the data group that homes this vShard, lands
        // before the ack too: RESTORE's guard reads it on every node, after a
        // restart as well.
        let group_id = match group_of_vshard(&self.shared, self.vshard_id) {
            Ok(group_id) => group_id,
            Err(error) => {
                tracing::error!(
                    vshard_id = self.vshard_id,
                    %error,
                    "calvin: no data group for this vShard; the commit's write mark stays \
                     in memory only"
                );
                return;
            }
        };
        let marks = &self.shared.tenant_marks;
        marks.raise(
            group_id,
            tenant_id,
            commit_hlc,
            MarkSite::CalvinFlush,
            collection,
        );
        if let Err(error) = marks.persist(self.shared.credentials.catalog()) {
            // The mark stays pending; the next persist on this node writes it.
            tracing::error!(
                vshard_id = self.vshard_id,
                %error,
                "calvin: the commit's write mark did not persist yet"
            );
        }
    }
}
