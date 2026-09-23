// SPDX-License-Identifier: BUSL-1.1

//! `CalvinReadResult` handling and dependent-read barrier timeout sweeps.

use tracing::warn;

use super::super::barrier::ReadResultEvent;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Handle a `ReadResultEvent` from the per-vshard Raft apply loop.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn handle_read_result(
        &mut self,
        event: ReadResultEvent,
    ) {
        let txn_id = TxnId::new(event.epoch, event.position);

        let barrier = match self.dependent_barrier.get_mut(&txn_id) {
            Some(b) => b,
            None => {
                // No barrier for this txn — may have already timed out or
                // been dispatched. Log and ignore.
                warn!(
                    vshard_id = self.vshard_id,
                    epoch = event.epoch,
                    position = event.position,
                    passive_vshard = event.passive_vshard,
                    "calvin: received CalvinReadResult for unknown txn; ignoring"
                );
                return;
            }
        };

        barrier.waiting_for.remove(&event.passive_vshard);
        barrier.received.insert(event.passive_vshard, event.values);

        if !barrier.is_complete() {
            return;
        }

        // All passive results in — remove barrier and dispatch active.
        let barrier = self
            .dependent_barrier
            .remove(&txn_id)
            .expect("barrier just confirmed present");

        let injected_reads = barrier.assemble_injected_reads();
        let lock_owner = barrier.lock_owner;
        let txn = barrier.txn;

        self.dispatch_active_txn(txn, txn_id, lock_owner, injected_reads);
    }

    /// Check all pending dependent barriers for timeout.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn check_dependent_barrier_timeouts(
        &mut self,
    ) {
        // Collect timed-out txn ids first (avoid borrowing issues).
        let timed_out: Vec<TxnId> = self
            .dependent_barrier
            .iter()
            .filter(|(_, b)| b.is_timed_out())
            .map(|(id, _)| *id)
            .collect();

        for txn_id in timed_out {
            if let Some(barrier) = self.dependent_barrier.remove(&txn_id) {
                warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    still_waiting = ?barrier.waiting_for,
                    "calvin: dependent-read barrier timed out; releasing locks"
                );
                self.metrics.record_executor_error();
                self.metrics.record_infra_abort(
                    crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::PASSIVE_PARTICIPANT_TIMEOUT,
                );
                // A barrier txn never entered `pending`: release its locks
                // under the owner the barrier recorded.
                self.on_unpending_txn_complete(txn_id, barrier.lock_owner);
            }
        }
    }
}
