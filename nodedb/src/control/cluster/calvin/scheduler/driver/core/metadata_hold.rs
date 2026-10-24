// SPDX-License-Identifier: BUSL-1.1

//! Hold a sequenced transaction until this node's catalog reached the one
//! its coordinator planned it against.
//!
//! The sequencer group and the metadata group apply on independent loops.
//! Without this hold, a replica can run a transaction that writes a
//! collection before it applied that collection's creation: the write lands
//! on an unregistered collection, and the creation's storage clear then
//! erases it. The coordinator stamps its applied metadata index on the class
//! (`TxClass::metadata_floor`).
//!
//! A held transaction closes the intake gate, so every later input waits
//! behind it. The processing order stays the log order on every replica.
//!
//! The wait has no deadline, like the data-group hold: applying the txn
//! earlier breaks the order the hold keeps. It ends without applying only when
//! the metadata group left this node. That cause is local to this replica, so
//! no replica can abort the txn in a way the others reach too. The scheduler
//! halts instead and never marks the position applied: the txn replays from
//! the sequencer log on the next boot, and the other replicas apply it as
//! usual. The originator's completion comes from the sequencer group, which
//! this replica's halt does not block.

use std::time::{Duration, Instant};

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::calvin::types::SequencedTxn;
use tracing::warn;

use super::halt::{HaltReason, HaltStep};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

/// How often the run loop re-checks a held transaction's floor.
pub(in crate::control::cluster::calvin::scheduler::driver::core) const HOLD_POLL: Duration =
    Duration::from_millis(10);

/// How often a hold that still waits logs a warning.
const HOLD_WARN_EVERY: Duration = Duration::from_secs(5);

/// A sequenced transaction waiting for this node's metadata apply.
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct MetadataHold {
    txn: SequencedTxn,
    // no-determinism: only paces the warning log.
    warned_at: Instant,
}

impl Scheduler {
    /// Whether this node's metadata apply is below `txn`'s floor.
    fn metadata_floor_unreached(&self, txn: &SequencedTxn) -> bool {
        let floor = txn.tx_class.metadata_floor;
        floor != 0
            && self
                .shared
                .applied_index_watcher(METADATA_GROUP_ID)
                .current()
                < floor
    }

    /// Process `txn` now, or hold it until this node's metadata apply
    /// reaches its floor.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn process_or_hold_for_metadata(
        &mut self,
        txn: SequencedTxn,
    ) {
        if self.metadata_floor_unreached(&txn) {
            self.metadata_hold = Some(MetadataHold {
                txn,
                // no-determinism: only paces the warning log.
                warned_at: Instant::now(),
            });
            return;
        }
        self.process_new_txn(txn);
    }

    /// Process the held transaction once its floor is reached.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resume_metadata_hold(
        &mut self,
    ) {
        let Some(mut hold) = self.metadata_hold.take() else {
            return;
        };
        if !self.metadata_floor_unreached(&hold.txn) {
            self.process_new_txn(hold.txn);
            return;
        }
        let watcher = self.shared.applied_index_watcher(METADATA_GROUP_ID);
        if watcher.is_closed() {
            let floor = hold.txn.tx_class.metadata_floor;
            self.halt_apply(
                TxnId::new(hold.txn.epoch, hold.txn.position),
                HaltReason::MetadataGroupGone,
                HaltStep::MetadataHold,
                format!(
                    "the metadata group left this node at applied index {} before it \
                     reached the txn's floor {floor}; the txn stays unapplied and replays \
                     on the next boot",
                    watcher.current()
                ),
            );
            return;
        }
        if hold.warned_at.elapsed() >= HOLD_WARN_EVERY {
            warn!(
                vshard_id = self.vshard_id,
                epoch = hold.txn.epoch,
                position = hold.txn.position,
                metadata_floor = hold.txn.tx_class.metadata_floor,
                metadata_applied = self
                    .shared
                    .applied_index_watcher(METADATA_GROUP_ID)
                    .current(),
                "a sequenced txn waits for this node's metadata apply to reach the catalog \
                 its coordinator planned it against"
            );
            // no-determinism: only paces the warning log.
            hold.warned_at = Instant::now();
        }
        self.metadata_hold = Some(hold);
    }

    /// Whether a transaction waits for this node's metadata apply.
    pub fn holds_for_metadata(&self) -> bool {
        self.metadata_hold.is_some()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::SchedulerInput;

    use super::super::intake::IntakeClosure;
    use super::super::test_support::{build_test_scheduler, make_sequenced_txn};
    use super::*;
    use crate::control::cluster::calvin::scheduler::lock_manager::{AcquireOutcome, TxnId};

    /// A txn planned against a catalog this node has not applied yet waits,
    /// and closes intake behind it. Once the metadata apply reaches its
    /// floor, the txn runs. The metadata watcher bumps only after the
    /// applier returned, so the collection's creation, storage clear
    /// included, is done by then.
    #[tokio::test]
    async fn a_txn_waits_for_its_metadata_floor_and_runs_once_reached() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        let watcher = scheduler.shared.applied_index_watcher(METADATA_GROUP_ID);
        let floor = watcher.current() + 3;
        let mut txn = make_sequenced_txn(1, 0);
        std::sync::Arc::make_mut(&mut txn.tx_class).metadata_floor = floor;

        // A conflicting holder on the txn's key: once it runs, it blocks
        // instead of dispatching to a Data Plane this test does not run.
        let keys = crate::control::cluster::calvin::scheduler::driver::helpers::expand_rw_set(
            &txn,
            crate::control::cluster::calvin::scheduler::driver::helpers::LockSite::On(
                scheduler.vshard_id,
            ),
        );
        {
            let mut lm = scheduler
                .lock_manager
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert_eq!(
                lm.acquire(TxnId::new(u64::MAX, 0), keys).outcome,
                AcquireOutcome::Ready
            );
        }

        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(txn)));
        assert!(scheduler.holds_for_metadata());
        assert_eq!(
            scheduler.intake_closure(),
            Some(IntakeClosure::MetadataCatchUp),
            "later input waits behind the held txn"
        );
        assert!(scheduler.blocked.is_empty() && scheduler.pending.is_empty());

        scheduler.resume_metadata_hold();
        assert!(
            scheduler.holds_for_metadata(),
            "the txn keeps waiting below its floor"
        );

        watcher.bump(floor);
        scheduler.resume_metadata_hold();
        assert!(!scheduler.holds_for_metadata());
        assert!(
            scheduler.blocked.contains_key(&TxnId::new(1, 0)),
            "the released txn runs"
        );
        assert_eq!(scheduler.intake_closure(), None);
    }

    /// A metadata group that left this node ends the hold: the scheduler
    /// halts with the txn unapplied, and intake stays closed.
    #[tokio::test]
    async fn a_gone_metadata_group_halts_the_held_txn() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        let watcher = scheduler.shared.applied_index_watcher(METADATA_GROUP_ID);
        let mut txn = make_sequenced_txn(1, 0);
        std::sync::Arc::make_mut(&mut txn.tx_class).metadata_floor = watcher.current() + 3;

        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(txn)));
        assert!(scheduler.holds_for_metadata());

        watcher.close();
        scheduler.resume_metadata_hold();
        assert!(!scheduler.holds_for_metadata(), "the hold ended");
        assert!(scheduler.is_apply_halted());
        let halt = scheduler.apply_halt().expect("the scheduler halted");
        assert_eq!(halt.reason, HaltReason::MetadataGroupGone);
        assert_eq!(scheduler.intake_closure(), Some(IntakeClosure::ApplyHalted));
        assert!(
            scheduler.blocked.is_empty() && scheduler.pending.is_empty(),
            "the txn never ran on this replica"
        );
    }

    /// A class with no floor never waits.
    #[tokio::test]
    async fn a_txn_without_a_floor_runs_at_once() {
        let (scheduler, _dir) = build_test_scheduler(0);
        let txn = make_sequenced_txn(1, 0);
        assert_eq!(txn.tx_class.metadata_floor, 0);
        assert!(!scheduler.metadata_floor_unreached(&txn));
    }
}
