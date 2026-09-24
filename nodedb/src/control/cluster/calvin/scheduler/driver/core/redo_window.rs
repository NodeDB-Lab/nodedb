// SPDX-License-Identifier: BUSL-1.1

//! Close the outcome-floor window of a committed txn's `TransactionRedo`
//! record.
//!
//! The record joins the pending txn when it is appended. The window settles
//! when the txn completes: the flush applied the record. It holds when the
//! txn stays unapplied here, because restart replay must reach the record.

use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Hold the redo record of `txn_id`: the scheduler halted with the txn
    /// unapplied.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn hold_redo_records(
        &mut self,
        txn_id: TxnId,
    ) {
        if let Some(records) = self
            .pending
            .get_mut(&txn_id)
            .and_then(|pending| pending.redo_records.take())
        {
            records.hold();
        }
    }

    /// Hold the redo record of every pending txn: the scheduler stops with
    /// them unapplied.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn hold_all_redo_records(
        &mut self,
    ) {
        for pending in self.pending.values_mut() {
            if let Some(records) = pending.redo_records.take() {
                records.hold();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::halt::{HaltReason, HaltStep};
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::scheduler_with_pending;
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
    use crate::control::server::dispatch_utils::MintedRecords;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    /// Attach an appended redo record to the pending txn, as a committed
    /// resolve does.
    fn attach_redo_record(scheduler: &mut Scheduler, txn_id: TxnId) -> Lsn {
        let records = MintedRecords::open(&scheduler.shared.outcome_floor);
        let lsn = records
            .appender(&scheduler.shared.wal, crate::wal::manager::NO_APPLY_KEY)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"redo",
            )
            .expect("append");
        if let Some(pending) = scheduler.pending.get_mut(&txn_id) {
            pending.redo_records = Some(records);
        }
        lsn
    }

    fn awaiting_flush() -> CommitState {
        CommitState::AwaitingResolve {
            committed: true,
            redo_lsn: None,
        }
    }

    #[tokio::test]
    async fn a_halted_txn_holds_its_redo_record() {
        let txn_id = TxnId::new(3, 1);
        let (mut scheduler, _dir) = scheduler_with_pending(txn_id, awaiting_flush());
        let lsn = attach_redo_record(&mut scheduler, txn_id);

        scheduler.halt_apply(
            txn_id,
            HaltReason::FlushFailed,
            HaltStep::Flush,
            "flush refused".to_string(),
        );

        let floor = &scheduler.shared.outcome_floor;
        assert!(
            floor.floor() < lsn,
            "the held record keeps the floor below it"
        );
        assert_eq!(floor.leaked_windows(), 0);
    }

    #[tokio::test]
    async fn a_completed_txn_settles_its_redo_record() {
        let txn_id = TxnId::new(3, 2);
        let (mut scheduler, _dir) = scheduler_with_pending(txn_id, awaiting_flush());
        let lsn = attach_redo_record(&mut scheduler, txn_id);

        scheduler.on_txn_complete(txn_id);

        let floor = &scheduler.shared.outcome_floor;
        assert_eq!(floor.floor(), lsn);
        assert_eq!(floor.leaked_windows(), 0);
    }

    #[tokio::test]
    async fn a_stopping_scheduler_holds_every_pending_redo_record() {
        let txn_id = TxnId::new(3, 3);
        let (mut scheduler, _dir) = scheduler_with_pending(txn_id, awaiting_flush());
        let lsn = attach_redo_record(&mut scheduler, txn_id);

        scheduler.hold_all_redo_records();

        let floor = &scheduler.shared.outcome_floor;
        assert!(floor.floor() < lsn);
        assert_eq!(floor.leaked_windows(), 0);
    }
}
