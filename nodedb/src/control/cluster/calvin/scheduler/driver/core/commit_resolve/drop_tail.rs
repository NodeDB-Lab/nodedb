// SPDX-License-Identifier: BUSL-1.1

//! The end of a staged slice that commits no log entry: an aborted txn, or a
//! committed slice with no local write. Its drop answered, so it owes an
//! empty `CompletionAck` and completes.

use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason;

impl Scheduler {
    /// Complete `txn_id` once its drop answered.
    ///
    /// A drop that did not answer `Ok` still completes the txn: no replica
    /// writes it, whatever the drop did on this core.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn finish_drop(
        &mut self,
        txn_id: TxnId,
        response: &Response,
    ) {
        if response.status != Status::Ok {
            tracing::error!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                error = ?response.error_code.as_deref(),
                "calvin: drop answered with an error; completing the txn, since no replica \
                 writes it"
            );
            self.metrics.record_executor_error();
            self.metrics
                .record_infra_abort(infra_abort_reason::IO_ERROR);
        }
        self.complete_without_entry(txn_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ErrorCode;
    use crate::control::cluster::calvin::scheduler::driver::core::owed::OwedKind;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        error_response, scheduler_with_pending, staged_response,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;

    /// A drop that answers `Ok` completes the txn and owes its ack.
    #[tokio::test]
    async fn an_answered_drop_completes_the_txn_and_owes_its_ack() {
        let txn_id = TxnId::new(9, 1);
        let (mut scheduler, _dir) = scheduler_with_pending(txn_id, CommitState::AwaitingDrop);

        scheduler.finish_drop(txn_id, &staged_response(Status::Ok, None));

        assert!(scheduler.applied.is_applied(9, 1));
        assert!(scheduler.ledger.is_applied(9, 1), "no log entry marks it");
        assert!(!scheduler.pending.contains_key(&txn_id));
        assert!(
            scheduler
                .owed
                .contains_key(&(txn_id, OwedKind::CompletionAck))
        );
    }

    /// A drop that returns an error still completes the txn and owes its
    /// ack: no replica writes a txn that ends with no log entry.
    #[tokio::test]
    async fn a_drop_error_still_completes_the_txn() {
        let txn_id = TxnId::new(9, 2);
        let (mut scheduler, _dir) = scheduler_with_pending(txn_id, CommitState::AwaitingDrop);

        scheduler.finish_drop(
            txn_id,
            &error_response(ErrorCode::Internal {
                detail: "core failed".to_string(),
            }),
        );

        assert!(scheduler.applied.is_applied(9, 2));
        assert!(!scheduler.pending.contains_key(&txn_id));
        assert!(!scheduler.is_apply_halted());
        assert!(
            scheduler
                .owed
                .contains_key(&(txn_id, OwedKind::CompletionAck))
        );
    }
}
