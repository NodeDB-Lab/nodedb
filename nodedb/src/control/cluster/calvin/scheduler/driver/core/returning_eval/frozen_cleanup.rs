// SPDX-License-Identifier: BUSL-1.1

//! The removal of the stored frozen replies of finished txns.
//!
//! - A txn's finish removes its row when one is stored. The probe reads no
//!   reply bytes, and the remove is idempotent.
//! - The stall tick's sweep removes the row of every txn the applied ledger
//!   marks finished. It catches a row the apply loop stored after the
//!   finish's probe, and a row of a txn that finished on no scheduler here.

use super::super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::frozen_replies;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Remove the stored frozen reply of `txn_id`, which finished here. A
    /// failed probe or remove leaves the row, and the stall tick's sweep
    /// removes it.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn forget_frozen_reply(
        &self,
        txn_id: TxnId,
    ) {
        let catalog = self.shared.credentials.catalog();
        let removed = frozen_replies::holds(catalog, self.vshard_id, txn_id).and_then(|held| {
            if held {
                frozen_replies::remove(catalog, self.vshard_id, txn_id)
            } else {
                Ok(())
            }
        });
        if let Err(error) = removed {
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                %error,
                "calvin: the frozen reply of a finished txn was not removed; the stall tick's \
                 sweep removes it"
            );
            crate::diag::calvin_frozen_reply_store_failed(
                self.vshard_id,
                Some((txn_id.epoch, txn_id.position)),
                "remove",
                &error,
            );
        }
    }

    /// Remove every stored frozen reply of this vShard whose txn finished.
    /// A failed pass keeps the rows, and the next stall tick retries.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn sweep_finished_frozen_replies(
        &self,
    ) {
        if let Err(error) = frozen_replies::sweep_finished(
            self.shared.credentials.catalog(),
            self.vshard_id,
            &self.ledger,
        ) {
            tracing::warn!(
                vshard_id = self.vshard_id,
                %error,
                "calvin: the frozen replies of finished txns were not removed; retrying on \
                 the next stall tick"
            );
            crate::diag::calvin_frozen_reply_store_failed(self.vshard_id, None, "remove", &error);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::calvin::CalvinCompletionRegistry;

    use super::super::fixtures::{
        FROZEN_TXN as TXN, apply_frozen_reply, frozen_leader as leader, image_rows,
        stores_frozen_reply,
    };
    use super::*;
    use crate::bridge::envelope::{StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, following_pending, make_local_write_txn,
        staged_response, test_coll_vshard,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
    use crate::control::wal_replication::types::CalvinFrozenReply;

    fn holds(scheduler: &Scheduler) -> bool {
        stores_frozen_reply(scheduler, TXN)
    }

    fn store(scheduler: &Scheduler, txn: TxnId) {
        frozen_replies::store(
            scheduler.shared.credentials.catalog(),
            &CalvinFrozenReply {
                epoch: txn.epoch,
                position: txn.position,
                vshard: scheduler.vshard_id,
                plans: Vec::new(),
            },
        )
        .expect("store");
    }

    /// An abort verdict ends the txn with no install. Its frozen state goes:
    /// the leader's in-memory reply with the txn, and the stored row.
    #[tokio::test]
    async fn an_abort_verdict_drops_the_frozen_state() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) = leader(&registry);
        let pending = scheduler.pending.get_mut(&TXN).expect("staged");
        pending.returning.images.insert(0, image_rows(2));
        scheduler.resolve_staged_commit(TXN, &staged_response(Status::Ok, Some(StageVote::Commit)));
        scheduler.settle_returning_eval().await;
        apply_frozen_reply(&scheduler, TXN);
        scheduler.vote_stored_frozen_replies();
        assert_eq!(
            scheduler.pending.get(&TXN).map(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        );
        assert!(holds(&scheduler));

        scheduler.resume_on_verdict(TXN, false);
        let pending = scheduler.pending.get(&TXN).expect("dropping");
        assert_eq!(pending.commit_state, CommitState::AwaitingDrop);
        let request_id = pending.awaiting.expect("the drop is awaited");
        scheduler.handle_completion(TXN, request_id, Some(staged_response(Status::Ok, None)));

        assert!(!scheduler.pending.contains_key(&TXN), "the txn finished");
        assert!(!holds(&scheduler), "the stored reply is spent");
    }

    /// A follower drops a stored reply at the abort verdict, though no
    /// store on this node noted it: a row a snapshot install or a boot
    /// left goes like any other.
    #[tokio::test]
    async fn a_follower_drops_the_stored_reply_at_an_abort() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), Arc::clone(&registry));
        scheduler
            .pending
            .insert(TXN, following_pending(make_local_write_txn(3, 0), TXN));
        store(&scheduler, TXN);

        scheduler.resume_on_verdict(TXN, false);

        assert!(!scheduler.pending.contains_key(&TXN));
        assert!(!holds(&scheduler));
    }

    /// A row the apply loop stores after the finish probed is not lost: the
    /// stall tick's sweep removes it once the ledger marks the txn finished.
    /// A finish with no stored row writes nothing and keeps other rows.
    #[tokio::test]
    async fn a_row_stored_after_the_finish_goes_on_the_sweep() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (scheduler, _dir, _data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), Arc::clone(&registry));
        let other = TxnId::new(TXN.epoch + 1, 0);
        store(&scheduler, other);
        scheduler.forget_frozen_reply(TXN);
        assert!(
            stores_frozen_reply(&scheduler, other),
            "another txn's row stays"
        );

        scheduler.ledger.mark_terminal(TXN.epoch, TXN.position);
        store(&scheduler, TXN);
        scheduler.sweep_finished_frozen_replies();
        assert!(!holds(&scheduler));
        assert!(stores_frozen_reply(&scheduler, other));
    }
}
