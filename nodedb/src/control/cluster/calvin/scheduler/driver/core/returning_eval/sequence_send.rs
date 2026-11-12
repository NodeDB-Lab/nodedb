// SPDX-License-Identifier: BUSL-1.1

//! Send a slice's `RETURNING` sequence changes, or its refusal, to every
//! participant whose evaluation follows them.
//!
//! - A stored plan's changes go once the plan is stored, before the commit
//!   vote. They come from the stored reply, never from the leader's own
//!   evaluation, so every leader of the vShard sends the same changes.
//! - An abort vote sends a refusal for every local plan not sent yet. A
//!   participant that follows such a plan votes abort at once. Every
//!   participant's vote completes the verdict's tally, so a waiting
//!   participant must vote.
//! - The stall tick sends the stored changes again on a leader that holds
//!   the txn after its vote, so a leader that died before its changes
//!   landed does not leave a follower waiting.
//!
//! The changes travel as one `ReplicatedWrite::CalvinSequenceEffects` entry
//! per target vShard, through the target's data-group log (see
//! [`super::super::propose`]). A target counts the first copy of each
//! plan's changes or refusal. A copy sent again changes nothing. A proposal
//! that never lands leaves the target waiting, and the target's timeout
//! entry ends its wait on every replica alike.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_cluster::calvin::types::TxClass;

use super::super::propose::{CalvinSequenceEffectsProposal, propose_calvin_sequence_effects};
use super::super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
use crate::control::cluster::calvin::scheduler::frozen_replies;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::wal_replication::types::{CalvinFrozenReply, PlanSequenceEffects};

impl Scheduler {
    /// Send the changes of every plan of `stored`, `txn_id`'s stored reply
    /// on this vShard, that this leader did not send yet.
    pub(super) fn send_sequence_effects(&mut self, txn_id: TxnId, stored: &CalvinFrozenReply) {
        let plans = stored
            .plans
            .iter()
            .map(|plan| PlanSequenceEffects {
                plan_index: plan.plan_index,
                effects: plan.sequence_effects.clone(),
                refused: false,
            })
            .collect();
        self.send_plans(txn_id, plans);
    }

    /// Send a refusal for every plan of `txn_id` this vShard evaluates that
    /// this leader did not send yet. Runs with each abort vote.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn send_sequence_refusal(
        &mut self,
        txn_id: TxnId,
    ) {
        let vshard_id = self.vshard_id;
        let Some(contract) = self
            .pending
            .get(&txn_id)
            .and_then(|pending| pending.txn.tx_class.returning.as_ref())
            .filter(|contract| contract.orders_sequences())
        else {
            return;
        };
        let plans = contract
            .plans
            .iter()
            .filter(|plan| plan.vshards.contains(&vshard_id))
            .map(|plan| PlanSequenceEffects {
                plan_index: plan.plan_index,
                effects: Vec::new(),
                refused: true,
            })
            .collect();
        self.send_plans(txn_id, plans);
    }

    /// Send the stored changes of every txn this leader holds after its
    /// vote. Runs on the stall tick, on the leader only.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resend_stored_sequence_effects(
        &mut self,
    ) {
        if !self.role.is_leader() {
            return;
        }
        let voted: Vec<TxnId> = self
            .pending
            .iter()
            .filter(|(_, pending)| {
                let Some(contract) = pending.txn.tx_class.returning.as_ref() else {
                    return false;
                };
                // A txn whose every local plan went sends nothing more.
                let unsent = contract.plans.iter().any(|plan| {
                    plan.vshards.contains(&self.vshard_id)
                        && !pending.returning.sent.contains(&plan.plan_index)
                });
                pending.commit_state != CommitState::Staged && contract.orders_sequences() && unsent
            })
            .map(|(txn_id, _)| *txn_id)
            .collect();
        for txn_id in voted {
            match frozen_replies::load(self.shared.credentials.catalog(), self.vshard_id, txn_id) {
                Ok(Some(stored)) => self.send_sequence_effects(txn_id, &stored),
                Ok(None) => {}
                Err(error) => tracing::debug!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    %error,
                    "calvin: the stored frozen reply does not read; the next stall tick sends \
                     its changes"
                ),
            }
        }
    }

    /// Send `plans` of `txn_id` that this leader did not send yet to every
    /// target vShard that follows them. Each proposal runs on its own task
    /// until `config.passive_timeout()` passes.
    fn send_plans(&mut self, txn_id: TxnId, plans: Vec<PlanSequenceEffects>) {
        let vshard_id = self.vshard_id;
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // Nothing is marked sent: a later pass sends again.
            tracing::warn!(
                vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: RETURNING sequence changes not sent: no Tokio runtime"
            );
            return;
        };
        let Some(pending) = self.pending.get_mut(&txn_id) else {
            return;
        };
        let tx_class: Arc<TxClass> = Arc::clone(&pending.txn.tx_class);
        let Some(contract) = tx_class.returning.as_ref() else {
            return;
        };
        let mut by_target: BTreeMap<u32, Vec<PlanSequenceEffects>> = BTreeMap::new();
        for plan in plans {
            if !pending.returning.sent.insert(plan.plan_index) {
                continue;
            }
            for target in contract.sequence_targets(plan.plan_index, vshard_id) {
                by_target.entry(target).or_default().push(plan.clone());
            }
        }
        let budget = self.config.passive_timeout();
        for (target_vshard, plans) in by_target {
            let proposal = CalvinSequenceEffectsProposal {
                target_vshard,
                epoch: txn_id.epoch,
                position: txn_id.position,
                tenant_id: tx_class.tenant_id,
                database_id: tx_class.database_id,
                source_vshard: vshard_id,
                plans,
            };
            let state = Arc::downgrade(&self.shared);
            handle.spawn(async move {
                // no-determinism: the proposal's retry budget is liveness only; the log order decides the result.
                let deadline = tokio::time::Instant::now() + budget;
                if let Err(error) = propose_calvin_sequence_effects(state, proposal, deadline).await
                {
                    tracing::warn!(
                        target_vshard,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        %error,
                        "calvin: RETURNING sequence changes not proposed before their \
                         deadline; the target's timeout entry ends the txn"
                    );
                }
            });
        }
    }
}
