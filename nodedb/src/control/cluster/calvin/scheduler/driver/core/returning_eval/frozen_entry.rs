// SPDX-License-Identifier: BUSL-1.1

//! The frozen-reply entry a leader proposes for each round of its
//! evaluation, before its commit vote.
//!
//! A finished round's replies travel as one
//! `ReplicatedWrite::CalvinFrozenReply` entry in the vShard's data-group log,
//! with every plan the stored reply held when the round started. The
//! leader proposes straight to its group, as it proposes a redo: the write
//! gate exempts the entry, which writes no row. The next round, or the
//! commit vote after the last, waits until the apply loop stored the
//! round's plans on this node. So each plan is durable on a quorum before
//! any participant reads its sequence changes, and before any COMMIT
//! verdict, and the reply precedes the slice's redo in the log on every
//! replica.
//!
//! | Outcome | Action |
//! |---|---|
//! | `NotLeader` | Demote. The next leader stages the txn again and reads the stored reply. |
//! | The stored reply holds the round | Start the next round, or vote commit. |
//! | Another copy extended the stored reply first | Drop the round, and evaluate again from the stored reply. |
//! | Proposed, then the group applied past the entry with no change stored | Propose again. |
//! | A transient refusal | Propose again on the stall tick. |
//! | The entry cannot be built | Vote abort. |
//!
//! A stored reply changes only when a later copy extends it (see
//! `CalvinFrozenReply::extended_by`), so every replica keeps the same
//! row. `frozen_cleanup` removes the rows of finished txns.

use std::collections::BTreeSet;

use nodedb_cluster::ClusterError;
use nodedb_raft::RaftError;

use super::super::scheduler::Scheduler;
use super::evaluate::{ReturningRefusal, frozen_effects};
use crate::control::cluster::calvin::scheduler::driver::staged_returning::OwedFrozenEntry;
use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
use crate::control::cluster::calvin::scheduler::frozen_replies;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::wal_replication::types::{CalvinFrozenReply, FrozenPlanReply};
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};

/// Where a stored reply stands against a round's owed entry.
enum RoundStore {
    /// The stored reply holds the round's plans.
    Held,
    /// Another copy changed the stored reply, and it lacks the round: the
    /// owed entry no longer extends it.
    Superseded,
    /// The stored reply is as the round found it.
    Waiting,
}

impl Scheduler {
    /// Propose `txn_id`'s frozen replies of `round`, which its evaluation
    /// left on the txn, after the stored plans `base`. The next step waits
    /// until the stored reply holds the round.
    pub(super) fn propose_frozen_reply(
        &mut self,
        txn_id: TxnId,
        base: BTreeSet<u32>,
        round: BTreeSet<u32>,
    ) {
        let bytes = match self.frozen_entry_bytes(txn_id, &base) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return,
            Err(error) => {
                self.vote_refused(
                    txn_id,
                    ReturningRefusal::internal(format!(
                        "calvin RETURNING: the frozen-reply entry cannot be built: {error}"
                    )),
                );
                return;
            }
        };
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.returning.frozen_entry = Some(OwedFrozenEntry {
                bytes,
                proposed: None,
                base,
                round,
            });
        }
        self.send_frozen_entry(txn_id);
        // An earlier leader's copy can be stored already.
        self.advance_on_stored_frozen(txn_id);
    }

    /// The encoded entry of `txn_id`'s stored plans `base` and the frozen
    /// replies its round left on the txn. `None` for a txn this scheduler
    /// does not hold.
    fn frozen_entry_bytes(
        &self,
        txn_id: TxnId,
        base: &BTreeSet<u32>,
    ) -> crate::Result<Option<Vec<u8>>> {
        let Some(pending) = self.pending.get(&txn_id) else {
            return Ok(None);
        };
        let tx_class = &pending.txn.tx_class;
        let mut plans: Vec<FrozenPlanReply> = if base.is_empty() {
            Vec::new()
        } else {
            frozen_replies::load(self.shared.credentials.catalog(), self.vshard_id, txn_id)?
                .map(|stored| stored.plans)
                .unwrap_or_default()
                .into_iter()
                .filter(|plan| base.contains(&plan.plan_index))
                .collect()
        };
        plans.extend(pending.returning.frozen.iter().map(|(plan_index, reply)| {
            FrozenPlanReply {
                plan_index: *plan_index,
                payload: reply.payload.clone(),
                column_types: reply
                    .shaped
                    .column_types
                    .iter()
                    .map(|ty| ty.wire_name().to_owned())
                    .collect(),
                sequence_effects: frozen_effects(&reply.sequence_effects),
                rejected: pending.returning.rejections.get(plan_index).cloned(),
            }
        }));
        plans.sort_by_key(|plan| plan.plan_index);
        let entry = ReplicatedEntry::new(
            tx_class.tenant_id.as_u64(),
            tx_class.database_id.as_u64(),
            self.vshard_id,
            ReplicatedWrite::CalvinFrozenReply {
                reply: CalvinFrozenReply {
                    epoch: txn_id.epoch,
                    position: txn_id.position,
                    vshard: self.vshard_id,
                    plans,
                },
            },
        );
        entry.encode().map(Some)
    }

    /// Append `txn_id`'s owed frozen-reply entry to this vShard's
    /// data-group log, and record where it landed.
    fn send_frozen_entry(&mut self, txn_id: TxnId) {
        let Some(bytes) = self
            .pending
            .get(&txn_id)
            .and_then(|pending| pending.returning.frozen_entry.as_ref())
            .map(|owed| owed.bytes.clone())
        else {
            return;
        };
        let proposed = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.propose(self.vshard_id, bytes)
        };
        match proposed {
            Ok(at) => {
                if let Some(owed) = self
                    .pending
                    .get_mut(&txn_id)
                    .and_then(|pending| pending.returning.frozen_entry.as_mut())
                {
                    owed.proposed = Some(at);
                }
            }
            Err(ClusterError::Raft(RaftError::NotLeader { .. })) => self.on_not_leader(),
            Err(error) => tracing::debug!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                %error,
                "calvin: frozen-reply entry not proposed; the stall tick proposes it again"
            ),
        }
    }

    /// Take the next step of every staged txn whose owed round the stored
    /// reply holds, or can no longer extend.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn vote_stored_frozen_replies(
        &mut self,
    ) {
        let waiting: Vec<TxnId> = self
            .pending
            .iter()
            .filter(|(_, pending)| {
                pending.commit_state == CommitState::Staged
                    && pending.returning.frozen_entry.is_some()
            })
            .map(|(txn_id, _)| *txn_id)
            .collect();
        for txn_id in waiting {
            self.advance_on_stored_frozen(txn_id);
        }
    }

    /// Take the next step of `txn_id` once the stored reply holds its owed
    /// round: the next round, or the commit vote. A round another copy
    /// superseded is dropped with its transcripts, and the txn evaluates
    /// again from the stored reply.
    fn advance_on_stored_frozen(&mut self, txn_id: TxnId) {
        let Some(owed) = self
            .pending
            .get(&txn_id)
            .filter(|pending| pending.commit_state == CommitState::Staged)
            .and_then(|pending| pending.returning.frozen_entry.as_ref())
        else {
            return;
        };
        let (base, round) = (owed.base.clone(), owed.round.clone());
        let stored =
            match frozen_replies::load(self.shared.credentials.catalog(), self.vshard_id, txn_id) {
                Ok(stored) => stored,
                Err(error) => {
                    // The step waits: the next pass reads the row again.
                    tracing::warn!(
                        vshard_id = self.vshard_id,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        %error,
                        "calvin: the stored frozen reply does not read; the vote waits"
                    );
                    crate::diag::calvin_frozen_reply_store_failed(
                        self.vshard_id,
                        Some((txn_id.epoch, txn_id.position)),
                        "load",
                        &error,
                    );
                    return;
                }
            };
        let held: BTreeSet<u32> = stored
            .iter()
            .flat_map(|reply| &reply.plans)
            .map(|plan| plan.plan_index)
            .collect();
        let standing = if round.iter().all(|plan| held.contains(plan)) {
            RoundStore::Held
        } else if held != base {
            RoundStore::Superseded
        } else {
            RoundStore::Waiting
        };
        match standing {
            RoundStore::Waiting => return,
            RoundStore::Held => {}
            RoundStore::Superseded => {
                tracing::info!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "calvin: another copy extended the stored frozen reply first; evaluating \
                     the round again from it"
                );
                if let Some(pending) = self.pending.get(&txn_id) {
                    for plan in &round {
                        pending.returning.transcripts.reset(*plan);
                    }
                }
            }
        }
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.returning.frozen_entry = None;
            pending.returning.frozen.clear();
        }
        self.advance_returning(txn_id);
    }

    /// Propose again every owed frozen-reply entry that is not in the log:
    /// never proposed, refused, or proposed and passed by the group's
    /// applied index with no reply stored, which a new term's entries
    /// overwrote. Runs on the stall tick, on the leader only.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn retry_frozen_proposals(
        &mut self,
    ) {
        if !self.role.is_leader() {
            return;
        }
        self.vote_stored_frozen_replies();
        let owed: Vec<(TxnId, Option<(u64, u64)>)> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.commit_state == CommitState::Staged)
            .filter_map(|(txn_id, pending)| {
                let owed = pending.returning.frozen_entry.as_ref()?;
                Some((*txn_id, owed.proposed))
            })
            .collect();
        for (txn_id, proposed) in owed {
            let lost = match proposed {
                None => true,
                Some((group_id, log_index)) => {
                    self.shared.applied_index_watcher(group_id).current() >= log_index
                }
            };
            if lost {
                self.send_frozen_entry(txn_id);
            }
            if !self.role.is_leader() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_cluster::calvin::{CalvinCompletionRegistry, VerdictOutcome};

    use super::super::fixtures::{
        FROZEN_TXN as TXN, frozen_leader as leader, image_rows, sequence_next, stored_plan_zero,
        stores_frozen_reply,
    };
    use super::*;
    use crate::bridge::envelope::{ErrorCode, StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        error_response, staged_response, test_coll_vshard,
    };

    fn holds(scheduler: &Scheduler) -> bool {
        stores_frozen_reply(scheduler, TXN)
    }

    /// The txn's COMMIT verdict, as the sequencer log applies it.
    fn commit_verdict(registry: &CalvinCompletionRegistry) {
        registry.note_verdict(
            nodedb_cluster::calvin::TxnId::new(TXN.epoch, TXN.position),
            VerdictOutcome::Commit,
        );
    }

    /// An earlier leader voted commit and its frozen reply is stored. This
    /// leader's stage failed, and the COMMIT verdict restages the txn. The
    /// restage reads the stored reply: it votes and resolves, evaluates
    /// nothing, and draws no sequence value.
    #[tokio::test]
    async fn a_restage_after_commit_reads_the_stored_reply() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) = leader(&registry);
        frozen_replies::store(
            scheduler.shared.credentials.catalog(),
            &stored_plan_zero(test_coll_vshard(), TXN),
        )
        .expect("an earlier leader's reply is stored");
        scheduler.resolve_staged_commit(
            TXN,
            &error_response(ErrorCode::Internal {
                detail: "stage failed".to_string(),
            }),
        );
        commit_verdict(&registry);
        scheduler.resume_on_verdict(TXN, true);
        assert_eq!(
            scheduler.pending.get(&TXN).map(|p| p.commit_state),
            Some(CommitState::AwaitingRestage)
        );
        if let Some(restage) = scheduler.restages.get_mut(&TXN) {
            // no-determinism: test-only backoff end in the past.
            restage.due = Some(Instant::now() - Duration::from_millis(1));
        }
        scheduler.restage_due();
        let pending = scheduler.pending.get_mut(&TXN).expect("staged again");
        assert_eq!(pending.commit_state, CommitState::Staged);
        pending.returning.images.insert(0, image_rows(2));

        scheduler.resolve_staged_commit(TXN, &staged_response(Status::Ok, Some(StageVote::Commit)));

        let pending = scheduler.pending.get(&TXN).expect("resolving");
        assert_eq!(pending.commit_state, CommitState::AwaitingRedoResolve);
        assert_eq!(pending.returning.evaluating, None, "nothing evaluates");
        assert!(pending.returning.frozen.is_empty());
        assert_eq!(
            sequence_next(&scheduler.shared, "s"),
            1,
            "the registry drew no value"
        );
        assert!(holds(&scheduler), "the stored reply stays for the install");
    }

    /// The stall tick proposes an entry the log does not hold again.
    #[tokio::test]
    async fn a_refused_proposal_is_proposed_again_on_the_stall_tick() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) = leader(&registry);
        let pending = scheduler.pending.get_mut(&TXN).expect("staged");
        pending.returning.images.insert(0, image_rows(1));
        scheduler.resolve_staged_commit(TXN, &staged_response(Status::Ok, Some(StageVote::Commit)));
        scheduler.settle_returning_eval().await;
        let owed = scheduler
            .pending
            .get_mut(&TXN)
            .and_then(|pending| pending.returning.frozen_entry.as_mut())
            .expect("an owed entry");
        owed.proposed = None;

        scheduler.retry_frozen_proposals();

        assert!(
            scheduler
                .pending
                .get(&TXN)
                .and_then(|pending| pending.returning.frozen_entry.as_ref())
                .and_then(|owed| owed.proposed)
                .is_some(),
            "the entry is in the log again"
        );
    }
}
