// SPDX-License-Identifier: BUSL-1.1

//! Owing, proposing, and re-proposing the scheduler's sequencer entries.
//!
//! Re-proposing is safe because every entry kind applies idempotently in the
//! completion registry. A vote is stored per vShard, so a repeat overwrites
//! it with the same value, and the verdict it completes is emitted once. An
//! ack is a set insert per vShard, and the completion fires once. The
//! mismatch and routing-failure signals set a flag, and each fires its
//! waiter once.

use tracing::{debug, error};

use nodedb_cluster::calvin::ParticipantProgress;

use super::entry::{OwedEntry, OwedKind, SchedulerProposal};
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::driver::core::sequencer_proposer::SequencerProposer;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Owe `proposal` for `txn_id` and propose it once.
    ///
    /// The entry stays owed until this node's completion registry shows it
    /// applied. [`Self::retry_owed_sequencer_entries`] proposes it again on
    /// the stall tick until then.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn propose_sequencer_entry(
        &mut self,
        txn_id: TxnId,
        proposal: SchedulerProposal,
    ) {
        let kind = proposal.kind();
        let bytes = match zerompk::to_msgpack_vec(&proposal.entry(txn_id, self.vshard_id)) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    kind = kind.label(),
                    error = %e,
                    "calvin: failed to encode a sequencer entry; it cannot be proposed",
                );
                return;
            }
        };
        let entry_seen = self.participant_progress(txn_id).is_some();
        let in_flight = propose_owed(
            self.sequencer_proposer.as_ref(),
            self.vshard_id,
            txn_id,
            kind,
            bytes.clone(),
        );
        self.owed.insert(
            (txn_id, kind),
            OwedEntry {
                bytes,
                entry_seen,
                in_flight,
            },
        );
    }

    /// Drop every owed entry this node applied, and propose the rest again.
    ///
    /// Runs on the stall tick. An entry proposed since the previous sweep is
    /// skipped once, so an entry on its way through Raft is not proposed a
    /// second time before it can apply.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn retry_owed_sequencer_entries(
        &mut self,
    ) {
        let registry = &self.registry;
        let vshard_id = self.vshard_id;
        self.owed.retain(|(txn_id, kind), owed| {
            let progress = registry.participant_progress(cluster_txn_id(*txn_id), vshard_id);
            owed.entry_seen |= progress.is_some();
            !kind.is_applied(progress, owed.entry_seen)
        });

        for ((txn_id, kind), owed) in self.owed.iter_mut() {
            if owed.in_flight {
                owed.in_flight = false;
                continue;
            }
            self.metrics
                .record_sequencer_propose_retry(kind.metric_index());
            owed.in_flight = propose_owed(
                self.sequencer_proposer.as_ref(),
                vshard_id,
                *txn_id,
                *kind,
                owed.bytes.clone(),
            );
        }
    }

    /// The registry's view of `txn_id` for this scheduler's vShard.
    fn participant_progress(&self, txn_id: TxnId) -> Option<ParticipantProgress> {
        self.registry
            .participant_progress(cluster_txn_id(txn_id), self.vshard_id)
    }
}

/// The completion registry's key for `txn_id`.
fn cluster_txn_id(txn_id: TxnId) -> nodedb_cluster::calvin::TxnId {
    nodedb_cluster::calvin::TxnId::new(txn_id.epoch, txn_id.position)
}

/// Propose `bytes`. Returns `true` when the entry left this node.
fn propose_owed(
    proposer: &dyn SequencerProposer,
    vshard_id: u32,
    txn_id: TxnId,
    kind: OwedKind,
    bytes: Vec<u8>,
) -> bool {
    match proposer.propose(bytes) {
        Ok(_) => true,
        Err(e) => {
            debug!(
                vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                kind = kind.label(),
                error = %e,
                "calvin: sequencer entry not proposed; the stall tick proposes it again",
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use nodedb_cluster::calvin::{ParticipantVote, SequencerEntry};

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        CapturingProposer, elect_data_group_leader,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler, make_sequenced_txn, scheduler_with_pending, spawn_scheduler_loop,
        staged_pending, staged_response,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
    use crate::control::cluster::calvin::scheduler::metrics::sequencer_propose_kind;

    const VSHARD: u32 = 7;

    fn retries(scheduler: &Scheduler, kind: usize) -> u64 {
        scheduler.metrics.sequencer_propose_retry_counts[kind].load(Ordering::Relaxed)
    }

    /// The leader's vote survives two refused proposals, then stops being
    /// proposed once the tally holds it.
    #[tokio::test]
    async fn staged_leader_vote_is_reproposed_until_the_tally_shows_it() {
        let (mut scheduler, _dir) = build_test_scheduler(VSHARD);
        let proposer = CapturingProposer::failing_first(2);
        scheduler.sequencer_proposer = proposer.clone();
        elect_data_group_leader(&scheduler);
        let txn_id = TxnId::new(20, 1);
        scheduler.registry.seed_expected(cluster_txn_id(txn_id), 2);
        scheduler
            .pending
            .insert(txn_id, staged_pending(make_sequenced_txn(20, 1), txn_id));

        scheduler.resolve_staged_commit(txn_id, &staged_response(Status::Ok, Some(true)));
        assert_eq!(proposer.attempt_count(), 1, "the first proposal is refused");

        scheduler.retry_owed_sequencer_entries();
        assert_eq!(proposer.attempt_count(), 2, "the second is refused too");
        assert!(proposer.accepted().is_empty());

        scheduler.retry_owed_sequencer_entries();
        assert_eq!(
            proposer.accepted(),
            vec![SequencerEntry::Vote {
                epoch: 20,
                position: 1,
                vshard: VSHARD,
                commit: true,
            }]
        );

        scheduler.retry_owed_sequencer_entries();
        assert_eq!(
            proposer.attempt_count(),
            3,
            "an accepted entry gets one tick to apply"
        );
        scheduler.retry_owed_sequencer_entries();
        assert_eq!(
            proposer.attempt_count(),
            4,
            "an accepted entry that did not apply is proposed again"
        );

        scheduler
            .registry
            .note_vote(cluster_txn_id(txn_id), VSHARD, ParticipantVote::Commit);
        scheduler.retry_owed_sequencer_entries();
        scheduler.retry_owed_sequencer_entries();
        assert_eq!(
            proposer.attempt_count(),
            4,
            "an applied vote is not proposed"
        );
        assert!(scheduler.owed.is_empty());
        assert_eq!(retries(&scheduler, sequencer_propose_kind::VOTE), 3);
    }

    /// The completion ack stays owed after the txn leaves `pending`, and
    /// stops being proposed once the registry records it.
    #[tokio::test]
    async fn completion_ack_is_reproposed_until_the_registry_records_it() {
        let txn_id = TxnId::new(21, 0);
        let (mut scheduler, _dir) = scheduler_with_pending(
            txn_id,
            CommitState::AwaitingResolve {
                committed: false,
                redo_lsn: None,
            },
        );
        let proposer = CapturingProposer::failing_first(1);
        scheduler.sequencer_proposer = proposer.clone();
        scheduler.registry.seed_expected(cluster_txn_id(txn_id), 2);

        scheduler
            .finish_resolved_commit(txn_id, staged_response(Status::Ok, None), false, None)
            .await;
        assert!(!scheduler.pending.contains_key(&txn_id));
        assert_eq!(proposer.attempt_count(), 1, "the first proposal is refused");

        scheduler.retry_owed_sequencer_entries();
        assert_eq!(
            proposer.accepted(),
            vec![SequencerEntry::CompletionAck {
                epoch: 21,
                position: 0,
                vshard_id: VSHARD,
            }]
        );
        scheduler.retry_owed_sequencer_entries();
        scheduler.retry_owed_sequencer_entries();
        assert_eq!(proposer.attempt_count(), 3);

        scheduler
            .registry
            .note_completion_ack(cluster_txn_id(txn_id), VSHARD);
        scheduler.retry_owed_sequencer_entries();
        scheduler.retry_owed_sequencer_entries();
        assert_eq!(
            proposer.attempt_count(),
            3,
            "an applied ack is not proposed"
        );
        assert!(scheduler.owed.is_empty());
        assert_eq!(
            retries(&scheduler, sequencer_propose_kind::COMPLETION_ACK),
            2
        );
    }

    /// The registry removes a txn's entry once its outcome fired. An owed
    /// entry for such a txn is settled, not proposed again.
    #[tokio::test]
    async fn entry_for_a_txn_whose_outcome_fired_is_settled() {
        let (mut scheduler, _dir) = build_test_scheduler(VSHARD);
        let proposer = CapturingProposer::failing_first(1);
        scheduler.sequencer_proposer = proposer.clone();
        let txn_id = TxnId::new(22, 0);
        let _outcome = scheduler
            .registry
            .register_completion(cluster_txn_id(txn_id), 1);

        scheduler.propose_sequencer_entry(txn_id, SchedulerProposal::CompletionAck);
        scheduler
            .registry
            .note_completion_ack(cluster_txn_id(txn_id), VSHARD);
        assert_eq!(scheduler.participant_progress(txn_id), None);

        scheduler.retry_owed_sequencer_entries();
        assert_eq!(proposer.attempt_count(), 1);
        assert!(scheduler.owed.is_empty());
    }

    /// A txn this node never seeded keeps its entry owed: a missing registry
    /// entry that was never seen does not settle it.
    #[tokio::test]
    async fn entry_for_an_unseeded_txn_stays_owed() {
        let (mut scheduler, _dir) = build_test_scheduler(VSHARD);
        let proposer = CapturingProposer::accepting();
        scheduler.sequencer_proposer = proposer.clone();
        let txn_id = TxnId::new(23, 0);

        scheduler.propose_sequencer_entry(txn_id, SchedulerProposal::OllpMismatch);
        scheduler.retry_owed_sequencer_entries();
        scheduler.retry_owed_sequencer_entries();

        assert_eq!(proposer.attempt_count(), 2);
        assert_eq!(scheduler.owed.len(), 1);
    }

    /// The run loop's stall tick drives the retry.
    #[tokio::test]
    async fn run_loop_reproposes_a_refused_entry_on_the_stall_tick() {
        let (mut scheduler, _dir) = build_test_scheduler(VSHARD);
        let proposer = CapturingProposer::failing_first(1);
        scheduler.sequencer_proposer = proposer.clone();
        scheduler.propose_sequencer_entry(
            TxnId::new(24, 0),
            SchedulerProposal::RoutingFailed {
                detail: "unroutable".to_string(),
            },
        );
        assert!(proposer.accepted().is_empty());

        let running = spawn_scheduler_loop(scheduler);
        let accepted = tokio::time::timeout(Duration::from_secs(5), async {
            while proposer.accepted().is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        running.stop().await;

        assert!(accepted.is_ok(), "the stall tick proposes the entry again");
    }
}
