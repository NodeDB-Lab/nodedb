// SPDX-License-Identifier: BUSL-1.1

//! Dependent-read barrier entries and the barrier's decision.
//!
//! The data-group apply loop folds every `CalvinReadResult` and
//! `CalvinReadTimeout` entry into the txn's stored barrier row and this
//! vShard's read-result buffer. The scheduler takes a txn's entries into its
//! `barrier_logs` once it holds the txn: at the grant, and on each push for a
//! txn it holds. A txn the buffer holds as stored reads its row. A barrier then
//! decides from the log alone (see
//! [`crate::control::cluster::calvin::scheduler::BarrierLog`]), so every
//! replica that leads the vShard decides it alike:
//!
//! - complete: the leader checks the values against the ones the coordinator
//!   read, and stages the active slice or votes `PredictionDrift`;
//! - timed out: the leader votes abort;
//! - waiting: the barrier stays open, unless an abort verdict is stored.
//!
//! A follower keeps the log of each dependent txn it holds. A promotion
//! opens the barrier from it and reaches the decision the earlier leader
//! reached.

use std::sync::Arc;

use tracing::{error, info, warn};

use nodedb_cluster::calvin::AbortReason;

use super::super::barrier::{PendingDependentBarrier, expected_reads_drift};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::barrier_store;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::{BarrierLog, BarrierOutcome, BufferedEvents};

impl Scheduler {
    /// Take the buffered barrier entries of every dependent txn this
    /// scheduler holds, and decide each open barrier they reach.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn drain_read_results(
        &mut self,
    ) {
        let buffer = Arc::clone(&self.read_results);
        let taken = buffer.take_held(|txn_id| self.holds_barrier_txn(txn_id));
        for (txn_id, events) in taken {
            if self.absorb(txn_id, events) {
                self.decide_barrier(txn_id);
            }
        }
    }

    /// Take the buffered barrier entries of `txn_id`, which this scheduler
    /// holds from now on.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn absorb_buffered_reads(
        &mut self,
        txn_id: TxnId,
    ) {
        if let Some(events) = self.read_results.take(txn_id) {
            self.absorb(txn_id, events);
        }
    }

    /// Fold `events` of `txn_id` into its barrier log. A stored txn's
    /// events are its row's, extended with the ones memory held. Returns
    /// whether the log took them.
    ///
    /// A row this node cannot read goes back to the buffer, still stored:
    /// the barrier stays open, and the next pass reads the row again.
    fn absorb(&mut self, txn_id: TxnId, events: BufferedEvents) -> bool {
        let mut log = if events.stored {
            match barrier_store::load_log(self.shared.credentials.catalog(), self.vshard_id, txn_id)
            {
                Ok(stored) => stored,
                Err(e) => {
                    error!(
                        vshard_id = self.vshard_id,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        error = %e,
                        "calvin: the barrier row of a held txn does not load; reading it again \
                         on the next pass"
                    );
                    crate::diag::calvin_barrier_log_store_failed(
                        self.vshard_id,
                        Some((txn_id.epoch, txn_id.position)),
                        "load",
                        &e,
                    );
                    self.read_results.restore(txn_id, events);
                    return false;
                }
            }
        } else {
            BarrierLog::default()
        };
        log.extend(events.memory);
        self.barrier_logs.entry(txn_id).or_default().extend(log);
        true
    }

    /// Whether `txn_id` is a dependent txn this scheduler holds: at a
    /// barrier, or in `pending`.
    fn holds_barrier_txn(&self, txn_id: TxnId) -> bool {
        self.dependent_barrier.contains_key(&txn_id)
            || self
                .pending
                .get(&txn_id)
                .is_some_and(|pending| pending.txn.tx_class.dependent_reads.is_some())
    }

    /// Open `barrier` for `txn_id` and decide it from the entries the log
    /// holds already.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn open_barrier(
        &mut self,
        txn_id: TxnId,
        barrier: PendingDependentBarrier,
    ) {
        self.dependent_barrier.insert(txn_id, barrier);
        self.absorb_buffered_reads(txn_id);
        self.decide_barrier(txn_id);
    }

    /// Decide the open barrier of `txn_id` from its log.
    fn decide_barrier(&mut self, txn_id: TxnId) {
        let Some(barrier) = self.dependent_barrier.get(&txn_id) else {
            return;
        };
        let empty = BarrierLog::default();
        let log = self.barrier_logs.get(&txn_id).unwrap_or(&empty);
        match log.outcome(&barrier.passive) {
            BarrierOutcome::Complete => self.complete_barrier(txn_id),
            BarrierOutcome::TimedOut => {
                let missing = log.missing(&barrier.passive);
                warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    ?missing,
                    "calvin: the log timed out the dependent-read barrier; voting abort"
                );
                self.metrics.record_executor_error();
                self.metrics.record_infra_abort(
                    crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::PASSIVE_PARTICIPANT_TIMEOUT,
                );
                self.abort_open_barrier(
                    txn_id,
                    AbortReason::ParticipantError,
                    format!(
                        "the dependent-read barrier timed out in the log before the read \
                         results of passive vShards {missing:?}"
                    ),
                );
            }
            BarrierOutcome::Waiting => {
                // An abort verdict is stored already: a peer voted abort. The
                // txn stages nothing here, so its locks free at once.
                let verdict = self.registry.verdict(nodedb_cluster::calvin::TxnId::new(
                    txn_id.epoch,
                    txn_id.position,
                ));
                if verdict == Some(false) {
                    self.abort_open_barrier(
                        txn_id,
                        AbortReason::ParticipantError,
                        "an abort verdict reached the txn's open dependent-read barrier".to_owned(),
                    );
                }
            }
        }
    }

    /// Close the complete barrier of `txn_id`: stage the active slice with
    /// the read values, or vote `PredictionDrift` when a value differs from
    /// the one the coordinator read.
    fn complete_barrier(&mut self, txn_id: TxnId) {
        let Some(barrier) = self.dependent_barrier.remove(&txn_id) else {
            return;
        };
        let injected = self
            .barrier_logs
            .get(&txn_id)
            .map(BarrierLog::injected_reads)
            .unwrap_or_default();
        let drift = barrier
            .txn
            .tx_class
            .dependent_reads
            .as_ref()
            .and_then(|spec| expected_reads_drift(spec, &injected));
        if let Some(detail) = drift {
            info!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                %detail,
                "calvin: a passive read moved since the coordinator read it; voting prediction drift"
            );
            self.park_unstaged(
                barrier.txn,
                txn_id,
                barrier.lock_owner,
                AbortReason::PredictionDrift,
                detail,
            );
            return;
        }
        self.dispatch_active_txn(barrier.txn, txn_id, barrier.lock_owner, injected);
    }

    /// Close the open barrier of `txn_id` with an abort vote for `reason`.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn abort_open_barrier(
        &mut self,
        txn_id: TxnId,
        reason: AbortReason,
        detail: String,
    ) {
        let Some(barrier) = self.dependent_barrier.remove(&txn_id) else {
            return;
        };
        self.park_unstaged(barrier.txn, txn_id, barrier.lock_owner, reason, detail);
    }

    /// Hand the barrier log of every txn this scheduler did not finish back
    /// to the vShard's read-result buffer. A stopping scheduler calls it, so
    /// the next scheduler of the vShard starts from the same entries.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn return_barrier_logs(
        &mut self,
    ) {
        for (txn_id, log) in std::mem::take(&mut self.barrier_logs) {
            if !self.ledger.is_applied(txn_id.epoch, txn_id.position) {
                self.read_results.restore(
                    txn_id,
                    BufferedEvents {
                        memory: log,
                        stored: false,
                    },
                );
            }
        }
    }

    /// Remove every stored barrier row of this vShard whose txn finished.
    /// A failed pass keeps the rows, and the next stall tick retries.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn sweep_finished_barrier_rows(
        &self,
    ) {
        if let Err(e) = barrier_store::sweep_finished(
            self.shared.credentials.catalog(),
            self.vshard_id,
            &self.ledger,
        ) {
            warn!(
                vshard_id = self.vshard_id,
                error = %e,
                "calvin: the barrier rows of finished txns were not removed; retrying on \
                 the next stall tick"
            );
            crate::diag::calvin_barrier_log_store_failed(self.vshard_id, None, "remove", &e);
        }
    }

    /// Drop every barrier entry of `txn_id`, which finished here: the
    /// buffered ones, and its stored row.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn forget_barrier_entries(
        &mut self,
        txn_id: TxnId,
        held_log: bool,
    ) {
        let buffered = self.read_results.forget_txn(txn_id);
        if !held_log && !buffered {
            return;
        }
        // A failed remove leaves the row of a finished txn. No barrier reads
        // it, and the stall tick's sweep removes it.
        if let Err(e) =
            barrier_store::remove_log(self.shared.credentials.catalog(), self.vshard_id, txn_id)
        {
            warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                error = %e,
                "calvin: the barrier row of a finished txn was not removed"
            );
            crate::diag::calvin_barrier_log_store_failed(
                self.vshard_id,
                Some((txn_id.epoch, txn_id.position)),
                "remove",
                &e,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nodedb_cluster::calvin::types::{DependentReadSpec, SchedulerInput, SequencedTxn};
    use nodedb_cluster::calvin::{
        AbortReason, CalvinCompletionRegistry, SequencerEntry, VerdictOutcome,
    };
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_physical::physical_plan::meta::{MetaOp, PassiveReadKeyId};
    use nodedb_types::{QualifiedCollection, Value};

    use super::*;
    use crate::bridge::dispatch::CoreChannelDataSide;
    use crate::control::cluster::calvin::scheduler::BarrierEvent;
    use crate::control::cluster::calvin::scheduler::driver::core::process::LedgerMark;
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        CapturingProposer, lead_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_local_write_txn, test_coll_vshard,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;

    /// The passive vShard of every test txn: not the scheduler's own.
    const PASSIVE: u32 = 9;

    fn row() -> PassiveReadKeyId {
        PassiveReadKeyId::kv(
            QualifiedCollection::from_stored("items".to_owned()),
            b"alice:sword".to_vec(),
        )
    }

    /// A dependent txn that writes `test_coll` and reads `row()` on
    /// [`PASSIVE`], whose coordinator read `expected` for it.
    fn dependent_txn(epoch: u64, expected: Option<&[u8]>) -> SequencedTxn {
        let mut txn = make_local_write_txn(epoch, 0);
        txn.tx_class.dependent_reads = Some(DependentReadSpec {
            passive_reads: BTreeMap::from([(PASSIVE, Vec::new())]),
            expected: BTreeMap::from([(row(), expected.map(<[u8]>::to_vec))]),
        });
        txn
    }

    fn read(value: &[u8]) -> BarrierEvent {
        BarrierEvent::Read {
            passive_vshard: PASSIVE,
            values: vec![(row(), Value::Bytes(value.to_vec()))],
        }
    }

    /// A scheduler on the `test_coll` vShard with a capturing proposer. Not
    /// a leader until the test makes it one.
    fn scheduler() -> (
        Scheduler,
        tempfile::TempDir,
        CoreChannelDataSide,
        Arc<CapturingProposer>,
        Arc<CalvinCompletionRegistry>,
    ) {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, dir, data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), Arc::clone(&registry));
        let proposer = CapturingProposer::accepting();
        scheduler.sequencer_proposer = proposer.clone();
        (scheduler, dir, data_side, proposer, registry)
    }

    fn buffer(scheduler: &Scheduler, epoch: u64, event: BarrierEvent) {
        scheduler
            .read_results
            .push(TxnId::new(epoch, 0), event, 64, true);
    }

    fn grant(scheduler: &mut Scheduler, txn: SequencedTxn) {
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(txn)));
    }

    /// Whether the Data Plane received the active stage of `(epoch, 0)`.
    fn staged_active(data_side: &mut CoreChannelDataSide, epoch: u64) -> bool {
        let mut staged = false;
        while let Ok(request) = data_side.request_rx.try_pop() {
            staged |= matches!(
                request.inner.plan,
                PhysicalPlan::Meta(MetaOp::CalvinExecuteActive { epoch: e, position: 0, .. })
                    if e == epoch
            );
        }
        staged
    }

    fn abort_votes(proposer: &CapturingProposer) -> Vec<AbortReason> {
        proposer
            .accepted()
            .into_iter()
            .filter_map(|entry| match entry {
                SequencerEntry::AbortVote { reason, .. } => Some(reason),
                _ => None,
            })
            .collect()
    }

    /// A read result the log applied before this node granted the txn waits
    /// in the buffer. The grant opens the barrier from it, and the active
    /// slice stages at once.
    #[tokio::test]
    async fn a_result_buffered_before_the_grant_completes_the_barrier() {
        let (mut scheduler, _dir, mut data_side, proposer, _registry) = scheduler();
        lead_data_group(&mut scheduler);
        buffer(&scheduler, 50, read(b"v1"));

        grant(&mut scheduler, dependent_txn(50, Some(b"v1")));

        assert!(staged_active(&mut data_side, 50));
        assert!(scheduler.dependent_barrier.is_empty());
        assert_eq!(scheduler.read_results.waiting_txns(), 0);
        assert_eq!(
            scheduler
                .pending
                .get(&TxnId::new(50, 0))
                .map(|p| p.commit_state),
            Some(CommitState::Staged)
        );
        assert!(abort_votes(&proposer).is_empty());
    }

    /// A restarted node finds a txn's result in its stored row alone. The
    /// grant opens the barrier from the row, the active slice stages, and
    /// the txn's finish removes the row.
    #[tokio::test]
    async fn a_stored_result_completes_the_barrier_after_a_restart() {
        let (mut scheduler, _dir, mut data_side, proposer, _registry) = scheduler();
        lead_data_group(&mut scheduler);
        let txn_id = TxnId::new(51, 0);
        let catalog = scheduler.shared.credentials.catalog();
        barrier_store::save_event(catalog, scheduler.vshard_id, txn_id, &read(b"v1"))
            .expect("store the result");
        scheduler.read_results.mark_stored([txn_id]);

        grant(&mut scheduler, dependent_txn(51, Some(b"v1")));

        assert!(staged_active(&mut data_side, 51));
        assert!(scheduler.dependent_barrier.is_empty());
        assert_eq!(scheduler.read_results.waiting_txns(), 0);
        assert!(abort_votes(&proposer).is_empty());

        let lock_owner = scheduler
            .pending
            .get(&txn_id)
            .map(|pending| pending.lock_owner)
            .expect("the staged txn is pending");
        scheduler.release_and_mark_applied(txn_id, lock_owner, LedgerMark::ByApply);
        assert!(
            scheduler
                .shared
                .credentials
                .catalog()
                .load_calvin_barrier_log(scheduler.vshard_id, 51, 0)
                .expect("load")
                .is_none(),
            "the finished txn's row is removed"
        );
    }

    /// A result pushed while the barrier is open completes it on the drain.
    #[tokio::test]
    async fn a_result_after_the_grant_completes_the_open_barrier() {
        let (mut scheduler, _dir, mut data_side, _proposer, _registry) = scheduler();
        lead_data_group(&mut scheduler);
        grant(&mut scheduler, dependent_txn(51, Some(b"v1")));
        assert!(scheduler.dependent_barrier.contains_key(&TxnId::new(51, 0)));

        buffer(&scheduler, 51, read(b"v1"));
        scheduler.drain_read_results();

        assert!(staged_active(&mut data_side, 51));
        assert!(scheduler.dependent_barrier.is_empty());
    }

    /// The timeout entry above every result changes nothing: the barrier
    /// completed in the log first.
    #[tokio::test]
    async fn a_timeout_after_every_result_is_ignored() {
        let (mut scheduler, _dir, mut data_side, proposer, _registry) = scheduler();
        lead_data_group(&mut scheduler);
        buffer(&scheduler, 52, read(b"v1"));
        buffer(&scheduler, 52, BarrierEvent::Timeout);

        grant(&mut scheduler, dependent_txn(52, Some(b"v1")));

        assert!(staged_active(&mut data_side, 52));
        assert!(abort_votes(&proposer).is_empty());
    }

    /// A timeout entry below a result times the barrier out: the leader
    /// votes abort, and the late result does not count.
    #[tokio::test]
    async fn a_timeout_before_the_result_votes_abort() {
        let (mut scheduler, _dir, mut data_side, proposer, _registry) = scheduler();
        lead_data_group(&mut scheduler);
        grant(&mut scheduler, dependent_txn(53, Some(b"v1")));

        buffer(&scheduler, 53, BarrierEvent::Timeout);
        buffer(&scheduler, 53, read(b"v1"));
        scheduler.drain_read_results();

        assert!(!staged_active(&mut data_side, 53));
        assert_eq!(
            scheduler
                .pending
                .get(&TxnId::new(53, 0))
                .map(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        );
        assert_eq!(abort_votes(&proposer), vec![AbortReason::ParticipantError]);
        assert!(!scheduler.applied.is_applied(53, 0));
    }

    /// A value that moved since the coordinator read it votes
    /// `PredictionDrift` and stages nothing.
    #[tokio::test]
    async fn a_moved_value_votes_prediction_drift() {
        let (mut scheduler, _dir, mut data_side, proposer, _registry) = scheduler();
        lead_data_group(&mut scheduler);
        buffer(&scheduler, 54, read(b"v2"));

        grant(&mut scheduler, dependent_txn(54, Some(b"v1")));

        assert!(!staged_active(&mut data_side, 54));
        assert_eq!(abort_votes(&proposer), vec![AbortReason::PredictionDrift]);
    }

    /// A follower holds the log of a dependent txn its leader committed. The
    /// result reached the follower before its grant, and the follower waited
    /// past any timeout of its own. Promoted, it opens the barrier from the
    /// log, stages the slice, and neither votes abort nor halts.
    #[tokio::test]
    async fn a_promoted_follower_stages_the_committed_txn_without_halting() {
        let (mut scheduler, _dir, mut data_side, proposer, registry) = scheduler();
        scheduler.config.dependent_read_passive_timeout_ms = 20;
        buffer(&scheduler, 55, read(b"v1"));
        grant(&mut scheduler, dependent_txn(55, Some(b"v1")));
        let txn_id = TxnId::new(55, 0);
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Following)
        );
        // The earlier leader voted commit and the verdict is stored.
        registry.note_verdict(
            nodedb_cluster::calvin::TxnId::new(55, 0),
            VerdictOutcome::Commit,
        );
        scheduler.resume_on_verdict(txn_id, true);
        // The leader's barrier timeout passes on the follower's clock.
        // no-determinism: test-only pause past the configured timeout.
        tokio::time::sleep(scheduler.config.passive_timeout()).await;

        lead_data_group(&mut scheduler);

        assert!(staged_active(&mut data_side, 55));
        assert!(scheduler.apply_halt().is_none());
        assert!(abort_votes(&proposer).is_empty());
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Staged)
        );
    }

    /// A follower whose log timed the barrier out votes abort once
    /// promoted: the decision the earlier leader reached from the same log.
    #[tokio::test]
    async fn a_promoted_follower_decides_a_logged_timeout_alike() {
        let (mut scheduler, _dir, mut data_side, proposer, _registry) = scheduler();
        grant(&mut scheduler, dependent_txn(56, Some(b"v1")));
        buffer(&scheduler, 56, BarrierEvent::Timeout);
        scheduler.drain_read_results();
        buffer(&scheduler, 56, read(b"v1"));
        scheduler.drain_read_results();

        lead_data_group(&mut scheduler);

        assert!(!staged_active(&mut data_side, 56));
        assert_eq!(abort_votes(&proposer), vec![AbortReason::ParticipantError]);
    }

    /// An abort verdict ends an open barrier: the txn drops with no stage.
    #[tokio::test]
    async fn an_abort_verdict_ends_an_open_barrier() {
        let (mut scheduler, _dir, mut data_side, _proposer, registry) = scheduler();
        lead_data_group(&mut scheduler);
        grant(&mut scheduler, dependent_txn(57, Some(b"v1")));
        let txn_id = TxnId::new(57, 0);
        registry.note_verdict(
            nodedb_cluster::calvin::TxnId::new(57, 0),
            VerdictOutcome::Abort(AbortReason::ParticipantError),
        );

        scheduler.resume_on_verdict(txn_id, false);

        assert!(scheduler.dependent_barrier.is_empty());
        assert!(!staged_active(&mut data_side, 57));
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingDrop)
        );
    }

    /// The leader proposes the barrier's timeout entry once its timeout
    /// passes, and decides nothing until the entry applies.
    #[tokio::test]
    async fn the_leader_proposes_a_due_timeout_and_waits_for_the_log() {
        let (mut scheduler, _dir, _data_side, proposer, _registry) = scheduler();
        lead_data_group(&mut scheduler);
        grant(&mut scheduler, dependent_txn(58, Some(b"v1")));
        let txn_id = TxnId::new(58, 0);
        let group_id = {
            let mr = scheduler
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            mr.routing()
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .group_for_vshard(scheduler.vshard_id)
                .expect("routed vShard")
        };
        let term_start = scheduler
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .term_start_index(group_id)
            .expect("term start");
        if let Some(barrier) = scheduler.dependent_barrier.get_mut(&txn_id) {
            // no-determinism: test-only deadline already passed.
            barrier.timeout_due = std::time::Instant::now() - std::time::Duration::from_millis(1);
        }

        scheduler.propose_due_read_timeouts();

        assert!(
            scheduler
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .log_term_at(group_id, term_start + 1)
                .is_some(),
            "the timeout entry is in the group's log"
        );
        let barrier = scheduler
            .dependent_barrier
            .get(&txn_id)
            .expect("the barrier stays open until the entry applies");
        assert!(barrier.timeout_due > std::time::Instant::now());
        assert!(abort_votes(&proposer).is_empty());
    }

    /// A stopping scheduler hands the log of an unfinished txn back to the
    /// buffer for the next scheduler of the vShard.
    #[tokio::test]
    async fn a_stopping_scheduler_hands_its_logs_back() {
        let (mut scheduler, _dir, _data_side, _proposer, _registry) = scheduler();
        grant(&mut scheduler, dependent_txn(59, Some(b"v1")));
        buffer(&scheduler, 59, read(b"v1"));
        scheduler.drain_read_results();
        assert_eq!(scheduler.read_results.waiting_txns(), 0);

        scheduler.return_barrier_logs();

        assert_eq!(scheduler.read_results.waiting_txns(), 1);
        assert!(scheduler.barrier_logs.is_empty());
    }
}
