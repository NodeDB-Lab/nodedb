// SPDX-License-Identifier: BUSL-1.1

//! Propose a staged txn's commit vote and park it on the verdict barrier.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::control::cluster::calvin::scheduler::driver::core::owed::SchedulerProposal;
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::driver::core::staged_vote::StagedVote;
use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Propose `vote` as this leader's commit vote for `txn_id`, then park
    /// the txn on the verdict barrier. `stage_error` is the text a vote
    /// other than a commit keeps for a COMMIT verdict's restage.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn cast_staged_vote(
        &mut self,
        txn_id: TxnId,
        vote: StagedVote,
        stage_error: Option<String>,
    ) {
        // Durably propose this leader's commit vote via the sequencer Raft
        // group. Only the leader stages, so its vote is the vShard's. The
        // sequencer aggregates every participant's vote into the single
        // global verdict this txn parks on below. An abort travels as
        // `AbortVote` so its cause and refusal survive to the coordinator.
        // The vote stays owed until the tally holds it, so a refused or dropped proposal is
        // proposed again rather than lost. A promoted leader's vote for a
        // txn whose vote applied already changes nothing: the first counts.
        self.propose_sequencer_entry(
            txn_id,
            SchedulerProposal::Vote {
                abort: vote.abort_cause(),
            },
        );

        if vote == StagedVote::SerializationConflict {
            // The staged slice's read-set was no longer current: observe it on
            // the node-global counter. A participant error never validated a
            // read-set, so it must not count here.
            self.shared
                .calvin
                .counters
                .read_set_validation_failures
                .fetch_add(1, Ordering::Relaxed);
        }

        // PARK on the barrier: transition to `AwaitingVerdict` and arm the stall
        // deadline. Do NOT dispatch resolve/drop here — the GLOBAL verdict, not
        // this local vote, decides. If the txn already vanished (torn down
        // elsewhere), there is nothing to park.
        //
        // A stage error parks too. The leader votes abort, and every
        // participant drops at the abort verdict. A COMMIT verdict for it
        // can follow only a vote an earlier leader cast: `resume_on_verdict`
        // restages the txn.
        match self.pending.get_mut(&txn_id) {
            Some(pending) => {
                // The frozen `RETURNING` replies stay on the txn while it
                // parks: `pending.returning.frozen`, keyed by plan index.
                let frozen = &pending.returning.frozen;
                if !frozen.is_empty() {
                    tracing::debug!(
                        vshard_id = self.vshard_id,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        plans = frozen.len(),
                        image_rows = pending.returning.row_count(),
                        rows = frozen
                            .values()
                            .map(|reply| reply.shaped.rows.len())
                            .sum::<usize>(),
                        payload_bytes = frozen
                            .values()
                            .map(|reply| reply.payload.len())
                            .sum::<usize>(),
                        sequence_effects = frozen
                            .values()
                            .map(|reply| reply.sequence_effects.len())
                            .sum::<usize>(),
                        "calvin: frozen RETURNING replies held for the verdict"
                    );
                }
                pending.commit_state = CommitState::AwaitingVerdict;
                pending.stage_error = stage_error;
                // no-determinism: local stall-warning deadline only; the global replicated verdict, not this wall-clock, decides commit/abort.
                pending.verdict_deadline = Some(Instant::now() + self.config.verdict_stall_warn());
            }
            None => return,
        }

        // PROBE on park (correctness backstop): the verdict can already be
        // durable — on replay, or a push that raced ahead of this park. Resume
        // immediately if so; the double-resume guard in `resume_on_verdict`
        // makes a later duplicate push/probe a no-op.
        if let Some(verdict) = self.registry.verdict(nodedb_cluster::calvin::TxnId::new(
            txn_id.epoch,
            txn_id.position,
        )) {
            self.resume_on_verdict(txn_id, verdict);
        }
    }
}
