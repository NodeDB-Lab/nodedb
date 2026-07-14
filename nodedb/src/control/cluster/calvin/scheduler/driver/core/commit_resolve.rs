// SPDX-License-Identifier: BUSL-1.1

//! Verdict-driven commit resolution for staged static Calvin transactions.
//!
//! A static Calvin dispatch STAGES its transaction on the Data Plane (validate
//! the read-set + buffer the plans, no base mutation). Its executor response
//! carries the local commit vote on `read_set_valid`. This module drives the
//! final step: dispatch a flush (commit, after `commit_redo` has WAL-appended
//! the resolved `TransactionRedo`) or drop (abort) of the staged buffer, wait
//! for its response, then run the commit tail (deposit applied result, record
//! write versions — plus a `CalvinApplied` WAL fallback when no redo record
//! was appended — propose `CompletionAck`) for a flush, or ack-only for a
//! drop.

use std::sync::atomic::Ordering;
use std::time::Instant;

use nodedb_cluster::calvin::{SequencerEntry, VerdictSignal};

use super::super::types::CommitState;
use super::scheduler::Scheduler;
use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

impl Scheduler {
    /// Cast this participant's local commit vote for a staged transaction, then
    /// PARK it on the cross-shard commit barrier awaiting the durable GLOBAL
    /// verdict — it does NOT self-decide flush-or-drop on its local vote.
    ///
    /// The staged executor response is validate-only: its `read_set_valid` is
    /// this shard's local commit vote (`Some(true)` => commit, `Some(false)` =>
    /// abort; a `None` from the active/dependent path is treated as commit). The
    /// leader proposes that vote via the sequencer Raft group; the sequencer
    /// aggregates all participants' votes into a single authoritative
    /// `SequencerEntry::Verdict`, applied on every replica.
    ///
    /// This method moves the txn to [`CommitState::AwaitingVerdict`] WITHOUT
    /// dispatching a resolve or drop, then immediately probes
    /// `registry.verdict(txn)`: if the verdict is already durable (replay, or a
    /// push we raced) it resumes at once via [`Self::resume_on_verdict`];
    /// otherwise it stays parked, holding locks and its staged buffer, until the
    /// verdict push, a later probe, or the stall re-probe sweep delivers the
    /// verdict. Resuming (in `resume_on_verdict`) is where the flush/drop is
    /// dispatched and the flushed/dropped counters bump — using the GLOBAL
    /// verdict, never the local vote.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resolve_staged_commit(
        &mut self,
        txn_id: TxnId,
        staged_response: &Response,
    ) {
        let committed = staged_response.read_set_valid != Some(false);

        // Durably propose this participant's commit vote via the sequencer
        // Raft group, leader-guarded like `OllpMismatch`: only the data-group
        // leader ran read-set validation, so only a leader's vote is
        // authoritative. The sequencer aggregates every participant's vote into
        // the single `SequencerEntry::Verdict` this txn parks on below.
        if self.is_group_leader() {
            self.propose_sequencer_entry(
                SequencerEntry::Vote {
                    epoch: txn_id.epoch,
                    position: txn_id.position,
                    vshard: self.vshard_id,
                    commit: committed,
                },
                txn_id,
                "commit vote",
            );
        }

        if !committed {
            // The staged slice's read-set was no longer current: observe it, the
            // same node-global signal the direct-apply path records.
            self.shared
                .calvin_counters
                .read_set_validation_failures
                .fetch_add(1, Ordering::Relaxed);
        }

        // PARK on the barrier: transition to `AwaitingVerdict` and arm the stall
        // deadline. Do NOT dispatch resolve/drop here — the GLOBAL verdict, not
        // this local vote, decides. If the txn already vanished (torn down
        // elsewhere), there is nothing to park.
        match self.pending.get_mut(&txn_id) {
            Some(pending) => {
                pending.commit_state = Some(CommitState::AwaitingVerdict);
                pending.verdict_deadline = Some(Instant::now() + self.config.verdict_stall_warn());
            }
            None => return,
        }

        // PROBE on park (correctness backstop): the verdict may already be
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

    /// Resume a txn parked in [`CommitState::AwaitingVerdict`] once the durable
    /// GLOBAL verdict is known: dispatch its flush (commit) or drop (abort).
    ///
    /// `committed` is the authoritative cross-shard verdict — NOT this shard's
    /// local vote. On commit, dispatches `MetaOp::CalvinResolve` and moves the
    /// txn to [`CommitState::AwaitingRedoResolve`] (the resolved redo is
    /// WAL-appended and the flush dispatched from [`Self::finish_redo_resolve`]).
    /// On abort, dispatches the drop directly and moves the txn to
    /// [`CommitState::AwaitingResolve`]. Bumps the flushed / dropped counter. The
    /// commit tail runs later in [`Self::finish_resolved_commit`], once the
    /// flush/drop response arrives.
    ///
    /// Double-resume guard: the verdict push and the probe-on-park (and the
    /// stall re-probe sweep) can all fire for one txn, so this first confirms the
    /// txn is still `Some(AwaitingVerdict)` — if it already transitioned out
    /// (resolve/drop dispatched, or completed), this is a no-op. This guarantees
    /// the flush/drop is dispatched exactly once.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resume_on_verdict(
        &mut self,
        txn_id: TxnId,
        committed: bool,
    ) {
        // Guard: only a still-parked txn resumes. Mirrors `handle_completion`'s
        // state-match so a duplicate push/probe/timeout is idempotent.
        if !matches!(
            self.pending.get(&txn_id).and_then(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        ) {
            return;
        }

        let dispatched = if committed {
            // Resolve the staged post-images into a replayable `RedoRecord`
            // first; the redo is WAL-appended (in `finish_redo_resolve`) before
            // the flush is dispatched, restoring restart durability for this
            // vShard's slice of a multi-shard Calvin commit.
            self.dispatch_calvin_resolve(txn_id)
        } else {
            self.dispatch_commit_resolution(txn_id, false, None)
        };
        if !dispatched {
            // Resolve/drop dispatch failed: complete the txn as an infra error so
            // its locks release and the epoch advances rather than stalling. The
            // staged buffer is reclaimed by a later drop or on core teardown.
            self.metrics.record_executor_error();
            self.metrics
                .record_infra_abort(infra_abort_reason::IO_ERROR);
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
            return;
        }

        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.commit_state = Some(if committed {
                CommitState::AwaitingRedoResolve
            } else {
                CommitState::AwaitingResolve {
                    committed: false,
                    redo_lsn: None,
                }
            });
            // No longer parked: clear the stall deadline.
            pending.verdict_deadline = None;
        }

        if committed {
            self.shared
                .calvin_counters
                .commits_flushed
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.shared
                .calvin_counters
                .commits_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Handle a pushed [`VerdictSignal`] from this node's completion registry.
    ///
    /// Matches the signal to the parked txn by `(epoch, position)` and resumes
    /// it. A signal for a txn this scheduler does not host, or one that already
    /// resumed, is a harmless no-op (the double-resume guard covers the latter).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn handle_verdict_signal(
        &mut self,
        signal: VerdictSignal,
    ) {
        let txn_id = TxnId::new(signal.epoch, signal.position);
        self.resume_on_verdict(txn_id, signal.commit);
    }

    /// Sweep parked `AwaitingVerdict` txns whose stall deadline has passed.
    ///
    /// For each stalled txn, RE-PROBE the durable verdict: if it is now known,
    /// resume (a push we dropped on a full channel, or a verdict that landed
    /// after the last probe). If it is STILL unknown, KEEP WAITING — hold locks,
    /// emit a stall metric + warning, and re-arm the deadline so the warning is
    /// rate-limited rather than per-iteration. It NEVER releases locks and NEVER
    /// unilaterally aborts: a participant cannot know whether a peer already
    /// flushed a COMMIT, so aborting one side while a peer committed would tear
    /// the transaction. The verdict is guaranteed to arrive eventually — a
    /// post-failover leader re-aggregates the replicated votes (seeded on every
    /// replica) into the same verdict — so waiting is always the safe action.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn check_awaiting_verdict_stalls(
        &mut self,
    ) {
        let now = Instant::now();
        let stalled: Vec<TxnId> = self
            .pending
            .iter()
            .filter(|(_, p)| matches!(p.commit_state, Some(CommitState::AwaitingVerdict)))
            .filter(|(_, p)| p.verdict_deadline.is_some_and(|d| now >= d))
            .map(|(id, _)| *id)
            .collect();

        for txn_id in stalled {
            if let Some(verdict) = self.registry.verdict(nodedb_cluster::calvin::TxnId::new(
                txn_id.epoch,
                txn_id.position,
            )) {
                self.resume_on_verdict(txn_id, verdict);
                continue;
            }

            // Verdict still unknown: keep waiting, hold locks, never abort.
            self.metrics.record_verdict_stall();
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: staged txn still awaiting the cross-shard verdict past its stall \
                 deadline; HOLDING locks and waiting (never aborting — a peer may have already \
                 flushed a commit). The verdict is guaranteed to arrive."
            );
            if let Some(pending) = self.pending.get_mut(&txn_id) {
                pending.verdict_deadline = Some(now + self.config.verdict_stall_warn());
            }
        }
    }

    /// Run the commit tail once a flush/drop response has returned.
    ///
    /// On a successful flush the full commit tail runs (deposit applied result,
    /// `CalvinApplied` WAL + write-version recording, `CompletionAck`). On a
    /// successful drop only the `CompletionAck` is proposed — the coordinator's
    /// completion waiter still fires and the epoch advances, but nothing was
    /// written so there is no result to deposit, no apply LSN, and no versions
    /// to record. A non-`Ok` resolve response is treated as an executor error.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn finish_resolved_commit(
        &mut self,
        txn_id: TxnId,
        response: Response,
        committed: bool,
        redo_lsn: Option<crate::types::Lsn>,
    ) {
        if response.status == Status::Ok {
            if committed {
                self.commit_apply_tail(txn_id, response, redo_lsn);
            } else {
                self.propose_sequencer_entry(
                    SequencerEntry::CompletionAck {
                        epoch: txn_id.epoch,
                        position: txn_id.position,
                        vshard_id: self.vshard_id,
                    },
                    txn_id,
                    "completion ack (dropped)",
                );
            }
        } else {
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                committed,
                "calvin: flush/drop response was not Ok; locks NOT released (shard degraded)"
            );
            self.metrics.record_executor_error();
            self.metrics
                .record_infra_abort(infra_abort_reason::IO_ERROR);
        }

        self.metrics.record_completed();
        self.on_txn_complete(txn_id);
    }

    /// Deposit the applied result, durably mark the apply, record the apply's
    /// write versions, and propose the `CompletionAck`.
    ///
    /// Shared by the flush-completion path and the direct-apply (dependent /
    /// active) apply path.
    ///
    /// `redo_lsn` is `Some(lsn)` when a `TransactionRedo` record was already
    /// WAL-appended for this commit's non-empty write set (`finish_redo_resolve`)
    /// — that record already IS the durable applied marker, so only write
    /// versions are recorded at it. `None` (a drop, an empty-ops staged commit,
    /// or the direct-apply dependent/active path, which carries no redo record)
    /// falls back to appending a `CalvinApplied` marker here, exactly as before
    /// this record existed.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn commit_apply_tail(
        &mut self,
        txn_id: TxnId,
        response: Response,
        redo_lsn: Option<crate::types::Lsn>,
    ) {
        // Deposit the FULL applied Response (affected-count + watermark + any
        // RETURNING rows) into the local sidecar BEFORE proposing the replicated
        // CompletionAck. The ack fires the coordinator's completion oneshot on
        // every sequencer member, so depositing first guarantees the result is
        // present by the time the coordinator drains it — no lost result, no
        // race.
        //
        // Gated on the PRIMARY-WRITE participant: any participant whose slice
        // carries the user's non-edge DML (Document/KV/Vector/etc.), as opposed
        // to the implicit graph-edge cleanup that dual-homes alongside it. A
        // multi-collection cross-shard COMMIT has MANY primary-write
        // participants — each a plain affected-count write — and they coalesce:
        // the first applied response stands for the coordinator (which discards
        // it for a COMMIT tag anyway), and the plain-write siblings do not
        // conflict. Only a genuine cross-shard RETURNING union — two
        // participants each carrying RETURNING rows — records `Conflict`.
        // Results travel via this in-process sidecar only — never the sequencer
        // Raft log.
        let (has_primary_write, has_returning) = self
            .pending
            .get(&txn_id)
            .map(|p| (p.has_primary_write, p.has_returning))
            .unwrap_or((false, false));
        if has_primary_write {
            use std::collections::hash_map::Entry;

            use crate::control::state::CalvinApplyResult;

            let key = nodedb_cluster::calvin::TxnId::new(txn_id.epoch, txn_id.position);
            let mut results = self
                .shared
                .calvin_apply_results
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            match results.entry(key) {
                Entry::Vacant(slot) => {
                    slot.insert(CalvinApplyResult::Single {
                        response,
                        has_returning,
                    });
                }
                Entry::Occupied(mut slot) => {
                    // Derive both facts from the existing entry BEFORE any
                    // insert, so the immutable borrow does not outlive the
                    // mutable one.
                    let existing_returning = matches!(
                        slot.get(),
                        CalvinApplyResult::Single {
                            has_returning: true,
                            ..
                        }
                    );
                    let already_conflict = matches!(slot.get(), CalvinApplyResult::Conflict);

                    if already_conflict {
                        // A RETURNING union was already recorded; stays Conflict.
                    } else if has_returning && existing_returning {
                        // Two RETURNING-bearing participants for one Calvin txn:
                        // a cross-shard RETURNING union, which is unsupported.
                        // Record Conflict so the coordinator fails the statement
                        // loudly rather than returning one shard's partial rows.
                        tracing::error!(
                            epoch = txn_id.epoch,
                            position = txn_id.position,
                            vshard = self.vshard_id,
                            "two RETURNING-bearing participants for one Calvin txn — cross-shard \
                             RETURNING union unsupported"
                        );
                        slot.insert(CalvinApplyResult::Conflict);
                    } else if has_returning {
                        // The incoming participant carries the rows; the existing
                        // entry was a plain affected-count sibling. Rows win.
                        slot.insert(CalvinApplyResult::Single {
                            response,
                            has_returning: true,
                        });
                    } else {
                        // Incoming is a plain write; keep the existing entry — a
                        // multi-collection cross-shard COMMIT coalesces (the
                        // coordinator discards it for a COMMIT tag anyway).
                    }
                }
            }
        }
        match redo_lsn {
            // The TransactionRedo record already durably marks this apply — the
            // SAME shard-local WAL-LSN space fast-path writes and read
            // watermarks use. Record the apply's per-key write versions at it;
            // no second (CalvinApplied) marker is written.
            Some(lsn) => self.record_calvin_write_versions(txn_id, lsn),
            None => match self.shared.wal.append_calvin_applied(
                crate::types::VShardId::new(self.vshard_id),
                txn_id.epoch,
                txn_id.position,
            ) {
                // The CalvinApplied WAL LSN is the committed write-LSN for this
                // apply — the SAME shard-local WAL-LSN space fast-path writes and
                // read watermarks use. Record the apply's per-key write versions
                // at it now that it exists (it did not at dispatch time).
                Ok(applied_lsn) => self.record_calvin_write_versions(txn_id, applied_lsn),
                Err(e) => {
                    tracing::error!(
                        vshard_id = self.vshard_id,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        error = %e,
                        "calvin: failed to write CalvinApplied WAL record"
                    );
                }
            },
        }
        self.propose_sequencer_entry(
            SequencerEntry::CompletionAck {
                epoch: txn_id.epoch,
                position: txn_id.position,
                vshard_id: self.vshard_id,
            },
            txn_id,
            "completion ack",
        );
    }

    /// Dispatch a flush (`committed = true`) or drop (`committed = false`) of a
    /// staged transaction's commit-pending buffer back to its core, registering
    /// a response bridge so the resolve response re-enters the completion loop.
    ///
    /// `wal_lsn` is the just-appended `TransactionRedo` LSN for a flush whose
    /// resolved redo carried ops (`Some`); `None` for a drop, or a flush whose
    /// resolved redo was empty (falls back to the `CalvinApplied` marker in
    /// `commit_apply_tail`).
    ///
    /// Returns `false` if the dispatch failed (the caller then completes the txn
    /// as an infra error). Mirrors the exempt dispatch shape of the
    /// static/active dispatch and the write-version record op.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_commit_resolution(
        &mut self,
        txn_id: TxnId,
        committed: bool,
        wal_lsn: Option<crate::types::Lsn>,
    ) -> bool {
        let Some(pending) = self.pending.get(&txn_id) else {
            return false;
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let epoch = txn_id.epoch;
        let position = txn_id.position;

        let plan = if committed {
            PhysicalPlan::Meta(MetaOp::CalvinFlush { epoch, position })
        } else {
            PhysicalPlan::Meta(MetaOp::CalvinDrop { epoch, position })
        };

        let request_id = self.next_request_id();
        // The redo record (if any) was already WAL-appended by the caller
        // before this dispatch — see `finish_redo_resolve` — so its LSN
        // rides on the envelope here rather than being allocated post-apply.
        let request = self.build_exempt_request(request_id, tenant_id, plan, wal_lsn);

        let resp_rx = self.shared.tracker.register(request_id);
        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };
        if let Err(e) = dispatch_result {
            self.shared.tracker.cancel(&request_id);
            tracing::error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                committed,
                error = %e,
                "calvin: flush/drop dispatch failed"
            );
            return false;
        }

        // The resolve response re-enters the completion loop under the SAME
        // txn_id, now in `AwaitingResolve`, where `finish_resolved_commit` runs.
        self.spawn_response_bridge(txn_id, request_id, resp_rx);
        true
    }
}
