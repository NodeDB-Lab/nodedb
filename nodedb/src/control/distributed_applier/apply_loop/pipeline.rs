// SPDX-License-Identifier: BUSL-1.1

//! The apply pipeline over every group this node applies.
//!
//! Each group's entries start in log order and settle in log order. A write
//! starts with its enqueue, and the next entry of its group waits for that
//! enqueue to return. Once enqueued, the write runs on its core and finishes
//! whenever its core answers. A write the core parks holds its own position
//! and no other: later entries of its group start and finish, and other
//! groups never wait on it. Only the group's applied index and durable floor
//! wait for it to finish.

use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};

use crate::control::distributed_applier::applier::ApplyBatch;
use crate::control::distributed_applier::proposal_ledger::ProposalLedger;
use crate::control::server::dispatch_utils::publish_settled_changes;
use crate::control::state::SharedState;

use super::bookkeeping::record_durable_apply;
use super::context::{ApplyContext, ApplyFuture, LoopEvent, LoopFuture, Started, StartedEntry};
use super::group_watch::GroupWatch;
use super::lane::{Lane, QueuedEntry, Slot, SlotState};
use super::metadata_floor::{HeldEntry, hold_for_metadata};
use super::proposal_gate::{EntryOutcome, ProposalGate};
use super::snapshot_gate::{EntryAdmission, admit_entry, hold_permit};
use super::start::{Prepared, prepare_entry};

/// Every group's lane, and the enqueues and applies that run.
pub(super) struct Pipeline<'a> {
    ctx: ApplyContext<'a>,
    lanes: HashMap<u64, Lane>,
    gate: ProposalGate,
    watch: GroupWatch,
    running: FuturesUnordered<LoopFuture<'a>>,
    /// Set by a pump that left an entry queued because a snapshot install
    /// held its group's apply gate.
    install_blocked: bool,
}

impl<'a> Pipeline<'a> {
    pub fn new(ctx: ApplyContext<'a>, ledger: ProposalLedger) -> Self {
        Self {
            ctx,
            lanes: HashMap::new(),
            gate: ProposalGate::new(ledger),
            watch: GroupWatch::with_cuts(ctx.state.pitr.recorded_cuts()),
            running: FuturesUnordered::new(),
            install_blocked: false,
        }
    }

    /// Whether the last pump left an entry queued behind a snapshot install.
    /// The loop pumps again once an install releases its gate.
    pub fn install_blocked(&self) -> bool {
        self.install_blocked
    }

    /// Queue a batch the applier handed off, behind its group's earlier
    /// entries.
    pub fn accept(&mut self, batch: ApplyBatch) {
        let lane = self
            .lanes
            .entry(batch.group_id)
            .or_insert_with(|| Lane::new(batch.group_id));
        let first = lane.backlog.len();
        lane.backlog
            .extend(batch.entries.into_iter().map(QueuedEntry::new));
        // Every handed-off entry is committed. A snapshot this node builds
        // carries their keys.
        self.ctx.tracker.note_committed(
            batch.group_id,
            lane.backlog
                .range(first..)
                .map(|queued| (queued.entry.index, queued.proposal_key())),
        );
    }

    /// Whether any enqueue or apply runs.
    pub fn has_running(&self) -> bool {
        !self.running.is_empty()
    }

    /// The next enqueue to return or apply to finish. `None` when nothing
    /// runs.
    pub async fn next_event(&mut self) -> Option<LoopEvent<'a>> {
        self.running.next().await
    }

    /// Handle `event`, then every other event that is ready already.
    pub fn handle(&mut self, event: LoopEvent<'a>) {
        self.handle_one(event);
        while let Some(Some(event)) = self.running.next().now_or_never() {
            self.handle_one(event);
        }
    }

    fn handle_one(&mut self, event: LoopEvent<'a>) {
        let (group_id, log_index, handled) = match event {
            LoopEvent::Enqueued {
                group_id,
                log_index,
                entry,
            } => (
                group_id,
                log_index,
                self.enqueued(group_id, log_index, entry),
            ),
            LoopEvent::Finished(finished) => {
                self.ctx
                    .tracker
                    .note_concluded(finished.group_id, finished.log_index);
                let gate = &mut self.gate;
                let wrote_rows = finished.outcome.wrote_rows();
                let handled = self.lanes.get_mut(&finished.group_id).is_some_and(|lane| {
                    lane.conclude(finished.log_index, wrote_rows, |proposal_key| {
                        gate.conclude(proposal_key, true, finished.outcome)
                    })
                });
                (finished.group_id, finished.log_index, handled)
            }
        };
        // The entry's enqueue returned, or its exclusive apply finished:
        // every entry of the group through it reached its core.
        release_admission_holds(self.ctx.state, group_id, log_index);
        if !handled {
            // Every enqueue and apply the pipeline runs has a slot in its
            // group's lane in the matching state: the pump pushes both
            // together and only this call moves them on.
            tracing::error!(
                group_id,
                log_index,
                "an apply-loop event has no matching entry in its group's lane"
            );
        }
    }

    fn enqueued(&mut self, group_id: u64, log_index: u64, entry: StartedEntry<'a>) -> bool {
        let StartedEntry {
            started,
            collection,
            user_write,
        } = entry;
        let Some(lane) = self.lanes.get_mut(&group_id) else {
            return false;
        };
        match started {
            Started::Running(apply) => {
                self.running.push(finished_event(apply));
                lane.enqueued(log_index, SlotState::Running, collection, user_write)
            }
            Started::Concluded(outcome) => {
                // The write concluded without reaching its core. It leaves
                // its enqueue and concludes in one step.
                self.ctx.tracker.note_concluded(group_id, log_index);
                let gate = &mut self.gate;
                let wrote_rows = outcome.wrote_rows();
                lane.enqueued(log_index, SlotState::Running, collection, user_write)
                    && lane.conclude(log_index, wrote_rows, |proposal_key| {
                        gate.conclude(proposal_key, true, outcome)
                    })
            }
        }
    }

    /// Start every entry each group can start now, in log order.
    pub fn pump(&mut self) {
        self.install_blocked = false;
        // Keys a snapshot install carried, before any entry starts: a later
        // copy of one of those proposals is a duplicate here too.
        for key in self.ctx.tracker.take_restored_keys() {
            self.gate.note_restored(key);
        }
        let groups: Vec<u64> = self
            .lanes
            .iter()
            .filter(|(_, lane)| !lane.backlog.is_empty())
            .map(|(group_id, _)| *group_id)
            .collect();
        for group_id in groups {
            self.pump_group(group_id);
        }
    }

    fn pump_group(&mut self, group_id: u64) {
        let shared: &'a Arc<SharedState> = self.ctx.state;
        let gates = shared.raft_apply_gates.get().map(|gates| &**gates);
        while let Some(queued) = self.next_startable(group_id) {
            let log_index = queued.entry.index;
            let proposal_key = queued.proposal_key();
            let covered_through = self.ctx.tracker.covered_through(group_id);
            let permit = match admit_entry(gates, group_id, log_index, covered_through) {
                EntryAdmission::Installing => {
                    if let Some(lane) = self.lanes.get_mut(&group_id) {
                        lane.backlog.push_front(queued);
                    }
                    self.install_blocked = true;
                    return;
                }
                EntryAdmission::Start(permit) => permit,
                // At or below the snapshot's Raft index: the installed state
                // and the Raft boundary both hold the entry.
                EntryAdmission::Covered => {
                    if !self.conclude_covered(
                        group_id,
                        log_index,
                        proposal_key,
                        EntryOutcome::Skipped,
                    ) {
                        return;
                    }
                    continue;
                }
                // Above the Raft index and at or below the cut: only the
                // installed state holds the entry. It concludes without
                // applying and extends the durable prefix, so a restart never
                // delivers it again.
                EntryAdmission::CoveredByCut => {
                    if !self.conclude_covered(
                        group_id,
                        log_index,
                        proposal_key,
                        EntryOutcome::Covered,
                    ) {
                        return;
                    }
                    continue;
                }
            };
            // Every entry that leaves the backlog for good starts. A snapshot
            // builder cuts at the highest one.
            self.ctx.tracker.note_started(group_id, log_index);
            // Stamped before the entry is prepared: a barrier raises the cut
            // floor only for the entries after it.
            let write_mark = queued
                .write_stamp()
                .map(|(tenant_id, write_hlc, restore_id)| {
                    (
                        tenant_id,
                        self.watch.commit_hlc(group_id, log_index, write_hlc),
                        restore_id,
                    )
                });
            // A second copy of an applied proposal never reaches the funnel,
            // so its plan is classified here. Its first copy can sit above
            // the saved floor, and this copy then carries the mark again.
            let repeat_writes =
                self.gate.prior_wrote_rows(proposal_key) && queued.plan_writes_user_data();
            let held = HeldEntry {
                group_id,
                log_index,
                proposal_key,
                metadata_floor: queued.metadata_floor(),
            };
            let prepared = hold_for_metadata(
                self.ctx.state,
                self.ctx.tracker,
                held,
                prepare_entry(self.ctx, &mut self.watch, &self.gate, group_id, queued),
            );
            let (state, blocks, user_write) = match prepared {
                Prepared::Concluded(outcome) => {
                    release_admission_holds(shared, group_id, log_index);
                    let user_write = matches!(outcome, EntryOutcome::Repeat) && repeat_writes;
                    (
                        SlotState::Concluded(self.gate.conclude(proposal_key, false, outcome)),
                        false,
                        user_write,
                    )
                }
                Prepared::Barrier => {
                    release_admission_holds(shared, group_id, log_index);
                    (SlotState::Barrier, false, false)
                }
                Prepared::Enqueue(enqueue) => {
                    self.gate.open(proposal_key);
                    self.ctx.tracker.note_dispatched(group_id, log_index);
                    let enqueue = hold_permit(permit, enqueue);
                    self.running
                        .push(Box::pin(enqueue.map(move |entry| LoopEvent::Enqueued {
                            group_id,
                            log_index,
                            entry,
                        })));
                    // The enqueue reports whether the plan writes user data.
                    (SlotState::Starting, true, false)
                }
                Prepared::Exclusive(apply) => {
                    // An array op or cell write: user data.
                    self.gate.open(proposal_key);
                    self.ctx.tracker.note_dispatched(group_id, log_index);
                    self.running
                        .push(finished_event(hold_permit(permit, apply)));
                    (SlotState::Running, true, true)
                }
            };
            let Some(lane) = self.lanes.get_mut(&group_id) else {
                return;
            };
            if blocks {
                lane.blocking = Some(log_index);
            }
            lane.push(Slot {
                log_index,
                proposal_key,
                collection: None,
                write_mark,
                user_write,
                state,
            });
        }
    }

    /// Conclude an entry an installed snapshot holds, without reaching a
    /// core. It counts as started. Its waiter learns the write committed
    /// without a result. Returns `false` when the group has no lane.
    fn conclude_covered(
        &mut self,
        group_id: u64,
        log_index: u64,
        proposal_key: u64,
        outcome: EntryOutcome,
    ) -> bool {
        self.ctx.tracker.note_started(group_id, log_index);
        release_admission_holds(self.ctx.state, group_id, log_index);
        self.ctx
            .tracker
            .complete_covered(group_id, log_index, proposal_key);
        let concluded = SlotState::Concluded(self.gate.conclude(proposal_key, false, outcome));
        let Some(lane) = self.lanes.get_mut(&group_id) else {
            return false;
        };
        lane.push(Slot {
            log_index,
            proposal_key,
            collection: None,
            write_mark: None,
            user_write: false,
            state: concluded,
        });
        true
    }

    /// Take the next entry of `group_id` when it can start now.
    ///
    /// It waits while the group's previous write is in its enqueue or an
    /// exclusive entry of the group runs, while it is exclusive and an
    /// earlier entry of the group has not concluded, and while a copy of its
    /// proposal runs: the ledger decides it once that copy concludes.
    fn next_startable(&mut self, group_id: u64) -> Option<QueuedEntry> {
        let lane = self.lanes.get_mut(&group_id)?;
        if lane.blocking.is_some() {
            return None;
        }
        let front = lane.backlog.front()?;
        if front.is_exclusive() && lane.has_running() {
            return None;
        }
        if self.gate.in_flight(front.proposal_key()) {
            return None;
        }
        lane.backlog.pop_front()
    }

    /// Settle every group's concluded entries in log order, release their
    /// window, and save each durable floor that moved.
    ///
    /// The tenant write marks of the settled entries are persisted before any
    /// floor that covers them. An entry above the saved floor is delivered
    /// again after a restart and records its mark again, so every committed
    /// write keeps a durable mark.
    pub fn settle(&mut self) {
        let state = self.ctx.state;
        let tracker = self.ctx.tracker;
        for (group_id, lane) in &mut self.lanes {
            let first = lane.front_index();
            let settled = lane.settle(tracker, &state.tenant_marks);
            if settled > 0 {
                tracker.window().release(*group_id, settled);
                // The entries' change events publish in log order, at their
                // log positions, on every replica.
                if let (Some(first), Some(last)) = (first, lane.last_settled()) {
                    publish_settled_changes(state, *group_id, first, last);
                }
            }
            if !lane.floor_pending() {
                continue;
            }
            // One save per pass that moved the floor, never one per entry:
            // each save is an fsync, and the pass coalesces every apply that
            // finished before it.
            if let Err(error) = state.tenant_marks.persist(state.credentials.catalog()) {
                // The floor stays where it is, so every entry above it keeps
                // its place in the log. The next pass persists and saves again.
                tracing::error!(
                    group_id = *group_id,
                    %error,
                    "apply loop: tenant write marks did not persist; the applied floor waits"
                );
                continue;
            }
            if let Some(floor) = lane.take_floor_to_save() {
                // Every entry the floor covers keeps its changes on disk.
                state.change_stream.journal_group(*group_id, floor);
                record_durable_apply(state, *group_id, floor);
            }
        }
    }
}

/// Release the lock keys this node's write gate holds for entries of
/// `group_id` through `log_index`: their applies started, in log order.
fn release_admission_holds(state: &SharedState, group_id: u64, log_index: u64) {
    state
        .calvin
        .admission_holds
        .release_through(group_id, log_index);
}

fn finished_event(apply: ApplyFuture<'_>) -> LoopFuture<'_> {
    Box::pin(apply.map(LoopEvent::Finished))
}
