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
//!
//! An entry this replica cannot apply as its peers do holds its group: no
//! later entry of the group starts until a snapshot install covers it (see
//! [`super::held_entry`]).

use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};

use crate::control::distributed_applier::applier::ApplyBatch;
use crate::control::distributed_applier::proposal_ledger::ProposalLedger;
use crate::control::state::SharedState;

use super::bookkeeping::release_admission_holds;
use super::context::{ApplyContext, LoopEvent, LoopFuture, Started, StartedEntry};
use super::group_watch::GroupWatch;
use super::held_entry::{
    HeldDebts, HoldCheck, keep_snapshot_owed, note_hold, release_covered_hold,
};
use super::lane::{Lane, QueuedEntry, Slot, SlotState};
use super::metadata_floor::{HeldEntry, hold_for_metadata};
use super::mirror_hold::{MirrorTurn, mirror_turn};
use super::panicked::{conclude_panicked, finished_event, guarded};
use super::proposal_gate::{EntryOutcome, HoldReason, ProposalGate};
use super::snapshot_gate::{EntryAdmission, admit_entry, hold_permit};
use super::start::{Prepared, prepare_entry};

/// Every group's lane, and the enqueues and applies that run.
pub(super) struct Pipeline<'a> {
    ctx: ApplyContext<'a>,
    lanes: HashMap<u64, Lane>,
    gate: ProposalGate,
    watch: GroupWatch,
    running: FuturesUnordered<LoopFuture<'a>>,
    debts: HeldDebts,
    /// Set by a pump that left an entry queued because a snapshot install
    /// held its group's apply gate.
    install_blocked: bool,
    /// Set by a pump that left a mirror entry waiting for its cut.
    mirror_blocked: bool,
}

impl<'a> Pipeline<'a> {
    pub fn new(ctx: ApplyContext<'a>, ledger: ProposalLedger) -> Self {
        Self {
            ctx,
            lanes: HashMap::new(),
            gate: ProposalGate::new(ledger),
            watch: GroupWatch::with_cuts(ctx.state.pitr.recorded_cuts()),
            running: FuturesUnordered::new(),
            debts: HeldDebts::default(),
            install_blocked: false,
            mirror_blocked: false,
        }
    }

    /// Whether the last pump left a mirror entry waiting for its cut. The loop
    /// pumps again once the cut watch moves.
    pub fn awaits_mirror(&self) -> bool {
        self.mirror_blocked
    }

    /// Whether a group waits for a snapshot install: the last pump left an
    /// entry queued behind one, or an entry holds its group's lane. The loop
    /// pumps again once an install releases its gate.
    pub fn awaits_install(&self) -> bool {
        self.install_blocked || self.lanes.values().any(Lane::is_held)
    }

    /// Whether a held group's snapshot debt did not persist. The loop wakes
    /// on a timer to write it again.
    pub fn retries_debt(&self) -> bool {
        self.debts.retry_due()
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
                let hold = finished.outcome.hold_reason();
                let handled = self.lanes.get_mut(&finished.group_id).is_some_and(|lane| {
                    lane.conclude(finished.log_index, wrote_rows, |proposal_key| {
                        gate.conclude(proposal_key, true, finished.outcome)
                    })
                });
                // The pump that follows records the group's debt under its
                // apply gate.
                if handled && let Some(reason) = hold {
                    note_hold(finished.group_id, finished.log_index, reason);
                }
                (finished.group_id, finished.log_index, handled)
            }
            LoopEvent::Panicked {
                group_id,
                log_index,
                detail,
            } => {
                let gate = &mut self.gate;
                let handled = self.lanes.get_mut(&group_id).is_some_and(|lane| {
                    conclude_panicked(self.ctx, lane, gate, log_index, &detail)
                });
                // The pump that follows records the group's debt.
                if handled {
                    note_hold(group_id, log_index, HoldReason::ApplyPanicked);
                }
                (group_id, log_index, handled)
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
                self.running
                    .push(finished_event(group_id, log_index, apply));
                lane.enqueued(log_index, SlotState::Running, collection, user_write)
            }
            Started::Concluded(outcome) => {
                // The write concluded without reaching its core. It leaves
                // its enqueue and concludes in one step.
                self.ctx.tracker.note_concluded(group_id, log_index);
                let gate = &mut self.gate;
                let wrote_rows = outcome.wrote_rows();
                let hold = outcome.hold_reason();
                let handled = lane.enqueued(log_index, SlotState::Running, collection, user_write)
                    && lane.conclude(log_index, wrote_rows, |proposal_key| {
                        gate.conclude(proposal_key, true, outcome)
                    });
                if handled && let Some(reason) = hold {
                    note_hold(group_id, log_index, reason);
                }
                handled
            }
        }
    }

    /// Start every entry each group can start now, in log order.
    pub fn pump(&mut self) {
        self.install_blocked = false;
        self.mirror_blocked = false;
        // Keys a snapshot install carried, before any entry starts: a later
        // copy of one of those proposals is a duplicate here too.
        for key in self.ctx.tracker.take_restored_keys() {
            self.gate.note_restored(key);
        }
        let groups: Vec<u64> = self
            .lanes
            .iter()
            .filter(|(_, lane)| !lane.backlog.is_empty() || lane.is_held())
            .map(|(group_id, _)| *group_id)
            .collect();
        for group_id in groups {
            self.pump_group(group_id);
            // The entries left wait behind an earlier one. Their barrier
            // events fold now: a barrier needs no catalog.
            if !self.install_blocked
                && let Some(lane) = self.lanes.get_mut(&group_id)
            {
                super::barrier_prefold::prefold_backlog(self.ctx, lane, group_id);
            }
        }
    }

    fn pump_group(&mut self, group_id: u64) {
        let shared: &'a Arc<SharedState> = self.ctx.state;
        let gates = shared.raft_apply_gates.get().map(|gates| &**gates);
        let Some(lane) = self.lanes.get_mut(&group_id) else {
            return;
        };
        match release_covered_hold(self.ctx, lane, &mut self.gate, &mut self.debts, group_id) {
            HoldCheck::Free => {}
            HoldCheck::Held => return,
            HoldCheck::Installing => {
                self.install_blocked = true;
                return;
            }
        }
        while let Some(queued) = self.next_startable(group_id) {
            let log_index = queued.entry.index;
            let proposal_key = queued.proposal_key();
            let covered_through = self.ctx.tracker.covered_through(group_id);
            let admission = admit_entry(gates, group_id, log_index, covered_through);
            // Before the entry joins the lane: no entry above the installed
            // index settles before the lane covers it.
            if let Some(lane) = self.lanes.get_mut(&group_id)
                && let Some(proposal_key) = lane.cover_through(admission.installed_through)
            {
                // The install holds every entry through its index: a held
                // one concludes.
                self.gate.note_covered(proposal_key);
            }
            let permit = match admission.decision {
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
                    super::cut_barrier::note_covered_barrier(shared, group_id, &queued);
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
                    super::cut_barrier::note_covered_barrier(shared, group_id, &queued);
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
            // Below an install's coverage the installed state holds the entry,
            // so only an entry past the checks above reads its cut's verdict.
            match mirror_turn(shared, &queued) {
                MirrorTurn::Proceed => {}
                MirrorTurn::Wait => {
                    if let Some(lane) = self.lanes.get_mut(&group_id) {
                        lane.backlog.push_front(queued);
                    }
                    self.mirror_blocked = true;
                    return;
                }
                MirrorTurn::Skip => {
                    self.ctx.tracker.forget_committed(group_id, log_index);
                    let outcome = EntryOutcome::Skipped;
                    if !self.conclude_covered(group_id, log_index, proposal_key, outcome) {
                        return;
                    }
                    continue;
                }
            }
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
            // An entry of a later term ends every earlier term of the group:
            // no earlier term adds a chunk or a final entry after it.
            shared
                .redo_chunks
                .end_terms_before(group_id, queued.entry.term);
            let prepared = hold_for_metadata(
                self.ctx.state,
                self.ctx.tracker,
                held,
                prepare_entry(self.ctx, &mut self.watch, &self.gate, group_id, queued),
            );
            let mut hold = None;
            let (state, blocks, user_write) = match prepared {
                Prepared::Concluded(outcome) => {
                    release_admission_holds(shared, group_id, log_index);
                    let user_write = matches!(outcome, EntryOutcome::Repeat) && repeat_writes;
                    hold = outcome.hold_reason();
                    (
                        SlotState::from(self.gate.conclude(proposal_key, false, outcome)),
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
                    let enqueued = Box::pin(enqueue.map(move |entry| LoopEvent::Enqueued {
                        group_id,
                        log_index,
                        entry,
                    }));
                    self.running.push(guarded(group_id, log_index, enqueued));
                    // The enqueue reports whether the plan writes user data.
                    (SlotState::Starting, true, false)
                }
                Prepared::Exclusive(apply) => {
                    // An array op or cell write: user data.
                    self.gate.open(proposal_key);
                    self.ctx.tracker.note_dispatched(group_id, log_index);
                    self.running.push(finished_event(
                        group_id,
                        log_index,
                        hold_permit(permit, apply),
                    ));
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
            // The entry's admission permit is still held: no install that
            // covers the entry settles the debt first.
            if let Some(reason) = hold {
                note_hold(group_id, log_index, reason);
                let installed_through = admission.installed_through;
                keep_snapshot_owed(
                    shared,
                    &mut self.debts,
                    group_id,
                    log_index,
                    installed_through,
                );
            }
        }
    }

    /// Conclude an entry an installed snapshot holds, or a mirror entry that
    /// never applies, without reaching a core. It counts as started. Its
    /// waiter learns the write committed without a result. Returns `false`
    /// when the group has no lane.
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
        let concluded = SlotState::from(self.gate.conclude(proposal_key, false, outcome));
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
    /// It waits while an entry holds the group's lane, while the group's
    /// previous write is in its enqueue or an exclusive entry of the group
    /// runs, while it is exclusive and an earlier entry of the group has not
    /// concluded, and while a copy of its proposal runs: the ledger decides
    /// it once that copy concludes.
    fn next_startable(&mut self, group_id: u64) -> Option<QueuedEntry> {
        self.lanes
            .get_mut(&group_id)?
            .next_startable(|key| self.gate.in_flight(key))
    }

    /// Settle every group's concluded entries in log order. See
    /// [`super::settle::settle_lanes`].
    pub fn settle(&mut self) {
        super::settle::settle_lanes(self.ctx, &mut self.lanes);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::{GroupAppliedWatchers, GroupApplyGates};
    use nodedb_raft::message::LogEntry;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::distributed_applier::proposal_ledger::PROPOSAL_LEDGER_CAPACITY;
    use crate::control::distributed_applier::propose_tracker::ProposeTracker;
    use crate::wal::WalManager;

    use super::super::context::FinishedApply;
    use super::super::proposal_gate::PrefixStep;

    const GROUP: u64 = 4;

    fn slot(log_index: u64, state: SlotState) -> Slot {
        Slot {
            log_index,
            proposal_key: 0,
            collection: None,
            write_mark: None,
            user_write: false,
            state,
        }
    }

    /// Entry 5 holds its group with entry 6 queued behind it. Entry 6 never
    /// starts and the applied index stays at 4 until an install covers 6.
    /// Then 5 and 6 conclude covered and the applied index moves to 6.
    #[tokio::test]
    async fn a_held_entry_stops_its_group_until_an_install_covers_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let gates = Arc::new(GroupApplyGates::new());
        gates.mount(GROUP);
        assert!(state.raft_apply_gates.set(Arc::clone(&gates)).is_ok());
        let watchers = Arc::new(GroupAppliedWatchers::new());
        let tracker = Arc::new(ProposeTracker::new().with_group_watchers(Arc::clone(&watchers)));
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };
        let mut pipeline = Pipeline::new(ctx, ProposalLedger::new(PROPOSAL_LEDGER_CAPACITY));

        let mut lane = Lane::new(GROUP);
        lane.push(slot(4, SlotState::Concluded(PrefixStep::Record(true))));
        lane.push(slot(5, SlotState::Held));
        lane.backlog.push_back(QueuedEntry::new(LogEntry {
            term: 1,
            index: 6,
            data: Vec::new(),
        }));
        pipeline.lanes.insert(GROUP, lane);

        pipeline.pump();
        pipeline.settle();
        assert_eq!(
            pipeline.lanes[&GROUP].backlog.len(),
            1,
            "entry 6 waits behind the held entry"
        );
        assert!(
            tracker.started_through(GROUP) < 6,
            "entry 6 never starts while entry 5 holds"
        );
        assert_eq!(watchers.get_or_create(GROUP).current(), 4);
        assert!(pipeline.awaits_install());
        assert!(
            state.redo_chunks.owes_snapshot(GROUP),
            "a held group owes a snapshot install"
        );
        assert!(!pipeline.retries_debt(), "the debt persisted");

        // A snapshot through entry 4 installs: entry 5 still holds.
        gates.install(GROUP).await.adopted(4);
        pipeline.pump();
        pipeline.settle();
        assert!(pipeline.lanes[&GROUP].is_held());
        assert_eq!(watchers.get_or_create(GROUP).current(), 4);

        // A snapshot through entry 6 installs.
        gates.install(GROUP).await.adopted(6);
        pipeline.pump();
        pipeline.settle();
        let lane = &pipeline.lanes[&GROUP];
        assert!(!lane.is_held());
        assert!(lane.backlog.is_empty());
        assert_eq!(tracker.started_through(GROUP), 6);
        assert_eq!(
            watchers.get_or_create(GROUP).current(),
            6,
            "entries 5 and 6 settle once the install covers them"
        );
        assert!(!pipeline.awaits_install());
    }

    /// The apply of a running entry that never returns a result.
    fn exploding_apply() -> FinishedApply {
        panic!("the apply of entry 5 exploded")
    }

    /// Entry 5's apply panics with entry 6 queued behind it. Its waiter
    /// receives an error at once, entry 5 holds the group, entry 6 never
    /// starts, and the applied index stays at 4.
    #[tokio::test]
    async fn a_panicking_apply_answers_its_waiter_and_holds_its_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let gates = Arc::new(GroupApplyGates::new());
        gates.mount(GROUP);
        assert!(state.raft_apply_gates.set(Arc::clone(&gates)).is_ok());
        let watchers = Arc::new(GroupAppliedWatchers::new());
        let tracker = Arc::new(ProposeTracker::new().with_group_watchers(Arc::clone(&watchers)));
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };
        let mut pipeline = Pipeline::new(ctx, ProposalLedger::new(PROPOSAL_LEDGER_CAPACITY));

        const KEY: u64 = 77;
        let waiter = tracker.register(GROUP, 5, KEY);
        let mut lane = Lane::new(GROUP);
        lane.push(slot(4, SlotState::Concluded(PrefixStep::Record(true))));
        lane.push(Slot {
            proposal_key: KEY,
            ..slot(5, SlotState::Running)
        });
        lane.backlog.push_back(QueuedEntry::new(LogEntry {
            term: 1,
            index: 6,
            data: Vec::new(),
        }));
        pipeline.lanes.insert(GROUP, lane);
        pipeline.running.push(finished_event(
            GROUP,
            5,
            Box::pin(async { exploding_apply() }),
        ));

        let event = pipeline.next_event().await.expect("the apply concludes");
        assert!(matches!(event, LoopEvent::Panicked { log_index: 5, .. }));
        pipeline.handle(event);
        pipeline.pump();
        pipeline.settle();

        let answered = waiter.await.expect("the waiter is answered");
        assert!(answered.is_err(), "a panicked apply answers with an error");
        let lane = &pipeline.lanes[&GROUP];
        assert_eq!(lane.held_index(), Some(5), "entry 5 holds its group");
        assert_eq!(lane.backlog.len(), 1, "entry 6 waits behind entry 5");
        assert_eq!(watchers.get_or_create(GROUP).current(), 4);
        assert!(pipeline.awaits_install());
        assert!(
            state.redo_chunks.owes_snapshot(GROUP),
            "a group held by a panic owes a snapshot install"
        );
    }

    /// A held group's debt write fails twice and files one report. The
    /// loop's Raft-tick wake writes it again, with no batch or install, and
    /// the debt persists.
    ///
    /// The recorder is process-wide: nextest runs this test in its own
    /// process.
    #[tokio::test(start_paused = true)]
    async fn a_failed_debt_write_persists_on_a_later_tick() {
        let reports = tempfile::tempdir().expect("reports dir");
        assert!(
            faultbox::init(
                faultbox::Config::new("nodedb-test", "0.0.0", reports.path())
                    .install_panic_hook(false),
            ),
            "this test must be the first initializer of the recorder"
        );
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let gates = Arc::new(GroupApplyGates::new());
        gates.mount(GROUP);
        assert!(state.raft_apply_gates.set(Arc::clone(&gates)).is_ok());
        let watchers = Arc::new(GroupAppliedWatchers::new());
        let tracker = Arc::new(ProposeTracker::new().with_group_watchers(watchers));
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };
        let mut pipeline = Pipeline::new(ctx, ProposalLedger::new(PROPOSAL_LEDGER_CAPACITY));
        let mut lane = Lane::new(GROUP);
        lane.push(slot(4, SlotState::Concluded(PrefixStep::Record(true))));
        lane.push(slot(5, SlotState::Held));
        pipeline.lanes.insert(GROUP, lane);
        let catalog = state.credentials.catalog();

        catalog.fail_next_redo_snapshot_owed_write_for_test();
        pipeline.pump();
        assert!(pipeline.retries_debt());
        catalog.fail_next_redo_snapshot_owed_write_for_test();
        pipeline.pump();
        assert!(pipeline.retries_debt(), "the second write failed too");
        assert!(!state.redo_chunks.owes_snapshot(GROUP));

        // The sender stays open: only the timed wake can pump.
        let (_tx, mut rx) = tokio::sync::mpsc::channel::<ApplyBatch>(1);
        let ran = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            super::super::driver::drive(&mut pipeline, &mut rx, &state),
        )
        .await;
        assert!(ran.is_err(), "the loop waits for the held entry's install");
        assert!(!pipeline.retries_debt(), "a tick wrote the debt");
        assert!(state.redo_chunks.owes_snapshot(GROUP));
        assert!(pipeline.lanes[&GROUP].is_held());

        let debt_reports: u64 = faultbox::reader::list(reports.path())
            .expect("list reports")
            .iter()
            .filter(|group| {
                group.first.domain_kind.as_deref() == Some("nodedb.held_entry_debt_not_recorded")
            })
            .map(faultbox::reader::Group::occurrences)
            .sum();
        assert_eq!(debt_reports, 1, "one report for the failure run");
    }
}
