// SPDX-License-Identifier: BUSL-1.1

//! One group's place in the apply pipeline: the entries not yet started, in
//! log order, and the started entries not yet settled, in log order.
//!
//! Entries start in log order, and a write's enqueue returns before the next
//! entry of its group starts, so every core receives a group's writes in the
//! order the log fixed. They finish in any order. They settle in log
//! order: the group's applied index is the highest index with every earlier
//! entry finished, and its durable floor is the highest index with every
//! earlier entry durable.

use std::collections::VecDeque;

use nodedb_raft::message::LogEntry;

use crate::control::distributed_applier::applied_index::AppliedPrefix;
use crate::control::distributed_applier::propose_tracker::{ApplyingEntry, ProposeTracker};
use crate::control::server::shared::write_admission::plan_writes_user_data;
use crate::control::state::tenant_marks::{MarkSite, TenantMarks};
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite, from_replicated_entry};

use super::proposal_gate::PrefixStep;

/// A committed entry handed to the loop and not yet started.
pub(super) struct QueuedEntry {
    pub entry: LogEntry,
    /// The entry decoded once on arrival. `None` for a leader-change no-op or
    /// bytes that do not decode as a replicated entry.
    pub decoded: Option<ReplicatedEntry>,
}

impl QueuedEntry {
    pub fn new(entry: LogEntry) -> Self {
        let decoded = if entry.data.is_empty() {
            None
        } else {
            ReplicatedEntry::from_bytes(&entry.data)
        };
        Self { entry, decoded }
    }

    /// The proposal's idempotency key, `0` when the entry carries none.
    pub fn proposal_key(&self) -> u64 {
        self.decoded.as_ref().map_or(0, |e| e.idempotency_key)
    }

    /// The metadata index the entry's proposer had applied, `0` when the
    /// entry carries none.
    pub fn metadata_floor(&self) -> u64 {
        self.decoded.as_ref().map_or(0, |e| e.metadata_floor)
    }

    /// `(tenant_id, write_hlc)` of an entry that writes a tenant's data. A
    /// cut barrier and a Calvin read result write nothing, and an entry with
    /// no proposer stamp has no commit HLC to record.
    pub fn write_stamp(&self) -> Option<(u64, u64)> {
        let decoded = self.decoded.as_ref()?;
        if decoded.write_hlc == 0
            || matches!(
                decoded.write,
                ReplicatedWrite::CutBarrier { .. } | ReplicatedWrite::CalvinReadResult { .. }
            )
        {
            return None;
        }
        Some((decoded.tenant_id, decoded.write_hlc))
    }

    /// Whether the entry's plan writes user data, as the write funnel decides
    /// for the writes it records. Decoded here only for an entry that never
    /// reaches the funnel: a second copy of an applied proposal.
    pub fn plan_writes_user_data(&self) -> bool {
        let Some(decoded) = self.decoded.as_ref() else {
            return false;
        };
        match decoded.write {
            ReplicatedWrite::ArrayOp { .. }
            | ReplicatedWrite::ArrayCellPut { .. }
            | ReplicatedWrite::ArrayCellDelete { .. }
            | ReplicatedWrite::TransactionRedo { .. } => true,
            ReplicatedWrite::ArraySchema { .. }
            | ReplicatedWrite::CutBarrier { .. }
            | ReplicatedWrite::CalvinReadResult { .. } => false,
            _ => matches!(
                from_replicated_entry(&self.entry.data, None),
                Ok(Some((_, _, plan, _))) if plan_writes_user_data(&plan)
            ),
        }
    }

    /// Whether the entry must apply with nothing else of its group in
    /// flight. The array paths await their own write inside the apply, so
    /// the loop cannot fix their arrival order at the core any other way,
    /// and a schema import must follow every earlier entry's apply.
    pub fn is_exclusive(&self) -> bool {
        self.decoded.as_ref().is_some_and(|e| {
            matches!(
                e.write,
                ReplicatedWrite::ArrayOp { .. }
                    | ReplicatedWrite::ArraySchema { .. }
                    | ReplicatedWrite::ArrayCellPut { .. }
                    | ReplicatedWrite::ArrayCellDelete { .. }
            )
        })
    }
}

/// Where a started entry stands.
pub(super) enum SlotState {
    /// The write's enqueue runs.
    Starting,
    /// The apply runs; its outcome arrives as a finished apply.
    Running,
    /// A backup's cut barrier. It completes once every earlier entry of its
    /// group settled.
    Barrier,
    /// The entry concluded.
    Concluded(PrefixStep),
}

/// A started entry not yet settled.
pub(super) struct Slot {
    pub log_index: u64,
    pub proposal_key: u64,
    /// The collection the entry writes, when its apply named one.
    pub collection: Option<String>,
    /// `(tenant_id, commit_hlc)` the entry records on its tenant's mark in
    /// this group once it settles, when it carries a proposer stamp.
    pub write_mark: Option<(u64, u64)>,
    /// Whether the entry's plan writes user data. Only such an entry raises
    /// its tenant's mark.
    pub user_write: bool,
    pub state: SlotState,
}

/// One group's apply pipeline.
pub(super) struct Lane {
    group_id: u64,
    pub backlog: VecDeque<QueuedEntry>,
    slots: VecDeque<Slot>,
    /// The entry the group's next start waits on: a write whose enqueue
    /// runs, or an exclusive entry that runs.
    pub blocking: Option<u64>,
    /// The durable prefix over every entry this process settled for the
    /// group. A break holds for the life of the process: an index saved past
    /// a non-durable entry would let the next boot skip it.
    prefix: AppliedPrefix,
    /// The floor last saved for the group.
    saved_floor: Option<u64>,
}

impl Lane {
    pub fn new(group_id: u64) -> Self {
        Self {
            group_id,
            backlog: VecDeque::new(),
            slots: VecDeque::new(),
            blocking: None,
            prefix: AppliedPrefix::new(),
            saved_floor: None,
        }
    }

    /// Whether any started entry of the group has not concluded.
    pub fn has_running(&self) -> bool {
        self.slots
            .iter()
            .any(|slot| matches!(slot.state, SlotState::Starting | SlotState::Running))
    }

    /// Record that the enqueue of the entry at `log_index` returned: its
    /// state is `state` now, its apply named `collection`, and `user_write`
    /// says whether its plan writes user data. Returns `false` when no entry
    /// at that index is starting.
    pub fn enqueued(
        &mut self,
        log_index: u64,
        state: SlotState,
        collection: Option<String>,
        user_write: bool,
    ) -> bool {
        let Some(slot) = self.slot_mut(log_index) else {
            return false;
        };
        if !matches!(slot.state, SlotState::Starting) {
            return false;
        }
        slot.state = state;
        slot.user_write = user_write;
        if collection.is_some() {
            slot.collection = collection;
        }
        if self.blocking == Some(log_index) {
            self.blocking = None;
        }
        true
    }

    fn slot_mut(&mut self, log_index: u64) -> Option<&mut Slot> {
        let position = self
            .slots
            .binary_search_by_key(&log_index, |slot| slot.log_index)
            .ok()?;
        self.slots.get_mut(position)
    }

    pub fn push(&mut self, slot: Slot) {
        self.slots.push_back(slot);
    }

    /// Conclude the running entry at `log_index`: `conclude` receives its
    /// proposal key and returns how it moves the prefix. `wrote_rows` says
    /// whether the apply wrote the entry's rows; an entry that wrote none
    /// raises no tenant write mark. Returns `false` when no running entry
    /// has that index.
    pub fn conclude(
        &mut self,
        log_index: u64,
        wrote_rows: bool,
        conclude: impl FnOnce(u64) -> PrefixStep,
    ) -> bool {
        let Some(slot) = self.slot_mut(log_index) else {
            return false;
        };
        if !matches!(slot.state, SlotState::Running) {
            return false;
        }
        slot.user_write &= wrote_rows;
        slot.state = SlotState::Concluded(conclude(slot.proposal_key));
        if self.blocking == Some(log_index) {
            self.blocking = None;
        }
        true
    }

    /// Settle every concluded entry at the front, in log order: complete a
    /// barrier's waiter, raise the tenant's write mark in this group, extend
    /// or break the durable prefix, and advance the applied watermark. The
    /// mark rises first, so a reader that waited for the applied index sees
    /// it. Returns how many entries settled.
    pub fn settle(&mut self, tracker: &ProposeTracker, marks: &TenantMarks) -> usize {
        let mut settled = 0;
        while let Some(front) = self.slots.front() {
            let step = match front.state {
                SlotState::Starting | SlotState::Running => break,
                SlotState::Barrier => {
                    // Every entry before the barrier finished; a waiting
                    // backup may snapshot this group now.
                    tracker.complete(
                        self.group_id,
                        front.log_index,
                        front.proposal_key,
                        Ok(
                            crate::control::distributed_applier::AppliedWrite::unversioned(
                                Vec::new(),
                            ),
                        ),
                    );
                    PrefixStep::Neutral
                }
                SlotState::Concluded(step) => step,
            };
            let log_index = front.log_index;
            if front.user_write
                && let Some((tenant_id, commit_hlc)) = front.write_mark
            {
                marks.raise(
                    self.group_id,
                    tenant_id,
                    commit_hlc,
                    MarkSite::ReplicatedApply,
                    front.collection.as_deref(),
                );
            }
            self.slots.pop_front();
            match step {
                PrefixStep::Neutral => self.prefix.skip(),
                PrefixStep::Record(durable) => self.prefix.record(log_index, durable),
            }
            tracker.note_applied(self.group_id, log_index);
            settled += 1;
        }
        tracker.note_applying(
            self.group_id,
            self.slots.front().map(|slot| ApplyingEntry {
                group_id: self.group_id,
                log_index: slot.log_index,
                collection: slot.collection.clone(),
            }),
        );
        settled
    }

    /// Whether the durable floor moved past the floor last saved.
    pub fn floor_pending(&self) -> bool {
        self.prefix
            .floor()
            .is_some_and(|floor| self.saved_floor.is_none_or(|saved| saved < floor))
    }

    /// The durable floor to save, when it moved past the floor last saved.
    pub fn take_floor_to_save(&mut self) -> Option<u64> {
        let floor = self.prefix.floor()?;
        if self.saved_floor.is_some_and(|saved| saved >= floor) {
            return None;
        }
        self.saved_floor = Some(floor);
        Some(floor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn entries_settle_in_log_order_whatever_order_they_finish() {
        let tracker = ProposeTracker::new();
        let mut lane = Lane::new(1);
        lane.push(slot(5, SlotState::Running));
        lane.push(slot(6, SlotState::Running));
        lane.push(slot(7, SlotState::Running));

        assert!(lane.conclude(7, true, |_| PrefixStep::Record(true)));
        assert!(lane.conclude(6, true, |_| PrefixStep::Record(true)));
        assert!(
            !lane.conclude(6, true, |_| PrefixStep::Record(true)),
            "6 concluded already"
        );
        assert_eq!(
            lane.settle(&tracker, &TenantMarks::default()),
            0,
            "entry 5 still runs"
        );
        assert_eq!(lane.take_floor_to_save(), None);
        assert_eq!(
            tracker.applying(1).map(|entry| entry.log_index),
            Some(5),
            "the timeout diagnostic names the entry the group waits behind"
        );

        assert!(lane.conclude(5, true, |_| PrefixStep::Record(true)));
        assert_eq!(lane.settle(&tracker, &TenantMarks::default()), 3);
        assert_eq!(lane.take_floor_to_save(), Some(7));
        assert_eq!(lane.take_floor_to_save(), None, "a saved floor saves once");
        assert!(tracker.applying(1).is_none());
    }

    #[test]
    fn a_non_durable_entry_holds_the_floor_across_later_settles() {
        let tracker = ProposeTracker::new();
        let mut lane = Lane::new(1);
        lane.push(slot(1, SlotState::Concluded(PrefixStep::Record(true))));
        lane.push(slot(2, SlotState::Concluded(PrefixStep::Record(false))));
        lane.settle(&tracker, &TenantMarks::default());
        assert_eq!(lane.take_floor_to_save(), Some(1));

        lane.push(slot(3, SlotState::Concluded(PrefixStep::Record(true))));
        lane.settle(&tracker, &TenantMarks::default());
        assert_eq!(
            lane.take_floor_to_save(),
            None,
            "a floor past entry 2 would let the next boot skip it"
        );
    }

    #[test]
    fn an_enqueue_that_returns_unblocks_the_next_start() {
        let tracker = ProposeTracker::new();
        let mut lane = Lane::new(2);
        lane.push(slot(4, SlotState::Starting));
        lane.blocking = Some(4);
        assert!(lane.has_running());

        assert!(lane.enqueued(4, SlotState::Running, Some("docs".to_owned()), true));
        assert_eq!(lane.blocking, None);
        lane.settle(&tracker, &TenantMarks::default());
        assert_eq!(
            tracker.applying(2).and_then(|entry| entry.collection),
            Some("docs".to_owned())
        );
        assert!(
            !lane.enqueued(4, SlotState::Running, None, false),
            "4 left its enqueue"
        );
    }

    #[test]
    fn a_user_write_raises_its_mark_before_the_applied_index_moves() {
        let tracker = ProposeTracker::new();
        let marks = TenantMarks::default();
        let mut lane = Lane::new(4);
        let mut write = slot(1, SlotState::Concluded(PrefixStep::Record(true)));
        write.write_mark = Some((7, 500));
        write.user_write = true;
        write.collection = Some("docs".to_owned());
        let mut index_change = slot(2, SlotState::Concluded(PrefixStep::Record(true)));
        index_change.write_mark = Some((7, 900));
        lane.push(write);
        lane.push(index_change);

        lane.settle(&tracker, &marks);
        let mark = marks.get(4, 7).expect("the write's mark");
        assert_eq!(
            mark.hlc, 500,
            "an entry that writes no user data raises no mark"
        );
        assert_eq!(mark.collection.as_deref(), Some("docs"));
    }

    #[test]
    fn a_refused_user_write_raises_no_mark() {
        let tracker = ProposeTracker::new();
        let marks = TenantMarks::default();
        let mut lane = Lane::new(4);
        let mut refused = slot(1, SlotState::Running);
        refused.write_mark = Some((7, 500));
        refused.user_write = true;
        refused.collection = Some("docs".to_owned());
        lane.push(refused);

        assert!(lane.conclude(1, false, |_| PrefixStep::Record(true)));
        lane.settle(&tracker, &marks);
        assert_eq!(
            marks.get(4, 7),
            None,
            "a write that changed no row must not refuse a restore"
        );
    }

    #[test]
    fn a_barrier_completes_only_after_every_earlier_entry() {
        let tracker = ProposeTracker::new();
        let mut lane = Lane::new(3);
        lane.push(slot(1, SlotState::Running));
        lane.push(slot(2, SlotState::Barrier));
        let mut barrier = tracker.register(3, 2, 0);

        lane.settle(&tracker, &TenantMarks::default());
        assert!(barrier.try_recv().is_err(), "entry 1 still runs");

        assert!(lane.conclude(1, true, |_| PrefixStep::Record(true)));
        lane.settle(&tracker, &TenantMarks::default());
        assert!(matches!(barrier.try_recv(), Ok(Ok(_))));
    }
}
