// SPDX-License-Identifier: BUSL-1.1

//! Three-tier priority task queue for the Data Plane core loop.
//!
//! Replaces the single `VecDeque<ExecutionTask>` with three queues:
//!
//! | Tier     | Priorities          | Drain budget per cycle |
//! |----------|---------------------|------------------------|
//! | Critical | `Critical`          | 8 slots                |
//! | High     | `High`              | 4 slots                |
//! | Low      | `Normal`,`Background`| 2 slots               |
//!
//! **Drain algorithm.** Each call to [`PriorityQueues::pop_next`] pulls one
//! task using the 8:4:2 ratio. Empty tiers donate their unused slots to the
//! next lower tier so no cycle is wasted when, e.g., Critical is empty.
//!
//! **Starvation prevention.** Because lower-priority work always gets at least
//! 2 slots *and* inherits unused upper-tier slots, it can never be permanently
//! starved even under sustained Critical load.
//!
//! **Snapshot barriers.** Every task carries its arrival sequence on this
//! core. A tenant snapshot is a barrier: it runs only after every task that
//! arrived before it, in any tier, and every task that arrived after it waits,
//! in any tier, until it has run. The dispatcher keeps each database's
//! requests in dispatch order on the ring, and a snapshot reads only its own
//! database, so the snapshot sees exactly the writes of its database
//! dispatched before it.

use std::collections::VecDeque;
use std::time::Instant;

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use crate::bridge::envelope::Priority;
use crate::data::executor::task::ExecutionTask;

/// Drain budget per tier per cycle (8 Critical : 4 High : 2 Low).
const BUDGET_CRITICAL: usize = 8;
const BUDGET_HIGH: usize = 4;
const BUDGET_LOW: usize = 2;

/// The first arrival sequence. A task put back at the front of the queue
/// takes a sequence below every queued one, so the sequence starts high.
const FIRST_SEQUENCE: u64 = 1 << 62;

/// A task held in the priority queue alongside its enqueue timestamp.
///
/// The timestamp is used to record IO wait latency in `IoMetrics`.
pub struct QueuedTask {
    pub task: ExecutionTask,
    /// Nanosecond timestamp (from `Instant`) captured when the task was
    /// pushed.  Used by the IO metrics path to compute wait time.
    pub enqueued_at: Instant,
    /// Arrival sequence on this core. Each tier holds its tasks in rising
    /// sequence.
    seq: u64,
}

impl QueuedTask {
    fn new(task: ExecutionTask, seq: u64) -> Self {
        Self {
            task,
            enqueued_at: Instant::now(),
            seq,
        }
    }
}

/// Whether `task` is a barrier: a tenant snapshot.
fn is_barrier(task: &ExecutionTask) -> bool {
    matches!(
        task.request.plan,
        PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot { .. })
    )
}

#[derive(Clone, Copy)]
enum Tier {
    Critical,
    High,
    Low,
}

impl Tier {
    fn of(task: &ExecutionTask) -> Self {
        match task.request.priority {
            Priority::Critical => Self::Critical,
            Priority::High => Self::High,
            Priority::Normal | Priority::Background => Self::Low,
        }
    }
}

/// Three-tier bounded-ratio priority queue.
///
/// `!Send` — lives on a single Data Plane core alongside `CoreLoop`.
pub struct PriorityQueues {
    /// `Critical` priority tasks.
    critical: VecDeque<QueuedTask>,
    /// `High` priority tasks.
    high: VecDeque<QueuedTask>,
    /// `Normal` and `Background` priority tasks (merged low tier).
    low: VecDeque<QueuedTask>,
    /// The sequence the next pushed task takes.
    next_seq: u64,
    /// The sequences of queued barriers, rising.
    barriers: VecDeque<u64>,
}

impl PriorityQueues {
    /// Create empty queues.
    pub fn new() -> Self {
        Self {
            critical: VecDeque::new(),
            high: VecDeque::new(),
            low: VecDeque::new(),
            next_seq: FIRST_SEQUENCE,
            barriers: VecDeque::new(),
        }
    }

    fn tier(&self, tier: Tier) -> &VecDeque<QueuedTask> {
        match tier {
            Tier::Critical => &self.critical,
            Tier::High => &self.high,
            Tier::Low => &self.low,
        }
    }

    fn tier_mut(&mut self, tier: Tier) -> &mut VecDeque<QueuedTask> {
        match tier {
            Tier::Critical => &mut self.critical,
            Tier::High => &mut self.high,
            Tier::Low => &mut self.low,
        }
    }

    /// Whether the front task of `tier` can run now. Before the first queued
    /// barrier every task can. The barrier itself can once no task that
    /// arrived before it is queued. A task after it cannot.
    fn front_runs(&self, tier: Tier) -> bool {
        let Some(front) = self.tier(tier).front() else {
            return false;
        };
        let Some(&barrier) = self.barriers.front() else {
            return true;
        };
        if front.seq < barrier {
            return true;
        }
        front.seq == barrier
            && [Tier::Critical, Tier::High, Tier::Low]
                .iter()
                .all(|&other| self.tier(other).front().is_none_or(|t| t.seq >= barrier))
    }

    /// Pop the front task of `tier` when it can run.
    fn pop_tier(&mut self, tier: Tier) -> Option<QueuedTask> {
        if !self.front_runs(tier) {
            return None;
        }
        let queued = self.tier_mut(tier).pop_front()?;
        if self.barriers.front() == Some(&queued.seq) {
            self.barriers.pop_front();
        }
        Some(queued)
    }

    /// Queue `task` at `seq` on its tier: at the back when `front` is false.
    fn insert(&mut self, task: ExecutionTask, seq: u64, front: bool) {
        let tier = Tier::of(&task);
        if is_barrier(&task) {
            if front {
                self.barriers.push_front(seq);
            } else {
                self.barriers.push_back(seq);
            }
        }
        let queued = QueuedTask::new(task, seq);
        if front {
            self.tier_mut(tier).push_front(queued);
        } else {
            self.tier_mut(tier).push_back(queued);
        }
    }

    /// Enqueue a task at the appropriate tier.
    pub fn push(&mut self, task: ExecutionTask) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.insert(task, seq, false);
    }

    /// Total tasks across all tiers.
    pub fn len(&self) -> usize {
        self.critical.len() + self.high.len() + self.low.len()
    }

    /// `true` if all tiers are empty.
    pub fn is_empty(&self) -> bool {
        self.critical.is_empty() && self.high.is_empty() && self.low.is_empty()
    }

    /// Pending count for the Critical tier.
    pub fn critical_len(&self) -> usize {
        self.critical.len()
    }

    /// Pending count for the High tier.
    pub fn high_len(&self) -> usize {
        self.high.len()
    }

    /// Pending count for the Low tier.
    pub fn low_len(&self) -> usize {
        self.low.len()
    }

    /// Peek at the front task that can run, without removing it.
    ///
    /// Returns the highest-priority task that can run (Critical → High →
    /// Low). Used by `poll_write_batch` to decide whether to start a batch.
    pub fn front(&self) -> Option<&ExecutionTask> {
        [Tier::Critical, Tier::High, Tier::Low]
            .into_iter()
            .find(|&tier| self.front_runs(tier))
            .and_then(|tier| self.tier(tier).front())
            .map(|qt| &qt.task)
    }

    /// Remove and return the highest-priority task that can run, without
    /// applying the drain ratio.
    ///
    /// Used by `poll_write_batch` when collecting a write-coalesce batch.
    pub fn pop_front(&mut self) -> Option<ExecutionTask> {
        self.pop_tier(Tier::Critical)
            .or_else(|| self.pop_tier(Tier::High))
            .or_else(|| self.pop_tier(Tier::Low))
            .map(|qt| qt.task)
    }

    /// Iterate over all tasks across all tiers in priority order (Critical → High → Low).
    ///
    /// Used by the cancel handler to locate a task by request ID.
    pub fn iter(&self) -> impl Iterator<Item = &ExecutionTask> {
        self.critical
            .iter()
            .chain(self.high.iter())
            .chain(self.low.iter())
            .map(|qt| &qt.task)
    }

    /// Remove the task at `pos` (position in priority order: Critical first, then High, then Low).
    ///
    /// Used by the cancel handler after `iter().position(...)`.
    pub fn remove(&mut self, pos: usize) {
        let crit_len = self.critical.len();
        let high_len = self.high.len();
        let removed = if pos < crit_len {
            self.critical.remove(pos)
        } else if pos < crit_len + high_len {
            self.high.remove(pos - crit_len)
        } else {
            self.low.remove(pos - crit_len - high_len)
        };
        if let Some(removed) = removed {
            self.barriers.retain(|&seq| seq != removed.seq);
        }
    }

    /// Push a task back to the front of the queue.
    ///
    /// Used by `poll_write_batch` to return tasks that could not be batched.
    /// The task takes a sequence below every queued task, so it is the next
    /// candidate for its tier and stays ahead of every barrier.
    pub fn push_front(&mut self, task: ExecutionTask) {
        let lowest = [Tier::Critical, Tier::High, Tier::Low]
            .into_iter()
            .filter_map(|tier| self.tier(tier).front().map(|t| t.seq))
            .min()
            .unwrap_or(self.next_seq);
        self.insert(task, lowest - 1, true);
    }

    /// Pop the next task according to the 8:4:2 drain ratio.
    ///
    /// Each call to `pop_next` pulls one task from whichever tier has
    /// remaining budget in the current cycle.  Once a tier's budget for the
    /// cycle is exhausted the next lower tier is tried; if *that* is also
    /// exhausted or empty the remaining budget cascades further down. A tier
    /// whose front task waits behind a barrier counts as empty.
    ///
    /// Cycle state is maintained via the mutable `cycle` counter passed by
    /// the caller (reset to 0 to start a new cycle).  The cycle counter
    /// counts tasks already dequeued in the current 14-slot window
    /// (8 + 4 + 2 = 14).
    ///
    /// Returns `None` when all tiers are empty.
    pub fn pop_next(&mut self, cycle: &mut usize) -> Option<QueuedTask> {
        // Within a 14-slot window: slots 0–7 = Critical, 8–11 = High, 12–13 = Low.
        // When a preferred tier is empty its slots go to the next lower tier.

        const CYCLE_LEN: usize = BUDGET_CRITICAL + BUDGET_HIGH + BUDGET_LOW; // 14

        let pos = *cycle % CYCLE_LEN;

        let order = if pos < BUDGET_CRITICAL {
            [Tier::Critical, Tier::High, Tier::Low]
        } else if pos < BUDGET_CRITICAL + BUDGET_HIGH {
            [Tier::High, Tier::Critical, Tier::Low]
        } else {
            [Tier::Low, Tier::High, Tier::Critical]
        };
        let task = order.into_iter().find_map(|tier| self.pop_tier(tier));

        if task.is_some() {
            *cycle = cycle.wrapping_add(1);
        }

        task
    }
}

impl Default for PriorityQueues {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::bridge::envelope::{Priority, Request};
    use crate::data::executor::task::ExecutionTask;
    use crate::event::EventSource;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_physical::physical_plan::meta::MetaOp;

    fn make_task(priority: Priority) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Compact),
            deadline: Instant::now() + Duration::from_secs(60),
            priority,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Eventual,
            idempotency_key: None,
            event_source: EventSource::User,
            user_roles: vec![],
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            entry_version: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    /// Feed equal numbers of all three tiers; verify drain order reflects
    /// the 8:4:2 ratio within each 14-slot cycle.
    #[test]
    fn drain_respects_ratio() {
        let mut q = PriorityQueues::new();

        // Push 8 Critical, 4 High, 2 Low (exactly one cycle's worth).
        for _ in 0..8 {
            q.push(make_task(Priority::Critical));
        }
        for _ in 0..4 {
            q.push(make_task(Priority::High));
        }
        for _ in 0..2 {
            q.push(make_task(Priority::Normal));
        }

        let mut cycle = 0usize;
        let mut critical_count = 0usize;
        let mut high_count = 0usize;
        let mut low_count = 0usize;

        while let Some(qt) = q.pop_next(&mut cycle) {
            match qt.task.request.priority {
                Priority::Critical => critical_count += 1,
                Priority::High => high_count += 1,
                Priority::Normal | Priority::Background => low_count += 1,
            }
        }

        assert_eq!(critical_count, 8);
        assert_eq!(high_count, 4);
        assert_eq!(low_count, 2);
    }

    /// Push 100 Critical + 100 Normal; verify that all Normal tasks eventually
    /// drain (no permanent starvation).
    #[test]
    fn no_permanent_starvation() {
        let mut q = PriorityQueues::new();

        for _ in 0..100 {
            q.push(make_task(Priority::Critical));
        }
        for _ in 0..100 {
            q.push(make_task(Priority::Normal));
        }

        let mut cycle = 0usize;
        let mut normal_drained = 0usize;
        let mut total = 0usize;

        while let Some(qt) = q.pop_next(&mut cycle) {
            total += 1;
            if matches!(
                qt.task.request.priority,
                Priority::Normal | Priority::Background
            ) {
                normal_drained += 1;
            }
        }

        assert_eq!(total, 200, "all 200 tasks must drain");
        assert_eq!(normal_drained, 100, "all 100 Normal tasks must drain");
    }

    /// Verify empty-tier slot donation: 8 Critical-only tasks drain without
    /// stalling when High and Low tiers are empty.
    #[test]
    fn empty_tier_slot_donation() {
        let mut q = PriorityQueues::new();

        for _ in 0..16 {
            q.push(make_task(Priority::Critical));
        }

        let mut cycle = 0usize;
        let mut drained = 0usize;
        while q.pop_next(&mut cycle).is_some() {
            drained += 1;
        }
        assert_eq!(drained, 16);
    }

    fn snapshot_task() -> ExecutionTask {
        let mut task = make_task(Priority::Normal);
        task.request.plan = PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
            tenant_id: 1,
            cut_watermark: None,
            cut_capture: None,
            arrays: false,
        });
        task
    }

    fn tagged(priority: Priority, id: u64) -> ExecutionTask {
        let mut task = make_task(priority);
        task.request.request_id = RequestId::new(id);
        task
    }

    fn drain_ids(q: &mut PriorityQueues) -> Vec<u64> {
        let mut cycle = 0usize;
        let mut ids = Vec::new();
        while let Some(qt) = q.pop_next(&mut cycle) {
            ids.push(qt.task.request_id().as_u64());
        }
        ids
    }

    /// A High write that arrived before a tenant snapshot runs before it, and
    /// one that arrived after it runs after it, whatever the tier ratio does.
    #[test]
    fn a_snapshot_is_a_barrier_across_tiers() {
        let mut q = PriorityQueues::new();
        q.push(tagged(Priority::High, 1));
        q.push(tagged(Priority::Normal, 2));
        let mut snapshot = snapshot_task();
        snapshot.request.request_id = RequestId::new(3);
        q.push(snapshot);
        q.push(tagged(Priority::High, 4));
        q.push(tagged(Priority::Critical, 5));
        q.push(tagged(Priority::Normal, 6));

        let ids = drain_ids(&mut q);
        let at = |id: u64| ids.iter().position(|&x| x == id).expect("drained");
        assert!(at(1) < at(3), "the earlier High write is visible: {ids:?}");
        assert!(
            at(2) < at(3),
            "the earlier Normal write is visible: {ids:?}"
        );
        for later in [4, 5, 6] {
            assert!(at(3) < at(later), "a later write waits: {ids:?}");
        }
        assert_eq!(ids.len(), 6);
    }

    /// `pop_front` and `front` honor the barrier, and a task put back stays
    /// ahead of it.
    #[test]
    fn pop_front_honors_the_barrier_and_push_front_stays_ahead() {
        let mut q = PriorityQueues::new();
        q.push(tagged(Priority::Normal, 1));
        let mut snapshot = snapshot_task();
        snapshot.request.request_id = RequestId::new(2);
        q.push(snapshot);
        q.push(tagged(Priority::Critical, 3));

        assert_eq!(q.front().map(|t| t.request_id().as_u64()), Some(1));
        let first = q.pop_front().expect("the earlier write");
        assert_eq!(first.request_id().as_u64(), 1);
        q.push_front(first);
        let order: Vec<u64> = std::iter::from_fn(|| q.pop_front())
            .map(|t| t.request_id().as_u64())
            .collect();
        assert_eq!(order, [1, 2, 3]);
    }

    /// A cancelled snapshot releases the tasks behind it.
    #[test]
    fn a_removed_barrier_releases_later_tasks() {
        let mut q = PriorityQueues::new();
        let mut snapshot = snapshot_task();
        snapshot.request.request_id = RequestId::new(1);
        q.push(snapshot);
        q.push(tagged(Priority::High, 2));
        let pos = q
            .iter()
            .position(|t| t.request_id().as_u64() == 1)
            .expect("queued");
        q.remove(pos);
        assert_eq!(drain_ids(&mut q), [2]);
    }
}
