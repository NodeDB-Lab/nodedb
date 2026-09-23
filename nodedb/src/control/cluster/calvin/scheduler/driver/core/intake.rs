// SPDX-License-Identifier: BUSL-1.1

//! Intake gate for the Calvin scheduler.
//!
//! The scheduler takes new sequenced input only while it can make progress.
//! The gate closes while a capacity-refused dispatch waits for re-send, or
//! while the in-flight backlog sits at [`SchedulerConfig::max_inflight_backlog`].
//! A closed gate disables the run loop's receiver arm and skips the catch-up
//! drain. Completions, verdicts, read results, promotions, and capacity
//! wakeups stay active, because they drain the backlog.
//!
//! Input left unread fills the bounded fan-out channel. The sequencer apply
//! path then drops the next input and arms catch-up for this vShard, and the
//! drain replays it from the committed log once the gate opens.
//!
//! The gate changes only when inputs are read, never the order they are
//! processed in, so replicas stay deterministic.
//!
//! [`SchedulerConfig::max_inflight_backlog`]: super::super::config::SchedulerConfig::max_inflight_backlog

use tracing::debug;

use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::metrics::intake_closure_reason;

/// Why the intake gate is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum IntakeClosure {
    /// A capacity-refused dispatch waits in the deferred FIFO.
    DeferredDispatch,
    /// The in-flight backlog is at its bound, and some of it progresses
    /// without new input.
    BacklogFull,
}

impl IntakeClosure {
    /// The `nodedb_calvin_intake_gate_closed_total` reason index.
    fn metric_reason(self) -> usize {
        match self {
            Self::DeferredDispatch => intake_closure_reason::DEFERRED_DISPATCH,
            Self::BacklogFull => intake_closure_reason::BACKLOG_FULL,
        }
    }
}

/// Last observed intake gate state, kept to detect transitions.
#[derive(Debug, Default)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct IntakeGate {
    closed: Option<IntakeClosure>,
}

impl Scheduler {
    /// Pending, blocked, and dependent-barrier txns.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn inflight_backlog(
        &self,
    ) -> usize {
        self.pending.len() + self.blocked.len() + self.dependent_barrier.len()
    }

    /// Why intake must stay closed, or `None` when the gate is open.
    ///
    /// The backlog bound applies only while a pending txn or a dependent
    /// barrier exists. Those finish on executor responses, verdicts, and read
    /// results. A backlog of blocked txns alone can wait on a reservation
    /// release, which arrives as input, so it keeps the gate open.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn intake_closure(
        &self,
    ) -> Option<IntakeClosure> {
        if self.has_deferred_dispatch() {
            return Some(IntakeClosure::DeferredDispatch);
        }
        let drains_without_input = !self.pending.is_empty() || !self.dependent_barrier.is_empty();
        if drains_without_input && self.inflight_backlog() >= self.config.max_inflight_backlog {
            return Some(IntakeClosure::BacklogFull);
        }
        None
    }

    /// Re-evaluate the intake gate and return whether it is open.
    ///
    /// Publishes the backlog gauge on every call. Logs each transition at
    /// debug, and counts each open-to-closed transition by reason.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn refresh_intake_gate(
        &mut self,
    ) -> bool {
        let backlog = self.inflight_backlog();
        let closure = self.intake_closure();
        self.metrics.set_intake_backlog(backlog);
        let previous = self.intake.closed;
        if closure == previous {
            return closure.is_none();
        }
        match closure {
            Some(reason) => {
                if previous.is_none() {
                    self.metrics
                        .record_intake_gate_closed(reason.metric_reason());
                }
                debug!(
                    vshard_id = self.vshard_id,
                    ?reason,
                    backlog,
                    deferred = self.deferred_dispatch_len(),
                    "calvin scheduler: intake gate closed"
                );
            }
            None => {
                debug!(
                    vshard_id = self.vshard_id,
                    backlog, "calvin scheduler: intake gate opened"
                );
            }
        }
        self.metrics.set_intake_gate_closed(closure.is_some());
        self.intake.closed = closure;
        closure.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use nodedb_cluster::calvin::CalvinCompletionRegistry;
    use nodedb_cluster::calvin::types::SchedulerInput;
    use nodedb_types::TenantId;
    use tokio::sync::mpsc;

    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler, build_test_scheduler_with_data_side, fill_tenant_inflight,
        make_sequenced_txn, make_validate_only_txn, release_filler, spawn_scheduler_loop,
        staged_pending, test_coll_vshard,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::BlockedTxn;
    use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

    /// How long a held input must stay unread. Several liveness ticks of the
    /// spawned loop fit in it.
    const HOLD_WAIT: Duration = Duration::from_millis(300);

    /// How long a test waits for the loop to read an input.
    const CONSUME_WAIT: Duration = Duration::from_secs(5);

    /// Whether the loop reads every input sent on `input_tx` within `wait`.
    async fn inputs_consumed_within(
        input_tx: &mpsc::Sender<SchedulerInput>,
        wait: Duration,
    ) -> bool {
        let drained = async {
            while input_tx.capacity() < input_tx.max_capacity() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(wait, drained).await.is_ok()
    }

    fn blocked_fixture(epoch: u64) -> BlockedTxn {
        BlockedTxn {
            txn: make_sequenced_txn(epoch, 0),
            keys: BTreeSet::new(),
            // no-determinism: test-only blocked_at timestamp for a fabricated BlockedTxn fixture.
            blocked_at: Instant::now(),
        }
    }

    /// A backlog of blocked txns alone keeps intake open at the bound: a
    /// reservation release they wait on arrives as input.
    #[tokio::test]
    async fn blocked_only_backlog_at_bound_keeps_intake_open() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        scheduler.config.max_inflight_backlog = 1;
        scheduler
            .blocked
            .insert(TxnId::new(3, 0), blocked_fixture(3));

        assert_eq!(scheduler.intake_closure(), None);
    }

    /// A pending txn that fills the backlog bound closes intake.
    #[tokio::test]
    async fn pending_backlog_at_bound_closes_intake() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        scheduler.config.max_inflight_backlog = 2;
        let held = TxnId::new(3, 0);
        scheduler
            .pending
            .insert(held, staged_pending(make_sequenced_txn(3, 0), held));
        assert_eq!(scheduler.intake_closure(), None, "below the bound");

        scheduler
            .blocked
            .insert(TxnId::new(4, 0), blocked_fixture(4));
        assert_eq!(
            scheduler.intake_closure(),
            Some(IntakeClosure::BacklogFull),
            "at the bound with a pending txn"
        );
    }

    /// With a dispatch deferred at capacity, the run loop leaves a new input
    /// unread. It reads the input once capacity frees and the deferral drains.
    #[tokio::test]
    async fn deferred_dispatch_holds_new_input_until_capacity_frees() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let shared = Arc::clone(&scheduler.shared);
        let fillers = fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));
        scheduler.process_scheduler_input(SchedulerInput::Txn(make_validate_only_txn(3, 0)));
        assert!(
            scheduler.has_deferred_dispatch(),
            "the stage dispatch defers"
        );
        let metrics = Arc::clone(&scheduler.metrics);

        let running = spawn_scheduler_loop(scheduler);
        running
            .input_tx()
            .send(SchedulerInput::Txn(make_validate_only_txn(4, 0)))
            .await
            .expect("the loop's input channel is open");

        let read_while_deferred = inputs_consumed_within(running.input_tx(), HOLD_WAIT).await;
        let gate_closed = metrics.intake_gate_closed.load(Ordering::Relaxed);
        let deferred_closures = metrics.intake_gate_closed_counts
            [intake_closure_reason::DEFERRED_DISPATCH]
            .load(Ordering::Relaxed);

        release_filler(&shared, &mut data_side, fillers[0]);
        let read_after_capacity = inputs_consumed_within(running.input_tx(), CONSUME_WAIT).await;
        running.stop().await;

        assert!(
            !read_while_deferred,
            "the loop must not read input while a dispatch is deferred"
        );
        assert_eq!(gate_closed, 1, "the gate gauge reports closed");
        assert_eq!(deferred_closures, 1, "one closure for a deferred dispatch");
        assert!(
            read_after_capacity,
            "the loop must read the input once the deferral drains"
        );
    }

    /// With the backlog at the configured bound, the run loop leaves a new
    /// input unread.
    #[tokio::test]
    async fn full_backlog_holds_new_input() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        scheduler.config.max_inflight_backlog = 1;
        let held = TxnId::new(3, 0);
        scheduler
            .pending
            .insert(held, staged_pending(make_validate_only_txn(3, 0), held));
        let metrics = Arc::clone(&scheduler.metrics);

        let running = spawn_scheduler_loop(scheduler);
        running
            .input_tx()
            .send(SchedulerInput::Txn(make_validate_only_txn(4, 0)))
            .await
            .expect("the loop's input channel is open");

        let read_at_bound = inputs_consumed_within(running.input_tx(), HOLD_WAIT).await;
        running.stop().await;

        assert!(
            !read_at_bound,
            "the loop must not read input while the backlog is at its bound"
        );
        assert_eq!(metrics.intake_gate_closed.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.intake_backlog.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics.intake_gate_closed_counts[intake_closure_reason::BACKLOG_FULL]
                .load(Ordering::Relaxed),
            1
        );
    }
}
