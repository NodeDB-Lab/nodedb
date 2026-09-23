// SPDX-License-Identifier: BUSL-1.1

//! Scheduler observability metrics.
//!
//! All counters are `AtomicU64` so they can be read from a Prometheus scrape
//! handler without crossing plane boundaries.  The Data Plane never writes
//! these; they are updated only on the Control Plane (scheduler task).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Histogram bucket boundaries (ms) for `nodedb_calvin_executor_txn_duration_ms`:
/// `[1, 5, 10, 50, 100, 500, 1000, 5000, 30000]`.
pub const EXECUTOR_TXN_DURATION_BUCKETS: &[u64] = &[1, 5, 10, 50, 100, 500, 1000, 5000, 30_000];

/// Per-vshard Calvin scheduler metrics.
pub struct SchedulerMetrics {
    /// Total transactions dispatched to the Data Plane executor.
    pub dispatch_count: AtomicU64,
    /// Total transactions that were blocked on lock acquisition.
    pub blocked_count: AtomicU64,
    /// Total lock-wait duration in milliseconds (sum across all txns).
    pub lock_wait_ms_total: AtomicU64,
    /// Total transactions whose response was received from the executor.
    pub completed_count: AtomicU64,
    /// Total executor errors (infrastructure-level failures).
    pub executor_error_count: AtomicU64,
    /// Highest epoch number applied so far.
    pub last_applied_epoch: AtomicU64,
    /// Histogram bucket counts for `nodedb_calvin_executor_txn_duration_ms`.
    ///
    /// Length == `EXECUTOR_TXN_DURATION_BUCKETS.len() + 1` (+Inf slot).
    pub executor_txn_duration_buckets: [AtomicU64; 10],
    /// Running sum of executor txn durations in ms (for the histogram `_sum`).
    pub executor_txn_duration_sum_ms: AtomicU64,
    /// Total infrastructure aborts by reason.
    ///
    /// Reasons (indexes): 0=wal_crc_error, 1=io_error, 2=oom, 3=disk_full,
    /// 4=corruption_detected, 5=passive_participant_timeout.
    pub infra_abort_counts: [AtomicU64; 6],
    /// Times a staged txn parked in `AwaitingVerdict` passed its stall deadline
    /// with the durable global verdict still unknown. This is NOT an abort: the
    /// scheduler keeps waiting and holding locks (a unilateral abort while a
    /// peer may already have flushed a commit would tear the transaction). A
    /// non-zero, growing value flags a stuck sequencer / partitioned verdict
    /// path that needs operator attention, never a correctness action here.
    pub verdict_stall_count: AtomicU64,
    /// Total `SchedulerInput`s replayed by the sequencer-fan-out catch-up drain
    /// (`drain_catch_up`). A non-zero value means this replica dropped at least
    /// one fan-out `try_send` (channel Full/Closed) and the drain reconstructed
    /// the missed input from the committed sequencer Raft log — the mechanism
    /// that keeps this replica's lock table convergent with its peers.
    pub catch_up_replayed: AtomicU64,
    /// Times the catch-up drain found the sequencer Raft log compacted below the
    /// dropped index (`LogCompacted`). Reachable only via a sequencer-group
    /// snapshot-install resync, where the installed snapshot already subsumes the
    /// dropped index, so the state is already correct. A non-zero, growing value
    /// flags that catch-up is relying on snapshot coverage rather than log replay
    /// and warrants operator attention.
    pub catch_up_log_compacted: AtomicU64,
    /// Scheduler dispatches the bridge dispatcher refused at capacity. Each
    /// refusal parks the request for re-send; none is an abort.
    pub dispatch_deferred_count: AtomicU64,
    /// Requests parked for re-send right now, waiting for dispatcher capacity.
    pub dispatch_deferred_depth: AtomicU64,
    /// Intake gate state: 1 while the scheduler takes no new sequenced input,
    /// 0 while it does.
    pub intake_gate_closed: AtomicU64,
    /// In-flight backlog: pending, blocked, and dependent-barrier txns.
    pub intake_backlog: AtomicU64,
    /// Intake gate closures by reason. Indexes are the constants in
    /// [`intake_closure_reason`].
    pub intake_gate_closed_counts: [AtomicU64; 3],
    /// Apply halt state: 1 once the scheduler halted, 0 while it applies.
    pub apply_halted: AtomicU64,
    /// Reason of the halt. An index into [`apply_halt_reason`], read only
    /// while `apply_halted` is 1.
    pub apply_halt_reason: AtomicU64,
    /// Owed sequencer entries re-proposed because their effect was not yet
    /// applied, by kind. Indexes are the constants in
    /// [`sequencer_propose_kind`].
    pub sequencer_propose_retry_counts: [AtomicU64; 4],
}

/// Reason codes for `nodedb_calvin_infra_abort_total`.
pub mod infra_abort_reason {
    pub const WAL_CRC_ERROR: usize = 0;
    pub const IO_ERROR: usize = 1;
    pub const OOM: usize = 2;
    pub const DISK_FULL: usize = 3;
    pub const CORRUPTION_DETECTED: usize = 4;
    pub const PASSIVE_PARTICIPANT_TIMEOUT: usize = 5;

    pub const LABELS: &[&str] = &[
        "wal_crc_error",
        "io_error",
        "oom",
        "disk_full",
        "corruption_detected",
        "passive_participant_timeout",
    ];
}

/// Reason codes for `nodedb_calvin_intake_gate_closed_total`.
pub mod intake_closure_reason {
    pub const DEFERRED_DISPATCH: usize = 0;
    pub const BACKLOG_FULL: usize = 1;
    pub const APPLY_HALTED: usize = 2;

    pub const LABELS: &[&str] = &["deferred_dispatch", "backlog_full", "apply_halted"];
}

/// Reason codes for `nodedb_calvin_apply_halted`.
pub mod apply_halt_reason {
    pub const DRAINING: usize = 0;
    pub const DISPATCH_REFUSED: usize = 1;
    pub const RESPONSE_DISCONNECTED: usize = 2;
    pub const RESOLVE_FAILED: usize = 3;
    pub const FLUSH_FAILED: usize = 4;
    pub const LOCAL_STAGE_FAILED: usize = 5;
    pub const IDENTITY_BIND_FAILED: usize = 6;
    pub const WAL_APPEND_FAILED: usize = 7;

    pub const LABELS: &[&str] = &[
        "draining",
        "dispatch_refused",
        "response_disconnected",
        "resolve_failed",
        "flush_failed",
        "local_stage_failed",
        "identity_bind_failed",
        "wal_append_failed",
    ];
}

/// Kind codes for `nodedb_calvin_sequencer_propose_retry_total`.
pub mod sequencer_propose_kind {
    pub const VOTE: usize = 0;
    pub const COMPLETION_ACK: usize = 1;
    pub const OLLP_MISMATCH: usize = 2;
    pub const ROUTING_FAILED: usize = 3;

    pub const LABELS: &[&str] = &["vote", "completion_ack", "ollp_mismatch", "routing_failed"];
}

impl SchedulerMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record that a transaction was dispatched.
    pub fn record_dispatch(&self) {
        self.dispatch_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a transaction was blocked on lock acquisition.
    pub fn record_blocked(&self) {
        self.blocked_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record lock-wait latency for one transaction.
    pub fn record_lock_wait_ms(&self, ms: u64) {
        self.lock_wait_ms_total.fetch_add(ms, Ordering::Relaxed);
    }

    /// Record that an executor response was received.
    pub fn record_completed(&self) {
        self.completed_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an executor infrastructure error.
    pub fn record_executor_error(&self) {
        self.executor_error_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an infrastructure abort with a typed reason.
    ///
    /// `reason` must be one of the constants in [`infra_abort_reason`].
    pub fn record_infra_abort(&self, reason: usize) {
        if let Some(counter) = self.infra_abort_counts.get(reason) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record that a parked `AwaitingVerdict` txn passed its stall deadline
    /// without a durable verdict. Observability only — the scheduler keeps
    /// waiting; it never aborts on a stall.
    pub fn record_verdict_stall(&self) {
        self.verdict_stall_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that the catch-up drain replayed `n` sequencer inputs.
    pub fn record_catch_up_replayed(&self, n: u64) {
        self.catch_up_replayed.fetch_add(n, Ordering::Relaxed);
    }

    /// Record that the catch-up drain hit a compacted sequencer log.
    pub fn record_catch_up_log_compacted(&self) {
        self.catch_up_log_compacted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that the dispatcher refused a scheduler dispatch at capacity.
    pub fn record_dispatch_deferred(&self) {
        self.dispatch_deferred_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Set the number of requests parked for re-send.
    pub fn set_dispatch_deferred_depth(&self, depth: usize) {
        self.dispatch_deferred_depth
            .store(depth as u64, Ordering::Relaxed);
    }

    /// Record that the intake gate closed for `reason`.
    ///
    /// `reason` must be one of the constants in [`intake_closure_reason`].
    pub fn record_intake_gate_closed(&self, reason: usize) {
        if let Some(counter) = self.intake_gate_closed_counts.get(reason) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Set the intake gate state gauge.
    pub fn set_intake_gate_closed(&self, closed: bool) {
        self.intake_gate_closed
            .store(u64::from(closed), Ordering::Relaxed);
    }

    /// Set the apply halt gauge for `reason`.
    ///
    /// `reason` must be one of the constants in [`apply_halt_reason`].
    pub fn set_apply_halted(&self, reason: usize) {
        self.apply_halt_reason
            .store(reason as u64, Ordering::Relaxed);
        self.apply_halted.store(1, Ordering::Relaxed);
    }

    /// Record that an owed sequencer entry of `kind` was re-proposed.
    ///
    /// `kind` must be one of the constants in [`sequencer_propose_kind`].
    pub fn record_sequencer_propose_retry(&self, kind: usize) {
        if let Some(counter) = self.sequencer_propose_retry_counts.get(kind) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Set the in-flight backlog gauge.
    pub fn set_intake_backlog(&self, backlog: usize) {
        self.intake_backlog.store(backlog as u64, Ordering::Relaxed);
    }

    /// Record the end-to-end executor txn duration (dispatch → response).
    ///
    /// Increments the appropriate histogram bucket and the running sum.
    pub fn record_executor_txn_duration_ms(&self, ms: u64) {
        self.executor_txn_duration_sum_ms
            .fetch_add(ms, Ordering::Relaxed);
        let bucket_idx = EXECUTOR_TXN_DURATION_BUCKETS
            .iter()
            .position(|&b| ms <= b)
            .unwrap_or(EXECUTOR_TXN_DURATION_BUCKETS.len());
        self.executor_txn_duration_buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Update the `last_applied_epoch` high-water mark.
    pub fn update_last_applied_epoch(&self, epoch: u64) {
        // CAS loop to ensure monotonic update without a mutex.
        let mut current = self.last_applied_epoch.load(Ordering::Relaxed);
        loop {
            if epoch <= current {
                break;
            }
            match self.last_applied_epoch.compare_exchange_weak(
                current,
                epoch,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Render a Prometheus text-format exposition.
    pub fn render_prometheus(&self, vshard_id: u32) -> String {
        use std::fmt::Write as _;

        let label = format!("vshard=\"{vshard_id}\"");
        let mut out = format!(
            "# HELP nodedb_calvin_scheduler_dispatch_total Total txns dispatched to executor\n\
             # TYPE nodedb_calvin_scheduler_dispatch_total counter\n\
             nodedb_calvin_scheduler_dispatch_total{{{label}}} {}\n\
             # HELP nodedb_calvin_scheduler_blocked_total Total txns blocked on lock acquisition\n\
             # TYPE nodedb_calvin_scheduler_blocked_total counter\n\
             nodedb_calvin_scheduler_blocked_total{{{label}}} {}\n\
             # HELP nodedb_calvin_scheduler_lock_wait_ms_total Sum of lock-wait ms across all txns\n\
             # TYPE nodedb_calvin_scheduler_lock_wait_ms_total counter\n\
             nodedb_calvin_scheduler_lock_wait_ms_total{{{label}}} {}\n\
             # HELP nodedb_calvin_scheduler_completed_total Total executor responses received\n\
             # TYPE nodedb_calvin_scheduler_completed_total counter\n\
             nodedb_calvin_scheduler_completed_total{{{label}}} {}\n\
             # HELP nodedb_calvin_scheduler_executor_errors_total Executor infrastructure errors\n\
             # TYPE nodedb_calvin_scheduler_executor_errors_total counter\n\
             nodedb_calvin_scheduler_executor_errors_total{{{label}}} {}\n\
             # HELP nodedb_calvin_scheduler_last_applied_epoch Last applied epoch number\n\
             # TYPE nodedb_calvin_scheduler_last_applied_epoch gauge\n\
             nodedb_calvin_scheduler_last_applied_epoch{{{label}}} {}\n",
            self.dispatch_count.load(Ordering::Relaxed),
            self.blocked_count.load(Ordering::Relaxed),
            self.lock_wait_ms_total.load(Ordering::Relaxed),
            self.completed_count.load(Ordering::Relaxed),
            self.executor_error_count.load(Ordering::Relaxed),
            self.last_applied_epoch.load(Ordering::Relaxed),
        );

        // Executor txn duration histogram.
        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_executor_txn_duration_ms \
             End-to-end executor transaction duration in milliseconds."
        );
        let _ = writeln!(
            out,
            "# TYPE nodedb_calvin_executor_txn_duration_ms histogram"
        );
        let mut cumulative: u64 = 0;
        for (i, &boundary) in EXECUTOR_TXN_DURATION_BUCKETS.iter().enumerate() {
            cumulative += self.executor_txn_duration_buckets[i].load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                "nodedb_calvin_executor_txn_duration_ms_bucket{{{label},le=\"{boundary}\"}} \
                 {cumulative}"
            );
        }
        cumulative += self.executor_txn_duration_buckets[EXECUTOR_TXN_DURATION_BUCKETS.len()]
            .load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "nodedb_calvin_executor_txn_duration_ms_bucket{{{label},le=\"+Inf\"}} {cumulative}"
        );
        let _ = writeln!(
            out,
            "nodedb_calvin_executor_txn_duration_ms_sum{{{label}}} {}",
            self.executor_txn_duration_sum_ms.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "nodedb_calvin_executor_txn_duration_ms_count{{{label}}} {cumulative}"
        );

        // Infrastructure abort counters.
        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_infra_abort_total \
             Total infrastructure-level aborts during Calvin execution."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_infra_abort_total counter");
        for (i, &reason_label) in infra_abort_reason::LABELS.iter().enumerate() {
            let _ = writeln!(
                out,
                "nodedb_calvin_infra_abort_total{{{label},reason=\"{reason_label}\"}} {}",
                self.infra_abort_counts[i].load(Ordering::Relaxed)
            );
        }

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_verdict_stall_total \
             Times a staged txn's verdict wait passed its stall deadline (no abort)."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_verdict_stall_total counter");
        let _ = writeln!(
            out,
            "nodedb_calvin_verdict_stall_total{{{label}}} {}",
            self.verdict_stall_count.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_catch_up_replayed_total \
             Sequencer inputs replayed by the fan-out catch-up drain."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_catch_up_replayed_total counter");
        let _ = writeln!(
            out,
            "nodedb_calvin_catch_up_replayed_total{{{label}}} {}",
            self.catch_up_replayed.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_catch_up_log_compacted_total \
             Times the catch-up drain found the sequencer log compacted below the \
             dropped index (state is snapshot-covered)."
        );
        let _ = writeln!(
            out,
            "# TYPE nodedb_calvin_catch_up_log_compacted_total counter"
        );
        let _ = writeln!(
            out,
            "nodedb_calvin_catch_up_log_compacted_total{{{label}}} {}",
            self.catch_up_log_compacted.load(Ordering::Relaxed)
        );

        self.render_flow_prometheus(&mut out, &label);

        out
    }
}

impl Default for SchedulerMetrics {
    fn default() -> Self {
        Self {
            dispatch_count: AtomicU64::new(0),
            blocked_count: AtomicU64::new(0),
            lock_wait_ms_total: AtomicU64::new(0),
            completed_count: AtomicU64::new(0),
            executor_error_count: AtomicU64::new(0),
            last_applied_epoch: AtomicU64::new(0),
            executor_txn_duration_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            executor_txn_duration_sum_ms: AtomicU64::new(0),
            infra_abort_counts: std::array::from_fn(|_| AtomicU64::new(0)),
            verdict_stall_count: AtomicU64::new(0),
            catch_up_replayed: AtomicU64::new(0),
            catch_up_log_compacted: AtomicU64::new(0),
            dispatch_deferred_count: AtomicU64::new(0),
            dispatch_deferred_depth: AtomicU64::new(0),
            intake_gate_closed: AtomicU64::new(0),
            intake_backlog: AtomicU64::new(0),
            intake_gate_closed_counts: std::array::from_fn(|_| AtomicU64::new(0)),
            apply_halted: AtomicU64::new(0),
            apply_halt_reason: AtomicU64::new(0),
            sequencer_propose_retry_counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_count_increments() {
        let m = SchedulerMetrics::new();
        assert_eq!(m.dispatch_count.load(Ordering::Relaxed), 0);
        m.record_dispatch();
        m.record_dispatch();
        assert_eq!(m.dispatch_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn lock_wait_ms_records_correctly() {
        let m = SchedulerMetrics::new();
        m.record_lock_wait_ms(10);
        m.record_lock_wait_ms(25);
        assert_eq!(m.lock_wait_ms_total.load(Ordering::Relaxed), 35);
    }
}
