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
