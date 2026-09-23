// SPDX-License-Identifier: BUSL-1.1

//! Configuration for the Calvin scheduler driver.

use std::time::Duration;

use crate::bridge::dispatch::DATA_PLANE_QUEUE_CAPACITY;

/// Tuning parameters for a [`super::core::Scheduler`] instance.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Bounded channel capacity for incoming [`nodedb_cluster::calvin::types::SequencedTxn`]s.
    pub channel_capacity: usize,
    /// Transaction deadline multiplier (× epoch_duration_ms).
    pub txn_deadline_multiplier: u32,
    /// Epoch duration in milliseconds (used to compute deadlines).
    pub epoch_duration_ms: u64,
    /// Timeout for passive participant responses in dependent-read txns.
    ///
    /// Default: `epoch_duration_ms * 3` milliseconds.
    ///
    /// `Instant::now()` is used for barrier timeouts (observability / off-WAL
    /// path only; never influences WAL bytes).
    pub dependent_read_passive_timeout_ms: u64,
    /// Stall deadline for a staged txn parked awaiting the durable global
    /// verdict. On expiry the scheduler re-probes the registry and, if the
    /// verdict is still unknown, emits a stall warning + metric and KEEPS
    /// WAITING (holding locks, never aborting) — a unilateral abort could tear a
    /// cross-shard commit. This is purely a liveness/observability knob: it
    /// controls how often the stall is surfaced, never whether the txn aborts.
    ///
    /// Default: `epoch_duration_ms * 250` milliseconds.
    pub verdict_stall_warn_ms: u64,
    /// In-flight backlog at which the scheduler stops taking new sequenced
    /// input. The backlog counts pending, blocked, and dependent-barrier txns.
    ///
    /// The bound applies only while some backlog txn progresses without new
    /// input. A backlog of blocked txns alone keeps intake open, because a
    /// reservation release they wait on arrives as input.
    ///
    /// Default: [`DATA_PLANE_QUEUE_CAPACITY`]. Every dispatch of one vShard
    /// goes to one Data Plane core, whose queue holds that many requests. A
    /// larger backlog cannot hold a dispatch slot per txn at once.
    pub max_inflight_backlog: usize,
    /// Most sequencer log entries one catch-up drain reads and replays.
    ///
    /// Default: `channel_capacity`. A drop happens only when the fan-out
    /// channel is full, so one window covers about one channel of missed
    /// entries. Windows run back to back while intake is open.
    pub catch_up_window: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        let epoch_duration_ms = 20u64;
        let channel_capacity = 512usize;
        Self {
            channel_capacity,
            txn_deadline_multiplier: 3,
            epoch_duration_ms,
            dependent_read_passive_timeout_ms: epoch_duration_ms * 3,
            verdict_stall_warn_ms: epoch_duration_ms * 250,
            max_inflight_backlog: DATA_PLANE_QUEUE_CAPACITY,
            catch_up_window: channel_capacity as u64,
        }
    }
}

impl SchedulerConfig {
    /// Passive timeout as a `Duration`.
    pub fn passive_timeout(&self) -> Duration {
        Duration::from_millis(self.dependent_read_passive_timeout_ms)
    }

    /// Verdict-wait stall warning interval as a `Duration`.
    pub fn verdict_stall_warn(&self) -> Duration {
        Duration::from_millis(self.verdict_stall_warn_ms)
    }
}
