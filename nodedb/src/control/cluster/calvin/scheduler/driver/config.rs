// SPDX-License-Identifier: BUSL-1.1

//! Configuration for the Calvin scheduler driver.
//!
//! The `[tuning.calvin]` section sets every knob except the epoch duration,
//! which comes from the sequencer config.

use std::time::Duration;

use nodedb_cluster::calvin::SequencerConfig;
use nodedb_types::config::tuning::CalvinTuning;

/// Tuning parameters for a [`super::core::Scheduler`] instance.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Bounded channel capacity for incoming [`nodedb_cluster::calvin::types::SequencedTxn`]s.
    pub channel_capacity: usize,
    /// Transaction deadline multiplier (× epoch_duration_ms).
    pub txn_deadline_multiplier: u32,
    /// Epoch duration in milliseconds (used to compute deadlines). Equal to
    /// the sequencer's epoch duration.
    pub epoch_duration_ms: u64,
    /// Timeout for passive participant responses in dependent-read txns.
    ///
    /// Default: 60 milliseconds, three default epochs.
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
    /// Default: 5000 milliseconds, 250 default epochs.
    pub verdict_stall_warn_ms: u64,
    /// In-flight backlog at which the scheduler stops taking new sequenced
    /// input. The backlog counts pending, blocked, and dependent-barrier txns.
    ///
    /// The bound applies only while some backlog txn progresses without new
    /// input. A backlog of blocked txns alone keeps intake open, because a
    /// reservation release they wait on arrives as input.
    ///
    /// Default: [`crate::bridge::dispatch::DATA_PLANE_QUEUE_CAPACITY`]. Every
    /// dispatch of one vShard goes to one Data Plane core, whose queue holds
    /// that many requests. A larger backlog cannot hold a dispatch slot per
    /// txn at once.
    pub max_inflight_backlog: usize,
    /// Most sequencer log entries one catch-up drain reads and replays.
    ///
    /// Default: 512, the default `channel_capacity`. A drop happens only when
    /// the fan-out channel is full, so one window covers about one channel of
    /// missed entries. Windows run back to back while intake is open.
    pub catch_up_window: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self::from_tuning(&CalvinTuning::default(), &SequencerConfig::default())
    }
}

impl SchedulerConfig {
    /// The scheduler config `tuning` sets, on the epoch duration of the
    /// sequencer that feeds the scheduler.
    pub fn from_tuning(tuning: &CalvinTuning, sequencer: &SequencerConfig) -> Self {
        Self {
            channel_capacity: tuning.channel_capacity,
            txn_deadline_multiplier: tuning.txn_deadline_multiplier,
            epoch_duration_ms: u64::try_from(sequencer.epoch_duration.as_millis())
                .unwrap_or(u64::MAX),
            dependent_read_passive_timeout_ms: tuning.dependent_read_passive_timeout_ms,
            verdict_stall_warn_ms: tuning.verdict_stall_warn_ms,
            max_inflight_backlog: tuning.max_inflight_backlog,
            catch_up_window: tuning.catch_up_window,
        }
    }

    /// Passive timeout as a `Duration`.
    pub fn passive_timeout(&self) -> Duration {
        Duration::from_millis(self.dependent_read_passive_timeout_ms)
    }

    /// Verdict-wait stall warning interval as a `Duration`.
    pub fn verdict_stall_warn(&self) -> Duration {
        Duration::from_millis(self.verdict_stall_warn_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::DATA_PLANE_QUEUE_CAPACITY;

    #[test]
    fn the_default_tuning_builds_the_shipped_scheduler_config() {
        let config = SchedulerConfig::default();
        assert_eq!(config.channel_capacity, 512);
        assert_eq!(config.txn_deadline_multiplier, 3);
        assert_eq!(config.epoch_duration_ms, 20);
        assert_eq!(config.dependent_read_passive_timeout_ms, 60);
        assert_eq!(config.verdict_stall_warn_ms, 5_000);
        assert_eq!(config.catch_up_window, 512);
    }

    /// The tuning default restates the dispatcher queue capacity, which the
    /// tuning crate cannot import. The two must not drift apart.
    #[test]
    fn the_default_backlog_is_one_dispatcher_queue() {
        assert_eq!(
            CalvinTuning::default().max_inflight_backlog,
            DATA_PLANE_QUEUE_CAPACITY
        );
        assert_eq!(
            SchedulerConfig::default().max_inflight_backlog,
            DATA_PLANE_QUEUE_CAPACITY
        );
    }

    #[test]
    fn tuning_overrides_reach_the_scheduler_config() {
        let tuning = CalvinTuning {
            max_inflight_backlog: 128,
            verdict_stall_warn_ms: 9_000,
            ..CalvinTuning::default()
        };
        let sequencer = SequencerConfig {
            epoch_duration: Duration::from_millis(50),
            ..SequencerConfig::default()
        };
        let config = SchedulerConfig::from_tuning(&tuning, &sequencer);
        assert_eq!(config.max_inflight_backlog, 128);
        assert_eq!(config.verdict_stall_warn(), Duration::from_millis(9_000));
        assert_eq!(config.epoch_duration_ms, 50);
    }
}
