// SPDX-License-Identifier: BUSL-1.1

//! Configuration for the Calvin scheduler driver.
//!
//! The `[tuning.calvin]` section sets every knob except the epoch duration,
//! which comes from the sequencer config.

use std::time::Duration;

use nodedb_cluster::calvin::SequencerConfig;
use nodedb_types::config::tuning::CalvinTuning;

/// The longest wait between two restages of one committed txn.
pub const MAX_RESTAGE_BACKOFF: Duration = Duration::from_secs(60);

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
    /// Wait of a dependent-read barrier for its passive read results before
    /// the data-group leader proposes the barrier's timeout entry.
    ///
    /// Default: 5000 milliseconds, 250 default epochs.
    ///
    /// The leader's clock decides only when it proposes the entry. The
    /// entry's place in the log decides the barrier.
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
    /// Most restages of a committed txn whose stage failed on this leader.
    /// The scheduler halts once they run out.
    ///
    /// Default: 5.
    pub restage_attempts: u32,
    /// Wait before the first restage of a committed txn. Each later restage
    /// waits twice as long. The wait only delays a retry of a decided
    /// verdict, so it never changes what any replica applies.
    ///
    /// Default: 100 milliseconds, five default epochs.
    pub restage_backoff_ms: u64,
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
            restage_attempts: tuning.restage_attempts,
            restage_backoff_ms: tuning.restage_backoff_ms,
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

    /// The wait before restage number `done + 1`, after `done` restages:
    /// the base wait doubled once per restage done, at most
    /// [`MAX_RESTAGE_BACKOFF`].
    pub fn restage_backoff(&self, done: u32) -> Duration {
        let factor = 1u64.checked_shl(done).unwrap_or(u64::MAX);
        Duration::from_millis(self.restage_backoff_ms.saturating_mul(factor))
            .min(MAX_RESTAGE_BACKOFF)
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
        assert_eq!(config.dependent_read_passive_timeout_ms, 5_000);
        assert_eq!(config.verdict_stall_warn_ms, 5_000);
        assert_eq!(config.catch_up_window, 512);
        assert_eq!(config.restage_attempts, 5);
        assert_eq!(config.restage_backoff_ms, 100);
    }

    /// Each restage waits twice as long as the one before, up to the cap.
    /// A count past the shift width saturates instead of wrapping.
    #[test]
    fn the_restage_backoff_doubles_per_restage() {
        let config = SchedulerConfig::default();
        assert_eq!(config.restage_backoff(0), Duration::from_millis(100));
        assert_eq!(config.restage_backoff(1), Duration::from_millis(200));
        assert_eq!(config.restage_backoff(4), Duration::from_millis(1_600));
        assert_eq!(config.restage_backoff(20), MAX_RESTAGE_BACKOFF);
        assert_eq!(config.restage_backoff(64), MAX_RESTAGE_BACKOFF);
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
            restage_attempts: 2,
            restage_backoff_ms: 7,
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
        assert_eq!(config.restage_attempts, 2);
        assert_eq!(config.restage_backoff(0), Duration::from_millis(7));
    }
}
