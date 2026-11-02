// SPDX-License-Identifier: Apache-2.0

//! Version cleanup tuning: the cadence and batch size of the background pass
//! that removes superseded row versions below the cleanup horizon.

use std::time::Duration;

use serde::{Deserialize, Serialize};

fn default_interval_ms() -> u64 {
    10_000
}

fn default_batch_units() -> u32 {
    64
}

/// Tuning of the version cleanup pass.
///
/// Every engine keeps a superseded version until the cleanup horizon passes
/// it. Each pass reads the horizon and purges every core below it. A core
/// purges at most `batch_units` stores (one engine of one collection each)
/// per request, so other requests run between the batches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionCleanupTuning {
    /// Interval between cleanup passes, in milliseconds. `0` stops the pass,
    /// and history then grows without bound.
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,

    /// Stores one core purges per request. `0` reads as `1`.
    #[serde(default = "default_batch_units")]
    pub batch_units: u32,
}

impl VersionCleanupTuning {
    /// The interval between passes. `None` when the pass is off.
    pub fn interval(&self) -> Option<Duration> {
        (self.interval_ms > 0).then(|| Duration::from_millis(self.interval_ms))
    }

    /// Stores one core purges per request, at least one.
    pub fn batch(&self) -> u32 {
        self.batch_units.max(1)
    }
}

impl Default for VersionCleanupTuning {
    fn default() -> Self {
        Self {
            interval_ms: default_interval_ms(),
            batch_units: default_batch_units(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_run_every_ten_seconds_in_batches_of_64() {
        let tuning = VersionCleanupTuning::default();
        assert_eq!(tuning.interval(), Some(Duration::from_secs(10)));
        assert_eq!(tuning.batch(), 64);
    }

    #[test]
    fn zero_interval_stops_the_pass_and_zero_batch_reads_as_one() {
        let tuning: VersionCleanupTuning =
            toml::from_str("interval_ms = 0\nbatch_units = 0").expect("deserialize");
        assert_eq!(tuning.interval(), None);
        assert_eq!(tuning.batch(), 1);
    }

    #[test]
    fn empty_table_keeps_the_defaults() {
        let tuning: VersionCleanupTuning = toml::from_str("").expect("deserialize");
        assert_eq!(tuning.interval_ms, 10_000);
        assert_eq!(tuning.batch_units, 64);
    }
}
