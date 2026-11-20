// SPDX-License-Identifier: BUSL-1.1

//! The checkpoint manager's intervals.

use std::time::Duration;

/// Configuration for the checkpoint manager. Both intervals are non-zero by
/// construction: a zero period makes the tick timer panic.
#[derive(Debug, Clone)]
pub struct CheckpointManagerConfig {
    interval: Duration,
    core_timeout: Duration,
    archive_interval: Duration,
}

impl CheckpointManagerConfig {
    /// Fails with `Error::Config` naming the setting when an interval is zero.
    pub fn new(
        interval: Duration,
        core_timeout: Duration,
        archive_interval: Duration,
    ) -> crate::Result<Self> {
        for (value, field) in [
            (interval, "checkpoint.interval_secs"),
            (archive_interval, "checkpoint.wal_archive_interval_secs"),
        ] {
            if value.is_zero() {
                return Err(crate::Error::Config {
                    detail: format!("invalid value '0' for {field}: expected a positive integer"),
                });
            }
        }
        Ok(Self {
            interval,
            core_timeout,
            archive_interval,
        })
    }

    /// Interval between checkpoint cycles.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Timeout for individual core checkpoint responses.
    pub fn core_timeout(&self) -> Duration {
        self.core_timeout
    }

    /// Interval between WAL archive passes. Bounds the archive lag of a
    /// sealed segment.
    pub fn archive_interval(&self) -> Duration {
        self.archive_interval
    }
}

impl Default for CheckpointManagerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(300),
            core_timeout: Duration::from_secs(30),
            archive_interval: Duration::from_secs(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_intervals_are_config_errors_not_panics() {
        let s = Duration::from_secs(1);
        let zero = Duration::ZERO;
        for (interval, archive, field) in [
            (zero, s, "checkpoint.interval_secs"),
            (s, zero, "checkpoint.wal_archive_interval_secs"),
        ] {
            let err = CheckpointManagerConfig::new(interval, s, archive).unwrap_err();
            assert!(matches!(err, crate::Error::Config { .. }), "{err}");
            assert!(err.to_string().contains(field), "{err}");
        }
        assert!(CheckpointManagerConfig::new(s, s, s).is_ok());
    }
}
