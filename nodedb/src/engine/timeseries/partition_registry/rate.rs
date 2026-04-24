//! Exponentially weighted moving average rate estimator for AUTO interval mode.

use nodedb_types::timeseries::PartitionInterval;

/// Exponentially weighted moving average rate estimator.
#[derive(Debug, Clone)]
pub struct RateEstimator {
    /// Current estimated rows per second.
    rate: f64,
    /// Smoothing factor (0..1). Higher = more weight to recent samples.
    alpha: f64,
    /// Last update timestamp (ms).
    last_update_ms: i64,
    /// Rows accumulated since last update.
    rows_since_update: u64,
}

impl RateEstimator {
    pub fn new() -> Self {
        Self {
            rate: 0.0,
            alpha: 0.1,
            last_update_ms: 0,
            rows_since_update: 0,
        }
    }

    /// Record `n` rows ingested at timestamp `now_ms`.
    pub fn record(&mut self, n: u64, now_ms: i64) {
        if self.last_update_ms == 0 {
            self.last_update_ms = now_ms;
            self.rows_since_update = n;
            return;
        }

        self.rows_since_update += n;
        let elapsed_ms = now_ms - self.last_update_ms;

        // Update every second.
        if elapsed_ms >= 1000 {
            let elapsed_s = elapsed_ms as f64 / 1000.0;
            let instant_rate = self.rows_since_update as f64 / elapsed_s;
            self.rate = self.alpha * instant_rate + (1.0 - self.alpha) * self.rate;
            self.last_update_ms = now_ms;
            self.rows_since_update = 0;
        }
    }

    /// Current estimated rows/second.
    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// Select an interval based on current rate.
    pub fn suggest_interval(&self) -> PartitionInterval {
        let r = self.rate;
        if r > 100_000.0 {
            PartitionInterval::Duration(3_600_000) // 1h
        } else if r > 1_000.0 {
            PartitionInterval::Duration(86_400_000) // 1d
        } else if r > 10.0 {
            PartitionInterval::Duration(604_800_000) // 1w
        } else if r > 0.1 {
            PartitionInterval::Month
        } else {
            PartitionInterval::Unbounded
        }
    }
}

impl Default for RateEstimator {
    fn default() -> Self {
        Self::new()
    }
}
