// SPDX-License-Identifier: BUSL-1.1

//! PartitionRegistry core — struct definition, construction, partition creation,
//! interval management, and basic accessors.

use std::collections::BTreeMap;

use nodedb_types::timeseries::{
    PartitionInterval, PartitionMeta, PartitionState, TieredPartitionConfig,
};

use super::entry::{PartitionEntry, format_partition_dir};
use super::rate::RateEstimator;

/// Registry of all partitions for one timeseries collection.
pub struct PartitionRegistry {
    /// Partitions keyed by start timestamp.
    pub(super) partitions: BTreeMap<i64, PartitionEntry>,
    /// Current effective interval (may differ from config if AUTO + adapted).
    pub(super) current_interval: PartitionInterval,
    /// Rate estimator for AUTO mode.
    pub(super) rate_estimator: RateEstimator,
    /// Collection config.
    pub(super) config: TieredPartitionConfig,
}

impl PartitionRegistry {
    pub fn new(config: TieredPartitionConfig) -> Self {
        let current_interval = match &config.partition_by {
            PartitionInterval::Auto => PartitionInterval::Duration(86_400_000), // start at 1d
            other => other.clone(),
        };
        Self {
            partitions: BTreeMap::new(),
            current_interval,
            rate_estimator: RateEstimator::new(),
            config,
        }
    }

    /// Get or create a partition for a given timestamp.
    ///
    /// Returns the partition directory name and whether it's new.
    pub fn get_or_create_partition(&mut self, timestamp_ms: i64) -> (&PartitionEntry, bool) {
        let (start, end) = self.partition_boundaries(timestamp_ms);

        use std::collections::btree_map::Entry;
        match self.partitions.entry(start) {
            Entry::Occupied(e) => (e.into_mut(), false),
            Entry::Vacant(e) => {
                let dir_name = format_partition_dir(start, end);
                let entry = PartitionEntry {
                    meta: PartitionMeta {
                        min_ts: timestamp_ms,
                        max_ts: timestamp_ms,
                        row_count: 0,
                        size_bytes: 0,
                        schema_version: 1,
                        state: PartitionState::Active,
                        interval_ms: self.current_interval.as_millis().unwrap_or(0),
                        last_flushed_wal_lsn: 0,
                        column_stats: std::collections::HashMap::new(),
                        max_system_ts: 0,
                    },
                    dir_name,
                };
                (e.insert(entry), true)
            }
        }
    }

    /// Compute partition boundaries (start_ms, end_ms) for a given timestamp.
    fn partition_boundaries(&self, timestamp_ms: i64) -> (i64, i64) {
        match &self.current_interval {
            PartitionInterval::Duration(ms) => {
                let ms = *ms as i64;
                let start = (timestamp_ms / ms) * ms;
                (start, start + ms)
            }
            PartitionInterval::Month => {
                // Align to calendar month start/end (simplified: 30d).
                let approx_month_ms = 30 * 86_400_000i64;
                let start = (timestamp_ms / approx_month_ms) * approx_month_ms;
                (start, start + approx_month_ms)
            }
            PartitionInterval::Year => {
                let approx_year_ms = 365 * 86_400_000i64;
                let start = (timestamp_ms / approx_year_ms) * approx_year_ms;
                (start, start + approx_year_ms)
            }
            PartitionInterval::Unbounded => {
                // Single partition: start=0, end=MAX.
                (0, i64::MAX)
            }
            PartitionInterval::Auto => {
                // Should have been resolved to a concrete interval in new().
                let ms = 86_400_000i64; // fallback 1d
                let start = (timestamp_ms / ms) * ms;
                (start, start + ms)
            }
            _ => {
                let ms = 86_400_000i64;
                let start = (timestamp_ms / ms) * ms;
                (start, start + ms)
            }
        }
    }

    /// Record ingest for adaptive interval algorithm.
    pub fn record_ingest(&mut self, row_count: u64, now_ms: i64) {
        self.rate_estimator.record(row_count, now_ms);
    }

    /// Seal a partition (mark immutable).
    pub fn seal_partition(&mut self, start_ts: i64) -> bool {
        if let Some(entry) = self.partitions.get_mut(&start_ts)
            && entry.meta.state == PartitionState::Active
        {
            entry.meta.state = PartitionState::Sealed;

            // AUTO mode: check size floor after sealing.
            if matches!(self.config.partition_by, PartitionInterval::Auto)
                && entry.meta.row_count < 1000
            {
                self.widen_interval();
            }
            return true;
        }
        false
    }

    /// Double the current interval (AUTO mode size floor promotion).
    fn widen_interval(&mut self) {
        self.current_interval = match &self.current_interval {
            PartitionInterval::Duration(ms) => {
                let doubled = ms * 2;
                if doubled >= 30 * 86_400_000 {
                    PartitionInterval::Month
                } else {
                    PartitionInterval::Duration(doubled)
                }
            }
            PartitionInterval::Month => PartitionInterval::Year,
            PartitionInterval::Year | PartitionInterval::Unbounded => PartitionInterval::Unbounded,
            other => other.clone(),
        };
    }

    /// Check if AUTO mode should narrow the interval (rate increased).
    pub fn maybe_narrow_interval(&mut self) {
        if !matches!(self.config.partition_by, PartitionInterval::Auto) {
            return;
        }
        let suggested = self.rate_estimator.suggest_interval();
        if let (Some(suggested_ms), Some(current_ms)) =
            (suggested.as_millis(), self.current_interval.as_millis())
            && suggested_ms < current_ms
        {
            self.current_interval = suggested;
        }
    }

    /// Update a partition's metadata (e.g., after flush updates row_count).
    pub fn update_meta(&mut self, start_ts: i64, meta: PartitionMeta) {
        if let Some(entry) = self.partitions.get_mut(&start_ts) {
            entry.meta = meta;
        }
    }

    /// Change the partition interval (online ALTER).
    /// Only affects new partitions — existing partitions keep their width.
    pub fn set_partition_interval(&mut self, interval: PartitionInterval) {
        self.current_interval = interval.clone();
        self.config.partition_by = interval;
    }

    // -- Accessors --

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Rows across every registered partition.
    pub fn total_row_count(&self) -> u64 {
        self.partitions.values().map(|e| e.meta.row_count).sum()
    }

    pub fn active_count(&self) -> usize {
        self.partitions
            .values()
            .filter(|e| e.meta.state == PartitionState::Active)
            .count()
    }

    pub fn sealed_count(&self) -> usize {
        self.partitions
            .values()
            .filter(|e| e.meta.state == PartitionState::Sealed)
            .count()
    }

    pub fn current_interval(&self) -> &PartitionInterval {
        &self.current_interval
    }

    pub fn rate(&self) -> f64 {
        self.rate_estimator.rate()
    }

    pub fn get(&self, start_ts: i64) -> Option<&PartitionEntry> {
        self.partitions.get(&start_ts)
    }

    /// Mutable access to a partition entry (for merge/compaction state updates).
    pub fn get_mut(&mut self, start_ts: i64) -> Option<&mut PartitionEntry> {
        self.partitions.get_mut(&start_ts)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&i64, &PartitionEntry)> {
        self.partitions.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> TieredPartitionConfig {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000); // 1d
        cfg.merge_after_ms = 7 * 86_400_000;
        cfg.merge_count = 3;
        cfg.retention_period_ms = 30 * 86_400_000;
        cfg
    }

    #[test]
    fn create_partition() {
        let mut reg = PartitionRegistry::new(test_config());
        let (entry, is_new) = reg.get_or_create_partition(86_400_000 * 5 + 1000);
        assert!(is_new);
        assert_eq!(entry.meta.state, PartitionState::Active);
        assert!(entry.dir_name.starts_with("ts-"));

        // Same timestamp range → same partition.
        let (_, is_new2) = reg.get_or_create_partition(86_400_000 * 5 + 2000);
        assert!(!is_new2);
        assert_eq!(reg.partition_count(), 1);
    }

    #[test]
    fn different_days_different_partitions() {
        let mut reg = PartitionRegistry::new(test_config());
        reg.get_or_create_partition(86_400_000); // day 1
        reg.get_or_create_partition(86_400_000 * 2); // day 2
        reg.get_or_create_partition(86_400_000 * 3); // day 3
        assert_eq!(reg.partition_count(), 3);
    }

    #[test]
    fn seal_partition() {
        let mut reg = PartitionRegistry::new(test_config());
        let day1_start = 86_400_000i64;
        reg.get_or_create_partition(day1_start);
        assert_eq!(reg.active_count(), 1);

        assert!(reg.seal_partition(day1_start));
        assert_eq!(reg.active_count(), 0);
        assert_eq!(reg.sealed_count(), 1);
    }

    #[test]
    fn auto_mode_widen_on_small_partition() {
        let mut cfg = test_config();
        cfg.partition_by = PartitionInterval::Auto;
        let mut reg = PartitionRegistry::new(cfg);

        // Start at 1d.
        assert_eq!(reg.current_interval().as_millis(), Some(86_400_000));

        // Create and seal a partition with < 1000 rows.
        let start = 86_400_000i64;
        reg.get_or_create_partition(start);
        if let Some(entry) = reg.partitions.get_mut(&start) {
            entry.meta.row_count = 50;
        }
        reg.seal_partition(start);

        // Interval should have doubled to 2d.
        assert_eq!(reg.current_interval().as_millis(), Some(2 * 86_400_000));
    }

    #[test]
    fn set_partition_interval_online() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;

        // Create some 1d partitions.
        reg.get_or_create_partition(day_ms);
        reg.get_or_create_partition(2 * day_ms);

        // Change to 3d.
        reg.set_partition_interval(PartitionInterval::Duration(3 * day_ms as u64));
        assert_eq!(reg.current_interval().as_millis(), Some(3 * 86_400_000));

        // New partition uses 3d boundaries.
        reg.get_or_create_partition(10 * day_ms);
        assert_eq!(reg.partition_count(), 3);
    }

    #[test]
    fn unbounded_partition() {
        let mut cfg = test_config();
        cfg.partition_by = PartitionInterval::Unbounded;
        let mut reg = PartitionRegistry::new(cfg);

        reg.get_or_create_partition(1000);
        reg.get_or_create_partition(999_999_999);
        // All go to the same unbounded partition.
        assert_eq!(reg.partition_count(), 1);
    }
}
