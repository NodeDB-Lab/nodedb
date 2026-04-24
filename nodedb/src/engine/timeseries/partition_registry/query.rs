//! Query-side partition selection: overlap pruning, merge candidate selection,
//! retention expiry scan.

use nodedb_types::timeseries::{PartitionState, TimeRange};

use super::entry::PartitionEntry;
use super::registry::PartitionRegistry;

impl PartitionRegistry {
    /// Find partitions that overlap a time range (for queries).
    pub fn query_partitions(&self, range: &TimeRange) -> Vec<&PartitionEntry> {
        self.partitions
            .values()
            .filter(|e| e.meta.is_queryable() && e.meta.overlaps(range))
            .collect()
    }

    /// Find partitions eligible for merging.
    ///
    /// Returns groups of `merge_count` consecutive sealed partitions
    /// that are all older than `merge_after` relative to `now_ms`.
    pub fn find_mergeable(&self, now_ms: i64) -> Vec<Vec<i64>> {
        let merge_after = self.config.merge_after_ms as i64;
        let merge_count = self.config.merge_count as usize;

        let sealed: Vec<i64> = self
            .partitions
            .iter()
            .filter(|(_, e)| {
                e.meta.state == PartitionState::Sealed && (now_ms - e.meta.max_ts) > merge_after
            })
            .map(|(&start, _)| start)
            .collect();

        let mut groups = Vec::new();
        let mut i = 0;
        while i + merge_count <= sealed.len() {
            groups.push(sealed[i..i + merge_count].to_vec());
            i += merge_count;
        }
        groups
    }

    /// Find partitions eligible for retention drop.
    ///
    /// When `bitemporal` is true, staleness is evaluated against each
    /// partition's `max_system_ts` (falling back to `max_ts` when 0 —
    /// partitions written before bitemporal tracking existed). This
    /// lets a late-arriving backfill with old event-time but current
    /// system-time survive the retention window.
    pub fn find_expired(&self, now_ms: i64, bitemporal: bool) -> Vec<i64> {
        if self.config.retention_period_ms == 0 {
            return Vec::new();
        }
        let cutoff = now_ms - self.config.retention_period_ms as i64;
        self.partitions
            .iter()
            .filter(|(_, e)| {
                let axis_ts = if bitemporal && e.meta.max_system_ts > 0 {
                    e.meta.max_system_ts
                } else {
                    e.meta.max_ts
                };
                axis_ts < cutoff && e.meta.state != PartitionState::Deleted
            })
            .map(|(&start, _)| start)
            .collect()
    }
}
