// SPDX-License-Identifier: BUSL-1.1

//! Query-side partition selection: overlap pruning and the retention expiry
//! scan.

use nodedb_types::timeseries::{PartitionState, TimeRange};

use nodedb_columnar::VersionCut;

use super::entry::{PartitionEntry, PartitionKey};
use super::registry::PartitionRegistry;

/// Whether a read at `cut` sees rows of `entry` past any TRUNCATE end.
pub fn open_at_cut(entry: &PartitionEntry, cut: VersionCut) -> bool {
    match cut {
        VersionCut::Current => entry.meta.open_at(None),
        VersionCut::At(at) => entry.meta.open_at(Some(at)),
        VersionCut::All => true,
    }
}

impl PartitionRegistry {
    /// The partitions overlapping `range` that hold rows a read at `cut`
    /// sees. A partition a TRUNCATE ended at or below the cut holds none:
    /// a current read skips every ended partition, an audit read skips none.
    pub fn query_partitions(&self, range: &TimeRange, cut: VersionCut) -> Vec<&PartitionEntry> {
        self.partitions
            .values()
            .filter(|e| e.meta.is_queryable() && e.meta.overlaps(range) && open_at_cut(e, cut))
            .collect()
    }

    /// The partitions a TRUNCATE has not ended: those a current read sees.
    pub fn current_partitions(&self) -> impl Iterator<Item = &PartitionEntry> {
        self.partitions
            .values()
            .filter(|e| e.meta.state != PartitionState::Deleted && e.meta.ended_at.is_none())
    }

    /// Find partitions eligible for retention drop.
    ///
    /// When `bitemporal` is true, staleness is evaluated against each
    /// millisecond of the partition's `max_system_ts` ordinal (falling back
    /// to `max_ts` when 0 —
    /// partitions written before bitemporal tracking existed). This
    /// lets a late-arriving backfill with old event-time but current
    /// system-time survive the retention window.
    pub fn find_expired(&self, now_ms: i64, bitemporal: bool) -> Vec<PartitionKey> {
        if self.config.retention_period_ms == 0 {
            return Vec::new();
        }
        let cutoff = now_ms - self.config.retention_period_ms as i64;
        self.partitions
            .iter()
            .filter(|(_, e)| {
                let axis_ts = if bitemporal && e.meta.max_system_ts > 0 {
                    nodedb_types::ordinal_to_ms(e.meta.max_system_ts)
                } else {
                    e.meta.max_ts
                };
                axis_ts < cutoff && e.meta.state != PartitionState::Deleted
            })
            .map(|(key, _)| key.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{PartitionInterval, TieredPartitionConfig};

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
    fn query_partitions_pruning() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;
        for d in 1..=10 {
            let (_, _) = reg.get_or_create_partition(d * day_ms);
        }

        // Query days 3-5.
        let range = TimeRange::new(3 * day_ms, 5 * day_ms + day_ms - 1);
        let results = reg.query_partitions(&range, VersionCut::Current);
        assert_eq!(results.len(), 3);
    }

    /// A partition a TRUNCATE ended at 100 answers a read before 100 and an
    /// audit read, and no current read.
    #[test]
    fn an_ended_partition_answers_reads_before_its_end() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;
        let dir = reg.get_or_create_partition(day_ms).0.dir_name.clone();
        reg.get_or_create_partition(2 * day_ms);
        let key = reg.key_of_dir(&dir).expect("registered");
        if let Some(entry) = reg.partitions.get_mut(&key) {
            entry.meta.ended_at = Some(100);
        }
        let range = TimeRange::new(0, 10 * day_ms);
        assert_eq!(reg.query_partitions(&range, VersionCut::Current).len(), 1);
        assert_eq!(reg.query_partitions(&range, VersionCut::At(99)).len(), 2);
        assert_eq!(reg.query_partitions(&range, VersionCut::At(100)).len(), 1);
        assert_eq!(reg.query_partitions(&range, VersionCut::All).len(), 2);
        assert_eq!(reg.current_partitions().count(), 1);
    }

    #[test]
    fn find_expired() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;

        for d in 1..=5 {
            let start = d * day_ms;
            let dir = reg.get_or_create_partition(start).0.dir_name.clone();
            let key = reg.key_of_dir(&dir).expect("registered");
            // Manually set max_ts so retention check works.
            if let Some(entry) = reg.partitions.get_mut(&key) {
                entry.meta.max_ts = start + day_ms - 1;
            }
        }

        // 40 days later, retention=30d → days 1-9 expired (but only 1-5 exist).
        let now = 40 * day_ms;
        let expired = reg.find_expired(now, false);
        assert_eq!(expired.len(), 5);
    }
}
