use nodedb_types::timeseries::{
    PartitionInterval, PartitionMeta, PartitionState, TieredPartitionConfig, TimeRange,
};

use super::entry::format_partition_dir;
use super::rate::RateEstimator;
use super::registry::PartitionRegistry;

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
fn query_partitions_pruning() {
    let mut reg = PartitionRegistry::new(test_config());
    let day_ms = 86_400_000i64;
    for d in 1..=10 {
        let (_, _) = reg.get_or_create_partition(d * day_ms);
    }

    // Query days 3-5.
    let range = TimeRange::new(3 * day_ms, 5 * day_ms + day_ms - 1);
    let results = reg.query_partitions(&range);
    assert_eq!(results.len(), 3);
}

#[test]
fn find_mergeable() {
    let mut reg = PartitionRegistry::new(test_config());
    let day_ms = 86_400_000i64;

    // Create and seal 6 partitions.
    for d in 1..=6 {
        reg.get_or_create_partition(d * day_ms);
        reg.seal_partition(d * day_ms);
    }

    // None mergeable yet (merge_after = 7d, data is "today").
    let now = 7 * day_ms;
    assert!(reg.find_mergeable(now).is_empty());

    // 15 days later, all are old enough. merge_count=3 → 2 groups.
    let now = 22 * day_ms;
    let groups = reg.find_mergeable(now);
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].len(), 3);
}

#[test]
fn find_expired() {
    let mut reg = PartitionRegistry::new(test_config());
    let day_ms = 86_400_000i64;

    for d in 1..=5 {
        let start = d * day_ms;
        reg.get_or_create_partition(start);
        // Manually set max_ts so retention check works.
        if let Some(entry) = reg.partitions.get_mut(&start) {
            entry.meta.max_ts = start + day_ms - 1;
        }
    }

    // 40 days later, retention=30d → days 1-9 expired (but only 1-5 exist).
    let now = 40 * day_ms;
    let expired = reg.find_expired(now, false);
    assert_eq!(expired.len(), 5);
}

#[test]
fn commit_merge_and_purge() {
    let mut reg = PartitionRegistry::new(test_config());
    let day_ms = 86_400_000i64;

    let starts: Vec<i64> = (1..=3).map(|d| d * day_ms).collect();
    for &s in &starts {
        reg.get_or_create_partition(s);
        reg.seal_partition(s);
    }

    // Merge.
    for &s in &starts {
        reg.mark_merging(s);
    }

    let merged_meta = PartitionMeta {
        min_ts: starts[0],
        max_ts: starts[2] + day_ms - 1,
        row_count: 3000,
        size_bytes: 1024,
        schema_version: 1,
        state: PartitionState::Merged,
        interval_ms: 3 * day_ms as u64,
        last_flushed_wal_lsn: 100,
        column_stats: std::collections::HashMap::new(),
        max_system_ts: 0,
    };
    reg.commit_merge(merged_meta, "ts-merged".into(), &starts);

    // Sources are deleted, merged exists. The merged partition's min_ts
    // equals starts[0], so it overwrites one deleted entry → 3 total
    // (1 merged at starts[0], 2 deleted at starts[1] and starts[2]).
    assert_eq!(reg.partition_count(), 3);
    let dirs = reg.purge_deleted();
    assert_eq!(dirs.len(), 2); // starts[1] and starts[2]
    assert_eq!(reg.partition_count(), 1); // only the merged partition
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
fn rate_estimator_basic() {
    let mut est = RateEstimator::new();
    // Simulate 1000 rows/sec for 5 seconds.
    for i in 0..5 {
        est.record(1000, i * 1000);
    }
    // Rate should be approaching 1000.
    assert!(est.rate() > 100.0); // EWMA takes time to converge.
}

#[test]
fn format_partition_dir_test() {
    let dir = format_partition_dir(1_704_067_200_000, 1_704_153_600_000);
    // 2024-01-01 00:00:00 to 2024-01-02 00:00:00
    assert_eq!(dir, "ts-20240101-000000_20240102-000000");
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
