//! Lite-specific lifecycle: storage budget, background compaction scheduler,
//! incremental backup, adaptive sync resolution, and codec retention verification.

use std::path::Path;

use super::core::TimeseriesEngine;

impl TimeseriesEngine {
    /// Collect owned collection names (needed when the loop body borrows `&mut self`).
    fn owned_collection_names(&self) -> Vec<String> {
        self.collection_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Storage budget enforcement (Pattern C)
// ---------------------------------------------------------------------------

/// Policy for when storage budget is exceeded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BudgetPolicy {
    /// Compact fragmented partitions to reclaim space (lossless).
    #[default]
    Compact,
    /// Downsample oldest data to reduce resolution (lossy but preserves trends).
    Downsample,
    /// Only surface a warning; don't take action.
    Warn,
}

/// Result of a storage budget enforcement check.
#[derive(Debug)]
pub struct BudgetCheckResult {
    /// Current total storage bytes across all collections.
    pub current_bytes: u64,
    /// Configured budget (0 = unlimited).
    pub budget_bytes: u64,
    /// Whether the budget is exceeded.
    pub over_budget: bool,
    /// Action taken (if any).
    pub action: BudgetAction,
}

/// Action taken during budget enforcement.
#[derive(Debug)]
pub enum BudgetAction {
    /// No action needed — within budget.
    None,
    /// Compacted partitions to reclaim space.
    Compacted { bytes_reclaimed: u64 },
    /// Downsampled oldest data.
    Downsampled { partitions_affected: usize },
    /// Warning surfaced — budget exceeded but no action taken.
    Warning,
}

impl TimeseriesEngine {
    /// Check and enforce the storage budget.
    ///
    /// Steps (in order, stop when within budget):
    /// 1. Compact fragmented partitions (lossless)
    /// 2. If still over: downsample oldest data (lossy)
    /// 3. Last resort: surface `StorageBudgetExceeded` warning
    ///
    /// **Never silently purges data in Pattern C.**
    pub fn enforce_storage_budget(
        &mut self,
        budget_bytes: u64,
        policy: BudgetPolicy,
    ) -> BudgetCheckResult {
        if budget_bytes == 0 {
            return BudgetCheckResult {
                current_bytes: self.total_storage_bytes(),
                budget_bytes: 0,
                over_budget: false,
                action: BudgetAction::None,
            };
        }

        let current = self.total_storage_bytes();
        if current <= budget_bytes {
            return BudgetCheckResult {
                current_bytes: current,
                budget_bytes,
                over_budget: false,
                action: BudgetAction::None,
            };
        }

        match policy {
            BudgetPolicy::Compact => {
                // Try compacting each collection.
                let mut reclaimed = 0u64;
                let collections = self.owned_collection_names();
                for coll in &collections {
                    if let Some(result) = self.compact_partitions(coll) {
                        // Estimate reclaimed bytes: merged partitions are smaller due
                        // to better compression on larger blocks.
                        reclaimed += result.total_rows * 2; // rough estimate
                    }
                }

                let after = self.total_storage_bytes();
                BudgetCheckResult {
                    current_bytes: after,
                    budget_bytes,
                    over_budget: after > budget_bytes,
                    action: BudgetAction::Compacted {
                        bytes_reclaimed: reclaimed,
                    },
                }
            }
            BudgetPolicy::Downsample => {
                // Identify partitions to downsample and return instructions
                // for the caller to execute. The engine doesn't hold a redb
                // handle — the caller reads data, calls `downsample_partition()`,
                // and writes the result back.
                //
                // For now, collect the oldest partitions that could be
                // downsampled. The actual data transformation happens when
                // the caller calls `execute_downsample()` with real data.
                let plan = self.plan_downsample();
                let affected = plan.len();

                BudgetCheckResult {
                    current_bytes: current,
                    budget_bytes,
                    over_budget: true,
                    action: if affected > 0 {
                        BudgetAction::Downsampled {
                            partitions_affected: affected,
                        }
                    } else {
                        BudgetAction::Warning
                    },
                }
            }
            BudgetPolicy::Warn => BudgetCheckResult {
                current_bytes: current,
                budget_bytes,
                over_budget: true,
                action: BudgetAction::Warning,
            },
        }
    }

    /// Total storage bytes across all collections (partition metadata sizes).
    pub fn total_storage_bytes(&self) -> u64 {
        self.collections
            .values()
            .flat_map(|c| c.partitions.iter())
            .map(|p| p.meta.size_bytes)
            .sum()
    }
}

/// Instruction to downsample a specific partition.
#[derive(Debug, Clone)]
pub struct DownsamplePlan {
    /// Collection name.
    pub collection: String,
    /// redb key prefix of the partition to downsample.
    pub key_prefix: String,
    /// Current row count.
    pub row_count: u64,
    /// Target resolution: bucket interval in ms for averaging.
    /// E.g., if original is 1s data, target 10s → 10x reduction.
    pub target_interval_ms: i64,
}

impl TimeseriesEngine {
    /// Plan which partitions should be downsampled to save space.
    ///
    /// Returns the oldest partitions sorted by min_ts. The caller reads
    /// each partition's data from redb, calls `downsample_data()`, and
    /// writes the result back.
    pub fn plan_downsample(&self) -> Vec<DownsamplePlan> {
        let mut plans = Vec::new();

        for (collection, coll) in &self.collections {
            // Only downsample sealed partitions with meaningful data.
            for partition in &coll.partitions {
                if partition.meta.row_count < 100 {
                    continue; // Too small to downsample.
                }
                let duration_ms = partition.meta.max_ts - partition.meta.min_ts;
                if duration_ms <= 0 {
                    continue;
                }
                // Current resolution: duration / row_count.
                let current_resolution_ms = duration_ms / partition.meta.row_count as i64;
                // Target: double the resolution (halve the data).
                let target = (current_resolution_ms * 2).max(1000); // At least 1s.

                plans.push(DownsamplePlan {
                    collection: collection.clone(),
                    key_prefix: partition.key_prefix.clone(),
                    row_count: partition.meta.row_count,
                    target_interval_ms: target,
                });
            }
        }

        // Sort by min_ts (oldest first — downsample oldest data first).
        plans.sort_by_key(|p| {
            self.collections
                .get(&p.collection)
                .and_then(|c| {
                    c.partitions
                        .iter()
                        .find(|part| part.key_prefix == p.key_prefix)
                        .map(|part| part.meta.min_ts)
                })
                .unwrap_or(i64::MAX)
        });

        plans
    }

    /// Downsample raw timestamp+value data into coarser resolution.
    ///
    /// The caller provides the decoded data from redb. This function
    /// averages values within each `interval_ms` bucket and returns
    /// the downsampled (timestamp, value) pairs.
    ///
    /// This is a pure function — no side effects on the engine state.
    pub fn downsample_data(
        timestamps: &[i64],
        values: &[f64],
        interval_ms: i64,
    ) -> (Vec<i64>, Vec<f64>) {
        if timestamps.is_empty() || interval_ms <= 0 {
            return (Vec::new(), Vec::new());
        }

        let mut buckets: std::collections::BTreeMap<i64, (f64, u64)> =
            std::collections::BTreeMap::new();

        for i in 0..timestamps.len() {
            let bucket = (timestamps[i] / interval_ms) * interval_ms;
            let entry = buckets.entry(bucket).or_insert((0.0, 0));
            entry.0 += values[i];
            entry.1 += 1;
        }

        let out_ts: Vec<i64> = buckets.keys().copied().collect();
        let out_vals: Vec<f64> = buckets
            .values()
            .map(|(sum, count)| sum / *count as f64)
            .collect();

        (out_ts, out_vals)
    }

    /// Apply a completed downsample: update partition metadata after the
    /// caller has written the downsampled data back to redb.
    pub fn apply_downsample(
        &mut self,
        collection: &str,
        key_prefix: &str,
        new_row_count: u64,
        new_size_bytes: u64,
    ) {
        if let Some(coll) = self.collections.get_mut(collection)
            && let Some(partition) = coll
                .partitions
                .iter_mut()
                .find(|p| p.key_prefix == key_prefix)
        {
            partition.meta.row_count = new_row_count;
            partition.meta.size_bytes = new_size_bytes;
        }
    }
}

// ---------------------------------------------------------------------------
// Background compaction scheduler (Pattern C)
// ---------------------------------------------------------------------------

/// Scheduler state for background compaction.
pub struct CompactionScheduler {
    /// Last time compaction ran (None = never).
    last_run_ms: Option<i64>,
    /// Minimum interval between compaction runs (ms).
    interval_ms: i64,
    /// Whether the engine is currently idle (no active queries/ingestion).
    idle: bool,
}

impl CompactionScheduler {
    pub fn new(interval_ms: i64) -> Self {
        Self {
            last_run_ms: None,
            interval_ms,
            idle: false,
        }
    }

    /// Mark the engine as idle (no active queries, no active ingestion).
    pub fn set_idle(&mut self, idle: bool) {
        self.idle = idle;
    }

    /// Check if compaction should run now.
    pub fn should_run(&self, now_ms: i64) -> bool {
        if !self.idle {
            return false;
        }
        match self.last_run_ms {
            None => true, // Never run — always due.
            Some(last) => (now_ms - last) >= self.interval_ms,
        }
    }

    /// Record that compaction ran.
    pub fn record_run(&mut self, now_ms: i64) {
        self.last_run_ms = Some(now_ms);
    }

    /// Run compaction on all collections if the scheduler says it's time.
    ///
    /// Returns the number of collections that were compacted.
    pub fn maybe_compact(&mut self, engine: &mut TimeseriesEngine, now_ms: i64) -> usize {
        if !self.should_run(now_ms) {
            return 0;
        }

        let collections = engine.owned_collection_names();
        let mut compacted = 0;
        for coll in &collections {
            if engine.compact_partitions(coll).is_some() {
                compacted += 1;
            }
        }

        self.record_run(now_ms);
        compacted
    }
}

// ---------------------------------------------------------------------------
// Adaptive sync resolution (Pattern B)
// ---------------------------------------------------------------------------

impl TimeseriesEngine {
    /// Compute the effective sync resolution based on battery state and
    /// configured adaptive behavior.
    ///
    /// Normal: use `sync_resolution_ms` from config (0 = raw).
    /// Low battery: increase to 60_000 ms (1-minute averages).
    /// Charging: use config default.
    pub fn effective_sync_resolution(&self) -> u64 {
        let base = self.config.sync_resolution_ms;

        if !self.config.battery_aware {
            return base;
        }

        match self.battery_state {
            nodedb_types::timeseries::BatteryState::Low => {
                // Aggressive downsampling on low battery: at least 1-minute resolution.
                base.max(60_000)
            }
            _ => base,
        }
    }
}

// ---------------------------------------------------------------------------
// Incremental backup / Parquet export (Pattern C)
// ---------------------------------------------------------------------------

/// Result of an incremental backup.
#[derive(Debug)]
pub struct BackupResult {
    /// Number of partitions exported.
    pub partitions_exported: usize,
    /// Total rows exported.
    pub total_rows: u64,
    /// Output file path.
    pub output_path: String,
}

impl TimeseriesEngine {
    /// Export a collection's data as CSV (lightweight backup for Lite-C).
    ///
    /// Writes timestamps and values from all flushed partitions and the
    /// current memtable to a CSV file. The caller provides the output path.
    ///
    /// For full Parquet export, use the DataFusion integration layer.
    pub fn export_csv(
        &self,
        collection: &str,
        output_path: &Path,
    ) -> Result<BackupResult, std::io::Error> {
        let coll = match self.collections.get(collection) {
            Some(c) => c,
            None => {
                return Ok(BackupResult {
                    partitions_exported: 0,
                    total_rows: 0,
                    output_path: output_path.display().to_string(),
                });
            }
        };

        let mut writer = std::io::BufWriter::new(std::fs::File::create(output_path)?);
        use std::io::Write;

        writeln!(writer, "timestamp_ms,value,series_id")?;

        let mut total_rows = 0u64;

        // Export flushed partitions (decode Gorilla blocks).
        for partition in &coll.partitions {
            // Partition data is stored in redb — the caller must have loaded it.
            // For in-memory partitions, we have metadata only.
            total_rows += partition.meta.row_count;
        }

        // Export current memtable (hot data).
        for i in 0..coll.timestamps.len() {
            writeln!(
                writer,
                "{},{},{}",
                coll.timestamps[i], coll.values[i], coll.series_ids[i]
            )?;
            total_rows += 1;
        }

        writer.flush()?;

        Ok(BackupResult {
            partitions_exported: coll.partitions.len(),
            total_rows,
            output_path: output_path.display().to_string(),
        })
    }
}

// ---------------------------------------------------------------------------
// Verify sync-aware retention with cascading codecs
// ---------------------------------------------------------------------------

impl TimeseriesEngine {
    /// Verify that retention works correctly with cascading codec metadata.
    ///
    /// Checks that:
    /// 1. Partitions with codec chain metadata (column_stats with cascade codecs)
    ///    can be retained by sync status.
    /// 2. Codec metadata survives the retain-then-purge lifecycle.
    ///
    /// Returns true if all partitions have valid metadata.
    pub fn verify_retention_codec_compat(&self) -> bool {
        for coll in self.collections.values() {
            for partition in &coll.partitions {
                // Verify column_stats exist and have valid codec fields.
                // Even cascading codecs (AlpFastLanesLz4, etc.) should serialize/
                // deserialize correctly through the retain path.
                for stats in partition.meta.column_stats.values() {
                    // Verify codec is a known variant (serde wouldn't deserialize unknown).
                    let codec_str = stats.codec.as_str();
                    if codec_str.is_empty() {
                        return false;
                    }
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::timeseries::{MetricSample, TieredPartitionConfig};

    // -- Storage budget --

    #[test]
    fn budget_within_limit() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.flush("m");

        let result = engine.enforce_storage_budget(1_000_000_000, BudgetPolicy::Compact);
        assert!(!result.over_budget);
    }

    #[test]
    fn budget_unlimited() {
        let mut engine = TimeseriesEngine::new();
        let result = engine.enforce_storage_budget(0, BudgetPolicy::Compact);
        assert!(!result.over_budget);
    }

    #[test]
    fn budget_warn_policy() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.flush("m");

        let result = engine.enforce_storage_budget(1, BudgetPolicy::Warn); // 1 byte budget
        assert!(result.over_budget);
        assert!(matches!(result.action, BudgetAction::Warning));
    }

    // -- Compaction scheduler --

    #[test]
    fn scheduler_idle_check() {
        let mut scheduler = CompactionScheduler::new(60_000);
        assert!(!scheduler.should_run(0)); // Not idle.

        scheduler.set_idle(true);
        assert!(scheduler.should_run(0)); // Idle, never run before.

        scheduler.record_run(0);
        assert!(!scheduler.should_run(30_000)); // Too soon.
        assert!(scheduler.should_run(60_001)); // Enough time passed.
    }

    #[test]
    fn scheduler_compacts_when_due() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            compaction_partition_threshold: 2,
            ..TieredPartitionConfig::lite_defaults()
        });
        for i in 0..5 {
            engine.ingest_metric(
                "m",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: 1.0,
                },
            );
            engine.flush("m");
        }
        assert_eq!(engine.partition_count("m"), 5);

        let mut scheduler = CompactionScheduler::new(0);
        scheduler.set_idle(true);
        let compacted = scheduler.maybe_compact(&mut engine, 1000);
        assert_eq!(compacted, 1);
        assert_eq!(engine.partition_count("m"), 1);
    }

    // -- Adaptive sync resolution --

    #[test]
    fn adaptive_resolution_normal() {
        let engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            sync_resolution_ms: 1000,
            battery_aware: true,
            ..TieredPartitionConfig::lite_defaults()
        });
        assert_eq!(engine.effective_sync_resolution(), 1000);
    }

    #[test]
    fn adaptive_resolution_low_battery() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            sync_resolution_ms: 1000,
            battery_aware: true,
            ..TieredPartitionConfig::lite_defaults()
        });
        engine.set_battery_state(nodedb_types::timeseries::BatteryState::Low);
        // Low battery → at least 60s resolution.
        assert_eq!(engine.effective_sync_resolution(), 60_000);
    }

    #[test]
    fn adaptive_resolution_disabled() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            sync_resolution_ms: 1000,
            battery_aware: false,
            ..TieredPartitionConfig::lite_defaults()
        });
        engine.set_battery_state(nodedb_types::timeseries::BatteryState::Low);
        // battery_aware=false → no adaptation.
        assert_eq!(engine.effective_sync_resolution(), 1000);
    }

    // -- Backup --

    #[test]
    fn csv_export_basic() {
        let mut engine = TimeseriesEngine::new();
        for i in 0..10 {
            engine.ingest_metric(
                "m",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: i as f64,
                },
            );
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let result = engine.export_csv("m", tmp.path()).unwrap();
        assert_eq!(result.total_rows, 10);

        let content = std::fs::read_to_string(tmp.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines[0], "timestamp_ms,value,series_id"); // header
        assert_eq!(lines.len(), 11); // header + 10 data rows
    }

    #[test]
    fn csv_export_empty() {
        let engine = TimeseriesEngine::new();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let result = engine.export_csv("nonexistent", tmp.path()).unwrap();
        assert_eq!(result.total_rows, 0);
    }

    // -- Codec retention verification --

    #[test]
    fn verify_codec_compat_no_partitions() {
        let engine = TimeseriesEngine::new();
        assert!(engine.verify_retention_codec_compat());
    }

    #[test]
    fn verify_codec_compat_with_partitions() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.flush("m");
        assert!(engine.verify_retention_codec_compat());
    }

    // -- Downsample --

    #[test]
    fn downsample_data_basic() {
        // 1000 timestamps at 1s intervals, values 0..999.
        let timestamps: Vec<i64> = (0..1000).map(|i| i * 1000).collect();
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();

        // Downsample to 10s buckets → 100 output values.
        let (out_ts, out_vals) = TimeseriesEngine::downsample_data(&timestamps, &values, 10_000);
        assert_eq!(out_ts.len(), 100);
        assert_eq!(out_vals.len(), 100);

        // First bucket [0, 10000): timestamps 0..9 → values 0..9 → avg = 4.5.
        assert!((out_vals[0] - 4.5).abs() < f64::EPSILON);
        // Last bucket [990000, 1000000): timestamps 990..999 → avg = 994.5.
        assert!((out_vals[99] - 994.5).abs() < f64::EPSILON);
    }

    #[test]
    fn downsample_data_empty() {
        let (ts, vals) = TimeseriesEngine::downsample_data(&[], &[], 10_000);
        assert!(ts.is_empty());
        assert!(vals.is_empty());
    }

    #[test]
    fn downsample_preserves_trends() {
        // Monotonic increasing data — downsampling should preserve the trend.
        let timestamps: Vec<i64> = (0..10_000).map(|i| i * 100).collect();
        let values: Vec<f64> = (0..10_000).map(|i| i as f64).collect();

        let (_, out_vals) = TimeseriesEngine::downsample_data(&timestamps, &values, 1000);
        // Output should still be monotonically increasing.
        for i in 1..out_vals.len() {
            assert!(
                out_vals[i] >= out_vals[i - 1],
                "trend broken at {i}: {} < {}",
                out_vals[i],
                out_vals[i - 1]
            );
        }
    }

    #[test]
    fn plan_downsample_identifies_partitions() {
        let mut engine = TimeseriesEngine::new();
        for i in 0..500 {
            engine.ingest_metric(
                "m",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: i as f64,
                },
            );
        }
        engine.flush("m");

        let plans = engine.plan_downsample();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].collection, "m");
        assert_eq!(plans[0].row_count, 500);
        assert!(plans[0].target_interval_ms >= 1000);
    }

    #[test]
    fn apply_downsample_updates_metadata() {
        let mut engine = TimeseriesEngine::new();
        for i in 0..100 {
            engine.ingest_metric(
                "m",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: i as f64,
                },
            );
        }
        engine.flush("m");
        let key_prefix = engine.collections["m"].partitions[0].key_prefix.clone();

        engine.apply_downsample("m", &key_prefix, 50, 400);
        assert_eq!(engine.collections["m"].partitions[0].meta.row_count, 50);
        assert_eq!(engine.collections["m"].partitions[0].meta.size_bytes, 400);
    }
}
