//! Scatter-gather timeseries aggregation across shards.
//!
//! For sharded deployments, `metrics` is spread across multiple vShards.
//! The scatter-gather pattern:
//! 1. Control Plane fans out query to all shards owning `metrics` data
//! 2. Each shard computes local partial aggregates
//! 3. Control Plane merges partials into the final result

use serde::{Deserialize, Serialize};

use super::merge::{PartialAgg, PartialAggMerger};

/// A scatter-gather plan for a timeseries aggregation query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterGatherPlan {
    /// Collection to scan.
    pub collection: String,
    /// Time range filter.
    pub start_ms: i64,
    pub end_ms: i64,
    /// Column to aggregate.
    pub value_column: String,
    /// Time bucket interval (0 = single aggregate over entire range).
    pub bucket_interval_ms: i64,
    /// Target shard IDs to fan out to.
    pub shard_ids: Vec<u16>,
}

/// Result from a single shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardResult {
    /// Which shard produced this result.
    pub shard_id: u16,
    /// Partial aggregates per time bucket.
    pub partials: Vec<PartialAgg>,
}

/// Merge results from multiple shards into a final result.
///
/// This is the coordinator-side merge called after all shard responses arrive.
pub fn merge_shard_results(shard_results: &[ShardResult]) -> Vec<PartialAgg> {
    let mut merger = PartialAggMerger::new();
    for result in shard_results {
        merger.add_shard_results(&result.partials);
    }
    merger.finalize()
}

/// Determine which shards own data for a given collection.
///
/// In NodeDB's vShard model, a collection maps to a set of vShards via
/// hash routing. This function returns all shard IDs that might contain
/// data for the collection (conservative — includes all shards that could
/// have received data via the routing function).
pub fn shards_for_collection(_collection: &str, total_shards: u16) -> Vec<u16> {
    if total_shards == 0 {
        return Vec::new();
    }
    // In the current routing model, ILP routes by series key hash, so
    // data for a collection can be on ANY shard. For scatter-gather,
    // we must fan out to all shards.
    // Future: maintain a shard→collection bitmap to narrow the fan-out.
    (0..total_shards).collect()
}

/// Consolidate continuous aggregate results from multiple shards.
///
/// Each shard runs its own `ContinuousAggregateManager` and stores
/// aggregate results locally. For cross-shard queries, the coordinator
/// collects per-shard aggregate partials and merges them.
///
/// This is the same merge path as raw scatter-gather — continuous
/// aggregates just have fewer rows (pre-bucketed), so it's faster.
pub fn consolidate_aggregate_results(shard_results: &[ShardResult]) -> Vec<PartialAgg> {
    merge_shard_results(shard_results)
}

/// Estimate the cost of a scatter-gather query (for query planning).
///
/// Returns estimated total rows to scan across all shards.
pub fn estimate_scatter_cost(
    shard_row_counts: &[u64], // per-shard row estimate for this collection
    selectivity: f64,         // fraction of rows matching the time range
) -> u64 {
    let total_rows: u64 = shard_row_counts.iter().sum();
    (total_rows as f64 * selectivity) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_two_shards() {
        let shard_a = ShardResult {
            shard_id: 0,
            partials: vec![
                PartialAgg {
                    count: 100,
                    sum: 5000.0,
                    ..PartialAgg::from_single(0, 1, 50.0)
                },
                PartialAgg {
                    count: 100,
                    sum: 6000.0,
                    ..PartialAgg::from_single(60_000, 60001, 60.0)
                },
            ],
        };
        let shard_b = ShardResult {
            shard_id: 1,
            partials: vec![
                PartialAgg {
                    count: 80,
                    sum: 4000.0,
                    ..PartialAgg::from_single(0, 2, 50.0)
                },
                PartialAgg {
                    count: 80,
                    sum: 5600.0,
                    ..PartialAgg::from_single(60_000, 60002, 70.0)
                },
            ],
        };

        let merged = merge_shard_results(&[shard_a, shard_b]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].count, 180);
        assert_eq!(merged[0].sum, 9000.0);
        assert_eq!(merged[1].count, 180);
    }

    #[test]
    fn shards_for_collection_all() {
        let shards = shards_for_collection("metrics", 10);
        assert_eq!(shards.len(), 10);
    }

    #[test]
    fn estimate_cost() {
        let row_counts = vec![1_000_000u64; 10]; // 10 shards, 1M rows each
        let cost = estimate_scatter_cost(&row_counts, 0.01); // 1% selectivity
        assert_eq!(cost, 100_000); // 10M * 0.01
    }
}
