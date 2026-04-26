//! Merge functions for distributed array query results.
//!
//! Slice merges concatenate row sets from shards and re-sort by Hilbert
//! prefix (Tier 8.1 fills in the sort key). Aggregate merges combine
//! per-shard partial aggregates using reducer-specific arithmetic
//! (SUM/COUNT/MIN/MAX — same Welford technique as the timeseries merger).

use serde::{Deserialize, Serialize};

use super::wire::{ArrayShardAggResp, ArrayShardSliceResp};

/// Partial aggregate contributed by a single shard for one group-by bucket.
///
/// Carries enough state for all supported reducers (SUM, COUNT, MIN, MAX,
/// MEAN). Welford fields enable variance/stddev if a future reducer needs it.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ArrayAggPartial {
    /// Group-by dimension value, or 0 when group_by_dim < 0 (scalar aggregate).
    pub group_key: i64,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    /// Welford mean — enables MEAN without a second pass.
    pub welford_mean: f64,
    pub welford_m2: f64,
}

impl ArrayAggPartial {
    /// Create from a single cell value.
    pub fn from_single(group_key: i64, val: f64) -> Self {
        Self {
            group_key,
            count: 1,
            sum: val,
            min: val,
            max: val,
            welford_mean: val,
            welford_m2: 0.0,
        }
    }

    /// Merge another partial into this one using parallel Welford.
    pub fn merge(&mut self, other: &ArrayAggPartial) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
            return;
        }
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        let new_count = self.count + other.count;
        let delta = other.welford_mean - self.welford_mean;
        let combined_mean = (self.welford_mean * self.count as f64
            + other.welford_mean * other.count as f64)
            / new_count as f64;
        let combined_m2 = self.welford_m2
            + other.welford_m2
            + delta * delta * (self.count as f64 * other.count as f64) / new_count as f64;
        self.welford_mean = combined_mean;
        self.welford_m2 = combined_m2;
        self.count = new_count;
    }
}

/// Returns `true` if any shard reported that `system_as_of` fell below its
/// oldest tile version and it produced zero rows as a result.
///
/// Callers combine this flag via logical OR across shards to propagate the
/// below-horizon signal to the upstream coordinator response.
pub fn any_truncated_before_horizon_slice(shard_resps: &[ArrayShardSliceResp]) -> bool {
    shard_resps.iter().any(|r| r.truncated_before_horizon)
}

/// Returns `true` if any shard reported that `system_as_of` fell below its
/// oldest tile version, causing the shard to contribute zero partials.
pub fn any_truncated_before_horizon_agg(shard_resps: &[ArrayShardAggResp]) -> bool {
    shard_resps.iter().any(|r| r.truncated_before_horizon)
}

/// Merge row batches from multiple shards into one result set.
///
/// Rows are concatenated in shard-arrival order (order-independent for
/// an unsorted slice). If `coordinator_limit > 0` the merged list is
/// truncated to at most `coordinator_limit` rows after concatenation —
/// this is the final cut-off after shards have already applied their own
/// per-shard limit via `ArrayShardSliceReq::limit`.
///
/// Pass `coordinator_limit = 0` to return all rows without truncation.
pub fn merge_slice_rows(
    shard_resps: &[ArrayShardSliceResp],
    coordinator_limit: u32,
) -> Vec<Vec<u8>> {
    let total: usize = shard_resps.iter().map(|r| r.rows_msgpack.len()).sum();
    let cap = if coordinator_limit > 0 {
        total.min(coordinator_limit as usize)
    } else {
        total
    };
    let mut merged = Vec::with_capacity(cap);
    'outer: for resp in shard_resps {
        for row in &resp.rows_msgpack {
            if coordinator_limit > 0 && merged.len() >= coordinator_limit as usize {
                break 'outer;
            }
            merged.push(row.clone());
        }
    }
    merged
}

/// Merge per-shard partial aggregates into one result per group-by key.
///
/// Groups by `group_key`; uses `ArrayAggPartial::merge` for each group.
pub fn reduce_agg_partials(shard_resps: &[ArrayShardAggResp]) -> Vec<ArrayAggPartial> {
    use std::collections::BTreeMap;
    let mut buckets: BTreeMap<i64, ArrayAggPartial> = BTreeMap::new();
    for resp in shard_resps {
        for partial in &resp.partials {
            buckets
                .entry(partial.group_key)
                .and_modify(|existing| existing.merge(partial))
                .or_insert_with(|| partial.clone());
        }
    }
    buckets.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reduce_sum_across_shards() {
        let resp_a = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![ArrayAggPartial::from_single(0, 10.0)],
            truncated_before_horizon: false,
        };
        let resp_b = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![ArrayAggPartial::from_single(0, 20.0)],
            truncated_before_horizon: false,
        };
        let merged = reduce_agg_partials(&[resp_a, resp_b]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].count, 2);
        assert!((merged[0].sum - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn reduce_separate_group_keys() {
        let resp = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![
                ArrayAggPartial::from_single(0, 5.0),
                ArrayAggPartial::from_single(1, 15.0),
            ],
            truncated_before_horizon: false,
        };
        let merged = reduce_agg_partials(&[resp]);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_empty_partial_is_noop() {
        let mut a = ArrayAggPartial::from_single(0, 42.0);
        let empty = ArrayAggPartial {
            count: 0,
            ..ArrayAggPartial::from_single(0, 0.0)
        };
        a.merge(&empty);
        assert_eq!(a.count, 1);
        assert!((a.sum - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn merge_slice_rows_concatenates() {
        let r0 = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: vec![vec![1u8], vec![2u8]],
            truncated: false,
            truncated_before_horizon: false,
        };
        let r1 = ArrayShardSliceResp {
            shard_id: 1,
            rows_msgpack: vec![vec![3u8]],
            truncated: false,
            truncated_before_horizon: false,
        };
        let rows = merge_slice_rows(&[r0, r1], 0);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn merge_slice_rows_applies_coordinator_limit() {
        let resp = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: vec![vec![1u8], vec![2u8], vec![3u8], vec![4u8], vec![5u8]],
            truncated: false,
            truncated_before_horizon: false,
        };
        let rows = merge_slice_rows(&[resp], 3);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![1u8]);
        assert_eq!(rows[2], vec![3u8]);
    }

    #[test]
    fn reduce_min_across_shards() {
        let resp_a = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![ArrayAggPartial::from_single(0, 5.0)],
            truncated_before_horizon: false,
        };
        let resp_b = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![ArrayAggPartial::from_single(0, 3.0)],
            truncated_before_horizon: false,
        };
        let merged = reduce_agg_partials(&[resp_a, resp_b]);
        assert_eq!(merged.len(), 1);
        assert!((merged[0].min - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn reduce_max_across_shards() {
        let resp_a = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![ArrayAggPartial::from_single(0, 5.0)],
            truncated_before_horizon: false,
        };
        let resp_b = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![ArrayAggPartial::from_single(0, 99.0)],
            truncated_before_horizon: false,
        };
        let merged = reduce_agg_partials(&[resp_a, resp_b]);
        assert_eq!(merged.len(), 1);
        assert!((merged[0].max - 99.0).abs() < f64::EPSILON);
    }

    #[test]
    fn reduce_avg_welford_merge_exact() {
        // Two shards: shard A has one value of 10, shard B has one value of 20.
        // Combined mean should be exactly 15.
        let mut a = ArrayAggPartial::from_single(0, 10.0);
        let b = ArrayAggPartial::from_single(0, 20.0);
        a.merge(&b);
        // welford_mean after merge = 15.0
        assert!((a.welford_mean - 15.0).abs() < 1e-9);
        assert_eq!(a.count, 2);
        assert!((a.sum - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn reduce_grouped_overlapping_keys() {
        // Shard A: groups 0→5, 1→10. Shard B: groups 1→20, 2→30.
        let resp_a = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![
                ArrayAggPartial::from_single(0, 5.0),
                ArrayAggPartial::from_single(1, 10.0),
            ],
            truncated_before_horizon: false,
        };
        let resp_b = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![
                ArrayAggPartial::from_single(1, 20.0),
                ArrayAggPartial::from_single(2, 30.0),
            ],
            truncated_before_horizon: false,
        };
        let merged = reduce_agg_partials(&[resp_a, resp_b]);
        assert_eq!(merged.len(), 3);
        let g0 = merged.iter().find(|p| p.group_key == 0).unwrap();
        let g1 = merged.iter().find(|p| p.group_key == 1).unwrap();
        let g2 = merged.iter().find(|p| p.group_key == 2).unwrap();
        assert!((g0.sum - 5.0).abs() < f64::EPSILON);
        assert!((g1.sum - 30.0).abs() < f64::EPSILON);
        assert!((g2.sum - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn truncated_before_horizon_or_combines_across_shards() {
        let r0 = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: vec![],
            truncated: false,
            truncated_before_horizon: true,
        };
        let r1 = ArrayShardSliceResp {
            shard_id: 1,
            rows_msgpack: vec![vec![1u8]],
            truncated: false,
            truncated_before_horizon: false,
        };
        assert!(any_truncated_before_horizon_slice(&[r0, r1]));

        let a0 = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![],
            truncated_before_horizon: false,
        };
        let a1 = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![],
            truncated_before_horizon: true,
        };
        assert!(any_truncated_before_horizon_agg(&[a0, a1]));

        let a_none = ArrayShardAggResp {
            shard_id: 2,
            partials: vec![],
            truncated_before_horizon: false,
        };
        assert!(!any_truncated_before_horizon_agg(&[a_none]));
    }

    #[test]
    fn reduce_grouped_disjoint_keys() {
        // Shard A has only group 0; shard B has only group 1 — no overlap.
        let resp_a = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![ArrayAggPartial::from_single(0, 7.0)],
            truncated_before_horizon: false,
        };
        let resp_b = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![ArrayAggPartial::from_single(1, 13.0)],
            truncated_before_horizon: false,
        };
        let merged = reduce_agg_partials(&[resp_a, resp_b]);
        assert_eq!(merged.len(), 2);
        let g0 = merged.iter().find(|p| p.group_key == 0).unwrap();
        let g1 = merged.iter().find(|p| p.group_key == 1).unwrap();
        assert_eq!(g0.count, 1);
        assert_eq!(g1.count, 1);
    }

    #[test]
    fn merge_slice_rows_limit_across_shards() {
        let r0 = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: vec![vec![1u8], vec![2u8]],
            truncated: false,
            truncated_before_horizon: false,
        };
        let r1 = ArrayShardSliceResp {
            shard_id: 1,
            rows_msgpack: vec![vec![3u8], vec![4u8]],
            truncated: false,
            truncated_before_horizon: false,
        };
        // Total 4 rows, limit 3 → first 3.
        let rows = merge_slice_rows(&[r0, r1], 3);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn merge_slice_rows_zero_limit_is_unlimited() {
        let resp = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: (0u8..20).map(|i| vec![i]).collect(),
            truncated: false,
            truncated_before_horizon: false,
        };
        let rows = merge_slice_rows(&[resp], 0);
        assert_eq!(rows.len(), 20);
    }
}
