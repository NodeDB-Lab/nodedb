// SPDX-License-Identifier: BUSL-1.1

//! Merge functions for distributed array query results.
//!
//! Plain slice merges (`merge_slice_rows`) concatenate row sets from shards
//! in arrival order and apply the coordinator-side limit, preserving each
//! shard's intra-shard order. Cross-shard ordering reflects arrival, since
//! a plain slice row carries no per-row sort key. A globally Hilbert-ordered
//! merge would require carrying a parallel prefix column on the wire and
//! a k-way merge here — that is a wire-format change, not a merger change,
//! and lives outside this module.
//!
//! Audit-log slice merges (`merge_slice_rows_sorted`) do carry a sort key:
//! each native-msgpack row holds a `_ts_system` column, so these are merged
//! globally by system-time ascending before the limit is applied.
//!
//! Aggregate merges combine per-shard partial aggregates using
//! reducer-specific arithmetic (SUM/COUNT/MIN/MAX — same Welford
//! technique as the timeseries merger).

use serde::{Deserialize, Serialize};

use nodedb_types::array_agg::ValueDist;

use super::agg_number::ArrayAggNumber;
use super::group_key::ArrayAggGroupKey;
use super::wire::{ArrayShardAggResp, ArrayShardSliceResp};
use crate::error::{ClusterError, Result};

/// Partial aggregate contributed by a single shard for one group-by bucket.
///
/// Carries enough state for every reducer: SUM, COUNT, MIN, MAX and MEAN
/// from the running numbers, the variances and standard deviations from the
/// Welford fields, MEDIAN / PERCENTILE / MODE from the exact `dist`, and
/// HISTOGRAM from `bins`. Every field merges exactly.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ArrayAggPartial {
    /// Group-by dimension value, or `Scalar` when group_by_dim < 0.
    pub group_key: ArrayAggGroupKey,
    pub count: u64,
    /// Exact for an integer attr.
    pub sum: ArrayAggNumber,
    pub min: ArrayAggNumber,
    pub max: ArrayAggNumber,
    /// Welford mean — enables MEAN without a second pass.
    pub welford_mean: f64,
    pub welford_m2: f64,
    /// Every distinct value with its count. `Empty` for a reducer that
    /// needs no distribution.
    pub dist: ValueDist,
    /// Histogram bin counts. Empty for any other reducer.
    pub bins: Vec<u64>,
}

impl ArrayAggPartial {
    /// Create from a single float cell value.
    pub fn from_single(group_key: ArrayAggGroupKey, val: f64) -> Self {
        Self::from_number(group_key, ArrayAggNumber::Float(val))
    }

    /// Create from a single integer cell value.
    pub fn from_single_int(group_key: ArrayAggGroupKey, val: i64) -> Self {
        Self::from_number(group_key, ArrayAggNumber::int(i128::from(val)))
    }

    fn from_number(group_key: ArrayAggGroupKey, val: ArrayAggNumber) -> Self {
        Self {
            group_key,
            count: 1,
            sum: val,
            min: val,
            max: val,
            welford_mean: val.as_f64(),
            welford_m2: 0.0,
            dist: ValueDist::Empty,
            bins: Vec::new(),
        }
    }

    /// The partial of a group no cell contributed to. Merging it is a no-op.
    pub fn empty(group_key: ArrayAggGroupKey) -> Self {
        Self {
            group_key,
            count: 0,
            sum: ArrayAggNumber::Float(0.0),
            min: ArrayAggNumber::Float(f64::INFINITY),
            max: ArrayAggNumber::Float(f64::NEG_INFINITY),
            welford_mean: 0.0,
            welford_m2: 0.0,
            dist: ValueDist::Empty,
            bins: Vec::new(),
        }
    }

    /// Merge another partial into this one using parallel Welford.
    ///
    /// Histogram bins add even when a side counted no value in range, so a
    /// group keeps its bins.
    pub fn merge(&mut self, other: &ArrayAggPartial) {
        if self.bins.len() < other.bins.len() {
            self.bins.resize(other.bins.len(), 0);
        }
        for (slot, n) in self.bins.iter_mut().zip(&other.bins) {
            *slot = slot.saturating_add(*n);
        }
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            let bins = std::mem::take(&mut self.bins);
            *self = other.clone();
            self.bins = bins;
            return;
        }
        self.dist = std::mem::replace(&mut self.dist, ValueDist::Empty).merge(other.dist.clone());
        self.sum = self.sum.sum_with(other.sum);
        self.min = self.min.min_with(other.min);
        self.max = self.max.max_with(other.max);
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

/// Returns `true` if any shard reported that the system-time cutoff fell below its
/// oldest tile version and it produced zero rows as a result.
///
/// Callers combine this flag via logical OR across shards to propagate the
/// below-horizon signal to the upstream coordinator response.
pub fn any_truncated_before_horizon_slice(shard_resps: &[ArrayShardSliceResp]) -> bool {
    shard_resps.iter().any(|r| r.truncated_before_horizon)
}

/// Returns `true` if any shard reported that the system-time cutoff fell below its
/// oldest tile version, causing the shard to contribute zero partials.
pub fn any_truncated_before_horizon_agg(shard_resps: &[ArrayShardAggResp]) -> bool {
    shard_resps.iter().any(|r| r.truncated_before_horizon)
}

/// The write versions every shard leg observed, one per vShard. Each leg
/// reads its own vShard, so every leg's version is kept. Two legs that report
/// one vShard keep the higher version.
pub fn merge_read_versions<'a>(
    legs: impl IntoIterator<Item = &'a [nodedb_types::ShardVersion]>,
) -> Vec<nodedb_types::ShardVersion> {
    let mut merged: Vec<nodedb_types::ShardVersion> = Vec::new();
    for shard in legs.into_iter().flatten() {
        match merged.iter_mut().find(|seen| seen.vshard == shard.vshard) {
            Some(seen) => seen.version = seen.version.max(shard.version),
            None => merged.push(*shard),
        }
    }
    merged
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

/// Merge row batches from multiple shards for an audit-log (`AllVersions`) fan-out.
///
/// Unlike `merge_slice_rows`, which concatenates rows in shard-arrival order,
/// this function decodes each row's system-time and sorts globally by that
/// time ascending (ties broken by the raw msgpack byte sequence as a stable
/// coord proxy) before applying the limit.
///
/// Each row is the native (untagged) msgpack map emitted by the array slice
/// handler — one field per dim and per attr, then `"_ts_system": <int>`,
/// written in schema order so one cell's rows encode identically. In
/// audit-log mode every row carries `_ts_system`; a row that fails to decode
/// or is missing that column is a wire-contract violation (corruption or a
/// handler bug), so this function returns an error rather than silently
/// mis-ordering the result — see the "no silent partial success" rule.
///
/// Pass `coordinator_limit = 0` to return all rows without truncation.
pub fn merge_slice_rows_sorted(
    shard_resps: &[ArrayShardSliceResp],
    coordinator_limit: u32,
) -> Result<Vec<Vec<u8>>> {
    // Collect all raw rows from all shards. Each entry is (system_time_ms, raw_bytes).
    let mut tagged: Vec<(i64, Vec<u8>)> = Vec::new();
    for resp in shard_resps {
        for row in &resp.rows_msgpack {
            let ts = decode_system_time(row)?;
            tagged.push((ts, row.clone()));
        }
    }
    // Stable sort: ascending by system_time, then by raw bytes for tie-breaking.
    tagged.sort_by(|(ts_a, bytes_a), (ts_b, bytes_b)| {
        ts_a.cmp(ts_b).then_with(|| bytes_a.cmp(bytes_b))
    });
    let total = tagged.len();
    let cap = if coordinator_limit > 0 {
        total.min(coordinator_limit as usize)
    } else {
        total
    };
    Ok(tagged.into_iter().take(cap).map(|(_, b)| b).collect())
}

/// Decode the `_ts_system` system-time column from a native-msgpack audit-log row.
///
/// Rows are encoded by the array slice handler via the native (untagged)
/// `value_to_msgpack` writer, so they decode back to a `Value::Object` map.
/// In audit-log mode the handler always stamps `_ts_system`; its absence — or
/// a row that doesn't decode as an object at all — means the wire payload is
/// corrupt or the handler contract was broken, both of which are hard errors.
fn decode_system_time(bytes: &[u8]) -> Result<i64> {
    let value = nodedb_types::value_from_msgpack(bytes).map_err(|e| ClusterError::Codec {
        detail: format!("audit-log row decode in sorted merge: {e}"),
    })?;
    match value {
        nodedb_types::Value::Object(map) => match map.get("_ts_system") {
            Some(nodedb_types::Value::Integer(ts)) => Ok(*ts),
            Some(other) => Err(ClusterError::Codec {
                detail: format!(
                    "audit-log row _ts_system is {}, expected integer",
                    other.type_name()
                ),
            }),
            None => Err(ClusterError::Codec {
                detail: "audit-log row missing _ts_system column in sorted merge".into(),
            }),
        },
        other => Err(ClusterError::Codec {
            detail: format!(
                "audit-log row decoded as {}, expected object map",
                other.type_name()
            ),
        }),
    }
}

/// Merge per-shard partial aggregates into one result per group-by key.
///
/// Groups by `group_key`; uses `ArrayAggPartial::merge` for each group.
pub fn reduce_agg_partials(shard_resps: &[ArrayShardAggResp]) -> Vec<ArrayAggPartial> {
    use std::collections::BTreeMap;
    let mut buckets: BTreeMap<ArrayAggGroupKey, ArrayAggPartial> = BTreeMap::new();
    for resp in shard_resps {
        for partial in &resp.partials {
            buckets
                .entry(partial.group_key.clone())
                .and_modify(|existing| existing.merge(partial))
                .or_insert_with(|| partial.clone());
        }
    }
    buckets.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn g(k: i64) -> ArrayAggGroupKey {
        ArrayAggGroupKey::Int64(k)
    }

    #[test]
    fn reduce_sum_across_shards() {
        let resp_a = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![ArrayAggPartial::from_single(g(0), 10.0)],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let resp_b = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![ArrayAggPartial::from_single(g(0), 20.0)],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let merged = reduce_agg_partials(&[resp_a, resp_b]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].count, 2);
        assert!((merged[0].sum.as_f64() - 30.0).abs() < f64::EPSILON);
    }

    /// An integer SUM across shards keeps every unit past 2^53.
    #[test]
    fn reduce_integer_sum_is_exact_across_shards() {
        let shard = |shard_id, v| ArrayShardAggResp {
            shard_id,
            partials: vec![ArrayAggPartial::from_single_int(g(0), v)],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let merged = reduce_agg_partials(&[shard(0, 1 << 53), shard(1, 1)]);
        assert_eq!(merged[0].sum.as_i128(), Some((1 << 53) + 1));
        assert_eq!(merged[0].max.as_i128(), Some(1 << 53));
        assert_eq!(merged[0].min.as_i128(), Some(1));
    }

    #[test]
    fn reduce_separate_group_keys() {
        let resp = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![
                ArrayAggPartial::from_single(g(0), 5.0),
                ArrayAggPartial::from_single(g(1), 15.0),
            ],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let merged = reduce_agg_partials(&[resp]);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_empty_partial_is_noop() {
        let mut a = ArrayAggPartial::from_single(g(0), 42.0);
        let empty = ArrayAggPartial {
            count: 0,
            ..ArrayAggPartial::from_single(g(0), 0.0)
        };
        a.merge(&empty);
        assert_eq!(a.count, 1);
        assert!((a.sum.as_f64() - 42.0).abs() < f64::EPSILON);
    }

    /// Distributions and histogram bins merge exactly across shards, and a
    /// shard with no in-range value still keeps its bins.
    #[test]
    fn dist_and_bins_merge_across_shards() {
        use nodedb_types::array_agg::IntRun;
        let with = |runs: Vec<(i64, u64)>, bins: Vec<u64>| ArrayAggPartial {
            count: runs.iter().map(|r| r.1).sum(),
            dist: ValueDist::Ints(
                runs.into_iter()
                    .map(|(value, count)| IntRun { value, count })
                    .collect(),
            ),
            bins,
            ..ArrayAggPartial::empty(g(0))
        };
        let mut a = with(vec![(1, 1), (5, 2)], vec![1, 2]);
        a.merge(&with(vec![(5, 1), (9, 1)], vec![0, 2]));
        assert_eq!(a.count, 5);
        assert_eq!(a.dist.percentile(0.5), Ok(Some(5.0)));
        assert_eq!(a.bins, vec![1, 4]);

        let mut empty = ArrayAggPartial {
            bins: vec![0, 0],
            ..ArrayAggPartial::empty(g(0))
        };
        empty.merge(&with(vec![(3, 1)], vec![1, 0]));
        assert_eq!(empty.bins, vec![1, 0]);
        assert_eq!(empty.count, 1);
    }

    #[test]
    fn merge_slice_rows_concatenates() {
        let r0 = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: vec![vec![1u8], vec![2u8]],
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let r1 = ArrayShardSliceResp {
            shard_id: 1,
            rows_msgpack: vec![vec![3u8]],
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
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
            read_versions: Vec::new(),
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
            partials: vec![ArrayAggPartial::from_single(g(0), 5.0)],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let resp_b = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![ArrayAggPartial::from_single(g(0), 3.0)],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let merged = reduce_agg_partials(&[resp_a, resp_b]);
        assert_eq!(merged.len(), 1);
        assert!((merged[0].min.as_f64() - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn reduce_max_across_shards() {
        let resp_a = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![ArrayAggPartial::from_single(g(0), 5.0)],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let resp_b = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![ArrayAggPartial::from_single(g(0), 99.0)],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let merged = reduce_agg_partials(&[resp_a, resp_b]);
        assert_eq!(merged.len(), 1);
        assert!((merged[0].max.as_f64() - 99.0).abs() < f64::EPSILON);
    }

    #[test]
    fn reduce_avg_welford_merge_exact() {
        // Two shards: shard A has one value of 10, shard B has one value of 20.
        // Combined mean should be exactly 15.
        let mut a = ArrayAggPartial::from_single(g(0), 10.0);
        let b = ArrayAggPartial::from_single(g(0), 20.0);
        a.merge(&b);
        // welford_mean after merge = 15.0
        assert!((a.welford_mean - 15.0).abs() < 1e-9);
        assert_eq!(a.count, 2);
        assert!((a.sum.as_f64() - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn reduce_grouped_overlapping_keys() {
        // Shard A: groups 0→5, 1→10. Shard B: groups 1→20, 2→30.
        let resp_a = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![
                ArrayAggPartial::from_single(g(0), 5.0),
                ArrayAggPartial::from_single(g(1), 10.0),
            ],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let resp_b = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![
                ArrayAggPartial::from_single(g(1), 20.0),
                ArrayAggPartial::from_single(g(2), 30.0),
            ],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let merged = reduce_agg_partials(&[resp_a, resp_b]);
        assert_eq!(merged.len(), 3);
        let g0 = merged.iter().find(|p| p.group_key == g(0)).unwrap();
        let g1 = merged.iter().find(|p| p.group_key == g(1)).unwrap();
        let g2 = merged.iter().find(|p| p.group_key == g(2)).unwrap();
        assert!((g0.sum.as_f64() - 5.0).abs() < f64::EPSILON);
        assert!((g1.sum.as_f64() - 30.0).abs() < f64::EPSILON);
        assert!((g2.sum.as_f64() - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn truncated_before_horizon_or_combines_across_shards() {
        let r0 = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: vec![],
            truncated: false,
            truncated_before_horizon: true,
            read_versions: Vec::new(),
        };
        let r1 = ArrayShardSliceResp {
            shard_id: 1,
            rows_msgpack: vec![vec![1u8]],
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        assert!(any_truncated_before_horizon_slice(&[r0, r1]));

        let a0 = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let a1 = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![],
            truncated_before_horizon: true,
            read_versions: Vec::new(),
        };
        assert!(any_truncated_before_horizon_agg(&[a0, a1]));

        let a_none = ArrayShardAggResp {
            shard_id: 2,
            partials: vec![],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        assert!(!any_truncated_before_horizon_agg(&[a_none]));
    }

    #[test]
    fn reduce_grouped_disjoint_keys() {
        // Shard A has only group 0; shard B has only group 1 — no overlap.
        let resp_a = ArrayShardAggResp {
            shard_id: 0,
            partials: vec![ArrayAggPartial::from_single(g(0), 7.0)],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let resp_b = ArrayShardAggResp {
            shard_id: 1,
            partials: vec![ArrayAggPartial::from_single(g(1), 13.0)],
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let merged = reduce_agg_partials(&[resp_a, resp_b]);
        assert_eq!(merged.len(), 2);
        let g0 = merged.iter().find(|p| p.group_key == g(0)).unwrap();
        let g1 = merged.iter().find(|p| p.group_key == g(1)).unwrap();
        assert_eq!(g0.count, 1);
        assert_eq!(g1.count, 1);
    }

    /// Float64 and String group keys merge by value across shards, and
    /// distinct values stay distinct groups.
    #[test]
    fn reduce_merges_typed_keys_by_value() {
        let shard = |shard_id, partials| ArrayShardAggResp {
            shard_id,
            partials,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let f = ArrayAggGroupKey::Float64;
        let s = |v: &str| ArrayAggGroupKey::String(v.into());
        let merged = reduce_agg_partials(&[
            shard(
                0,
                vec![
                    ArrayAggPartial::from_single(f(101.5), 1.0),
                    ArrayAggPartial::from_single(s("chrX"), 4.0),
                ],
            ),
            shard(
                1,
                vec![
                    ArrayAggPartial::from_single(f(101.5), 2.0),
                    ArrayAggPartial::from_single(f(101.25), 8.0),
                    ArrayAggPartial::from_single(s("chrX"), 16.0),
                ],
            ),
        ]);
        assert_eq!(merged.len(), 3);
        let sum_of = |key: ArrayAggGroupKey| {
            merged
                .iter()
                .find(|p| p.group_key == key)
                .map(|p| p.sum.as_f64())
                .expect("group present")
        };
        assert!((sum_of(f(101.5)) - 3.0).abs() < f64::EPSILON);
        assert!((sum_of(f(101.25)) - 8.0).abs() < f64::EPSILON);
        assert!((sum_of(s("chrX")) - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn merge_slice_rows_limit_across_shards() {
        let r0 = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: vec![vec![1u8], vec![2u8]],
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let r1 = ArrayShardSliceResp {
            shard_id: 1,
            rows_msgpack: vec![vec![3u8], vec![4u8]],
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        // Total 4 rows, limit 3 → first 3.
        let rows = merge_slice_rows(&[r0, r1], 3);
        assert_eq!(rows.len(), 3);
    }

    /// Build a wire row exactly as the array slice handler does: a native
    /// (untagged) msgpack encoding of `Value::ArrayCell` carrying `_ts_system`.
    fn audit_row(system_time: i64, coord: i64) -> Vec<u8> {
        use nodedb_types::Value;
        use nodedb_types::array_cell::ArrayCell;
        let cell = Value::ArrayCell(ArrayCell {
            coords: vec![Value::Integer(coord)],
            attrs: vec![Value::Float(1.0)],
            system_time: Some(system_time),
        });
        nodedb_types::value_to_msgpack(&cell).expect("encode audit row")
    }

    #[test]
    fn merge_slice_rows_sorted_orders_by_system_time_across_shards() {
        // Shard 0 emits ts=30 then ts=10; shard 1 emits ts=20. The merged
        // output must be globally ascending by system-time: 10, 20, 30.
        let r0 = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: vec![audit_row(30, 3), audit_row(10, 1)],
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let r1 = ArrayShardSliceResp {
            shard_id: 1,
            rows_msgpack: vec![audit_row(20, 2)],
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let rows = merge_slice_rows_sorted(&[r0, r1], 0).expect("sorted merge");
        let times: Vec<i64> = rows
            .iter()
            .map(|b| decode_system_time(b).unwrap())
            .collect();
        assert_eq!(times, vec![10, 20, 30]);
    }

    #[test]
    fn merge_slice_rows_sorted_applies_limit_after_global_sort() {
        // Limit must cut the globally-sorted sequence, not per-shard arrival.
        let r0 = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: vec![audit_row(50, 5), audit_row(10, 1)],
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let r1 = ArrayShardSliceResp {
            shard_id: 1,
            rows_msgpack: vec![audit_row(30, 3), audit_row(20, 2)],
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let rows = merge_slice_rows_sorted(&[r0, r1], 2).expect("sorted merge");
        let times: Vec<i64> = rows
            .iter()
            .map(|b| decode_system_time(b).unwrap())
            .collect();
        assert_eq!(times, vec![10, 20]);
    }

    #[test]
    fn merge_slice_rows_sorted_rejects_corrupt_row() {
        // A row that doesn't decode as a native object map is a wire-contract
        // violation and must surface as an error, not sort silently to the end.
        let resp = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: vec![audit_row(10, 1), vec![0xFF, 0xFF, 0xFF]],
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        assert!(merge_slice_rows_sorted(&[resp], 0).is_err());
    }

    #[test]
    fn agg_partial_wire_shape_is_tuple_with_horizon_flag() {
        // The Data Plane `return_partial` path encodes `(partials, flag)` as a
        // 2-element msgpack array (see `encode_bitemporal_agg_partial`). The
        // cluster executor MUST decode that tuple — decoding it as a bare
        // `Vec<ArrayAggPartial>` is a shape mismatch that fails outright and
        // also drops the below-horizon signal. This pins the contract so the
        // producer and consumer can't drift apart again.
        let partials = vec![
            ArrayAggPartial::from_single(g(0), 10.0),
            ArrayAggPartial::from_single(g(1), 20.0),
        ];
        let bytes = zerompk::to_msgpack_vec(&(&partials, true)).expect("encode tuple");

        // Correct shape round-trips, carrying the flag.
        let (decoded, flag): (Vec<ArrayAggPartial>, bool) =
            zerompk::from_msgpack(&bytes).expect("decode tuple");
        assert_eq!(decoded.len(), 2);
        assert!(flag);

        // The old (buggy) bare-Vec decode must fail — guards the regression.
        assert!(zerompk::from_msgpack::<Vec<ArrayAggPartial>>(&bytes).is_err());
    }

    #[test]
    fn decode_system_time_reads_ts_system_column() {
        let bytes = audit_row(1_700_000_000_123, 7);
        assert_eq!(decode_system_time(&bytes).unwrap(), 1_700_000_000_123);
    }

    #[test]
    fn merge_slice_rows_zero_limit_is_unlimited() {
        let resp = ArrayShardSliceResp {
            shard_id: 0,
            rows_msgpack: (0u8..20).map(|i| vec![i]).collect(),
            truncated: false,
            truncated_before_horizon: false,
            read_versions: Vec::new(),
        };
        let rows = merge_slice_rows(&[resp], 0);
        assert_eq!(rows.len(), 20);
    }

    fn shard_version(vshard: u32, index: u64) -> nodedb_types::ShardVersion {
        nodedb_types::ShardVersion {
            vshard,
            version: nodedb_types::WriteVersion::logged(1, index),
        }
    }

    /// Every leg's version survives the merge, a leg that matched no row
    /// included. A vShard two legs report keeps the higher version.
    #[test]
    fn the_merge_keeps_every_legs_version() {
        let legs = [
            vec![shard_version(3, 10)],
            Vec::new(),
            vec![shard_version(7, 4)],
            vec![shard_version(3, 12)],
        ];
        let merged = merge_read_versions(legs.iter().map(Vec::as_slice));
        assert_eq!(merged, vec![shard_version(3, 12), shard_version(7, 4)]);
    }
}
