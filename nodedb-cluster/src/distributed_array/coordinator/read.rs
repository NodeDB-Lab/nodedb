// SPDX-License-Identifier: BUSL-1.1

//! Read-path coordinator for distributed array operations.
//!
//! [`ArrayCoordinator`] drives fan-out reads (`coord_slice`, `coord_agg`,
//! `coord_surrogate_bitmap_scan`) to the set of vShards whose Hilbert range
//! overlaps the slice predicate.

use std::sync::Arc;

use crate::circuit_breaker::CircuitBreaker;
use crate::error::{ClusterError, Result};

use super::super::merge::{
    ArrayAggPartial, any_truncated_before_horizon_agg, merge_slice_rows, merge_slice_rows_sorted,
    reduce_agg_partials,
};
use super::super::rpc::ShardRpcDispatch;
use super::super::scatter::{FanOutParams, FanOutPartitionedParams, fan_out, fan_out_partitioned};
use super::super::wire::{
    ArrayShardAggReq, ArrayShardAggResp, ArrayShardDeleteReq, ArrayShardDeleteResp,
    ArrayShardSliceReq, ArrayShardSliceResp, ArrayShardSurrogateBitmapReq,
    ArrayShardSurrogateBitmapResp,
};

/// Parameters common to read-path coordinator entry points (broadcast fan-out).
pub struct ArrayCoordParams {
    pub source_node: u64,
    /// Pre-computed target shard IDs (overlapping shards for reads).
    pub shard_ids: Vec<u32>,
    /// Per-shard RPC timeout in milliseconds.
    pub timeout_ms: u64,
    /// Hilbert routing granularity (1–16). 0 means no shard-side routing
    /// validation (e.g. when the coordinator was constructed without slice
    /// range information, as in tests or unbounded scans).
    pub prefix_bits: u8,
    /// Inclusive Hilbert-prefix ranges `(lo, hi)` that this read covers.
    /// Forwarded to each shard so it can verify it still owns the range.
    /// Empty means unbounded — the shard skips routing validation.
    pub slice_hilbert_ranges: Vec<(u64, u64)>,
}

/// Result of a coordinated slice fan-out.
///
/// Carries the merged shard rows together with the OR-reduced
/// `truncated_before_horizon` flag so the upstream caller can surface a
/// below-horizon warning to the client. Mirrors the single-node
/// `ArraySliceResponse` shape so downstream encoding is symmetric.
#[derive(Debug, Clone, Default)]
pub struct CoordSliceResult {
    pub rows: Vec<Vec<u8>>,
    pub truncated_before_horizon: bool,
}

/// Result of a coordinated aggregate fan-out.
///
/// Carries the merged per-group partials together with the OR-reduced
/// `truncated_before_horizon` flag, mirroring [`CoordSliceResult`]. Dropping
/// the flag here would let a below-horizon bitemporal aggregate report
/// complete results — the same silent-partial-success bug the slice path
/// avoids.
#[derive(Debug, Clone, Default)]
pub struct CoordAggResult {
    pub partials: Vec<ArrayAggPartial>,
    pub truncated_before_horizon: bool,
}

/// Compute the inclusive Hilbert-prefix range `[lo, hi]` that vShard `shard_id`
/// owns given the array's routing granularity `prefix_bits`.
///
/// Each bucket `b = shard_id / stride` covers the Hilbert range
/// `[b << (64 - prefix_bits), ((b + 1) << (64 - prefix_bits)) - 1]`.
/// The stride is `VSHARD_COUNT >> prefix_bits` (floored at 1).
pub(super) fn shard_hilbert_range_for_vshard(shard_id: u32, prefix_bits: u8) -> (u64, u64) {
    use crate::routing::VSHARD_COUNT;
    let stride = (VSHARD_COUNT >> (prefix_bits as u32)).max(1);
    let bucket = shard_id / stride;
    let shift = 64u8.saturating_sub(prefix_bits);
    let lo = (bucket as u64) << shift;
    let hi = if shift == 0 {
        u64::MAX
    } else {
        lo.saturating_add((1u64 << shift).saturating_sub(1))
    };
    (lo, hi)
}

/// Coordinator for distributed array read operations.
pub struct ArrayCoordinator {
    pub(super) params: ArrayCoordParams,
    pub(super) dispatch: Arc<dyn ShardRpcDispatch>,
    pub(super) circuit_breaker: Arc<CircuitBreaker>,
}

impl ArrayCoordinator {
    pub fn new(
        params: ArrayCoordParams,
        dispatch: Arc<dyn ShardRpcDispatch>,
        circuit_breaker: Arc<CircuitBreaker>,
    ) -> Self {
        Self {
            params,
            dispatch,
            circuit_breaker,
        }
    }

    /// Construct an `ArrayCoordinator` whose target shards are computed from
    /// the Hilbert-prefix ranges that overlap a slice predicate.
    ///
    /// `slice_hilbert_ranges` — `(lo, hi)` pairs computed by the planner from
    /// the `Slice` predicate. Pass an empty slice for an unbounded scan.
    /// `prefix_bits` — the array's routing granularity from the catalog entry.
    /// `total_shards` — the number of active vShards in the cluster.
    pub fn for_slice(
        source_node: u64,
        timeout_ms: u64,
        slice_hilbert_ranges: &[(u64, u64)],
        prefix_bits: u8,
        total_shards: u32,
        dispatch: Arc<dyn ShardRpcDispatch>,
        circuit_breaker: Arc<CircuitBreaker>,
    ) -> crate::error::Result<Self> {
        let shard_ids = super::super::routing::array_vshards_for_slice(
            slice_hilbert_ranges,
            prefix_bits,
            total_shards,
        )?;
        Ok(Self {
            params: ArrayCoordParams {
                source_node,
                shard_ids,
                timeout_ms,
                prefix_bits,
                slice_hilbert_ranges: slice_hilbert_ranges.to_vec(),
            },
            dispatch,
            circuit_breaker,
        })
    }

    /// Fan out a coord-range slice to all target shards and merge the rows.
    ///
    /// Each shard receives the full slice request with the caller-supplied
    /// `limit` pushed down so shards can stop scanning early. The coordinator
    /// stamps a per-shard `shard_hilbert_range` so each shard only returns
    /// cells whose Hilbert prefix falls within its owned range, preventing
    /// duplicate rows in single-node harnesses where all vShards share one
    /// Data Plane. The coordinator applies the same `limit` as a final
    /// cut-off on the merged result.
    ///
    /// When `system_time` is `AllVersions`, the coordinator merge-sorts rows
    /// from all shards by `ArrayCell::system_time` ascending (k-way merge via
    /// `merge_slice_rows_sorted`) before applying the limit, to preserve global
    /// audit ordering. For `Current`/`AsOf`, rows are concatenated in arrival
    /// order (existing behavior).
    ///
    /// Returns merged rows plus the OR-reduced `truncated_before_horizon`
    /// flag across all shards. If any shard fails the entire operation
    /// returns `Err` — partial results are not silently dropped.
    pub async fn coord_slice(
        &self,
        req: ArrayShardSliceReq,
        coordinator_limit: u32,
        system_time: nodedb_types::SystemTimeScope,
    ) -> Result<CoordSliceResult> {
        let prefix_bits = self.params.prefix_bits;
        let per_shard: Vec<(u32, Vec<u8>)> = self
            .params
            .shard_ids
            .iter()
            .map(|&shard_id| {
                let shard_hilbert_range = if prefix_bits > 0 {
                    Some(shard_hilbert_range_for_vshard(shard_id, prefix_bits))
                } else {
                    None
                };
                let per_shard_req = ArrayShardSliceReq {
                    prefix_bits,
                    slice_hilbert_ranges: self.params.slice_hilbert_ranges.clone(),
                    shard_hilbert_range,
                    ..req.clone()
                };
                let bytes =
                    zerompk::to_msgpack_vec(&per_shard_req).map_err(|e| ClusterError::Codec {
                        detail: format!("ArrayShardSliceReq serialise: {e}"),
                    })?;
                Ok((shard_id, bytes))
            })
            .collect::<Result<Vec<_>>>()?;

        let fo_params = FanOutPartitionedParams {
            source_node: self.params.source_node,
            timeout_ms: self.params.timeout_ms,
        };
        let raw = fan_out_partitioned(
            &fo_params,
            super::super::opcodes::ARRAY_SHARD_SLICE_REQ,
            &per_shard,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await?;
        let resps = decode_resps::<ArrayShardSliceResp>(&raw)?;
        let truncated_before_horizon =
            super::super::merge::any_truncated_before_horizon_slice(&resps);
        let rows = if system_time.is_all_versions() {
            // Audit-log: merge-sort by ArrayCell::system_time ascending across
            // shards to preserve global ordering, then truncate to limit.
            merge_slice_rows_sorted(&resps, coordinator_limit)?
        } else {
            merge_slice_rows(&resps, coordinator_limit)
        };
        Ok(CoordSliceResult {
            rows,
            truncated_before_horizon,
        })
    }

    /// Fan out an aggregate request and reduce partial aggregates from all shards.
    ///
    /// Each shard receives its own `shard_hilbert_range` so it can apply a
    /// Hilbert-prefix pre-filter and only count cells in its partition. This
    /// prevents double-counting in configurations where multiple vShards share
    /// a single Data Plane executor (e.g. single-node harnesses).
    pub async fn coord_agg(&self, req: ArrayShardAggReq) -> Result<CoordAggResult> {
        let prefix_bits = self.params.prefix_bits;
        let per_shard: Vec<(u32, Vec<u8>)> = self
            .params
            .shard_ids
            .iter()
            .map(|&shard_id| {
                let hilbert_range = if prefix_bits > 0 {
                    Some(shard_hilbert_range_for_vshard(shard_id, prefix_bits))
                } else {
                    None
                };
                let per_shard_req = ArrayShardAggReq {
                    shard_hilbert_range: hilbert_range,
                    ..req.clone()
                };
                let bytes =
                    zerompk::to_msgpack_vec(&per_shard_req).map_err(|e| ClusterError::Codec {
                        detail: format!("ArrayShardAggReq serialise: {e}"),
                    })?;
                Ok((shard_id, bytes))
            })
            .collect::<Result<Vec<_>>>()?;

        let fo_params = FanOutPartitionedParams {
            source_node: self.params.source_node,
            timeout_ms: self.params.timeout_ms,
        };
        let raw = fan_out_partitioned(
            &fo_params,
            super::super::opcodes::ARRAY_SHARD_AGG_REQ,
            &per_shard,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await?;
        let resps = decode_resps::<ArrayShardAggResp>(&raw)?;
        Ok(CoordAggResult {
            partials: reduce_agg_partials(&resps),
            truncated_before_horizon: any_truncated_before_horizon_agg(&resps),
        })
    }

    /// Forward a coord-based delete to the shard(s) that own the cells.
    pub async fn coord_delete(
        &self,
        req: ArrayShardDeleteReq,
    ) -> Result<Vec<ArrayShardDeleteResp>> {
        let req_bytes = zerompk::to_msgpack_vec(&req).map_err(|e| ClusterError::Codec {
            detail: format!("ArrayShardDeleteReq serialise: {e}"),
        })?;
        let raw = fan_out(
            &self.fan_out_params(),
            super::super::opcodes::ARRAY_SHARD_DELETE_REQ,
            &req_bytes,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await?;
        decode_resps::<ArrayShardDeleteResp>(&raw)
    }

    /// Fan out a surrogate bitmap scan, collect per-shard bitmap bytes, and
    /// union all bitmaps on the coordinator.
    ///
    /// Returns the zerompk-encoded union `SurrogateBitmap` covering all shards.
    pub async fn coord_surrogate_bitmap_scan(
        &self,
        req: ArrayShardSurrogateBitmapReq,
    ) -> Result<Vec<ArrayShardSurrogateBitmapResp>> {
        let req_bytes = zerompk::to_msgpack_vec(&req).map_err(|e| ClusterError::Codec {
            detail: format!("ArrayShardSurrogateBitmapReq serialise: {e}"),
        })?;
        let raw = fan_out(
            &self.fan_out_params(),
            super::super::opcodes::ARRAY_SHARD_SURROGATE_BITMAP_REQ,
            &req_bytes,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await?;
        decode_resps::<ArrayShardSurrogateBitmapResp>(&raw)
    }

    pub(super) fn fan_out_params(&self) -> FanOutParams {
        FanOutParams {
            shard_ids: self.params.shard_ids.clone(),
            timeout_ms: self.params.timeout_ms,
            source_node: self.params.source_node,
        }
    }
}

/// Deserialise a slice of raw `(shard_id, bytes)` pairs into typed responses.
pub(super) fn decode_resps<T>(raw: &[(u32, Vec<u8>)]) -> Result<Vec<T>>
where
    T: for<'a> zerompk::FromMessagePack<'a>,
{
    raw.iter()
        .map(|(_, bytes)| {
            zerompk::from_msgpack(bytes).map_err(|e| ClusterError::Codec {
                detail: format!("array response deserialise: {e}"),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use crate::circuit_breaker::CircuitBreakerConfig;
    use crate::wire::{VShardEnvelope, VShardMessageType};

    use super::*;

    /// Mock dispatch that returns a pre-serialised `ArrayShardSliceResp`.
    struct SliceEchoDispatch {
        /// Rows to return from each shard.
        rows: Vec<Vec<u8>>,
    }

    #[async_trait]
    impl ShardRpcDispatch for SliceEchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            let resp = ArrayShardSliceResp {
                shard_id: req.vshard_id,
                rows_msgpack: self.rows.clone(),
                truncated: false,
                truncated_before_horizon: false,
            };
            let payload = zerompk::to_msgpack_vec(&resp).unwrap();
            Ok(VShardEnvelope::new(
                VShardMessageType::ArrayShardSliceResp,
                req.target_node,
                req.source_node,
                req.vshard_id,
                payload,
            ))
        }
    }

    /// Mock dispatch that returns a pre-canned `ArrayShardAggResp`.
    struct AggEchoDispatch {
        partials: Vec<ArrayAggPartial>,
    }

    #[async_trait]
    impl ShardRpcDispatch for AggEchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            let resp = ArrayShardAggResp {
                shard_id: req.vshard_id,
                partials: self.partials.clone(),
                truncated_before_horizon: false,
            };
            let payload = zerompk::to_msgpack_vec(&resp).unwrap();
            Ok(VShardEnvelope::new(
                VShardMessageType::ArrayShardSliceResp,
                req.target_node,
                req.source_node,
                req.vshard_id,
                payload,
            ))
        }
    }

    fn make_coordinator(
        shard_ids: Vec<u32>,
        dispatch: Arc<dyn ShardRpcDispatch>,
    ) -> ArrayCoordinator {
        ArrayCoordinator::new(
            ArrayCoordParams {
                source_node: 1,
                shard_ids,
                timeout_ms: 1000,
                // Tests use prefix_bits=0 so shard-side routing validation
                // is skipped — mock executors don't need to match Hilbert
                // ownership.
                prefix_bits: 0,
                slice_hilbert_ranges: vec![],
            },
            dispatch,
            Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default())),
        )
    }

    #[tokio::test]
    async fn coord_slice_merges_rows_from_all_shards() {
        let row_a = zerompk::to_msgpack_vec(&"row-a").unwrap();
        let row_b = zerompk::to_msgpack_vec(&"row-b").unwrap();
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(SliceEchoDispatch {
            rows: vec![row_a.clone(), row_b.clone()],
        });
        let coord = make_coordinator(vec![0, 1, 2], dispatch);
        let req = ArrayShardSliceReq {
            array_id_msgpack: vec![],
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 100,
            cell_filter_msgpack: vec![],
            prefix_bits: 0,
            slice_hilbert_ranges: vec![],
            shard_hilbert_range: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            txn_id: None,
        };

        // 3 shards × 2 rows each = 6 merged rows.
        let result = coord
            .coord_slice(req, 0, nodedb_types::SystemTimeScope::Current)
            .await
            .expect("coord_slice should succeed");
        assert_eq!(result.rows.len(), 6);
        assert!(!result.truncated_before_horizon);
    }

    #[tokio::test]
    async fn coord_slice_applies_coordinator_limit() {
        let row = zerompk::to_msgpack_vec(&"row").unwrap();
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(SliceEchoDispatch {
            rows: vec![row.clone(), row.clone(), row.clone()],
        });
        // 2 shards × 3 rows = 6 total, but limit = 4.
        let coord = make_coordinator(vec![0, 1], dispatch);
        let req = ArrayShardSliceReq {
            array_id_msgpack: vec![],
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 3,
            cell_filter_msgpack: vec![],
            prefix_bits: 0,
            slice_hilbert_ranges: vec![],
            shard_hilbert_range: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            txn_id: None,
        };

        let result = coord
            .coord_slice(req, 4, nodedb_types::SystemTimeScope::Current)
            .await
            .expect("coord_slice with limit should succeed");
        assert_eq!(result.rows.len(), 4);
    }

    fn make_agg_req() -> ArrayShardAggReq {
        // Sum reducer c_enum = 0.
        ArrayShardAggReq {
            array_id_msgpack: vec![],
            attr_idx: 0,
            reducer_msgpack: vec![0x00],
            group_by_dim: -1,
            cell_filter_msgpack: vec![],
            shard_hilbert_range: None,
            system_as_of: None,
            valid_at_ms: None,
            txn_id: None,
        }
    }

    #[tokio::test]
    async fn coord_agg_merges_scalar_partials_from_shards() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(AggEchoDispatch {
            partials: vec![ArrayAggPartial::from_single(0, 10.0)],
        });
        // 3 shards each returning a partial with sum=10 → merged sum=30.
        let coord = make_coordinator(vec![0, 1, 2], dispatch);
        let merged = coord
            .coord_agg(make_agg_req())
            .await
            .expect("coord_agg should succeed");

        assert_eq!(merged.partials.len(), 1);
        assert_eq!(merged.partials[0].count, 3);
        assert!((merged.partials[0].sum - 30.0).abs() < f64::EPSILON);
        assert!(!merged.truncated_before_horizon);
    }

    #[tokio::test]
    async fn coord_agg_with_empty_shards_returns_empty() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(AggEchoDispatch { partials: vec![] });
        let coord = make_coordinator(vec![0, 1], dispatch);
        let merged = coord
            .coord_agg(make_agg_req())
            .await
            .expect("coord_agg with empty shards should succeed");
        assert!(merged.partials.is_empty());
    }

    #[tokio::test]
    async fn coord_agg_merges_grouped_partials_across_shards() {
        // Shard 0 returns group_key=0 partial, shard 1 also group_key=0 + group_key=1.
        struct GroupedDispatch {
            shard0_partials: Vec<ArrayAggPartial>,
            shard1_partials: Vec<ArrayAggPartial>,
        }

        #[async_trait]
        impl ShardRpcDispatch for GroupedDispatch {
            async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
                let partials = if req.vshard_id == 0 {
                    self.shard0_partials.clone()
                } else {
                    self.shard1_partials.clone()
                };
                let resp = ArrayShardAggResp {
                    shard_id: req.vshard_id,
                    partials,
                    truncated_before_horizon: false,
                };
                let payload = zerompk::to_msgpack_vec(&resp).unwrap();
                Ok(VShardEnvelope::new(
                    VShardMessageType::ArrayShardSliceResp,
                    req.target_node,
                    req.source_node,
                    req.vshard_id,
                    payload,
                ))
            }
        }

        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(GroupedDispatch {
            shard0_partials: vec![ArrayAggPartial::from_single(0, 5.0)],
            shard1_partials: vec![
                ArrayAggPartial::from_single(0, 15.0),
                ArrayAggPartial::from_single(1, 20.0),
            ],
        });
        let coord = make_coordinator(vec![0, 1], dispatch);
        let merged = coord
            .coord_agg(make_agg_req())
            .await
            .expect("grouped coord_agg should succeed");

        // group_key=0: sum=5+15=20, count=2; group_key=1: sum=20, count=1.
        assert_eq!(merged.partials.len(), 2);
        let g0 = merged
            .partials
            .iter()
            .find(|p| p.group_key == 0)
            .expect("group 0");
        let g1 = merged
            .partials
            .iter()
            .find(|p| p.group_key == 1)
            .expect("group 1");
        assert!((g0.sum - 20.0).abs() < f64::EPSILON);
        assert_eq!(g0.count, 2);
        assert!((g1.sum - 20.0).abs() < f64::EPSILON);
        assert_eq!(g1.count, 1);
    }

    #[tokio::test]
    async fn coord_agg_or_reduces_truncated_before_horizon() {
        // One shard reports below-horizon; the coordinator must OR-reduce the
        // flag so the upstream caller can surface an incomplete-result signal.
        // Dropping it here was a silent-partial-success bug.
        struct HorizonDispatch;

        #[async_trait]
        impl ShardRpcDispatch for HorizonDispatch {
            async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
                // Shard 1 is below horizon (zero partials); shard 0 has data.
                let (partials, below) = if req.vshard_id == 0 {
                    (vec![ArrayAggPartial::from_single(0, 10.0)], false)
                } else {
                    (vec![], true)
                };
                let resp = ArrayShardAggResp {
                    shard_id: req.vshard_id,
                    partials,
                    truncated_before_horizon: below,
                };
                let payload = zerompk::to_msgpack_vec(&resp).unwrap();
                Ok(VShardEnvelope::new(
                    VShardMessageType::ArrayShardSliceResp,
                    req.target_node,
                    req.source_node,
                    req.vshard_id,
                    payload,
                ))
            }
        }

        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(HorizonDispatch);
        let coord = make_coordinator(vec![0, 1], dispatch);
        let merged = coord
            .coord_agg(make_agg_req())
            .await
            .expect("coord_agg should succeed");
        assert!(
            merged.truncated_before_horizon,
            "coordinator must OR-reduce the below-horizon flag across shards"
        );
    }

    #[tokio::test]
    async fn coord_slice_zero_limit_returns_all() {
        let row = zerompk::to_msgpack_vec(&"r").unwrap();
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(SliceEchoDispatch {
            rows: vec![row.clone(); 10],
        });
        let coord = make_coordinator(vec![0, 1], dispatch);
        let req = ArrayShardSliceReq {
            array_id_msgpack: vec![],
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 0,
            cell_filter_msgpack: vec![],
            prefix_bits: 0,
            slice_hilbert_ranges: vec![],
            shard_hilbert_range: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            txn_id: None,
        };

        // coordinator_limit = 0 → no cutoff → 20 rows.
        let result = coord
            .coord_slice(req, 0, nodedb_types::SystemTimeScope::Current)
            .await
            .expect("coord_slice unlimited should succeed");
        assert_eq!(result.rows.len(), 20);
    }
}
