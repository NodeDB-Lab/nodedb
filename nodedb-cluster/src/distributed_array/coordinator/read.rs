// SPDX-License-Identifier: BUSL-1.1

//! Read-path coordinator for distributed array operations.
//!
//! [`ArrayCoordinator`] drives fan-out reads (`coord_slice`, `coord_agg`,
//! `coord_surrogate_bitmap_scan`) to the vShards that own a tile the slice
//! predicate covers.

use std::sync::Arc;

use crate::circuit_breaker::CircuitBreaker;
use crate::error::{ClusterError, Result};

use super::super::merge::{
    ArrayAggPartial, any_truncated_before_horizon_agg, merge_read_versions, merge_slice_rows,
    merge_slice_rows_sorted, reduce_agg_partials,
};
use super::super::routing::shard_bucket_for_vshard;
use super::super::rpc::ShardRpcDispatch;
use super::super::scatter::{
    ArrayCoordError, FanOutParams, FanOutPartitionedParams, fan_out, fan_out_partitioned,
};
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
    /// Routing granularity (1–16). 0 skips shard-side routing validation
    /// and the per-shard bucket filter.
    pub prefix_bits: u8,
    /// Tile keys this read covers, at least one per bucket. Forwarded to
    /// each shard so it checks that it still owns one. Empty means
    /// unbounded, and the shard skips routing validation.
    pub slice_tile_keys: Vec<u64>,
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
    /// The write versions every shard leg observed, one per vShard.
    pub read_versions: Vec<nodedb_types::ShardVersion>,
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
    /// The write versions every shard leg observed, one per vShard.
    pub read_versions: Vec<nodedb_types::ShardVersion>,
}

/// The bucket filter for a read sent to vShard `shard_id`. `None` when
/// `prefix_bits` is 0, which reads every tile.
pub(super) fn shard_bucket(
    shard_id: u32,
    prefix_bits: u8,
) -> Result<Option<nodedb_types::ShardBucket>> {
    if prefix_bits == 0 {
        return Ok(None);
    }
    shard_bucket_for_vshard(shard_id, prefix_bits).map(Some)
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

    /// Construct an `ArrayCoordinator` whose target shards own the tiles a
    /// slice predicate covers.
    ///
    /// `slice_tile_keys` — tile keys computed by the planner from the
    /// `Slice` predicate. Pass an empty slice for an unbounded scan.
    /// `prefix_bits` — the array's routing granularity from the catalog entry.
    /// `total_shards` — the number of active vShards in the cluster.
    pub fn for_slice(
        source_node: u64,
        timeout_ms: u64,
        slice_tile_keys: &[u64],
        prefix_bits: u8,
        total_shards: u32,
        dispatch: Arc<dyn ShardRpcDispatch>,
        circuit_breaker: Arc<CircuitBreaker>,
    ) -> crate::error::Result<Self> {
        let shard_ids = super::super::routing::array_vshards_for_slice(
            slice_tile_keys,
            prefix_bits,
            total_shards,
        )?;
        Ok(Self {
            params: ArrayCoordParams {
                source_node,
                shard_ids,
                timeout_ms,
                prefix_bits,
                slice_tile_keys: slice_tile_keys.to_vec(),
            },
            dispatch,
            circuit_breaker,
        })
    }

    /// Fan out a coord-range slice to all target shards and merge the rows.
    ///
    /// Each shard receives the full slice request with the caller-supplied
    /// `limit` pushed down so shards can stop scanning early. The coordinator
    /// stamps a per-shard `shard_bucket`, so each shard returns only cells
    /// whose tile key routes to it. vShards that share one Data Plane then
    /// return no duplicate rows. The coordinator applies the same `limit` as a final
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
    ) -> std::result::Result<CoordSliceResult, ArrayCoordError> {
        let prefix_bits = self.params.prefix_bits;
        let per_shard: Vec<(u32, Vec<u8>)> = self
            .params
            .shard_ids
            .iter()
            .map(|&shard_id| {
                let per_shard_req = ArrayShardSliceReq {
                    prefix_bits,
                    slice_tile_keys: self.params.slice_tile_keys.clone(),
                    shard_bucket: shard_bucket(shard_id, prefix_bits)?,
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
            read_versions: merge_read_versions(resps.iter().map(|r| r.read_versions.as_slice())),
        })
    }

    /// Fan out an aggregate request and reduce partial aggregates from all shards.
    ///
    /// Each shard receives its own `shard_bucket` and counts only cells whose
    /// tile key routes to it. vShards that share one Data Plane executor then
    /// count no cell twice.
    pub async fn coord_agg(
        &self,
        req: ArrayShardAggReq,
    ) -> std::result::Result<CoordAggResult, ArrayCoordError> {
        let prefix_bits = self.params.prefix_bits;
        let per_shard: Vec<(u32, Vec<u8>)> = self
            .params
            .shard_ids
            .iter()
            .map(|&shard_id| {
                let per_shard_req = ArrayShardAggReq {
                    shard_bucket: shard_bucket(shard_id, prefix_bits)?,
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
            read_versions: merge_read_versions(resps.iter().map(|r| r.read_versions.as_slice())),
        })
    }

    /// Forward a coord-based delete to the shard(s) that own the cells.
    pub async fn coord_delete(
        &self,
        req: ArrayShardDeleteReq,
    ) -> std::result::Result<Vec<ArrayShardDeleteResp>, ArrayCoordError> {
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
        Ok(decode_resps::<ArrayShardDeleteResp>(&raw)?)
    }

    /// Fan out a surrogate bitmap scan, collect per-shard bitmap bytes, and
    /// union all bitmaps on the coordinator.
    ///
    /// Returns the zerompk-encoded union `SurrogateBitmap` covering all shards.
    pub async fn coord_surrogate_bitmap_scan(
        &self,
        req: ArrayShardSurrogateBitmapReq,
    ) -> std::result::Result<Vec<ArrayShardSurrogateBitmapResp>, ArrayCoordError> {
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
        Ok(decode_resps::<ArrayShardSurrogateBitmapResp>(&raw)?)
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
    use crate::distributed_array::group_key::ArrayAggGroupKey;
    use crate::wire::{VShardEnvelope, VShardMessageType};

    fn g(k: i64) -> ArrayAggGroupKey {
        ArrayAggGroupKey::Int64(k)
    }

    use super::*;

    /// The version a mock shard leg reports for its vShard.
    fn leg_versions(vshard: u32) -> Vec<nodedb_types::ShardVersion> {
        vec![nodedb_types::ShardVersion {
            vshard,
            version: nodedb_types::WriteVersion::logged(1, u64::from(vshard) + 100),
        }]
    }

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
                read_versions: leg_versions(req.vshard_id),
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
                read_versions: leg_versions(req.vshard_id),
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
                // prefix_bits=0 skips shard-side routing validation and the
                // bucket filter, so mock executors ignore ownership.
                prefix_bits: 0,
                slice_tile_keys: vec![],
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
            slice_tile_keys: vec![],
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            txn_id: None,
            zone_msgpack: vec![],
            physical: false,
        };

        // 3 shards × 2 rows each = 6 merged rows.
        let result = coord
            .coord_slice(req, 0, nodedb_types::SystemTimeScope::Current)
            .await
            .expect("coord_slice should succeed");
        assert_eq!(result.rows.len(), 6);
        assert!(!result.truncated_before_horizon);
        let mut versions = result.read_versions;
        versions.sort();
        assert_eq!(
            versions,
            [0, 1, 2]
                .into_iter()
                .flat_map(leg_versions)
                .collect::<Vec<_>>(),
            "the merge keeps every shard leg's version"
        );
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
            slice_tile_keys: vec![],
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            txn_id: None,
            zone_msgpack: vec![],
            physical: false,
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
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            txn_id: None,
            region_msgpack: vec![],
            physical: false,
        }
    }

    #[tokio::test]
    async fn coord_agg_merges_scalar_partials_from_shards() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(AggEchoDispatch {
            partials: vec![ArrayAggPartial::from_single(ArrayAggGroupKey::Scalar, 10.0)],
        });
        // 3 shards each returning a partial with sum=10 → merged sum=30.
        let coord = make_coordinator(vec![0, 1, 2], dispatch);
        let merged = coord
            .coord_agg(make_agg_req())
            .await
            .expect("coord_agg should succeed");

        assert_eq!(merged.partials.len(), 1);
        assert_eq!(merged.partials[0].count, 3);
        assert!((merged.partials[0].sum.as_f64() - 30.0).abs() < f64::EPSILON);
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
                    read_versions: Vec::new(),
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
            shard0_partials: vec![ArrayAggPartial::from_single(g(0), 5.0)],
            shard1_partials: vec![
                ArrayAggPartial::from_single(g(0), 15.0),
                ArrayAggPartial::from_single(g(1), 20.0),
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
            .find(|p| p.group_key == g(0))
            .expect("group 0");
        let g1 = merged
            .partials
            .iter()
            .find(|p| p.group_key == g(1))
            .expect("group 1");
        assert!((g0.sum.as_f64() - 20.0).abs() < f64::EPSILON);
        assert_eq!(g0.count, 2);
        assert!((g1.sum.as_f64() - 20.0).abs() < f64::EPSILON);
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
                    (
                        vec![ArrayAggPartial::from_single(ArrayAggGroupKey::Scalar, 10.0)],
                        false,
                    )
                } else {
                    (vec![], true)
                };
                let resp = ArrayShardAggResp {
                    shard_id: req.vshard_id,
                    partials,
                    truncated_before_horizon: below,
                    read_versions: Vec::new(),
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
            slice_tile_keys: vec![],
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            txn_id: None,
            zone_msgpack: vec![],
            physical: false,
        };

        // coordinator_limit = 0 → no cutoff → 20 rows.
        let result = coord
            .coord_slice(req, 0, nodedb_types::SystemTimeScope::Current)
            .await
            .expect("coord_slice unlimited should succeed");
        assert_eq!(result.rows.len(), 20);
    }

    /// Serves a fixed set of tile keys, each as one row, keeping only the
    /// keys the request's `shard_bucket` admits.
    struct BucketFilterDispatch {
        tile_keys: Vec<u64>,
    }

    #[async_trait]
    impl ShardRpcDispatch for BucketFilterDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            let slice: ArrayShardSliceReq = zerompk::from_msgpack(&req.payload).unwrap();
            let rows = self
                .tile_keys
                .iter()
                .filter(|&&k| nodedb_types::array_route::shard_bucket_admits(slice.shard_bucket, k))
                .map(|k| zerompk::to_msgpack_vec(k).unwrap())
                .collect();
            let resp = ArrayShardSliceResp {
                shard_id: req.vshard_id,
                rows_msgpack: rows,
                truncated: false,
                truncated_before_horizon: false,
                read_versions: Vec::new(),
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

    /// Over every primary vShard, the bucket filters return each tile once.
    #[tokio::test]
    async fn bucket_filters_partition_every_tile_across_shards() {
        let tile_keys: Vec<u64> = (0u64..4096).chain([1 << 40, u64::MAX]).collect();
        for prefix_bits in [1u8, 8, 10, 16] {
            let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(BucketFilterDispatch {
                tile_keys: tile_keys.clone(),
            });
            let coord = ArrayCoordinator::for_slice(
                1,
                1000,
                &[],
                prefix_bits,
                crate::routing::VSHARD_COUNT,
                dispatch,
                Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default())),
            )
            .expect("for_slice");
            let req = ArrayShardSliceReq {
                array_id_msgpack: vec![],
                slice_msgpack: vec![],
                attr_projection: vec![],
                limit: 0,
                cell_filter_msgpack: vec![],
                prefix_bits: 0,
                slice_tile_keys: vec![],
                shard_bucket: None,
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
                txn_id: None,
                zone_msgpack: vec![],
                physical: false,
            };
            let result = coord
                .coord_slice(req, 0, nodedb_types::SystemTimeScope::Current)
                .await
                .expect("coord_slice");
            let mut got: Vec<u64> = result
                .rows
                .iter()
                .map(|r| zerompk::from_msgpack::<u64>(r).unwrap())
                .collect();
            got.sort_unstable();
            let mut want = tile_keys.clone();
            want.sort_unstable();
            assert_eq!(got, want, "prefix_bits={prefix_bits}");
        }
    }
}
