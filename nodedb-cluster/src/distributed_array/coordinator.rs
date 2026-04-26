//! Distributed array query coordinator.
//!
//! Runs on the Control Plane. Dispatches array read/write operations
//! to the owning vShards via `fan_out` / `fan_out_partitioned`, collects
//! shard responses, and merges them. Mirrors the shape of `TsCoordinator`
//! and `SpatialScatterGather`.
//!
//! Each public entry point corresponds to one `ArrayOp` variant that
//! has a distributed counterpart:
//!
//! | Entry point                   | Distributed ArrayOp      |
//! |-------------------------------|--------------------------|
//! | `coord_slice`                 | `Slice`                  |
//! | `coord_agg`                   | `Aggregate`              |
//! | `coord_put`                   | `Put`                    |
//! | `coord_delete`                | `Delete`                 |
//! | `coord_surrogate_bitmap_scan` | `SurrogateBitmapScan`    |
//!
//! ## Routing
//!
//! Read operations (`coord_slice`, `coord_agg`, `coord_surrogate_bitmap_scan`)
//! receive the target `shard_ids` pre-computed by the caller via
//! `array_vshards_for_slice` — only shards whose Hilbert range overlaps
//! the slice predicate are contacted.
//!
//! Write operations (`coord_put`) receive pre-partitioned cell batches: the
//! caller (the Control Plane planner) groups cells by Hilbert prefix bucket
//! using `array_vshard_for_tile` and passes one `ArrayShardPutReq` per shard.
//! This coordinator does not decode the cell payload — it dispatches each
//! shard's batch via `fan_out_partitioned`.

use std::sync::Arc;

use crate::circuit_breaker::CircuitBreaker;
use crate::error::{ClusterError, Result};

use super::merge::{ArrayAggPartial, merge_slice_rows, reduce_agg_partials};
use super::partition::{partition_delete_coords, partition_put_cells};
use super::rpc::ShardRpcDispatch;
use super::scatter::{FanOutParams, FanOutPartitionedParams, fan_out, fan_out_partitioned};
use super::wire::{
    ArrayShardAggReq, ArrayShardAggResp, ArrayShardDeleteReq, ArrayShardDeleteResp,
    ArrayShardPutReq, ArrayShardPutResp, ArrayShardSliceReq, ArrayShardSliceResp,
    ArrayShardSurrogateBitmapReq, ArrayShardSurrogateBitmapResp,
};

/// Parameters common to read-path coordinator entry points (broadcast fan-out).
pub struct ArrayCoordParams {
    pub source_node: u64,
    /// Pre-computed target shard IDs (overlapping shards for reads).
    pub shard_ids: Vec<u16>,
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

/// Parameters for write-path coordinator entry points (partitioned fan-out).
pub struct ArrayWriteCoordParams {
    pub source_node: u64,
    pub timeout_ms: u64,
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

/// Compute the inclusive Hilbert-prefix range `[lo, hi]` that vShard `shard_id`
/// owns given the array's routing granularity `prefix_bits`.
///
/// Each bucket `b = shard_id / stride` covers the Hilbert range
/// `[b << (64 - prefix_bits), ((b + 1) << (64 - prefix_bits)) - 1]`.
/// The stride is `VSHARD_COUNT >> prefix_bits` (floored at 1).
fn shard_hilbert_range_for_vshard(shard_id: u16, prefix_bits: u8) -> (u64, u64) {
    use crate::routing::VSHARD_COUNT;
    let stride = ((VSHARD_COUNT as u32) >> (prefix_bits as u32)).max(1) as u16;
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

/// Coordinator for distributed array operations.
pub struct ArrayCoordinator {
    params: ArrayCoordParams,
    dispatch: Arc<dyn ShardRpcDispatch>,
    circuit_breaker: Arc<CircuitBreaker>,
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
        total_shards: u16,
        dispatch: Arc<dyn ShardRpcDispatch>,
        circuit_breaker: Arc<CircuitBreaker>,
    ) -> crate::error::Result<Self> {
        let shard_ids = super::routing::array_vshards_for_slice(
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
    /// Returns merged rows plus the OR-reduced `truncated_before_horizon`
    /// flag across all shards. If any shard fails the entire operation
    /// returns `Err` — partial results are not silently dropped.
    pub async fn coord_slice(
        &self,
        req: ArrayShardSliceReq,
        coordinator_limit: u32,
    ) -> Result<CoordSliceResult> {
        let prefix_bits = self.params.prefix_bits;
        let per_shard: Vec<(u16, Vec<u8>)> = self
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
            super::opcodes::ARRAY_SHARD_SLICE_REQ,
            &per_shard,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await?;
        let resps = decode_resps::<ArrayShardSliceResp>(&raw)?;
        let truncated_before_horizon = super::merge::any_truncated_before_horizon_slice(&resps);
        let rows = merge_slice_rows(&resps, coordinator_limit);
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
    pub async fn coord_agg(&self, req: ArrayShardAggReq) -> Result<Vec<ArrayAggPartial>> {
        let prefix_bits = self.params.prefix_bits;
        let per_shard: Vec<(u16, Vec<u8>)> = self
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
            super::opcodes::ARRAY_SHARD_AGG_REQ,
            &per_shard,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await?;
        let resps = decode_resps::<ArrayShardAggResp>(&raw)?;
        Ok(reduce_agg_partials(&resps))
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
            super::opcodes::ARRAY_SHARD_DELETE_REQ,
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
            super::opcodes::ARRAY_SHARD_SURROGATE_BITMAP_REQ,
            &req_bytes,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await?;
        decode_resps::<ArrayShardSurrogateBitmapResp>(&raw)
    }

    fn fan_out_params(&self) -> FanOutParams {
        FanOutParams {
            shard_ids: self.params.shard_ids.clone(),
            timeout_ms: self.params.timeout_ms,
            source_node: self.params.source_node,
        }
    }
}

/// Forward pre-partitioned cell writes to the owning shards.
///
/// The caller groups cells by Hilbert prefix bucket using
/// `array_vshard_for_tile` and produces one `ArrayShardPutReq` per
/// target shard. This function dispatches each batch to its shard via
/// `fan_out_partitioned` and collects acknowledgements.
///
/// No cell payload is decoded inside this function — the coordinator
/// has no dependency on `nodedb-array`.
pub async fn coord_put_partitioned(
    params: &ArrayWriteCoordParams,
    per_shard: Vec<(u16, ArrayShardPutReq)>,
    dispatch: &Arc<dyn ShardRpcDispatch>,
    circuit_breaker: &Arc<CircuitBreaker>,
) -> Result<Vec<ArrayShardPutResp>> {
    if per_shard.is_empty() {
        return Ok(Vec::new());
    }

    let fo_params = FanOutPartitionedParams {
        timeout_ms: params.timeout_ms,
        source_node: params.source_node,
    };

    let encoded: Result<Vec<(u16, Vec<u8>)>> = per_shard
        .iter()
        .map(|(shard_id, req)| {
            zerompk::to_msgpack_vec(req)
                .map(|bytes| (*shard_id, bytes))
                .map_err(|e| ClusterError::Codec {
                    detail: format!("ArrayShardPutReq serialise (shard {shard_id}): {e}"),
                })
        })
        .collect();

    let raw = fan_out_partitioned(
        &fo_params,
        super::opcodes::ARRAY_SHARD_PUT_REQ,
        &encoded?,
        dispatch,
        circuit_breaker,
    )
    .await?;

    decode_resps::<ArrayShardPutResp>(&raw)
}

/// Partition a flat cell list by Hilbert tile and fan out to owning shards.
///
/// `cells` — each element is `(hilbert_prefix, zerompk-encoded single-cell bytes)`.
/// The Hilbert prefix is computed by the caller (the Control Plane planner) from
/// the cell's coord tuple and the array schema; this function does not decode
/// cell bytes.
///
/// `prefix_bits` — routing granularity (1–16) from the array catalog entry.
/// `wal_lsn` — WAL sequence number allocated by the Control Plane for this batch.
///
/// Atomicity is per-shard only: if cells span multiple shards each shard's write
/// is committed independently. A partial failure returns the first error encountered;
/// cells that were already committed to other shards are not rolled back.
pub async fn coord_put(
    params: &ArrayWriteCoordParams,
    array_id_msgpack: Vec<u8>,
    prefix_bits: u8,
    wal_lsn: u64,
    cells: &[(u64, Vec<u8>)],
    dispatch: &Arc<dyn ShardRpcDispatch>,
    circuit_breaker: &Arc<CircuitBreaker>,
) -> Result<Vec<ArrayShardPutResp>> {
    if cells.is_empty() {
        return Ok(Vec::new());
    }

    let buckets = partition_put_cells(cells, prefix_bits)?;

    let per_shard: Vec<(u16, ArrayShardPutReq)> = buckets
        .into_iter()
        .map(|b| {
            let req = ArrayShardPutReq {
                array_id_msgpack: array_id_msgpack.clone(),
                cells_msgpack: b.cells_msgpack,
                wal_lsn,
                representative_hilbert_prefix: b.representative_hilbert_prefix,
                prefix_bits,
            };
            (b.vshard_id, req)
        })
        .collect();

    coord_put_partitioned(params, per_shard, dispatch, circuit_breaker).await
}

/// Partition a flat coord list by Hilbert tile and fan delete requests to owning shards.
///
/// `coords` — each element is `(hilbert_prefix, zerompk-encoded single-coord bytes)`.
/// `prefix_bits` — routing granularity (1–16).
/// `wal_lsn` — WAL sequence number allocated by the Control Plane.
///
/// Atomicity is per-shard only (same contract as `coord_put`).
pub async fn coord_delete(
    params: &ArrayWriteCoordParams,
    array_id_msgpack: Vec<u8>,
    prefix_bits: u8,
    wal_lsn: u64,
    coords: &[(u64, Vec<u8>)],
    dispatch: &Arc<dyn ShardRpcDispatch>,
    circuit_breaker: &Arc<CircuitBreaker>,
) -> Result<Vec<ArrayShardDeleteResp>> {
    if coords.is_empty() {
        return Ok(Vec::new());
    }

    let buckets = partition_delete_coords(coords, prefix_bits)?;

    let fo_params = FanOutPartitionedParams {
        timeout_ms: params.timeout_ms,
        source_node: params.source_node,
    };

    let encoded: Result<Vec<(u16, Vec<u8>)>> = buckets
        .into_iter()
        .map(|b| {
            let req = ArrayShardDeleteReq {
                array_id_msgpack: array_id_msgpack.clone(),
                coords_msgpack: b.coords_msgpack,
                wal_lsn,
                representative_hilbert_prefix: b.representative_hilbert_prefix,
                prefix_bits,
            };
            zerompk::to_msgpack_vec(&req)
                .map(|bytes| (b.vshard_id, bytes))
                .map_err(|e| ClusterError::Codec {
                    detail: format!("ArrayShardDeleteReq serialise (shard {}): {e}", b.vshard_id),
                })
        })
        .collect();

    let raw = fan_out_partitioned(
        &fo_params,
        super::opcodes::ARRAY_SHARD_DELETE_REQ,
        &encoded?,
        dispatch,
        circuit_breaker,
    )
    .await?;

    decode_resps::<ArrayShardDeleteResp>(&raw)
}

/// Deserialise a slice of raw `(shard_id, bytes)` pairs into typed responses.
fn decode_resps<T>(raw: &[(u16, Vec<u8>)]) -> Result<Vec<T>>
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
    use std::sync::Arc;

    use async_trait::async_trait;

    use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
    use crate::error::Result;
    use crate::wire::{VShardEnvelope, VShardMessageType};

    use super::super::merge::ArrayAggPartial;
    use super::super::rpc::ShardRpcDispatch;
    use super::super::wire::{
        ArrayShardAggReq, ArrayShardAggResp, ArrayShardSliceReq, ArrayShardSliceResp,
    };
    use super::{ArrayCoordParams, ArrayCoordinator};

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
        shard_ids: Vec<u16>,
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
            system_as_of: None,
            valid_at_ms: None,
        };

        // 3 shards × 2 rows each = 6 merged rows.
        let result = coord
            .coord_slice(req, 0)
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
            system_as_of: None,
            valid_at_ms: None,
        };

        let result = coord
            .coord_slice(req, 4)
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

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].count, 3);
        assert!((merged[0].sum - 30.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn coord_agg_with_empty_shards_returns_empty() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(AggEchoDispatch { partials: vec![] });
        let coord = make_coordinator(vec![0, 1], dispatch);
        let merged = coord
            .coord_agg(make_agg_req())
            .await
            .expect("coord_agg with empty shards should succeed");
        assert!(merged.is_empty());
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
        assert_eq!(merged.len(), 2);
        let g0 = merged.iter().find(|p| p.group_key == 0).expect("group 0");
        let g1 = merged.iter().find(|p| p.group_key == 1).expect("group 1");
        assert!((g0.sum - 20.0).abs() < f64::EPSILON);
        assert_eq!(g0.count, 2);
        assert!((g1.sum - 20.0).abs() < f64::EPSILON);
        assert_eq!(g1.count, 1);
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
            system_as_of: None,
            valid_at_ms: None,
        };

        // coordinator_limit = 0 → no cutoff → 20 rows.
        let result = coord
            .coord_slice(req, 0)
            .await
            .expect("coord_slice unlimited should succeed");
        assert_eq!(result.rows.len(), 20);
    }

    // ── coord_put / coord_delete tests ────────────────────────────────────

    use super::super::wire::{ArrayShardDeleteResp, ArrayShardPutReq, ArrayShardPutResp};
    use super::{ArrayWriteCoordParams, coord_delete, coord_put};
    use crate::error::ClusterError;

    /// Records which vShard IDs were called and echoes back an `ArrayShardPutResp`.
    struct PutEchoDispatch;

    #[async_trait]
    impl ShardRpcDispatch for PutEchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            let shard_req: ArrayShardPutReq = zerompk::from_msgpack(&req.payload).unwrap();
            let resp = ArrayShardPutResp {
                shard_id: req.vshard_id,
                applied_lsn: shard_req.wal_lsn,
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

    /// Dispatch that always returns a Codec error — used for failure-propagation tests.
    struct FailDispatch;

    #[async_trait]
    impl ShardRpcDispatch for FailDispatch {
        async fn call(&self, _req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            Err(ClusterError::Codec {
                detail: "injected failure".into(),
            })
        }
    }

    /// Echo dispatch for delete that returns an `ArrayShardDeleteResp`.
    struct DeleteEchoDispatch;

    #[async_trait]
    impl ShardRpcDispatch for DeleteEchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            // Decode a minimal ArrayShardDeleteReq to echo back the wal_lsn.
            use super::super::wire::ArrayShardDeleteReq;
            let shard_req: ArrayShardDeleteReq = zerompk::from_msgpack(&req.payload).unwrap();
            let resp = ArrayShardDeleteResp {
                shard_id: req.vshard_id,
                applied_lsn: shard_req.wal_lsn,
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

    fn write_params() -> ArrayWriteCoordParams {
        ArrayWriteCoordParams {
            source_node: 1,
            timeout_ms: 1000,
        }
    }

    fn cb() -> Arc<CircuitBreaker> {
        Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default()))
    }

    #[tokio::test]
    async fn coord_put_partitions_cells_by_tile() {
        // prefix_bits=10, stride=1 → vshard == top-10-bit bucket.
        // p0 → bucket 0 → vshard 0
        // p1 → bucket 1 → vshard 1
        // p2 → bucket 2 → vshard 2
        let p0 = 0x0000_0000_0000_0000u64;
        let p1 = 0x0040_0000_0000_0000u64;
        let p2 = 0x0080_0000_0000_0000u64;

        let cells = vec![
            (p0, vec![0x01u8]),
            (p1, vec![0x02u8]),
            (p0, vec![0x03u8]),
            (p2, vec![0x04u8]),
            (p1, vec![0x05u8]),
        ];

        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(PutEchoDispatch);
        let mut resps = coord_put(&write_params(), vec![], 10, 42, &cells, &dispatch, &cb())
            .await
            .expect("coord_put should succeed");

        resps.sort_by_key(|r| r.shard_id);
        assert_eq!(resps.len(), 3, "should fan-out to 3 shards");
        assert_eq!(resps[0].shard_id, 0);
        assert_eq!(resps[1].shard_id, 1);
        assert_eq!(resps[2].shard_id, 2);
        // Each shard echoes back wal_lsn=42.
        for r in &resps {
            assert_eq!(r.applied_lsn, 42);
        }
    }

    #[tokio::test]
    async fn coord_put_aggregates_partial_failures() {
        // A failing dispatch must surface as an error, not silent partial success.
        let cells = vec![(0u64, vec![0xAAu8])];
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(FailDispatch);
        let err = coord_put(&write_params(), vec![], 10, 1, &cells, &dispatch, &cb())
            .await
            .expect_err("coord_put with failing shard should return error");
        assert!(
            matches!(err, ClusterError::Codec { .. }),
            "expected Codec error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn coord_delete_partitions_by_tile() {
        let p0 = 0x0000_0000_0000_0000u64;
        let p1 = 0x0040_0000_0000_0000u64;

        let coords = vec![(p0, vec![0xAAu8]), (p1, vec![0xBBu8]), (p0, vec![0xCCu8])];

        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(DeleteEchoDispatch);
        let mut resps = coord_delete(&write_params(), vec![], 10, 55, &coords, &dispatch, &cb())
            .await
            .expect("coord_delete should succeed");

        resps.sort_by_key(|r| r.shard_id);
        assert_eq!(resps.len(), 2, "should fan-out to 2 shards");
        assert_eq!(resps[0].shard_id, 0);
        assert_eq!(resps[1].shard_id, 1);
        for r in &resps {
            assert_eq!(r.applied_lsn, 55);
        }
    }
}
