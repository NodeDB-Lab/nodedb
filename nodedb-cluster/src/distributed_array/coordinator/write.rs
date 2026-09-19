// SPDX-License-Identifier: BUSL-1.1

//! Write-path coordinator for distributed array operations.
//!
//! Provides [`coord_put`], [`coord_put_partitioned`], and [`coord_delete`]
//! — functions that partition a flat cell/coord list by Hilbert tile and
//! fan writes to the owning vShards.

use std::sync::Arc;

use crate::circuit_breaker::CircuitBreaker;
use crate::error::{ClusterError, Result};

use super::super::partition::{partition_delete_coords, partition_put_cells};
use super::super::rpc::ShardRpcDispatch;
use super::super::scatter::{FanOutPartitionedParams, fan_out_partitioned};
use super::super::wire::{
    ArrayShardDeleteReq, ArrayShardDeleteResp, ArrayShardPutReq, ArrayShardPutResp,
};
use super::read::decode_resps;

/// Parameters for write-path coordinator entry points (partitioned fan-out).
pub struct ArrayWriteCoordParams {
    pub source_node: u64,
    pub timeout_ms: u64,
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
    per_shard: Vec<(u32, ArrayShardPutReq)>,
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

    let encoded: Result<Vec<(u32, Vec<u8>)>> = per_shard
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
        super::super::opcodes::ARRAY_SHARD_PUT_REQ,
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

    let per_shard: Vec<(u32, ArrayShardPutReq)> = buckets
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

    let encoded: Result<Vec<(u32, Vec<u8>)>> = buckets
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
        super::super::opcodes::ARRAY_SHARD_DELETE_REQ,
        &encoded?,
        dispatch,
        circuit_breaker,
    )
    .await?;

    decode_resps::<ArrayShardDeleteResp>(&raw)
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
    use crate::wire::{VShardEnvelope, VShardMessageType};

    use super::*;

    /// Records which vShard IDs were called and echoes back an `ArrayShardPutResp`.
    struct PutEchoDispatch;

    #[async_trait]
    impl ShardRpcDispatch for PutEchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            let shard_req: ArrayShardPutReq = zerompk::from_msgpack(&req.payload).unwrap();
            let resp = ArrayShardPutResp {
                shard_id: req.vshard_id,
                applied_lsn: shard_req.wal_lsn,
                // The shard's cell count, as the Data Plane's `{"inserted": n}`
                // would report for a batch with no conflicts.
                affected: zerompk::from_msgpack::<Vec<Vec<u8>>>(&shard_req.cells_msgpack)
                    .unwrap()
                    .len() as u64,
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
            use crate::distributed_array::wire::ArrayShardDeleteReq;
            let shard_req: ArrayShardDeleteReq = zerompk::from_msgpack(&req.payload).unwrap();
            let resp = ArrayShardDeleteResp {
                shard_id: req.vshard_id,
                applied_lsn: shard_req.wal_lsn,
                // The shard's coordinate count, as the Data Plane's
                // `{"deleted": n}` would report when every cell exists.
                affected: zerompk::from_msgpack::<Vec<Vec<u8>>>(&shard_req.coords_msgpack)
                    .unwrap()
                    .len() as u64,
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
    async fn coord_put_reports_real_affected_per_shard_for_summing() {
        // Two shards with one and two cells. The coordinator hands back each
        // shard's own `affected` (what the Data Plane reported), so the caller
        // sums shard reports rather than counting the cells it sent.
        let p0 = 0x0000_0000_0000_0000u64;
        let p1 = 0x0040_0000_0000_0000u64;
        let cells = vec![(p0, vec![0x01u8]), (p1, vec![0x02u8]), (p1, vec![0x03u8])];

        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(PutEchoDispatch);
        let mut resps = coord_put(&write_params(), vec![], 10, 7, &cells, &dispatch, &cb())
            .await
            .expect("coord_put should succeed");
        resps.sort_by_key(|r| r.affected);

        let per_shard: Vec<u64> = resps.iter().map(|r| r.affected).collect();
        assert_eq!(per_shard, vec![1, 2], "each shard reports its own count");
        let total_affected: u64 = per_shard.iter().sum();
        assert_eq!(total_affected, 3);
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
