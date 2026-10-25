// SPDX-License-Identifier: BUSL-1.1

//! Distributed `ARRAY_ELEMENTWISE`: each shard pairs the cells it owns.
//!
//! Two arrays with one schema hash and one `prefix_bits` route every
//! coordinate to the same shard, so a pair never spans shards. The
//! coordinator sends each primary vShard the request with that shard's
//! bucket, and concatenates the shards' rows. The binary op runs on each
//! shard's Data Plane.

use crate::error::{ClusterError, Result};

use super::super::merge::merge_read_versions;
use super::super::scatter::{ArrayCoordError, FanOutPartitionedParams, fan_out_partitioned};
use super::super::wire::{ArrayShardElementwiseReq, ArrayShardElementwiseResp};
use super::read::{ArrayCoordinator, decode_resps, shard_bucket};

/// Result of a coordinated elementwise fan-out.
#[derive(Debug, Clone, Default)]
pub struct CoordElementwiseResult {
    /// Every shard's rows, in shard-arrival order.
    pub rows: Vec<Vec<u8>>,
    /// OR of every shard's below-horizon signal.
    pub truncated_before_horizon: bool,
    /// The write versions every shard leg observed, one per vShard.
    pub read_versions: Vec<nodedb_types::ShardVersion>,
}

impl ArrayCoordinator {
    /// Fan out an elementwise request to every target shard and concatenate
    /// the rows. Each shard pairs only the cells in its own bucket. Any shard
    /// error fails the whole operation.
    pub async fn coord_elementwise(
        &self,
        req: ArrayShardElementwiseReq,
    ) -> std::result::Result<CoordElementwiseResult, ArrayCoordError> {
        let prefix_bits = self.params.prefix_bits;
        let per_shard: Vec<(u32, Vec<u8>)> = self
            .params
            .shard_ids
            .iter()
            .map(|&shard_id| {
                let per_shard_req = ArrayShardElementwiseReq {
                    shard_bucket: shard_bucket(shard_id, prefix_bits)?,
                    ..req.clone()
                };
                let bytes =
                    zerompk::to_msgpack_vec(&per_shard_req).map_err(|e| ClusterError::Codec {
                        detail: format!("ArrayShardElementwiseReq serialise: {e}"),
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
            super::super::opcodes::ARRAY_SHARD_ELEMENTWISE_REQ,
            &per_shard,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await?;
        let resps = decode_resps::<ArrayShardElementwiseResp>(&raw)?;
        Ok(CoordElementwiseResult {
            truncated_before_horizon: resps.iter().any(|r| r.truncated_before_horizon),
            read_versions: merge_read_versions(resps.iter().map(|r| r.read_versions.as_slice())),
            rows: resps.into_iter().flat_map(|r| r.rows_msgpack).collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;
    use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
    use crate::distributed_array::coordinator::read::ArrayCoordParams;
    use crate::distributed_array::rpc::ShardRpcDispatch;
    use crate::wire::{VShardEnvelope, VShardMessageType};

    /// Answers each shard with one row naming the bucket it was sent.
    struct BucketEcho;

    #[async_trait]
    impl ShardRpcDispatch for BucketEcho {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            assert_eq!(req.msg_type, VShardMessageType::ArrayShardElementwiseReq);
            let shard_req: ArrayShardElementwiseReq =
                zerompk::from_msgpack(&req.payload).expect("request decodes");
            let bucket = shard_req.shard_bucket.expect("each shard gets its bucket");
            let resp = ArrayShardElementwiseResp {
                shard_id: req.vshard_id,
                rows_msgpack: vec![bucket.bucket.to_be_bytes().to_vec()],
                // One shard reports its cutoff below the horizon.
                truncated_before_horizon: bucket.bucket == 2,
                read_versions: Vec::new(),
            };
            Ok(VShardEnvelope::new(
                VShardMessageType::ArrayShardElementwiseResp,
                req.target_node,
                req.source_node,
                req.vshard_id,
                zerompk::to_msgpack_vec(&resp).expect("encode"),
            ))
        }
    }

    fn request() -> ArrayShardElementwiseReq {
        ArrayShardElementwiseReq {
            left_id_msgpack: Vec::new(),
            right_id_msgpack: Vec::new(),
            op_msgpack: vec![0x00],
            attr_idx: 0,
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            txn_id: None,
        }
    }

    /// Every primary shard gets its own bucket, and every shard's rows reach
    /// the merged result.
    #[tokio::test]
    async fn every_shard_pairs_its_own_bucket() {
        let bits = 2u8;
        let shard_ids =
            crate::distributed_array::routing::array_vshards_for_slice(&[], bits, 1024).unwrap();
        assert_eq!(shard_ids.len(), 4);
        let coord = ArrayCoordinator::new(
            ArrayCoordParams {
                source_node: 1,
                shard_ids,
                timeout_ms: 1000,
                prefix_bits: bits,
                slice_tile_keys: Vec::new(),
            },
            Arc::new(BucketEcho),
            Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default())),
        );
        let result = coord
            .coord_elementwise(request())
            .await
            .expect("fan-out succeeds");
        let mut buckets: Vec<u32> = result
            .rows
            .iter()
            .map(|row| u32::from_be_bytes(row.as_slice().try_into().expect("4 bytes")))
            .collect();
        buckets.sort_unstable();
        assert_eq!(buckets, vec![0, 1, 2, 3]);
        assert!(
            result.truncated_before_horizon,
            "one shard's below-horizon signal reaches the result"
        );
    }
}
