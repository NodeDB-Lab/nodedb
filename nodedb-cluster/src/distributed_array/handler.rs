//! Shard-side RPC handler for incoming array operations.
//!
//! `handle_array_shard_rpc` is called by the vshard dispatch table
//! (see `crate::vshard_handler`) when an incoming `VShardEnvelope`
//! carries an array opcode. It decodes the msgpack payload, delegates
//! to the local array engine through `ArrayLocalExecutor`, and returns
//! a serialised response envelope payload.
//!
//! The `ArrayLocalExecutor` trait is defined in `local_executor.rs` and
//! implemented in the main `nodedb` binary, which has access to the SPSC
//! bridge and the Data Plane array engine. This keeps `nodedb-cluster`
//! free of a compile-time dependency on the engine crates.

use std::sync::Arc;

use crate::error::{ClusterError, Result};

use super::local_executor::ArrayLocalExecutor;
use super::opcodes::{
    ARRAY_SHARD_AGG_REQ, ARRAY_SHARD_DELETE_REQ, ARRAY_SHARD_PUT_REQ, ARRAY_SHARD_SLICE_REQ,
    ARRAY_SHARD_SURROGATE_BITMAP_REQ,
};
use super::routing::{array_vshard_for_tile, array_vshards_for_slice};
use super::wire::{
    ArrayShardAggReq, ArrayShardAggResp, ArrayShardDeleteReq, ArrayShardDeleteResp,
    ArrayShardPutReq, ArrayShardPutResp, ArrayShardSliceReq, ArrayShardSliceResp,
    ArrayShardSurrogateBitmapReq, ArrayShardSurrogateBitmapResp,
};

/// Dispatch an incoming array shard RPC to the appropriate local handler.
///
/// `opcode` is the `VShardMessageType` discriminant (u16).
/// `local_vshard_id` is this shard's own vShard ID (for routing validation).
/// `payload` is the zerompk-encoded request body.
/// `executor` reaches into the local Data Plane array engine.
///
/// Returns the zerompk-encoded response body on success, ready to be placed
/// into a response `VShardEnvelope`.
pub async fn handle_array_shard_rpc(
    opcode: u16,
    local_vshard_id: u16,
    payload: &[u8],
    executor: &Arc<dyn ArrayLocalExecutor>,
) -> Result<Vec<u8>> {
    match opcode {
        ARRAY_SHARD_SLICE_REQ => handle_slice(local_vshard_id, payload, executor).await,
        ARRAY_SHARD_AGG_REQ => handle_agg(local_vshard_id, payload, executor).await,
        ARRAY_SHARD_PUT_REQ => handle_put(local_vshard_id, payload, executor).await,
        ARRAY_SHARD_DELETE_REQ => handle_delete(local_vshard_id, payload, executor).await,
        ARRAY_SHARD_SURROGATE_BITMAP_REQ => {
            handle_surrogate_bitmap(local_vshard_id, payload, executor).await
        }
        other => Err(ClusterError::Codec {
            detail: format!("handle_array_shard_rpc: unknown opcode {other}"),
        }),
    }
}

async fn handle_slice(
    local_vshard_id: u16,
    payload: &[u8],
    executor: &Arc<dyn ArrayLocalExecutor>,
) -> Result<Vec<u8>> {
    let req: ArrayShardSliceReq =
        zerompk::from_msgpack(payload).map_err(|e| ClusterError::Codec {
            detail: format!("ArrayShardSliceReq decode: {e}"),
        })?;

    validate_slice_routing(&req, local_vshard_id)?;

    let rows = executor.exec_slice(&req).await?;

    let truncated = req.limit > 0 && rows.len() >= req.limit as usize;
    let resp = ArrayShardSliceResp {
        shard_id: local_vshard_id,
        rows_msgpack: rows,
        truncated,
        truncated_before_horizon: false,
    };
    serialise(resp)
}

async fn handle_agg(
    local_vshard_id: u16,
    payload: &[u8],
    executor: &Arc<dyn ArrayLocalExecutor>,
) -> Result<Vec<u8>> {
    let req: ArrayShardAggReq =
        zerompk::from_msgpack(payload).map_err(|e| ClusterError::Codec {
            detail: format!("ArrayShardAggReq decode: {e}"),
        })?;

    let partials = executor.exec_agg(&req).await?;

    let resp = ArrayShardAggResp {
        shard_id: local_vshard_id,
        partials,
        truncated_before_horizon: false,
    };
    serialise(resp)
}

async fn handle_put(
    local_vshard_id: u16,
    payload: &[u8],
    executor: &Arc<dyn ArrayLocalExecutor>,
) -> Result<Vec<u8>> {
    let req: ArrayShardPutReq =
        zerompk::from_msgpack(payload).map_err(|e| ClusterError::Codec {
            detail: format!("ArrayShardPutReq decode: {e}"),
        })?;

    // Validate routing before dispatching: the PUT must have been sent to
    // the correct shard. A mismatch means the coordinator used a stale
    // routing table and the write must be rejected so the caller can retry
    // with a refreshed routing table rather than silently writing to the
    // wrong shard.
    validate_put_routing(&req, local_vshard_id)?;

    let applied_lsn = executor.exec_put(&req).await?;
    let resp = ArrayShardPutResp {
        shard_id: local_vshard_id,
        applied_lsn,
    };
    serialise(resp)
}

async fn handle_delete(
    local_vshard_id: u16,
    payload: &[u8],
    executor: &Arc<dyn ArrayLocalExecutor>,
) -> Result<Vec<u8>> {
    let req: ArrayShardDeleteReq =
        zerompk::from_msgpack(payload).map_err(|e| ClusterError::Codec {
            detail: format!("ArrayShardDeleteReq decode: {e}"),
        })?;

    validate_delete_routing(&req, local_vshard_id)?;

    let applied_lsn = executor
        .exec_delete(&req.array_id_msgpack, &req.coords_msgpack, req.wal_lsn)
        .await?;
    let resp = ArrayShardDeleteResp {
        shard_id: local_vshard_id,
        applied_lsn,
    };
    serialise(resp)
}

async fn handle_surrogate_bitmap(
    local_vshard_id: u16,
    payload: &[u8],
    executor: &Arc<dyn ArrayLocalExecutor>,
) -> Result<Vec<u8>> {
    let req: ArrayShardSurrogateBitmapReq =
        zerompk::from_msgpack(payload).map_err(|e| ClusterError::Codec {
            detail: format!("ArrayShardSurrogateBitmapReq decode: {e}"),
        })?;

    let bitmap_msgpack = executor
        .exec_surrogate_bitmap_scan(&req.array_id_msgpack, &req.slice_msgpack)
        .await?;

    let resp = ArrayShardSurrogateBitmapResp {
        shard_id: local_vshard_id,
        bitmap_msgpack,
    };
    serialise(resp)
}

/// Validate that a PUT request is routed to the correct shard.
///
/// Returns `Ok(())` when routing is correct or when `prefix_bits == 0`
/// (which means the request predates Hilbert routing and skips validation).
/// Returns `Err(ClusterError::WrongOwner)` when the coordinator used a stale
/// routing table; the coordinator should refresh and retry.
fn validate_put_routing(req: &ArrayShardPutReq, local_vshard_id: u16) -> Result<()> {
    if req.prefix_bits == 0 {
        return Ok(());
    }
    let expected = array_vshard_for_tile(req.representative_hilbert_prefix, req.prefix_bits)?;
    if expected != local_vshard_id {
        return Err(ClusterError::WrongOwner {
            vshard_id: local_vshard_id,
            expected_owner_node: None,
        });
    }
    Ok(())
}

/// Validate that a SLICE request is routed to the correct shard.
///
/// Returns `Ok(())` when `prefix_bits == 0` (no validation) or when
/// `local_vshard_id` falls within the Hilbert ranges carried by the request.
/// Returns `Err(ClusterError::WrongOwner)` when this shard no longer covers
/// any of the requested ranges, indicating a stale routing table.
fn validate_slice_routing(req: &ArrayShardSliceReq, local_vshard_id: u16) -> Result<()> {
    if req.prefix_bits == 0 || req.slice_hilbert_ranges.is_empty() {
        return Ok(());
    }
    // Compute which vShards overlap the slice ranges. If local_vshard_id is not
    // among them, this node no longer owns the required Hilbert range.
    let covered = array_vshards_for_slice(&req.slice_hilbert_ranges, req.prefix_bits, u16::MAX)?;
    if covered.binary_search(&local_vshard_id).is_err() {
        return Err(ClusterError::WrongOwner {
            vshard_id: local_vshard_id,
            expected_owner_node: None,
        });
    }
    Ok(())
}

/// Validate that a DELETE request is routed to the correct shard.
///
/// Returns `Ok(())` when routing is correct or when `prefix_bits == 0`.
/// Returns `Err(ClusterError::WrongOwner)` on misroute.
fn validate_delete_routing(req: &ArrayShardDeleteReq, local_vshard_id: u16) -> Result<()> {
    if req.prefix_bits == 0 {
        return Ok(());
    }
    let expected = array_vshard_for_tile(req.representative_hilbert_prefix, req.prefix_bits)?;
    if expected != local_vshard_id {
        return Err(ClusterError::WrongOwner {
            vshard_id: local_vshard_id,
            expected_owner_node: None,
        });
    }
    Ok(())
}

fn serialise<T: zerompk::ToMessagePack>(val: T) -> Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&val).map_err(|e| ClusterError::Codec {
        detail: format!("array response serialise: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use crate::distributed_array::merge::ArrayAggPartial;
    use crate::distributed_array::wire::{ArrayShardAggReq, ArrayShardPutReq};
    use crate::error::Result;

    use super::super::local_executor::ArrayLocalExecutor;
    use super::super::opcodes::{
        ARRAY_SHARD_AGG_REQ, ARRAY_SHARD_DELETE_REQ, ARRAY_SHARD_PUT_REQ, ARRAY_SHARD_SLICE_REQ,
        ARRAY_SHARD_SURROGATE_BITMAP_REQ,
    };
    use super::super::wire::{
        ArrayShardAggResp, ArrayShardDeleteResp, ArrayShardPutResp, ArrayShardSliceResp,
        ArrayShardSurrogateBitmapResp,
    };
    use super::handle_array_shard_rpc;

    /// Mock executor that returns a fixed set of row bytes for slice,
    /// a fixed bitmap for surrogate scan, fixed partials for agg, and
    /// echoes back the `wal_lsn` for put/delete.
    struct StubExecutor {
        rows: Vec<Vec<u8>>,
        bitmap: Vec<u8>,
        partials: Vec<ArrayAggPartial>,
    }

    #[async_trait]
    impl ArrayLocalExecutor for StubExecutor {
        async fn exec_slice(
            &self,
            _req: &super::super::wire::ArrayShardSliceReq,
        ) -> Result<Vec<Vec<u8>>> {
            Ok(self.rows.clone())
        }

        async fn exec_surrogate_bitmap_scan(
            &self,
            _array_id_msgpack: &[u8],
            _slice_msgpack: &[u8],
        ) -> Result<Vec<u8>> {
            Ok(self.bitmap.clone())
        }

        async fn exec_agg(&self, _req: &ArrayShardAggReq) -> Result<Vec<ArrayAggPartial>> {
            Ok(self.partials.clone())
        }

        async fn exec_put(&self, req: &ArrayShardPutReq) -> Result<u64> {
            Ok(req.wal_lsn)
        }

        async fn exec_delete(
            &self,
            _array_id_msgpack: &[u8],
            _coords_msgpack: &[u8],
            wal_lsn: u64,
        ) -> Result<u64> {
            Ok(wal_lsn)
        }
    }

    fn make_slice_req_bytes() -> Vec<u8> {
        let req = super::super::wire::ArrayShardSliceReq {
            array_id_msgpack: vec![],
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 10,
            cell_filter_msgpack: vec![],
            // prefix_bits=0 skips routing validation in the handler.
            prefix_bits: 0,
            slice_hilbert_ranges: vec![],
            shard_hilbert_range: None,
            system_as_of: None,
            valid_at_ms: None,
        };
        zerompk::to_msgpack_vec(&req).unwrap()
    }

    fn make_agg_req_bytes() -> Vec<u8> {
        // Encode a minimal Sum reducer (c_enum byte).
        // We use rmp_serde directly since we don't have ArrayReducer here,
        // but any single-byte msgpack value works as the reducer payload for
        // StubExecutor (it never decodes it).
        let reducer_bytes = vec![0x00u8]; // Sum = 0 as c_enum
        let req = super::super::wire::ArrayShardAggReq {
            array_id_msgpack: vec![],
            attr_idx: 0,
            reducer_msgpack: reducer_bytes,
            group_by_dim: -1,
            cell_filter_msgpack: vec![],
            shard_hilbert_range: None,
            system_as_of: None,
            valid_at_ms: None,
        };
        zerompk::to_msgpack_vec(&req).unwrap()
    }

    fn make_bitmap_req_bytes() -> Vec<u8> {
        let req = super::super::wire::ArrayShardSurrogateBitmapReq {
            array_id_msgpack: vec![],
            slice_msgpack: vec![],
        };
        zerompk::to_msgpack_vec(&req).unwrap()
    }

    #[tokio::test]
    async fn handle_slice_returns_executor_rows() {
        let row = b"row-data".to_vec();
        let executor: Arc<dyn ArrayLocalExecutor> = Arc::new(StubExecutor {
            rows: vec![row.clone(), row.clone()],
            bitmap: vec![],
            partials: vec![],
        });
        let payload = make_slice_req_bytes();
        let resp_bytes = handle_array_shard_rpc(ARRAY_SHARD_SLICE_REQ, 0, &payload, &executor)
            .await
            .expect("slice handler should succeed");

        let resp: ArrayShardSliceResp =
            zerompk::from_msgpack(&resp_bytes).expect("response should deserialise");
        assert_eq!(resp.rows_msgpack.len(), 2);
        assert_eq!(resp.rows_msgpack[0], row);
    }

    #[tokio::test]
    async fn handle_surrogate_bitmap_returns_executor_bitmap() {
        let bitmap = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let executor: Arc<dyn ArrayLocalExecutor> = Arc::new(StubExecutor {
            rows: vec![],
            bitmap: bitmap.clone(),
            partials: vec![],
        });
        let payload = make_bitmap_req_bytes();
        let resp_bytes =
            handle_array_shard_rpc(ARRAY_SHARD_SURROGATE_BITMAP_REQ, 1, &payload, &executor)
                .await
                .expect("bitmap handler should succeed");

        let resp: ArrayShardSurrogateBitmapResp =
            zerompk::from_msgpack(&resp_bytes).expect("response should deserialise");
        assert_eq!(resp.shard_id, 1);
        assert_eq!(resp.bitmap_msgpack, bitmap);
    }

    #[tokio::test]
    async fn handle_agg_returns_executor_partials() {
        let partial = ArrayAggPartial::from_single(0, 42.0);
        let executor: Arc<dyn ArrayLocalExecutor> = Arc::new(StubExecutor {
            rows: vec![],
            bitmap: vec![],
            partials: vec![partial.clone()],
        });
        let payload = make_agg_req_bytes();
        let resp_bytes = handle_array_shard_rpc(ARRAY_SHARD_AGG_REQ, 3, &payload, &executor)
            .await
            .expect("agg handler should succeed");

        let resp: ArrayShardAggResp =
            zerompk::from_msgpack(&resp_bytes).expect("response should deserialise");
        assert_eq!(resp.shard_id, 3);
        assert_eq!(resp.partials.len(), 1);
        assert_eq!(resp.partials[0].count, 1);
        assert!((resp.partials[0].sum - 42.0).abs() < f64::EPSILON);
    }

    fn make_put_req_bytes(vshard_id: u16, prefix_bits: u8) -> Vec<u8> {
        // Build a minimal ArrayShardPutReq with routing metadata set so
        // validate_put_routing can confirm the request is destined for
        // the right shard. Use prefix_bits=0 to skip routing validation
        // in tests that only care about the executor round-trip.
        let req = super::super::wire::ArrayShardPutReq {
            array_id_msgpack: vec![],
            cells_msgpack: vec![],
            wal_lsn: 77,
            representative_hilbert_prefix: if prefix_bits == 0 {
                0
            } else {
                // Route to bucket 0 (top bits all zero) → vshard 0 with any prefix_bits.
                0u64
            },
            prefix_bits,
        };
        // Adjust routing so the request would target vshard_id: set bits so
        // bucket = vshard_id when stride = 1 (prefix_bits=10, VSHARD_COUNT=1024).
        let _ = vshard_id; // routing byte chosen above; test uses prefix_bits=0.
        zerompk::to_msgpack_vec(&req).unwrap()
    }

    fn make_delete_req_bytes() -> Vec<u8> {
        let req = super::super::wire::ArrayShardDeleteReq {
            array_id_msgpack: vec![],
            coords_msgpack: vec![],
            wal_lsn: 88,
            // prefix_bits=0 skips routing validation in the handler.
            representative_hilbert_prefix: 0,
            prefix_bits: 0,
        };
        zerompk::to_msgpack_vec(&req).unwrap()
    }

    #[tokio::test]
    async fn handle_put_delegates_to_executor_and_echoes_lsn() {
        let executor: Arc<dyn ArrayLocalExecutor> = Arc::new(StubExecutor {
            rows: vec![],
            bitmap: vec![],
            partials: vec![],
        });
        // prefix_bits=0 disables routing validation.
        let payload = make_put_req_bytes(0, 0);
        let resp_bytes = handle_array_shard_rpc(ARRAY_SHARD_PUT_REQ, 0, &payload, &executor)
            .await
            .expect("put handler should succeed");

        let resp: ArrayShardPutResp =
            zerompk::from_msgpack(&resp_bytes).expect("response should deserialise");
        assert_eq!(resp.shard_id, 0);
        assert_eq!(resp.applied_lsn, 77);
    }

    #[tokio::test]
    async fn handle_delete_delegates_to_executor_and_echoes_lsn() {
        let executor: Arc<dyn ArrayLocalExecutor> = Arc::new(StubExecutor {
            rows: vec![],
            bitmap: vec![],
            partials: vec![],
        });
        let payload = make_delete_req_bytes();
        let resp_bytes = handle_array_shard_rpc(ARRAY_SHARD_DELETE_REQ, 2, &payload, &executor)
            .await
            .expect("delete handler should succeed");

        let resp: ArrayShardDeleteResp =
            zerompk::from_msgpack(&resp_bytes).expect("response should deserialise");
        assert_eq!(resp.shard_id, 2);
        assert_eq!(resp.applied_lsn, 88);
    }

    #[tokio::test]
    async fn handle_put_rejects_misrouted_request() {
        let executor: Arc<dyn ArrayLocalExecutor> = Arc::new(StubExecutor {
            rows: vec![],
            bitmap: vec![],
            partials: vec![],
        });
        // prefix_bits=10, stride=1 → bucket = top 10 bits of hilbert_prefix.
        // hilbert_prefix = 0 → bucket 0 → expected vshard 0.
        // We tell the handler we're vshard 7 → mismatch → error.
        let req = super::super::wire::ArrayShardPutReq {
            array_id_msgpack: vec![],
            cells_msgpack: vec![],
            wal_lsn: 1,
            representative_hilbert_prefix: 0,
            prefix_bits: 10,
        };
        let payload = zerompk::to_msgpack_vec(&req).unwrap();
        let err = handle_array_shard_rpc(ARRAY_SHARD_PUT_REQ, 7, &payload, &executor)
            .await
            .expect_err("misrouted put should fail");
        assert!(
            matches!(
                err,
                crate::error::ClusterError::WrongOwner { vshard_id: 7, .. }
            ),
            "expected WrongOwner for vshard 7, got {err:?}"
        );
    }

    #[test]
    fn validate_slice_routing_rejects_disjoint_range() {
        // prefix_bits=10, stride=1 → vshard == bucket == top 10 bits.
        // Hilbert range [0x0040_0000_0000_0000, 0x0040_0000_0000_0000]
        // covers bucket 1 → vshard 1.
        // Local shard is vshard 5 → disjoint → WrongOwner.
        let req = super::super::wire::ArrayShardSliceReq {
            array_id_msgpack: vec![],
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 10,
            cell_filter_msgpack: vec![],
            prefix_bits: 10,
            slice_hilbert_ranges: vec![(0x0040_0000_0000_0000, 0x0040_0000_0000_0000)],
            shard_hilbert_range: None,
            system_as_of: None,
            valid_at_ms: None,
        };
        let err = super::validate_slice_routing(&req, 5)
            .expect_err("disjoint Hilbert range should reject");
        assert!(
            matches!(
                err,
                crate::error::ClusterError::WrongOwner { vshard_id: 5, .. }
            ),
            "expected WrongOwner for vshard 5, got {err:?}"
        );
    }

    #[test]
    fn validate_slice_routing_accepts_overlapping_range() {
        // Same range as above → vshard 1. Local shard IS vshard 1 → accept.
        let req = super::super::wire::ArrayShardSliceReq {
            array_id_msgpack: vec![],
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 10,
            cell_filter_msgpack: vec![],
            prefix_bits: 10,
            slice_hilbert_ranges: vec![(0x0040_0000_0000_0000, 0x0040_0000_0000_0000)],
            shard_hilbert_range: None,
            system_as_of: None,
            valid_at_ms: None,
        };
        super::validate_slice_routing(&req, 1).expect("overlapping range should accept");
    }

    #[tokio::test]
    async fn handle_unknown_opcode_returns_codec_error() {
        let executor: Arc<dyn ArrayLocalExecutor> = Arc::new(StubExecutor {
            rows: vec![],
            bitmap: vec![],
            partials: vec![],
        });
        let err = handle_array_shard_rpc(0xFF, 0, &[], &executor)
            .await
            .expect_err("unknown opcode should fail");
        assert!(
            matches!(err, crate::error::ClusterError::Codec { .. }),
            "{err:?}"
        );
    }
}
