// SPDX-License-Identifier: BUSL-1.1

//! `ShufflePush` streaming RPC — cross-node streaming shuffle (E1).
//!
//! A producer opens one bidi stream per target partition and writes a
//! `ShufflePushRequest` frame, then a sequence of `ShufflePushChunk` frames
//! (each a standalone msgpack array of rows, mirroring `ExecuteStreamChunk`),
//! terminated by exactly one `ShufflePushEnd` frame. Unlike `ExecuteStream`
//! the direction is producer → receiver: the chunks travel on the *send*
//! half and the receiver does not write chunks back.
//!
//! Discriminants 25/26/27 are permanently assigned to these variants.

use super::discriminants::*;
use super::execute::{DescriptorVersionEntry, TypedClusterError};
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

// ── Wire types ──────────────────────────────────────────────────────────────

/// Opening frame of a shuffle push stream.
///
/// Carries the routing key `(shuffle_id, part, side)` plus the partition fan-out
/// (`num_parts`) and the number of producers (`producer_count`) the receiver
/// must see an `End` from before the per-part build barrier is complete.
///
/// `side` is `0` for the build side and `1` for the probe side of a hash join.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShufflePushRequest {
    pub shuffle_id: u64,
    pub part: u32,
    /// `0` = build side, `1` = probe side.
    pub side: u8,
    pub num_parts: u32,
    pub producer_count: u32,
}

/// One streamed chunk of a shuffle push stream.
///
/// `payload` is a standalone msgpack array of row elements — the same
/// convention as [`ExecuteStreamChunk`](super::execute::ExecuteStreamChunk)
/// and `RowBatch.payload`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShufflePushChunk {
    pub payload: Vec<u8>,
}

/// Terminal frame of a shuffle push stream.
///
/// `error: None` is a clean EOF (all chunks delivered for this producer).
/// `error: Some(e)` is a terminal failure — any chunks already delivered are
/// valid, but this producer's contribution is incomplete and the receiver must
/// surface the error. Mirrors
/// [`ExecuteStreamEnd`](super::execute::ExecuteStreamEnd).
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShufflePushEnd {
    pub error: Option<TypedClusterError>,
}

/// One `part -> owning node id` mapping entry of a [`ShuffleProduceRequest`]'s
/// `part_node_map`. The coordinator computes this routing table; the producer
/// node uses it to direct each hash-partitioned row's `(part)` to its owner
/// (looping back for parts it owns itself).
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PartNodeEntry {
    pub part: u32,
    pub node_id: u64,
}

/// Cross-node shuffle PRODUCER trigger (E4a).
///
/// A coordinator sends this to a producer node to make it execute a LOCAL scan
/// fragment (`plan_bytes`), hash-partition each output row on `keys`, and fan the
/// rows out to the per-part owners (`part_node_map`) as `ShufflePush` streams —
/// one `ShufflePushEnd` to EVERY part for this `side` so each receiver's per-part
/// barrier reaches `producer_count`. The producer replies with exactly one
/// [`ShuffleProduceResponse`]; it never streams the scanned rows back.
///
/// `side` is `0` for the build side and `1` for the probe side of a hash join.
///
/// The `plan_bytes` / `tenant_id` / `database_id` / `deadline_remaining_ms` /
/// `trace_id` / `descriptor_versions` fields mirror
/// [`ExecuteRequest`](super::execute::ExecuteRequest) so the producer can reuse
/// the existing local streaming-execution prologue verbatim.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShuffleProduceRequest {
    pub shuffle_id: u64,
    /// `0` = build side, `1` = probe side — the local side this producer scans.
    pub side: u8,
    pub num_parts: u32,
    /// How many producers will `End` each `(part, side)`; forwarded into every
    /// emitted `ShufflePushRequest` so receivers size their build barrier.
    pub producer_count: u32,
    /// Local-side join field names; each output row is hashed on these.
    pub keys: Vec<String>,
    /// `part -> owning node id` (coordinator-computed, sorted by `part`).
    pub part_node_map: Vec<PartNodeEntry>,
    /// Encoded `PhysicalPlan` of the local scan fragment to execute.
    pub plan_bytes: Vec<u8>,
    pub tenant_id: u64,
    pub database_id: u64,
    pub deadline_remaining_ms: u64,
    pub trace_id: [u8; 16],
    pub descriptor_versions: Vec<DescriptorVersionEntry>,
    /// Raft groups the producer confirms before its scan, as a linearizable
    /// read leg (see `ExecuteRequest::read_groups`). Empty otherwise.
    pub read_groups: Vec<u64>,
}

/// Terminal reply to a [`ShuffleProduceRequest`].
///
/// `error: None` means the producer fanned out every row and `End`ed every part
/// cleanly. `error: Some(e)` means the local scan failed; the producer has
/// already `End`ed every part with the same error so each receiver fails fast.
///
/// Cross-version safety: new fields are appended (mirroring
/// [`ExecuteResponse`](super::execute::ExecuteResponse)'s LSN fields).
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShuffleProduceResponse {
    pub error: Option<TypedClusterError>,
    /// The read versions the producer's local scan observed, one per vShard
    /// read. Empty for a failed produce. The coordinator folds them across
    /// producers for cross-shard OCC read validation of an in-transaction
    /// distributed aggregate. Mirrors
    /// [`ExecuteResponse::read_versions`](super::execute::ExecuteResponse::read_versions).
    pub read_versions: Vec<nodedb_types::ShardVersion>,
}

/// One `(left_key, right_key)` equi-join pair of a [`ShuffleConsumeRequest`]'s
/// `on` list. The part-owner reconstructs the borrowed join spec from these.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct JoinKeyPair {
    /// Probe-side (left) join field name.
    pub left: String,
    /// Build-side (right) join field name.
    pub right: String,
}

/// Cross-node shuffle CONSUMER trigger (E4b).
///
/// A coordinator sends this to a part-owner node to make it complete one part of
/// a distributed shuffle join: wait for BOTH staged sides of `(shuffle_id, part)`
/// to finalize, run the node-local grace-hash join over them, and reply with the
/// join-result rows. The part-owner replies with exactly one
/// [`ShuffleConsumeResponse`].
///
/// The `on` / `join_type` / `limit` / `probe_qualifier` / `index_qualifier`
/// fields are the owned join spec the consumer reconstructs into a borrowed
/// `JoinParams` for the node-local grace join. `tenant_id` / `database_id` /
/// `deadline_remaining_ms` / `trace_id` mirror
/// [`ExecuteRequest`](super::execute::ExecuteRequest) so the local dispatch
/// prologue is reused.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShuffleConsumeRequest {
    pub shuffle_id: u64,
    /// The part this node owns and must complete.
    pub part: u32,
    /// Equi-join key pairs `(left_key, right_key)`.
    pub on: Vec<JoinKeyPair>,
    /// Join type (`inner` / `left` / `right` / `full`).
    pub join_type: String,
    /// Output row cap. `u64::MAX` = no explicit LIMIT (budget-bounded).
    pub limit: u64,
    /// Column qualifier (prefix) for probe-side (left) columns.
    pub probe_qualifier: String,
    /// Column qualifier (prefix) for build-side (right/index) columns.
    pub index_qualifier: String,
    pub tenant_id: u64,
    pub database_id: u64,
    pub deadline_remaining_ms: u64,
    pub trace_id: [u8; 16],
}

/// Terminal reply to a [`ShuffleConsumeRequest`].
///
/// `error: None` carries the join-result rows in `rows` (a msgpack array of
/// joined rows — the same `encode_binary_rows` shape every join path emits).
/// `error: Some(e)` means the consume failed (missing inbox, finalize timeout,
/// producer terminal error, or join error); `rows` is empty in that case.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShuffleConsumeResponse {
    /// Msgpack array of join-result rows. Empty on error.
    pub rows: Vec<u8>,
    pub error: Option<TypedClusterError>,
}

/// One post-aggregation sort key of a [`ShuffleAggregateConsumeRequest`]'s
/// `sort_keys` list: a column name plus its sort direction. A named struct
/// (rather than a `(String, bool)` tuple) so rkyv can derive its codec, mirroring
/// how [`JoinKeyPair`] wraps an `on` tuple.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SortKey {
    /// Sort column name.
    pub column: String,
    /// `true` = ascending, `false` = descending.
    pub ascending: bool,
}

/// Cross-node distributed GROUP BY shuffle CONSUMER trigger (E5b).
///
/// Single-sided sibling of [`ShuffleConsumeRequest`]: a coordinator sends this to
/// a part-owner node to make it complete one part of a distributed GROUP BY
/// shuffle — wait for the part's ONE staged side (side `0`) to finalize, merge
/// the staged partial `GroupState`s, finalize / HAVING-filter / sort / LIMIT, and
/// reply with the result rows. The part-owner replies with exactly one
/// [`ShuffleAggregateConsumeResponse`].
///
/// Unlike the join consumer this waits for only the single producer side (`0`);
/// there is no probe side. The `group_by` / `aggregates_bytes` / `having` /
/// `limit` / `sort_keys` fields are the owned aggregate spec the consumer
/// reconstructs into a node-local `ShuffleAggregateConsume` plan. `tenant_id` /
/// `database_id` / `deadline_remaining_ms` / `trace_id` mirror
/// [`ExecuteRequest`](super::execute::ExecuteRequest) so the local dispatch
/// prologue is reused.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShuffleAggregateConsumeRequest {
    pub shuffle_id: u64,
    /// The part this node owns and must complete.
    pub part: u32,
    /// GROUP BY columns (the key the partial states were grouped on).
    pub group_by: Vec<String>,
    /// Opaque zerompk-encoded `Vec<AggregateSpec>` (nodedb-cluster does not
    /// inspect it; the host-crate hook decodes it — mirrors how `plan_bytes`
    /// stays opaque to the transport).
    pub aggregates_bytes: Vec<u8>,
    /// Msgpack HAVING predicate blob (empty = no HAVING).
    pub having: Vec<u8>,
    /// Output row cap after sort. `u64::MAX` = no explicit LIMIT.
    pub limit: u64,
    /// Post-aggregation sort keys.
    pub sort_keys: Vec<SortKey>,
    pub tenant_id: u64,
    pub database_id: u64,
    pub deadline_remaining_ms: u64,
    pub trace_id: [u8; 16],
}

/// Terminal reply to a [`ShuffleAggregateConsumeRequest`].
///
/// `error: None` carries the finalized GROUP BY result rows in `rows` (a msgpack
/// array of rows — the same shape every aggregate path emits). `error: Some(e)`
/// means the consume failed (missing inbox, finalize timeout, producer terminal
/// error, or merge/finalize error); `rows` is empty in that case.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShuffleAggregateConsumeResponse {
    /// Msgpack array of finalized aggregate rows. Empty on error.
    pub rows: Vec<u8>,
    pub error: Option<TypedClusterError>,
}

// ── Codec ────────────────────────────────────────────────────────────────────

macro_rules! to_bytes {
    ($msg:expr) => {
        rkyv::to_bytes::<rkyv::rancor::Error>($msg)
            .map(|b| b.to_vec())
            .map_err(|e| ClusterError::Codec {
                detail: format!("rkyv serialize: {e}"),
            })
    };
}

macro_rules! from_bytes {
    ($payload:expr, $T:ty, $name:expr) => {{
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity($payload.len());
        aligned.extend_from_slice($payload);
        rkyv::from_bytes::<$T, rkyv::rancor::Error>(&aligned).map_err(|e| ClusterError::Codec {
            detail: format!("rkyv deserialize {}: {e}", $name),
        })
    }};
}

pub(super) fn encode_shuffle_push_req(msg: &ShufflePushRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_SHUFFLE_PUSH_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_shuffle_push_chunk(msg: &ShufflePushChunk, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_SHUFFLE_PUSH_CHUNK, &to_bytes!(msg)?, out)
}
pub(super) fn encode_shuffle_push_end(msg: &ShufflePushEnd, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_SHUFFLE_PUSH_END, &to_bytes!(msg)?, out)
}

pub(super) fn decode_shuffle_push_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShufflePushRequest(from_bytes!(
        payload,
        ShufflePushRequest,
        "ShufflePushRequest"
    )?))
}
pub(super) fn decode_shuffle_push_chunk(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShufflePushChunk(from_bytes!(
        payload,
        ShufflePushChunk,
        "ShufflePushChunk"
    )?))
}
pub(super) fn decode_shuffle_push_end(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShufflePushEnd(from_bytes!(
        payload,
        ShufflePushEnd,
        "ShufflePushEnd"
    )?))
}

pub(super) fn encode_shuffle_produce_req(
    msg: &ShuffleProduceRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_SHUFFLE_PRODUCE_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_shuffle_produce_resp(
    msg: &ShuffleProduceResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_SHUFFLE_PRODUCE_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_shuffle_produce_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShuffleProduceRequest(from_bytes!(
        payload,
        ShuffleProduceRequest,
        "ShuffleProduceRequest"
    )?))
}
pub(super) fn decode_shuffle_produce_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShuffleProduceResponse(from_bytes!(
        payload,
        ShuffleProduceResponse,
        "ShuffleProduceResponse"
    )?))
}

pub(super) fn encode_shuffle_consume_req(
    msg: &ShuffleConsumeRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_SHUFFLE_CONSUME_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_shuffle_consume_resp(
    msg: &ShuffleConsumeResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_SHUFFLE_CONSUME_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_shuffle_consume_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShuffleConsumeRequest(from_bytes!(
        payload,
        ShuffleConsumeRequest,
        "ShuffleConsumeRequest"
    )?))
}
pub(super) fn decode_shuffle_consume_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShuffleConsumeResponse(from_bytes!(
        payload,
        ShuffleConsumeResponse,
        "ShuffleConsumeResponse"
    )?))
}

pub(super) fn encode_shuffle_agg_consume_req(
    msg: &ShuffleAggregateConsumeRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_SHUFFLE_AGG_CONSUME_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_shuffle_agg_consume_resp(
    msg: &ShuffleAggregateConsumeResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_SHUFFLE_AGG_CONSUME_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_shuffle_agg_consume_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShuffleAggregateConsumeRequest(from_bytes!(
        payload,
        ShuffleAggregateConsumeRequest,
        "ShuffleAggregateConsumeRequest"
    )?))
}
pub(super) fn decode_shuffle_agg_consume_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ShuffleAggregateConsumeResponse(from_bytes!(
        payload,
        ShuffleAggregateConsumeResponse,
        "ShuffleAggregateConsumeResponse"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_req(req: ShufflePushRequest) -> ShufflePushRequest {
        let rpc = RaftRpc::ShufflePushRequest(req);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::ShufflePushRequest(r) => r,
            other => panic!("expected ShufflePushRequest, got {other:?}"),
        }
    }

    fn roundtrip_chunk(chunk: ShufflePushChunk) -> ShufflePushChunk {
        let rpc = RaftRpc::ShufflePushChunk(chunk);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::ShufflePushChunk(c) => c,
            other => panic!("expected ShufflePushChunk, got {other:?}"),
        }
    }

    fn roundtrip_end(end: ShufflePushEnd) -> ShufflePushEnd {
        let rpc = RaftRpc::ShufflePushEnd(end);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::ShufflePushEnd(e) => e,
            other => panic!("expected ShufflePushEnd, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_shuffle_push_request() {
        let req = ShufflePushRequest {
            shuffle_id: 0xDEAD_BEEF_1234_5678,
            part: 7,
            side: 1,
            num_parts: 16,
            producer_count: 3,
        };
        let decoded = roundtrip_req(req.clone());
        assert_eq!(decoded.shuffle_id, req.shuffle_id);
        assert_eq!(decoded.part, 7);
        assert_eq!(decoded.side, 1);
        assert_eq!(decoded.num_parts, 16);
        assert_eq!(decoded.producer_count, 3);
    }

    #[test]
    fn roundtrip_shuffle_push_request_build_side() {
        let req = ShufflePushRequest {
            shuffle_id: 1,
            part: 0,
            side: 0,
            num_parts: 1,
            producer_count: 1,
        };
        let decoded = roundtrip_req(req);
        assert_eq!(decoded.side, 0);
        assert_eq!(decoded.num_parts, 1);
        assert_eq!(decoded.producer_count, 1);
    }

    #[test]
    fn roundtrip_shuffle_push_chunk_payload() {
        let chunk = ShufflePushChunk {
            payload: vec![0x93, 0x01, 0x02, 0x03],
        };
        let decoded = roundtrip_chunk(chunk.clone());
        assert_eq!(decoded.payload, chunk.payload);
    }

    #[test]
    fn roundtrip_shuffle_push_chunk_empty_payload() {
        let decoded = roundtrip_chunk(ShufflePushChunk { payload: vec![] });
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn roundtrip_shuffle_push_end_clean_eof() {
        let decoded = roundtrip_end(ShufflePushEnd { error: None });
        assert!(decoded.error.is_none());
    }

    #[test]
    fn roundtrip_shuffle_push_end_terminal_error() {
        let decoded = roundtrip_end(ShufflePushEnd {
            error: Some(TypedClusterError::Internal {
                code: 0xABCD,
                message: "shuffle producer failed mid-flight".into(),
            }),
        });
        match decoded.error {
            Some(TypedClusterError::Internal { code, message }) => {
                assert_eq!(code, 0xABCD);
                assert!(message.contains("shuffle producer"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    fn roundtrip_produce_req(req: ShuffleProduceRequest) -> ShuffleProduceRequest {
        let rpc = RaftRpc::ShuffleProduceRequest(req);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::ShuffleProduceRequest(r) => r,
            other => panic!("expected ShuffleProduceRequest, got {other:?}"),
        }
    }

    fn roundtrip_produce_resp(resp: ShuffleProduceResponse) -> ShuffleProduceResponse {
        let rpc = RaftRpc::ShuffleProduceResponse(resp);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::ShuffleProduceResponse(r) => r,
            other => panic!("expected ShuffleProduceResponse, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_shuffle_produce_request() {
        let req = ShuffleProduceRequest {
            shuffle_id: 0x1234_5678_9ABC_DEF0,
            side: 1,
            num_parts: 4,
            producer_count: 2,
            keys: vec!["k".into(), "tenant.id".into()],
            part_node_map: vec![
                PartNodeEntry {
                    part: 0,
                    node_id: 7,
                },
                PartNodeEntry {
                    part: 1,
                    node_id: 9,
                },
                PartNodeEntry {
                    part: 2,
                    node_id: 7,
                },
                PartNodeEntry {
                    part: 3,
                    node_id: 9,
                },
            ],
            plan_bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
            tenant_id: 42,
            database_id: 3,
            deadline_remaining_ms: 7000,
            trace_id: [5u8; 16],
            descriptor_versions: vec![DescriptorVersionEntry {
                collection: "orders".into(),
                version: 11,
            }],
            read_groups: vec![3, 8],
        };
        let decoded = roundtrip_produce_req(req.clone());
        assert_eq!(decoded.shuffle_id, req.shuffle_id);
        assert_eq!(decoded.side, 1);
        assert_eq!(decoded.num_parts, 4);
        assert_eq!(decoded.producer_count, 2);
        assert_eq!(decoded.keys, vec!["k".to_string(), "tenant.id".to_string()]);
        assert_eq!(decoded.part_node_map.len(), 4);
        assert_eq!(decoded.part_node_map[2].part, 2);
        assert_eq!(decoded.part_node_map[2].node_id, 7);
        assert_eq!(decoded.plan_bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(decoded.tenant_id, 42);
        assert_eq!(decoded.database_id, 3);
        assert_eq!(decoded.deadline_remaining_ms, 7000);
        assert_eq!(decoded.trace_id, [5u8; 16]);
        assert_eq!(decoded.descriptor_versions.len(), 1);
        assert_eq!(decoded.descriptor_versions[0].collection, "orders");
        assert_eq!(decoded.descriptor_versions[0].version, 11);
        assert_eq!(decoded.read_groups, vec![3, 8]);
    }

    #[test]
    fn roundtrip_shuffle_produce_request_empty_keys_and_map() {
        let req = ShuffleProduceRequest {
            shuffle_id: 1,
            side: 0,
            num_parts: 1,
            producer_count: 1,
            keys: vec![],
            part_node_map: vec![],
            plan_bytes: vec![],
            tenant_id: 0,
            database_id: 0,
            deadline_remaining_ms: 1000,
            trace_id: [0u8; 16],
            descriptor_versions: vec![],
            read_groups: vec![],
        };
        let decoded = roundtrip_produce_req(req);
        assert!(decoded.keys.is_empty());
        assert!(decoded.part_node_map.is_empty());
        assert!(decoded.descriptor_versions.is_empty());
    }

    #[test]
    fn roundtrip_shuffle_produce_response_clean() {
        let read_versions = vec![nodedb_types::ShardVersion {
            vshard: 9,
            version: nodedb_types::WriteVersion::logged(1, 0xABCD_1234),
        }];
        let decoded = roundtrip_produce_resp(ShuffleProduceResponse {
            error: None,
            read_versions: read_versions.clone(),
        });
        assert!(decoded.error.is_none());
        assert_eq!(
            decoded.read_versions, read_versions,
            "producer read versions roundtrip on the produce reply"
        );
    }

    #[test]
    fn roundtrip_shuffle_produce_response_error() {
        let decoded = roundtrip_produce_resp(ShuffleProduceResponse {
            error: Some(TypedClusterError::Internal {
                code: 0x55,
                message: "produce scan failed".into(),
            }),
            read_versions: Vec::new(),
        });
        assert!(
            decoded.read_versions.is_empty(),
            "a failed produce carries no read versions"
        );
        match decoded.error {
            Some(TypedClusterError::Internal { code, message }) => {
                assert_eq!(code, 0x55);
                assert!(message.contains("produce scan"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    fn roundtrip_consume_req(req: ShuffleConsumeRequest) -> ShuffleConsumeRequest {
        let rpc = RaftRpc::ShuffleConsumeRequest(req);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::ShuffleConsumeRequest(r) => r,
            other => panic!("expected ShuffleConsumeRequest, got {other:?}"),
        }
    }

    fn roundtrip_consume_resp(resp: ShuffleConsumeResponse) -> ShuffleConsumeResponse {
        let rpc = RaftRpc::ShuffleConsumeResponse(resp);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::ShuffleConsumeResponse(r) => r,
            other => panic!("expected ShuffleConsumeResponse, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_shuffle_consume_request() {
        let req = ShuffleConsumeRequest {
            shuffle_id: 0x0FED_CBA9_8765_4321,
            part: 3,
            on: vec![
                JoinKeyPair {
                    left: "lk".into(),
                    right: "rk".into(),
                },
                JoinKeyPair {
                    left: "tenant".into(),
                    right: "tenant_id".into(),
                },
            ],
            join_type: "inner".into(),
            limit: 1234,
            probe_qualifier: "l".into(),
            index_qualifier: "r".into(),
            tenant_id: 9,
            database_id: 4,
            deadline_remaining_ms: 8000,
            trace_id: [3u8; 16],
        };
        let decoded = roundtrip_consume_req(req.clone());
        assert_eq!(decoded.shuffle_id, req.shuffle_id);
        assert_eq!(decoded.part, 3);
        assert_eq!(decoded.on.len(), 2);
        assert_eq!(decoded.on[0].left, "lk");
        assert_eq!(decoded.on[0].right, "rk");
        assert_eq!(decoded.on[1].left, "tenant");
        assert_eq!(decoded.on[1].right, "tenant_id");
        assert_eq!(decoded.join_type, "inner");
        assert_eq!(decoded.limit, 1234);
        assert_eq!(decoded.probe_qualifier, "l");
        assert_eq!(decoded.index_qualifier, "r");
        assert_eq!(decoded.tenant_id, 9);
        assert_eq!(decoded.database_id, 4);
        assert_eq!(decoded.deadline_remaining_ms, 8000);
        assert_eq!(decoded.trace_id, [3u8; 16]);
    }

    #[test]
    fn roundtrip_shuffle_consume_request_empty_keys() {
        let req = ShuffleConsumeRequest {
            shuffle_id: 1,
            part: 0,
            on: vec![],
            join_type: "left".into(),
            limit: u64::MAX,
            probe_qualifier: String::new(),
            index_qualifier: String::new(),
            tenant_id: 0,
            database_id: 0,
            deadline_remaining_ms: 1000,
            trace_id: [0u8; 16],
        };
        let decoded = roundtrip_consume_req(req);
        assert!(decoded.on.is_empty());
        assert_eq!(decoded.limit, u64::MAX);
        assert_eq!(decoded.join_type, "left");
    }

    #[test]
    fn roundtrip_shuffle_consume_response_rows() {
        let decoded = roundtrip_consume_resp(ShuffleConsumeResponse {
            rows: vec![0x92, 0x01, 0x02],
            error: None,
        });
        assert_eq!(decoded.rows, vec![0x92, 0x01, 0x02]);
        assert!(decoded.error.is_none());
    }

    #[test]
    fn roundtrip_shuffle_consume_response_error() {
        let decoded = roundtrip_consume_resp(ShuffleConsumeResponse {
            rows: vec![],
            error: Some(TypedClusterError::DeadlineExceeded { elapsed_ms: 8000 }),
        });
        assert!(decoded.rows.is_empty());
        match decoded.error {
            Some(TypedClusterError::DeadlineExceeded { elapsed_ms }) => {
                assert_eq!(elapsed_ms, 8000);
            }
            other => panic!("expected DeadlineExceeded, got {other:?}"),
        }
    }

    fn roundtrip_agg_consume_req(
        req: ShuffleAggregateConsumeRequest,
    ) -> ShuffleAggregateConsumeRequest {
        let rpc = RaftRpc::ShuffleAggregateConsumeRequest(req);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::ShuffleAggregateConsumeRequest(r) => r,
            other => panic!("expected ShuffleAggregateConsumeRequest, got {other:?}"),
        }
    }

    fn roundtrip_agg_consume_resp(
        resp: ShuffleAggregateConsumeResponse,
    ) -> ShuffleAggregateConsumeResponse {
        let rpc = RaftRpc::ShuffleAggregateConsumeResponse(resp);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::ShuffleAggregateConsumeResponse(r) => r,
            other => panic!("expected ShuffleAggregateConsumeResponse, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_shuffle_agg_consume_request() {
        let req = ShuffleAggregateConsumeRequest {
            shuffle_id: 0x0FED_CBA9_8765_4321,
            part: 2,
            group_by: vec!["k".into(), "region".into()],
            aggregates_bytes: vec![0x91, 0x01, 0x02],
            having: vec![0xC0],
            limit: 4321,
            sort_keys: vec![
                SortKey {
                    column: "k".into(),
                    ascending: true,
                },
                SortKey {
                    column: "total".into(),
                    ascending: false,
                },
            ],
            tenant_id: 9,
            database_id: 4,
            deadline_remaining_ms: 8000,
            trace_id: [3u8; 16],
        };
        let decoded = roundtrip_agg_consume_req(req.clone());
        assert_eq!(decoded.shuffle_id, req.shuffle_id);
        assert_eq!(decoded.part, 2);
        assert_eq!(
            decoded.group_by,
            vec!["k".to_string(), "region".to_string()]
        );
        assert_eq!(decoded.aggregates_bytes, vec![0x91, 0x01, 0x02]);
        assert_eq!(decoded.having, vec![0xC0]);
        assert_eq!(decoded.limit, 4321);
        assert_eq!(decoded.sort_keys.len(), 2);
        assert_eq!(decoded.sort_keys[0].column, "k");
        assert!(decoded.sort_keys[0].ascending);
        assert_eq!(decoded.sort_keys[1].column, "total");
        assert!(!decoded.sort_keys[1].ascending);
        assert_eq!(decoded.tenant_id, 9);
        assert_eq!(decoded.database_id, 4);
        assert_eq!(decoded.deadline_remaining_ms, 8000);
        assert_eq!(decoded.trace_id, [3u8; 16]);
    }

    #[test]
    fn roundtrip_shuffle_agg_consume_request_empty() {
        let req = ShuffleAggregateConsumeRequest {
            shuffle_id: 1,
            part: 0,
            group_by: vec![],
            aggregates_bytes: vec![],
            having: vec![],
            limit: u64::MAX,
            sort_keys: vec![],
            tenant_id: 0,
            database_id: 0,
            deadline_remaining_ms: 1000,
            trace_id: [0u8; 16],
        };
        let decoded = roundtrip_agg_consume_req(req);
        assert!(decoded.group_by.is_empty());
        assert!(decoded.aggregates_bytes.is_empty());
        assert!(decoded.sort_keys.is_empty());
        assert_eq!(decoded.limit, u64::MAX);
    }

    #[test]
    fn roundtrip_shuffle_agg_consume_response_rows() {
        let decoded = roundtrip_agg_consume_resp(ShuffleAggregateConsumeResponse {
            rows: vec![0x92, 0x01, 0x02],
            error: None,
        });
        assert_eq!(decoded.rows, vec![0x92, 0x01, 0x02]);
        assert!(decoded.error.is_none());
    }

    #[test]
    fn roundtrip_shuffle_agg_consume_response_error() {
        let decoded = roundtrip_agg_consume_resp(ShuffleAggregateConsumeResponse {
            rows: vec![],
            error: Some(TypedClusterError::DeadlineExceeded { elapsed_ms: 9000 }),
        });
        assert!(decoded.rows.is_empty());
        match decoded.error {
            Some(TypedClusterError::DeadlineExceeded { elapsed_ms }) => {
                assert_eq!(elapsed_ms, 9000);
            }
            other => panic!("expected DeadlineExceeded, got {other:?}"),
        }
    }
}
