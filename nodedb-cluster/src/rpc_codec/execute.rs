// SPDX-License-Identifier: BUSL-1.1

//! ExecuteRequest / ExecuteResponse — cross-node physical-plan execution RPC.
//!
//! Discriminants 18 and 19 are permanently assigned to these variants.

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

// ── Wire types ──────────────────────────────────────────────────────────────

/// A single (collection, version) entry sent by the caller to let the receiver
/// validate descriptor freshness before executing the plan.
///
/// Cross-version safety: new optional fields should be added as `Option<T>`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct DescriptorVersionEntry {
    pub collection: String,
    pub version: u64,
}

/// Send an already-planned `PhysicalPlan` to a remote node for execution.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ExecuteRequest {
    /// zerompk-encoded PhysicalPlan (via nodedb::bridge::physical_plan::wire::encode).
    pub plan_bytes: Vec<u8>,
    /// Tenant ID authenticated on the originating node; trusted on the receiver.
    pub tenant_id: u64,
    /// Database scope authenticated on the originating node; trusted on the receiver.
    /// `0` maps to `DatabaseId::DEFAULT` (the built-in `default` database).
    pub database_id: u64,
    /// Milliseconds remaining until the caller's deadline.
    /// 0 means the deadline has already expired — receiver returns DeadlineExceeded.
    pub deadline_remaining_ms: u64,
    /// Distributed trace ID for observability (16-byte W3C-compatible TraceId).
    pub trace_id: [u8; 16],
    /// Caller's view of descriptor versions for every collection touched by the plan.
    pub descriptor_versions: Vec<DescriptorVersionEntry>,
}

/// Response to an `ExecuteRequest`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ExecuteResponse {
    pub success: bool,
    /// Raw Data Plane response payloads, one per result set.
    pub payloads: Vec<Vec<u8>>,
    pub error: Option<TypedClusterError>,
    /// Max read watermark LSN observed by the executing node's cores; 0 for
    /// writes/errors. Mirrors [`ExecuteStreamChunk::watermark_lsn`]: raw `u64`
    /// on the wire, converted to `Lsn` at the coordinator via `Lsn::new`.
    pub watermark_lsn: u64,
}

/// Typed error returned by the remote executor.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum TypedClusterError {
    NotLeader {
        group_id: u64,
        leader_node_id: Option<u64>,
        leader_addr: Option<String>,
        term: u64,
    },
    DescriptorMismatch {
        collection: String,
        expected_version: u64,
        actual_version: u64,
    },
    DeadlineExceeded {
        elapsed_ms: u64,
    },
    /// Catch-all. `code` is a `nodedb_types::error::ErrorCode` as u32.
    Internal {
        code: u32,
        message: String,
    },
}

/// One streamed chunk of an `ExecuteStreamRequest` result.
///
/// Mirrors a `RowBatch` on the coordinator side: `payload` is a standalone
/// msgpack array of row elements (the exact bytes the Data Plane produced for a
/// single scan frame); `watermark_lsn` is that frame's read watermark. A
/// streaming response is a sequence of these followed by exactly one
/// [`ExecuteStreamEnd`].
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ExecuteStreamChunk {
    pub payload: Vec<u8>,
    pub watermark_lsn: u64,
}

/// Terminal frame of an `ExecuteStreamRequest` result.
///
/// `error: None` is a clean EOF (all chunks delivered). `error: Some(e)` is a
/// terminal failure — any chunks already delivered are valid, but the result is
/// incomplete and the consumer must surface the error.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ExecuteStreamEnd {
    pub error: Option<TypedClusterError>,
}

impl ExecuteResponse {
    pub fn ok(payloads: Vec<Vec<u8>>, watermark_lsn: u64) -> Self {
        Self {
            success: true,
            payloads,
            error: None,
            watermark_lsn,
        }
    }
    pub fn err(error: TypedClusterError) -> Self {
        Self {
            success: false,
            payloads: vec![],
            error: Some(error),
            watermark_lsn: 0,
        }
    }
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

pub(super) fn encode_execute_req(msg: &ExecuteRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_EXECUTE_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_execute_resp(msg: &ExecuteResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_EXECUTE_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_execute_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ExecuteRequest(from_bytes!(
        payload,
        ExecuteRequest,
        "ExecuteRequest"
    )?))
}
pub(super) fn decode_execute_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ExecuteResponse(from_bytes!(
        payload,
        ExecuteResponse,
        "ExecuteResponse"
    )?))
}

pub(super) fn encode_execute_stream_req(msg: &ExecuteRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_EXECUTE_STREAM_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_execute_stream_chunk(
    msg: &ExecuteStreamChunk,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_EXECUTE_STREAM_CHUNK, &to_bytes!(msg)?, out)
}
pub(super) fn encode_execute_stream_end(msg: &ExecuteStreamEnd, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_EXECUTE_STREAM_END, &to_bytes!(msg)?, out)
}

pub(super) fn decode_execute_stream_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ExecuteStreamRequest(from_bytes!(
        payload,
        ExecuteRequest,
        "ExecuteStreamRequest"
    )?))
}
pub(super) fn decode_execute_stream_chunk(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ExecuteStreamChunk(from_bytes!(
        payload,
        ExecuteStreamChunk,
        "ExecuteStreamChunk"
    )?))
}
pub(super) fn decode_execute_stream_end(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ExecuteStreamEnd(from_bytes!(
        payload,
        ExecuteStreamEnd,
        "ExecuteStreamEnd"
    )?))
}

/// Numeric code for `TypedClusterError::Internal` when plan bytes fail to decode.
pub const PLAN_DECODE_FAILED: u32 = 0x_CE00_0001;

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_req(req: ExecuteRequest) -> ExecuteRequest {
        let rpc = RaftRpc::ExecuteRequest(req);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ExecuteRequest(r) => r,
            other => panic!("expected ExecuteRequest, got {other:?}"),
        }
    }

    fn roundtrip_resp(resp: ExecuteResponse) -> ExecuteResponse {
        let rpc = RaftRpc::ExecuteResponse(resp);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ExecuteResponse(r) => r,
            other => panic!("expected ExecuteResponse, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_execute_request_basic() {
        let req = ExecuteRequest {
            plan_bytes: b"msgpack-plan-bytes".to_vec(),
            tenant_id: 7,
            database_id: 0,
            deadline_remaining_ms: 5000,
            trace_id: [
                0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34, 0x56, 0x78, 0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34,
                0x56, 0x78,
            ],
            descriptor_versions: vec![
                DescriptorVersionEntry {
                    collection: "orders".into(),
                    version: 42,
                },
                DescriptorVersionEntry {
                    collection: "users".into(),
                    version: 1,
                },
            ],
        };
        let decoded = roundtrip_req(req.clone());
        assert_eq!(decoded.plan_bytes, req.plan_bytes);
        assert_eq!(decoded.tenant_id, 7);
        assert_eq!(decoded.deadline_remaining_ms, 5000);
        assert_eq!(
            decoded.trace_id, req.trace_id,
            "trace_id roundtrips correctly"
        );
        assert_eq!(decoded.descriptor_versions.len(), 2);
        assert_eq!(decoded.descriptor_versions[0].collection, "orders");
        assert_eq!(decoded.descriptor_versions[0].version, 42);
    }

    #[test]
    fn roundtrip_execute_request_empty_descriptors() {
        let req = ExecuteRequest {
            plan_bytes: vec![0xAB, 0xCD],
            tenant_id: 0,
            database_id: 0,
            deadline_remaining_ms: 1000,
            trace_id: [0u8; 16],
            descriptor_versions: vec![],
        };
        let decoded = roundtrip_req(req);
        assert!(decoded.descriptor_versions.is_empty());
    }

    #[test]
    fn roundtrip_execute_response_success() {
        let resp = ExecuteResponse::ok(vec![b"row1".to_vec(), b"row2".to_vec()], 0xCAFE_1234);
        let decoded = roundtrip_resp(resp);
        assert!(decoded.success);
        assert_eq!(decoded.payloads.len(), 2);
        assert_eq!(decoded.payloads[0], b"row1");
        assert!(decoded.error.is_none());
        assert_eq!(
            decoded.watermark_lsn, 0xCAFE_1234,
            "read watermark roundtrips on the response body"
        );
    }

    #[test]
    fn roundtrip_execute_response_not_leader() {
        let resp = ExecuteResponse::err(TypedClusterError::NotLeader {
            group_id: 3,
            leader_node_id: Some(1),
            leader_addr: Some("10.0.0.1:9400".into()),
            term: 7,
        });
        let decoded = roundtrip_resp(resp);
        assert!(!decoded.success);
        assert_eq!(
            decoded.watermark_lsn, 0,
            "error responses carry no watermark"
        );
        match decoded.error {
            Some(TypedClusterError::NotLeader {
                group_id,
                leader_node_id,
                leader_addr,
                term,
            }) => {
                assert_eq!(group_id, 3);
                assert_eq!(leader_node_id, Some(1));
                assert_eq!(leader_addr.as_deref(), Some("10.0.0.1:9400"));
                assert_eq!(term, 7);
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_execute_response_descriptor_mismatch() {
        let resp = ExecuteResponse::err(TypedClusterError::DescriptorMismatch {
            collection: "orders".into(),
            expected_version: 5,
            actual_version: 6,
        });
        let decoded = roundtrip_resp(resp);
        match decoded.error {
            Some(TypedClusterError::DescriptorMismatch {
                collection,
                expected_version,
                actual_version,
            }) => {
                assert_eq!(collection, "orders");
                assert_eq!(expected_version, 5);
                assert_eq!(actual_version, 6);
            }
            other => panic!("expected DescriptorMismatch, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_execute_response_deadline_exceeded() {
        let resp = ExecuteResponse::err(TypedClusterError::DeadlineExceeded { elapsed_ms: 3000 });
        let decoded = roundtrip_resp(resp);
        match decoded.error {
            Some(TypedClusterError::DeadlineExceeded { elapsed_ms }) => {
                assert_eq!(elapsed_ms, 3000)
            }
            other => panic!("expected DeadlineExceeded, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_execute_response_internal_error() {
        let resp = ExecuteResponse::err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: "failed to decode plan".into(),
        });
        let decoded = roundtrip_resp(resp);
        match decoded.error {
            Some(TypedClusterError::Internal { code, message }) => {
                assert_eq!(code, PLAN_DECODE_FAILED);
                assert!(message.contains("plan"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    fn roundtrip_stream_chunk(chunk: ExecuteStreamChunk) -> ExecuteStreamChunk {
        let rpc = RaftRpc::ExecuteStreamChunk(chunk);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ExecuteStreamChunk(c) => c,
            other => panic!("expected ExecuteStreamChunk, got {other:?}"),
        }
    }

    fn roundtrip_stream_end(end: ExecuteStreamEnd) -> ExecuteStreamEnd {
        let rpc = RaftRpc::ExecuteStreamEnd(end);
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ExecuteStreamEnd(e) => e,
            other => panic!("expected ExecuteStreamEnd, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_execute_stream_request_reuses_execute_request_body() {
        let req = ExecuteRequest {
            plan_bytes: b"streaming-plan".to_vec(),
            tenant_id: 11,
            database_id: 2,
            deadline_remaining_ms: 4242,
            trace_id: [9u8; 16],
            descriptor_versions: vec![DescriptorVersionEntry {
                collection: "wide".into(),
                version: 3,
            }],
        };
        let rpc = RaftRpc::ExecuteStreamRequest(req.clone());
        let encoded = super::super::encode(&rpc).unwrap();
        match super::super::decode(&encoded).unwrap() {
            RaftRpc::ExecuteStreamRequest(r) => {
                assert_eq!(r.plan_bytes, req.plan_bytes);
                assert_eq!(r.tenant_id, 11);
                assert_eq!(r.database_id, 2);
                assert_eq!(r.deadline_remaining_ms, 4242);
                assert_eq!(r.trace_id, req.trace_id);
                assert_eq!(r.descriptor_versions.len(), 1);
                assert_eq!(r.descriptor_versions[0].collection, "wide");
                assert_eq!(r.descriptor_versions[0].version, 3);
            }
            other => panic!("expected ExecuteStreamRequest, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_execute_stream_chunk_payload_and_lsn() {
        let chunk = ExecuteStreamChunk {
            payload: vec![0x91, 0x01, 0x02, 0x03],
            watermark_lsn: 0xDEAD_BEEF,
        };
        let decoded = roundtrip_stream_chunk(chunk.clone());
        assert_eq!(decoded.payload, chunk.payload);
        assert_eq!(decoded.watermark_lsn, 0xDEAD_BEEF);
    }

    #[test]
    fn roundtrip_execute_stream_end_clean_eof() {
        let decoded = roundtrip_stream_end(ExecuteStreamEnd { error: None });
        assert!(decoded.error.is_none());
    }

    #[test]
    fn roundtrip_execute_stream_end_terminal_error() {
        let decoded = roundtrip_stream_end(ExecuteStreamEnd {
            error: Some(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: "stream failed mid-flight".into(),
            }),
        });
        match decoded.error {
            Some(TypedClusterError::Internal { code, message }) => {
                assert_eq!(code, PLAN_DECODE_FAILED);
                assert!(message.contains("stream failed"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_execute_response_not_leader_no_hint() {
        let resp = ExecuteResponse::err(TypedClusterError::NotLeader {
            group_id: 0,
            leader_node_id: None,
            leader_addr: None,
            term: 0,
        });
        let decoded = roundtrip_resp(resp);
        match decoded.error {
            Some(TypedClusterError::NotLeader {
                leader_node_id,
                leader_addr,
                ..
            }) => {
                assert!(leader_node_id.is_none());
                assert!(leader_addr.is_none());
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }
}
