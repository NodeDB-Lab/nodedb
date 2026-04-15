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
    pub tenant_id: u32,
    /// Milliseconds remaining until the caller's deadline.
    /// 0 means the deadline has already expired — receiver returns DeadlineExceeded.
    pub deadline_remaining_ms: u64,
    /// Distributed trace ID for observability.
    pub trace_id: u64,
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

impl ExecuteResponse {
    pub fn ok(payloads: Vec<Vec<u8>>) -> Self {
        Self {
            success: true,
            payloads,
            error: None,
        }
    }
    pub fn err(error: TypedClusterError) -> Self {
        Self {
            success: false,
            payloads: vec![],
            error: Some(error),
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
            deadline_remaining_ms: 5000,
            trace_id: 0xDEAD_BEEF_1234_5678,
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
        assert_eq!(decoded.trace_id, req.trace_id);
        assert_eq!(decoded.descriptor_versions.len(), 2);
        assert_eq!(decoded.descriptor_versions[0].collection, "orders");
        assert_eq!(decoded.descriptor_versions[0].version, 42);
    }

    #[test]
    fn roundtrip_execute_request_empty_descriptors() {
        let req = ExecuteRequest {
            plan_bytes: vec![0xAB, 0xCD],
            tenant_id: 0,
            deadline_remaining_ms: 1000,
            trace_id: 0,
            descriptor_versions: vec![],
        };
        let decoded = roundtrip_req(req);
        assert!(decoded.descriptor_versions.is_empty());
    }

    #[test]
    fn roundtrip_execute_response_success() {
        let resp = ExecuteResponse::ok(vec![b"row1".to_vec(), b"row2".to_vec()]);
        let decoded = roundtrip_resp(resp);
        assert!(decoded.success);
        assert_eq!(decoded.payloads.len(), 2);
        assert_eq!(decoded.payloads[0], b"row1");
        assert!(decoded.error.is_none());
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
