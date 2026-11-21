// SPDX-License-Identifier: BUSL-1.1

//! Routed-surrogate-exchange one-shot RPC.
//!
//! A coordinator needs the AUTHORITATIVE global surrogate (the stable `u32`
//! cross-engine identity) for keys whose collection home vShard is led by
//! ANOTHER node. It sends one [`AssignSurrogateRequest`] per batch of keys to
//! that vShard's LEADER. The leader IS the home, so its answer is the
//! first-wins value every owner stores. It mints every unbound key of the
//! batch in one Raft entry and replies with one [`AssignSurrogateResponse`].
//! One-shot request/response, no streaming.
//!
//! The same exchange also serves READ-ONLY resolution. With `lookup_only` set,
//! the leader looks each binding up and never allocates. A key that names no
//! existing row answers `None`.
//!
//! Discriminants 34/35 are permanently assigned to these variants.

use nodedb_types::WireBytes;

use super::discriminants::*;
use super::execute::TypedClusterError;
use super::header::{MAX_RPC_PAYLOAD_SIZE, write_frame};
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// The largest total of key bytes one request carries, each key counted with
/// [`ASSIGN_SURROGATE_KEY_OVERHEAD`].
///
/// The leader proposes the batch's unbound keys as one Raft entry. That entry
/// encodes each key byte in up to two bytes and travels in one AppendEntries
/// frame. A quarter of the frame cap keeps the entry and its envelope inside
/// that frame.
pub const ASSIGN_SURROGATE_KEY_BUDGET: usize = MAX_RPC_PAYLOAD_SIZE as usize / 4;

/// Bytes counted per key beside the key's own bytes: its length and offset in
/// the request, its answer in the reply, and its surrogate in the Raft entry.
pub const ASSIGN_SURROGATE_KEY_OVERHEAD: usize = 16;

// ── Wire types ──────────────────────────────────────────────────────────────

/// Coordinator → leader routed-surrogate-exchange request.
///
/// Carries the keys `(collection, pks)` plus the identity scope
/// (`database_id`, `tenant_id`) and the home vShard (`vshard_id`) they route
/// to. Every key belongs to the one collection. The `deadline_remaining_ms`
/// and `trace_id` fields mirror [`ExecuteRequest`](super::execute::ExecuteRequest).
/// The leader works to `deadline_remaining_ms` and refuses a request whose
/// budget is spent.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct AssignSurrogateRequest {
    /// Home vShard every key of `pks` routes to.
    pub vshard_id: u32,
    pub database_id: u64,
    pub tenant_id: u64,
    /// Bare catalog name of the collection the surrogates are scoped to. With
    /// `database_id` it forms the canonical collection key.
    pub collection: String,
    /// Primary-key bytes of each key to resolve. The reply answers them in
    /// this order.
    pub pks: Vec<WireBytes>,
    pub deadline_remaining_ms: u64,
    pub trace_id: [u8; 16],
    /// `true` = READ-ONLY resolution: the leader looks each binding up and
    /// never allocates. A key expected to name an existing row uses it. An
    /// allocation there mints identity for a row that does not exist.
    /// `false` = get-or-create assign.
    pub lookup_only: bool,
}

/// Terminal reply to an [`AssignSurrogateRequest`].
///
/// With `error: None`, `surrogates` holds one answer per key of the request,
/// in its order:
/// - `Some(s)`: the key's authoritative surrogate.
/// - `None`: the key has no binding. Only a `lookup_only` request answers it.
///
/// With `error: Some(e)` the leader ran nothing to completion and
/// `surrogates` is empty.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct AssignSurrogateResponse {
    /// One answer per requested key, in request order. An explicit `Option`
    /// marks a miss: zero is a reserved sentinel that also appears in
    /// catalog-less fixtures, so it cannot mean "no such row".
    pub surrogates: Vec<Option<u32>>,
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

pub(super) fn encode_assign_surrogate_req(
    msg: &AssignSurrogateRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_ASSIGN_SURROGATE_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_assign_surrogate_resp(
    msg: &AssignSurrogateResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_ASSIGN_SURROGATE_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_assign_surrogate_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::AssignSurrogateRequest(from_bytes!(
        payload,
        AssignSurrogateRequest,
        "AssignSurrogateRequest"
    )?))
}
pub(super) fn decode_assign_surrogate_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::AssignSurrogateResponse(from_bytes!(
        payload,
        AssignSurrogateResponse,
        "AssignSurrogateResponse"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_req(req: AssignSurrogateRequest) -> AssignSurrogateRequest {
        let rpc = RaftRpc::AssignSurrogateRequest(req);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::AssignSurrogateRequest(r) => r,
            other => panic!("expected AssignSurrogateRequest, got {other:?}"),
        }
    }

    fn roundtrip_resp(resp: AssignSurrogateResponse) -> AssignSurrogateResponse {
        let rpc = RaftRpc::AssignSurrogateResponse(resp);
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        match super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
        {
            RaftRpc::AssignSurrogateResponse(r) => r,
            other => panic!("expected AssignSurrogateResponse, got {other:?}"),
        }
    }

    /// A batched request keeps every key, in order, byte for byte.
    #[test]
    fn roundtrip_batched_assign_request() {
        let pks: Vec<WireBytes> = vec![
            WireBytes::from(b"alice".to_vec()),
            WireBytes::from(vec![0x00, 0x80, 0xFF]),
            WireBytes::from(Vec::new()),
            WireBytes::from(b"bob".to_vec()),
        ];
        let req = AssignSurrogateRequest {
            vshard_id: 512,
            database_id: 7,
            tenant_id: 42,
            collection: "people".into(),
            pks: pks.clone(),
            deadline_remaining_ms: 5000,
            trace_id: [9u8; 16],
            lookup_only: false,
        };
        let decoded = roundtrip_req(req);
        assert_eq!(decoded.vshard_id, 512);
        assert_eq!(decoded.database_id, 7);
        assert_eq!(decoded.tenant_id, 42);
        assert_eq!(decoded.collection, "people");
        assert_eq!(decoded.pks, pks);
        assert_eq!(decoded.deadline_remaining_ms, 5000);
        assert_eq!(decoded.trace_id, [9u8; 16]);
        assert!(!decoded.lookup_only);
    }

    /// A request that fills the key budget encodes inside one RPC frame.
    #[test]
    fn a_full_key_budget_fits_one_frame() {
        let key_len = 1024;
        let count = ASSIGN_SURROGATE_KEY_BUDGET / (key_len + ASSIGN_SURROGATE_KEY_OVERHEAD);
        let req = AssignSurrogateRequest {
            vshard_id: 1,
            database_id: 1,
            tenant_id: 1,
            collection: "people".into(),
            pks: vec![WireBytes::from(vec![0xFFu8; key_len]); count],
            deadline_remaining_ms: 1000,
            trace_id: [0u8; 16],
            lookup_only: false,
        };
        let encoded = super::super::encode(
            &RaftRpc::AssignSurrogateRequest(req),
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap();
        assert!(encoded.len() <= MAX_RPC_PAYLOAD_SIZE as usize);
    }

    #[test]
    fn roundtrip_assign_request_with_no_keys() {
        let req = AssignSurrogateRequest {
            vshard_id: 0,
            database_id: 0,
            tenant_id: 0,
            collection: String::new(),
            pks: Vec::new(),
            deadline_remaining_ms: 1000,
            trace_id: [0u8; 16],
            lookup_only: false,
        };
        let decoded = roundtrip_req(req);
        assert!(decoded.pks.is_empty());
        assert!(decoded.collection.is_empty());
        assert_eq!(decoded.deadline_remaining_ms, 1000);
    }

    /// A read-only request keeps its flag on the wire. A dropped flag turns a
    /// lookup into an allocating assign on the leader.
    #[test]
    fn roundtrip_lookup_only_request() {
        let req = AssignSurrogateRequest {
            vshard_id: 3,
            database_id: 1,
            tenant_id: 2,
            collection: "accounts".into(),
            pks: vec![
                WireBytes::from(b"acc-1".to_vec()),
                WireBytes::from(b"acc-2".to_vec()),
            ],
            deadline_remaining_ms: 1000,
            trace_id: [1u8; 16],
            lookup_only: true,
        };
        let decoded = roundtrip_req(req);
        assert!(decoded.lookup_only);
        assert_eq!(decoded.pks.len(), 2);
    }

    /// A batched reply keeps each answer in its slot, a miss as `None`.
    #[test]
    fn roundtrip_batched_response_with_a_miss() {
        let decoded = roundtrip_resp(AssignSurrogateResponse {
            surrogates: vec![Some(12345), None, Some(0), Some(7)],
            error: None,
        });
        assert_eq!(
            decoded.surrogates,
            vec![Some(12345), None, Some(0), Some(7)]
        );
        assert!(decoded.error.is_none());
    }

    #[test]
    fn roundtrip_assign_surrogate_response_error() {
        let decoded = roundtrip_resp(AssignSurrogateResponse {
            surrogates: Vec::new(),
            error: Some(TypedClusterError::Internal {
                code: 0x7,
                message: "assign-remote-surrogate not configured".into(),
            }),
        });
        assert!(decoded.surrogates.is_empty());
        match decoded.error {
            Some(TypedClusterError::Internal { code, message }) => {
                assert_eq!(code, 0x7);
                assert!(message.contains("assign-remote-surrogate"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }
}
