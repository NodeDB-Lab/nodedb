// SPDX-License-Identifier: BUSL-1.1

//! OrdinalFloorRequest / OrdinalFloorResponse wire types and codecs.
//!
//! A group's leader sends its system-time floor to every voter. A voter at
//! the leader's term raises its own floor to it, makes it durable, and
//! answers the floor it holds. A leader that closes a read cut spreads the
//! cut this way. A new leader gathers the highest floor this way before it
//! stamps a write.

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// The leader of `group_id` at `term` sends its floor.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct OrdinalFloorRequest {
    pub group_id: u64,
    pub term: u64,
    pub floor: i64,
}

/// A voter's answer to an [`OrdinalFloorRequest`].
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct OrdinalFloorResponse {
    /// The voter's term for the group, `0` when it does not host the group.
    pub term: u64,
    /// The floor the voter holds durably, once it took the leader's. `None`
    /// when the voter is not at the request's term: the answer counts toward
    /// no quorum.
    pub floor: Option<i64>,
}

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

pub(super) fn encode_ordinal_floor_req(msg: &OrdinalFloorRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_ORDINAL_FLOOR_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_ordinal_floor_resp(
    msg: &OrdinalFloorResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_ORDINAL_FLOOR_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_ordinal_floor_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::OrdinalFloorRequest(from_bytes!(
        payload,
        OrdinalFloorRequest,
        "OrdinalFloorRequest"
    )?))
}
pub(super) fn decode_ordinal_floor_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::OrdinalFloorResponse(from_bytes!(
        payload,
        OrdinalFloorResponse,
        "OrdinalFloorResponse"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_epoch::ClusterEpochState;
    use crate::rpc_codec::{decode, encode};

    fn roundtrip(rpc: RaftRpc) -> RaftRpc {
        let epoch = ClusterEpochState::default();
        let encoded = encode(&rpc, &epoch).expect("encode");
        decode(&encoded, &epoch).expect("decode")
    }

    #[test]
    fn a_floor_exchange_roundtrips() {
        let request = OrdinalFloorRequest {
            group_id: 4,
            term: 9,
            floor: 1_700_000_000_000_000_123,
        };
        match roundtrip(RaftRpc::OrdinalFloorRequest(request.clone())) {
            RaftRpc::OrdinalFloorRequest(decoded) => assert_eq!(decoded, request),
            other => panic!("expected OrdinalFloorRequest, got {other:?}"),
        }
        for floor in [Some(55), None] {
            let response = OrdinalFloorResponse { term: 9, floor };
            match roundtrip(RaftRpc::OrdinalFloorResponse(response.clone())) {
                RaftRpc::OrdinalFloorResponse(decoded) => assert_eq!(decoded, response),
                other => panic!("expected OrdinalFloorResponse, got {other:?}"),
            }
        }
    }
}
