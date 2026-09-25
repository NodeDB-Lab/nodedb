// SPDX-License-Identifier: BUSL-1.1

//! ReadIndexRequest / ReadIndexResponse wire types and codecs.
//!
//! A node that does not lead a Raft group asks the group leader for a read
//! index. The leader confirms its leadership against a quorum and answers
//! with its commit index at the time of the request. Once the asking node has
//! applied the group through that index, its state includes every entry
//! committed before the request.

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// Ask the leader of `group_id` for a confirmed read index.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ReadIndexRequest {
    pub group_id: u64,
    /// How long the leader may wait for a quorum to confirm it.
    pub timeout_ms: u64,
}

/// The leader's answer to a [`ReadIndexRequest`].
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum ReadIndexOutcome {
    /// A quorum confirmed the leader. Reads may be served at `read_index`.
    Confirmed { read_index: u64 },
    /// The receiver does not lead the group. `leader_hint` names the leader
    /// it knows of.
    NotLeader { leader_hint: Option<u64> },
    /// The receiver leads the group, but no quorum answered in time.
    Timeout { waited_ms: u64 },
}

/// Response to a [`ReadIndexRequest`].
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ReadIndexResponse {
    pub outcome: ReadIndexOutcome,
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

pub(super) fn encode_read_index_req(msg: &ReadIndexRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_READ_INDEX_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_read_index_resp(msg: &ReadIndexResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_READ_INDEX_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_read_index_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ReadIndexRequest(from_bytes!(
        payload,
        ReadIndexRequest,
        "ReadIndexRequest"
    )?))
}
pub(super) fn decode_read_index_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::ReadIndexResponse(from_bytes!(
        payload,
        ReadIndexResponse,
        "ReadIndexResponse"
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
    fn a_request_survives_the_wire() {
        let rpc = roundtrip(RaftRpc::ReadIndexRequest(ReadIndexRequest {
            group_id: 7,
            timeout_ms: 750,
        }));
        match rpc {
            RaftRpc::ReadIndexRequest(req) => {
                assert_eq!(req.group_id, 7);
                assert_eq!(req.timeout_ms, 750);
            }
            other => panic!("decoded the wrong variant: {other:?}"),
        }
    }

    #[test]
    fn every_outcome_survives_the_wire() {
        for outcome in [
            ReadIndexOutcome::Confirmed { read_index: 42 },
            ReadIndexOutcome::NotLeader {
                leader_hint: Some(3),
            },
            ReadIndexOutcome::NotLeader { leader_hint: None },
            ReadIndexOutcome::Timeout { waited_ms: 750 },
        ] {
            let rpc = roundtrip(RaftRpc::ReadIndexResponse(ReadIndexResponse {
                outcome: outcome.clone(),
            }));
            match rpc {
                RaftRpc::ReadIndexResponse(resp) => assert_eq!(resp.outcome, outcome),
                other => panic!("decoded the wrong variant: {other:?}"),
            }
        }
    }
}
