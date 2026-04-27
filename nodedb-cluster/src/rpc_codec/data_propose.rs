//! DataProposeRequest / DataProposeResponse wire types and codecs.
//!
//! Used to forward data-group (non-metadata) Raft proposals from a follower
//! node to the group leader. The leader applies the proposal locally and
//! returns `(group_id, log_index)` so the forwarder can register a
//! `ProposeTracker` waiter and await commit.

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// Forward an opaque data-group proposal payload to the data-group leader.
///
/// `vshard_id` identifies the vShard (and thus the Raft group) the entry
/// belongs to. `bytes` is the serialized `ReplicatedEntry`.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct DataProposeRequest {
    pub vshard_id: u16,
    pub bytes: Vec<u8>,
}

/// Response to a forwarded data-group proposal.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct DataProposeResponse {
    pub success: bool,
    pub group_id: u64,
    pub log_index: u64,
    pub leader_hint: Option<u64>,
    pub error_message: String,
}

impl DataProposeResponse {
    pub fn ok(group_id: u64, log_index: u64) -> Self {
        Self {
            success: true,
            group_id,
            log_index,
            leader_hint: None,
            error_message: String::new(),
        }
    }

    pub fn err(message: impl Into<String>, leader_hint: Option<u64>) -> Self {
        Self {
            success: false,
            group_id: 0,
            log_index: 0,
            leader_hint,
            error_message: message.into(),
        }
    }
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

pub(super) fn encode_data_propose_req(msg: &DataProposeRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_DATA_PROPOSE_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_data_propose_resp(msg: &DataProposeResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_DATA_PROPOSE_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_data_propose_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::DataProposeRequest(from_bytes!(
        payload,
        DataProposeRequest,
        "DataProposeRequest"
    )?))
}
pub(super) fn decode_data_propose_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::DataProposeResponse(from_bytes!(
        payload,
        DataProposeResponse,
        "DataProposeResponse"
    )?))
}
