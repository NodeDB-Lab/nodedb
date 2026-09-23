// SPDX-License-Identifier: BUSL-1.1

//! DataProposeRequest / DataProposeResponse wire types and codecs.
//!
//! Used to forward a non-metadata Raft proposal from a node that does not
//! lead the target group to the group leader. The target is a vShard's data
//! group or the Calvin sequencer group. The leader applies the proposal
//! locally and returns `(group_id, log_index)`.

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// The Raft group a forwarded proposal is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum ProposeTarget {
    /// The data group that owns this vShard. The bytes are a serialized
    /// `ReplicatedEntry`.
    VShard(u32),
    /// The Calvin sequencer group. The bytes are a msgpack-encoded
    /// `SequencerEntry`.
    Sequencer,
}

/// Forward an opaque proposal payload to the leader of its target group.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct DataProposeRequest {
    pub target: ProposeTarget,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_epoch::ClusterEpochState;
    use crate::rpc_codec::{decode, encode};

    fn roundtrip(target: ProposeTarget) -> DataProposeRequest {
        let rpc = RaftRpc::DataProposeRequest(DataProposeRequest {
            target,
            bytes: vec![1, 2, 3],
        });
        let epoch = ClusterEpochState::default();
        let encoded = encode(&rpc, &epoch).expect("encode");
        match decode(&encoded, &epoch).expect("decode") {
            RaftRpc::DataProposeRequest(req) => req,
            other => panic!("decoded the wrong variant: {other:?}"),
        }
    }

    #[test]
    fn sequencer_target_survives_the_wire() {
        let req = roundtrip(ProposeTarget::Sequencer);
        assert_eq!(req.target, ProposeTarget::Sequencer);
        assert_eq!(req.bytes, vec![1, 2, 3]);
    }

    #[test]
    fn vshard_target_survives_the_wire() {
        let req = roundtrip(ProposeTarget::VShard(42));
        assert_eq!(req.target, ProposeTarget::VShard(42));
    }
}
