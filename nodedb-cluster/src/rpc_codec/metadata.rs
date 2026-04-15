//! MetadataProposeRequest / MetadataProposeResponse wire types and codecs.

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// Forward an opaque metadata-group proposal payload to the metadata-group leader.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct MetadataProposeRequest {
    pub bytes: Vec<u8>,
}

/// Response to a forwarded metadata-group proposal.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct MetadataProposeResponse {
    pub success: bool,
    pub log_index: u64,
    pub leader_hint: Option<u64>,
    pub error_message: String,
}

impl MetadataProposeResponse {
    pub fn ok(log_index: u64) -> Self {
        Self {
            success: true,
            log_index,
            leader_hint: None,
            error_message: String::new(),
        }
    }

    pub fn err(message: impl Into<String>, leader_hint: Option<u64>) -> Self {
        Self {
            success: false,
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

pub(super) fn encode_metadata_propose_req(
    msg: &MetadataProposeRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_METADATA_PROPOSE_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_metadata_propose_resp(
    msg: &MetadataProposeResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_METADATA_PROPOSE_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_metadata_propose_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::MetadataProposeRequest(from_bytes!(
        payload,
        MetadataProposeRequest,
        "MetadataProposeRequest"
    )?))
}
pub(super) fn decode_metadata_propose_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::MetadataProposeResponse(from_bytes!(
        payload,
        MetadataProposeResponse,
        "MetadataProposeResponse"
    )?))
}
