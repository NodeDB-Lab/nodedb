//! Top-level `RaftRpc` enum and `encode` / `decode` dispatcher.

use nodedb_raft::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};

use super::cluster_mgmt::{
    JoinRequest, JoinResponse, PingRequest, PongResponse, TopologyAck, TopologyUpdate,
};
use super::discriminants::*;
use super::execute::{ExecuteRequest, ExecuteResponse};
use super::header::HEADER_SIZE;
use super::metadata::{MetadataProposeRequest, MetadataProposeResponse};
use super::{cluster_mgmt, execute, metadata, raft_msgs, vshard};
use crate::error::{ClusterError, Result};

/// An RPC message — Raft consensus or cluster management.
#[derive(Debug, Clone)]
pub enum RaftRpc {
    // Raft consensus
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
    // Cluster management
    JoinRequest(JoinRequest),
    JoinResponse(JoinResponse),
    // Health check
    Ping(PingRequest),
    Pong(PongResponse),
    // Topology broadcast
    TopologyUpdate(TopologyUpdate),
    TopologyAck(TopologyAck),
    // Discriminants 13/14 (ForwardRequest/ForwardResponse) retired in C-δ.6.
    // VShardEnvelope
    VShardEnvelope(Vec<u8>),
    // Metadata-group proposal forwarding (group 0)
    MetadataProposeRequest(MetadataProposeRequest),
    MetadataProposeResponse(MetadataProposeResponse),
    // Physical-plan execution (Batch C-β onwards)
    ExecuteRequest(ExecuteRequest),
    ExecuteResponse(ExecuteResponse),
}

/// Encode a [`RaftRpc`] into a framed binary message.
pub fn encode(rpc: &RaftRpc) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(HEADER_SIZE + 64);
    match rpc {
        RaftRpc::AppendEntriesRequest(m) => raft_msgs::encode_append_entries_req(m, &mut out),
        RaftRpc::AppendEntriesResponse(m) => raft_msgs::encode_append_entries_resp(m, &mut out),
        RaftRpc::RequestVoteRequest(m) => raft_msgs::encode_request_vote_req(m, &mut out),
        RaftRpc::RequestVoteResponse(m) => raft_msgs::encode_request_vote_resp(m, &mut out),
        RaftRpc::InstallSnapshotRequest(m) => raft_msgs::encode_install_snapshot_req(m, &mut out),
        RaftRpc::InstallSnapshotResponse(m) => raft_msgs::encode_install_snapshot_resp(m, &mut out),
        RaftRpc::JoinRequest(m) => cluster_mgmt::encode_join_req(m, &mut out),
        RaftRpc::JoinResponse(m) => cluster_mgmt::encode_join_resp(m, &mut out),
        RaftRpc::Ping(m) => cluster_mgmt::encode_ping(m, &mut out),
        RaftRpc::Pong(m) => cluster_mgmt::encode_pong(m, &mut out),
        RaftRpc::TopologyUpdate(m) => cluster_mgmt::encode_topology_update(m, &mut out),
        RaftRpc::TopologyAck(m) => cluster_mgmt::encode_topology_ack(m, &mut out),
        RaftRpc::VShardEnvelope(bytes) => vshard::encode_vshard_envelope(bytes, &mut out),
        RaftRpc::MetadataProposeRequest(m) => metadata::encode_metadata_propose_req(m, &mut out),
        RaftRpc::MetadataProposeResponse(m) => metadata::encode_metadata_propose_resp(m, &mut out),
        RaftRpc::ExecuteRequest(m) => execute::encode_execute_req(m, &mut out),
        RaftRpc::ExecuteResponse(m) => execute::encode_execute_resp(m, &mut out),
    }?;
    Ok(out)
}

/// Decode a framed binary message into a [`RaftRpc`].
pub fn decode(data: &[u8]) -> Result<RaftRpc> {
    let (rpc_type, payload) = super::header::parse_frame(data)?;
    match rpc_type {
        RPC_APPEND_ENTRIES_REQ => raft_msgs::decode_append_entries_req(payload),
        RPC_APPEND_ENTRIES_RESP => raft_msgs::decode_append_entries_resp(payload),
        RPC_REQUEST_VOTE_REQ => raft_msgs::decode_request_vote_req(payload),
        RPC_REQUEST_VOTE_RESP => raft_msgs::decode_request_vote_resp(payload),
        RPC_INSTALL_SNAPSHOT_REQ => raft_msgs::decode_install_snapshot_req(payload),
        RPC_INSTALL_SNAPSHOT_RESP => raft_msgs::decode_install_snapshot_resp(payload),
        RPC_JOIN_REQ => cluster_mgmt::decode_join_req(payload),
        RPC_JOIN_RESP => cluster_mgmt::decode_join_resp(payload),
        RPC_PING => cluster_mgmt::decode_ping(payload),
        RPC_PONG => cluster_mgmt::decode_pong(payload),
        RPC_TOPOLOGY_UPDATE => cluster_mgmt::decode_topology_update(payload),
        RPC_TOPOLOGY_ACK => cluster_mgmt::decode_topology_ack(payload),
        // Discriminants 13/14 (ForwardRequest/ForwardResponse) are retired.
        // A node receiving these has a peer still running an older version.
        // Return a typed error so the operator sees a clear message.
        RPC_FORWARD_REQ | RPC_FORWARD_RESP => Err(ClusterError::Codec {
            detail: format!(
                "rpc_type {rpc_type} is a retired wire variant (ForwardRequest/ForwardResponse, \
                 retired in C-δ.6); upgrade all cluster nodes to remove this peer"
            ),
        }),
        RPC_VSHARD_ENVELOPE => vshard::decode_vshard_envelope(payload),
        RPC_METADATA_PROPOSE_REQ => metadata::decode_metadata_propose_req(payload),
        RPC_METADATA_PROPOSE_RESP => metadata::decode_metadata_propose_resp(payload),
        RPC_EXECUTE_REQ => execute::decode_execute_req(payload),
        RPC_EXECUTE_RESP => execute::decode_execute_resp(payload),
        _ => Err(ClusterError::Codec {
            detail: format!("unknown rpc_type: {rpc_type}"),
        }),
    }
}

/// Return the total frame size for a buffer that starts with a valid header.
pub fn frame_size(header: &[u8; HEADER_SIZE]) -> Result<usize> {
    super::header::frame_size(header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_raft::message::{AppendEntriesResponse, RequestVoteResponse};

    #[test]
    fn crc_corruption_detected() {
        let rpc = RaftRpc::RequestVoteResponse(RequestVoteResponse {
            term: 1,
            vote_granted: false,
        });
        let mut encoded = encode(&rpc).unwrap();
        if let Some(last) = encoded.last_mut() {
            *last ^= 0x01;
        }
        let err = decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("CRC32C mismatch"), "{err}");
    }

    #[test]
    fn version_mismatch_rejected() {
        let rpc = RaftRpc::RequestVoteResponse(RequestVoteResponse {
            term: 1,
            vote_granted: false,
        });
        let mut encoded = encode(&rpc).unwrap();
        encoded[0] = 99;
        let err = decode(&encoded).unwrap_err();
        assert!(
            err.to_string().contains("unsupported wire version"),
            "{err}"
        );
    }

    #[test]
    fn truncated_frame_rejected() {
        let err = decode(&[1, 2, 3]).unwrap_err();
        assert!(err.to_string().contains("frame too short"), "{err}");
    }

    #[test]
    fn unknown_rpc_type_rejected() {
        let rpc = RaftRpc::RequestVoteResponse(RequestVoteResponse {
            term: 1,
            vote_granted: false,
        });
        let mut encoded = encode(&rpc).unwrap();
        encoded[1] = 255;
        let err = decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("unknown rpc_type"), "{err}");
    }

    #[test]
    fn payload_too_large_rejected() {
        use super::super::header::MAX_RPC_PAYLOAD_SIZE;
        let mut frame = vec![0u8; HEADER_SIZE];
        frame[0] = crate::wire::WIRE_VERSION as u8;
        frame[1] = RPC_APPEND_ENTRIES_REQ;
        let huge: u32 = MAX_RPC_PAYLOAD_SIZE + 1;
        frame[2..6].copy_from_slice(&huge.to_le_bytes());
        let err = decode(&frame).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"), "{err}");
    }

    #[test]
    fn frame_size_helper() {
        let rpc = RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: 5,
        });
        let encoded = encode(&rpc).unwrap();
        let header: [u8; HEADER_SIZE] = encoded[..HEADER_SIZE].try_into().unwrap();
        let size = frame_size(&header).unwrap();
        assert_eq!(size, encoded.len());
    }
}
