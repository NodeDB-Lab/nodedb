// SPDX-License-Identifier: BUSL-1.1

//! Cluster management wire types and codecs.

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// Wire-level redirect contract between the join-flow producer
/// and the client-side parser.
pub const LEADER_REDIRECT_PREFIX: &str = "not leader; retry at ";

/// Request to join an existing cluster.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct JoinRequest {
    pub node_id: u64,
    pub listen_addr: String,
    pub wire_version: u16,
    /// Joiner's `nodedb_types::wire_version::WIRE_BUILD_ID`. Compared for
    /// exact equality against this node's own build — see `handle_join_request`.
    pub build_id: String,
    /// SPIFFE URI SAN from the joiner's mTLS leaf certificate, if present.
    pub spiffe_id: Option<String>,
    /// SHA-256 SPKI fingerprint of the joiner's mTLS leaf certificate.
    /// Stored as `Vec<u8>` for rkyv compatibility; always 32 bytes when present.
    pub spki_pin: Option<Vec<u8>>,
    /// Bound UDP address of the joiner's SWIM failure detector.
    pub swim_addr: Option<String>,
}

/// Response to a join request — carries full cluster state.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct JoinResponse {
    pub success: bool,
    pub error: String,
    pub cluster_id: u64,
    pub nodes: Vec<JoinNodeInfo>,
    pub vshard_to_group: Vec<u64>,
    pub groups: Vec<JoinGroupInfo>,
}

/// Node info in the join response wire format.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct JoinNodeInfo {
    pub node_id: u64,
    pub addr: String,
    pub state: u8,
    pub raft_groups: Vec<u64>,
    pub wire_version: u16,
    /// SPIFFE URI SAN for this node, if known.
    pub spiffe_id: Option<String>,
    /// SHA-256 SPKI fingerprint for this node.
    /// Stored as `Vec<u8>` for rkyv compatibility; always 32 bytes when present.
    pub spki_pin: Option<Vec<u8>>,
    /// Bound UDP address of this node's SWIM failure detector, if it runs one.
    pub swim_addr: Option<String>,
}

/// Raft group membership in the join response wire format.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct JoinGroupInfo {
    pub group_id: u64,
    pub leader: u64,
    pub members: Vec<u64>,
    pub learners: Vec<u64>,
}

/// Health check ping.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PingRequest {
    pub sender_id: u64,
    pub topology_version: u64,
    /// The sender's oldest-snapshot report (see `snapshot_board`).
    /// `NO_SNAPSHOT_REPORT` on a bare probe.
    pub oldest_snapshot: i64,
}

/// Health check pong.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PongResponse {
    pub responder_id: u64,
    pub topology_version: u64,
    /// The responder's oldest-snapshot report (see `snapshot_board`).
    /// `NO_SNAPSHOT_REPORT` from a responder with no board.
    pub oldest_snapshot: i64,
}

/// Push topology update to a peer.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct TopologyUpdate {
    pub version: u64,
    pub nodes: Vec<JoinNodeInfo>,
}

/// Acknowledgement of a topology update.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct TopologyAck {
    pub responder_id: u64,
    pub accepted_version: u64,
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

pub(super) fn encode_join_req(msg: &JoinRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_JOIN_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_join_resp(msg: &JoinResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_JOIN_RESP, &to_bytes!(msg)?, out)
}
pub(super) fn encode_ping(msg: &PingRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_PING, &to_bytes!(msg)?, out)
}
pub(super) fn encode_pong(msg: &PongResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_PONG, &to_bytes!(msg)?, out)
}
pub(super) fn encode_topology_update(msg: &TopologyUpdate, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_TOPOLOGY_UPDATE, &to_bytes!(msg)?, out)
}
pub(super) fn encode_topology_ack(msg: &TopologyAck, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_TOPOLOGY_ACK, &to_bytes!(msg)?, out)
}

pub(super) fn decode_join_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::JoinRequest(from_bytes!(
        payload,
        JoinRequest,
        "JoinRequest"
    )?))
}
pub(super) fn decode_join_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::JoinResponse(from_bytes!(
        payload,
        JoinResponse,
        "JoinResponse"
    )?))
}
pub(super) fn decode_ping(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::Ping(from_bytes!(
        payload,
        PingRequest,
        "PingRequest"
    )?))
}
pub(super) fn decode_pong(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::Pong(from_bytes!(
        payload,
        PongResponse,
        "PongResponse"
    )?))
}
pub(super) fn decode_topology_update(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::TopologyUpdate(from_bytes!(
        payload,
        TopologyUpdate,
        "TopologyUpdate"
    )?))
}
pub(super) fn decode_topology_ack(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::TopologyAck(from_bytes!(
        payload,
        TopologyAck,
        "TopologyAck"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(rpc: RaftRpc) -> RaftRpc {
        let encoded =
            super::super::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
                .unwrap();
        super::super::decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap()
    }

    #[test]
    fn roundtrip_join_request() {
        let req = JoinRequest {
            node_id: 42,
            listen_addr: "10.0.0.5:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: Some("spiffe://cluster.local/node/42".into()),
            spki_pin: Some(vec![0xabu8; 32]),
            swim_addr: Some("10.0.0.5:9401".into()),
        };
        match roundtrip(RaftRpc::JoinRequest(req)) {
            RaftRpc::JoinRequest(d) => {
                assert_eq!(d.node_id, 42);
                assert_eq!(d.listen_addr, "10.0.0.5:9400");
                assert_eq!(
                    d.spiffe_id.as_deref(),
                    Some("spiffe://cluster.local/node/42")
                );
                assert_eq!(d.spki_pin.as_deref(), Some([0xabu8; 32].as_ref()));
                assert_eq!(d.build_id, nodedb_types::wire_version::WIRE_BUILD_ID);
                assert_eq!(d.swim_addr.as_deref(), Some("10.0.0.5:9401"));
            }
            other => panic!("expected JoinRequest, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_join_response() {
        let resp = JoinResponse {
            success: true,
            error: String::new(),
            cluster_id: 12345,
            nodes: vec![JoinNodeInfo {
                node_id: 1,
                addr: "10.0.0.1:9400".into(),
                state: 1,
                raft_groups: vec![0, 1],
                wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
                spiffe_id: None,
                spki_pin: None,
                swim_addr: Some("10.0.0.1:9401".into()),
            }],
            vshard_to_group: (0..1024u64).map(|i| i % 4).collect(),
            groups: vec![JoinGroupInfo {
                group_id: 0,
                leader: 1,
                members: vec![1],
                learners: vec![],
            }],
        };
        match roundtrip(RaftRpc::JoinResponse(resp)) {
            RaftRpc::JoinResponse(d) => {
                assert!(d.success);
                assert_eq!(d.nodes.len(), 1);
                assert_eq!(d.vshard_to_group.len(), 1024);
                assert_eq!(d.nodes[0].swim_addr.as_deref(), Some("10.0.0.1:9401"));
            }
            other => panic!("expected JoinResponse, got {other:?}"),
        }
    }
}
