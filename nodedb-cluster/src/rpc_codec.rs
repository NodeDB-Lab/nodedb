//! Raft RPC binary codec.
//!
//! Encodes/decodes all Raft RPC messages into a compact binary wire format
//! using rkyv (zero-copy deserialization). Every frame includes a CRC32C
//! integrity checksum and a version field for protocol evolution.
//!
//! Wire layout (8-byte header + payload):
//!
//! ```text
//! ┌─────────┬──────────┬────────────┬──────────┬─────────────────────┐
//! │ version │ rpc_type │ payload_len│ crc32c   │ rkyv payload bytes  │
//! │  1 byte │  1 byte  │  4 bytes   │ 4 bytes  │  payload_len bytes  │
//! └─────────┴──────────┴────────────┴──────────┴─────────────────────┘
//! ```
//!
//! - `version`: Wire protocol version (currently `1`).
//! - `rpc_type`: Discriminant for [`RaftRpc`] variant.
//! - `payload_len`: Little-endian u32, byte count of the rkyv payload.
//! - `crc32c`: CRC32C over the rkyv payload bytes only.

use crate::error::{ClusterError, Result};
use nodedb_raft::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};

/// Current wire protocol version.
const WIRE_VERSION: u8 = 1;

/// Header size in bytes: version(1) + rpc_type(1) + payload_len(4) + crc32c(4).
pub const HEADER_SIZE: usize = 10;

/// Maximum payload size (64 MiB) — prevents degenerate allocations from corrupt frames.
const MAX_PAYLOAD_SIZE: u32 = 64 * 1024 * 1024;

/// RPC type discriminants.
const RPC_APPEND_ENTRIES_REQ: u8 = 1;
const RPC_APPEND_ENTRIES_RESP: u8 = 2;
const RPC_REQUEST_VOTE_REQ: u8 = 3;
const RPC_REQUEST_VOTE_RESP: u8 = 4;
const RPC_INSTALL_SNAPSHOT_REQ: u8 = 5;
const RPC_INSTALL_SNAPSHOT_RESP: u8 = 6;
const RPC_JOIN_REQ: u8 = 7;
const RPC_JOIN_RESP: u8 = 8;
const RPC_PING: u8 = 9;
const RPC_PONG: u8 = 10;
const RPC_TOPOLOGY_UPDATE: u8 = 11;
const RPC_TOPOLOGY_ACK: u8 = 12;
const RPC_FORWARD_REQ: u8 = 13;
const RPC_FORWARD_RESP: u8 = 14;

// ── Cluster management wire types ───────────────────────────────────

/// Forward a SQL query to the leader node for a vShard.
///
/// Used when a client connects to a non-leader node. The receiving node
/// re-plans and executes the SQL locally against its Data Plane.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ForwardRequest {
    /// The SQL statement to execute.
    pub sql: String,
    /// Tenant ID (authenticated on the originating node, trusted here).
    pub tenant_id: u32,
    /// Milliseconds remaining until the client's deadline.
    pub deadline_remaining_ms: u64,
    /// Distributed trace ID for observability.
    pub trace_id: u64,
}

/// Response to a forwarded SQL query.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ForwardResponse {
    /// True if the query succeeded.
    pub success: bool,
    /// Result payloads — one per result set produced by the query.
    /// Each payload is the raw bytes from the Data Plane response.
    pub payloads: Vec<Vec<u8>>,
    /// Non-empty if success=false.
    pub error_message: String,
}

/// Health check ping.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PingRequest {
    pub sender_id: u64,
    /// Sender's current topology version — lets the responder detect staleness.
    pub topology_version: u64,
}

/// Health check pong.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PongResponse {
    pub responder_id: u64,
    pub topology_version: u64,
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

/// Request to join an existing cluster.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct JoinRequest {
    pub node_id: u64,
    /// Listen address for Raft RPCs (e.g. "10.0.0.5:9400").
    pub listen_addr: String,
}

/// Response to a join request — carries full cluster state.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct JoinResponse {
    pub success: bool,
    pub error: String,
    /// All nodes in the cluster.
    pub nodes: Vec<JoinNodeInfo>,
    /// vShard → Raft group mapping (1024 entries).
    pub vshard_to_group: Vec<u64>,
    /// Raft group membership.
    pub groups: Vec<JoinGroupInfo>,
}

/// Node info in the join response wire format.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct JoinNodeInfo {
    pub node_id: u64,
    pub addr: String,
    /// NodeState as u8 (0=Joining, 1=Active, 2=Draining, 3=Decommissioned).
    pub state: u8,
    pub raft_groups: Vec<u64>,
}

/// Raft group membership in the join response wire format.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct JoinGroupInfo {
    pub group_id: u64,
    pub leader: u64,
    pub members: Vec<u64>,
}

// ── RPC enum ────────────────────────────────────────────────────────

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
    // Query forwarding
    ForwardRequest(ForwardRequest),
    ForwardResponse(ForwardResponse),
}

impl RaftRpc {
    fn rpc_type(&self) -> u8 {
        match self {
            Self::AppendEntriesRequest(_) => RPC_APPEND_ENTRIES_REQ,
            Self::AppendEntriesResponse(_) => RPC_APPEND_ENTRIES_RESP,
            Self::RequestVoteRequest(_) => RPC_REQUEST_VOTE_REQ,
            Self::RequestVoteResponse(_) => RPC_REQUEST_VOTE_RESP,
            Self::InstallSnapshotRequest(_) => RPC_INSTALL_SNAPSHOT_REQ,
            Self::InstallSnapshotResponse(_) => RPC_INSTALL_SNAPSHOT_RESP,
            Self::JoinRequest(_) => RPC_JOIN_REQ,
            Self::JoinResponse(_) => RPC_JOIN_RESP,
            Self::Ping(_) => RPC_PING,
            Self::Pong(_) => RPC_PONG,
            Self::TopologyUpdate(_) => RPC_TOPOLOGY_UPDATE,
            Self::TopologyAck(_) => RPC_TOPOLOGY_ACK,
            Self::ForwardRequest(_) => RPC_FORWARD_REQ,
            Self::ForwardResponse(_) => RPC_FORWARD_RESP,
        }
    }
}

/// Encode a [`RaftRpc`] into a framed binary message.
pub fn encode(rpc: &RaftRpc) -> Result<Vec<u8>> {
    let payload = serialize_payload(rpc)?;
    let payload_len: u32 = payload.len().try_into().map_err(|_| ClusterError::Codec {
        detail: format!("payload too large: {} bytes", payload.len()),
    })?;

    let crc = crc32c::crc32c(&payload);

    let mut frame = Vec::with_capacity(HEADER_SIZE + payload.len());
    frame.push(WIRE_VERSION);
    frame.push(rpc.rpc_type());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);

    Ok(frame)
}

/// Decode a framed binary message into a [`RaftRpc`].
pub fn decode(data: &[u8]) -> Result<RaftRpc> {
    if data.len() < HEADER_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("frame too short: {} bytes, need {HEADER_SIZE}", data.len()),
        });
    }

    let version = data[0];
    if version != WIRE_VERSION {
        return Err(ClusterError::Codec {
            detail: format!("unsupported wire version: {version}, expected {WIRE_VERSION}"),
        });
    }

    let rpc_type = data[1];
    let payload_len = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
    let expected_crc = u32::from_le_bytes([data[6], data[7], data[8], data[9]]);

    if payload_len > MAX_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("payload length {payload_len} exceeds maximum {MAX_PAYLOAD_SIZE}"),
        });
    }

    let expected_total = HEADER_SIZE + payload_len as usize;
    if data.len() < expected_total {
        return Err(ClusterError::Codec {
            detail: format!(
                "frame truncated: got {} bytes, expected {expected_total}",
                data.len()
            ),
        });
    }

    let payload = &data[HEADER_SIZE..expected_total];

    let actual_crc = crc32c::crc32c(payload);
    if actual_crc != expected_crc {
        return Err(ClusterError::Codec {
            detail: format!(
                "CRC32C mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
            ),
        });
    }

    deserialize_payload(rpc_type, payload)
}

/// Return the total frame size for a buffer that starts with a valid header.
/// Useful for stream framing — read the header, then read the remaining payload.
pub fn frame_size(header: &[u8; HEADER_SIZE]) -> Result<usize> {
    let payload_len = u32::from_le_bytes([header[2], header[3], header[4], header[5]]);
    if payload_len > MAX_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("payload length {payload_len} exceeds maximum {MAX_PAYLOAD_SIZE}"),
        });
    }
    Ok(HEADER_SIZE + payload_len as usize)
}

// ── Serialization helpers ───────────────────────────────────────────

fn serialize_payload(rpc: &RaftRpc) -> Result<Vec<u8>> {
    let bytes = match rpc {
        RaftRpc::AppendEntriesRequest(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::AppendEntriesResponse(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::RequestVoteRequest(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::RequestVoteResponse(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::InstallSnapshotRequest(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::InstallSnapshotResponse(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::JoinRequest(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::JoinResponse(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::Ping(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::Pong(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::TopologyUpdate(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::TopologyAck(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::ForwardRequest(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
        RaftRpc::ForwardResponse(msg) => rkyv::to_bytes::<rkyv::rancor::Error>(msg),
    };
    bytes.map(|b| b.to_vec()).map_err(|e| ClusterError::Codec {
        detail: format!("rkyv serialize failed: {e}"),
    })
}

fn deserialize_payload(rpc_type: u8, payload: &[u8]) -> Result<RaftRpc> {
    // rkyv requires aligned data for zero-copy access. Network-received slices
    // are not guaranteed to be aligned, so copy into an AlignedVec first.
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);

    match rpc_type {
        RPC_APPEND_ENTRIES_REQ => {
            let msg = rkyv::from_bytes::<AppendEntriesRequest, rkyv::rancor::Error>(&aligned)
                .map_err(|e| ClusterError::Codec {
                    detail: format!("rkyv deserialize AppendEntriesRequest: {e}"),
                })?;
            Ok(RaftRpc::AppendEntriesRequest(msg))
        }
        RPC_APPEND_ENTRIES_RESP => {
            let msg = rkyv::from_bytes::<AppendEntriesResponse, rkyv::rancor::Error>(&aligned)
                .map_err(|e| ClusterError::Codec {
                    detail: format!("rkyv deserialize AppendEntriesResponse: {e}"),
                })?;
            Ok(RaftRpc::AppendEntriesResponse(msg))
        }
        RPC_REQUEST_VOTE_REQ => {
            let msg = rkyv::from_bytes::<RequestVoteRequest, rkyv::rancor::Error>(&aligned)
                .map_err(|e| ClusterError::Codec {
                    detail: format!("rkyv deserialize RequestVoteRequest: {e}"),
                })?;
            Ok(RaftRpc::RequestVoteRequest(msg))
        }
        RPC_REQUEST_VOTE_RESP => {
            let msg = rkyv::from_bytes::<RequestVoteResponse, rkyv::rancor::Error>(&aligned)
                .map_err(|e| ClusterError::Codec {
                    detail: format!("rkyv deserialize RequestVoteResponse: {e}"),
                })?;
            Ok(RaftRpc::RequestVoteResponse(msg))
        }
        RPC_INSTALL_SNAPSHOT_REQ => {
            let msg = rkyv::from_bytes::<InstallSnapshotRequest, rkyv::rancor::Error>(&aligned)
                .map_err(|e| ClusterError::Codec {
                    detail: format!("rkyv deserialize InstallSnapshotRequest: {e}"),
                })?;
            Ok(RaftRpc::InstallSnapshotRequest(msg))
        }
        RPC_INSTALL_SNAPSHOT_RESP => {
            let msg = rkyv::from_bytes::<InstallSnapshotResponse, rkyv::rancor::Error>(&aligned)
                .map_err(|e| ClusterError::Codec {
                    detail: format!("rkyv deserialize InstallSnapshotResponse: {e}"),
                })?;
            Ok(RaftRpc::InstallSnapshotResponse(msg))
        }
        RPC_JOIN_REQ => {
            let msg =
                rkyv::from_bytes::<JoinRequest, rkyv::rancor::Error>(&aligned).map_err(|e| {
                    ClusterError::Codec {
                        detail: format!("rkyv deserialize JoinRequest: {e}"),
                    }
                })?;
            Ok(RaftRpc::JoinRequest(msg))
        }
        RPC_JOIN_RESP => {
            let msg =
                rkyv::from_bytes::<JoinResponse, rkyv::rancor::Error>(&aligned).map_err(|e| {
                    ClusterError::Codec {
                        detail: format!("rkyv deserialize JoinResponse: {e}"),
                    }
                })?;
            Ok(RaftRpc::JoinResponse(msg))
        }
        RPC_PING => {
            let msg =
                rkyv::from_bytes::<PingRequest, rkyv::rancor::Error>(&aligned).map_err(|e| {
                    ClusterError::Codec {
                        detail: format!("rkyv deserialize PingRequest: {e}"),
                    }
                })?;
            Ok(RaftRpc::Ping(msg))
        }
        RPC_PONG => {
            let msg =
                rkyv::from_bytes::<PongResponse, rkyv::rancor::Error>(&aligned).map_err(|e| {
                    ClusterError::Codec {
                        detail: format!("rkyv deserialize PongResponse: {e}"),
                    }
                })?;
            Ok(RaftRpc::Pong(msg))
        }
        RPC_TOPOLOGY_UPDATE => {
            let msg =
                rkyv::from_bytes::<TopologyUpdate, rkyv::rancor::Error>(&aligned).map_err(|e| {
                    ClusterError::Codec {
                        detail: format!("rkyv deserialize TopologyUpdate: {e}"),
                    }
                })?;
            Ok(RaftRpc::TopologyUpdate(msg))
        }
        RPC_TOPOLOGY_ACK => {
            let msg =
                rkyv::from_bytes::<TopologyAck, rkyv::rancor::Error>(&aligned).map_err(|e| {
                    ClusterError::Codec {
                        detail: format!("rkyv deserialize TopologyAck: {e}"),
                    }
                })?;
            Ok(RaftRpc::TopologyAck(msg))
        }
        RPC_FORWARD_REQ => {
            let msg = rkyv::from_bytes::<ForwardRequest, rkyv::rancor::Error>(&aligned)
                .map_err(|e| ClusterError::Codec {
                    detail: format!("rkyv deserialize ForwardRequest: {e}"),
                })?;
            Ok(RaftRpc::ForwardRequest(msg))
        }
        RPC_FORWARD_RESP => {
            let msg = rkyv::from_bytes::<ForwardResponse, rkyv::rancor::Error>(&aligned)
                .map_err(|e| ClusterError::Codec {
                    detail: format!("rkyv deserialize ForwardResponse: {e}"),
                })?;
            Ok(RaftRpc::ForwardResponse(msg))
        }
        _ => Err(ClusterError::Codec {
            detail: format!("unknown rpc_type: {rpc_type}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_raft::message::LogEntry;

    #[test]
    fn roundtrip_append_entries_request() {
        let req = AppendEntriesRequest {
            term: 5,
            leader_id: 1,
            prev_log_index: 99,
            prev_log_term: 4,
            entries: vec![
                LogEntry {
                    term: 5,
                    index: 100,
                    data: b"put x=1".to_vec(),
                },
                LogEntry {
                    term: 5,
                    index: 101,
                    data: b"put y=2".to_vec(),
                },
            ],
            leader_commit: 98,
            group_id: 7,
        };

        let rpc = RaftRpc::AppendEntriesRequest(req.clone());
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::AppendEntriesRequest(d) => {
                assert_eq!(d.term, req.term);
                assert_eq!(d.leader_id, req.leader_id);
                assert_eq!(d.prev_log_index, req.prev_log_index);
                assert_eq!(d.prev_log_term, req.prev_log_term);
                assert_eq!(d.entries.len(), 2);
                assert_eq!(d.entries[0].data, b"put x=1");
                assert_eq!(d.entries[1].data, b"put y=2");
                assert_eq!(d.leader_commit, req.leader_commit);
                assert_eq!(d.group_id, req.group_id);
            }
            other => panic!("expected AppendEntriesRequest, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_append_entries_heartbeat() {
        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 1,
            prev_log_index: 10,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 8,
            group_id: 0,
        };

        let rpc = RaftRpc::AppendEntriesRequest(req);
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::AppendEntriesRequest(d) => {
                assert!(d.entries.is_empty());
                assert_eq!(d.term, 3);
            }
            other => panic!("expected heartbeat, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_append_entries_response() {
        let resp = AppendEntriesResponse {
            term: 5,
            success: true,
            last_log_index: 100,
        };

        let rpc = RaftRpc::AppendEntriesResponse(resp);
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::AppendEntriesResponse(d) => {
                assert_eq!(d.term, 5);
                assert!(d.success);
                assert_eq!(d.last_log_index, 100);
            }
            other => panic!("expected AppendEntriesResponse, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_request_vote_request() {
        let req = RequestVoteRequest {
            term: 10,
            candidate_id: 3,
            last_log_index: 200,
            last_log_term: 9,
            group_id: 42,
        };

        let rpc = RaftRpc::RequestVoteRequest(req);
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::RequestVoteRequest(d) => {
                assert_eq!(d.term, 10);
                assert_eq!(d.candidate_id, 3);
                assert_eq!(d.last_log_index, 200);
                assert_eq!(d.last_log_term, 9);
                assert_eq!(d.group_id, 42);
            }
            other => panic!("expected RequestVoteRequest, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_request_vote_response() {
        let resp = RequestVoteResponse {
            term: 10,
            vote_granted: true,
        };

        let rpc = RaftRpc::RequestVoteResponse(resp);
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::RequestVoteResponse(d) => {
                assert_eq!(d.term, 10);
                assert!(d.vote_granted);
            }
            other => panic!("expected RequestVoteResponse, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_install_snapshot_request() {
        let data: Vec<u8> = [0xDE, 0xAD, 0xBE, 0xEF]
            .iter()
            .copied()
            .cycle()
            .take(1024)
            .collect();
        let req = InstallSnapshotRequest {
            term: 7,
            leader_id: 1,
            last_included_index: 500,
            last_included_term: 6,
            offset: 0,
            data: data.clone(),
            done: false,
            group_id: 3,
        };

        let rpc = RaftRpc::InstallSnapshotRequest(req);
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::InstallSnapshotRequest(d) => {
                assert_eq!(d.term, 7);
                assert_eq!(d.leader_id, 1);
                assert_eq!(d.last_included_index, 500);
                assert_eq!(d.last_included_term, 6);
                assert_eq!(d.offset, 0);
                assert_eq!(d.data, data);
                assert!(!d.done);
                assert_eq!(d.group_id, 3);
            }
            other => panic!("expected InstallSnapshotRequest, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_install_snapshot_final_chunk() {
        let req = InstallSnapshotRequest {
            term: 7,
            leader_id: 1,
            last_included_index: 500,
            last_included_term: 6,
            offset: 4096,
            data: vec![0xFF; 128],
            done: true,
            group_id: 3,
        };

        let rpc = RaftRpc::InstallSnapshotRequest(req);
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::InstallSnapshotRequest(d) => {
                assert!(d.done);
                assert_eq!(d.offset, 4096);
            }
            other => panic!("expected InstallSnapshotRequest, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_install_snapshot_response() {
        let resp = InstallSnapshotResponse { term: 7 };

        let rpc = RaftRpc::InstallSnapshotResponse(resp);
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::InstallSnapshotResponse(d) => {
                assert_eq!(d.term, 7);
            }
            other => panic!("expected InstallSnapshotResponse, got {other:?}"),
        }
    }

    #[test]
    fn crc_corruption_detected() {
        let rpc = RaftRpc::RequestVoteResponse(RequestVoteResponse {
            term: 1,
            vote_granted: false,
        });
        let mut encoded = encode(&rpc).unwrap();

        // Flip a bit in the payload.
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

        // Set version to 99.
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

        // Set rpc_type to 255.
        encoded[1] = 255;

        // CRC will mismatch because we didn't change payload — but the rpc_type
        // byte is in the header, not covered by CRC. The decode will fail on
        // unknown rpc_type after CRC passes. Actually, CRC only covers payload,
        // so the type corruption is caught by the type discriminant check.
        // However, the CRC is still valid (payload unchanged), so we get the
        // unknown type error.
        let err = decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("unknown rpc_type"), "{err}");
    }

    #[test]
    fn payload_too_large_rejected() {
        // Craft a header claiming a massive payload.
        let mut frame = vec![0u8; HEADER_SIZE];
        frame[0] = WIRE_VERSION;
        frame[1] = RPC_APPEND_ENTRIES_REQ;
        let huge: u32 = MAX_PAYLOAD_SIZE + 1;
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

    #[test]
    fn large_snapshot_roundtrip() {
        // 1 MiB snapshot chunk.
        let data = vec![0xAB; 1024 * 1024];
        let req = InstallSnapshotRequest {
            term: 100,
            leader_id: 5,
            last_included_index: 999_999,
            last_included_term: 99,
            offset: 0,
            data: data.clone(),
            done: false,
            group_id: 0,
        };

        let rpc = RaftRpc::InstallSnapshotRequest(req);
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::InstallSnapshotRequest(d) => {
                assert_eq!(d.data.len(), 1024 * 1024);
                assert_eq!(d.data, data);
            }
            other => panic!("expected InstallSnapshotRequest, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_join_request() {
        let req = JoinRequest {
            node_id: 42,
            listen_addr: "10.0.0.5:9400".into(),
        };

        let rpc = RaftRpc::JoinRequest(req);
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::JoinRequest(d) => {
                assert_eq!(d.node_id, 42);
                assert_eq!(d.listen_addr, "10.0.0.5:9400");
            }
            other => panic!("expected JoinRequest, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_join_response() {
        let resp = JoinResponse {
            success: true,
            error: String::new(),
            nodes: vec![
                JoinNodeInfo {
                    node_id: 1,
                    addr: "10.0.0.1:9400".into(),
                    state: 1,
                    raft_groups: vec![0, 1],
                },
                JoinNodeInfo {
                    node_id: 2,
                    addr: "10.0.0.2:9400".into(),
                    state: 1,
                    raft_groups: vec![0, 1],
                },
            ],
            vshard_to_group: (0..1024u64).map(|i| i % 4).collect(),
            groups: vec![JoinGroupInfo {
                group_id: 0,
                leader: 1,
                members: vec![1, 2],
            }],
        };

        let rpc = RaftRpc::JoinResponse(resp);
        let encoded = encode(&rpc).unwrap();
        let decoded = decode(&encoded).unwrap();

        match decoded {
            RaftRpc::JoinResponse(d) => {
                assert!(d.success);
                assert_eq!(d.nodes.len(), 2);
                assert_eq!(d.vshard_to_group.len(), 1024);
                assert_eq!(d.groups.len(), 1);
                assert_eq!(d.groups[0].leader, 1);
            }
            other => panic!("expected JoinResponse, got {other:?}"),
        }
    }
}
