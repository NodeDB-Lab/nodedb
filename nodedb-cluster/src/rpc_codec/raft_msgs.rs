//! Raft consensus wire types and codecs.

use nodedb_raft::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

macro_rules! rkyv_to_bytes {
    ($msg:expr) => {
        rkyv::to_bytes::<rkyv::rancor::Error>($msg)
            .map(|b| b.to_vec())
            .map_err(|e| ClusterError::Codec {
                detail: format!("rkyv serialize: {e}"),
            })
    };
}

macro_rules! rkyv_from_bytes {
    ($payload:expr, $T:ty, $name:expr) => {{
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity($payload.len());
        aligned.extend_from_slice($payload);
        rkyv::from_bytes::<$T, rkyv::rancor::Error>(&aligned).map_err(|e| ClusterError::Codec {
            detail: format!("rkyv deserialize {}: {e}", $name),
        })
    }};
}

pub(super) fn encode_append_entries_req(
    msg: &AppendEntriesRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_APPEND_ENTRIES_REQ, &rkyv_to_bytes!(msg)?, out)
}
pub(super) fn encode_append_entries_resp(
    msg: &AppendEntriesResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_APPEND_ENTRIES_RESP, &rkyv_to_bytes!(msg)?, out)
}
pub(super) fn encode_request_vote_req(msg: &RequestVoteRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_REQUEST_VOTE_REQ, &rkyv_to_bytes!(msg)?, out)
}
pub(super) fn encode_request_vote_resp(msg: &RequestVoteResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_REQUEST_VOTE_RESP, &rkyv_to_bytes!(msg)?, out)
}
pub(super) fn encode_install_snapshot_req(
    msg: &InstallSnapshotRequest,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_INSTALL_SNAPSHOT_REQ, &rkyv_to_bytes!(msg)?, out)
}
pub(super) fn encode_install_snapshot_resp(
    msg: &InstallSnapshotResponse,
    out: &mut Vec<u8>,
) -> Result<()> {
    write_frame(RPC_INSTALL_SNAPSHOT_RESP, &rkyv_to_bytes!(msg)?, out)
}

pub(super) fn decode_append_entries_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::AppendEntriesRequest(rkyv_from_bytes!(
        payload,
        AppendEntriesRequest,
        "AppendEntriesRequest"
    )?))
}
pub(super) fn decode_append_entries_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::AppendEntriesResponse(rkyv_from_bytes!(
        payload,
        AppendEntriesResponse,
        "AppendEntriesResponse"
    )?))
}
pub(super) fn decode_request_vote_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::RequestVoteRequest(rkyv_from_bytes!(
        payload,
        RequestVoteRequest,
        "RequestVoteRequest"
    )?))
}
pub(super) fn decode_request_vote_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::RequestVoteResponse(rkyv_from_bytes!(
        payload,
        RequestVoteResponse,
        "RequestVoteResponse"
    )?))
}
pub(super) fn decode_install_snapshot_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::InstallSnapshotRequest(rkyv_from_bytes!(
        payload,
        InstallSnapshotRequest,
        "InstallSnapshotRequest"
    )?))
}
pub(super) fn decode_install_snapshot_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::InstallSnapshotResponse(rkyv_from_bytes!(
        payload,
        InstallSnapshotResponse,
        "InstallSnapshotResponse"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_raft::message::LogEntry;

    fn roundtrip(rpc: RaftRpc) -> RaftRpc {
        let encoded = super::super::encode(&rpc).unwrap();
        super::super::decode(&encoded).unwrap()
    }

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
        match roundtrip(RaftRpc::AppendEntriesRequest(req)) {
            RaftRpc::AppendEntriesRequest(d) => {
                assert_eq!(d.term, 5);
                assert_eq!(d.entries.len(), 2);
                assert_eq!(d.entries[0].data, b"put x=1");
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
        match roundtrip(RaftRpc::AppendEntriesRequest(req)) {
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
        match roundtrip(RaftRpc::AppendEntriesResponse(resp)) {
            RaftRpc::AppendEntriesResponse(d) => {
                assert_eq!(d.term, 5);
                assert!(d.success);
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
        match roundtrip(RaftRpc::RequestVoteRequest(req)) {
            RaftRpc::RequestVoteRequest(d) => {
                assert_eq!(d.term, 10);
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
        match roundtrip(RaftRpc::RequestVoteResponse(resp)) {
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
        match roundtrip(RaftRpc::InstallSnapshotRequest(req)) {
            RaftRpc::InstallSnapshotRequest(d) => {
                assert_eq!(d.term, 7);
                assert_eq!(d.data, data);
                assert!(!d.done);
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
        match roundtrip(RaftRpc::InstallSnapshotRequest(req)) {
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
        match roundtrip(RaftRpc::InstallSnapshotResponse(resp)) {
            RaftRpc::InstallSnapshotResponse(d) => assert_eq!(d.term, 7),
            other => panic!("expected InstallSnapshotResponse, got {other:?}"),
        }
    }

    #[test]
    fn large_snapshot_roundtrip() {
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
        match roundtrip(RaftRpc::InstallSnapshotRequest(req)) {
            RaftRpc::InstallSnapshotRequest(d) => {
                assert_eq!(d.data.len(), 1024 * 1024);
                assert_eq!(d.data, data);
            }
            other => panic!("expected InstallSnapshotRequest, got {other:?}"),
        }
    }
}
