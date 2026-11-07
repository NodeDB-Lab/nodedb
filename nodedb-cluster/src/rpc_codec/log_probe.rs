// SPDX-License-Identifier: BUSL-1.1

//! LogProbeRequest / LogProbeResponse wire types and codecs.
//!
//! A node that proposed entries it does not apply yet asks a replica whether
//! they committed. No apply can answer: a mirror's entry holds its group's lane
//! until a later metadata entry, which the proposer sends only after this
//! answer. Any replica can answer, because a committed index holds one entry on
//! every replica.
//!
//! The probe names each entry by its bytes' digest. A leader change can place
//! another entry at the same index, and a digest tells the two apart whatever
//! term either one landed in.

use sha2::{Digest, Sha256};

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// The most entries one probe names. A caller with more sends more probes.
pub const MAX_LOG_PROBE_ITEMS: usize = 1024;

/// The digest a probe names an entry by.
pub fn log_entry_digest(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

/// One entry a probe asks about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LogProbeItem {
    pub index: u64,
    pub digest: [u8; 32],
}

/// Ask a replica of `group_id` what it committed.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LogProbeRequest {
    pub group_id: u64,
    /// The entries to compare. Empty asks only for the replica's progress.
    pub items: Vec<LogProbeItem>,
}

/// One replica's answer.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LogProbeResponse {
    /// Whether the replier hosts a replica of the group. The other fields are
    /// `0` and `None` when it does not.
    pub hosted: bool,
    pub commit_index: u64,
    /// The highest index whose apply is durable on the replier.
    pub durable_applied: u64,
    /// The lowest asked index at or below `commit_index` that does not hold the
    /// asked bytes. An asked index this replica compacted counts: it cannot
    /// vouch for it.
    pub mismatch: Option<u64>,
}

impl LogProbeResponse {
    /// The answer of a node with no replica of the group.
    pub fn not_hosted() -> Self {
        Self {
            hosted: false,
            commit_index: 0,
            durable_applied: 0,
            mismatch: None,
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

pub(super) fn encode_log_probe_req(msg: &LogProbeRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_LOG_PROBE_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_log_probe_resp(msg: &LogProbeResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_LOG_PROBE_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_log_probe_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::LogProbeRequest(from_bytes!(
        payload,
        LogProbeRequest,
        "LogProbeRequest"
    )?))
}
pub(super) fn decode_log_probe_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::LogProbeResponse(from_bytes!(
        payload,
        LogProbeResponse,
        "LogProbeResponse"
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_epoch::ClusterEpochState;
    use crate::rpc_codec::{decode, encode};

    #[test]
    fn a_probe_and_its_answer_roundtrip() {
        let epoch = ClusterEpochState::default();
        let request = LogProbeRequest {
            group_id: 4,
            items: vec![LogProbeItem {
                index: 9,
                digest: log_entry_digest(b"entry"),
            }],
        };
        let bytes = encode(&RaftRpc::LogProbeRequest(request.clone()), &epoch).expect("encode");
        match decode(&bytes, &epoch).expect("decode") {
            RaftRpc::LogProbeRequest(decoded) => assert_eq!(decoded, request),
            other => panic!("expected a log probe request, got {other:?}"),
        }
        let response = LogProbeResponse {
            hosted: true,
            commit_index: 12,
            durable_applied: 3,
            mismatch: Some(9),
        };
        let bytes = encode(&RaftRpc::LogProbeResponse(response.clone()), &epoch).expect("encode");
        match decode(&bytes, &epoch).expect("decode") {
            RaftRpc::LogProbeResponse(decoded) => assert_eq!(decoded, response),
            other => panic!("expected a log probe response, got {other:?}"),
        }
    }

    #[test]
    fn a_digest_tells_two_entries_apart() {
        assert_eq!(log_entry_digest(b"a"), log_entry_digest(b"a"));
        assert_ne!(log_entry_digest(b"a"), log_entry_digest(b"b"));
    }
}
