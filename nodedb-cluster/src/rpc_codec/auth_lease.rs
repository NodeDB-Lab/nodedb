// SPDX-License-Identifier: BUSL-1.1

//! Authorization lease wire types and codecs.
//!
//! Every node plans permission-checked statements from its local
//! authorization state only while it holds a lease from the metadata group
//! leader. A node renews its lease with a coverage report: for each Raft group
//! it names, the index through which its local authorization state holds
//! every change.
//!
//! A writer acknowledges an authorization change only after a barrier on the
//! metadata leader releases it. The barrier releases once every node holding
//! an unexpired lease reported coverage of the change, or its lease expired.

use super::discriminants::*;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::{ClusterError, Result};

/// A holder's claim on one Raft group: its local authorization state holds
/// every change of `group_id` at or below `through`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct GroupCoverage {
    pub group_id: u64,
    pub through: u64,
}

/// Renew the sender's authorization lease.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct AuthLeaseRenewRequest {
    pub node_id: u64,
    /// Coverage of every group the sender knows. A group it does not
    /// replicate is reported at `u64::MAX`: the sender never plans against it.
    pub coverage: Vec<GroupCoverage>,
}

/// The leader's answer to an [`AuthLeaseRenewRequest`].
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum AuthLeaseRenewOutcome {
    /// The lease runs `lease_ms` from the moment the sender sent the request.
    Granted { lease_ms: u64 },
    /// The report does not cover every acknowledged change. The sender's
    /// lease is not extended.
    Withheld,
    /// The receiver does not lead the metadata group.
    NotLeader { leader_hint: Option<u64> },
}

/// Response to an [`AuthLeaseRenewRequest`].
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct AuthLeaseRenewResponse {
    pub outcome: AuthLeaseRenewOutcome,
}

/// Hold the sender's acknowledgement until every lease holder covers
/// `targets`, or its lease expired.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct AuthBarrierRequest {
    pub targets: Vec<GroupCoverage>,
    /// How long the leader may hold the request.
    pub timeout_ms: u64,
}

/// The leader's answer to an [`AuthBarrierRequest`].
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum AuthBarrierOutcome {
    /// No node can plan against state older than the targets.
    Released,
    /// The receiver does not lead the metadata group.
    NotLeader { leader_hint: Option<u64> },
    /// The barrier did not release in time.
    Timeout { waited_ms: u64 },
}

/// Response to an [`AuthBarrierRequest`].
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct AuthBarrierResponse {
    pub outcome: AuthBarrierOutcome,
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

pub(super) fn encode_renew_req(msg: &AuthLeaseRenewRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_AUTH_LEASE_RENEW_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_renew_resp(msg: &AuthLeaseRenewResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_AUTH_LEASE_RENEW_RESP, &to_bytes!(msg)?, out)
}
pub(super) fn encode_barrier_req(msg: &AuthBarrierRequest, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_AUTH_BARRIER_REQ, &to_bytes!(msg)?, out)
}
pub(super) fn encode_barrier_resp(msg: &AuthBarrierResponse, out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_AUTH_BARRIER_RESP, &to_bytes!(msg)?, out)
}

pub(super) fn decode_renew_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::AuthLeaseRenewRequest(from_bytes!(
        payload,
        AuthLeaseRenewRequest,
        "AuthLeaseRenewRequest"
    )?))
}
pub(super) fn decode_renew_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::AuthLeaseRenewResponse(from_bytes!(
        payload,
        AuthLeaseRenewResponse,
        "AuthLeaseRenewResponse"
    )?))
}
pub(super) fn decode_barrier_req(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::AuthBarrierRequest(from_bytes!(
        payload,
        AuthBarrierRequest,
        "AuthBarrierRequest"
    )?))
}
pub(super) fn decode_barrier_resp(payload: &[u8]) -> Result<RaftRpc> {
    Ok(RaftRpc::AuthBarrierResponse(from_bytes!(
        payload,
        AuthBarrierResponse,
        "AuthBarrierResponse"
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

    fn coverage() -> Vec<GroupCoverage> {
        vec![
            GroupCoverage {
                group_id: 0,
                through: 41,
            },
            GroupCoverage {
                group_id: 3,
                through: u64::MAX,
            },
        ]
    }

    #[test]
    fn a_renewal_survives_the_wire() {
        match roundtrip(RaftRpc::AuthLeaseRenewRequest(AuthLeaseRenewRequest {
            node_id: 2,
            coverage: coverage(),
        })) {
            RaftRpc::AuthLeaseRenewRequest(req) => {
                assert_eq!(req.node_id, 2);
                assert_eq!(req.coverage, coverage());
            }
            other => panic!("decoded the wrong variant: {other:?}"),
        }
        for outcome in [
            AuthLeaseRenewOutcome::Granted { lease_ms: 150 },
            AuthLeaseRenewOutcome::Withheld,
            AuthLeaseRenewOutcome::NotLeader {
                leader_hint: Some(1),
            },
        ] {
            match roundtrip(RaftRpc::AuthLeaseRenewResponse(AuthLeaseRenewResponse {
                outcome: outcome.clone(),
            })) {
                RaftRpc::AuthLeaseRenewResponse(resp) => assert_eq!(resp.outcome, outcome),
                other => panic!("decoded the wrong variant: {other:?}"),
            }
        }
    }

    #[test]
    fn a_barrier_survives_the_wire() {
        match roundtrip(RaftRpc::AuthBarrierRequest(AuthBarrierRequest {
            targets: coverage(),
            timeout_ms: 5000,
        })) {
            RaftRpc::AuthBarrierRequest(req) => {
                assert_eq!(req.targets, coverage());
                assert_eq!(req.timeout_ms, 5000);
            }
            other => panic!("decoded the wrong variant: {other:?}"),
        }
        for outcome in [
            AuthBarrierOutcome::Released,
            AuthBarrierOutcome::NotLeader { leader_hint: None },
            AuthBarrierOutcome::Timeout { waited_ms: 5000 },
        ] {
            match roundtrip(RaftRpc::AuthBarrierResponse(AuthBarrierResponse {
                outcome: outcome.clone(),
            })) {
                RaftRpc::AuthBarrierResponse(resp) => assert_eq!(resp.outcome, outcome),
                other => panic!("decoded the wrong variant: {other:?}"),
            }
        }
    }
}
