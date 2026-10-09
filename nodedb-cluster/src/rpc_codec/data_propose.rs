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
    /// The data group that owns `vshard_id`. The bytes are a serialized
    /// `ReplicatedEntry`.
    ///
    /// `deadline_remaining_ms` is what remains of the proposer's deadline
    /// when it sends the request. The leader's write gate stops waiting for
    /// the entry's keys once this budget runs out. `0` means the deadline
    /// passed, and the leader refuses the entry unproposed.
    VShard {
        vshard_id: u32,
        deadline_remaining_ms: u64,
    },
    /// The Calvin sequencer group. The bytes are a msgpack-encoded
    /// `SequencerEntry`.
    Sequencer,
}

/// The budget a forward carries for `deadline`: the milliseconds left now.
/// `0` when the deadline passed.
pub fn remaining_budget_ms(deadline: tokio::time::Instant) -> u64 {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX)
}

/// The leader's deadline for a forward that carries `deadline_remaining_ms`.
/// `None` when the budget is spent.
///
/// The budget is used as it arrived. The proposer resolved it once, from the
/// statement's own timeout or its node default. Re-deciding it against the
/// leader's default gives one statement two budgets.
pub fn forwarded_deadline(deadline_remaining_ms: u64) -> Option<tokio::time::Instant> {
    match deadline_remaining_ms {
        0 => None,
        ms => Some(tokio::time::Instant::now() + std::time::Duration::from_millis(ms)),
    }
}

/// Forward an opaque proposal payload to the leader of its target group.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct DataProposeRequest {
    pub target: ProposeTarget,
    pub bytes: Vec<u8>,
}

/// Why a leader refused a forwarded proposal, typed so the forwarding node
/// can tell a transient refusal from a final one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum ForwardedProposeRefusal {
    /// The node does not lead the target group. `leader_hint` names the
    /// leader it knows, if any, at `leader_term`.
    NotLeader,
    /// A leadership transfer of the target group is in flight.
    LeadershipTransferInProgress,
    /// The leader's write gate routes the write through the Calvin
    /// sequencer.
    RouteToSequencer,
    /// The proposer's deadline passed before the leader's write gate
    /// admitted the write. The leader did not propose it.
    AdmissionTimedOut,
    /// Any other failure; `error_message` describes it.
    Failed,
}

/// Response to a forwarded data-group proposal.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct DataProposeResponse {
    pub success: bool,
    pub group_id: u64,
    pub log_index: u64,
    pub leader_hint: Option<u64>,
    /// The refusing node's term, which `leader_hint` is known at. `0` on
    /// success and on a refusal that is not `NotLeader`.
    pub leader_term: u64,
    /// The typed reason of a refusal. `None` on success.
    pub refusal: Option<ForwardedProposeRefusal>,
    pub error_message: String,
}

impl DataProposeResponse {
    pub fn ok(group_id: u64, log_index: u64) -> Self {
        Self {
            success: true,
            group_id,
            log_index,
            leader_hint: None,
            leader_term: 0,
            refusal: None,
            error_message: String::new(),
        }
    }

    /// The response for a proposal the leader refused with `error`.
    pub fn refused(error: &ClusterError) -> Self {
        let (refusal, leader_hint, leader_term) = match error {
            ClusterError::Raft(nodedb_raft::RaftError::NotLeader { leader_hint, term }) => {
                (ForwardedProposeRefusal::NotLeader, *leader_hint, *term)
            }
            ClusterError::Raft(nodedb_raft::RaftError::LeadershipTransferInProgress) => (
                ForwardedProposeRefusal::LeadershipTransferInProgress,
                None,
                0,
            ),
            ClusterError::Calvin(crate::error::CalvinError::RouteToSequencer) => {
                (ForwardedProposeRefusal::RouteToSequencer, None, 0)
            }
            ClusterError::Calvin(crate::error::CalvinError::AdmissionTimedOut) => {
                (ForwardedProposeRefusal::AdmissionTimedOut, None, 0)
            }
            // A refusal with no retry contract. The forwarding node reads it
            // as a transport error carrying the leader's message.
            ClusterError::Raft(
                nodedb_raft::RaftError::LogCompacted { .. }
                | nodedb_raft::RaftError::CompactionAheadOfApplied { .. }
                | nodedb_raft::RaftError::ProposalRejected { .. }
                | nodedb_raft::RaftError::InvalidTransferTarget { .. }
                | nodedb_raft::RaftError::GroupNotFound { .. }
                | nodedb_raft::RaftError::Transport { .. }
                | nodedb_raft::RaftError::Storage { .. }
                | nodedb_raft::RaftError::Serialization { .. }
                | nodedb_raft::RaftError::SnapshotFormat { .. }
                | nodedb_raft::RaftError::Shutdown,
            )
            | ClusterError::VShardNotMapped { .. }
            | ClusterError::GroupNotFound { .. }
            | ClusterError::LearnerNotCaughtUp { .. }
            | ClusterError::MigrationInProgress { .. }
            | ClusterError::MigrationPauseBudgetExceeded { .. }
            | ClusterError::NodeUnreachable { .. }
            | ClusterError::GhostNotFound { .. }
            | ClusterError::Transport { .. }
            | ClusterError::ShardTimeout { .. }
            | ClusterError::Unanswered { .. }
            | ClusterError::StreamTerminal { .. }
            | ClusterError::Storage { .. }
            | ClusterError::DataPlane { .. }
            | ClusterError::Codec { .. }
            | ClusterError::UnsupportedWireVersion { .. }
            | ClusterError::CircuitOpen { .. }
            | ClusterError::JoinGroupDisappeared { .. }
            | ClusterError::JoinCommitTimeout { .. }
            | ClusterError::ReadIndexNotLeader { .. }
            | ClusterError::ReadIndexTimeout { .. }
            | ClusterError::Config { .. }
            | ClusterError::MigrationCheckpoint(_)
            | ClusterError::MigrationRecovery(_)
            | ClusterError::WrongOwner { .. }
            | ClusterError::Calvin(_)
            | ClusterError::SnapshotCrcMismatch { .. }
            | ClusterError::SnapshotOffsetRegression { .. }
            | ClusterError::PartialSnapshotCorrupt { .. }
            | ClusterError::PartialSnapshotCleanupFailed { .. }
            | ClusterError::SnapshotApplyFailed { .. }
            | ClusterError::Mirror(_)
            | ClusterError::BspBarrier(_)
            | ClusterError::VectorGather(_)
            | ClusterError::SpatialGather(_)
            | ClusterError::Bm25Gather(_)
            | ClusterError::TsGather(_)
            | ClusterError::ShufflePush(_)
            | ClusterError::RemoteUntyped { .. }
            | ClusterError::ShardExecution { .. } => (ForwardedProposeRefusal::Failed, None, 0),
        };
        Self {
            success: false,
            group_id: 0,
            log_index: 0,
            leader_hint,
            leader_term,
            refusal: Some(refusal),
            error_message: error.to_string(),
        }
    }

    /// The typed error a refused response stands for on the forwarding node.
    /// A refusal the leader typed keeps its Raft error. Any other becomes
    /// `ClusterError::RemoteUntyped` with the leader's message. The leader
    /// answered, so it is never a link failure.
    pub fn refusal_error(&self) -> ClusterError {
        match self.refusal {
            Some(ForwardedProposeRefusal::NotLeader) => {
                ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                    leader_hint: self.leader_hint,
                    term: self.leader_term,
                })
            }
            Some(ForwardedProposeRefusal::LeadershipTransferInProgress) => {
                ClusterError::Raft(nodedb_raft::RaftError::LeadershipTransferInProgress)
            }
            Some(ForwardedProposeRefusal::RouteToSequencer) => {
                ClusterError::Calvin(crate::error::CalvinError::RouteToSequencer)
            }
            Some(ForwardedProposeRefusal::AdmissionTimedOut) => {
                ClusterError::Calvin(crate::error::CalvinError::AdmissionTimedOut)
            }
            Some(ForwardedProposeRefusal::Failed) | None => ClusterError::RemoteUntyped {
                detail: format!("data propose forward refused: {}", self.error_message),
            },
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

    /// The vShard target keeps the proposer's remaining budget.
    #[test]
    fn vshard_target_keeps_its_deadline_across_the_wire() {
        let target = ProposeTarget::VShard {
            vshard_id: 42,
            deadline_remaining_ms: 1_500,
        };
        let req = roundtrip(target);
        assert_eq!(req.target, target);
    }

    /// A spent budget gives the leader no deadline, so it proposes nothing.
    #[test]
    fn a_spent_budget_has_no_leader_deadline() {
        assert_eq!(forwarded_deadline(0), None);
    }

    /// The leader's deadline ends no later than the budget the forward
    /// carries.
    #[test]
    fn the_leader_deadline_ends_within_the_forwarded_budget() {
        let budget = std::time::Duration::from_millis(50);
        let latest = tokio::time::Instant::now() + budget;
        let deadline = forwarded_deadline(50).expect("a live budget");
        let received = tokio::time::Instant::now() + budget;
        assert!(deadline <= received);
        assert!(deadline >= latest);
    }

    /// A passed deadline leaves no budget to forward.
    #[test]
    fn a_passed_deadline_leaves_no_budget() {
        assert_eq!(remaining_budget_ms(tokio::time::Instant::now()), 0);
        let live = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        assert!(remaining_budget_ms(live) > 9_000);
    }

    fn refusal_across_the_wire(error: ClusterError) -> ClusterError {
        let rpc = RaftRpc::DataProposeResponse(DataProposeResponse::refused(&error));
        let epoch = ClusterEpochState::default();
        let encoded = encode(&rpc, &epoch).expect("encode");
        match decode(&encoded, &epoch).expect("decode") {
            RaftRpc::DataProposeResponse(resp) => {
                assert!(!resp.success);
                resp.refusal_error()
            }
            other => panic!("decoded the wrong variant: {other:?}"),
        }
    }

    #[test]
    fn a_transfer_in_progress_keeps_its_raft_error_across_the_wire() {
        let error = refusal_across_the_wire(ClusterError::Raft(
            nodedb_raft::RaftError::LeadershipTransferInProgress,
        ));
        assert!(matches!(
            error,
            ClusterError::Raft(nodedb_raft::RaftError::LeadershipTransferInProgress)
        ));
    }

    #[test]
    fn a_not_leader_keeps_its_hint_across_the_wire() {
        let error =
            refusal_across_the_wire(ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                leader_hint: Some(3),
                term: 9,
            }));
        assert!(matches!(
            error,
            ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                leader_hint: Some(3),
                term: 9,
            })
        ));
    }

    /// A write-gate refusal keeps its Calvin error across the wire, so the
    /// forwarding node routes the write or reports the deadline.
    #[test]
    fn a_write_gate_refusal_keeps_its_calvin_error_across_the_wire() {
        let routed = refusal_across_the_wire(ClusterError::Calvin(
            crate::error::CalvinError::RouteToSequencer,
        ));
        assert!(matches!(
            routed,
            ClusterError::Calvin(crate::error::CalvinError::RouteToSequencer)
        ));
        let timed_out = refusal_across_the_wire(ClusterError::Calvin(
            crate::error::CalvinError::AdmissionTimedOut,
        ));
        assert!(matches!(
            timed_out,
            ClusterError::Calvin(crate::error::CalvinError::AdmissionTimedOut)
        ));
    }

    #[test]
    fn any_other_refusal_is_an_untyped_answer() {
        let error = refusal_across_the_wire(ClusterError::VShardNotMapped { vshard_id: 7 });
        assert!(matches!(error, ClusterError::RemoteUntyped { .. }));
        assert!(!error.is_link_failure());
    }
}
