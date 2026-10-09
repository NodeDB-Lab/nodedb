// SPDX-License-Identifier: BUSL-1.1

//! The error an async Raft propose returns to its statement.

use nodedb_cluster::{CalvinError, ClusterError};
use nodedb_raft::RaftError;

use crate::bridge::envelope::ErrorCode;
use crate::types::VShardId;

/// The error of a write whose deadline passed before any leader proposed it.
///
/// A definite refusal: nothing entered the log, so nothing applies. A client
/// sees the same deadline SQLSTATE as an unknown outcome. A caller that
/// matches the code can tell the two apart.
pub(super) fn expired_before_propose() -> crate::Error {
    crate::Error::DataPlane(ErrorCode::ExpiredBeforeExecution)
}

/// The error an async propose returns for a cluster error.
///
/// A group with no leader to take the proposal right now accepts the same
/// proposal once it has one, so the proposal is retried:
/// [`crate::Error::NoLeader`]. That covers an election, a leadership transfer
/// in flight, a leader that stepped down after this node or a forwarding node
/// chose it, a vShard whose owner is moving, and a leader this node cannot
/// reach. A forwarded refusal arrives here with its typed Raft error
/// (`DataProposeResponse::refusal_error`). A typed verdict keeps its class.
/// Every other failure is final here.
pub(super) fn async_propose_error(vshard_id: u32, error: ClusterError) -> crate::Error {
    match error {
        ClusterError::Raft(
            RaftError::LeadershipTransferInProgress | RaftError::NotLeader { .. },
        )
        | ClusterError::ReadIndexNotLeader { .. }
        | ClusterError::MigrationInProgress { .. }
        | ClusterError::WrongOwner { .. }
        // The forward never reached the leader whole: the leader is not in
        // this node's topology, or the connect, stream open or write failed.
        // Nothing was proposed. A written forward ends as `Unanswered`.
        | ClusterError::Transport { .. } => crate::Error::NoLeader {
            vshard_id: VShardId::new(vshard_id),
        },
        // The forward did not answer before its timeout. The leader can have
        // proposed the write, so its outcome is unknown: the statement's
        // deadline class, the same class the array fan-out gives it. A
        // forward whose stream failed after the request was written can
        // also have been proposed, so it takes the same class.
        ClusterError::ShardTimeout { .. } | ClusterError::Unanswered { .. } => {
            crate::Error::DeadlineExceeded {
                request_id: crate::types::RequestId::new(0),
            }
        }
        ClusterError::DataPlane { code } => crate::Error::DataPlane(code.into()),
        // The leader's write gate waited for the write's lock keys until the
        // caller's deadline, and proposed nothing.
        ClusterError::Calvin(CalvinError::AdmissionTimedOut) => expired_before_propose(),
        ClusterError::ShardExecution { error, .. } | ClusterError::StreamTerminal { error, .. } => {
            crate::Error::from(*error)
        }
        other @ (ClusterError::Raft(
            RaftError::LogCompacted { .. }
            | RaftError::CompactionAheadOfApplied { .. }
            | RaftError::ProposalRejected { .. }
            | RaftError::InvalidTransferTarget { .. }
            | RaftError::GroupNotFound { .. }
            | RaftError::Transport { .. }
            | RaftError::Storage { .. }
            | RaftError::Serialization { .. }
            | RaftError::SnapshotFormat { .. }
            | RaftError::Shutdown,
        )
        | ClusterError::VShardNotMapped { .. }
        | ClusterError::GroupNotFound { .. }
        | ClusterError::LearnerNotCaughtUp { .. }
        | ClusterError::MigrationPauseBudgetExceeded { .. }
        | ClusterError::NodeUnreachable { .. }
        | ClusterError::GhostNotFound { .. }
        | ClusterError::Storage { .. }
        | ClusterError::Codec { .. }
        | ClusterError::UnsupportedWireVersion { .. }
        | ClusterError::CircuitOpen { .. }
        | ClusterError::JoinGroupDisappeared { .. }
        | ClusterError::JoinCommitTimeout { .. }
        | ClusterError::ReadIndexTimeout { .. }
        | ClusterError::Config { .. }
        | ClusterError::MigrationCheckpoint(_)
        | ClusterError::MigrationRecovery(_)
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
        | ClusterError::RemoteUntyped { .. }) => crate::Error::Internal {
            detail: format!("raft propose (async): {other}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_leader_is_retryable() {
        let error = ClusterError::Raft(RaftError::NotLeader {
            leader_hint: None,
            term: 1,
        });
        assert!(matches!(
            async_propose_error(3, error),
            crate::Error::NoLeader { .. }
        ));
    }

    /// A moving vShard has no owner to take the proposal until the
    /// cut-over, so the statement answers the retryable no-leader class.
    #[test]
    fn a_moving_vshard_is_retryable() {
        let error = ClusterError::WrongOwner {
            vshard_id: 3,
            expected_owner_node: None,
        };
        assert!(matches!(
            async_propose_error(3, error),
            crate::Error::NoLeader { .. }
        ));
    }

    /// An admission timeout proposed nothing: a definite refusal, never an
    /// unknown outcome.
    #[test]
    fn an_admission_timeout_is_a_definite_refusal() {
        let error = async_propose_error(3, ClusterError::Calvin(CalvinError::AdmissionTimedOut));
        let crate::Error::DataPlane(code) = &error else {
            panic!("expected a Data-Plane verdict, got {error:?}");
        };
        assert_eq!(code, &ErrorCode::ExpiredBeforeExecution);
        assert!(crate::control::server::dispatch_utils::write_definitely_not_applied(code));
    }

    /// A forward with no reply can have been proposed. Its timeout stays an
    /// unknown outcome, never a definite refusal.
    #[test]
    fn a_forward_timeout_is_an_unknown_outcome() {
        let error = async_propose_error(
            3,
            ClusterError::ShardTimeout {
                vshard_id: 3,
                elapsed_ms: 50,
            },
        );
        assert!(matches!(error, crate::Error::DeadlineExceeded { .. }));
        assert!(
            !crate::control::server::dispatch_utils::write_definitely_not_applied(
                &ErrorCode::from(error)
            )
        );
    }

    /// A forward whose stream failed after the request was written can have
    /// been proposed. It is an unknown outcome, never an internal error.
    #[test]
    fn a_sent_forward_with_no_answer_is_an_unknown_outcome() {
        let error = async_propose_error(
            3,
            ClusterError::Unanswered {
                node_id: 2,
                detail: "connection lost".into(),
            },
        );
        assert!(matches!(error, crate::Error::DeadlineExceeded { .. }));
        assert!(
            !crate::control::server::dispatch_utils::write_definitely_not_applied(
                &ErrorCode::from(error)
            )
        );
    }

    /// A forward that never went out proposed nothing. The proposal is
    /// retried, never reported as an unknown outcome.
    #[test]
    fn an_unsent_forward_is_retried() {
        let error = async_propose_error(
            3,
            ClusterError::Transport {
                detail: "connect to node 2 refused".into(),
            },
        );
        assert!(matches!(error, crate::Error::NoLeader { .. }));
    }
}
