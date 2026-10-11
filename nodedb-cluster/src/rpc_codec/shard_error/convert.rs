// SPDX-License-Identifier: BUSL-1.1

//! Conversion between `ClusterError` and its wire mirror.
//!
//! Both matches are exhaustive with no catch-all, so a new `ClusterError`
//! variant fails to compile here until it has a wire form.

use super::wire::ShardErrorWire;
use crate::error::ClusterError;

impl From<ClusterError> for ShardErrorWire {
    fn from(error: ClusterError) -> Self {
        match error {
            ClusterError::Raft(error) => Self::Raft {
                error: error.into(),
            },
            ClusterError::VShardNotMapped { vshard_id } => Self::VShardNotMapped { vshard_id },
            ClusterError::GroupNotFound { group_id } => Self::GroupNotFound { group_id },
            ClusterError::LearnerNotCaughtUp {
                group_id,
                node_id,
                match_index,
                commit_index,
            } => Self::LearnerNotCaughtUp {
                group_id,
                node_id,
                match_index,
                commit_index,
            },
            ClusterError::MigrationInProgress { vshard_id } => {
                Self::MigrationInProgress { vshard_id }
            }
            ClusterError::MigrationPauseBudgetExceeded {
                estimated_us,
                budget_us,
            } => Self::MigrationPauseBudgetExceeded {
                estimated_us,
                budget_us,
            },
            ClusterError::NodeUnreachable { node_id } => Self::NodeUnreachable { node_id },
            ClusterError::GhostNotFound { node_id, shard_id } => {
                Self::GhostNotFound { node_id, shard_id }
            }
            ClusterError::Transport { detail } => Self::Transport { detail },
            ClusterError::Unanswered { node_id, detail } => Self::Unanswered { node_id, detail },
            ClusterError::ShardTimeout {
                vshard_id,
                elapsed_ms,
            } => Self::ShardTimeout {
                vshard_id,
                elapsed_ms,
            },
            ClusterError::StreamTerminal { error, detail } => Self::StreamTerminal {
                error: *error,
                detail,
            },
            ClusterError::Storage { detail } => Self::Storage { detail },
            ClusterError::DataPlane { code } => Self::DataPlane { code },
            ClusterError::Codec { detail } => Self::Codec { detail },
            ClusterError::UnsupportedWireVersion {
                got,
                supported_min,
                supported_max,
            } => Self::UnsupportedWireVersion {
                got,
                supported_min,
                supported_max,
            },
            ClusterError::CircuitOpen { node_id, failures } => {
                Self::CircuitOpen { node_id, failures }
            }
            ClusterError::JoinGroupDisappeared { group_id } => {
                Self::JoinGroupDisappeared { group_id }
            }
            ClusterError::JoinCommitTimeout {
                group_id,
                log_index,
            } => Self::JoinCommitTimeout {
                group_id,
                log_index,
            },
            ClusterError::ReadIndexNotLeader { group_id } => Self::ReadIndexNotLeader { group_id },
            ClusterError::ReadIndexTimeout {
                group_id,
                waited_ms,
            } => Self::ReadIndexTimeout {
                group_id,
                waited_ms,
            },
            ClusterError::Config { detail } => Self::Config { detail },
            ClusterError::WrongOwner {
                vshard_id,
                expected_owner_node,
            } => Self::WrongOwner {
                vshard_id,
                expected_owner_node,
            },
            ClusterError::SnapshotCrcMismatch {
                group_id,
                stored,
                computed,
            } => Self::SnapshotCrcMismatch {
                group_id,
                stored,
                computed,
            },
            ClusterError::SnapshotOffsetRegression {
                group_id,
                expected,
                actual,
            } => Self::SnapshotOffsetRegression {
                group_id,
                expected,
                actual,
            },
            ClusterError::PartialSnapshotCorrupt { group_id, detail } => {
                Self::PartialSnapshotCorrupt { group_id, detail }
            }
            ClusterError::PartialSnapshotCleanupFailed { group_id, detail } => {
                Self::PartialSnapshotCleanupFailed { group_id, detail }
            }
            ClusterError::SnapshotApplyFailed { group_id, detail } => {
                Self::SnapshotApplyFailed { group_id, detail }
            }
            ClusterError::RemoteUntyped { detail } => Self::Untyped { detail },
            ClusterError::ShardExecution { error, detail } => Self::ShardExecution {
                error: *error,
                detail,
            },
            // Coordinator-side error families. Their message crosses.
            other @ (ClusterError::MigrationCheckpoint(_)
            | ClusterError::MigrationRecovery(_)
            | ClusterError::Calvin(_)
            | ClusterError::Mirror(_)
            | ClusterError::BspBarrier(_)
            | ClusterError::VectorGather(_)
            | ClusterError::SpatialGather(_)
            | ClusterError::Bm25Gather(_)
            | ClusterError::TsGather(_)
            | ClusterError::ShufflePush(_)) => Self::Untyped {
                detail: other.to_string(),
            },
        }
    }
}

impl From<ShardErrorWire> for ClusterError {
    fn from(wire: ShardErrorWire) -> Self {
        match wire {
            ShardErrorWire::Raft { error } => Self::Raft(error.into()),
            ShardErrorWire::VShardNotMapped { vshard_id } => Self::VShardNotMapped { vshard_id },
            ShardErrorWire::GroupNotFound { group_id } => Self::GroupNotFound { group_id },
            ShardErrorWire::LearnerNotCaughtUp {
                group_id,
                node_id,
                match_index,
                commit_index,
            } => Self::LearnerNotCaughtUp {
                group_id,
                node_id,
                match_index,
                commit_index,
            },
            ShardErrorWire::MigrationInProgress { vshard_id } => {
                Self::MigrationInProgress { vshard_id }
            }
            ShardErrorWire::MigrationPauseBudgetExceeded {
                estimated_us,
                budget_us,
            } => Self::MigrationPauseBudgetExceeded {
                estimated_us,
                budget_us,
            },
            ShardErrorWire::NodeUnreachable { node_id } => Self::NodeUnreachable { node_id },
            ShardErrorWire::GhostNotFound { node_id, shard_id } => {
                Self::GhostNotFound { node_id, shard_id }
            }
            ShardErrorWire::Transport { detail } => Self::Transport { detail },
            ShardErrorWire::Unanswered { node_id, detail } => Self::Unanswered { node_id, detail },
            ShardErrorWire::ShardTimeout {
                vshard_id,
                elapsed_ms,
            } => Self::ShardTimeout {
                vshard_id,
                elapsed_ms,
            },
            ShardErrorWire::StreamTerminal { error, detail } => Self::StreamTerminal {
                error: Box::new(error),
                detail,
            },
            ShardErrorWire::Storage { detail } => Self::Storage { detail },
            ShardErrorWire::DataPlane { code } => Self::DataPlane { code },
            ShardErrorWire::Codec { detail } => Self::Codec { detail },
            ShardErrorWire::UnsupportedWireVersion {
                got,
                supported_min,
                supported_max,
            } => Self::UnsupportedWireVersion {
                got,
                supported_min,
                supported_max,
            },
            ShardErrorWire::CircuitOpen { node_id, failures } => {
                Self::CircuitOpen { node_id, failures }
            }
            ShardErrorWire::JoinGroupDisappeared { group_id } => {
                Self::JoinGroupDisappeared { group_id }
            }
            ShardErrorWire::JoinCommitTimeout {
                group_id,
                log_index,
            } => Self::JoinCommitTimeout {
                group_id,
                log_index,
            },
            ShardErrorWire::ReadIndexNotLeader { group_id } => {
                Self::ReadIndexNotLeader { group_id }
            }
            ShardErrorWire::ReadIndexTimeout {
                group_id,
                waited_ms,
            } => Self::ReadIndexTimeout {
                group_id,
                waited_ms,
            },
            ShardErrorWire::Config { detail } => Self::Config { detail },
            ShardErrorWire::WrongOwner {
                vshard_id,
                expected_owner_node,
            } => Self::WrongOwner {
                vshard_id,
                expected_owner_node,
            },
            ShardErrorWire::SnapshotCrcMismatch {
                group_id,
                stored,
                computed,
            } => Self::SnapshotCrcMismatch {
                group_id,
                stored,
                computed,
            },
            ShardErrorWire::SnapshotOffsetRegression {
                group_id,
                expected,
                actual,
            } => Self::SnapshotOffsetRegression {
                group_id,
                expected,
                actual,
            },
            ShardErrorWire::PartialSnapshotCorrupt { group_id, detail } => {
                Self::PartialSnapshotCorrupt { group_id, detail }
            }
            ShardErrorWire::PartialSnapshotCleanupFailed { group_id, detail } => {
                Self::PartialSnapshotCleanupFailed { group_id, detail }
            }
            ShardErrorWire::SnapshotApplyFailed { group_id, detail } => {
                Self::SnapshotApplyFailed { group_id, detail }
            }
            ShardErrorWire::Untyped { detail } => Self::RemoteUntyped { detail },
            ShardErrorWire::ShardExecution { error, detail } => Self::ShardExecution {
                error: Box::new(error),
                detail,
            },
        }
    }
}
