// SPDX-License-Identifier: BUSL-1.1

//! Raft's answers to the two questions read placement asks.
//!
//! The Control Plane decides what a read needs — a confirmed leader, or a
//! replica within a staleness bound — but only the Raft coordinator, over in
//! `nodedb-cluster`, can answer either. This trait is the seam between them:
//! `start_raft` installs an implementation on every node.
//!
//! A refusal carries no leader hint. The caller already read the routing
//! table to decide the read belonged here, so it builds the redirect from
//! what it knows rather than having a second, staler answer passed back.

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;

use nodedb_cluster::error::ClusterError;
use nodedb_cluster::multi_raft::MultiRaft;

/// Why a linearizable read cannot be served here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadIndexRefusal {
    /// This node does not lead the group, or lost leadership mid-probe.
    NotLeader,
    /// Still leading, but no quorum answered in time.
    Timeout { waited_ms: u64 },
}

/// Answers whether this node can serve a read of a given Raft group.
#[async_trait]
pub trait RaftReadGate: Send + Sync {
    /// Confirm leadership of `group_id` against a quorum, returning the index
    /// the read can be served at.
    async fn confirm_leader(
        &self,
        group_id: u64,
        timeout: Duration,
    ) -> Result<u64, ReadIndexRefusal>;

    /// Whether this node's replica of `group_id` is within `max_staleness` of
    /// the leader. Local state only — no quorum round, so this does not block.
    fn within_staleness_bound(&self, group_id: u64, max_staleness: Duration) -> bool;

    /// Whether this node leads `group_id` under a leader lease that is valid
    /// now. Local state only, so this does not block.
    ///
    /// The lease lapses before any other node can win an election for the
    /// group, so at most one node answers `true` for a group at a time.
    fn holds_leader_lease(&self, group_id: u64) -> bool;

    /// The Raft term of this node's valid leader lease on `group_id`, `None`
    /// when it holds none. Local state only, so this does not block.
    ///
    /// Each later leader of the group leads at a higher term, so the term
    /// fences work a previous leaseholder started.
    fn leader_lease_term(&self, group_id: u64) -> Option<u64>;

    /// The index a read of `group_id` can be served at under this node's
    /// valid leader lease: the commit index, `None` when it holds no lease.
    /// Local state only, so this does not block.
    ///
    /// The read is linearizable once this node has applied the group through
    /// the index.
    fn lease_read_index(&self, group_id: u64) -> Option<u64>;

    /// A read index for `group_id` on any node: confirmed here when this node
    /// leads the group, asked of the leader otherwise.
    ///
    /// Once this node has applied the group through the returned index, its
    /// state holds every entry committed before the call. A gate whose node
    /// always leads its groups answers with [`Self::confirm_leader`].
    async fn read_index(&self, group_id: u64, timeout: Duration) -> Result<u64, ReadIndexRefusal> {
        self.confirm_leader(group_id, timeout).await
    }
}

/// The Raft loop type the production gate forwards read-index requests
/// through.
type GateRaftLoop = nodedb_cluster::RaftLoop<
    crate::control::cluster::spsc_applier::SpscCommitApplier,
    crate::control::LocalPlanExecutor,
>;

/// Production implementation, backed by the Raft loop's coordinator.
///
/// Holds the loop weakly: the loop keeps `SharedState` alive, and the gate
/// lives on `SharedState`, so a strong reference will pin both.
pub struct MultiRaftReadGate {
    multi_raft: Arc<Mutex<MultiRaft>>,
    raft_loop: Weak<GateRaftLoop>,
}

impl MultiRaftReadGate {
    pub fn new(multi_raft: Arc<Mutex<MultiRaft>>, raft_loop: Weak<GateRaftLoop>) -> Self {
        Self {
            multi_raft,
            raft_loop,
        }
    }
}

/// The refusal a read-index error means to the caller.
fn refusal_of(error: ClusterError) -> ReadIndexRefusal {
    match error {
        ClusterError::ReadIndexTimeout { waited_ms, .. } => ReadIndexRefusal::Timeout { waited_ms },
        // Not hosted here, not leading, leadership lost mid-probe, or the
        // leader unreachable: the caller asks again later. Every other error
        // also leaves this node unable to prove its leadership now.
        ClusterError::Raft(_)
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
        | ClusterError::ShardExecution { .. } => ReadIndexRefusal::NotLeader,
    }
}

#[async_trait]
impl RaftReadGate for MultiRaftReadGate {
    async fn confirm_leader(
        &self,
        group_id: u64,
        timeout: Duration,
    ) -> Result<u64, ReadIndexRefusal> {
        nodedb_cluster::confirm_read_index(&self.multi_raft, group_id, timeout)
            .await
            .map_err(refusal_of)
    }

    async fn read_index(&self, group_id: u64, timeout: Duration) -> Result<u64, ReadIndexRefusal> {
        let Some(raft_loop) = self.raft_loop.upgrade() else {
            // The loop is gone: the node is shutting down.
            return Err(ReadIndexRefusal::NotLeader);
        };
        raft_loop
            .read_index_via_leader(group_id, timeout)
            .await
            .map_err(refusal_of)
    }

    fn within_staleness_bound(&self, group_id: u64, max_staleness: Duration) -> bool {
        self.multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .within_staleness_bound(group_id, max_staleness)
    }

    fn holds_leader_lease(&self, group_id: u64) -> bool {
        self.multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lease_read_index(group_id)
            .is_some()
    }

    fn leader_lease_term(&self, group_id: u64) -> Option<u64> {
        self.multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lease_term(group_id)
    }

    fn lease_read_index(&self, group_id: u64) -> Option<u64> {
        self.multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lease_read_index(group_id)
    }
}
