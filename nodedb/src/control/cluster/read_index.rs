// SPDX-License-Identifier: BUSL-1.1

//! Raft's answers to the two questions read placement asks.
//!
//! The Control Plane decides what a read needs — a confirmed leader, or a
//! replica within a staleness bound — but only the Raft coordinator, over in
//! `nodedb-cluster`, can answer either. This trait is the seam between them:
//! `start_raft` installs an implementation, and single-node deployments
//! leave it unset because there is no quorum and no replica to weigh.
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
    /// the read may be served at.
    async fn confirm_leader(
        &self,
        group_id: u64,
        timeout: Duration,
    ) -> Result<u64, ReadIndexRefusal>;

    /// Whether this node's replica of `group_id` is within `max_staleness` of
    /// the leader. Local state only — no quorum round, so this does not block.
    fn within_staleness_bound(&self, group_id: u64, max_staleness: Duration) -> bool;

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
/// lives on `SharedState`, so a strong reference would pin both.
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
        // leader unreachable: the caller asks again later.
        _ => ReadIndexRefusal::NotLeader,
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
}
