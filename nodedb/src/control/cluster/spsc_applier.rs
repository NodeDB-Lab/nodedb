//! CommitApplier for committed data-group Raft entries.
//!
//! When a Raft entry is committed across the quorum, it is pushed into the
//! `DistributedApplier`'s bounded mpsc channel, which a background task
//! drains — dispatching to the Data Plane via SPSC and notifying proposers
//! via `ProposeTracker`.

use std::sync::Arc;

use crate::control::distributed_applier::DistributedApplier;

/// CommitApplier that delegates committed data-group Raft entries to
/// the `DistributedApplier` apply loop.
///
/// This is the bridge between the synchronous Raft tick loop (which calls
/// `apply_committed`) and the async `run_apply_loop` task (which does the
/// actual Data Plane dispatch and proposer notification).
pub struct SpscCommitApplier {
    applier: Arc<DistributedApplier>,
}

impl SpscCommitApplier {
    pub fn new(
        _shared: Arc<crate::control::state::SharedState>,
        applier: Arc<DistributedApplier>,
    ) -> Self {
        Self { applier }
    }
}

impl nodedb_cluster::CommitApplier for SpscCommitApplier {
    fn apply_committed(&self, group_id: u64, entries: &[nodedb_raft::message::LogEntry]) -> u64 {
        self.applier.apply_committed(group_id, entries)
    }
}
