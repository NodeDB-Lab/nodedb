//! Bundle of everything needed to run the cluster after startup.

use std::sync::{Arc, Mutex, RwLock};

use crate::control::cluster::applied_index_watcher::AppliedIndexWatcher;

/// Everything the main server needs to wire the cluster into the rest of
/// the process. Produced by [`super::init::init_cluster`] and consumed by
/// [`super::start_raft::start_raft`].
pub struct ClusterHandle {
    /// The QUIC transport (for serving and sending RPCs).
    pub transport: Arc<nodedb_cluster::NexarTransport>,
    /// Cluster topology (shared with SharedState).
    pub topology: Arc<RwLock<nodedb_cluster::ClusterTopology>>,
    /// Cluster routing table (shared with SharedState).
    pub routing: Arc<RwLock<nodedb_cluster::RoutingTable>>,
    /// Lifecycle phase tracker shared with `start_cluster` and the
    /// `/cluster/status` + metrics readers.
    pub lifecycle: nodedb_cluster::ClusterLifecycleTracker,
    /// Live replicated metadata cache. Populated by the
    /// `MetadataCommitApplier` and shared with `SharedState` so planners,
    /// pgwire handlers, and HTTP catalog endpoints can read descriptors
    /// without going back to redb.
    pub metadata_cache: Arc<RwLock<nodedb_cluster::MetadataCache>>,
    /// Watcher used by the metadata proposer to block on local apply.
    pub applied_index_watcher: Arc<AppliedIndexWatcher>,
    /// This node's ID.
    pub node_id: u64,
    /// `MultiRaft` constructed by `start_cluster` with the correct
    /// voter / learner membership already applied. `start_raft` takes
    /// this via `Mutex::lock + .take()` and moves it into the
    /// `RaftLoop`. Wrapped in `Mutex<Option<_>>` so the handle itself
    /// stays `Clone` while still guaranteeing single-transfer
    /// semantics at runtime.
    pub multi_raft: Mutex<Option<nodedb_cluster::MultiRaft>>,
}
