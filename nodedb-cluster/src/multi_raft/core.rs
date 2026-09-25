// SPDX-License-Identifier: BUSL-1.1

//! `MultiRaft` struct, constructors, group lifecycle and tick.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tracing::info;

use nodedb_raft::node::RaftConfig;
use nodedb_raft::{RaftNode, Ready};

use crate::error::{ClusterError, Result};
use crate::raft_storage::RedbLogStorage;
use crate::routing::RoutingTable;

/// Multi-Raft coordinator managing multiple Raft groups on a single node.
///
/// This coordinator:
/// - Manages all Raft groups hosted on this node
/// - Batches heartbeats across groups sharing the same leader
/// - Routes incoming RPCs to the correct group
/// - Collects `Ready` output from all groups for the caller to execute
pub struct MultiRaft {
    /// This node's ID.
    pub(super) node_id: u64,
    /// Raft groups hosted on this node (group_id → RaftNode).
    pub(super) groups: HashMap<u64, RaftNode<RedbLogStorage>>,
    /// Routing table (vShard → group mapping).
    ///
    /// This is the SAME `Arc<RwLock<RoutingTable>>` held by
    /// `ClusterState.routing` / `shared.cluster_routing`, so committed Raft
    /// conf-changes applied here (via `apply_conf_change`) write THROUGH to
    /// the one table the query/data plane reads. Raft is the convergence
    /// mechanism on every applying node (leader and follower).
    pub(super) routing: Arc<RwLock<RoutingTable>>,
    /// Default election timeout range.
    pub(super) election_timeout_min: Duration,
    pub(super) election_timeout_max: Duration,
    /// Heartbeat interval.
    pub(super) heartbeat_interval: Duration,
    /// Auto-compaction threshold applied to every group created on this
    /// node. `None` (default) disables auto-compaction. See
    /// [`RaftConfig::log_compaction_threshold`].
    pub(super) log_compaction_threshold: Option<u64>,
    /// Data directory for persistent Raft log storage.
    pub(super) data_dir: PathBuf,
    /// Per-group count of `InstallSnapshot` transfers currently in flight.
    /// Compaction is deferred for any group with an active transfer so the
    /// snapshot boundary never advances mid-transfer.
    pub(super) in_flight_snapshots: Arc<crate::raft_loop::in_flight_snapshots::InFlightSnapshots>,
}

/// Aggregated output from all Raft groups after a tick.
#[derive(Debug, Default)]
pub struct MultiRaftReady {
    /// Per-group ready output: (group_id, Ready).
    pub groups: Vec<(u64, Ready)>,
}

impl MultiRaftReady {
    pub fn is_empty(&self) -> bool {
        self.groups.iter().all(|(_gid, r)| r.is_empty())
    }

    /// Total committed entries across all groups.
    pub fn total_committed(&self) -> usize {
        self.groups
            .iter()
            .map(|(_, r)| r.committed_entries.len())
            .sum()
    }
}

impl MultiRaft {
    /// Construct a `MultiRaft` owning its routing table by value.
    ///
    /// Wraps the table in a fresh `Arc<RwLock<_>>`. Used by tests that do not
    /// need to share the routing handle with a `ClusterState`. Production
    /// construction sites use [`MultiRaft::new_with_shared_routing`] so the
    /// data plane and Raft state machine read/write the SAME table.
    pub fn new(node_id: u64, routing: RoutingTable, data_dir: PathBuf) -> Self {
        Self::new_with_shared_routing(node_id, Arc::new(RwLock::new(routing)), data_dir)
    }

    /// Construct a `MultiRaft` sharing the given routing handle.
    ///
    /// The passed `Arc<RwLock<RoutingTable>>` MUST be the same handle stored
    /// in `ClusterState.routing` so committed conf-changes converge the
    /// data-plane routing view.
    pub fn new_with_shared_routing(
        node_id: u64,
        routing: Arc<RwLock<RoutingTable>>,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            node_id,
            groups: HashMap::new(),
            routing,
            election_timeout_min: Duration::from_secs(2),
            election_timeout_max: Duration::from_secs(5),
            heartbeat_interval: Duration::from_millis(50),
            log_compaction_threshold: None,
            data_dir,
            in_flight_snapshots: Arc::new(
                crate::raft_loop::in_flight_snapshots::InFlightSnapshots::default(),
            ),
        }
    }

    /// Configure election timeout range.
    pub fn with_election_timeout(mut self, min: Duration, max: Duration) -> Self {
        self.election_timeout_min = min;
        self.election_timeout_max = max;
        self
    }

    /// Configure heartbeat interval.
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// The shortest election timeout a group on this node waits before it
    /// campaigns.
    pub fn election_timeout_min(&self) -> Duration {
        self.election_timeout_min
    }

    /// How often a leader on this node sends heartbeats.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Configure the auto-compaction threshold for every group created on
    /// this node. `None` disables auto-compaction (the default). See
    /// [`RaftConfig::log_compaction_threshold`].
    pub fn with_log_compaction_threshold(mut self, threshold: Option<u64>) -> Self {
        self.log_compaction_threshold = threshold;
        self
    }

    /// Initialize a Raft group on this node as a voting member.
    ///
    /// `peers` is the list of other voters in the group (excluding self).
    /// For a learner-start group, use `add_group_as_learner` instead.
    pub fn add_group(&mut self, group_id: u64, peers: Vec<u64>) -> Result<()> {
        self.add_group_inner(group_id, peers, vec![], false)
    }

    /// Initialize a Raft group on this node as a non-voting learner.
    ///
    /// The local node boots in the `Learner` role and will not stand for
    /// election until it is promoted by a `PromoteLearner` conf change.
    ///
    /// `voters` is the full voter set of the group (excluding self).
    /// `learners` is the learner set of the group excluding self — usually
    /// empty unless multiple learners are being admitted in the same round.
    pub fn add_group_as_learner(
        &mut self,
        group_id: u64,
        voters: Vec<u64>,
        learners: Vec<u64>,
    ) -> Result<()> {
        self.add_group_inner(group_id, voters, learners, true)
    }

    fn add_group_inner(
        &mut self,
        group_id: u64,
        peers: Vec<u64>,
        learners: Vec<u64>,
        starts_as_learner: bool,
    ) -> Result<()> {
        let config = RaftConfig {
            node_id: self.node_id,
            group_id,
            peers,
            learners,
            observers: vec![],
            starts_as_learner,
            starts_as_observer: false,
            election_timeout_min: self.election_timeout_min,
            election_timeout_max: self.election_timeout_max,
            heartbeat_interval: self.heartbeat_interval,
            log_compaction_threshold: self.log_compaction_threshold,
        };

        let storage_path = self.data_dir.join(format!("raft/group-{group_id}.redb"));
        let storage = RedbLogStorage::open(&storage_path).map_err(|e| ClusterError::Transport {
            detail: format!("failed to open raft storage for group {group_id}: {e}"),
        })?;
        let mut node = RaftNode::new(config, storage);
        // Reload durable state (HardState + log) from redb before mounting the
        // group. On a restart this recovers the persisted term/voted_for — so
        // a restarted voter cannot forget its vote and double-vote — AND the
        // persisted log entries, so the node does not depend on full
        // re-replication from the leader to recover its log. On a fresh group
        // the storage is empty and this is a no-op (default HardState, empty
        // log). Also resets the election timeout.
        node.restore()?;
        self.groups.insert(group_id, node);

        info!(
            node = self.node_id,
            group = group_id,
            as_learner = starts_as_learner,
            path = %storage_path.display(),
            "added raft group with persistent storage"
        );
        Ok(())
    }

    /// Tick all Raft groups. Returns aggregated ready output.
    ///
    /// Any HardState staged by a tick (an election term bump + self-vote from
    /// an election timeout) is durably persisted BEFORE the aggregated `Ready`
    /// — and therefore the vote requests it carries — is returned for
    /// dispatch. A persist failure aborts the tick so the caller never sends
    /// vote requests for a term that was not made durable.
    pub fn tick(&mut self) -> Result<MultiRaftReady> {
        let mut ready = MultiRaftReady::default();

        for (&group_id, node) in &mut self.groups {
            node.tick();
            node.persist_hard_state_if_dirty()?;
            let r = node.take_ready();
            if !r.is_empty() {
                ready.groups.push((group_id, r));
            }
        }

        Ok(ready)
    }

    /// Clone of the shared routing handle.
    ///
    /// Returns an `Arc` clone pointing at the same `RwLock<RoutingTable>` the
    /// data plane reads. Callers that need a `RoutingTable` value take a tight
    /// read guard and clone it out.
    pub fn routing(&self) -> Arc<RwLock<RoutingTable>> {
        self.routing.clone()
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Clone of the in-flight `InstallSnapshot` tracker.
    ///
    /// The tick loop clones this to mark snapshot transfers active for their
    /// lifetime; `maybe_compact_group` reads it to defer compaction while a
    /// transfer is in flight.
    pub fn in_flight_snapshots(
        &self,
    ) -> Arc<crate::raft_loop::in_flight_snapshots::InFlightSnapshots> {
        self.in_flight_snapshots.clone()
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Whether this node hosts the given Raft group.
    pub fn contains_group(&self, group_id: u64) -> bool {
        self.groups.contains_key(&group_id)
    }

    /// IDs of every Raft group hosted on this node, including groups
    /// that do not own vShards (for example the Calvin sequencer).
    pub fn group_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.groups.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Mutable access to the underlying Raft groups (for testing / bootstrap).
    pub fn groups_mut(&mut self) -> &mut HashMap<u64, RaftNode<RedbLogStorage>> {
        &mut self.groups
    }
}

// Re-export LogEntry so callers of `read_committed_entries` can name the type.
pub use nodedb_raft::LogEntry;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi_raft::status::GroupMembership;
    use std::time::Instant;

    #[test]
    fn single_node_multi_raft() {
        let dir = tempfile::tempdir().unwrap();
        // uniform(4, ...) creates 4 data groups (1..=4) plus metadata group 0.
        let rt = RoutingTable::uniform(4, &[1], 1);
        let mut mr = MultiRaft::new(1, rt.clone(), dir.path().to_path_buf());

        for gid in rt.group_ids() {
            mr.add_group(gid, vec![]).unwrap();
        }
        // 4 data groups + 1 metadata group.
        assert_eq!(mr.group_count(), 5);

        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let ready = mr.tick().unwrap();
        assert_eq!(ready.groups.len(), 5);
    }

    #[test]
    fn propose_routes_to_correct_group() {
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(4, &[1], 1);
        let mut mr = MultiRaft::new(1, rt.clone(), dir.path().to_path_buf());

        for gid in rt.group_ids() {
            mr.add_group(gid, vec![]).unwrap();
        }
        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        mr.tick().unwrap();
        for (gid, ready) in mr.tick().unwrap().groups {
            if let Some(last) = ready.committed_entries.last() {
                mr.advance_applied(gid, last.index).unwrap();
            }
        }

        // vshard 0 maps to data group 1, vshard 256 also maps to group 1 (256 % 4 + 1 = 1).
        let (_gid, idx) = mr.propose(0, b"cmd-shard-0".to_vec()).unwrap();
        assert!(idx > 0);

        let (_gid, idx) = mr.propose(256, b"cmd-shard-256".to_vec()).unwrap();
        assert!(idx > 0);
    }

    #[test]
    fn add_group_as_learner_starts_in_learner_role() {
        use nodedb_raft::NodeRole;
        let dir = tempfile::tempdir().unwrap();
        // uniform(1, ...) creates data group 1 plus metadata group 0.
        let rt = RoutingTable::uniform(1, &[1, 2], 2);
        let mut mr = MultiRaft::new(2, rt, dir.path().to_path_buf());

        // Data group 1: join as learner (node 1 is the voter, we're node 2 = learner).
        mr.add_group_as_learner(1, vec![1], vec![]).unwrap();

        let node = mr.groups.get(&1).unwrap();
        assert_eq!(node.role(), NodeRole::Learner);
        assert_eq!(node.voters(), &[1]);
    }

    #[test]
    fn group_membership_includes_non_routing_learner_group() {
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(2, rt, dir.path().to_path_buf());
        let non_routing_group = u64::MAX - 7;
        mr.add_group_as_learner(non_routing_group, vec![1], vec![])
            .unwrap();

        assert!(mr.group_ids().contains(&non_routing_group));
        assert_eq!(
            mr.group_membership(non_routing_group),
            Some(GroupMembership {
                group_id: non_routing_group,
                leader_id: 0,
                voters: vec![1],
                learners: vec![2],
            })
        );
    }
}
