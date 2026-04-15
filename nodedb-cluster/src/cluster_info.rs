//! Observability view of the cluster — snapshot types, trait for
//! querying per-group Raft status, and the `ClusterObserver` handle
//! bundled into `SharedState` for HTTP / metrics readers.
//!
//! The split between `ClusterObserver` (operational aggregation) and
//! the individual sources it pulls from (`ClusterTopology`,
//! `RoutingTable`, `ClusterLifecycleTracker`, `GroupStatusProvider`)
//! keeps every consumer parameter-free: an HTTP handler takes a
//! single `Arc<ClusterObserver>` and gets a complete, serialisable
//! snapshot of everything the cluster surface exposes.

use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::forward::PlanExecutor;
use crate::lifecycle_state::{ClusterLifecycleState, ClusterLifecycleTracker};
use crate::multi_raft::GroupStatus;
use crate::raft_loop::{CommitApplier, RaftLoop};
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;

/// Read-only accessor for per-group Raft status.
///
/// Implemented for every `RaftLoop` via a blanket impl so the main
/// binary can coerce `Arc<RaftLoop<...>>` to `Arc<dyn
/// GroupStatusProvider + Send + Sync>` without thinking about the
/// `CommitApplier` / `PlanExecutor` type parameters.
pub trait GroupStatusProvider: Send + Sync {
    /// Current status of every Raft group hosted on this node.
    fn group_statuses(&self) -> Vec<GroupStatus>;
}

impl<A, P> GroupStatusProvider for RaftLoop<A, P>
where
    A: CommitApplier,
    P: PlanExecutor,
{
    fn group_statuses(&self) -> Vec<GroupStatus> {
        RaftLoop::group_statuses(self)
    }
}

/// Aggregated observability handle for the cluster.
///
/// Stored in `SharedState::cluster_observer` as an
/// `Arc<ClusterObserver>` so HTTP route handlers and the metrics
/// endpoint can build snapshots without threading four separate
/// handles through every call.
///
/// Construction is done exactly once — after `start_cluster` has
/// returned and `start_raft` has built the `RaftLoop`. See
/// `nodedb::control::cluster::start_raft` for the wiring.
pub struct ClusterObserver {
    /// This node's id.
    pub node_id: u64,
    /// Lifecycle phase tracker (shared with `start_cluster`).
    pub lifecycle: ClusterLifecycleTracker,
    /// Shared cluster topology.
    pub topology: Arc<RwLock<ClusterTopology>>,
    /// Shared routing table.
    pub routing: Arc<RwLock<RoutingTable>>,
    /// Type-erased per-group status provider backed by `RaftLoop`.
    pub group_status: Arc<dyn GroupStatusProvider + Send + Sync>,
}

impl ClusterObserver {
    pub fn new(
        node_id: u64,
        lifecycle: ClusterLifecycleTracker,
        topology: Arc<RwLock<ClusterTopology>>,
        routing: Arc<RwLock<RoutingTable>>,
        group_status: Arc<dyn GroupStatusProvider + Send + Sync>,
    ) -> Self {
        Self {
            node_id,
            lifecycle,
            topology,
            routing,
            group_status,
        }
    }

    /// Build a complete `ClusterInfoSnapshot` for rendering via
    /// `/cluster/status` or `/metrics`.
    pub fn snapshot(&self) -> ClusterInfoSnapshot {
        let lifecycle = self.lifecycle.current();
        let peers: Vec<PeerSnapshot> = {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            topo.all_nodes()
                .map(|n| PeerSnapshot {
                    node_id: n.node_id,
                    addr: n.addr.clone(),
                    state: format!("{:?}", n.state),
                })
                .collect()
        };

        // Merge Raft group status (from the live RaftLoop) with the
        // routing-table members/learners view. The routing table is
        // authoritative for "who is supposed to be in this group";
        // the RaftLoop is authoritative for "who is leader / what's
        // the commit index right now".
        let routing_snapshot: Vec<(u64, Vec<u64>, Vec<u64>)> = {
            let rt = self.routing.read().unwrap_or_else(|p| p.into_inner());
            rt.group_members()
                .iter()
                .map(|(&gid, info)| (gid, info.members.clone(), info.learners.clone()))
                .collect()
        };

        let raft_groups = self.group_status.group_statuses();
        let groups: Vec<GroupSnapshot> = raft_groups
            .into_iter()
            .map(|gs| {
                let (members, learners) = routing_snapshot
                    .iter()
                    .find(|(gid, _, _)| *gid == gs.group_id)
                    .map(|(_, m, l)| (m.clone(), l.clone()))
                    .unwrap_or_default();
                GroupSnapshot {
                    group_id: gs.group_id,
                    role: gs.role,
                    leader_id: gs.leader_id,
                    term: gs.term,
                    commit_index: gs.commit_index,
                    last_applied: gs.last_applied,
                    member_count: gs.member_count,
                    vshard_count: gs.vshard_count,
                    members,
                    learners,
                }
            })
            .collect();

        ClusterInfoSnapshot {
            node_id: self.node_id,
            lifecycle,
            peers,
            groups,
        }
    }
}

/// Serialisable snapshot of the full cluster observability surface.
///
/// This is the JSON shape returned by `GET /cluster/status` and the
/// source-of-truth for the Prometheus gauges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfoSnapshot {
    /// This node's id.
    pub node_id: u64,
    /// Current lifecycle phase.
    pub lifecycle: ClusterLifecycleState,
    /// Every peer known to this node.
    pub peers: Vec<PeerSnapshot>,
    /// Every Raft group hosted on this node.
    pub groups: Vec<GroupSnapshot>,
}

impl ClusterInfoSnapshot {
    /// Convenience accessor used by the metrics endpoint.
    pub fn lifecycle_label(&self) -> &'static str {
        self.lifecycle.label()
    }

    /// Number of peers in the topology snapshot. Drives the
    /// `nodedb_cluster_members` Prometheus gauge.
    pub fn members_count(&self) -> usize {
        self.peers.len()
    }

    /// Number of Raft groups hosted locally.
    pub fn groups_count(&self) -> usize {
        self.groups.len()
    }
}

/// One peer entry rendered in `/cluster/status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSnapshot {
    pub node_id: u64,
    pub addr: String,
    pub state: String,
}

/// One Raft group entry rendered in `/cluster/status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSnapshot {
    pub group_id: u64,
    pub role: String,
    pub leader_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub member_count: usize,
    pub vshard_count: usize,
    pub members: Vec<u64>,
    pub learners: Vec<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{NodeInfo, NodeState};

    /// Fake provider that returns a canned list — lets us test the
    /// snapshot builder end-to-end without spinning up a real
    /// `MultiRaft`.
    struct FakeProvider(Vec<GroupStatus>);
    impl GroupStatusProvider for FakeProvider {
        fn group_statuses(&self) -> Vec<GroupStatus> {
            self.0.clone()
        }
    }

    fn make_observer(
        lifecycle: ClusterLifecycleTracker,
        peers: Vec<NodeInfo>,
        routing: RoutingTable,
        raft_groups: Vec<GroupStatus>,
    ) -> ClusterObserver {
        let mut topology = ClusterTopology::new();
        for p in peers {
            topology.add_node(p);
        }
        ClusterObserver::new(
            1,
            lifecycle,
            Arc::new(RwLock::new(topology)),
            Arc::new(RwLock::new(routing)),
            Arc::new(FakeProvider(raft_groups)),
        )
    }

    fn gs(group_id: u64, role: &str, leader: u64) -> GroupStatus {
        GroupStatus {
            group_id,
            role: role.into(),
            leader_id: leader,
            term: 1,
            commit_index: 5,
            last_applied: 5,
            member_count: 3,
            vshard_count: 512,
        }
    }

    #[test]
    fn snapshot_renders_full_state() {
        let lifecycle = ClusterLifecycleTracker::new();
        lifecycle.to_ready(3);

        let peers = vec![
            NodeInfo::new(1, "10.0.0.1:9400".parse().unwrap(), NodeState::Active),
            NodeInfo::new(2, "10.0.0.2:9400".parse().unwrap(), NodeState::Active),
            NodeInfo::new(3, "10.0.0.3:9400".parse().unwrap(), NodeState::Active),
        ];

        let mut routing = RoutingTable::uniform(2, &[1, 2, 3], 3);
        // Inject a learner to verify it lands in the snapshot.
        routing.add_group_learner(0, 4);

        let raft_groups = vec![gs(0, "Leader", 1), gs(1, "Follower", 2)];

        let observer = make_observer(lifecycle, peers, routing, raft_groups);
        let snap = observer.snapshot();

        assert_eq!(snap.node_id, 1);
        assert_eq!(snap.lifecycle_label(), "ready");
        assert_eq!(snap.members_count(), 3);
        assert_eq!(snap.groups_count(), 2);

        // Group 0 should carry both members (1,2,3) and the injected
        // learner (4).
        let g0 = snap
            .groups
            .iter()
            .find(|g| g.group_id == 0)
            .expect("group 0 present");
        assert_eq!(g0.role, "Leader");
        assert_eq!(g0.leader_id, 1);
        assert!(g0.members.contains(&1));
        assert!(g0.learners.contains(&4));

        // Peer snapshots preserve addresses.
        let addrs: Vec<&str> = snap.peers.iter().map(|p| p.addr.as_str()).collect();
        assert!(addrs.contains(&"10.0.0.1:9400"));
        assert!(addrs.contains(&"10.0.0.3:9400"));
    }

    #[test]
    fn snapshot_without_groups() {
        // A node whose RaftLoop reports zero groups (not in cluster
        // mode, but we got a stub observer). Topology still rendered.
        let lifecycle = ClusterLifecycleTracker::new();
        lifecycle.to_bootstrapping();
        let peers = vec![NodeInfo::new(
            1,
            "127.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        )];
        let routing = RoutingTable::uniform(1, &[1], 1);
        let observer = make_observer(lifecycle, peers, routing, vec![]);

        let snap = observer.snapshot();
        assert_eq!(snap.lifecycle_label(), "bootstrapping");
        assert_eq!(snap.members_count(), 1);
        assert_eq!(snap.groups_count(), 0);
    }

    #[test]
    fn snapshot_is_json_roundtrippable() {
        // The shape is consumed by clients, so serde must round-trip
        // losslessly for every variant. This keeps downstream tooling
        // honest as fields are added.
        let lifecycle = ClusterLifecycleTracker::new();
        lifecycle.to_joining(2);
        let peers = vec![NodeInfo::new(
            1,
            "127.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        )];
        let routing = RoutingTable::uniform(1, &[1], 1);
        let observer = make_observer(lifecycle, peers, routing, vec![gs(0, "Leader", 1)]);
        let snap = observer.snapshot();

        let json = serde_json::to_string(&snap).expect("serialize");
        let back: ClusterInfoSnapshot = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.node_id, snap.node_id);
        assert_eq!(back.peers.len(), 1);
        assert_eq!(back.groups.len(), 1);
        // Joining preserves the attempt field through serde.
        match back.lifecycle {
            ClusterLifecycleState::Joining { attempt } => assert_eq!(attempt, 2),
            other => panic!("expected Joining, got {other:?}"),
        }
    }
}
