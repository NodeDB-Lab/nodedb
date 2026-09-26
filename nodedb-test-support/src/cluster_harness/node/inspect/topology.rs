// SPDX-License-Identifier: BUSL-1.1

//! Raft / topology / routing inspector methods on [`TestClusterNode`].

use nodedb_types::DatabaseId;

use crate::cluster_harness::node::lifecycle::TestClusterNode;

impl TestClusterNode {
    /// Number of nodes currently visible in this node's topology view.
    pub fn topology_size(&self) -> usize {
        self.shared
            .cluster_topology
            .as_ref()
            .map(|t| t.read().unwrap_or_else(|p| p.into_inner()).node_count())
            .unwrap_or(0)
    }

    /// Number of nodes in the `Active` state from this node's view. Unlike
    /// [`Self::topology_size`], unreachable / failed peers do NOT count —
    /// this drops as soon as the health subsystem marks a peer down, well
    /// before the ghost sweeper reaps the topology entry. Use this in
    /// tests that need to gate on "peer X is observed dead by survivors"
    /// rather than the much-later ghost reap.
    pub fn active_topology_size(&self) -> usize {
        self.shared
            .cluster_topology
            .as_ref()
            .map(|t| {
                t.read()
                    .unwrap_or_else(|p| p.into_inner())
                    .active_nodes()
                    .len()
            })
            .unwrap_or(0)
    }

    /// Observed metadata-group leader id from this node's local Raft
    /// state, or `0` if no leader is known yet (election in progress).
    /// Polled by the cluster harness `spawn_three()` to gate test
    /// execution on a stable leader — otherwise tests racing the first
    /// election see `not leader (leader hint: None)` errors when CPU
    /// pressure delays the initial heartbeats past topology convergence.
    pub fn metadata_group_leader(&self) -> u64 {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return 0;
        };
        observer
            .group_status
            .upgrade()
            .map(|gs| gs.group_statuses())
            .unwrap_or_default()
            .into_iter()
            .find(|g| g.group_id == nodedb_cluster::METADATA_GROUP_ID)
            .map(|g| g.leader_id)
            .unwrap_or(0)
    }

    /// Snapshot of `(group_id, leader_id)` for every Raft group hosted
    /// on this node. Used by the harness to gate cluster startup on
    /// every group having a stable leader — without this, the first
    /// data-group write (after `spawn_three()` returns) can race the
    /// data-group leader election: the proposer forwards to the
    /// routing-table-hinted leader (still `0` because the data group
    /// hasn't elected yet), local propose returns Ok with a bogus
    /// log_index that nobody has actually committed, the apply path
    /// never finds that index, and the row is silently lost while the
    /// proposer's tracker fires Ok on a no-op or unrelated entry.
    pub fn all_group_leaders(&self) -> Vec<(u64, u64)> {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return Vec::new();
        };
        observer
            .group_status
            .upgrade()
            .map(|gs| gs.group_statuses())
            .unwrap_or_default()
            .into_iter()
            .map(|g| (g.group_id, g.leader_id))
            .collect()
    }

    /// Highest compacted snapshot index for `group_id` from this node's
    /// local Raft state, or `0` if the group isn't hosted here or hasn't
    /// compacted yet. A non-zero value means the log has been compacted
    /// past the start — a lagging peer below this index can only be caught
    /// up via `InstallSnapshot`, never `AppendEntries`. Used by the
    /// install-snapshot end-to-end test to assert compaction happened on
    /// the leader *before* the learner joined.
    pub fn group_snapshot_index(&self, group_id: u64) -> u64 {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return 0;
        };
        observer
            .group_status
            .upgrade()
            .map(|gs| gs.group_statuses())
            .unwrap_or_default()
            .into_iter()
            .find(|g| g.group_id == group_id)
            .map(|g| g.snapshot_index)
            .unwrap_or(0)
    }

    /// Maximum compacted snapshot index across every **data** Raft group
    /// hosted on this node (i.e. excluding the metadata group, id 0). A
    /// non-zero value proves at least one data group's log has compacted
    /// past the start, so a fresh learner on that group must be caught up
    /// via `InstallSnapshot`.
    pub fn max_data_group_snapshot_index(&self) -> u64 {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return 0;
        };
        observer
            .group_status
            .upgrade()
            .map(|gs| gs.group_statuses())
            .unwrap_or_default()
            .into_iter()
            .filter(|g| g.group_id != nodedb_cluster::METADATA_GROUP_ID)
            .map(|g| g.snapshot_index)
            .max()
            .unwrap_or(0)
    }

    /// True iff this node LOCALLY hosts (has mounted) a replica of Raft
    /// data group `group_id` — i.e. `group_id` appears in this node's own
    /// `group_statuses()`, which (unlike a pgwire query) only reflects
    /// groups actually running on this node and can NEVER be satisfied by
    /// the gateway forwarding a read to some other hosting node.
    /// `METADATA_GROUP_ID` never counts, even if passed in by mistake.
    pub fn hosts_data_group(&self, group_id: u64) -> bool {
        if group_id == nodedb_cluster::METADATA_GROUP_ID {
            return false;
        }
        let Some(observer) = self.shared.cluster_observer.get() else {
            return false;
        };
        observer
            .group_status
            .upgrade()
            .map(|gs| gs.group_statuses())
            .unwrap_or_default()
            .into_iter()
            .any(|g| g.group_id == group_id)
    }

    /// True iff this node is a current replica of data group `group_id`: it
    /// has the group mounted, and its own routing table lists it as a voter or
    /// learner of the group.
    ///
    /// [`Self::hosts_data_group`] alone does not answer this. A node removed
    /// from a group keeps its mounted replica: a join adds the joiner as a
    /// learner of every group, and placement convergence later removes the
    /// nodes outside the group's placement from its membership only.
    pub fn replicates_data_group(&self, group_id: u64) -> bool {
        if !self.hosts_data_group(group_id) {
            return false;
        }
        let Some(routing) = self.shared.cluster_routing.as_ref() else {
            return false;
        };
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        routing.group_info(group_id).is_some_and(|info| {
            info.members.contains(&self.node_id) || info.learners.contains(&self.node_id)
        })
    }

    /// The local `snapshot_index` for `group_id` from this node's own Raft
    /// state, or `0` if the group isn't hosted here.
    ///
    /// A non-zero value observed on a freshly-joined node whose log starts
    /// beyond the compacted region proves it was caught up by a real
    /// `InstallSnapshot` — `AppendEntries` alone cannot advance a
    /// compacted-past log, so the only way a learner ends up with a
    /// non-zero local snapshot_index for a group it never saw as log
    /// entries is that it applied a snapshot built by the leader's
    /// `DataPlaneSnapshotBuilder`.
    pub fn local_snapshot_index_for_group(&self, group_id: u64) -> u64 {
        self.group_snapshot_index(group_id)
    }

    /// One-line dump of this node's `GroupStatus` for `group_id`
    /// (role/leader/term/commit/applied/last_log/snapshot/members/learners), or
    /// `"<not hosted>"` if the group isn't mounted here. Used to enrich cluster
    /// test failure messages with the full per-node Raft-group picture (e.g. the
    /// install-snapshot end-to-end oracle dumps every node's view on timeout).
    pub fn group_status_line(&self, group_id: u64) -> String {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return format!("node {}: <no observer>", self.node_id);
        };
        match observer
            .group_status
            .upgrade()
            .map(|gs| gs.group_statuses())
            .unwrap_or_default()
            .into_iter()
            .find(|g| g.group_id == group_id)
        {
            Some(g) => format!(
                "node {} g{}: role={} leader={} term={} commit={} applied={} last_log={} snap={} members={} learners={}",
                self.node_id,
                g.group_id,
                g.role,
                g.leader_id,
                g.term,
                g.commit_index,
                g.last_applied,
                g.last_log_index,
                g.snapshot_index,
                g.member_count,
                g.learner_count,
            ),
            None => format!("node {} g{}: <not hosted>", self.node_id, group_id),
        }
    }

    /// Resolve the Raft data group id that `collection` maps to, via this
    /// node's own routing table view. Returns `None` if `cluster_routing`
    /// isn't wired (non-cluster node) or the collection's vshard has no
    /// group mapping yet (e.g. before the CREATE COLLECTION DDL has
    /// propagated).
    pub fn group_id_for_collection(&self, collection: &str) -> Option<u64> {
        let vshard =
            nodedb_cluster::routing::vshard_for_collection(DatabaseId::DEFAULT, collection);
        self.shared
            .cluster_routing
            .as_ref()?
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_for_vshard(vshard)
            .ok()
    }

    /// Observed data-group (group 1) leader id from this node's local Raft
    /// state, or `0` if no leader is known yet.
    pub fn data_group_leader(&self) -> u64 {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return 0;
        };
        observer
            .group_status
            .upgrade()
            .map(|gs| gs.group_statuses())
            .unwrap_or_default()
            .into_iter()
            .find(|g| g.group_id == 1)
            .map(|g| g.leader_id)
            .unwrap_or(0)
    }

    /// Count of `DocumentOp::BackfillIndex` handler invocations on
    /// this node's Data Plane since startup. A CREATE INDEX against a
    /// cluster must fan out backfill to every node — this counter
    /// exposes whether the local core actually executed the primitive
    /// (a positive value) versus merely replicating Raft state from
    /// the coordinator (counter stays 0).
    pub fn document_index_backfill_count(&self) -> u64 {
        self.shared
            .system_metrics
            .as_ref()
            .map(|m| {
                m.document_index_backfills
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .unwrap_or(0)
    }

    /// Direct accessor for the `applied_index` watermark — used by
    /// the lease tests to assert that the fast-path acquire did NOT
    /// advance raft.
    pub fn metadata_applied_index(&self) -> u64 {
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.applied_index
    }

    /// Force the routing table on this node to point `group_id` at `fake_leader`,
    /// creating a stale route.
    ///
    /// When the gateway on this node next dispatches to `group_id`, it will send
    /// the request to `fake_leader` instead of the real leader. The remote node
    /// (which is NOT the leader for that group) will return `TypedClusterError::NotLeader`,
    /// causing `retry_not_leader` to update the routing table and retry against
    /// the real leader. This is the canonical way to exercise the NotLeader retry
    /// path in tests without needing a real leadership change (which is slow and
    /// flaky).
    pub fn force_stale_route_for_test(&self, group_id: u64, fake_leader: u64) {
        if let Some(ref routing) = self.shared.cluster_routing {
            let mut table = routing.write().unwrap_or_else(|p| p.into_inner());
            table.set_leader(group_id, fake_leader);
        }
    }

    /// Read the current `not_leader_retry_count` from this node's shared gateway.
    ///
    /// Returns 0 if the gateway has not been constructed yet (shouldn't happen
    /// in tests since the harness wires the gateway during spawn).
    pub fn not_leader_retry_count(&self) -> u64 {
        self.shared
            .gateway
            .get()
            .map(|gw| gw.not_leader_retry_count())
            .unwrap_or(0)
    }
}
