// SPDX-License-Identifier: BUSL-1.1

//! `MultiRaft` struct, constructors, group lifecycle and tick.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nodedb_raft::{RaftNode, Ready};

use crate::applied_watcher::GroupAppliedWatchers;
use crate::error::Result;
use crate::group_disk::StagedLogStorage;
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
    ///
    /// Each group's storage stages its writes. The group's own writer thread
    /// makes them durable, never under this struct's lock.
    pub(super) groups: HashMap<u64, RaftNode<StagedLogStorage>>,
    /// Routing table (vShard → group mapping).
    ///
    /// This is the SAME `Arc<RwLock<RoutingTable>>` held by
    /// `ClusterState.routing` / `shared.cluster_routing`, so committed Raft
    /// conf-changes applied here (via `apply_conf_change`) write THROUGH to
    /// the one table the query/data plane reads. Raft is the convergence
    /// mechanism on every applying node (leader and follower).
    ///
    /// Lock order: this struct's mutex first, then this routing guard.
    /// See the lock-order section on [`RoutingTable`].
    pub(super) routing: Arc<RwLock<RoutingTable>>,
    /// Default election timeout range.
    pub(super) election_timeout_min: Duration,
    pub(super) election_timeout_max: Duration,
    /// Heartbeat interval.
    pub(super) heartbeat_interval: Duration,
    /// Auto-compaction threshold applied to every group created on this
    /// node. `None` (default) disables auto-compaction. See
    /// [`nodedb_raft::node::RaftConfig::log_compaction_threshold`].
    pub(super) log_compaction_threshold: Option<u64>,
    /// Data directory for persistent Raft log storage.
    pub(super) data_dir: PathBuf,
    /// Per-group count of `InstallSnapshot` transfers currently in flight.
    /// Compaction is deferred for any group with an active transfer so the
    /// snapshot boundary never advances mid-transfer.
    pub(super) in_flight_snapshots: Arc<crate::raft_loop::in_flight_snapshots::InFlightSnapshots>,
    /// Per-group gate that orders committed-entry applies against snapshot
    /// installs.
    pub(super) apply_gates: Arc<crate::raft_loop::apply_gate::GroupApplyGates>,
    /// Per-group ceiling on log compaction: the highest index an archiver
    /// holds a copy of. A group with a ceiling never compacts past it, so no
    /// entry leaves the log before it is archived.
    pub(super) compaction_ceilings: HashMap<u64, Arc<std::sync::atomic::AtomicU64>>,
    /// The node clock the metadata entries this node stamps as leader take
    /// their stamp from. `None` proposes them unstamped.
    pub(super) metadata_clock: Option<Arc<nodedb_types::HlcClock>>,
    /// The per-group applied watchers. A group mounted here seeds its
    /// watcher with the applied index it restored, because entries at or
    /// below it are never delivered again. `None` seeds nothing.
    pub(super) applied_watchers: Option<Arc<GroupAppliedWatchers>>,
    /// Decides whether a data group mounted here takes a snapshot before any
    /// log entry. `None` requires none.
    pub(super) snapshot_requirement: Option<SnapshotRequirement>,
    /// Notified when any group's disk makes a batch durable. The Raft loop
    /// drives the groups on it, so a commit the disk completed does not wait
    /// for the next tick.
    pub(super) drive_signal: Arc<tokio::sync::Notify>,
}

/// What this node's replica of a data group needs before it takes log
/// entries: nothing, a snapshot it may lead without, or a snapshot it must
/// not lead without. Called with the group id and the first index the
/// Calvin sequencer log still holds here, under the `MultiRaft` lock: it
/// must not take that lock.
pub type SnapshotRequirement = Arc<dyn Fn(u64, u64) -> nodedb_raft::ReplicaState + Send + Sync>;

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
            apply_gates: Arc::new(crate::raft_loop::apply_gate::GroupApplyGates::new()),
            compaction_ceilings: HashMap::new(),
            metadata_clock: None,
            applied_watchers: None,
            snapshot_requirement: None,
            drive_signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// The signal the groups' disks notify when a batch becomes durable.
    pub fn drive_signal(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.drive_signal)
    }

    /// Install the snapshot requirement for data groups, and apply it to
    /// every data group mounted already.
    pub fn set_snapshot_requirement(&mut self, requirement: SnapshotRequirement) {
        let sequencer_first = self.sequencer_first_available();
        for (&group_id, node) in &mut self.groups {
            if is_data_group(group_id) {
                node.set_replica_state(requirement(group_id, sequencer_first));
            }
        }
        self.snapshot_requirement = Some(requirement);
    }

    /// The auto-compaction threshold every group on this node runs with,
    /// `None` when logs are never compacted.
    pub fn log_compaction_threshold(&self) -> Option<u64> {
        self.log_compaction_threshold
    }

    /// The first index the Calvin sequencer log holds here, `1` when this
    /// node hosts no sequencer replica.
    pub(super) fn sequencer_first_available(&self) -> u64 {
        self.groups
            .get(&crate::calvin::SEQUENCER_GROUP_ID)
            .map_or(1, |node| node.first_available_index())
    }

    /// Install the per-group applied watchers. Every group mounted here, now
    /// or later, starts its watcher at the applied index it restored: the
    /// durable applied floor, whose entries applied before the restart and
    /// are never delivered again.
    pub fn set_applied_watchers(&mut self, watchers: Arc<GroupAppliedWatchers>) {
        for (&group_id, node) in &self.groups {
            seed_watcher(&watchers, group_id, node.last_applied());
        }
        self.applied_watchers = Some(watchers);
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

    /// The longest election timeout a group on this node waits before it
    /// campaigns.
    pub fn election_timeout_max(&self) -> Duration {
        self.election_timeout_max
    }

    /// How often a leader on this node sends heartbeats.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Configure the auto-compaction threshold for every group created on
    /// this node. `None` disables auto-compaction (the default). See
    /// [`nodedb_raft::node::RaftConfig::log_compaction_threshold`].
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

    /// A ticket for every storage write `group_id` staged so far, or `None`
    /// when all of them are durable or the group is not mounted. A caller
    /// takes it right after the Raft call whose writes a reply depends on,
    /// and awaits it after it releases this struct's lock.
    pub fn durability_ticket(&self, group_id: u64) -> Option<crate::group_disk::DurabilityTicket> {
        self.groups.get(&group_id)?.storage().ticket()
    }

    /// Block until every write each group staged so far is durable. For boot
    /// and tests, off the async threads: a running node waits on a
    /// [`Self::durability_ticket`] instead.
    pub fn wait_all_durable_blocking(&self) {
        for node in self.groups.values() {
            if let Some(ticket) = node.storage().ticket() {
                ticket.wait_blocking();
            }
        }
    }

    /// Tick all Raft groups. Returns aggregated ready output.
    ///
    /// Any HardState a tick changes (an election term bump + self-vote from
    /// an election timeout) is staged on the group's disk before the
    /// aggregated `Ready` returns. The caller awaits the group's
    /// [`Self::durability_ticket`] before it sends the vote requests that
    /// `Ready` carries, so no vote request leaves for a term that is not
    /// durable. A staging error aborts the tick.
    pub fn tick(&mut self) -> Result<MultiRaftReady> {
        self.collect_ready(true)
    }

    /// Collect the output every group has ready, without advancing any
    /// group's election or heartbeat clock. A leader counts the entries its
    /// disk made durable since the last call, so a commit the disk completed
    /// lands in this Ready.
    pub fn drive(&mut self) -> Result<MultiRaftReady> {
        self.collect_ready(false)
    }

    fn collect_ready(&mut self, advance_clock: bool) -> Result<MultiRaftReady> {
        let mut ready = MultiRaftReady::default();

        for (&group_id, node) in &mut self.groups {
            node.on_storage_progress();
            if advance_clock {
                node.tick();
            }
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

    /// Clone of the per-group apply gates. Every applier of committed entries
    /// and every snapshot install of this node goes through them.
    pub fn apply_gates(&self) -> Arc<crate::raft_loop::apply_gate::GroupApplyGates> {
        self.apply_gates.clone()
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
    pub fn groups_mut(&mut self) -> &mut HashMap<u64, RaftNode<StagedLogStorage>> {
        &mut self.groups
    }
}

/// Start `group_id`'s watcher at `restored`, the applied index the group
/// restored from storage. A fresh group restores 0 and seeds nothing.
pub(super) fn seed_watcher(watchers: &GroupAppliedWatchers, group_id: u64, restored: u64) {
    if restored > 0 {
        watchers.bump(group_id, restored);
    }
}

/// Whether `group_id` is a data group: neither the metadata group nor the
/// Calvin sequencer group.
pub(super) fn is_data_group(group_id: u64) -> bool {
    group_id != crate::metadata_group::METADATA_GROUP_ID
        && group_id != crate::calvin::SEQUENCER_GROUP_ID
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

        // The first tick elects every group's leader. Each leader's no-op
        // commits once its disk holds it, and the next tick delivers it.
        mr.tick().unwrap();
        mr.wait_all_durable_blocking();
        let ready = mr.tick().unwrap();
        assert_eq!(ready.groups.len(), 5);
    }

    /// Drive a single-node group to a leader that holds and has delivered its
    /// election no-op.
    fn single_node_leader(dir: &std::path::Path) -> MultiRaft {
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.to_path_buf());
        mr.add_group(0, vec![]).unwrap();
        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        mr.tick().unwrap();
        mr.wait_all_durable_blocking();
        for (gid, ready) in mr.tick().unwrap().groups {
            if let Some(last) = ready.committed_entries.last() {
                mr.advance_applied(gid, last.index).unwrap();
            }
        }
        mr
    }

    #[tokio::test]
    async fn a_durable_proposal_commits_on_drive_without_a_tick() {
        let dir = tempfile::tempdir().unwrap();
        let mut mr = single_node_leader(dir.path());
        let signal = mr.drive_signal();

        mr.propose_to_group(0, b"payload".to_vec()).unwrap();

        // The disk wakes the signal once the entry is durable. A permit left
        // by an earlier write can wake it first, so drive until it commits.
        let committed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                signal.notified().await;
                let ready = mr.drive().unwrap();
                if ready.groups.iter().any(|(_, r)| {
                    r.committed_entries
                        .iter()
                        .any(|entry| entry.data == b"payload")
                }) {
                    return;
                }
            }
        })
        .await;
        assert!(
            committed.is_ok(),
            "a durable proposal must commit without waiting for a tick"
        );
    }

    #[test]
    fn drive_does_not_advance_the_election_clock() {
        use nodedb_raft::NodeRole;
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();
        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        mr.drive().unwrap();
        assert_ne!(
            mr.groups[&0].role(),
            NodeRole::Leader,
            "drive must not run the election timer"
        );

        mr.tick().unwrap();
        assert_eq!(mr.groups[&0].role(), NodeRole::Leader);
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
    fn compaction_never_passes_the_ceiling() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr =
            MultiRaft::new(1, rt, dir.path().to_path_buf()).with_log_compaction_threshold(Some(1));
        mr.add_group(0, vec![]).unwrap();
        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        let ceiling = std::sync::Arc::new(AtomicU64::new(0));
        mr.set_compaction_ceiling(0, std::sync::Arc::clone(&ceiling));
        let apply = |mr: &mut MultiRaft| {
            mr.wait_all_durable_blocking();
            for (gid, ready) in mr.tick().unwrap().groups {
                if let Some(last) = ready.committed_entries.last() {
                    mr.advance_applied(gid, last.index).unwrap();
                    mr.save_applied_index(gid, last.index).unwrap();
                }
            }
        };
        mr.tick().unwrap();
        apply(&mut mr);
        for i in 0..5u8 {
            mr.propose_to_group(0, vec![i]).unwrap();
        }
        for _ in 0..3 {
            apply(&mut mr);
        }
        let applied = mr.groups[&0].last_applied();
        assert!(applied >= 5, "applied {applied}");

        assert!(!mr.maybe_compact_group(0, applied).unwrap());
        assert_eq!(mr.first_available_index(0), Some(1));

        ceiling.store(3, Ordering::Release);
        assert!(mr.maybe_compact_group(0, applied).unwrap());
        assert_eq!(mr.first_available_index(0), Some(4));
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
