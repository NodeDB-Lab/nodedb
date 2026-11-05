// SPDX-License-Identifier: BUSL-1.1

//! The vShards this node leads, with the Raft group and term of each.
//!
//! Each Raft group's leader counts its vShards' stored bytes, so the cluster
//! sums each vShard once. Every replica stores the same bytes. Counting on the
//! leader alone keeps replicas out of the total.

use std::collections::HashMap;

use nodedb_cluster::RoutingTable;

use crate::control::gateway::live_leaders::LiveLeaders;
use crate::control::state::SharedState;
use crate::types::VShardId;

/// The group a node with no cluster routing files every vShard under. Such a
/// node has no peers, so no other report names this group.
pub const LOCAL_GROUP: LedGroup = LedGroup {
    group_id: 0,
    term: 0,
};

/// A Raft group this node leads, at the term it leads in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LedGroup {
    pub group_id: u64,
    /// The leader term. `0` when the leader source carries no term.
    pub term: u64,
}

/// The vShards this node leads, and the group of each.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LedSet {
    /// The led vShards, in ascending order.
    pub vshards: Vec<u32>,
    group_of: HashMap<u32, LedGroup>,
}

impl LedSet {
    /// The group of led vShard `vshard`, `None` when this node does not lead it.
    pub fn group_of(&self, vshard: u32) -> Option<LedGroup> {
        self.group_of.get(&vshard).copied()
    }

    /// Every led group, in ascending group order, without repeats.
    pub fn groups(&self) -> Vec<LedGroup> {
        let mut groups: Vec<LedGroup> = self.group_of.values().copied().collect();
        groups.sort_unstable_by_key(|group| group.group_id);
        groups.dedup();
        groups
    }

    fn insert(&mut self, vshard: u32, group: LedGroup) {
        self.vshards.push(vshard);
        self.group_of.insert(vshard, group);
    }
}

/// The vShards whose Raft group this node leads. A node with no cluster
/// routing leads every vShard, under [`LOCAL_GROUP`].
///
/// The live Raft snapshot is taken before the routing guard. See
/// [`LiveLeaders`] for the lock order.
pub fn led_vshards(state: &SharedState) -> LedSet {
    let Some(routing_lock) = state.cluster_routing.as_ref() else {
        let mut led = LedSet::default();
        for vshard in 0..VShardId::COUNT {
            led.insert(vshard, LOCAL_GROUP);
        }
        return led;
    };
    let live = LiveLeaders::snapshot(state);
    let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
    led_by(&routing, state.node_id, &live)
}

/// The vShards of `routing` whose group `node_id` leads.
///
/// The group's live leader and its live term decide. The routing-table hint
/// and its term decide only when no Raft status source is wired. A group with
/// no known leader is led by no node, so no node counts it until an election
/// ends.
fn led_by(routing: &RoutingTable, node_id: u64, live: &LiveLeaders) -> LedSet {
    let mut leads: HashMap<u64, Option<LedGroup>> = HashMap::new();
    let mut led = LedSet::default();
    for vshard in 0..VShardId::COUNT {
        let Ok(group_id) = routing.group_for_vshard(vshard) else {
            continue;
        };
        let group = *leads.entry(group_id).or_insert_with(|| {
            let (leader, term) = if live.is_wired() {
                (live.leader_of(group_id), live.term_of(group_id))
            } else {
                routing
                    .group_info(group_id)
                    .map_or((0, 0), |info| (info.leader, info.leader_term))
            };
            (leader != 0 && leader == node_id).then_some(LedGroup { group_id, term })
        });
        if let Some(group) = group {
            led.insert(vshard, group);
        }
    }
    led
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use nodedb_cluster::GroupStatus;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn state(dir: &std::path::Path) -> Arc<SharedState> {
        let wal = Arc::new(WalManager::open_for_testing(&dir.join("led.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("shared state")
    }

    /// A node with no cluster routing leads every vShard.
    #[test]
    fn a_single_node_leads_every_vshard() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = state(dir.path());
        let expected: Vec<u32> = (0..VShardId::COUNT).collect();
        let led = led_vshards(&state);
        assert_eq!(led.vshards, expected);
        assert_eq!(led.groups(), vec![LOCAL_GROUP]);
    }

    /// With no Raft status source, the routing-table leader hint decides.
    #[test]
    fn the_hint_decides_without_a_status_source() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut state = state(dir.path());
        // Two data groups at RF 2 on nodes 1 and 2. Group 1 holds the even
        // vShards and group 2 the odd ones.
        let mut table = RoutingTable::uniform(2, &[1, 2], 2);
        let groups: Vec<u64> = table.group_ids();
        let even_group = table.group_for_vshard(0).expect("vShard 0 group");
        for group_id in groups {
            let leader = if group_id == even_group { 1 } else { 2 };
            table.set_leader(group_id, leader);
        }
        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.cluster_routing = Some(Arc::new(RwLock::new(table)));
        shared.node_id = 1;

        let even: Vec<u32> = (0..VShardId::COUNT).filter(|v| v % 2 == 0).collect();
        let led = led_vshards(&state);
        assert_eq!(led.vshards, even);
        assert_eq!(
            led.group_of(0).map(|group| group.group_id),
            Some(even_group)
        );
        assert_eq!(led.group_of(1), None);
    }

    /// With a status source, the live leader decides over the hint, and a
    /// group with no live leader is led by no node.
    #[test]
    fn the_live_leader_decides_over_the_hint() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut state = state(dir.path());
        // Three data groups. Group `1 + v % 3` holds vShard `v`.
        let mut table = RoutingTable::uniform(3, &[1, 2], 2);
        // The hint names node 1 for every group.
        for group_id in [1, 2, 3] {
            table.set_leader(group_id, 1);
        }
        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.cluster_routing = Some(Arc::new(RwLock::new(table)));
        shared.node_id = 1;
        // Raft knows node 2 leads group 1, node 1 leads group 2, and no leader
        // of group 3.
        let status: Arc<dyn Fn() -> Vec<GroupStatus> + Send + Sync> =
            Arc::new(|| vec![status(1, 2, 4), status(2, 1, 9)]);
        assert!(shared.raft_status_fn.set(status).is_ok());

        let expected: Vec<u32> = (0..VShardId::COUNT).filter(|v| v % 3 == 1).collect();
        let led = led_vshards(&state);
        assert_eq!(led.vshards, expected);
        // The led group carries the live term.
        assert_eq!(
            led.groups(),
            vec![LedGroup {
                group_id: 2,
                term: 9
            }]
        );
    }

    fn status(group_id: u64, leader_id: u64, term: u64) -> GroupStatus {
        GroupStatus {
            group_id,
            role: "Follower".into(),
            leader_id,
            term,
            commit_index: 0,
            last_applied: 0,
            last_log_index: 0,
            snapshot_index: 0,
            replica_state: nodedb_raft::ReplicaState::Whole,
            member_count: 2,
            learner_count: 0,
            vshard_count: 0,
        }
    }
}
