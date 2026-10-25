// SPDX-License-Identifier: BUSL-1.1

//! The vShards whose data this node holds as a replica.
//!
//! A vShard is resident here while its data group:
//! - is mounted here: its replica applies the group's log;
//! - lists this node as a voter or a learner;
//! - names this node in its placement: a replica of it is entering here.
//!
//! A vShard outside all three left this node. Its new owners serve it, so
//! the copy here is stale.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use nodedb_cluster::RoutingTable;

/// The vShards resident on `node_id`, and the data group of every vShard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidencyView {
    pub resident: BTreeSet<u32>,
    /// The data group each vShard routes to.
    pub group_of: BTreeMap<u32, u64>,
}

impl ResidencyView {
    /// The residency of `node_id`, which mounts the groups `mounted`, under
    /// `routing`.
    pub fn of(node_id: u64, mounted: &HashSet<u64>, routing: &RoutingTable) -> Self {
        let mut groups: BTreeSet<u64> = mounted.iter().copied().collect();
        for (group_id, info) in routing.group_members() {
            let placed = info
                .placement
                .as_ref()
                .is_some_and(|placement| placement.contains(&node_id));
            if placed || info.members.contains(&node_id) || info.learners.contains(&node_id) {
                groups.insert(*group_id);
            }
        }
        let resident = groups
            .iter()
            .flat_map(|group_id| routing.vshards_for_group(*group_id))
            .collect();
        let group_of = routing
            .vshard_to_group()
            .iter()
            .enumerate()
            .filter_map(|(vshard, group)| u32::try_from(vshard).ok().map(|v| (v, *group)))
            .collect();
        Self { resident, group_of }
    }

    /// Whether `vshard_id` is resident here.
    pub fn holds(&self, vshard_id: u32) -> bool {
        self.resident.contains(&vshard_id)
    }

    /// The data groups of `vshards`.
    pub fn groups_of(&self, vshards: &[u32]) -> BTreeSet<u64> {
        vshards
            .iter()
            .filter_map(|vshard| self.group_of.get(vshard).copied())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Data groups 1 and 2 over nodes 1..=3, every node a voter of both.
    fn table() -> RoutingTable {
        RoutingTable::uniform(2, &[1, 2, 3], 3)
    }

    fn vshards(routing: &RoutingTable, group_id: u64) -> BTreeSet<u32> {
        routing.vshards_for_group(group_id).into_iter().collect()
    }

    #[test]
    fn a_voter_holds_every_vshard_of_its_groups() {
        let rt = table();
        let residency = ResidencyView::of(1, &HashSet::new(), &rt);
        let all: BTreeSet<u32> = vshards(&rt, 1).union(&vshards(&rt, 2)).copied().collect();
        assert_eq!(residency.resident, all);
    }

    #[test]
    fn a_left_group_leaves_its_vshards() {
        let mut rt = table();
        rt.set_placement(1, vec![2, 3]);
        rt.set_group_members(1, vec![2, 3]);
        let residency = ResidencyView::of(1, &HashSet::new(), &rt);
        assert_eq!(residency.resident, vshards(&rt, 2));
        let left: Vec<u32> = vshards(&rt, 1).into_iter().collect();
        assert_eq!(residency.groups_of(&left), BTreeSet::from([1]));
    }

    #[test]
    fn a_mount_a_learner_or_a_placement_keeps_the_vshards() {
        let mut rt = table();
        rt.set_group_members(1, vec![2, 3]);
        // Still mounted: the replica applies the group's log.
        let mounted = HashSet::from([1]);
        assert!(
            ResidencyView::of(1, &mounted, &rt)
                .resident
                .is_superset(&vshards(&rt, 1))
        );
        // A learner replicates the group.
        rt.set_group_learners(1, vec![1]);
        assert!(
            ResidencyView::of(1, &HashSet::new(), &rt)
                .resident
                .is_superset(&vshards(&rt, 1))
        );
        // A placement naming the node: a replica is entering.
        rt.set_group_learners(1, Vec::new());
        rt.set_placement(1, vec![1, 2, 3]);
        assert!(
            ResidencyView::of(1, &HashSet::new(), &rt)
                .resident
                .is_superset(&vshards(&rt, 1))
        );
    }
}
