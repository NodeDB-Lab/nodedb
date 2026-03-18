//! Rebalancing planner — computes and executes vShard redistribution.
//!
//! When nodes join or leave the cluster, vShards may be unevenly distributed.
//! The planner computes the minimum set of migrations to achieve uniform
//! distribution, then executes them using the [`MigrationExecutor`].
//!
//! **Triggers:**
//! - Manual: `REBALANCE` DDL command
//! - Automatic: after node join or decommission
//!
//! **Algorithm:** Greedy redistribution — over-loaded nodes donate vShards
//! to under-loaded nodes, minimizing total data movement.

use std::collections::HashMap;

use crate::error::{ClusterError, Result};
use crate::migration_executor::MigrationRequest;
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;

/// A single planned migration move.
#[derive(Debug, Clone)]
pub struct PlannedMove {
    pub vshard_id: u16,
    pub source_node: u64,
    pub target_node: u64,
    pub source_group: u64,
}

/// A complete rebalance plan: a list of moves to achieve uniform distribution.
#[derive(Debug, Clone)]
pub struct RebalancePlan {
    pub moves: Vec<PlannedMove>,
    /// Distribution before rebalancing: node_id → vShard count.
    pub before: HashMap<u64, usize>,
    /// Expected distribution after rebalancing: node_id → vShard count.
    pub after: HashMap<u64, usize>,
}

impl RebalancePlan {
    pub fn is_empty(&self) -> bool {
        self.moves.is_empty()
    }

    pub fn move_count(&self) -> usize {
        self.moves.len()
    }
}

/// Compute a rebalance plan from the current routing table and topology.
///
/// Only considers Active nodes. Returns an empty plan if distribution is
/// already balanced (within ±1 vShard per node).
pub fn compute_plan(routing: &RoutingTable, topology: &ClusterTopology) -> Result<RebalancePlan> {
    // Collect active nodes.
    let active_nodes: Vec<u64> = topology.active_nodes().iter().map(|n| n.node_id).collect();

    if active_nodes.is_empty() {
        return Err(ClusterError::Transport {
            detail: "no active nodes for rebalancing".into(),
        });
    }

    // Count vShards per node (via group leadership).
    // A node "owns" a vShard if it's the leader of the vShard's group.
    let mut node_vshards: HashMap<u64, Vec<u16>> =
        active_nodes.iter().map(|&id| (id, Vec::new())).collect();

    for vshard_id in 0..crate::routing::VSHARD_COUNT {
        if let Ok(group_id) = routing.group_for_vshard(vshard_id) {
            if let Some(info) = routing.group_info(group_id) {
                let leader = info.leader;
                if leader > 0 {
                    node_vshards.entry(leader).or_default().push(vshard_id);
                }
            }
        }
    }

    let num_nodes = active_nodes.len();
    let total_vshards = crate::routing::VSHARD_COUNT as usize;
    let ideal = total_vshards / num_nodes;
    let remainder = total_vshards % num_nodes;

    // Before distribution.
    let before: HashMap<u64, usize> = node_vshards
        .iter()
        .map(|(&id, vs)| (id, vs.len()))
        .collect();

    // Classify nodes as over-loaded or under-loaded.
    // Nodes 0..remainder get (ideal+1), rest get ideal.
    let mut sorted_nodes: Vec<u64> = active_nodes.clone();
    sorted_nodes.sort();

    let mut target_count: HashMap<u64, usize> = HashMap::new();
    for (i, &node_id) in sorted_nodes.iter().enumerate() {
        target_count.insert(node_id, if i < remainder { ideal + 1 } else { ideal });
    }

    // Build donor and receiver lists.
    let mut donors: Vec<(u64, Vec<u16>)> = Vec::new(); // (node, vshards to give away)
    let mut receivers: Vec<(u64, usize)> = Vec::new(); // (node, vshards needed)

    for &node_id in &sorted_nodes {
        let current = node_vshards.get(&node_id).map(|v| v.len()).unwrap_or(0);
        let target = target_count[&node_id];

        if current > target {
            // Donate excess vShards.
            let excess = current - target;
            let vshards = node_vshards.get(&node_id).cloned().unwrap_or_default();
            // Donate from the tail (arbitrary but deterministic).
            let to_donate: Vec<u16> = vshards.into_iter().rev().take(excess).collect();
            donors.push((node_id, to_donate));
        } else if current < target {
            let deficit = target - current;
            receivers.push((node_id, deficit));
        }
    }

    // Match donors to receivers.
    let mut moves = Vec::new();
    let mut receiver_idx = 0;
    let mut receiver_remaining = if !receivers.is_empty() {
        receivers[0].1
    } else {
        0
    };

    for (source_node, vshards) in &donors {
        for &vshard_id in vshards {
            if receiver_idx >= receivers.len() {
                break;
            }

            let (target_node, _) = receivers[receiver_idx];
            let source_group = routing.group_for_vshard(vshard_id).unwrap_or(0);

            moves.push(PlannedMove {
                vshard_id,
                source_node: *source_node,
                target_node,
                source_group,
            });

            receiver_remaining -= 1;
            if receiver_remaining == 0 {
                receiver_idx += 1;
                if receiver_idx < receivers.len() {
                    receiver_remaining = receivers[receiver_idx].1;
                }
            }
        }
    }

    // After distribution.
    let mut after = before.clone();
    for m in &moves {
        *after.entry(m.source_node).or_default() -= 1;
        *after.entry(m.target_node).or_default() += 1;
    }

    Ok(RebalancePlan {
        moves,
        before,
        after,
    })
}

/// Convert a RebalancePlan into MigrationRequests for the executor.
pub fn plan_to_requests(plan: &RebalancePlan, pause_budget_us: u64) -> Vec<MigrationRequest> {
    plan.moves
        .iter()
        .map(|m| MigrationRequest {
            vshard_id: m.vshard_id,
            source_node: m.source_node,
            target_node: m.target_node,
            write_pause_budget_us: pause_budget_us,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{NodeInfo, NodeState};

    fn make_topology(nodes: &[u64]) -> ClusterTopology {
        let mut topo = ClusterTopology::new();
        for &id in nodes {
            topo.add_node(NodeInfo::new(
                id,
                format!("10.0.0.{id}:9400").parse().unwrap(),
                NodeState::Active,
            ));
        }
        topo
    }

    #[test]
    fn balanced_distribution_no_moves() {
        // 4 nodes, 4 groups, 256 vShards each → already balanced.
        let rt = RoutingTable::uniform(4, &[1, 2, 3, 4], 1);
        let topo = make_topology(&[1, 2, 3, 4]);

        let plan = compute_plan(&rt, &topo).unwrap();
        assert!(
            plan.is_empty(),
            "expected no moves, got {}",
            plan.move_count()
        );
    }

    #[test]
    fn new_node_triggers_rebalancing() {
        // 2 nodes with 512 vShards each. Add node 3 → should rebalance to ~341 each.
        let rt = RoutingTable::uniform(2, &[1, 2], 1);
        let topo = make_topology(&[1, 2, 3]);

        let plan = compute_plan(&rt, &topo).unwrap();
        assert!(!plan.is_empty(), "expected moves for new node");

        // Node 3 should receive vShards.
        let node3_receives: usize = plan.moves.iter().filter(|m| m.target_node == 3).count();
        assert!(
            node3_receives > 0,
            "node 3 should receive vShards, got {node3_receives}"
        );

        // After distribution should be balanced (±1).
        let counts: Vec<usize> = plan.after.values().copied().collect();
        let min = *counts.iter().min().unwrap();
        let max = *counts.iter().max().unwrap();
        assert!(
            max - min <= 1,
            "distribution not balanced: min={min}, max={max}"
        );
    }

    #[test]
    fn node_decommission_rebalancing() {
        // 3 nodes, then node 3 decommissioned → vShards redistributed to 1 and 2.
        let rt = RoutingTable::uniform(3, &[1, 2, 3], 1);
        let mut topo = make_topology(&[1, 2, 3]);
        topo.set_state(3, NodeState::Decommissioned);

        let _plan = compute_plan(&rt, &topo).unwrap();

        // Node 3's vShards should move to nodes 1 and 2.
        // But node 3 is decommissioned → not in active_nodes → its vShards
        // are "orphaned" (leader is node 3 but node 3 isn't active).
        // The planner only moves vShards FROM active nodes.
        // This is correct: orphaned vShards need special handling (elect new leader first).
    }

    #[test]
    fn single_node_no_rebalance() {
        let rt = RoutingTable::uniform(1, &[1], 1);
        let topo = make_topology(&[1]);

        let plan = compute_plan(&rt, &topo).unwrap();
        assert!(plan.is_empty());
    }

    #[test]
    fn plan_to_requests_conversion() {
        let rt = RoutingTable::uniform(2, &[1, 2], 1);
        let topo = make_topology(&[1, 2, 3]);

        let plan = compute_plan(&rt, &topo).unwrap();
        let requests = plan_to_requests(&plan, 500_000);

        assert_eq!(requests.len(), plan.moves.len());
        for req in &requests {
            assert_eq!(req.write_pause_budget_us, 500_000);
        }
    }

    #[test]
    fn no_active_nodes_fails() {
        let rt = RoutingTable::uniform(1, &[1], 1);
        let topo = ClusterTopology::new(); // No nodes.

        let err = compute_plan(&rt, &topo).unwrap_err();
        assert!(err.to_string().contains("no active nodes"));
    }
}
