//! Load-imbalance plan computation.
//!
//! Given a snapshot of per-node `LoadMetrics` and the current routing
//! table, decide whether the cluster is imbalanced enough to justify
//! moves and, if so, emit a bounded list of `PlannedMove`s from the
//! hottest nodes to the coldest ones.
//!
//! ## Trigger
//!
//! The rebalancer fires when, after normalizing every node's score:
//!
//! > `max - min  >  threshold_pct / 100  *  mean`
//!
//! ...i.e. the hottest node is more than `threshold_pct`% above the
//! cluster mean relative to the coldest one. This is intentionally
//! not a per-node check: single-hot-node scenarios below the
//! cluster mean delta are handled by the separate
//! `rebalance_scheduler` CPU/queue triggers.
//!
//! ## Move selection
//!
//! For each hot→cold pair, the planner walks the routing table in
//! stable (sorted by group_id, then vshard_id) order and picks
//! vshards the hot node is currently leading. It caps moves at
//! `max_moves_per_group` moves from any single group (so one
//! over-replicated group can't consume the entire in-flight budget)
//! and at `max_moves_total` across the whole plan (so the dispatcher
//! never has more than that many migrations in flight at once).
//!
//! Determinism: the plan is deterministic given the same inputs,
//! including tie-breaks. Two nodes computing the plan at the same
//! instant produce byte-identical outputs.

use std::collections::HashMap;

use tracing::debug;

use crate::rebalance::PlannedMove;
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;

use super::metrics::{LoadMetrics, LoadWeights, normalized_score};

/// Configuration for [`compute_load_based_plan`].
#[derive(Debug, Clone)]
pub struct RebalancerPlanConfig {
    /// If `(max - min) > (threshold_pct / 100) * mean`, we plan moves.
    /// Default: 20%.
    pub imbalance_threshold_pct: u8,
    /// Maximum moves from any single Raft group per plan. Default 1.
    pub max_moves_per_group: usize,
    /// Maximum moves in the entire plan. Default 10.
    pub max_moves_total: usize,
    /// Weights applied to the load dimensions when scoring.
    pub weights: LoadWeights,
}

impl Default for RebalancerPlanConfig {
    fn default() -> Self {
        Self {
            imbalance_threshold_pct: 20,
            max_moves_per_group: 1,
            max_moves_total: 10,
            weights: LoadWeights::default(),
        }
    }
}

/// Compute a load-driven rebalance plan. Returns an empty vector if
/// the cluster is already within the imbalance threshold or if there
/// are fewer than two nodes to compare.
pub fn compute_load_based_plan(
    metrics: &[LoadMetrics],
    routing: &RoutingTable,
    topology: &ClusterTopology,
    cfg: &RebalancerPlanConfig,
) -> Vec<PlannedMove> {
    if metrics.len() < 2 {
        return Vec::new();
    }

    // Score every node, then sort ascending so the hot list and cold
    // list are natural slices. `f64` isn't Ord, so use total_cmp for
    // NaN-free deterministic ordering.
    let mut scored: Vec<(u64, f64)> = metrics
        .iter()
        .map(|m| (m.node_id, normalized_score(m, &cfg.weights)))
        .collect();
    scored.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

    let min = scored.first().map(|(_, s)| *s).unwrap_or(0.0);
    let max = scored.last().map(|(_, s)| *s).unwrap_or(0.0);
    let mean: f64 = scored.iter().map(|(_, s)| *s).sum::<f64>() / scored.len() as f64;

    // Imbalance gate. A zero-mean cluster (everything idle) is
    // considered already balanced — nothing to move.
    if mean <= 0.0 {
        return Vec::new();
    }
    let threshold = (cfg.imbalance_threshold_pct as f64 / 100.0) * mean;
    if (max - min) <= threshold {
        debug!(
            max,
            min, mean, threshold, "rebalancer: cluster within imbalance threshold"
        );
        return Vec::new();
    }

    // Only Active nodes are valid migration targets. Cold candidates
    // must be Active and must not already be the source for a move.
    let active_set: std::collections::HashSet<u64> =
        topology.active_nodes().iter().map(|n| n.node_id).collect();

    // Hot = strictly above mean; cold = strictly below mean. Using
    // the mean as the split point (rather than index-based halving)
    // correctly handles asymmetric distributions where a single
    // outlier pulls one node above an otherwise balanced cluster —
    // the below-mean nodes stay in the cold set even if they tie
    // with each other.
    let hot_nodes: Vec<u64> = scored
        .iter()
        .rev() // hottest first
        .filter(|(_, s)| *s > mean)
        .map(|(id, _)| *id)
        .collect();
    let cold_nodes: Vec<u64> = scored
        .iter()
        .filter(|(_, s)| *s < mean)
        .filter(|(id, _)| active_set.contains(id))
        .map(|(id, _)| *id)
        .collect();

    if cold_nodes.is_empty() {
        return Vec::new();
    }

    // Walk routing in stable order — group id ascending, then vshard
    // id ascending — and pick moves until we hit the caps.
    let mut group_ids: Vec<u64> = routing.group_members().keys().copied().collect();
    group_ids.sort_unstable();

    let mut moves: Vec<PlannedMove> = Vec::new();
    let mut per_group_count: HashMap<u64, usize> = HashMap::new();
    let mut cold_cursor = 0usize;

    'outer: for hot in &hot_nodes {
        if !active_set.contains(hot) {
            continue;
        }
        for &gid in &group_ids {
            if moves.len() >= cfg.max_moves_total {
                break 'outer;
            }
            let info = match routing.group_info(gid) {
                Some(i) => i,
                None => continue,
            };
            if info.leader != *hot {
                continue;
            }
            if *per_group_count.get(&gid).unwrap_or(&0) >= cfg.max_moves_per_group {
                continue;
            }
            // Pick the group's lowest vshard id deterministically.
            let mut vshards = routing.vshards_for_group(gid);
            vshards.sort_unstable();
            let Some(&vshard_id) = vshards.first() else {
                continue;
            };
            let target = cold_nodes[cold_cursor % cold_nodes.len()];
            if target == *hot {
                continue;
            }
            moves.push(PlannedMove {
                vshard_id,
                source_node: *hot,
                target_node: target,
                source_group: gid,
            });
            *per_group_count.entry(gid).or_default() += 1;
            cold_cursor += 1;
        }
    }

    moves
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{NodeInfo, NodeState};
    use std::net::SocketAddr;

    fn topo(nodes: &[u64]) -> ClusterTopology {
        let mut t = ClusterTopology::new();
        for (i, id) in nodes.iter().enumerate() {
            let a: SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            t.add_node(NodeInfo::new(*id, a, NodeState::Active));
        }
        t
    }

    fn lm(id: u64, v: u32, bytes_mib: u64, w: f64, r: f64) -> LoadMetrics {
        LoadMetrics {
            node_id: id,
            vshards_led: v,
            bytes_stored: bytes_mib * 1_048_576,
            writes_per_sec: w,
            reads_per_sec: r,
            qps_recent: 0.0,
            p95_latency_us: 0,
            cpu_utilization: 0.0,
        }
    }

    #[test]
    fn empty_metrics_returns_empty_plan() {
        let t = topo(&[1, 2]);
        let r = RoutingTable::uniform(2, &[1, 2], 1);
        let plan = compute_load_based_plan(&[], &r, &t, &RebalancerPlanConfig::default());
        assert!(plan.is_empty());
    }

    #[test]
    fn single_node_returns_empty_plan() {
        let t = topo(&[1]);
        let r = RoutingTable::uniform(1, &[1], 1);
        let plan = compute_load_based_plan(
            &[lm(1, 100, 100, 100.0, 100.0)],
            &r,
            &t,
            &RebalancerPlanConfig::default(),
        );
        assert!(plan.is_empty());
    }

    #[test]
    fn balanced_cluster_no_moves() {
        let t = topo(&[1, 2, 3]);
        let r = RoutingTable::uniform(3, &[1, 2, 3], 1);
        let metrics = vec![
            lm(1, 10, 100, 50.0, 50.0),
            lm(2, 10, 100, 50.0, 50.0),
            lm(3, 10, 100, 50.0, 50.0),
        ];
        let plan = compute_load_based_plan(&metrics, &r, &t, &RebalancerPlanConfig::default());
        assert!(plan.is_empty());
    }

    #[test]
    fn imbalance_above_threshold_triggers_moves() {
        let t = topo(&[1, 2, 3]);
        let r = RoutingTable::uniform(6, &[1, 2, 3], 1);
        // Node 1 massively overloaded.
        let metrics = vec![
            lm(1, 200, 1000, 500.0, 500.0),
            lm(2, 10, 50, 25.0, 25.0),
            lm(3, 10, 50, 25.0, 25.0),
        ];
        let plan = compute_load_based_plan(&metrics, &r, &t, &RebalancerPlanConfig::default());
        assert!(!plan.is_empty());
        // Every move must source from node 1.
        for m in &plan {
            assert_eq!(m.source_node, 1);
        }
    }

    #[test]
    fn plan_respects_max_moves_total() {
        let t = topo(&[1, 2]);
        // 20 groups so node 1 can lead many.
        let mut r = RoutingTable::uniform(20, &[1, 2], 1);
        for gid in 0..20 {
            r.set_leader(gid, 1);
        }
        let metrics = vec![lm(1, 2000, 10_000, 5000.0, 5000.0), lm(2, 1, 1, 1.0, 1.0)];
        let cfg = RebalancerPlanConfig {
            max_moves_total: 4,
            max_moves_per_group: 1,
            ..Default::default()
        };
        let plan = compute_load_based_plan(&metrics, &r, &t, &cfg);
        assert_eq!(plan.len(), 4);
    }

    #[test]
    fn plan_respects_max_moves_per_group() {
        let t = topo(&[1, 2]);
        let mut r = RoutingTable::uniform(3, &[1, 2], 1);
        for gid in 0..3 {
            r.set_leader(gid, 1);
        }
        let metrics = vec![lm(1, 2000, 10_000, 5000.0, 5000.0), lm(2, 1, 1, 1.0, 1.0)];
        let cfg = RebalancerPlanConfig {
            max_moves_total: 99,
            max_moves_per_group: 1,
            ..Default::default()
        };
        let plan = compute_load_based_plan(&metrics, &r, &t, &cfg);
        // With max_moves_per_group=1 and 3 groups, at most 3 moves.
        assert!(plan.len() <= 3);
        let mut by_group: HashMap<u64, usize> = HashMap::new();
        for m in &plan {
            *by_group.entry(m.source_group).or_default() += 1;
        }
        for (_, count) in by_group {
            assert!(count <= 1);
        }
    }

    #[test]
    fn plan_is_deterministic() {
        let t = topo(&[1, 2, 3]);
        let mut r = RoutingTable::uniform(6, &[1, 2, 3], 1);
        for gid in 0..6 {
            r.set_leader(gid, 1);
        }
        let metrics = vec![
            lm(1, 500, 5000, 200.0, 200.0),
            lm(2, 5, 5, 5.0, 5.0),
            lm(3, 5, 5, 5.0, 5.0),
        ];
        let cfg = RebalancerPlanConfig::default();
        let p1 = compute_load_based_plan(&metrics, &r, &t, &cfg);
        let p2 = compute_load_based_plan(&metrics, &r, &t, &cfg);
        let p1_tuples: Vec<_> = p1
            .iter()
            .map(|m| (m.vshard_id, m.source_node, m.target_node, m.source_group))
            .collect();
        let p2_tuples: Vec<_> = p2
            .iter()
            .map(|m| (m.vshard_id, m.source_node, m.target_node, m.source_group))
            .collect();
        assert_eq!(p1_tuples, p2_tuples);
    }

    #[test]
    fn idle_cluster_never_triggers() {
        let t = topo(&[1, 2, 3]);
        let r = RoutingTable::uniform(3, &[1, 2, 3], 1);
        let metrics = vec![
            lm(1, 0, 0, 0.0, 0.0),
            lm(2, 0, 0, 0.0, 0.0),
            lm(3, 0, 0, 0.0, 0.0),
        ];
        let plan = compute_load_based_plan(&metrics, &r, &t, &RebalancerPlanConfig::default());
        assert!(plan.is_empty());
    }

    #[test]
    fn cold_node_must_be_active() {
        // Node 3 is not Active (it's Draining) → cannot receive.
        let mut t = topo(&[1, 2, 3]);
        t.set_state(3, NodeState::Draining);
        let mut r = RoutingTable::uniform(2, &[1, 2, 3], 1);
        r.set_leader(0, 1);
        r.set_leader(1, 1);
        let metrics = vec![
            lm(1, 500, 5000, 200.0, 200.0),
            lm(2, 5, 5, 5.0, 5.0),
            lm(3, 0, 0, 0.0, 0.0),
        ];
        let plan = compute_load_based_plan(&metrics, &r, &t, &RebalancerPlanConfig::default());
        for m in &plan {
            assert_ne!(m.target_node, 3, "Draining node must not receive moves");
        }
    }
}
