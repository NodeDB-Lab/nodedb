//! Rebalance scheduler: automatic trigger-based shard redistribution.
//!
//! Monitors cluster health metrics and triggers rebalancing when:
//! - CPU utilization exceeds threshold on any node
//! - SPSC queue pressure exceeds threshold
//! - Shard size imbalance exceeds threshold
//!
//! Enforces tenant fairness: no single tenant's migration can consume
//! more than `max_concurrent_migrations_per_tenant` slots.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tracing::{debug, info};

use crate::error::Result;
use crate::rebalance;
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;

/// Rebalance scheduler configuration.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// CPU utilization threshold (0-100) to trigger rebalancing.
    pub cpu_threshold: u8,
    /// SPSC queue utilization threshold (0-100).
    pub queue_threshold: u8,
    /// Shard count imbalance ratio: trigger if max/min > this.
    pub imbalance_ratio: f64,
    /// Minimum interval between rebalance checks.
    pub check_interval: Duration,
    /// Maximum concurrent migrations cluster-wide.
    pub max_concurrent_migrations: usize,
    /// Maximum concurrent migrations per tenant.
    pub max_concurrent_per_tenant: usize,
    /// Cooldown after a rebalance completes before allowing another.
    pub cooldown: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            cpu_threshold: 80,
            queue_threshold: 85,
            imbalance_ratio: 1.5,
            check_interval: Duration::from_secs(60),
            max_concurrent_migrations: 4,
            max_concurrent_per_tenant: 2,
            cooldown: Duration::from_secs(300),
        }
    }
}

/// Per-node health metrics snapshot.
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    pub node_id: u64,
    pub cpu_utilization: u8,
    pub queue_utilization: u8,
    pub shard_count: usize,
    /// Total data size in bytes across all shards on this node.
    pub total_data_bytes: u64,
}

/// Trigger reasons for rebalancing.
#[derive(Debug, Clone, PartialEq)]
pub enum RebalanceTrigger {
    /// CPU overloaded on a node.
    CpuOverload { node_id: u64, utilization: u8 },
    /// SPSC queue pressure on a node.
    QueuePressure { node_id: u64, utilization: u8 },
    /// Shard count imbalance across nodes.
    ShardImbalance { ratio: f64 },
    /// Manual trigger (admin command).
    Manual,
    /// Node join/leave triggered automatic rebalance.
    MembershipChange,
}

/// Rebalance scheduler state.
pub struct RebalanceScheduler {
    config: SchedulerConfig,
    /// Last time a rebalance check was performed.
    last_check: Option<Instant>,
    /// Last time a rebalance completed.
    last_rebalance: Option<Instant>,
    /// Currently active migrations: `(vshard_id → tenant_id)`.
    active_migrations: HashMap<u16, u32>,
}

impl RebalanceScheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            last_check: None,
            last_rebalance: None,
            active_migrations: HashMap::new(),
        }
    }

    /// Evaluate cluster health and determine if rebalancing is needed.
    ///
    /// Returns the trigger reason if rebalancing should start, or None.
    pub fn evaluate(&mut self, metrics: &[NodeMetrics]) -> Option<RebalanceTrigger> {
        let now = Instant::now();

        // Respect check interval.
        if let Some(last) = self.last_check
            && now.duration_since(last) < self.config.check_interval
        {
            return None;
        }
        self.last_check = Some(now);

        // Respect cooldown.
        if let Some(last) = self.last_rebalance
            && now.duration_since(last) < self.config.cooldown
        {
            return None;
        }

        // Respect max concurrent migrations.
        if self.active_migrations.len() >= self.config.max_concurrent_migrations {
            debug!(
                active = self.active_migrations.len(),
                max = self.config.max_concurrent_migrations,
                "rebalance: max concurrent migrations reached"
            );
            return None;
        }

        // Check CPU overload.
        for m in metrics {
            if m.cpu_utilization >= self.config.cpu_threshold {
                info!(
                    node_id = m.node_id,
                    cpu = m.cpu_utilization,
                    threshold = self.config.cpu_threshold,
                    "rebalance triggered: CPU overload"
                );
                return Some(RebalanceTrigger::CpuOverload {
                    node_id: m.node_id,
                    utilization: m.cpu_utilization,
                });
            }
        }

        // Check queue pressure.
        for m in metrics {
            if m.queue_utilization >= self.config.queue_threshold {
                return Some(RebalanceTrigger::QueuePressure {
                    node_id: m.node_id,
                    utilization: m.queue_utilization,
                });
            }
        }

        // Check shard count imbalance.
        if metrics.len() >= 2 {
            let max_shards = metrics.iter().map(|m| m.shard_count).max().unwrap_or(0);
            let min_shards = metrics
                .iter()
                .map(|m| m.shard_count)
                .min()
                .unwrap_or(1)
                .max(1);
            let ratio = max_shards as f64 / min_shards as f64;
            if ratio > self.config.imbalance_ratio {
                return Some(RebalanceTrigger::ShardImbalance { ratio });
            }
        }

        None
    }

    /// Record that a migration has started.
    pub fn migration_started(&mut self, vshard_id: u16, tenant_id: u32) {
        self.active_migrations.insert(vshard_id, tenant_id);
    }

    /// Record that a migration has completed.
    pub fn migration_completed(&mut self, vshard_id: u16) {
        self.active_migrations.remove(&vshard_id);
        if self.active_migrations.is_empty() {
            self.last_rebalance = Some(Instant::now());
        }
    }

    /// Check if a specific tenant can start another migration.
    pub fn can_migrate_tenant(&self, tenant_id: u32) -> bool {
        let tenant_count = self
            .active_migrations
            .values()
            .filter(|&&t| t == tenant_id)
            .count();
        tenant_count < self.config.max_concurrent_per_tenant
    }

    /// Number of currently active migrations.
    pub fn active_count(&self) -> usize {
        self.active_migrations.len()
    }

    /// Trigger a manual rebalance (bypasses cooldown).
    pub fn trigger_manual(&mut self) -> RebalanceTrigger {
        self.last_rebalance = None; // Clear cooldown.
        RebalanceTrigger::Manual
    }

    /// Trigger rebalance due to membership change (bypasses cooldown).
    pub fn trigger_membership_change(&mut self) -> RebalanceTrigger {
        self.last_rebalance = None;
        RebalanceTrigger::MembershipChange
    }
}

/// Plan a hot shard split: split an overloaded shard into two.
///
/// The overloaded shard's vShards are redistributed: half stay on the
/// original node, half migrate to the least-loaded node. No full-cluster
/// pause — only the affected vShards are briefly paused during cut-over.
pub fn plan_hot_split(
    overloaded_group: u64,
    routing: &RoutingTable,
    topology: &ClusterTopology,
) -> Result<Vec<rebalance::PlannedMove>> {
    let vshards = routing.vshards_for_group(overloaded_group);
    if vshards.len() <= 1 {
        return Ok(Vec::new()); // Can't split a single vShard.
    }

    // Find the least-loaded active node.
    let active = topology.active_nodes();
    if active.len() < 2 {
        return Ok(Vec::new()); // Need at least 2 nodes to split.
    }

    // Count vShards per node.
    let mut node_loads: HashMap<u64, usize> = HashMap::new();
    for group_id in routing.group_ids() {
        if let Some(info) = routing.group_info(group_id) {
            let count = routing.vshards_for_group(group_id).len();
            *node_loads.entry(info.leader).or_default() += count;
        }
    }

    let current_leader = routing
        .group_info(overloaded_group)
        .map(|g| g.leader)
        .unwrap_or(0);

    let target_node = active
        .iter()
        .filter(|n| n.node_id != current_leader)
        .min_by_key(|n| node_loads.get(&n.node_id).unwrap_or(&0))
        .map(|n| n.node_id)
        .unwrap_or(0);

    if target_node == 0 {
        return Ok(Vec::new());
    }

    // Split: move half the vShards to the target node.
    let half = vshards.len() / 2;
    let moves: Vec<rebalance::PlannedMove> = vshards[..half]
        .iter()
        .map(|&vshard_id| rebalance::PlannedMove {
            vshard_id,
            source_node: current_leader,
            target_node,
            source_group: overloaded_group,
        })
        .collect();

    info!(
        overloaded_group,
        source = current_leader,
        target = target_node,
        vshards_to_move = moves.len(),
        "hot shard split planned"
    );
    Ok(moves)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{NodeInfo, NodeState};
    use std::net::SocketAddr;

    fn make_metrics(nodes: &[(u64, u8, u8, usize)]) -> Vec<NodeMetrics> {
        nodes
            .iter()
            .map(|&(id, cpu, queue, shards)| NodeMetrics {
                node_id: id,
                cpu_utilization: cpu,
                queue_utilization: queue,
                shard_count: shards,
                total_data_bytes: 0,
            })
            .collect()
    }

    #[test]
    fn cpu_overload_triggers() {
        let mut sched = RebalanceScheduler::new(SchedulerConfig::default());
        let metrics = make_metrics(&[(1, 90, 50, 100), (2, 30, 20, 100)]);
        let trigger = sched.evaluate(&metrics);
        assert!(matches!(
            trigger,
            Some(RebalanceTrigger::CpuOverload { node_id: 1, .. })
        ));
    }

    #[test]
    fn balanced_cluster_no_trigger() {
        let mut sched = RebalanceScheduler::new(SchedulerConfig::default());
        let metrics = make_metrics(&[(1, 50, 40, 100), (2, 45, 35, 100)]);
        let trigger = sched.evaluate(&metrics);
        assert!(trigger.is_none());
    }

    #[test]
    fn imbalance_triggers() {
        let mut sched = RebalanceScheduler::new(SchedulerConfig {
            imbalance_ratio: 1.5,
            ..Default::default()
        });
        // 200 vs 50 = ratio 4.0 > 1.5.
        let metrics = make_metrics(&[(1, 50, 40, 200), (2, 50, 40, 50)]);
        let trigger = sched.evaluate(&metrics);
        assert!(matches!(
            trigger,
            Some(RebalanceTrigger::ShardImbalance { .. })
        ));
    }

    #[test]
    fn tenant_fairness() {
        let mut sched = RebalanceScheduler::new(SchedulerConfig {
            max_concurrent_per_tenant: 2,
            ..Default::default()
        });
        sched.migration_started(1, 100);
        sched.migration_started(2, 100);
        assert!(!sched.can_migrate_tenant(100));
        assert!(sched.can_migrate_tenant(200));
    }

    #[test]
    fn hot_split_plan() {
        let mut topo = ClusterTopology::new();
        let addr1: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        topo.add_node(NodeInfo::new(1, addr1, NodeState::Active));
        topo.add_node(NodeInfo::new(2, addr2, NodeState::Active));

        // uniform(2, ...) creates data groups 1 and 2 (+ metadata group 0).
        let routing = RoutingTable::uniform(2, &[1, 2], 1);
        let moves = plan_hot_split(1, &routing, &topo).unwrap();
        // Data group 1 has 512 vShards → split moves up to 256.
        assert!(!moves.is_empty());
        assert!(moves.len() <= 512);
    }
}
