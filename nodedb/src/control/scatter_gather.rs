//! Scatter-gather coordinator for cross-shard graph traversals.
//!
//! Cross-shard hops are NOT handled by forwarding the traversal to
//! the remote shard. Instead, the Data Plane returns partial results (the set
//! of cross-shard edge targets) to the Control Plane, which batches and
//! dispatches them to the appropriate target cores.
//!
//! This keeps the Data Plane stateless per-request and avoids distributed
//! deadlocks from recursive cross-shard calls.
//!
//! ## Vectorized Scatter Envelopes
//!
//! The Data Plane MUST NOT emit one SPSC message per unresolved cross-shard edge.
//! Instead, for each hop level, cross-shard destinations are accumulated into a
//! single vectorized envelope grouped by target shard:
//! `{ shard_id -> [node_id, ...] }`.

use std::collections::{HashMap, HashSet};

use tracing::{debug, warn};

use crate::control::state::SharedState;
use crate::engine::graph::traversal_options::{GraphResponseMeta, GraphTraversalOptions};
use crate::types::{TenantId, VShardId};

/// A batch of node IDs targeted at a specific shard.
///
/// Produced by the scatter phase when graph traversal discovers nodes
/// that live on a different shard than the current core.
#[derive(Debug, Clone)]
pub struct ScatterBatch {
    /// Target shard for this batch of node IDs.
    pub target_shard: VShardId,
    /// Node IDs that need to be explored on the target shard.
    pub node_ids: Vec<String>,
}

/// Vectorized scatter envelope for one hop level.
///
/// Groups all cross-shard destinations by target shard, preventing
/// scatter amplification.
#[derive(Debug, Clone, Default)]
pub struct ScatterEnvelope {
    /// Batches grouped by target shard.
    batches: HashMap<VShardId, Vec<String>>,
}

impl ScatterEnvelope {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a node ID destined for a specific shard.
    pub fn add(&mut self, shard: VShardId, node_id: String) {
        self.batches.entry(shard).or_default().push(node_id);
    }

    /// Number of distinct shards in this envelope.
    pub fn shard_count(&self) -> usize {
        self.batches.len()
    }

    /// Consume into scatter batches.
    pub fn into_batches(self) -> Vec<ScatterBatch> {
        self.batches
            .into_iter()
            .map(|(shard, node_ids)| ScatterBatch {
                target_shard: shard,
                node_ids,
            })
            .collect()
    }

    /// Total number of node IDs across all shards.
    pub fn total_nodes(&self) -> usize {
        self.batches.values().map(|v| v.len()).sum()
    }

    /// Check if the envelope is empty.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// Result of applying adaptive fan-out limits to a scatter envelope.
#[derive(Debug)]
pub enum FanOutDecision {
    /// All batches can proceed. No limits hit.
    Proceed {
        batches: Vec<ScatterBatch>,
        meta: GraphResponseMeta,
    },
    /// Soft limit exceeded but continuing. Response annotated with warning.
    ProceedWithWarning {
        batches: Vec<ScatterBatch>,
        meta: GraphResponseMeta,
    },
    /// Hard limit exceeded. If fan_out_partial, return partial results.
    /// Otherwise, return FAN_OUT_EXCEEDED error.
    Exceeded {
        /// Batches that were dispatched before limit was hit (for partial mode).
        dispatched: Vec<ScatterBatch>,
        /// Batches that were skipped.
        skipped: Vec<ScatterBatch>,
        meta: GraphResponseMeta,
    },
}

/// Apply adaptive fan-out limits to a scatter envelope.
/// - Soft limit (default 12): query continues, response annotated with warning
/// - Hard limit (default 16): query terminates with FAN_OUT_EXCEEDED unless
///   fan_out_partial is true, in which case partial results are returned
pub fn apply_fan_out_limits(
    envelope: ScatterEnvelope,
    options: &GraphTraversalOptions,
) -> FanOutDecision {
    let shard_count = envelope.shard_count() as u16;

    if shard_count <= options.fan_out_soft {
        // Under soft limit — all clear.
        FanOutDecision::Proceed {
            batches: envelope.into_batches(),
            meta: GraphResponseMeta {
                shards_reached: shard_count,
                ..Default::default()
            },
        }
    } else if shard_count <= options.fan_out_hard {
        // Between soft and hard limit — proceed with warning.
        let batches = envelope.into_batches();
        let meta = GraphResponseMeta::with_warning(shard_count, 0, options.fan_out_hard);
        FanOutDecision::ProceedWithWarning { batches, meta }
    } else {
        // Exceeded hard limit.
        let mut all_batches = envelope.into_batches();
        let hard = options.fan_out_hard as usize;
        let skipped = all_batches.split_off(hard);
        let skipped_count = skipped.len() as u16;
        let dispatched_count = all_batches.len() as u16;

        let meta = if options.fan_out_partial {
            GraphResponseMeta::with_truncation(dispatched_count, skipped_count)
        } else {
            GraphResponseMeta {
                shards_reached: dispatched_count,
                shards_skipped: skipped_count,
                truncated: true,
                fan_out_warning: None,
                approximate: true,
            }
        };

        FanOutDecision::Exceeded {
            dispatched: all_batches,
            skipped,
            meta,
        }
    }
}

/// Merge partial traversal results from multiple shards.
///
/// Deduplicates node IDs and accumulates all discovered nodes.
pub fn merge_traversal_results(
    local_nodes: Vec<String>,
    shard_results: &[Vec<String>],
) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut merged = Vec::new();

    for node in local_nodes {
        if seen.insert(node.clone()) {
            merged.push(node);
        }
    }

    for result in shard_results {
        for node in result {
            if seen.insert(node.clone()) {
                merged.push(node.clone());
            }
        }
    }

    merged
}

/// Coordinate a single cross-shard graph hop from the Control Plane.
///
/// Given a set of locally-discovered node IDs and a pre-built scatter envelope,
/// this function:
/// 1. Applies adaptive fan-out limits to the envelope.
/// 2. For each shard batch that passes the limit check, forwards a
///    `GRAPH TRAVERSE FROM '<node>' DEPTH 1` query to the leader node that
///    owns that shard via the cluster transport.
/// 3. Merges all remote results with `local_nodes` via deduplication.
///
/// Returns the merged node list and the aggregate `GraphResponseMeta`.
///
/// # Cluster mode only
///
/// This function assumes `shared.cluster_routing` and `shared.gateway`
/// are `Some`. Callers must check `shared.cluster_routing.is_some()` before
/// calling this function.
/// Parameters for a cross-shard graph traversal hop.
pub struct CrossShardHopParams<'a> {
    pub local_nodes: Vec<String>,
    pub envelope: ScatterEnvelope,
    pub options: &'a GraphTraversalOptions,
    pub edge_label: Option<&'a str>,
    pub direction: crate::engine::graph::edge_store::Direction,
    pub remaining_depth: usize,
}

pub async fn coordinate_cross_shard_hop(
    shared: &SharedState,
    tenant_id: TenantId,
    params: CrossShardHopParams<'_>,
) -> crate::Result<(Vec<String>, GraphResponseMeta)> {
    let CrossShardHopParams {
        local_nodes,
        envelope: cross_shard_targets,
        options,
        edge_label,
        direction,
        remaining_depth,
    } = params;
    // Fast path: nothing to scatter.
    if cross_shard_targets.is_empty() {
        return Ok((local_nodes, GraphResponseMeta::default()));
    }

    let decision = apply_fan_out_limits(cross_shard_targets, options);

    let (batches, mut meta) = match decision {
        FanOutDecision::Proceed { batches, meta } => (batches, meta),
        FanOutDecision::ProceedWithWarning { batches, meta } => {
            debug!(
                shards = meta.shards_reached,
                warning = ?meta.fan_out_warning,
                "cross-shard hop: fan-out soft limit exceeded, continuing"
            );
            (batches, meta)
        }
        FanOutDecision::Exceeded {
            dispatched,
            skipped,
            meta,
        } => {
            if options.fan_out_partial {
                debug!(
                    dispatched = dispatched.len(),
                    skipped = skipped.len(),
                    "cross-shard hop: hard fan-out limit, returning partial results"
                );
                (dispatched, meta)
            } else {
                return Err(crate::Error::FanOutExceeded {
                    shards_touched: meta.shards_reached + meta.shards_skipped,
                    limit: options.fan_out_hard,
                });
            }
        }
    };

    // Acquire the routing table and gateway once.
    let routing = match &shared.cluster_routing {
        Some(r) => r,
        None => {
            // Should not happen — callers must check. Return local results.
            warn!("coordinate_cross_shard_hop called without cluster routing");
            return Ok((local_nodes, meta));
        }
    };
    let gateway = match &shared.gateway {
        Some(g) => g.clone(),
        None => {
            warn!("coordinate_cross_shard_hop called without gateway");
            return Ok((local_nodes, meta));
        }
    };

    // Build SQL fragments for the label/direction clause (used for every node).
    let label_clause = match edge_label {
        Some(lbl) => format!(" LABEL '{lbl}'"),
        None => String::new(),
    };
    let direction_word = match direction {
        crate::engine::graph::edge_store::Direction::In => "in",
        crate::engine::graph::edge_store::Direction::Out => "out",
        crate::engine::graph::edge_store::Direction::Both => "both",
    };
    // We always traverse exactly 1 depth per scatter batch because the caller
    // drives the outer BFS loop. `remaining_depth` is included for completeness
    // but each forwarded request probes depth 1 so the Control Plane maintains
    // authoritative hop counting.
    let hop_depth = remaining_depth.min(1);

    // Fan out to all batches in parallel.
    let mut join_handles = Vec::with_capacity(batches.len());

    for batch in batches {
        let shard_id = batch.target_shard;
        let leader_node = {
            let rt = routing.read().unwrap_or_else(|p| p.into_inner());
            match rt.leader_for_vshard(shard_id.as_u16()) {
                Ok(node) => node,
                Err(e) => {
                    warn!(%shard_id, error = %e, "no leader for shard, skipping batch");
                    continue;
                }
            }
        };

        // Skip batches that target the local node — those nodes are already
        // covered by the local BFS that was executed before this call.
        if leader_node == shared.node_id {
            continue;
        }

        let gateway_clone = gateway.clone();
        let credentials_clone = std::sync::Arc::clone(&shared.credentials);
        let retention_clone = std::sync::Arc::clone(&shared.retention_policy_registry);
        let tenant_id_u32 = tenant_id.as_u32();
        let label_sql = label_clause.clone();
        let direction_sql = direction_word.to_string();

        join_handles.push(tokio::spawn(async move {
            let mut shard_results: Vec<String> = Vec::new();
            let mut any_error = false;

            for node_id in batch.node_ids {
                let sql = format!(
                    "GRAPH TRAVERSE FROM '{node_id}' DEPTH {hop_depth}{label_sql} DIRECTION {direction_sql}"
                );

                let gw_ctx = crate::control::gateway::core::QueryContext {
                    tenant_id: crate::types::TenantId::new(tenant_id_u32),
                    trace_id: 0,
                };

                // Build a fresh QueryContext per traversal using cloned inputs
                // (same pattern as QueryContext::for_state but without &SharedState).
                let plan_ctx = crate::control::planner::context::QueryContext::with_catalog(
                    std::sync::Arc::clone(&credentials_clone),
                    Some(std::sync::Arc::clone(&retention_clone)),
                );

                let sql_for_plan = sql.clone();
                let plan_result = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(
                        plan_ctx.plan_sql(
                            &sql_for_plan,
                            crate::types::TenantId::new(tenant_id_u32),
                        ),
                    )
                });

                let physical_plan = match plan_result {
                    Ok(tasks) => match tasks.into_iter().next().map(|t| t.plan) {
                        Some(p) => p,
                        None => continue,
                    },
                    Err(e) => {
                        warn!(
                            shard = %shard_id,
                            error = %e,
                            "remote graph traverse plan failed"
                        );
                        any_error = true;
                        continue;
                    }
                };

                match gateway_clone.execute(&gw_ctx, physical_plan).await {
                    Ok(payloads) => {
                        for payload in payloads {
                            if let Ok(nodes) = sonic_rs::from_slice::<Vec<String>>(&payload) {
                                shard_results.extend(nodes);
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            shard = %shard_id,
                            error = %e,
                            "remote graph traverse dispatch failed"
                        );
                        any_error = true;
                    }
                }
            }

            (shard_results, any_error)
        }));
    }

    // Collect all remote results.
    let mut remote_results: Vec<Vec<String>> = Vec::with_capacity(join_handles.len());
    for handle in join_handles {
        match handle.await {
            Ok((nodes, _had_error)) => {
                if !nodes.is_empty() {
                    remote_results.push(nodes);
                }
            }
            Err(e) => {
                warn!(error = %e, "cross-shard hop task panicked");
            }
        }
    }

    // Update meta with the number of shards that actually responded.
    meta.shards_reached = remote_results.len() as u16;

    // Deduplicate and merge local + remote results.
    let merged = merge_traversal_results(local_nodes, &remote_results);
    Ok((merged, meta))
}

/// Partition a set of node IDs into local nodes (served by this node) and
/// a `ScatterEnvelope` grouping remote nodes by their target shard.
///
/// "Local" means the shard's leader is `local_node_id`. Any node whose
/// `VShardId::from_key` maps to a shard led by a different node is remote.
///
/// When `cluster_routing` is `None` (single-node mode), all nodes are
/// considered local and the envelope is empty.
pub fn partition_local_remote(
    node_ids: &[String],
    local_node_id: u64,
    routing: &nodedb_cluster::RoutingTable,
) -> (Vec<String>, ScatterEnvelope) {
    let mut local = Vec::new();
    let mut envelope = ScatterEnvelope::new();

    for node_id in node_ids {
        let shard = VShardId::from_key(node_id.as_bytes());
        let leader = routing
            .leader_for_vshard(shard.as_u16())
            .unwrap_or(local_node_id);

        if leader == local_node_id {
            local.push(node_id.clone());
        } else {
            envelope.add(shard, node_id.clone());
        }
    }

    (local, envelope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scatter_envelope_grouping() {
        let mut env = ScatterEnvelope::new();
        env.add(VShardId::new(0), "a".into());
        env.add(VShardId::new(0), "b".into());
        env.add(VShardId::new(1), "c".into());

        assert_eq!(env.shard_count(), 2);
        assert_eq!(env.total_nodes(), 3);

        let batches = env.into_batches();
        assert_eq!(batches.len(), 2);
    }

    #[test]
    fn fan_out_under_soft_limit() {
        let mut env = ScatterEnvelope::new();
        for i in 0..5u16 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let decision = apply_fan_out_limits(env, &GraphTraversalOptions::default());
        match decision {
            FanOutDecision::Proceed { batches, meta } => {
                assert_eq!(batches.len(), 5);
                assert!(meta.is_clean());
                assert_eq!(meta.shards_reached, 5);
            }
            _ => panic!("expected Proceed"),
        }
    }

    #[test]
    fn fan_out_between_soft_and_hard() {
        let mut env = ScatterEnvelope::new();
        for i in 0..14u16 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let decision = apply_fan_out_limits(env, &GraphTraversalOptions::default());
        match decision {
            FanOutDecision::ProceedWithWarning { batches, meta } => {
                assert_eq!(batches.len(), 14);
                assert!(!meta.is_clean());
                assert!(meta.approximate);
                assert_eq!(meta.fan_out_warning, Some("14/16".to_string()));
            }
            _ => panic!("expected ProceedWithWarning"),
        }
    }

    #[test]
    fn fan_out_exceeded_no_partial() {
        let mut env = ScatterEnvelope::new();
        for i in 0..20u16 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let opts = GraphTraversalOptions {
            fan_out_partial: false,
            ..Default::default()
        };
        let decision = apply_fan_out_limits(env, &opts);
        match decision {
            FanOutDecision::Exceeded {
                dispatched,
                skipped,
                meta,
            } => {
                assert_eq!(dispatched.len(), 16);
                assert_eq!(skipped.len(), 4);
                assert!(meta.truncated);
                assert_eq!(meta.shards_reached, 16);
                assert_eq!(meta.shards_skipped, 4);
            }
            _ => panic!("expected Exceeded"),
        }
    }

    #[test]
    fn fan_out_exceeded_with_partial() {
        let mut env = ScatterEnvelope::new();
        for i in 0..20u16 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let opts = GraphTraversalOptions {
            fan_out_partial: true,
            ..Default::default()
        };
        let decision = apply_fan_out_limits(env, &opts);
        match decision {
            FanOutDecision::Exceeded {
                dispatched, meta, ..
            } => {
                assert_eq!(dispatched.len(), 16);
                assert!(meta.truncated);
            }
            _ => panic!("expected Exceeded"),
        }
    }

    #[test]
    fn merge_deduplicates() {
        let local = vec!["a".into(), "b".into(), "c".into()];
        let shard1 = vec!["b".into(), "d".into()];
        let shard2 = vec!["c".into(), "e".into()];

        let merged = merge_traversal_results(local, &[shard1, shard2]);
        assert_eq!(merged.len(), 5);
        assert!(merged.contains(&"a".to_string()));
        assert!(merged.contains(&"d".to_string()));
        assert!(merged.contains(&"e".to_string()));
    }

    #[test]
    fn empty_envelope() {
        let env = ScatterEnvelope::new();
        assert!(env.is_empty());
        assert_eq!(env.shard_count(), 0);
        assert_eq!(env.total_nodes(), 0);
    }

    #[test]
    fn custom_limits() {
        let mut env = ScatterEnvelope::new();
        for i in 0..10u16 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let opts = GraphTraversalOptions {
            fan_out_soft: 4,
            fan_out_hard: 8,
            fan_out_partial: true,
            max_visited: 100_000,
        };
        let decision = apply_fan_out_limits(env, &opts);
        match decision {
            FanOutDecision::Exceeded {
                dispatched,
                skipped,
                ..
            } => {
                assert_eq!(dispatched.len(), 8);
                assert_eq!(skipped.len(), 2);
            }
            _ => panic!("expected Exceeded"),
        }
    }
}
