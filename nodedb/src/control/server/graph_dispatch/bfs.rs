//! `cross_core_bfs` — multi-hop BFS that drives `GRAPH TRAVERSE`, the
//! tree DDL aggregates (`TREE_SUM`, `TREE_CHILDREN`) and any other
//! breadth-first walk that needs to see the full cross-core / cross-shard
//! neighborhood of each frontier node.

use std::collections::HashSet;

use sonic_rs;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::bridge::physical_plan::GraphOp;
use crate::control::scatter_gather;
use crate::control::state::SharedState;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{Lsn, RequestId, TenantId};

/// Cross-core BFS orchestration for graph traversal.
///
/// Implements multi-hop BFS. In single-node mode, broadcasts `GraphNeighbors`
/// to all local Data Plane cores. In cluster mode, after each local hop the
/// frontier is partitioned into local/remote nodes and remote nodes are
/// coordinated via `scatter_gather::coordinate_cross_shard_hop`.
///
/// Returns a JSON array of all discovered node IDs.
pub async fn cross_core_bfs(
    shared: &SharedState,
    tenant_id: TenantId,
    start_nodes: Vec<String>,
    edge_label: Option<String>,
    direction: crate::engine::graph::edge_store::Direction,
    max_depth: usize,
) -> crate::Result<Response> {
    cross_core_bfs_with_options(
        shared,
        tenant_id,
        start_nodes,
        edge_label,
        direction,
        max_depth,
        &GraphTraversalOptions::default(),
    )
    .await
}

/// Cross-core BFS with explicit traversal options (fan-out limits, partial mode).
///
/// This is the full cluster-aware entry point. `cross_core_bfs` delegates here
/// with default options for backward compatibility.
pub async fn cross_core_bfs_with_options(
    shared: &SharedState,
    tenant_id: TenantId,
    start_nodes: Vec<String>,
    edge_label: Option<String>,
    direction: crate::engine::graph::edge_store::Direction,
    max_depth: usize,
    options: &GraphTraversalOptions,
) -> crate::Result<Response> {
    let cluster_mode = shared.cluster_routing.is_some();

    let mut visited: HashSet<String> = HashSet::new();
    let mut all_discovered: Vec<String> = Vec::new();
    let mut frontier: Vec<String> = start_nodes.clone();

    for node in &start_nodes {
        visited.insert(node.clone());
        all_discovered.push(node.clone());
    }

    for depth in 0..max_depth {
        if frontier.is_empty() {
            break;
        }

        // ── Local hop ──────────────────────────────────────────────────────
        //
        // Single broadcast per hop: `NeighborsMulti` carries the whole
        // frontier in one plan so the Control Plane makes one RPC per
        // hop regardless of frontier size. Previously this loop issued
        // `O(frontier)` serial broadcasts (§issue-53 bug 3).
        let mut local_hop_results: Vec<String> = Vec::new();

        // Cap this hop's handler-side allocation to the remaining budget
        // under `max_visited` so a single wide hop cannot blow past the
        // cap on the Data Plane side. `u32::MAX` on an underflow-ed
        // subtraction is defensively clamped.
        let remaining_budget = options
            .max_visited
            .saturating_sub(all_discovered.len())
            .min(u32::MAX as usize) as u32;
        let plan = PhysicalPlan::Graph(GraphOp::NeighborsMulti {
            node_ids: frontier.clone(),
            edge_label: edge_label.clone(),
            direction,
            max_results: remaining_budget,
            rls_filters: Vec::new(),
        });

        let resp =
            crate::control::server::broadcast::broadcast_to_all_cores(shared, tenant_id, plan, 0)
                .await?;

        if !resp.payload.is_empty() {
            let json_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            if let Ok(arr) = sonic_rs::from_str::<Vec<serde_json::Value>>(&json_text) {
                for item in arr {
                    if let Some(neighbor) = item.get("node").and_then(|v| v.as_str()) {
                        local_hop_results.push(neighbor.to_string());
                        // Mid-hop max_visited check — prevents a single
                        // wide hop from pushing far past the configured
                        // cap before the between-hop check fires.
                        if all_discovered.len() + local_hop_results.len()
                            >= options.max_visited
                        {
                            break;
                        }
                    }
                }
            }
        }

        // ── Cluster mode: scatter-gather for cross-shard frontier ──────────
        let merged_hop_results = if cluster_mode {
            // Partition newly-discovered nodes into local vs cross-shard.
            let (local_nodes, cross_shard_envelope) = {
                let routing = shared
                    .cluster_routing
                    .as_ref()
                    .expect("cluster_routing checked above");
                let rt = routing.read().unwrap_or_else(|p| p.into_inner());
                scatter_gather::partition_local_remote(&local_hop_results, shared.node_id, &rt)
            };

            if cross_shard_envelope.is_empty() {
                // All results are local — no cross-shard work needed.
                local_nodes
            } else {
                let remaining_depth = max_depth.saturating_sub(depth + 1);
                let (merged, _meta) = scatter_gather::coordinate_cross_shard_hop(
                    shared,
                    tenant_id,
                    scatter_gather::CrossShardHopParams {
                        local_nodes,
                        envelope: cross_shard_envelope,
                        options,
                        edge_label: edge_label.as_deref(),
                        direction,
                        remaining_depth,
                    },
                )
                .await?;
                merged
            }
        } else {
            // Single-node mode: all results are already local.
            local_hop_results
        };

        // ── Extend global visited set and compute next frontier ────────────
        let mut next_frontier: Vec<String> = Vec::new();
        for node in merged_hop_results {
            if visited.insert(node.clone()) {
                next_frontier.push(node.clone());
                all_discovered.push(node);
            }
        }

        frontier = next_frontier;

        // Enforce max_visited cap across all hops.
        if all_discovered.len() >= options.max_visited {
            break;
        }
    }

    let payload = match sonic_rs::to_vec(&all_discovered) {
        Ok(v) => v,
        Err(e) => {
            return Err(crate::Error::Serialization {
                format: "json".into(),
                detail: e.to_string(),
            });
        }
    };

    Ok(Response {
        request_id: RequestId::new(0),
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(payload),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
    })
}
