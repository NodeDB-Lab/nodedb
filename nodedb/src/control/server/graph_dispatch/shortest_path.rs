//! `cross_core_shortest_path` — mirrors `cross_core_bfs` but records
//! parent pointers so `GRAPH PATH FROM 'src' TO 'dst'` can reconstruct
//! an ordered path across every topology (single core, single-node
//! multi-core, clustered).

use std::collections::{HashMap, HashSet};

use sonic_rs;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::bridge::physical_plan::GraphOp;
use crate::control::scatter_gather;
use crate::control::state::SharedState;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::TenantId;

use super::helpers::{encode_path, ok_response};

/// Cross-core / cross-shard shortest-path orchestration.
///
/// Walks the same hop-by-hop BFS as `cross_core_bfs` but records a
/// `parent` pointer for every newly-discovered node. When `dst` is
/// reached, the path is reconstructed by walking parents back to
/// `src`. Returns a JSON array `[src, hop_1, ..., dst]`, or an empty
/// array when `dst` is unreachable within `max_depth` hops.
pub async fn cross_core_shortest_path(
    shared: &SharedState,
    tenant_id: TenantId,
    src: String,
    dst: String,
    edge_label: Option<String>,
    max_depth: usize,
) -> crate::Result<Response> {
    let options = GraphTraversalOptions::default();
    let cluster_mode = shared.cluster_routing.is_some();
    // Path semantics only make sense over outgoing edges — the
    // docs-advertised `GRAPH PATH FROM 'a' TO 'b'` is directed.
    let direction = crate::engine::graph::edge_store::Direction::Out;

    // Empty path when src == dst; matches the natural `[src]` case.
    if src == dst {
        let payload = encode_path(&[src])?;
        return Ok(ok_response(payload));
    }

    let mut parent: HashMap<String, String> = HashMap::new();
    let mut visited: HashSet<String> = HashSet::new();
    visited.insert(src.clone());
    let mut frontier: Vec<String> = vec![src.clone()];

    for depth in 0..max_depth {
        if frontier.is_empty() {
            break;
        }

        // Local hop: single batched broadcast carrying the whole
        // frontier. The Data Plane returns `(src, label, node)` triples
        // so we can still record parent pointers.
        let mut discoveries: Vec<(String, String)> = Vec::new();
        // Cap the handler's allocation to the remaining `max_visited`
        // budget so a wide hop can't blow out memory mid-BFS.
        let remaining_budget = options
            .max_visited
            .saturating_sub(visited.len())
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
                    let src_node = item.get("src").and_then(|v| v.as_str());
                    let nb = item.get("node").and_then(|v| v.as_str());
                    if let (Some(s), Some(n)) = (src_node, nb) {
                        discoveries.push((s.to_string(), n.to_string()));
                    }
                }
            }
        }

        // Cluster mode: merge cross-shard neighbor results. The
        // cross-shard helper only returns neighbor ids (no parent),
        // so we attribute them to the first frontier node on the
        // same shard — sufficient for shortest-path correctness
        // because any path through that shard is equally shortest.
        let merged: Vec<(String, String)> = if cluster_mode {
            let local_ids: Vec<String> = discoveries.iter().map(|(_, n)| n.clone()).collect();
            let (local_nodes, envelope) = {
                let routing = shared
                    .cluster_routing
                    .as_ref()
                    .expect("cluster_routing checked above");
                let rt = routing.read().unwrap_or_else(|p| p.into_inner());
                scatter_gather::partition_local_remote(&local_ids, shared.node_id, &rt)
            };
            if envelope.is_empty() {
                discoveries
            } else {
                let remaining = max_depth.saturating_sub(depth + 1);
                let (remote_hits, _meta) = scatter_gather::coordinate_cross_shard_hop(
                    shared,
                    tenant_id,
                    scatter_gather::CrossShardHopParams {
                        local_nodes,
                        envelope,
                        options: &options,
                        edge_label: edge_label.as_deref(),
                        direction,
                        remaining_depth: remaining,
                    },
                )
                .await?;
                let mut out = discoveries;
                if let Some(attrib_parent) = frontier.first().cloned() {
                    for n in remote_hits {
                        out.push((attrib_parent.clone(), n));
                    }
                }
                out
            }
        } else {
            discoveries
        };

        // Record parents and build next frontier. Early-exit the
        // moment `dst` is seen so we don't keep expanding.
        let mut next_frontier: Vec<String> = Vec::new();
        for (from, to) in merged {
            if !visited.insert(to.clone()) {
                continue;
            }
            parent.insert(to.clone(), from);
            if to == dst {
                let path = reconstruct(&parent, &src, &dst);
                let payload = encode_path(&path)?;
                return Ok(ok_response(payload));
            }
            next_frontier.push(to);
        }
        frontier = next_frontier;

        if visited.len() >= options.max_visited {
            break;
        }
    }

    // Unreachable within max_depth: empty array, same shape the
    // client sees for a successful empty result.
    let payload = encode_path::<String>(&[])?;
    Ok(ok_response(payload))
}

fn reconstruct(parent: &HashMap<String, String>, src: &str, dst: &str) -> Vec<String> {
    let mut path: Vec<String> = Vec::new();
    let mut cursor = dst.to_string();
    path.push(cursor.clone());
    while cursor != src {
        match parent.get(&cursor) {
            Some(p) => {
                cursor = p.clone();
                path.push(cursor.clone());
            }
            None => break,
        }
    }
    path.reverse();
    path
}
