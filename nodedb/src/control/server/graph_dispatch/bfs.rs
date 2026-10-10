// SPDX-License-Identifier: BUSL-1.1

//! `cross_core_bfs` — multi-hop BFS that drives the tree DDL aggregates
//! (`TREE_SUM`, `TREE_CHILDREN`) and any other breadth-first walk that
//! needs a flat reachable-node set across the full cross-core /
//! cross-shard neighborhood of each frontier node.
//!
//! The shared per-hop scatter/decode/merge logic lives in
//! [`super::hop::execute_neighbor_hop`]; this dispatcher only retains
//! the merged destination set. `GRAPH TRAVERSE`, which needs the
//! `{nodes,edges}` subgraph shape the remote client decodes, lives in
//! [`super::traverse_subgraph::cross_core_traverse_subgraph`].

use std::collections::HashSet;

use crate::bridge::envelope::Response;
use crate::control::state::SharedState;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TenantId, TxnId};

use super::helpers::{encode_path, ok_response};
use super::hop::{NeighborHopParams, execute_neighbor_hop};
use super::presence::{PresenceScope, graph_nodes_present};
use super::shard_reads::ShardReadLog;

/// Parameters for [`cross_core_bfs_with_options`].
pub struct CrossCoreBfsParams<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// Database-qualified collection scope, or `None` for a label-only
    /// traversal.
    pub collection: Option<&'a str>,
    pub start_nodes: Vec<String>,
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [String],
    pub direction: crate::engine::graph::edge_store::Direction,
    pub max_depth: usize,
    pub options: &'a GraphTraversalOptions,
    /// Each node that expands part of the walk confirms its groups first.
    pub linearizable: bool,
    /// The session's transaction. The walk sees its staged edge writes.
    pub txn_id: Option<TxnId>,
}

/// Cross-core BFS with explicit traversal options.
///
/// This is the cluster-aware entry point. Callers pass
/// `&GraphTraversalOptions::default()` for standard traversal.
///
/// The walk runs level by level, as one core's BFS does
/// (`CsrIndex::traverse_bfs`): each hop fetches every neighbor of the
/// frontier, and the level's new nodes are admitted in node-name order until
/// the visit cap. A capped walk admits the same nodes here as on one core.
pub async fn cross_core_bfs_with_options(
    shared: &SharedState,
    params: CrossCoreBfsParams<'_>,
) -> crate::Result<Response> {
    let CrossCoreBfsParams {
        tenant_id,
        database_id,
        collection,
        start_nodes,
        edge_labels,
        direction,
        max_depth,
        options,
        linearizable,
        txn_id,
    } = params;
    let cap = walk_visit_cap(shared, options);
    let whole = whole_hop();
    let mut visited: HashSet<String> = HashSet::new();
    let mut all_discovered: Vec<String> = Vec::new();
    let mut frontier: Vec<String> = start_nodes;
    let mut reads = ShardReadLog::new();

    // One core's walk drops a start its graph does not hold. The presence
    // read spans every collection, so it joins the read-set unscoped.
    let mut presence_reads = ShardReadLog::new();
    let present = graph_nodes_present(
        shared,
        PresenceScope {
            tenant_id,
            database_id,
            options: &whole,
            linearizable,
            txn_id,
        },
        &frontier,
        &mut presence_reads,
    )
    .await?;
    presence_reads.publish(tenant_id, database_id, None);

    frontier.retain(|node| present.contains(node) && visited.insert(node.clone()));
    all_discovered.extend(frontier.iter().cloned());

    for _depth in 0..max_depth {
        if frontier.is_empty() || all_discovered.len() >= cap {
            break;
        }

        let hop = execute_neighbor_hop(
            shared,
            tenant_id,
            database_id,
            NeighborHopParams {
                collection,
                frontier: &frontier,
                edge_labels,
                direction,
                options: &whole,
                discovered_so_far: all_discovered.len(),
                linearizable,
                edge_predicate: &[],
                with_properties: false,
                txn_id,
            },
        )
        .await?;

        reads.merge(hop.reads);
        frontier = admit_by_name(
            hop.merged_destinations,
            &mut visited,
            all_discovered.len(),
            cap,
        );
        all_discovered.extend(frontier.iter().cloned());
    }

    // Every vShard the walk expanded joins the transaction read-set.
    reads.publish(tenant_id, database_id, collection.map(str::to_owned));
    Ok(ok_response(encode_path(&all_discovered)?))
}

/// The visit cap of a hop or subgraph walk: the plan's cap, bounded by this
/// node's graph tuning, as one core bounds it (`CoreLoop::walk_visit_cap`).
pub(super) fn walk_visit_cap(shared: &SharedState, options: &GraphTraversalOptions) -> usize {
    options.max_visited.min(shared.tuning.graph.max_visited)
}

/// Hop options that return every neighbor of the frontier. A capped walk
/// admits each level's nodes in name order, so it reads the whole level.
pub(super) fn whole_hop() -> GraphTraversalOptions {
    GraphTraversalOptions {
        max_visited: usize::MAX,
    }
}

/// Admit the unvisited `candidates` in node-name order until the walk holds
/// `cap` nodes, `discovered` of them already admitted. Marks each admitted
/// node visited. Returns the admitted nodes: the next frontier.
pub(super) fn admit_by_name(
    mut candidates: Vec<String>,
    visited: &mut HashSet<String>,
    discovered: usize,
    cap: usize,
) -> Vec<String> {
    candidates.retain(|node| !visited.contains(node));
    candidates.sort();
    candidates.dedup();
    candidates.truncate(cap.saturating_sub(discovered));
    for node in &candidates {
        visited.insert(node.clone());
    }
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_capped_level_is_admitted_in_name_order() {
        let mut visited: HashSet<String> = ["a".to_string()].into_iter().collect();
        let candidates = ["z", "m", "a", "b", "m"].map(str::to_string).to_vec();
        let next = admit_by_name(candidates, &mut visited, 1, 3);
        assert_eq!(next, vec!["b", "m"]);
        assert!(visited.contains("b") && visited.contains("m") && !visited.contains("z"));
    }
}
