// SPDX-License-Identifier: Apache-2.0

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::EdgeFilter;
use nodedb_types::id::NodeId;
use std::collections::HashMap;

use super::super::trait_def::NodeDb;

pub(in crate::traits::core) fn graph_pagerank_default(
    collection: &str,
    personalization: Option<HashMap<String, f64>>,
    damping: Option<f64>,
    max_iterations: Option<u32>,
) -> NodeDbResult<Vec<(String, f64)>> {
    let _ = (collection, personalization, damping, max_iterations);
    Err(NodeDbError::storage(
        "graph_pagerank is not implemented for this NodeDb backend",
    ))
}

/// Default forward-BFS shortest-path search built on `graph_traverse`.
///
/// See [`crate::traits::core::NodeDb::graph_shortest_path`] for the
/// full contract.
pub(in crate::traits::core) async fn graph_shortest_path_default<T: NodeDb + ?Sized>(
    this: &T,
    collection: &str,
    from: &NodeId,
    to: &NodeId,
    max_depth: u8,
    edge_filter: Option<&EdgeFilter>,
) -> NodeDbResult<Option<Vec<NodeId>>> {
    if from == to {
        return Ok(Some(vec![from.clone()]));
    }
    if max_depth == 0 {
        return Ok(None);
    }

    // Map of `node -> parent` used to reconstruct the path once the
    // target is reached. The source has no parent entry.
    let mut parent: HashMap<NodeId, NodeId> = HashMap::new();
    let mut frontier: Vec<NodeId> = vec![from.clone()];

    for _ in 0..max_depth {
        let mut next_frontier: Vec<NodeId> = Vec::new();
        for node in &frontier {
            let sg = this
                .graph_traverse(
                    collection,
                    node,
                    1,
                    nodedb_types::graph::Direction::Out,
                    edge_filter,
                )
                .await?;
            for edge in &sg.edges {
                // Only follow edges originating from the current
                // node — `graph_traverse` may include adjacent
                // edges that don't extend the BFS frontier.
                if &edge.from != node {
                    continue;
                }
                let dst = &edge.to;
                if dst == from || parent.contains_key(dst) {
                    continue;
                }
                parent.insert(dst.clone(), node.clone());
                if dst == to {
                    let mut path = vec![to.clone()];
                    let mut cur = to.clone();
                    while &cur != from {
                        let p = parent
                            .get(&cur)
                            .ok_or_else(|| {
                                NodeDbError::storage(format!(
                                    "graph shortest path parent missing for '{}': retry traversal",
                                    cur.as_str()
                                ))
                            })?
                            .clone();
                        path.push(p.clone());
                        cur = p;
                    }
                    path.reverse();
                    return Ok(Some(path));
                }
                next_frontier.push(dst.clone());
            }
        }
        if next_frontier.is_empty() {
            return Ok(None);
        }
        frontier = next_frontier;
    }
    Ok(None)
}
