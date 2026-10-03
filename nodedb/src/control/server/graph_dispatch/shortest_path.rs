// SPDX-License-Identifier: BUSL-1.1

//! `cross_core_shortest_path` — the bidirectional search of one core's
//! `CsrIndex::shortest_path`, run hop by hop across every topology (single
//! core, single-node multi-core, clustered), so `GRAPH PATH FROM 'src' TO
//! 'dst'` answers the same path everywhere.
//!
//! Each step expands one forward level (outgoing edges of the forward
//! frontier) and then one backward level (incoming edges of the backward
//! frontier) through [`super::hop::execute_neighbor_hop`]: every frontier node
//! expands at the node that owns its key vShard, and every crossed edge comes
//! back as a `(frontier node, label, neighbour)` triple. A level relaxes its
//! edges in `(neighbour, frontier node)` name order: a new node's parent is
//! the smallest-named frontier node reaching it, and the search stops at the
//! first node both sides reached. The visit cap is checked before each step.
//! One core does all of this in the same order, so a capped search answers
//! the same path here as there.
//!
//! Round `k` meets on a path of `2k - 1` edges (forward level) or `2k` edges
//! (backward level), so the first meeting is a shortest path, and
//! `max_depth.div_ceil(2)` rounds reach every path of at most `max_depth`
//! edges. A meeting on a longer path answers no path.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::bridge::envelope::Response;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TenantId, TxnId};

use super::bfs::{walk_visit_cap, whole_hop};
use super::helpers::{encode_path, ok_response};
use super::hop::{NeighborHopParams, execute_neighbor_hop};
use super::neighbor_rows::NeighborRow;
use super::presence::{PresenceScope, graph_nodes_present};
use super::shard_reads::ShardReadLog;
use nodedb_types::filter::MetadataFilter;

/// Parameters for [`cross_core_shortest_path`].
pub struct CrossCoreShortestPathParams {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// Database-qualified collection whose edges the path walks, or `None` to
    /// walk the edges of every collection, as a single node's Data Plane does
    /// for a path plan with no collection.
    pub collection: Option<String>,
    pub src: String,
    pub dst: String,
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: Vec<String>,
    pub max_depth: usize,
    /// The plan's traversal options. The visit cap is
    /// `options.max_visited`, bounded by this node's graph tuning.
    pub options: GraphTraversalOptions,
    /// Each node that expands part of the walk confirms its groups first.
    pub linearizable: bool,
    /// AND-ed edge-property predicate. A path crosses only edges it admits:
    /// the forward side tests each outgoing edge, the backward side each
    /// incoming edge in its physical orientation. Empty admits every edge.
    /// Non-empty requires `collection`.
    pub edge_predicate: Vec<MetadataFilter>,
    /// The session's transaction. The path sees its staged edge writes.
    pub txn_id: Option<TxnId>,
}

/// Node → the node it was reached from. An endpoint maps to itself.
type Parents = HashMap<String, String>;

/// Cross-core / cross-shard shortest-path orchestration.
///
/// Returns a JSON array `[src, hop_1, ..., dst]`, `[src]` when `src == dst`,
/// or an empty array when no path is found or either endpoint is absent from
/// the graph. `GRAPH PATH` is directed: the
/// forward side follows outgoing edges, the backward side incoming ones.
pub async fn cross_core_shortest_path(
    shared: &SharedState,
    params: CrossCoreShortestPathParams,
) -> crate::Result<Response> {
    let CrossCoreShortestPathParams {
        tenant_id,
        database_id,
        collection,
        src,
        dst,
        edge_labels,
        max_depth,
        options,
        linearizable,
        edge_predicate,
        txn_id,
    } = params;
    let whole = whole_hop();
    // One core finds no path when either endpoint is absent from its graph,
    // before it compares them. The presence read spans every collection, so it
    // joins the read-set unscoped.
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
        &[src.clone(), dst.clone()],
        &mut presence_reads,
    )
    .await?;
    presence_reads.publish(shared, tenant_id, database_id, None);
    if !present.contains(&src) || !present.contains(&dst) {
        return Ok(ok_response(encode_path::<String>(&[])?));
    }
    if src == dst {
        return Ok(ok_response(encode_path(&[src])?));
    }
    let mut reads = ShardReadLog::new();
    let cap = walk_visit_cap(shared, &options);
    let mut fwd: Parents = HashMap::from([(src.clone(), src.clone())]);
    let mut bwd: Parents = HashMap::from([(dst.clone(), dst.clone())]);
    let mut fwd_frontier = vec![src];
    let mut bwd_frontier = vec![dst];
    let mut path: Vec<String> = Vec::new();
    let mut met = false;

    for _round in 0..max_depth.div_ceil(2) {
        if fwd.len() + bwd.len() >= cap {
            break;
        }
        for forward in [true, false] {
            let (frontier, direction) = if forward {
                (&fwd_frontier, Direction::Out)
            } else {
                (&bwd_frontier, Direction::In)
            };
            let triples = if frontier.is_empty() {
                Vec::new()
            } else {
                let hop = execute_neighbor_hop(
                    shared,
                    tenant_id,
                    database_id,
                    NeighborHopParams {
                        collection: collection.as_deref(),
                        frontier,
                        edge_labels: &edge_labels,
                        direction,
                        options: &whole,
                        discovered_so_far: fwd.len() + bwd.len(),
                        linearizable,
                        edge_predicate: &edge_predicate,
                        with_properties: false,
                        txn_id,
                    },
                )
                .await?;
                reads.merge(hop.reads);
                hop.rows.into_iter().map(NeighborRow::into_triple).collect()
            };
            let (this, other) = if forward {
                (&mut fwd, &bwd)
            } else {
                (&mut bwd, &fwd)
            };
            let (next, meeting) = relax_level(triples, this, other);
            if let Some(meeting) = meeting {
                path = within_depth(reconstruct(&meeting, &fwd, &bwd), max_depth);
                met = true;
                break;
            }
            if forward {
                fwd_frontier = next;
            } else {
                bwd_frontier = next;
            }
        }
        if met || (fwd_frontier.is_empty() && bwd_frontier.is_empty()) {
            break;
        }
    }

    // Every vShard the walk expanded joins the transaction read-set.
    reads.publish(shared, tenant_id, database_id, collection);
    Ok(ok_response(encode_path(&path)?))
}

/// Relax one level's `(frontier node, label, neighbour)` edges into `this`
/// side, in `(neighbour, frontier node)` name order. Returns the level's new
/// nodes, and the first neighbour the `other` side already reached.
fn relax_level(
    mut triples: Vec<(String, String, String)>,
    this: &mut Parents,
    other: &Parents,
) -> (Vec<String>, Option<String>) {
    triples.sort_by(|a, b| (&a.2, &a.0).cmp(&(&b.2, &b.0)));
    let mut next = Vec::new();
    for (from, _label, to) in triples {
        if let Entry::Vacant(slot) = this.entry(to.clone()) {
            slot.insert(from);
            next.push(to.clone());
        }
        if other.contains_key(&to) {
            return (next, Some(to));
        }
    }
    (next, None)
}

/// `path` when it has at most `max_depth` edges, else no path.
fn within_depth(path: Vec<String>, max_depth: usize) -> Vec<String> {
    if path.len().saturating_sub(1) <= max_depth {
        path
    } else {
        Vec::new()
    }
}

/// The path through `meeting`: forward parents back to the source, then
/// backward parents on to the destination.
fn reconstruct(meeting: &str, fwd: &Parents, bwd: &Parents) -> Vec<String> {
    let mut path = vec![meeting.to_string()];
    let mut cursor = meeting;
    while let Some(parent) = fwd.get(cursor).filter(|p| p.as_str() != cursor) {
        path.push(parent.clone());
        cursor = parent.as_str();
    }
    path.reverse();
    let mut cursor = meeting;
    while let Some(parent) = bwd.get(cursor).filter(|p| p.as_str() != cursor) {
        path.push(parent.clone());
        cursor = parent.as_str();
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parents(pairs: &[(&str, &str)]) -> Parents {
        pairs
            .iter()
            .map(|(child, parent)| (child.to_string(), parent.to_string()))
            .collect()
    }

    #[test]
    fn a_path_joins_both_sides_at_the_meeting_node() {
        let fwd = parents(&[("a", "a"), ("b", "a")]);
        let bwd = parents(&[("d", "d"), ("c", "d"), ("b", "c")]);
        assert_eq!(reconstruct("b", &fwd, &bwd), vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn a_path_longer_than_max_depth_is_no_path() {
        let path = || ["a", "b", "c"].map(str::to_string).to_vec();
        assert!(within_depth(path(), 1).is_empty());
        assert_eq!(within_depth(path(), 2), path());
    }

    #[test]
    fn a_level_relaxes_in_name_order() {
        let mut this = parents(&[("a", "a")]);
        let other = parents(&[("d", "d"), ("b", "d"), ("z", "d")]);
        let triples = vec![
            ("a".to_string(), "L".to_string(), "z".to_string()),
            ("a".to_string(), "L".to_string(), "b".to_string()),
        ];
        let (next, meeting) = relax_level(triples, &mut this, &other);
        assert_eq!(meeting.as_deref(), Some("b"));
        assert_eq!(next, vec!["b"]);
    }
}
