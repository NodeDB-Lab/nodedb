// SPDX-License-Identifier: BUSL-1.1

//! Read-your-own-writes merge for GRAPH reads.
//!
//! The durable neighbor list a CSR partition returns reflects only
//! committed state. When a request carries a `txn_id` with a staged
//! `GraphTxnOverlay`, these functions fold that transaction's pending edge
//! writes into the durable result: staged tombstones subtract a durable
//! neighbor, staged puts add one.
//!
//! - `Neighbors` and depth-1 `Hop`: [`merge_graph_txn_overlay_neighbors`],
//!   [`merge_hop_single_hop`].
//! - Multi-hop `Hop`: [`build_graph_overlay_delta`], pushed into the BFS.
//! - `NeighborsMulti`, the hop of the `GRAPH TRAVERSE` / `GRAPH PATH` walk
//!   coordinators: [`merge_staged_hop_pass`], which also carries each staged
//!   put's property map to the edge predicate and the returned properties.
//!
//! Pure function, not a `CoreLoop` method: callers resolve the overlay via
//! `self.graph_txn_overlays.get(&txn_id)` and pass it in, so this logic is
//! unit-testable without constructing a full `CoreLoop`.

use std::collections::HashSet;

use crate::data::executor::handlers::graph_edge_predicate::Orientation;
use crate::data::executor::handlers::transaction::overlay::{GraphCollKey, GraphTxnOverlay};
use crate::engine::graph::csr::GraphOverlayDelta;
use crate::engine::graph::edge_store::Direction;
use crate::types::TenantId;
use nodedb_graph::csr::index::LabelFilter;
use nodedb_types::DatabaseId;

/// Translate a transaction's [`GraphTxnOverlay`] into a shared-crate
/// [`GraphOverlayDelta`] scoped to `(database_id, tenant)`, for the multi-hop
/// `Hop` (depth > 1) and `Subgraph` read-your-own-writes paths.
///
/// Neighbors / single-hop `Hop` use [`merge_graph_txn_overlay_neighbors`] /
/// [`merge_hop_single_hop`] instead; the traversal engine itself cannot merge
/// staged edges through staged-only intermediate nodes, so multi-hop pushes
/// the whole delta down into `traverse_bfs` / `subgraph`.
pub(in crate::data::executor) fn build_graph_overlay_delta(
    overlay: &GraphTxnOverlay,
    database_id: DatabaseId,
    tenant: TenantId,
) -> GraphOverlayDelta {
    let mut delta = GraphOverlayDelta::new();
    for (src, label, dst) in overlay.all_staged_edges(database_id, tenant) {
        delta.stage_edge(&src, &label, &dst);
    }
    for (src, label, dst) in overlay.all_tombstones(database_id, tenant) {
        delta.stage_tombstone(&src, &label, &dst);
    }
    delta
}

/// Merge a transaction's staged GRAPH edge writes into a durable `(label,
/// node)` neighbor list for `node_id`, respecting `direction` and
/// `edge_labels`. An empty `edge_labels` keeps every staged edge. Otherwise a
/// staged edge whose label is any listed label passes. No-op (returns
/// `durable` unchanged) when `overlay` is `None`.
pub(in crate::data::executor) fn merge_graph_txn_overlay_neighbors(
    overlay: Option<&GraphTxnOverlay>,
    database_id: DatabaseId,
    tenant: TenantId,
    node_id: &str,
    edge_labels: &[&str],
    direction: Direction,
    durable: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let Some(overlay) = overlay else {
        return durable;
    };

    // Subtract any durable neighbor whose backing edge was tombstoned in
    // this transaction.
    let mut merged: Vec<(String, String)> = durable
        .into_iter()
        .filter(|(label, other)| {
            let (src, dst) = edge_endpoints(direction, node_id, other);
            !overlay.is_edge_tombstoned_any_collection(database_id, tenant, src, label, dst)
        })
        .collect();

    // Add staged edges matching direction + label that aren't already
    // present (a staged put re-adding an edge that survived the tombstone
    // filter above, or a brand-new edge).
    let mut staged: Vec<(String, String, Vec<u8>)> = Vec::new();
    if matches!(direction, Direction::Out | Direction::Both) {
        staged.extend(overlay.edges_for_src_any_collection(database_id, tenant, node_id));
    }
    if matches!(direction, Direction::In | Direction::Both) {
        staged.extend(overlay.edges_for_dst_any_collection(database_id, tenant, node_id));
    }
    for (label, other, _props) in staged {
        if !LabelFilter::keeps_name(edge_labels, &label) {
            continue;
        }
        if !merged.iter().any(|(l, n)| *l == label && *n == other) {
            merged.push((label, other));
        }
    }
    merged
}

/// Merge a transaction's staged GRAPH edge writes into `Hop`'s durable BFS
/// result, for the single-hop case (`depth == 1`) only. Multi-hop `Hop`
/// pushes [`build_graph_overlay_delta`] into the BFS instead.
///
/// `durable_neighbors_of` fetches one start node's durable `(label, node)`
/// neighbor list on demand (the caller's `csr_partition(..).neighbors(..)`),
/// so this function stays free of any `CoreLoop` dependency.
///
/// When `has_bitmap` is `true` (a `frontier_bitmap` prefilter is active),
/// tombstone subtraction is still applied (always safe -- removing a result
/// never violates a prefilter), but staged-edge addition is skipped, since a
/// brand-new staged node's bitmap membership can't be validated here.
///
/// Bundled into a params struct (rather than a long positional argument
/// list) since the durable-neighbor fetch closure, the merge identity
/// (database/tenant/label/direction), and the BFS-result bookkeeping
/// (depth/bitmap/durable_result) are each a distinct concern.
pub(in crate::data::executor) struct HopMergeParams<'a, F>
where
    F: Fn(&str) -> Vec<(String, String)>,
{
    pub overlay: Option<&'a GraphTxnOverlay>,
    pub durable_neighbors_of: F,
    pub starts: &'a [&'a str],
    pub depth: usize,
    pub database_id: DatabaseId,
    pub tenant: TenantId,
    /// Empty keeps every staged edge. Otherwise an edge with any listed label.
    pub label_filter: &'a [&'a str],
    pub direction: Direction,
    pub has_bitmap: bool,
    pub durable_result: Vec<String>,
}

pub(in crate::data::executor) fn merge_hop_single_hop<F>(
    params: HopMergeParams<'_, F>,
) -> Vec<String>
where
    F: Fn(&str) -> Vec<(String, String)>,
{
    let HopMergeParams {
        overlay,
        durable_neighbors_of,
        starts,
        depth,
        database_id,
        tenant,
        label_filter,
        direction,
        has_bitmap,
        durable_result,
    } = params;

    if depth != 1 {
        return durable_result;
    }
    let Some(overlay) = overlay else {
        return durable_result;
    };

    let mut merged: std::collections::HashSet<String> = durable_result.into_iter().collect();
    for start in starts {
        let durable_here = durable_neighbors_of(start);
        let durable_names: std::collections::HashSet<&str> =
            durable_here.iter().map(|(_, n)| n.as_str()).collect();
        let merged_here = merge_graph_txn_overlay_neighbors(
            Some(overlay),
            database_id,
            tenant,
            start,
            label_filter,
            direction,
            durable_here.clone(),
        );
        let merged_names: std::collections::HashSet<&str> =
            merged_here.iter().map(|(_, n)| n.as_str()).collect();

        for name in durable_names.difference(&merged_names) {
            merged.remove(*name);
        }
        if !has_bitmap {
            for name in merged_names.difference(&durable_names) {
                merged.insert((*name).to_string());
            }
        }
    }
    merged.into_iter().collect()
}

/// Resolve the `(src, dst)` pair for a tombstone lookup given the direction
/// a durable neighbor entry was returned under: for `Out`, `other` is the
/// dst and `node_id` is the src; for `In`, `other` is the src and `node_id`
/// is the dst. `Both` is resolved as `Out`'s shape -- a mismatched guess
/// only costs a missed subtraction on a durable `Both` result, and neither
/// `Neighbors` nor `Hop` (the only two callers of this merge) is invoked
/// with `Both` by any current planner path.
fn edge_endpoints<'a>(
    direction: Direction,
    node_id: &'a str,
    other: &'a str,
) -> (&'a str, &'a str) {
    match direction {
        Direction::In => (other, node_id),
        Direction::Out | Direction::Both => (node_id, other),
    }
}

/// Merge a transaction's staged edge writes in one collection into the
/// durable `(src, label, dst)` edges of `node_id`, respecting `direction`
/// and `edge_labels`. An empty `edge_labels` keeps every staged edge.
/// Otherwise a staged edge whose label is any listed label passes. A staged
/// tombstone removes a durable edge. A staged put adds an edge the durable
/// list lacks. The result is sorted and holds no duplicates. Returns
/// `durable` sorted when `overlay` is `None`.
pub(in crate::data::executor) fn merge_graph_txn_overlay_collection_edges(
    overlay: Option<&GraphTxnOverlay>,
    coll_key: &GraphCollKey,
    node_id: &str,
    edge_labels: &[&str],
    direction: Direction,
    durable: Vec<(String, String, String)>,
) -> Vec<(String, String, String)> {
    let mut merged: std::collections::BTreeSet<(String, String, String)> =
        durable.into_iter().collect();
    let Some(overlay) = overlay else {
        return merged.into_iter().collect();
    };
    merged.retain(|(src, label, dst)| !overlay.is_edge_tombstoned(coll_key, src, label, dst));
    let label_matches = |label: &str| LabelFilter::keeps_name(edge_labels, label);
    if matches!(direction, Direction::Out | Direction::Both) {
        for (label, dst, _) in overlay.edges_for_src(coll_key, node_id) {
            if label_matches(label) {
                merged.insert((node_id.to_string(), label.to_string(), dst.to_string()));
            }
        }
    }
    if matches!(direction, Direction::In | Direction::Both) {
        for (label, src, _) in overlay.edges_for_dst(coll_key, node_id) {
            if label_matches(label) {
                merged.insert((src.to_string(), label.to_string(), node_id.to_string()));
            }
        }
    }
    merged.into_iter().collect()
}

/// The scope a `NeighborsMulti` hop merges its transaction's staged edge
/// writes in.
pub(in crate::data::executor) struct StagedHopScope<'a> {
    pub database_id: DatabaseId,
    pub tenant: TenantId,
    /// Collection scope, or `None` for a label-only hop over every
    /// collection.
    pub collection: Option<&'a str>,
    /// Empty keeps every staged edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [&'a str],
}

/// One neighbour row of an oriented pass after the staged merge:
/// `(label, neighbour, staged properties)`. The staged properties are the
/// map of a put this transaction staged for the crossed edge in the hop's
/// collection, and `None` for a label-only hop.
pub(in crate::data::executor) type StagedNeighbor<'o> = (String, String, Option<&'o [u8]>);

/// Merge a transaction's staged edge writes into one oriented pass of a
/// `NeighborsMulti` hop from `node`.
///
/// - A durable edge the transaction tombstoned drops out.
/// - A durable edge the transaction re-put carries the staged map.
/// - A staged put the durable list lacks joins, with its staged map, when
///   its label passes `scope.edge_labels`.
pub(in crate::data::executor) fn merge_staged_hop_pass<'o>(
    overlay: &'o GraphTxnOverlay,
    scope: &StagedHopScope<'_>,
    node: &str,
    pass: Orientation,
    durable: Vec<(String, String)>,
) -> Vec<StagedNeighbor<'o>> {
    let StagedHopScope {
        database_id,
        tenant,
        collection,
        edge_labels,
    } = *scope;
    let coll_key: Option<GraphCollKey> = collection.map(|c| (database_id, tenant, c.to_owned()));
    let mut merged: Vec<StagedNeighbor<'o>> = Vec::with_capacity(durable.len());
    for (label, other) in durable {
        let (src, dst) = pass.endpoints(node, &other);
        let (tombstoned, staged) = match &coll_key {
            Some(key) => (
                overlay.is_edge_tombstoned(key, src, &label, dst),
                overlay.staged_edge_properties(key, src, &label, dst),
            ),
            None => (
                overlay.is_edge_tombstoned_any_collection(database_id, tenant, src, &label, dst),
                None,
            ),
        };
        if !tombstoned {
            merged.push((label, other, staged));
        }
    }

    let staged_rows: Vec<StagedNeighbor<'o>> = match (&coll_key, pass) {
        (Some(key), Orientation::Out) => overlay
            .edges_for_src(key, node)
            .map(|(label, dst, props)| (label.to_owned(), dst.to_owned(), Some(props)))
            .collect(),
        (Some(key), Orientation::In) => overlay
            .edges_for_dst(key, node)
            .map(|(label, src, props)| (label.to_owned(), src.to_owned(), Some(props)))
            .collect(),
        (None, Orientation::Out) => overlay
            .edges_for_src_any_collection(database_id, tenant, node)
            .into_iter()
            .map(|(label, dst, _)| (label, dst, None))
            .collect(),
        (None, Orientation::In) => overlay
            .edges_for_dst_any_collection(database_id, tenant, node)
            .into_iter()
            .map(|(label, src, _)| (label, src, None))
            .collect(),
    };
    if staged_rows.is_empty() {
        return merged;
    }
    let mut present: HashSet<(String, String)> = merged
        .iter()
        .map(|(label, other, _)| (label.clone(), other.clone()))
        .collect();
    for (label, other, staged) in staged_rows {
        if !LabelFilter::keeps_name(edge_labels, &label) {
            continue;
        }
        if present.insert((label.clone(), other.clone())) {
            merged.push((label, other, staged));
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant() -> TenantId {
        TenantId::new(1)
    }

    fn coll_key(coll: &str) -> (DatabaseId, TenantId, String) {
        (DatabaseId::new(1), tenant(), coll.to_string())
    }

    #[test]
    fn no_overlay_returns_durable_unchanged() {
        let durable = vec![("knows".to_string(), "b".to_string())];
        let out = merge_graph_txn_overlay_neighbors(
            None,
            DatabaseId::new(1),
            tenant(),
            "a",
            &[],
            Direction::Out,
            durable.clone(),
        );
        assert_eq!(out, durable);
    }

    #[test]
    fn staged_put_added_for_out_direction() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());

        let out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "a",
            &[],
            Direction::Out,
            Vec::new(),
        );
        assert_eq!(out, vec![("knows".to_string(), "b".to_string())]);
    }

    #[test]
    fn staged_put_added_for_in_direction() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());

        let out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "b",
            &[],
            Direction::In,
            Vec::new(),
        );
        assert_eq!(out, vec![("knows".to_string(), "a".to_string())]);
    }

    #[test]
    fn tombstoned_durable_edge_excluded() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "c");

        let durable = vec![("knows".to_string(), "c".to_string())];
        let out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "a",
            &[],
            Direction::Out,
            durable,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn label_filter_excludes_non_matching_staged_edge() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "other_label", "b", Vec::new());

        let out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "a",
            &["knows"],
            Direction::Out,
            Vec::new(),
        );
        assert!(out.is_empty());
    }

    #[test]
    fn label_set_keeps_staged_edges_under_any_listed_label() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());
        overlay.stage_edge_put(coll_key("g"), "a", "works", "c", Vec::new());
        overlay.stage_edge_put(coll_key("g"), "a", "likes", "d", Vec::new());

        let mut out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "a",
            &["knows", "works", "absent"],
            Direction::Out,
            Vec::new(),
        );
        out.sort();
        assert_eq!(
            out,
            vec![
                ("knows".to_string(), "b".to_string()),
                ("works".to_string(), "c".to_string()),
            ]
        );
    }

    #[test]
    fn build_delta_carries_staged_edges_and_tombstones() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());
        overlay.stage_edge_delete(coll_key("g"), "x", "knows", "y");

        let delta = build_graph_overlay_delta(&overlay, DatabaseId::new(1), tenant());
        assert!(!delta.is_empty());
        let out: Vec<_> = delta.out_neighbors("a", &[]).collect();
        assert_eq!(out, vec![("knows", "b")]);
        let inn: Vec<_> = delta.in_neighbors("b", &[]).collect();
        assert_eq!(inn, vec![("knows", "a")]);
        assert!(delta.is_tombstoned("x", "knows", "y"));
    }

    #[test]
    fn build_delta_scopes_to_database_tenant() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());

        // A different database id sees none of the staged edges.
        let delta = build_graph_overlay_delta(&overlay, DatabaseId::new(999), tenant());
        assert!(delta.is_empty());
    }

    #[test]
    fn unrelated_node_unaffected() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());

        let out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "z",
            &[],
            Direction::Out,
            Vec::new(),
        );
        assert!(out.is_empty());
    }

    fn triple(src: &str, dst: &str) -> (String, String, String) {
        (src.to_string(), "knows".to_string(), dst.to_string())
    }

    /// The collection merge folds only the named collection's staged writes:
    /// a tombstone there removes a durable edge, a put there adds one, and
    /// another collection's put is not an edge of this collection.
    #[test]
    fn collection_merge_folds_only_its_own_collection() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "b");
        overlay.stage_edge_put(coll_key("g"), "c", "knows", "a", Vec::new());
        overlay.stage_edge_put(coll_key("other"), "a", "knows", "d", Vec::new());

        let out = merge_graph_txn_overlay_collection_edges(
            Some(&overlay),
            &coll_key("g"),
            "a",
            &[],
            Direction::Both,
            vec![triple("a", "b"), triple("a", "e")],
        );
        assert_eq!(out, vec![triple("a", "e"), triple("c", "a")]);
    }

    fn hop_scope<'a>(collection: Option<&'a str>, labels: &'a [&'a str]) -> StagedHopScope<'a> {
        StagedHopScope {
            database_id: DatabaseId::new(1),
            tenant: tenant(),
            collection,
            edge_labels: labels,
        }
    }

    fn pair(label: &str, node: &str) -> (String, String) {
        (label.to_string(), node.to_string())
    }

    /// In one collection: a tombstone drops a durable edge, a re-put carries
    /// its staged map, a new put joins with its map, and another
    /// collection's put stays out.
    #[test]
    fn staged_hop_pass_folds_its_collection() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "b");
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "c", vec![0x80]);
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "d", vec![0x81]);
        overlay.stage_edge_put(coll_key("g"), "a", "likes", "e", Vec::new());
        overlay.stage_edge_put(coll_key("other"), "a", "knows", "f", Vec::new());

        let mut out = merge_staged_hop_pass(
            &overlay,
            &hop_scope(Some("g"), &["knows"]),
            "a",
            Orientation::Out,
            vec![pair("knows", "b"), pair("knows", "c"), pair("knows", "z")],
        );
        out.sort();
        assert_eq!(
            out,
            vec![
                ("knows".to_string(), "c".to_string(), Some(&[0x80u8][..])),
                ("knows".to_string(), "d".to_string(), Some(&[0x81u8][..])),
                ("knows".to_string(), "z".to_string(), None),
            ]
        );
    }

    /// An incoming pass resolves each row to its physical `(src, dst)`.
    #[test]
    fn staged_hop_pass_reads_incoming_edges_by_destination() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "x", "knows", "a");
        overlay.stage_edge_put(coll_key("g"), "y", "knows", "a", vec![0x80]);
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "w", Vec::new());

        let mut out = merge_staged_hop_pass(
            &overlay,
            &hop_scope(Some("g"), &[]),
            "a",
            Orientation::In,
            vec![pair("knows", "x"), pair("knows", "v")],
        );
        out.sort();
        assert_eq!(
            out,
            vec![
                ("knows".to_string(), "v".to_string(), None),
                ("knows".to_string(), "y".to_string(), Some(&[0x80u8][..])),
            ]
        );
    }

    /// A label-only hop folds every collection's writes and carries no map.
    #[test]
    fn staged_hop_pass_without_collection_spans_collections() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "b");
        overlay.stage_edge_put(coll_key("other"), "a", "knows", "f", vec![0x80]);

        let mut out = merge_staged_hop_pass(
            &overlay,
            &hop_scope(None, &[]),
            "a",
            Orientation::Out,
            vec![pair("knows", "b")],
        );
        out.sort();
        assert_eq!(out, vec![("knows".to_string(), "f".to_string(), None)]);
    }

    #[test]
    fn collection_merge_without_overlay_sorts_and_dedups() {
        let out = merge_graph_txn_overlay_collection_edges(
            None,
            &coll_key("g"),
            "a",
            &[],
            Direction::Both,
            vec![triple("a", "z"), triple("a", "b"), triple("a", "z")],
        );
        assert_eq!(out, vec![triple("a", "b"), triple("a", "z")]);
    }
}
