// SPDX-License-Identifier: BUSL-1.1

//! Read-your-own-writes merge for GRAPH reads.
//!
//! The durable neighbor list a CSR partition returns reflects only
//! committed state. When a request carries a `txn_id` with a staged
//! `GraphTxnOverlay`, these functions fold that transaction's pending edge
//! writes into the durable result: staged tombstones subtract a durable
//! neighbor, staged puts add one.
//!
//! - `Neighbors`: [`merge_graph_txn_overlay_neighbors`].
//! - `Hop`, `Path` and `Subgraph`: [`build_graph_overlay_delta`], pushed
//!   into the walk.
//! - `NeighborsMulti`, the hop of the `GRAPH TRAVERSE` / `GRAPH PATH` walk
//!   coordinators: [`merge_staged_hop_pass`], which also carries each staged
//!   put's property map to the edge predicate and the returned properties.
//!
//! Each merge reads one orientation at a time, so every row resolves to its
//! physical `(src, dst)`. A `Both` read runs an outgoing pass, then an
//! incoming pass.
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
/// [`GraphOverlayDelta`] scoped to `(database_id, tenant)`, for the `Hop`,
/// `Path` and `Subgraph` read-your-own-writes walks. The walk follows staged
/// edges through staged-only nodes.
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

/// Merge a transaction's staged GRAPH edge writes into the durable `(label,
/// node)` neighbours of `node_id` in `direction`.
///
/// `durable_of` reads the durable neighbours of one orientation. Each pass
/// merges through [`merge_staged_hop_pass`], so `scope.collection` limits
/// the staged writes to one collection, and `None` spans every collection.
/// A `Both` read is an outgoing pass, then an incoming pass. Each row then
/// resolves to its physical edge, so a staged tombstone removes only that
/// edge, and a staged put joins unless that same edge is already present.
pub(in crate::data::executor) fn merge_graph_txn_overlay_neighbors(
    overlay: &GraphTxnOverlay,
    scope: &StagedHopScope<'_>,
    node_id: &str,
    direction: Direction,
    durable_of: impl Fn(Direction) -> Vec<(String, String)>,
) -> Vec<(String, String)> {
    let mut merged: Vec<(String, String)> = Vec::new();
    for &pass in Orientation::passes(direction) {
        let durable = durable_of(pass.direction());
        // A self-loop is one edge. `Both` read it in the outbound pass.
        let repeats_self_loop =
            |node: &str| direction == Direction::Both && pass == Orientation::In && node == node_id;
        merged.extend(
            merge_staged_hop_pass(overlay, scope, node_id, pass, durable)
                .into_iter()
                .filter(|(_, node, _)| !repeats_self_loop(node))
                .map(|(label, node, _)| (label, node)),
        );
    }
    merged
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

    /// The neighbours merge of `node` over the durable edges `(src, label,
    /// dst)`, sorted.
    fn neighbors(
        overlay: &GraphTxnOverlay,
        collection: Option<&str>,
        labels: &[&str],
        node: &str,
        direction: Direction,
        durable_edges: &[(&str, &str, &str)],
    ) -> Vec<(String, String)> {
        let scope = StagedHopScope {
            database_id: DatabaseId::new(1),
            tenant: tenant(),
            collection,
            edge_labels: labels,
        };
        let durable_of = |pass: Direction| {
            let mut rows = Vec::new();
            for &(src, label, dst) in durable_edges {
                if matches!(pass, Direction::Out | Direction::Both) && src == node {
                    rows.push((label.to_string(), dst.to_string()));
                }
                if matches!(pass, Direction::In | Direction::Both) && dst == node {
                    rows.push((label.to_string(), src.to_string()));
                }
            }
            rows
        };
        let mut out =
            merge_graph_txn_overlay_neighbors(overlay, &scope, node, direction, durable_of);
        out.sort();
        out
    }

    #[test]
    fn staged_put_added_for_out_direction() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());

        let out = neighbors(&overlay, None, &[], "a", Direction::Out, &[]);
        assert_eq!(out, vec![pair("knows", "b")]);
    }

    #[test]
    fn staged_put_added_for_in_direction() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());

        let out = neighbors(&overlay, None, &[], "b", Direction::In, &[]);
        assert_eq!(out, vec![pair("knows", "a")]);
    }

    #[test]
    fn tombstoned_durable_edge_excluded() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "c");

        let durable = [("a", "knows", "c")];
        let out = neighbors(&overlay, None, &[], "a", Direction::Out, &durable);
        assert!(out.is_empty());
    }

    /// A named collection merges only that collection's staged writes.
    /// Another collection's put and tombstone leave its neighbours alone.
    /// No collection spans every collection.
    #[test]
    fn a_named_collection_merges_only_its_own_staged_writes() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "c", Vec::new());
        overlay.stage_edge_put(coll_key("other"), "a", "knows", "d", Vec::new());
        overlay.stage_edge_delete(coll_key("other"), "a", "knows", "b");
        let durable = [("a", "knows", "b")];

        let scoped = neighbors(&overlay, Some("g"), &[], "a", Direction::Out, &durable);
        assert_eq!(scoped, vec![pair("knows", "b"), pair("knows", "c")]);

        let spanning = neighbors(&overlay, None, &[], "a", Direction::Out, &durable);
        assert_eq!(spanning, vec![pair("knows", "c"), pair("knows", "d")]);
    }

    /// `Both` reads the incoming pass by destination: deleting `a -> b`
    /// removes `a` from the neighbours of `b`.
    #[test]
    fn both_drops_a_deleted_in_edge_queried_from_its_destination() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "b");
        let durable = [("a", "knows", "b")];

        let out = neighbors(&overlay, Some("g"), &[], "b", Direction::Both, &durable);
        assert!(out.is_empty(), "{out:?}");
    }

    /// Deleting the reverse edge `b -> a` beside a live `a -> b` removes
    /// only the reverse row.
    #[test]
    fn both_drops_only_the_deleted_reverse_edge() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "b", "knows", "a");
        let durable = [("a", "knows", "b"), ("b", "knows", "a")];

        let out = neighbors(&overlay, Some("g"), &[], "a", Direction::Both, &durable);
        assert_eq!(out, vec![pair("knows", "b")]);
    }

    /// A staged reverse put `b -> a` beside a durable `a -> b` is a second
    /// physical edge, so `Both` returns a row for each.
    #[test]
    fn both_keeps_a_staged_reverse_put_beside_the_forward_edge() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "b", "knows", "a", Vec::new());
        let durable = [("a", "knows", "b")];

        let out = neighbors(&overlay, Some("g"), &[], "a", Direction::Both, &durable);
        assert_eq!(out, vec![pair("knows", "b"), pair("knows", "b")]);
        let out = neighbors(&overlay, None, &[], "a", Direction::Both, &durable);
        assert_eq!(out, vec![pair("knows", "b"), pair("knows", "b")]);
    }

    #[test]
    fn label_filter_excludes_non_matching_staged_edge() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "other_label", "b", Vec::new());

        let out = neighbors(&overlay, None, &["knows"], "a", Direction::Out, &[]);
        assert!(out.is_empty());
    }

    #[test]
    fn label_set_keeps_staged_edges_under_any_listed_label() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());
        overlay.stage_edge_put(coll_key("g"), "a", "works", "c", Vec::new());
        overlay.stage_edge_put(coll_key("g"), "a", "likes", "d", Vec::new());

        let out = neighbors(
            &overlay,
            None,
            &["knows", "works", "absent"],
            "a",
            Direction::Out,
            &[],
        );
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

        let out = neighbors(&overlay, None, &[], "z", Direction::Out, &[]);
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
