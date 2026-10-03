// SPDX-License-Identifier: BUSL-1.1

//! Cross-collection isolation: the in-memory graph CSR.
//!
//! Regression for the bug where two graph-enabled collections in the same
//! `(database, tenant)` shared ONE in-memory CSR adjacency index with no
//! collection axis, so a `MATCH ... IN <collA>` (and GraphRAG expansion, which
//! both read the CSR via the collection-scoped neighbor primitive) walked edges
//! inserted under `<collB>`.
//!
//! The fix tags every CSR edge with the collection it was inserted under
//! (nodes, node-labels and surrogates stay shared across collections — only
//! edges carry the collection axis) and filters the collection-scoped read
//! paths by it. These tests exercise that primitive directly: one CSR
//! partition (one database/tenant) holding TWO collections, asserting a
//! collection-scoped read sees only its own edges while the index's own
//! unscoped read still sees the merged graph. That merged view is an internal
//! primitive, not a statement: every client traversal names a collection, since
//! one that named none could not be authorized.
//!
//! Pre-fix these assertions cannot hold: the CSR had no per-edge collection
//! tag, so a collection-scoped read of collection A necessarily also returned
//! collection B's edges.

use nodedb::engine::graph::csr::{CsrIndex, Direction};
use nodedb_mem::EngineId;

const COLL_A: &str = "collection_a";
const COLL_B: &str = "collection_b";

/// Build one CSR partition holding distinct edges under two collections.
///
/// A: alice -KNOWS-> bob, bob -KNOWS-> carol
/// B: alice -KNOWS-> dave, dave -KNOWS-> erin
///
/// `alice` is shared across both collections (nodes are shared) but its
/// out-edges belong to different collections.
fn build_two_collection_csr() -> CsrIndex {
    let mut csr = CsrIndex::new(super::support::memory::test_scoped_memory(EngineId::Graph));
    csr.add_edge_in_collection("alice", "KNOWS", "bob", COLL_A)
        .unwrap();
    csr.add_edge_in_collection("bob", "KNOWS", "carol", COLL_A)
        .unwrap();
    csr.add_edge_in_collection("alice", "KNOWS", "dave", COLL_B)
        .unwrap();
    csr.add_edge_in_collection("dave", "KNOWS", "erin", COLL_B)
        .unwrap();
    csr
}

fn out_neighbor_names(csr: &CsrIndex, node: &str, collection: &str) -> Vec<String> {
    let mut names: Vec<String> = csr
        .neighbors_in_collection(node, &["KNOWS"], Direction::Out, collection)
        .into_iter()
        .map(|(_label, n)| n)
        .collect();
    names.sort();
    names
}

/// A collection-scoped read of collection A returns ONLY A's edges — never B's —
/// even though `alice` has out-edges in both collections in the same partition.
#[test]
fn scoped_neighbors_do_not_leak_across_collections() {
    let csr = build_two_collection_csr();

    // Collection A: alice → bob only. NOT dave (that edge is collection B).
    assert_eq!(
        out_neighbor_names(&csr, "alice", COLL_A),
        vec!["bob".to_string()],
        "collection A scoped read must see alice→bob and NOT alice→dave (B)"
    );

    // Symmetric: collection B: alice → dave only.
    assert_eq!(
        out_neighbor_names(&csr, "alice", COLL_B),
        vec!["dave".to_string()],
        "collection B scoped read must see alice→dave and NOT alice→bob (A)"
    );

    // A node whose edges are entirely in A is invisible to a B-scoped read.
    assert!(
        out_neighbor_names(&csr, "bob", COLL_B).is_empty(),
        "bob's KNOWS edge is in collection A; a B-scoped read must not see it"
    );
    assert_eq!(
        out_neighbor_names(&csr, "bob", COLL_A),
        vec!["carol".to_string()],
    );
}

/// The same guarantee must hold after `compact()` merges the write buffer into
/// the dense CSR arrays — the collection tag must survive compaction, not just
/// live in the buffer.
#[test]
fn scoped_neighbors_isolated_after_compaction() {
    let mut csr = build_two_collection_csr();
    csr.compact()
        .expect("compaction without a governor cannot fail");

    assert_eq!(
        out_neighbor_names(&csr, "alice", COLL_A),
        vec!["bob".to_string()],
        "post-compaction: A must see only alice→bob"
    );
    assert_eq!(
        out_neighbor_names(&csr, "alice", COLL_B),
        vec!["dave".to_string()],
        "post-compaction: B must see only alice→dave"
    );
}

/// An `IN '<collection>'` clause naming a collection with no edges in this
/// partition matches nothing — it must NOT fall back to the merged view (which
/// would re-introduce the leak).
#[test]
fn scoped_read_of_unknown_collection_is_empty_not_merged() {
    let csr = build_two_collection_csr();
    assert!(
        out_neighbor_names(&csr, "alice", "collection_never_written").is_empty(),
        "a scoped read of an unknown collection must match nothing, never the merged graph"
    );
}

/// The index's collection-less read still sees every collection's edges. It is
/// reachable only from inside the engine — no statement can ask for it — and it
/// proves the two
/// collections genuinely share ONE partition: the per-edge tag — not a separate
/// per-collection partition — is what scopes the MATCH/RAG reads above.
#[test]
fn unscoped_neighbors_see_the_merged_graph() {
    let csr = build_two_collection_csr();
    let mut merged: Vec<String> = csr
        .neighbors("alice", &["KNOWS"], Direction::Out)
        .into_iter()
        .map(|(_label, n)| n)
        .collect();
    merged.sort();
    assert_eq!(
        merged,
        vec!["bob".to_string(), "dave".to_string()],
        "the collection-less merged view sees BOTH collections' edges for alice"
    );
}

/// The SAME triple `(X, KNOWS, Y)` inserted under BOTH collections must exist
/// as TWO distinct edges — each collection retains its own copy. A collA-scoped
/// read AND a collB-scoped read must each return it. Pre-identity-fix, edge
/// identity was `(src,label,dst)` so the second insert deduped away and collB's
/// read MISSED the shared triple (a false negative).
#[test]
fn shared_triple_is_visible_in_both_collections() {
    let mut csr = CsrIndex::new(super::support::memory::test_scoped_memory(EngineId::Graph));
    csr.add_edge_in_collection("x", "KNOWS", "y", COLL_A)
        .unwrap();
    csr.add_edge_in_collection("x", "KNOWS", "y", COLL_B)
        .unwrap();

    // Both collections see their own copy of the shared triple (buffer path).
    assert_eq!(out_neighbor_names(&csr, "x", COLL_A), vec!["y".to_string()]);
    assert_eq!(out_neighbor_names(&csr, "x", COLL_B), vec!["y".to_string()]);

    // And after compaction (dense path) — the collection tag on each copy
    // must survive the buffer→dense merge without collapsing to one edge.
    csr.compact()
        .expect("compaction without a governor cannot fail");
    assert_eq!(out_neighbor_names(&csr, "x", COLL_A), vec!["y".to_string()]);
    assert_eq!(out_neighbor_names(&csr, "x", COLL_B), vec!["y".to_string()]);
}

/// Removing collA's copy of a shared triple leaves collB's copy intact —
/// collection-scoped removal keys on the full `(src,label,dst,collection)`
/// identity, so it never over-deletes the other collection's edge.
#[test]
fn scoped_removal_of_shared_triple_leaves_other_collection_intact() {
    let mut csr = CsrIndex::new(super::support::memory::test_scoped_memory(EngineId::Graph));
    csr.add_edge_in_collection("x", "KNOWS", "y", COLL_A)
        .unwrap();
    csr.add_edge_in_collection("x", "KNOWS", "y", COLL_B)
        .unwrap();

    // Remove only collA's copy.
    csr.remove_edge_in_collection("x", "KNOWS", "y", COLL_A);
    assert!(
        out_neighbor_names(&csr, "x", COLL_A).is_empty(),
        "collA's copy of the shared triple must be gone"
    );
    assert_eq!(
        out_neighbor_names(&csr, "x", COLL_B),
        vec!["y".to_string()],
        "collB's copy of the shared triple must remain after collA's removal"
    );

    // Same guarantee across a compaction boundary (dense tombstone survives).
    csr.compact()
        .expect("compaction without a governor cannot fail");
    assert!(out_neighbor_names(&csr, "x", COLL_A).is_empty());
    assert_eq!(out_neighbor_names(&csr, "x", COLL_B), vec!["y".to_string()]);
}

/// Tenant/database scoping is orthogonal to (and preserved alongside) the new
/// collection axis: distinct partitions never share edges. Modeled here at the
/// CSR level — two independent `CsrIndex` partitions (as `ShardedCsrIndex`
/// keys them per `(database, tenant)`) never observe each other's edges
/// regardless of collection. The dedicated cross-tenant executor test
/// (`test_tenant_isolation_graph`) covers the end-to-end path.
#[test]
fn distinct_partitions_do_not_share_edges() {
    let mut tenant_a = CsrIndex::new(super::support::memory::test_scoped_memory(EngineId::Graph));
    let tenant_b = CsrIndex::new(super::support::memory::test_scoped_memory(EngineId::Graph));
    tenant_a
        .add_edge_in_collection("alice", "KNOWS", "bob", COLL_A)
        .unwrap();

    // Tenant B's partition never received the edge, under any collection.
    assert!(tenant_b.neighbors("alice", &[], Direction::Out).is_empty());
    assert!(
        out_neighbor_names(&tenant_b, "alice", COLL_A).is_empty(),
        "a separate partition must not see another partition's edges"
    );
    // Tenant A still has it.
    assert_eq!(
        out_neighbor_names(&tenant_a, "alice", COLL_A),
        vec!["bob".to_string()]
    );
}
