// SPDX-License-Identifier: BUSL-1.1

//! Read-your-own-writes for graph reads on one core: each read carries the
//! transaction's `txn_id`, and the core merges that transaction's staged
//! edge writes into its answer.
//!
//! - Label sets: `Neighbors` and `Hop` read back a staged edge under any
//!   listed label.
//! - `NeighborsMulti`, the hop of the `GRAPH TRAVERSE` / `GRAPH PATH` walks:
//!   a staged put adds an edge, a staged delete removes one, and a staged
//!   property change is the map the edge predicate tests and the row returns.

use nodedb::bridge::envelope::Status;
use nodedb::engine::graph::edge_store::Direction;
use nodedb::types::TxnId;
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_types::Value;
use nodedb_types::filter::MetadataFilter;

use super::helpers::*;
use super::test_graph_savepoint_overlay::{
    neighbor_nodes, send_txn, stage_edge_delete, stage_edge_put, stage_edge_put_with,
};

const COLLECTION: &str = "g";

type Core = (
    nodedb::data::executor::core_loop::CoreLoop,
    nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    tempfile::TempDir,
);

fn properties(json: serde_json::Value) -> Vec<u8> {
    nodedb_types::json_msgpack::json_to_msgpack(&json).expect("encode properties")
}

fn collection() -> nodedb_types::QualifiedCollection {
    nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, COLLECTION)
}

/// A core holding the durable `edges` as `(src, label, dst, properties)`.
fn core_with(edges: &[(&str, &str, &str, Vec<u8>)]) -> Core {
    let (mut core, mut tx, mut rx, dir) = make_core();
    for (src, label, dst, props) in edges {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: collection(),
                src_id: (*src).into(),
                label: (*label).into(),
                dst_id: (*dst).into(),
                properties: props.clone(),
                src_surrogate: doc_surrogate(src),
                dst_surrogate: doc_surrogate(dst),
            }),
        );
    }
    (core, tx, rx, dir)
}

/// Stage `plan` under `txn_id` and require it to be accepted.
fn stage(core: &mut Core, txn_id: TxnId, plan: PhysicalPlan) {
    let (core, tx, rx, _) = core;
    let resp = send_txn(core, tx, rx, txn_id, plan);
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
}

/// The `NeighborsMulti` hop of a walk from `node`.
fn walk_hop(
    node: &str,
    direction: Direction,
    edge_predicate: Vec<MetadataFilter>,
    with_properties: bool,
) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::NeighborsMulti {
        collection: Some(collection()),
        node_ids: vec![node.into()],
        edge_labels: Vec::new(),
        direction,
        max_results: 0,
        rls_filters: Vec::new(),
        edge_predicate,
        with_properties,
    })
}

/// A walk hop with no predicate that returns no properties.
fn plain_hop(node: &str, direction: Direction) -> PhysicalPlan {
    walk_hop(node, direction, Vec::new(), false)
}

fn score_above(n: i64) -> Vec<MetadataFilter> {
    vec![MetadataFilter::Gt {
        field: "score".into(),
        value: Value::Integer(n),
    }]
}

/// Run `plan` inside `txn_id`, or outside any transaction for `None`, and
/// return its rows as `(src, node, properties)`, sorted.
fn read_rows(
    core: &mut Core,
    txn_id: Option<TxnId>,
    plan: PhysicalPlan,
) -> Vec<(String, String, Option<serde_json::Value>)> {
    let (core, tx, rx, _) = core;
    let payload = match txn_id {
        Some(txn_id) => {
            let resp = send_txn(core, tx, rx, txn_id, plan);
            assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
            resp.payload.as_ref().to_vec()
        }
        None => send_ok(core, tx, rx, plan),
    };
    let rows: Vec<serde_json::Value> =
        serde_json::from_value(payload_value(&payload)).expect("rows array");
    let mut out: Vec<(String, String, Option<serde_json::Value>)> = rows
        .iter()
        .map(|row| {
            (
                row["src"].as_str().expect("src").to_string(),
                row["node"].as_str().expect("node").to_string(),
                row.get("properties").cloned(),
            )
        })
        .collect();
    out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    out
}

fn bare(src: &str, node: &str) -> (String, String, Option<serde_json::Value>) {
    (src.into(), node.into(), None)
}

fn with(
    src: &str,
    node: &str,
    props: serde_json::Value,
) -> (String, String, Option<serde_json::Value>) {
    (src.into(), node.into(), Some(props))
}

#[test]
fn a_walk_hop_reads_a_staged_edge_with_its_staged_properties() {
    let mut core = core_with(&[("a", "L", "b", properties(serde_json::json!({"score": 9})))]);
    let txn_id = TxnId::new(11);
    stage(
        &mut core,
        txn_id,
        stage_edge_put_with(
            COLLECTION,
            "a",
            "L",
            "c",
            properties(serde_json::json!({"score": 7})),
        ),
    );
    stage(
        &mut core,
        txn_id,
        stage_edge_put_with(
            COLLECTION,
            "a",
            "L",
            "d",
            properties(serde_json::json!({"score": 1})),
        ),
    );

    let inside = read_rows(
        &mut core,
        Some(txn_id),
        walk_hop("a", Direction::Out, score_above(5), true),
    );
    assert_eq!(
        inside,
        vec![
            with("a", "b", serde_json::json!({"score": 9})),
            with("a", "c", serde_json::json!({"score": 7})),
        ],
        "the predicate admits the staged c and rejects the staged d"
    );
    let outside = read_rows(
        &mut core,
        None,
        walk_hop("a", Direction::Out, score_above(5), true),
    );
    assert_eq!(
        outside,
        vec![with("a", "b", serde_json::json!({"score": 9}))]
    );

    // The incoming pass of `c` finds the staged edge in its physical shape.
    let incoming = read_rows(&mut core, Some(txn_id), plain_hop("c", Direction::In));
    assert_eq!(incoming, vec![bare("c", "a")]);
}

#[test]
fn a_walk_hop_drops_a_staged_delete() {
    let mut core = core_with(&[
        ("a", "L", "b", Vec::new()),
        ("a", "L", "c", Vec::new()),
        ("e", "L", "a", Vec::new()),
    ]);
    let txn_id = TxnId::new(12);
    stage(
        &mut core,
        txn_id,
        stage_edge_delete(COLLECTION, "a", "L", "b"),
    );
    stage(
        &mut core,
        txn_id,
        stage_edge_delete(COLLECTION, "e", "L", "a"),
    );

    let inside = read_rows(&mut core, Some(txn_id), plain_hop("a", Direction::Both));
    assert_eq!(inside, vec![bare("a", "c")]);
    let outside = read_rows(&mut core, None, plain_hop("a", Direction::Both));
    assert_eq!(
        outside,
        vec![bare("a", "b"), bare("a", "c"), bare("a", "e")]
    );
    let reverse = read_rows(&mut core, Some(txn_id), plain_hop("b", Direction::In));
    assert!(
        reverse.is_empty(),
        "the deleted edge is gone from b: {reverse:?}"
    );
}

#[test]
fn a_walk_hop_tests_and_returns_a_staged_property_change() {
    let mut core = core_with(&[("a", "L", "b", properties(serde_json::json!({"score": 9})))]);
    let txn_id = TxnId::new(13);
    stage(
        &mut core,
        txn_id,
        stage_edge_put_with(
            COLLECTION,
            "a",
            "L",
            "b",
            properties(serde_json::json!({"score": 1})),
        ),
    );

    let filtered = read_rows(
        &mut core,
        Some(txn_id),
        walk_hop("a", Direction::Out, score_above(5), false),
    );
    assert!(
        filtered.is_empty(),
        "the staged score 1 fails score > 5: {filtered:?}"
    );
    let with_properties = walk_hop("a", Direction::Out, Vec::new(), true);
    let returned = read_rows(&mut core, Some(txn_id), with_properties);
    assert_eq!(
        returned,
        vec![with("a", "b", serde_json::json!({"score": 1}))]
    );
    let outside = read_rows(
        &mut core,
        None,
        walk_hop("a", Direction::Out, score_above(5), false),
    );
    assert_eq!(outside, vec![bare("a", "b")]);
}

/// A label-only walk hop folds a transaction's writes in every collection.
#[test]
fn a_label_only_walk_hop_reads_staged_edges() {
    let mut core = core_with(&[("a", "L", "b", Vec::new())]);
    let txn_id = TxnId::new(14);
    stage(&mut core, txn_id, stage_edge_put(COLLECTION, "a", "L", "c"));
    stage(
        &mut core,
        txn_id,
        stage_edge_delete(COLLECTION, "a", "L", "b"),
    );

    let plan = PhysicalPlan::Graph(GraphOp::NeighborsMulti {
        collection: None,
        node_ids: vec!["a".into()],
        edge_labels: vec!["L".into()],
        direction: Direction::Out,
        max_results: 0,
        rls_filters: Vec::new(),
        edge_predicate: Vec::new(),
        with_properties: false,
    });
    assert_eq!(
        read_rows(&mut core, Some(txn_id), plan),
        vec![bare("a", "c")]
    );
}

fn hop_nodes(payload: &[u8]) -> Vec<String> {
    let mut nodes: Vec<String> = serde_json::from_value(payload_value(payload)).unwrap();
    nodes.sort();
    nodes
}

fn label_set_hop(depth: usize) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::Hop {
        start_nodes: vec!["a".into()],
        edge_labels: vec!["knows".into(), "works".into()],
        direction: Direction::Out,
        depth,
        options: Default::default(),
        rls_filters: Vec::new(),
        frontier_bitmap: None,
        collection: None,
    })
}

/// A staged edge under the second listed label is read back at depth 1 and
/// at depth > 1. A staged edge under an unlisted label is not.
#[test]
fn staged_edge_under_second_listed_label_is_read_back() {
    let mut core = core_with(&[("a", "knows", "b", Vec::new())]);
    let txn_id = TxnId::new(4);
    for (src, label, dst) in [
        ("a", "works", "c"),
        ("c", "works", "d"),
        ("a", "likes", "x"),
    ] {
        stage(
            &mut core,
            txn_id,
            stage_edge_put(COLLECTION, src, label, dst),
        );
    }
    let (core, tx, rx, _dir) = &mut core;

    let resp = send_txn(
        core,
        tx,
        rx,
        txn_id,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "a".into(),
            edge_labels: vec!["knows".into(), "works".into()],
            direction: Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    let mut nodes = neighbor_nodes(resp.payload.as_ref());
    nodes.sort();
    assert_eq!(nodes, vec!["b", "c"], "depth-1 neighbors");

    let resp = send_txn(core, tx, rx, txn_id, label_set_hop(1));
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    assert_eq!(
        hop_nodes(resp.payload.as_ref()),
        vec!["a", "b", "c"],
        "depth-1 hop"
    );

    let resp = send_txn(core, tx, rx, txn_id, label_set_hop(2));
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    assert_eq!(
        hop_nodes(resp.payload.as_ref()),
        vec!["a", "b", "c", "d"],
        "depth-2 hop follows staged edges through a staged-only node"
    );
}
