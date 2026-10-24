// SPDX-License-Identifier: BUSL-1.1

//! Graph walks inside a transaction on a core that holds no CSR partition
//! for the tenant.
//!
//! The tenant's only edges are the transaction's staged writes. `Hop`,
//! `Path` and `Subgraph` follow them, and drop a start no staged edge
//! names, as a walk over a durable partition does.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb::engine::graph::edge_store::Direction;
use nodedb::types::TxnId;
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};

use super::helpers::*;
use super::test_graph_savepoint_overlay::{send_txn, stage_edge_put};

const COLLECTION: &str = "g";

type Core = (
    nodedb::data::executor::core_loop::CoreLoop,
    nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    tempfile::TempDir,
);

/// A core with no durable edge, and the staged `a -L-> x -L-> y` under
/// `txn_id`.
fn staged_chain(txn_id: TxnId) -> Core {
    let (mut core, mut tx, mut rx, dir) = make_core();
    for (src, dst) in [("a", "x"), ("x", "y")] {
        let resp = send_txn(
            &mut core,
            &mut tx,
            &mut rx,
            txn_id,
            stage_edge_put(COLLECTION, src, "L", dst),
        );
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    }
    (core, tx, rx, dir)
}

/// Run `plan` inside `txn_id` and return its response.
fn read(core: &mut Core, txn_id: TxnId, plan: PhysicalPlan) -> nodedb::bridge::envelope::Response {
    let (core, tx, rx, _) = core;
    send_txn(core, tx, rx, txn_id, plan)
}

fn hop(start_nodes: &[&str], depth: usize) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::Hop {
        start_nodes: start_nodes.iter().map(|n| n.to_string()).collect(),
        edge_labels: vec!["L".into()],
        direction: Direction::Out,
        depth,
        options: Default::default(),
        rls_filters: Vec::new(),
        frontier_bitmap: None,
        collection: None,
        scope: nodedb_physical::physical_plan::GraphReadScope::default(),
    })
}

fn hop_nodes(core: &mut Core, txn_id: TxnId, plan: PhysicalPlan) -> Vec<String> {
    let resp = read(core, txn_id, plan);
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    let mut nodes: Vec<String> =
        serde_json::from_value(payload_value(resp.payload.as_ref())).expect("node array");
    nodes.sort();
    nodes
}

fn path(src: &str, dst: &str) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::Path {
        src: src.into(),
        dst: dst.into(),
        edge_labels: vec!["L".into()],
        max_depth: 5,
        options: Default::default(),
        rls_filters: Vec::new(),
        frontier_bitmap: None,
        collection: None,
        scope: nodedb_physical::physical_plan::GraphReadScope::default(),
    })
}

#[test]
fn a_hop_follows_staged_edges_without_a_partition() {
    let txn_id = TxnId::new(21);
    let mut core = staged_chain(txn_id);

    assert_eq!(
        hop_nodes(&mut core, txn_id, hop(&["a"], 1)),
        vec!["a", "x"],
        "depth 1 keeps the staged start"
    );
    assert_eq!(
        hop_nodes(&mut core, txn_id, hop(&["a"], 2)),
        vec!["a", "x", "y"],
        "depth 2 walks through the staged-only node"
    );
    assert_eq!(
        hop_nodes(&mut core, txn_id, hop(&["a", "ghost"], 2)),
        vec!["a", "x", "y"],
        "a start no staged edge names is dropped"
    );
}

#[test]
fn a_path_follows_staged_edges_without_a_partition() {
    let txn_id = TxnId::new(22);
    let mut core = staged_chain(txn_id);

    let resp = read(&mut core, txn_id, path("a", "y"));
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    let found: Vec<String> =
        serde_json::from_value(payload_value(resp.payload.as_ref())).expect("path array");
    assert_eq!(found, vec!["a", "x", "y"]);

    let resp = read(&mut core, txn_id, path("ghost", "ghost"));
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::NotFound),
        "an absent node has no path to itself"
    );
}

#[test]
fn a_subgraph_follows_staged_edges_without_a_partition() {
    let txn_id = TxnId::new(23);
    let mut core = staged_chain(txn_id);

    let resp = read(
        &mut core,
        txn_id,
        PhysicalPlan::Graph(GraphOp::Subgraph {
            start_nodes: vec!["a".into()],
            edge_labels: vec!["L".into()],
            depth: 2,
            options: Default::default(),
            rls_filters: Vec::new(),
            collection: None,
            scope: nodedb_physical::physical_plan::GraphReadScope::default(),
        }),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    let edges: Vec<serde_json::Value> =
        serde_json::from_value(payload_value(resp.payload.as_ref())).expect("edge array");
    let mut triples: Vec<(String, String, String)> = edges
        .iter()
        .map(|edge| {
            (
                edge["src"].as_str().expect("src").to_string(),
                edge["label"].as_str().expect("label").to_string(),
                edge["dst"].as_str().expect("dst").to_string(),
            )
        })
        .collect();
    triples.sort();
    assert_eq!(
        triples,
        vec![
            ("a".to_string(), "L".to_string(), "x".to_string()),
            ("x".to_string(), "L".to_string(), "y".to_string()),
        ]
    );
}
