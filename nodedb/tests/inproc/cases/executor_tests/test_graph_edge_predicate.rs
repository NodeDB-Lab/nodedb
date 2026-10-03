// SPDX-License-Identifier: BUSL-1.1

//! `NeighborsMulti` edge-property work on one core: the predicate runs
//! before a row counts against `max_results`, `Both` tests each edge in its
//! stored orientation, and rows carry the edge's property object on request.

use nodedb::bridge::envelope::Status;
use nodedb::engine::graph::edge_store::Direction;
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_types::Value;
use nodedb_types::filter::MetadataFilter;

use super::helpers::*;

fn collection() -> nodedb_types::QualifiedCollection {
    nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, "col")
}

fn properties(json: serde_json::Value) -> Vec<u8> {
    nodedb_types::json_msgpack::json_to_msgpack(&json).expect("encode properties")
}

type Core = (
    nodedb::data::executor::core_loop::CoreLoop,
    nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    tempfile::TempDir,
);

/// A core holding `edges` as `(src, dst, properties)` under label `L`.
fn core_with(edges: &[(&str, &str, Vec<u8>)]) -> Core {
    let (mut core, mut tx, mut rx, dir) = make_core();
    for (src, dst, props) in edges {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: collection(),
                src_id: (*src).into(),
                label: "L".into(),
                dst_id: (*dst).into(),
                properties: props.clone(),
                src_surrogate: doc_surrogate(src),
                dst_surrogate: doc_surrogate(dst),
            }),
        );
    }
    (core, tx, rx, dir)
}

fn neighbors_multi(
    node: &str,
    direction: Direction,
    max_results: u32,
    edge_predicate: Vec<MetadataFilter>,
    with_properties: bool,
) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::NeighborsMulti {
        collection: Some(collection()),
        node_ids: vec![node.into()],
        edge_labels: Vec::new(),
        direction,
        max_results,
        rls_filters: Vec::new(),
        edge_predicate,
        with_properties,
    })
}

fn score_above(n: i64) -> Vec<MetadataFilter> {
    vec![MetadataFilter::Gt {
        field: "score".into(),
        value: Value::Integer(n),
    }]
}

/// `(src, label, node)` of every row, sorted.
fn rows(payload: &[u8]) -> Vec<(String, String, String)> {
    let rows: Vec<serde_json::Value> =
        serde_json::from_value(payload_value(payload)).expect("rows array");
    let mut out: Vec<(String, String, String)> = rows
        .iter()
        .map(|row| {
            (
                row["src"].as_str().expect("src").to_string(),
                row["label"].as_str().expect("label").to_string(),
                row["node"].as_str().expect("node").to_string(),
            )
        })
        .collect();
    out.sort();
    out
}

fn row(src: &str, node: &str) -> (String, String, String) {
    (src.into(), "L".into(), node.into())
}

#[test]
fn the_visit_cap_counts_only_admitted_edges() {
    let (mut core, mut tx, mut rx, _dir) = core_with(&[
        ("a", "b", properties(serde_json::json!({"score": 1}))),
        ("a", "c", properties(serde_json::json!({"score": 9}))),
        ("a", "d", properties(serde_json::json!({"score": 2}))),
    ]);

    let filtered = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        neighbors_multi("a", Direction::Out, 1, score_above(5), false),
    );
    assert_eq!(
        filtered.status,
        Status::Ok,
        "one admitted edge fits a cap of one"
    );
    assert_eq!(rows(&filtered.payload), vec![row("a", "c")]);

    let unfiltered = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        neighbors_multi("a", Direction::Out, 1, Vec::new(), false),
    );
    assert_eq!(
        unfiltered.status,
        Status::Partial,
        "three edges overflow a cap of one"
    );
}

#[test]
fn both_tests_each_edge_in_its_stored_orientation() {
    let (mut core, mut tx, mut rx, _dir) = core_with(&[
        ("a", "b", properties(serde_json::json!({"score": 9}))),
        ("c", "a", properties(serde_json::json!({"score": 9}))),
        ("a", "d", properties(serde_json::json!({"score": 1}))),
        ("e", "a", properties(serde_json::json!({"score": 1}))),
    ]);
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        neighbors_multi("a", Direction::Both, 0, score_above(5), false),
    );
    assert_eq!(rows(&payload), vec![row("a", "b"), row("a", "c")]);
}

#[test]
fn rows_carry_edge_properties_on_request() {
    let (mut core, mut tx, mut rx, _dir) = core_with(&[
        (
            "a",
            "b",
            properties(serde_json::json!({"score": 9, "kind": "road"})),
        ),
        ("a", "c", Vec::new()),
    ]);
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        neighbors_multi("a", Direction::Out, 0, Vec::new(), true),
    );
    let mut rows: Vec<serde_json::Value> =
        serde_json::from_value(payload_value(&payload)).expect("rows array");
    rows.sort_by(|x, y| x["node"].as_str().cmp(&y["node"].as_str()));
    assert_eq!(
        rows[0]["properties"],
        serde_json::json!({"score": 9, "kind": "road"})
    );
    assert_eq!(rows[1]["properties"], serde_json::json!({}));

    let without = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        neighbors_multi("a", Direction::Out, 0, Vec::new(), false),
    );
    let without: Vec<serde_json::Value> =
        serde_json::from_value(payload_value(&without)).expect("rows array");
    assert!(
        without.iter().all(|row| row.get("properties").is_none()),
        "rows carry no properties unless asked: {without:?}"
    );
}

#[test]
fn a_predicate_without_a_collection_is_refused() {
    let (mut core, mut tx, mut rx, _dir) =
        core_with(&[("a", "b", properties(serde_json::json!({"score": 9})))]);
    let response = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::NeighborsMulti {
            collection: None,
            node_ids: vec!["a".into()],
            edge_labels: Vec::new(),
            direction: Direction::Out,
            max_results: 0,
            rls_filters: Vec::new(),
            edge_predicate: score_above(5),
            with_properties: false,
        }),
    );
    assert_eq!(response.status, Status::Error);
}
