// SPDX-License-Identifier: BUSL-1.1

//! `GRAPH TRAVERSE` from a start the graph does not hold.
//!
//! The graph holds a node exactly while an edge names it. A start no edge
//! names yields the empty subgraph, as `GRAPH PATH` yields no path for an
//! absent endpoint. A start the graph holds is in the result even when no
//! edge passes the walk's label filter.

use crate::harness::TestServer;

/// The JSON of a one-cell `result` answer.
async fn result_json(server: &TestServer, sql: &str) -> serde_json::Value {
    let rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let [text] = rows.as_slice() else {
        panic!("{sql}: expected one result row, got {rows:?}");
    };
    serde_json::from_str(text).unwrap_or_else(|e| panic!("{sql}: result is not JSON: {e}"))
}

async fn seed(server: &TestServer, collection: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{collection}' FROM 'a' TO 'b' TYPE 'l'"
        ))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_absent_start_yields_the_empty_subgraph() {
    let server = TestServer::start().await;
    seed(&server, "trv_absent").await;

    let subgraph = result_json(
        &server,
        "GRAPH TRAVERSE IN 'trv_absent' FROM 'ghost' DEPTH 2 DIRECTION both",
    )
    .await;
    assert_eq!(
        subgraph,
        serde_json::json!({ "nodes": [], "edges": [] }),
        "a start no edge names is absent from the graph"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_present_start_with_no_matching_edge_yields_only_the_start() {
    let server = TestServer::start().await;
    seed(&server, "trv_unmatched").await;

    let subgraph = result_json(
        &server,
        "GRAPH TRAVERSE IN 'trv_unmatched' FROM 'a' DEPTH 2 LABEL 'other' DIRECTION out",
    )
    .await;
    assert_eq!(
        subgraph,
        serde_json::json!({ "nodes": [{ "id": "a", "depth": 0 }], "edges": [] }),
        "a held start stays in the result when no edge passes the label filter"
    );
}
