// SPDX-License-Identifier: BUSL-1.1

//! Read-your-own-writes for the multi-hop graph walks. Inside `BEGIN`,
//! `GRAPH TRAVERSE` and `GRAPH PATH` see the transaction's staged
//! `GRAPH INSERT EDGE` and `GRAPH DELETE EDGE`:
//!
//! - A staged insert adds an edge to the walk.
//! - A staged delete removes one.
//! - A staged re-insert with new properties is the map `EDGE WHERE` tests
//!   and `GRAPH TRAVERSE` returns.
//!
//! `ROLLBACK` discards all three.

use std::collections::BTreeSet;

use crate::harness::TestServer;

const COLLECTION: &str = "gw_tx";

async fn insert_edge(server: &TestServer, src: &str, dst: &str, score: i64) {
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{COLLECTION}' FROM '{src}' TO '{dst}' TYPE 'LINK' \
             PROPERTIES {{ score: {score} }}"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {e}"));
}

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

fn node_ids(subgraph: &serde_json::Value) -> BTreeSet<String> {
    subgraph["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .map(|node| node["id"].as_str().expect("node id").to_string())
        .collect()
}

/// `(from, to, properties)` of every result edge.
fn edges(subgraph: &serde_json::Value) -> BTreeSet<(String, String, String)> {
    subgraph["edges"]
        .as_array()
        .expect("edges array")
        .iter()
        .map(|edge| {
            (
                edge["from"].as_str().expect("from").to_string(),
                edge["to"].as_str().expect("to").to_string(),
                edge["properties"].to_string(),
            )
        })
        .collect()
}

fn edge(from: &str, to: &str, score: i64) -> (String, String, String) {
    (
        from.into(),
        to.into(),
        serde_json::json!({ "score": score }).to_string(),
    )
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

async fn traverse(server: &TestServer, predicate: &str) -> serde_json::Value {
    result_json(
        server,
        &format!("GRAPH TRAVERSE IN '{COLLECTION}' FROM 'a' DEPTH 2 {predicate}"),
    )
    .await
}

async fn path(server: &TestServer, dst: &str, predicate: &str) -> serde_json::Value {
    result_json(
        server,
        &format!("GRAPH PATH IN '{COLLECTION}' FROM 'a' TO '{dst}' MAX_DEPTH 4 {predicate}"),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn walks_see_staged_edge_writes_and_rollback_discards_them() {
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .unwrap();
    // Committed: a -> b -> c and a -> d, every edge score 9.
    for (src, dst) in [("a", "b"), ("b", "c"), ("a", "d")] {
        insert_edge(&server, src, dst, 9).await;
    }

    server.exec("BEGIN").await.unwrap();
    // Staged add.
    insert_edge(&server, "a", "e", 7).await;
    // Staged delete.
    server
        .exec(&format!(
            "GRAPH DELETE EDGE IN '{COLLECTION}' FROM 'a' TO 'b' TYPE 'LINK'"
        ))
        .await
        .unwrap();
    // Staged property change.
    insert_edge(&server, "a", "d", 1).await;

    let all = traverse(&server, "").await;
    assert_eq!(node_ids(&all), set(&["a", "d", "e"]), "{all}");
    assert_eq!(
        edges(&all),
        BTreeSet::from([edge("a", "d", 1), edge("a", "e", 7)]),
        "{all}"
    );
    let filtered = traverse(&server, "EDGE WHERE score > 5").await;
    assert_eq!(node_ids(&filtered), set(&["a", "e"]), "{filtered}");
    assert_eq!(
        edges(&filtered),
        BTreeSet::from([edge("a", "e", 7)]),
        "{filtered}"
    );

    assert_eq!(path(&server, "c", "").await, serde_json::json!([]));
    assert_eq!(path(&server, "e", "").await, serde_json::json!(["a", "e"]));
    assert_eq!(
        path(&server, "d", "EDGE WHERE score > 5").await,
        serde_json::json!([]),
        "the staged score 1 fails the predicate"
    );

    server.exec("ROLLBACK").await.unwrap();

    let restored = traverse(&server, "EDGE WHERE score > 5").await;
    assert_eq!(
        node_ids(&restored),
        set(&["a", "b", "c", "d"]),
        "{restored}"
    );
    assert_eq!(
        path(&server, "c", "").await,
        serde_json::json!(["a", "b", "c"])
    );
    assert_eq!(path(&server, "e", "").await, serde_json::json!([]));
    assert_eq!(
        path(&server, "d", "EDGE WHERE score > 5").await,
        serde_json::json!(["a", "d"])
    );
}
