// SPDX-License-Identifier: BUSL-1.1

//! `GRAPH INSERT EDGE` with several edges in one statement.
//!
//! A clause an edge already has starts the next edge. The statement binds its
//! endpoints in one exchange and writes every edge as one unit, so the tests
//! check that each edge lands on both of its homes with its own properties,
//! in autocommit and inside a transaction.

use std::collections::BTreeSet;

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// The row count carried by the first `CommandComplete` in `sql`'s response.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    for m in messages {
        if let SimpleQueryMessage::CommandComplete(n) = m {
            return n;
        }
    }
    panic!("statement reported no command tag: {sql}")
}

/// The node ids `GRAPH NEIGHBORS` returns.
async fn neighbors(
    server: &TestServer,
    coll: &str,
    node: &str,
    direction: &str,
) -> BTreeSet<String> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{coll}' OF '{node}' LABEL 'L' DIRECTION {direction}"
        ))
        .await
        .unwrap_or_else(|e| panic!("neighbors of {node}: {e}"));
    let mut out = BTreeSet::new();
    for row in rows {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&row).unwrap_or_default();
        for entry in parsed {
            if let Some(n) = entry.get("node").and_then(|v| v.as_str()) {
                out.insert(n.to_string());
            }
        }
    }
    out
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

/// `n` edges `src<i> -> dst<i>` whose endpoints spread over many vShards.
fn many_edges(n: usize) -> String {
    (0..n)
        .map(|i| format!("FROM 'src{i}' TO 'dst{i}' TYPE 'L' PROPERTIES {{ w: {i} }}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_edge_of_a_statement_lands_on_both_homes() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ie_many").await.unwrap();

    let count = affected(
        &server,
        &format!("GRAPH INSERT EDGE IN 'ie_many' {}", many_edges(40)),
    )
    .await;
    assert_eq!(count, 40, "the tag counts every edge of the statement");

    for i in [0, 7, 21, 39] {
        assert_eq!(
            neighbors(&server, "ie_many", &format!("src{i}"), "out").await,
            set(&[&format!("dst{i}")]),
            "forward row of edge {i}"
        );
        assert_eq!(
            neighbors(&server, "ie_many", &format!("dst{i}"), "in").await,
            set(&[&format!("src{i}")]),
            "reverse row of edge {i}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_endpoint_binds_once_and_keeps_every_edge() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ie_star").await.unwrap();

    let edges = (0..12)
        .map(|i| format!("FROM 'root' TO 'leaf{i}' TYPE 'L'"))
        .collect::<Vec<_>>()
        .join(" ");
    assert_eq!(
        affected(&server, &format!("GRAPH INSERT EDGE IN 'ie_star' {edges}")).await,
        12
    );

    let leaves: Vec<String> = (0..12).map(|i| format!("leaf{i}")).collect();
    let want: BTreeSet<String> = leaves.into_iter().collect();
    assert_eq!(neighbors(&server, "ie_star", "root", "out").await, want);
    assert_eq!(
        neighbors(&server, "ie_star", "leaf5", "in").await,
        set(&["root"])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_edge_keeps_its_own_properties() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ie_props").await.unwrap();

    server
        .exec(
            "GRAPH INSERT EDGE IN 'ie_props' \
             FROM 'a' TO 'heavy' TYPE 'L' PROPERTIES { score: 9 } \
             FROM 'a' TO 'light' TYPE 'L' PROPERTIES { score: 1 }",
        )
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH TRAVERSE IN 'ie_props' FROM 'a' DEPTH 1 EDGE WHERE score > 5")
        .await
        .unwrap();
    let [text] = rows.as_slice() else {
        panic!("expected one result row, got {rows:?}");
    };
    let got: serde_json::Value = serde_json::from_str(text).unwrap();
    let nodes: BTreeSet<String> = got["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .map(|node| node["id"].as_str().expect("node id").to_string())
        .collect();
    assert_eq!(nodes, set(&["a", "heavy"]), "{got}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_stages_every_edge_and_rollback_drops_them() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ie_txn").await.unwrap();

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(
            &server,
            &format!("GRAPH INSERT EDGE IN 'ie_txn' {}", many_edges(6))
        )
        .await,
        6
    );
    assert_eq!(
        neighbors(&server, "ie_txn", "src3", "out").await,
        set(&["dst3"]),
        "a transaction reads its own staged edges"
    );
    server.exec("ROLLBACK").await.unwrap();
    assert!(
        neighbors(&server, "ie_txn", "src3", "out").await.is_empty(),
        "ROLLBACK drops the staged edges"
    );

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("GRAPH INSERT EDGE IN 'ie_txn' {}", many_edges(6)))
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();
    assert_eq!(
        neighbors(&server, "ie_txn", "src3", "out").await,
        set(&["dst3"]),
        "COMMIT keeps the staged edges"
    );
    assert_eq!(
        neighbors(&server, "ie_txn", "dst5", "in").await,
        set(&["src5"])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_incomplete_later_edge_refuses_the_whole_statement() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ie_bad").await.unwrap();

    let error = server
        .exec("GRAPH INSERT EDGE IN 'ie_bad' FROM 'a' TO 'b' TYPE 'L' FROM 'c' TYPE 'L'")
        .await
        .expect_err("an edge without TO is refused");
    assert!(error.contains("requires TO"), "{error}");
    assert!(
        neighbors(&server, "ie_bad", "a", "out").await.is_empty(),
        "a refused statement writes none of its edges"
    );
}
