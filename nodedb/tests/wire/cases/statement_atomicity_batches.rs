// SPDX-License-Identifier: BUSL-1.1

//! Atomic autocommit statements whose writes span several tasks or
//! vShards: a multi-row insert of edge documents (the rows on the
//! collection's vShard, each mirrored edge on its endpoints' vShards), the
//! same rows as a `{}` batch, a `GRAPH INSERT EDGE` batch, and a `COPY`. A
//! failing row fails the statement with its SQLSTATE and writes nothing:
//! no row and no edge.

use crate::harness::TestServer;

use super::returning_vote_single_shard::{create, rows_of};
use super::statement_atomicity::{Native, fails_everywhere, fails_on_simple_and_native};

/// The destination nodes of `node`'s outgoing `l` edges in `collection`.
async fn neighbors_of(server: &TestServer, collection: &str, node: &str) -> Vec<String> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{node}' LABEL 'l' DIRECTION out"
        ))
        .await
        .unwrap_or_else(|e| panic!("neighbors of {node}: {e}"));
    let mut out = Vec::new();
    for row in rows {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&row).unwrap_or_default();
        for entry in parsed {
            if let Some(n) = entry.get("node").and_then(|v| v.as_str()) {
                out.push(n.to_string());
            }
        }
    }
    out
}

/// An edge-bearing collection holding `e0` from `p` to `q`.
async fn edge_collection(server: &TestServer, collection: &str) {
    create(server, &format!("CREATE COLLECTION {collection}")).await;
    create(
        server,
        &format!("INSERT INTO {collection} {{ id: 'e0', _from: 'p', _to: 'q', _type: 'l' }}"),
    )
    .await;
    assert_eq!(neighbors_of(server, collection, "p").await, ["q"]);
}

/// Assert `collection` still holds only `e0` and none of the refused rows'
/// edges.
async fn holds_only_the_seed(server: &TestServer, collection: &str) {
    assert_eq!(
        rows_of(server, &format!("SELECT id FROM {collection}")).await,
        ["e0"]
    );
    for node in ["a", "c"] {
        assert!(
            neighbors_of(server, collection, node).await.is_empty(),
            "{collection}: no edge from {node}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_shard_multi_row_insert_writes_nothing() {
    let server = TestServer::start().await;
    edge_collection(&server, "sab_rows").await;
    let mut native = Native::open(&server).await;

    fails_everywhere(
        &server,
        &mut native,
        "INSERT INTO sab_rows (id, _from, _to, _type) VALUES \
         ('e1', 'a', 'b', 'l'), ('e2', 'c', 'd', 'l'), ('e0', 'x', 'y', 'l')",
        "23505",
    )
    .await;
    holds_only_the_seed(&server, "sab_rows").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_brace_batch_writes_nothing() {
    let server = TestServer::start().await;
    edge_collection(&server, "sab_brace").await;
    let mut native = Native::open(&server).await;

    fails_everywhere(
        &server,
        &mut native,
        "INSERT INTO sab_brace [\
         { id: 'e1', _from: 'a', _to: 'b', _type: 'l' }, \
         { id: 'e2', _from: 'c', _to: 'd', _type: 'l' }, \
         { id: 'e0', _from: 'x', _to: 'y', _type: 'l' }]",
        "23505",
    )
    .await;
    holds_only_the_seed(&server, "sab_brace").await;
}

/// The third edge's empty `TYPE` refuses the batch before any edge lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_batch_writes_nothing() {
    let server = TestServer::start().await;
    create(&server, "CREATE COLLECTION sab_edges").await;
    let mut native = Native::open(&server).await;

    fails_on_simple_and_native(
        &server,
        &mut native,
        "GRAPH INSERT EDGE IN 'sab_edges' FROM 'a' TO 'b' TYPE 'l', \
         FROM 'c' TO 'd' TYPE 'l', FROM 'e' TO 'f' TYPE ''",
        "42601",
    )
    .await;
    for node in ["a", "c", "e"] {
        assert!(
            neighbors_of(&server, "sab_edges", node).await.is_empty(),
            "no edge from {node}"
        );
    }
}

/// A `COPY` whose third row repeats a stored key fails with `23505` and
/// stores none of its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_with_a_refused_row_writes_nothing() {
    use std::io::Write;

    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION sab_copy (id TEXT PRIMARY KEY, n INT)",
    )
    .await;
    create(&server, "INSERT INTO sab_copy (id, n) VALUES ('c', 0)").await;

    let mut file = tempfile::Builder::new()
        .suffix(".ndjson")
        .tempfile()
        .expect("create temp file");
    file.write_all(
        b"{\"id\": \"a\", \"n\": 1}\n{\"id\": \"b\", \"n\": 2}\n{\"id\": \"c\", \"n\": 3}\n",
    )
    .expect("write temp file");
    file.flush().expect("flush temp file");
    let path = file.path().to_string_lossy().to_string();

    let mut native = Native::open(&server).await;
    fails_on_simple_and_native(
        &server,
        &mut native,
        &format!("COPY sab_copy FROM '{path}'"),
        "23505",
    )
    .await;
    assert_eq!(
        rows_of(&server, "SELECT id, n FROM sab_copy").await,
        ["c\t0"]
    );
}
