// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE of an edge-bearing collection ahead of a predicate write in one
//! request. The request runs as an implicit transaction.
//!
//! The transaction carries one edge share per vShard, so the rows and the edges
//! commit together. A read after the TRUNCATE in the same request still sees the
//! edges, so that request is refused whole.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::protocol::opcodes::ResponseStatus;

use crate::harness::TestServer;

/// Data Plane cores the server runs.
const CORES: usize = 2;
/// The edge-bearing collection.
const NODES: &str = "ti_nodes";
/// The collection the predicate write moves.
const OTHER: &str = "ti_other";

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

async fn fixture() -> TestServer {
    let server = TestServer::start_multicores(CORES).await;
    exec(&server, &format!("CREATE COLLECTION {NODES}")).await;
    for id in ["a", "b"] {
        exec(&server, &format!("UPSERT INTO {NODES} {{ id: '{id}' }}")).await;
    }
    exec(
        &server,
        &format!("GRAPH INSERT EDGE IN '{NODES}' FROM 'a' TO 'b' TYPE 'l'"),
    )
    .await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {OTHER} (id TEXT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &server,
        &format!("INSERT INTO {OTHER} (id, v) VALUES ('r1', 'old')"),
    )
    .await;
    assert_eq!(neighbors(&server).await, ["b"], "the seeded edge");
    server
}

/// The nodes `GRAPH NEIGHBORS` returns for `a`.
async fn neighbors(server: &TestServer) -> Vec<String> {
    let messages = server
        .client
        .simple_query(&format!(
            "GRAPH NEIGHBORS IN '{NODES}' OF 'a' DIRECTION out"
        ))
        .await
        .expect("GRAPH NEIGHBORS");
    let mut out = Vec::new();
    for message in messages {
        let tokio_postgres::SimpleQueryMessage::Row(row) = message else {
            continue;
        };
        let Some(text) = row.get(0) else {
            continue;
        };
        let parsed: Vec<serde_json::Value> = serde_json::from_str(text).unwrap_or_default();
        out.extend(
            parsed
                .iter()
                .filter_map(|entry| entry.get("node").and_then(|v| v.as_str()))
                .map(str::to_owned),
        );
    }
    out
}

async fn assert_all_committed(server: &TestServer) {
    let rows = server
        .query_text(&format!("SELECT id FROM {NODES}"))
        .await
        .expect("read the nodes");
    assert!(rows.is_empty(), "the rows are gone: {rows:?}");
    assert!(
        neighbors(server).await.is_empty(),
        "the TRUNCATE cuts the edges"
    );
    assert_eq!(
        server
            .query_text(&format!("SELECT v FROM {OTHER}"))
            .await
            .expect("read the other collection"),
        ["new"],
        "the predicate write commits with it"
    );
}

fn request() -> String {
    format!("TRUNCATE {NODES}; UPDATE {OTHER} SET v = 'new' WHERE v = 'old'")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_truncate_ahead_of_a_predicate_write_cuts_the_edges() {
    let server = fixture().await;

    server.http_exec(&request()).await;

    assert_all_committed(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_truncate_ahead_of_a_predicate_write_cuts_the_edges() {
    let server = fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;

    let response = send_sql(&mut stream, 2, &request()).await;

    assert_ne!(
        response.status,
        ResponseStatus::Error,
        "the request: {:?}",
        response.error.map(|error| error.message)
    );
    assert_all_committed(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_after_the_truncate_refuses_the_request_whole() {
    let server = fixture().await;

    let (status, body) = server
        .http_query(&format!("{}; SELECT v FROM {OTHER}", request()))
        .await;

    assert_eq!(status, 501, "feature not supported: {body}");
    assert!(body.contains(NODES), "the refusal names it: {body}");
    assert_eq!(neighbors(&server).await, ["b"], "the edge stays");
    assert_eq!(
        server
            .query_text(&format!("SELECT v FROM {OTHER}"))
            .await
            .expect("read the other collection"),
        ["old"],
        "nothing applied"
    );
}

/// A client block can read after the statement, so it refuses the TRUNCATE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_block_refuses_the_truncate() {
    let server = fixture().await;

    let begin = server.client.simple_query("BEGIN").await;
    begin.expect("BEGIN");
    let error = server
        .client
        .simple_query(&format!("TRUNCATE {NODES}"))
        .await
        .expect_err("the block refuses it");
    assert_eq!(error.code().map(|code| code.code()), Some("0A000"));
    server
        .client
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK");

    assert_eq!(neighbors(&server).await, ["b"], "the edge stays");
}
