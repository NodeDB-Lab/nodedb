// SPDX-License-Identifier: BUSL-1.1

//! One request whose statements report different command verbs.
//!
//! HTTP and native SQL plan a request as one task set, and its tag folds over
//! every task. Different verbs answer `MIXED` with the summed count. The
//! request's shape never fails it.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};

use crate::harness::TestServer;

/// Data Plane cores the server runs.
const CORES: usize = 2;
const ONE: &str = "mv_one";
const TWO: &str = "mv_two";

/// Two strict collections, each with two rows.
async fn fixture() -> TestServer {
    let server = TestServer::start_multicores(CORES).await;
    for collection in [ONE, TWO] {
        server
            .exec(&format!(
                "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, v TEXT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .expect("create the collection");
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, v) VALUES ('r1', 'a'), ('r2', 'b')"
            ))
            .await
            .expect("seed the collection");
    }
    server
}

/// A predicate `UPDATE` then a predicate `DELETE`, one row each.
fn predicate_request() -> String {
    format!("UPDATE {ONE} SET v = 'x' WHERE v = 'a'; DELETE FROM {TWO} WHERE v = 'b'")
}

/// A point `UPDATE` then a point `DELETE` of one collection.
fn point_request() -> String {
    format!("UPDATE {ONE} SET v = 'x' WHERE id = 'r1'; DELETE FROM {ONE} WHERE id = 'r2'")
}

async fn values(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT v FROM {collection} ORDER BY id"))
        .await
        .expect("read the collection")
}

async fn assert_predicate_request_applied(server: &TestServer) {
    assert_eq!(values(server, ONE).await, ["x", "b"]);
    assert_eq!(values(server, TWO).await, ["a"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_predicate_update_and_delete_answer_their_summed_count() {
    let server = fixture().await;

    let (status, body) = server.http_query(&predicate_request()).await;

    assert_eq!(status, 200, "the request answered: {body}");
    let answer: serde_json::Value = serde_json::from_str(&body).expect("a JSON answer");
    assert_eq!(answer["rows"], serde_json::json!([{ "affected": 2 }]));
    assert_predicate_request_applied(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_predicate_update_and_delete_answer_mixed() {
    let server = fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;

    let response = send_sql(&mut stream, 2, &predicate_request()).await;

    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(response.command.as_deref(), Some("MIXED"));
    assert_eq!(response.rows_affected, Some(2));
    assert_predicate_request_applied(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_point_update_and_delete_both_apply() {
    let server = fixture().await;

    server.http_exec(&point_request()).await;

    assert_eq!(values(&server, ONE).await, ["x"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_point_update_and_delete_answer_mixed() {
    let server = fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;

    let response = send_sql(&mut stream, 2, &point_request()).await;

    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(response.command.as_deref(), Some("MIXED"));
    assert_eq!(response.rows_affected, Some(2));
    assert_eq!(values(&server, ONE).await, ["x"]);
}
