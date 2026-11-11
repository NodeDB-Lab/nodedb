// SPDX-License-Identifier: BUSL-1.1

//! A statement the DDL router runs behaves the same alone or in a request.
//!
//! Alone, the router runs it with its own steps: sequence defaults, field
//! inference, vector writes, engine functions. A request of several
//! statements that holds one takes the sequential request runner, which
//! runs it through the router too. So such a statement, as statement 1 or
//! 2 of a request, stores what it stores alone, and the plain statement
//! beside it applies.
//!
//! A plain INSERT and SELECT pair is not claimed and applies as one request.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};

use crate::harness::TestServer;

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// The rows of `collection`, ordered by key.
async fn rows(server: &TestServer, collection: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!("SELECT id, v FROM {collection} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
}

/// A one-core server with `rc_lone`, `rc_first` and `rc_second`, each keyed
/// by its own sequence, and `rc_plain`, keyed by text.
async fn fixture() -> TestServer {
    let server = TestServer::start().await;
    for name in ["rc_lone", "rc_first", "rc_second"] {
        exec(&server, &format!("CREATE SEQUENCE {name}_seq")).await;
        exec(
            &server,
            &format!(
                "CREATE COLLECTION {name} (\
                    id BIGINT DEFAULT nextval('{name}_seq') PRIMARY KEY, \
                    v TEXT)"
            ),
        )
        .await;
    }
    exec(
        &server,
        "CREATE COLLECTION rc_plain (id TEXT PRIMARY KEY, v TEXT)",
    )
    .await;
    server
}

/// The plain statements each request pairs with the claimed one.
const PLAIN: &str = "INSERT INTO rc_plain (id, v) VALUES ('p1', 'b')";
const PLAIN_SECOND: &str = "INSERT INTO rc_plain (id, v) VALUES ('p2', 'b')";

/// Check that `sql` applies over HTTP and answers one summed reply.
async fn assert_http_applies(server: &TestServer, sql: &str) {
    let (status, body) = server.http_query(sql).await;
    assert_eq!(status, 200, "{sql}: {body}");
    let body: serde_json::Value = serde_json::from_str(&body).expect("a JSON body");
    // The reply is the sum of both statements, with no per-statement array.
    assert!(body.get("statements").is_none(), "{sql}: {body}");
    assert!(body["rows"].is_array(), "{sql}: {body}");
}

/// Check that both plain statements applied.
async fn assert_plain_applied(server: &TestServer) {
    assert_eq!(rows(server, "rc_plain").await, [["p1", "b"], ["p2", "b"]]);
}

/// Run `claimed` alone into `rc_lone`, then as statement 1 and 2 of a
/// request into `rc_first` and `rc_second`. `claimed` names its target as
/// `{t}`. All three store the same row.
async fn assert_write_alone_or_in_request(claimed: &str) {
    let server = fixture().await;
    let on = |table: &str| claimed.replace("{t}", table);

    server.http_exec(&on("rc_lone")).await;
    assert_http_applies(&server, &format!("{}; {PLAIN}", on("rc_first"))).await;
    assert_http_applies(&server, &format!("{PLAIN_SECOND}; {}", on("rc_second"))).await;

    let lone = rows(&server, "rc_lone").await;
    assert_eq!(
        lone,
        [["1", "a"]],
        "the router fills the key from the sequence"
    );
    for table in ["rc_first", "rc_second"] {
        assert_eq!(rows(&server, table).await, lone, "{table}");
    }
    assert_plain_applied(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upsert_with_a_sequence_default_runs_alone_or_in_a_request() {
    assert_write_alone_or_in_request("UPSERT INTO {t} (v) VALUES ('a')").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_object_literal_insert_runs_alone_or_in_a_request() {
    assert_write_alone_or_in_request("INSERT INTO {t} { v: 'a' }").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kv_incr_runs_alone_or_in_a_request() {
    let server = fixture().await;
    exec(
        &server,
        "CREATE COLLECTION rc_kv (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
    )
    .await;
    exec(&server, "INSERT INTO rc_kv (key, n) VALUES ('ctr', 5)").await;
    let incr = "SELECT KV_INCR('rc_kv', 'ctr', 3)";

    let (status, after_incr) = server.http_query(incr).await;
    assert_eq!(status, 200, "{incr}: {after_incr}");
    assert_http_applies(&server, &format!("{incr}; {PLAIN}")).await;
    assert_http_applies(&server, &format!("{PLAIN_SECOND}; {incr}")).await;

    // A zero step reads the counter. Each request moved it.
    let (status, read) = server.http_query("SELECT KV_INCR('rc_kv', 'ctr', 0)").await;
    assert_eq!(status, 200, "{read}");
    assert_ne!(read, after_incr, "each request moves the counter");
    assert_plain_applied(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_topk_runs_alone_or_in_a_request() {
    let server = fixture().await;
    exec(
        &server,
        "CREATE COLLECTION rc_scores (id STRING PRIMARY KEY, score INT) WITH (engine='kv')",
    )
    .await;
    exec(&server, "INSERT INTO rc_scores { id: 'a', score: 10 }").await;
    exec(
        &server,
        "CREATE SORTED INDEX rc_idx ON rc_scores (score DESC) KEY id",
    )
    .await;
    let topk = "SELECT * FROM TOPK(rc_idx, 3)";

    let (status, body) = server.http_query(topk).await;
    assert_eq!(status, 200, "{topk}: {body}");
    assert_http_applies(&server, &format!("{topk}; {PLAIN}")).await;
    assert_http_applies(&server, &format!("{PLAIN_SECOND}; {topk}")).await;
    assert_plain_applied(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_runs_a_claimed_statement_in_a_request() {
    let server = fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let alone = send_sql(&mut stream, 2, "UPSERT INTO rc_lone (v) VALUES ('a')").await;
    assert!(alone.error.is_none(), "the lone UPSERT applies: {alone:?}");

    let sql = format!("{PLAIN}; UPSERT INTO rc_second (v) VALUES ('a')");
    let paired = send_sql(&mut stream, 3, &sql).await;
    assert!(paired.error.is_none(), "the request applies: {paired:?}");
    // One frame sums the INSERT and the routed UPSERT counts.
    assert_eq!(paired.rows_affected, Some(2), "{paired:?}");
    assert_eq!(rows(&server, "rc_lone").await, [["1", "a"]]);
    assert_eq!(rows(&server, "rc_second").await, [["1", "a"]]);
    assert_eq!(rows(&server, "rc_plain").await, [["p1", "b"]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_insert_and_select_pair_applies_as_one_request() {
    let server = fixture().await;
    let sql = format!("{PLAIN}; SELECT id, v FROM rc_plain");
    let (status, body) = server.http_query(&sql).await;
    assert_eq!(status, 200, "{sql}: {body}");
    assert_eq!(rows(&server, "rc_plain").await, [["p1", "b"]]);
}
