// SPDX-License-Identifier: BUSL-1.1

//! A statement the DDL router runs behaves the same alone or in a request.
//!
//! Alone, the router runs it with its own steps: sequence defaults, field
//! inference, vector writes, engine functions. A request of several
//! statements plans each statement without the router. So such a statement,
//! as statement 1 or 2 of a request, answers 0A000 (HTTP 501). The error
//! names the statement and tells the client to send it as its own request.
//! The request applies nothing.
//!
//! A plain INSERT and SELECT pair is not claimed and applies as one request.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};

use crate::harness::TestServer;

/// The text every refusal holds.
const NEXT_ACTION: &str = "Send this statement as its own request";

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

/// The plain statement each refused request pairs with the claimed one.
const PLAIN: &str = "INSERT INTO rc_plain (id, v) VALUES ('p1', 'b')";

/// Check that `sql` answers the 0A000 refusal of statement `index` of 2
/// over HTTP.
async fn assert_http_refused(server: &TestServer, sql: &str, index: usize) {
    let (status, body) = server.http_query(sql).await;
    assert_eq!(status, 501, "{sql}: {body}");
    let statement = format!("statement {index} of 2");
    assert!(
        body.contains(&statement) && body.contains(NEXT_ACTION),
        "the refusal names {statement} and the next action: {body}"
    );
}

/// Run `claimed` alone into `rc_lone`, then as statement 1 and 2 of a
/// request into `rc_first` and `rc_second`. `claimed` names its target as
/// `{t}`.
async fn assert_write_alone_or_refused(claimed: &str) {
    let server = fixture().await;
    let on = |table: &str| claimed.replace("{t}", table);

    server.http_exec(&on("rc_lone")).await;
    assert_http_refused(&server, &format!("{}; {PLAIN}", on("rc_first")), 1).await;
    assert_http_refused(&server, &format!("{PLAIN}; {}", on("rc_second")), 2).await;

    assert_eq!(
        rows(&server, "rc_lone").await,
        [["1", "a"]],
        "the router fills the key from the sequence"
    );
    for table in ["rc_first", "rc_second", "rc_plain"] {
        assert!(
            rows(&server, table).await.is_empty(),
            "a refused request applies nothing to {table}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upsert_with_a_sequence_default_runs_alone_or_is_refused() {
    assert_write_alone_or_refused("UPSERT INTO {t} (v) VALUES ('a')").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_object_literal_insert_runs_alone_or_is_refused() {
    assert_write_alone_or_refused("INSERT INTO {t} { v: 'a' }").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kv_incr_runs_alone_or_is_refused() {
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
    assert_http_refused(&server, &format!("{incr}; {PLAIN}"), 1).await;
    assert_http_refused(&server, &format!("{PLAIN}; {incr}"), 2).await;

    // A zero step reads the counter. It answers what the lone step answered,
    // so neither refused request moved it.
    let (status, read) = server.http_query("SELECT KV_INCR('rc_kv', 'ctr', 0)").await;
    assert_eq!(status, 200, "{read}");
    assert_eq!(
        read, after_incr,
        "a refused request must not move the counter"
    );
    assert!(rows(&server, "rc_plain").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_topk_runs_alone_or_is_refused() {
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
    assert_http_refused(&server, &format!("{topk}; {PLAIN}"), 1).await;
    assert_http_refused(&server, &format!("{PLAIN}; {topk}"), 2).await;
    assert!(rows(&server, "rc_plain").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_refuses_a_claimed_statement_in_a_request() {
    let server = fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let alone = send_sql(&mut stream, 2, "UPSERT INTO rc_lone (v) VALUES ('a')").await;
    assert!(alone.error.is_none(), "the lone UPSERT applies: {alone:?}");

    let sql = format!("{PLAIN}; UPSERT INTO rc_second (v) VALUES ('a')");
    let paired = send_sql(&mut stream, 3, &sql).await;
    let error = paired.error.expect("the request must be refused");
    assert_eq!(error.code, "0A000", "{error:?}");
    assert!(
        error.message.contains("statement 2 of 2") && error.message.contains(NEXT_ACTION),
        "{error:?}"
    );
    assert_eq!(rows(&server, "rc_lone").await, [["1", "a"]]);
    assert!(rows(&server, "rc_second").await.is_empty());
    assert!(rows(&server, "rc_plain").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_insert_and_select_pair_applies_as_one_request() {
    let server = fixture().await;
    let sql = format!("{PLAIN}; SELECT id, v FROM rc_plain");
    let (status, body) = server.http_query(&sql).await;
    assert_eq!(status, 200, "{sql}: {body}");
    assert_eq!(rows(&server, "rc_plain").await, [["p1", "b"]]);
}
