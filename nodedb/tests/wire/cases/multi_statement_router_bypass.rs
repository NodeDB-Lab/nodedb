// SPDX-License-Identifier: BUSL-1.1

//! A statement the DDL router runs never plans through the request planner.
//!
//! The router takes a lone `UPSERT .. VALUES` and a lone object-literal
//! `INSERT` over HTTP `/v1/query`. A request of several statements that
//! holds one takes the sequential request runner, which runs it through the
//! router. So the same statement in a request stores the row it stores
//! alone. Each case fills a key from a sequence default.

use super::wire_support::{exec, rows};
use crate::harness::TestServer;

/// A one-core server with `lone` and `paired`, each keyed by its own
/// sequence, and `other`, keyed by text.
async fn fixture(lone: &str, paired: &str) -> TestServer {
    let server = TestServer::start().await;
    for name in [lone, paired] {
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
        "CREATE COLLECTION rb_other (id TEXT PRIMARY KEY, v TEXT)",
    )
    .await;
    server
}

/// Check that `lone` and `paired` hold `expected`, and that `rb_other`
/// holds the row of the request's second statement.
async fn assert_same_rows(server: &TestServer, lone: &str, paired: &str, expected: &[[&str; 2]]) {
    for table in [lone, paired] {
        assert_eq!(
            rows(server, table).await,
            expected,
            "the router path must store the expected row in {table}"
        );
    }
    assert_eq!(rows(server, "rb_other").await, [["o1", "b"]]);
}

/// Check that `sql` applies and answers one summed reply.
async fn assert_applies(server: &TestServer, sql: &str) {
    let (status, body) = server.http_query(sql).await;
    assert_eq!(status, 200, "{sql}: {body}");
    let body: serde_json::Value = serde_json::from_str(&body).expect("a JSON body");
    // The reply is the sum of both statements, with no per-statement array.
    assert!(body.get("statements").is_none(), "{sql}: {body}");
    assert!(body["rows"].is_array(), "{sql}: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upsert_with_a_sequence_key_stores_the_same_row_alone_or_in_a_request() {
    let (lone, paired) = ("rb_upsert_lone", "rb_upsert_paired");
    let server = fixture(lone, paired).await;

    server
        .http_exec(&format!("UPSERT INTO {lone} (v) VALUES ('a')"))
        .await;
    assert_applies(
        &server,
        &format!(
            "UPSERT INTO {paired} (v) VALUES ('a'); \
             UPSERT INTO rb_other (id, v) VALUES ('o1', 'b')"
        ),
    )
    .await;

    assert_same_rows(&server, lone, paired, &[["1", "a"]]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_object_literal_insert_with_a_sequence_key_stores_the_same_row_alone_or_in_a_request() {
    let (lone, paired) = ("rb_literal_lone", "rb_literal_paired");
    let server = fixture(lone, paired).await;

    server
        .http_exec(&format!("INSERT INTO {lone} {{ v: 'a' }}"))
        .await;
    assert_applies(
        &server,
        &format!(
            "INSERT INTO {paired} {{ v: 'a' }}; \
             INSERT INTO rb_other {{ id: 'o1', v: 'b' }}"
        ),
    )
    .await;

    assert_same_rows(&server, lone, paired, &[["1", "a"]]).await;
}
