// SPDX-License-Identifier: BUSL-1.1

//! A statement the DDL router runs never plans through the request planner.
//!
//! The router takes a lone `UPSERT .. VALUES` and a lone object-literal
//! `INSERT` over HTTP `/v1/query`. A request of several statements skips the
//! router. So the same statement in a request answers 0A000 (HTTP 501) and
//! applies nothing. Each case fills a key from a sequence default.

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

/// Check that `lone` holds `expected` and that `paired` and `rb_other`
/// hold nothing.
async fn assert_lone_rows(server: &TestServer, lone: &str, paired: &str, expected: &[[&str; 2]]) {
    assert_eq!(
        rows(server, lone).await,
        expected,
        "the router path must store the expected row in {lone}"
    );
    for refused in [paired, "rb_other"] {
        assert!(
            rows(server, refused).await.is_empty(),
            "a refused request must apply nothing to {refused}"
        );
    }
}

/// Check that `sql` answers the 0A000 refusal of its first statement.
async fn assert_refused(server: &TestServer, sql: &str) {
    let (status, body) = server.http_query(sql).await;
    assert_eq!(status, 501, "{sql}: {body}");
    assert!(
        body.contains("statement 1 of 2") && body.contains("as its own request"),
        "{sql}: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upsert_with_a_sequence_key_stores_alone_and_is_refused_in_a_request() {
    let (lone, paired) = ("rb_upsert_lone", "rb_upsert_paired");
    let server = fixture(lone, paired).await;

    server
        .http_exec(&format!("UPSERT INTO {lone} (v) VALUES ('a')"))
        .await;
    assert_refused(
        &server,
        &format!(
            "UPSERT INTO {paired} (v) VALUES ('a'); \
             UPSERT INTO rb_other (id, v) VALUES ('o1', 'b')"
        ),
    )
    .await;

    assert_lone_rows(&server, lone, paired, &[["1", "a"]]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_object_literal_insert_with_a_sequence_key_stores_alone_and_is_refused_in_a_request() {
    let (lone, paired) = ("rb_literal_lone", "rb_literal_paired");
    let server = fixture(lone, paired).await;

    server
        .http_exec(&format!("INSERT INTO {lone} {{ v: 'a' }}"))
        .await;
    assert_refused(
        &server,
        &format!(
            "INSERT INTO {paired} {{ v: 'a' }}; \
             INSERT INTO rb_other {{ id: 'o1', v: 'b' }}"
        ),
    )
    .await;

    assert_lone_rows(&server, lone, paired, &[["1", "a"]]).await;
}
