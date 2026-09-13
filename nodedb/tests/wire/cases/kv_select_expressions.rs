// SPDX-License-Identifier: BUSL-1.1

//! kv-engine SELECT projection expression evaluation.
//!
//! Previously the kv scan carried neither the SELECT projection list nor
//! computed columns, so expression projections were never evaluated: the
//! column came back NULL at response shaping (`SELECT 1 + 1 FROM kv`
//! returned an empty column) and sequence accessors could not raise their
//! typed 0A000 either. The scan now carries projection + computed columns
//! like the document/columnar paths, so:
//!
//! - scalar expressions over kv rows evaluate per row;
//! - `nextval`/`currval`/`setval` in a kv SELECT list raise 0A000 instead
//!   of silently NULLing.

use crate::harness::TestServer;

async fn setup(server: &TestServer) {
    server
        .exec("CREATE COLLECTION kvsel (id BIGINT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kvsel (id, v) VALUES (1, 'hello'), (2, 'world')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scalar_expressions_evaluate_per_row() {
    let server = TestServer::start().await;
    setup(&server).await;

    let rows = server
        .query_named_rows("SELECT upper(v) AS u, 1 + 1 AS s FROM kvsel ORDER BY id")
        .await
        .expect("rows");
    assert_eq!(rows.len(), 2, "{rows:?}");
    let u: Vec<_> = rows
        .iter()
        .map(|r| r.get("u").map(|s| s.as_str()))
        .collect();
    assert_eq!(u, vec![Some("HELLO"), Some("WORLD")], "{rows:?}");
    let s: Vec<_> = rows
        .iter()
        .map(|r| r.get("s").map(|s| s.as_str()))
        .collect();
    assert_eq!(s, vec![Some("2"), Some("2")], "{rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accessor_in_select_list_raises_0a000() {
    let server = TestServer::start().await;
    setup(&server).await;
    server
        .expect_error("SELECT nextval('nope') FROM kvsel", "0A000")
        .await;
    server
        .expect_error("SELECT currval('nope') FROM kvsel", "0A000")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_projection_still_returns_stored_columns() {
    let server = TestServer::start().await;
    setup(&server).await;
    let rows = server
        .query_named_rows("SELECT id, v FROM kvsel ORDER BY id")
        .await
        .expect("rows");
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        rows.iter()
            .map(|r| r.get("v").map(|s| s.as_str()))
            .collect::<Vec<_>>(),
        vec![Some("hello"), Some("world")]
    );
}
