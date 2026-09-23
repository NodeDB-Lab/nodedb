// SPDX-License-Identifier: BUSL-1.1

//! A KV counter on an absent key creates a row of the collection's shape.
//!
//! In a typed collection, `KV_INCR` / `KV_INCR_FLOAT` on a key that does not
//! exist stores the row `INSERT (key, column) VALUES (key, delta)` stores,
//! DEFAULTs included, so a `SELECT` of the column reads the value. RESP treats
//! every value as a byte string, as `SET` and `GET` do, so a RESP `INCR`
//! stores decimal text in any collection.

use crate::harness::TestServer;
use crate::harness::resp_client::Reply;

async fn create_typed(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION kvfresh (key TEXT PRIMARY KEY, n INT, \
             status TEXT DEFAULT 'new') WITH (engine='kv')",
        )
        .await
        .unwrap();
}

async fn row_of(server: &TestServer, key: &str) -> std::collections::HashMap<String, String> {
    let rows = server
        .query_named_rows(&format!(
            "SELECT n, status FROM kvfresh WHERE key = '{key}'"
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one row for {key}: {rows:?}");
    rows.into_iter().next().unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_incr_on_an_absent_key_creates_a_typed_row() {
    let server = TestServer::start().await;
    create_typed(&server).await;

    server
        .query_text("SELECT KV_INCR('kvfresh', 'a', 5)")
        .await
        .unwrap();
    let row = row_of(&server, "a").await;
    assert_eq!(row.get("n").map(String::as_str), Some("5"), "{row:?}");
    assert_eq!(
        row.get("status").map(String::as_str),
        Some("new"),
        "the fresh row carries the DEFAULT an insert stores: {row:?}"
    );

    server
        .query_text("SELECT KV_INCR('kvfresh', 'a', 2)")
        .await
        .unwrap();
    assert_eq!(
        row_of(&server, "a").await.get("n").map(String::as_str),
        Some("7")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_incr_float_on_an_absent_key_creates_a_typed_row() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION kvscore (key TEXT PRIMARY KEY, score FLOAT, \
             status TEXT DEFAULT 'new') WITH (engine='kv')",
        )
        .await
        .unwrap();

    server
        .query_text("SELECT KV_INCR_FLOAT('kvscore', 'b', 2.5)")
        .await
        .unwrap();
    let rows = server
        .query_named_rows("SELECT score, status FROM kvscore WHERE key = 'b'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].get("score").map(String::as_str), Some("2.5"));
    assert_eq!(rows[0].get("status").map(String::as_str), Some("new"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_typed_fresh_row_survives_restart() {
    let server = TestServer::start().await;
    create_typed(&server).await;
    server
        .query_text("SELECT KV_INCR('kvfresh', 'c', 9)")
        .await
        .unwrap();

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let row = row_of(&server, "c").await;
    assert_eq!(row.get("n").map(String::as_str), Some("9"), "{row:?}");
    assert_eq!(row.get("status").map(String::as_str), Some("new"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_incr_on_an_absent_key_stores_decimal_text_in_a_typed_collection() {
    let server = TestServer::start().await;
    create_typed(&server).await;
    let mut resp = server.resp_session("kvfresh_resp_user", "kvfresh").await;

    assert_eq!(resp.cmd(&["INCR", "r"]).await, Reply::Integer(1));
    assert_eq!(
        resp.cmd(&["GET", "r"]).await,
        Reply::Bulk(Some("1".into())),
        "RESP GET returns the byte string RESP INCR stored"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_incrbyfloat_adds_a_twenty_digit_delta_exactly() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION kvexact (key STRING PRIMARY KEY, value STRING) \
             WITH (engine='kv')",
        )
        .await
        .unwrap();
    let mut resp = server.resp_session("kvexact_user", "kvexact").await;

    assert_eq!(
        resp.cmd(&["SET", "k", "1"]).await,
        Reply::Simple("OK".into())
    );
    assert_eq!(
        resp.cmd(&["INCRBYFLOAT", "k", "0.12345678901234567891"])
            .await,
        Reply::Bulk(Some("1.12345678901234567891".into()))
    );
    assert_eq!(
        resp.cmd(&["GET", "k"]).await,
        Reply::Bulk(Some("1.12345678901234567891".into()))
    );
}
