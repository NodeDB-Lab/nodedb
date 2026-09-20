// SPDX-License-Identifier: BUSL-1.1

//! `ON CONFLICT DO UPDATE` / `UPSERT` against a KV row stored as raw bytes.
//!
//! A KV row written through the single-`value` column form, or through RESP
//! `SET`, stores its scalar as raw bytes rather than a msgpack map. Every
//! read-modify-write must decode that body as the `{"value": ...}` row reads
//! present and write the result back as raw bytes: a multi-byte value must
//! not fail with a msgpack decode error, and a one-byte value (a valid
//! msgpack fixint) must not be discarded and re-encoded as a map. RESP `GET`
//! returns the stored bytes verbatim, so it is the check that the shape held.

use crate::harness::TestServer;
use crate::harness::resp_client::{Reply, RespClient};

const COLLECTION: &str = "kvb";

async fn create_bare_value_collection(server: &TestServer) {
    server
        .exec(&format!(
            "CREATE COLLECTION {COLLECTION} (key STRING PRIMARY KEY, value STRING) \
             WITH (engine='kv')"
        ))
        .await
        .unwrap();
}

async fn value_of(server: &TestServer, key: &str) -> String {
    let rows = server
        .query_text(&format!(
            "SELECT value FROM {COLLECTION} WHERE key = '{key}'"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one row for {key}, got {rows:?}"
    );
    rows[0].clone()
}

async fn resp_get(client: &mut RespClient, key: &str) -> Reply {
    client.cmd(&["GET", key]).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_bare_value_on_conflict_do_update_overwrites() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;

    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('k', 'first')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('k', 'second-longer-value') \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        ))
        .await
        .unwrap();

    assert_eq!(value_of(&server, "k").await, "second-longer-value");

    let mut resp = server.resp_session("kvb_overwrite_user", COLLECTION).await;
    assert_eq!(
        resp_get(&mut resp, "k").await,
        Reply::Bulk(Some("second-longer-value".to_string())),
        "the merged body must stay raw bytes, not become a msgpack map"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_bare_value_on_conflict_do_update_single_byte_value_keeps_shape() {
    // '1' is 0x31, a valid msgpack fixint. Decoding it as msgpack would
    // read the integer 49, discard it, and write `{value: "2"}` as a map.
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;

    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('k', '1')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('k', '2') \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        ))
        .await
        .unwrap();

    assert_eq!(value_of(&server, "k").await, "2");

    let mut resp = server.resp_session("kvb_singlebyte_user", COLLECTION).await;
    assert_eq!(
        resp_get(&mut resp, "k").await,
        Reply::Bulk(Some("2".to_string())),
        "a one-byte raw value must be written back as one raw byte"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_upsert_keyword_bare_value_form_overwrites() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;

    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('k', 'first')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "UPSERT INTO {COLLECTION} (key, value) VALUES ('k', 'second')"
        ))
        .await
        .unwrap();

    assert_eq!(value_of(&server, "k").await, "second");

    let mut resp = server.resp_session("kvb_upsert_user", COLLECTION).await;
    assert_eq!(
        resp_get(&mut resp, "k").await,
        Reply::Bulk(Some("second".to_string()))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_resp_set_then_sql_on_conflict_do_update_merges_raw_body() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;

    let mut resp = server.resp_session("kvb_merge_user", COLLECTION).await;
    assert_eq!(
        resp.cmd(&["SET", "k", "set-via-resp"]).await,
        Reply::Simple("OK".to_string())
    );

    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('k', 'merged-via-sql') \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        ))
        .await
        .unwrap();

    assert_eq!(value_of(&server, "k").await, "merged-via-sql");
    assert_eq!(
        resp_get(&mut resp, "k").await,
        Reply::Bulk(Some("merged-via-sql".to_string())),
        "a RESP-written raw body merged by SQL must read back raw over RESP"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_bare_value_on_conflict_do_update_refuses_a_typed_column() {
    // `n` is declared, but a row inserted through the bare `(key, value)`
    // form is stored raw and holds only `value`. Assigning `n` would have to
    // change its shape; that is refused, never silently done.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kvn (key STRING PRIMARY KEY, value STRING, n INT) WITH (engine='kv')")
        .await
        .unwrap();

    server
        .exec("INSERT INTO kvn (key, value) VALUES ('k', 'first')")
        .await
        .unwrap();
    server
        .expect_error(
            "INSERT INTO kvn (key, value) VALUES ('k', 'second') \
             ON CONFLICT (key) DO UPDATE SET n = 1",
            "cannot set n",
        )
        .await;

    let rows = server
        .query_text("SELECT value FROM kvn WHERE key = 'k'")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["first".to_string()],
        "a refused merge must leave the stored row untouched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_bare_value_on_conflict_do_update_inside_transaction_commits() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;

    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('k', 'first')"
        ))
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('k', 'second-in-txn') \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        ))
        .await
        .unwrap();
    assert_eq!(
        value_of(&server, "k").await,
        "second-in-txn",
        "the staged merge must be visible to the transaction's own read"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(value_of(&server, "k").await, "second-in-txn");

    let mut resp = server.resp_session("kvb_txn_user", COLLECTION).await;
    assert_eq!(
        resp_get(&mut resp, "k").await,
        Reply::Bulk(Some("second-in-txn".to_string())),
        "the committed body must stay raw bytes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_bare_value_on_conflict_do_update_survives_restart() {
    // `INSERT ... ON CONFLICT DO UPDATE` is WAL-logged as a delta (the
    // incoming row plus the assignments), so replay re-runs the merge against
    // the replayed raw body. A one-byte value pins the fixint case too.
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;

    for (key, first, second) in [("k", "first", "second-longer-value"), ("one", "1", "2")] {
        server
            .exec(&format!(
                "INSERT INTO {COLLECTION} (key, value) VALUES ('{key}', '{first}')"
            ))
            .await
            .unwrap();
        server
            .exec(&format!(
                "INSERT INTO {COLLECTION} (key, value) VALUES ('{key}', '{second}') \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
            ))
            .await
            .unwrap();
    }

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    assert_eq!(value_of(&server, "k").await, "second-longer-value");
    assert_eq!(value_of(&server, "one").await, "2");

    let mut resp = server.resp_session("kvb_restart_user", COLLECTION).await;
    assert_eq!(
        resp_get(&mut resp, "k").await,
        Reply::Bulk(Some("second-longer-value".to_string())),
        "the replayed merge must be raw bytes"
    );
    assert_eq!(
        resp_get(&mut resp, "one").await,
        Reply::Bulk(Some("2".to_string()))
    );
}
