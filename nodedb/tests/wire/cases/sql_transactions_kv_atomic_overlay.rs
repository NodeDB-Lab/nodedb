// SPDX-License-Identifier: BUSL-1.1

//! In-transaction read-your-own-writes for the value-computing KV atomic ops
//! (`KV_INCR`, `KV_INCR_FLOAT`, `KV_CAS`, `KV_GETSET`) and `BatchPut`.
//!
//! Extends the KV point-write staging overlay (`sql_transactions_kv_overlay.rs`)
//! to these ops: staged inside `BEGIN..COMMIT`, they return the SAME computed
//! result the autocommit handler returns, chain correctly against each other
//! within the same transaction, and are discarded on `ROLLBACK`. COMMIT's
//! durable replay is unchanged.
//!
//! RYOW / persistence is asserted via a follow-up `SELECT KV_INCR(k, 0)` (a
//! true no-op add) rather than `SELECT n FROM c WHERE key = ...`, because the
//! ordinary SQL projection read is unreliable for a row any atomic op has
//! ever touched: the base `KvEngine`'s `atomic_put` (`engine/kv/
//! engine_atomic.rs`) unconditionally writes with `Surrogate::ZERO`,
//! discarding a real surrogate a prior `INSERT`/`UPSERT` assigned that row
//! for SQL/surrogate-indexed access. This reproduces identically in
//! autocommit (no transaction involved) -- confirmed by direct probe -- so
//! it is a pre-existing base-engine gap, not something introduced by the
//! staging work this suite covers, and is out of scope to fix here.
//! `SELECT KV_INCR(k, 0)` sidesteps it: it reads the same way every `KV_*`
//! call does (`resolve_kv_current` / the base engine's own `table.get(key)`
//! by raw key bytes), which is unaffected by the surrogate reset.

use crate::harness::TestServer;

async fn setup(server: &TestServer) {
    server
        .exec("CREATE COLLECTION c (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();
}

/// Parse the JSON payload `SELECT KV_*(...)` returns as its single text column.
fn json_of(rows: &[String]) -> serde_json::Value {
    serde_json::from_str(&rows[0]).expect("KV_* result must be JSON")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incr_in_tx_returns_computed_value_and_chains() {
    let server = TestServer::start().await;
    setup(&server).await;

    server
        .exec("INSERT INTO c (key, n) VALUES ('ctr', 5)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();

    // First staged INCR: 5 + 3 = 8.
    let rows = server
        .query_text("SELECT KV_INCR('c', 'ctr', 3)")
        .await
        .unwrap();
    assert_eq!(json_of(&rows)["value"], 8);

    // In-tx read-your-own-writes: a no-op (+0) INCR must observe the staged
    // value (see module doc for why this replaces a `SELECT n FROM c`
    // check).
    let peek = server
        .query_text("SELECT KV_INCR('c', 'ctr', 0)")
        .await
        .unwrap();
    assert_eq!(
        json_of(&peek)["value"],
        8,
        "in-tx read must observe the staged INCR"
    );

    // A second real staged INCR chains off the FIRST staged value: 8 + 2 =
    // 10, not 5 + 2 = 7.
    let rows2 = server
        .query_text("SELECT KV_INCR('c', 'ctr', 2)")
        .await
        .unwrap();
    assert_eq!(
        json_of(&rows2)["value"],
        10,
        "second in-tx INCR must chain off the first staged value"
    );

    server.exec("COMMIT").await.unwrap();

    let committed = server
        .query_text("SELECT KV_INCR('c', 'ctr', 0)")
        .await
        .unwrap();
    assert_eq!(
        json_of(&committed)["value"],
        10,
        "COMMIT must persist the chained INCR"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incr_in_tx_rollback_reverts_to_base_value() {
    let server = TestServer::start().await;
    setup(&server).await;

    server
        .exec("INSERT INTO c (key, n) VALUES ('ctr', 5)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    let rows = server
        .query_text("SELECT KV_INCR('c', 'ctr', 100)")
        .await
        .unwrap();
    assert_eq!(json_of(&rows)["value"], 105);
    server.exec("ROLLBACK").await.unwrap();

    let after = server
        .query_text("SELECT KV_INCR('c', 'ctr', 0)")
        .await
        .unwrap();
    assert_eq!(
        json_of(&after)["value"],
        5,
        "ROLLBACK must discard the staged INCR"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incr_on_absent_key_in_tx_creates_it_from_zero() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let rows = server
        .query_text("SELECT KV_INCR('c', 'fresh', 7)")
        .await
        .unwrap();
    assert_eq!(json_of(&rows)["value"], 7);
    server.exec("COMMIT").await.unwrap();

    let after = server
        .query_text("SELECT KV_INCR('c', 'fresh', 0)")
        .await
        .unwrap();
    assert_eq!(json_of(&after)["value"], 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incr_float_in_tx_ryow() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let rows = server
        .query_text("SELECT KV_INCR_FLOAT('c', 'dmg', 2.5)")
        .await
        .unwrap();
    assert!((json_of(&rows)["value"].as_f64().unwrap() - 2.5).abs() < f64::EPSILON);

    let rows2 = server
        .query_text("SELECT KV_INCR_FLOAT('c', 'dmg', 1.5)")
        .await
        .unwrap();
    assert!(
        (json_of(&rows2)["value"].as_f64().unwrap() - 4.0).abs() < f64::EPSILON,
        "second in-tx INCR_FLOAT must chain off the first staged value"
    );

    server.exec("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cas_in_tx_create_then_chain_then_fail() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    // Create-if-absent: expected empty, key does not exist yet.
    let rows = server
        .query_text("SELECT KV_CAS('c', 'state', '', 'idle')")
        .await
        .unwrap();
    let v = json_of(&rows);
    assert_eq!(v["success"], true);
    assert_eq!(v["current_value"], serde_json::Value::Null);

    // A later CAS in the same transaction must compare against the STAGED
    // value ('idle'), not absence.
    let rows2 = server
        .query_text("SELECT KV_CAS('c', 'state', 'idle', 'in_match')")
        .await
        .unwrap();
    let v2 = json_of(&rows2);
    assert_eq!(
        v2["success"], true,
        "CAS must match against the staged 'idle' value from the prior statement"
    );

    // A CAS against a now-stale expected value must fail, reporting the
    // current (staged) value.
    let rows3 = server
        .query_text("SELECT KV_CAS('c', 'state', 'idle', 'ended')")
        .await
        .unwrap();
    let v3 = json_of(&rows3);
    assert_eq!(v3["success"], false);

    server.exec("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn getset_in_tx_returns_staged_old_value() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let rows = server
        .query_text("SELECT KV_GETSET('c', 'tok', 'first-token')")
        .await
        .unwrap();
    assert_eq!(json_of(&rows)["old_value"], serde_json::Value::Null);

    // Second GETSET in the same transaction must report the FIRST staged
    // value as its old value.
    let rows2 = server
        .query_text("SELECT KV_GETSET('c', 'tok', 'second-token')")
        .await
        .unwrap();
    let old_b64 = json_of(&rows2)["old_value"]
        .as_str()
        .expect("old_value must be a base64 string")
        .to_string();
    let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, old_b64)
        .expect("old_value must be valid base64");
    assert_eq!(decoded, b"first-token");

    server.exec("COMMIT").await.unwrap();
}

// `BatchPut` has no SQL surface (`KV_BATCH_PUT` does not exist); its only
// caller is the native protocol's `KvBatchPut` direct-op path
// (`native/dispatch/direct_ops.rs`), which now routes every direct op
// through the same `route_in_tx_write`/`stage_write` staging gate the
// SQL-planned dispatch loops use (`dispatch_single_task` in
// `direct_ops.rs`), so `KvOp::BatchPut`'s `is_stageable_write` /
// `stageable_write_shape` classification and its Data Plane staging handler
// (`stage_kv_atomic::stage_kv_batch_put`) are exercised end-to-end. See
// `nodedb/tests/native_direct_op_txn_overlay.rs` for the native-protocol
// coverage (staged BatchPut visible read-your-own-writes, discarded on
// ROLLBACK, persisted on COMMIT) and `staging_predicates`'s unit tests for
// coverage of the classification itself.
