// SPDX-License-Identifier: BUSL-1.1

//! PRIMARY KEY implies NOT NULL: NULL keys raise `23502` (not_null_violation).
//!
//! The document (schemaless) and kv engines used to accept a NULL primary
//! key — explicit or by omission — and commit several NULL-keyed rows per
//! collection. Uniqueness still applied to real values, so the key was
//! neither unique nor non-null for those rows, and readback disagreed
//! between scan and aggregate paths on the same `IS NULL` predicate. These
//! tests pin the write-time rejection on every engine that routes through a
//! declared key, plus the exemption for collections whose key is synthetic
//! (auto-id schemaless), which legitimately mint fresh identities.

use crate::harness::TestServer;

fn assert_23502(result: &Result<(), String>, collection: &str, column: &str) {
    let message = match result {
        Ok(()) => panic!("expected 23502 for null {collection}.{column}, statement succeeded"),
        Err(message) => message,
    };
    assert!(
        message.contains("23502"),
        "expected SQLSTATE 23502 for {collection}.{column}, got: {message}"
    );
    assert!(
        message.contains(column),
        "error must name the column '{column}': {message}"
    );
}

/// Schemaless document with a declared key: explicit NULL and omitted key
/// both raise; real values still insert and duplicates still raise 23505.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_document_rejects_null_and_omitted_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk2 (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap();

    assert_23502(
        &server
            .exec("INSERT INTO pk2 (id, v) VALUES (NULL, 'explicit-null')")
            .await,
        "pk2",
        "id",
    );
    assert_23502(
        &server
            .exec("INSERT INTO pk2 (v) VALUES ('omitted-pk')")
            .await,
        "pk2",
        "id",
    );
    // UPSERT takes the same gate.
    assert_23502(
        &server
            .exec("UPSERT INTO pk2 (id, v) VALUES (NULL, 'upsert-null')")
            .await,
        "pk2",
        "id",
    );

    server
        .exec("INSERT INTO pk2 (id, v) VALUES (1, 'ok')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO pk2 (id, v) VALUES (2, 'ok2')")
        .await
        .unwrap();
    let dup = server
        .exec("INSERT INTO pk2 (id, v) VALUES (1, 'dup')")
        .await
        .unwrap_err();
    assert!(
        dup.contains("duplicate"),
        "real duplicate must still raise uniqueness: {dup}"
    );

    let rows = server
        .query_named_rows("SELECT count(*) AS n FROM pk2")
        .await
        .expect("count must work");
    assert_eq!(rows[0].get("n").map(String::as_str), Some("2"), "{rows:?}");
}

/// kv: omitted and explicit-NULL keys raise; the key column is named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_rejects_null_and_omitted_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk4 (k TEXT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();

    assert_23502(
        &server
            .exec("INSERT INTO pk4 (v) VALUES ('kv-omitted')")
            .await,
        "pk4",
        "k",
    );
    assert_23502(
        &server
            .exec("INSERT INTO pk4 (k, v) VALUES (NULL, 'kv-null')")
            .await,
        "pk4",
        "k",
    );

    server
        .exec("INSERT INTO pk4 (k, v) VALUES ('a', '1')")
        .await
        .unwrap();
    let rows = server
        .query_named_rows("SELECT k, v FROM pk4")
        .await
        .expect("valid row readable");
    assert_eq!(rows.len(), 1, "{rows:?}");
}

/// document_strict used to reject via an internal tuple-serialization error;
/// the plan-time gate now surfaces the clean 23502.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_surfaces_clean_23502_for_null_key() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION pk3 (id INT PRIMARY KEY, v TEXT) WITH (engine = 'document_strict')",
        )
        .await
        .unwrap();

    assert_23502(
        &server
            .exec("INSERT INTO pk3 (id, v) VALUES (NULL, 'strict-null')")
            .await,
        "pk3",
        "id",
    );
    let empty = server
        .query_named_rows("SELECT count(*) AS n FROM pk3")
        .await
        .expect("count works");
    assert_eq!(
        empty[0].get("n").map(String::as_str),
        Some("0"),
        "{empty:?}"
    );
}

/// The exemption: a schemaless collection created without a column list has
/// a synthetic key, so an omitted key still mints a fresh identity — that is
/// the auto-id contract, and NULL reads on dynamic fields keep folding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_id_schemaless_still_mints_identity_on_omitted_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_auto WITH (engine = 'document_schemaless')")
        .await
        .unwrap();

    server
        .exec("INSERT INTO pk_auto (dyn_a) VALUES ('r1')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO pk_auto (dyn_a) VALUES ('r2')")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT dyn_a FROM pk_auto ORDER BY dyn_a")
        .await
        .expect("both rows readable");
    assert_eq!(rows.len(), 2, "two distinct auto-id rows: {rows:?}");
}
