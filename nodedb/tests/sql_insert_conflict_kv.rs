//! INSERT conflict semantics for key-value engine.
//!
//! Same structural flaw as the document engines: the SQL planner collapses
//! INSERT and UPSERT into `SqlPlan::KvInsert`, and `convert_kv_insert`
//! routes every row to `KvOp::Put` (RESP-SET-style upsert). The INSERT-vs-
//! UPSERT distinction must survive planning: duplicate-key INSERT raises
//! 23505, `ON CONFLICT DO NOTHING` no-ops, and `ON CONFLICT (key) DO
//! UPDATE` / `UPSERT` are the opt-in overwrite paths. `KvOp::Put` stays
//! reserved for UPSERT + RESP SET semantics.

mod common;

use common::pgwire_harness::TestServer;

// ── Key-value ───────────────────────────────────────────────────────────────
//
// KV shares the same structural flaw as the strict/schemaless document
// engines: the SQL planner collapses INSERT and UPSERT into a single
// `SqlPlan::KvInsert` variant, and the physical-plan converter routes
// every row to `KvOp::Put` (a Redis-SET-style upsert). The INSERT-vs-
// UPSERT distinction must survive planning for KV the same way it does
// for documents — duplicate-key INSERT must raise 23505, ON CONFLICT
// DO NOTHING must no-op, and UPSERT / ON CONFLICT DO UPDATE remain the
// opt-in overwrite paths. `KvOp::Put` stays reserved for UPSERT + RESP
// SET semantics.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_duplicate_key_raises_unique_violation() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION c TYPE KEY_VALUE (key TEXT PRIMARY KEY, n INT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO c (key, n) VALUES ('k', 1)")
        .await
        .unwrap();

    match server
        .client
        .simple_query("INSERT INTO c (key, n) VALUES ('k', 2)")
        .await
    {
        Ok(_) => panic!("expected unique_violation, got success"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
            assert!(
                db_err.message().to_lowercase().contains("k"),
                "error must name the conflicting key, got: {}",
                db_err.message()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_duplicate_key_preserves_original_row() {
    // Silent-overwrite regression guard: even if the error surfaces, a
    // future routing regression to `KvOp::Put` would overwrite the row
    // beneath the error. Assert the original n=1 survives.
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION c TYPE KEY_VALUE (key TEXT PRIMARY KEY, n INT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO c (key, n) VALUES ('k', 1)")
        .await
        .unwrap();

    let _ = server
        .client
        .simple_query("INSERT INTO c (key, n) VALUES ('k', 2)")
        .await;

    let rows = server
        .query_text("SELECT n FROM c WHERE key = 'k'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected exactly one row, got {rows:?}");
    assert!(
        rows[0].contains("\"n\":1"),
        "duplicate-key INSERT must not overwrite the original row, got: {}",
        rows[0]
    );
    assert!(
        !rows[0].contains("\"n\":2"),
        "original row was overwritten with the rejected value: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_duplicate_key_resp_value_form_raises_unique_violation() {
    // `convert_kv_insert` has a distinct code path for the simple
    // `(key, value)` shape (raw bytes, RESP opaque contract) vs the
    // typed-columns shape. Both paths route to `KvOp::Put`, so both
    // must be covered.
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION c TYPE KEY_VALUE (key STRING PRIMARY KEY, value STRING)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO c (key, value) VALUES ('k', 'first')")
        .await
        .unwrap();

    match server
        .client
        .simple_query("INSERT INTO c (key, value) VALUES ('k', 'second')")
        .await
    {
        Ok(_) => panic!("expected unique_violation on RESP (key,value) form, got success"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
        }
    }

    let rows = server
        .query_text("SELECT value FROM c WHERE key = 'k'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("first"),
        "RESP-shape duplicate INSERT must not overwrite, got: {}",
        rows[0]
    );
    assert!(
        !rows[0].contains("second"),
        "RESP-shape original row was overwritten: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_on_conflict_do_nothing_is_noop() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION c TYPE KEY_VALUE (key TEXT PRIMARY KEY, n INT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO c (key, n) VALUES ('k', 1)")
        .await
        .unwrap();

    // Must succeed (no error), must not overwrite.
    server
        .exec("INSERT INTO c (key, n) VALUES ('k', 2) ON CONFLICT DO NOTHING")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM c WHERE key = 'k'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("\"n\":1"),
        "ON CONFLICT DO NOTHING must leave the original row intact, got: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_on_conflict_do_update_overwrites() {
    // Opt-in overwrite path must keep working so the fix for the
    // default-INSERT path doesn't strand users without an overwrite
    // option on the KV engine.
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION c TYPE KEY_VALUE (key TEXT PRIMARY KEY, n INT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO c (key, n) VALUES ('k', 1)")
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO c (key, n) VALUES ('k', 2) \
             ON CONFLICT (key) DO UPDATE SET n = EXCLUDED.n",
        )
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM c WHERE key = 'k'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("\"n\":2"), "got: {}", rows[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_upsert_keyword_overwrites() {
    // Regression guard on the explicit UPSERT grammar path for KV —
    // the RESP-SET-style overwrite semantics must be preserved.
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION c TYPE KEY_VALUE (key TEXT PRIMARY KEY, n INT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO c (key, n) VALUES ('k', 1)")
        .await
        .unwrap();

    server
        .exec("UPSERT INTO c (key, n) VALUES ('k', 2)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM c WHERE key = 'k'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("\"n\":2"), "got: {}", rows[0]);
}
