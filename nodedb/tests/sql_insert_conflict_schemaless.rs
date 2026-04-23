//! INSERT conflict semantics for schemaless document engine.
//!
//! Shares the same spec as the strict engine — the schemaless path also
//! routes through `DocumentOp::PointPut`, so the same invariants apply:
//! duplicate-id INSERT raises 23505, `ON CONFLICT DO NOTHING` is a no-op.

mod common;

use common::pgwire_harness::TestServer;

// ── Schemaless document ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_duplicate_id_raises_unique_violation() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    match server
        .client
        .simple_query("INSERT INTO docs (id, n) VALUES ('dup', 2)")
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
                db_err.message().to_lowercase().contains("dup"),
                "error must name the conflicting id, got: {}",
                db_err.message()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_duplicate_id_preserves_original_row() {
    // Silent-overwrite regression guard on the schemaless path — the
    // DocumentSchemaless arm of `convert_insert` shares the PointPut
    // routing, so the same check must hold.
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    let _ = server
        .client
        .simple_query("INSERT INTO docs (id, n) VALUES ('dup', 2)")
        .await;

    let rows = server
        .query_text("SELECT n FROM docs WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected exactly one row, got {rows:?}");
    assert!(
        rows[0].contains("\"n\":1"),
        "duplicate-id INSERT must not overwrite the original row, got: {}",
        rows[0]
    );
    assert!(
        !rows[0].contains("\"n\":2"),
        "original row was overwritten with the rejected value: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_on_conflict_do_nothing_is_noop() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 2) ON CONFLICT DO NOTHING")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM docs WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("\"n\":1"), "got: {}", rows[0]);
}
