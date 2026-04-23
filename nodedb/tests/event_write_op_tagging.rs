//! Write-event tag correctness: the `WriteOp` emitted to the Event Plane
//! must reflect whether the mutation created a new row or replaced an
//! existing one. Downstream consumers (AFTER triggers, CDC) branch on
//! `Insert` vs `Update`; a wrong tag silently delivers the wrong event.
//!
//! These tests drive real SQL through the harness and observe which
//! AFTER trigger fires. Sync triggers are used so the audit table is
//! populated by the time the outer statement returns.

mod common;

use common::pgwire_harness::TestServer;

/// UPSERT onto an existing primary key must emit `WriteOp::Update`.
/// An `AFTER UPDATE` trigger must fire; an `AFTER INSERT` trigger must not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_on_existing_row_emits_update() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION src").await.unwrap();
    server.exec("CREATE COLLECTION ins_log").await.unwrap();
    server.exec("CREATE COLLECTION upd_log").await.unwrap();

    server
        .exec(
            "CREATE SYNC TRIGGER on_ins AFTER INSERT ON src FOR EACH ROW \
             BEGIN INSERT INTO ins_log (id, src_id) VALUES (NEW.id || '_i', NEW.id); END;",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE SYNC TRIGGER on_upd AFTER UPDATE ON src FOR EACH ROW \
             BEGIN INSERT INTO upd_log (id, src_id) VALUES (NEW.id || '_u', NEW.id); END;",
        )
        .await
        .unwrap();

    // Seed the row (fires AFTER INSERT — expected).
    server
        .exec("UPSERT INTO src (id, v) VALUES ('a', 1)")
        .await
        .unwrap();
    // Overwrite the row (must fire AFTER UPDATE, not AFTER INSERT).
    server
        .exec("UPSERT INTO src (id, v) VALUES ('a', 2)")
        .await
        .unwrap();

    let ins_rows = server
        .query_text("SELECT id FROM ins_log ORDER BY id")
        .await
        .unwrap();
    let upd_rows = server
        .query_text("SELECT id FROM upd_log ORDER BY id")
        .await
        .unwrap();

    assert_eq!(
        ins_rows.len(),
        1,
        "AFTER INSERT must fire once (seed only); got: {ins_rows:?}"
    );
    assert_eq!(
        upd_rows.len(),
        1,
        "AFTER UPDATE must fire on the overwrite; got: {upd_rows:?}"
    );
}

/// UPSERT onto a fresh primary key must emit `WriteOp::Insert`.
/// An `AFTER INSERT` trigger must fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_on_new_row_emits_insert() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION src").await.unwrap();
    server.exec("CREATE COLLECTION ins_log").await.unwrap();

    server
        .exec(
            "CREATE SYNC TRIGGER on_ins AFTER INSERT ON src FOR EACH ROW \
             BEGIN INSERT INTO ins_log (id, src_id) VALUES (NEW.id || '_i', NEW.id); END;",
        )
        .await
        .unwrap();

    server
        .exec("UPSERT INTO src (id, v) VALUES ('a', 1)")
        .await
        .unwrap();

    let rows = server.query_text("SELECT id FROM ins_log").await.unwrap();
    assert_eq!(rows.len(), 1, "AFTER INSERT must fire; got: {rows:?}");
}

/// `INSERT ... ON CONFLICT (id) DO UPDATE SET ...` onto an existing row
/// must emit `WriteOp::Update`. AFTER UPDATE fires, AFTER INSERT does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_on_conflict_do_update_emits_update_on_overwrite() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION src").await.unwrap();
    server.exec("CREATE COLLECTION ins_log").await.unwrap();
    server.exec("CREATE COLLECTION upd_log").await.unwrap();

    server
        .exec(
            "CREATE SYNC TRIGGER on_ins AFTER INSERT ON src FOR EACH ROW \
             BEGIN INSERT INTO ins_log (id, src_id) VALUES (NEW.id || '_i', NEW.id); END;",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE SYNC TRIGGER on_upd AFTER UPDATE ON src FOR EACH ROW \
             BEGIN INSERT INTO upd_log (id, src_id) VALUES (NEW.id || '_u', NEW.id); END;",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO src (id, v) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO src (id, v) VALUES ('a', 2) \
             ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        )
        .await
        .unwrap();

    let ins_rows = server
        .query_text("SELECT id FROM ins_log ORDER BY id")
        .await
        .unwrap();
    let upd_rows = server
        .query_text("SELECT id FROM upd_log ORDER BY id")
        .await
        .unwrap();

    assert_eq!(
        ins_rows.len(),
        1,
        "AFTER INSERT must fire for the first (new-row) insert only; got: {ins_rows:?}"
    );
    assert_eq!(
        upd_rows.len(),
        1,
        "AFTER UPDATE must fire on the conflict path; got: {upd_rows:?}"
    );
}

/// `INSERT ... ON CONFLICT DO NOTHING` on an existing row must emit no
/// event — the write is a silent no-op, so neither AFTER INSERT nor
/// AFTER UPDATE should fire. Regression guard: the bug class includes
/// both "wrong tag" and "emit when no mutation occurred".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_on_conflict_do_nothing_emits_no_event() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION src").await.unwrap();
    server.exec("CREATE COLLECTION ins_log").await.unwrap();
    server.exec("CREATE COLLECTION upd_log").await.unwrap();

    server
        .exec(
            "CREATE SYNC TRIGGER on_ins AFTER INSERT ON src FOR EACH ROW \
             BEGIN INSERT INTO ins_log (id, src_id) VALUES (NEW.id || '_i', NEW.id); END;",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE SYNC TRIGGER on_upd AFTER UPDATE ON src FOR EACH ROW \
             BEGIN INSERT INTO upd_log (id, src_id) VALUES (NEW.id || '_u', NEW.id); END;",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO src (id, v) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO src (id, v) VALUES ('a', 2) ON CONFLICT DO NOTHING")
        .await
        .unwrap();

    let ins_rows = server
        .query_text("SELECT id FROM ins_log ORDER BY id")
        .await
        .unwrap();
    let upd_rows = server
        .query_text("SELECT id FROM upd_log ORDER BY id")
        .await
        .unwrap();

    assert_eq!(
        ins_rows.len(),
        1,
        "only the first insert should fire AFTER INSERT; got: {ins_rows:?}"
    );
    assert!(
        upd_rows.is_empty(),
        "DO NOTHING must not fire AFTER UPDATE; got: {upd_rows:?}"
    );
}
