// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for the txn-buffering atomicity fix: a documented set
//! of writes (`ArrayOp::{Put, Delete}`, `DocumentOp::{BatchInsert, Merge,
//! UpdateFromJoin}`, plus `CrdtOp` and `VectorOp` variants not exercised by a
//! standalone SQL surface) must not execute IMMEDIATELY against base state
//! inside an explicit `BEGIN ... COMMIT` block — that would be visible before
//! COMMIT, and NOT undone by ROLLBACK. They are buffered like every other
//! transactional write: ROLLBACK discards them, COMMIT is required to
//! persist them, and — the deliberate trade-off documented in
//! `control/server/shared/write_admission/predicate/txn_buffering.rs` — a
//! read later in the SAME transaction no longer observes a buffered write
//! until COMMIT (read-your-own-writes is lost for the buffered ops). Array
//! writes are the exception: `ArrayOp::{Put, Delete}` stages into
//! `ArrayTxnOverlay` at statement time, so a same-transaction read sees it
//! (`array_insert_in_txn_visible_to_same_txn_read` below, on a standalone
//! server; the full contract lives in `sql_transactions_array_overlay.rs`
//! and, for the cluster wrapper, `sql_transactions_cluster_array_overlay.rs`).
//!
//! `CrdtOp` and `VectorOp` flipped variants have no direct SQL entry point on
//! the existing pgwire surface, so the fix is pinned via the Array and
//! Document cases below, which exercise the identical `exec_tx_passthrough`
//! COMMIT-replay path.
//!
//! `INSERT INTO ARRAY` / `DELETE FROM ARRAY` do not plan as `ArrayOp` on the
//! default `TestServer::start()`. Whenever a cluster topology exists -- which
//! that server has, since `server.single_node_calvin` defaults on --
//! `plan_sql()` emits the Control-Plane routing wrapper
//! `ClusterArrayOp::{Put, Delete}` instead (`array_convert/dml.rs`, gated on
//! `ctx.cluster_enabled`). The wrapper has no Data-Plane handler, so the
//! staging gate reshapes it into one `ArrayOp::{Put, Delete}` per owning
//! vShard (`session::txn_expand`), buffers every per-shard task, and stages
//! each into its shard's overlay (`session::array_fanout_stage`); COMMIT
//! replays the buffered tasks. The atomicity cases below therefore exercise
//! the wrapper end to end. The read-your-own-writes case uses
//! `TestServer::start_standalone()`, where the planner emits the single-node
//! `ArrayOp` form.

use crate::harness::TestServer;

/// Query a single-cell 2D array by exact coordinates. Returns the matched
/// rows (0 or 1 for a point query).
async fn array_cell_rows(server: &TestServer, array: &str, row: i64, col: i64) -> Vec<Vec<String>> {
    server
        .query_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('{array}', '{{\"row\":[{row},{row}],\"col\":[{col},{col}]}}', '*', 10)"
        ))
        .await
        .unwrap()
}

async fn setup_array(server: &TestServer, array: &str) {
    server
        .exec(&format!(
            "CREATE ARRAY {array} \
             DIMS (row INT64, col INT64) \
             ATTRS (value FLOAT64) \
             TILE_EXTENTS (10, 10)"
        ))
        .await
        .unwrap();
}

/// `BEGIN; INSERT INTO ARRAY; ROLLBACK` must leave the cell ABSENT. On the
/// pre-fix predicate, `ArrayOp::Put` classified as an unbuffered write: the
/// insert executed immediately against base state and SURVIVED the
/// ROLLBACK — this assertion is exactly what caught that bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_insert_rollback_discards_cell() {
    let server = TestServer::start().await;
    setup_array(&server, "arr_atomic_rb").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO ARRAY arr_atomic_rb COORDS (1, 1) VALUES (7.0)")
        .await
        .unwrap();
    server.client.simple_query("ROLLBACK").await.unwrap();

    let rows = array_cell_rows(&server, "arr_atomic_rb", 1, 1).await;
    assert!(
        rows.is_empty(),
        "ROLLBACK must discard a buffered ArrayOp::Put; found cell: {rows:?}"
    );
}

/// `BEGIN; INSERT INTO ARRAY; COMMIT` must leave the cell PRESENT: buffering
/// the write does not silently drop it, COMMIT still durably applies it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_insert_commit_persists_cell() {
    let server = TestServer::start().await;
    setup_array(&server, "arr_atomic_commit").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO ARRAY arr_atomic_commit COORDS (2, 2) VALUES (9.0)")
        .await
        .unwrap();
    server.client.simple_query("COMMIT").await.unwrap();

    let rows = array_cell_rows(&server, "arr_atomic_commit", 2, 2).await;
    assert_eq!(
        rows.len(),
        1,
        "COMMIT must durably persist a buffered ArrayOp::Put; got {rows:?}"
    );
}

/// `BEGIN; DELETE FROM ARRAY; ROLLBACK` must leave the pre-existing cell
/// PRESENT (the delete must not have executed against base state).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_delete_rollback_restores_cell() {
    let server = TestServer::start().await;
    setup_array(&server, "arr_atomic_del_rb").await;
    server
        .exec("INSERT INTO ARRAY arr_atomic_del_rb COORDS (3, 3) VALUES (5.0)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DELETE FROM ARRAY arr_atomic_del_rb WHERE COORDS IN ((3, 3))")
        .await
        .unwrap();
    server.client.simple_query("ROLLBACK").await.unwrap();

    let rows = array_cell_rows(&server, "arr_atomic_del_rb", 3, 3).await;
    assert_eq!(
        rows.len(),
        1,
        "ROLLBACK must discard a buffered ArrayOp::Delete; cell must survive, got {rows:?}"
    );
}

/// On a standalone server the single-node `ArrayOp::Put` stages into
/// `ArrayTxnOverlay` at statement time, so a read later in the SAME
/// transaction observes the cell before COMMIT, ROLLBACK removes it, and a
/// base cell committed before BEGIN survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_insert_in_txn_visible_to_same_txn_read() {
    let server = TestServer::start_standalone().await;
    setup_array(&server, "arr_atomic_ryow").await;
    server
        .exec("INSERT INTO ARRAY arr_atomic_ryow COORDS (5, 5) VALUES (2.0)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO ARRAY arr_atomic_ryow COORDS (4, 4) VALUES (1.0)")
        .await
        .unwrap();

    let rows = array_cell_rows(&server, "arr_atomic_ryow", 4, 4).await;
    assert_eq!(
        rows.len(),
        1,
        "a staged ArrayOp::Put must be visible to a read in the same \
         transaction before COMMIT (read-your-own-writes); got {rows:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    let rows = array_cell_rows(&server, "arr_atomic_ryow", 4, 4).await;
    assert!(
        rows.is_empty(),
        "ROLLBACK must discard the staged cell; found {rows:?}"
    );
    let base = array_cell_rows(&server, "arr_atomic_ryow", 5, 5).await;
    assert_eq!(
        base.len(),
        1,
        "base cell must survive the rollback; got {base:?}"
    );
}

// ── Document MERGE: rollback discards, commit persists ─────────────────────

async fn create_merge_schema(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION merge_atomic_target (\
                id TEXT PRIMARY KEY, \
                name TEXT, \
                score INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION merge_atomic_source (\
                id TEXT PRIMARY KEY, \
                name TEXT, \
                score INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO merge_atomic_target (id, name, score) VALUES ('a', 'alpha', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO merge_atomic_source (id, name, score) VALUES ('a', 'ALPHA_UPD', 99)")
        .await
        .unwrap();
}

const MERGE_SQL: &str = "MERGE INTO merge_atomic_target t \
     USING merge_atomic_source s ON t.id = s.id \
     WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score";

/// `BEGIN; MERGE; ROLLBACK` must leave the target row UNCHANGED. On the
/// pre-fix predicate, `DocumentOp::Merge` classified as an unbuffered write:
/// the merge executed immediately against base state and SURVIVED the
/// ROLLBACK — this assertion is exactly what caught that bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_merge_rollback_discards_changes() {
    let server = TestServer::start().await;
    create_merge_schema(&server).await;

    server.exec("BEGIN").await.unwrap();
    server.exec(MERGE_SQL).await.unwrap();
    server.client.simple_query("ROLLBACK").await.unwrap();

    let rows = server
        .query_rows("SELECT name, score FROM merge_atomic_target WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec!["alpha".to_string(), "10".to_string()],
        "ROLLBACK must discard a buffered DocumentOp::Merge; got {rows:?}"
    );
}

/// `BEGIN; MERGE; COMMIT` must leave the target row updated: buffering the
/// merge does not silently drop it, COMMIT still durably applies it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_merge_commit_persists_changes() {
    let server = TestServer::start().await;
    create_merge_schema(&server).await;

    server.exec("BEGIN").await.unwrap();
    server.exec(MERGE_SQL).await.unwrap();
    server.client.simple_query("COMMIT").await.unwrap();

    let rows = server
        .query_rows("SELECT name, score FROM merge_atomic_target WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec!["ALPHA_UPD".to_string(), "99".to_string()],
        "COMMIT must durably persist a buffered DocumentOp::Merge; got {rows:?}"
    );
}
