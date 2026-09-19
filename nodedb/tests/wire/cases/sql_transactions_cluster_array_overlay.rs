// SPDX-License-Identifier: BUSL-1.1

//! In-transaction ARRAY reads (`ARRAY_SLICE`, `ARRAY_AGG`) observe the
//! transaction's own uncommitted cell writes (read-your-own-writes), and
//! `INSERT INTO ARRAY` / `DELETE FROM ARRAY` answer with a real affected
//! count at statement time, on the CLUSTER plan path.
//!
//! Every case runs on the default `TestServer::start()`: `single_node_calvin`
//! is on, so `plan_sql()` emits the `ClusterArrayOp::{Put, Delete, Slice,
//! Agg}` routing wrappers. The staging gate fans a `Put` / `Delete` out into
//! one `ArrayOp::{Put, Delete}` per owning vShard (`session::txn_expand`),
//! buffers every per-shard task, and stages each into its shard's
//! `ArrayTxnOverlay` on that shard's leader (`session::array_fanout_stage`).
//! A `Slice` / `Agg` carries the session's `TxnId` to every shard
//! (`ArrayShardSliceReq::txn_id`, `ArrayShardAggReq::txn_id`), so each
//! shard folds its own staged cells into its rows or partial. COMMIT
//! replays the buffered per-shard tasks; ROLLBACK drops every shard's
//! overlay. The single-node form is covered by
//! `sql_transactions_array_overlay.rs`.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn create_array(server: &TestServer, array: &str) {
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

/// Rows of `ARRAY_SLICE` over the closed window `[r0, r1] x [c0, c1]`.
async fn slice_rows(
    server: &TestServer,
    array: &str,
    window: ((i64, i64), (i64, i64)),
) -> Vec<Vec<String>> {
    let ((r0, r1), (c0, c1)) = window;
    server
        .query_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('{array}', '{{\"row\":[{r0},{r1}],\"col\":[{c0},{c1}]}}', '*', 100)"
        ))
        .await
        .unwrap()
}

/// Rows of a point `ARRAY_SLICE` at `(row, col)`.
async fn cell_rows(server: &TestServer, array: &str, row: i64, col: i64) -> Vec<Vec<String>> {
    slice_rows(server, array, ((row, row), (col, col))).await
}

/// The scalar `ARRAY_AGG` result for `reducer` over `value`.
async fn agg(server: &TestServer, array: &str, reducer: &str) -> f64 {
    let rows = server
        .query_named_rows(&format!(
            "SELECT * FROM ARRAY_AGG('{array}', 'value', '{reducer}')"
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one scalar agg row: {rows:?}");
    let text = rows[0]
        .get("result")
        .unwrap_or_else(|| panic!("agg row has no result column: {rows:?}"));
    text.parse()
        .unwrap_or_else(|e| panic!("agg result not a float: {text}: {e}"))
}

/// Every `CommandComplete` count in `sql`'s simple-query response, in wire
/// order, plus the number of `Row` messages that arrived alongside them.
async fn command_tags(server: &TestServer, sql: &str) -> (Vec<u64>, usize) {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    let mut tags = Vec::new();
    let mut rows = 0;
    for m in messages {
        match m {
            SimpleQueryMessage::CommandComplete(n) => tags.push(n),
            SimpleQueryMessage::Row(_) => rows += 1,
            _ => {}
        }
    }
    (tags, rows)
}

/// The row count carried by the first `CommandComplete` in `sql`'s response.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let (tags, _) = command_tags(server, sql).await;
    tags.first()
        .copied()
        .unwrap_or_else(|| panic!("statement reported no command tag: {sql}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_staged_put_visible_to_same_txn_slice() {
    let server = TestServer::start().await;
    create_array(&server, "carr_ov_slice").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO ARRAY carr_ov_slice COORDS (1, 1) VALUES (7.0)")
        .await
        .unwrap();

    let rows = cell_rows(&server, "carr_ov_slice", 1, 1).await;
    assert_eq!(
        rows.len(),
        1,
        "in-tx ARRAY_SLICE must observe the transaction's own staged cell, got {rows:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_staged_put_rollback_discards_and_leaves_base_cell_intact() {
    let server = TestServer::start().await;
    create_array(&server, "carr_ov_rb").await;
    server
        .exec("INSERT INTO ARRAY carr_ov_rb COORDS (2, 2) VALUES (5.0)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO ARRAY carr_ov_rb COORDS (3, 3) VALUES (9.0)")
        .await
        .unwrap();
    assert_eq!(cell_rows(&server, "carr_ov_rb", 3, 3).await.len(), 1);
    // The base cell stays visible alongside the staged one.
    assert_eq!(cell_rows(&server, "carr_ov_rb", 2, 2).await.len(), 1);

    server.client.simple_query("ROLLBACK").await.unwrap();

    let staged = cell_rows(&server, "carr_ov_rb", 3, 3).await;
    assert!(
        staged.is_empty(),
        "rolled-back staged cell must not persist, got {staged:?}"
    );
    let base = cell_rows(&server, "carr_ov_rb", 2, 2).await;
    assert_eq!(
        base.len(),
        1,
        "base cell must survive the rollback, got {base:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_staged_put_outside_slice_window_excluded() {
    let server = TestServer::start().await;
    create_array(&server, "carr_ov_window").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO ARRAY carr_ov_window COORDS (8, 8) VALUES (1.0)")
        .await
        .unwrap();

    let outside = slice_rows(&server, "carr_ov_window", ((0, 4), (0, 4))).await;
    assert!(
        outside.is_empty(),
        "a staged cell outside the slice window must not appear, got {outside:?}"
    );
    let inside = slice_rows(&server, "carr_ov_window", ((5, 9), (5, 9))).await;
    assert_eq!(
        inside.len(),
        1,
        "the staged cell must appear inside its window, got {inside:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_staged_put_on_one_array_not_visible_on_another() {
    let server = TestServer::start().await;
    create_array(&server, "carr_ov_a").await;
    create_array(&server, "carr_ov_b").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO ARRAY carr_ov_a COORDS (1, 1) VALUES (1.0)")
        .await
        .unwrap();

    let other = cell_rows(&server, "carr_ov_b", 1, 1).await;
    assert!(
        other.is_empty(),
        "a staged cell on one array must not leak into another, got {other:?}"
    );
    assert_eq!(cell_rows(&server, "carr_ov_a", 1, 1).await.len(), 1);

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_staged_delete_hides_base_cell_from_slice_and_agg() {
    let server = TestServer::start().await;
    create_array(&server, "carr_ov_del").await;
    server
        .exec("INSERT INTO ARRAY carr_ov_del COORDS (1, 1) VALUES (10.0)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ARRAY carr_ov_del COORDS (2, 2) VALUES (20.0)")
        .await
        .unwrap();
    assert_eq!(agg(&server, "carr_ov_del", "count").await, 2.0);

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DELETE FROM ARRAY carr_ov_del WHERE COORDS IN ((1, 1))")
        .await
        .unwrap();

    let hidden = cell_rows(&server, "carr_ov_del", 1, 1).await;
    assert!(
        hidden.is_empty(),
        "a same-tx DELETE FROM ARRAY must hide the base cell from ARRAY_SLICE, got {hidden:?}"
    );
    assert_eq!(
        cell_rows(&server, "carr_ov_del", 2, 2).await.len(),
        1,
        "the untouched sibling cell stays visible"
    );
    assert_eq!(
        agg(&server, "carr_ov_del", "count").await,
        1.0,
        "ARRAY_AGG count must exclude the staged-deleted cell"
    );
    assert_eq!(
        agg(&server, "carr_ov_del", "sum").await,
        20.0,
        "ARRAY_AGG sum must exclude the staged-deleted cell"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    assert_eq!(
        cell_rows(&server, "carr_ov_del", 1, 1).await.len(),
        1,
        "ROLLBACK must restore the base cell"
    );
    assert_eq!(agg(&server, "carr_ov_del", "count").await, 2.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_in_txn_array_dml_answers_real_command_tags() {
    let server = TestServer::start().await;
    create_array(&server, "carr_ov_tags").await;
    server
        .exec("INSERT INTO ARRAY carr_ov_tags COORDS (9, 9) VALUES (1.0)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();

    // Three cells in one statement: the per-shard counts sum to the tag
    // whatever vShards the cells partition to.
    let inserted = affected(
        &server,
        "INSERT INTO ARRAY carr_ov_tags \
         COORDS (1, 1) VALUES (1.0), \
         COORDS (1, 2) VALUES (2.0), \
         COORDS (1, 3) VALUES (3.0)",
    )
    .await;
    assert_eq!(
        inserted, 3,
        "INSERT INTO ARRAY of three cells must answer INSERT 3"
    );

    // A base cell (committed before BEGIN) counts as existing.
    let deleted_base = affected(
        &server,
        "DELETE FROM ARRAY carr_ov_tags WHERE COORDS IN ((9, 9))",
    )
    .await;
    assert_eq!(
        deleted_base, 1,
        "deleting an existing base cell answers DELETE 1"
    );

    // A cell staged earlier in this transaction counts as existing.
    let deleted_staged = affected(
        &server,
        "DELETE FROM ARRAY carr_ov_tags WHERE COORDS IN ((1, 2))",
    )
    .await;
    assert_eq!(
        deleted_staged, 1,
        "deleting a same-tx staged cell answers DELETE 1"
    );

    // An absent cell, and a cell already staged-deleted, count as nothing.
    let deleted_absent = affected(
        &server,
        "DELETE FROM ARRAY carr_ov_tags WHERE COORDS IN ((7, 7))",
    )
    .await;
    assert_eq!(
        deleted_absent, 0,
        "deleting an absent cell answers DELETE 0"
    );
    let deleted_twice = affected(
        &server,
        "DELETE FROM ARRAY carr_ov_tags WHERE COORDS IN ((9, 9))",
    )
    .await;
    assert_eq!(
        deleted_twice, 0,
        "deleting an already staged-deleted cell answers DELETE 0"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_staged_put_visible_to_same_txn_agg() {
    let server = TestServer::start().await;
    create_array(&server, "carr_ov_agg").await;
    server
        .exec("INSERT INTO ARRAY carr_ov_agg COORDS (1, 1) VALUES (10.0)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO ARRAY carr_ov_agg COORDS (2, 2) VALUES (5.0)")
        .await
        .unwrap();

    // Each shard folds only its own staged cells into its partial, so the
    // coordinator's merge counts the staged cell exactly once.
    assert_eq!(
        agg(&server, "carr_ov_agg", "count").await,
        2.0,
        "ARRAY_AGG count must include the staged cell"
    );
    assert_eq!(
        agg(&server, "carr_ov_agg", "sum").await,
        15.0,
        "ARRAY_AGG sum must include the staged cell's value"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    assert_eq!(agg(&server, "carr_ov_agg", "count").await, 1.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_staged_put_commit_persists_cell() {
    let server = TestServer::start().await;
    create_array(&server, "carr_ov_commit").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO ARRAY carr_ov_commit COORDS (4, 4) VALUES (4.0)")
        .await
        .unwrap();
    assert_eq!(cell_rows(&server, "carr_ov_commit", 4, 4).await.len(), 1);
    server.client.simple_query("COMMIT").await.unwrap();

    let rows = cell_rows(&server, "carr_ov_commit", 4, 4).await;
    assert_eq!(
        rows.len(),
        1,
        "COMMIT must durably persist the staged cell, got {rows:?}"
    );
}
