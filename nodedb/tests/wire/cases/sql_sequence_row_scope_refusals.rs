// SPDX-License-Identifier: BUSL-1.1

//! Refusals that survive the per-row SELECT-list evaluation rule.
//!
//! Only the SELECT list of a top-level SELECT over a relation evaluates a
//! sequence accessor per row (see `sql_sequence_row_scope.rs`). Every other
//! row-scope clause — ORDER BY, GROUP BY, HAVING, JOIN ON, an aggregate
//! argument, a window function, a nested subquery, UPDATE SET, and the
//! source of `INSERT ... SELECT` — still refuses with `0A000`
//! (feature_not_supported).

use crate::harness::TestServer;

/// Create sequence `s` and a kv collection `t` (`id BIGINT PRIMARY KEY, v
/// TEXT`) with rows `(1,'a'), (2,'b'), (3,'c')`.
async fn seed(server: &TestServer) {
    server.exec("CREATE SEQUENCE s").await.unwrap();
    server
        .exec("CREATE COLLECTION t (id BIGINT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
}

/// `ORDER BY` sorts before the SELECT list is materialized, so the accessor
/// there would decide row order from side-effecting state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_in_order_by_is_refused() {
    let server = TestServer::start().await;
    seed(&server).await;

    server
        .expect_error("SELECT id FROM t ORDER BY nextval('s')", "0A000")
        .await;
}

/// `GROUP BY` groups before any per-output-row evaluation exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_in_group_by_is_refused() {
    let server = TestServer::start().await;
    seed(&server).await;

    server
        .expect_error("SELECT COUNT(*) FROM t GROUP BY nextval('s')", "0A000")
        .await;
}

/// `HAVING` filters groups before the SELECT list runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_in_having_is_refused() {
    let server = TestServer::start().await;
    seed(&server).await;
    server
        .exec("CREATE COLLECTION g (id BIGINT PRIMARY KEY, grp TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO g (id, grp) VALUES (1, 'a'), (2, 'a'), (3, 'b')")
        .await
        .unwrap();

    server
        .expect_error(
            "SELECT grp FROM g GROUP BY grp HAVING nextval('s') > 0",
            "0A000",
        )
        .await;
}

/// A `JOIN ON` predicate decides which rows exist before the SELECT list
/// runs over them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_in_join_on_is_refused() {
    let server = TestServer::start().await;
    seed(&server).await;

    server
        .expect_error(
            "SELECT a.id FROM t a JOIN t b ON a.id = nextval('s')",
            "0A000",
        )
        .await;
}

/// `UPDATE ... SET` evaluates its expression once per matched row inside
/// the write path, not the read-side per-row evaluator the SELECT-list rule
/// covers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_in_update_set_is_refused() {
    let server = TestServer::start().await;
    seed(&server).await;

    server
        .expect_error("UPDATE t SET v = nextval('s')", "0A000")
        .await;
}

/// An aggregate's argument is evaluated inside the aggregation step, before
/// any output row exists to attribute the accessor call to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_as_an_aggregate_argument_is_refused() {
    let server = TestServer::start().await;
    seed(&server).await;

    server
        .expect_error("SELECT SUM(nextval('s')) FROM t", "0A000")
        .await;
}

/// A window function's `ORDER BY` runs before the SELECT list, the same
/// reason the top-level `ORDER BY` refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_in_a_window_order_by_is_refused() {
    let server = TestServer::start().await;
    seed(&server).await;

    server
        .expect_error(
            "SELECT ROW_NUMBER() OVER (ORDER BY nextval('s')) FROM t",
            "0A000",
        )
        .await;
}

/// `INSERT ... SELECT` accessors live in the source SELECT's list feeding a
/// write target, not a plain top-level SELECT's output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_as_an_insert_select_source_is_refused() {
    let server = TestServer::start().await;
    seed(&server).await;
    server
        .exec("CREATE COLLECTION t2 (id BIGINT PRIMARY KEY) WITH (engine = 'kv')")
        .await
        .unwrap();

    server
        .expect_error("INSERT INTO t2 (id) SELECT nextval('s') FROM t", "0A000")
        .await;
}
