// SPDX-License-Identifier: BUSL-1.1

//! A `FOR EACH STATEMENT` trigger runs once per statement. The row path of a
//! write fires only `FOR EACH ROW` triggers, so a statement trigger does not
//! also run once per row.

use crate::harness::TestServer;

const STRICT: &str = "(id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')";

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// `orders` and a one-row `counter`. Each statement trigger adds 1 to the
/// counter, so the counter reads the number of times the trigger body ran.
async fn setup(server: &TestServer, trigger_ddl: &str) {
    exec(server, &format!("CREATE COLLECTION orders {STRICT}")).await;
    exec(server, &format!("CREATE COLLECTION counter {STRICT}")).await;
    exec(server, "INSERT INTO counter (id, v) VALUES ('c', 0)").await;
    exec(server, trigger_ddl).await;
}

async fn counter(server: &TestServer) -> String {
    let rows = server
        .query_rows("SELECT v FROM counter WHERE id = 'c'")
        .await
        .unwrap_or_else(|e| panic!("read counter: {e}"));
    rows[0][0].clone()
}

const BUMP: &str = "BEGIN UPDATE counter SET v = v + 1 WHERE id = 'c'; END";

/// One INSERT of three rows runs an AFTER INSERT statement trigger once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_insert_statement_trigger_runs_once_per_statement() {
    let server = TestServer::start().await;
    setup(
        &server,
        &format!("CREATE SYNC TRIGGER t AFTER INSERT ON orders FOR EACH STATEMENT {BUMP}"),
    )
    .await;
    exec(
        &server,
        "INSERT INTO orders (id, v) VALUES ('a', 1), ('b', 2), ('c', 3)",
    )
    .await;
    assert_eq!(counter(&server).await, "1");
}

/// One UPDATE of two rows runs an AFTER UPDATE statement trigger once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_update_statement_trigger_runs_once_per_statement() {
    let server = TestServer::start().await;
    setup(
        &server,
        &format!("CREATE SYNC TRIGGER t AFTER UPDATE ON orders FOR EACH STATEMENT {BUMP}"),
    )
    .await;
    exec(
        &server,
        "INSERT INTO orders (id, v) VALUES ('a', 1), ('b', 2)",
    )
    .await;
    exec(&server, "UPDATE orders SET v = v + 1").await;
    assert_eq!(counter(&server).await, "1");
}

/// One DELETE of two rows runs an AFTER DELETE statement trigger once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_delete_statement_trigger_runs_once_per_statement() {
    let server = TestServer::start().await;
    setup(
        &server,
        &format!("CREATE SYNC TRIGGER t AFTER DELETE ON orders FOR EACH STATEMENT {BUMP}"),
    )
    .await;
    exec(
        &server,
        "INSERT INTO orders (id, v) VALUES ('a', 1), ('b', 2)",
    )
    .await;
    exec(&server, "DELETE FROM orders WHERE v > 0").await;
    assert_eq!(counter(&server).await, "1");
}

/// A BEFORE INSERT statement trigger does not run from the row path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_insert_statement_trigger_does_not_run_per_row() {
    let server = TestServer::start().await;
    setup(
        &server,
        &format!("CREATE TRIGGER t BEFORE INSERT ON orders FOR EACH STATEMENT {BUMP}"),
    )
    .await;
    exec(
        &server,
        "INSERT INTO orders (id, v) VALUES ('a', 1), ('b', 2), ('c', 3)",
    )
    .await;
    assert_eq!(counter(&server).await, "0");
}
