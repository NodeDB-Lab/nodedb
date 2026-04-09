//! Integration tests for SQL transaction behavior.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_persists_buffered_writes() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION txn_test TYPE DOCUMENT STRICT (id TEXT PRIMARY KEY, val INT)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO txn_test (id, val) VALUES ('t1', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO txn_test (id, val) VALUES ('t2', 20)")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    let rows = server
        .query_text("SELECT id FROM txn_test WHERE id = 't1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_discards_buffered_write_and_missing_row_is_empty() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION txn_test TYPE DOCUMENT STRICT (id TEXT PRIMARY KEY, val INT)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO txn_test (id, val) VALUES ('t3', 30)")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();

    let rows = server
        .query_text("SELECT id FROM txn_test WHERE id = 't3'")
        .await
        .unwrap();
    assert!(rows.is_empty(), "rolled-back row should not be visible");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_table_add_column_refreshes_strict_schema() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION alter_test TYPE DOCUMENT STRICT (id TEXT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO alter_test (id, name) VALUES ('a1', 'Alice')")
        .await
        .unwrap();

    server
        .exec("ALTER TABLE alter_test ADD COLUMN score INT DEFAULT 0")
        .await
        .unwrap();
    server
        .exec("INSERT INTO alter_test (id, name, score) VALUES ('a3', 'New', 100)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM alter_test WHERE id = 'a3'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("a3"),
        "expected row to include inserted id"
    );
}
