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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_add_column_refreshes_strict_schema() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION memories TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                name TEXT NOT NULL)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO memories (id, name) VALUES ('m1', 'first')")
        .await
        .unwrap();

    // `ALTER COLLECTION ... ADD COLUMN` must reach the catalog-generic
    // add-column handler — the same path exercised by `ALTER TABLE` above.
    server
        .exec("ALTER COLLECTION memories ADD COLUMN is_latest BOOL DEFAULT true")
        .await
        .unwrap();

    server
        .exec("INSERT INTO memories (id, name, is_latest) VALUES ('m2', 'second', false)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM memories WHERE id = 'm2'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("m2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_drop_column() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION memories TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                name TEXT NOT NULL, \
                scratch TEXT)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO memories (id, name, scratch) VALUES ('m1', 'first', 'temp')")
        .await
        .unwrap();

    server
        .exec("ALTER COLLECTION memories DROP COLUMN scratch")
        .await
        .unwrap();

    // New inserts without the dropped column still succeed, and old data reads.
    server
        .exec("INSERT INTO memories (id, name) VALUES ('m2', 'second')")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT id FROM memories WHERE id = 'm2'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_rename_column() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION memories TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                name TEXT NOT NULL)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO memories (id, name) VALUES ('m1', 'first')")
        .await
        .unwrap();

    server
        .exec("ALTER COLLECTION memories RENAME COLUMN name TO title")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT title FROM memories WHERE id = 'm1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("\"title\":\"first\""),
        "expected renamed column 'title' = 'first', got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_alter_column_type() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION measurements TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                value INT NOT NULL)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO measurements (id, value) VALUES ('m1', 42)")
        .await
        .unwrap();

    server
        .exec("ALTER COLLECTION measurements ALTER COLUMN value TYPE BIGINT")
        .await
        .unwrap();

    // Re-insert using the widened type.
    server
        .exec("INSERT INTO measurements (id, value) VALUES ('m2', 9999999999)")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT id FROM measurements WHERE id = 'm2'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}
