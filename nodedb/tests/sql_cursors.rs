//! Integration tests for server-side cursors.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declare_fetch_close() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION items").await.unwrap();
    server
        .exec("INSERT INTO items (id) VALUES ('item-1')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DECLARE my_cursor CURSOR FOR SELECT * FROM items")
        .await
        .unwrap();
    let rows = server.query_text("FETCH 10 FROM my_cursor").await.unwrap();
    assert!(!rows.is_empty(), "cursor should return at least one row");
    server.exec("CLOSE my_cursor").await.unwrap();
    server.exec("COMMIT").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_all_returns_data() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION data").await.unwrap();
    server
        .exec("INSERT INTO data (id) VALUES ('d1')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DECLARE c CURSOR FOR SELECT * FROM data")
        .await
        .unwrap();
    let rows = server.query_text("FETCH ALL FROM c").await.unwrap();
    assert!(!rows.is_empty(), "FETCH ALL should return data");
    server.exec("CLOSE c").await.unwrap();
    server.exec("COMMIT").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_then_fetch() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION nums").await.unwrap();
    server
        .exec("INSERT INTO nums (id) VALUES ('n-1')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DECLARE c CURSOR FOR SELECT * FROM nums")
        .await
        .unwrap();
    // MOVE past all rows, then FETCH should return nothing.
    server.exec("MOVE FORWARD 100 IN c").await.unwrap();
    let rows = server.query_text("FETCH 1 FROM c").await.unwrap();
    assert!(rows.is_empty(), "FETCH after MOVE past end should be empty");
    server.exec("CLOSE c").await.unwrap();
    server.exec("COMMIT").await.unwrap();
}
