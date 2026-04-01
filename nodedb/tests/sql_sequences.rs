//! Integration tests for sequences: CREATE/DROP/ALTER/SHOW SEQUENCE, SERIAL.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_drop_sequence() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE order_seq START 1 INCREMENT 1")
        .await
        .unwrap();
    let rows = server.query_text("SHOW SEQUENCES").await.unwrap();
    assert!(!rows.is_empty(), "SHOW SEQUENCES should list the sequence");
    server.exec("DROP SEQUENCE order_seq").await.unwrap();
    server
        .expect_error("DROP SEQUENCE order_seq", "does not exist")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_sequence_restart() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE s1 START 1 INCREMENT 1")
        .await
        .unwrap();
    server
        .exec("ALTER SEQUENCE s1 RESTART WITH 100")
        .await
        .unwrap();
    server.exec("DROP SEQUENCE s1").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequence_options() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE cyc START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 5 CYCLE CACHE 10")
        .await
        .unwrap();
    server.exec("DROP SEQUENCE cyc").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_creates_implicit_sequence() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION orders FIELDS (id SERIAL, name TEXT)")
        .await
        .unwrap();

    let rows = server.query_text("SHOW SEQUENCES").await.unwrap();
    assert!(
        !rows.is_empty(),
        "SERIAL should create an implicit sequence"
    );

    server.exec("DROP COLLECTION orders").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_sequence_if_exists() {
    let server = TestServer::start().await;

    // DROP IF EXISTS on non-existent should not error.
    server
        .exec("DROP SEQUENCE IF EXISTS nonexistent")
        .await
        .unwrap();
}
