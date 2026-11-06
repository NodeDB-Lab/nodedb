// SPDX-License-Identifier: BUSL-1.1

//! A transaction that writes an array outside the default database.
//!
//! The record's collection list names the array, and the write note resolves
//! that name against the database. A bare name there fail-stops the core after
//! the record installs. Each test commits, reads the cell, and restarts, so
//! replay meets the record too.

use crate::harness::TestServer;

const DATABASE: &str = "arr_txn_db";

async fn named_array(server: &TestServer, array: &str) {
    server
        .exec(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("create database");
    server
        .exec(&format!("USE DATABASE {DATABASE}"))
        .await
        .expect("use database");
    server
        .exec(&format!(
            "CREATE ARRAY {array} DIMS (x INT64 [0..15]) ATTRS (v INT64) TILE_EXTENTS (16) \
             CELL_ORDER ROW_MAJOR"
        ))
        .await
        .expect("create array");
}

async fn cells(server: &TestServer, array: &str) -> usize {
    server
        .query_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('{array}', '{{x: [0, 15]}}', ['v'], 100)"
        ))
        .await
        .expect("the array reads")
        .len()
}

/// Reopen on the same data directory, with no checkpoint, so replay installs the
/// record.
async fn restart(server: TestServer) -> TestServer {
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;
    server
        .exec(&format!("USE DATABASE {DATABASE}"))
        .await
        .expect("use database after the restart");
    server
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_inserts_an_array_cell_in_a_named_database_and_survives_a_restart() {
    const ARRAY: &str = "arr_txn_put";
    let server = TestServer::start().await;
    named_array(&server, ARRAY).await;

    server.exec("BEGIN").await.expect("begin");
    server
        .exec(&format!("INSERT INTO ARRAY {ARRAY} COORDS (1) VALUES (11)"))
        .await
        .expect("insert");
    server.exec("COMMIT").await.expect("commit");
    assert_eq!(cells(&server, ARRAY).await, 1);

    // The core stays up: a later write commits.
    server
        .exec(&format!("INSERT INTO ARRAY {ARRAY} COORDS (2) VALUES (12)"))
        .await
        .expect("a write after the transaction");
    assert_eq!(cells(&server, ARRAY).await, 2);

    let server = restart(server).await;
    assert_eq!(cells(&server, ARRAY).await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_deletes_an_array_cell_in_a_named_database_and_survives_a_restart() {
    const ARRAY: &str = "arr_txn_del";
    let server = TestServer::start().await;
    named_array(&server, ARRAY).await;
    for coord in [3, 4] {
        server
            .exec(&format!(
                "INSERT INTO ARRAY {ARRAY} COORDS ({coord}) VALUES (5)"
            ))
            .await
            .expect("insert");
    }

    server.exec("BEGIN").await.expect("begin");
    server
        .exec(&format!("DELETE FROM ARRAY {ARRAY} WHERE COORDS IN ((3))"))
        .await
        .expect("delete");
    server.exec("COMMIT").await.expect("commit");
    assert_eq!(cells(&server, ARRAY).await, 1);

    let server = restart(server).await;
    assert_eq!(cells(&server, ARRAY).await, 1);
}
