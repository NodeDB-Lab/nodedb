// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the `CONVERT COLLECTION` column list.
//!
//! Covers the two properties a column definition must hold:
//! - A parameter list carrying a comma stays one column definition.
//! - A `DEFAULT` clause reaches the created column and fills a later insert.
//!
//! A `DEFAULT` the server cannot evaluate is refused at CONVERT with SQLSTATE
//! `42883`, the SQLSTATE `CREATE COLLECTION` raises for the same clause.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_accepts_a_parameter_list_with_a_space_after_the_comma() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION conv_decimal").await.unwrap();

    server
        .exec(
            "CONVERT COLLECTION conv_decimal TO document_strict \
             (id TEXT PRIMARY KEY, amount DECIMAL(10, 2))",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO conv_decimal (id, amount) VALUES ('d1', 12.50)")
        .await
        .unwrap();

    let rows = server
        .query_text_joined("SELECT amount FROM conv_decimal")
        .await
        .unwrap();
    assert!(
        rows.iter().any(|row| row.contains("12.5")),
        "the parameterized column must be declared and hold its value: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_carries_a_column_default_into_a_later_insert() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION conv_default").await.unwrap();

    server
        .exec(
            "CONVERT COLLECTION conv_default TO document_strict \
             (id TEXT PRIMARY KEY, status TEXT DEFAULT 'pending')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO conv_default (id) VALUES ('c1')")
        .await
        .unwrap();

    let rows = server
        .query_text_joined("SELECT status FROM conv_default")
        .await
        .unwrap();
    assert!(
        rows.iter().any(|row| row.contains("pending")),
        "the converted column must take its DEFAULT: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_refuses_a_default_naming_an_unknown_function() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION conv_bad_default")
        .await
        .unwrap();

    server
        .expect_error(
            "CONVERT COLLECTION conv_bad_default TO document_strict \
             (id TEXT PRIMARY KEY, status TEXT DEFAULT no_such_function())",
            "42883",
        )
        .await;
}
