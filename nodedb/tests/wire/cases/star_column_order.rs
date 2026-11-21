// SPDX-License-Identifier: BUSL-1.1

//! `SELECT *` and `INSERT ... RETURNING *` on a `document_strict` collection
//! list the columns in declared order. The simple query protocol listed them
//! in name order, and the extended protocol in declared order, so a client
//! that reads by position got the wrong value.

use tokio_postgres::SimpleQueryMessage;

use crate::harness::TestServer;

const DECLARED: [&str; 3] = ["id", "name", "n"];

async fn seeded() -> TestServer {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION sco (id INT PRIMARY KEY, name TEXT, n INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    srv.exec("INSERT INTO sco (id, name, n) VALUES (1, 'a', 10)")
        .await
        .unwrap();
    srv
}

/// The column names of the first row a simple query answers.
async fn simple_columns(srv: &TestServer, sql: &str) -> Vec<String> {
    let msgs = srv
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    for msg in msgs {
        if let SimpleQueryMessage::Row(row) = msg {
            return row.columns().iter().map(|c| c.name().to_string()).collect();
        }
    }
    panic!("{sql}: no row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_star_lists_declared_order_on_the_simple_query() {
    let srv = seeded().await;
    assert_eq!(
        simple_columns(&srv, "SELECT * FROM sco").await,
        DECLARED.map(String::from)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_star_lists_declared_order_on_the_extended_query() {
    let srv = seeded().await;
    let rows = srv
        .client
        .query("SELECT * FROM sco", &[])
        .await
        .expect("extended SELECT *");
    let names: Vec<String> = rows[0]
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    assert_eq!(names, DECLARED.map(String::from));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_star_lists_declared_order_on_the_simple_query() {
    let srv = seeded().await;
    assert_eq!(
        simple_columns(
            &srv,
            "INSERT INTO sco (id, name, n) VALUES (2, 'b', 20) RETURNING *"
        )
        .await,
        DECLARED.map(String::from)
    );
}
