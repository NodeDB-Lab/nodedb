// SPDX-License-Identifier: BUSL-1.1

//! A set operation with a branch over a shadowed clone reads every other
//! branch under the session transaction, so it sees the transaction's own
//! staged rows.

use crate::harness::TestServer;

async fn exec_all(server: &TestServer, sqls: &[&str]) {
    for sql in sqls {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn sorted_ids(rows: Vec<Vec<String>>) -> Vec<String> {
    let mut ids: Vec<String> = rows.into_iter().map(|row| row[0].clone()).collect();
    ids.sort_unstable();
    ids
}

/// `UNION` of a shadowed clone collection and a plain collection, read
/// inside `BEGIN`, returns the source-only clone rows and the row the
/// transaction staged into the plain collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn union_over_shadowed_clone_reads_staged_rows_of_plain_branch() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE DATABASE csot_src",
            "USE DATABASE csot_src",
            "CREATE COLLECTION so_items (id TEXT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')",
            "INSERT INTO so_items (id, v) VALUES ('s1', 'a')",
            "INSERT INTO so_items (id, v) VALUES ('s2', 'b')",
            "USE DATABASE default",
            "CLONE DATABASE csot_clone FROM csot_src",
            "USE DATABASE csot_clone",
            "CREATE COLLECTION plain_items (id TEXT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')",
        ],
    )
    .await;

    let union = "SELECT id FROM so_items UNION SELECT id FROM plain_items";

    server.exec("BEGIN").await.expect("BEGIN");
    server
        .exec("INSERT INTO plain_items (id, v) VALUES ('p1', 'c')")
        .await
        .expect("stage a plain row");
    let rows = server
        .query_rows(union)
        .await
        .expect("UNION over a shadowed clone inside a transaction");
    assert_eq!(
        sorted_ids(rows),
        vec!["p1", "s1", "s2"],
        "the plain branch reads the transaction's staged row"
    );
    server.exec("COMMIT").await.expect("COMMIT");

    let rows = server.query_rows(union).await.expect("UNION after COMMIT");
    assert_eq!(
        sorted_ids(rows),
        vec!["p1", "s1", "s2"],
        "the committed row stays visible"
    );
}
