// SPDX-License-Identifier: BUSL-1.1

//! A session COMMIT whose redo passes `tuning.calvin.max_redo_entry_bytes`
//! travels through its data-group log as chunk entries and a final entry.
//! The commit installs every row once, live and after a WAL-only restart.

use crate::harness::TestServer;

/// The redo entry limit the server boots with: the smallest the config takes.
const ENTRY_LIMIT: usize = 64 * 1024;

/// Rows one statement inserts.
const ROWS_PER_STATEMENT: usize = 100;

/// Statements one transaction runs. With 400-byte values the redo holds
/// several times `ENTRY_LIMIT`.
const STATEMENTS: usize = 6;

async fn count(srv: &TestServer, coll: &str) -> usize {
    let rows = srv
        .query_text(&format!("SELECT COUNT(*) FROM {coll}"))
        .await
        .unwrap();
    rows[0].parse().expect("COUNT(*) is a number")
}

/// Insert `STATEMENTS * ROWS_PER_STATEMENT` rows with ids from `prefix` in
/// one transaction.
async fn commit_large_transaction(srv: &TestServer, coll: &str, prefix: &str) {
    let filler = "x".repeat(400);
    srv.exec("BEGIN").await.unwrap();
    for statement in 0..STATEMENTS {
        let values: Vec<String> = (0..ROWS_PER_STATEMENT)
            .map(|row| {
                let id = statement * ROWS_PER_STATEMENT + row;
                format!("('{prefix}{id}', '{filler}')")
            })
            .collect();
        srv.exec(&format!(
            "INSERT INTO {coll} (id, value) VALUES {}",
            values.join(", ")
        ))
        .await
        .unwrap();
    }
    srv.exec("COMMIT").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chunked_session_commit_installs_once() {
    let srv = TestServer::start_with_max_redo_entry_bytes(ENTRY_LIMIT).await;
    srv.exec(
        "CREATE COLLECTION chunked_commit (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();

    commit_large_transaction(&srv, "chunked_commit", "a").await;
    let expected = STATEMENTS * ROWS_PER_STATEMENT;
    assert_eq!(
        count(&srv, "chunked_commit").await,
        expected,
        "a chunked commit must install every row exactly once"
    );

    // The finished stream frees its bytes and its floor: a second large
    // commit chunks and installs too.
    commit_large_transaction(&srv, "chunked_commit", "b").await;
    assert_eq!(count(&srv, "chunked_commit").await, 2 * expected);

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, _dir) = TestServer::open_on_path_with_max_redo_entry_bytes(dir, ENTRY_LIMIT).await;
    assert_eq!(
        count(&srv, "chunked_commit").await,
        2 * expected,
        "WAL replay of a chunked commit must restore every row exactly once"
    );
}
