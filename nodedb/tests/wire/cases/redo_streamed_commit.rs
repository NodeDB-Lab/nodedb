// SPDX-License-Identifier: BUSL-1.1

//! A session COMMIT reads its resolved redo from the owning core in parts and
//! proposes them as chunk entries while it reads.
//!
//! - A transaction whose staging overlay spilled to disk commits a redo of
//!   several chunk entries. Every row reads back with its own value, live
//!   and after a WAL-only restart.
//! - A stream that fails after its first chunk installs nothing. A later
//!   commit installs as usual. Requires `--features failpoints`.

use std::collections::BTreeMap;

use crate::harness::TestServer;

/// The redo entry limit the server boots with: the smallest the config takes.
const ENTRY_LIMIT: usize = 64 * 1024;

/// The staging overlay's RAM threshold, past which it spills to disk.
const OVERLAY_LIMIT: usize = 64 * 1024;

/// Rows one statement inserts.
const ROWS_PER_STATEMENT: usize = 100;

/// Statements one transaction runs. With 400-byte values the redo holds
/// several times `ENTRY_LIMIT`, and the overlay passes `OVERLAY_LIMIT`.
const STATEMENTS: usize = 10;

fn value_of(id: &str) -> String {
    format!("{id}-{}", "x".repeat(400))
}

/// Insert `STATEMENTS * ROWS_PER_STATEMENT` rows with ids from `prefix` in
/// one transaction, and COMMIT it.
async fn commit_large_transaction(
    srv: &TestServer,
    coll: &str,
    prefix: &str,
) -> Result<(), String> {
    srv.exec("BEGIN").await?;
    for statement in 0..STATEMENTS {
        let values: Vec<String> = (0..ROWS_PER_STATEMENT)
            .map(|row| {
                let id = format!("{prefix}{}", statement * ROWS_PER_STATEMENT + row);
                format!("('{id}', '{}')", value_of(&id))
            })
            .collect();
        srv.exec(&format!(
            "INSERT INTO {coll} (id, value) VALUES {}",
            values.join(", ")
        ))
        .await?;
    }
    srv.exec("COMMIT").await
}

/// Every row of `coll`, by id.
async fn rows_of(srv: &TestServer, coll: &str) -> BTreeMap<String, String> {
    srv.query_rows(&format!("SELECT id, value FROM {coll}"))
        .await
        .expect("SELECT rows")
        .into_iter()
        .map(|row| (row[0].clone(), row[1].clone()))
        .collect()
}

/// Assert `coll` holds exactly the rows of the large transactions of
/// `prefixes`, each with its own value.
async fn assert_rows(srv: &TestServer, coll: &str, prefixes: &[&str], when: &str) {
    let rows = rows_of(srv, coll).await;
    assert_eq!(
        rows.len(),
        prefixes.len() * STATEMENTS * ROWS_PER_STATEMENT,
        "{when}: every row installs exactly once"
    );
    for prefix in prefixes {
        for n in 0..STATEMENTS * ROWS_PER_STATEMENT {
            let id = format!("{prefix}{n}");
            assert_eq!(
                rows.get(&id).map(String::as_str),
                Some(value_of(&id).as_str()),
                "{when}: row {id} reads back with its own value"
            );
        }
    }
}

async fn create(srv: &TestServer, coll: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_strict')"
    ))
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spilled_transaction_commits_a_streamed_redo_of_several_chunks() {
    let srv = TestServer::start_with_redo_and_overlay_bytes(ENTRY_LIMIT, OVERLAY_LIMIT).await;
    create(&srv, "streamed_commit").await;

    commit_large_transaction(&srv, "streamed_commit", "a")
        .await
        .expect("a streamed commit succeeds");
    assert_rows(&srv, "streamed_commit", &["a"], "live").await;

    // The finished stream frees its bytes, so a second one commits too.
    commit_large_transaction(&srv, "streamed_commit", "b")
        .await
        .expect("a second streamed commit succeeds");
    assert_rows(&srv, "streamed_commit", &["a", "b"], "live").await;

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, _dir) = TestServer::open_on_path_with_max_redo_entry_bytes(dir, ENTRY_LIMIT).await;
    assert_rows(&srv, "streamed_commit", &["a", "b"], "after restart").await;
}

/// The stream fails before its second chunk. COMMIT fails, the chunk that
/// applied is abandoned, and no row is visible. A commit whose redo fits one
/// entry never reaches the fail point and installs.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_fails_mid_way_leaves_nothing_visible() {
    let srv = TestServer::start_with_max_redo_entry_bytes_and_failpoints(
        ENTRY_LIMIT,
        "redo_stream::before_second_chunk=fail(injected stream failure)",
    )
    .await;
    create(&srv, "streamed_abort").await;

    let committed = commit_large_transaction(&srv, "streamed_abort", "a").await;
    assert!(
        committed.is_err(),
        "a stream that fails after its first chunk fails COMMIT"
    );
    // The failed COMMIT ended the transaction. A ROLLBACK outside one only
    // warns, so its answer is not checked.
    let _ = srv.exec("ROLLBACK").await;
    assert!(
        rows_of(&srv, "streamed_abort").await.is_empty(),
        "no row of a failed stream is visible"
    );

    srv.exec("BEGIN").await.unwrap();
    srv.exec("INSERT INTO streamed_abort (id, value) VALUES ('small', 'one')")
        .await
        .unwrap();
    srv.exec("COMMIT").await.expect("an inline commit succeeds");
    let rows = rows_of(&srv, "streamed_abort").await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows.get("small").map(String::as_str), Some("one"));
}
