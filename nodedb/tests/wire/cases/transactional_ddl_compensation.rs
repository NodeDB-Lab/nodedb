// SPDX-License-Identifier: BUSL-1.1

//! Compiled only with `--features failpoints`.
//!
//! Proves the atomic-cutover compensation path: when a same-transaction
//! `CREATE COLLECTION` finalizes to the catalog before its buffered `INSERT`
//! dispatches, and that dispatch then fails, COMMIT must undo the finalized
//! collection rather than leave an orphaned, uncataloged-but-unreachable (or
//! here, cataloged-but-empty-forever) collection behind.

#[cfg(feature = "failpoints")]
use crate::harness::TestServer;

/// Names of every collection visible to the harness connection.
#[cfg(feature = "failpoints")]
async fn collection_names(server: &TestServer) -> Vec<String> {
    server
        .query_rows("SHOW COLLECTIONS")
        .await
        .expect("SHOW COLLECTIONS must succeed")
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect()
}

/// Forces `commit::single_shard_redo_commit` (see
/// `control::server::shared::session::commit::single_shard::commit_redo`)
/// to fail before the redo record is committed, so no WAL or disk state
/// changes from the injected failure itself.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_failure_after_create_compensates_the_finalized_collection() {
    let server = TestServer::start_with_failpoints(
        "commit::single_shard_redo_commit=fail(injected dispatch failure)",
    )
    .await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION txn_ddl_compensate (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO txn_ddl_compensate (id, val) VALUES ('a', 1)")
        .await
        .unwrap();

    let commit_result = server.exec("COMMIT").await;
    assert!(
        commit_result.is_err(),
        "COMMIT must abort when the buffered batch dispatch fails, got: {commit_result:?}"
    );

    // `compensate_finalized_ddl` proposes and awaits the compensating purge
    // synchronously inside `run_commit`, before COMMIT's response returns —
    // no polling wait needed for it to have landed by now.
    let names = collection_names(&server).await;
    assert!(
        !names.contains(&"txn_ddl_compensate".to_string()),
        "the finalized collection must be compensated away after the dispatch \
         failure, saw: {names:?}"
    );

    // The name must be reusable — a fresh CREATE under the same name proves
    // no orphaned catalog row survives. `compensate_finalized` only awaits
    // the fail-closed catalog write (`prepare_purge`, marks the row
    // inactive) synchronously; the row's full removal is a separate async
    // Data-Plane reclaim, so the name can take a moment to free up.
    let mut last_error = String::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match server
            .exec(
                "CREATE COLLECTION txn_ddl_compensate (id TEXT PRIMARY KEY) \
                 WITH (engine='document_strict')",
            )
            .await
        {
            Ok(()) => break,
            Err(e) if std::time::Instant::now() < deadline => last_error = e,
            Err(e) => panic!(
                "the name must be free to reuse after compensation, \
                 last error: {last_error}, final error: {e}"
            ),
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
