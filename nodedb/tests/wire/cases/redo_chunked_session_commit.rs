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

/// A chunked commit installs as a marker whose content stays in the WAL's
/// chunk records. Every restart rebuilds it from them: the rows and the
/// values the commit wrote come back after each of two restarts, with later
/// writes on top.
#[tokio::test(flavor = "multi_thread")]
async fn a_chunked_commit_survives_two_restarts() {
    let srv = TestServer::start_with_max_redo_entry_bytes(ENTRY_LIMIT).await;
    srv.exec(
        "CREATE COLLECTION chunked_restart (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();
    commit_large_transaction(&srv, "chunked_restart", "r").await;
    let expected = STATEMENTS * ROWS_PER_STATEMENT;

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, dir) = TestServer::open_on_path_with_max_redo_entry_bytes(dir, ENTRY_LIMIT).await;
    assert_eq!(
        count(&srv, "chunked_restart").await,
        expected,
        "the first restart must rebuild every row of the chunked commit"
    );
    srv.exec("UPDATE chunked_restart SET value = 'later' WHERE id = 'r0'")
        .await
        .unwrap();

    srv.graceful_shutdown().await;
    let (srv, _dir) = TestServer::open_on_path_with_max_redo_entry_bytes(dir, ENTRY_LIMIT).await;
    assert_eq!(
        count(&srv, "chunked_restart").await,
        expected,
        "the second restart must rebuild every row exactly once"
    );
    let first = srv
        .query_text("SELECT value FROM chunked_restart WHERE id = 'r0'")
        .await
        .unwrap();
    assert_eq!(
        first,
        vec!["later".to_string()],
        "the later write stays on top"
    );
    let last = srv
        .query_text(&format!(
            "SELECT value FROM chunked_restart WHERE id = 'r{}'",
            expected - 1
        ))
        .await
        .unwrap();
    assert_eq!(
        last,
        vec!["x".repeat(400)],
        "the commit's own values come back"
    );
}

/// Poll the HTTP change feed of `coll` until it holds `expected` events,
/// then return them.
async fn poll_changes(srv: &TestServer, coll: &str, expected: usize) -> Vec<serde_json::Value> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let body: serde_json::Value = client
            .get(format!(
                "http://127.0.0.1:{}/v1/cdc/{coll}/poll?since_ms=0&limit=10000",
                srv.http_port
            ))
            .send()
            .await
            .expect("poll the change feed")
            .json()
            .await
            .expect("a JSON change page");
        let changes = body["changes"].as_array().cloned().unwrap_or_default();
        if changes.len() >= expected || tokio::time::Instant::now() >= deadline {
            assert_eq!(body["has_more"], serde_json::json!(false), "{body}");
            return changes;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// A chunked commit binds its rows' identities from its content page by page
/// before it installs, and stages its change events page by page once it
/// installed. Its rows read back by key, and its events carry the same shape
/// a small commit's events carry: one event per row, every one at the
/// commit's one position.
#[tokio::test(flavor = "multi_thread")]
async fn a_chunked_commit_binds_its_rows_and_publishes_one_event_per_row() {
    let srv = TestServer::start_with_max_redo_entry_bytes(ENTRY_LIMIT).await;
    srv.exec(
        "CREATE COLLECTION chunked_feed (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();

    srv.exec("BEGIN").await.unwrap();
    srv.exec("INSERT INTO chunked_feed (id, value) VALUES ('s0', 'small'), ('s1', 'small')")
        .await
        .unwrap();
    srv.exec("COMMIT").await.unwrap();
    commit_large_transaction(&srv, "chunked_feed", "b").await;
    let large = STATEMENTS * ROWS_PER_STATEMENT;

    // Every row binds: a read by key finds each one.
    for id in [0, large / 2, large - 1] {
        let rows = srv
            .query_text(&format!(
                "SELECT value FROM chunked_feed WHERE id = 'b{id}'"
            ))
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec!["x".repeat(400)],
            "row b{id} reads back by its key"
        );
    }

    let changes = poll_changes(&srv, "chunked_feed", large + 2).await;
    assert_eq!(
        changes.len(),
        large + 2,
        "one event per row of both commits"
    );
    let (small, chunked): (Vec<_>, Vec<_>) = changes.iter().partition(|change| {
        change["document_id"]
            .as_str()
            .is_some_and(|id| id.starts_with('s'))
    });
    assert_eq!(small.len(), 2);
    assert_eq!(chunked.len(), large);

    let keys = |change: &serde_json::Value| {
        let mut keys: Vec<String> = change
            .as_object()
            .expect("an event object")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    };
    let small_op = small[0]["operation"].clone();
    let small_lsn = small[0]["lsn"].clone();
    let chunked_lsn = chunked[0]["lsn"].clone();
    assert_ne!(
        small_lsn, chunked_lsn,
        "each commit publishes at its own position"
    );
    for change in &chunked {
        assert_eq!(
            keys(change),
            keys(small[0]),
            "the same event shape: {change}"
        );
        assert_eq!(
            change["operation"], small_op,
            "an insert, as the small commit's"
        );
        assert_eq!(
            change["lsn"], chunked_lsn,
            "every row at the commit's one position"
        );
        assert_eq!(change["collection"], serde_json::json!("chunked_feed"));
    }
    let mut ids: Vec<&str> = chunked
        .iter()
        .map(|change| change["document_id"].as_str().expect("a document id"))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), large, "every chunked row publishes exactly once");
}
