// SPDX-License-Identifier: BUSL-1.1

//! A RESTORE re-issues rows without firing AFTER triggers.
//!
//! A trigger fired when its row was first written. The restored row carries
//! the `restore` event source, so neither an ASYNC nor a DEFERRED trigger
//! fires for it again.
//!
//! Each test ends with a sentinel write on another collection, whose own
//! trigger writes a `sentinel` marker. The server runs one core, and the
//! Event Plane handles that core's events in order. So once the sentinel
//! marker lands, any trigger the restore fired has landed too.

use std::time::Duration;

use crate::harness::TestServer;

const TENANT: u64 = 1;

/// How long to wait for an asynchronous trigger to land.
const MARKER_TIMEOUT: Duration = Duration::from_secs(30);

/// Poll `audit` until it holds `marker` at least once. Fails once
/// `MARKER_TIMEOUT` passes.
async fn wait_for_marker(server: &TestServer, marker: &str) {
    let deadline = tokio::time::Instant::now() + MARKER_TIMEOUT;
    loop {
        let rows = markers(server, marker).await;
        if !rows.is_empty() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the '{marker}' marker in audit"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Every `audit` row carrying `marker`.
async fn markers(server: &TestServer, marker: &str) -> Vec<String> {
    server
        .query_text(&format!(
            "SELECT marker FROM audit WHERE marker = '{marker}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("read audit markers: {e}"))
}

async fn exec_all(server: &TestServer, statements: &[&str]) {
    for sql in statements {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

async fn backup_and_restore(server: &TestServer) {
    let backup = super::backup_support::drain_backup(&server.client, TENANT)
        .await
        .expect("BACKUP TENANT");
    super::backup_support::push_restore(&server.client, TENANT, backup)
        .await
        .expect("RESTORE with no write after the backup");
}

/// A KV row restores through the durable re-issue path. Its ASYNC AFTER
/// trigger does not fire again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_kv_row_fires_no_async_after_trigger() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE COLLECTION audit",
            "CREATE COLLECTION kv_src (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')",
            "CREATE COLLECTION kv_sentinel (key STRING PRIMARY KEY, value STRING) \
             WITH (engine='kv')",
            "CREATE TRIGGER kv_src_audit AFTER INSERT OR UPDATE ON kv_src FOR EACH ROW \
             BEGIN INSERT INTO audit (marker) VALUES ('fired'); END;",
            "CREATE TRIGGER kv_sentinel_audit AFTER INSERT ON kv_sentinel FOR EACH ROW \
             BEGIN INSERT INTO audit (marker) VALUES ('sentinel'); END;",
            "INSERT INTO kv_src (key, value) VALUES ('a', 'x')",
        ],
    )
    .await;
    wait_for_marker(&server, "fired").await;

    backup_and_restore(&server).await;

    exec_all(
        &server,
        &["INSERT INTO kv_sentinel (key, value) VALUES ('s', 'x')"],
    )
    .await;
    wait_for_marker(&server, "sentinel").await;

    assert_eq!(
        markers(&server, "fired").await.len(),
        1,
        "only the original insert fires the trigger, the restore does not"
    );
    let restored = server
        .query_text("SELECT key FROM kv_src")
        .await
        .expect("read the restored rows");
    assert_eq!(restored, vec!["a".to_string()]);
}

/// A document row restores through the redo re-issue path. Its DEFERRED
/// AFTER trigger does not fire again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_document_row_fires_no_deferred_after_trigger() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE COLLECTION audit",
            "CREATE COLLECTION doc_src (id TEXT PRIMARY KEY, v INT) \
             WITH (engine='document_strict')",
            "CREATE COLLECTION doc_sentinel (id TEXT PRIMARY KEY, v INT) \
             WITH (engine='document_strict')",
            "CREATE DEFERRED TRIGGER doc_src_audit AFTER INSERT OR UPDATE ON doc_src \
             FOR EACH ROW BEGIN INSERT INTO audit (marker) VALUES ('fired'); END;",
            "CREATE DEFERRED TRIGGER doc_sentinel_audit AFTER INSERT ON doc_sentinel \
             FOR EACH ROW BEGIN INSERT INTO audit (marker) VALUES ('sentinel'); END;",
            "BEGIN",
            "INSERT INTO doc_src (id, v) VALUES ('a', 1)",
            "COMMIT",
        ],
    )
    .await;
    wait_for_marker(&server, "fired").await;

    backup_and_restore(&server).await;

    exec_all(
        &server,
        &[
            "BEGIN",
            "INSERT INTO doc_sentinel (id, v) VALUES ('s', 1)",
            "COMMIT",
        ],
    )
    .await;
    wait_for_marker(&server, "sentinel").await;

    assert_eq!(
        markers(&server, "fired").await.len(),
        1,
        "only the original insert fires the trigger, the restore does not"
    );
    let restored = server
        .query_text("SELECT id FROM doc_src")
        .await
        .expect("read the restored rows");
    assert_eq!(restored, vec!["a".to_string()]);
}
