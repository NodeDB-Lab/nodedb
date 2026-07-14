// SPDX-License-Identifier: BUSL-1.1

//! Regression test: an abandoned PGWIRE-protocol transaction's Data-Plane
//! staging overlays (`txn_overlays` / `graph_txn_overlays`, keyed by
//! `txn_id`) leaked forever if the connection ended (abrupt disconnect,
//! idle/absolute timeout, or clean EOF) without a `COMMIT`/`ROLLBACK` --
//! nothing dispatched `MetaOp::DropTxnOverlay` for it. The pgwire listener
//! now calls `NodeDbPgHandlerFactory::on_connection_end` on every connection
//! exit, which reclaims any still-open transaction's overlay via
//! `lifecycle::run_rollback` and drops the shared session entry.
//!
//! This test proves reclamation (rather than just "row not visible", which
//! cannot distinguish reclaimed from leaked-but-invisible) via the
//! `active_txn_overlays` gauge on `SystemMetrics`: it rises when the
//! transaction's first staged write materializes the overlay, and must fall
//! back to baseline once the abandoned connection is torn down.

mod common;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::pgwire_harness::TestServer;

/// Poll `active_txn_overlays` until `pred` is satisfied or `deadline` elapses.
/// Returns the last observed value; callers assert against it so a timeout
/// panics with the actual value instead of a bare "timed out".
async fn poll_gauge(server: &TestServer, deadline: Duration, pred: impl Fn(u64) -> bool) -> u64 {
    let metrics = server
        .shared
        .system_metrics
        .as_ref()
        .expect("system_metrics must be wired into the pgwire harness");
    let start = Instant::now();
    loop {
        let value = metrics.active_txn_overlays.load(Ordering::Relaxed);
        if pred(value) || start.elapsed() >= deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_abandoned_txn_overlay_reclaimed_on_teardown() {
    let server = TestServer::start().await;

    let baseline = server
        .shared
        .system_metrics
        .as_ref()
        .expect("system_metrics must be wired into the pgwire harness")
        .active_txn_overlays
        .load(Ordering::Relaxed);
    assert_eq!(baseline, 0, "gauge must start at zero: {baseline}");

    // Open a SEPARATE connection the test OWNS (not `server.client`, which the
    // harness owns) so we can close it mid-transaction. tokio-postgres runs the
    // socket in a spawned task; we keep its JoinHandle to abort it later.
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
        server.pg_port
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("owned pgwire connection must connect");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "CREATE COLLECTION pg_txn_overlay_teardown (id STRING PRIMARY KEY, n INT) \
             WITH (engine='document_schemaless')",
        )
        .await
        .expect("CREATE must succeed");

    client
        .batch_execute("BEGIN")
        .await
        .expect("BEGIN must succeed");
    client
        .batch_execute("INSERT INTO pg_txn_overlay_teardown (id, n) VALUES ('a', 1)")
        .await
        .expect("in-tx INSERT must succeed");

    // The staged write must have materialized the overlay, bumping the gauge
    // above baseline -- confirms there is something to reclaim.
    let after_stage = poll_gauge(&server, Duration::from_secs(5), |v| v > baseline).await;
    assert!(
        after_stage > baseline,
        "staged write must raise active_txn_overlays above baseline {baseline}, got {after_stage}"
    );

    // Abruptly abandon the connection -- no COMMIT/ROLLBACK. Dropping the
    // Client resolves the Connection future; aborting its task guarantees the
    // socket is dropped so the server sees EOF and runs `on_connection_end`.
    drop(client);
    conn_handle.abort();
    let _ = conn_handle.await;

    // The abandoned transaction's overlay must be reclaimed on teardown,
    // bringing the gauge back down to baseline.
    let after_teardown = poll_gauge(&server, Duration::from_secs(5), |v| v == baseline).await;
    assert_eq!(
        after_teardown, baseline,
        "abandoned txn overlay must be reclaimed on connection teardown, \
         active_txn_overlays still {after_teardown} (baseline {baseline})"
    );

    // Belt-and-suspenders: the harness-owned autocommit connection must not see
    // the staged row -- it was never committed.
    let rows = server
        .query_text("SELECT n FROM pg_txn_overlay_teardown WHERE id = 'a'")
        .await
        .expect("post-teardown SELECT must succeed");
    assert!(
        rows.is_empty(),
        "the never-committed staged row must not be visible: {rows:?}"
    );
}
