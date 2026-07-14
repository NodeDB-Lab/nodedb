// SPDX-License-Identifier: BUSL-1.1

//! Regression test: a pgwire connection left idle-in-transaction (BEGIN + a
//! staged INSERT, then silence — no COMMIT/ROLLBACK, no disconnect) used to
//! hold its Data-Plane staging overlay indefinitely, because pgwire's
//! `process_socket` owns the connection loop and never times out on its own.
//!
//! The pgwire listener now wraps each connection in an idle/absolute
//! session-timeout watchdog. When a connection is idle-eligible (zero
//! in-flight statements AND silent past the idle window) the watchdog drops
//! the socket future to force-close the connection, then runs the existing
//! `on_connection_end` hook, which reclaims the abandoned transaction's
//! overlay and drops the session entry.
//!
//! Unlike `pgwire_txn_overlay_teardown_reclaim.rs` (which force-closes by
//! dropping the client), this test NEVER disconnects — the client and its
//! connection task stay alive and silent. Reclamation is therefore proof that
//! the idle watchdog itself force-closed the connection. The
//! `active_txn_overlays` gauge on `SystemMetrics` rises when the staged write
//! materializes the overlay and must fall back to baseline once the idle
//! watchdog fires.

mod common;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::pgwire_harness::TestServer;

/// Poll `active_txn_overlays` until `pred` is satisfied or `deadline` elapses.
/// Returns the last observed value so callers can panic with the actual gauge
/// on timeout instead of a bare "timed out".
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
async fn pgwire_idle_in_transaction_overlay_reclaimed_by_watchdog() {
    // 1s idle window: an idle-in-transaction connection is force-closed ~1s
    // after its last statement completes.
    let server = TestServer::start_with_idle_timeout(1).await;

    let baseline = server
        .shared
        .system_metrics
        .as_ref()
        .expect("system_metrics must be wired into the pgwire harness")
        .active_txn_overlays
        .load(Ordering::Relaxed);
    assert_eq!(baseline, 0, "gauge must start at zero: {baseline}");

    // Open a SEPARATE connection the test OWNS (not `server.client`, which the
    // harness owns) so we control its lifetime. tokio-postgres runs the socket
    // in a spawned task; keep both the client and its JoinHandle alive so the
    // connection stays genuinely idle-open (not dropped) for the whole test.
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
            "CREATE COLLECTION pg_idle_overlay_reclaim (id STRING PRIMARY KEY, n INT) \
             WITH (engine='document_schemaless')",
        )
        .await
        .expect("CREATE must succeed");

    client
        .batch_execute("BEGIN")
        .await
        .expect("BEGIN must succeed");
    client
        .batch_execute("INSERT INTO pg_idle_overlay_reclaim (id, n) VALUES ('a', 1)")
        .await
        .expect("in-tx INSERT must succeed");

    // The staged write must have materialized the overlay, bumping the gauge
    // above baseline — confirms there is something to reclaim.
    let after_stage = poll_gauge(&server, Duration::from_secs(5), |v| v > baseline).await;
    assert!(
        after_stage > baseline,
        "staged write must raise active_txn_overlays above baseline {baseline}, got {after_stage}"
    );

    // Do NOTHING: issue no further queries. The connection is now
    // idle-in-transaction. The idle watchdog (1s window) must force-close it
    // and the `on_connection_end` hook must reclaim the overlay, bringing the
    // gauge back to baseline. Deadline generous enough to cover the 1s idle
    // window plus the watchdog's 1s re-check tick plus reclamation.
    let after_idle = poll_gauge(&server, Duration::from_secs(8), |v| v == baseline).await;
    assert_eq!(
        after_idle, baseline,
        "idle-in-transaction overlay must be reclaimed by the idle watchdog, \
         active_txn_overlays still {after_idle} (baseline {baseline})"
    );

    // Keep the client + connection task alive until AFTER the assertion so the
    // connection was genuinely idle-open (not dropped) the whole time — proving
    // the watchdog, not a disconnect, drove reclamation.
    drop(client);
    conn_handle.abort();
    let _ = conn_handle.await;
}
