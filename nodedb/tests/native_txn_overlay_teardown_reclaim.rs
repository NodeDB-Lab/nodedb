// SPDX-License-Identifier: BUSL-1.1

//! Regression test: an abandoned NATIVE-protocol transaction's Data-Plane
//! staging overlays (`txn_overlays` / `graph_txn_overlays`, keyed by
//! `txn_id`) leaked forever if the connection ended (abrupt disconnect,
//! idle/absolute timeout, or clean EOF) without a `COMMIT`/`ROLLBACK` --
//! nothing dispatched `MetaOp::DropTxnOverlay` for it. `NativeSession::run`
//! now wraps the frame loop and reclaims any still-open transaction's
//! overlay via `lifecycle::run_rollback` on every exit path.
//!
//! This test proves reclamation (rather than just "row not visible", which
//! cannot distinguish reclaimed from leaked-but-invisible) via the
//! `active_txn_overlays` gauge on `SystemMetrics`: it rises when the
//! transaction's first staged write materializes the overlay, and must
//! fall back to baseline once the abandoned connection is torn down.

mod common;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::native_harness::{NativeTestServer, do_handshake, send_sql};

use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;

/// Poll `active_txn_overlays` until `pred` is satisfied or `deadline` elapses.
/// Returns the last observed value; callers assert against it so a timeout
/// panics with the actual value instead of a bare "timed out".
async fn poll_gauge(
    server: &NativeTestServer,
    deadline: Duration,
    pred: impl Fn(u64) -> bool,
) -> u64 {
    let metrics = server
        .shared
        .system_metrics
        .as_ref()
        .expect("system_metrics must be wired into the native harness");
    let start = Instant::now();
    loop {
        let value = metrics.active_txn_overlays.load(Ordering::Relaxed);
        if pred(value) || start.elapsed() >= deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn native_abandoned_txn_overlay_reclaimed_on_teardown() {
    let server = NativeTestServer::start().await;

    let baseline = server
        .shared
        .system_metrics
        .as_ref()
        .expect("system_metrics must be wired into the native harness")
        .active_txn_overlays
        .load(Ordering::Relaxed);
    assert_eq!(baseline, 0, "gauge must start at zero: {baseline}");

    {
        let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
            .await
            .expect("handshake");

        let create_resp = send_sql(
            &mut stream,
            1,
            "CREATE COLLECTION native_txn_overlay_teardown (id STRING PRIMARY KEY, n INT) \
             WITH (engine='document_schemaless')",
        )
        .await;
        assert_ne!(
            create_resp.status,
            ResponseStatus::Error,
            "CREATE must succeed: {create_resp:?}"
        );

        let begin_resp = send_sql(&mut stream, 2, "BEGIN").await;
        assert_ne!(
            begin_resp.status,
            ResponseStatus::Error,
            "BEGIN must succeed: {begin_resp:?}"
        );

        let staged_insert = send_sql(
            &mut stream,
            3,
            "INSERT INTO native_txn_overlay_teardown (id, n) VALUES ('a', 1)",
        )
        .await;
        assert_ne!(
            staged_insert.status,
            ResponseStatus::Error,
            "in-tx INSERT must succeed: {staged_insert:?}"
        );

        // The staged write must have materialized the overlay, bumping the
        // gauge above baseline -- confirms there is something to reclaim.
        let after_stage = poll_gauge(&server, Duration::from_secs(5), |v| v > baseline).await;
        assert!(
            after_stage > baseline,
            "staged write must raise active_txn_overlays above baseline {baseline}, \
             got {after_stage}"
        );

        // Abruptly abandon the connection -- no COMMIT/ROLLBACK. Dropping the
        // raw TcpStream (returned by `do_handshake`) closes the socket,
        // triggering the server-side EOF/error teardown path.
        drop(stream);
    }

    // The abandoned transaction's overlay must be reclaimed on teardown,
    // bringing the gauge back down to baseline.
    let after_teardown = poll_gauge(&server, Duration::from_secs(5), |v| v == baseline).await;
    assert_eq!(
        after_teardown, baseline,
        "abandoned txn overlay must be reclaimed on connection teardown, \
         active_txn_overlays still {after_teardown} (baseline {baseline})"
    );

    // Belt-and-suspenders: a fresh autocommit connection must not see the
    // staged row -- it was never committed.
    let (mut stream2, _ack2) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    let check = send_sql(
        &mut stream2,
        1,
        "SELECT * FROM native_txn_overlay_teardown WHERE id = 'a'",
    )
    .await;
    assert_ne!(
        check.status,
        ResponseStatus::Error,
        "post-teardown SELECT must succeed: {check:?}"
    );
    assert_eq!(
        check.rows.as_ref().map(Vec::len).unwrap_or(0),
        0,
        "the never-committed staged row must not be visible: {check:?}"
    );

    server.shutdown().await;
}
