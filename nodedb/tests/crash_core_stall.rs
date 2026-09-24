// SPDX-License-Identifier: BUSL-1.1

//! A wedged Data Plane core must turn `/healthz` 503 naming the core, then
//! clear once the core resumes. Unit tests in `control/cluster/core_stall.rs`
//! cover the decision and rendering; nothing there wedges a real core.
//!
//! Lever: the `calvin_static::during_overlay_stage` failpoint runs inside a
//! core's `tick()`. `FailAction::Sleep` freezes its heartbeat without killing
//! the thread.
//!
//! Reaching it needs 2+ distinct vShards, the sole gate `classify_dispatch`
//! puts on `execute_calvin_execute_static`. vShard count is independent of
//! core count, so `NODEDB_DATA_PLANE_CORES=1` still homes both endpoints of a
//! cross-vShard `GRAPH INSERT EDGE` on the one core.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use crash_harness::CrashHarness;
use nodedb_types::id::VShardId;
use std::time::{Duration, Instant};

/// Freeze duration. The monitor samples every 5s, so a stall needs two
/// consecutive frozen samples — 10s worst case. 12s covers it with slack.
const WEDGE_SLEEP_MILLIS: u64 = 12_000;

/// Deadline for `/healthz` to report 503. Covers the freeze plus two sampling
/// windows, with slack for a loaded runner.
const STALL_DETECT_DEADLINE: Duration = Duration::from_secs(60);

/// Static stage calls the wedge write runs on the one core. The edge's two
/// endpoints sit on distinct vShards, and each vShard stages separately, so
/// the failpoint sleeps once per vShard.
const WEDGED_STAGES: u64 = 2;

/// Monitor sampling window.
const SAMPLE_WINDOW_MILLIS: u64 = 5_000;

/// Deadline for `/healthz` to return to 200, counted from the first 503.
/// The 503 can appear as early as two sampling windows into the first sleep.
/// The core then stays frozen for the rest of every stage's sleep. The marker
/// clears one sampling window after the core resumes. The deadline covers the
/// whole freeze plus two sampling windows of slack for a loaded runner.
const RECOVERY_DEADLINE: Duration =
    Duration::from_millis(WEDGE_SLEEP_MILLIS * WEDGED_STAGES + 2 * SAMPLE_WINDOW_MILLIS);

/// Deadline for the write to terminate, so a hung write fails with a clear
/// message instead of running out nextest's kill budget.
const WRITE_COMPLETE_DEADLINE: Duration = Duration::from_secs(30);

/// A `(src, dst)` node-key pair on different vShards. `insert_edge` homes each
/// endpoint with `VShardId::from_key`, so the edge is cross-shard.
fn distinct_vshard_node_keys() -> (String, String) {
    let dst = "core_stall_hub".to_string();
    let vdst = VShardId::from_key(dst.as_bytes()).as_u32();
    for i in 0u32..4096 {
        let src = format!("core_stall_src_{i}");
        if VShardId::from_key(src.as_bytes()).as_u32() != vdst {
            return (src, dst);
        }
    }
    panic!("could not find a node key on a distinct vShard from the hub in 4096 tries");
}

/// Issue one statement on its own connection. `CrashHarness::exec` borrows
/// `&self` and cannot move into a `'static` task.
async fn issue_write(conn_str: String, sql: String) -> Result<(), String> {
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .map_err(|e| e.to_string())?;
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let result = client.simple_query(&sql).await;
    drop(client);
    let _ = conn_handle.await;
    result.map(|_| ()).map_err(|e| match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => e.to_string(),
    })
}

/// Poll `/healthz` until `predicate` accepts the raw response, or panic
/// naming the last response seen at the deadline.
fn poll_healthz_until(
    port: u16,
    deadline: Duration,
    what: &str,
    predicate: impl Fn(&str) -> bool,
) -> String {
    let start = Instant::now();
    let mut last = String::new();
    while start.elapsed() < deadline {
        if let Some(resp) = crash_harness::fetch_healthz(port) {
            if predicate(&resp) {
                return resp;
            }
            last = resp;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    panic!("/healthz did not {what} within {deadline:?}; last response seen:\n{last}");
}

/// A write that wedges the sole Data Plane core must turn `/healthz` 503
/// naming that core, and clear once the core resumes.
#[tokio::test(flavor = "multi_thread")]
async fn wedged_core_flips_healthz_to_503_and_recovers() {
    let mut h = CrashHarness::new();

    // Armed before boot: the child reads failpoints from its startup env.
    h.set_env(
        "NODEDB_FAILPOINTS",
        &format!("calvin_static::during_overlay_stage=sleep({WEDGE_SLEEP_MILLIS})"),
    );
    h.spawn();
    h.wait_ready();

    // `/healthz` reports ready before the Calvin sequencer elects a leader.
    // The probe write is single-shard, so it never reaches the failpoint.
    h.wait_for_calvin_ready(Duration::from_secs(30)).await;

    h.exec("CREATE COLLECTION core_stall_graph").await;

    let (src, dst) = distinct_vshard_node_keys();
    let sql = format!("GRAPH INSERT EDGE IN 'core_stall_graph' FROM '{src}' TO '{dst}' TYPE 'l'");

    // Backgrounded: the call blocks for the whole freeze, and the test polls
    // `/healthz` while it is blocked.
    let write_task = tokio::spawn(issue_write(h.pgwire_conn_str(), sql));

    let stalled_body = poll_healthz_until(
        h.http_port,
        STALL_DETECT_DEADLINE,
        "report 503 for the wedged core",
        |resp| resp.starts_with("HTTP/1.1 503"),
    );
    assert!(
        stalled_body.contains("\"reason\":\"data_plane_core_stalled\""),
        "503 must name the data-plane-core-stall reason, not some other \
         503 condition: {stalled_body}"
    );
    assert!(
        stalled_body.contains("\"stalled_cores\":[0]"),
        "503 body must name core 0 — the harness's sole Data Plane core: \
         {stalled_body}"
    );

    let recovered_body = poll_healthz_until(
        h.http_port,
        RECOVERY_DEADLINE,
        "recover to 200 once the core resumed ticking",
        |resp| resp.starts_with("HTTP/1.1 200"),
    );
    assert!(
        !recovered_body.contains("data_plane_core_stalled"),
        "a recovered /healthz must not still carry the stall reason: {recovered_body}"
    );

    let write_result = tokio::time::timeout(WRITE_COMPLETE_DEADLINE, write_task)
        .await
        .expect("wedge write task did not finish within its own deadline")
        .expect("wedge write task panicked");
    // The write must terminate, not commit. A freeze starves the scheduler's
    // drain of the core, its sequencer inputs are dropped, and the transaction
    // takes a global abort verdict. Aborting is correct; hanging is not.
    if let Err(sqlstate) = &write_result {
        assert!(
            sqlstate.starts_with("40001") || sqlstate.starts_with("XX000"),
            "a wedged cross-shard write may abort, but only with a retryable \
             abort — never a silent or unclassified failure: {sqlstate}"
        );
    }
}

/// Control: the same cross-shard edge insert with nothing wedged. Attributes
/// the wedged run's abort to the freeze rather than to the statement.
#[tokio::test(flavor = "multi_thread")]
async fn cross_shard_edge_insert_commits_without_a_wedge() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready();
    h.wait_for_calvin_ready(Duration::from_secs(30)).await;
    h.exec("CREATE COLLECTION core_stall_control").await;

    let (src, dst) = distinct_vshard_node_keys();
    let sql = format!("GRAPH INSERT EDGE IN 'core_stall_control' FROM '{src}' TO '{dst}' TYPE 'l'");
    let result = issue_write(h.pgwire_conn_str(), sql).await;
    assert!(
        result.is_ok(),
        "an unwedged cross-shard edge insert must commit: {result:?}"
    );
}
