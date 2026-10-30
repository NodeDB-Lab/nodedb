// SPDX-License-Identifier: BUSL-1.1

//! Fail-stop-on-readiness-gate-failure boot regression.
//!
//! When a boot-path readiness gate fails, the process must exit non-zero, the
//! same contract the graceful shutdown path already guarantees via the
//! explicit `std::process::exit(0)` in `main.rs`. The boot-ERROR path has no
//! such explicit exit: `await_cluster_ready(...).await?` propagates `Err` out
//! of `server_main` and relies on `main` returning, which is not guaranteed to
//! terminate the process — Data Plane cores run on plain `std::thread`, so a
//! panicked core thread does not unwind the Tokio runtime, and any background
//! Tokio task still running (health loop, scheduler, etc.) keeps the runtime
//! alive when `main` drops it on the way out. The observed symptom is a
//! process that stays alive with every listener port closed, requiring an
//! external force-stop to recover.
//!
//! This test forces exactly that: a WAL-replay fail point panics the sole
//! Data Plane core's thread before it signals replay completion, which drops
//! the `replay_done` sender. `bootstrap/cluster_ready.rs` waits for every
//! core's replay before any boot step that dispatches to a core, turns the
//! dropped sender into "data plane core exited before signalling WAL replay
//! completion", and fails the `data-plane-replay` gate, which must fail-stop
//! the whole process.
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use crash_harness::CrashHarness;
use std::time::Duration;

/// Bound on how long a fail-stopped boot is given to actually exit. The
/// replay wait sees the dropped sender at once, so this bounds only the boot
/// up to that wait and the exit after it.
const BOOT_FAILURE_TIMEOUT: Duration = Duration::from_secs(60);

/// A readiness-gate failure during WAL replay must fail-stop the process
/// rather than leave it alive with every listener port closed.
#[tokio::test(flavor = "multi_thread")]
async fn replay_readiness_gate_failure_fails_the_boot() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready();

    // One acknowledged write is enough to make the WAL non-empty: replay is a
    // no-op on an empty WAL, so the fail point below would never fire without
    // it (`replay_all_wal` returns early when `records.is_empty()`).
    h.exec(
        "CREATE COLLECTION replay_fail_stop (id TEXT PRIMARY KEY, v INT) \
         WITH (engine='document_strict')",
    )
    .await;
    h.exec("INSERT INTO replay_fail_stop (id, v) VALUES ('a', 1)")
        .await;

    // Hard kill: the write must still be sitting in the WAL, unreachable by
    // any other means, so the next boot genuinely has to replay it.
    h.kill_9();

    // Armed with `=panic`, not `=abort`: `abort()` kills the process
    // immediately and would pass trivially, proving nothing about the
    // readiness-gate path. `panic` unwinds only the core thread that hits the
    // fail point, leaving the Control Plane to reach — and fail — the
    // readiness gate, which is the scenario under test.
    h.set_env("NODEDB_FAILPOINTS", "replay::before_engine_passes=panic");

    // The server must never become `/healthz`-ready, and must exit non-zero
    // within the timeout rather than lingering with its ports closed.
    h.spawn_expect_boot_failure(BOOT_FAILURE_TIMEOUT);

    // `spawn_expect_boot_failure` alone only proves SOME boot condition
    // fail-stopped the process — pin the operator-visible diagnosis so a
    // future regression that fail-stops for an unrelated reason does not
    // pass this test by accident.
    let log = h.server_log();
    assert!(
        log.contains("nodedb: process panic intercepted"),
        "the server exited during the replay boot, but not via a panic — the armed fail point \
         `replay::before_engine_passes` never fired, so this test proves NOTHING about the \
         readiness-gate fail-stop path.\nServer output:\n{log}"
    );
    assert!(
        log.contains("StartupError: server failed to start — aborting startup"),
        "the server panicked and exited, but without the fail-stop diagnosis — an operator \
         would see a dead process and no stated reason.\nServer output:\n{log}"
    );
    // The dead core is reported by the replay wait: no boot step dispatches
    // to a core before every core signalled its replay.
    assert!(
        log.contains("data plane core exited before signalling WAL replay completion"),
        "the server fail-stopped, but not through the replay wait this test targets — its \
         dead-core error is missing.\nServer output:\n{log}"
    );
}
