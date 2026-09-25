// SPDX-License-Identifier: BUSL-1.1

//! Crash injection in the checkpoint's marker→truncate window: a checkpoint
//! writes its marker, then deletes sealed segments below the checkpoint LSN.
//! A crash between them must not let recovery read the marker as proof
//! truncation already ran and skip replay of records still on disk. Crash
//! is injected via `NODEDB_FAILPOINTS` abort right after the marker is
//! durable. Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use crash_harness::CrashHarness;
use std::time::Duration;

/// Short enough that a checkpoint fires within the test's lifetime — the
/// default interval dwarfs it and the window would never open.
const CHECKPOINT_INTERVAL_SECS: &str = "2";

/// Bounded wait for the injected abort. A timeout means no checkpoint ran, so
/// the crash never happened and the test proved nothing.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_rows_survive_a_crash_between_checkpoint_marker_and_truncate() {
    let mut h = CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", CHECKPOINT_INTERVAL_SECS)
        // Checkpoint-manager logs at debug so a timeout shows whether a
        // checkpoint even started and where it stopped.
        .with_env(
            "RUST_LOG",
            "warn,nodedb::control::checkpoint_manager=debug,nodedb::bootstrap=info",
        )
        .with_env(
            "NODEDB_FAILPOINTS",
            "checkpoint::after_marker_before_truncate=abort",
        );
    h.spawn();
    h.wait_ready();

    h.exec("CREATE COLLECTION ckpt_window (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await;
    for i in 0..5 {
        h.exec(&format!(
            "INSERT INTO ckpt_window (k, v) VALUES ('row{i}', 'value{i}')"
        ))
        .await;
    }

    // Sanity before the crash: a later failure is then attributable to
    // recovery rather than to test setup.
    let live = h
        .query_col(
            "SELECT v FROM ckpt_window WHERE k LIKE 'row%' ORDER BY k",
            "v",
        )
        .await;
    assert_eq!(
        live.len(),
        5,
        "rows must read back before the crash: {live:?}"
    );

    // The next checkpoint cycle writes its marker and dies on the spot.
    h.await_self_crash(CRASH_TIMEOUT);

    // Boot 2 runs disarmed, so its checkpoint cycles run to completion and the
    // read below cannot race an abort.
    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();

    let recovered = h
        .query_col(
            "SELECT v FROM ckpt_window WHERE k LIKE 'row%' ORDER BY k",
            "v",
        )
        .await;
    assert_eq!(
        recovered,
        (0..5).map(|i| format!("value{i}")).collect::<Vec<_>>(),
        "acknowledged rows were lost after a crash between the checkpoint marker and truncation \
         — recovery treated the marker as proof of a truncation that never ran (got {recovered:?})"
    );

    // Boot 3 is armed again. A replayed core reports its floor at once, so
    // its first cycle reaches the same window with no new write. The cycle
    // waits for the gateway, so the boot reports ready first.
    h.kill_9();
    h.set_env(
        "NODEDB_FAILPOINTS",
        "checkpoint::after_marker_before_truncate=abort",
    );
    h.reopen();
    h.await_self_crash(CRASH_TIMEOUT);

    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();
    let after = h
        .query_col(
            "SELECT v FROM ckpt_window WHERE k LIKE 'row%' ORDER BY k",
            "v",
        )
        .await;
    assert_eq!(
        after.len(),
        5,
        "rows lost across a second crash between the checkpoint marker and truncation, this time \
         on a server that had already replayed the WAL once: {after:?}"
    );
}
