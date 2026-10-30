// SPDX-License-Identifier: BUSL-1.1

//! A restart whose WAL replay outlasts the statement deadline still boots.
//!
//! A Data Plane core drains no request until its WAL replay ends, while every
//! request's deadline runs from its dispatch. Boot work that dispatched to the
//! cores during replay timed out against it: the schema rehydrate failed the
//! boot, and a committed Calvin redo reported as not applied halted the
//! scheduler for good. Nothing was checkpointed, so every later boot replayed
//! the same tail and failed the same way.
//!
//! Each test lowers the statement deadline, slows replay past it with the
//! `replay::before_engine_passes` fail point, and checks the restart boots
//! with every row present exactly once. Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::{CrashHarness, diagnostics};

/// The statement deadline the server boots with.
const DEADLINE_SECS: u64 = 5;

/// How long the restart holds each core's replay: past the deadline.
const SLOW_REPLAY_MS: u64 = 8_000;

/// The smallest redo entry limit the config takes, so a commit of a few
/// hundred kilobytes travels as a chunked redo stream.
const ENTRY_LIMIT: usize = 64 * 1024;

/// Rows one statement inserts, and statements the chunked commit runs.
const ROWS_PER_STATEMENT: usize = 100;
const STATEMENTS: usize = 6;

/// Bounded wait for the injected abort.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

fn harness() -> CrashHarness {
    let h = CrashHarness::new();
    let config = h.data_dir().join("slow_replay.toml");
    std::fs::write(
        &config,
        format!(
            "[tuning.network]\ndefault_deadline_secs = {DEADLINE_SECS}\n\n\
             [tuning.calvin]\nmax_redo_entry_bytes = {ENTRY_LIMIT}\n"
        ),
    )
    .expect("write config file");
    let path = config.to_string_lossy().to_string();
    h.with_env("NODEDB_CONFIG", &path)
}

/// Restart with every core's replay held past the statement deadline, and
/// wait for ready.
fn reopen_with_slow_replay(h: &mut CrashHarness) {
    h.set_env(
        "NODEDB_FAILPOINTS",
        &format!("replay::before_engine_passes=sleep({SLOW_REPLAY_MS})"),
    );
    let started = Instant::now();
    h.spawn();
    h.wait_ready_extended();
    // The sleep fail point logs nothing, so the boot's length shows it held
    // the replay. A shorter boot proves nothing about a slow replay.
    assert!(
        started.elapsed() >= Duration::from_millis(SLOW_REPLAY_MS),
        "the restart was ready in {:?}, under the {SLOW_REPLAY_MS}ms replay hold, so the \
         slowed replay never ran",
        started.elapsed()
    );
}

async fn count(h: &CrashHarness, coll: &str) -> usize {
    let rows = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {coll}"), 0)
        .await;
    assert_eq!(rows.len(), 1, "expected one COUNT(*) row, got {rows:?}");
    rows[0].parse().expect("COUNT(*) is a number")
}

fn assert_no_calvin_halt(h: &CrashHarness) {
    let log = h.server_log();
    assert!(
        !log.contains("apply halted"),
        "a Calvin scheduler halted during the slow-replay boot.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&log)
    );
}

/// The schema rehydrate dispatches to every core. It waits for the replay
/// instead of timing out against it.
#[tokio::test(flavor = "multi_thread")]
async fn a_strict_collection_boots_through_a_slow_replay() {
    let mut h = harness();
    h.spawn();
    h.wait_ready_extended();
    h.exec(
        "CREATE COLLECTION slow_strict (id STRING PRIMARY KEY, n INT) \
         WITH (engine='document_strict')",
    )
    .await;
    for id in 0..20 {
        h.exec(&format!(
            "INSERT INTO slow_strict (id, n) VALUES ('r{id}', {id})"
        ))
        .await;
    }
    h.kill_9();

    reopen_with_slow_replay(&mut h);

    assert_eq!(count(&h, "slow_strict").await, 20);
    let typed = h
        .query_col_idx("SELECT n FROM slow_strict WHERE id = 'r7'", 0)
        .await;
    assert_eq!(typed, vec!["7".to_string()]);
    assert_no_calvin_halt(&h);
}

/// Run one large transaction on its own connection. Returns whether COMMIT
/// answered success: a server that aborts mid-commit answers nothing.
async fn commit_large_transaction(h: &CrashHarness, coll: &str) -> bool {
    let (client, connection) = tokio_postgres::connect(&h.pgwire_conn_str(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    let conn = tokio::spawn(async move {
        let _ = connection.await;
    });
    let filler = "x".repeat(400);
    client.simple_query("BEGIN").await.expect("BEGIN");
    for statement in 0..STATEMENTS {
        let values: Vec<String> = (0..ROWS_PER_STATEMENT)
            .map(|row| {
                let id = statement * ROWS_PER_STATEMENT + row;
                format!("('c{id}', '{filler}')")
            })
            .collect();
        client
            .simple_query(&format!(
                "INSERT INTO {coll} (id, value) VALUES {}",
                values.join(", ")
            ))
            .await
            .expect("INSERT inside the transaction");
    }
    let committed = client.simple_query("COMMIT").await.is_ok();
    drop(client);
    let _ = conn.await;
    committed
}

/// A committed redo re-delivered at boot installs once the cores replayed. It
/// never reports as not applied, so no scheduler halts.
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_redo_installs_through_a_slow_replay() {
    let fail_point = "redo_chunk::before_final_install";
    let mut h = harness();
    h.set_env("NODEDB_FAILPOINTS", &format!("{fail_point}=abort"));
    h.spawn();
    h.wait_ready_extended();
    h.exec(
        "CREATE COLLECTION slow_redo (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_strict')",
    )
    .await;
    let committed = commit_large_transaction(&h, "slow_redo").await;
    assert!(
        !committed,
        "COMMIT answered success, but the server was armed to abort at `{fail_point}`"
    );
    h.await_self_crash(CRASH_TIMEOUT);
    let log = h.server_log();
    assert!(
        log.contains(&format!("fail_point aborting process: {fail_point}")),
        "the server exited, but not at `{fail_point}`, so this test proves nothing about a \
         re-delivered redo.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&log)
    );

    reopen_with_slow_replay(&mut h);

    assert_eq!(
        count(&h, "slow_redo").await,
        ROWS_PER_STATEMENT * STATEMENTS,
        "the re-delivered redo must install every row exactly once"
    );
    assert_no_calvin_halt(&h);
}
