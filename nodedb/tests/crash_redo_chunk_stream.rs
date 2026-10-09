// SPDX-License-Identifier: BUSL-1.1

//! A chunked session redo survives a crash inside its stream.
//!
//! The server boots with `tuning.calvin.max_redo_entry_bytes` at its floor,
//! so a commit of a few hundred kilobytes travels as chunk entries and a
//! final entry. Each test kills the server at a fail point inside the stream
//! and restarts it on the same data directory:
//!
//! - after every chunk is durable and before the final entry applies: boot
//!   rebuilds the stream from its WAL records, and the re-delivered final
//!   entry installs every row once;
//! - after the first chunk is durable: no final entry exists, so no row
//!   installs, and the stream drops at the group's next term. A later
//!   chunked commit installs as usual.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::Duration;

use crash_harness::{CrashHarness, diagnostics};

/// The redo entry limit the server boots with: the smallest the config takes.
const ENTRY_LIMIT: usize = 64 * 1024;

/// Rows one statement inserts.
const ROWS_PER_STATEMENT: usize = 100;

/// Statements one transaction runs. With 400-byte values the redo holds
/// several times `ENTRY_LIMIT`.
const STATEMENTS: usize = 6;

/// Bounded wait for the injected abort.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

/// A harness whose server reads a config file with the lowered entry limit.
fn harness() -> CrashHarness {
    let h = CrashHarness::new();
    let config = h.data_dir().join("redo_chunk.toml");
    std::fs::write(
        &config,
        format!("[tuning.calvin]\nmax_redo_entry_bytes = {ENTRY_LIMIT}\n"),
    )
    .expect("write config file");
    let path = config.to_string_lossy().to_string();
    h.with_env("NODEDB_CONFIG", &path)
}

async fn count(h: &CrashHarness, coll: &str) -> usize {
    let rows = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {coll}"), 0)
        .await;
    assert_eq!(rows.len(), 1, "expected one COUNT(*) row, got {rows:?}");
    rows[0].parse().expect("COUNT(*) is a number")
}

/// Run one large transaction on its own connection. Returns whether COMMIT
/// answered success: a server that aborts mid-commit answers nothing.
async fn commit_large_transaction(h: &CrashHarness, coll: &str, prefix: &str) -> bool {
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
                format!("('{prefix}{id}', '{filler}')")
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

/// Arm `fail_point`, crash the server inside a chunked commit, and restart it
/// without the fail point.
async fn crash_inside_a_chunked_commit(h: &mut CrashHarness, fail_point: &str) {
    h.set_env("NODEDB_FAILPOINTS", &format!("{fail_point}=abort"));
    h.spawn();
    h.wait_ready();
    h.exec(
        "CREATE COLLECTION chunk_crash (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_strict')",
    )
    .await;
    let committed = commit_large_transaction(h, "chunk_crash", "a").await;
    assert!(
        !committed,
        "COMMIT answered success, but the server was armed to abort at `{fail_point}`"
    );
    h.await_self_crash(CRASH_TIMEOUT);
    let log = h.server_log();
    assert!(
        log.contains(&format!("fail_point aborting process: {fail_point}")),
        "the server exited, but not at the armed fail point `{fail_point}`, so this test \
         proves nothing about a crash inside a redo stream.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&log)
    );
    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_before_the_final_entry_applies_installs_the_whole_redo_once() {
    let mut h = harness();
    crash_inside_a_chunked_commit(&mut h, "redo_chunk::before_final_install").await;
    let expected = STATEMENTS * ROWS_PER_STATEMENT;
    assert_eq!(
        count(&h, "chunk_crash").await,
        expected,
        "the committed final entry must install every row of its rebuilt stream once\n{}",
        diagnostics::log_tail_section(&h.server_log())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_between_chunk_entries_installs_nothing_and_frees_the_stream() {
    let mut h = harness();
    crash_inside_a_chunked_commit(&mut h, "redo_chunk::after_chunk_durable").await;
    assert_eq!(
        count(&h, "chunk_crash").await,
        0,
        "a stream with no final entry must install no row"
    );
    assert!(
        commit_large_transaction(&h, "chunk_crash", "b").await,
        "a chunked commit after the crash must succeed\n{}",
        diagnostics::log_tail_section(&h.server_log())
    );
    assert_eq!(
        count(&h, "chunk_crash").await,
        STATEMENTS * ROWS_PER_STATEMENT
    );
}
