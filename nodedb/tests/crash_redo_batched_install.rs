// SPDX-License-Identifier: BUSL-1.1

//! A chunked commit's batched install survives a process abort inside it.
//!
//! The server boots with `tuning.calvin.max_redo_entry_bytes` at its floor,
//! so one commit of about 100 KiB travels as a redo stream, and with
//! `tuning.calvin.redo_stream_batch_bytes` at 16 KiB its install runs in
//! several batches. Each test reads the install's durable-batch log lines to
//! check the record spans at least three batches. Every source row folds
//! `amount` into one account's materialized sum. Each test aborts the
//! process at a fail point of the batched install and restarts it on the
//! same data directory:
//!
//! - after the third batch is durable;
//! - after every batch is durable, before the write set is stored;
//! - in a rollback, after its first durable checkpoint;
//! - in a rollback, after every entry reversed and before its durable end.
//!
//! Boot completes the record forward from the durable state. Every source
//! row reads back once, the sum holds one fold of every row, and a second
//! restart keeps both.
//!
//! A timeseries commit installs at a memtable budget of one byte, so the
//! install flushes after every batch. Its test aborts after the third
//! batch's flush, before that batch's cursor is durable. Every row reads
//! back once after each of two restarts.
//!
//! The checkpoint interval is pushed beyond the test's runtime, so the
//! values read after each restart come from WAL replay and the durable
//! stores alone.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use crash_harness::log_fields::log_field;
use crash_harness::redo_batched::{
    MIN_BATCHES, abort_installs_forward_once, await_abort_at, count, harness_with,
};
use crash_harness::{CrashHarness, diagnostics};

/// The failure that turns the install into a rollback: the document arm's
/// last batch fails once it wrote.
const LATE_FAILURE: &str = "redo_apply::batched::install_batch_fails::Document=fail(late batch)";

#[tokio::test(flavor = "multi_thread")]
async fn abort_after_a_durable_batch_folds_once() {
    abort_installs_forward_once(
        "bi_batch",
        "redo_apply::batched::after_batch_durable::Document::2",
        &[],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_before_the_final_capture_folds_once() {
    abort_installs_forward_once(
        "bi_capture",
        "redo_apply::batched::before_final_capture",
        &[],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_mid_rollback_installs_forward_once() {
    abort_installs_forward_once(
        "bi_rollback",
        "redo_apply::batched::rollback_after_checkpoint",
        &[LATE_FAILURE],
    )
    .await;
}

// ── Timeseries flushes between batches ───────────────────────────────────

/// The timeseries config the partial-flush test boots with: every memtable
/// that holds a row is over its budget.
const TS_BUDGET_CONFIG: &str = "[tuning.timeseries]\nmemtable_budget_bytes = 1\n";

/// Statements the timeseries transaction runs, and rows each inserts. With
/// 200-byte pads the redo passes `ENTRY_LIMIT` and spans several batches.
const TS_STATEMENTS: usize = 8;
const TS_ROWS_PER_STATEMENT: usize = 50;
const TS_ROWS: usize = TS_STATEMENTS * TS_ROWS_PER_STATEMENT;

/// The install logs each flush between batches at debug level.
const PARTIAL_FLUSH: &str = "batched install flushed between batches";

/// Run the timeseries transaction on its own connection. Returns whether
/// COMMIT answered success.
async fn commit_large_ts_transaction(h: &CrashHarness, collection: &str) -> bool {
    let Ok((client, connection)) =
        tokio_postgres::connect(&h.pgwire_conn_str(), tokio_postgres::NoTls).await
    else {
        return false;
    };
    let conn = tokio::spawn(async move {
        let _ = connection.await;
    });
    let pad = "x".repeat(200);
    let mut committed = client.simple_query("BEGIN").await.is_ok();
    for statement in 0..TS_STATEMENTS {
        if !committed {
            break;
        }
        let values: Vec<String> = (0..TS_ROWS_PER_STATEMENT)
            .map(|row| {
                let id = statement * TS_ROWS_PER_STATEMENT + row;
                let ts = 1_700_000_000_000u64 + id as u64;
                format!("('e{id}', {ts}, 1.0, '{pad}')")
            })
            .collect();
        committed = client
            .simple_query(&format!(
                "INSERT INTO {collection} (id, ts, value, pad) VALUES {}",
                values.join(", ")
            ))
            .await
            .is_ok();
    }
    let committed = committed && client.simple_query("COMMIT").await.is_ok();
    drop(client);
    let _ = conn.await;
    committed
}

/// Every row of the timeseries commit reads back once.
async fn assert_ts_rows_once(h: &CrashHarness, collection: &str, when: &str) {
    let rows = count(h, collection).await;
    assert_eq!(
        rows,
        TS_ROWS,
        "{when}: {collection} holds {rows} rows, not every row of the commit once.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );
}

/// The install flushed the timeseries memtable after batches 0, 1 and 2,
/// and the process died after the third flush, before batch 2's cursor was
/// durable. The partitions name the record's ops below each batch's end,
/// so boot replays each row once, and a second boot again.
#[tokio::test(flavor = "multi_thread")]
async fn abort_after_a_partial_ts_flush_appends_each_row_once() {
    const ABORT_AT: &str = "redo_apply::batched::after_partial_flush::Timeseries::3";
    let collection = "bi_ts_flush";
    let mut h = harness_with(TS_BUDGET_CONFIG);
    h.spawn();
    h.wait_ready();
    h.exec(&format!(
        "CREATE COLLECTION {collection} \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT, pad TEXT) \
         WITH (engine='timeseries')"
    ))
    .await;

    h.kill_9();
    h.set_env("NODEDB_FAILPOINTS", &format!("{ABORT_AT}=abort"));
    h.reopen();
    let committed = commit_large_ts_transaction(&h, collection).await;
    assert!(
        !committed,
        "COMMIT answered success, but the server was armed to abort at `{ABORT_AT}`"
    );
    let log = await_abort_at(&mut h, ABORT_AT);
    let mut flushed = log_field(&log, PARTIAL_FLUSH, "batch");
    flushed.sort_unstable();
    flushed.dedup();
    assert!(
        flushed.len() >= MIN_BATCHES,
        "the install flushed after batches {flushed:?}, fewer than {MIN_BATCHES}.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&log)
    );

    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();
    assert_ts_rows_once(&h, collection, "after the first restart").await;
    h.kill_9();
    h.reopen();
    assert_ts_rows_once(&h, collection, "after a second restart").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_before_the_rollback_ends_leaves_no_partial_rows() {
    abort_installs_forward_once(
        "bi_rollback_end",
        "redo_apply::batched::before_rollback_end",
        &[LATE_FAILURE],
    )
    .await;
}
