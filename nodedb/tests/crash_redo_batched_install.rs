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

use std::time::Duration;

use crash_harness::log_fields::{log_field, same_value};
use crash_harness::vshards::{name_on_vshard, vshard_of};
use crash_harness::{CrashHarness, diagnostics};

/// The redo entry limit the server boots with: the smallest the config
/// takes, so the commit travels as a stream.
const ENTRY_LIMIT: usize = 64 * 1024;

/// The bytes one install batch holds.
const BATCH_BYTES: usize = 16 * 1024;

/// Rows one statement inserts.
const ROWS_PER_STATEMENT: usize = 100;

/// Statements one transaction runs. With 200-byte pads the redo passes
/// `ENTRY_LIMIT` and spans several batches of `BATCH_BYTES`.
const STATEMENTS: usize = 4;

/// Batches the record spans at least, so a cut lands between batches.
const MIN_BATCHES: usize = 3;

/// The install logs each durable batch at this level.
const BATCH_LOG: &str =
    "error,nodedb::data::executor::handlers::transaction::redo_apply::batched=debug";
const BATCH_DURABLE: &str = "batched install batch durable";

const ROWS: usize = ROWS_PER_STATEMENT * STATEMENTS;

const NO_CHECKPOINT_SECS: &str = "3600";
const CRASH_TIMEOUT: Duration = Duration::from_secs(30);

const ACCOUNT: &str = "acc-1";
const SEED: u64 = 100;

/// The source and the target that folds it, on one vShard: the commit
/// installs on one core, without Calvin.
struct Names {
    source: String,
    target: String,
}

fn names(prefix: &str) -> Names {
    let source = format!("{prefix}_src");
    let vshard = vshard_of(&source);
    Names {
        target: name_on_vshard(&format!("{prefix}_bal"), vshard),
        source,
    }
}

/// A harness whose server reads a config file with the lowered entry limit.
fn harness() -> CrashHarness {
    harness_with("")
}

/// [`harness`], with `extra` appended to its config file.
fn harness_with(extra: &str) -> CrashHarness {
    let h = CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", NO_CHECKPOINT_SECS)
        .with_env("RUST_LOG", BATCH_LOG);
    let config = h.data_dir().join("redo_batched.toml");
    std::fs::write(
        &config,
        format!(
            "[tuning.calvin]\nmax_redo_entry_bytes = {ENTRY_LIMIT}\n\
             redo_stream_batch_bytes = {BATCH_BYTES}\n{extra}"
        ),
    )
    .expect("write config file");
    let path = config.to_string_lossy().to_string();
    h.with_env("NODEDB_CONFIG", &path)
}

/// Create the source, the target, its sum and the seeded account row.
async fn create(h: &CrashHarness, names: &Names) {
    h.exec(&format!(
        "CREATE COLLECTION {} (id TEXT PRIMARY KEY, owner TEXT) WITH (engine='document_strict')",
        names.target
    ))
    .await;
    h.exec(&format!(
        "CREATE COLLECTION {} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT, pad TEXT) \
         WITH (engine='document_strict')",
        names.source
    ))
    .await;
    let (source, target) = (&names.source, &names.target);
    h.exec(&format!(
        "ALTER COLLECTION {target} ADD COLUMN balance TEXT \
         MATERIALIZED_SUM SOURCE {source} \
         ON {source}.account_id = {target}.id VALUE {source}.amount"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {target} (id, owner, balance) VALUES ('{ACCOUNT}', 'alice', '{SEED}')"
    ))
    .await;
}

/// Run the large transaction on its own connection. Returns whether COMMIT
/// answered success: a server that aborts mid-install answers nothing.
async fn commit_large_transaction(h: &CrashHarness, source: &str) -> bool {
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
    for statement in 0..STATEMENTS {
        if !committed {
            break;
        }
        let values: Vec<String> = (0..ROWS_PER_STATEMENT)
            .map(|row| {
                let id = statement * ROWS_PER_STATEMENT + row;
                format!("('e{id}', '{ACCOUNT}', '1', '{pad}')")
            })
            .collect();
        committed = client
            .simple_query(&format!(
                "INSERT INTO {source} (id, account_id, amount, pad) VALUES {}",
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

async fn count(h: &CrashHarness, collection: &str) -> usize {
    let rows = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {collection}"), 0)
        .await;
    assert_eq!(rows.len(), 1, "expected one COUNT(*) row, got {rows:?}");
    rows[0].parse().expect("COUNT(*) is a number")
}

/// Every source row reads back once, and the sum holds one fold of each.
async fn assert_installed_once(h: &CrashHarness, names: &Names, when: &str) {
    let rows = count(h, &names.source).await;
    assert_eq!(
        rows,
        ROWS,
        "{when}: the source holds {rows} rows, not every row of the commit once.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );
    let balance = h
        .query_col_idx(
            &format!(
                "SELECT balance FROM {} WHERE id = '{ACCOUNT}'",
                names.target
            ),
            0,
        )
        .await;
    let expected = (SEED + ROWS as u64).to_string();
    assert!(
        balance.len() == 1 && same_value(&balance[0], &expected),
        "{when}: {} holds {balance:?}, not one fold of every row ({expected}).{}\n{}",
        names.target,
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );
}

/// Abort the commit's install at `abort_at`, with `armed` fail points
/// besides, then restart twice. Each restart holds every row once and one
/// fold of each.
async fn abort_installs_forward_once(prefix: &str, abort_at: &str, armed: &[&str]) {
    let names = names(prefix);
    let mut h = harness();
    h.spawn();
    h.wait_ready();
    create(&h, &names).await;

    let mut spec: Vec<String> = armed.iter().map(|entry| entry.to_string()).collect();
    spec.push(format!("{abort_at}=abort"));
    h.kill_9();
    h.set_env("NODEDB_FAILPOINTS", &spec.join(","));
    h.reopen();

    let committed = commit_large_transaction(&h, &names.source).await;
    assert!(
        !committed,
        "COMMIT answered success, but the server was armed to abort at `{abort_at}`"
    );
    h.await_self_crash(CRASH_TIMEOUT);
    let log = h.server_log();
    assert!(
        log.contains(&format!("fail_point aborting process: {abort_at}")),
        "the server exited, but not at `{abort_at}`, so this run proves nothing about a \
         batched install cut there.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&log)
    );
    // Every batch the install made durable logs its index. Batches with no
    // op are never made durable, so the count is a floor.
    let mut batches = log_field(&log, BATCH_DURABLE, "batch");
    batches.sort_unstable();
    batches.dedup();
    assert!(
        batches.len() >= MIN_BATCHES,
        "the install made batches {batches:?} durable, fewer than {MIN_BATCHES}, so the cut \
         at `{abort_at}` proves nothing about a record across batches.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&log)
    );

    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();
    assert_installed_once(&h, &names, "after the first restart").await;

    // A second boot replays the record again. A second fold shows here as
    // a raised total.
    h.kill_9();
    h.reopen();
    assert_installed_once(&h, &names, "after a second restart").await;
}

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
    h.await_self_crash(CRASH_TIMEOUT);
    let log = h.server_log();
    assert!(
        log.contains(&format!("fail_point aborting process: {ABORT_AT}")),
        "the server exited, but not at `{ABORT_AT}`.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&log)
    );
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
