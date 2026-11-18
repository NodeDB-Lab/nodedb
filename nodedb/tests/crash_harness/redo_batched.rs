// SPDX-License-Identifier: BUSL-1.1

//! A chunked commit whose batched install spans several batches, for the
//! crash tests that abort the process inside the install.
//!
//! The server boots with `tuning.calvin.max_redo_entry_bytes` at its floor,
//! so one commit of about 100 KiB travels as a redo stream, and with
//! `tuning.calvin.redo_stream_batch_bytes` at 16 KiB its install runs in
//! several batches. Every source row folds `amount` into one account's
//! materialized sum.

use std::time::Duration;

use super::log_fields::{log_field, same_value};
use super::vshards::{name_on_vshard, vshard_of};
use super::{CrashHarness, diagnostics};

/// The redo entry limit the server boots with: the smallest the config
/// takes, so the commit travels as a stream.
pub const ENTRY_LIMIT: usize = 64 * 1024;

/// The bytes one install batch holds.
pub const BATCH_BYTES: usize = 16 * 1024;

/// Rows one statement inserts.
pub const ROWS_PER_STATEMENT: usize = 100;

/// Statements one transaction runs. With 200-byte pads the redo passes
/// `ENTRY_LIMIT` and spans several batches of `BATCH_BYTES`.
pub const STATEMENTS: usize = 4;

/// Batches the record spans at least, so a cut lands between batches.
pub const MIN_BATCHES: usize = 3;

/// The install logs each durable batch, each flush between batches and
/// each park at this level.
pub const BATCH_LOG: &str =
    "error,nodedb::data::executor::handlers::transaction::redo_apply::batched=debug";
pub const BATCH_DURABLE: &str = "batched install batch durable";

pub const ROWS: usize = ROWS_PER_STATEMENT * STATEMENTS;

pub const NO_CHECKPOINT_SECS: &str = "3600";
pub const CRASH_TIMEOUT: Duration = Duration::from_secs(30);

pub const ACCOUNT: &str = "acc-1";
pub const SEED: u64 = 100;

/// The source and the target that folds it, on one vShard: the commit
/// installs on one core, without Calvin.
pub struct Names {
    pub source: String,
    pub target: String,
}

pub fn names(prefix: &str) -> Names {
    let source = format!("{prefix}_src");
    let vshard = vshard_of(&source);
    Names {
        target: name_on_vshard(&format!("{prefix}_bal"), vshard),
        source,
    }
}

/// A harness whose server reads a config file with the lowered entry limit.
pub fn harness() -> CrashHarness {
    harness_with("")
}

/// [`harness`], with `extra` appended to its config file.
pub fn harness_with(extra: &str) -> CrashHarness {
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
pub async fn create(h: &CrashHarness, names: &Names) {
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
pub async fn commit_large_transaction(h: &CrashHarness, source: &str) -> bool {
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

pub async fn count(h: &CrashHarness, collection: &str) -> usize {
    let rows = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {collection}"), 0)
        .await;
    assert_eq!(rows.len(), 1, "expected one COUNT(*) row, got {rows:?}");
    rows[0].parse().expect("COUNT(*) is a number")
}

/// Every source row reads back once, and the sum holds one fold of each.
pub async fn assert_installed_once(h: &CrashHarness, names: &Names, when: &str) {
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

/// Wait for the server to exit, and check it aborted at `abort_at`, so the
/// run says something about a cut there. Returns the server log.
pub fn await_abort_at(h: &mut CrashHarness, abort_at: &str) -> String {
    h.await_self_crash(CRASH_TIMEOUT);
    let log = h.server_log();
    assert!(
        log.contains(&format!("fail_point aborting process: {abort_at}")),
        "the server exited, but not at `{abort_at}`.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&log)
    );
    log
}

/// Abort the commit's install at `abort_at`, with `armed` fail points
/// besides, then restart twice. Each restart holds every row once and one
/// fold of each.
pub async fn abort_installs_forward_once(prefix: &str, abort_at: &str, armed: &[&str]) {
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
    let log = await_abort_at(&mut h, abort_at);
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
