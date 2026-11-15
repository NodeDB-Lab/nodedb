// SPDX-License-Identifier: BUSL-1.1

//! A committed redo install cut after its install pass and before its write
//! set is stored folds its materialized sum exactly once.
//!
//! The install folds the source row into its target inside the source row's
//! own store transaction, with the store's durability deferred until the
//! write set is stored. The process aborts at
//! `redo_apply::after_install_pass::<source>`: the fold is not durable, and
//! the record's WAL copy is. Boot judges the record never durable, replay
//! folds it with the sum targets its WAL copy carries, and boot journals the
//! fold rows before the node serves. A second restart replays the record
//! plainly and keeps the single-fold total.
//!
//! The record's WAL copy must be durable before the abort. Each transaction
//! also writes a pause collection that sorts before the source, so the
//! install first sleeps at `redo_apply::after_install_pass::<pause>`. During
//! the sleep the test inserts a row into another collection. Its surrogate
//! binding syncs the WAL, which makes the record durable. The test checks
//! the WAL holds the record before it restarts the server.
//!
//! The checkpoint interval is pushed beyond the test's runtime, so the values
//! read after each restart come from WAL replay alone.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::log_fields::same_value;
use crash_harness::vshards::names_on_distinct_vshards;
use crash_harness::{CrashHarness, diagnostics};
use nodedb::wal::{RedoRecord, WalManager};
use nodedb_types::id::DatabaseId;
use nodedb_wal::record::RecordType;

/// A checkpoint between the crash and the reads holds the balances
/// independent of the WAL, and a doubled fold then hides in it.
const NO_CHECKPOINT_SECS: &str = "3600";

const CALVIN_READY_TIMEOUT: Duration = Duration::from_secs(30);
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the install sleeps after its install pass before it aborts.
const PAUSE_MS: u64 = 8_000;
/// How long the test waits for the install to reach its sleep.
const REACH_PAUSE: Duration = Duration::from_secs(3);

const ACCOUNT: &str = "acc-1";
const SEED: &str = "100";
const AMOUNT: &str = "25";
const SINGLE_FOLD: &str = "125";

fn vshard_of(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// The first name `<prefix>_<n>` that homes on `vshard`.
fn name_on_vshard(prefix: &str, vshard: u32) -> String {
    (0..1u32 << 16)
        .map(|i| format!("{prefix}_{i}"))
        .find(|name| vshard_of(name) == vshard)
        .unwrap_or_else(|| panic!("no {prefix} name on vShard {vshard} in 65536 tries"))
}

/// The collections of one test. `pause` sorts before `source`, so an install
/// that writes both reaches the pause's fail point first.
struct Names {
    source: String,
    pause: String,
    /// Targets folded on the source's core.
    targets: Vec<String>,
    syncer: String,
}

/// The balance of the account in `target`, once it moved off the seed or
/// `SETTLE_TIMEOUT` passed.
async fn settled_balance(h: &CrashHarness, target: &str) -> String {
    let sql = format!("SELECT balance FROM {target} WHERE id = '{ACCOUNT}'");
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        let read = h.query_col_idx(&sql, 0).await;
        assert_eq!(read.len(), 1, "{target} holds one account row: {read:?}");
        if !same_value(&read[0], SEED) || Instant::now() >= deadline {
            return read[0].clone();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Create the collections, the sums and the seeded account rows.
async fn create(h: &CrashHarness, names: &Names) {
    for collection in names.targets.iter().chain([&names.pause, &names.syncer]) {
        h.exec(&format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await;
    }
    h.exec(&format!(
        "CREATE COLLECTION {} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
         WITH (engine='document_strict')",
        names.source
    ))
    .await;
    for target in &names.targets {
        let source = &names.source;
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
}

/// Whether the WAL of the stopped server holds a committed record that
/// carries the source's sum targets.
fn wal_holds_folding_record(h: &CrashHarness, source: &str) -> bool {
    let wal = WalManager::open_for_testing(&h.data_dir().join("wal")).expect("open the WAL");
    wal.replay()
        .expect("read the WAL")
        .iter()
        .filter(|record| {
            RecordType::from_raw(record.logical_record_type()) == Some(RecordType::TransactionRedo)
        })
        .filter_map(|record| RedoRecord::from_bytes(&record.payload).ok())
        .any(|redo| {
            redo.sum_targets
                .iter()
                .any(|targets| targets.collection == source)
        })
}

/// Commit one transaction that writes the pause row and the source row,
/// cut it after its install pass with its WAL copy durable, then restart
/// twice. Every target holds the single-fold total after each restart.
async fn cut_install_folds_once(names: Names) {
    let mut h = CrashHarness::new().with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", NO_CHECKPOINT_SECS);
    h.spawn();
    h.wait_ready();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    create(&h, &names).await;

    // The next boot sleeps once an install of the transaction passed its
    // install pass, then aborts. Boot itself installs no such record.
    h.kill_9();
    h.set_env(
        "NODEDB_FAILPOINTS",
        &format!(
            "redo_apply::after_install_pass::{}=sleep({PAUSE_MS}),\
             redo_apply::after_install_pass::{}=abort",
            names.pause, names.source
        ),
    );
    h.reopen();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;

    // The client loses its connection with the process, so its result says
    // nothing.
    let conn_str = h.pgwire_conn_str();
    let transaction = format!(
        "BEGIN; \
         INSERT INTO {} (id, owner) VALUES ('p1', 'pause'); \
         INSERT INTO {} (id, account_id, amount) VALUES ('e1', '{ACCOUNT}', '{AMOUNT}'); \
         COMMIT",
        names.pause, names.source
    );
    let commit = tokio::spawn(run_detached(conn_str.clone(), transaction));
    tokio::time::sleep(REACH_PAUSE).await;
    // The row's surrogate binding syncs the WAL while the install sleeps.
    let sync = tokio::spawn(run_detached(
        conn_str,
        format!(
            "INSERT INTO {} (id, owner) VALUES ('s1', 'sync')",
            names.syncer
        ),
    ));
    h.await_self_crash(CRASH_TIMEOUT);
    let marker = format!(
        "fail_point aborting process: redo_apply::after_install_pass::{}",
        names.source
    );
    assert!(
        h.server_log().contains(&marker),
        "the process exited, but not after the source install pass.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );
    let _ = commit.await;
    let _ = sync.await;
    assert!(
        wal_holds_folding_record(&h, &names.source),
        "the crash cut the record's WAL copy, so this run proves nothing about a durable \
         record with a reverted fold.{}",
        h.keep_data_dir_note()
    );

    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    for target in &names.targets {
        let balance = settled_balance(&h, target).await;
        assert!(
            same_value(&balance, SINGLE_FOLD),
            "{target} holds balance {balance}, not the single-fold total {SINGLE_FOLD}.{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
    }
    assert_eq!(
        h.query_col_idx(&format!("SELECT id FROM {}", names.source), 0)
            .await,
        vec!["e1".to_string()],
        "the source row is stored once"
    );

    // One more boot replays the record plainly. A second fold shows here as
    // a doubled total.
    h.kill_9();
    h.reopen();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    for target in &names.targets {
        let read = h
            .query_col_idx(
                &format!("SELECT balance FROM {target} WHERE id = '{ACCOUNT}'"),
                0,
            )
            .await;
        assert!(
            read.len() == 1 && same_value(&read[0], SINGLE_FOLD),
            "after a second restart {target} holds {read:?}, not the single-fold total \
             {SINGLE_FOLD}.{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
    }
}

/// Run `sql` on a fresh connection and ignore the result: the process dies
/// under it.
async fn run_detached(conn_str: String, sql: String) -> Result<(), String> {
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .map_err(|e| e.to_string())?;
    tokio::spawn(connection);
    client
        .simple_query(&sql)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// A single-shard commit installs through `ApplyTransactionRedo` without
/// Calvin: the source, its target and the pause row share one vShard.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_shard_install_cut_after_its_install_pass_folds_once() {
    let [source, syncer] = names_on_distinct_vshards(["z_cut_src", "cut_sync"]);
    let vshard = vshard_of(&source);
    let names = Names {
        pause: name_on_vshard("a_cut_pause", vshard),
        targets: vec![name_on_vshard("cut_bal", vshard)],
        source,
        syncer,
    };
    assert!(names.pause < names.source);
    cut_install_folds_once(names).await;
}

/// A Calvin slice installs through the same committed-redo install: the
/// source's second target homes on another vShard, so the transaction
/// commits through the Calvin scheduler. The local target folds inline in
/// the source slice's install, which the crash cuts.
#[tokio::test(flavor = "multi_thread")]
async fn a_calvin_install_cut_after_its_install_pass_folds_once() {
    let [source, remote, syncer] =
        names_on_distinct_vshards(["z_cal_src", "cal_remote", "cal_sync"]);
    let vshard = vshard_of(&source);
    let names = Names {
        pause: name_on_vshard("a_cal_pause", vshard),
        targets: vec![name_on_vshard("cal_local", vshard), remote],
        source,
        syncer,
    };
    assert!(names.pause < names.source);
    cut_install_folds_once(names).await;
}
