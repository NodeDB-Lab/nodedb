// SPDX-License-Identifier: BUSL-1.1

//! A Calvin install cut by a crash after its write set is stored applies
//! once.
//!
//! The source collection carries two materialized sums. One target shares
//! the source's vShard, so the source slice's install folds it inline. The
//! other target homes on another vShard, so its balance travels as an
//! `ApplyBalanceDelta` slice. One source `INSERT` therefore commits through
//! the Calvin scheduler, and each slice installs from a stamped redo entry.
//!
//! The process aborts at `core::after_capture::<source>`: the core stored the
//! source install's write set, and its parts are not journalled or synced.
//! Boot journals the install from the stored write set. The stamp of that
//! record marks the position applied, so a redelivered copy of the entry
//! installs nothing, and both balances hold the single-apply total.
//!
//! The checkpoint interval is pushed beyond the test's runtime, so the values
//! read after the crash come from WAL replay alone.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crash_harness::log_fields::same_value;
use crash_harness::vshards::names_on_distinct_vshards;
use crash_harness::{CrashHarness, diagnostics};
use nodedb::control::cluster::calvin::scheduler::{CalvinAppliedLedgers, recover_all_applied};
use nodedb::control::security::catalog::SystemCatalog;
use nodedb::wal::WalManager;
use nodedb_types::id::DatabaseId;

/// A checkpoint between the crash and the reads holds the balances
/// independent of the WAL, and a doubled fold then hides in it.
const NO_CHECKPOINT_SECS: &str = "3600";

/// How long the Calvin sequencer may take to elect its leader after a boot.
const CALVIN_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the process may take to abort once the insert is sent.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a balance may take to move off its seed after the restart.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);

const ACCOUNT: &str = "acc-1";
const SEED: &str = "100";
const AMOUNT: &str = "25";
const SINGLE_APPLY: &str = "125";

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

/// The applied ledgers boot fills from the stopped server's WAL and the
/// state its catalog saved.
fn boot_ledgers(h: &CrashHarness) -> CalvinAppliedLedgers {
    let wal = WalManager::open_for_testing(&h.data_dir().join("wal")).expect("open the WAL");
    let catalog = SystemCatalog::open(&h.data_dir().join("system.redb")).expect("open the catalog");
    let ledgers = CalvinAppliedLedgers::default();
    let recovered =
        recover_all_applied(&wal, &catalog, &|_| None).expect("recover applied positions");
    for (vshard_id, state) in recovered {
        ledgers.install(vshard_id, state.fully_applied_epoch, state.applied_tail);
    }
    ledgers
}

#[tokio::test(flavor = "multi_thread")]
async fn a_calvin_install_cut_after_its_write_set_is_stored_applies_once() {
    let [source, remote] = names_on_distinct_vshards(["cut_entries", "cut_remote"]);
    let local = name_on_vshard("cut_local", vshard_of(&source));
    assert_ne!(vshard_of(&source), vshard_of(&remote));

    let mut h = CrashHarness::new().with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", NO_CHECKPOINT_SECS);
    h.spawn();
    h.wait_ready();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    for target in [&local, &remote] {
        h.exec(&format!(
            "CREATE COLLECTION {target} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await;
    }
    h.exec(&format!(
        "CREATE COLLECTION {source} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
         WITH (engine='document_strict')"
    ))
    .await;
    for target in [&local, &remote] {
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

    // The next boot aborts once a write to the source stored its write set.
    // The abort matches only a journalled write, so boot itself passes it.
    h.kill_9();
    h.set_env(
        "NODEDB_FAILPOINTS",
        &format!("core::after_capture::{source}=abort"),
    );
    h.reopen();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;

    // The insert's client loses its connection with the process, so its
    // result says nothing.
    let conn_str = h.pgwire_conn_str();
    let insert = format!(
        "INSERT INTO {source} (id, account_id, amount) VALUES ('e1', '{ACCOUNT}', '{AMOUNT}')"
    );
    let insert_task = tokio::spawn(async move {
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(connection);
        client
            .simple_query(&insert)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    });
    h.await_self_crash(CRASH_TIMEOUT);
    let marker = format!("fail_point aborting process: core::after_capture::{source}");
    assert!(
        h.server_log().contains(&marker),
        "the process exited, but not after the source install stored its write set.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );
    let _ = insert_task.await;

    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;

    for target in [&local, &remote] {
        let balance = settled_balance(&h, target).await;
        assert!(
            same_value(&balance, SINGLE_APPLY),
            "{target} holds balance {balance}, not the single-apply total {SINGLE_APPLY}.{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
    }
    assert_eq!(
        h.query_col_idx(&format!("SELECT id FROM {source}"), 0)
            .await,
        vec!["e1".to_string()],
        "the source row is stored once"
    );

    // The ledgers a boot fills from this data directory report the
    // transaction's position applied on both of its vShards.
    h.kill_9();
    let ledgers = boot_ledgers(&h);
    let tail = |vshard: u32| -> BTreeSet<(u64, u32)> {
        ledgers
            .get(vshard)
            .map(|ledger| ledger.snapshot().1)
            .unwrap_or_default()
    };
    let shared: Vec<(u64, u32)> = tail(vshard_of(&source))
        .intersection(&tail(vshard_of(&remote)))
        .copied()
        .collect();
    assert_eq!(
        shared.len(),
        1,
        "the cross-shard transaction is the one position applied on both vShards: {shared:?}"
    );
    let (epoch, position) = shared[0];
    for vshard in [vshard_of(&source), vshard_of(&remote)] {
        assert!(
            ledgers
                .get(vshard)
                .is_some_and(|ledger| ledger.is_applied(epoch, position)),
            "the ledger of vShard {vshard} reports ({epoch}, {position}) applied"
        );
    }

    // One more boot replays the WAL and takes every redelivered entry again.
    // A second install of either slice folds a second time, and the reads
    // below then show the doubled total.
    h.reopen();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    for target in [&local, &remote] {
        let read = h
            .query_col_idx(
                &format!("SELECT balance FROM {target} WHERE id = '{ACCOUNT}'"),
                0,
            )
            .await;
        assert!(
            read.len() == 1 && same_value(&read[0], SINGLE_APPLY),
            "after a second restart {target} holds {read:?}, not the single-apply total \
             {SINGLE_APPLY}: a copy of the entry installed again.{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
    }
    assert_eq!(
        h.query_col_idx(&format!("SELECT id FROM {source}"), 0)
            .await,
        vec!["e1".to_string()],
        "after a second restart the source row is stored once"
    );
}
