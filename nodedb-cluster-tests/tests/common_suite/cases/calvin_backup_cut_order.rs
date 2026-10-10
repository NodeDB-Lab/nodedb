// SPDX-License-Identifier: BUSL-1.1

//! A database backup taken while cross-shard Calvin transactions commit
//! restores whole transactions: no transaction is in the backup on one data
//! group only.
//!
//! 1. A 3-node cluster with one backup root holds `accts`, whose `balance` is
//!    the materialized sum of `entries.amount`, and `entries`, in distinct
//!    data groups. Every account starts at 100.
//! 2. The fail gate `calvin::before_redo_propose::<entries>` holds the
//!    entries slice of every transaction after its resolve, on the entries
//!    group's leader. Transaction `held` adds 25 to `acc-0`: its balance
//!    slice installs, its entries slice waits.
//! 3. Writers commit more cross-shard transactions on the other accounts.
//!    `BACKUP DATABASE default` starts meanwhile. Its cut's marker follows
//!    `held` in the sequencer log, so the entries group's barrier must follow
//!    `held`'s entries redo, which is still unproposed. The backup stays
//!    unfinished.
//! 4. The test releases the gate. The held redo installs, the barriers
//!    follow, and the writers keep committing while the backup finishes.
//! 5. A fresh cluster restores the backup. `held`'s entry is in it, and every
//!    account's balance is 100 plus the amounts of its restored entries.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use nodedb_types::fail_point::{FailAction, FailGuard};

use super::calvin_multishard_fixture::Fixture;
use super::calvin_replica_content::{run_retrying, strict_session, sum_ddl, vshard_of};
use crate::common::cluster_harness::shared_steps::{db_detail, name_where};
use crate::common::cluster_harness::{TestCluster, wait_for, wait_for_async};

/// Every account, each starting at [`BASE`].
const ACCOUNTS: [&str; 4] = ["acc-0", "acc-1", "acc-2", "acc-3"];

/// The balance every account starts at.
const BASE: i64 = 100;

/// The amount the held transaction adds to `acc-0`.
const HELD_AMOUNT: i64 = 25;

/// How long each step gets.
const STEP_DEADLINE: Duration = Duration::from_secs(30);

/// Pause between two polls of a step.
const STEP_POLL: Duration = Duration::from_millis(20);

/// How long the backup must stay unfinished while the held redo waits.
const PARKED_FOR: Duration = Duration::from_millis(1500);

/// Most transactions one writer commits.
const MAX_WRITES: u32 = 40;

/// The backup object, under the shared backup root.
const OBJECT: &str = "calvin_cut/default.ndbb";

/// Every row `sql` returns through `client`, each column as text.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<String>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}`: {}", db_detail(&e)))
        .into_iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).unwrap_or_default().to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// Commit cross-shard transactions that add to `account`, one at a time,
/// until `stop` or [`MAX_WRITES`]. Returns how many committed.
async fn write_until(
    client: tokio_postgres::Client,
    entries: String,
    account: &'static str,
    stop: Arc<AtomicBool>,
) -> u32 {
    let mut written = 0;
    while !stop.load(Ordering::Relaxed) && written < MAX_WRITES {
        let sql = format!(
            "BEGIN; \
             INSERT INTO {entries} (id, account_id, amount) \
             VALUES ('{account}-w{written}', '{account}', '{}'); \
             COMMIT",
            written % 7 + 1
        );
        run_retrying(&client, "a concurrent Calvin commit", &sql).await;
        written += 1;
    }
    written
}

/// Run `sql` on node 0 of `cluster`. Panics with the server's error.
async fn exec(cluster: &TestCluster, sql: &str) {
    cluster.nodes[0]
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}`: {}", db_detail(&e)));
}

/// Read `accts` and `entries` on node 1 of `cluster`, and check that each
/// account's balance is [`BASE`] plus the amounts of its entries, and that
/// the held transaction's entry is there.
async fn assert_whole_transactions(cluster: &TestCluster, accts: &str, entries: &str) {
    let client = &cluster.nodes[1].client;
    let balances = rows(client, &format!("SELECT id, balance FROM {accts}")).await;
    let restored = rows(
        client,
        &format!("SELECT id, account_id, amount FROM {entries}"),
    )
    .await;
    assert!(
        restored.iter().any(|row| row[0] == "held"),
        "the held transaction was sequenced before the backup's cut, so its entry is in \
         the backup; restored entries: {restored:?}"
    );
    assert_eq!(
        balances.len(),
        ACCOUNTS.len(),
        "every account is restored: {balances:?}"
    );
    for row in &balances {
        let account = &row[0];
        let balance: i64 = row[1]
            .parse()
            .unwrap_or_else(|e| panic!("{account}'s balance '{}': {e}", row[1]));
        let added: i64 = restored
            .iter()
            .filter(|entry| entry[1] == *account)
            .map(|entry| {
                entry[2]
                    .parse::<i64>()
                    .unwrap_or_else(|e| panic!("entry {}'s amount '{}': {e}", entry[0], entry[2]))
            })
            .sum();
        assert_eq!(
            balance,
            BASE + added,
            "{account}: a transaction is in the backup on one data group only. Balance \
             {balance}, base {BASE} plus restored entries {added}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_backup_during_calvin_commits_restores_whole_transactions() {
    let root_dir = tempfile::tempdir().expect("backup root");
    let root = root_dir
        .path()
        .canonicalize()
        .expect("canonical backup root");
    let uri = format!("file://{}/{OBJECT}", root.display());
    let source = TestCluster::spawn_three_with_backup_root(root.clone())
        .await
        .expect("source cluster");
    let fx = Fixture::from_cluster(source).await;
    let probe = &fx.cluster.nodes[0];
    let entries = name_where("cut_order_entries", |name| {
        probe.group_id_for_collection(name).is_some()
    });
    let group = probe
        .group_id_for_collection(&entries)
        .expect("the entries vShard maps to a group");
    let accts = name_where("cut_order_accts", |name| {
        vshard_of(name) != vshard_of(&entries)
            && probe
                .group_id_for_collection(name)
                .is_some_and(|other| other != group)
    });
    let [accts_ddl, entries_ddl, sum] = sum_ddl(&entries, &accts);
    fx.create(&[accts_ddl, entries_ddl]).await;
    fx.cluster
        .exec_ddl_on_any_leader(&sum)
        .await
        .unwrap_or_else(|e| panic!("{sum}: {e}"));
    for collection in [&accts, &entries] {
        fx.wait_group_mounted(collection).await;
    }
    for account in ACCOUNTS {
        run_retrying(
            &fx.cluster.nodes[0].client,
            "seed an account",
            &format!(
                "INSERT INTO {accts} (id, owner, balance) VALUES ('{account}', 'seed', '{BASE}')"
            ),
        )
        .await;
    }
    fx.converge().await;

    let gate_dir = tempfile::tempdir().expect("gate directory");
    let release = gate_dir.path().join("propose");
    let parked = gate_dir.path().join("propose.parked");
    let gate = FailGuard::install(
        &format!("calvin::before_redo_propose::{entries}"),
        FailAction::WaitForFile(release.clone()),
    );

    // The held transaction: its balance slice installs, its entries slice
    // waits at the gate.
    let held_client = strict_session(fx.coordinator()).await;
    let held_sql = format!(
        "BEGIN; \
         INSERT INTO {entries} (id, account_id, amount) VALUES ('held', 'acc-0', '{HELD_AMOUNT}'); \
         COMMIT"
    );
    let held = tokio::spawn(async move {
        held_client
            .simple_query(&held_sql)
            .await
            .map(|_| ())
            .map_err(|e| db_detail(&e))
    });
    wait_for(
        "the entries leader holds the held slice's redo",
        STEP_DEADLINE,
        STEP_POLL,
        || parked.exists(),
    )
    .await;
    let coordinator = fx.coordinator();
    let accts_name = accts.as_str();
    let held_balance = (BASE + HELD_AMOUNT).to_string();
    wait_for_async(
        "the held transaction's balance slice installs",
        STEP_DEADLINE,
        STEP_POLL,
        || {
            let held_balance = held_balance.clone();
            async move {
                rows(
                    &coordinator.client,
                    &format!("SELECT balance FROM {accts_name} WHERE id = 'acc-0'"),
                )
                .await
                    == vec![vec![held_balance]]
            }
        },
    )
    .await;

    // Writers on the other accounts, before and after the backup's cut.
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for (idx, account) in ACCOUNTS.into_iter().skip(1).enumerate() {
        let node = &fx.cluster.nodes[idx % fx.cluster.nodes.len()];
        let client = strict_session(node).await;
        writers.push(tokio::spawn(write_until(
            client,
            entries.clone(),
            account,
            Arc::clone(&stop),
        )));
    }

    let backup_client = strict_session(&fx.cluster.nodes[0]).await;
    let backup_sql = format!("BACKUP DATABASE default TO '{uri}'");
    let backup = tokio::spawn(async move {
        backup_client
            .simple_query(&backup_sql)
            .await
            .map(|_| ())
            .map_err(|e| db_detail(&e))
    });
    tokio::time::sleep(PARKED_FOR).await;
    assert!(
        !backup.is_finished(),
        "the backup finished while a transaction sequenced before its cut was not installed"
    );

    std::fs::write(&release, b"release").expect("release the held redo");
    held.await
        .expect("held transaction task")
        .unwrap_or_else(|e| panic!("the held transaction: {e}"));
    backup
        .await
        .expect("backup task")
        .unwrap_or_else(|e| panic!("BACKUP DATABASE: {e}"));
    stop.store(true, Ordering::Relaxed);
    for writer in writers {
        let written = writer.await.expect("writer task");
        assert!(written > 0, "every writer committed while the backup ran");
    }
    drop(gate);
    assert!(root.join(OBJECT).exists(), "the backup wrote its object");
    fx.cluster.shutdown().await;

    let target = TestCluster::spawn_three_with_backup_root(root.clone())
        .await
        .expect("target cluster");
    exec(&target, &format!("RESTORE DATABASE default FROM '{uri}'")).await;
    target.wait_for_full_apply_convergence(STEP_DEADLINE).await;
    assert_whole_transactions(&target, &accts, &entries).await;
    target.shutdown().await;
}
