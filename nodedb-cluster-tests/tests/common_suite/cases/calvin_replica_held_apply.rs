// SPDX-License-Identifier: BUSL-1.1

//! A replica whose apply is held while fast-path writes and Calvin
//! transactions hit the same rows converges byte-equal once released, and
//! the hold touches that replica only.
//!
//! A 3-node cluster holds three collections, each on its own vShard:
//! - `accts`: strict, its `balance` is the materialized sum of
//!   `entries.amount` per account,
//! - `entries`: strict, the sum's source,
//! - `notes`: keyed.
//!
//! The fail gate `funnel::before_dispatch::<notes>` is armed for one
//! follower `F` of the `notes` group, through a node-scoped guard. Every
//! node shares the one fail-point registry, so the scope alone keeps the
//! other nodes running.
//!
//! - A probe insert into `notes` commits. Every other replica of the group
//!   installs it, and `F` still stores the `notes` content it had before.
//! - Two sessions on the other two nodes run at once: single-shard
//!   autocommit writes to `accts` and `notes`, and Calvin transactions that
//!   insert `entries` rows and update the same `notes` rows. Both finish
//!   while `F` is held, and `F` still stores its old `notes` content.
//! - The gate releases. Each vShard's documents and index entries become
//!   byte-equal on every replica, read from each node's own cores. Each
//!   account's served balance equals the sum of its served entries, and no
//!   node halted its Calvin apply.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::path::Path;
use std::time::Duration;

use nodedb_test_support::fail_point::{FailAction, FailGuard};

use super::calvin_dependent_read_fixture::assert_no_apply_halt;
use super::calvin_multishard_fixture::{Fixture, keyed_ddl};
use super::calvin_replica_content::{
    VShardContent, assert_balances_match_entries, local_content, names_on_distinct_vshards,
    run_retrying, strict_session, sum_ddl, vshard_of, wait_replicas_agree,
};
use crate::common::cluster_harness::shared_steps::group_members;
use crate::common::cluster_harness::{GroupLeader, TestCluster, TestClusterNode, wait_for_async};

/// Rounds each session runs.
const ROUNDS: usize = 10;

/// Accounts in `accts`, `acc-0` to `acc-{ACCOUNTS - 1}`.
const ACCOUNTS: usize = 3;

/// Seeded `notes` rows, `n0` to `n{NOTES - 1}`.
const NOTES: usize = 4;

/// How long each step around the hold gets.
const STEP_DEADLINE: Duration = Duration::from_secs(30);

/// Pause between two polls of a step.
const STEP_POLL: Duration = Duration::from_millis(50);

/// How long the workload gets to finish while `F` is held.
const HOLD_WINDOW: Duration = Duration::from_secs(90);

/// The three collections of the case.
struct Names {
    accts: String,
    entries: String,
    notes: String,
}

impl Names {
    fn all(&self) -> [&str; 3] {
        [
            self.accts.as_str(),
            self.entries.as_str(),
            self.notes.as_str(),
        ]
    }
}

/// Single-shard autocommit writes: an `accts` column the sum leaves alone,
/// and a `notes` row the Calvin session also updates.
async fn fast_path_writes(client: &tokio_postgres::Client, names: &Names) {
    for k in 0..ROUNDS {
        let account = k % ACCOUNTS;
        let note = k % NOTES;
        let statements = [
            format!(
                "UPDATE {} SET owner = 'fp{k}' WHERE id = 'acc-{account}'",
                names.accts
            ),
            format!(
                "UPDATE {} SET v = 'fp{k}' WHERE id = 'n{note}'",
                names.notes
            ),
        ];
        for sql in &statements {
            run_retrying(client, &format!("fast path round {k}"), sql).await;
        }
    }
}

/// Cross-shard transactions: each inserts an entry, which moves its
/// account's balance on another vShard, and updates a `notes` row.
async fn calvin_writes(client: &tokio_postgres::Client, names: &Names) {
    for k in 0..ROUNDS {
        let account = k % ACCOUNTS;
        let note = k % NOTES;
        let amount = k + 1;
        let txn = format!(
            "BEGIN; \
             INSERT INTO {entries} (id, account_id, amount) \
             VALUES ('e{k}', 'acc-{account}', '{amount}'); \
             UPDATE {notes} SET v = 'cv{k}' WHERE id = 'n{note}'; \
             COMMIT",
            entries = names.entries,
            notes = names.notes,
        );
        run_retrying(client, &format!("calvin round {k}"), &txn).await;
    }
}

/// `node`'s stored content of the vShard `collection` homes to.
async fn vshard_content(node: &TestClusterNode, collection: &str) -> VShardContent {
    local_content(node, &[collection])
        .await
        .remove(&vshard_of(collection))
        .unwrap_or_default()
}

/// The index in `cluster.nodes` of node `node_id`.
fn index_of(cluster: &TestCluster, node_id: u64) -> usize {
    cluster
        .nodes
        .iter()
        .position(|node| node.node_id == node_id)
        .unwrap_or_else(|| panic!("node {node_id} is live"))
}

/// Wait until the held apply parked at the gate, so `parked` exists, and
/// every node of `replicas` stores `collection` content other than `before`.
async fn wait_others_moved_on(
    cluster: &TestCluster,
    replicas: &[usize],
    collection: &str,
    before: &VShardContent,
    parked: &Path,
) {
    wait_for_async(
        "the held follower parks, and every other replica installs the probe",
        STEP_DEADLINE,
        STEP_POLL,
        || async move {
            if !parked.exists() {
                return false;
            }
            for idx in replicas {
                if vshard_content(&cluster.nodes[*idx], collection).await == *before {
                    return false;
                }
            }
            true
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_replica_held_under_fast_path_and_calvin_writes_converges_once_released() {
    let [accts, entries, notes] =
        names_on_distinct_vshards(["held_accts", "held_entries", "held_notes"]);
    let names = Names {
        accts,
        entries,
        notes,
    };
    let [accts_ddl, entries_ddl, sum] = sum_ddl(&names.entries, &names.accts);
    let fx = Fixture::spawn(&[accts_ddl, entries_ddl, keyed_ddl(&names.notes)]).await;
    fx.cluster
        .exec_ddl_on_any_leader(&sum)
        .await
        .unwrap_or_else(|e| panic!("{sum}: {e}"));
    for collection in names.all() {
        fx.wait_group_mounted(collection).await;
    }

    let seeder = &fx.cluster.nodes[0].client;
    for account in 0..ACCOUNTS {
        run_retrying(
            seeder,
            "seed an account",
            &format!(
                "INSERT INTO {} (id, owner, balance) VALUES ('acc-{account}', 'seed', '0')",
                names.accts
            ),
        )
        .await;
    }
    for note in 0..NOTES {
        run_retrying(
            seeder,
            "seed a note",
            &format!(
                "INSERT INTO {} (id, v) VALUES ('n{note}', 'seed')",
                names.notes
            ),
        )
        .await;
    }
    fx.converge().await;
    // Stable leaders: a leadership move onto the held follower would make
    // the workload wait for the hold.
    fx.cluster.wait_for_preferred_leaders(STEP_DEADLINE).await;

    let group = fx.cluster.nodes[0]
        .group_id_for_collection(&names.notes)
        .expect("the notes data group");
    let leader = fx
        .cluster
        .wait_for_group_leader(group, GroupLeader::Any, STEP_DEADLINE)
        .await;
    let held_id = group_members(&fx.cluster.nodes[0], group)
        .into_iter()
        .find(|member| *member != leader)
        .expect("the notes group has a follower");
    let held = index_of(&fx.cluster, held_id);
    let others: Vec<usize> = (0..fx.cluster.nodes.len())
        .filter(|idx| *idx != held)
        .collect();
    let &[fast_node, calvin_node] = others.as_slice() else {
        panic!("a 3-node cluster has two nodes besides the held one, found {others:?}");
    };
    let replicas: Vec<usize> = others
        .iter()
        .copied()
        .filter(|idx| fx.cluster.nodes[*idx].replicates_data_group(group))
        .collect();
    assert!(
        !replicas.is_empty(),
        "the notes group has a replica besides the held follower"
    );
    let before = vshard_content(&fx.cluster.nodes[held], &names.notes).await;

    let gate_dir = tempfile::tempdir().expect("gate directory");
    let release = gate_dir.path().join("release");
    let parked = gate_dir.path().join("release.parked");
    let gate = FailGuard::for_node(
        held_id,
        &format!("funnel::before_dispatch::{}", names.notes),
        FailAction::WaitForFile(release.clone()),
    );

    let fast = &fx.cluster.nodes[fast_node].client;
    run_retrying(
        fast,
        "probe insert",
        &format!("INSERT INTO {} (id, v) VALUES ('probe', 'p')", names.notes),
    )
    .await;
    wait_others_moved_on(&fx.cluster, &replicas, &names.notes, &before, &parked).await;
    let held_after_probe = vshard_content(&fx.cluster.nodes[held], &names.notes).await;

    let calvin = strict_session(&fx.cluster.nodes[calvin_node]).await;
    let mut workload = Box::pin(async {
        tokio::join!(
            fast_path_writes(fast, &names),
            calvin_writes(&calvin, &names),
        )
    });
    let finished_while_held = tokio::time::timeout(HOLD_WINDOW, workload.as_mut())
        .await
        .is_ok();
    let held_after_workload = vshard_content(&fx.cluster.nodes[held], &names.notes).await;

    // Release before any assertion, so a failed one leaves no apply parked.
    std::fs::write(&release, b"release").expect("release the held apply");
    drop(gate);
    if !finished_while_held {
        workload.as_mut().await;
    }
    drop(workload);
    assert!(
        held_after_probe == before && held_after_workload == before,
        "node {held_id} installed a notes write while its apply was held: {} entries before, \
         {} after the probe, {} after the workload",
        before.len(),
        held_after_probe.len(),
        held_after_workload.len()
    );
    assert!(
        finished_while_held,
        "the workload did not finish within {HOLD_WINDOW:?} while only node {held_id}'s \
         notes apply was held"
    );

    fx.converge().await;
    let content = wait_replicas_agree(&fx.cluster, &names.all()).await;
    for collection in names.all() {
        assert!(
            content
                .get(&vshard_of(collection))
                .is_some_and(|stored| !stored.is_empty()),
            "{collection} holds rows after the workload; equal empty replicas prove nothing"
        );
    }
    assert_balances_match_entries(
        &fx.coordinator().client,
        &names.accts,
        &names.entries,
        ACCOUNTS,
    )
    .await;
    assert_no_apply_halt(&fx.cluster);

    fx.cluster.shutdown().await;
}
