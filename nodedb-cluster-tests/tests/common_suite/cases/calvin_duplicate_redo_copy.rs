// SPDX-License-Identifier: BUSL-1.1

//! A Calvin slice whose redo installed, while its scheduler has not heard
//! of the install, gets its redo proposed again. The second copy also
//! commits, and installs nothing: every replica applies each slice once.
//!
//! 1. A 3-node cluster holds `accts`, whose `balance` is the materialized
//!    sum of `entries.amount`, and `entries`, on distinct vShards. `acc-0`
//!    starts at 100.
//! 2. The fail gates `calvin::before_redo_applied_push::<entries>` and
//!    `calvin::before_redo_applied_push::<accts>` withhold the `RedoApplied`
//!    event of each slice's install. The apply concludes, so each data group
//!    applies past the redo with no install heard, and the leader's stall
//!    tick proposes the redo again.
//! 3. Once every node skipped a second copy, the test releases the gates.
//! 4. The client's COMMIT succeeds once. `acc-0` reads 140, not the 180 of a
//!    second install. Every node installed each slice it replicates once,
//!    and every replica holds the same content.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb_types::fail_point::{FailAction, FailGuard};

use super::calvin_multishard_fixture::{Fixture, tags};
use super::calvin_replica_content::{
    first_column, names_on_distinct_vshards, run_retrying, sum_ddl, wait_replicas_agree,
};
use crate::common::cluster_harness::{TestClusterNode, wait_for};

/// How long the test waits for the first install, and for every node to
/// skip a second copy.
const HOLD_DEADLINE: Duration = Duration::from_secs(30);

/// How long the replicas get to report their installs after the release.
const INSTALL_DEADLINE: Duration = Duration::from_secs(15);

/// Committed Calvin slices whose stamped redo installed on `node`.
fn installs(node: &TestClusterNode) -> u64 {
    node.shared
        .calvin
        .counters
        .write_versions_recorded
        .load(Ordering::Relaxed)
}

/// Stamped redo copies `node` applied that installed nothing.
fn skipped_copies(node: &TestClusterNode) -> u64 {
    node.shared
        .calvin
        .counters
        .redo_copies_skipped
        .load(Ordering::Relaxed)
}

/// How many of `collections` `node` replicates: each is one slice of the
/// transaction.
fn slices_on(node: &TestClusterNode, collections: &[&str]) -> u64 {
    let hosted = collections
        .iter()
        .filter(|collection| {
            node.group_id_for_collection(collection)
                .is_some_and(|group| node.replicates_data_group(group))
        })
        .count();
    u64::try_from(hosted).unwrap_or(u64::MAX)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_second_redo_copy_that_commits_installs_nothing() {
    let [accts, entries] = names_on_distinct_vshards(["dup_redo_accts", "dup_redo_entries"]);
    let collections = [accts.as_str(), entries.as_str()];
    let [accts_ddl, entries_ddl, sum] = sum_ddl(&entries, &accts);
    let fx = Fixture::spawn(&[accts_ddl, entries_ddl]).await;
    fx.cluster
        .exec_ddl_on_any_leader(&sum)
        .await
        .unwrap_or_else(|e| panic!("{sum}: {e}"));
    for collection in collections {
        fx.wait_group_mounted(collection).await;
    }
    run_retrying(
        &fx.cluster.nodes[0].client,
        "seed the account",
        &format!("INSERT INTO {accts} (id, owner, balance) VALUES ('acc-0', 'seed', '100')"),
    )
    .await;
    fx.converge().await;

    let installs_before: Vec<u64> = fx.cluster.nodes.iter().map(installs).collect();
    let skipped_before: Vec<u64> = fx.cluster.nodes.iter().map(skipped_copies).collect();
    let mut session = fx.raw().await;
    let txn = format!(
        "BEGIN; \
         INSERT INTO {entries} (id, account_id, amount) VALUES ('e1', 'acc-0', '25'); \
         INSERT INTO {entries} (id, account_id, amount) VALUES ('e2', 'acc-0', '15'); \
         COMMIT"
    );

    let gate_dir = tempfile::tempdir().expect("gate directory");
    let release = gate_dir.path().join("release");
    let parked = gate_dir.path().join("release.parked");
    let committed = {
        let _gates = collections.map(|collection| {
            FailGuard::install(
                &format!("calvin::before_redo_applied_push::{collection}"),
                FailAction::WaitForFile(release.clone()),
            )
        });
        let commit = tags(&mut session, &txn);
        let copy_again = async {
            wait_for(
                "a replica installs a slice and withholds its RedoApplied",
                HOLD_DEADLINE,
                Duration::from_millis(20),
                || parked.exists(),
            )
            .await;
            wait_for(
                "every node applies a second redo copy that installs nothing",
                HOLD_DEADLINE,
                Duration::from_millis(50),
                || {
                    fx.cluster
                        .nodes
                        .iter()
                        .zip(&skipped_before)
                        .all(|(node, before)| skipped_copies(node) > *before)
                },
            )
            .await;
            std::fs::write(&release, b"release").expect("release the withheld events");
        };
        let (committed, ()) = tokio::join!(commit, copy_again);
        committed
    };

    assert_eq!(
        committed
            .iter()
            .filter(|tag| tag.as_str() == "COMMIT")
            .count(),
        1,
        "the client's COMMIT answers once, found tags {committed:?}"
    );
    assert_eq!(
        committed.last().map(String::as_str),
        Some("COMMIT"),
        "the transaction ends committed, found tags {committed:?}"
    );

    fx.converge().await;
    wait_for(
        "every node reports the install of each slice it replicates",
        INSTALL_DEADLINE,
        Duration::from_millis(50),
        || {
            fx.cluster
                .nodes
                .iter()
                .zip(&installs_before)
                .all(|(node, before)| installs(node) - before >= slices_on(node, &collections))
        },
    )
    .await;
    for (node, before) in fx.cluster.nodes.iter().zip(&installs_before) {
        assert_eq!(
            installs(node) - before,
            slices_on(node, &collections),
            "node {} installs each slice it replicates once, whatever copies its log holds",
            node.node_id
        );
    }

    wait_replicas_agree(&fx.cluster, &collections).await;
    let balance = first_column(
        &fx.coordinator().client,
        &format!("SELECT balance FROM {accts} WHERE id = 'acc-0'"),
    )
    .await;
    assert_eq!(
        balance,
        vec!["140".to_owned()],
        "100 + 25 + 15 lands once; a second install of the balance slice reads 180"
    );
    let rows = first_column(
        &fx.coordinator().client,
        &format!("SELECT id FROM {entries}"),
    )
    .await;
    assert_eq!(rows.len(), 2, "the entries slice wrote its two rows once");

    fx.cluster.shutdown().await;
}
