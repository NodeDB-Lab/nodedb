// SPDX-License-Identifier: BUSL-1.1

//! A Calvin slice's redo that its leader proposed, and that commits only in
//! the next term under a new leader, installs once. The new leader stages
//! nothing for that position.
//!
//! 1. A 3-node cluster holds `accts`, whose `balance` is the materialized
//!    sum of `entries.amount`, and `entries`, in distinct data groups.
//!    `acc-0` starts at 100. Every data group is led by its preferred
//!    leader, so the leader balance moves nothing by itself.
//! 2. The fail gate `calvin::before_redo_propose::<entries>` holds the
//!    entries slice after its vote and resolve, on the leader `L` of the
//!    entries group `G`. The balance slice installs meanwhile.
//! 3. The test arms `raft::hold_commit::group<G>` on node `L` only, then
//!    releases the redo. `L` proposes it, the entry replicates to every replica, and
//!    `L` commits nothing.
//! 4. The test moves `G`'s leadership to another replica `N`. The entry of
//!    the old term commits with `N`'s first entry of the new term. `N`
//!    opens its stage gate only once that entry applied, so it finds the
//!    position applied and stages nothing.
//! 5. The client's COMMIT succeeds once. No node resolved a commit or
//!    restaged after the hold, no node applied a second copy, each replica
//!    of `G` installed the slice once, and `acc-0` reads 125, the value of
//!    a single install.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb_test_support::fail_point::{FailAction, FailGuard};

use super::calvin_multishard_fixture::{Fixture, tags};
use super::calvin_replica_content::{
    first_column, run_retrying, sum_ddl, vshard_of, wait_replicas_agree,
};
use crate::common::cluster_harness::shared_steps::{group_members, group_status, name_where};
use crate::common::cluster_harness::{
    GroupLeader, TestCluster, TestClusterNode, wait_for, wait_for_async,
};

/// How long each step of the hold gets.
const STEP_DEADLINE: Duration = Duration::from_secs(30);

/// Pause between two polls of a step.
const STEP_POLL: Duration = Duration::from_millis(20);

/// One node's Calvin counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Counters {
    /// Committed slices this node's leaders resolved.
    commits: u64,
    /// Restages this node's leaders dispatched.
    restages: u64,
    /// Stamped redo copies this node applied that installed nothing.
    skipped_copies: u64,
    /// Stamped redo installs on this node.
    installs: u64,
}

impl Counters {
    fn of(node: &TestClusterNode) -> Self {
        let counters = &node.shared.calvin.counters;
        Self {
            commits: counters.commits_flushed.load(Ordering::Relaxed),
            restages: counters.stage_restages.load(Ordering::Relaxed),
            skipped_copies: counters.redo_copies_skipped.load(Ordering::Relaxed),
            installs: counters.write_versions_recorded.load(Ordering::Relaxed),
        }
    }
}

/// The served balance of `acc-0`, `None` while no leader serves it.
async fn balance(node: &TestClusterNode, accts: &str) -> Option<String> {
    let sql = format!("SELECT balance FROM {accts} WHERE id = 'acc-0'");
    let rows = node.client.simple_query(&sql).await.ok()?;
    rows.into_iter().find_map(|msg| match msg {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    })
}

/// The index in `cluster.nodes` of node `node_id`.
fn index_of(cluster: &TestCluster, node_id: u64) -> usize {
    cluster
        .nodes
        .iter()
        .position(|node| node.node_id == node_id)
        .unwrap_or_else(|| panic!("node {node_id} is live"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn an_old_term_redo_copy_committed_by_the_new_leader_installs_once() {
    let fx = Fixture::from_cluster(TestCluster::spawn_three().await.expect("3-node cluster")).await;
    let probe = &fx.cluster.nodes[0];
    let entries = name_where("old_term_entries", |name| {
        probe.group_id_for_collection(name).is_some()
    });
    let group = probe
        .group_id_for_collection(&entries)
        .expect("the entries vShard maps to a group");
    let accts = name_where("old_term_accts", |name| {
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
    run_retrying(
        &fx.cluster.nodes[0].client,
        "seed the account",
        &format!("INSERT INTO {accts} (id, owner, balance) VALUES ('acc-0', 'seed', '100')"),
    )
    .await;
    fx.converge().await;
    fx.cluster.wait_for_preferred_leaders(STEP_DEADLINE).await;

    let mut session = fx.raw().await;
    let txn = format!(
        "BEGIN; \
         INSERT INTO {entries} (id, account_id, amount) VALUES ('e1', 'acc-0', '25'); \
         COMMIT"
    );
    let gate_dir = tempfile::tempdir().expect("gate directory");
    let propose_release = gate_dir.path().join("propose");
    let propose_parked = gate_dir.path().join("propose.parked");
    let commit_release = gate_dir.path().join("commit");
    let (committed, before) = {
        let _propose_gate = FailGuard::install(
            &format!("calvin::before_redo_propose::{entries}"),
            FailAction::WaitForFile(propose_release.clone()),
        );
        let commit = tags(&mut session, &txn);
        let move_leader = async {
            wait_for(
                "the entries leader holds the slice's redo after its resolve",
                STEP_DEADLINE,
                STEP_POLL,
                || propose_parked.exists(),
            )
            .await;
            let old_leader = fx
                .cluster
                .wait_for_group_leader(group, GroupLeader::Any, STEP_DEADLINE)
                .await;
            let old = index_of(&fx.cluster, old_leader);
            // The balance slice installs on every replica while the entries
            // slice is held. Every count below then belongs to the held slice.
            let coordinator = fx.coordinator();
            let accts = accts.as_str();
            wait_for_async(
                "the balance slice installs",
                STEP_DEADLINE,
                STEP_POLL,
                || async move { balance(coordinator, accts).await.as_deref() == Some("125") },
            )
            .await;
            fx.converge().await;
            let before: Vec<Counters> = fx.cluster.nodes.iter().map(Counters::of).collect();
            let held_status = group_status(&fx.cluster.nodes[old], group)
                .expect("the old leader hosts the entries group");

            let _commit_gate = FailGuard::for_node(
                old_leader,
                &format!("raft::hold_commit::group{group}"),
                FailAction::WaitForFile(commit_release.clone()),
            );
            std::fs::write(&propose_release, b"release").expect("release the held redo");
            let new_leader = group_members(&fx.cluster.nodes[0], group)
                .into_iter()
                .find(|member| *member != old_leader)
                .expect("the entries group has a replica besides its leader");
            let new = index_of(&fx.cluster, new_leader);
            let mut redo_index = 0;
            wait_for(
                "the old leader appends the redo, and the new leader holds it, uncommitted",
                STEP_DEADLINE,
                STEP_POLL,
                || {
                    let (Some(old_status), Some(new_status)) = (
                        group_status(&fx.cluster.nodes[old], group),
                        group_status(&fx.cluster.nodes[new], group),
                    ) else {
                        return false;
                    };
                    redo_index = old_status.last_log_index;
                    old_status.last_log_index > held_status.last_log_index
                        && old_status.commit_index < old_status.last_log_index
                        && new_status.last_log_index >= old_status.last_log_index
                },
            )
            .await;

            fx.cluster.transfer_leadership(group, new_leader).await;
            let new_status = group_status(&fx.cluster.nodes[new], group)
                .expect("the new leader hosts the entries group");
            assert!(
                new_status.term > held_status.term,
                "the new leader leads a later term"
            );
            std::fs::write(&commit_release, b"release").expect("release the commit gate");
            wait_for(
                "the redo of the old term commits under the new leader",
                STEP_DEADLINE,
                STEP_POLL,
                || {
                    group_status(&fx.cluster.nodes[new], group)
                        .is_some_and(|status| status.commit_index >= redo_index)
                },
            )
            .await;
            before
        };
        tokio::join!(commit, move_leader)
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
    let replicas: Vec<bool> = fx
        .cluster
        .nodes
        .iter()
        .map(|node| node.replicates_data_group(group))
        .collect();
    wait_for(
        "every replica of the entries group installs the slice",
        STEP_DEADLINE,
        STEP_POLL,
        || {
            fx.cluster
                .nodes
                .iter()
                .zip(&before)
                .zip(&replicas)
                .all(|((node, before), replica)| {
                    !replica || Counters::of(node).installs > before.installs
                })
        },
    )
    .await;
    for ((node, before), replica) in fx.cluster.nodes.iter().zip(&before).zip(&replicas) {
        let after = Counters::of(node);
        assert_eq!(
            after.installs - before.installs,
            u64::from(*replica),
            "node {} installs the slice once when it replicates the entries group",
            node.node_id
        );
        assert_eq!(
            after.skipped_copies, before.skipped_copies,
            "node {} applies no second copy of the redo",
            node.node_id
        );
        assert_eq!(
            after.commits, before.commits,
            "node {} resolves no commit after the hold: no leader staged the position again",
            node.node_id
        );
        assert_eq!(
            after.restages, before.restages,
            "node {} restages nothing",
            node.node_id
        );
    }

    let collections = [accts.as_str(), entries.as_str()];
    wait_replicas_agree(&fx.cluster, &collections).await;
    let client = &fx.coordinator().client;
    assert_eq!(
        first_column(
            client,
            &format!("SELECT balance FROM {accts} WHERE id = 'acc-0'")
        )
        .await,
        vec!["125".to_owned()],
        "100 + 25 lands once; a second install reads 150"
    );
    assert_eq!(
        first_column(client, &format!("SELECT id FROM {entries}")).await,
        vec!["e1".to_owned()],
        "the entries slice wrote its row once"
    );

    fx.cluster.shutdown().await;
}
