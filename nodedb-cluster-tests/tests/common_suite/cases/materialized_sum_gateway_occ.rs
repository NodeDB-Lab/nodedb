// SPDX-License-Identifier: BUSL-1.1

//! A pgwire UPDATE received on a follower keeps its settlement reads when the
//! leader of every group it touches is one remote node.
//!
//! Node `L` leads both the source group (`xs_entries`) and the target group
//! (`xs_accounts`, holding the materialized sum). Follower `F` receives
//! `UPDATE xs_entries SET amount = '40' WHERE id = 'e1'`. Every group the
//! statement touches has the same remote leader, so placement picks the
//! gateway forward. A forward checks no read, so a settled statement must
//! commit through Calvin instead.
//!
//! The fail gate `stmt::before_route` parks the statement on `F` after
//! planning and before routing. While it is parked, `L` rewrites the same
//! source row. After the release and convergence the balance must equal
//! `100 + sum(source amounts)`. The statement on `F` can commit after a
//! retry or fail with a serialization error. Both outcomes keep the sum exact.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::path::PathBuf;
use std::time::Duration;

use nodedb_test_support::fail_point::{FailAction, FailGuard};

use super::calvin_replica_content::run_retrying;
use super::materialized_sum_cross_shard::{SOURCE, TARGET, balance_of, declare_binding, seed};
use crate::common::cluster_harness::shared_steps::group_of;
use crate::common::cluster_harness::{
    GroupLeader, TestCluster, read_once_a_leader_exists, wait_for,
};

/// Opening balance of the account, before any source row counts.
const START: i64 = 100;

/// How long each wait in the case gets.
const STEP_DEADLINE: Duration = Duration::from_secs(30);

/// Pause between two polls.
const STEP_POLL: Duration = Duration::from_millis(20);

/// Bring up the cluster with `nodes[0]` leading the source and target
/// groups, then seed `acc-1` at `START` with one entry `e1` of 25.
async fn cluster_led_by_first_node() -> TestCluster {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    declare_binding(&cluster).await;
    cluster.wait_for_full_apply_convergence(STEP_DEADLINE).await;

    let leader = cluster.nodes[0].node_id;
    for collection in [SOURCE, TARGET] {
        let group = group_of(&cluster.nodes[0], collection);
        cluster.transfer_leadership(group, leader).await;
        let found = cluster
            .wait_for_group_leader(group, GroupLeader::Node(leader), STEP_DEADLINE)
            .await;
        assert_eq!(
            found, leader,
            "node {leader} must lead the group of {collection}"
        );
        assert_eq!(
            cluster.group_leader(group),
            Some(leader),
            "group {group} of {collection} must have node {leader} as its one leader"
        );
    }

    seed(&cluster, "acc-1", &START.to_string(), &[("e1", "25")]).await;
    assert_eq!(
        balance_of(&cluster.nodes[0].client, "acc-1").await,
        "125",
        "the seeded balance must be 100 + 25"
    );
    cluster
}

/// The sum of every `SOURCE` amount, read through node `idx`.
async fn source_sum(cluster: &TestCluster, idx: usize) -> f64 {
    let sql = format!("SELECT amount FROM {SOURCE}");
    // A leader change refuses a read with `55P03` before it runs, so it runs again.
    let rows = read_once_a_leader_exists("read source amounts", STEP_DEADLINE, STEP_POLL, || {
        cluster.nodes[idx].client.simple_query(&sql)
    })
    .await;
    rows.into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        })
        .map(|amount| {
            amount
                .parse::<f64>()
                .unwrap_or_else(|_| panic!("amount '{amount}' must parse as a number"))
        })
        .sum()
}

/// Assert, on every node, that the balance of `acc-1` equals
/// `START + sum(source amounts)`.
async fn assert_balance_equals_source_sum(cluster: &TestCluster) {
    cluster.wait_for_full_apply_convergence(STEP_DEADLINE).await;
    for idx in 0..cluster.nodes.len() {
        let sum = source_sum(cluster, idx).await;
        let text = balance_of(&cluster.nodes[idx].client, "acc-1").await;
        let balance: f64 = text
            .parse()
            .unwrap_or_else(|_| panic!("balance '{text}' must parse as a number"));
        assert!(
            (balance - (START as f64 + sum)).abs() < 1e-9,
            "node {}: the balance must equal the source sum: balance {balance}, \
             expected {START} + {sum}",
            cluster.nodes[idx].node_id
        );
    }
}

/// Send `statement` from a follower that parks it before routing, rewrite
/// the source row `e1` on the leader meanwhile, release it, and check that
/// the balance equals the source sum on every node.
async fn forwarded_statement_then_move_source(statement: &'static str) {
    let cluster = cluster_led_by_first_node().await;
    let follower = &cluster.nodes[1];
    assert_ne!(
        follower.node_id, cluster.nodes[0].node_id,
        "the statement must arrive on a node other than the leader"
    );

    let mut conn = follower.raw_pgwire().await;

    let gate_dir = tempfile::tempdir().expect("gate directory");
    let release: PathBuf = gate_dir.path().join("release");
    let parked = gate_dir.path().join("release.parked");
    let _gate = FailGuard::for_node(
        follower.node_id,
        "stmt::before_route",
        FailAction::WaitForFile(release.clone()),
    );

    // The raw session panics on an ErrorResponse, so a join error is the
    // statement failing with a serialization error. That outcome is allowed.
    let forwarded = tokio::spawn(async move { conn.simple_query(statement).await });

    wait_for(
        "the follower's statement parks before routing",
        STEP_DEADLINE,
        STEP_POLL,
        || parked.exists(),
    )
    .await;

    // A `40001` abort writes nothing and a `55P03` refusal precedes the run,
    // so a retry applies the overwrite once.
    run_retrying(
        &cluster.nodes[0].client,
        "update on the leader",
        &format!("UPDATE {SOURCE} SET amount = '100' WHERE id = 'e1'"),
    )
    .await;

    std::fs::write(&release, b"go").expect("release the gate");
    match forwarded.await {
        Ok(_) => {}
        Err(join) => eprintln!("the forwarded statement ended with an error: {join}"),
    }

    assert_balance_equals_source_sum(&cluster).await;
}

/// A point UPDATE forwarded from a follower to the one leader of all its
/// groups stays exact when the source row changes on the leader mid-statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_forwarded_update_keeps_the_balance_equal_to_the_source_sum() {
    forwarded_statement_then_move_source("UPDATE xs_entries SET amount = '40' WHERE id = 'e1'")
        .await;
}

/// The same for a predicate UPDATE, whose rows the coordinator reads first.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_forwarded_predicate_update_keeps_the_balance_equal_to_the_source_sum() {
    forwarded_statement_then_move_source("UPDATE xs_entries SET amount = '40' WHERE amount = '25'")
        .await;
}
