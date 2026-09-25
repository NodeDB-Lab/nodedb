// SPDX-License-Identifier: BUSL-1.1

//! Point reads by primary key after a Calvin write, on every node.
//!
//! The coordinator assigns each row's surrogate in its own catalog at plan
//! time. Every Calvin participant — leader and follower — installs that
//! `pk → surrogate` binding when it applies its slice, so a later
//! `WHERE id = ...` resolves on every node, including one that is neither the
//! coordinator nor a member of the vShard's group (it forwards to the owner,
//! which re-resolves the key against its own catalog).

use super::calvin_multishard_fixture::{Fixture, keyed_ddl, tags};
use super::vshard_names::distinct_vshard_collections;

/// `BEGIN; INSERT a; INSERT b; COMMIT` from a non-leader coordinator, one wire
/// message per statement: each in-block INSERT answers `INSERT 0 1`, COMMIT
/// answers exactly `COMMIT`, and both rows land on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_shard_transaction_commit_reports_commit_and_rows_land() {
    let (col_a, col_b) = distinct_vshard_collections("tagfold_txn_a", "tagfold_txn_b");
    let fx = Fixture::spawn(&[keyed_ddl(&col_a), keyed_ddl(&col_b)]).await;
    let mut conn = fx.raw().await;

    assert_eq!(tags(&mut conn, "BEGIN").await, vec!["BEGIN"]);
    assert_eq!(
        tags(
            &mut conn,
            &format!("INSERT INTO {col_a} (id, v) VALUES ('k1', 'hello')")
        )
        .await,
        vec!["INSERT 0 1"],
        "in-block INSERT into {col_a} answers its own tag"
    );
    assert_eq!(
        tags(
            &mut conn,
            &format!("INSERT INTO {col_b} (id, v) VALUES ('k2', 'world')")
        )
        .await,
        vec!["INSERT 0 1"],
        "in-block INSERT into {col_b} answers its own tag"
    );
    assert_eq!(
        tags(&mut conn, "COMMIT").await,
        vec!["COMMIT"],
        "the Calvin flush answers exactly one COMMIT tag"
    );

    fx.converge().await;
    fx.wait_rows_on_every_node(&format!("SELECT v FROM {col_a} WHERE id = 'k1'"), 1)
        .await;
    fx.wait_rows_on_every_node(&format!("SELECT v FROM {col_b} WHERE id = 'k2'"), 1)
        .await;

    fx.cluster.shutdown().await;
}

/// An autocommit single-collection INSERT from a coordinator that does not
/// own the collection's vShard answers `INSERT 0 1`, and the row resolves by
/// primary key on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_shard_autocommit_insert_is_readable_by_pk_on_every_node() {
    let coll = "tagfold_autocommit_pk";
    let fx = Fixture::spawn(&[keyed_ddl(coll)]).await;
    let mut conn = fx.raw().await;

    assert_eq!(
        tags(
            &mut conn,
            &format!("INSERT INTO {coll} (id, v) VALUES ('k1', 'hello')")
        )
        .await,
        vec!["INSERT 0 1"]
    );

    fx.converge().await;
    fx.wait_rows_on_every_node(&format!("SELECT v FROM {coll} WHERE id = 'k1'"), 1)
        .await;
    fx.wait_rows_on_every_node(&format!("SELECT v FROM {coll} WHERE id = 'absent'"), 0)
        .await;

    fx.cluster.shutdown().await;
}

/// A node added as a Raft learner after both data groups are mounted joins
/// them as a non-voting member: it applies the replicated log but never
/// coordinated the transaction, so its catalog holds no binding the
/// coordinator minted.
///
/// After a Calvin commit from one of the original three, a point read by
/// primary key from the learner resolves: the owner it forwards to, and the
/// learner's own apply, must both install the coordinator's binding.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_pk_read_from_learner_node_after_calvin_commit() {
    let (col_a, col_b) = distinct_vshard_collections("tagfold_nm_a", "tagfold_nm_b");
    let mut fx = Fixture::spawn(&[keyed_ddl(&col_a), keyed_ddl(&col_b)]).await;
    fx.wait_group_mounted(&col_a).await;
    fx.wait_group_mounted(&col_b).await;

    let gid_a = fx.cluster.nodes[0]
        .group_id_for_collection(&col_a)
        .expect("col_a group resolved after mount");

    // Add a 4th node as a learner now that both groups are already mounted
    // by the original 3 nodes at the default replication factor (3).
    let learner_id = fx
        .cluster
        .add_learner_node()
        .await
        .expect("add learner node")
        .node_id;
    let reader = fx
        .cluster
        .nodes
        .iter()
        .position(|n| n.node_id == learner_id)
        .expect("learner present in cluster");

    // The 4th node joins `col_a`'s group after it was mounted: it applies
    // the replicated log but never coordinated this transaction, so its
    // catalog holds no binding the coordinator minted. It joins as a
    // learner, and the rebalancer can promote it to a voter at any point,
    // so either role holds the premise. It must not lead the group.
    let status = fx.cluster.nodes[reader].group_status_line(gid_a);
    assert!(
        status.contains("role=Learner") || status.contains("role=Follower"),
        "node {learner_id} must replicate {col_a}'s group {gid_a} without leading it: {status}"
    );

    // Coordinator is one of the original 3, chosen by the fixture default
    // (not the sequencer leader); it hosts both groups since it was present
    // when they were created.
    let mut conn = fx.raw().await;

    assert_eq!(tags(&mut conn, "BEGIN").await, vec!["BEGIN"]);
    assert_eq!(
        tags(
            &mut conn,
            &format!("INSERT INTO {col_a} (id, v) VALUES ('k1', 'hello')")
        )
        .await,
        vec!["INSERT 0 1"]
    );
    assert_eq!(
        tags(
            &mut conn,
            &format!("INSERT INTO {col_b} (id, v) VALUES ('k2', 'world')")
        )
        .await,
        vec!["INSERT 0 1"]
    );
    assert_eq!(tags(&mut conn, "COMMIT").await, vec!["COMMIT"]);

    fx.converge().await;
    fx.wait_rows_on_every_node(&format!("SELECT v FROM {col_a} WHERE id = 'k1'"), 1)
        .await;
    fx.wait_rows_on_node(
        reader,
        &format!("SELECT v FROM {col_a} WHERE id = 'absent'"),
        0,
    )
    .await;

    fx.cluster.shutdown().await;
}
