// SPDX-License-Identifier: BUSL-1.1

//! A transaction's read validates at whichever replica leads its vShard at
//! COMMIT.
//!
//! A transaction reads a row of `reads`, then writes `writes` on another
//! vShard, so its COMMIT runs through Calvin with the read's vShard as a
//! read-only participant. The read's version is the data-group log position
//! of the last write to `reads`, the same on every replica.
//!
//! The read runs while a non-preferred replica leads the read's data group.
//! Before the COMMIT, leadership moves back to the preferred replica. The
//! leader balance never moves a group away from its preferred leader, so that
//! replica, never the one that served the read, validates it.
//!
//! - No write to `reads` after the read: the validating leader finds the
//!   read current, and the COMMIT succeeds.
//! - A write to the read row committed after the read: its log position is
//!   above the read's version, so the validating leader aborts the COMMIT
//!   with SQLSTATE 40001.

use std::time::Duration;

use super::calvin_multishard_fixture::{Fixture, data_rows, keyed_ddl, row_count};
use super::calvin_replica_content::{names_on_distinct_vshards, run_retrying, strict_session};
use crate::common::cluster_harness::GroupLeader;
use crate::common::cluster_harness::shared_steps::{db_detail, group_members, group_of};

/// SQLSTATE of a transaction aborted by a conflict.
const SERIALIZATION_FAILURE: &str = "40001";

/// How long a group gets to show a leader.
const LEADER_WAIT: Duration = Duration::from_secs(30);

/// Attempts at a read served by the non-preferred leader. The leader balance
/// can move the group back between the move and the read. The attempt then
/// rolls back and starts again.
const READ_ATTEMPTS: usize = 10;

/// A 3-node cluster holding `reads` and `writes` on distinct vShards, with
/// row `r1` committed in `reads` and replicated everywhere.
async fn seeded(prefixes: [&str; 2]) -> (Fixture, String, String) {
    let [reads, writes] = names_on_distinct_vshards(prefixes);
    let fx = Fixture::spawn(&[keyed_ddl(&reads), keyed_ddl(&writes)]).await;
    for collection in [&reads, &writes] {
        fx.wait_group_mounted(collection).await;
    }
    run_retrying(
        &fx.cluster.nodes[0].client,
        "seed the read row",
        &format!("INSERT INTO {reads} (id, v) VALUES ('r1', 'seed')"),
    )
    .await;
    fx.converge().await;
    (fx, reads, writes)
}

/// The leaders a case moves `reads`' data group between.
struct Leaders {
    group: u64,
    /// The preferred leader, which validates the read at COMMIT.
    validating: u64,
    /// Another replica, which serves the read.
    serving: u64,
}

/// Settle every group on its preferred leader and pick the two leaders of
/// `collection`'s data group.
async fn leaders_of(fx: &Fixture, collection: &str) -> Leaders {
    fx.cluster.wait_for_preferred_leaders(LEADER_WAIT).await;
    let group = group_of(&fx.cluster.nodes[0], collection);
    let validating = fx
        .cluster
        .wait_for_group_leader(group, GroupLeader::Any, LEADER_WAIT)
        .await;
    let serving = group_members(&fx.cluster.nodes[0], group)
        .into_iter()
        .find(|member| *member != validating)
        .unwrap_or_else(|| panic!("group {group} has a replica besides its leader {validating}"));
    Leaders {
        group,
        validating,
        serving,
    }
}

/// Open a transaction on the coordinator whose read of `r1` from `reads`
/// the serving leader answered, and buffer a write to `writes`. Return its
/// session.
async fn read_on_serving_leader(
    fx: &Fixture,
    leaders: &Leaders,
    reads: &str,
    writes: &str,
) -> tokio_postgres::Client {
    let session = strict_session(fx.coordinator()).await;
    for _ in 0..READ_ATTEMPTS {
        fx.cluster
            .transfer_leadership(leaders.group, leaders.serving)
            .await;
        session
            .simple_query("BEGIN")
            .await
            .unwrap_or_else(|e| panic!("BEGIN: {}", db_detail(&e)));
        let rows = session
            .simple_query(&format!("SELECT v FROM {reads} WHERE id = 'r1'"))
            .await
            .unwrap_or_else(|e| panic!("in-transaction read: {}", db_detail(&e)));
        assert_eq!(data_rows(&rows), 1, "the transaction reads the seeded row");
        if fx.cluster.group_leader(leaders.group) != Some(leaders.serving) {
            session
                .simple_query("ROLLBACK")
                .await
                .unwrap_or_else(|e| panic!("ROLLBACK: {}", db_detail(&e)));
            continue;
        }
        session
            .simple_query(&format!(
                "INSERT INTO {writes} (id, v) VALUES ('w1', 'txn')"
            ))
            .await
            .unwrap_or_else(|e| panic!("buffer the write: {}", db_detail(&e)));
        return session;
    }
    panic!(
        "node {} did not hold the leadership of group {} across a read in {READ_ATTEMPTS} attempts",
        leaders.serving, leaders.group
    );
}

/// Move the leadership back to the preferred leader, which then validates
/// the read at COMMIT.
async fn hand_back(fx: &Fixture, leaders: &Leaders) {
    fx.cluster
        .transfer_leadership(leaders.group, leaders.validating)
        .await;
    fx.cluster
        .wait_for_group_leader(
            leaders.group,
            GroupLeader::Node(leaders.validating),
            LEADER_WAIT,
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_current_read_commits_after_its_leader_moves() {
    let (fx, reads, writes) = seeded(["lvm_ok_reads", "lvm_ok_writes"]).await;
    let leaders = leaders_of(&fx, &reads).await;
    let session = read_on_serving_leader(&fx, &leaders, &reads, &writes).await;
    hand_back(&fx, &leaders).await;

    session.simple_query("COMMIT").await.unwrap_or_else(|e| {
        panic!(
            "a read the serving leader answered is current at the validating \
             leader, so the commit succeeds: {}",
            db_detail(&e)
        )
    });
    fx.converge().await;
    assert_eq!(
        row_count(
            &fx.coordinator().client,
            &format!("SELECT id FROM {writes} WHERE id = 'w1'")
        )
        .await,
        1,
        "the committed transaction's write is visible"
    );

    fx.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_write_after_the_read_aborts_the_commit_after_its_leader_moves() {
    let (fx, reads, writes) = seeded(["lvm_stale_reads", "lvm_stale_writes"]).await;
    let leaders = leaders_of(&fx, &reads).await;
    let session = read_on_serving_leader(&fx, &leaders, &reads, &writes).await;
    hand_back(&fx, &leaders).await;
    let writer = (fx.coordinator + 1) % fx.cluster.nodes.len();
    run_retrying(
        &fx.cluster.nodes[writer].client,
        "a conflicting write to the read row",
        &format!("UPDATE {reads} SET v = 'moved' WHERE id = 'r1'"),
    )
    .await;
    fx.converge().await;

    let error = session
        .simple_query("COMMIT")
        .await
        .expect_err("a write committed after the read makes the read stale");
    assert_eq!(
        error.as_db_error().map(|db| db.code().code()),
        Some(SERIALIZATION_FAILURE),
        "the stale read aborts the commit with a serialization failure: {}",
        db_detail(&error)
    );
    fx.converge().await;
    assert_eq!(
        row_count(
            &fx.coordinator().client,
            &format!("SELECT id FROM {writes} WHERE id = 'w1'")
        )
        .await,
        0,
        "the aborted transaction's write is not visible"
    );

    fx.cluster.shutdown().await;
}
