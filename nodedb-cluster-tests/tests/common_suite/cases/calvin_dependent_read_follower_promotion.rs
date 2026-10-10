// SPDX-License-Identifier: BUSL-1.1

//! A follower that buffered a dependent-read result before its grant does
//! not halt once it leads: the barrier decides from the log, not a clock.
//!
//! 1. Node `F` leads neither the source's nor the destination's data group.
//!    Its metadata apply is held, and a DDL through another node moves the
//!    metadata log past it. The item move `T` carries the coordinator's
//!    metadata floor, so `F`'s schedulers hold `T` ungranted.
//! 2. The source vShard reads the item and broadcasts it. The broadcast
//!    applies on `F` before `F` granted `T`. The destination's leader stages
//!    `T` and votes commit, and the fail gate
//!    `calvin::before_redo_propose::<dst>` holds its redo.
//! 3. `F` is released and grants `T` as a follower, and takes the buffered
//!    result. Leadership of the destination's group moves to `F`. `F` opens
//!    the barrier from the result, stages `T`, and proposes its redo once
//!    the gate is released.
//! 4. `T` commits on every replica, and no node halts a scheduler with
//!    `LocalStageFailed`.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use nodedb::control::cluster::metadata_applier::METADATA_APPLY_HOLD_POINT;
use nodedb_test_support::fail_point::{FailAction, FailGuard};

use super::calvin_dependent_read_fixture::{ItemMove, assert_no_apply_halt, waiting_barrier_txns};
use super::calvin_replica_content::strict_session;
use crate::common::cluster_harness::shared_steps::{db_detail, leader_of};
use crate::common::cluster_harness::wait_for;

/// How long the test waits for a held step.
const HOLD_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_promoted_follower_commits_the_move_without_halting() {
    let mv = ItemMove::spawn("dep_promote").await;
    mv.cluster().wait_for_preferred_leaders(HOLD_DEADLINE).await;
    let source_bytes = mv.source_bytes().await;
    let source_group = mv.group_of(&mv.source);
    let dest_group = mv.group_of(&mv.dest);
    let dest_vshard = mv.dest_vshard();
    let nodes = &mv.cluster().nodes;
    let source_leader = leader_of(&nodes[0], source_group);
    let dest_leader = leader_of(&nodes[0], dest_group);
    let follower = nodes
        .iter()
        .position(|node| node.node_id != source_leader && node.node_id != dest_leader)
        .expect("three nodes hold a node that leads neither group");
    let follower_id = nodes[follower].node_id;
    let coordinator = (0..nodes.len())
        .find(|idx| *idx != follower)
        .expect("a coordinator besides the follower");
    let session = strict_session(&nodes[coordinator]).await;

    // Hold the follower's metadata apply and move the metadata log past it.
    let meta_dir = tempfile::tempdir().expect("metadata gate directory");
    let meta_release = meta_dir.path().join("release");
    let meta_parked = meta_dir.path().join("release.parked");
    let _meta_hold = FailGuard::for_node(
        follower_id,
        METADATA_APPLY_HOLD_POINT,
        FailAction::WaitForFile(meta_release.clone()),
    );
    nodes[coordinator]
        .exec("CREATE TOPIC dep_promote_lag")
        .await
        .unwrap_or_else(|e| panic!("the DDL commits without the follower: {e}"));
    let metadata = nodedb_cluster::METADATA_GROUP_ID;
    wait_for(
        "the follower's metadata apply lags the coordinator's",
        HOLD_DEADLINE,
        Duration::from_millis(20),
        || {
            meta_parked.exists()
                && nodes[follower]
                    .shared
                    .applied_index_watcher(metadata)
                    .current()
                    < nodes[coordinator]
                        .shared
                        .applied_index_watcher(metadata)
                        .current()
        },
    )
    .await;

    let redo_dir = tempfile::tempdir().expect("redo gate directory");
    let redo_release = redo_dir.path().join("release");
    let redo_parked = redo_dir.path().join("release.parked");
    let redo_gate = FailGuard::install(
        &format!("calvin::before_redo_propose::{}", mv.dest),
        FailAction::WaitForFile(redo_release.clone()),
    );

    let transfer = mv.transfer_sql();
    let (moved, ()) = tokio::join!(session.simple_query(&transfer), async {
        // Two waits, so a timeout names the half that never held.
        wait_for(
            "the follower buffered the read result before its grant",
            HOLD_DEADLINE,
            Duration::from_millis(20),
            || waiting_barrier_txns(&nodes[follower], dest_vshard) >= 1,
        )
        .await;
        wait_for(
            "the destination leader voted commit and holds the redo",
            HOLD_DEADLINE,
            Duration::from_millis(20),
            || redo_parked.exists(),
        )
        .await;
        assert!(
            nodes[follower]
                .shared
                .applied_index_watcher(metadata)
                .current()
                < nodes[coordinator]
                    .shared
                    .applied_index_watcher(metadata)
                    .current(),
            "the follower's metadata apply still lags, so its grant waits"
        );
        std::fs::write(&meta_release, b"release").expect("release the follower's metadata");
        wait_for(
            "the follower granted the move and took its buffered result",
            HOLD_DEADLINE,
            Duration::from_millis(20),
            || waiting_barrier_txns(&nodes[follower], dest_vshard) == 0,
        )
        .await;
        mv.cluster()
            .transfer_leadership(dest_group, follower_id)
            .await;
        std::fs::write(&redo_release, b"release").expect("release the redo");
    });
    moved.unwrap_or_else(|e| panic!("the move commits under the new leader: {}", db_detail(&e)));
    drop(redo_gate);

    mv.wait_replicas_agree(Some(&source_bytes)).await;
    assert_no_apply_halt(mv.cluster());

    let ItemMove { fx, .. } = mv;
    fx.cluster.shutdown().await;
}
