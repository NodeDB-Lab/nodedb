// SPDX-License-Identifier: BUSL-1.1

//! A replica that restarts after a dependent-read result applied, and before
//! the txn finished, reaches the barrier outcome its peers reached: the
//! result's stored row survives the restart.
//!
//! 1. Node `R` leads neither the source's nor the destination's data group.
//!    The fail gate `calvin::before_redo_propose::<dst>` holds the
//!    destination redo of the item move `T`, so `T` does not finish on the
//!    destination.
//! 2. The source vShard's read result applies on `R` and lands in its stored
//!    barrier row. The test pins the result's entry by its log index. A
//!    later write to the destination group moves `R`'s durable applied
//!    floor past that index, so a restart never delivers the entry again.
//! 3. `R` stops and starts again, and applies no pinned entry again. A
//!    duplicate entry at a later index can apply. Leadership of the
//!    destination's group moves to `R`. `R` opens `T`'s barrier from the
//!    stored row, stages `T`, and proposes its redo once the gate is
//!    released.
//! 4. `T` commits on every replica, and no node halts a scheduler.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use nodedb_types::fail_point::{FailAction, FailGuard};

use super::calvin_dependent_read_fixture::{
    ItemMove, assert_no_apply_halt, read_entries, stored_barrier_rows, waiting_barrier_txns,
};
use super::calvin_replica_content::strict_session;
use crate::common::cluster_harness::shared_steps::{db_detail, leader_of};
use crate::common::cluster_harness::wait_for;

/// How long the test waits for a held step.
const HOLD_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_restarted_replica_reaches_the_barrier_outcome_from_its_stored_row() {
    let mut mv = ItemMove::spawn("dep_restart").await;
    mv.cluster().wait_for_preferred_leaders(HOLD_DEADLINE).await;
    let source_bytes = mv.source_bytes().await;
    let source_group = mv.group_of(&mv.source);
    let dest_group = mv.group_of(&mv.dest);
    let dest_vshard = mv.dest_vshard();
    let (restarted, restarted_id, coordinator) = {
        let nodes = &mv.cluster().nodes;
        let source_leader = leader_of(&nodes[0], source_group);
        let dest_leader = leader_of(&nodes[0], dest_group);
        let restarted = nodes
            .iter()
            .position(|node| {
                node.node_id != source_leader
                    && node.node_id != dest_leader
                    && node.replicates_data_group(dest_group)
            })
            .expect("three nodes hold a destination replica that leads neither group");
        let coordinator = (0..nodes.len())
            .find(|idx| *idx != restarted)
            .expect("a coordinator besides the restarted node");
        (restarted, nodes[restarted].node_id, coordinator)
    };
    let session = strict_session(&mv.cluster().nodes[coordinator]).await;
    let filler = strict_session(&mv.cluster().nodes[coordinator]).await;

    let redo_dir = tempfile::tempdir().expect("redo gate directory");
    let redo_release = redo_dir.path().join("release");
    let redo_parked = redo_dir.path().join("release.parked");
    let redo_gate = FailGuard::install(
        &format!("calvin::before_redo_propose::{}", mv.dest),
        FailAction::WaitForFile(redo_release.clone()),
    );

    let transfer = mv.transfer_sql();
    let filler_sql = format!(
        "INSERT INTO {} (key, owner, note) VALUES ('dave:axe', 'dave', 'filler')",
        mv.dest
    );
    let (moved, ()) = tokio::join!(session.simple_query(&transfer), async {
        {
            let node = &mv.cluster().nodes[restarted];
            wait_for(
                "the destination leader holds the redo, and the replica stored the read result",
                HOLD_DEADLINE,
                Duration::from_millis(20),
                || {
                    redo_parked.exists()
                        && !read_entries(node, dest_vshard).is_empty()
                        && stored_barrier_rows(node, dest_vshard) >= 1
                },
            )
            .await;
        }
        // The read-result entries the replica applied before it stops. The
        // first is the one the barrier counts.
        let applied_before = read_entries(&mv.cluster().nodes[restarted], dest_vshard);
        let pinned = applied_before
            .iter()
            .map(|(_, index)| *index)
            .max()
            .expect("the replica applied a read result");
        filler
            .simple_query(&filler_sql)
            .await
            .unwrap_or_else(|e| panic!("the filler write commits: {}", db_detail(&e)));
        let dest_leader_applied = {
            let nodes = &mv.cluster().nodes;
            let leader = leader_of(&nodes[coordinator], dest_group);
            nodes
                .iter()
                .find(|node| node.node_id == leader)
                .expect("the destination leader runs")
                .shared
                .applied_index_watcher(dest_group)
                .current()
        };
        {
            let node = &mv.cluster().nodes[restarted];
            wait_for(
                "the replica's durable applied floor passes the read result's entry",
                HOLD_DEADLINE,
                Duration::from_millis(20),
                || node.shared.pitr.durable_applied(dest_group) >= dest_leader_applied.max(pinned),
            )
            .await;
        }

        let stopped = mv
            .fx
            .cluster
            .stop_member(restarted)
            .await
            .expect("stop the replica");
        mv.fx
            .cluster
            .restart_member(stopped)
            .await
            .expect("restart the replica");
        {
            let node = &mv.cluster().nodes[restarted];
            assert_eq!(node.node_id, restarted_id, "the replica keeps its index");
            // Boot resumes delivery above the durable floor, which passed
            // every entry applied before the stop. A duplicate read-result
            // entry the log holds later can apply now: it sits at another
            // index, and the barrier counts the first copy alone.
            let again: Vec<(u64, u64)> = read_entries(node, dest_vshard)
                .into_iter()
                .filter(|entry| applied_before.contains(entry))
                .collect();
            assert!(
                again.is_empty(),
                "the restart delivers again the read-result entries {again:?}"
            );
            assert_eq!(
                stored_barrier_rows(node, dest_vshard),
                1,
                "the stored row survives the restart"
            );
        }
        mv.cluster()
            .transfer_leadership(dest_group, restarted_id)
            .await;
        std::fs::write(&redo_release, b"release").expect("release the redo");
    });
    moved.unwrap_or_else(|e| {
        panic!(
            "the move commits under the restarted leader: {}",
            db_detail(&e)
        )
    });
    drop(redo_gate);

    mv.wait_replicas_agree(Some(&source_bytes)).await;
    wait_for(
        "every node drops the barrier entries and rows of the finished move",
        HOLD_DEADLINE,
        Duration::from_millis(20),
        || {
            mv.cluster().nodes.iter().all(|node| {
                waiting_barrier_txns(node, dest_vshard) == 0
                    && stored_barrier_rows(node, dest_vshard) == 0
            })
        },
    )
    .await;
    assert_no_apply_halt(mv.cluster());

    let ItemMove { fx, .. } = mv;
    fx.cluster.shutdown().await;
}
