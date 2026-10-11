// SPDX-License-Identifier: BUSL-1.1

//! A read result that reaches a vShard before the vShard granted its txn is
//! kept, and the barrier the grant opens completes from it.
//!
//! 1. A transaction `P` writes `bob:sword` at the destination and a row at
//!    the source. The fail gate `calvin::before_redo_propose::<dst>` holds
//!    its destination redo, so every destination replica keeps `P`'s lock
//!    on `bob:sword`.
//! 2. The item move `T` queues behind that lock on the destination. Its
//!    source vShard holds no conflicting lock, reads the item, and
//!    broadcasts it. The broadcast applies on every destination replica
//!    before the replica granted `T`. It waits in the replica's read-result
//!    buffer and in its stored barrier row.
//! 3. Released, `P` commits and frees the lock. Each replica grants `T` and
//!    takes the buffered result. The destination's leader stages at once,
//!    and `T` commits with the item's bytes on every replica. Each replica
//!    then drops the txn's buffered entries and its row.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use nodedb_test_support::fail_point::{FailAction, FailGuard};

use super::calvin_dependent_read_fixture::{
    ItemMove, assert_no_apply_halt, stored_barrier_rows, waiting_barrier_txns,
};
use super::calvin_replica_content::strict_session;
use crate::common::cluster_harness::shared_steps::db_detail;
use crate::common::cluster_harness::wait_for;

/// How long the test waits for a held step.
const HOLD_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_read_result_before_the_grant_is_applied_not_dropped() {
    let mv = ItemMove::spawn("dep_early").await;
    let source_bytes = mv.source_bytes().await;
    let dest_group = mv.group_of(&mv.dest);
    let dest_vshard = mv.dest_vshard();
    let holder = strict_session(mv.fx.coordinator()).await;
    let mover = strict_session(mv.fx.coordinator()).await;

    let gate_dir = tempfile::tempdir().expect("gate directory");
    let release = gate_dir.path().join("release");
    let parked = gate_dir.path().join("release.parked");
    let gate = FailGuard::install(
        &format!("calvin::before_redo_propose::{}", mv.dest),
        FailAction::WaitForFile(release.clone()),
    );

    let hold_lock = format!(
        "BEGIN; \
         INSERT INTO {dest} (key, owner, note) VALUES ('bob:sword', 'bob', 'old'); \
         INSERT INTO {source} (key, owner, note) VALUES ('carol:shield', 'carol', 'x'); \
         COMMIT",
        dest = mv.dest,
        source = mv.source,
    );
    let transfer = mv.transfer_sql();
    let (held, moved, ()) = tokio::join!(
        holder.simple_query(&hold_lock),
        async {
            wait_for(
                "the destination leader holds P's redo, and with it the lock",
                HOLD_DEADLINE,
                Duration::from_millis(20),
                || parked.exists(),
            )
            .await;
            mover.simple_query(&transfer).await
        },
        async {
            wait_for(
                "every destination replica buffers the read result before its grant",
                HOLD_DEADLINE,
                Duration::from_millis(20),
                || {
                    parked.exists()
                        && mv
                            .cluster()
                            .nodes
                            .iter()
                            .filter(|node| node.replicates_data_group(dest_group))
                            .all(|node| {
                                waiting_barrier_txns(node, dest_vshard) >= 1
                                    && stored_barrier_rows(node, dest_vshard) >= 1
                            })
                },
            )
            .await;
            std::fs::write(&release, b"release").expect("release P's redo");
        },
    );
    held.unwrap_or_else(|e| panic!("P commits: {}", db_detail(&e)));
    moved.unwrap_or_else(|e| {
        panic!(
            "the move commits from the buffered result: {}",
            db_detail(&e)
        )
    });
    drop(gate);

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
