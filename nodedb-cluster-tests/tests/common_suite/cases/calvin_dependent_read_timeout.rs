// SPDX-License-Identifier: BUSL-1.1

//! A dependent-read barrier whose read result never arrives aborts on every
//! replica alike, through the timeout entry its leader puts in the log.
//!
//! 1. The fail gate `calvin::before_read_result_propose::<src>` holds the
//!    source vShard's broadcast of the item it read.
//! 2. The active vShards' barriers wait past
//!    `tuning.calvin.dependent_read_passive_timeout_ms`. Each data-group
//!    leader proposes the barrier's timeout entry, and every replica folds
//!    it ahead of the read result.
//! 3. The move fails. No replica moved the item, and no scheduler halted.
//! 4. Released, the stale broadcast reaches both logs after the txn
//!    finished. Every replica applies it and drops it, so no buffer and no
//!    stored row keeps it.
//! 5. A second move commits: the aborted txn left no lock behind.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use nodedb_types::fail_point::{FailAction, FailGuard};

use super::calvin_dependent_read_fixture::{
    ItemMove, assert_no_apply_halt, reads_applied, stored_barrier_rows, waiting_barrier_txns,
};
use super::calvin_replica_content::strict_session;
use crate::common::cluster_harness::shared_steps::db_detail;
use crate::common::cluster_harness::wait_for;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_read_result_held_past_the_timeout_aborts_on_every_replica() {
    let mv = ItemMove::spawn("dep_timeout").await;
    let source_bytes = mv.source_bytes().await;
    let session = strict_session(mv.fx.coordinator()).await;

    let gate_dir = tempfile::tempdir().expect("gate directory");
    let release = gate_dir.path().join("release");
    let _gate = FailGuard::install(
        &format!("calvin::before_read_result_propose::{}", mv.source),
        FailAction::WaitForFile(release.clone()),
    );

    let aborted = session
        .simple_query(&mv.transfer_sql())
        .await
        .expect_err("the move aborts once its barrier times out in the log");
    assert!(
        gate_dir.path().join("release.parked").exists(),
        "the source vShard read the item and held its broadcast: {}",
        db_detail(&aborted)
    );

    mv.wait_replicas_agree(None).await;
    assert_no_apply_halt(mv.cluster());

    let source_group = mv.group_of(&mv.source);
    let dest_group = mv.group_of(&mv.dest);
    let source_vshard = mv.source_vshard();
    let dest_vshard = mv.dest_vshard();
    for node in &mv.cluster().nodes {
        assert_eq!(
            reads_applied(node, source_vshard) + reads_applied(node, dest_vshard),
            0,
            "node {} applied no read result while the gate held the broadcast",
            node.node_id
        );
    }
    std::fs::write(&release, b"release").expect("release the held broadcast");
    // The released broadcast reaches both data-group logs. Every replica of
    // each group applies its entry for the finished txn and keeps neither a
    // buffered event nor a row of it.
    wait_for(
        "every replica applies the stale broadcast and drops it",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            mv.cluster().nodes.iter().all(|node| {
                let source_done = !node.replicates_data_group(source_group)
                    || reads_applied(node, source_vshard) >= 1;
                let dest_done = !node.replicates_data_group(dest_group)
                    || reads_applied(node, dest_vshard) >= 1;
                source_done
                    && dest_done
                    && [source_vshard, dest_vshard].iter().all(|vshard| {
                        waiting_barrier_txns(node, *vshard) == 0
                            && stored_barrier_rows(node, *vshard) == 0
                    })
            })
        },
    )
    .await;

    session
        .simple_query(&mv.transfer_sql())
        .await
        .unwrap_or_else(|e| panic!("the second move commits: {}", db_detail(&e)));
    mv.wait_replicas_agree(Some(&source_bytes)).await;
    assert_no_apply_halt(mv.cluster());

    let ItemMove { fx, .. } = mv;
    fx.cluster.shutdown().await;
}
