// SPDX-License-Identifier: BUSL-1.1

//! A cross-shard write whose value depends on a row of another vShard
//! commits on every replica as one dependent-read Calvin transaction.
//!
//! `SELECT TRANSFER_ITEM(src, dst, 'sword', 'alice', 'bob')` moves an item
//! between key-value collections on two vShards. The source vShard reads the
//! item under the transaction's locks and broadcasts it through the
//! data-group log of both active vShards. Every replica of the source then
//! lacks the item, and every replica of the destination holds the bytes the
//! source held. A second move of the same item finds it gone.

use super::calvin_dependent_read_fixture::{ItemMove, assert_no_apply_halt};
use super::calvin_replica_content::strict_session;
use crate::common::cluster_harness::shared_steps::db_detail;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_cross_shard_item_move_commits_on_every_replica() {
    let mv = ItemMove::spawn("dep_commit").await;
    let source_bytes = mv.source_bytes().await;
    let session = strict_session(mv.fx.coordinator()).await;

    let reply = session
        .simple_query(&mv.transfer_sql())
        .await
        .unwrap_or_else(|e| panic!("the move commits: {}", db_detail(&e)));
    let text: Vec<String> = reply
        .into_iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(text.len(), 1, "the move answers one row");
    assert!(
        text[0].contains("bob:sword") && text[0].contains(&mv.dest),
        "the answer names the item's new home: {}",
        text[0]
    );

    mv.wait_replicas_agree(Some(&source_bytes)).await;

    let again = session
        .simple_query(&mv.transfer_sql())
        .await
        .expect_err("the item left the source");
    let detail = db_detail(&again).to_lowercase();
    assert!(
        detail.contains("not found") || detail.contains("not_found") || detail.contains("22023"),
        "a second move reports the item missing, got: {detail}"
    );

    assert_no_apply_halt(mv.cluster());
    let ItemMove { fx, .. } = mv;
    fx.cluster.shutdown().await;
}
