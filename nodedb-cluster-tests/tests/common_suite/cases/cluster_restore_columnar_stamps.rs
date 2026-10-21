// SPDX-License-Identifier: BUSL-1.1

//! A cluster RESTORE keeps the system time of every bitemporal columnar
//! version.
//!
//! One cluster writes two versions of one key into a bitemporal columnar
//! collection and records, for each version's `_ts_system`, the rows an
//! `AS OF` read at it returns. A fresh cluster restores the backup. Every
//! node then reads its own replica and answers each original stamp with the
//! same rows, before and after every node restarts.

use std::collections::BTreeSet;
use std::time::Duration;

use super::cluster_restore_documents_restart::{drain_backup, push_restore};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, read_once_a_leader_exists};

const COLL: &str = "crc_metrics";

/// Every row `sql` returns through `node`, its cells joined, sorted.
async fn rows(node: &TestClusterNode, sql: &str) -> Vec<String> {
    let messages = read_once_a_leader_exists(
        sql,
        Duration::from_secs(30),
        Duration::from_millis(100),
        || node.client.simple_query(sql),
    )
    .await;
    let mut out: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("NULL"))
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// Each `_ts_system` of the collection's versions on `node`, with the rows a
/// read `AS OF SYSTEM TIME` that stamp returns there.
async fn cut_view(node: &TestClusterNode) -> Vec<(String, Vec<String>)> {
    let stamps: BTreeSet<String> = rows(
        node,
        &format!("SELECT _ts_system FROM {COLL} AS OF SYSTEM TIME NULL"),
    )
    .await
    .into_iter()
    .collect();
    let mut view = Vec::with_capacity(stamps.len());
    for stamp in stamps {
        let at = rows(
            node,
            &format!("SELECT id, v FROM {COLL} AS OF SYSTEM TIME {stamp}"),
        )
        .await;
        view.push((stamp, at));
    }
    view
}

/// Every node of `cluster` reads its own replica and answers each original
/// stamp as the source did.
async fn assert_stamps_kept(cluster: &TestCluster, before: &[(String, Vec<String>)], stage: &str) {
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    for node in &cluster.nodes {
        node.client
            .simple_query("SET default_read_consistency = 'eventual'")
            .await
            .unwrap_or_else(|e| panic!("node {}: eventual reads: {e}", node.node_id));
        assert_eq!(
            cut_view(node).await,
            before,
            "{stage}, node {}: each restored version keeps its system time",
            node.node_id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_keeps_every_columnar_versions_system_time() {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    source
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} (id BIGINT PRIMARY KEY, v BIGINT) \
             WITH (engine='columnar', bitemporal=true)"
        ))
        .await
        .expect("CREATE COLLECTION");
    for sql in [
        format!("INSERT INTO {COLL} (id, v) VALUES (1, 10)"),
        format!("INSERT INTO {COLL} (id, v) VALUES (2, 5)"),
        // A declared primary key refuses a second INSERT of a current key,
        // bitemporal or not: the second version comes from an UPDATE.
        format!("UPDATE {COLL} SET v = 20 WHERE id = 1"),
    ] {
        source.nodes[0]
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        // Distinct milliseconds, so each version has its own cut.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    source
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let before = cut_view(&source.nodes[0]).await;
    assert_eq!(before.len(), 3, "three versions, three system times");
    assert_eq!(
        before.first().map(|(_, at)| at.clone()),
        Some(vec!["1|10".to_string()]),
        "the first cut reads the first version alone"
    );
    let backup = drain_backup(&source.nodes[0].client).await;
    source.shutdown().await;

    let target = TestCluster::spawn_three().await.expect("target cluster");
    push_restore(&target.nodes[1].client, backup).await;
    assert_stamps_kept(&target, &before, "after the restore").await;

    let target = target
        .restart_all()
        .await
        .unwrap_or_else(|e| panic!("restart every node: {e}"));
    assert_stamps_kept(&target, &before, "after every node restarted").await;

    target.shutdown().await;
}
