// SPDX-License-Identifier: BUSL-1.1

//! A transaction's UPDATE and DELETE on a bitemporal columnar collection keep
//! every version.
//!
//! The resolve stamps the transaction's rows with its one system time, raised
//! above each key's newest version, and the redo carries the stamps. Every
//! replica installs the new version of an updated key and the tombstone of a
//! deleted key at that stamp, and keeps the base rows as history. A rolled
//! back transaction installs nothing. A TRUNCATE, autocommit or inside a
//! transaction, closes every current key the same way.

use std::time::Duration;

use super::bitemporal_replica_stamps::{cut_view, rows, stored_stamps};
use crate::common::cluster_harness::{TestCluster, TestClusterNode};

const COLL: &str = "btx_cols";
const TRUNC: &str = "btx_trunc";

/// Run `statements` through one session of `node`, panicking on an error.
async fn session_run(node: &TestClusterNode, statements: &[String]) {
    for sql in statements {
        node.client
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("node {}: {sql}: {e}", node.node_id));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_transaction_keeps_every_columnar_version_it_supersedes() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} (id BIGINT PRIMARY KEY, v BIGINT) \
             WITH (engine='columnar', bitemporal=true)"
        ))
        .await
        .expect("CREATE COLLECTION");
    let writer = &cluster.nodes[2];
    for sql in [
        format!("INSERT INTO {COLL} (id, v) VALUES (1, 10)"),
        format!("INSERT INTO {COLL} (id, v) VALUES (2, 20)"),
    ] {
        writer
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        // Distinct milliseconds, so each version has its own cut.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let seeded = stored_stamps(&cluster.nodes[0], COLL).await;
    assert_eq!(seeded.len(), 2, "two inserted versions: {seeded:?}");

    let txn = |end: &str| {
        vec![
            "BEGIN".to_string(),
            format!("UPDATE {COLL} SET v = 11 WHERE id = 1"),
            format!("DELETE FROM {COLL} WHERE id = 2"),
            end.to_string(),
        ]
    };

    // A rolled back transaction leaves the history as it was.
    session_run(writer, &txn("ROLLBACK")).await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    for node in &cluster.nodes {
        assert_eq!(
            stored_stamps(node, COLL).await,
            seeded,
            "node {}: a rollback installs no version",
            node.node_id
        );
    }

    tokio::time::sleep(Duration::from_millis(5)).await;
    session_run(writer, &txn("COMMIT")).await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let first = stored_stamps(&cluster.nodes[0], COLL).await;
    assert_eq!(
        first.len(),
        4,
        "both inserted versions, id 1's new version and id 2's tombstone: {first:?}"
    );
    assert_eq!(first[..2], seeded[..], "the base versions stay");
    assert_eq!(
        first[2], first[3],
        "the transaction's versions share its stamp"
    );
    assert!(
        first[2] > first[1],
        "the transaction stamps after the base: {first:?}"
    );
    for node in &cluster.nodes[1..] {
        assert_eq!(
            stored_stamps(node, COLL).await,
            first,
            "node {}: every version stamps the ordinal node {} stamps",
            node.node_id,
            cluster.nodes[0].node_id
        );
    }

    for node in &cluster.nodes {
        node.client
            .simple_query("SET default_read_consistency = 'eventual'")
            .await
            .unwrap_or_else(|e| panic!("node {}: eventual reads: {e}", node.node_id));
        assert_eq!(
            rows(node, &format!("SELECT id, v FROM {COLL}")).await,
            vec!["1|11".to_string()],
            "node {}: the current state after the commit",
            node.node_id
        );
        let view = cut_view(node, COLL, "id, v").await;
        assert_eq!(
            view.into_iter().map(|(_, at)| at).collect::<Vec<_>>(),
            vec![
                vec!["1|10".to_string()],
                vec!["1|10".to_string(), "2|20".to_string()],
                vec!["1|11".to_string()],
            ],
            "node {}: each cut returns the state at its stamp",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

/// A TRUNCATE of a bitemporal columnar collection closes every current key
/// with a tombstone at one ordinal and keeps every version, autocommit and
/// inside a transaction alike. Every replica stores the same ordinals, and a
/// cut before each truncate returns the rows it closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_truncate_closes_every_columnar_key_and_keeps_history() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {TRUNC} (id BIGINT PRIMARY KEY, v BIGINT) \
             WITH (engine='columnar', bitemporal=true)"
        ))
        .await
        .expect("CREATE COLLECTION");
    let writer = &cluster.nodes[2];
    let pause = || tokio::time::sleep(Duration::from_millis(5));
    for sql in [
        format!("INSERT INTO {TRUNC} (id, v) VALUES (1, 10)"),
        format!("INSERT INTO {TRUNC} (id, v) VALUES (2, 20)"),
        format!("TRUNCATE {TRUNC}"),
        format!("INSERT INTO {TRUNC} (id, v) VALUES (3, 30)"),
    ] {
        writer
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        pause().await;
    }
    session_run(
        writer,
        &[
            "BEGIN".to_string(),
            format!("TRUNCATE {TRUNC}"),
            "COMMIT".to_string(),
        ],
    )
    .await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let first = stored_stamps(&cluster.nodes[0], TRUNC).await;
    assert_eq!(
        first.len(),
        6,
        "three versions and three tombstones: {first:?}"
    );
    for node in &cluster.nodes[1..] {
        assert_eq!(
            stored_stamps(node, TRUNC).await,
            first,
            "node {}: every version stamps the ordinal node {} stamps",
            node.node_id,
            cluster.nodes[0].node_id
        );
    }

    for node in &cluster.nodes {
        node.client
            .simple_query("SET default_read_consistency = 'eventual'")
            .await
            .unwrap_or_else(|e| panic!("node {}: eventual reads: {e}", node.node_id));
        assert_eq!(
            rows(node, &format!("SELECT id, v FROM {TRUNC}")).await,
            Vec::<String>::new(),
            "node {}: no current row after the truncates",
            node.node_id
        );
        let view = cut_view(node, TRUNC, "id, v").await;
        assert_eq!(
            view.into_iter().map(|(_, at)| at).collect::<Vec<_>>(),
            vec![
                vec!["1|10".to_string()],
                vec!["1|10".to_string(), "2|20".to_string()],
                Vec::new(),
                vec!["3|30".to_string()],
                Vec::new(),
            ],
            "node {}: a cut before each truncate returns the rows it closed",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
