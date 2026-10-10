// SPDX-License-Identifier: BUSL-1.1

//! A cluster rewinds to a restore point with `nodedb restore --cluster`.
//!
//! Three nodes share one cold store and one snapshot store. Rows land in
//! collections spread over both data groups, every node takes a base, more
//! rows land, and a restore point is taken. After the point, more rows land
//! and a collection is created. Every node stops, restores its data
//! directory to the point, and starts. Each node's own replica then holds
//! exactly the rows written before the point, the later collection is gone,
//! and the cluster takes new writes.

use std::time::Duration;

use crate::common;
use common::cluster_harness::shared_steps::db_detail;
use common::cluster_harness::{
    PitrStorage, TestCluster, TestClusterNode, read_once_a_leader_exists,
};

const COLLECTIONS: &[&str] = &["pitr_a", "pitr_b", "pitr_c", "pitr_d"];

/// The first column of every row `sql` returns on `node`, sorted.
async fn column(node: &TestClusterNode, sql: &str) -> Vec<String> {
    let messages = read_once_a_leader_exists(
        sql,
        Duration::from_secs(30),
        Duration::from_millis(100),
        || node.client.simple_query(sql),
    )
    .await;
    let mut values: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    values.sort();
    values
}

async fn insert_rows(cluster: &TestCluster, prefix: &str) {
    for collection in COLLECTIONS {
        for i in 0..3 {
            let sql = format!("INSERT INTO {collection} (id, v) VALUES ('{prefix}-{i}', {i})");
            cluster.nodes[0]
                .exec(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }
}

fn ids(prefixes: &[&str]) -> Vec<String> {
    let mut ids: Vec<String> = prefixes
        .iter()
        .flat_map(|prefix| (0..3).map(move |i| format!("{prefix}-{i}")))
        .collect();
    ids.sort();
    ids
}

/// Take a restore point through `node` and return its id.
async fn create_restore_point(node: &TestClusterNode) -> u64 {
    let messages = node
        .client
        .simple_query("CREATE RESTORE POINT")
        .await
        .unwrap_or_else(|e| panic!("CREATE RESTORE POINT: {}", db_detail(&e)));
    messages
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .and_then(|id| id.parse().ok())
        .unwrap_or_else(|| panic!("CREATE RESTORE POINT returned no id: {messages:?}"))
}

/// A PITR cluster with every collection created, rows `before-base`
/// written, and a base taken on every node.
async fn cluster_with_bases(pitr: &PitrStorage) -> TestCluster {
    let cluster = TestCluster::spawn_three_with_pitr(pitr.clone())
        .await
        .expect("cluster");
    for collection in COLLECTIONS {
        create_collection(&cluster, collection).await;
    }
    insert_rows(&cluster, "before-base").await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    for node in &cluster.nodes {
        nodedb::control::pitr::take_base_now(&node.shared)
            .await
            .unwrap_or_else(|e| panic!("node {}: base: {e}", node.node_id));
    }
    cluster
}

async fn create_collection(cluster: &TestCluster, collection: &str) {
    let sql = format!(
        "CREATE COLLECTION {collection} (id STRING PRIMARY KEY, v INT) \
         WITH (engine='document_strict')"
    );
    cluster
        .exec_ddl_on_any_leader(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Wait until every node archived `point`, stop every node, restore each to
/// the point, and start them.
async fn restore_every_node(cluster: TestCluster, pitr: &PitrStorage, point: u64) -> TestCluster {
    for node in &cluster.nodes {
        pitr.await_point_archived(node, point)
            .await
            .unwrap_or_else(|e| panic!("node {}: {e}", node.node_id));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let stopped = cluster.stop_all().await.expect("stop every node");
    for node in stopped.nodes() {
        let report = pitr
            .restore_node(&node, point)
            .await
            .unwrap_or_else(|e| panic!("node {}: restore: {e}", node.node_id));
        assert!(
            report.contains("restore complete"),
            "node {}: {report}",
            node.node_id
        );
    }
    let cluster = stopped.start_all().await.expect("start every node");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    for node in &cluster.nodes {
        node.client
            .simple_query("SET default_read_consistency = 'eventual'")
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "node {}: set eventual reads: {}",
                    node.node_id,
                    db_detail(&e)
                )
            });
    }
    cluster
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_restores_every_group_to_a_restore_point() {
    let root = tempfile::tempdir().expect("storage root");
    let pitr = PitrStorage::create(root.path()).expect("PITR storage");
    let cluster = cluster_with_bases(&pitr).await;

    insert_rows(&cluster, "before-point").await;
    let point = create_restore_point(&cluster.nodes[0]).await;
    for node in &cluster.nodes {
        pitr.await_point_archived(node, point)
            .await
            .unwrap_or_else(|e| panic!("node {}: {e}", node.node_id));
    }
    insert_rows(&cluster, "after-point").await;
    create_collection(&cluster, "pitr_late").await;

    let cluster = restore_every_node(cluster, &pitr, point).await;
    let expected = ids(&["before-base", "before-point"]);
    for node in &cluster.nodes {
        let id = node.node_id;
        for collection in COLLECTIONS {
            assert_eq!(
                column(node, &format!("SELECT id FROM {collection}")).await,
                expected,
                "node {id}: {collection} holds exactly the rows written before the point"
            );
        }
        assert!(
            node.exec("SELECT id FROM pitr_late").await.is_err(),
            "node {id}: the collection created after the point is gone"
        );
    }

    cluster.nodes[1]
        .exec("INSERT INTO pitr_a (id, v) VALUES ('after-restore', 9)")
        .await
        .unwrap_or_else(|e| panic!("a write after the restore: {e}"));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let mut with_new = expected.clone();
    with_new.push("after-restore".into());
    with_new.sort();
    for node in &cluster.nodes {
        assert_eq!(
            column(node, "SELECT id FROM pitr_a").await,
            with_new,
            "node {}: the restored cluster replicates a new write",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

/// A DDL issued after the point's watermark and applied before the point's
/// metadata entry is absent after the restore, and so are its rows. The
/// point parks at the gate `restore_point::after_watermark` after it takes
/// the watermark. The test issues the DDL while the point is parked, then
/// releases it, so the DDL lands first in the metadata log.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ddl_after_the_watermark_is_absent_though_it_precedes_the_point() {
    use nodedb_test_support::fail_point::{FailAction, FailGuard};

    let root = tempfile::tempdir().expect("storage root");
    let pitr = PitrStorage::create(root.path()).expect("PITR storage");
    let cluster = cluster_with_bases(&pitr).await;

    let gate_dir = tempfile::tempdir().expect("gate dir");
    let release = gate_dir.path().join("release-point");
    let parked = gate_dir.path().join("release-point.parked");
    let hold = FailGuard::install(
        "restore_point::after_watermark",
        FailAction::WaitForFile(release.clone()),
    );
    let points_before = column(&cluster.nodes[0], "SHOW RESTORE POINTS").await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        cluster.nodes[0].pg_addr.port()
    );
    let creator = tokio::spawn(async move {
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("connect for the restore point");
        tokio::spawn(connection);
        let messages = client
            .simple_query("CREATE RESTORE POINT")
            .await
            .unwrap_or_else(|e| panic!("CREATE RESTORE POINT: {}", db_detail(&e)));
        messages
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
                _ => None,
            })
            .and_then(|id| id.parse::<u64>().ok())
            .expect("CREATE RESTORE POINT returns its id")
    });
    // The point took its watermark and is parked. This DDL and its rows
    // carry later HLCs and apply first.
    tokio::time::timeout(Duration::from_secs(30), async {
        while !parked.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the restore point reaches its gate");
    create_collection(&cluster, "pitr_mid").await;
    for i in 0..3 {
        let sql = format!("INSERT INTO pitr_mid (id, v) VALUES ('mid-{i}', {i})");
        cluster.nodes[0]
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert!(
        !creator.is_finished(),
        "the point stays parked while the DDL and its rows apply"
    );
    assert_eq!(
        column(&cluster.nodes[0], "SHOW RESTORE POINTS").await,
        points_before,
        "the parked point has not proposed its entry"
    );
    std::fs::write(&release, b"").expect("release the restore point");
    let point = creator.await.expect("restore point task");
    drop(hold);

    let cluster = restore_every_node(cluster, &pitr, point).await;
    let expected = ids(&["before-base"]);
    for node in &cluster.nodes {
        let id = node.node_id;
        assert!(
            node.exec("SELECT id FROM pitr_mid").await.is_err(),
            "node {id}: the collection created after the watermark is gone"
        );
        assert_eq!(
            column(node, "SELECT id FROM pitr_a").await,
            expected,
            "node {id}: the collections created before the watermark hold their rows"
        );
    }
    // A same-name collection starts empty: no row of the dropped DDL came
    // back in any engine.
    create_collection(&cluster, "pitr_mid").await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    for node in &cluster.nodes {
        assert!(
            column(node, "SELECT id FROM pitr_mid").await.is_empty(),
            "node {}: no row written after the watermark survives",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
