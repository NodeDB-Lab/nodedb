// SPDX-License-Identifier: BUSL-1.1

//! A Calvin participant on another node reports its apply to the
//! coordinator through its completion ack.
//!
//! With a replication factor of 1, each data group lives on one node. The
//! client's session runs on the sequencer leader, which coordinates every
//! Calvin transaction. Each collection below lives on another node, so the
//! coordinator holds no replica of the participant, and no local apply
//! result reaches it. Only the participant's completion ack does.
//!
//! - A strict cross-shard transaction writes a timeseries row and a
//!   document. The transaction parks after its ingest resolved. A write on
//!   the owner then gives the ingest's new column another type, so the
//!   ingest's install rejects its row at its log position. The COMMIT
//!   reports that rejected row.
//!
//!   A fresh column comes only from a raw ILP line (see `ts_native_ingest`).
//!   So the collection declares only its `ts` time key, and the
//!   transaction's line and the owner's line both go through the native
//!   `TimeseriesIngest` opcode.
//! - A `DELETE ... RETURNING` on an edge-bearing collection commits through
//!   the dependent Calvin path. Its reply carries the deleted row.
//!
//! File name contains "calvin" so nextest applies the cluster test-group
//! serialization.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use crate::common;
use common::cluster_harness::shared_steps::{fail_stopped, sequencer_admitted};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use nodedb_test_support::fail_point::{FailAction, FailGuard};
use nodedb_test_support::native_harness::send_sql;
use nodedb_types::{DatabaseId, TenantId};

use super::ts_native_ingest::{
    assert_native_ok, ingest_native, native_session, rejection_warnings,
};
use super::vshard_names::distinct_vshard_collections;

const TENANT: u64 = 1;
const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(50);
/// Candidate collection names tried before giving up.
const MAX_TRIES: u32 = 512;
/// Implicit-edge documents seeded before the `RETURNING` delete.
const SOURCES: usize = 4;

fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn exec(node: &TestClusterNode, sql: &str) -> Vec<tokio_postgres::SimpleQueryMessage> {
    node.client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {}", pg_detail(&e)))
}

/// Whether `collection` is edge-bearing in `node`'s local catalog.
fn edge_bearing(node: &TestClusterNode, collection: &str) -> bool {
    node.shared
        .credentials
        .catalog()
        .load_collections_for_tenant(DatabaseId::DEFAULT, TENANT)
        .map(|collections| {
            collections
                .iter()
                .any(|c| c.name == collection && c.has_implicit_edges)
        })
        .unwrap_or(false)
}

/// The node that alone replicates the data group of `collection`, once
/// placement converged to one replica.
async fn owner_of<'a>(cluster: &'a TestCluster, collection: &str) -> &'a TestClusterNode {
    let group_id = cluster.nodes[0]
        .group_id_for_collection(collection)
        .unwrap_or_else(|| panic!("the data group of {collection}"));
    wait_for(
        &format!("exactly one node replicates group {group_id}"),
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .filter(|node| node.replicates_data_group(group_id))
                .count()
                == 1
        },
    )
    .await;
    cluster
        .nodes
        .iter()
        .find(|node| node.replicates_data_group(group_id))
        .unwrap_or_else(|| panic!("the replica of group {group_id}"))
}

/// A collection name `{prefix}_{i}` whose data group lives on a node other
/// than `coordinator`.
async fn collection_off(cluster: &TestCluster, coordinator: u64, prefix: &str) -> String {
    for i in 0..MAX_TRIES {
        let name = format!("{prefix}_{i}");
        if owner_of(cluster, &name).await.node_id != coordinator {
            return name;
        }
    }
    panic!("no collection under {prefix} lives off node {coordinator} in {MAX_TRIES} tries");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn remote_participants_report_counts_and_rows_through_their_acks() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("spawn 3-node cluster");
    wait_for(
        "every node sees one sequencer leader",
        CONVERGE,
        STEP,
        || {
            let leader = cluster.nodes[0].sequencer_leader();
            leader != 0 && cluster.nodes.iter().all(|n| n.sequencer_leader() == leader)
        },
    )
    .await;
    let leader_id = cluster.nodes[0].sequencer_leader();
    let coordinator = cluster
        .nodes
        .iter()
        .find(|node| node.node_id == leader_id)
        .expect("the sequencer leader is a cluster node");

    remote_ingest_reports_its_rejected_row(&cluster, coordinator).await;
    remote_delete_returns_its_row(&cluster, coordinator).await;

    for node in &cluster.nodes {
        assert!(
            !fail_stopped(node),
            "node {} fail-stopped a core",
            node.node_id
        );
    }
    cluster.shutdown().await;
}

/// The timeseries participant's install rejects a row its resolve accepted.
/// The COMMIT on the coordinator reports it.
async fn remote_ingest_reports_its_rejected_row(
    cluster: &TestCluster,
    coordinator: &TestClusterNode,
) {
    let series = collection_off(cluster, coordinator.node_id, "remote_report_ts").await;
    let (_, documents) = distinct_vshard_collections(&series, "remote_report_doc");
    let owner = owner_of(cluster, &series).await;

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {series} (ts BIGINT TIME_KEY) WITH (engine='timeseries')"
        ))
        .await
        .expect("create the timeseries collection");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {documents}"))
        .await
        .expect("create the document collection");
    wait_for("every node sees both collections", CONVERGE, STEP, || {
        cluster
            .nodes
            .iter()
            .all(|n| n.cached_collection_count() >= 2)
    })
    .await;
    let mut session = native_session(coordinator).await;
    assert_native_ok(
        &ingest_native(
            &mut session,
            1,
            &series,
            &format!("{series} value=0.5 1000000000"),
        )
        .await,
        "the warm-up ingest",
    );

    // The transaction parks after its ingest resolved `extra` as a float,
    // before the sequencer admits it.
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-commit");
    let parked = gate_dir.path().join("release-commit.parked");
    let gate = FailGuard::install(
        &format!("calvin::after_stamp::{series}"),
        FailAction::WaitForFile(release.clone()),
    );

    assert_native_ok(
        &send_sql(&mut session, 2, "SET cross_shard_txn = 'strict'").await,
        "SET cross_shard_txn",
    );
    let admitted_before = sequencer_admitted(coordinator);
    assert_native_ok(&send_sql(&mut session, 3, "BEGIN").await, "BEGIN");
    // The transaction's line gives `extra`, which no schema holds yet, a
    // float.
    assert_native_ok(
        &ingest_native(
            &mut session,
            4,
            &series,
            &format!("{series} value=1.0,extra=1.5 2000000000"),
        )
        .await,
        "the staged ingest",
    );
    assert_native_ok(
        &send_sql(
            &mut session,
            5,
            &format!("INSERT INTO {documents} {{ id: 'd-1', n: 1 }}"),
        )
        .await,
        "the staged document insert",
    );
    let commit = tokio::spawn(async move { send_sql(&mut session, 6, "COMMIT").await });
    wait_for(
        "the transaction parks after its stamp",
        CONVERGE,
        STEP,
        || parked.exists(),
    )
    .await;

    // The owner's replica types `extra` as a string.
    let mut on_owner = native_session(owner).await;
    assert_native_ok(
        &ingest_native(
            &mut on_owner,
            1,
            &series,
            &format!("{series} value=2.0,extra=\"text\" 3000000000"),
        )
        .await,
        "the owner's ingest",
    );
    std::fs::write(&release, b"release").expect("release the transaction");
    let committed = commit.await.expect("transaction task");
    drop(gate);

    assert_native_ok(&committed, "COMMIT");
    assert!(
        sequencer_admitted(coordinator) > admitted_before,
        "the cross-shard COMMIT is sequenced through Calvin"
    );
    let notices = rejection_warnings(&committed, &series);
    assert!(
        notices.iter().any(|notice| notice.contains("1 line(s)")),
        "the COMMIT reports the row the remote install rejected, got {notices:?}"
    );
    assert!(
        !coordinator.replicates_data_group(
            coordinator
                .group_id_for_collection(&series)
                .expect("the series data group")
        ),
        "the coordinator holds no replica of the timeseries participant"
    );

    // The owner stores the warm-up row and the string row. The transaction's
    // row never lands.
    wait_for_stored_rows(owner, &series, 2).await;
}

/// Wait until `node`'s own replica of `collection` stores `expected` rows,
/// then check it stores exactly that many.
async fn wait_for_stored_rows(node: &TestClusterNode, collection: &str, expected: usize) {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    let mut rows = Vec::new();
    while tokio::time::Instant::now() < deadline {
        rows = node
            .timeseries_rows_local(TenantId::new(TENANT), collection)
            .await;
        if rows.len() == expected {
            break;
        }
        tokio::time::sleep(STEP).await;
    }
    assert_eq!(
        rows.len(),
        expected,
        "node {} stores {rows:?} of {collection}",
        node.node_id
    );
}

/// The document participant's `RETURNING` rows reach the coordinator only
/// through its completion ack.
async fn remote_delete_returns_its_row(cluster: &TestCluster, coordinator: &TestClusterNode) {
    let documents = collection_off(cluster, coordinator.node_id, "remote_report_edges").await;
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {documents} WITH (engine='document_schemaless')"
        ))
        .await
        .expect("create the edge-bearing collection");
    wait_for("every node sees the collection", CONVERGE, STEP, || {
        cluster
            .nodes
            .iter()
            .all(|n| n.cached_collection_count() >= 3)
    })
    .await;
    for i in 0..SOURCES {
        exec(
            coordinator,
            &format!(
                "INSERT INTO {documents} {{ id: 'edge_{i}', _from: 'src_{i}', _to: 'hub', _type: 'l' }}"
            ),
        )
        .await;
    }
    wait_for("the collection is edge-bearing", CONVERGE, STEP, || {
        edge_bearing(coordinator, &documents)
    })
    .await;

    let admitted_before = sequencer_admitted(coordinator);
    let messages = exec(
        coordinator,
        &format!("DELETE FROM {documents} WHERE id = 'edge_2' RETURNING *"),
    )
    .await;
    assert!(
        sequencer_admitted(coordinator) > admitted_before,
        "the RETURNING delete commits through Calvin"
    );
    let ids: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get("id").map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        ids,
        vec!["edge_2".to_owned()],
        "the delete answers the row its remote participant deleted"
    );
}
