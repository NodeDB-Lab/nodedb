// SPDX-License-Identifier: BUSL-1.1

//! A backup's consistent cut binds a remote source node too.
//!
//! The backup's coordinator snapshots every vShard from the leader of its
//! group. The test parks a write on the collection's group leader only, at
//! the fail gate `funnel::before_dispatch::node<N>::<collection>`, between
//! its record and its core. The coordinator is another node, so its own
//! replica applies the write and its own cut passes. The leader must take the
//! same cut before it snapshots: the backup waits until the parked write
//! applies there, and the envelope holds the row. After the purge settles on
//! every node, a restore brings the row back on every node.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use nodedb_types::backup_envelope::{DEFAULT_MAX_TOTAL_BYTES, parse_encrypted};
use nodedb_types::fail_point::{FailAction, FailGuard};

use crate::common;
use common::cluster_harness::wait::wait_for;
use common::cluster_harness::{TestCluster, TestClusterNode};

/// Fixed test KEK the cluster harness injects into every node.
const TEST_KEK: [u8; 32] = [0x42u8; 32];

const TENANT: u64 = 1;
const COLLECTION: &str = "rcut_docs";

/// How long the parked write and the backup must stay unfinished.
const PARKED_FOR: Duration = Duration::from_millis(1500);

fn db_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn drain_backup(client: &tokio_postgres::Client) -> Result<Vec<u8>, String> {
    let stream = client
        .copy_out(&format!("COPY (BACKUP TENANT {TENANT}) TO STDOUT"))
        .await
        .map_err(|e| db_detail(&e))?;
    let mut bytes = Vec::new();
    let mut stream = Box::pin(stream);
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.map_err(|e| db_detail(&e))?);
    }
    Ok(bytes)
}

async fn push_restore(client: &tokio_postgres::Client, envelope: Vec<u8>) -> Result<(), String> {
    let sink = client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({TENANT}) FROM STDIN"))
        .await
        .map_err(|e| db_detail(&e))?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(envelope))
        .await
        .map_err(|e| db_detail(&e))?;
    sink.as_mut()
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| db_detail(&e))
}

/// The leader of `group_id` in `node`'s routing table: the node a backup
/// coordinated there snapshots the group's vShards from.
fn routing_leader(node: &TestClusterNode, group_id: u64) -> u64 {
    node.shared
        .cluster_routing
        .as_ref()
        .and_then(|routing| {
            routing
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .group_info(group_id)
                .map(|info| info.leader)
        })
        .unwrap_or(0)
}

/// Whether `node` purged `collection`: its catalog row is gone and its WAL
/// tombstone is recorded, so the async purge ran on its Data Plane.
fn purged_on(node: &TestClusterNode, collection: &str) -> bool {
    let catalog = node.shared.credentials.catalog();
    let active = matches!(
        catalog.get_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, collection),
        Ok(Some(c)) if c.is_active
    );
    let tombstoned = catalog
        .load_wal_tombstones()
        .map(|set| {
            set.iter()
                .any(|(_, tenant, name, lsn)| tenant == TENANT && name == collection && lsn > 0)
        })
        .unwrap_or(false);
    !active && tombstoned
}

/// Whether the backup `envelope` holds a KV row of `collection` whose key
/// carries `key`.
fn envelope_holds_kv_row(envelope: &[u8], collection: &str, key: &[u8]) -> bool {
    let parsed =
        parse_encrypted(envelope, DEFAULT_MAX_TOTAL_BYTES, &TEST_KEK).expect("parse the envelope");
    parsed
        .sections
        .iter()
        .filter_map(|section| {
            zerompk::from_msgpack::<nodedb::types::TenantDataSnapshot>(&section.body).ok()
        })
        .flat_map(|snapshot| snapshot.kv_tables)
        .filter(|(name, _)| name == collection)
        .filter_map(|(_, rows)| zerompk::from_msgpack::<Vec<(Vec<u8>, Vec<u8>, u64)>>(&rows).ok())
        .flatten()
        .any(|(row_key, _, _)| row_key.windows(key.len()).any(|window| window == key))
}

async fn connect(pg_addr: std::net::SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=nodedb dbname=default",
        pg_addr.ip(),
        pg_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("connect a second client");
    tokio::spawn(connection);
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_waits_for_a_write_held_on_a_remote_source_node() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} (key STRING PRIMARY KEY, value STRING) \
             WITH (engine='kv')"
        ))
        .await
        .expect("CREATE COLLECTION");

    // The coordinator snapshots the collection's vShard from the group leader
    // its routing table names. Wait until every node's table names the
    // elected leader, so the coordinator picked below names it too.
    let group_id = cluster.nodes[0]
        .group_id_for_collection(COLLECTION)
        .expect("the collection's data group");
    let elected = || {
        cluster.nodes[0]
            .all_group_leaders()
            .into_iter()
            .find(|(group, _)| *group == group_id)
            .map_or(0, |(_, leader)| leader)
    };
    wait_for(
        "every routing table names the elected leader of the collection's group",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            let leader = elected();
            leader != 0
                && cluster
                    .nodes
                    .iter()
                    .all(|node| routing_leader(node, group_id) == leader)
        },
    )
    .await;
    let source = elected();
    let coordinator = cluster
        .nodes
        .iter()
        .position(|node| node.node_id != source)
        .expect("a node that is not the source");

    // Park the write on the source node only.
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-source-apply");
    let _gate = FailGuard::install(
        &format!("funnel::before_dispatch::node{source}::{COLLECTION}"),
        FailAction::WaitForFile(release.clone()),
    );

    let writer = connect(cluster.nodes[coordinator].pg_addr).await;
    let insert = tokio::spawn(async move {
        writer
            .simple_query(&format!(
                "INSERT INTO {COLLECTION} (key, value) VALUES ('held', 'x')"
            ))
            .await
            .map(|_| ())
            .map_err(|e| db_detail(&e))
    });
    tokio::time::sleep(PARKED_FOR).await;

    let backup_client = connect(cluster.nodes[coordinator].pg_addr).await;
    let backup = tokio::spawn(async move { drain_backup(&backup_client).await });
    tokio::time::sleep(PARKED_FOR).await;
    assert!(
        !backup.is_finished(),
        "the backup snapshotted node {source} before a write held there had its outcome"
    );

    std::fs::write(&release, b"release").expect("release the held apply");
    insert
        .await
        .expect("insert task")
        .unwrap_or_else(|e| panic!("insert: {e}"));
    let envelope = backup
        .await
        .expect("backup task")
        .unwrap_or_else(|e| panic!("backup: {e}"));

    assert!(
        envelope_holds_kv_row(&envelope, COLLECTION, b"held"),
        "the backup missed a write committed before its cut"
    );

    cluster
        .exec_ddl_on_any_leader(&format!("DROP COLLECTION {COLLECTION} PURGE"))
        .await
        .expect("purge the collection");
    // The purge reaches each node's Data Plane asynchronously. A purge that
    // lands after the restore would remove the restored row.
    wait_for(
        "every node purged the collection",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|node| purged_on(node, COLLECTION)),
    )
    .await;
    push_restore(&cluster.nodes[coordinator].client, envelope)
        .await
        .unwrap_or_else(|e| panic!("restore: {e}"));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
    for node in &cluster.nodes {
        let rows = node
            .client
            .simple_query(&format!(
                "SELECT value FROM {COLLECTION} WHERE key = 'held'"
            ))
            .await
            .unwrap_or_else(|e| panic!("read the restored row: {}", db_detail(&e)));
        let values: Vec<String> = rows
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
                _ => None,
            })
            .collect();
        assert_eq!(
            values,
            vec!["x".to_owned()],
            "node {} does not hold the restored row",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
