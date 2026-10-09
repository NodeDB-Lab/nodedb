// SPDX-License-Identifier: BUSL-1.1

//! An autocommit write and a Calvin cross-shard transaction on one row of a
//! replicated vShard leave the same final row on every replica.
//!
//! The data-group leader admits every replicated write through its write
//! gate before the propose. The gate takes the write's row key and its
//! collection `Intent` on the vShard's Calvin lock table, so a Calvin
//! transaction on the same row orders before or after the write on the
//! leader, never between its propose and its apply start. Followers apply
//! the committed entry in log order.
//!
//! Each round races a cross-shard transaction that writes the row against an
//! autocommit write of the row from another node. After the last round every
//! replica that hosts the row's group must hold the same image of it.

use nodedb::types::{TenantDataSnapshot, TenantId};

use super::calvin_multishard_fixture::{Fixture, schemaless_ddl};
use super::vshard_names::distinct_vshard_collections;
use crate::common::cluster_harness::TestClusterNode;
use crate::common::cluster_harness::shared_steps::key_collection;
use crate::common::pgwire_harness::raw_pgwire::RawPgConn;

const TENANT: u64 = 1;
const ROUNDS: usize = 8;

/// Whether the server answered `messages` with an error.
fn failed(messages: &[(u8, Vec<u8>)]) -> bool {
    messages.iter().any(|(tag, _)| *tag == b'E')
}

/// Run one cross-shard transaction that writes the row `r` of `rows` and the
/// row `s` of `other`. A refused statement or commit rolls the transaction
/// back: the race may abort it, and convergence is the property under test.
async fn calvin_round(conn: &mut RawPgConn, rows: &str, other: &str, round: usize) {
    let statements = [
        "BEGIN".to_owned(),
        format!("UPDATE {rows} SET v = 'calvin_{round}' WHERE id = 'r'"),
        format!("UPDATE {other} SET v = 'calvin_{round}' WHERE id = 's'"),
        "COMMIT".to_owned(),
    ];
    for sql in &statements {
        if failed(&conn.simple_query(sql).await) {
            conn.simple_query("ROLLBACK").await;
            return;
        }
    }
}

/// Every stored row image of `collection` on `node`, from its own tenant
/// snapshot.
async fn local_row_images(node: &TestClusterNode, collection: &str) -> Vec<serde_json::Value> {
    let bytes = node.create_tenant_snapshot(TenantId::new(TENANT)).await;
    let snapshot: TenantDataSnapshot = zerompk::from_msgpack(&bytes).unwrap_or_default();
    snapshot
        .documents
        .into_iter()
        .filter(|(key, _)| key_collection(key) == Some(collection))
        .map(|(key, value)| {
            nodedb_types::json_from_msgpack(&value)
                .unwrap_or_else(|e| panic!("stored row {key} decodes: {e}"))
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn autocommit_and_calvin_writes_of_one_row_converge_on_every_replica() {
    let (rows, other) = distinct_vshard_collections("wag_rows", "wag_other");
    let fx = Fixture::spawn(&[schemaless_ddl(&rows), schemaless_ddl(&other)]).await;
    fx.wait_group_mounted(&rows).await;
    fx.wait_group_mounted(&other).await;

    let mut seed = fx.raw().await;
    for sql in [
        format!("INSERT INTO {rows} {{ id: 'r', v: 'seed' }}"),
        format!("INSERT INTO {other} {{ id: 's', v: 'seed' }}"),
    ] {
        assert!(!failed(&seed.simple_query(&sql).await), "seed: {sql}");
    }
    fx.converge().await;

    // The autocommit writer runs on a node other than the transaction's
    // coordinator, so its write reaches the row's leader on its own route.
    let writer = (fx.coordinator + 1) % fx.cluster.nodes.len();
    let mut txn_conn = fx.raw().await;
    for round in 0..ROUNDS {
        let autocommit = async {
            // A refused autocommit leaves the row as the transaction wrote
            // it; convergence still holds.
            let _ = fx.cluster.nodes[writer]
                .client
                .simple_query(&format!(
                    "UPDATE {rows} SET v = 'auto_{round}' WHERE id = 'r'"
                ))
                .await;
        };
        tokio::join!(
            calvin_round(&mut txn_conn, &rows, &other, round),
            autocommit
        );
    }
    fx.converge().await;

    let group_id = fx.cluster.nodes[0]
        .group_id_for_collection(&rows)
        .expect("the row's collection maps to a data group");
    let mut images = Vec::new();
    for (idx, node) in fx.cluster.nodes.iter().enumerate() {
        if !node.hosts_data_group(group_id) {
            continue;
        }
        let node_images = local_row_images(node, &rows).await;
        assert_eq!(
            node_images.len(),
            1,
            "node {idx} holds exactly the one row: {node_images:?}"
        );
        images.push((idx, node_images));
    }
    assert!(images.len() > 1, "the row's group has several replicas");
    let (first_idx, first) = &images[0];
    for (idx, image) in &images[1..] {
        assert_eq!(
            image, first,
            "replica {idx} diverged from replica {first_idx}"
        );
    }
}
