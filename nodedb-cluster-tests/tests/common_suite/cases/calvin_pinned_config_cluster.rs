// SPDX-License-Identifier: BUSL-1.1

//! A replica that installs a committed Calvin redo after it applied a later
//! typeguard derives the redo's rows under the config the proposer pinned.
//!
//! 1. A 3-node cluster caps a redo entry at 64 KiB. It holds a schemaless
//!    collection `a` and a keyed collection `b` on distinct vShards, with
//!    every data group on all three nodes.
//! 2. Follower `C` of `a`'s data group parks the first chunk entry it
//!    applies on its gate `redo_chunk::before_apply::0`. A chunk applies
//!    with nothing else of its group in flight, so every later entry of the
//!    group waits behind it on `C`.
//! 3. A strict cross-shard transaction writes many rows to `a` and one row
//!    to `b`. Its redo on `a`'s vShard passes the entry cap, so it travels as
//!    chunk entries. It commits on the other replicas and answers the client
//!    while `C` holds.
//! 4. A typeguard that derives `lowered` from `status` is added to `a`. `C`
//!    applies it through the metadata group, which the gate does not hold.
//! 5. Released, `C` installs the redo. Every replica of `a`, `C` included,
//!    stores the transaction's rows with no `lowered` field.
//! 6. A plain insert into `a` after the typeguard stores `lowered` on every
//!    replica, so the typeguard derives where no pin holds it back.
//!
//! Requires `--features failpoints`. File name contains "cluster" so nextest
//! applies the cluster test group.

#![cfg(feature = "failpoints")]

use std::collections::HashMap;
use std::time::Duration;

use nodedb::types::{DatabaseId, TenantId};
use nodedb_test_support::cluster_harness::snapshot_rows::current_document_rows;
use nodedb_test_support::fail_point::{FailAction, FailGuard};
use nodedb_types::Value;

use super::calvin_multishard_fixture::{Fixture, keyed_ddl, schemaless_ddl};
use super::calvin_replica_content::strict_session;
use super::vshard_names::distinct_vshard_collections;
use crate::common::cluster_harness::shared_steps::{db_detail, group_members, key_collection};
use crate::common::cluster_harness::{
    GroupLeader, TestCluster, TestClusterNode, wait_for, wait_for_async,
};

/// The tenant the `nodedb` pgwire user writes as.
const TENANT: u64 = 1;

/// The redo entry limit every node runs: the smallest the config takes.
const ENTRY_LIMIT: usize = 64 * 1024;

/// Rows one statement of the transaction inserts into `a`.
const ROWS_PER_STATEMENT: usize = 100;

/// Statements of the transaction that insert into `a`. With 400-byte pads
/// the redo on `a`'s vShard holds several times `ENTRY_LIMIT`.
const STATEMENTS: usize = 6;

/// Rows the transaction writes to `a`.
const TXN_ROWS: usize = STATEMENTS * ROWS_PER_STATEMENT;

/// How long each step gets.
const STEP_DEADLINE: Duration = Duration::from_secs(30);

/// Pause between two polls of a step.
const STEP_POLL: Duration = Duration::from_millis(50);

/// The `status` of every row the transaction writes.
const TXN_STATUS: &str = "OPEN";

/// The `status` of the row a plain insert writes after the typeguard.
const LATER_STATUS: &str = "SHUT";

/// The index in `cluster.nodes` of node `node_id`.
fn index_of(cluster: &TestCluster, node_id: u64) -> usize {
    cluster
        .nodes
        .iter()
        .position(|node| node.node_id == node_id)
        .unwrap_or_else(|| panic!("node {node_id} is live"))
}

/// The current rows of `collection` that `node` stores, decoded, from the
/// tenant snapshot of each of its cores. The read never leaves `node`.
async fn stored_rows(node: &TestClusterNode, collection: &str) -> Vec<HashMap<String, Value>> {
    let mut rows = Vec::new();
    for core in 0..node.num_cores() {
        let snapshot = node
            .tenant_snapshot_on_core(core, TenantId::new(TENANT))
            .await;
        for (key, body) in current_document_rows(&snapshot) {
            if key_collection(&key) != Some(collection) {
                continue;
            }
            // A body is a plain msgpack map, or the tagged native object
            // when a field's type needs its tag.
            let decoded = if body
                .first()
                .is_some_and(|&b| b & 0xf0 == 0x80 || b == 0xde || b == 0xdf)
            {
                nodedb_types::value_from_msgpack(&body).map_err(|e| e.to_string())
            } else {
                zerompk::from_msgpack::<Value>(&body).map_err(|e| e.to_string())
            };
            match decoded {
                Ok(Value::Object(fields)) => rows.push(fields),
                Ok(other) => panic!("row {key:?} of {collection} is not an object: {other:?}"),
                Err(e) => panic!("row {key:?} of {collection} does not decode: {e}"),
            }
        }
    }
    rows
}

/// The rows among `rows` whose `status` is `status`.
fn with_status(rows: Vec<HashMap<String, Value>>, status: &str) -> Vec<HashMap<String, Value>> {
    rows.into_iter()
        .filter(|row| row.get("status") == Some(&Value::String(status.to_owned())))
        .collect()
}

/// The `count` rows of `collection` whose `status` is `status`, as `node`
/// stores them, once `node` holds all of them.
async fn rows_with_status(
    node: &TestClusterNode,
    collection: &str,
    status: &str,
    count: usize,
) -> Vec<HashMap<String, Value>> {
    wait_for_async(
        &format!(
            "node {} stores {count} {collection} rows with status {status}",
            node.node_id
        ),
        STEP_DEADLINE,
        Duration::from_millis(100),
        || async move { with_status(stored_rows(node, collection).await, status).len() == count },
    )
    .await;
    with_status(stored_rows(node, collection).await, status)
}

/// Whether `node`'s catalog row of `collection` carries a typeguard.
fn has_typeguard(node: &TestClusterNode, collection: &str) -> bool {
    node.shared
        .credentials
        .catalog()
        .get_collection(DatabaseId::DEFAULT, TENANT, collection)
        .ok()
        .flatten()
        .is_some_and(|stored| !stored.type_guards.is_empty())
}

/// Commit one strict cross-shard transaction that writes `TXN_ROWS` rows to
/// `col_a` and one row to `col_b`.
async fn commit_large_transaction(client: &tokio_postgres::Client, col_a: &str, col_b: &str) {
    let pad = "x".repeat(400);
    client.simple_query("BEGIN").await.expect("BEGIN");
    for statement in 0..STATEMENTS {
        let values: Vec<String> = (0..ROWS_PER_STATEMENT)
            .map(|row| {
                let id = statement * ROWS_PER_STATEMENT + row;
                format!("('r{id}', '{TXN_STATUS}', '{pad}')")
            })
            .collect();
        let insert = format!(
            "INSERT INTO {col_a} (id, status, pad) VALUES {}",
            values.join(", ")
        );
        client
            .simple_query(&insert)
            .await
            .unwrap_or_else(|e| panic!("INSERT into {col_a}: {}", db_detail(&e)));
    }
    client
        .simple_query(&format!("INSERT INTO {col_b} (id, v) VALUES ('k2', 'b')"))
        .await
        .unwrap_or_else(|e| panic!("INSERT into {col_b}: {}", db_detail(&e)));
    client
        .simple_query("COMMIT")
        .await
        .unwrap_or_else(|e| panic!("COMMIT: {}", db_detail(&e)));
}

/// Every row of `rows` on `node` carries `lowered = expected`. A `None`
/// `expected` means no row carries `lowered`.
fn assert_lowered(node: &TestClusterNode, rows: &[HashMap<String, Value>], expected: Option<&str>) {
    let expected = expected.map(|value| Value::String(value.to_owned()));
    for row in rows {
        assert_eq!(
            row.get("lowered"),
            expected.as_ref(),
            "node {} stores {row:?}",
            node.node_id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_replica_installing_after_a_typeguard_stores_the_pinned_rows() {
    let cluster = TestCluster::spawn_three_with_redo_entry_limit(ENTRY_LIMIT)
        .await
        .expect("3-node cluster");
    let fx = Fixture::from_cluster(cluster).await;
    let (col_a, col_b) = distinct_vshard_collections("calvin_pinned_0", "calvin_pinned");
    fx.create(&[schemaless_ddl(&col_a), keyed_ddl(&col_b)])
        .await;
    fx.wait_group_mounted(&col_a).await;
    fx.wait_group_mounted(&col_b).await;
    wait_for(
        "every node replicates both collections' groups",
        STEP_DEADLINE,
        STEP_POLL,
        || {
            fx.cluster.nodes.iter().all(|node| {
                [&col_a, &col_b].iter().all(|coll| {
                    node.group_id_for_collection(coll)
                        .is_some_and(|group| node.replicates_data_group(group))
                })
            })
        },
    )
    .await;
    fx.cluster.wait_for_preferred_leaders(STEP_DEADLINE).await;

    let group = fx.cluster.nodes[0]
        .group_id_for_collection(&col_a)
        .expect("a's data group");
    let leader = fx
        .cluster
        .wait_for_group_leader(group, GroupLeader::Any, STEP_DEADLINE)
        .await;
    // The held node coordinates nothing: its held apply then delays only
    // the commit's redo.
    let coordinator_id = fx.coordinator().node_id;
    let held_id = group_members(&fx.cluster.nodes[0], group)
        .into_iter()
        .find(|member| *member != leader && *member != coordinator_id)
        .expect("a's group has a follower that does not coordinate");
    let held = &fx.cluster.nodes[index_of(&fx.cluster, held_id)];

    let gate_dir = tempfile::tempdir().expect("gate directory");
    let release = gate_dir.path().join("chunk");
    let parked = gate_dir.path().join("chunk.parked");
    let gate = FailGuard::for_node(
        held_id,
        "redo_chunk::before_apply::0",
        FailAction::WaitForFile(release.clone()),
    );

    let session = strict_session(fx.coordinator()).await;
    commit_large_transaction(&session, &col_a, &col_b).await;
    wait_for(
        "the held node parks the redo's first chunk",
        STEP_DEADLINE,
        STEP_POLL,
        || parked.exists(),
    )
    .await;
    for node in fx
        .cluster
        .nodes
        .iter()
        .filter(|node| node.node_id != held_id)
    {
        rows_with_status(node, &col_a, TXN_STATUS, TXN_ROWS).await;
    }

    let typeguard = format!("CREATE TYPEGUARD ON {col_a} (lowered STRING VALUE LOWER(status))");
    fx.cluster
        .exec_ddl_on_any_leader(&typeguard)
        .await
        .unwrap_or_else(|e| panic!("{typeguard}: {e}"));
    wait_for(
        "every node sees the typeguard",
        STEP_DEADLINE,
        STEP_POLL,
        || {
            fx.cluster
                .nodes
                .iter()
                .all(|node| has_typeguard(node, &col_a))
        },
    )
    .await;
    assert!(
        stored_rows(held, &col_a).await.is_empty(),
        "node {held_id} installs no row of the held redo before the gate opens"
    );

    std::fs::write(&release, b"release").expect("release the parked chunk");
    drop(gate);

    let held_rows = rows_with_status(held, &col_a, TXN_STATUS, TXN_ROWS).await;
    assert_lowered(held, &held_rows, None);
    fx.converge().await;
    for node in &fx.cluster.nodes {
        let rows = rows_with_status(node, &col_a, TXN_STATUS, TXN_ROWS).await;
        assert_lowered(node, &rows, None);
    }

    let later = format!("INSERT INTO {col_a} (id, status) VALUES ('later', '{LATER_STATUS}')");
    fx.coordinator()
        .client
        .simple_query(&later)
        .await
        .unwrap_or_else(|e| panic!("{later}: {}", db_detail(&e)));
    fx.converge().await;
    let lowered = LATER_STATUS.to_lowercase();
    for node in &fx.cluster.nodes {
        let rows = rows_with_status(node, &col_a, LATER_STATUS, 1).await;
        assert_lowered(node, &rows, Some(&lowered));
    }

    fx.cluster.shutdown().await;
}
