// SPDX-License-Identifier: BUSL-1.1

//! A cluster RESTORE re-issues document rows, their index entries, and graph
//! edges as replicated writes, so every replica holds them durably.
//!
//! One cluster backs up a schemaless collection with a secondary index and a
//! unique index, a strict collection, a strict `bitemporal=true` collection
//! with two versions of one row, and two edges. A second cluster restores the
//! backup through one node, then every node restarts. Before and after the
//! restart, each node reads its own replica: every row, both index lookups,
//! both versions, and both edges.

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};

use crate::common;
use common::cluster_harness::{TestCluster, TestClusterNode, read_once_a_leader_exists};

const TENANT: u64 = 1;

const COLLECTIONS: &[&str] = &[
    "CREATE COLLECTION crd_people (id STRING PRIMARY KEY, city STRING, email STRING) \
     WITH (engine='document_schemaless')",
    "CREATE COLLECTION crd_accounts (id STRING PRIMARY KEY, owner STRING, balance INT) \
     WITH (engine='document_strict')",
    "CREATE COLLECTION crd_ledger (id STRING PRIMARY KEY, value STRING) \
     WITH (engine='document_strict', bitemporal=true)",
    "CREATE INDEX ON crd_people (city)",
    "CREATE UNIQUE INDEX crd_people_email ON crd_people (email)",
];

const WRITES: &[&str] = &[
    "INSERT INTO crd_people (id, city, email) VALUES ('alice', 'paris', 'a@x')",
    "INSERT INTO crd_people (id, city, email) VALUES ('bob', 'rome', 'b@x')",
    "INSERT INTO crd_people (id, city, email) VALUES ('carol', 'paris', 'c@x')",
    "GRAPH INSERT EDGE IN 'crd_people' FROM 'alice' TO 'bob' TYPE 'knows'",
    "GRAPH INSERT EDGE IN 'crd_people' FROM 'bob' TO 'carol' TYPE 'knows'",
    "INSERT INTO crd_accounts (id, owner, balance) VALUES ('acc1', 'alice', 10)",
    "INSERT INTO crd_accounts (id, owner, balance) VALUES ('acc2', 'bob', 20)",
    "INSERT INTO crd_ledger (id, value) VALUES ('e1', 'draft')",
    "UPDATE crd_ledger SET value = 'final' WHERE id = 'e1'",
];

fn db_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn drain_backup(client: &tokio_postgres::Client) -> Vec<u8> {
    let stream = client
        .copy_out(&format!("COPY (BACKUP TENANT {TENANT}) TO STDOUT"))
        .await
        .unwrap_or_else(|e| panic!("copy_out: {}", db_detail(&e)));
    let mut bytes = Vec::new();
    let mut stream = Box::pin(stream);
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.unwrap_or_else(|e| panic!("chunk: {}", db_detail(&e))));
    }
    bytes
}

async fn push_restore(client: &tokio_postgres::Client, envelope: Vec<u8>) {
    let sink = client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({TENANT}) FROM STDIN"))
        .await
        .unwrap_or_else(|e| panic!("copy_in: {}", db_detail(&e)));
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(envelope))
        .await
        .unwrap_or_else(|e| panic!("send: {}", db_detail(&e)));
    sink.as_mut()
        .finish()
        .await
        .unwrap_or_else(|e| panic!("restore: {}", db_detail(&e)));
}

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

/// Every text cell `sql` returns on `node`, joined.
async fn text(node: &TestClusterNode, sql: &str) -> String {
    let messages = read_once_a_leader_exists(
        sql,
        Duration::from_secs(30),
        Duration::from_millis(100),
        || node.client.simple_query(sql),
    )
    .await;
    messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .filter_map(|i| row.get(i))
                    .collect::<String>(),
            ),
            _ => None,
        })
        .collect()
}

/// Every restored row, index entry, version and edge reads back from
/// `node`'s own replica.
async fn assert_restored_on(node: &TestClusterNode, stage: &str) {
    let id = node.node_id;
    node.client
        .simple_query("SET default_read_consistency = 'eventual'")
        .await
        .unwrap_or_else(|e| panic!("node {id}: set eventual reads: {}", db_detail(&e)));
    assert_eq!(
        column(node, "SELECT id FROM crd_people WHERE city = 'paris'").await,
        vec!["alice", "carol"],
        "{stage}, node {id}: the secondary index lookup"
    );
    assert_eq!(
        column(node, "SELECT id FROM crd_people WHERE email = 'b@x'").await,
        vec!["bob"],
        "{stage}, node {id}: the unique index lookup"
    );
    assert_eq!(
        column(node, "SELECT balance FROM crd_accounts WHERE id = 'acc2'").await,
        vec!["20"],
        "{stage}, node {id}: the strict row by primary key"
    );
    assert_eq!(
        column(node, "SELECT owner FROM crd_accounts").await,
        vec!["alice", "bob"],
        "{stage}, node {id}: every strict row"
    );
    assert_eq!(
        column(node, "SELECT value FROM crd_ledger WHERE id = 'e1'").await,
        vec!["final"],
        "{stage}, node {id}: the bitemporal row's current version"
    );
    assert_eq!(
        column(node, "SELECT value FROM crd_ledger AS OF SYSTEM TIME NULL").await,
        vec!["draft", "final"],
        "{stage}, node {id}: the bitemporal row keeps both versions"
    );
    let from_alice = text(
        node,
        "GRAPH NEIGHBORS IN 'crd_people' OF 'alice' LABEL 'knows' DIRECTION out",
    )
    .await;
    assert!(
        from_alice.contains("bob"),
        "{stage}, node {id}: the edge alice -> bob, got {from_alice}"
    );
    let from_bob = text(
        node,
        "GRAPH NEIGHBORS IN 'crd_people' OF 'bob' LABEL 'knows' DIRECTION out",
    )
    .await;
    assert!(
        from_bob.contains("carol"),
        "{stage}, node {id}: the edge bob -> carol, got {from_bob}"
    );
}

async fn assert_restored(cluster: &TestCluster, stage: &str) {
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    for node in &cluster.nodes {
        assert_restored_on(node, stage).await;
    }
    let duplicate = cluster.nodes[0]
        .exec("INSERT INTO crd_people (id, city, email) VALUES ('dave', 'oslo', 'a@x')")
        .await;
    assert!(
        duplicate.is_err(),
        "{stage}: the unique index must refuse a restored row's email"
    );
}

async fn source_backup() -> Vec<u8> {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    for sql in COLLECTIONS {
        source
            .exec_ddl_on_any_leader(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    for sql in WRITES {
        source.nodes[0]
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    source
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let backup = drain_backup(&source.nodes[0].client).await;
    source.shutdown().await;
    backup
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restored_documents_indexes_and_edges_survive_a_full_cluster_restart() {
    let backup = source_backup().await;

    let target = TestCluster::spawn_three().await.expect("target cluster");
    push_restore(&target.nodes[1].client, backup).await;
    assert_restored(&target, "after the restore").await;

    let target = target
        .restart_all()
        .await
        .unwrap_or_else(|e| panic!("restart every node: {e}"));
    assert_restored(&target, "after every node restarted").await;

    target.shutdown().await;
}
