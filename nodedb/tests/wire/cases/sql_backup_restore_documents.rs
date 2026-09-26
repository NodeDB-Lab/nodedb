// SPDX-License-Identifier: BUSL-1.1

//! RESTORE re-issues document rows, their index entries, and graph edges as
//! durable writes.
//!
//! The backup holds a schemaless collection with a secondary index and a
//! unique index, a strict collection, a strict `bitemporal=true` collection
//! with two versions of one row, and two edges. A fresh server restores it,
//! then restarts. Before and after the restart every row reads back, the
//! index lookups answer, the unique index refuses a duplicate, the bitemporal
//! row keeps both versions, and the edges traverse.
//!
//! Both single-node modes run it: the default server, whose data groups are
//! Raft groups of one, and a standalone server with no Raft groups.

use super::backup_support::{drain_backup, push_restore};
use crate::harness::TestServer;

const TENANT: u64 = 1;

const SETUP: &[&str] = &[
    "CREATE COLLECTION rd_people (id STRING PRIMARY KEY, city STRING, email STRING) \
     WITH (engine='document_schemaless')",
    "CREATE INDEX ON rd_people (city)",
    "CREATE UNIQUE INDEX rd_people_email ON rd_people (email)",
    "INSERT INTO rd_people (id, city, email) VALUES ('alice', 'paris', 'a@x')",
    "INSERT INTO rd_people (id, city, email) VALUES ('bob', 'rome', 'b@x')",
    "INSERT INTO rd_people (id, city, email) VALUES ('carol', 'paris', 'c@x')",
    "GRAPH INSERT EDGE IN 'rd_people' FROM 'alice' TO 'bob' TYPE 'knows'",
    "GRAPH INSERT EDGE IN 'rd_people' FROM 'bob' TO 'carol' TYPE 'knows'",
    "CREATE COLLECTION rd_accounts (id STRING PRIMARY KEY, owner STRING, balance INT) \
     WITH (engine='document_strict')",
    "INSERT INTO rd_accounts (id, owner, balance) VALUES ('acc1', 'alice', 10)",
    "INSERT INTO rd_accounts (id, owner, balance) VALUES ('acc2', 'bob', 20)",
    "CREATE COLLECTION rd_ledger (id STRING PRIMARY KEY, value STRING) \
     WITH (engine='document_strict', bitemporal=true)",
    "INSERT INTO rd_ledger (id, value) VALUES ('e1', 'draft')",
    "UPDATE rd_ledger SET value = 'final' WHERE id = 'e1'",
];

async fn column(server: &TestServer, sql: &str) -> Vec<String> {
    let mut values: Vec<String> = server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect();
    values.sort();
    values
}

async fn neighbors(server: &TestServer, node: &str) -> String {
    server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN 'rd_people' OF '{node}' LABEL 'knows' DIRECTION out"
        ))
        .await
        .unwrap_or_else(|e| panic!("neighbors of {node}: {e}"))
        .join("")
}

/// Every restored row, index entry, version and edge reads back on `server`.
async fn assert_restored(server: &TestServer, stage: &str) {
    assert_eq!(
        column(server, "SELECT id FROM rd_people WHERE city = 'paris'").await,
        vec!["alice", "carol"],
        "{stage}: the secondary index lookup"
    );
    assert_eq!(
        column(server, "SELECT id FROM rd_people WHERE email = 'b@x'").await,
        vec!["bob"],
        "{stage}: the unique index lookup"
    );
    let duplicate = server
        .exec("INSERT INTO rd_people (id, city, email) VALUES ('dave', 'oslo', 'a@x')")
        .await;
    assert!(
        duplicate.is_err(),
        "{stage}: the unique index must refuse a restored row's email"
    );
    assert_eq!(
        column(server, "SELECT balance FROM rd_accounts WHERE id = 'acc2'").await,
        vec!["20"],
        "{stage}: the strict row by primary key"
    );
    assert_eq!(
        column(server, "SELECT owner FROM rd_accounts").await,
        vec!["alice", "bob"],
        "{stage}: every strict row"
    );
    assert_eq!(
        column(server, "SELECT value FROM rd_ledger WHERE id = 'e1'").await,
        vec!["final"],
        "{stage}: the bitemporal row's current version"
    );
    assert_eq!(
        column(server, "SELECT value FROM rd_ledger AS OF SYSTEM TIME NULL").await,
        vec!["draft", "final"],
        "{stage}: the bitemporal row keeps both versions"
    );
    let from_alice = neighbors(server, "alice").await;
    assert!(
        from_alice.contains("bob"),
        "{stage}: the edge alice -> bob, got {from_alice}"
    );
    let from_bob = neighbors(server, "bob").await;
    assert!(
        from_bob.contains("carol"),
        "{stage}: the edge bob -> carol, got {from_bob}"
    );
}

async fn source_backup() -> Vec<u8> {
    let source = TestServer::start().await;
    for sql in SETUP {
        source
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    drain_backup(&source.client, TENANT)
        .await
        .expect("take the backup")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restored_documents_indexes_and_edges_survive_a_restart() {
    let backup = source_backup().await;

    let target = TestServer::start().await;
    push_restore(&target.client, TENANT, backup)
        .await
        .unwrap_or_else(|e| panic!("restore: {e}"));
    assert_restored(&target, "after the restore").await;

    let (target, dir) = target.take_dir();
    target.graceful_shutdown().await;
    let (target, _dir) = TestServer::open_on_path(dir).await;
    assert_restored(&target, "after a restart").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restored_documents_indexes_and_edges_survive_a_standalone_restart() {
    let backup = source_backup().await;

    let target = TestServer::start_standalone().await;
    push_restore(&target.client, TENANT, backup)
        .await
        .unwrap_or_else(|e| panic!("restore: {e}"));
    assert_restored(&target, "after the restore").await;

    let (target, dir) = target.take_dir();
    target.graceful_shutdown().await;
    let (target, _dir) = TestServer::open_on_path_standalone(dir).await;
    assert_restored(&target, "after a restart").await;
}
