// SPDX-License-Identifier: BUSL-1.1

//! A document secondary index is built and purged on every replica through
//! the collection's data group.
//!
//! 1. A 3-node cluster holds a schemaless collection with rows before any
//!    index exists.
//! 2. `CREATE INDEX` builds the index. Every node's own index entries find
//!    every row.
//! 3. `DROP INDEX` purges the index. No node keeps an entry of it.
//! 4. The rows take new values, and the same index is created again as a
//!    partial index. Every node's entries hold exactly the new values of the
//!    rows the predicate admits, and no node serves a value of the first
//!    index.
//!
//! Each read inspects one node's own cores and never leaves that node. File
//! name contains "cluster" so nextest applies the cluster test group.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nodedb::types::TenantId;

use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};

/// The tenant the `nodedb` pgwire user writes as.
const TENANT: u64 = 1;

const COLLECTION: &str = "doc_index_lifecycle";

const INDEX: &str = "doc_index_lifecycle_email";

/// How long each step gets.
const STEP_DEADLINE: Duration = Duration::from_secs(30);

/// Pause between two polls of a step.
const STEP_POLL: Duration = Duration::from_millis(100);

/// The rows the collection holds: `(id, first email, status)`.
const ROWS: [(&str, &str, &str); 6] = [
    ("u1", "a1@first.test", "active"),
    ("u2", "a2@first.test", "active"),
    ("u3", "a3@first.test", "active"),
    ("u4", "d4@first.test", "disabled"),
    ("u5", "d5@first.test", "disabled"),
    ("u6", "d6@first.test", "disabled"),
];

/// The email a row takes before the index is created again.
fn second_email(id: &str) -> String {
    format!("{id}@second.test")
}

/// Every value by which the `email` index entries stored on `node` find a
/// row now. Reads the entries of each core of `node`, and keeps an entry
/// whose newest version is live.
async fn live_index_values(node: &TestClusterNode) -> BTreeSet<String> {
    // `(field, value, doc_id)` -> (newest system time, live).
    let mut newest: BTreeMap<(String, String, String), (i64, bool)> = BTreeMap::new();
    for core in 0..node.num_cores() {
        let snapshot = node
            .tenant_snapshot_on_core(core, TenantId::new(TENANT))
            .await;
        for (key, tag) in &snapshot.indexes_versioned {
            // `{db}:{tid}:{collection}:{field}:{value}:{doc_id}\x00{system_from}`.
            let mut parts = key.splitn(5, ':');
            let (Some(_db), Some(_tid), Some(collection), Some(field), Some(rest)) = (
                parts.next(),
                parts.next(),
                parts.next(),
                parts.next(),
                parts.next(),
            ) else {
                panic!("index key {key:?} has no field");
            };
            // The index names the `email` field, as `email` or `$.email`.
            if collection != COLLECTION || !field.ends_with("email") {
                continue;
            }
            let (entry, sys) = rest
                .split_once('\x00')
                .unwrap_or_else(|| panic!("index key {key:?} carries no system time"));
            let sys: i64 = sys
                .parse()
                .unwrap_or_else(|e| panic!("index key {key:?} system time: {e}"));
            let (value, doc_id) = entry
                .rsplit_once(':')
                .unwrap_or_else(|| panic!("index key {key:?} has no doc id"));
            let live = tag.first() == Some(&0x00);
            let slot = newest
                .entry((field.to_owned(), value.to_owned(), doc_id.to_owned()))
                .or_insert((sys, live));
            if sys >= slot.0 {
                *slot = (sys, live);
            }
        }
    }
    newest
        .into_iter()
        .filter(|(_, (_, live))| *live)
        .map(|((_, value, _), _)| value)
        .collect()
}

/// Wait until every node's own index entries find exactly `expected`.
async fn every_node_serves(cluster: &TestCluster, expected: &BTreeSet<String>, step: &str) {
    for node in &cluster.nodes {
        wait_for_async(
            &format!(
                "{step}: node {} stores index entries for exactly {expected:?}",
                node.node_id
            ),
            STEP_DEADLINE,
            STEP_POLL,
            || async move { live_index_values(node).await == *expected },
        )
        .await;
    }
}

async fn exec(cluster: &TestCluster, sql: &str) {
    cluster
        .exec_ddl_on_any_leader(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn create_and_drop_index_reach_every_replica() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    exec(&cluster, &format!("CREATE COLLECTION {COLLECTION}")).await;
    wait_for(
        "collection visible on all nodes",
        STEP_DEADLINE,
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    for (id, email, status) in ROWS {
        exec(
            &cluster,
            &format!(
                "INSERT INTO {COLLECTION} (id, email, status) \
                 VALUES ('{id}', '{email}', '{status}')"
            ),
        )
        .await;
    }

    // The rows exist before the index, so only the build indexes them.
    exec(
        &cluster,
        &format!("CREATE INDEX {INDEX} ON {COLLECTION}(email)"),
    )
    .await;
    let first: BTreeSet<String> = ROWS.iter().map(|(_, email, _)| email.to_string()).collect();
    every_node_serves(&cluster, &first, "after CREATE INDEX").await;

    exec(&cluster, &format!("DROP INDEX {INDEX}")).await;
    every_node_serves(&cluster, &BTreeSet::new(), "after DROP INDEX").await;

    for (id, _, _) in ROWS {
        exec(
            &cluster,
            &format!(
                "UPDATE {COLLECTION} SET email = '{}' WHERE id = '{id}'",
                second_email(id)
            ),
        )
        .await;
    }

    // A partial index admits the active rows alone. An entry the first
    // index left on a node shows up as a value outside this set.
    exec(
        &cluster,
        &format!("CREATE INDEX {INDEX} ON {COLLECTION}(email) WHERE status = 'active'"),
    )
    .await;
    let second: BTreeSet<String> = ROWS
        .iter()
        .filter(|(_, _, status)| *status == "active")
        .map(|(id, _, _)| second_email(id))
        .collect();
    every_node_serves(&cluster, &second, "after the second CREATE INDEX").await;

    cluster.shutdown().await;
}
