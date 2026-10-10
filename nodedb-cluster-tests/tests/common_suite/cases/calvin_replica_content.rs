// SPDX-License-Identifier: BUSL-1.1

//! Per-replica stored content for the Calvin redo convergence cases.
//!
//! A replica's content comes from the tenant snapshot of each Data-Plane
//! core of its node. The read never leaves the node, so it shows the rows
//! that replica installed, never a row a gateway fetched from a leader.
//!
//! A collection homes to one vShard. The content of a vShard is every
//! document and index entry of its collections: storage key and stored
//! bytes, in key order. Two replicas that installed the same entries in the
//! same order hold equal content.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nodedb::types::{DatabaseId, TenantId};

use crate::common::cluster_harness::shared_steps::{db_detail, key_collection, name_where};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, is_no_serving_leader};

/// The tenant the `nodedb` pgwire user writes as.
const TENANT: u64 = 1;

/// SQLSTATE of a transaction aborted by a conflict. It wrote nothing.
const SERIALIZATION_FAILURE: &str = "40001";

/// How long a refused statement is retried.
const RETRY_DEADLINE: Duration = Duration::from_secs(30);

/// Pause between two attempts of a refused statement.
const RETRY_BACKOFF: Duration = Duration::from_millis(50);

/// How long the replicas get to agree after the cluster quiesced.
const AGREE_DEADLINE: Duration = Duration::from_secs(30);

/// Pause between two content reads while the replicas disagree.
const AGREE_POLL: Duration = Duration::from_millis(200);

/// The most differing keys a disagreement report names per vShard.
const REPORT_KEYS: usize = 10;

/// One vShard's stored content on one replica, keyed by table and storage
/// key.
pub(super) type VShardContent = BTreeMap<String, Vec<u8>>;

/// The vShard `collection` in the default database homes to.
pub(super) fn vshard_of(collection: &str) -> u32 {
    nodedb_cluster::routing::vshard_for_collection(nodedb_types::CollectionKey::from_bare(
        DatabaseId::DEFAULT,
        collection,
    ))
}

/// One collection name per prefix, `{prefix}_{i}` for the lowest `i` whose
/// vShard differs from the vShard of every name picked before it.
pub(super) fn names_on_distinct_vshards<const N: usize>(prefixes: [&str; N]) -> [String; N] {
    let mut taken: Vec<u32> = Vec::with_capacity(N);
    prefixes.map(|prefix| {
        let name = name_where(prefix, |candidate| !taken.contains(&vshard_of(candidate)));
        taken.push(vshard_of(&name));
        name
    })
}

/// A fresh session on `node` in strict cross-shard mode.
pub(super) async fn strict_session(node: &TestClusterNode) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=nodedb dbname=default",
            node.pg_addr.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect a session");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");
    client
}

/// Run `sql` through `client` as one request. A `40001` refusal aborted the
/// transaction and wrote nothing, and a missing leader refused it before it
/// ran: either runs again until [`RETRY_DEADLINE`]. Any other error panics
/// with `what`, the SQLSTATE and the detail.
pub(super) async fn run_retrying(client: &tokio_postgres::Client, what: &str, sql: &str) {
    let deadline = tokio::time::Instant::now() + RETRY_DEADLINE;
    loop {
        let error = match client.simple_query(sql).await {
            Ok(_) => return,
            Err(error) => error,
        };
        let retryable = is_no_serving_leader(&error)
            || error
                .as_db_error()
                .is_some_and(|db| db.code().code() == SERIALIZATION_FAILURE);
        if !retryable || tokio::time::Instant::now() >= deadline {
            panic!("{what}: {}", db_detail(&error));
        }
        // A failed block can leave the session in an aborted transaction.
        // Outside one, ROLLBACK only warns, so its result does not matter.
        let _ = client.simple_query("ROLLBACK").await;
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
}

/// Create `target` and `source` as strict collections and declare
/// `target.balance` as the materialized sum of `source.amount` over
/// `source.account_id = target.id`.
pub(super) fn sum_ddl(source: &str, target: &str) -> [String; 3] {
    [
        format!(
            "CREATE COLLECTION {target} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
        format!(
            "CREATE COLLECTION {source} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ),
        format!(
            "ALTER COLLECTION {target} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {source} \
             ON {source}.account_id = {target}.id VALUE {source}.amount"
        ),
    ]
}

/// The first column of every row `sql` returns through `client`.
pub(super) async fn first_column(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}`: {}", db_detail(&e)))
        .into_iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

/// The content of each vShard `collections` home to, as `node` stores it.
pub(super) async fn local_content(
    node: &TestClusterNode,
    collections: &[&str],
) -> BTreeMap<u32, VShardContent> {
    let mut by_vshard: BTreeMap<u32, VShardContent> = collections
        .iter()
        .map(|collection| (vshard_of(collection), VShardContent::new()))
        .collect();
    for core in 0..node.num_cores() {
        let snapshot = node
            .tenant_snapshot_on_core(core, TenantId::new(TENANT))
            .await;
        for (table, entries) in [("doc", snapshot.documents), ("idx", snapshot.indexes)] {
            for (key, value) in entries {
                let Some(collection) = key_collection(&key) else {
                    continue;
                };
                if !collections.contains(&collection) {
                    continue;
                }
                let vshard = vshard_of(collection);
                if let Some(content) = by_vshard.get_mut(&vshard) {
                    content.insert(format!("{table}:{key}"), value);
                }
            }
        }
    }
    by_vshard
}

/// The indexes in `cluster.nodes` of the replicas of `collection`'s data
/// group. Panics when fewer than two nodes replicate it: one replica agrees
/// with itself and proves nothing.
fn replicas_of(cluster: &TestCluster, collection: &str) -> Vec<usize> {
    let replicas: Vec<usize> = cluster
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| {
            node.group_id_for_collection(collection)
                .is_some_and(|group| node.replicates_data_group(group))
        })
        .map(|(idx, _)| idx)
        .collect();
    assert!(
        replicas.len() >= 2,
        "{collection}'s data group has {} replica(s); the comparison needs two or more",
        replicas.len()
    );
    replicas
}

/// Wait until every replica of each vShard `collections` home to holds the
/// same content, and return that content per vShard. Panics at the deadline
/// with the node ids and the keys that differ.
pub(super) async fn wait_replicas_agree(
    cluster: &TestCluster,
    collections: &[&str],
) -> BTreeMap<u32, VShardContent> {
    let replicas: BTreeMap<u32, Vec<usize>> = collections
        .iter()
        .map(|collection| (vshard_of(collection), replicas_of(cluster, collection)))
        .collect();
    let deadline = tokio::time::Instant::now() + AGREE_DEADLINE;
    loop {
        let mut per_node = Vec::with_capacity(cluster.nodes.len());
        for node in &cluster.nodes {
            per_node.push(local_content(node, collections).await);
        }
        let disagreements: Vec<String> = replicas
            .iter()
            .filter_map(|(vshard, nodes)| disagreement(cluster, &per_node, *vshard, nodes))
            .collect();
        if disagreements.is_empty() {
            return replicas
                .iter()
                .map(|(vshard, nodes)| {
                    let content = per_node[nodes[0]].get(vshard).cloned().unwrap_or_default();
                    (*vshard, content)
                })
                .collect();
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "replicas still hold different content after quiesce:\n{}",
                disagreements.join("\n")
            );
        }
        tokio::time::sleep(AGREE_POLL).await;
    }
}

/// A report of how `vshard`'s replicas `nodes` differ, `None` when they
/// agree. A key is listed when one replica lacks it or stores other bytes.
fn disagreement(
    cluster: &TestCluster,
    per_node: &[BTreeMap<u32, VShardContent>],
    vshard: u32,
    nodes: &[usize],
) -> Option<String> {
    let empty = VShardContent::new();
    let content = |idx: usize| per_node[idx].get(&vshard).unwrap_or(&empty);
    let reference = content(nodes[0]);
    let reference_id = cluster.nodes[nodes[0]].node_id;
    let differing: Vec<String> = nodes[1..]
        .iter()
        .filter(|idx| content(**idx) != reference)
        .flat_map(|idx| {
            let other = content(*idx);
            let other_id = cluster.nodes[*idx].node_id;
            let keys: BTreeSet<&String> = reference.keys().chain(other.keys()).collect();
            keys.into_iter()
                .filter(|key| reference.get(*key) != other.get(*key))
                .map(|key| format!("node {reference_id} vs node {other_id}: {key}"))
                .collect::<Vec<_>>()
        })
        .take(REPORT_KEYS)
        .collect();
    (!differing.is_empty()).then(|| {
        let sizes: Vec<String> = nodes
            .iter()
            .map(|idx| {
                format!(
                    "node {}: {} entries",
                    cluster.nodes[*idx].node_id,
                    content(*idx).len()
                )
            })
            .collect();
        format!(
            "vShard {vshard} ({}): {}",
            sizes.join(", "),
            differing.join("; ")
        )
    })
}
