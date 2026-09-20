// SPDX-License-Identifier: BUSL-1.1

//! Shared fixture for the Calvin multi-shard DML cases: a 3-node cluster with
//! a coordinator that is not the sequencer leader, raw pgwire in strict
//! cross-shard mode, and per-node row-count waits.
//!
//! Cross-shard fan-out comes from two sources:
//! - explicit transactions writing two collections on distinct vShards
//!   (`vshard_names::distinct_vshard_collections`),
//! - implicit graph edges whose `_from` endpoint hashes (`VShardId::from_key`)
//!   to a vShard other than the document's collection
//!   (`vshard_names::key_on_other_vshard`).

use std::time::Duration;

use tokio_postgres::SimpleQueryMessage;

use super::vshard_names::key_on_other_vshard;
use crate::common::cluster_harness::{
    TestCluster, TestClusterNode, is_no_serving_leader, wait_for, wait_for_async,
};
use crate::common::pgwire_harness::raw_pgwire::{RawPgConn, command_tags};

const CONVERGENCE: Duration = Duration::from_secs(15);

/// A 3-node cluster with a coordinator that is not the sequencer leader.
pub(super) struct Fixture {
    pub(super) cluster: TestCluster,
    pub(super) coordinator: usize,
}

impl Fixture {
    /// Spawn, run every `ddl` statement, and wait for the collections and a
    /// stable sequencer leader to be visible on every node.
    pub(super) async fn spawn(ddl: &[String]) -> Self {
        let fx =
            Self::from_cluster(TestCluster::spawn_three().await.expect("3-node cluster")).await;
        fx.create(ddl).await;
        fx
    }

    /// Wait for a stable sequencer leader on every node and pick a
    /// coordinator that is not it.
    pub(super) async fn from_cluster(cluster: TestCluster) -> Self {
        wait_for(
            "sequencer-group leader elected and visible on every node",
            CONVERGENCE,
            Duration::from_millis(50),
            || {
                let leader = cluster.nodes[0].sequencer_leader();
                leader != 0 && cluster.nodes.iter().all(|n| n.sequencer_leader() == leader)
            },
        )
        .await;
        let leader = cluster.nodes[0].sequencer_leader();
        let coordinator = cluster
            .nodes
            .iter()
            .position(|n| n.shared.node_id != leader)
            .expect("a non-sequencer-leader coordinator exists in a 3-node cluster");
        Self {
            cluster,
            coordinator,
        }
    }

    /// Run every `ddl` statement and wait for the collections to be visible
    /// on every node.
    pub(super) async fn create(&self, ddl: &[String]) {
        let before = self
            .cluster
            .nodes
            .iter()
            .map(|n| n.cached_collection_count())
            .min()
            .unwrap_or(0);
        for stmt in ddl {
            self.cluster
                .exec_ddl_on_any_leader(stmt)
                .await
                .unwrap_or_else(|e| panic!("{stmt}: {e}"));
        }
        let expected = before + ddl.len();
        wait_for(
            "all 3 nodes see every collection",
            CONVERGENCE,
            Duration::from_millis(50),
            || {
                self.cluster
                    .nodes
                    .iter()
                    .all(|n| n.cached_collection_count() >= expected)
            },
        )
        .await;
    }

    pub(super) fn coordinator(&self) -> &TestClusterNode {
        &self.cluster.nodes[self.coordinator]
    }

    /// Wait until `collection`'s data group is mounted cluster-wide: every
    /// node resolves it to a group id, and at least one node actually hosts
    /// that group locally (`hosts_data_group`, a live self-report, not a
    /// placement prediction).
    pub(super) async fn wait_group_mounted(&self, collection: &str) {
        wait_for(
            &format!("{collection}'s data group mounted"),
            CONVERGENCE,
            Duration::from_millis(50),
            || {
                self.cluster
                    .nodes
                    .iter()
                    .all(|n| n.group_id_for_collection(collection).is_some())
                    && self.cluster.nodes.iter().any(|n| {
                        n.group_id_for_collection(collection)
                            .is_some_and(|gid| n.hosts_data_group(gid))
                    })
            },
        )
        .await;
    }

    /// A raw pgwire session on the coordinator in strict cross-shard mode.
    pub(super) async fn raw(&self) -> RawPgConn {
        let mut conn = self.coordinator().raw_pgwire().await;
        conn.simple_query("SET cross_shard_txn = 'strict'").await;
        conn
    }

    pub(super) async fn converge(&self) {
        self.cluster
            .wait_for_full_apply_convergence(CONVERGENCE)
            .await;
    }

    /// Row count of `sql` on the coordinator.
    pub(super) async fn count_on_coordinator(&self, sql: &str) -> usize {
        row_count(&self.coordinator().client, sql).await
    }

    /// Wait until `sql` returns exactly `expected` rows on every node.
    pub(super) async fn wait_rows_on_every_node(&self, sql: &str, expected: usize) {
        for idx in 0..self.cluster.nodes.len() {
            self.wait_rows_on_node(idx, sql, expected).await;
        }
    }

    /// Wait until `sql` returns exactly `expected` rows on node `idx`.
    pub(super) async fn wait_rows_on_node(&self, idx: usize, sql: &str, expected: usize) {
        let node = &self.cluster.nodes[idx];
        wait_for_async(
            &format!("node {idx}: `{sql}` returns {expected} rows"),
            CONVERGENCE,
            Duration::from_millis(100),
            || async move {
                match node.client.simple_query(sql).await {
                    Ok(msgs) => data_rows(&msgs) == expected,
                    Err(e) if is_no_serving_leader(&e) => false,
                    Err(e) => panic!("node {idx}: `{sql}`: {e}"),
                }
            },
        )
        .await;
    }
}

pub(super) fn data_rows(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

pub(super) async fn row_count(client: &tokio_postgres::Client, sql: &str) -> usize {
    let msgs = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
    data_rows(&msgs)
}

/// Send one statement on the raw connection and return its command tags.
pub(super) async fn tags(conn: &mut RawPgConn, sql: &str) -> Vec<String> {
    command_tags(&conn.simple_query(sql).await)
}

/// Native `(rows_affected, command)` for one statement on the coordinator.
pub(super) async fn native_outcome(node: &TestClusterNode, sql: &str) -> (u64, Option<String>) {
    let result = node
        .native_client()
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("native `{sql}`: {e}"));
    (result.rows_affected, result.command)
}

pub(super) fn schemaless_ddl(coll: &str) -> String {
    format!("CREATE COLLECTION {coll} WITH (engine='document_schemaless')")
}

pub(super) fn keyed_ddl(coll: &str) -> String {
    format!("CREATE COLLECTION {coll} (id TEXT PRIMARY KEY, v TEXT)")
}

/// One implicit-edge document whose `_from` hashes away from `coll`'s vShard.
pub(super) fn edge_doc_sql(coll: &str, id: &str, mark: &str) -> String {
    let src = key_on_other_vshard(coll, &format!("src_{id}"));
    format!(
        "INSERT INTO {coll} \
         {{ id: '{id}', _from: '{src}', _to: 'hub', _type: 'l', mark: '{mark}' }}"
    )
}

/// Three implicit-edge documents in one `VALUES` list; two carry `mark='del'`.
pub(super) fn edge_batch_sql(coll: &str, ids: [&str; 3]) -> String {
    let rows: Vec<String> = ids
        .iter()
        .zip(["del", "del", "keep"])
        .map(|(id, mark)| {
            let src = key_on_other_vshard(coll, &format!("src_{id}"));
            format!("('{id}', '{src}', 'hub', 'l', '{mark}')")
        })
        .collect();
    format!(
        "INSERT INTO {coll} (id, _from, _to, _type, mark) VALUES {}",
        rows.join(", ")
    )
}
