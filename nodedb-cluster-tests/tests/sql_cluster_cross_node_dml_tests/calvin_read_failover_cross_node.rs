// SPDX-License-Identifier: BUSL-1.1

//! A read made before a leader change validates at the new leader.
//!
//! A transaction reads a row of a collection whose home group leader is `A`,
//! so `A` serves the read. Its version is the data-group log position of the
//! last write to the collection, the same on every replica. `A` is then cut
//! off from both other nodes, they elect `B`, and a write to the read row
//! commits at `B` at a later log position. The partition heals. The
//! transaction also writes, so its COMMIT goes through Calvin, and `B`
//! validates the read. `B`'s version of the read row is above the read's,
//! so the commit must abort with a retryable serialization error.
//!
//! The control case runs the same leader change with no write to the read
//! row. Its commit must succeed, so the leader change alone never aborts a
//! transaction, and the abort above comes from the write.

use std::time::Duration;

use nodedb_client::NativeClient;
use nodedb_client::native::pool::PoolConfig;
use nodedb_types::id::VShardId;
use nodedb_types::{CollectionKey, DatabaseId};

use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};
use crate::common::occ_shuffle::pg_detail;

const DOCS: &str = "crf_docs";
const LOG: &str = "crf_log";
/// The native transport's message for a serialization abort.
const SERIALIZATION_ABORT: &str = "could not serialize access due to concurrent update";

fn pinned_native_client(node: &TestClusterNode) -> NativeClient {
    node.native_client_with(|base| PoolConfig {
        max_size: 1,
        ..base
    })
}

/// Sever node `a` and node `b` from each other, both ways, or heal the link.
fn link(cluster: &TestCluster, a: usize, b: usize, severed: bool) {
    let (a_node, b_node) = (&cluster.nodes[a], &cluster.nodes[b]);
    let a_transport = a_node
        .shared
        .cluster_transport
        .as_ref()
        .expect("cluster transport");
    let b_transport = b_node
        .shared
        .cluster_transport
        .as_ref()
        .expect("cluster transport");
    if severed {
        a_transport.sever(b_node.node_id);
        b_transport.sever(a_node.node_id);
    } else {
        a_transport.heal(b_node.node_id);
        b_transport.heal(a_node.node_id);
    }
}

/// Read the seeded row in a transaction served by the old leader, move the
/// group's leadership by a partition, write the read row at the new leader
/// when `conflict` is set, heal, and return the transaction's COMMIT result.
async fn commit_after_a_leader_change(
    conflict: bool,
) -> (TestCluster, nodedb_types::error::NodeDbResult<()>) {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    for coll in [DOCS, LOG] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {coll}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {coll}: {e}"));
    }
    wait_for(
        "all 3 nodes see the collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 2)
        },
    )
    .await;
    cluster.nodes[0]
        .client
        .simple_query(&format!("INSERT INTO {DOCS} (id, value) VALUES ('a', '1')"))
        .await
        .unwrap_or_else(|e| panic!("seed row: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    let group = cluster.nodes[0]
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(
            VShardId::from_collection(CollectionKey::from_bare(DatabaseId::DEFAULT, DOCS)).as_u32(),
        )
        .expect("the collection's group");
    let leader_of = |observer: usize| -> u64 {
        cluster.nodes[observer]
            .all_group_leaders()
            .into_iter()
            .find(|(g, _)| *g == group)
            .map(|(_, leader)| leader)
            .unwrap_or(0)
    };
    let old_leader_id = leader_of(0);
    let old = cluster
        .nodes
        .iter()
        .position(|n| n.node_id == old_leader_id)
        .expect("the collection's group has a leader");
    let reader = (old + 1) % 3;
    let writer = (old + 2) % 3;

    let driver = pinned_native_client(&cluster.nodes[reader]);
    driver.begin().await.expect("native BEGIN");
    driver
        .query(&format!("SELECT value FROM {DOCS} WHERE id = 'a'"))
        .await
        .expect("in-txn read served by the old leader");
    driver
        .query(&format!(
            "INSERT INTO {LOG} (id, value) VALUES ('entry', '1')"
        ))
        .await
        .expect("buffer a write so the commit goes through Calvin");

    // Move the group's leadership: cut the old leader off until the other two
    // elect a new one, then commit a write to the read row there.
    link(&cluster, old, reader, true);
    link(&cluster, old, writer, true);
    wait_for(
        "the reader and the writer elect a new leader of the collection's group",
        Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let leader = leader_of(writer);
            leader != 0 && leader != old_leader_id
        },
    )
    .await;
    if conflict {
        let conflicting = format!("UPDATE {DOCS} SET value = '2' WHERE id = 'a'");
        wait_for_async(
            "the conflicting write commits at the new leader",
            Duration::from_secs(30),
            Duration::from_millis(200),
            || async {
                cluster.nodes[writer]
                    .client
                    .simple_query(&conflicting)
                    .await
                    .is_ok()
            },
        )
        .await;
    }
    link(&cluster, old, reader, false);
    link(&cluster, old, writer, false);
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let committed = driver.commit().await;
    (cluster, committed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_write_after_the_read_aborts_the_commit_across_a_leader_change() {
    let (cluster, committed) = commit_after_a_leader_change(true).await;
    let err = committed
        .expect_err("a write committed after the read must abort the commit at the new leader");
    assert!(
        err.message().contains(SERIALIZATION_ABORT),
        "expected a retryable serialization abort, got: {err}"
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_current_read_commits_across_a_leader_change() {
    let (cluster, committed) = commit_after_a_leader_change(false).await;
    committed
        .unwrap_or_else(|e| panic!("a read no write moved commits after the leader change: {e}"));
    cluster.shutdown().await;
}
