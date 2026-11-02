// SPDX-License-Identifier: BUSL-1.1

//! A transaction's homed read is never validated by a node that lost
//! leadership.
//!
//! A transaction reads the neighbors of `r0` through a node that does not
//! lead `r0`'s key vShard, so the read is homed on that vShard's leader `L`.
//! It writes one log row, so its COMMIT checks its reads.
//! `L` is then cut off from both other nodes. They elect a new leader, and an
//! edge out of `r0` commits there. `L` is reconnected to the reader's node
//! only, still cut off from the rest of the cluster. The reader's COMMIT must
//! fail: `L` holds no leader lease, and the new leader's versions do
//! not compare with the read `L` served.

use std::time::Duration;

use nodedb_types::id::VShardId;

use crate::common::cluster_harness::{TestCluster, wait_for, wait_for_async};
use crate::common::occ_shuffle::pg_detail;

const EDGES: &str = "hrf_edges";
const READ_KEY: &str = "r0";
/// The collection the transaction writes one row to.
const LOG: &str = "hrf_log";

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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_deposed_leader_never_validates_a_homed_read() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    for collection in [EDGES, LOG] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {collection}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {collection}: {e}"));
    }
    wait_for(
        "all 3 nodes see both collections",
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
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN '{EDGES}' FROM '{READ_KEY}' TO 's0' TYPE 'K'"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed edge: {}", pg_detail(&e)));
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
        .group_for_vshard(VShardId::from_key(READ_KEY.as_bytes()).as_u32())
        .expect("the read key's group");
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
        .expect("the read key's group has a leader");
    let reader = (old + 1) % 3;
    let writer = (old + 2) % 3;
    let read_sql = format!("GRAPH NEIGHBORS IN '{EDGES}' OF '{READ_KEY}' DIRECTION out");

    let client = &cluster.nodes[reader].client;
    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query(&read_sql)
        .await
        .unwrap_or_else(|e| panic!("in-txn NEIGHBORS: {}", pg_detail(&e)));
    client
        .simple_query(&format!(
            "INSERT INTO {LOG} (id, value) VALUES ('entry', '1')"
        ))
        .await
        .unwrap_or_else(|e| panic!("in-txn log row: {}", pg_detail(&e)));

    // Cut the leader off. The other two elect a new leader of the group.
    link(&cluster, old, reader, true);
    link(&cluster, old, writer, true);
    wait_for(
        "the reader and the writer elect a new leader of the read key's group",
        Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let leader = leader_of(writer);
            leader != 0 && leader != old_leader_id
        },
    )
    .await;
    let conflicting =
        format!("GRAPH INSERT EDGE IN '{EDGES}' FROM '{READ_KEY}' TO 'fresh' TYPE 'K'");
    wait_for_async(
        "the conflicting edge commits at the new leader",
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

    // The deposed leader can reach the reader again, never the writer.
    link(&cluster, old, reader, false);
    let commit = client.simple_query("COMMIT").await;
    assert!(
        commit.is_err(),
        "a homed read the deposed leader served committed past a conflicting write: {commit:?}"
    );

    link(&cluster, old, writer, false);
    cluster.shutdown().await;
}
