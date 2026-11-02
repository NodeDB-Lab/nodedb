// SPDX-License-Identifier: BUSL-1.1

//! A transaction's homed graph read is checked on the one core that owns the
//! read vShard.
//!
//! With a replication factor of 1 and two cores per node, a group's leader
//! runs its vShards on both of its cores. vShard `v` belongs to group
//! `1 + v % GROUPS` and runs on core `v % CORES`. With an even group count
//! every group's vShards share one parity, so a group sits on one core, and
//! the leader balance gives each node one group. [`GROUPS`] is odd, so every
//! group spans both cores. A transaction reads the neighbors of `r`
//! through another node, so the read is homed on `r`'s key vShard, and it
//! writes one log row, so its COMMIT checks its reads. A concurrent edge
//! between two keys that the same leader owns on its other core must not
//! abort the COMMIT: it changed nothing the read saw. A concurrent edge out
//! of `r` must abort it. A read-only transaction read every row at its
//! snapshot, so it commits across the same edge.

use std::collections::HashMap;
use std::time::Duration;

use nodedb_types::id::VShardId;

use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for};
use crate::common::occ_shuffle::{pg_detail, pg_sqlstate};

const EDGES: &str = "hpc_edges";
/// The collection each read-write transaction writes one row to.
const LOG: &str = "hpc_log";
const CORES: usize = 2;
/// Data groups of the cluster. Odd, so every group spans both cores.
const GROUPS: u64 = 3;

/// The node that leads `key`'s key vShard, and the core it runs it on.
fn owner<'a>(
    cluster: &'a TestCluster,
    group_of: &HashMap<u32, u64>,
    leaders: &HashMap<u64, u64>,
    key: &str,
) -> (&'a TestClusterNode, usize) {
    let vshard = VShardId::from_key(key.as_bytes());
    let leader = group_of
        .get(&vshard.as_u32())
        .and_then(|g| leaders.get(g))
        .copied()
        .unwrap_or_else(|| panic!("vShard {} of '{key}' has a leader", vshard.as_u32()));
    let node = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == leader)
        .unwrap_or_else(|| panic!("leader {leader} is a cluster node"));
    let core = node
        .shared
        .dispatcher
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .router()
        .resolve(vshard)
        .unwrap_or_else(|| panic!("a core owns vShard {}", vshard.as_u32()));
    (node, core)
}

async fn insert_edge(node: &TestClusterNode, src: &str, dst: &str) {
    node.client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN '{EDGES}' FROM '{src}' TO '{dst}' TYPE 'K'"
        ))
        .await
        .unwrap_or_else(|e| panic!("edge {src}->{dst}: {}", pg_detail(&e)));
}

/// Open a transaction on `node` that reads `read_sql` and, when `log_id` is
/// set, writes that log row.
async fn begin_reading(node: &TestClusterNode, read_sql: &str, log_id: Option<&str>) {
    node.client.simple_query("BEGIN").await.expect("BEGIN");
    node.client
        .simple_query(read_sql)
        .await
        .unwrap_or_else(|e| panic!("in-txn NEIGHBORS: {}", pg_detail(&e)));
    if let Some(id) = log_id {
        node.client
            .simple_query(&format!(
                "INSERT INTO {LOG} (id, value) VALUES ('{id}', '1')"
            ))
            .await
            .unwrap_or_else(|e| panic!("in-txn log row {id}: {}", pg_detail(&e)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_write_on_another_core_of_the_leader_keeps_the_read() {
    let cluster = TestCluster::spawn_three_with_groups_rf_and_cores(GROUPS, 1, CORES)
        .await
        .expect("3-node RF1 cluster");
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
    let group_of: HashMap<u32, u64> = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let mut map = HashMap::new();
        for g in routing.group_ids().into_iter().filter(|g| *g != 0) {
            for vs in routing.vshards_for_group(g) {
                map.insert(vs, g);
            }
        }
        map
    };
    // At replication factor 1 each node hosts only its own groups, so the
    // leaders come from every group's replica.
    wait_for(
        "every data group has a leader",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            let leaders = cluster.data_group_leaders();
            group_of.values().all(|g| leaders.contains_key(g))
        },
    )
    .await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;
    let leaders: HashMap<u64, u64> = cluster.data_group_leaders();

    // `r` is read. `x` and `y` live on `r`'s leader, on the other core.
    let reader_key = "r0";
    let (leader, read_core) = owner(&cluster, &group_of, &leaders, reader_key);
    let other_core: Vec<String> = (0..10_000)
        .map(|i| format!("oc{i}"))
        .filter(|key| {
            let (node, core) = owner(&cluster, &group_of, &leaders, key);
            node.node_id == leader.node_id && core != read_core
        })
        .take(2)
        .collect();
    assert_eq!(
        other_core.len(),
        2,
        "the leader of '{reader_key}' owns keys on its other core"
    );
    let coordinator = cluster
        .nodes
        .iter()
        .find(|n| n.node_id != leader.node_id)
        .expect("a node that does not lead the read vShard");
    let writer = cluster
        .nodes
        .iter()
        .find(|n| n.node_id != coordinator.node_id)
        .expect("a second node");

    insert_edge(writer, reader_key, "r_seed").await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;
    let read_sql = format!("GRAPH NEIGHBORS IN '{EDGES}' OF '{reader_key}' DIRECTION out");

    // A write on the leader's other core leaves the read current.
    begin_reading(coordinator, &read_sql, Some("other_core")).await;
    insert_edge(writer, &other_core[0], &other_core[1]).await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;
    coordinator
        .client
        .simple_query("COMMIT")
        .await
        .unwrap_or_else(|e| {
            panic!(
                "a write on another core of the read vShard's leader must not abort: {}",
                pg_detail(&e)
            )
        });

    // A read-only transaction commits across a write on the read vShard.
    begin_reading(coordinator, &read_sql, None).await;
    insert_edge(writer, reader_key, "r_read_only").await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;
    coordinator
        .client
        .simple_query("COMMIT")
        .await
        .unwrap_or_else(|e| {
            panic!(
                "a read-only transaction commits at its snapshot: {}",
                pg_detail(&e)
            )
        });

    // A write on the read vShard aborts a transaction that also writes.
    begin_reading(coordinator, &read_sql, Some("read_vshard")).await;
    insert_edge(writer, reader_key, "r_fresh").await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;
    let err = coordinator
        .client
        .simple_query("COMMIT")
        .await
        .expect_err("a write on the read vShard must abort the transaction");
    assert_eq!(
        pg_sqlstate(&err).as_deref(),
        Some("40001"),
        "expected serialization_failure (40001), got: {}",
        pg_detail(&err)
    );

    cluster.shutdown().await;
}
