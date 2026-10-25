// SPDX-License-Identifier: BUSL-1.1

//! A bitemporal columnar `AS OF` reads one answer on every node.
//!
//! The data group's leader stamps each insert's `_ts_system` run as it admits
//! the entry, every replica installs that run, and its WAL record carries it.
//! A cut between two versions of a key reads the first on every node: after
//! replication catch-up, after the data group's leader stops (the new leader
//! serves the cut from the stamps it installed), and after the former leader
//! restarts and replays its WAL.

use std::time::Duration;

use nodedb_types::DatabaseId;

use crate::common::cluster_harness::{TestCluster, TestClusterNode, read_once_a_leader_exists};

const COLL: &str = "btc_cut";

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

/// The `v` of every row `sql` returns through `node`, sorted.
async fn values(node: &TestClusterNode, sql: &str) -> Vec<String> {
    let messages = read_once_a_leader_exists(
        sql,
        Duration::from_secs(30),
        Duration::from_millis(100),
        || node.client.simple_query(sql),
    )
    .await;
    let mut out: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_columnar_as_of_reads_one_answer_on_every_node() {
    let mut cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} (id BIGINT PRIMARY KEY, v BIGINT) \
             WITH (engine='columnar', bitemporal=true)"
        ))
        .await
        .expect("CREATE COLLECTION");

    cluster.nodes[0]
        .exec(&format!("INSERT INTO {COLL} (id, v) VALUES (1, 10)"))
        .await
        .expect("insert v1");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cut = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;
    cluster.nodes[0]
        // A declared primary key refuses a second INSERT of a current key,
        // bitemporal or not: the second version comes from an UPDATE.
        .exec(&format!("UPDATE {COLL} SET v = 20 WHERE id = 1"))
        .await
        .expect("insert v2");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let as_of = format!("SELECT v FROM {COLL} AS OF SYSTEM TIME {cut}");
    for node in &cluster.nodes {
        assert_eq!(
            values(node, &as_of).await,
            vec!["10".to_string()],
            "node {}: the cut reads the first version",
            node.node_id
        );
    }

    // Stop the data group's leader: a surviving replica serves the cut from
    // the ordinals it installed.
    let vshard = nodedb_cluster::routing::vshard_for_collection(
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, COLL),
    );
    let leader = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let group = routing.group_for_vshard(vshard).expect("vShard group");
        routing
            .group_info(group)
            .map(|info| info.leader)
            .unwrap_or(0)
    };
    assert!(leader != 0, "the collection's group has a leader");
    let index = cluster
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .expect("leader node present");
    let stopped = cluster.stop_member(index).await.expect("stop the leader");

    for node in &cluster.nodes {
        assert_eq!(
            values(node, &as_of).await,
            vec!["10".to_string()],
            "node {} after the leader stopped: the cut reads the first version",
            node.node_id
        );
    }

    // The former leader restarts from its own WAL and checkpoints, and
    // replays each record at the ordinals its live apply stamped.
    cluster
        .restart_member(stopped)
        .await
        .expect("restart the former leader");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    for node in &cluster.nodes {
        assert_eq!(
            values(node, &as_of).await,
            vec!["10".to_string()],
            "node {} after the restart: the cut reads the first version",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
