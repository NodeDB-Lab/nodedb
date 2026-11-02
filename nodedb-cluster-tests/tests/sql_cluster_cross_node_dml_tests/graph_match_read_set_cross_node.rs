// SPDX-License-Identifier: BUSL-1.1

//! A cluster MATCH inside a transaction puts every vShard it read on the
//! transaction's read-set.
//!
//! With a replication factor of 1, a pattern over a graph whose node keys
//! cover every data group reads edges on every node. Another transaction then
//! writes an edge on a vShard led by a different node than the reader's
//! coordinator. The reader also writes one log row, so its COMMIT checks its
//! reads and must fail with a serialization error, over pgwire and over the
//! native protocol: the commit goes through Calvin, whose participants
//! include every vShard the MATCH read.
//!
//! A control transaction with no concurrent write commits, so the check does
//! not abort everything. A read-only transaction read every edge at its
//! snapshot, so it commits across a concurrent edge.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use nodedb_client::NativeClient;
use nodedb_client::native::pool::PoolConfig;
use nodedb_types::id::VShardId;

use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};
use crate::common::occ_shuffle::{pg_detail, pg_sqlstate};

const EDGES: &str = "mrs_edges";
const LOG: &str = "mrs_log";
const MIN_NODES: usize = 24;
/// The native transport's message for a serialization abort.
const SERIALIZATION_ABORT: &str = "could not serialize access due to concurrent update";

/// Names `n0, n1, …`, enough that their key vShards cover every data group.
fn node_names(groups: &BTreeSet<u64>, group_of: &HashMap<u32, u64>) -> Vec<String> {
    let mut out = Vec::new();
    let mut covered = BTreeSet::new();
    let mut i = 0usize;
    while out.len() < MIN_NODES || covered != *groups {
        let name = format!("n{i}");
        let vshard = VShardId::from_key(name.as_bytes()).as_u32();
        covered.insert(group_of.get(&vshard).copied().unwrap_or(0));
        out.push(name);
        i += 1;
    }
    out
}

fn row_count(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// Insert `src -> dst` from `writer` and wait until `probe` sees it.
async fn concurrent_edge(writer: &TestClusterNode, probe: &TestClusterNode, src: &str, dst: &str) {
    writer
        .client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN '{EDGES}' FROM '{src}' TO '{dst}' TYPE 'K'"
        ))
        .await
        .unwrap_or_else(|e| panic!("concurrent edge {src}->{dst}: {}", pg_detail(&e)));
    wait_for_async(
        &format!("edge {src}->{dst} visible before COMMIT"),
        Duration::from_secs(15),
        Duration::from_millis(50),
        || async {
            let msgs = probe
                .client
                .simple_query(&format!(
                    "GRAPH NEIGHBORS IN '{EDGES}' OF '{src}' DIRECTION out"
                ))
                .await
                .expect("probe neighbors");
            msgs.iter().any(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    (0..row.len()).any(|i| row.get(i).is_some_and(|v| v.contains(dst)))
                }
                _ => false,
            })
        },
    )
    .await;
}

fn pinned_native_client(node: &TestClusterNode) -> NativeClient {
    node.native_client_with(|base| PoolConfig {
        max_size: 1,
        ..base
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_concurrent_edge_on_a_read_vshard_aborts_the_match_transaction() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("3-node RF1 cluster");
    for coll in [EDGES, LOG] {
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

    let (groups, group_of): (BTreeSet<u64>, HashMap<u32, u64>) = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let groups: BTreeSet<u64> = routing
            .group_ids()
            .into_iter()
            .filter(|g| *g != 0)
            .collect();
        let mut map = HashMap::new();
        for &g in &groups {
            for vs in routing.vshards_for_group(g) {
                map.insert(vs, g);
            }
        }
        (groups, map)
    };
    wait_for(
        "each data group has exactly one replica and a leader",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            groups.iter().all(|&g| {
                cluster
                    .nodes
                    .iter()
                    .filter(|node| node.replicates_data_group(g))
                    .count()
                    == 1
            }) && {
                let leaders = cluster.data_group_leaders();
                groups.iter().all(|g| leaders.contains_key(g))
            }
        },
    )
    .await;

    let names = node_names(&groups, &group_of);
    let n = names.len();
    for i in 0..n {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN '{EDGES}' FROM '{}' TO '{}' TYPE 'K'",
                names[i],
                names[(i + 1) % n]
            ))
            .await
            .unwrap_or_else(|e| panic!("insert ring edge {i}: {}", pg_detail(&e)));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    // The reader's coordinator, and ring nodes whose key vShard another node
    // leads: a write there lands on a vShard the pattern read on a remote node.
    let coordinator = &cluster.nodes[0];
    // Each node hosts only the groups placed on it, so the leaders come from
    // every group's replica.
    let leaders: HashMap<u64, u64> = cluster.data_group_leaders();
    let remote_sources: Vec<&String> = names
        .iter()
        .filter(|name| {
            let vshard = VShardId::from_key(name.as_bytes()).as_u32();
            group_of
                .get(&vshard)
                .and_then(|g| leaders.get(g))
                .is_some_and(|leader| *leader != coordinator.node_id)
        })
        .collect();
    assert!(
        remote_sources.len() >= 2,
        "the ring must have nodes led by other cluster nodes"
    );
    let writer = &cluster.nodes[1];
    let probe = &cluster.nodes[2];
    let match_sql = format!("MATCH (a)-[:K]->(b) IN '{EDGES}' RETURN a, b");

    // Control: nothing writes the graph, so the read-set still holds.
    coordinator
        .client
        .simple_query("BEGIN")
        .await
        .expect("BEGIN");
    let rows = coordinator
        .client
        .simple_query(&match_sql)
        .await
        .unwrap_or_else(|e| panic!("in-txn MATCH: {}", pg_detail(&e)));
    assert_eq!(row_count(&rows), n, "the MATCH returns every ring edge");
    coordinator
        .client
        .simple_query("COMMIT")
        .await
        .unwrap_or_else(|e| {
            panic!(
                "a MATCH transaction with no concurrent write must commit: {}",
                pg_detail(&e)
            )
        });

    // pgwire, read-only: the MATCH read its snapshot, so COMMIT succeeds
    // across a concurrent edge.
    coordinator
        .client
        .simple_query("BEGIN")
        .await
        .expect("BEGIN");
    coordinator
        .client
        .simple_query(&match_sql)
        .await
        .unwrap_or_else(|e| panic!("in-txn MATCH: {}", pg_detail(&e)));
    concurrent_edge(writer, probe, remote_sources[0], "fresh_read_only").await;
    coordinator
        .client
        .simple_query("COMMIT")
        .await
        .unwrap_or_else(|e| {
            panic!(
                "a read-only MATCH transaction commits at its snapshot: {}",
                pg_detail(&e)
            )
        });

    // pgwire, with a write: a concurrent edge on a remote vShard aborts COMMIT.
    coordinator
        .client
        .simple_query("BEGIN")
        .await
        .expect("BEGIN");
    coordinator
        .client
        .simple_query(&match_sql)
        .await
        .unwrap_or_else(|e| panic!("in-txn MATCH: {}", pg_detail(&e)));
    coordinator
        .client
        .simple_query(&format!(
            "INSERT INTO {LOG} (id, value) VALUES ('entry_pg', '1')"
        ))
        .await
        .unwrap_or_else(|e| panic!("buffer a pgwire log row: {}", pg_detail(&e)));
    concurrent_edge(writer, probe, remote_sources[0], "fresh_pg").await;
    let err = coordinator
        .client
        .simple_query("COMMIT")
        .await
        .expect_err("a MATCH read-set with a concurrently written vShard must not commit");
    assert_eq!(
        pg_sqlstate(&err).as_deref(),
        Some("40001"),
        "expected serialization_failure (40001), got: {}",
        pg_detail(&err)
    );

    // Native, with a write: the commit goes through Calvin, and the vShard the
    // concurrent edge landed on is a participant that finds its read stale.
    let driver = pinned_native_client(coordinator);
    driver.begin().await.expect("native BEGIN");
    driver.query(&match_sql).await.expect("in-txn native MATCH");
    driver
        .query(&format!(
            "INSERT INTO {LOG} (id, value) VALUES ('entry', '1')"
        ))
        .await
        .expect("buffer a write into the log collection");
    concurrent_edge(writer, probe, remote_sources[1], "fresh_native").await;
    let err = driver
        .commit()
        .await
        .expect_err("a native MATCH read-set with a concurrently written vShard must not commit");
    assert!(
        err.message().contains(SERIALIZATION_ABORT),
        "expected a serialization abort, got: {err}"
    );

    cluster.shutdown().await;
}
