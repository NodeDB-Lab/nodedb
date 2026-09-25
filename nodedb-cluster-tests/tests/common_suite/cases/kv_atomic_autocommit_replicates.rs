// SPDX-License-Identifier: BUSL-1.1
//! An autocommit SQL-function write reaches every replica.
//!
//! `KV_INCR` and `CREATE SORTED INDEX` build their `KvOp` by hand instead of
//! planning a statement. In cluster mode each one must be proposed through
//! the data group's Raft log like a planned write, so every replica applies
//! it. A write applied on the receiving node alone exists nowhere else.
//!
//! - `KV_INCR` runs on the counter's data-group leader, and the test kills
//!   that leader. A survivor must read the incremented counter: had the write
//!   applied on the leader alone, it died with it.
//! - `CREATE SORTED INDEX` builds a tree on the core that owns the rows. A
//!   sorted-index read runs on the node that receives it, so a count read on
//!   each follower reads that follower's own tree.

use crate::common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use nodedb::types::{DatabaseId, VShardId};

const COUNTERS: &str = "repl_kv_ctr";
const BOARD: &str = "repl_kv_board";
const INDEX: &str = "repl_kv_board_idx";

fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

/// The first column of the first row `sql` returns, or the error it raised.
async fn first_cell(client: &tokio_postgres::Client, sql: &str) -> Result<Option<String>, String> {
    let rows = client.simple_query(sql).await.map_err(|e| pg_detail(&e))?;
    Ok(rows.into_iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
        _ => None,
    }))
}

/// Whether a `SORTED_COUNT` read returned a count of three.
fn counts_three(read: &Result<Option<String>, String>) -> bool {
    matches!(read, Ok(Some(doc)) if doc.replace(' ', "").contains("\"count\":3"))
}

/// The leader node id of the data group that owns `collection`.
fn group_leader(cluster: &TestCluster, collection: &str) -> u64 {
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, collection);
    let routing = cluster.nodes[0]
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let group = routing
        .group_for_vshard(vshard.as_u32())
        .expect("the collection's vShard maps to a data group");
    routing
        .group_info(group)
        .map(|info| info.leader)
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_autocommit_kv_incr_survives_its_leader() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COUNTERS} (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')"
        ))
        .await
        .expect("create the counter collection");
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {COUNTERS} (key, n) VALUES ('ctr', 5)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed the counter: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let leader_id = group_leader(&cluster, COUNTERS);
    assert_ne!(leader_id, 0, "the counter's data group has no leader");
    let leader = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == leader_id)
        .expect("the leader node is in the cluster");
    let incremented = first_cell(
        &leader.client,
        &format!("SELECT KV_INCR('{COUNTERS}', 'ctr', 3)"),
    )
    .await
    .unwrap_or_else(|e| panic!("KV_INCR on the group leader: {e}"));
    assert!(
        incremented.as_deref().is_some_and(|doc| doc.contains('8')),
        "the live KV_INCR returns 5 + 3: {incremented:?}"
    );
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let mut nodes = cluster.nodes;
    let leader_idx = nodes
        .iter()
        .position(|n| n.node_id == leader_id)
        .expect("leader node present");
    nodes.remove(leader_idx).shutdown().await;

    let read = format!("SELECT n FROM {COUNTERS} WHERE key = 'ctr'");
    for node in &nodes {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut last = Err(String::from("never read"));
        while Instant::now() < deadline {
            last = first_cell(&node.client, &read).await;
            if matches!(&last, Ok(Some(n)) if n == "8") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_eq!(
            last,
            Ok(Some("8".to_string())),
            "survivor node {} must read the counter the autocommit KV_INCR moved; 5 means \
             the increment applied on the killed leader alone",
            node.node_id
        );
    }

    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sorted_index_is_built_on_every_replica() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {BOARD} (k TEXT PRIMARY KEY, score INT) WITH (engine='kv')"
        ))
        .await
        .expect("create the board collection");
    for (key, score) in [("p0", 10), ("p1", 20), ("p2", 30)] {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {BOARD} (k, score) VALUES ('{key}', {score})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {key}: {}", pg_detail(&e)));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let leader_id = group_leader(&cluster, BOARD);
    assert_ne!(leader_id, 0, "the board's data group has no leader");
    let leader = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == leader_id)
        .expect("the leader node is in the cluster");
    leader
        .client
        .simple_query(&format!(
            "CREATE SORTED INDEX {INDEX} ON {BOARD} (score DESC) KEY k"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE SORTED INDEX: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let read = format!("SELECT SORTED_COUNT({INDEX})");
    for node in cluster.nodes.iter().filter(|n| n.node_id != leader_id) {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut last = Err(String::from("never read"));
        while Instant::now() < deadline {
            last = first_cell(&node.client, &read).await;
            if counts_three(&last) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(
            counts_three(&last),
            "follower node {} must count every row in its own replica of the index tree; \
             a missing tree means the registration applied on the receiving node alone \
             (last read: {last:?})",
            node.node_id
        );
    }

    for node in cluster.nodes {
        node.shutdown().await;
    }
}
