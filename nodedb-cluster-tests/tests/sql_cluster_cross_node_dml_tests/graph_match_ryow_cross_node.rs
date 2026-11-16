// SPDX-License-Identifier: BUSL-1.1

//! Cross-NODE read-your-own-writes for a variable-length graph MATCH inside an
//! explicit transaction, driven from a coordinator that does NOT lead the staged
//! edge's home group — so staging MUST forward to the remote leader and the
//! same-transaction expansion's remote legs must carry the txn id to observe it.
//!
//! Graph edges are Raft-homed on `from_key(src)`. When the edge is staged inside
//! `BEGIN..ROLLBACK` it lives only in the owning group's per-transaction overlay
//! on that group's leader. If the coordinator running the transaction is a
//! DIFFERENT node than that leader, the `GRAPH INSERT EDGE` must forward to the
//! leader to stage, and a same-transaction `MATCH (a)-[:K*1..k]->(b)` whose
//! expansion continues onto the staged edge's shard must tag its remote
//! continuation with the txn id to read the staged overlay back.
//!
//! Topology (durable chain plus one durable onward hop unreachable until staged):
//!
//! ```text
//!   durable:  c0 -> c1              c2 -> c3   (c2 unreachable from c0)
//!   staged :        c1 -> c2                   (the only bridge; src homed remote)
//! ```
//!
//! `c1` (the staged edge's src) is chosen so its home group's leader is a
//! DIFFERENT node than the coordinator, guaranteeing the forward. The test
//! proves that from every non-leader coordinator: before `BEGIN` the anchored
//! expansion reaches only `{c1}`; inside the txn it reaches `{c1, c2, c3}` (the
//! remotely-staged edge is traversed and the durable `c2 -> c3` hop beyond it is
//! reached through it); after `ROLLBACK` it reaches only `{c1}` again.

use std::collections::HashSet;
use std::time::Duration;

use nodedb_types::id::VShardId;

use crate::common::cluster_harness::{TestCluster, wait_for, wait_for_async};

/// Data-Plane cores per node — exercises the multi-core scatter/continuation
/// fan-out on each node while the edge stays cross-NODE.
const CORES_PER_NODE: usize = 2;

/// Number of chain nodes `c0 -> c1 -> c2 -> c3`.
const CHAIN_LEN: usize = 4;

/// Build `len` distinct graph-node keys whose CONSECUTIVE entries home to
/// DISTINCT vShards. `VShardId::from_key` is a pure function of the key bytes and
/// is exactly how edges home each endpoint, so every hop crosses a shard
/// boundary (and, across a 3-node cluster, frequently a node boundary).
fn spread_chain(prefix: &str, len: usize) -> Vec<String> {
    let mut chain: Vec<String> = Vec::with_capacity(len);
    let mut prev_vshard: Option<u32> = None;
    let mut i = 0u32;
    while chain.len() < len {
        let key = format!("{prefix}_{i}");
        let vshard = VShardId::from_key(key.as_bytes()).as_u32();
        if prev_vshard != Some(vshard) {
            prev_vshard = Some(vshard);
            chain.push(key);
        }
        i += 1;
        assert!(
            i < 100_000,
            "could not build a spread chain of length {len} for prefix '{prefix}'"
        );
    }
    chain
}

/// All `b` bindings from a `MATCH (a)-[:K*1..k]->(b) ... RETURN b` result.
async fn varlen_b_bindings(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let msgs = client
        .simple_query(sql)
        .await
        .expect("simple_query varlen MATCH");
    let mut out = Vec::new();
    for m in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            out.push(r.get("b").unwrap_or("").to_string());
        }
    }
    out
}

/// From one coordinator connection: assert the anchored varlen MATCH reaches
/// only `durable_only` before staging, `full_set` inside the txn after staging
/// the bridge edge, and `durable_only` again after ROLLBACK.
async fn assert_staged_ryow(
    label: &str,
    client: &tokio_postgres::Client,
    match_sql: &str,
    stage_edge_sql: &str,
    durable_only: &HashSet<String>,
    full_set: &HashSet<String>,
) {
    // The durable prefix must be visible from this coordinator before we stage.
    wait_for_async(
        &format!("{label}: durable prefix visible before staging"),
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            let set: HashSet<String> = varlen_b_bindings(client, match_sql)
                .await
                .into_iter()
                .collect();
            &set == durable_only
        },
    )
    .await;
    let before: HashSet<String> = varlen_b_bindings(client, match_sql)
        .await
        .into_iter()
        .collect();
    assert_eq!(
        &before, durable_only,
        "{label}: before staging, anchored varlen MATCH must reach only the durable \
         prefix {durable_only:?}; got {before:?}"
    );

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query(stage_edge_sql)
        .await
        .expect("remote-led staged bridge edge must be accepted (forwarded to leader)");

    // Inside the txn: the remotely-staged edge is traversed and the durable hop
    // beyond it is reached through it, so the full set is reachable.
    wait_for_async(
        &format!("{label}: in-txn anchored varlen MATCH drains to the full bridged set"),
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            let set: HashSet<String> = varlen_b_bindings(client, match_sql)
                .await
                .into_iter()
                .collect();
            &set == full_set
        },
    )
    .await;
    let in_txn: HashSet<String> = varlen_b_bindings(client, match_sql)
        .await
        .into_iter()
        .collect();
    assert_eq!(
        &in_txn, full_set,
        "{label}: inside the txn, the anchored varlen MATCH must traverse the \
         remotely-staged bridge and continue onto the durable node beyond it; \
         expected {full_set:?}, got {in_txn:?}"
    );

    client.simple_query("ROLLBACK").await.expect("ROLLBACK");
    let after: HashSet<String> = varlen_b_bindings(client, match_sql)
        .await
        .into_iter()
        .collect();
    assert_eq!(
        &after, durable_only,
        "{label}: after ROLLBACK the staged bridge must be gone; anchored varlen \
         MATCH must reach only {durable_only:?} again; got {after:?}"
    );
}

/// A graph edge staged from a NON-leader coordinator (forwarded to the remote
/// home leader) is read-your-own-writes visible to a same-transaction
/// variable-length MATCH whose expansion continues onto the staged edge's shard
/// — proving cross-node staging forwarding plus txn-tagged remote continuation.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn staged_varlen_edge_is_read_your_own_writes_remote_led() {
    let cluster = TestCluster::spawn_three_with_cores(CORES_PER_NODE)
        .await
        .expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION rgraph")
        .await
        .expect("CREATE COLLECTION rgraph");

    wait_for(
        "all 3 nodes see rgraph",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    // Every group must have a stable, non-zero leader before we resolve the
    // staged edge's home leader (and so before any write races election).
    wait_for(
        "all groups have a stable leader",
        Duration::from_secs(15),
        Duration::from_millis(100),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.all_group_leaders().iter().all(|(_, l)| *l != 0))
        },
    )
    .await;

    let chain = spread_chain("ryow", CHAIN_LEN);
    // The staged bridge is c1 -> c2; its src c1 fixes the home group (edges home
    // on `from_key(src)`), which must be led by a node other than the chosen
    // coordinator so staging forwards.
    let staged_src = &chain[1];
    let src_vshard = VShardId::from_key(staged_src.as_bytes()).as_u32();
    let src_group = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        routing
            .group_for_vshard(src_vshard)
            .expect("group for staged edge src vshard")
    };
    let src_leader = cluster.nodes[0]
        .all_group_leaders()
        .into_iter()
        .find(|(g, _)| *g == src_group)
        .map(|(_, l)| l)
        .expect("leader for staged edge src group");
    let coord = cluster
        .nodes
        .iter()
        .position(|n| n.node_id != src_leader)
        .expect("a non-leader coordinator for the staged edge's home group");
    assert_ne!(
        cluster.nodes[coord].node_id, src_leader,
        "coordinator must NOT lead the staged edge's home group so staging forwards"
    );

    // Durable prefix c0 -> c1 (reachable from anchor c0), plus a durable onward
    // hop c2 -> c3 that is UNREACHABLE from c0 until the staged c1 -> c2 bridge.
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN 'rgraph' FROM '{}' TO '{}' TYPE 'K'",
            chain[0], chain[1]
        ))
        .await
        .unwrap_or_else(|e| panic!("durable insert {} -> {}: {e}", chain[0], chain[1]));
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN 'rgraph' FROM '{}' TO '{}' TYPE 'K'",
            chain[2], chain[3]
        ))
        .await
        .unwrap_or_else(|e| panic!("durable onward insert {} -> {}: {e}", chain[2], chain[3]));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let anchor = &chain[0];
    // Depth 3 covers all three hops of the fully-bridged chain, within the
    // default `max_graph_depth` quota (10).
    let match_sql = format!("MATCH (a)-[:K*1..3]->(b) WHERE a = '{anchor}' RETURN b");
    let stage_edge_sql = format!(
        "GRAPH INSERT EDGE IN 'rgraph' FROM '{}' TO '{}' TYPE 'K'",
        chain[1], chain[2]
    );

    let durable_only: HashSet<String> = [chain[1].clone()].into_iter().collect();
    let full_set: HashSet<String> = (1..CHAIN_LEN).map(|i| chain[i].clone()).collect();

    // Primary proof: from the computed non-leader coordinator, staging forwards
    // to the remote leader and RYOW still holds inside the txn.
    assert_staged_ryow(
        &format!(
            "computed remote coordinator (node {})",
            cluster.nodes[coord].node_id
        ),
        &cluster.nodes[coord].client,
        &match_sql,
        &stage_edge_sql,
        &durable_only,
        &full_set,
    )
    .await;

    // Strengthening: every OTHER node that does not lead the staged edge's home
    // group must also observe RYOW through a remote-led staging forward.
    for node in &cluster.nodes {
        if node.node_id == src_leader {
            continue;
        }
        assert_staged_ryow(
            &format!("remote coordinator (node {})", node.node_id),
            &node.client,
            &match_sql,
            &stage_edge_sql,
            &durable_only,
            &full_set,
        )
        .await;
    }

    cluster.shutdown().await;
}
