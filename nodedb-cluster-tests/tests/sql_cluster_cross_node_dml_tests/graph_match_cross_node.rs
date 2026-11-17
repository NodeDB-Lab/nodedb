// SPDX-License-Identifier: BUSL-1.1

//! Cross-node graph MATCH correctness (F1d-2 Phase B2).
//!
//! Graph edges are Raft-homed on `from_key(src)` and each core's CSR is
//! PARTITIONED (it holds only its owned nodes' out-edges). A multi-hop MATCH
//! pattern `(a)-[:K]->(b)-[:K]->(c)` therefore cannot complete on the
//! coordinator's local cores alone when the intermediate `b` (and the deeper
//! `c`) are homed on a DIFFERENT vShard than `a`: the edges off `b` live on
//! `b`'s owning shard.
//!
//! Phase B2 wires the Control-Plane scatter-all: round-0 broadcasts the
//! `Match` plan to LOCAL cores plus every distinct REMOTE owner, each shard
//! returns its completed rows plus an `UnresolvedExpansion` frontier of bound
//! zero-degree sources whose edges are homed elsewhere, and the coordinator
//! dispatches `MatchContinuation`s to the owning shards until no frontier
//! remains. This test proves the full 2-hop path rows come back from ANY
//! coordinator (round-0 scatter + >= 1 continuation round across the boundary),
//! and that a label-filtered MATCH over a cross-shard `GRAPH LABEL` (B-1) also
//! resolves cross-shard.
//!
//! Cross-shard placement is forced the same way the sibling traverse tests do:
//! many distinct `mid_i`/`dst_i` names spread across vShards spanning all three
//! nodes, so a meaningful fraction of the 2-hop chains cross a boundary. We
//! assert the COMPLETE, DISTINCT row set — a partial (continuation-dropping)
//! implementation would miss the cross-shard chains.

use std::collections::HashSet;
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// All `(a, b, c)` triples from a 2-hop `MATCH (a)-[:K]->(b)-[:K]->(c)` result
/// over pgwire simple-query (one row per match, columns `a`/`b`/`c`).
async fn match_2hop_rows(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Vec<(String, String, String)> {
    let msgs = client.simple_query(sql).await.expect("simple_query MATCH");
    let mut out = Vec::new();
    for m in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            let a = r.get("a").unwrap_or("").to_string();
            let b = r.get("b").unwrap_or("").to_string();
            let c = r.get("c").unwrap_or("").to_string();
            out.push((a, b, c));
        }
    }
    out
}

/// All `(b, c)` pairs from a label-filtered `MATCH (b:Label)-[:K]->(c)` result.
async fn match_labeled_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<(String, String)> {
    let msgs = client
        .simple_query(sql)
        .await
        .expect("simple_query labeled MATCH");
    let mut out = Vec::new();
    for m in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            let b = r.get("b").unwrap_or("").to_string();
            let c = r.get("c").unwrap_or("").to_string();
            out.push((b, c));
        }
    }
    out
}

/// A 2-hop `MATCH (a)-[:K]->(b)-[:K]->(c)` from ANY coordinator node must
/// return EVERY full path — including chains whose intermediate `b` and tail
/// `c` are homed on a different vShard than `a` — proving round-0 scatter plus
/// at least one cross-boundary continuation round.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn match_two_hop_returns_full_paths_cross_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION gmatch_xnode")
        .await
        .expect("CREATE COLLECTION gmatch_xnode");

    wait_for(
        "all 3 nodes see gmatch_xnode",
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

    // root -> mid_i -> dst_i for each i. Distinct `mid_i`/`dst_i` names spread
    // across vShards spanning all three nodes, so many of the second hops
    // (off `mid_i`) cross a boundary away from `root`'s shard — the case Phase
    // B2 continuations resolve.
    const CHAINS: usize = 12;
    for i in 0..CHAINS {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gmatch_xnode' FROM 'root' TO 'mid_{i}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert root -> mid_{i}: {e}"));
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gmatch_xnode' FROM 'mid_{i}' TO 'dst_{i}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert mid_{i} -> dst_{i}: {e}"));
    }

    // Expected full 2-hop path set.
    let expected: HashSet<(String, String, String)> = (0..CHAINS)
        .map(|i| ("root".to_string(), format!("mid_{i}"), format!("dst_{i}")))
        .collect();

    const MATCH_2HOP: &str = "MATCH (a)-[:K]->(b)-[:K]->(c) RETURN a, b, c";

    // Wait until EVERY node sees all CHAINS full paths — edges applied +
    // replicated AND each node's routing view converged so a non-owning
    // coordinator scatters round 0 and dispatches continuations to the owners.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all {CHAINS} 2-hop paths"),
            Duration::from_secs(25),
            Duration::from_millis(150),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = match_2hop_rows(&node.client, MATCH_2HOP).await;
                        let set: HashSet<_> = rows.into_iter().collect();
                        set == expected
                    })
                })
            },
        )
        .await;
    }

    // From every node: complete + distinct (no dup, no drop).
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = match_2hop_rows(&node.client, MATCH_2HOP).await;
        let set: HashSet<_> = rows.iter().cloned().collect();
        assert_eq!(
            rows.len(),
            set.len(),
            "node {idx} 2-hop MATCH returned duplicate rows: {rows:?}"
        );
        assert_eq!(
            set, expected,
            "node {idx} 2-hop MATCH must return every cross-shard path; got {rows:?}"
        );
    }

    // ── B-1 coverage: label a cross-shard intermediate, then a label-filtered
    // MATCH must resolve it cross-shard. `mid_7` lives on whatever vShard owns
    // it (distinct from `root` for most i); labeling it and matching
    // `(b:Hot)-[:K]->(c)` must still find `mid_7 -> dst_7` from ANY node.
    cluster.nodes[0]
        .client
        .simple_query("GRAPH LABEL 'mid_7' AS 'Hot'")
        .await
        .expect("GRAPH LABEL mid_7");

    const MATCH_LABELED: &str = "MATCH (b:Hot)-[:K]->(c) RETURN b, c";
    let expected_labeled = ("mid_7".to_string(), "dst_7".to_string());

    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees labeled cross-shard match"),
            Duration::from_secs(20),
            Duration::from_millis(150),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = match_labeled_rows(&node.client, MATCH_LABELED).await;
                        rows.contains(&expected_labeled)
                    })
                })
            },
        )
        .await;
    }

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = match_labeled_rows(&node.client, MATCH_LABELED).await;
        let set: HashSet<_> = rows.iter().cloned().collect();
        assert!(
            set.contains(&expected_labeled),
            "node {idx} labeled MATCH (b:Hot)-[:K]->(c) must find mid_7 -> dst_7; got {rows:?}"
        );
        // Only mid_7 carries the 'Hot' label, so exactly one labeled match.
        assert_eq!(
            set.len(),
            1,
            "node {idx} labeled MATCH must match only the single labeled node; got {rows:?}"
        );
    }

    cluster.shutdown().await;
}
