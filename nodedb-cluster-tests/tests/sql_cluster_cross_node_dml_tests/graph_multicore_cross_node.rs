// SPDX-License-Identifier: BUSL-1.1

//! Multi-core (2 cores/node) + multi-node graph correctness.
//!
//! With a single core per node each node's CSR holds all edges for its owned
//! vShards on that one core. With TWO cores per node the vShards are split
//! across both cores — so a cross-node graph query that only fans out to core 0
//! of each node would silently miss the edges homed to core 1, returning roughly
//! half the data without any error. This test proves the per-core fan-out fix:
//! both PageRank and 2-hop MATCH must return EVERY node/path regardless of which
//! core holds the edge.
//!
//! Topology: a single directed ring `n_0 -> n_1 -> ... -> n_{N-1} -> n_0`. The
//! ring is deliberately chosen to match the single-core `graph_algo_pagerank_cross_node`
//! topology so the ONLY new variable is `spawn_three_with_cores(2)` — every node
//! has out-degree exactly 1 (no dangling nodes), so correct PageRank ranks sum to
//! 1.0 and each rank is 1/N. (Distributed dangling-mass aggregation is covered by
//! a separate, dedicated test.) The same ring yields exactly N two-hop MATCH
//! paths `(n_i, n_{i+1}, n_{i+2})`, whose intermediate hops cross core and node
//! boundaries — exercising cross-shard MATCH continuation under multi-core
//! fan-out.

use std::collections::HashSet;
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

// ── helpers (local copies; small duplication across independent test files is
//    intentional — see the existing per-file pattern) ────────────────────────

/// All `(node_id, rank)` rows from a `GRAPH ALGO PAGERANK` result.
async fn pagerank_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<(String, f64)> {
    let msgs = client
        .simple_query(sql)
        .await
        .expect("simple_query PAGERANK");
    let mut out = Vec::new();
    for m in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            let node = r.get("node_id").unwrap_or("").to_string();
            let rank: f64 = r.get("rank").unwrap_or("0").parse().unwrap_or(0.0);
            out.push((node, rank));
        }
    }
    out
}

/// All `(a, b, c)` triples from a 2-hop `MATCH (a)-[:K]->(b)-[:K]->(c)` result.
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

// ── test ────────────────────────────────────────────────────────────────────

/// Proves that cross-node graph reads see EVERY core's slice of a node, not
/// just core 0. With 2 cores/node the ring's vShards spread across both cores of
/// each node; a correct per-core fan-out returns complete results, an incorrect
/// (core-0-only) implementation would drop ~half the data.
///
/// Two sub-assertions in one test to amortise the cluster spin-up cost:
///   1. PageRank completeness — exactly N distinct node_ids returned, ranks
///      sum to ≈ 1.0 (mass conserved across all cores on all nodes).
///   2. 2-hop MATCH completeness — every `(n_i, n_{i+1}, n_{i+2})` ring path is
///      returned from every coordinator, including chains whose intermediate
///      node lives on a different core/node than the anchor.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn graph_pagerank_and_match_multicore_cross_node() {
    // ── 2 cores per node — the ONLY structural difference from the
    //    single-core cross-node siblings.
    let cluster = TestCluster::spawn_three_with_cores(2)
        .await
        .expect("3-node 2-core cluster");

    // ── CREATE COLLECTION ─────────────────────────────────────────────────
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION gmc_xnode")
        .await
        .expect("CREATE COLLECTION gmc_xnode");

    wait_for(
        "all 3 nodes see gmc_xnode",
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

    // ── INSERT: directed ring n_0 -> n_1 -> ... -> n_{N-1} -> n_0 ──────────
    //
    // N=24 distinct node names hash across vShards spanning all three nodes.
    // With 2 cores per node each node's vShards split across both cores, so a
    // ring hop (and every second hop in a 2-hop MATCH) may cross a core
    // boundary as well as a node boundary. Every node has out-degree exactly 1
    // — no dangling nodes — so PageRank converges to 1/N per node and the ranks
    // sum precisely to 1.0.
    const N: usize = 24;
    for i in 0..N {
        let src = format!("n_{i}");
        let dst = format!("n_{}", (i + 1) % N);
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gmc_xnode' FROM '{src}' TO '{dst}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {e}"));
    }

    // Wait for all edges to propagate to every Raft group on every node before
    // querying — the authoritative applied-watermark barrier.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let ring_nodes: HashSet<String> = (0..N).map(|i| format!("n_{i}")).collect();

    // ITERATIONS 100: distributed BSP delivers cross-shard contributions with a
    // one-superstep delay, so each superstep's ranks only sum to 1.0 once the
    // ghost-predecessor ranks have converged. With 2 cores/node, intra-node edges
    // that were same-core (instant) at 1 core/node become cross-CORE ghosts
    // (delayed), so this ring needs more supersteps to converge than the default
    // budget the single-core sibling relies on. The converged result is identical
    // to single-node PageRank; the larger budget just guarantees convergence so
    // the mass-conservation invariant holds. (This graph has NO dangling nodes —
    // distributed dangling-mass aggregation is a separate concern.)
    const PAGERANK_SQL: &str = "GRAPH ALGO PAGERANK ON 'gmc_xnode' ITERATIONS 100";

    // Wait until EVERY node sees all N ring nodes in the PageRank result.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all {N} pagerank nodes (multicore)"),
            Duration::from_secs(30),
            Duration::from_millis(200),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = pagerank_rows(&node.client, PAGERANK_SQL).await;
                        rows.len() == N
                    })
                })
            },
        )
        .await;
    }

    // From every node: assert PageRank completeness + mass conservation.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = pagerank_rows(&node.client, PAGERANK_SQL).await;

        // (a) COMPLETENESS — exactly the N ring nodes; no dup, no drop.
        let names: HashSet<String> = rows.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(
            rows.len(),
            names.len(),
            "node {idx} PAGERANK returned duplicate node rows (multicore): {rows:?}"
        );
        assert_eq!(
            names, ring_nodes,
            "node {idx} PAGERANK must return exactly the N ring nodes (multicore) — \
             core-0-only fan-out bug? got {rows:?}"
        );

        // (b) Mass conservation: ranks sum to ≈ 1.0.
        let sum: f64 = rows.iter().map(|(_, r)| *r).sum();
        assert!(
            (sum - 1.0).abs() < 1e-3,
            "node {idx} multicore cross-shard PageRank ranks must sum to ~1.0; got {sum}"
        );
    }

    // ── 2-hop MATCH: expected set = the N ring chains (n_i, n_{i+1}, n_{i+2}) ─
    let expected_paths: HashSet<(String, String, String)> = (0..N)
        .map(|i| {
            (
                format!("n_{i}"),
                format!("n_{}", (i + 1) % N),
                format!("n_{}", (i + 2) % N),
            )
        })
        .collect();

    const MATCH_2HOP: &str = "MATCH (a)-[:K]->(b)-[:K]->(c) RETURN a, b, c";

    // Wait until every node sees all N ring 2-hop paths.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all {N} 2-hop paths (multicore)"),
            Duration::from_secs(25),
            Duration::from_millis(150),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = match_2hop_rows(&node.client, MATCH_2HOP).await;
                        let set: HashSet<_> = rows.into_iter().collect();
                        set == expected_paths
                    })
                })
            },
        )
        .await;
    }

    // From every node: MATCH completeness — exactly the N ring chains, no dups.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = match_2hop_rows(&node.client, MATCH_2HOP).await;
        let set: HashSet<_> = rows.iter().cloned().collect();

        assert_eq!(
            rows.len(),
            set.len(),
            "node {idx} 2-hop MATCH returned duplicate rows (multicore): {rows:?}"
        );
        assert_eq!(
            set, expected_paths,
            "node {idx} 2-hop MATCH must return exactly the N ring chains (multicore) — \
             core-0-only fan-out bug? got {rows:?}"
        );
    }

    cluster.shutdown().await;
}
