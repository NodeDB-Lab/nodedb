// SPDX-License-Identifier: BUSL-1.1

//! Cross-node distributed Personalized PageRank (PPR) correctness.
//!
//! Mirrors the single-node tests `personalized_pagerank_biases_toward_seed` and
//! `personalized_pagerank_unknown_seed_falls_back_to_uniform` over a 3-node
//! cluster. The distributed BSP path must match single-node semantics EXACTLY:
//!
//! 1. A seed concentrated on one node (`PERSONALIZATION {"n_0": 1.0}`) biases
//!    teleport AND dangling mass toward `n_0`, lifting its rank strictly above
//!    its peers and strictly above the uniform baseline `1/N`.
//! 2. Mass is conserved: ranks sum to ≈ 1.0 across all shards (the coordinator
//!    normalizes the seed by the CLUSTER-WIDE sum, never per-shard).
//! 3. An all-unknown seed (no name exists in the graph) falls back to uniform
//!    PageRank — the count phase reports zero cluster-wide seed hits, so the
//!    coordinator clears personalization and every node converges to `1/N`.
//!
//! The graph is a single directed ring `n_0 -> n_1 -> ... -> n_{N-1} -> n_0`
//! over N distinct names that hash to vShards spanning all three nodes, forcing
//! genuine cross-shard rank pushes (same fixture style as the standard
//! distributed PageRank test).

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn personalized_pagerank_biases_toward_seed_cross_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION gppr_xnode")
        .await
        .expect("CREATE COLLECTION gppr_xnode");

    wait_for(
        "all 3 nodes see gppr_xnode",
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

    // Ring over N distinct names spanning all three nodes.
    const N: usize = 24;
    for i in 0..N {
        let src = format!("n_{i}");
        let dst = format!("n_{}", (i + 1) % N);
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gppr_xnode' FROM '{src}' TO '{dst}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {e}"));
    }

    let expected_nodes: HashSet<String> = (0..N).map(|i| format!("n_{i}")).collect();

    // PERSONALIZATION takes a brace object literal (NOT a quoted string) — this is
    // exactly how the GRAPH ALGO parser tokenizes `PERSONALIZATION {…}`.
    //
    // Explicit `ITERATIONS 100`: seed-dominance is a CONVERGENCE property. On an
    // N-node directed ring the unit of initial mass placed on the seed travels the
    // ring as a decaying pulse that out-ranks the seed until it dissipates (~N
    // hops), and distributed BSP lags by one superstep per ghost hop. The default
    // 20 supersteps do not reliably converge a 24-node cross-shard ring, so we
    // request enough supersteps to reach the steady state the single-node test
    // (3-node ring) hits within the default budget. Well under the 1000 cap.
    const PPR_SQL: &str =
        r#"GRAPH ALGO PAGERANK ON 'gppr_xnode' ITERATIONS 100 PERSONALIZATION {"n_0": 1.0}"#;

    // Wait until every node sees all N nodes in the result (edges replicated +
    // routing converged) before asserting.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all {N} ppr nodes"),
            Duration::from_secs(30),
            Duration::from_millis(200),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = pagerank_rows(&node.client, PPR_SQL).await;
                        let names: HashSet<String> = rows.into_iter().map(|(n, _)| n).collect();
                        names == expected_nodes
                    })
                })
            },
        )
        .await;
    }

    let uniform = 1.0 / N as f64;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = pagerank_rows(&node.client, PPR_SQL).await;
        let ranks: HashMap<String, f64> = rows.iter().cloned().collect();

        assert_eq!(
            ranks.len(),
            N,
            "node {idx} PPR must return one row per distinct node; got {rows:?}"
        );

        let seed_rank = *ranks.get("n_0").expect("seed node n_0 present");

        // (i) Seeded node has strictly-max rank and is strictly above uniform.
        for (name, &r) in &ranks {
            if name != "n_0" {
                assert!(
                    seed_rank > r,
                    "node {idx}: seed n_0={seed_rank} must outrank {name}={r}"
                );
            }
        }
        assert!(
            seed_rank > uniform + 1e-6,
            "node {idx}: seed n_0={seed_rank} must exceed uniform {uniform}"
        );

        // (ii) Mass conservation: ranks sum to ≈ 1.0 (cluster-wide normalization).
        let sum: f64 = ranks.values().sum();
        assert!(
            (sum - 1.0).abs() < 1e-3,
            "node {idx} PPR ranks must sum to ~1.0 (mass conserved); got {sum}"
        );
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn personalized_pagerank_unknown_seed_falls_back_to_uniform_cross_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION gppr_unk_xnode")
        .await
        .expect("CREATE COLLECTION gppr_unk_xnode");

    wait_for(
        "all 3 nodes see gppr_unk_xnode",
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

    // Symmetric ring → exact uniform PageRank is 1/N each. A seed naming only a
    // nonexistent node must NOT zero the result — it falls back to uniform.
    const N: usize = 24;
    for i in 0..N {
        let src = format!("n_{i}");
        let dst = format!("n_{}", (i + 1) % N);
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gppr_unk_xnode' FROM '{src}' TO '{dst}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {e}"));
    }

    let expected_nodes: HashSet<String> = (0..N).map(|i| format!("n_{i}")).collect();

    const UNK_SQL: &str =
        r#"GRAPH ALGO PAGERANK ON 'gppr_unk_xnode' PERSONALIZATION {"does_not_exist": 1.0}"#;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all {N} unknown-seed nodes"),
            Duration::from_secs(30),
            Duration::from_millis(200),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = pagerank_rows(&node.client, UNK_SQL).await;
                        let names: HashSet<String> = rows.into_iter().map(|(n, _)| n).collect();
                        names == expected_nodes
                    })
                })
            },
        )
        .await;
    }

    let uniform = 1.0 / N as f64;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = pagerank_rows(&node.client, UNK_SQL).await;
        assert_eq!(rows.len(), N, "node {idx} must return one row per node");

        // Unknown seed → uniform fallback: every rank ≈ 1/N.
        for (name, r) in &rows {
            assert!(
                (r - uniform).abs() < 1e-3,
                "node {idx}: unknown-seed must fall back to uniform; {name}={r} != {uniform}"
            );
        }

        let sum: f64 = rows.iter().map(|(_, r)| *r).sum();
        assert!(
            (sum - 1.0).abs() < 1e-3,
            "node {idx} unknown-seed ranks must sum to ~1.0; got {sum}"
        );
    }

    cluster.shutdown().await;
}
