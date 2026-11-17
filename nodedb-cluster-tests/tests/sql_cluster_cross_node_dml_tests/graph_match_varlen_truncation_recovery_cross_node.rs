// SPDX-License-Identifier: BUSL-1.1

//! Cross-node variable-length MATCH truncation RECOVERY (completeness proof).
//!
//! A variable-length expansion `MATCH (a)-[:K*1..N]->(b)` is bounded by a hard
//! per-expansion cap (`graph.varlen_max_results` / `graph.varlen_max_frontier`,
//! default 100k). When an expansion exceeds the cap it MUST NOT silently drop
//! rows: the executor truncates at the current hop boundary and surfaces a
//! name-keyed resume cursor; the cross-shard coordinator re-dispatches the
//! cursor (fanned to all cores on the owning node) and drains it across resume
//! rounds, deduping rows, until nothing remains.
//!
//! This test makes the cap a real OPERATIONAL knob (not a test hack): the
//! cluster is spawned with a LOW `varlen_max_results` via node graph tuning, so
//! truncation fires on a small graph instead of needing 100k+ nodes. The graph
//! is a directed chain `n_0 -> n_1 -> ... -> n_{N-1}` with N > cap whose node
//! names hash across vShards spanning all three nodes, so the expansion both
//! crosses shard boundaries AND truncates at the cap.
//!
//! The completeness invariant: from EVERY node, `MATCH (a)-[:K*1..N]->(b)`
//! anchored at `n_0` must return the COMPLETE reachable set `{n_1 .. n_{N-1}}`
//! — exactly what a single uncapped pass would return. A truncation-dropping
//! implementation would return fewer rows; the recovery pipeline must drain the
//! resume rounds to the full set. Multiple cores per node exercise the
//! multi-core name-keyed resume fan-out.

use std::collections::HashSet;
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Length of the directed chain `n_0 -> ... -> n_{N-1}` (N nodes, N-1 edges).
/// Chosen so the reachable tail (N-1 nodes) far exceeds `VARLEN_CAP` (forcing
/// repeated truncation + resume) while the query depth `N-1` stays within the
/// default `max_graph_depth` tenant quota (10), so the pattern is admitted and
/// the varlen *result* cap — not the depth quota — is what drives truncation.
const CHAIN_LEN: usize = 10;

/// Low per-expansion result cap wired via node graph tuning. Far below
/// CHAIN_LEN-1 reachable nodes, so the expansion truncates repeatedly and only
/// the cross-shard resume drain can recover the complete set.
const VARLEN_CAP: usize = 3;

/// Data-Plane cores PER NODE — exercises the multi-core name-keyed resume path
/// (a resume cursor is fanned to all cores; only the owning core resolves it).
const CORES_PER_NODE: usize = 2;

/// All `b` bindings from a `MATCH (a)-[:K*1..N]->(b) ... RETURN b` result.
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

/// A capped variable-length `MATCH (a)-[:K*1..N]->(b)` anchored at `n_0` must,
/// from ANY coordinator node, return the COMPLETE reachable set `{n_1 ..
/// n_{N-1}}` — proving the cross-shard truncation-recovery pipeline drains
/// every resume round rather than silently dropping rows at the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn varlen_match_capped_drains_to_complete_set_cross_node() {
    // Spawn with a LOW varlen result cap (the operational knob) and multiple
    // cores per node so the multi-core resume fan-out is exercised.
    let cluster = TestCluster::spawn_three_with_varlen_caps_and_cores(
        CORES_PER_NODE,
        VARLEN_CAP,
        // Leave the frontier cap at its default so truncation is driven by the
        // result cap deterministically (a chain has a width-1 frontier anyway).
        nodedb_types::config::tuning::DEFAULT_VARLEN_MAX_FRONTIER,
    )
    .await
    .expect("3-node cluster with low varlen cap");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION gvarlen_xnode")
        .await
        .expect("CREATE COLLECTION gvarlen_xnode");

    wait_for(
        "all 3 nodes see gvarlen_xnode",
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

    // Directed chain n_0 -> n_1 -> ... -> n_{CHAIN_LEN-1}. Distinct names hash
    // across vShards spanning all three nodes, so the expansion crosses shard
    // boundaries (cross-shard continuation) AND exceeds VARLEN_CAP (truncation
    // + resume). Both recovery mechanisms must compose for completeness.
    for i in 0..CHAIN_LEN - 1 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gvarlen_xnode' FROM 'n_{i}' TO 'n_{}' TYPE 'K'",
                i + 1
            ))
            .await
            .unwrap_or_else(|e| panic!("insert n_{i} -> n_{}: {e}", i + 1));
    }

    // Ground truth: a single UNCAPPED `*1..N` pass from n_0 reaches every later
    // node on the chain, i.e. {n_1 .. n_{CHAIN_LEN-1}}. This is what the capped
    // distributed query must reproduce by draining resume rounds.
    let expected: HashSet<String> = (1..CHAIN_LEN).map(|i| format!("n_{i}")).collect();
    assert_eq!(
        expected.len(),
        CHAIN_LEN - 1,
        "ground-truth reachable set is the whole chain tail"
    );
    assert!(
        expected.len() > VARLEN_CAP,
        "reachable set ({}) must exceed the cap ({VARLEN_CAP}) so truncation \
         actually fires and recovery is exercised",
        expected.len()
    );

    // `*1..(CHAIN_LEN-1)` covers the whole chain tail; anchor at n_0 via WHERE so
    // `b` ranges over exactly the reachable tail. The depth `CHAIN_LEN-1` stays
    // within the default `max_graph_depth` quota so the pattern is admitted.
    let depth = CHAIN_LEN - 1;
    let match_sql = format!("MATCH (a)-[:K*1..{depth}]->(b) WHERE a = 'n_0' RETURN b");

    // Wait until EVERY node converges on the COMPLETE set — edges applied +
    // replicated AND routing converged so a non-owning coordinator scatters,
    // continues across boundaries, and drains every truncation resume round.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let sql = match_sql.clone();
        let expected_ref = &expected;
        wait_for(
            &format!("node {idx} drains capped varlen MATCH to complete set"),
            Duration::from_secs(30),
            Duration::from_millis(150),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = varlen_b_bindings(&node.client, &sql).await;
                        let set: HashSet<String> = rows.into_iter().collect();
                        &set == expected_ref
                    })
                })
            },
        )
        .await;
    }

    // Hard assertion from every node: the DISTINCT result set equals the
    // COMPLETE expected set. A truncation-dropping implementation returns a
    // strict subset; the recovery pipeline must return all CHAIN_LEN-1 nodes.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = varlen_b_bindings(&node.client, &match_sql).await;
        let set: HashSet<String> = rows.iter().cloned().collect();
        assert_eq!(
            set,
            expected,
            "node {idx}: capped varlen MATCH must drain to the COMPLETE reachable \
             set {{n_1..n_{}}}; got {rows:?}",
            CHAIN_LEN - 1
        );
    }

    cluster.shutdown().await;
}
