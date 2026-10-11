// SPDX-License-Identifier: BUSL-1.1

//! A graph algorithm fails when one Data-Plane core fails its round.
//!
//! Each node runs 2 cores, and each core holds only the graph nodes its own
//! vShards home to. The fail point `graph::wcc_superstep::core1` fails the
//! WCC round on core 1 of every node. The all-core gather must fail the query
//! with that core's error. Merging core 0 alone returns a partial component
//! set as a complete answer.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::collections::HashSet;
use std::time::Duration;

use nodedb_test_support::fail_point::FailGuard;

use crate::common::cluster_harness::{TestCluster, wait_for};

const WCC_SQL: &str = "GRAPH ALGO WCC ON 'gwcc_core_err'";
const CORE1_FAULT: &str = "graph::wcc_superstep::core1";
const FAULT_DETAIL: &str = "injected wcc core fault";
const CHAIN_LEN: usize = 12;

/// The `node_id` set of a WCC result, or the query's error text.
async fn wcc_nodes(client: &tokio_postgres::Client) -> Result<HashSet<String>, String> {
    let msgs = client
        .simple_query(WCC_SQL)
        .await
        .map_err(|e| match e.as_db_error() {
            Some(db) => format!("{}: {}", db.code().code(), db.message()),
            None => format!("{e}"),
        })?;
    let mut out = HashSet::new();
    for m in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            out.insert(r.get("node_id").unwrap_or("").to_string());
        }
    }
    Ok(out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn wcc_fails_when_one_core_fails() {
    let cluster = TestCluster::spawn_three_with_cores(2)
        .await
        .expect("3-node 2-core cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION gwcc_core_err")
        .await
        .expect("CREATE COLLECTION gwcc_core_err");

    wait_for(
        "all 3 nodes see gwcc_core_err",
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

    // The chain's names hash to vShards on every node and on both cores.
    for i in 0..CHAIN_LEN - 1 {
        let src = format!("c_{i}");
        let dst = format!("c_{}", i + 1);
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gwcc_core_err' FROM '{src}' TO '{dst}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {e}"));
    }
    let chain: HashSet<String> = (0..CHAIN_LEN).map(|i| format!("c_{i}")).collect();

    for idx in 0..cluster.nodes.len() {
        wait_for(
            &format!("node {idx} sees all {CHAIN_LEN} wcc nodes"),
            Duration::from_secs(30),
            Duration::from_millis(200),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        wcc_nodes(&cluster.nodes[idx].client).await.ok() == Some(chain.clone())
                    })
                })
            },
        )
        .await;
    }

    {
        let _fault = FailGuard::fail(CORE1_FAULT, FAULT_DETAIL);
        for idx in 0..cluster.nodes.len() {
            match wcc_nodes(&cluster.nodes[idx].client).await {
                Ok(nodes) => panic!(
                    "node {idx}: WCC returned {} of {CHAIN_LEN} nodes while core 1 failed; \
                     a failed core must fail the query",
                    nodes.len()
                ),
                Err(error) => assert!(
                    error.contains(FAULT_DETAIL),
                    "node {idx}: the query must fail with the core's own error, got {error}"
                ),
            }
        }
    }

    // With the fault cleared, every node returns the full component again.
    for idx in 0..cluster.nodes.len() {
        assert_eq!(
            wcc_nodes(&cluster.nodes[idx].client).await,
            Ok(chain.clone()),
            "node {idx}: WCC after the fault clears"
        );
    }

    cluster.shutdown().await;
}
