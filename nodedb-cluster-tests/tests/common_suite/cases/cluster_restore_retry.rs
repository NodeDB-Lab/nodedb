// SPDX-License-Identifier: BUSL-1.1

//! A RESTORE that failed part-way is retried without `FORCE` and converges.
//!
//! The fail point `restore::reissue::before_edges` fails the first attempt
//! after the document rows committed and before any edge. Those rows raised
//! write marks newer than the envelope's watermark. Each mark carries the id
//! of the restore that wrote it, so the retry of the same envelope passes the
//! staleness guard and writes every row and edge. A client write after that
//! still refuses a further restore of the envelope.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::collections::HashSet;
use std::time::Duration;

use nodedb_test_support::fail_point::FailGuard;

use crate::common::cluster_harness::shared_steps::{db_detail, drain_backup, try_push_restore};
use crate::common::cluster_harness::{TestCluster, wait_for, wait_for_async};

const TENANT: u64 = 1;
const DOCS: &str = "rr_docs";
const GRAPH: &str = "rr_graph";
const ROWS: usize = 6;
const FAULT: &str = "restore::reissue::before_edges";

async fn count_docs(client: &tokio_postgres::Client) -> Option<usize> {
    let rows = client
        .simple_query(&format!("SELECT COUNT(*) FROM {DOCS}"))
        .await
        .ok()?;
    rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).and_then(|s| s.parse().ok()),
        _ => None,
    })
}

/// The sources a reverse 1-hop traversal from `hub` reaches, or `None` while
/// the query fails.
async fn hub_sources(client: &tokio_postgres::Client) -> Option<HashSet<String>> {
    let sql = format!("GRAPH TRAVERSE IN '{GRAPH}' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in");
    let msgs = client.simple_query(&sql).await.ok()?;
    let raw = msgs.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get("result").map(str::to_string),
        _ => None,
    })?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    Some(
        value
            .get("nodes")?
            .as_array()?
            .iter()
            .filter_map(|n| n.get("id").and_then(|id| id.as_str()).map(str::to_string))
            .filter(|id| id != "hub")
            .collect(),
    )
}

fn sources() -> HashSet<String> {
    (0..ROWS).map(|i| format!("src_{i}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_restore_failed_part_way_retries_without_force() {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    source
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {DOCS} (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE docs");
    source
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {GRAPH}"))
        .await
        .expect("CREATE graph");
    wait_for(
        "every source node sees both collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            source
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 2)
        },
    )
    .await;
    for i in 0..ROWS {
        for sql in [
            format!("INSERT INTO {DOCS} (id, v) VALUES ('k{i}', 'v{i}')"),
            format!("GRAPH INSERT EDGE IN '{GRAPH}' FROM 'src_{i}' TO 'hub' TYPE 'l'"),
        ] {
            source.nodes[0]
                .client
                .simple_query(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {}", db_detail(&e)));
        }
    }
    source
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
    let bytes = drain_backup(&source.nodes[0].client, TENANT).await;
    source.shutdown().await;

    let target = TestCluster::spawn_three().await.expect("target cluster");

    // First attempt: the rows commit, then the re-issue fails before edges.
    {
        let _fault = FailGuard::fail(FAULT, "injected before the edge re-issue");
        let refused = try_push_restore(&target.nodes[0].client, TENANT, bytes.clone()).await;
        assert!(refused.is_err(), "the first RESTORE must fail at {FAULT}");
    }
    target
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    // Retry without FORCE: the guard knows the first attempt's writes.
    try_push_restore(&target.nodes[0].client, TENANT, bytes.clone())
        .await
        .unwrap_or_else(|e| panic!("the retried RESTORE must pass the guard: {e}"));
    target
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    for idx in 0..target.nodes.len() {
        let client = &target.nodes[idx].client;
        wait_for_async(
            &format!("node {idx} holds every restored row and edge"),
            Duration::from_secs(30),
            Duration::from_millis(100),
            || async move {
                count_docs(client).await == Some(ROWS)
                    && hub_sources(client).await == Some(sources())
            },
        )
        .await;
    }

    // A client write after the restore still refuses a further restore of
    // the same envelope.
    target.nodes[0]
        .client
        .simple_query(&format!("INSERT INTO {DOCS} (id, v) VALUES ('late', 'x')"))
        .await
        .unwrap_or_else(|e| panic!("late insert: {}", db_detail(&e)));
    let refused = try_push_restore(&target.nodes[0].client, TENANT, bytes)
        .await
        .expect_err("a client write newer than the envelope refuses the restore");
    assert!(
        refused.contains("restore refused"),
        "the guard names the refusal: {refused}"
    );

    target.shutdown().await;
}
