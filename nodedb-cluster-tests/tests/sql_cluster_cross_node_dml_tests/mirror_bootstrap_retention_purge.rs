// SPDX-License-Identifier: BUSL-1.1

//! An audit purge decided on a source removes the same history from its mirror.
//!
//! Each test runs two clusters joined the way an operator joins them. A test puts
//! a decision into effect itself, as the metadata leader's loop does, so it waits
//! for no policy interval.

use std::time::Duration;

use nodedb::control::retention_log::{AuditPurge, enact_audit_retention};
use nodedb::engine::bitemporal::BitemporalEngineKind;
use nodedb_types::DatabaseId;

use super::mirror_bootstrap_fixture::{await_mirror_reads, joined_rows, rows};
use super::mirror_bootstrap_retention_fixture::{
    TENANT, now_ms, tuned_following_mirror, tuned_source_with,
};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for_async};

/// How long a decision waits for the versions to pass every horizon.
const PURGE_REACH: Duration = Duration::from_secs(150);

async fn decide_purge(
    source: &TestCluster,
    database: DatabaseId,
    engine: BitemporalEngineKind,
    object: &str,
) {
    enact_audit_retention(
        &source.nodes[0].shared,
        AuditPurge {
            tenant_id: TENANT,
            database_id: database,
            engine,
            object,
            retain_ms: 1,
        },
    )
    .await
    .expect("the decision lands");
}

/// A read at a purged cut, or `None` while the node cannot answer at all. A purge
/// can leave the cut unreadable: that refusal is an answer too.
async fn probe(node: &TestClusterNode, sql: &str) -> Option<Vec<String>> {
    match node.client.simple_query(sql).await {
        Ok(messages) => Some(joined_rows(&messages)),
        Err(error) if error.as_db_error().is_some() => Some(vec!["refused".to_string()]),
        Err(_) => None,
    }
}

/// Decide until `sql`, a read of a cut below the cutoff, stops answering `before`
/// on both clusters.
async fn decide_until_the_cut_is_gone(
    source: &TestCluster,
    mirror: &TestCluster,
    database: DatabaseId,
    purge: (BitemporalEngineKind, &str),
    sql: &str,
    before: &[String],
) {
    wait_for_async(
        "the version below the cutoff leaves every copy",
        PURGE_REACH,
        Duration::from_secs(1),
        || async {
            decide_purge(source, database, purge.0, purge.1).await;
            let mut gone = true;
            for node in source.nodes.iter().chain(&mirror.nodes) {
                let answer = probe(node, sql).await;
                gone &= answer.is_some_and(|rows| rows != before);
            }
            gone
        },
    )
    .await;
}

/// Decide until `sql`, a read of every version, holds one row on both clusters.
async fn decide_until_one_version_stays(
    source: &TestCluster,
    mirror: &TestCluster,
    database: DatabaseId,
    purge: (BitemporalEngineKind, &str),
    sql: &str,
) {
    wait_for_async(
        "the history leaves every copy",
        PURGE_REACH,
        Duration::from_secs(1),
        || async {
            decide_purge(source, database, purge.0, purge.1).await;
            let source_kept = rows(&source.nodes[0], sql).await.len();
            let mirror_kept = rows(&mirror.nodes[0], sql).await.len();
            source_kept == 1 && mirror_kept == 1
        },
    )
    .await;
}

/// Two versions of `sql`'s row, with a cut between them.
async fn write_around_a_cut(source: &TestCluster, older: &[&str], newer: &[&str]) -> i64 {
    for statement in older {
        source.nodes[0]
            .exec(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cut = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;
    for statement in newer {
        source.nodes[0]
            .exec(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    cut
}

/// An audit purge removes history on the source and on its mirror. The current
/// row stays on both.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_purges_the_history_its_source_purges() {
    const DATABASE: &str = "retention_audit";
    const HISTORY: &str = "SELECT _ts_system FROM r_hist AS OF SYSTEM TIME NULL";
    let source = tuned_source_with(
        DATABASE,
        &["CREATE COLLECTION r_hist (id TEXT PRIMARY KEY, n INT) \
           WITH (engine='document_strict', bitemporal=true)"],
    )
    .await;
    source.nodes[0]
        .exec("INSERT INTO r_hist (id, n) VALUES ('a', 1)")
        .await
        .expect("insert");
    for n in 2..5 {
        source.nodes[0]
            .exec(&format!("UPDATE r_hist SET n = {n} WHERE id = 'a'"))
            .await
            .expect("update");
    }
    let (mirror, database, _pki) = tuned_following_mirror(&source, DATABASE).await;
    wait_for_async(
        "the mirror holds the source's history",
        Duration::from_secs(90),
        Duration::from_millis(200),
        || async { rows(&mirror.nodes[0], HISTORY).await.len() == 4 },
    )
    .await;

    decide_until_one_version_stays(
        &source,
        &mirror,
        database,
        (BitemporalEngineKind::DocumentStrict, "r_hist"),
        HISTORY,
    )
    .await;
    await_mirror_reads(&mirror, "SELECT id, n FROM r_hist", &["a|4".to_string()]).await;

    mirror.shutdown().await;
    source.shutdown().await;
}

/// A CRDT collection's archived versions leave both clusters. The live row stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_purges_the_crdt_history_its_source_purges() {
    const DATABASE: &str = "retention_crdt";
    const HISTORY: &str = "SELECT _ts_system FROM r_crdt AS OF SYSTEM TIME NULL";
    let source = tuned_source_with(
        DATABASE,
        &["CREATE COLLECTION r_crdt (id TEXT PRIMARY KEY, n INT) \
           WITH (crdt='true', bitemporal=true)"],
    )
    .await;
    source.nodes[0]
        .exec("INSERT INTO r_crdt (id, n) VALUES ('a', 1)")
        .await
        .expect("insert");
    source.nodes[0]
        .exec("UPDATE r_crdt SET n = 2 WHERE id = 'a'")
        .await
        .expect("update");
    let (mirror, database, _pki) = tuned_following_mirror(&source, DATABASE).await;
    wait_for_async(
        "the mirror holds the source's history",
        Duration::from_secs(90),
        Duration::from_millis(200),
        || async { rows(&mirror.nodes[0], HISTORY).await.len() >= 2 },
    )
    .await;

    decide_until_one_version_stays(
        &source,
        &mirror,
        database,
        (BitemporalEngineKind::Crdt, "r_crdt"),
        HISTORY,
    )
    .await;
    await_mirror_reads(&mirror, "SELECT id, n FROM r_crdt", &["a|2".to_string()]).await;

    mirror.shutdown().await;
    source.shutdown().await;
}

/// An edge deleted below the cutoff leaves both clusters' history.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_purges_the_edge_versions_its_source_purges() {
    const DATABASE: &str = "retention_edges";
    let source = tuned_source_with(
        DATABASE,
        &[
            "CREATE COLLECTION r_g (id STRING PRIMARY KEY, value STRING) \
           WITH (engine='document_schemaless', bitemporal=true)",
        ],
    )
    .await;
    let cut = write_around_a_cut(
        &source,
        &["GRAPH INSERT EDGE IN 'r_g' FROM 'a' TO 'b' TYPE 'K'"],
        &[
            "GRAPH DELETE EDGE IN 'r_g' FROM 'a' TO 'b' TYPE 'K'",
            "GRAPH INSERT EDGE IN 'r_g' FROM 'c' TO 'd' TYPE 'K'",
        ],
    )
    .await;
    let at_cut = format!("MATCH (x)-[:K]->(y) IN 'r_g' AS OF SYSTEM TIME {cut} RETURN x, y");
    let before = rows(&source.nodes[0], &at_cut).await;
    assert!(!before.is_empty(), "the cut holds the deleted edge");
    let (mirror, database, _pki) = tuned_following_mirror(&source, DATABASE).await;
    await_mirror_reads(&mirror, &at_cut, &before).await;

    decide_until_the_cut_is_gone(
        &source,
        &mirror,
        database,
        (BitemporalEngineKind::EdgeStore, "r_g"),
        &at_cut,
        &before,
    )
    .await;
    let current = "MATCH (x)-[:K]->(y) IN 'r_g' RETURN x, y";
    let live = rows(&source.nodes[0], current).await;
    assert!(!live.is_empty(), "the live edge stays");
    await_mirror_reads(&mirror, current, &live).await;

    mirror.shutdown().await;
    source.shutdown().await;
}

/// An array's `audit_retain_ms` window leaves a superseded cell on neither cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_purges_the_array_versions_its_source_purges() {
    const DATABASE: &str = "retention_array";
    const CURRENT: &str = "SELECT * FROM ARRAY_SLICE('r_arr', '{x: [0, 0]}', ['v'], 10)";
    let source = tuned_source_with(
        DATABASE,
        &[
            "CREATE ARRAY r_arr DIMS (x INT64 [0..15]) ATTRS (v INT64) TILE_EXTENTS (16) \
           WITH (audit_retain_ms = 1000)",
        ],
    )
    .await;
    let cut = write_around_a_cut(
        &source,
        &[
            "INSERT INTO ARRAY r_arr COORDS (0) VALUES (10)",
            "SELECT ARRAY_FLUSH('r_arr')",
        ],
        &[
            "INSERT INTO ARRAY r_arr COORDS (0) VALUES (99)",
            "SELECT ARRAY_FLUSH('r_arr')",
        ],
    )
    .await;
    let at_cut = format!("{CURRENT} AS OF SYSTEM TIME {cut}");
    let before = rows(&source.nodes[0], &at_cut).await;
    assert!(!before.is_empty(), "the cut holds the first version");
    let live = rows(&source.nodes[0], CURRENT).await;
    assert_ne!(before, live, "the probe tells the cut from now");
    let (mirror, database, _pki) = tuned_following_mirror(&source, DATABASE).await;
    await_mirror_reads(&mirror, &at_cut, &before).await;

    decide_until_the_cut_is_gone(
        &source,
        &mirror,
        database,
        (BitemporalEngineKind::Array, "r_arr"),
        &at_cut,
        &before,
    )
    .await;
    await_mirror_reads(&mirror, CURRENT, &live).await;

    mirror.shutdown().await;
    source.shutdown().await;
}
