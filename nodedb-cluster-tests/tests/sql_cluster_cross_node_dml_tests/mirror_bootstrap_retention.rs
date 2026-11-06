// SPDX-License-Identifier: BUSL-1.1

//! Retention decisions enter the log, so what a source drops leaves its mirror.
//!
//! Each test runs two clusters joined the way an operator joins them. A test puts
//! a decision into effect itself, as the metadata leader's loop does, so it waits
//! for no policy interval.

use std::time::Duration;

use nodedb::control::retention_log::{AuditPurge, enact_audit_retention, enact_retention};
use nodedb::control::wal_replication::types::RetentionOp;
use nodedb::engine::bitemporal::BitemporalEngineKind;
use nodedb_types::config::tuning::NetworkTuning;
use nodedb_types::{DatabaseId, TenantId};

use super::mirror_bootstrap_fixture::{
    LinkPki, SOURCE_NAME, await_following, await_mirror_reads, boot_mirror_loops, joined_rows,
    link_pki, mirror_settings, read_mirror, rows, serve_source, use_database,
};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for_async};

const COPY: &str = "retention_copy";
/// The tenant of the harness superuser.
const TENANT: TenantId = TenantId::new(1);
const HOUR_MS: i64 = 3_600_000;

fn now_ms() -> i64 {
    nodedb_types::ordinal_to_ms(nodedb_types::wall_now_ns_i64())
}

async fn source_with(database: &str, ddl: &[&str]) -> TestCluster {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    seed_source(source, database, ddl).await
}

async fn tuned_source_with(database: &str, ddl: &[&str]) -> TestCluster {
    let source = TestCluster::spawn_three_with_network_tuning(short_hold())
        .await
        .expect("source cluster");
    seed_source(source, database, ddl).await
}

async fn seed_source(source: TestCluster, database: &str, ddl: &[&str]) -> TestCluster {
    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {database}"))
        .await
        .expect("create database");
    use_database(&source, database).await;
    for statement in ddl {
        source
            .exec_ddl_on_any_leader(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    source
}

async fn following_mirror(
    source: &TestCluster,
    database: &str,
) -> (TestCluster, DatabaseId, LinkPki) {
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    link_mirror(source, mirror, database).await
}

async fn tuned_following_mirror(
    source: &TestCluster,
    database: &str,
) -> (TestCluster, DatabaseId, LinkPki) {
    let mirror = TestCluster::spawn_three_with_network_tuning(short_hold())
        .await
        .expect("mirror cluster");
    link_mirror(source, mirror, database).await
}

async fn link_mirror(
    source: &TestCluster,
    mirror: TestCluster,
    database: &str,
) -> (TestCluster, DatabaseId, LinkPki) {
    let pki = link_pki();
    let seeds = serve_source(source, &pki, &[database]);
    boot_mirror_loops(&mirror, &mirror_settings(&pki, &seeds));
    mirror
        .exec_ddl_on_any_leader(&format!(
            "MIRROR DATABASE {COPY} FROM {SOURCE_NAME}.{database}"
        ))
        .await
        .expect("MIRROR DATABASE");
    let id = await_following(&mirror, COPY).await;
    read_mirror(&mirror, COPY).await;
    (mirror, id, pki)
}

/// A dropped partition leaves every source replica and the mirror. Its rows stay
/// in the aggregate, which folded them before the drop, on both clusters.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_drops_what_its_source_drops() {
    const DATABASE: &str = "retention_ts";
    const AGGREGATE: &str = "SELECT host, s, c FROM r_view";
    let source = source_with(
        DATABASE,
        &[
            "CREATE COLLECTION r_ts (ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
             WITH (engine='timeseries')",
            "CREATE CONTINUOUS AGGREGATE r_view ON r_ts BUCKET '1m' \
             AGGREGATE SUM(value) AS s, COUNT(*) AS c GROUP BY host \
             WITH (refresh_policy = 'manual')",
        ],
    )
    .await;
    for (ts, value) in [(1_000, 1.5), (2_000, 2.5)] {
        source.nodes[0]
            .exec(&format!(
                "INSERT INTO r_ts (ts, host, value) VALUES ({ts}, 'a', {value})"
            ))
            .await
            .expect("insert");
    }
    source.nodes[0]
        .exec("REFRESH CONTINUOUS AGGREGATE r_view")
        .await
        .expect("refresh");
    let (mirror, database, _pki) = following_mirror(&source, DATABASE).await;
    let held = vec!["1000|a".to_string(), "2000|a".to_string()];
    await_mirror_reads(&mirror, "SELECT ts, host FROM r_ts", &held).await;
    let aggregate_before = rows(&source.nodes[0], AGGREGATE).await;
    assert!(!aggregate_before.is_empty(), "the refresh folded the rows");

    enact_retention(
        &source.nodes[0].shared,
        TENANT,
        database,
        RetentionOp::TimeseriesPartitions {
            collection: "r_ts".to_string(),
            max_age_ms: HOUR_MS,
            decided_ms: now_ms(),
        },
    )
    .await
    .expect("the decision lands");

    // A row after the drop carries the next cut, which holds the drop.
    let fresh = now_ms();
    source.nodes[1]
        .exec(&format!(
            "INSERT INTO r_ts (ts, host, value) VALUES ({fresh}, 'b', 7.0)"
        ))
        .await
        .expect("insert");
    let kept = vec![format!("{fresh}|b")];
    for node in &source.nodes {
        node.exec("SET default_read_consistency = 'eventual'")
            .await
            .expect("local reads");
    }
    await_mirror_reads(&source, "SELECT ts, host FROM r_ts", &kept).await;
    await_mirror_reads(&mirror, "SELECT ts, host FROM r_ts", &kept).await;
    await_mirror_reads(&source, AGGREGATE, &aggregate_before).await;
    await_mirror_reads(&mirror, AGGREGATE, &aggregate_before).await;

    mirror.shutdown().await;
    source.shutdown().await;
}

/// A cleanup horizon short enough for a test: one second of snapshot age, ten of
/// statement deadline, and the fixed clock skew bound.
fn short_hold() -> NetworkTuning {
    NetworkTuning {
        default_deadline_secs: 10,
        copy_deadline_secs: 1,
        txn_snapshot_max_age_secs: 1,
        ..NetworkTuning::default()
    }
}

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

/// A mirror's own aggregate buckets leave only by the source's decision. One
/// decision reaches every copy, with one rewrite ordinal.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_applies_its_sources_bucket_retention() {
    const DATABASE: &str = "retention_buckets";
    const AGGREGATE: &str = "SELECT host, s, c FROM b_view";
    let source = source_with(
        DATABASE,
        &[
            "CREATE COLLECTION b_ts (ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
             WITH (engine='timeseries')",
            "CREATE CONTINUOUS AGGREGATE b_view ON b_ts BUCKET '1m' \
             AGGREGATE SUM(value) AS s, COUNT(*) AS c GROUP BY host \
             WITH (refresh_policy = 'manual', retention = '1h')",
        ],
    )
    .await;
    source.nodes[0]
        .exec("INSERT INTO b_ts (ts, host, value) VALUES (1000, 'a', 1.5)")
        .await
        .expect("insert");
    source.nodes[0]
        .exec("REFRESH CONTINUOUS AGGREGATE b_view")
        .await
        .expect("refresh");
    let (mirror, database, _pki) = following_mirror(&source, DATABASE).await;
    let before = rows(&source.nodes[0], AGGREGATE).await;
    assert!(!before.is_empty(), "the refresh folded the row");
    await_mirror_reads(&mirror, AGGREGATE, &before).await;

    enact_retention(
        &source.nodes[0].shared,
        TENANT,
        database,
        RetentionOp::ContinuousAggregates {
            decided_ms: now_ms(),
        },
    )
    .await
    .expect("the decision lands");
    source.nodes[0]
        .exec(&format!(
            "INSERT INTO b_ts (ts, host, value) VALUES ({}, 'c', 1.0)",
            now_ms()
        ))
        .await
        .expect("a row carries the next cut");
    await_mirror_reads(&mirror, AGGREGATE, &[]).await;

    mirror.shutdown().await;
    source.shutdown().await;
}
