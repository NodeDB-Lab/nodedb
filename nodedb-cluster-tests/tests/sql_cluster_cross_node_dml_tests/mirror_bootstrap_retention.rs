// SPDX-License-Identifier: BUSL-1.1

//! Retention decisions enter the log, so what a source drops leaves its mirror.
//!
//! Each test runs two clusters joined the way an operator joins them. A test puts
//! a decision into effect itself, as the metadata leader's loop does, so it waits
//! for no policy interval.

use nodedb::control::retention_log::enact_retention;
use nodedb::control::wal_replication::types::RetentionOp;

use super::mirror_bootstrap_fixture::{await_mirror_reads, rows};
use super::mirror_bootstrap_retention_fixture::{TENANT, following_mirror, now_ms, source_with};

const HOUR_MS: i64 = 3_600_000;

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
