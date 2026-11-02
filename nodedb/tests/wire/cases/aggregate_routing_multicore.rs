// SPDX-License-Identifier: BUSL-1.1

//! Aggregates on a multi-core server reach the core that owns each
//! collection: a generic timeseries aggregate, a grouping-set aggregate over
//! documents, and an aggregate over a join of collections homed on
//! different cores. A temporal join side must be a bitemporal collection.

use crate::harness::TestServer;

async fn sorted_rows(server: &TestServer, sql: &str) -> Vec<String> {
    let mut rows: Vec<String> = server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect();
    rows.sort();
    rows
}

async fn seed_timeseries(server: &TestServer, c: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {c} (ts BIGINT TIME_KEY, host TEXT, v BIGINT) \
             WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {c}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {c} (ts, host, v) VALUES \
             (1000, 'a', 1), (2000, 'a', 2), (3000, 'b', 3)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {c}: {e}"));
}

async fn seed_documents(server: &TestServer, c: &str, rows: &str) {
    server
        .exec(&format!("CREATE COLLECTION {c} TYPE DOCUMENT SCHEMALESS"))
        .await
        .unwrap_or_else(|e| panic!("create {c}: {e}"));
    server
        .exec(&format!("INSERT INTO {c} {rows}"))
        .await
        .unwrap_or_else(|e| panic!("seed {c}: {e}"));
}

/// A generic timeseries aggregate reads the owning core's rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generic_timeseries_aggregate_on_multiple_cores() {
    let server = TestServer::start_multicores(4).await;
    seed_timeseries(&server, "mc_ts").await;
    assert_eq!(
        sorted_rows(
            &server,
            "SELECT host, SUM(v) FROM mc_ts GROUP BY ROLLUP (host)"
        )
        .await,
        vec!["a|3", "b|3", "|6"]
    );
    assert_eq!(
        sorted_rows(&server, "SELECT COUNT(*) FILTER (WHERE v > 1) FROM mc_ts").await,
        vec!["2"]
    );
}

/// A grouping-set aggregate over documents spread across cores counts every
/// row once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollup_over_documents_on_multiple_cores() {
    let server = TestServer::start_multicores(4).await;
    seed_documents(
        &server,
        "mc_docs",
        "(id, k, n) VALUES ('1', 'x', 1), ('2', 'x', 2), ('3', 'y', 3), ('4', 'z', 4)",
    )
    .await;
    assert_eq!(
        sorted_rows(&server, "SELECT k, SUM(n) FROM mc_docs GROUP BY ROLLUP (k)").await,
        vec!["x|3", "y|3", "z|4", "|10"]
    );
}

/// An aggregate over a join of a timeseries collection and a document
/// collection reads both sides from their owning cores.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_aggregate_on_multiple_cores() {
    let server = TestServer::start_multicores(4).await;
    seed_timeseries(&server, "mc_join_ts").await;
    seed_documents(
        &server,
        "mc_join_hosts",
        "(id, name, region) VALUES ('h1', 'a', 'east'), ('h2', 'b', 'west')",
    )
    .await;
    assert_eq!(
        sorted_rows(
            &server,
            "SELECT h.region, COUNT(*), SUM(m.v) FROM mc_join_ts m \
             JOIN mc_join_hosts h ON m.host = h.name GROUP BY h.region",
        )
        .await,
        vec!["east|2|3", "west|1|3"]
    );
}

/// A valid-time read of a join side that is not bitemporal is refused: only
/// a bitemporal collection has valid-time columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn temporal_join_side_must_be_bitemporal() {
    let server = TestServer::start().await;
    seed_documents(&server, "tj_plain", "(id, k) VALUES ('1', 'x')").await;
    server
        .exec(
            "CREATE COLLECTION tj_temporal (id STRING PRIMARY KEY, k STRING) \
             WITH (engine='document_strict', bitemporal=true)",
        )
        .await
        .unwrap_or_else(|e| panic!("create tj_temporal: {e}"));
    server
        .exec("INSERT INTO tj_temporal (id, k) VALUES ('1', 'x')")
        .await
        .unwrap_or_else(|e| panic!("seed tj_temporal: {e}"));
    server
        .expect_error(
            "SELECT COUNT(*) FROM tj_plain FOR VALID_TIME CONTAINS 1700000000000 \
             JOIN tj_temporal ON tj_plain.k = tj_temporal.k",
            "requires a bitemporal collection",
        )
        .await;
}
