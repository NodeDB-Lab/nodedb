// SPDX-License-Identifier: BUSL-1.1

//! `TRUNCATE`, `DELETE`, and `UPDATE` contracts specific to the columnar
//! family (columnar, timeseries, spatial):
//!
//! * a spatial collection's R-tree follows its rows — a row a `DELETE`,
//!   `UPDATE`, or `TRUNCATE` removes never answers `ST_DWithin` again,
//! * a timeseries `TRUNCATE` resets the continuous aggregates over it and
//!   leaves them live for the next flush,
//! * inside `BEGIN..COMMIT`, a `TRUNCATE` on each engine hides the rows at
//!   the statement, `ROLLBACK` restores them, and `COMMIT` empties them.
//!
//! The per-engine autocommit contract lives in `truncate_engine_conformance`.

use super::truncate_engine_conformance::{assert_truncate_tag, row_count};
use crate::harness::TestServer;

/// Times Square, NYC — the anchor every spatial predicate below queries.
const WITHIN_5KM: &str =
    "ST_DWithin(loc, '{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}', 5000)";

/// Paris, far outside the 5km region.
const PARIS: &str = "{\"type\":\"Point\",\"coordinates\":[2.3522,48.8566]}";

async fn create_spatial(srv: &TestServer, collection: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {collection} COLUMNS (id TEXT, loc GEOMETRY) WITH (engine='spatial')"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {collection}: {e}"));
}

/// Two rows inside the region (`near_a`, `near_b`) and one far outside.
async fn seed_spatial(srv: &TestServer, collection: &str) {
    for (id, x, y) in [
        ("near_a", -73.9857, 40.7580),
        ("near_b", -73.9880, 40.7600),
        ("far", 2.3522, 48.8566),
    ] {
        srv.exec(&format!(
            "INSERT INTO {collection} (id, loc) VALUES ('{id}', ST_MakePoint({x}, {y}))"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
}

/// Sorted ids `ST_DWithin` answers for `collection`.
async fn near_ids(srv: &TestServer, collection: &str) -> Vec<String> {
    let mut ids: Vec<String> = srv
        .query_rows(&format!("SELECT id FROM {collection} WHERE {WITHIN_5KM}"))
        .await
        .unwrap_or_else(|e| panic!("ST_DWithin on {collection}: {e}"))
        .into_iter()
        .map(|r| r[0].clone())
        .collect();
    ids.sort();
    ids
}

/// Sorted ids a plain scan answers for `collection`.
async fn scan_ids(srv: &TestServer, collection: &str) -> Vec<String> {
    let mut ids: Vec<String> = srv
        .query_rows(&format!("SELECT id FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("scan {collection}: {e}"))
        .into_iter()
        .map(|r| r[0].clone())
        .collect();
    ids.sort();
    ids
}

fn ids(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// After `TRUNCATE`, the R-tree holds no entry: `ST_DWithin` answers nothing,
/// and a re-inserted row is found again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_spatial_then_st_dwithin_returns_nothing() {
    let srv = TestServer::start().await;
    create_spatial(&srv, "trunc_geo").await;
    seed_spatial(&srv, "trunc_geo").await;
    assert_eq!(
        near_ids(&srv, "trunc_geo").await,
        ids(&["near_a", "near_b"])
    );

    assert_truncate_tag(&srv, "trunc_geo").await;
    assert!(
        near_ids(&srv, "trunc_geo").await.is_empty(),
        "a truncated spatial collection must answer no ST_DWithin candidates"
    );

    srv.exec("INSERT INTO trunc_geo (id, loc) VALUES ('near_a', ST_MakePoint(-73.9857, 40.7580))")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    assert_eq!(near_ids(&srv, "trunc_geo").await, ids(&["near_a"]));
}

/// The leak regression: a row `DELETE` removes must leave the R-tree too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_spatial_row_removes_it_from_st_dwithin() {
    let srv = TestServer::start().await;
    create_spatial(&srv, "del_geo").await;
    seed_spatial(&srv, "del_geo").await;
    assert_eq!(near_ids(&srv, "del_geo").await, ids(&["near_a", "near_b"]));

    srv.exec("DELETE FROM del_geo WHERE id = 'near_a'")
        .await
        .unwrap_or_else(|e| panic!("delete: {e}"));

    assert_eq!(scan_ids(&srv, "del_geo").await, ids(&["far", "near_b"]));
    assert_eq!(
        near_ids(&srv, "del_geo").await,
        ids(&["near_b"]),
        "a deleted row must not answer ST_DWithin"
    );
}

/// An `UPDATE` that moves a row's geometry re-indexes it: the old position
/// stops matching and the new one starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_spatial_row_geometry_moves_it_in_st_dwithin() {
    let srv = TestServer::start().await;
    create_spatial(&srv, "upd_geo").await;
    seed_spatial(&srv, "upd_geo").await;

    srv.exec(&format!(
        "UPDATE upd_geo SET loc = '{PARIS}' WHERE id = 'near_a'"
    ))
    .await
    .unwrap_or_else(|e| panic!("move near_a out: {e}"));
    assert_eq!(
        near_ids(&srv, "upd_geo").await,
        ids(&["near_b"]),
        "a row moved out of the region must stop matching"
    );

    srv.exec(
        "UPDATE upd_geo SET loc = '{\"type\":\"Point\",\"coordinates\":[-73.9870,40.7590]}' \
         WHERE id = 'far'",
    )
    .await
    .unwrap_or_else(|e| panic!("move far in: {e}"));
    assert_eq!(
        near_ids(&srv, "upd_geo").await,
        ids(&["far", "near_b"]),
        "a row moved into the region must start matching"
    );
    assert_eq!(
        scan_ids(&srv, "upd_geo").await,
        ids(&["far", "near_a", "near_b"])
    );
}

/// `SHOW CONTINUOUS AGGREGATES` row for `name`: `(rows_aggregated,
/// materialized_buckets, stale)`.
async fn aggregate_state(srv: &TestServer, name: &str) -> (String, String, String) {
    let rows = srv
        .query_rows("SHOW CONTINUOUS AGGREGATES")
        .await
        .unwrap_or_else(|e| panic!("show continuous aggregates: {e}"));
    let row = rows
        .iter()
        .find(|r| r[0] == name)
        .unwrap_or_else(|| panic!("aggregate {name} missing from {rows:?}"));
    (row[5].clone(), row[6].clone(), row[7].clone())
}

/// A timeseries `TRUNCATE` resets its continuous aggregates and leaves them
/// live: no rows aggregated, no buckets, not stale, and the source accepts
/// new rows afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_timeseries_resets_continuous_aggregate() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION trunc_cagg_src \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create source: {e}"));
    srv.exec(
        "CREATE CONTINUOUS AGGREGATE trunc_cagg_view \
         ON trunc_cagg_src BUCKET '5m' \
         AGGREGATE SUM(value) AS total_value",
    )
    .await
    .unwrap_or_else(|e| panic!("create aggregate: {e}"));
    for (id, ts, v) in [("a", 1000, 1.5), ("b", 2000, 2.5), ("c", 3000, 3.5)] {
        srv.exec(&format!(
            "INSERT INTO trunc_cagg_src (id, ts, value) VALUES ('{id}', {ts}, {v})"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    assert_eq!(row_count(&srv, "trunc_cagg_src").await, 3);

    assert_truncate_tag(&srv, "trunc_cagg_src").await;
    assert_eq!(row_count(&srv, "trunc_cagg_src").await, 0);
    assert_eq!(
        aggregate_state(&srv, "trunc_cagg_view").await,
        ("0".to_string(), "0".to_string(), "false".to_string()),
        "a truncated source resets its aggregate and keeps it live"
    );

    srv.exec("INSERT INTO trunc_cagg_src (id, ts, value) VALUES ('z', 4000, 9.5)")
        .await
        .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    assert_eq!(row_count(&srv, "trunc_cagg_src").await, 1);
    assert_eq!(
        aggregate_state(&srv, "trunc_cagg_view").await.2,
        "false",
        "the aggregate stays live after the truncate"
    );
}

/// Shared shape of the in-transaction contract: `BEGIN; TRUNCATE` reads
/// empty at the statement, `ROLLBACK` restores the rows, and a second
/// `BEGIN; TRUNCATE; COMMIT` empties them for good.
async fn assert_transaction_contract(srv: &TestServer, collection: &str, seeded: &[&str]) {
    assert_eq!(scan_ids(srv, collection).await, ids(seeded));

    srv.exec("BEGIN")
        .await
        .unwrap_or_else(|e| panic!("begin: {e}"));
    assert_truncate_tag(srv, collection).await;
    assert!(
        scan_ids(srv, collection).await.is_empty(),
        "a SELECT inside the transaction must already read the truncate"
    );
    srv.exec("ROLLBACK")
        .await
        .unwrap_or_else(|e| panic!("rollback: {e}"));
    assert_eq!(
        scan_ids(srv, collection).await,
        ids(seeded),
        "ROLLBACK must restore every row"
    );

    srv.exec("BEGIN")
        .await
        .unwrap_or_else(|e| panic!("begin: {e}"));
    assert_truncate_tag(srv, collection).await;
    srv.exec("COMMIT")
        .await
        .unwrap_or_else(|e| panic!("commit: {e}"));
    assert!(
        scan_ids(srv, collection).await.is_empty(),
        "COMMIT must empty the collection"
    );
    assert_eq!(row_count(srv, collection).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_columnar_inside_transaction_rollback_restores_and_commit_empties() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION trunc_cols_txn COLUMNS (id TEXT, n INT) WITH (engine='columnar')")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));
    for (id, n) in [("a", 1), ("b", 2), ("c", 3)] {
        srv.exec(&format!(
            "INSERT INTO trunc_cols_txn (id, n) VALUES ('{id}', {n})"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    assert_transaction_contract(&srv, "trunc_cols_txn", &["a", "b", "c"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_timeseries_inside_transaction_rollback_restores_and_commit_empties() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION trunc_ts_txn \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create collection: {e}"));
    for (id, ts, v) in [("a", 1000, 10), ("b", 2000, 20), ("c", 3000, 30)] {
        srv.exec(&format!(
            "INSERT INTO trunc_ts_txn (id, ts, v) VALUES ('{id}', {ts}, {v})"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    assert_transaction_contract(&srv, "trunc_ts_txn", &["a", "b", "c"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_spatial_inside_transaction_rollback_restores_and_commit_empties() {
    let srv = TestServer::start().await;
    create_spatial(&srv, "trunc_geo_txn").await;
    seed_spatial(&srv, "trunc_geo_txn").await;
    assert_transaction_contract(&srv, "trunc_geo_txn", &["far", "near_a", "near_b"]).await;
    assert!(
        near_ids(&srv, "trunc_geo_txn").await.is_empty(),
        "the committed truncate must clear the R-tree too"
    );
}
