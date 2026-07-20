// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Timeseries engine.
//!
//! Covers: ingest, time-range scans, aggregations, retention policy creation,
//! continuous aggregate creation, and WAL durability.

mod common;
use common::pgwire_harness::TestServer;

#[tokio::test]
async fn ingest_and_time_range_scan() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_basic \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();

    for (i, ts) in [(1u32, 1000u64), (2, 2000), (3, 3000)] {
        srv.exec(&format!(
            "INSERT INTO ts_basic (id, ts, metric, value) VALUES ('p{i}', {ts}, 'cpu', {i}.0)"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows("SELECT id FROM ts_basic ORDER BY ts")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "p1");
    assert_eq!(rows[2][0], "p3");
}

#[tokio::test]
async fn sum_over_range() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_sum \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();

    for (i, v) in [(1u32, 10.0_f64), (2, 20.0), (3, 30.0), (4, 40.0)] {
        srv.exec(&format!(
            "INSERT INTO ts_sum (id, ts, value) VALUES ('s{i}', {i}000, {v})"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows("SELECT SUM(value) FROM ts_sum")
        .await
        .unwrap();
    let total: f64 = rows[0][0].parse().unwrap();
    assert!((total - 100.0).abs() < 0.01);
}

#[tokio::test]
async fn avg_aggregation() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_avg \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, temp FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO ts_avg (id, ts, temp) VALUES ('a1', 1000, 20.0)")
        .await
        .unwrap();
    srv.exec("INSERT INTO ts_avg (id, ts, temp) VALUES ('a2', 2000, 30.0)")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT AVG(temp) FROM ts_avg")
        .await
        .unwrap();
    let avg: f64 = rows[0][0].parse().unwrap();
    assert!((avg - 25.0).abs() < 0.01);
}

#[tokio::test]
async fn count_rows() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_cnt \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();

    for i in 0..5u32 {
        srv.exec(&format!(
            "INSERT INTO ts_cnt (id, ts, v) VALUES ('c{i}', {i}000, {i})"
        ))
        .await
        .unwrap();
    }

    let rows = srv.query_rows("SELECT COUNT(*) FROM ts_cnt").await.unwrap();
    assert_eq!(rows[0][0].parse::<u32>().unwrap(), 5);
}

#[tokio::test]
async fn retention_policy_creation() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_ret \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, v FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();

    srv.exec("CREATE RETENTION POLICY ret_pol ON ts_ret (RAW RETAIN '7d')")
        .await
        .unwrap();
}

#[tokio::test]
async fn continuous_aggregate_creation() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_cagg_src \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();

    srv.exec(
        "CREATE CONTINUOUS AGGREGATE ts_cagg_view \
         ON ts_cagg_src BUCKET '5m' \
         AGGREGATE SUM(value) AS total_value",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn wal_restart_durability() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_wal \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, val FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO ts_wal (id, ts, val) VALUES ('w1', 9999, 3.14)")
        .await
        .unwrap();

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;

    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    let rows = srv2
        .query_rows("SELECT val FROM ts_wal WHERE id = 'w1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let v: f64 = rows[0][0].parse().unwrap();
    #[allow(clippy::approx_constant)]
    let expected = 3.14_f64;
    assert!((v - expected).abs() < 0.01);
}

/// Regression for #172: a timeseries collection must survive a GRACEFUL
/// (SIGTERM → final-checkpoint flush) restart with EXACTLY its rows — and stay
/// stable across a SECOND graceful restart. The two restarts are the crux the
/// single-restart durability test above cannot see:
///   * restart 1 exercises the final-checkpoint flush + partition stamp; a
///     tail-loss drops the most-recent point here.
///   * restart 2 replays against the partition stamped on restart 1; a stamp
///     that under-covers the flushed set makes replay re-append the resident
///     rows, so the survivors come back DOUBLED. Both faces are silent on an
///     append-only engine, so only the exact count separates them.
#[tokio::test]
async fn timeseries_survives_repeated_graceful_restart_172() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_172 \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, val FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();

    // Five distinct WAL records (separate INSERTs) so the last is a real tail
    // record and any per-record flush/stamp error is observable.
    const N: usize = 5;
    for i in 0..N {
        srv.exec(&format!(
            "INSERT INTO ts_172 (id, ts, val) VALUES ('p{i}', {ts}, {i}.0)",
            ts = 1000 + i as u64 * 1000
        ))
        .await
        .unwrap();
    }

    // Live sanity: all N present before any restart, so any later discrepancy
    // is recovery, not ingest.
    let live = srv.query_rows("SELECT id FROM ts_172").await.unwrap();
    assert_eq!(
        live.len(),
        N,
        "all {N} points must read back pre-restart: {live:?}"
    );

    // ── Graceful restart 1 ── (runs the final checkpoint flush + stamp)
    // `open_on_path` returns the live data dir separately (the reopened server
    // only references the path), so we keep `dir` for the second restart rather
    // than calling `take_dir()` on the reopened server, which would hand back a
    // different, empty dir.
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, dir) = TestServer::open_on_path(dir).await;

    let after_1 = srv2
        .query_rows("SELECT id FROM ts_172 ORDER BY ts")
        .await
        .unwrap();
    assert_eq!(
        after_1.len(),
        N,
        "after ONE graceful restart, exactly {N} points must remain (fewer = the \
         tail record was lost; more = replay re-applied an already-flushed \
         record): {after_1:?}"
    );
    assert_eq!(
        after_1.last().map(|r| r[0].as_str()),
        Some("p4"),
        "the most-recent point must survive the first restart, not just the count"
    );

    // ── Graceful restart 2 ── (the mode that duplicated in #172)
    srv2.graceful_shutdown().await;
    let (srv3, _dir) = TestServer::open_on_path(dir).await;

    let after_2 = srv3.query_rows("SELECT id FROM ts_172").await.unwrap();
    assert_eq!(
        after_2.len(),
        N,
        "after a SECOND graceful restart the count must still be exactly {N}; \
         more means replay re-appended the partition-resident rows on top of \
         themselves (#172 duplication): {after_2:?}"
    );
}

// ── Continuous-aggregate registration must survive past the in-memory Data ──
//
// `CREATE CONTINUOUS AGGREGATE` currently dispatches `RegisterContinuousAggregate`
// straight to the Data Plane manager and returns success — no catalog row, no
// target collection, no Raft replication. Every assertion below pins one
// observable consequence of "no catalog persistence":
//
//   1. SHOW CONTINUOUS AGGREGATES must list the registration immediately.
//   2. The aggregate must still be listed after a restart.
//   3. The aggregate name must resolve as a queryable relation, not "unknown
//      table" — that is the whole point of materializing the result.
//
// All three are silent-failure spec assertions: if the CA registration ever
// regresses back to "succeeds locally, vanishes elsewhere", a single test in
// this group catches it.

/// SHOW CONTINUOUS AGGREGATES must list a freshly-created aggregate. The
/// `CREATE` path returns success without registering anything observable.
#[tokio::test]
async fn continuous_aggregate_visible_in_show_after_create() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_cagg_show_src \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec(
        "CREATE CONTINUOUS AGGREGATE ts_cagg_show_view \
         ON ts_cagg_show_src BUCKET '5m' \
         AGGREGATE SUM(value) AS total_value",
    )
    .await
    .unwrap();

    let rows = srv.query_rows("SHOW CONTINUOUS AGGREGATES").await.unwrap();

    assert!(
        rows.iter()
            .any(|r| r.first().map(String::as_str) == Some("ts_cagg_show_view")),
        "SHOW CONTINUOUS AGGREGATES must list the just-created aggregate; \
         got {rows:?}. Silent absence means the registration never reached \
         a durable location and only lives in transient Data Plane state."
    );
}

#[tokio::test]
async fn continuous_aggregate_drop_is_authorized_scoped_and_cache_consistent() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_cagg_drop_src \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec(
        "CREATE CONTINUOUS AGGREGATE ts_cagg_drop_view \
         ON ts_cagg_drop_src BUCKET '5m' \
         AGGREGATE SUM(value) AS total_value",
    )
    .await
    .unwrap();
    assert_eq!(
        srv.shared
            .permissions
            .get_owner_in_database(
                "continuous_aggregate",
                0,
                nodedb::types::TenantId::new(1),
                "ts_cagg_drop_view",
            )
            .as_deref(),
        Some("nodedb")
    );

    srv.exec("CREATE USER cagg_intruder PASSWORD 'probe-secret-99'")
        .await
        .unwrap();
    let (client, connection) = srv
        .connect_as("cagg_intruder", "probe-secret-99")
        .await
        .unwrap();
    assert!(
        client
            .simple_query("DROP CONTINUOUS AGGREGATE ts_cagg_drop_view")
            .await
            .is_err()
    );
    drop(client);
    connection.abort();

    srv.exec("DROP CONTINUOUS AGGREGATE IF EXISTS ts_cagg_drop_view")
        .await
        .unwrap();
    assert!(
        srv.shared
            .credentials
            .catalog()
            .get_continuous_aggregate(0, 1, "ts_cagg_drop_view")
            .unwrap()
            .is_none()
    );
    assert!(
        srv.shared
            .permissions
            .get_owner_in_database(
                "continuous_aggregate",
                0,
                nodedb::types::TenantId::new(1),
                "ts_cagg_drop_view",
            )
            .is_none()
    );
}

/// A continuous aggregate created before a restart must still be present
/// after one. The registration is meaningless if it disappears on every
/// process restart — that is the catalog-persistence gap.
#[tokio::test]
async fn continuous_aggregate_survives_restart() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_cagg_persist_src \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec(
        "CREATE CONTINUOUS AGGREGATE ts_cagg_persist_view \
         ON ts_cagg_persist_src BUCKET '5m' \
         AGGREGATE SUM(value) AS total_value",
    )
    .await
    .unwrap();

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;

    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    let rows = srv2.query_rows("SHOW CONTINUOUS AGGREGATES").await.unwrap();

    assert!(
        rows.iter()
            .any(|r| r.first().map(String::as_str) == Some("ts_cagg_persist_view")),
        "SHOW CONTINUOUS AGGREGATES must still list the aggregate after a \
         restart; got {rows:?}. Loss across restart is the catalog-persistence \
         gap: the registration is in-memory Data Plane state only."
    );
}

/// The aggregate name must resolve as a queryable relation — `SELECT
/// … FROM <ca_name>` must not error with "unknown table". The bench's
/// reported symptom is precisely that: queries against the
/// materialized name return `42P01` because no target collection ever
/// existed. Data correctness of the rolled-up rows is a separate
/// spec (it depends on a refresh path that doesn't ship yet); this
/// test only pins the "name resolves" half of the bug.
#[tokio::test]
async fn continuous_aggregate_name_is_queryable_after_create() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_cagg_query_src \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();

    srv.exec(
        "CREATE CONTINUOUS AGGREGATE ts_cagg_query_view \
         ON ts_cagg_query_src BUCKET '1m' \
         AGGREGATE SUM(value) AS total_value",
    )
    .await
    .unwrap();

    let _rows = srv
        .query_rows("SELECT * FROM ts_cagg_query_view")
        .await
        .unwrap_or_else(|e| {
            panic!(
                "SELECT against the CA name must succeed once the aggregate \
                 is registered (rolled-up data may be empty until a refresh \
                 path is wired, but the name must resolve to a real relation); \
                 got error: {e}. \"unknown table\" here is the bench's \
                 reported symptom — no target collection was ever \
                 materialized for the aggregate name."
            )
        });
}
