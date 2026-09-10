// SPDX-License-Identifier: BUSL-1.1

//! A stored instant renders the same however a query reaches it.
//!
//! A timeseries `TIMESTAMP` time key can be read three ways: a direct
//! `SELECT` of the column, the same column projected through a JOIN, and the
//! same column used as a `GROUP BY` key. Each route runs through its own
//! scan and its own encoder. All three name one stored instant, so all three
//! must render it identically.
//!
//! Every assertion here compares two reads of the SAME row against each
//! other. A hardcoded rendering would pin whichever route was written down
//! first and call the other one wrong, so the comparison states the
//! invariant instead. One absolute check anchors the pair, so the two routes
//! cannot agree on a value that denotes the wrong instant.

use crate::harness::TestServer;

/// One event time, years in the past, so a value the engine substitutes at
/// ingest (wall-clock "now") is separable from the value the INSERT supplied.
const EARLY: &str = "2020-03-05 10:00:00";
/// `EARLY` as a declared `TIMESTAMP` column renders it. The engine stores
/// 1583402400000 epoch milliseconds; a `TIMESTAMP` cell carries epoch
/// microseconds, which the pgwire encoder writes as ISO-8601 UTC.
const EARLY_ISO: &str = "2020-03-05T10:00:00.000000Z";
/// `EARLY` as epoch microseconds — 1583402400000 milliseconds times 1000.
/// A projection that announces no catalog type leaves its cells this number.
const EARLY_MICROS: &str = "1583402400000000";

/// Create a timeseries collection and a document collection that join on the
/// event's `host`, then insert exactly one row into each so the join yields
/// one row.
async fn setup(server: &TestServer, events: &str, hosts: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {events} \
             (captured_at TIMESTAMP TIME_KEY, host TEXT, v FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {events}: {e}"));
    server
        .exec(&format!(
            "CREATE COLLECTION {hosts} (id TEXT PRIMARY KEY, region TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {hosts}: {e}"));

    server
        .exec(&format!(
            "INSERT INTO {events} (captured_at, host, v) VALUES ('{EARLY}', 'h1', 1.5)"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert into {events}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {hosts} (id, region) VALUES ('h1', 'eu')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert into {hosts}: {e}"));
}

/// A time key projected through a JOIN renders as the direct `SELECT` renders
/// it. Both reads name one stored row, so a divergence is a rendering defect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_time_key_renders_the_same_through_a_join_as_through_a_select() {
    let server = TestServer::start().await;
    setup(&server, "tsj_join_events", "tsj_join_hosts").await;

    let direct = server
        .query_text("SELECT captured_at FROM tsj_join_events")
        .await
        .expect("direct SELECT of the declared time key must succeed");
    assert_eq!(direct.len(), 1, "one stored point: {direct:?}");

    let joined = server
        .query_text(
            "SELECT tsj_join_events.captured_at FROM tsj_join_events \
             INNER JOIN tsj_join_hosts ON tsj_join_events.host = tsj_join_hosts.id",
        )
        .await
        .expect("the same column projected through a JOIN must succeed");
    assert_eq!(
        joined.len(),
        1,
        "one event matches one host, so the join yields one row: {joined:?}"
    );

    assert_eq!(
        joined[0], direct[0],
        "the time key must render the same through a JOIN as through a SELECT: \
         joined={joined:?} direct={direct:?}"
    );
}

/// A time key used as a `GROUP BY` key renders as the direct `SELECT` renders
/// it. The aggregate encoder writes the group key itself, so it is a third
/// route to the same stored instant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_time_key_renders_the_same_through_an_aggregate_as_through_a_select() {
    let server = TestServer::start().await;
    setup(&server, "tsj_agg_events", "tsj_agg_hosts").await;

    let direct = server
        .query_text("SELECT captured_at FROM tsj_agg_events")
        .await
        .expect("direct SELECT of the declared time key must succeed");
    assert_eq!(direct.len(), 1, "one stored point: {direct:?}");

    let grouped = server
        .query_text("SELECT captured_at, COUNT(*) FROM tsj_agg_events GROUP BY captured_at")
        .await
        .expect("GROUP BY on the declared time key must succeed");
    assert_eq!(
        grouped.len(),
        1,
        "one stored point falls in one group: {grouped:?}"
    );

    assert_eq!(
        grouped[0], direct[0],
        "the time key must render the same as a GROUP BY key as through a SELECT: \
         grouped={grouped:?} direct={direct:?}"
    );
}

/// A time key read through a JOIN denotes the instant the INSERT supplied.
/// This anchors the comparisons above, which two wrong routes can otherwise
/// pass by agreeing with each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_time_key_denotes_the_stored_instant() {
    let server = TestServer::start().await;
    setup(&server, "tsj_abs_events", "tsj_abs_hosts").await;

    let joined = server
        .query_text(
            "SELECT tsj_abs_events.captured_at FROM tsj_abs_events \
             INNER JOIN tsj_abs_hosts ON tsj_abs_events.host = tsj_abs_hosts.id",
        )
        .await
        .expect("the time key projected through a JOIN must succeed");
    assert_eq!(joined.len(), 1, "the join yields one row: {joined:?}");

    // Two renderings denote 2020-03-05T10:00:00Z, and the expected value is
    // whichever one the join announces a type for. EARLY_ISO is that instant
    // written as ISO-8601 UTC, which a cell typed TIMESTAMP produces.
    // EARLY_MICROS is the same instant in epoch microseconds — the unit a
    // TIMESTAMP cell carries — which an untyped cell leaves as a number.
    // Epoch MILLISECONDS denote 1970-01-19 read either way, so a millisecond
    // value fails both arms.
    assert!(
        joined[0] == EARLY_ISO || joined[0] == EARLY_MICROS,
        "a joined time key must denote {EARLY}: expected {EARLY_ISO} \
         or {EARLY_MICROS}, got {joined:?}"
    );
}
