// SPDX-License-Identifier: BUSL-1.1

//! The `TIME_KEY` column a timeseries collection declares in its DDL is the
//! authoritative time column.
//!
//! Everything a user can observe about a timeseries collection must be
//! expressed in terms of the column name they declared:
//!
//! * the declared column round-trips the value the INSERT supplied,
//! * `SELECT *` projects the declared columns and nothing else — no
//!   engine-internal time column leaks into the result shape,
//! * time-range predicates, ordering, and `time_bucket()` all read the
//!   value that was inserted into the declared column, not a time the
//!   engine assigned at ingest.
//!
//! These hold for every legal time-key name and for both `TIMESTAMP` and
//! `BIGINT` time keys. A name the engine happens to recognise internally
//! (`ts`, `timestamp`, `time`) is not special: it is the user's column.
//!
//! A declared `TIMESTAMP` time key reads back as the instant that was
//! inserted: the engine stores epoch milliseconds and a `TIMESTAMP` cell is
//! read as epoch microseconds, so the two units meet as the row leaves the
//! scan. A `BIGINT` time key shares that storage column and is not a
//! timestamp, so it reads back as the number that was inserted.

use crate::harness::TestServer;

/// Two event times, hours apart and years in the past, so that any value the
/// engine substitutes at ingest (wall-clock "now") is trivially separable
/// from the values the INSERT actually supplied.
const EARLY: &str = "2020-03-05 10:00:00";
const LATE: &str = "2020-03-05 13:00:00";
/// Upper bound that both event times fall below but no ingest-assigned
/// wall-clock time ever will.
const AFTER_BOTH: &str = "2021-01-01 00:00:00";
/// `EARLY` as a declared `TIMESTAMP` column renders it. The engine stores
/// 1583402400000 epoch milliseconds; a `TIMESTAMP` cell carries epoch
/// microseconds, which the pgwire encoder writes as ISO-8601 UTC.
const EARLY_ISO: &str = "2020-03-05T10:00:00.000000Z";
/// `EARLY` as `SELECT *` renders it: a star projection announces no catalog
/// type, so its cells stay the raw stored number — here the same instant in
/// epoch microseconds.
const EARLY_MICROS: &str = "1583402400000000";

#[tokio::test]
async fn time_key_named_ts_round_trips() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION tk_ts (ts TIMESTAMP TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO tk_ts (ts, value) VALUES ('{EARLY}', 1.5)"
    ))
    .await
    .unwrap();

    let rows = srv
        .query_named_rows("SELECT ts, value FROM tk_ts")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one inserted row must read back: {rows:?}");
    let ts = rows[0].get("ts").map(String::as_str).unwrap_or("");
    assert!(
        !ts.is_empty(),
        "declared TIME_KEY column `ts` must not read back NULL: {rows:?}"
    );
    assert_eq!(
        ts, EARLY_ISO,
        "`ts` must round-trip the inserted event time"
    );
}

#[tokio::test]
async fn bigint_time_key_named_ts_round_trips() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION tk_ts_bigint (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO tk_ts_bigint (id, ts, value) VALUES ('p1', 1000, 1.0)")
        .await
        .unwrap();

    let rows = srv
        .query_named_rows("SELECT id, ts FROM tk_ts_bigint")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one inserted row must read back: {rows:?}");
    assert_eq!(
        rows[0].get("ts").map(String::as_str),
        Some("1000"),
        "declared BIGINT TIME_KEY `ts` must round-trip its inserted value: {rows:?}"
    );
}

#[tokio::test]
async fn select_star_projects_declared_columns_only() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION tk_star (ts TIMESTAMP TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO tk_star (ts, value) VALUES ('{EARLY}', 2.5)"
    ))
    .await
    .unwrap();

    let rows = srv.query_named_rows("SELECT * FROM tk_star").await.unwrap();
    assert_eq!(rows.len(), 1, "one inserted row must read back: {rows:?}");
    let names: Vec<&str> = rows[0].keys().map(String::as_str).collect();
    assert!(
        names.contains(&"ts"),
        "`SELECT *` must project the declared time column `ts`: {names:?}"
    );
    assert!(
        names.contains(&"value"),
        "`SELECT *` must project the declared column `value`: {names:?}"
    );
    assert_eq!(
        rows[0].get("ts").map(String::as_str),
        Some(EARLY_MICROS),
        "`SELECT *` must carry the inserted event time under `ts`: {rows:?}"
    );
}

#[tokio::test]
async fn custom_named_time_key_round_trips() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION tk_custom (captured_at TIMESTAMP TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO tk_custom (captured_at, value) VALUES ('{EARLY}', 1.5)"
    ))
    .await
    .unwrap();

    let rows = srv
        .query_named_rows("SELECT captured_at, value FROM tk_custom")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one inserted row must read back: {rows:?}");
    assert_eq!(
        rows[0].get("captured_at").map(String::as_str),
        Some(EARLY_ISO),
        "`captured_at` must round-trip the inserted event time: {rows:?}"
    );
}

/// A time-range predicate on the declared time key must filter on the values
/// the INSERT supplied. If the engine timestamps rows at ingest instead, both
/// rows sit at wall-clock "now" and the past-bounded predicate returns none.
#[tokio::test]
async fn custom_named_time_key_bounds_range_predicate() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION tk_range (captured_at TIMESTAMP TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    for (at, v) in [(EARLY, 1.0), (LATE, 2.0)] {
        srv.exec(&format!(
            "INSERT INTO tk_range (captured_at, value) VALUES ('{at}', {v})"
        ))
        .await
        .unwrap();
    }

    let all = srv
        .query_rows(&format!(
            "SELECT value FROM tk_range WHERE captured_at < '{AFTER_BOTH}'"
        ))
        .await
        .unwrap();
    assert_eq!(
        all.len(),
        2,
        "both event times precede {AFTER_BOTH}, so both rows must match: {all:?}"
    );

    let late_only = srv
        .query_rows(&format!(
            "SELECT value FROM tk_range WHERE captured_at > '{EARLY}'"
        ))
        .await
        .unwrap();
    assert_eq!(
        late_only.len(),
        1,
        "only the later event time exceeds {EARLY}: {late_only:?}"
    );
}

/// Ordering by the declared time key must order by the inserted event times,
/// not by ingest arrival. The rows are inserted late-first so arrival order
/// and event order disagree.
#[tokio::test]
async fn custom_named_time_key_orders_rows() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION tk_order (captured_at TIMESTAMP TIME_KEY, label TEXT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    for (at, label) in [(LATE, "late"), (EARLY, "early")] {
        srv.exec(&format!(
            "INSERT INTO tk_order (captured_at, label) VALUES ('{at}', '{label}')"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows("SELECT label FROM tk_order ORDER BY captured_at ASC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "both rows must read back: {rows:?}");
    assert_eq!(
        rows[0][0], "early",
        "ORDER BY the declared time key must sort by inserted event time: {rows:?}"
    );
    assert_eq!(rows[1][0], "late", "{rows:?}");
}

/// `time_bucket()` over the declared time key buckets by the inserted event
/// times. Two events three hours apart fall into two distinct hourly buckets;
/// rows stamped at ingest would collapse into one.
#[tokio::test]
async fn time_bucket_uses_declared_time_key() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION tk_bucket (captured_at TIMESTAMP TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    for (at, v) in [(EARLY, 1.0), (LATE, 2.0)] {
        srv.exec(&format!(
            "INSERT INTO tk_bucket (captured_at, value) VALUES ('{at}', {v})"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows(
            "SELECT time_bucket('1 hour', captured_at) AS bucket, COUNT(*) \
             FROM tk_bucket GROUP BY bucket",
        )
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "events three hours apart must land in two hourly buckets: {rows:?}"
    );
}

/// `timestamp` is a legal user column name. When it is not the time key it
/// carries the user's own value and type — the engine must not overwrite it
/// with a time of its own.
#[tokio::test]
async fn non_time_key_column_named_timestamp_keeps_its_value() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION tk_reserved \
         (captured_at TIMESTAMP TIME_KEY, timestamp TEXT, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO tk_reserved (captured_at, timestamp, value) \
         VALUES ('{EARLY}', 'sensor-clock', 1.0)"
    ))
    .await
    .unwrap();

    let rows = srv
        .query_named_rows("SELECT captured_at, timestamp, value FROM tk_reserved")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one inserted row must read back: {rows:?}");
    assert_eq!(
        rows[0].get("timestamp").map(String::as_str),
        Some("sensor-clock"),
        "a non-time-key column named `timestamp` must keep its inserted value: {rows:?}"
    );
    assert_eq!(
        rows[0].get("captured_at").map(String::as_str),
        Some(EARLY_ISO),
        "the declared time key must still round-trip: {rows:?}"
    );
}
