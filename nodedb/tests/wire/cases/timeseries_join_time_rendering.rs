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

/// A computed projection of a time key renders it as a direct read does.
/// `COALESCE` returns the stored cell untouched, so both reads name one instant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_computed_projection_of_a_time_key_matches_a_direct_read() {
    let server = TestServer::start().await;
    setup(&server, "tsj_comp_events", "tsj_comp_hosts").await;

    let direct = server
        .query_text("SELECT COALESCE(captured_at, captured_at) AS captured_at FROM tsj_comp_events")
        .await
        .expect("a computed projection of the time key must succeed");
    assert_eq!(direct.len(), 1, "one stored point: {direct:?}");

    let joined = server
        .query_text(
            "SELECT COALESCE(tsj_comp_events.captured_at, tsj_comp_events.captured_at) \
             AS captured_at FROM tsj_comp_events \
             INNER JOIN tsj_comp_hosts ON tsj_comp_events.host = tsj_comp_hosts.id",
        )
        .await
        .expect("the same computed projection over a JOIN must succeed");
    assert_eq!(joined.len(), 1, "the join yields one row: {joined:?}");

    assert_eq!(
        joined[0], direct[0],
        "a computed projection of the time key must render the same through a JOIN \
         as through a SELECT: joined={joined:?} direct={direct:?}"
    );
}

/// An aliased joined time key denotes the instant the INSERT supplied.
/// The alias renames the cell, so the emitted name must still carry the instant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aliased_joined_time_key_denotes_the_stored_instant() {
    let server = TestServer::start().await;
    setup(&server, "tsj_alias_events", "tsj_alias_hosts").await;

    let joined = server
        .query_text(
            "SELECT tsj_alias_events.captured_at AS ts FROM tsj_alias_events \
             INNER JOIN tsj_alias_hosts ON tsj_alias_events.host = tsj_alias_hosts.id",
        )
        .await
        .expect("an aliased time key projected through a JOIN must succeed");
    assert_eq!(joined.len(), 1, "the join yields one row: {joined:?}");

    // The expected value is EARLY, the inserted instant, in whichever unit the
    // alias announces a type for. EARLY_ISO is 2020-03-05T10:00:00Z as a cell
    // typed TIMESTAMP renders it; EARLY_MICROS is the same instant in the epoch
    // microseconds a TIMESTAMP cell carries, which an untyped cell leaves as a
    // number. The stored 1583402400000 milliseconds denote 1970-01-19 read
    // either way, so a millisecond value fails both arms.
    assert!(
        joined[0] == EARLY_ISO || joined[0] == EARLY_MICROS,
        "an aliased joined time key must denote {EARLY}: expected {EARLY_ISO} \
         or {EARLY_MICROS}, got {joined:?}"
    );
}

/// `EARLY` truncated to its hour, as `time_bucket('1 hour', ...)` denotes it.
/// `EARLY` sits on the hour, so the bucket is the same instant.
const EARLY_BUCKET_ISO: &str = EARLY_ISO;
const EARLY_BUCKET_MICROS: &str = EARLY_MICROS;

/// A transforming computed projection of a time key renders the same through
/// a JOIN as through a direct read. `time_bucket` arithmetic depends on the
/// unit the expression sees, so the two routes agree only when the cell
/// reaches the expression in one unit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_time_bucket_of_a_time_key_matches_between_a_join_and_a_direct_read() {
    let server = TestServer::start().await;
    setup(&server, "tsj_bucket_events", "tsj_bucket_hosts").await;

    let direct = server
        .query_text("SELECT time_bucket('1 hour', captured_at) AS bucket FROM tsj_bucket_events")
        .await
        .expect("time_bucket over the time key must succeed");
    assert_eq!(direct.len(), 1, "one stored point: {direct:?}");

    let joined = server
        .query_text(
            "SELECT time_bucket('1 hour', tsj_bucket_events.captured_at) AS bucket \
             FROM tsj_bucket_events \
             INNER JOIN tsj_bucket_hosts ON tsj_bucket_events.host = tsj_bucket_hosts.id",
        )
        .await
        .expect("time_bucket over the time key through a JOIN must succeed");
    assert_eq!(joined.len(), 1, "the join yields one row: {joined:?}");

    assert_eq!(
        joined[0], direct[0],
        "time_bucket of the time key must render the same through a JOIN as through \
         a SELECT: joined={joined:?} direct={direct:?}"
    );
}

/// `time_bucket` of a time key read directly denotes the bucket of the stored
/// instant. A bucket computed in one unit and rendered in another lands
/// decades away from `EARLY`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_time_bucket_of_a_time_key_denotes_the_stored_instant() {
    let server = TestServer::start().await;
    setup(&server, "tsj_bucket_abs_events", "tsj_bucket_abs_hosts").await;

    let direct = server
        .query_text(
            "SELECT time_bucket('1 hour', captured_at) AS bucket FROM tsj_bucket_abs_events",
        )
        .await
        .expect("time_bucket over the time key must succeed");
    assert_eq!(direct.len(), 1, "one stored point: {direct:?}");

    assert!(
        direct[0] == EARLY_BUCKET_ISO || direct[0] == EARLY_BUCKET_MICROS,
        "time_bucket of the time key must denote {EARLY}: expected {EARLY_BUCKET_ISO} \
         or {EARLY_BUCKET_MICROS}, got {direct:?}"
    );
}

/// `time_bucket` of a time key read through a JOIN denotes the bucket of the
/// stored instant. This anchors the comparison above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_time_bucket_of_a_time_key_denotes_the_stored_instant() {
    let server = TestServer::start().await;
    setup(&server, "tsj_bucket_jabs_events", "tsj_bucket_jabs_hosts").await;

    let joined = server
        .query_text(
            "SELECT time_bucket('1 hour', tsj_bucket_jabs_events.captured_at) AS bucket \
             FROM tsj_bucket_jabs_events \
             INNER JOIN tsj_bucket_jabs_hosts \
             ON tsj_bucket_jabs_events.host = tsj_bucket_jabs_hosts.id",
        )
        .await
        .expect("time_bucket over the time key through a JOIN must succeed");
    assert_eq!(joined.len(), 1, "the join yields one row: {joined:?}");

    assert!(
        joined[0] == EARLY_BUCKET_ISO || joined[0] == EARLY_BUCKET_MICROS,
        "a joined time_bucket of the time key must denote {EARLY}: expected \
         {EARLY_BUCKET_ISO} or {EARLY_BUCKET_MICROS}, got {joined:?}"
    );
}

/// `date_part` reads the calendar year of a time key directly. The docs list
/// `EXTRACT(field FROM ts)` (`date_part` is its function form) as a scalar over any timestamp, so a declared
/// timeseries instant must be readable by it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn date_part_year_of_a_time_key_reads_the_stored_year() {
    let server = TestServer::start().await;
    setup(&server, "tsj_extract_events", "tsj_extract_hosts").await;

    let direct = server
        .query_text("SELECT date_part('year', captured_at) AS y FROM tsj_extract_events")
        .await
        .expect("date_part over the time key must succeed");
    assert_eq!(direct.len(), 1, "one stored point: {direct:?}");
    assert_eq!(
        direct[0], "2020",
        "the stored instant is in 2020: {direct:?}"
    );
}

/// `date_part` reads the calendar year of a time key through a JOIN, the same
/// as directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn date_part_year_of_a_joined_time_key_reads_the_stored_year() {
    let server = TestServer::start().await;
    setup(&server, "tsj_extract_j_events", "tsj_extract_j_hosts").await;

    let joined = server
        .query_text(
            "SELECT date_part('year', tsj_extract_j_events.captured_at) AS y \
             FROM tsj_extract_j_events \
             INNER JOIN tsj_extract_j_hosts \
             ON tsj_extract_j_events.host = tsj_extract_j_hosts.id",
        )
        .await
        .expect("date_part over the time key through a JOIN must succeed");
    assert_eq!(joined.len(), 1, "the join yields one row: {joined:?}");
    assert_eq!(
        joined[0], "2020",
        "the stored instant is in 2020: {joined:?}"
    );
}

/// A JOIN whose `ON` compares a timeseries time key against a `TIMESTAMP`
/// column of a document collection matches the row that holds the same
/// instant. Both cells name `EARLY`, so the predicate holds only when the two
/// sides reach the comparison in one unit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_on_a_time_key_against_a_document_timestamp_matches_the_same_instant() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION tsj_xe_events \
             (captured_at TIMESTAMP TIME_KEY, host TEXT, v FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("create events");
    server
        .exec(
            "CREATE COLLECTION tsj_xe_marks (id TEXT PRIMARY KEY, seen_at TIMESTAMP) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create marks");
    server
        .exec(&format!(
            "INSERT INTO tsj_xe_events (captured_at, host, v) VALUES ('{EARLY}', 'h1', 1.5)"
        ))
        .await
        .expect("insert event");
    server
        .exec(&format!(
            "INSERT INTO tsj_xe_marks (id, seen_at) VALUES ('m1', '{EARLY}')"
        ))
        .await
        .expect("insert mark");

    let joined = server
        .query_text(
            "SELECT tsj_xe_marks.id FROM tsj_xe_events \
             INNER JOIN tsj_xe_marks ON tsj_xe_events.captured_at = tsj_xe_marks.seen_at",
        )
        .await
        .expect("a JOIN comparing two instants must succeed");
    assert_eq!(
        joined,
        vec!["m1".to_string()],
        "the event and the mark hold the same instant, so the join matches: {joined:?}"
    );
}

/// The feature-store JOIN the docs advertise: a feature row joins an event
/// row when the feature's time key is at or before the event's. Two
/// timeseries collections compare time keys in the `ON` clause, so the
/// predicate holds only when both sides reach it in one unit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_comparing_two_time_keys_matches_when_the_feature_precedes_the_event() {
    let server = TestServer::start().await;
    for name in ["tsj_tt_events", "tsj_tt_features"] {
        server
            .exec(&format!(
                "CREATE COLLECTION {name} \
                 (captured_at TIMESTAMP TIME_KEY, host TEXT, v FLOAT) \
                 WITH (engine='timeseries')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    server
        .exec(&format!(
            "INSERT INTO tsj_tt_events (captured_at, host, v) VALUES ('{EARLY}', 'h1', 1.5)"
        ))
        .await
        .expect("insert event");
    server
        .exec(
            "INSERT INTO tsj_tt_features (captured_at, host, v) \
             VALUES ('2020-03-05 09:00:00', 'h1', 2.5)",
        )
        .await
        .expect("insert feature");

    let joined = server
        .query_text(
            "SELECT tsj_tt_features.v FROM tsj_tt_events \
             INNER JOIN tsj_tt_features \
             ON tsj_tt_events.host = tsj_tt_features.host \
             AND tsj_tt_features.captured_at <= tsj_tt_events.captured_at",
        )
        .await
        .expect("a JOIN comparing two time keys must succeed");
    assert_eq!(
        joined,
        vec!["2.5".to_string()],
        "the feature precedes the event, so the join matches it: {joined:?}"
    );
}

/// `time_bucket` of a time key used as a `GROUP BY` key denotes the bucket of
/// the stored instant. `EARLY` sits on the hour, so its bucket is `EARLY`
/// itself; the aggregate encoder writes the group key, which is a fourth
/// route to the same stored instant alongside a direct read, a join, and a
/// plain `GROUP BY` on the column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_grouped_time_bucket_denotes_the_stored_instant() {
    let server = TestServer::start().await;
    setup(&server, "tsj_bucket_grp_events", "tsj_bucket_grp_hosts").await;

    let rows = server
        .query_rows(
            "SELECT time_bucket('1 hour', captured_at) AS bucket, host, COUNT(*) \
             FROM tsj_bucket_grp_events GROUP BY bucket, host",
        )
        .await
        .expect("GROUP BY a time_bucket key must succeed");
    assert_eq!(
        rows.len(),
        1,
        "one stored point falls in one group: {rows:?}"
    );

    assert!(
        rows[0][0] == EARLY_ISO || rows[0][0] == EARLY_MICROS,
        "a grouped time_bucket key must denote {EARLY}: expected {EARLY_ISO} \
         or {EARLY_MICROS}, got {rows:?}"
    );
}

/// A time key grouped through `time_bucket` renders the same as the same
/// column grouped directly. `EARLY` sits on the hour, so the bucket of that
/// instant is that instant, and a divergence between the two group-key
/// encoders is a rendering defect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_grouped_time_bucket_renders_as_the_time_key_grouped_directly_does() {
    let server = TestServer::start().await;
    setup(&server, "tsj_bucket_cmp_events", "tsj_bucket_cmp_hosts").await;

    let bucketed = server
        .query_text(
            "SELECT time_bucket('1 hour', captured_at) AS bucket, host, COUNT(*) \
             FROM tsj_bucket_cmp_events GROUP BY bucket, host",
        )
        .await
        .expect("GROUP BY a time_bucket key must succeed");
    assert_eq!(
        bucketed.len(),
        1,
        "one stored point falls in one group: {bucketed:?}"
    );

    let direct = server
        .query_text("SELECT captured_at, COUNT(*) FROM tsj_bucket_cmp_events GROUP BY captured_at")
        .await
        .expect("GROUP BY the time key directly must succeed");
    assert_eq!(
        direct.len(),
        1,
        "one stored point falls in one group: {direct:?}"
    );

    assert_eq!(
        bucketed[0], direct[0],
        "a time_bucket GROUP BY key must render the same as the time key grouped \
         directly: bucketed={bucketed:?} direct={direct:?}"
    );
}
