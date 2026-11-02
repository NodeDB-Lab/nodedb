// SPDX-License-Identifier: BUSL-1.1

//! Real process-kill WAL-durability regressions for the columnar-storage
//! family (Columnar, Timeseries) and the Array (ND sparse) engine.
//!
//! Same contract as `crash_recovery.rs`: a write acknowledged by the
//! server must survive `kill -9`, because the write path acks the
//! client only after the WAL append is persisted. Reopening the same
//! data directory on a fresh process replays the WAL and must restore
//! the row.

mod crash_harness;

use crash_harness::CrashHarness;

/// `SELECT COUNT(*) AS n FROM <collection>`, as a number. Recovery tests
/// assert an exact count, not presence — a presence check passes just as
/// happily when replay appended the row a second time.
async fn count_rows(h: &CrashHarness, collection: &str) -> u64 {
    let rows = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {collection}"), 0)
        .await;
    assert_eq!(rows.len(), 1, "expected one COUNT(*) row, got {rows:?}");
    rows[0]
        .parse()
        .unwrap_or_else(|e| panic!("COUNT(*) not a number: {rows:?}: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn columnar_write_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready();

    h.exec(
        "CREATE COLLECTION crash_columnar \
         COLUMNS (id TEXT, region TEXT, revenue FLOAT) \
         WITH (engine='columnar')",
    )
    .await;
    h.exec("INSERT INTO crash_columnar (id, region, revenue) VALUES ('r1', 'us', 100.0)")
        .await;

    // Live sanity BEFORE the crash: the row reads back pre-restart, so any
    // post-restart failure is attributable to recovery, not test setup.
    let live = h
        .query_col(
            "SELECT region FROM crash_columnar WHERE id = 'r1'",
            "region",
        )
        .await;
    assert_eq!(
        live,
        vec!["us".to_string()],
        "columnar row must read back BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col(
            "SELECT region FROM crash_columnar WHERE id = 'r1'",
            "region",
        )
        .await;
    assert_eq!(
        recovered,
        vec!["us".to_string()],
        "committed columnar write did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn timeseries_write_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready();

    h.exec(
        "CREATE COLLECTION crash_timeseries \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await;
    h.exec("INSERT INTO crash_timeseries (id, ts, metric, value) VALUES ('p1', 1000, 'cpu', 42.0)")
        .await;

    // Live sanity BEFORE the crash: the point reads back pre-restart, so any
    // post-restart failure is attributable to recovery, not test setup.
    let live = h
        .query_col(
            "SELECT metric FROM crash_timeseries WHERE id = 'p1'",
            "metric",
        )
        .await;
    assert_eq!(
        live,
        vec!["cpu".to_string()],
        "timeseries point must read back BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col(
            "SELECT metric FROM crash_timeseries WHERE id = 'p1'",
            "metric",
        )
        .await;
    assert_eq!(
        recovered,
        vec!["cpu".to_string()],
        "committed timeseries ingest did not survive kill -9 + WAL replay (got {recovered:?})"
    );

    // Exactly one row — not "at least one". The point above proves the write
    // was not LOST; only the count proves replay did not append it twice, which
    // on an append-only engine is the other way recovery goes wrong.
    let n = count_rows(&h, "crash_timeseries").await;
    assert_eq!(
        n, 1,
        "one point was ingested, so exactly one must exist after replay; {n} means \
         the record was replayed on top of state that already contained it"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn array_cell_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready();

    h.exec(
        "CREATE ARRAY crash_arr \
         DIMS (k INT64 [0..15]) \
         ATTRS (v FLOAT64) \
         TILE_EXTENTS (16) \
         CELL_ORDER ROW_MAJOR",
    )
    .await;
    h.exec("INSERT INTO ARRAY crash_arr COORDS (3) VALUES (42.0)")
        .await;

    // Live sanity before the crash. The scalar reducer path always emits a
    // single `result` column, independent of query form.
    let live = h
        .query_col("SELECT * FROM ARRAY_AGG('crash_arr', 'v', 'sum')", "result")
        .await;
    assert_eq!(live.len(), 1, "expected one scalar agg row: {live:?}");
    let live_val: f64 = live[0]
        .parse()
        .unwrap_or_else(|e| panic!("result not a float: {live:?}: {e}"));
    assert!(
        (live_val - 42.0).abs() < 0.01,
        "array cell must aggregate to 42.0 BEFORE the crash (test-setup sanity): got {live_val}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col("SELECT * FROM ARRAY_AGG('crash_arr', 'v', 'sum')", "result")
        .await;
    assert_eq!(
        recovered.len(),
        1,
        "expected one scalar agg row after reopen: {recovered:?}"
    );
    let recovered_val: f64 = recovered[0]
        .parse()
        .unwrap_or_else(|e| panic!("result not a float after reopen: {recovered:?}: {e}"));
    assert!(
        (recovered_val - 42.0).abs() < 0.01,
        "array cell did not survive kill -9 + WAL replay (got {recovered_val})"
    );
}

// ---------------------------------------------------------------------------
// A partial-record flush must not make replay duplicate the record
// ---------------------------------------------------------------------------

/// Distinct tag values one timeseries memtable generation may hold. Four is
/// enough to exhaust with four rows, instead of the shipped 100k ceiling.
const TS_TAG_CARDINALITY: &str = "4";

fn cardinality_harness() -> CrashHarness {
    CrashHarness::new().with_env("NODEDB_TS_MAX_TAG_CARDINALITY", TS_TAG_CARDINALITY)
}

/// A record that cannot be ingested whole must be flushed around, never
/// through: rows already on disk must not come back on replay. If a flush
/// fires between two rows of record L, the partition holds part of L but is
/// stamped L-1, so replay (which skips at-or-below its stamp) redundantly
/// replays L on top of what the partition already holds. The pre-crash count
/// is 8 either way; only the post-restart count separates the two cases,
/// since only replay consults the partition's stamp.
#[tokio::test(flavor = "multi_thread")]
async fn timeseries_partial_record_flush_does_not_duplicate_on_replay() {
    let mut h = cardinality_harness();
    h.spawn();
    h.wait_ready();

    // `host` is the one `SYMBOL` column, so it alone drives the tag
    // dictionary and the cardinality ceiling means exactly what this test
    // says it means. A `TEXT` column is a field and has no ceiling.
    h.exec(
        "CREATE COLLECTION crash_card \
         COLUMNS (id BIGINT, ts BIGINT TIME_KEY, host SYMBOL, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await;

    // Statement 1 (WAL record L-1): fill the tag dictionary exactly.
    h.exec(
        "INSERT INTO crash_card (id, ts, host, value) VALUES \
         (1, 1000, 'h0', 1.0), (2, 2000, 'h1', 1.0), \
         (3, 3000, 'h2', 1.0), (4, 4000, 'h3', 1.0)",
    )
    .await;

    // Statement 2 (WAL record L): new / known / new / known against the full
    // dictionary. Its own four rows carry only four distinct hosts, so a flush
    // taken BEFORE it (which resets the dictionaries) lets it in whole.
    h.exec(
        "INSERT INTO crash_card (id, ts, host, value) VALUES \
         (5, 5000, 'h4', 1.0), (6, 6000, 'h0', 1.0), \
         (7, 7000, 'h5', 1.0), (8, 8000, 'h1', 1.0)",
    )
    .await;

    // Live sanity: all eight rows present pre-restart, ruling out ingest drop.
    let live = count_rows(&h, "crash_card").await;
    assert_eq!(
        live, 8,
        "eight rows were ingested and must all read back BEFORE the crash \
         (test-setup sanity): got {live}"
    );

    h.kill_9();
    h.reopen();

    let recovered = count_rows(&h, "crash_card").await;
    assert_eq!(
        recovered, 8,
        "eight rows were acknowledged, so exactly eight must exist after replay. \
         More means a partition holding PART of the second record was stamped with \
         the first record's LSN, so replay did not skip the second record and \
         appended it on top of the rows already there (got {recovered})"
    );
}
