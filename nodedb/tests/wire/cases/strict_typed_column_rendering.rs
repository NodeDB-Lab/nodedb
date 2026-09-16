// SPDX-License-Identifier: BUSL-1.1

//! A `document_strict` `TIMESTAMP` column renders the stored instant the same
//! way whichever route reads it, and the same way a timeseries time key does.
//! A `columnar` `TIMESTAMP` column renders the same instant from the live
//! memtable and from a flushed segment.

use crate::harness::TestServer;

/// The instant every test in this file stores.
const EARLY: &str = "2020-03-05 10:00:00";
/// `EARLY` as a declared `TIMESTAMP` column renders it. The engine stores
/// 1583402400000 epoch milliseconds; a `TIMESTAMP` cell carries epoch
/// microseconds, which the pgwire encoder writes as ISO-8601 UTC.
const EARLY_ISO: &str = "2020-03-05T10:00:00.000000Z";
/// `EARLY` as epoch microseconds — 1583402400000 milliseconds times 1000.
/// A projection that announces no catalog type leaves its cells this number.
const EARLY_MICROS: &str = "1583402400000000";

/// A strict `document_strict` collection carrying a `TIMESTAMP` column, read
/// back with a direct `SELECT`, denotes the stored instant. Epoch
/// milliseconds — 1583402400000 — read as microseconds denote 1970-01-19, so
/// a millisecond value fails both arms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strict_timestamp_column_renders_the_stored_instant() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION strict_ts_direct \
             (id TEXT PRIMARY KEY, created_at TIMESTAMP) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create strict_ts_direct");
    server
        .exec(&format!(
            "INSERT INTO strict_ts_direct (id, created_at) VALUES ('r1', '{EARLY}')"
        ))
        .await
        .expect("insert into strict_ts_direct");

    let rows = server
        .query_text("SELECT created_at FROM strict_ts_direct WHERE id = 'r1'")
        .await
        .expect("SELECT of a strict TIMESTAMP column must succeed");
    assert_eq!(rows.len(), 1, "one stored row: {rows:?}");

    assert!(
        rows[0] == EARLY_ISO || rows[0] == EARLY_MICROS,
        "a strict TIMESTAMP column must denote {EARLY}: expected {EARLY_ISO} \
         or {EARLY_MICROS}, got {rows:?}"
    );
}

/// The same column, read back through `INSERT ... RETURNING` rather than a
/// follow-up `SELECT`, denotes the same stored instant. `RETURNING` runs its
/// own encoder over the just-written row, so it is a second route to the
/// same cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strict_timestamp_column_returned_by_insert_renders_the_stored_instant() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION strict_ts_returning \
             (id TEXT PRIMARY KEY, created_at TIMESTAMP) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create strict_ts_returning");

    let rows = server
        .query_text(&format!(
            "INSERT INTO strict_ts_returning (id, created_at) VALUES ('r2', '{EARLY}') \
             RETURNING created_at"
        ))
        .await
        .expect("INSERT ... RETURNING of a strict TIMESTAMP column must succeed");
    assert_eq!(rows.len(), 1, "one inserted row: {rows:?}");

    assert!(
        rows[0] == EARLY_ISO || rows[0] == EARLY_MICROS,
        "a strict TIMESTAMP column returned by INSERT must denote {EARLY}: expected \
         {EARLY_ISO} or {EARLY_MICROS}, got {rows:?}"
    );
}

/// A `document_strict` `TIMESTAMP` column and a timeseries `TIMESTAMP` time
/// key holding the same instant render identically. One instant renders one
/// way whichever engine stores it, so a divergence between the two engines'
/// encoders is a rendering defect, not an engine-specific choice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strict_timestamp_column_renders_the_same_as_a_timeseries_time_key() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION strict_ts_parity_doc \
             (id TEXT PRIMARY KEY, created_at TIMESTAMP) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create strict_ts_parity_doc");
    server
        .exec(&format!(
            "INSERT INTO strict_ts_parity_doc (id, created_at) VALUES ('r3', '{EARLY}')"
        ))
        .await
        .expect("insert into strict_ts_parity_doc");

    server
        .exec(
            "CREATE COLLECTION strict_ts_parity_ts \
             (captured_at TIMESTAMP TIME_KEY, host TEXT, v FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("create strict_ts_parity_ts");
    server
        .exec(&format!(
            "INSERT INTO strict_ts_parity_ts (captured_at, host, v) VALUES ('{EARLY}', 'h1', 1.5)"
        ))
        .await
        .expect("insert into strict_ts_parity_ts");

    let strict_reading = server
        .query_text("SELECT created_at FROM strict_ts_parity_doc WHERE id = 'r3'")
        .await
        .expect("SELECT of the strict TIMESTAMP column must succeed");
    assert_eq!(
        strict_reading.len(),
        1,
        "one stored row: {strict_reading:?}"
    );

    let timeseries_reading = server
        .query_text("SELECT captured_at FROM strict_ts_parity_ts")
        .await
        .expect("SELECT of the timeseries time key must succeed");
    assert_eq!(
        timeseries_reading.len(),
        1,
        "one stored point: {timeseries_reading:?}"
    );

    assert_eq!(
        strict_reading[0], timeseries_reading[0],
        "a strict TIMESTAMP column must render the same instant as a timeseries time \
         key: strict={strict_reading:?} timeseries={timeseries_reading:?}"
    );
}

/// A `columnar` collection carrying a `TIMESTAMP` column, read back with a
/// direct `SELECT` while the row is still in the live memtable, renders the
/// stored instant as ISO-8601. The columnar engine stores the cell as epoch
/// microseconds and reads it back as a typed instant, so the millisecond and
/// microsecond integer forms are both rendering defects here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_columnar_timestamp_column_renders_the_stored_instant() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION columnar_ts_direct \
             (id TEXT PRIMARY KEY, created_at TIMESTAMP) \
             WITH (engine='columnar')",
        )
        .await
        .expect("create columnar_ts_direct");
    server
        .exec(&format!(
            "INSERT INTO columnar_ts_direct (id, created_at) VALUES ('r1', '{EARLY}')"
        ))
        .await
        .expect("insert into columnar_ts_direct");

    let rows = server
        .query_text("SELECT created_at FROM columnar_ts_direct WHERE id = 'r1'")
        .await
        .expect("SELECT of a columnar TIMESTAMP column must succeed");
    assert_eq!(rows.len(), 1, "one stored row: {rows:?}");
    assert_eq!(
        rows[0], EARLY_ISO,
        "a columnar TIMESTAMP column must render {EARLY} as {EARLY_ISO}, got {rows:?}"
    );
}

/// The columnar engine has two read paths for one column: the live memtable
/// and the flushed segments a full memtable drains into. With a flush
/// threshold of two, the first insert is read from the memtable, and after
/// two more inserts the first rows live only in a flushed segment while the
/// last stays in the memtable. Every row renders the one instant identically
/// on both paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_columnar_timestamp_column_renders_the_same_before_and_after_flush() {
    let server = TestServer::start_with_columnar_flush_threshold(2).await;
    server
        .exec(
            "CREATE COLLECTION columnar_ts_flush \
             (id TEXT PRIMARY KEY, created_at TIMESTAMP) \
             WITH (engine='columnar')",
        )
        .await
        .expect("create columnar_ts_flush");
    server
        .exec(&format!(
            "INSERT INTO columnar_ts_flush (id, created_at) VALUES ('r1', '{EARLY}')"
        ))
        .await
        .expect("insert r1 into columnar_ts_flush");

    let before_flush = server
        .query_text("SELECT created_at FROM columnar_ts_flush WHERE id = 'r1'")
        .await
        .expect("SELECT from the live memtable must succeed");
    assert_eq!(
        before_flush,
        vec![EARLY_ISO.to_string()],
        "the live-memtable read must render {EARLY} as {EARLY_ISO}"
    );

    for id in ["r2", "r3"] {
        server
            .exec(&format!(
                "INSERT INTO columnar_ts_flush (id, created_at) VALUES ('{id}', '{EARLY}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {id} into columnar_ts_flush: {e}"));
    }

    let after_flush = server
        .query_text("SELECT created_at FROM columnar_ts_flush ORDER BY id")
        .await
        .expect("SELECT across a flushed segment and the memtable must succeed");
    assert_eq!(
        after_flush,
        vec![EARLY_ISO.to_string(); 3],
        "a flushed-segment cell and a live-memtable cell must render the one \
         instant identically: before={before_flush:?} after={after_flush:?}"
    );
}
