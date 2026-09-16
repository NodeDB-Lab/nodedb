// SPDX-License-Identifier: BUSL-1.1

//! A `document_strict` `TIMESTAMP` or `TIMESTAMPTZ` column renders the
//! stored instant as ISO-8601 whichever route reads it, and the same way a
//! timeseries time key does. A `columnar` `TIMESTAMP` column renders the
//! same instant from the live memtable and from a flushed segment. An
//! integer literal written into a `TIMESTAMP` column is epoch milliseconds
//! on every engine, and a literal that carries no instant is refused.

use crate::harness::TestServer;

/// The instant every test in this file stores.
const EARLY: &str = "2020-03-05 10:00:00";
/// `EARLY` as a declared timestamp column renders it: the engine stores the
/// instant as epoch microseconds and hands the encoder a typed instant,
/// which renders as ISO-8601 UTC.
const EARLY_ISO: &str = "2020-03-05T10:00:00.000000Z";
/// `EARLY` as seconds since the Unix epoch.
const EARLY_UNIX_SECS: u64 = 1_583_402_400;
/// `EARLY` as milliseconds since the Unix epoch, the unit an integer literal
/// written into a timestamp column denotes.
const EARLY_UNIX_MILLIS: i64 = 1_583_402_400_000;

/// A strict `document_strict` collection carrying a `TIMESTAMP` column, read
/// back with a direct `SELECT`, renders the stored instant as ISO-8601 —
/// never as an epoch integer.
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
    assert_eq!(
        rows,
        vec![EARLY_ISO.to_string()],
        "a strict TIMESTAMP column must render {EARLY} as {EARLY_ISO}"
    );
}

/// The `TIMESTAMPTZ` sibling renders the same instant the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strict_timestamptz_column_renders_the_stored_instant() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION strict_tstz_direct \
             (id TEXT PRIMARY KEY, created_at TIMESTAMPTZ) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create strict_tstz_direct");
    server
        .exec(&format!(
            "INSERT INTO strict_tstz_direct (id, created_at) VALUES ('r1', '{EARLY}')"
        ))
        .await
        .expect("insert into strict_tstz_direct");

    let rows = server
        .query_text("SELECT created_at FROM strict_tstz_direct WHERE id = 'r1'")
        .await
        .expect("SELECT of a strict TIMESTAMPTZ column must succeed");
    assert_eq!(
        rows,
        vec![EARLY_ISO.to_string()],
        "a strict TIMESTAMPTZ column must render {EARLY} as {EARLY_ISO}"
    );
}

/// Over the extended protocol the driver requests binary results, and a
/// timestamp column honours that: both `TIMESTAMP` and `TIMESTAMPTZ` decode
/// through the driver's `SystemTime` reader to the stored instant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strict_timestamp_column_decodes_from_binary() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION strict_ts_binary \
             (id TEXT PRIMARY KEY, at TIMESTAMP, at_tz TIMESTAMPTZ) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create strict_ts_binary");
    server
        .exec(&format!(
            "INSERT INTO strict_ts_binary (id, at, at_tz) VALUES ('r1', '{EARLY}', '{EARLY}')"
        ))
        .await
        .expect("insert into strict_ts_binary");

    let rows = server
        .client
        .query(
            "SELECT at, at_tz FROM strict_ts_binary WHERE id = $1",
            &[&"r1"],
        )
        .await
        .expect("extended-protocol SELECT of timestamp columns must succeed");
    assert_eq!(rows.len(), 1, "one stored row");
    let expected = std::time::UNIX_EPOCH + std::time::Duration::from_secs(EARLY_UNIX_SECS);
    assert_eq!(
        rows[0].get::<_, std::time::SystemTime>("at"),
        expected,
        "binary TIMESTAMP must decode to {EARLY}"
    );
    assert_eq!(
        rows[0].get::<_, std::time::SystemTime>("at_tz"),
        expected,
        "binary TIMESTAMPTZ must decode to {EARLY}"
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
    assert_eq!(
        rows,
        vec![EARLY_ISO.to_string()],
        "a strict TIMESTAMP column returned by INSERT must render {EARLY} as {EARLY_ISO}"
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

/// An integer literal written into a `TIMESTAMP` column is epoch
/// milliseconds, resolved once in the planner, so the four engines that
/// store a declared column — strict, key-value, columnar, and schemaless
/// document — all render the one instant it denotes. No engine stores the
/// bare integer, and no read path picks a unit for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_integer_literal_into_a_timestamp_column_is_epoch_milliseconds() {
    let server = TestServer::start().await;
    let collections = [
        ("int_ts_strict", "id", "document_strict"),
        ("int_ts_kv", "key", "kv"),
        ("int_ts_columnar", "id", "columnar"),
        ("int_ts_document", "id", "document_schemaless"),
    ];
    for (name, key, engine) in collections {
        server
            .exec(&format!(
                "CREATE COLLECTION {name} \
                 ({key} TEXT PRIMARY KEY, created_at TIMESTAMP) \
                 WITH (engine='{engine}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {name} on {engine}: {e}"));
        server
            .exec(&format!(
                "INSERT INTO {name} ({key}, created_at) VALUES ('r1', {EARLY_UNIX_MILLIS})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert an integer literal into {name}: {e}"));

        let rows = server
            .query_text(&format!("SELECT created_at FROM {name} WHERE {key} = 'r1'"))
            .await
            .unwrap_or_else(|e| panic!("SELECT of {name}.created_at: {e}"));
        assert_eq!(
            rows,
            vec![EARLY_ISO.to_string()],
            "{engine}: an integer literal into a TIMESTAMP column must render \
             {EARLY_UNIX_MILLIS} epoch milliseconds as {EARLY_ISO}"
        );
    }
}

/// A literal that carries no instant is refused at the statement, naming
/// the column, rather than stored under the `TIMESTAMP` column for a read
/// to fail on later. Text that spells no date and a boolean are both
/// refused, on an engine that persists the planner's value verbatim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_datetime_literal_into_a_timestamp_column_is_refused() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION refused_ts \
             (id TEXT PRIMARY KEY, created_at TIMESTAMP) \
             WITH (engine='document_schemaless')",
        )
        .await
        .expect("create refused_ts");

    let text_error = server
        .exec("INSERT INTO refused_ts (id, created_at) VALUES ('r2', 'not a date')")
        .await
        .expect_err("text that spells no date must be refused");
    assert!(
        text_error.contains("created_at"),
        "the refusal must name the column: {text_error}"
    );

    let bool_error = server
        .exec("INSERT INTO refused_ts (id, created_at) VALUES ('r3', true)")
        .await
        .expect_err("a boolean carries no instant and must be refused");
    assert!(
        bool_error.contains("created_at"),
        "the refusal must name the column: {bool_error}"
    );

    let rows = server
        .query_text("SELECT id FROM refused_ts")
        .await
        .expect("SELECT from refused_ts must succeed");
    assert!(
        rows.is_empty(),
        "a refused INSERT must store nothing: {rows:?}"
    );
}
