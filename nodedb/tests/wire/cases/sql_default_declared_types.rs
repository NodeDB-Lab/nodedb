// SPDX-License-Identifier: BUSL-1.1

//! Declared-type coercion for DEFAULT literals.
//!
//! A DEFAULT literal materialized at insert passes the same declared-type
//! coercion an explicit `VALUES` literal gets: a numeric literal defaulted
//! into a `TIMESTAMP` column is epoch milliseconds, a text literal is parsed,
//! and a literal the column cannot hold is refused naming the column.

use crate::harness::TestServer;

/// A numeric `DEFAULT` on a `TIMESTAMP` column is epoch milliseconds, the
/// same unit an explicit `VALUES` literal takes — on `document_strict` and
/// `columnar`.
async fn a_numeric_default_on_a_timestamp_column_is_epoch_milliseconds_on(engine: &str) {
    let server = TestServer::start().await;
    let name = format!("def_ts_epoch_{engine}");

    server
        .exec(&format!(
            "CREATE COLLECTION {name} (\
                id TEXT PRIMARY KEY, \
                at TIMESTAMP DEFAULT 1583402400000) WITH (engine='{engine}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));

    server
        .exec(&format!("INSERT INTO {name} (id) VALUES ('r1')"))
        .await
        .unwrap_or_else(|e| panic!("insert into {name}: {e}"));

    let rows = server
        .query_text(&format!("SELECT at FROM {name} WHERE id = 'r1'"))
        .await
        .unwrap_or_else(|e| panic!("select from {name}: {e}"));
    assert_eq!(rows.len(), 1, "row should exist");
    assert_eq!(
        rows[0], "2020-03-05T10:00:00.000000Z",
        "DEFAULT 1583402400000 on a TIMESTAMP column must render as epoch milliseconds: {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_numeric_default_on_a_timestamp_column_is_epoch_milliseconds_document_strict() {
    a_numeric_default_on_a_timestamp_column_is_epoch_milliseconds_on("document_strict").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_numeric_default_on_a_timestamp_column_is_epoch_milliseconds_columnar() {
    a_numeric_default_on_a_timestamp_column_is_epoch_milliseconds_on("columnar").await;
}

/// The timeseries engine has no `id TEXT PRIMARY KEY`; its row identity is
/// the `TIME_KEY` column, so the numeric `DEFAULT` sits on it. A row that
/// omits the time key takes the declared default, not the ingest clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_numeric_default_on_a_timestamp_column_is_epoch_milliseconds_timeseries() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION def_ts_epoch_timeseries \
             COLUMNS (at TIMESTAMP TIME_KEY DEFAULT 1583402400000, host TEXT, v FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("create def_ts_epoch_timeseries");

    server
        .exec("INSERT INTO def_ts_epoch_timeseries (host, v) VALUES ('h0', 1.0)")
        .await
        .expect("insert into def_ts_epoch_timeseries");

    let rows = server
        .query_text("SELECT at FROM def_ts_epoch_timeseries")
        .await
        .expect("select from def_ts_epoch_timeseries");
    assert_eq!(
        rows,
        vec!["2020-03-05T10:00:00.000000Z".to_string()],
        "DEFAULT 1583402400000 on TIME_KEY is the row's time key, in epoch milliseconds"
    );
}

/// A text `DEFAULT` on a `TIMESTAMP` column is parsed the same way an
/// explicit text literal is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_text_default_on_a_timestamp_column_is_parsed() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_ts_text (\
                id TEXT PRIMARY KEY, \
                at TIMESTAMP DEFAULT '2020-03-05 10:00:00') WITH (engine='document_strict')",
        )
        .await
        .expect("create def_ts_text");

    server
        .exec("INSERT INTO def_ts_text (id) VALUES ('r1')")
        .await
        .expect("insert into def_ts_text");

    let rows = server
        .query_text("SELECT at FROM def_ts_text WHERE id = 'r1'")
        .await
        .expect("select from def_ts_text");
    assert_eq!(rows.len(), 1, "row should exist");
    assert_eq!(
        rows[0], "2020-03-05T10:00:00.000000Z",
        "DEFAULT '2020-03-05 10:00:00' must parse to the same instant: {:?}",
        rows[0]
    );
}

/// A `DEFAULT` literal the declared column type cannot hold is refused at
/// `CREATE`, naming the column. A default is checked where it is declared,
/// so no insert can ever materialize a value the column cannot hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_default_the_column_cannot_hold_is_refused() {
    let server = TestServer::start().await;

    server
        .expect_error(
            "CREATE COLLECTION def_bad_ts (\
                id TEXT PRIMARY KEY, \
                at TIMESTAMP DEFAULT 'not a date') WITH (engine='document_strict')",
            "'at'",
        )
        .await;

    server
        .expect_error(
            "CREATE COLLECTION def_bad_smallint (\
                id TEXT PRIMARY KEY, \
                s SMALLINT DEFAULT 999999) WITH (engine='document_strict')",
            "'s'",
        )
        .await;
}

/// A `DEFAULT` on a timeseries collection's non-key column lands, the same
/// way it lands on every other engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_default_on_a_timeseries_collection_lands() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_ts_host_default \
             COLUMNS (ts BIGINT TIME_KEY, host TEXT DEFAULT 'h0', v FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("create def_ts_host_default");

    server
        .exec("INSERT INTO def_ts_host_default (ts, v) VALUES (1700000000000, 1.0)")
        .await
        .expect("insert into def_ts_host_default");

    let rows = server
        .query_text("SELECT host FROM def_ts_host_default")
        .await
        .expect("select from def_ts_host_default");
    assert_eq!(rows.len(), 1, "row should exist");
    assert_eq!(
        rows[0], "h0",
        "DEFAULT 'h0' must land on host: {:?}",
        rows[0]
    );
}
