// SPDX-License-Identifier: BUSL-1.1

//! `MIN` / `MAX` / `SUM` / `AVG` over integers stay exact end to end.
//!
//! `2^53 + 1` and `2^53` collapse to one `f64`, as do nanosecond timestamps
//! one tick apart. An aggregate that rounds through `f64` returns the wrong
//! extreme or a wrong total. A SUM past `i64::MAX` returns the exact total,
//! not a wrapped or rounded one. Each engine runs the same checks.

use crate::harness::TestServer;

const ABOVE: i64 = 9_007_199_254_740_993;
const AT: i64 = 9_007_199_254_740_992;
const NANOS: [i64; 3] = [
    1_700_000_000_000_000_002,
    1_700_000_000_000_000_001,
    1_700_000_000_000_000_003,
];

/// Insert `values` into `collection.v`, one row each. `ts` is the row
/// index, so a timeseries collection gets distinct time keys.
async fn insert_values(srv: &TestServer, collection: &str, values: &[i64]) {
    for (i, v) in values.iter().enumerate() {
        srv.exec(&format!(
            "INSERT INTO {collection} (id, ts, v) VALUES ('r{i}', {ts}, {v})",
            ts = 1_700_000_000_000_i64 + i as i64
        ))
        .await
        .unwrap();
    }
}

/// `SELECT MIN(v), MAX(v), SUM(v), AVG(v)` parsed: the three integer cells
/// exactly, AVG as an `f64`.
async fn min_max_sum_avg(srv: &TestServer, collection: &str) -> (i128, i128, i128, f64) {
    let rows = srv
        .query_rows(&format!(
            "SELECT MIN(v), MAX(v), SUM(v), AVG(v) FROM {collection}"
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one aggregate row, got {rows:?}");
    let int = |i: usize| {
        rows[0][i].parse::<i128>().unwrap_or_else(|_| {
            panic!(
                "{collection}: cell {i} must be an exact integer, got `{}`",
                rows[0][i]
            )
        })
    };
    let avg = rows[0][3]
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("{collection}: AVG must be numeric, got `{}`", rows[0][3]));
    (int(0), int(1), int(2), avg)
}

/// Run every exactness check against three fresh collections made by
/// `create(name)`.
async fn check_engine(srv: &TestServer, create: impl Fn(&str) -> String) {
    // Integers one apart above 2^53.
    srv.exec(&create("big")).await.unwrap();
    insert_values(srv, "big", &[ABOVE, AT]).await;
    let (min, max, sum, avg) = min_max_sum_avg(srv, "big").await;
    assert_eq!(min, i128::from(AT), "MIN above 2^53");
    assert_eq!(max, i128::from(ABOVE), "MAX above 2^53");
    assert_eq!(sum, i128::from(ABOVE) + i128::from(AT), "SUM above 2^53");
    assert_eq!(avg, AT as f64, "AVG above 2^53");

    // Nanosecond timestamps one tick apart.
    srv.exec(&create("nanos")).await.unwrap();
    insert_values(srv, "nanos", &NANOS).await;
    let (min, max, sum, _) = min_max_sum_avg(srv, "nanos").await;
    assert_eq!(min, i128::from(NANOS[1]), "MIN of nanosecond timestamps");
    assert_eq!(max, i128::from(NANOS[2]), "MAX of nanosecond timestamps");
    assert_eq!(
        sum,
        NANOS.iter().map(|&n| i128::from(n)).sum::<i128>(),
        "SUM of nanosecond timestamps"
    );

    // A total past i64::MAX is exact, never wrapped or rounded.
    srv.exec(&create("over")).await.unwrap();
    insert_values(srv, "over", &[i64::MAX, i64::MAX, 2]).await;
    let (min, max, sum, _) = min_max_sum_avg(srv, "over").await;
    assert_eq!(min, 2, "MIN beside i64::MAX");
    assert_eq!(max, i128::from(i64::MAX), "MAX at i64::MAX");
    assert_eq!(
        sum,
        2 * i128::from(i64::MAX) + 2,
        "SUM past i64::MAX must be the exact total"
    );
}

#[tokio::test]
async fn document_schemaless_integer_aggregates_are_exact() {
    let srv = TestServer::start().await;
    check_engine(&srv, |name| {
        format!("CREATE COLLECTION {name} WITH (engine='document_schemaless')")
    })
    .await;
}

#[tokio::test]
async fn columnar_integer_aggregates_are_exact() {
    let srv = TestServer::start().await;
    check_engine(&srv, |name| {
        format!(
            "CREATE COLLECTION {name} \
             COLUMNS (id TEXT, ts BIGINT, v BIGINT) \
             WITH (engine='columnar')"
        )
    })
    .await;
}

#[tokio::test]
async fn timeseries_integer_aggregates_are_exact() {
    let srv = TestServer::start().await;
    check_engine(&srv, |name| {
        format!(
            "CREATE COLLECTION {name} \
             COLUMNS (ts BIGINT TIME_KEY, id TEXT, v BIGINT) \
             WITH (engine='timeseries')"
        )
    })
    .await;
}

/// A schemaless column mixing integers and floats sums as a float, and
/// MIN / MAX return the original value of the winning row.
#[tokio::test]
async fn document_schemaless_mixed_int_float() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION mixed WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec("INSERT INTO mixed (id, v) VALUES ('a', 2), ('b', 0.5), ('c', 9007199254740993)")
        .await
        .unwrap();
    let rows = srv
        .query_rows("SELECT MIN(v), MAX(v), SUM(v) FROM mixed")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one aggregate row, got {rows:?}");
    assert_eq!(
        rows[0][0].parse::<f64>().unwrap(),
        0.5,
        "MIN keeps the float"
    );
    assert_eq!(
        rows[0][1].parse::<i128>().unwrap(),
        i128::from(ABOVE),
        "MAX keeps the exact integer"
    );
    assert_eq!(
        rows[0][2].parse::<f64>().unwrap(),
        (ABOVE + 2) as f64 + 0.5,
        "SUM with a float input is a float"
    );
}

/// A SUM whose exact total lies past the decimal range is refused as
/// `numeric_value_out_of_range`, the code a Control-Plane overflow carries.
/// Each input fits the decimal range, so the overflow is in the aggregate
/// itself.
#[tokio::test]
async fn document_schemaless_sum_past_decimal_range_is_22003() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION huge WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec(
        "INSERT INTO huge (id, v) VALUES \
         ('a', 50000000000000000000000000000), ('b', 50000000000000000000000000000)",
    )
    .await
    .unwrap();
    srv.expect_error("SELECT SUM(v) FROM huge", "SQLSTATE 22003")
        .await;
}

/// SUM over a `DECIMAL` column with fractions is the exact decimal total.
/// `0.1 + 0.2 + 0.3` through `f64` is `0.6000000000000001`.
#[tokio::test]
async fn decimal_column_sums_exactly() {
    let srv = TestServer::start().await;
    for (name, create) in [
        (
            "dec_strict",
            "CREATE COLLECTION dec_strict (id TEXT PRIMARY KEY, v DECIMAL) \
             WITH (engine='document_strict')",
        ),
        (
            "dec_columnar",
            "CREATE COLLECTION dec_columnar COLUMNS (id TEXT, v DECIMAL) \
             WITH (engine='columnar')",
        ),
    ] {
        srv.exec(create).await.unwrap();
        srv.exec(&format!(
            "INSERT INTO {name} (id, v) VALUES ('a', 0.1), ('b', 0.2), ('c', 0.3)"
        ))
        .await
        .unwrap();
        let rows = srv
            .query_rows(&format!("SELECT SUM(v) FROM {name}"))
            .await
            .unwrap();
        assert_eq!(rows, vec![vec!["0.6".to_string()]], "{name}");
    }
}
