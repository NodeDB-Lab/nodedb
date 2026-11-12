// SPDX-License-Identifier: BUSL-1.1

//! `SUM` over an `INT` column is `bigint` on every engine path, and its
//! binary-format cell is an 8-byte integer the client decodes as `i64`.
//!
//! The plain aggregate, the columnar aggregate, the timeseries grouped scan,
//! an aggregate split across cores, and a continuous aggregate all total
//! integer input exactly, so the cell matches the announced type.

use std::time::Duration;

use tokio_postgres::types::Type;

use crate::harness::TestServer;

/// The rows of `sql` over the extended protocol, whose results are binary,
/// with `SUM` column `sum_at` announced `int8` and decoded as `i64`.
async fn binary_sums(server: &TestServer, sql: &str, sum_at: usize) -> Vec<i64> {
    let statement = server
        .client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(statement.columns()[sum_at].type_(), &Type::INT8, "{sql}");
    server
        .client
        .query(&statement, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|row| row.get::<_, i64>(sum_at))
        .collect()
}

/// Insert `(id, host, i)` rows `(r0, a, 1)`, `(r1, a, 2)`, `(r2, b, 2147483647)`.
async fn seed_rows(server: &TestServer, name: &str) {
    for (index, (host, i)) in [("a", 1i64), ("a", 2), ("b", 2_147_483_647)]
        .into_iter()
        .enumerate()
    {
        server
            .exec(&format!(
                "INSERT INTO {name} (id, host, i) VALUES ('r{index}', '{host}', {i})"
            ))
            .await
            .unwrap_or_else(|e| panic!("{name}: insert r{index}: {e}"));
    }
}

async fn check_engine(server: &TestServer, name: &str) {
    assert_eq!(
        binary_sums(server, &format!("SELECT SUM(i) FROM {name}"), 0).await,
        [2_147_483_650],
        "{name}"
    );
    assert_eq!(
        binary_sums(
            server,
            &format!("SELECT host, SUM(i) FROM {name} GROUP BY host ORDER BY host"),
            1
        )
        .await,
        [3, 2_147_483_647],
        "{name}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sum_of_int_is_binary_bigint_on_document_and_columnar() {
    let server = TestServer::start().await;
    for (name, create) in [
        (
            "sb_strict",
            "CREATE COLLECTION sb_strict (id TEXT PRIMARY KEY, host TEXT, i INT) \
             WITH (engine='document_strict')",
        ),
        (
            "sb_columnar",
            "CREATE COLLECTION sb_columnar COLUMNS (id TEXT, host TEXT, i INT) \
             WITH (engine='columnar')",
        ),
    ] {
        server.exec(create).await.unwrap();
        seed_rows(&server, name).await;
        check_engine(&server, name).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sum_of_int_is_binary_bigint_across_cores() {
    let server = TestServer::start_multicores(4).await;
    server
        .exec(
            "CREATE COLLECTION sb_cores (id TEXT PRIMARY KEY, host TEXT, i INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    seed_rows(&server, "sb_cores").await;
    check_engine(&server, "sb_cores").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sum_of_int_is_binary_bigint_on_timeseries() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION sb_ts (ts BIGINT TIME_KEY, id TEXT, host TEXT, i INT) \
             WITH (engine='timeseries')",
        )
        .await
        .unwrap();
    for (ts, (index, (host, i))) in [1_000i64, 2_000, 3_000].into_iter().zip(
        [("a", 1i64), ("a", 2), ("b", 2_147_483_647)]
            .into_iter()
            .enumerate(),
    ) {
        server
            .exec(&format!(
                "INSERT INTO sb_ts (ts, id, host, i) VALUES ({ts}, 'r{index}', '{host}', {i})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert r{index}: {e}"));
    }
    check_engine(&server, "sb_ts").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sum_of_int_is_binary_bigint_on_a_continuous_aggregate() {
    // A one-byte memtable budget flushes every ingest, and a flush is what
    // an `OnFlush` aggregate folds.
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    server
        .exec(
            "CREATE COLLECTION sb_cagg_src (ts BIGINT TIME_KEY, host TEXT, i INT) \
             WITH (engine='timeseries')",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE CONTINUOUS AGGREGATE sb_cagg ON sb_cagg_src BUCKET '1m' \
             AGGREGATE SUM(i) AS s GROUP BY host",
        )
        .await
        .unwrap();
    for (ts, host, i) in [
        (1_000, "a", 1),
        (2_000, "a", 2),
        (3_000, "b", 2_147_483_647),
    ] {
        server
            .exec(&format!(
                "INSERT INTO sb_cagg_src (ts, host, i) VALUES ({ts}, '{host}', {i})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert at {ts}: {e}"));
    }

    let sql = "SELECT host, s FROM sb_cagg ORDER BY host";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let sums = loop {
        let sums = binary_sums(&server, sql, 1).await;
        if sums == [3, 2_147_483_647] || tokio::time::Instant::now() >= deadline {
            break sums;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(sums, [3, 2_147_483_647]);
}
