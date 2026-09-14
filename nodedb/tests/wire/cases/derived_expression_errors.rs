// SPDX-License-Identifier: BUSL-1.1

//! Expression errors over a constant derived table must raise, not fold to
//! NULL / empty rows. The derived body materializes as rows on the
//! coordinator; expression projections and aggregate/group-key arguments
//! evaluate against those rows per-row, so division raises 22012 and a bare
//! `nextval` stamps one value per output row. `currval`/`setval` have no
//! defined per-row value and still raise 0A000.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn projection_division_over_derived_raises() {
    let server = TestServer::start().await;
    server
        .expect_error("SELECT x/0 FROM (SELECT 1 AS x) s", "22012")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn projection_division_over_derived_with_filter_raises() {
    let server = TestServer::start().await;
    server
        .expect_error("SELECT x/0 FROM (SELECT 1 AS x) s WHERE x > 0", "22012")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_argument_division_over_derived_raises() {
    let server = TestServer::start().await;
    server
        .expect_error("SELECT sum(x/0) FROM (SELECT 1 AS x) s", "22012")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_division_over_derived_raises() {
    let server = TestServer::start().await;
    server
        .expect_error(
            "SELECT x, count(*) FROM (SELECT 1 AS x) s GROUP BY x/0",
            "22012",
        )
        .await;
}

/// A bare `nextval` over a derived table is stamped once per materialized
/// row. `currval` over the same shape stays refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accessor_over_derived_stamps_each_row() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE der_seq").await.unwrap();

    let rows = server
        .query_text("SELECT nextval('der_seq') FROM (SELECT 1 AS x) s")
        .await
        .expect("per-row nextval over a derived table must return the stamped value");
    assert_eq!(rows, vec!["1".to_string()], "one row, one value");

    server
        .expect_error("SELECT currval('der_seq') FROM (SELECT 1 AS x) s", "0A000")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_partition_division_over_derived_raises() {
    let server = TestServer::start().await;
    server
        .expect_error(
            "SELECT sum(x) OVER (PARTITION BY x/0) FROM (SELECT 1 AS x) s",
            "22012",
        )
        .await;
}
