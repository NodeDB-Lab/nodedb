// SPDX-License-Identifier: BUSL-1.1

//! Expression errors over a constant derived table must raise, not fold to
//! NULL / empty rows (issue #295). The derived body materializes as rows on
//! the coordinator; expression projections and aggregate/group-key arguments
//! evaluate against those rows per-row, so division raises 22012 and
//! sequence accessors raise 0A000 instead of silently vanishing.

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accessor_over_derived_is_loud() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE der_seq").await.unwrap();
    server
        .expect_error("SELECT nextval('der_seq') FROM (SELECT 1 AS x) s", "0A000")
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
