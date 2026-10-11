// SPDX-License-Identifier: BUSL-1.1

//! A transaction's cluster array read joins its read set.
//!
//! A 3-node cluster holds the array `grid`, whose cells spread over the tile
//! vShards of several nodes, and the keyed collection `log`. A transaction
//! reads an `ARRAY_SLICE` that spans the tiles of several shards, then
//! writes `log`. Each shard leg reports its vShard's write version, even
//! when it matched no cell, and COMMIT validates the read on every covered
//! vShard.
//!
//! - No write to `grid` after the read: the COMMIT succeeds.
//! - A cell written to a covered tile after the read moves that vShard's
//!   version past the read's, so the COMMIT aborts with SQLSTATE 40001 and
//!   the transaction's write is not visible.

use super::calvin_multishard_fixture::{Fixture, data_rows, keyed_ddl, row_count};
use super::calvin_replica_content::{run_retrying, strict_session};
use crate::common::cluster_harness::shared_steps::db_detail;

/// SQLSTATE of a transaction aborted by a conflict.
const SERIALIZATION_FAILURE: &str = "40001";

/// The slice the transaction reads: three tiles, one per `chr`.
const SLICE: &str =
    "SELECT * FROM ARRAY_SLICE('grid', '{chr: [0, 2], pos: [0, 99]}', ['qual'], 100)";

/// A 3-node cluster holding the array `grid`, seeded with three cells per
/// tile on `chr` 0 to 2, and the keyed collection `log`.
async fn seeded(log: &str) -> Fixture {
    let fx = Fixture::spawn(&[keyed_ddl(log)]).await;
    fx.wait_group_mounted(log).await;
    fx.cluster
        .exec_ddl_on_any_leader(
            "CREATE ARRAY grid \
             DIMS (chr INT64 [0..9], pos INT64 [0..99]) \
             ATTRS (qual FLOAT64) \
             TILE_EXTENTS (1, 100) \
             CELL_ORDER HILBERT",
        )
        .await
        .unwrap_or_else(|e| panic!("CREATE ARRAY grid: {e}"));
    run_retrying(
        &fx.cluster.nodes[0].client,
        "seed the array",
        "INSERT INTO ARRAY grid \
         COORDS (0, 10) VALUES (1.0), \
         COORDS (0, 20) VALUES (2.0), \
         COORDS (1, 10) VALUES (10.0), \
         COORDS (1, 20) VALUES (20.0), \
         COORDS (2, 10) VALUES (100.0), \
         COORDS (2, 20) VALUES (200.0)",
    )
    .await;
    fx.converge().await;
    fx
}

/// Open a transaction on the coordinator that reads the slice and buffers a
/// write to `log`, and return its session.
async fn read_slice_then_buffer_write(fx: &Fixture, log: &str) -> tokio_postgres::Client {
    let session = strict_session(fx.coordinator()).await;
    session
        .simple_query("BEGIN")
        .await
        .unwrap_or_else(|e| panic!("BEGIN: {}", db_detail(&e)));
    let rows = session
        .simple_query(SLICE)
        .await
        .unwrap_or_else(|e| panic!("in-transaction slice: {}", db_detail(&e)));
    assert_eq!(data_rows(&rows), 6, "the slice reads every seeded cell");
    session
        .simple_query(&format!("INSERT INTO {log} (id, v) VALUES ('w1', 'txn')"))
        .await
        .unwrap_or_else(|e| panic!("buffer the write: {}", db_detail(&e)));
    session
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_current_cluster_array_read_commits() {
    let log = "car_ok_log";
    let fx = seeded(log).await;
    let session = read_slice_then_buffer_write(&fx, log).await;

    session.simple_query("COMMIT").await.unwrap_or_else(|e| {
        panic!(
            "no write reached the slice's tiles, so the commit succeeds: {}",
            db_detail(&e)
        )
    });
    fx.converge().await;
    assert_eq!(
        row_count(
            &fx.coordinator().client,
            &format!("SELECT id FROM {log} WHERE id = 'w1'")
        )
        .await,
        1,
        "the committed transaction's write is visible"
    );

    fx.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_write_to_a_covered_tile_aborts_the_cluster_array_read() {
    let log = "car_stale_log";
    let fx = seeded(log).await;
    let session = read_slice_then_buffer_write(&fx, log).await;

    let writer = (fx.coordinator + 1) % fx.cluster.nodes.len();
    run_retrying(
        &fx.cluster.nodes[writer].client,
        "a conflicting write to a covered tile",
        "INSERT INTO ARRAY grid COORDS (1, 50) VALUES (5.0)",
    )
    .await;
    fx.converge().await;

    let error = session
        .simple_query("COMMIT")
        .await
        .expect_err("a cell written to a covered tile after the read makes it stale");
    assert_eq!(
        error.as_db_error().map(|db| db.code().code()),
        Some(SERIALIZATION_FAILURE),
        "the stale array read aborts the commit with a serialization failure: {}",
        db_detail(&error)
    );
    fx.converge().await;
    assert_eq!(
        row_count(
            &fx.coordinator().client,
            &format!("SELECT id FROM {log} WHERE id = 'w1'")
        )
        .await,
        0,
        "the aborted transaction's write is not visible"
    );

    fx.cluster.shutdown().await;
}
