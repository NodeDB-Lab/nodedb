// SPDX-License-Identifier: BUSL-1.1

//! A transaction block's array read conflicts with a concurrent write to
//! any tile it read.
//!
//! The server places each cell on the vShard its tile key routes to. The
//! default `TestServer` runs one core, so that core hosts every vShard. The
//! block reads the whole array, one read leg per tile vShard. A concurrent
//! write then lands in a tile that routes to another vShard than the seeded
//! cell's tile. It commits after the block's snapshot, so the block's
//! COMMIT must fail with `40001`: validation of that leg's vShard counts the
//! tile placed on it, and no other vShard on the core.

use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::tile::cell_tile_prefix;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_types::array_route::array_vshard_for_tile;

use crate::harness::TestServer;

const PASSWORD: &str = "txn-array-homing-3";
const ARRAY: &str = "homing_grid";
const PREFIX_BITS: u8 = 8;
const HIGH: i64 = 1023;
const TILE: u64 = 4;

/// The schema `CREATE ARRAY` below declares, for computing tile routes.
fn schema() -> nodedb_array::schema::ArraySchema {
    ArraySchemaBuilder::new(ARRAY)
        .dim(DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(HIGH)),
        ))
        .attr(AttrSpec::new("v", AttrType::Int64, true))
        .tile_extents(vec![TILE])
        .build()
        .expect("schema")
}

/// The vShard a cell write at `x` routes to by its tile key.
fn tile_route(x: i64) -> u32 {
    let key = cell_tile_prefix(&schema(), &[CoordValue::Int64(x)]).expect("tile key");
    array_vshard_for_tile(key, PREFIX_BITS).expect("route")
}

/// The seeded cell's coordinate.
const SEED: i64 = 0;

/// A coordinate whose tile key routes to another vShard than the seeded
/// cell's tile.
fn other_vshard_coordinate() -> i64 {
    let seed = tile_route(SEED);
    (0..=HIGH)
        .step_by(TILE as usize)
        .find(|x| tile_route(*x) != seed)
        .expect("a tile routing to another vShard")
}

fn sqlstate(error: &tokio_postgres::Error) -> Option<String> {
    error.as_db_error().map(|e| e.code().code().to_string())
}

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_concurrent_write_to_another_tile_vshard_aborts_an_array_read() {
    let rival = other_vshard_coordinate();

    let server = TestServer::start().await;
    server
        .exec(&format!(
            "CREATE ARRAY {ARRAY} \
             DIMS (x INT64 [0..{HIGH}]) \
             ATTRS (v INT64) \
             TILE_EXTENTS ({TILE}) \
             CELL_ORDER ROW_MAJOR \
             WITH (prefix_bits = {PREFIX_BITS})"
        ))
        .await
        .expect("CREATE ARRAY");
    server
        .exec("CREATE COLLECTION array_homing_side")
        .await
        .expect("CREATE COLLECTION side");
    server
        .exec(&format!(
            "INSERT INTO ARRAY {ARRAY} COORDS ({SEED}) VALUES (1)"
        ))
        .await
        .expect("seed cell");
    server
        .exec(&format!("CREATE USER array_homing_b PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec("GRANT ROLE readwrite TO array_homing_b")
        .await
        .expect("GRANT ROLE");

    let (b, handle) = server
        .connect_as("array_homing_b", PASSWORD)
        .await
        .expect("second connection");

    let a = &server.client;
    exec(a, "BEGIN").await;
    let rows = a
        .simple_query(&format!(
            "SELECT * FROM ARRAY_SLICE('{ARRAY}', '{{\"x\":[0,{HIGH}]}}', ['v'], 2000)"
        ))
        .await
        .expect("ARRAY_SLICE in the block");
    assert!(
        rows.iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))),
        "the block sees the seeded cell"
    );
    exec(a, "INSERT INTO array_homing_side (id, n) VALUES ('s', 1)").await;

    // A concurrent write to a tile on another vShard commits after the
    // block's snapshot.
    exec(
        &b,
        &format!("INSERT INTO ARRAY {ARRAY} COORDS ({rival}) VALUES (2)"),
    )
    .await;

    let error = a
        .simple_query("COMMIT")
        .await
        .expect_err("the block read the tile the concurrent write changed");
    assert_eq!(sqlstate(&error).as_deref(), Some("40001"), "{error:?}");

    drop(b);
    handle.abort();
}
