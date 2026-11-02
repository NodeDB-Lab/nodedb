// SPDX-License-Identifier: BUSL-1.1

//! A restore of an array whose tiles route to many vShards.
//!
//! A server places every array cell on the vShard of its tile key, and a read
//! leg reads the tiles of one such vShard. The restore re-issues each cell on
//! that same vShard, so a read after the restore sees every restored cell.
//! The array spans tiles on several vShards, on a server with several
//! cores, so a restore that placed a tile anywhere else is visible.

use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::tile::cell_tile_prefix;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_types::array_route::array_vshard_for_tile;

use super::backup_support::{drain_backup, push_restore};
use crate::harness::TestServer;

const TENANT: u64 = 1;
const ARRAY: &str = "spread_grid";
const PREFIX_BITS: u8 = 8;
const HIGH: i64 = 1023;
const TILE: u64 = 4;
const CORES: usize = 4;

/// The coordinates the test writes: one cell in every 64th tile.
fn coords() -> Vec<i64> {
    (0..=HIGH).step_by(256).collect()
}

/// The vShard a cell write at `x` routes to by its tile key.
fn tile_route(x: i64) -> u32 {
    let schema = ArraySchemaBuilder::new(ARRAY)
        .dim(DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(HIGH)),
        ))
        .attr(AttrSpec::new("v", AttrType::Int64, true))
        .tile_extents(vec![TILE])
        .build()
        .expect("schema");
    let key = cell_tile_prefix(&schema, &[CoordValue::Int64(x)]).expect("tile key");
    array_vshard_for_tile(key, PREFIX_BITS).expect("route")
}

async fn create(server: &TestServer) {
    server
        .exec(&format!(
            "CREATE ARRAY {ARRAY} \
             DIMS (x INT64 [0..{HIGH}]) \
             ATTRS (v INT64) \
             TILE_EXTENTS ({TILE}) \
             WITH (prefix_bits = {PREFIX_BITS})"
        ))
        .await
        .expect("create the array");
}

/// Every live cell of the array, as its `x` coordinate, sorted.
async fn live_xs(server: &TestServer) -> Vec<i64> {
    let rows = server
        .query_named_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('{ARRAY}', '{{\"x\":[0,{HIGH}]}}', ['v'], 2000)"
        ))
        .await
        .expect("slice the array");
    let mut xs: Vec<i64> = rows
        .iter()
        .map(|row| {
            row.get("x")
                .unwrap_or_else(|| panic!("no x column in {row:?}"))
                .parse()
                .unwrap_or_else(|e| panic!("x is not an integer in {row:?}: {e}"))
        })
        .collect();
    xs.sort_unstable();
    xs
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_places_every_tile_where_a_read_finds_it() {
    let routes: std::collections::BTreeSet<u32> = coords().into_iter().map(tile_route).collect();
    assert!(
        routes.len() > 1,
        "the tiles span several vShards: {routes:?}"
    );

    let source = TestServer::start_multicores(CORES).await;
    create(&source).await;
    for x in coords() {
        source
            .exec(&format!(
                "INSERT INTO ARRAY {ARRAY} COORDS ({x}) VALUES ({x})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert x = {x}: {e}"));
    }
    assert_eq!(
        live_xs(&source).await,
        coords(),
        "the source reads every cell"
    );
    let backup = drain_backup(&source.client, TENANT)
        .await
        .expect("take the backup");

    let target = TestServer::start_multicores(CORES).await;
    push_restore(&target.client, TENANT, backup)
        .await
        .expect("restore");
    assert_eq!(
        live_xs(&target).await,
        coords(),
        "a read after the restore sees every restored cell"
    );
}
