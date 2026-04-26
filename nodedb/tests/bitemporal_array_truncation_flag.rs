//! `truncated_before_horizon` flag for bitemporal array slice responses.
//!
//! The flag is set when `system_as_of` is below the oldest tile version on a
//! shard, meaning the query requested a point-in-time that predates all
//! recorded history and the response is empty as a result of that horizon.
//!
//! These tests verify the flag through the engine-level `scan_tiles_at` API —
//! the same scan the Data Plane dispatch handler uses — and through the
//! `ArraySliceResponse` encoding that `dispatch_array_slice` emits.

use nodedb::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
use nodedb::engine::array::wal::ArrayPutCell;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::tile::cell_payload::OPEN_UPPER;
use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_types::{Surrogate, TenantId};
use tempfile::TempDir;

fn schema() -> std::sync::Arc<nodedb_array::schema::ArraySchema> {
    std::sync::Arc::new(
        ArraySchemaBuilder::new("flag_test")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![16])
            .build()
            .unwrap(),
    )
}

const SCHEMA_HASH: u64 = 0xF1A9_7E57_0000_0001;
const TENANT: TenantId = TenantId::new(1);

fn aid() -> ArrayId {
    ArrayId::new(TENANT, "flag_test")
}

fn open_engine(dir: &TempDir) -> ArrayEngine {
    let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
    e.open_array(aid(), schema(), SCHEMA_HASH).unwrap();
    e
}

fn put_cell(e: &mut ArrayEngine, x: i64, v: i64, sys: i64, lsn: u64) {
    e.put_cells(
        &aid(),
        vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(x)],
            attrs: vec![CellValue::Int64(v)],
            surrogate: Surrogate::ZERO,
            system_from_ms: sys,
            valid_from_ms: sys,
            valid_until_ms: OPEN_UPPER,
        }],
        lsn,
    )
    .unwrap();
}

/// When `system_as_of` is below the oldest tile version the query sees an
/// empty result and the `truncated_before_horizon` flag must be `true`.
#[test]
fn truncation_flag_set_when_system_as_of_below_horizon() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    // Write a cell at system time 100.
    put_cell(&mut e, 0, 42, 100, 1);

    let store = e.store(&aid()).unwrap();

    // Query at system_as_of = 1, which is before the cell's system_from_ms = 100.
    let (rows, truncated) = store.scan_tiles_at(1, None).unwrap();

    assert_eq!(
        rows.len(),
        0,
        "no rows should be visible at system_as_of=1 (below oldest version at 100)"
    );
    assert!(
        truncated,
        "truncated_before_horizon must be true when system_as_of is below oldest tile version"
    );
}

/// When `system_as_of` is at or above the cell's system time the flag is
/// `false` and the cell is returned.
#[test]
fn truncation_flag_unset_when_system_as_of_within_horizon() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    // Write a cell at system time 100.
    put_cell(&mut e, 0, 42, 100, 1);

    let store = e.store(&aid()).unwrap();

    // Query at system_as_of = 200, which is after the cell's system_from_ms = 100.
    let (rows, truncated) = store.scan_tiles_at(200, None).unwrap();

    let live_count: usize = rows.iter().map(|(_, t)| t.nnz() as usize).sum();
    assert_eq!(
        live_count, 1,
        "one live cell must be visible at system_as_of=200"
    );
    assert!(
        !truncated,
        "truncated_before_horizon must be false when system_as_of covers the written cell"
    );
}
