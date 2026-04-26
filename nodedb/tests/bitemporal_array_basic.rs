//! Basic bitemporal semantics for the array engine.
//!
//! Three focused correctness properties:
//!
//! 1. Three versions round-trip through the ceiling resolver at every
//!    interesting cutoff.
//! 2. A tombstone is appended rather than applied in-place, and survives
//!    flush to disk.
//! 3. Valid-time filter falls back to an older system-time version when the
//!    current version's valid-time interval does not contain the query point.

use std::sync::Arc;

use nodedb::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
use nodedb::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};
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

// ── shared fixtures ────────────────────────────────────────────────────────────

fn schema() -> Arc<nodedb_array::schema::ArraySchema> {
    Arc::new(
        ArraySchemaBuilder::new("basic")
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

const SCHEMA_HASH: u64 = 0xBA51_CBA5_1CDE_F000;
const TENANT: TenantId = TenantId::new(1);

fn aid() -> ArrayId {
    ArrayId::new(TENANT, "basic")
}

fn open_engine(dir: &TempDir) -> ArrayEngine {
    let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
    e.open_array(aid(), schema(), SCHEMA_HASH).unwrap();
    e
}

fn put_cell(e: &mut ArrayEngine, x: i64, v: i64, sys: i64, vf: i64, vu: i64, lsn: u64) {
    e.put_cells(
        &aid(),
        vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(x)],
            attrs: vec![CellValue::Int64(v)],
            surrogate: Surrogate::ZERO,
            system_from_ms: sys,
            valid_from_ms: vf,
            valid_until_ms: vu,
        }],
        lsn,
    )
    .unwrap();
}

fn delete_cell(e: &mut ArrayEngine, x: i64, sys: i64, lsn: u64) {
    e.delete_cells(
        &aid(),
        vec![ArrayDeleteCell {
            coord: vec![CoordValue::Int64(x)],
            system_from_ms: sys,
            erasure: false,
        }],
        lsn,
    )
    .unwrap();
}

fn attr_val(tiles: &[(u64, nodedb_array::tile::sparse_tile::SparseTile)], label: &str) -> i64 {
    assert_eq!(
        tiles.len(),
        1,
        "{label}: expected 1 tile, got {}",
        tiles.len()
    );
    let tile = &tiles[0].1;
    assert_eq!(tile.nnz(), 1, "{label}: expected nnz=1");
    match &tile.attr_cols[0][0] {
        CellValue::Int64(n) => *n,
        other => panic!("{label}: unexpected attr {other:?}"),
    }
}

// ── test 1 ────────────────────────────────────────────────────────────────────

/// Write 3 versions of one cell at `system_from_ms` ∈ {100, 200, 300} each
/// with `valid_from_ms = system_from_ms` and `valid_until_ms = OPEN_UPPER`.
/// Sweep cutoffs and verify the ceiling resolver returns the correct version
/// (or `truncated_before_horizon` for cutoffs below the earliest version).
#[test]
fn three_versions_round_trip_through_ceiling() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    put_cell(&mut e, 0, 1, 100, 100, OPEN_UPPER, 1);
    put_cell(&mut e, 0, 2, 200, 200, OPEN_UPPER, 2);
    put_cell(&mut e, 0, 3, 300, 300, OPEN_UPPER, 3);

    let store = e.store(&aid()).unwrap();

    struct Case {
        cutoff: i64,
        expect_empty: bool,
        expect_truncated: bool,
        expect_val: Option<i64>,
    }

    let cases = [
        Case {
            cutoff: 50,
            expect_empty: true,
            expect_truncated: true,
            expect_val: None,
        },
        Case {
            cutoff: 99,
            expect_empty: true,
            expect_truncated: true,
            expect_val: None,
        },
        Case {
            cutoff: 100,
            expect_empty: false,
            expect_truncated: false,
            expect_val: Some(1),
        },
        Case {
            cutoff: 150,
            expect_empty: false,
            expect_truncated: false,
            expect_val: Some(1),
        },
        Case {
            cutoff: 200,
            expect_empty: false,
            expect_truncated: false,
            expect_val: Some(2),
        },
        Case {
            cutoff: 250,
            expect_empty: false,
            expect_truncated: false,
            expect_val: Some(2),
        },
        Case {
            cutoff: 300,
            expect_empty: false,
            expect_truncated: false,
            expect_val: Some(3),
        },
        Case {
            cutoff: 1000,
            expect_empty: false,
            expect_truncated: false,
            expect_val: Some(3),
        },
    ];

    for c in &cases {
        let (tiles, truncated) = store.scan_tiles_at(c.cutoff, None).unwrap();
        assert_eq!(
            truncated, c.expect_truncated,
            "cutoff {}: truncated_before_horizon mismatch",
            c.cutoff
        );
        if c.expect_empty {
            assert_eq!(tiles.len(), 0, "cutoff {}: expected 0 tiles", c.cutoff);
        } else {
            let v = attr_val(&tiles, &format!("cutoff {}", c.cutoff));
            assert_eq!(v, c.expect_val.unwrap(), "cutoff {}: wrong value", c.cutoff);
        }
    }
}

// ── test 2 ────────────────────────────────────────────────────────────────────

/// Write v1 at sys=100, delete (tombstone) at sys=200.
///
/// In-memtable bitemporality:
/// - Slice at system_as_of=150 returns v1.
/// - Slice at system_as_of=300 returns 0 rows (tombstone wins).
///
/// After flush, the tombstone tile (system_from_ms=200) lands in the
/// on-disk segment as an entry in the manifest. The segment-level tile
/// entry carries the tombstone's system_from_ms so ceiling scans can
/// locate the correct version range on disk.
#[test]
fn tombstone_appended_not_in_place_visible_after_flush() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    put_cell(&mut e, 1, 42, 100, 100, OPEN_UPPER, 1);
    delete_cell(&mut e, 1, 200, 2);

    // Verify in-memtable (before flush) that tombstone correctly shadows v1.
    {
        let store = e.store(&aid()).unwrap();

        // At sys=150 the tombstone hasn't happened yet → v1 is live.
        let (tiles_before, trunc_before) = store.scan_tiles_at(150, None).unwrap();
        assert!(!trunc_before, "sys=150: not truncated");
        assert_eq!(tiles_before.len(), 1, "sys=150: expected v1");
        let v = attr_val(&tiles_before, "sys=150");
        assert_eq!(v, 42, "sys=150: wrong value");

        // At sys=300 the tombstone is the latest in-memtable version → 0 live rows.
        let (tiles_after, trunc_after) = store.scan_tiles_at(300, None).unwrap();
        assert!(!trunc_after, "sys=300: not truncated");
        assert_eq!(
            tiles_after.len(),
            0,
            "sys=300: tombstone must win in-memtable"
        );
    }

    // Force flush so both tiles land in a segment file.
    e.flush(&aid(), 3).unwrap();

    // After flush, confirm the manifest contains a segment spanning sys=200.
    let store = e.store(&aid()).unwrap();
    let manifest = store.manifest();
    assert!(
        !manifest.segments.is_empty(),
        "manifest must contain at least one segment after flush"
    );
    let has_sys_200 = manifest
        .segments
        .iter()
        .any(|s| s.min_tile.system_from_ms <= 200 && s.max_tile.system_from_ms >= 200);
    assert!(
        has_sys_200,
        "at least one segment must span system_from_ms=200 (tombstone tile entry)"
    );
}

// ── test 3 ────────────────────────────────────────────────────────────────────

/// Same coord, two writes:
/// - v1: valid `[0, 100)` at sys=10
/// - v2: valid `[200, 300)` at sys=20
///
/// Queries:
/// - system_as_of=1000, valid_at_ms=50   → v1  (valid interval [0,100) contains 50)
/// - system_as_of=1000, valid_at_ms=150  → 0 rows (not in any interval)
/// - system_as_of=1000, valid_at_ms=250  → v2  (valid interval [200,300) contains 250)
#[test]
fn valid_time_filter_falls_back_to_older_system_version() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    // v1: valid [0, 100), system=10
    put_cell(&mut e, 2, 10, 10, 0, 100, 1);
    // v2: valid [200, 300), system=20
    put_cell(&mut e, 2, 20, 20, 200, 300, 2);

    let store = e.store(&aid()).unwrap();
    let sys = 1000_i64;

    // valid_at=50 → v1
    let (tiles, _) = store.scan_tiles_at(sys, Some(50)).unwrap();
    let v = attr_val(&tiles, "valid_at=50");
    assert_eq!(v, 10, "valid_at=50 must return v1");

    // valid_at=150 → neither interval contains 150
    let (tiles, _) = store.scan_tiles_at(sys, Some(150)).unwrap();
    assert_eq!(tiles.len(), 0, "valid_at=150: no interval contains 150");

    // valid_at=250 → v2 (newest system-time version that contains 250)
    let (tiles, _) = store.scan_tiles_at(sys, Some(250)).unwrap();
    let v = attr_val(&tiles, "valid_at=250");
    assert_eq!(v, 20, "valid_at=250 must return v2");
}
