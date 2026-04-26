//! Integration test: bitemporal array compaction respects audit retention.
//!
//! Verifies:
//! 1. All in-horizon versions survive compaction.
//! 2. All but the newest out-of-horizon version are dropped.
//! 3. The ceiling at the horizon still resolves correctly.
//! 4. Segment file size shrinks after compaction (sanity).

use std::sync::Arc;

use nodedb::engine::array::compaction::picker::partition_by_retention;
use nodedb::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_types::{Surrogate, TenantId};
use tempfile::TempDir;

use nodedb::engine::array::wal::ArrayPutCell;

fn schema() -> Arc<nodedb_array::schema::ArraySchema> {
    Arc::new(
        ArraySchemaBuilder::new("bt")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .unwrap(),
    )
}

fn aid() -> ArrayId {
    ArrayId::new(TenantId::new(1), "bt")
}

fn put_cell(e: &mut ArrayEngine, x: i64, v: i64, sys_ms: i64, lsn: u64) {
    e.put_cells(
        &aid(),
        vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(x)],
            attrs: vec![CellValue::Int64(v)],
            surrogate: Surrogate::ZERO,
            system_from_ms: sys_ms,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }],
        lsn,
    )
    .unwrap();
}

/// 30 days in milliseconds.
const THIRTY_DAYS_MS: i64 = 30 * 24 * 60 * 60 * 1000;

#[test]
fn compaction_respects_retention_partitioning() {
    // Simulate: now = 100_000 ms, horizon = 100_000 - 30_days_ms.
    // Versions at system_ms 100, 200, 300 are far in the "past" (outside horizon).
    // Version at now-1000 is inside the horizon.
    //
    // We test the pure `partition_by_retention` function directly since
    // the full compaction loop requires wiring audit_retain_ms into the
    // picker. The wiring contract is: picker calls this function, merger
    // receives only the `keep` set.
    use nodedb_array::segment::{TileEntry, TileKind};
    use nodedb_array::tile::mbr::TileMBR;
    use nodedb_array::types::TileId;

    fn te(sys_ms: i64) -> TileEntry {
        TileEntry::new(
            TileId::new(1, sys_ms),
            TileKind::Sparse,
            0,
            64,
            TileMBR::new(0, 0),
        )
    }

    let now_ms = 100_000_i64;
    // All 5 versions span past + present.
    let versions = vec![
        te(100),
        te(200),
        te(300),
        te(now_ms - 5000),
        te(now_ms - 1000),
    ];

    let p = partition_by_retention(&versions, now_ms, Some(THIRTY_DAYS_MS));

    // Everything is inside the window (THIRTY_DAYS_MS >> now_ms), so all kept.
    assert_eq!(
        p.keep.len(),
        5,
        "all versions inside 30d horizon must be kept"
    );
    assert_eq!(p.drop.len(), 0);

    // Now test with a tight horizon: only keep 5-second window.
    let p2 = partition_by_retention(&versions, now_ms, Some(5001));
    // horizon = 100_000 - 5001 = 94_999.
    // Versions 100, 200, 300 → outside. now-5000 = 95000 → inside. now-1000 → inside.
    // Outside ceiling = 300 (newest of 100/200/300). Drop 100 and 200.
    let keep_sys: Vec<i64> = {
        let mut v: Vec<i64> = p2.keep.iter().map(|e| e.tile_id.system_from_ms).collect();
        v.sort();
        v
    };
    assert_eq!(keep_sys, vec![300, now_ms - 5000, now_ms - 1000]);
    assert_eq!(p2.drop.len(), 2, "versions 100 and 200 should be dropped");
}

#[test]
fn ceiling_at_horizon_resolves_correctly_after_compaction() {
    // Write 4 versions of one cell at system times 100, 200, 300, 400.
    // Compact. Ceiling at system_as_of=250 must still resolve to v2 (sys=200).
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 1;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0xBEEF).unwrap();

    put_cell(&mut e, 0, 10, 100, 1);
    put_cell(&mut e, 0, 20, 200, 2);
    put_cell(&mut e, 0, 30, 300, 3);
    put_cell(&mut e, 0, 40, 400, 4);

    // Should have 4 L0 segments.
    assert_eq!(e.store(&aid()).unwrap().manifest().segments.len(), 4);

    // Record total size before compaction.
    let store = e.store(&aid()).unwrap();
    let pre_size: u64 = store
        .manifest()
        .segments
        .iter()
        .map(|s| {
            let path = store.root().join(&s.id);
            std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
        })
        .sum();
    let _ = store;

    let merged = e.maybe_compact(&aid()).unwrap();
    assert!(merged);
    assert_eq!(e.store(&aid()).unwrap().manifest().segments.len(), 1);

    // Post-compaction: ceiling scan at system_as_of=250 must return v2 (value=20).
    let store = e.store(&aid()).unwrap();
    let (resolved_tiles, truncated) = store.scan_tiles_at(250, None).unwrap();
    assert!(
        !truncated,
        "versions at 100 and 200 exist before cutoff 250"
    );

    // Find the cell at coord x=0.
    let coord = vec![CoordValue::Int64(0)];
    let found = resolved_tiles.iter().find_map(|(_, tile)| {
        let n = tile.nnz() as usize;
        for row in 0..n {
            let c: Vec<CoordValue> = tile
                .dim_dicts
                .iter()
                .map(|d| d.values[d.indices[row] as usize].clone())
                .collect();
            if c == coord {
                return tile.attr_cols.first().and_then(|col| col.get(row)).cloned();
            }
        }
        None
    });
    assert_eq!(
        found,
        Some(CellValue::Int64(20)),
        "ceiling at sys=250 must return v2 (value=20)"
    );

    // Size sanity: merged file should be smaller or equal to sum of inputs.
    let post_seg = e.store(&aid()).unwrap().manifest().segments[0].clone();
    let post_path = e.store(&aid()).unwrap().root().join(&post_seg.id);
    let post_size = std::fs::metadata(&post_path).unwrap().len();
    assert!(
        post_size <= pre_size,
        "merged segment ({post_size}b) should be <= sum of inputs ({pre_size}b)"
    );
}

#[test]
fn truncated_before_horizon_flag_set_when_cutoff_predates_all_data() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 1;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0xCAFE).unwrap();
    put_cell(&mut e, 0, 99, 500, 1);
    e.flush(&aid(), 2).unwrap();

    let store = e.store(&aid()).unwrap();
    // system_as_of=50 is before the only version at 500.
    let (rows, truncated) = store.scan_tiles_at(50, None).unwrap();
    assert!(
        truncated,
        "cutoff below all data must set truncated_before_horizon"
    );
    assert!(
        rows.is_empty(),
        "no cells should be returned when cutoff predates all data"
    );
}

#[test]
fn slice_at_system_cutoff_returns_old_version() {
    // v1 at sys=100, v2 at sys=200. Slice at system_as_of=150 must return v1.
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 999; // keep in memtable
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0x1).unwrap();
    put_cell(&mut e, 0, 10, 100, 1); // v1: value=10 at sys=100
    put_cell(&mut e, 0, 20, 200, 2); // v2: value=20 at sys=200

    let store = e.store(&aid()).unwrap();
    let (tiles, truncated) = store.scan_tiles_at(150, None).unwrap();
    assert!(!truncated);
    let coord = vec![CoordValue::Int64(0)];
    let val = tiles.iter().find_map(|(_, tile)| {
        let n = tile.nnz() as usize;
        for row in 0..n {
            let c: Vec<CoordValue> = tile
                .dim_dicts
                .iter()
                .map(|d| d.values[d.indices[row] as usize].clone())
                .collect();
            if c == coord {
                return tile.attr_cols.first().and_then(|col| col.get(row)).cloned();
            }
        }
        None
    });
    assert_eq!(
        val,
        Some(CellValue::Int64(10)),
        "system_as_of=150 must return v1 (value=10)"
    );
}

#[test]
fn slice_below_horizon_sets_truncation_flag() {
    // v1 at sys=100. Slice at system_as_of=50 → 0 rows, truncated=true.
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 999;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0x2).unwrap();
    put_cell(&mut e, 0, 10, 100, 1);

    let store = e.store(&aid()).unwrap();
    let (rows, truncated) = store.scan_tiles_at(50, None).unwrap();
    assert!(
        truncated,
        "system_as_of=50 is before all data, must set truncated"
    );
    assert!(rows.is_empty());
}

#[test]
fn aggregate_at_system_cutoff_uses_old_version() {
    // v1 at sys=100 (value=10), v2 at sys=200 (value=20).
    // scan_tiles_at(150) must return v1 → sum=10.
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 999;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0x3).unwrap();
    put_cell(&mut e, 0, 10, 100, 1);
    put_cell(&mut e, 0, 20, 200, 2);

    let store = e.store(&aid()).unwrap();
    let (tiles, _) = store.scan_tiles_at(150, None).unwrap();
    // Sum all v values across all tiles.
    let sum: i64 = tiles
        .iter()
        .flat_map(|(_, tile)| {
            tile.attr_cols
                .first()
                .map(|col| {
                    col.iter()
                        .filter_map(|v| {
                            if let CellValue::Int64(n) = v {
                                Some(*n)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        })
        .sum();
    assert_eq!(sum, 10, "system_as_of=150 includes only v1 (value=10)");
}

#[test]
fn slice_with_valid_time_filter_falls_back() {
    // v1: valid [0, 100), v2: valid [200, 300).
    // valid_at=50 → v1; valid_at=150 → NotFound (0 rows).
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 999;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0x4).unwrap();

    // Write v1 with valid [0,100).
    e.put_cells(
        &aid(),
        vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(0)],
            attrs: vec![CellValue::Int64(1)],
            surrogate: Surrogate::ZERO,
            system_from_ms: 100,
            valid_from_ms: 0,
            valid_until_ms: 100,
        }],
        1,
    )
    .unwrap();
    // Write v2 with valid [200,300).
    e.put_cells(
        &aid(),
        vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(0)],
            attrs: vec![CellValue::Int64(2)],
            surrogate: Surrogate::ZERO,
            system_from_ms: 200,
            valid_from_ms: 200,
            valid_until_ms: 300,
        }],
        2,
    )
    .unwrap();

    let store = e.store(&aid()).unwrap();
    let coord = vec![CoordValue::Int64(0)];

    // valid_at=50 → v1 (valid [0,100) covers 50).
    let (tiles_50, _) = store.scan_tiles_at(i64::MAX, Some(50)).unwrap();
    let val_50 = tiles_50.iter().find_map(|(_, tile)| {
        let n = tile.nnz() as usize;
        for row in 0..n {
            let c: Vec<CoordValue> = tile
                .dim_dicts
                .iter()
                .map(|d| d.values[d.indices[row] as usize].clone())
                .collect();
            if c == coord {
                return tile.attr_cols.first().and_then(|col| col.get(row)).cloned();
            }
        }
        None
    });
    assert_eq!(
        val_50,
        Some(CellValue::Int64(1)),
        "valid_at=50 must return v1"
    );

    // valid_at=150 → neither version covers 150 → 0 rows.
    let (tiles_150, _) = store.scan_tiles_at(i64::MAX, Some(150)).unwrap();
    let val_150 = tiles_150.iter().find_map(|(_, tile)| {
        let n = tile.nnz() as usize;
        for row in 0..n {
            let c: Vec<CoordValue> = tile
                .dim_dicts
                .iter()
                .map(|d| d.values[d.indices[row] as usize].clone())
                .collect();
            if c == coord {
                return tile.attr_cols.first().and_then(|col| col.get(row)).cloned();
            }
        }
        None
    });
    assert!(
        val_150.is_none(),
        "valid_at=150 must return 0 rows (gap in valid time)"
    );
}

#[test]
fn valid_time_filter_works_after_flush() {
    // Two versions of the same coord, flushed to segment so they are
    // segment-resident. Verifies that valid-time bounds survive the
    // memtable → segment round-trip and that valid-time filtering on
    // segment data is correct.
    //
    // v1: sys=100, valid=[0, 100)   — attrs=[1]
    // v2: sys=200, valid=[200, 300) — attrs=[2]
    //
    // valid_at=50  → v1  (interval [0,100) contains 50)
    // valid_at=150 → nothing (gap between the two intervals)
    // valid_at=250 → v2  (interval [200,300) contains 250)
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 1; // flush after every cell
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0x5).unwrap();

    e.put_cells(
        &aid(),
        vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(0)],
            attrs: vec![CellValue::Int64(1)],
            surrogate: Surrogate::ZERO,
            system_from_ms: 100,
            valid_from_ms: 0,
            valid_until_ms: 100,
        }],
        1,
    )
    .unwrap();

    e.put_cells(
        &aid(),
        vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(0)],
            attrs: vec![CellValue::Int64(2)],
            surrogate: Surrogate::ZERO,
            system_from_ms: 200,
            valid_from_ms: 200,
            valid_until_ms: 300,
        }],
        2,
    )
    .unwrap();

    // With flush_cell_threshold=1 both cells are already segment-resident.
    assert!(
        e.store(&aid()).unwrap().manifest().segments.len() >= 2,
        "both cells should be flushed to segments"
    );

    let store = e.store(&aid()).unwrap();
    let coord = vec![CoordValue::Int64(0)];

    let find_val = |tiles: &[(u64, nodedb_array::tile::sparse_tile::SparseTile)]| {
        tiles.iter().find_map(|(_, tile)| {
            let n = tile.nnz() as usize;
            for row in 0..n {
                let c: Vec<CoordValue> = tile
                    .dim_dicts
                    .iter()
                    .map(|d| d.values[d.indices[row] as usize].clone())
                    .collect();
                if c == coord {
                    return tile.attr_cols.first().and_then(|col| col.get(row)).cloned();
                }
            }
            None
        })
    };

    // valid_at=50 → v1
    let (tiles_50, _) = store.scan_tiles_at(i64::MAX, Some(50)).unwrap();
    assert_eq!(
        find_val(&tiles_50),
        Some(CellValue::Int64(1)),
        "valid_at=50 must return v1 (value=1)"
    );

    // valid_at=150 → nothing
    let (tiles_150, _) = store.scan_tiles_at(i64::MAX, Some(150)).unwrap();
    assert!(
        find_val(&tiles_150).is_none(),
        "valid_at=150 must return nothing (gap between intervals)"
    );

    // valid_at=250 → v2
    let (tiles_250, _) = store.scan_tiles_at(i64::MAX, Some(250)).unwrap();
    assert_eq!(
        find_val(&tiles_250),
        Some(CellValue::Int64(2)),
        "valid_at=250 must return v2 (value=2)"
    );
}
