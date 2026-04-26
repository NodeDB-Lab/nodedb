//! Integration tests: array compaction with `audit_retain_ms` retention.
//!
//! Regression coverage for the cell-loss bug: tile-versions are SPARSE —
//! each version stores only the cells written at its `system_from_ms`. A
//! tile-level retention drop therefore loses cells that exist exclusively
//! in an older tile-version. The merger must operate at cell granularity.

use std::sync::Arc;

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

use nodedb::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};

fn schema() -> Arc<nodedb_array::schema::ArraySchema> {
    Arc::new(
        ArraySchemaBuilder::new("ret")
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
    ArrayId::new(TenantId::new(1), "ret")
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

fn delete_cell(e: &mut ArrayEngine, x: i64, sys_ms: i64, lsn: u64) {
    e.delete_cells(
        &aid(),
        vec![ArrayDeleteCell {
            coord: vec![CoordValue::Int64(x)],
            system_from_ms: sys_ms,
            erasure: false,
        }],
        lsn,
    )
    .unwrap();
}

fn gdpr_erase(e: &mut ArrayEngine, x: i64, sys_ms: i64, lsn: u64) {
    e.gdpr_erase_cell(&aid(), vec![CoordValue::Int64(x)], sys_ms, lsn)
        .unwrap();
}

fn compact_all(e: &mut ArrayEngine, audit_retain_ms: Option<i64>, now_ms: i64) {
    loop {
        if !e.maybe_compact(&aid(), audit_retain_ms, now_ms).unwrap() {
            break;
        }
    }
}

fn ceiling_at(
    e: &ArrayEngine,
    x: i64,
    system_as_of: i64,
) -> nodedb_array::query::ceiling::CeilingResult {
    e.store(&aid())
        .unwrap()
        .ceiling_for_coord(&[CoordValue::Int64(x)], system_as_of, None)
        .unwrap()
}

// ── Regression: cell-loss bug ─────────────────────────────────────────────────

/// Cell A (x=0) written at T=100, Cell B (x=1) written at T=200. Same tile.
/// `audit_retain_ms=100`, `now_ms=400` → horizon=300; both versions outside.
///
/// Before the cell-level fix, the tile at T=100 was "superseded" by T=200 at
/// the tile-version level, so cell A (which only appears in T=100) was silently
/// dropped. After the fix, the ceiling tile must carry BOTH A and B.
#[test]
fn compaction_preserves_cells_in_separate_tile_versions() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 1;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0xBEEF).unwrap();

    put_cell(&mut e, 0, 10, 100, 1); // A: coord x=0
    put_cell(&mut e, 1, 20, 200, 2); // B: coord x=1
    put_cell(&mut e, 2, 30, 210, 3); // padding
    put_cell(&mut e, 3, 40, 220, 4); // padding

    compact_all(&mut e, Some(100), 400); // horizon = 400 - 100 = 300

    use nodedb_array::query::ceiling::CeilingResult;
    let a = ceiling_at(&e, 0, 400);
    let b = ceiling_at(&e, 1, 400);
    assert!(
        matches!(a, CeilingResult::Live(_)),
        "x=0 (cell A) must survive: {a:?}"
    );
    assert!(
        matches!(b, CeilingResult::Live(_)),
        "x=1 (cell B) must survive: {b:?}"
    );
}

// ── Tombstone collapse ────────────────────────────────────────────────────────

/// Live(A) at T=100, Tombstone(A) at T=200. horizon=300.
/// Ceiling must contain Tombstone(A): the deletion propagates past compaction.
#[test]
fn compaction_collapses_tombstone_outside_horizon() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 1;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0xCAFE).unwrap();

    put_cell(&mut e, 0, 10, 100, 1);
    delete_cell(&mut e, 0, 200, 2);
    put_cell(&mut e, 4, 99, 210, 3);
    put_cell(&mut e, 8, 99, 220, 4);

    // retain=700, now=1000 → horizon=300; T=100 and T=200 outside horizon.
    compact_all(&mut e, Some(700), 1000);

    use nodedb_array::query::ceiling::CeilingResult;
    let result = ceiling_at(&e, 0, i64::MAX);
    assert!(
        matches!(result, CeilingResult::Tombstoned),
        "x=0 must be tombstoned: {result:?}"
    );
}

// ── GDPR erasure ─────────────────────────────────────────────────────────────

/// Live(A) at T=100, GdprErased(A) at T=200. horizon=300.
/// After compaction, A must be gone: GDPR erases drop from the ceiling.
#[test]
fn compaction_drops_gdpr_erased_cell() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 1;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0xD00D).unwrap();

    put_cell(&mut e, 0, 10, 100, 1);
    gdpr_erase(&mut e, 0, 200, 2);
    put_cell(&mut e, 4, 99, 210, 3);
    put_cell(&mut e, 8, 99, 220, 4);

    compact_all(&mut e, Some(700), 1000);

    use nodedb_array::query::ceiling::CeilingResult;
    let result = ceiling_at(&e, 0, i64::MAX);
    assert!(
        matches!(result, CeilingResult::Erased | CeilingResult::NotFound),
        "x=0 must be gone after GDPR erasure: {result:?}"
    );
}

// ── In-horizon pass-through ───────────────────────────────────────────────────

/// Versions at T=400 and T=500, horizon=300. Both in-horizon: pass through as
/// separate tile versions (no ceiling merging).
#[test]
fn compaction_passes_through_inhorizon_versions() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 1;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0xF00D).unwrap();

    put_cell(&mut e, 0, 10, 400, 1);
    put_cell(&mut e, 0, 20, 500, 2);
    put_cell(&mut e, 4, 30, 410, 3);
    put_cell(&mut e, 8, 40, 420, 4);

    compact_all(&mut e, Some(100), 400); // horizon = 300

    use nodedb_array::query::ceiling::CeilingResult;
    // Ceiling at sys=450 must see the T=400 version (value=10).
    match ceiling_at(&e, 0, 450) {
        CeilingResult::Live(p) => assert_eq!(
            p.attrs.first(),
            Some(&CellValue::Int64(10)),
            "sys=450 must return T=400 version"
        ),
        other => panic!("expected Live at sys=450, got {other:?}"),
    }
    // Ceiling at sys=MAX must see the T=500 version (value=20).
    match ceiling_at(&e, 0, i64::MAX) {
        CeilingResult::Live(p) => assert_eq!(
            p.attrs.first(),
            Some(&CellValue::Int64(20)),
            "sys=MAX must return T=500 version"
        ),
        other => panic!("expected Live at sys=MAX, got {other:?}"),
    }
}

// ── No retention ─────────────────────────────────────────────────────────────

/// `audit_retain_ms = None`: all 4 tile versions of the same coord survive.
#[test]
fn compaction_no_retention_keeps_everything() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 1;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), 0xBEE5).unwrap();

    put_cell(&mut e, 0, 10, 100, 1);
    put_cell(&mut e, 0, 20, 200, 2);
    put_cell(&mut e, 0, 30, 300, 3);
    put_cell(&mut e, 0, 40, 400, 4);

    compact_all(&mut e, None, 0);

    let m = e.store(&aid()).unwrap().manifest();
    assert_eq!(m.segments.len(), 1);
    assert_eq!(
        m.segments[0].tile_count, 4,
        "all 4 versions must survive when audit_retain_ms=None"
    );
}
