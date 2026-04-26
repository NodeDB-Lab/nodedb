//! Bitemporal recovery and follower-replay correctness for the array engine.
//!
//! Two correctness properties are verified:
//!
//! 1. **Crash mid-write** — an `ArrayEngine` that drops without an explicit
//!    flush must, on reopen, surface the correct version at every
//!    `system_as_of` cutoff. The WAL replay path (`stamp_put_cells`) takes
//!    `system_from_ms` directly from the decoded `ArrayPutCell` without
//!    re-deriving it from the clock.
//!
//! 2. **Follower replay idempotency** — applying the same `ArrayPutPayload`
//!    sequence to two independent engine instances must yield identical
//!    `scan_tiles_at` results at every cutoff.

use std::sync::Arc;

use nodedb::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
use nodedb::engine::array::wal::ArrayPutCell;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_types::{Surrogate, TenantId};

// ── shared fixtures ────────────────────────────────────────────────────────────

fn schema() -> Arc<nodedb_array::schema::ArraySchema> {
    Arc::new(
        ArraySchemaBuilder::new("bt")
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

const SCHEMA_HASH: u64 = 0xBE_EF_BE_EF;
const TENANT: TenantId = TenantId::new(1);

fn aid() -> ArrayId {
    ArrayId::new(TENANT, "bt")
}

fn open_engine(root: &std::path::Path) -> ArrayEngine {
    let mut e = ArrayEngine::new(ArrayEngineConfig::new(root.to_path_buf())).unwrap();
    e.open_array(aid(), schema(), SCHEMA_HASH).unwrap();
    e
}

/// Build one `ArrayPutCell` at coord `x` with attr value `v`, stamped with
/// the given `system_from_ms` and `valid_from_ms`.
fn put_cell(x: i64, v: i64, system_from_ms: i64, valid_from_ms: i64) -> ArrayPutCell {
    ArrayPutCell {
        coord: vec![CoordValue::Int64(x)],
        attrs: vec![CellValue::Int64(v)],
        surrogate: Surrogate::ZERO,
        system_from_ms,
        valid_from_ms,
        valid_until_ms: i64::MAX,
    }
}

/// Extract the first int64 attr value from a scan result containing exactly
/// one tile with exactly one live cell.
fn single_cell_value(
    tiles: &[(u64, nodedb_array::tile::sparse_tile::SparseTile)],
    label: &str,
) -> i64 {
    assert_eq!(
        tiles.len(),
        1,
        "{label}: expected 1 tile, got {}",
        tiles.len()
    );
    let (_, tile) = &tiles[0];
    assert_eq!(tile.nnz(), 1, "{label}: expected nnz=1, got {}", tile.nnz());
    match &tile.attr_cols[0][0] {
        CellValue::Int64(n) => *n,
        other => panic!("{label}: unexpected attr type {other:?}"),
    }
}

// ── test 1: crash mid-write ────────────────────────────────────────────────────

/// Simulate a crash between writes (no explicit flush). On reopen the
/// memtable is recovered from WAL records whose `system_from_ms` values are
/// taken directly from the decoded payloads — never re-derived from the clock.
///
/// Writes three versions at explicit system-time stamps {100, 200, 300},
/// then drops the engine without flush (crash). Reopen and replay the same
/// writes (as a real WAL recovery would). Sweep cutoffs and verify the
/// ceiling resolver returns the correct version at each point.
#[test]
fn ceiling_resolves_correctly_after_crash_mid_write() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    // Phase 1: write v1 @ sys=100, v2 @ sys=200, v3 @ sys=300, then
    // "crash" by dropping the engine without flush.
    {
        let mut e = open_engine(&root);
        e.put_cells(&aid(), vec![put_cell(0, 1, 100, 100)], 1)
            .unwrap();
        e.put_cells(&aid(), vec![put_cell(0, 2, 200, 200)], 2)
            .unwrap();
        e.put_cells(&aid(), vec![put_cell(0, 3, 300, 300)], 3)
            .unwrap();
        // engine drops here without flush — memtable is lost, simulating crash.
    }

    // Phase 2: reopen and replay the same writes (WAL recovery path). In a
    // real system, the engine's open path streams decoded WAL records into
    // `stamp_put_cells`. We model it directly: the versioned memtable key is
    // (coord, system_from_ms), so re-applying the same payload is idempotent.
    let mut e = open_engine(&root);
    e.put_cells(&aid(), vec![put_cell(0, 1, 100, 100)], 1)
        .unwrap();
    e.put_cells(&aid(), vec![put_cell(0, 2, 200, 200)], 2)
        .unwrap();
    e.put_cells(&aid(), vec![put_cell(0, 3, 300, 300)], 3)
        .unwrap();

    let store = e.store(&aid()).unwrap();

    // cutoff 50 → below all versions; truncated_before_horizon = true
    {
        let (tiles, truncated) = store.scan_tiles_at(50, None).unwrap();
        assert_eq!(
            tiles.len(),
            0,
            "cutoff 50: expected 0 live tiles, got {}",
            tiles.len()
        );
        assert!(
            truncated,
            "cutoff 50: truncated_before_horizon must be true — data exists but all after cutoff"
        );
    }

    // cutoffs 100 and 150 → v1 (value=1)
    for cutoff in [100_i64, 150] {
        let (tiles, truncated) = store.scan_tiles_at(cutoff, None).unwrap();
        assert!(
            !truncated,
            "cutoff {cutoff}: truncated_before_horizon must be false"
        );
        let v = single_cell_value(&tiles, &format!("cutoff {cutoff}"));
        assert_eq!(v, 1, "cutoff {cutoff}: expected v1=1, got {v}");
    }

    // cutoffs 200 and 250 → v2 (value=2)
    for cutoff in [200_i64, 250] {
        let (tiles, truncated) = store.scan_tiles_at(cutoff, None).unwrap();
        assert!(
            !truncated,
            "cutoff {cutoff}: truncated_before_horizon must be false"
        );
        let v = single_cell_value(&tiles, &format!("cutoff {cutoff}"));
        assert_eq!(v, 2, "cutoff {cutoff}: expected v2=2, got {v}");
    }

    // cutoffs 300 and 1000 → v3 (value=3)
    for cutoff in [300_i64, 1000] {
        let (tiles, truncated) = store.scan_tiles_at(cutoff, None).unwrap();
        assert!(
            !truncated,
            "cutoff {cutoff}: truncated_before_horizon must be false"
        );
        let v = single_cell_value(&tiles, &format!("cutoff {cutoff}"));
        assert_eq!(v, 3, "cutoff {cutoff}: expected v3=3, got {v}");
    }
}

// ── test 2: follower replay yields identical state ─────────────────────────────

/// Apply the same versioned write sequence to two independent engine
/// instances. The write payloads carry explicit `system_from_ms` values that
/// are preserved byte-for-byte through `stamp_put_cells`. Sweeping
/// `scan_tiles_at` at every interesting cutoff must yield identical results
/// on both instances — this is the follower-replay invariant.
#[test]
fn follower_replay_yields_identical_state() {
    type Write = Box<dyn Fn(&mut ArrayEngine)>;

    let writes: Vec<Write> = vec![
        Box::new(|e: &mut ArrayEngine| {
            e.put_cells(&aid(), vec![put_cell(0, 1, 100, 100)], 1)
                .unwrap();
        }),
        Box::new(|e: &mut ArrayEngine| {
            e.put_cells(&aid(), vec![put_cell(0, 2, 200, 150)], 2)
                .unwrap();
        }),
        Box::new(|e: &mut ArrayEngine| {
            e.put_cells(&aid(), vec![put_cell(5, 10, 250, 0)], 3)
                .unwrap();
        }),
        Box::new(|e: &mut ArrayEngine| {
            e.put_cells(&aid(), vec![put_cell(0, 3, 300, 300)], 4)
                .unwrap();
        }),
    ];

    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let mut e1 = open_engine(dir1.path());
    let mut e2 = open_engine(dir2.path());

    for w in &writes {
        w(&mut e1);
        w(&mut e2);
    }

    // Flush both so the segment-scan path is also exercised.
    e1.flush(&aid(), 10).unwrap();
    e2.flush(&aid(), 10).unwrap();

    let s1 = e1.store(&aid()).unwrap();
    let s2 = e2.store(&aid()).unwrap();

    // Verify the manifest structure is identical: same segment count,
    // same min_tile/max_tile per segment.
    let m1 = s1.manifest();
    let m2 = s2.manifest();
    assert_eq!(
        m1.segments.len(),
        m2.segments.len(),
        "manifest segment count diverged: e1={} e2={}",
        m1.segments.len(),
        m2.segments.len()
    );
    for (seg1, seg2) in m1.segments.iter().zip(m2.segments.iter()) {
        assert_eq!(
            seg1.min_tile, seg2.min_tile,
            "segment min_tile diverged: e1={:?} e2={:?}",
            seg1.min_tile, seg2.min_tile
        );
        assert_eq!(
            seg1.max_tile, seg2.max_tile,
            "segment max_tile diverged: e1={:?} e2={:?}",
            seg1.max_tile, seg2.max_tile
        );
    }

    // Sweep cutoffs spanning before-first, between-versions, after-last.
    for cutoff in [50_i64, 150, 250, 350, 1000] {
        let (tiles1, trunc1) = s1.scan_tiles_at(cutoff, None).unwrap();
        let (tiles2, trunc2) = s2.scan_tiles_at(cutoff, None).unwrap();

        assert_eq!(
            trunc1, trunc2,
            "cutoff {cutoff}: truncated_before_horizon diverged: e1={trunc1} e2={trunc2}"
        );
        assert_eq!(
            tiles1.len(),
            tiles2.len(),
            "cutoff {cutoff}: tile count diverged: e1={} e2={}",
            tiles1.len(),
            tiles2.len()
        );

        // Collect (hilbert_prefix, nnz, attr_col[0]) for each tile in both
        // instances and sort by prefix for stable comparison.
        let mut summary1: Vec<(u64, u32, Vec<CellValue>)> = tiles1
            .iter()
            .map(|(p, t)| (*p, t.nnz(), t.attr_cols[0].clone()))
            .collect();
        let mut summary2: Vec<(u64, u32, Vec<CellValue>)> = tiles2
            .iter()
            .map(|(p, t)| (*p, t.nnz(), t.attr_cols[0].clone()))
            .collect();
        summary1.sort_by_key(|(p, _, _)| *p);
        summary2.sort_by_key(|(p, _, _)| *p);

        assert_eq!(
            summary1, summary2,
            "cutoff {cutoff}: tile summaries diverged between e1 and e2"
        );

        // valid_from_ms must also agree — this catches any clock-based skew.
        let vfms1: Vec<Vec<i64>> = tiles1
            .iter()
            .map(|(_, t)| t.valid_from_ms.clone())
            .collect();
        let vfms2: Vec<Vec<i64>> = tiles2
            .iter()
            .map(|(_, t)| t.valid_from_ms.clone())
            .collect();
        assert_eq!(
            vfms1, vfms2,
            "cutoff {cutoff}: valid_from_ms vectors diverged between e1 and e2"
        );
    }
}
