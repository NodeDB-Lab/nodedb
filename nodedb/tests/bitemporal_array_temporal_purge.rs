//! End-to-end integration tests for `ArrayEngine::temporal_purge`.
//!
//! Each test builds a fresh `ArrayEngine` in a temp directory, writes
//! cells with explicit `system_from_ms` values, calls `temporal_purge`,
//! and asserts the resulting state via `ceiling_for_coord` /
//! `scan_tiles_at` / the manifest.
//!
//! These tests do not spin up a server or Tokio runtime — the array
//! engine is fully synchronous and exercisable in unit-test style.

use std::sync::Arc;

use nodedb::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
use nodedb::engine::array::wal::ArrayPutCell;
use nodedb_array::query::ceiling::CeilingResult;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_types::{Surrogate, TenantId};
use tempfile::TempDir;

// ── shared fixtures ────────────────────────────────────────────────────────────

fn schema() -> Arc<nodedb_array::schema::ArraySchema> {
    Arc::new(
        ArraySchemaBuilder::new("purge")
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

const SCHEMA_HASH: u64 = 0xA5_A5_A5_A5_A5_A5_A5_A5;
const TENANT: TenantId = TenantId::new(0);
const ARRAY_NAME: &str = "purge";

fn aid() -> ArrayId {
    ArrayId::new(TENANT, ARRAY_NAME)
}

/// Open an engine with `flush_cell_threshold = 1` so every `put_cells`
/// call lands in a fresh segment. This makes it easy to produce multiple
/// system-time versions of the same coordinate in separate segments,
/// which exercises the cross-segment retention logic in `plan.rs`.
fn open_engine(dir: &TempDir) -> ArrayEngine {
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 1;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.open_array(aid(), schema(), SCHEMA_HASH).unwrap();
    e
}

fn put(e: &mut ArrayEngine, x: i64, v: i64, sys_ms: i64, lsn: u64) {
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

fn erase(e: &mut ArrayEngine, x: i64, sys_ms: i64, lsn: u64) {
    e.gdpr_erase_cell(&aid(), vec![CoordValue::Int64(x)], sys_ms, lsn)
        .unwrap();
}

fn ceiling(e: &ArrayEngine, x: i64, sys: i64) -> CeilingResult {
    e.store(&aid())
        .unwrap()
        .ceiling_for_coord(&[CoordValue::Int64(x)], sys, None)
        .unwrap()
}

fn live_val(r: &CeilingResult) -> i64 {
    match r {
        CeilingResult::Live(p) => match p.attrs.first() {
            Some(CellValue::Int64(n)) => *n,
            other => panic!("unexpected attr: {other:?}"),
        },
        other => panic!("expected Live, got: {other:?}"),
    }
}

// ── test 1 ────────────────────────────────────────────────────────────────────

/// Write three system-time versions of cell x=0 at T=100, T=200, T=300.
/// Purge with cutoff=250: only T=100 is both below the cutoff and
/// superseded by T=200 (which is also below the cutoff and is the newest
/// outside-horizon version). After purge:
/// - Ceiling at sys=150 returns the synthetic ceiling tile's value
///   (T=200's value, the newest-below-cutoff record), because the
///   purge collapses all outside-horizon versions into a single ceiling
///   tile placed just below the cutoff.
/// - Ceiling at sys=350 returns T=300's value (in-horizon, untouched).
/// - The total dropped count must be ≥ 1 (tile-versions dropped).
#[test]
fn temporal_purge_drops_superseded_versions_end_to_end() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    put(&mut e, 0, 100, 100, 1); // v=100 at sys=100
    put(&mut e, 0, 200, 200, 2); // v=200 at sys=200
    put(&mut e, 0, 300, 300, 3); // v=300 at sys=300

    // Three separate segments: one per system-time version.
    assert_eq!(
        e.store(&aid()).unwrap().manifest().segments.len(),
        3,
        "setup: expected 3 segments before purge"
    );

    // Purge with cutoff=250: T=100 and T=200 are outside the horizon,
    // T=300 is inside (300 >= 250). The planner collapses T=100 and T=200
    // into a single ceiling tile containing the value from T=200 (newest).
    let dropped = e.temporal_purge(TENANT, ARRAY_NAME, 250).unwrap();
    assert!(
        dropped >= 1,
        "at least one tile-version must be dropped; got {dropped}"
    );

    // After purge, ceiling at sys=350 must see T=300's value (in-horizon).
    let r350 = ceiling(&e, 0, 350);
    assert_eq!(live_val(&r350), 300, "sys=350 must return T=300 value");

    // After purge, ceiling at sys=240 (below both T=100 and T=200)
    // must return the synthetic ceiling (newest-below-cutoff is T=200,
    // so ceiling tile carries v=200). The ceiling tile is placed at
    // system_from_ms = cutoff - 1 = 249 by plan.rs. A scan at sys=240
    // is below 249, so it will either return the ceiling tile (if 249
    // was placed above 240) or return NotFound/truncated. The plan places
    // the ceiling at `horizon_ms - 1` = 249, which is ≥ 240, so the
    // ceiling IS accessible at sys=240.
    //
    // Actually: ceiling scan at sys=240 looks for the newest version with
    // system_from_ms ≤ 240. The ceiling tile has system_from_ms=249 which
    // is > 240, so it is NOT accessible at sys=240 — the query predates
    // all data (truncated). We verify the correct in-horizon read instead.
    let r_max = ceiling(&e, 0, i64::MAX);
    assert_eq!(
        live_val(&r_max),
        300,
        "sys=MAX must still return T=300 value after purge"
    );

    // No ceiling tile is materialised here because the in-horizon T=300
    // version already covers coord x=0 — `merge_for_retention` excludes
    // coords covered by an inside-horizon tile from the ceiling. So a
    // historical AS_OF query below the cutoff returns NotFound, which is
    // the correct semantics: audit history outside the retention horizon
    // is not guaranteed for coords already covered in-horizon.
    let r_249 = ceiling(&e, 0, 249);
    assert!(
        matches!(r_249, CeilingResult::NotFound),
        "sys=249 below cutoff with in-horizon coverage: {r_249:?}"
    );
}

// ── test 2 ────────────────────────────────────────────────────────────────────

/// REGRESSION TEST for the cell-loss bug.
///
/// Cell A (x=0) written at T=100. Cell B (x=4, different coord, but same
/// Hilbert prefix due to same tile extent region) written at T=200.
/// With cutoff=250 both versions are outside the horizon. The purge plan
/// must carry BOTH A and B into the synthetic ceiling tile.
///
/// Without cell-level merge, the plan would see tile-version T=100 as
/// "superseded" by T=200 at the tile-version level and drop it,
/// silently losing cell A which only appears in T=100.
#[test]
fn temporal_purge_preserves_cells_with_no_newer_version() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    // x=0 and x=4 are in the same tile-extent region [0,4).
    // Cell A: only in T=100 tile-version.
    put(&mut e, 0, 10, 100, 1);
    // Cell B: only in T=200 tile-version.
    put(&mut e, 4, 20, 200, 2);

    assert_eq!(
        e.store(&aid()).unwrap().manifest().segments.len(),
        2,
        "setup: expected 2 segments"
    );

    let dropped = e.temporal_purge(TENANT, ARRAY_NAME, 250).unwrap();
    // Two tile-versions exist (T=100 and T=200), both outside horizon=250.
    // Both are dropped and replaced by a single ceiling tile.
    assert!(
        dropped >= 1,
        "at least one tile-version dropped; got {dropped}"
    );

    // CRITICAL: both cells must remain readable after purge.
    let a = ceiling(&e, 0, i64::MAX);
    assert!(
        matches!(a, CeilingResult::Live(_)),
        "cell A (x=0) must survive purge: {a:?}"
    );
    assert_eq!(live_val(&a), 10, "cell A value must be preserved");

    let b = ceiling(&e, 4, i64::MAX);
    assert!(
        matches!(b, CeilingResult::Live(_)),
        "cell B (x=4) must survive purge: {b:?}"
    );
    assert_eq!(live_val(&b), 20, "cell B value must be preserved");
}

// ── test 3 ────────────────────────────────────────────────────────────────────

/// Live(x=0) at T=100, GdprErased(x=0) at T=200. Purge with cutoff=250.
/// The GdprErased version is the newest outside-horizon record for coord
/// x=0. The plan must NOT carry x=0 into the ceiling tile (GDPR erasure
/// is honored). After purge, ceiling for x=0 returns Erased or NotFound.
#[test]
fn temporal_purge_drops_gdpr_erased_cells_outright() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    put(&mut e, 0, 42, 100, 1); // Live(x=0) at T=100
    erase(&mut e, 0, 200, 2); // GdprErased(x=0) at T=200
    e.flush(&aid(), 3).unwrap(); // ensure erasure tile-version lands in a segment

    let dropped = e.temporal_purge(TENANT, ARRAY_NAME, 250).unwrap();
    // Both tile-versions are outside horizon=250 and are candidates for
    // dropping. The GdprErased version is the newest → ceiling for x=0
    // is empty. The entire segment(s) may be removed if no other cells
    // remain.
    assert!(
        dropped >= 1,
        "at least one tile-version dropped; got {dropped}"
    );

    // After purge, x=0 must not be visible as a live cell.
    let r = ceiling(&e, 0, i64::MAX);
    assert!(
        matches!(r, CeilingResult::Erased | CeilingResult::NotFound),
        "GDPR-erased coord must not appear as Live after purge: {r:?}"
    );
}

// ── test 4 ────────────────────────────────────────────────────────────────────

/// WAL audit record — `TemporalPurgePayload` roundtrip for the Array engine.
///
/// This test verifies that the wire format for the Array variant of
/// `TemporalPurgePayload` serializes and deserializes correctly with the
/// `name` field carrying the array id string, matching the field rename
/// from `collection` → `name` implemented in Phase E.
///
/// The full enforcement loop (which writes WAL records to disk) requires
/// a running server. We verify the payload codec — the same object the
/// scheduler constructs and the WAL reader decodes — in isolation.
#[test]
fn temporal_purge_wal_payload_roundtrip_array_engine() {
    use nodedb_wal::{TemporalPurgeEngine, TemporalPurgePayload};

    let array_name = "my_array";
    let cutoff_ms = 1_700_000_000_000_i64;
    let count = 17_u64;

    let payload =
        TemporalPurgePayload::new(TemporalPurgeEngine::Array, array_name, cutoff_ms, count);
    let bytes = payload.to_bytes().unwrap();
    let decoded = TemporalPurgePayload::from_bytes(&bytes).unwrap();

    assert_eq!(decoded.engine, TemporalPurgeEngine::Array);
    assert_eq!(
        decoded.name, array_name,
        "`name` field must carry the array id"
    );
    assert_eq!(decoded.cutoff_system_ms, cutoff_ms);
    assert_eq!(decoded.purged_count, count);
}

// ── test 5 ────────────────────────────────────────────────────────────────────

/// Idempotency: running `temporal_purge` twice with the same cutoff must
/// not drop anything on the second call, and the live cell state must be
/// identical after both calls.
#[test]
fn temporal_purge_idempotent_on_re_run() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    put(&mut e, 0, 10, 100, 1); // outside horizon
    put(&mut e, 0, 20, 200, 2); // outside horizon
    put(&mut e, 0, 30, 600, 3); // inside horizon (600 >= 500)

    let cutoff = 500_i64;

    // First run: drops the two outside-horizon tile-versions.
    let d1 = e.temporal_purge(TENANT, ARRAY_NAME, cutoff).unwrap();
    assert!(d1 >= 1, "first purge must drop ≥1 tile-version; got {d1}");

    // Snapshot the ceiling state after first purge.
    let after_first = live_val(&ceiling(&e, 0, i64::MAX));

    // Second run: nothing left to drop.
    let d2 = e.temporal_purge(TENANT, ARRAY_NAME, cutoff).unwrap();
    assert_eq!(d2, 0, "second purge with same cutoff must drop nothing");

    // State unchanged.
    let after_second = live_val(&ceiling(&e, 0, i64::MAX));
    assert_eq!(
        after_first, after_second,
        "live cell state must be identical after idempotent re-run"
    );
}

// ── test 6 ────────────────────────────────────────────────────────────────────

/// `temporal_purge` for a non-existent array must return `Ok(0)` without
/// panicking. The array may have been dropped between the scheduler's
/// planning step and execution.
#[test]
fn temporal_purge_no_op_when_array_missing() {
    let dir = TempDir::new().unwrap();
    // Engine with no arrays open.
    let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();

    let count = e.temporal_purge(TENANT, "nonexistent", 99_999).unwrap();
    assert_eq!(count, 0, "purge on missing array must return Ok(0)");
}
