// SPDX-License-Identifier: BUSL-1.1

//! End-to-end integration tests for `ArrayEngine::purge_array_versions_below`.
//!
//! Each test builds a fresh `ArrayEngine` in a temp directory, writes
//! cells with explicit `system_from_ms` values, calls `purge_array_versions_below`,
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
use nodedb_types::{DatabaseId, Surrogate, TenantId};
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
const DATABASE: DatabaseId = DatabaseId::DEFAULT;
const ARRAY_NAME: &str = "purge";

fn aid() -> ArrayId {
    ArrayId::new(TENANT, ARRAY_NAME)
}

/// Open an engine with `flush_cell_threshold = 1` so every `put` lands in
/// a fresh segment through `flush_if_full`. This makes it easy to produce multiple
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
        0,
        vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(x)],
            attrs: vec![CellValue::Int64(v)],
            surrogate: Surrogate::new(x as u32 + 1),
            system_from_ms: sys_ms,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }],
        lsn,
    )
    .unwrap();
    flush_if_full(e, lsn);
}

/// The threshold flush the executor runs after every write it applies,
/// stamped through the write's LSN.
fn flush_if_full(e: &mut ArrayEngine, lsn: u64) {
    if e.needs_flush(&aid()).unwrap() {
        e.flush(
            &aid(),
            nodedb::types::replay_stamp::ReplayStamp::through(lsn),
        )
        .unwrap();
    }
}

fn erase(e: &mut ArrayEngine, x: i64, sys_ms: i64, lsn: u64) {
    e.gdpr_erase_cell(&aid(), 0, vec![CoordValue::Int64(x)], sys_ms, lsn)
        .unwrap();
    flush_if_full(e, lsn);
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
/// Purge below horizon 250: T=100 is superseded by T=200, which is at or
/// below the horizon, so T=100 goes. T=200 is the version a read at the
/// horizon sees, so it stays, carried in one ceiling tile stamped 250.
/// T=300 sits above the horizon and stays untouched.
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

    let dropped = e
        .purge_array_versions_below(TENANT, DATABASE, ARRAY_NAME, 250)
        .unwrap();
    assert_eq!(
        dropped, 2,
        "both versions at or below the horizon are rewritten"
    );

    // The version visible at the horizon stays, at the horizon.
    assert_eq!(live_val(&ceiling(&e, 0, 250)), 200, "sys=250 sees T=200");
    assert_eq!(live_val(&ceiling(&e, 0, 299)), 200, "sys=299 sees T=200");
    assert_eq!(live_val(&ceiling(&e, 0, 350)), 300, "sys=350 sees T=300");
    assert_eq!(
        live_val(&ceiling(&e, 0, i64::MAX)),
        300,
        "sys=MAX must still return T=300 value after purge"
    );

    // History below the horizon is gone.
    let r_249 = ceiling(&e, 0, 249);
    assert!(
        matches!(r_249, CeilingResult::NotFound),
        "sys=249 is below the horizon: {r_249:?}"
    );
}

// ── test 2 ────────────────────────────────────────────────────────────────────

/// REGRESSION TEST for the cell-loss bug.
///
/// Cell A (x=0) written at T=100. Cell B (x=1, a different coord in the
/// same tile [0,4)) written at T=200. Horizon 250 places both tile-versions
/// at or below it, so purge collapses them into one ceiling tile stamped
/// 250. The ceiling must carry BOTH A and B.
///
/// Without cell-level merge, the plan would treat tile-version T=100 as
/// superseded by T=200 at the tile-version level and drop it, silently
/// losing cell A, which appears only in T=100.
#[test]
fn temporal_purge_preserves_cells_with_no_newer_version() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    // x=0 and x=1 share the tile [0,4).
    // Cell A: only in T=100 tile-version.
    put(&mut e, 0, 10, 100, 1);
    // Cell B: only in T=200 tile-version.
    put(&mut e, 1, 20, 200, 2);

    assert_eq!(
        e.store(&aid()).unwrap().manifest().segments.len(),
        2,
        "setup: expected 2 segments"
    );

    let dropped = e
        .purge_array_versions_below(TENANT, DATABASE, ARRAY_NAME, 250)
        .unwrap();
    assert_eq!(
        dropped, 2,
        "both tile-versions at or below the horizon collapse into one ceiling"
    );

    // Both cells stay readable at the horizon and above.
    for sys in [250, i64::MAX] {
        let a = ceiling(&e, 0, sys);
        assert_eq!(live_val(&a), 10, "cell A at sys={sys}");
        let b = ceiling(&e, 1, sys);
        assert_eq!(live_val(&b), 20, "cell B at sys={sys}");
    }

    // History below the horizon is gone for both cells.
    for x in [0, 1] {
        let r = ceiling(&e, x, 249);
        assert!(
            matches!(r, CeilingResult::NotFound),
            "x={x} at sys=249 is below the horizon: {r:?}"
        );
    }
}

/// Cell A (x=0) at T=100 and cell B (x=4) at T=200 sit in different tiles.
/// Each tile holds one live version at or below horizon 250 and no
/// successor, so that version already is the state at the horizon. Purge
/// drops nothing, and both versions keep their original stamps.
#[test]
fn temporal_purge_keeps_lone_versions_in_distinct_tiles() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    put(&mut e, 0, 10, 100, 1); // tile [0,4)
    put(&mut e, 4, 20, 200, 2); // tile [4,8)

    let dropped = e
        .purge_array_versions_below(TENANT, DATABASE, ARRAY_NAME, 250)
        .unwrap();
    assert_eq!(dropped, 0, "no version has a successor: nothing to purge");

    assert_eq!(live_val(&ceiling(&e, 0, 100)), 10, "A stays at its stamp");
    assert_eq!(live_val(&ceiling(&e, 0, i64::MAX)), 10);
    assert_eq!(live_val(&ceiling(&e, 4, 200)), 20, "B stays at its stamp");
    assert_eq!(live_val(&ceiling(&e, 4, i64::MAX)), 20);
    assert!(matches!(ceiling(&e, 4, 199), CeilingResult::NotFound));
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
    e.flush(&aid(), nodedb::types::replay_stamp::ReplayStamp::through(3))
        .unwrap(); // ensure erasure tile-version lands in a segment

    let dropped = e
        .purge_array_versions_below(TENANT, DATABASE, ARRAY_NAME, 250)
        .unwrap();
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
/// `name` field carrying the array id string, matching the field name
/// `name` that replaces `collection`.
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

/// Idempotency: running `purge_array_versions_below` twice with the same cutoff must
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
    let d1 = e
        .purge_array_versions_below(TENANT, DATABASE, ARRAY_NAME, cutoff)
        .unwrap();
    assert!(d1 >= 1, "first purge must drop ≥1 tile-version; got {d1}");

    // Snapshot the ceiling state after first purge.
    let after_first = live_val(&ceiling(&e, 0, i64::MAX));

    // Second run: nothing left to drop.
    let d2 = e
        .purge_array_versions_below(TENANT, DATABASE, ARRAY_NAME, cutoff)
        .unwrap();
    assert_eq!(d2, 0, "second purge with same cutoff must drop nothing");

    // State unchanged.
    let after_second = live_val(&ceiling(&e, 0, i64::MAX));
    assert_eq!(
        after_first, after_second,
        "live cell state must be identical after idempotent re-run"
    );
}

// ── test 6 ────────────────────────────────────────────────────────────────────

/// `purge_array_versions_below` for a non-existent array must return `Ok(0)` without
/// panicking. The array may have been dropped between the scheduler's
/// planning step and execution.
#[test]
fn temporal_purge_no_op_when_array_missing() {
    let dir = TempDir::new().unwrap();
    // Engine with no arrays open.
    let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();

    let count = e
        .purge_array_versions_below(TENANT, DATABASE, "nonexistent", 99_999)
        .unwrap();
    assert_eq!(count, 0, "purge on missing array must return Ok(0)");
}
