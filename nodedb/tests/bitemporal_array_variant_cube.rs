//! Canonical genomics example for bitemporal array semantics.
//!
//! This test documents the motivating real-world use case for bitemporal arrays:
//! genomic variant classification, where scientific interpretation of data
//! changes over time and retroactive corrections must be queryable.
//!
//! # Schema
//!
//! Three dimensions: `(sample_id i64, chromosome i64, position i64)`.
//! One attribute: `classification i64` (0 = benign, 1 = pathogenic, 2 = uncertain).
//!
//! # Scenario
//!
//! **Day 1** (`sys = day_1_ms`): A lab inserts 100 variant calls for samples 0..99,
//! all classified as `1` (pathogenic). The `valid_from_ms` equals `day_1_ms`,
//! signalling that this classification is believed to be true from day 1 onwards.
//!
//! **Day 30** (`sys = day_30_ms`): A revised study reclassifies samples 0..19 to
//! `0` (benign). Crucially, the lab re-inserts these 20 variants with
//! `valid_from_ms = day_1_ms` — the correction applies retroactively to day 1,
//! because the new evidence means the sample was *always* benign, we just
//! didn't know it yet. The `system_from_ms = day_30_ms` records when this
//! belief became known to the system.
//!
//! # What bitemporality preserves
//!
//! - **Reproducibility audit** (`AS OF SYSTEM TIME day_15_ms`): Reconstruct
//!   what the database said on day 15 — before the reclassification. All 100
//!   samples read as pathogenic. This is essential for regulatory compliance
//!   (e.g., FDA audit trails for diagnostic software).
//!
//! - **Current belief, applied to past** (`AS OF SYSTEM TIME day_30_ms, AS OF
//!   VALID TIME day_1_ms`): "Given what we know today, what was the classification
//!   on day 1?" Returns 80 pathogenic + 20 benign. This is the corrected
//!   historical view — useful for retrospective cohort studies.
//!
//! - **Full valid-time range** (`AS OF SYSTEM TIME day_30_ms, AS OF VALID TIME
//!   day_30_ms`): Same result as above because the reclassification's
//!   `valid_from_ms` was set to `day_1_ms`, so the new belief covers
//!   `day_30_ms` as well.

use std::sync::Arc;

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

// ── schema and fixtures ────────────────────────────────────────────────────────

/// 3-dim genomics schema: (sample_id, chromosome, position) → classification.
fn schema() -> Arc<nodedb_array::schema::ArraySchema> {
    Arc::new(
        ArraySchemaBuilder::new("variants")
            .dim(DimSpec::new(
                "sample_id",
                DimType::Int64,
                // 100 samples: 0..99
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(127)),
            ))
            .dim(DimSpec::new(
                "chromosome",
                DimType::Int64,
                // 24 human chromosomes (1-22, X=23, Y=24)
                Domain::new(DomainBound::Int64(1), DomainBound::Int64(24)),
            ))
            .dim(DimSpec::new(
                "position",
                DimType::Int64,
                // SNP position bucket (encoded as small index here for test simplicity)
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(127)),
            ))
            .attr(AttrSpec::new("classification", AttrType::Int64, true))
            .tile_extents(vec![32, 8, 32])
            .build()
            .unwrap(),
    )
}

const SCHEMA_HASH: u64 = 0xDE_AD_BE_EF_C0_DE;
const TENANT: TenantId = TenantId::new(1);

fn aid() -> ArrayId {
    ArrayId::new(TENANT, "variants")
}

fn open_engine(dir: &TempDir) -> ArrayEngine {
    let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
    e.open_array(aid(), schema(), SCHEMA_HASH).unwrap();
    e
}

fn coord(sample_id: i64, chromosome: i64, position: i64) -> Vec<CoordValue> {
    vec![
        CoordValue::Int64(sample_id),
        CoordValue::Int64(chromosome),
        CoordValue::Int64(position),
    ]
}

fn variant_cell(
    sample_id: i64,
    classification: i64,
    system_from_ms: i64,
    valid_from_ms: i64,
) -> ArrayPutCell {
    ArrayPutCell {
        coord: coord(sample_id, 1, 0),
        attrs: vec![CellValue::Int64(classification)],
        surrogate: Surrogate::ZERO,
        system_from_ms,
        valid_from_ms,
        valid_until_ms: OPEN_UPPER,
    }
}

// ── test ──────────────────────────────────────────────────────────────────────

/// Canonical genomics variant reclassification scenario.
///
/// Verifies three bitemporal query patterns that matter for clinical genomics:
/// audit reproducibility, retroactive correction, and full-range validity.
#[test]
fn genomics_variant_reclassification_bitemporal() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    // Simulated wall-clock values (ms since epoch).
    let day_1_ms: i64 = 1_000_000;
    let day_15_ms: i64 = 1_000_000 + 14 * 24 * 3600 * 1000;
    let day_30_ms: i64 = 1_000_000 + 29 * 24 * 3600 * 1000;

    // ── Day 1: insert 100 pathogenic classifications ───────────────────────────
    //
    // All 100 samples, chromosome 1, position 0.
    // system_from_ms = day_1_ms (when the system learned about these).
    // valid_from_ms  = day_1_ms (we believe this was true from day 1).
    {
        let cells: Vec<ArrayPutCell> = (0_i64..100)
            .map(|s| variant_cell(s, 1 /* pathogenic */, day_1_ms, day_1_ms))
            .collect();
        // Write in batches of 20 to avoid any single call with 100 cells
        // (stays under any auto-flush threshold for the test engine config).
        for (i, chunk) in cells.chunks(20).enumerate() {
            let lsn = (i as u64) + 1;
            e.put_cells(&aid(), chunk.to_vec(), lsn).unwrap();
        }
    }

    // ── Day 30: retroactively reclassify samples 0..19 to benign ─────────────
    //
    // system_from_ms = day_30_ms (the system learns about this on day 30).
    // valid_from_ms  = day_1_ms  (the new fact applies retroactively to day 1).
    {
        let cells: Vec<ArrayPutCell> = (0_i64..20)
            .map(|s| variant_cell(s, 0 /* benign */, day_30_ms, day_1_ms))
            .collect();
        e.put_cells(&aid(), cells, 10).unwrap();
    }

    let store = e.store(&aid()).unwrap();

    // ── Assertion 1: audit reproducibility ────────────────────────────────────
    //
    // AS OF SYSTEM TIME day_15_ms → the reclassification hasn't happened yet
    // (it's stamped at day_30_ms). All 100 samples must read as pathogenic (1).
    {
        let (tiles, truncated) = store.scan_tiles_at(day_15_ms, None).unwrap();
        assert!(!truncated, "day-15 audit: not truncated");

        // Count pathogenic rows across all tiles.
        let pathogenic_count: usize = tiles
            .iter()
            .map(|(_, tile)| {
                tile.attr_cols[0]
                    .iter()
                    .filter(|v| matches!(v, CellValue::Int64(1)))
                    .count()
            })
            .sum();
        let benign_count: usize = tiles
            .iter()
            .map(|(_, tile)| {
                tile.attr_cols[0]
                    .iter()
                    .filter(|v| matches!(v, CellValue::Int64(0)))
                    .count()
            })
            .sum();

        assert_eq!(
            pathogenic_count, 100,
            "day-15 audit: must see all 100 pathogenic (reclassification not yet in system)"
        );
        assert_eq!(
            benign_count, 0,
            "day-15 audit: must see 0 benign before reclassification"
        );
    }

    // ── Assertion 2: current belief, retroactively applied to day 1 ──────────
    //
    // AS OF SYSTEM TIME day_30_ms, AS OF VALID TIME day_1_ms →
    // the ceiling resolver picks the newest system-time version at or before
    // day_30_ms. For samples 0..19, that's the reclassification (benign).
    // For samples 20..99, that's the original (pathogenic).
    // Because reclassification's valid_from_ms = day_1_ms, the valid-time
    // filter for valid_at_ms = day_1_ms passes for the new versions too.
    {
        let (tiles, truncated) = store.scan_tiles_at(day_30_ms, Some(day_1_ms)).unwrap();
        assert!(!truncated, "day-30/vt=day1: not truncated");

        let pathogenic_count: usize = tiles
            .iter()
            .map(|(_, tile)| {
                tile.attr_cols[0]
                    .iter()
                    .filter(|v| matches!(v, CellValue::Int64(1)))
                    .count()
            })
            .sum();
        let benign_count: usize = tiles
            .iter()
            .map(|(_, tile)| {
                tile.attr_cols[0]
                    .iter()
                    .filter(|v| matches!(v, CellValue::Int64(0)))
                    .count()
            })
            .sum();

        assert_eq!(
            pathogenic_count, 80,
            "day-30/vt=day1: 80 pathogenic (samples 20..99)"
        );
        assert_eq!(
            benign_count, 20,
            "day-30/vt=day1: 20 benign (reclassified samples 0..19)"
        );
    }

    // ── Assertion 3: same result at valid_at_ms = day_30_ms ──────────────────
    //
    // AS OF SYSTEM TIME day_30_ms, AS OF VALID TIME day_30_ms →
    // the reclassification's valid_from_ms = day_1_ms, so valid_at_ms = day_30_ms
    // is still inside the [day_1_ms, OPEN_UPPER) interval. Result is 80+20.
    {
        let (tiles, truncated) = store.scan_tiles_at(day_30_ms, Some(day_30_ms)).unwrap();
        assert!(!truncated, "day-30/vt=day30: not truncated");

        let pathogenic_count: usize = tiles
            .iter()
            .map(|(_, tile)| {
                tile.attr_cols[0]
                    .iter()
                    .filter(|v| matches!(v, CellValue::Int64(1)))
                    .count()
            })
            .sum();
        let benign_count: usize = tiles
            .iter()
            .map(|(_, tile)| {
                tile.attr_cols[0]
                    .iter()
                    .filter(|v| matches!(v, CellValue::Int64(0)))
                    .count()
            })
            .sum();

        assert_eq!(
            pathogenic_count, 80,
            "day-30/vt=day30: 80 pathogenic (samples 20..99)"
        );
        assert_eq!(
            benign_count, 20,
            "day-30/vt=day30: 20 benign (reclassified samples 0..19)"
        );
    }
}
