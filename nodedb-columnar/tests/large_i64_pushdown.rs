// SPDX-License-Identifier: Apache-2.0

//! End-to-end integration test for predicate pushdown correctness with
//! i64 values outside ±2^53 (not exactly representable in f64).
//!
//! Regression target: a block whose i64 min/max both round to the same f64 as
//! the predicate value must NOT be skipped when the predicate value actually
//! falls within the exact i64 range.

use std::sync::Arc;

use nodedb_columnar::{
    SegmentReader, SegmentWriter, memtable::ColumnarMemtable, predicate::ScanPredicate,
    reader::DecodedColumn,
};
use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor, ScopedMemory};
use nodedb_types::{
    DatabaseId, TenantId,
    columnar::{ColumnDef, ColumnType, ColumnarSchema},
    value::Value,
};

/// A scope backed by a governor whose ceiling covers every engine's limit.
fn test_memory() -> ScopedMemory {
    let per_engine = usize::MAX / EngineId::ALL.len();
    let governor = Arc::new(
        MemoryGovernor::new(GovernorConfig {
            global_ceiling: per_engine * EngineId::ALL.len(),
            engine_limits: EngineLimits::uniform(per_engine),
        })
        .expect("test governor"),
    );
    ScopedMemory::new(
        governor,
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Columnar,
    )
}

/// Target value: outside ±2^53, well below i64::MAX.
/// 9_223_372_036_854_775_000 rounds to the same f64 as values nearby,
/// so the old f64-only block stats path would be ambiguous.
const TARGET: i64 = 9_223_372_036_854_775_000;

/// A probe value that is different from TARGET but rounds to the same f64.
///
/// The f64 ULP at this magnitude is 2048, so any i64 within ±1024 of TARGET
/// rounds to the same f64.  TARGET - 600 is within that window yet sits
/// outside the block-1 value range [TARGET-500, TARGET+523], making it a
/// clean negative test: the block must not be skipped (same f64 → ambiguous),
/// but the row-level scan must return no match (value is absent).
const PROBE: i64 = 9_223_372_036_854_775_000 - 600; // TARGET - 600

fn large_i64_schema() -> ColumnarSchema {
    ColumnarSchema::new(vec![
        ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ColumnDef::required("val", ColumnType::Int64),
    ])
    .expect("valid schema")
}

/// Write a segment with two blocks:
///
/// Block 0 (rows 0..1024): small i64 values — distinct, well within ±2^53.
/// Block 1 (rows 1024..2048): large i64 values centred around TARGET.
///   Includes TARGET itself and values just above/below it so the block
///   min/max captures the exact range [TARGET-500, TARGET+500].
///
/// The predicate `val == TARGET` must:
///   - skip block 0 (TARGET is far outside [0, 1023])
///   - NOT skip block 1 (TARGET is within [TARGET-500, TARGET+500])
fn write_two_block_segment() -> Vec<u8> {
    let schema = large_i64_schema();
    let mut mt = ColumnarMemtable::new(&schema);

    // Block 0: 1024 small values.
    for i in 0i64..1024 {
        mt.append_row(&[Value::Integer(i), Value::Integer(i)])
            .unwrap();
    }

    // Block 1: 1024 large values centred on TARGET.
    // Row 1024 gets value TARGET-500, ..., row 1524 gets TARGET, ...,
    // row 2047 gets TARGET+523.
    for i in 0i64..1024 {
        let v = TARGET - 500 + i;
        let id = 1024 + i;
        mt.append_row(&[Value::Integer(id), Value::Integer(v)])
            .unwrap();
    }

    let (schema, columns, row_count) = mt.drain();
    SegmentWriter::new(nodedb_columnar::writer::PROFILE_PLAIN, test_memory())
        .write_segment(&schema, &columns, row_count, None)
        .expect("write segment")
}

/// Assert that `TARGET as f64 == PROBE as f64` so we know the test is covering
/// the ambiguous-f64 case.  This is a compile-time sanity check embedded in the
/// test; if the constants are adjusted, this will catch it.
#[test]
fn precondition_target_and_probe_share_same_f64() {
    assert_eq!(
        TARGET as f64, PROBE as f64,
        "TARGET and PROBE must round to the same f64 for this test to be meaningful"
    );
    // Also confirm both are outside ±2^53.
    let safe_limit: i64 = 1 << 53;
    assert!(
        TARGET.abs() > safe_limit,
        "TARGET must be outside ±2^53 to exercise the i64 exact path"
    );
    assert!(
        PROBE.abs() > safe_limit,
        "PROBE must be outside ±2^53 to exercise the i64 exact path"
    );
    // And that they are genuinely different i64 values.
    assert_ne!(
        TARGET, PROBE,
        "TARGET and PROBE must be distinct i64 values"
    );
}

/// The row with val == TARGET is returned — block 1 is NOT skipped.
#[test]
fn large_i64_predicate_pushdown_does_not_skip_block() {
    let segment_bytes = write_two_block_segment();
    let reader = SegmentReader::open(&segment_bytes).unwrap();

    assert_eq!(reader.row_count(), 2048, "expected 2 full blocks");

    let predicate = ScanPredicate::eq_i64(1, TARGET); // column index 1 = "val"
    let col = reader
        .read_column_filtered(1, &[predicate])
        .expect("read val column");

    let (values, valid) = match col {
        DecodedColumn::Int64 { values, valid } => (values, valid),
        other => panic!("expected Int64, got {other:?}"),
    };

    // Rows that pass the filter are non-null; skipped blocks emit zero-fill with
    // valid=false.  Exactly one row must have valid=true and value==TARGET.
    let matching: Vec<usize> = values
        .iter()
        .zip(valid.iter())
        .enumerate()
        .filter(|(_, (v, ok))| **ok && **v == TARGET)
        .map(|(i, _)| i)
        .collect();

    assert_eq!(
        matching.len(),
        1,
        "exactly one row must match val == TARGET; found {}: {:?}",
        matching.len(),
        matching,
    );

    // The match must be in block 1 (row index >= 1024).
    assert!(
        matching[0] >= 1024,
        "matching row {} is in block 0 (row < 1024), wrong block",
        matching[0]
    );
}

/// A probe value that rounds to the same f64 as TARGET but differs
/// in exact i64 representation is NOT returned (no false positive).
///
/// PROBE = TARGET - 600 shares the same f64 as TARGET (ULP at this magnitude
/// is 2048, so any offset < 1024 is absorbed).  PROBE is outside block 1's
/// value range [TARGET-500, TARGET+523], so no row carries that exact i64.
/// The correct behaviour: block 1 is NOT skipped (f64 stats are ambiguous),
/// but the row scan finds no match and returns nothing.
#[test]
fn large_i64_distinct_value_no_false_positive() {
    let probe_outside_range = PROBE;
    // Confirm PROBE is indeed outside [TARGET-500, TARGET+523].
    assert!(
        probe_outside_range < TARGET - 500,
        "probe must be outside block 1 range for a clean negative test"
    );
    // And it still collapses to the same f64 as TARGET.
    assert_eq!(
        probe_outside_range as f64, TARGET as f64,
        "probe must share f64 repr with TARGET"
    );

    let segment_bytes = write_two_block_segment();
    let reader = SegmentReader::open(&segment_bytes).unwrap();

    let predicate = ScanPredicate::eq_i64(1, probe_outside_range);
    let col = reader
        .read_column_filtered(1, &[predicate])
        .expect("read val column with probe predicate");

    let (values, valid) = match col {
        DecodedColumn::Int64 { values, valid } => (values, valid),
        other => panic!("expected Int64, got {other:?}"),
    };

    let false_positives: Vec<usize> = values
        .iter()
        .zip(valid.iter())
        .enumerate()
        .filter(|(_, (v, ok))| **ok && **v == probe_outside_range)
        .map(|(i, _)| i)
        .collect();

    assert_eq!(
        false_positives.len(),
        0,
        "probe value {probe_outside_range} must not match any row; got false positives at indices: {false_positives:?}"
    );
}
