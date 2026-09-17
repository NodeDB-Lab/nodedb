// SPDX-License-Identifier: BUSL-1.1

//! Bridge between the timeseries-specific columnar types and the shared
//! `nodedb-columnar` crate types.
//!
//! The timeseries engine has `Symbol` columns (dictionary-encoded tags) that
//! don't exist in the shared columnar format. This bridge converts timeseries
//! drain results into the shared format for segment writing, handling Symbol
//! columns by writing them as Int64 (their u32 symbol IDs cast to i64).
//!
//! Symbol dictionaries are persisted separately alongside the segment.

use nodedb_columnar::format::BlockStats;
use nodedb_columnar::memtable::ColumnData as SharedColumnData;
use nodedb_columnar::predicate::ScanPredicate;
use nodedb_columnar::reader::{DecodedColumn, SegmentReader};
use nodedb_columnar::writer::SegmentWriter;
use nodedb_mem::ScopedMemory;
use nodedb_types::InstantKind;
use nodedb_types::columnar::{ColumnDef, ColumnType as SharedColumnType, ColumnarSchema};

use super::columnar_memtable::{
    ColumnData as TsColumnData, ColumnType as TsColumnType, ColumnarDrainResult, TimeKind,
};

/// Convert a timeseries ColumnarSchema to a shared ColumnarSchema.
///
/// `Symbol` columns are mapped to `Int64` in the shared schema (the u32
/// symbol IDs are stored as i64). The symbol dictionary is persisted
/// separately.
pub fn ts_schema_to_shared(ts_schema: &super::columnar_memtable::ColumnarSchema) -> ColumnarSchema {
    let columns: Vec<ColumnDef> = ts_schema
        .columns
        .iter()
        .map(|(name, ts_type)| {
            let shared_type = ts_column_type_to_shared(*ts_type);
            // All timeseries columns are non-nullable (metrics always present).
            ColumnDef::required(name.clone(), shared_type)
        })
        .collect();

    // Schema validation can't fail here since we're converting known-good types.
    ColumnarSchema {
        columns,
        version: 1,
    }
}

/// Map a timeseries ColumnType to a shared ColumnType.
///
/// A time column maps by its kind: a naive instant is `Timestamp`, a UTC
/// instant is `Timestamptz`, and a `Millis` column is the `Int64` its
/// declaration named. All three share `i64` storage on both sides.
fn ts_column_type_to_shared(ts_type: TsColumnType) -> SharedColumnType {
    match ts_type {
        TsColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive)) => {
            SharedColumnType::Timestamp
        }
        TsColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc)) => {
            SharedColumnType::Timestamptz
        }
        TsColumnType::Timestamp(TimeKind::Millis) => SharedColumnType::Int64,
        TsColumnType::Float64 => SharedColumnType::Float64,
        TsColumnType::Int64 => SharedColumnType::Int64,
        // Symbol columns store u32 IDs — represented as Int64 in shared format.
        TsColumnType::Symbol => SharedColumnType::Int64,
    }
}

/// Convert timeseries column data to shared ColumnData for the SegmentWriter.
///
/// Returns a Vec of shared ColumnData plus the row count.
pub fn ts_drain_to_shared_columns(drain: &ColumnarDrainResult) -> (Vec<SharedColumnData>, usize) {
    let row_count = drain.row_count as usize;
    let mut shared_columns = Vec::with_capacity(drain.columns.len());

    for (i, ts_col) in drain.columns.iter().enumerate() {
        let ts_type = drain.schema.columns[i].1;
        let shared = ts_column_to_shared(ts_col, ts_type, row_count);
        shared_columns.push(shared);
    }

    (shared_columns, row_count)
}

/// Convert a single timeseries ColumnData to a shared ColumnData.
fn ts_column_to_shared(
    ts_col: &TsColumnData,
    ts_type: TsColumnType,
    row_count: usize,
) -> SharedColumnData {
    match (ts_col, ts_type) {
        (TsColumnData::Timestamp(values), TsColumnType::Timestamp(TimeKind::Instant(_))) => {
            SharedColumnData::Timestamp {
                values: values.clone(),
                valid: None, // Timeseries columns are non-nullable.
            }
        }
        (TsColumnData::Timestamp(values), TsColumnType::Timestamp(TimeKind::Millis)) => {
            SharedColumnData::Int64 {
                values: values.clone(),
                valid: None,
            }
        }
        (TsColumnData::Float64(values), TsColumnType::Float64) => SharedColumnData::Float64 {
            values: values.clone(),
            valid: None,
        },
        (TsColumnData::Int64(values), TsColumnType::Int64) => SharedColumnData::Int64 {
            values: values.clone(),
            valid: None,
        },
        (TsColumnData::Symbol(sym_ids), TsColumnType::Symbol) => {
            // Convert u32 symbol IDs to i64 for the shared Int64 column.
            SharedColumnData::Int64 {
                values: sym_ids.iter().map(|&id| id as i64).collect(),
                valid: None,
            }
        }
        // Fallback: shouldn't happen if schema is consistent.
        _ => SharedColumnData::Int64 {
            values: vec![0; row_count],
            valid: Some(vec![false; row_count]),
        },
    }
}

/// Write a timeseries drain result as a shared columnar segment.
///
/// Returns the segment bytes. The caller is responsible for persisting the
/// symbol dictionaries separately.
///
/// When `kek` is `Some`, the segment is wrapped in an AES-256-GCM SEGC
/// envelope before being returned.
pub fn write_ts_drain_as_segment(
    drain: &ColumnarDrainResult,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    memory: ScopedMemory,
) -> Result<Vec<u8>, nodedb_columnar::ColumnarError> {
    let shared_schema = ts_schema_to_shared(&drain.schema);
    let (shared_columns, row_count) = ts_drain_to_shared_columns(drain);

    let writer = SegmentWriter::new(nodedb_columnar::writer::PROFILE_TIMESERIES, memory);
    writer.write_segment(&shared_schema, &shared_columns, row_count, kek)
}

/// Read block statistics from a shared segment footer for timeseries queries.
///
/// Returns the timestamp column's block stats (for time-range predicate pushdown).
pub fn extract_timestamp_block_stats(
    segment_data: &[u8],
) -> Result<Vec<BlockStats>, nodedb_columnar::ColumnarError> {
    let reader = SegmentReader::open(segment_data)?;
    let footer = reader.footer();

    // Timestamp is always column 0 in timeseries schemas.
    if footer.columns.is_empty() {
        return Ok(Vec::new());
    }
    Ok(footer.columns[0].block_stats.clone())
}

// ---------------------------------------------------------------------------
// Read path: shared segment → timeseries query data
// ---------------------------------------------------------------------------

/// Result of scanning a shared-format segment for timeseries data.
pub struct TsScanResult {
    /// Timestamp values (microseconds or milliseconds depending on schema).
    pub timestamps: Vec<i64>,
    /// Value column data (f64 for metrics).
    pub values: Vec<f64>,
    /// Validity masks — false for rows filtered out by predicates or block skip.
    pub ts_valid: Vec<bool>,
    pub val_valid: Vec<bool>,
}

/// Scan a shared-format segment for timeseries data with time-range filtering.
///
/// Uses block-level predicate pushdown: blocks whose timestamp range doesn't
/// overlap `[start_ms, end_ms]` are skipped entirely (not decompressed).
///
/// `ts_col_idx` is the timestamp column index (typically 0).
/// `val_col_idx` is the value column index to read.
pub fn scan_shared_segment(
    segment_data: &[u8],
    ts_col_idx: usize,
    val_col_idx: usize,
    start_ms: i64,
    end_ms: i64,
) -> Result<TsScanResult, nodedb_columnar::ColumnarError> {
    let reader = SegmentReader::open(segment_data)?;

    // Build predicates for block-level time-range skip.
    // Timestamp blocks with max < start or min > end can be skipped.
    // Use the lossless i64 constructors so that timestamps outside ±2^53
    // (which don't round-trip through f64 exactly) are compared correctly
    // against the exact min_i64/max_i64 fields written by BlockStats::integer().
    let predicates = vec![
        ScanPredicate::gte_i64(ts_col_idx, start_ms),
        ScanPredicate::lte_i64(ts_col_idx, end_ms),
    ];

    // Read timestamp column with predicate pushdown.
    let ts_decoded = reader.read_column_filtered(ts_col_idx, &predicates)?;
    let (timestamps, ts_valid) = decoded_to_i64(ts_decoded);

    // Read value column with same predicate pushdown (ensures row alignment).
    let val_decoded = reader.read_column_filtered(val_col_idx, &predicates)?;
    let (values, val_valid) = decoded_to_f64(val_decoded);

    Ok(TsScanResult {
        timestamps,
        values,
        ts_valid,
        val_valid,
    })
}

/// Scan a shared-format segment, returning only rows within a time range.
///
/// Unlike `scan_shared_segment` which does block-level filtering, this
/// additionally applies row-level filtering, returning only exact matches.
pub fn scan_shared_segment_filtered(
    segment_data: &[u8],
    ts_col_idx: usize,
    val_col_idx: usize,
    start_ms: i64,
    end_ms: i64,
) -> Result<(Vec<i64>, Vec<f64>), nodedb_columnar::ColumnarError> {
    let scan = scan_shared_segment(segment_data, ts_col_idx, val_col_idx, start_ms, end_ms)?;

    // Row-level filter: keep only rows where ts is within range AND valid.
    let mut filtered_ts = Vec::new();
    let mut filtered_vals = Vec::new();

    for i in 0..scan.timestamps.len() {
        if scan.ts_valid[i]
            && scan.val_valid[i]
            && scan.timestamps[i] >= start_ms
            && scan.timestamps[i] <= end_ms
        {
            filtered_ts.push(scan.timestamps[i]);
            filtered_vals.push(scan.values[i]);
        }
    }

    Ok((filtered_ts, filtered_vals))
}

/// Read a single column from a shared segment by index.
///
/// For timeseries queries that need an arbitrary column (e.g., tag columns
/// for GROUP BY).
pub fn read_column_from_shared(
    segment_data: &[u8],
    col_idx: usize,
) -> Result<DecodedColumn, nodedb_columnar::ColumnarError> {
    let reader = SegmentReader::open(segment_data)?;
    reader.read_column(col_idx)
}

/// Extract i64 values and validity from a DecodedColumn.
fn decoded_to_i64(col: DecodedColumn) -> (Vec<i64>, Vec<bool>) {
    match col {
        DecodedColumn::Int64 { values, valid } => (values, valid),
        DecodedColumn::Timestamp { values, valid } => (values, valid),
        DecodedColumn::Float64 { values, valid } => {
            (values.into_iter().map(|f| f as i64).collect(), valid)
        }
        _ => (Vec::new(), Vec::new()),
    }
}

/// Extract f64 values and validity from a DecodedColumn.
fn decoded_to_f64(col: DecodedColumn) -> (Vec<f64>, Vec<bool>) {
    match col {
        DecodedColumn::Float64 { values, valid } => (values, valid),
        DecodedColumn::Int64 { values, valid } => {
            (values.into_iter().map(|i| i as f64).collect(), valid)
        }
        DecodedColumn::Timestamp { values, valid } => {
            (values.into_iter().map(|i| i as f64).collect(), valid)
        }
        _ => (Vec::new(), Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{IngestResult, MetricSample};

    use super::super::columnar_memtable::{ColumnarMemtable, ColumnarMemtableConfig};
    use super::*;

    /// `ScopedMemory` bound to the timeseries engine, backed by a real
    /// governor with ample per-engine limits.
    fn test_memory() -> ScopedMemory {
        ScopedMemory::new(
            crate::data::executor::core_loop::test_governor(),
            nodedb_types::DatabaseId::DEFAULT,
            nodedb_types::TenantId::new(0),
            nodedb_mem::EngineId::Timeseries,
        )
    }

    fn default_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: crate::engine::timeseries::memtable::DEFAULT_MEMTABLE_BUDGET_BYTES,
            hard_memory_limit: 80 * 1024 * 1024,
            max_tag_cardinality: 100_000,
        }
    }

    #[test]
    fn schema_conversion() {
        let ts_schema = super::super::columnar_memtable::ColumnarSchema {
            columns: vec![
                (
                    "timestamp".into(),
                    TsColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive)),
                ),
                ("value".into(), TsColumnType::Float64),
                ("host".into(), TsColumnType::Symbol),
                (
                    "seen_at".into(),
                    TsColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc)),
                ),
                (
                    "_ts_system".into(),
                    TsColumnType::Timestamp(TimeKind::Millis),
                ),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 5],
        };

        let shared = ts_schema_to_shared(&ts_schema);
        assert_eq!(shared.columns.len(), 5);
        assert_eq!(shared.columns[0].column_type, SharedColumnType::Timestamp);
        assert_eq!(shared.columns[1].column_type, SharedColumnType::Float64);
        // Symbol → Int64 in shared format.
        assert_eq!(shared.columns[2].column_type, SharedColumnType::Int64);
        assert_eq!(shared.columns[3].column_type, SharedColumnType::Timestamptz);
        // A millisecond integer time column is the integer it was declared as.
        assert_eq!(shared.columns[4].column_type, SharedColumnType::Int64);
    }

    #[test]
    fn drain_to_shared_segment_roundtrip() {
        let mut mt = ColumnarMemtable::new_metric(default_config());

        for i in 0..100 {
            let result = mt.ingest_metric(
                i % 10,
                MetricSample {
                    timestamp_ms: 1000 + i as i64,
                    value: i as f64 * 0.5,
                },
            );
            assert_ne!(result, IngestResult::Rejected);
        }

        let drain = mt.drain();
        assert_eq!(drain.row_count, 100);

        // Write as shared segment.
        let segment =
            write_ts_drain_as_segment(&drain, None, test_memory()).expect("write segment");

        // Read back and verify.
        let reader = nodedb_columnar::reader::SegmentReader::open(&segment).expect("open");
        assert_eq!(reader.row_count(), 100);
        assert_eq!(reader.column_count(), 2); // timestamp + value

        // Verify timestamp column.
        let ts_col = reader.read_column(0).expect("read ts");
        match ts_col {
            nodedb_columnar::reader::DecodedColumn::Int64 { values, valid } => {
                assert_eq!(values.len(), 100);
                assert_eq!(values[0], 1000);
                assert_eq!(values[99], 1099);
                assert!(valid.iter().all(|&v| v));
            }
            _ => panic!("expected Int64 for timestamps"),
        }

        // Verify footer has timeseries profile tag.
        let footer = reader.footer();
        assert_eq!(
            footer.profile_tag,
            nodedb_columnar::writer::PROFILE_TIMESERIES
        );
    }

    #[test]
    fn timestamp_block_stats_extraction() {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        for i in 0..50 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 2000 + i as i64,
                    value: 1.0,
                },
            );
        }

        let drain = mt.drain();
        let segment = write_ts_drain_as_segment(&drain, None, test_memory()).expect("write");
        let stats = extract_timestamp_block_stats(&segment).expect("stats");

        assert_eq!(stats.len(), 1); // 50 rows < 1024 block size = 1 block.
        assert_eq!(stats[0].min, 2000.0);
        assert_eq!(stats[0].max, 2049.0);
        assert_eq!(stats[0].row_count, 50);
    }

    // -- Read path tests --

    fn write_test_segment(count: usize, start_ts: i64) -> Vec<u8> {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        for i in 0..count {
            mt.ingest_metric(
                (i % 5) as u64,
                MetricSample {
                    timestamp_ms: start_ts + i as i64,
                    value: i as f64 * 0.25,
                },
            );
        }
        let drain = mt.drain();
        write_ts_drain_as_segment(&drain, None, test_memory()).expect("write")
    }

    #[test]
    fn scan_full_range() {
        let segment = write_test_segment(100, 1000);

        let result = scan_shared_segment(&segment, 0, 1, 1000, 1099).expect("scan");

        assert_eq!(result.timestamps.len(), 100);
        assert_eq!(result.values.len(), 100);
        assert!(result.ts_valid.iter().all(|&v| v));
        assert_eq!(result.timestamps[0], 1000);
        assert_eq!(result.timestamps[99], 1099);
        assert!((result.values[0] - 0.0).abs() < 0.001);
        assert!((result.values[4] - 1.0).abs() < 0.001);
    }

    #[test]
    fn scan_filtered_range() {
        let segment = write_test_segment(100, 1000);

        // Only rows with ts in [1050, 1060].
        let (ts, vals) = scan_shared_segment_filtered(&segment, 0, 1, 1050, 1060).expect("scan");

        assert_eq!(ts.len(), 11); // 1050..=1060 = 11 rows.
        assert_eq!(ts[0], 1050);
        assert_eq!(ts[10], 1060);
        assert_eq!(vals.len(), 11);
    }

    #[test]
    fn scan_empty_range() {
        let segment = write_test_segment(100, 1000);

        // Range outside segment data.
        let (ts, vals) = scan_shared_segment_filtered(&segment, 0, 1, 5000, 6000).expect("scan");

        assert!(ts.is_empty());
        assert!(vals.is_empty());
    }

    #[test]
    fn scan_block_level_skip() {
        // Create a large segment with multiple blocks (>1024 rows).
        let segment = write_test_segment(3000, 0);

        // Query only the last block: ts in [2500, 2999].
        let result = scan_shared_segment(&segment, 0, 1, 2500, 2999).expect("scan");

        // Block 0 [0..1023] and block 1 [1024..2047] should be skipped.
        // Block 2 [2048..2999] should be read.
        // Skipped blocks produce null-filled rows (valid=false).
        let valid_count = result.ts_valid.iter().filter(|&&v| v).count();
        // Block 2 has 952 rows (3000-2048), all valid.
        assert_eq!(valid_count, 952);

        // Use filtered scan to get only the matching rows.
        let (ts, _) = scan_shared_segment_filtered(&segment, 0, 1, 2500, 2999).expect("filtered");
        assert_eq!(ts.len(), 500); // 2500..=2999 = 500 rows.
    }

    #[test]
    fn read_arbitrary_column() {
        let segment = write_test_segment(50, 1000);

        let col = read_column_from_shared(&segment, 1).expect("read value col");
        match col {
            DecodedColumn::Float64 { values, valid } => {
                assert_eq!(values.len(), 50);
                assert!(valid.iter().all(|&v| v));
                assert!((values[0] - 0.0).abs() < 0.001);
                assert!((values[4] - 1.0).abs() < 0.001);
            }
            _ => panic!("expected Float64 for value column"),
        }
    }

    // -- lossless i64 predicate pushdown for large timestamps -----------

    /// Write a segment whose timestamp column uses values far above 2^53.
    ///
    /// These values are not exactly representable in f64, so the old
    /// `ScanPredicate::gte(ts_col_idx, start_ms as f64)` path would round
    /// the predicate and produce incorrect block-skip decisions.
    fn write_large_ts_segment(start_ts: i64, count: usize) -> Vec<u8> {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        for i in 0..count {
            let ts = start_ts + i as i64;
            let result = mt.ingest_metric(
                (i % 3) as u64,
                MetricSample {
                    timestamp_ms: ts,
                    value: i as f64,
                },
            );
            assert_ne!(result, nodedb_types::timeseries::IngestResult::Rejected);
        }
        let drain = mt.drain();
        write_ts_drain_as_segment(&drain, None, test_memory()).expect("write large-ts segment")
    }

    /// end-to-end block skip with timestamps outside ±2^53.
    ///
    /// The segment contains 50 rows with timestamps in
    /// [LARGE_BASE, LARGE_BASE + 49]. We query with a range that is entirely
    /// above the segment's max timestamp. The block must be skipped (no valid
    /// rows returned).
    ///
    /// Without the fix (`start_ms as f64` cast), the predicate value would
    /// round to the same f64 as the block's min/max and the skip decision
    /// could be wrong.
    #[test]
    fn large_ts_block_skipped_when_range_above_segment() {
        // 2^53 + 1_000_000 — well outside the exact f64 representable range.
        const LARGE_BASE: i64 = 9_007_199_255_740_993_i64; // 2^53 + 1_000_000

        let segment = write_large_ts_segment(LARGE_BASE, 50);

        // Query a range entirely above the segment: [LARGE_BASE + 100, LARGE_BASE + 200].
        // The block contains [LARGE_BASE, LARGE_BASE + 49], so it should be skipped.
        let query_start = LARGE_BASE + 100;
        let query_end = LARGE_BASE + 200;
        let (ts, vals) =
            scan_shared_segment_filtered(&segment, 0, 1, query_start, query_end).expect("scan");

        assert!(
            ts.is_empty(),
            "expected no rows: block [{LARGE_BASE}, {}] is below query [{query_start}, {query_end}]",
            LARGE_BASE + 49
        );
        assert!(vals.is_empty());
    }

    /// end-to-end block NOT skipped when range overlaps large timestamps.
    ///
    /// The segment contains rows with timestamps in [LARGE_BASE, LARGE_BASE + 49].
    /// We query for [LARGE_BASE + 10, LARGE_BASE + 20]. The block must NOT be
    /// skipped, and the filtered scan must return exactly 11 rows.
    #[test]
    fn large_ts_block_not_skipped_when_range_overlaps_segment() {
        const LARGE_BASE: i64 = 9_007_199_255_740_993_i64;

        let segment = write_large_ts_segment(LARGE_BASE, 50);

        let query_start = LARGE_BASE + 10;
        let query_end = LARGE_BASE + 20;
        let (ts, _) =
            scan_shared_segment_filtered(&segment, 0, 1, query_start, query_end).expect("scan");

        assert_eq!(
            ts.len(),
            11,
            "expected 11 rows for ts in [{query_start}, {query_end}]"
        );
        assert_eq!(ts[0], query_start);
        assert_eq!(ts[10], query_end);
    }
}
