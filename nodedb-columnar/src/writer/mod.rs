//! Segment writer: drains a memtable into a compressed columnar segment.
//!
//! Encodes each column through the `nodedb-codec` pipeline in blocks of
//! BLOCK_SIZE rows. Computes per-block statistics for predicate pushdown.
//! Assembles the final segment: header + column blocks + footer with CRC32C.

mod block;
mod encode;
mod stats;

use nodedb_codec::{ColumnCodec, ColumnTypeHint};
use nodedb_types::columnar::{ColumnType, ColumnarSchema};

use crate::error::ColumnarError;
use crate::format::{ColumnMeta, HEADER_SIZE, SegmentFooter, SegmentHeader};
use crate::memtable::ColumnData;

use self::block::encode_column_blocks;
use self::encode::compute_schema_hash;

/// Profile tag values for the segment footer.
pub const PROFILE_PLAIN: u8 = 0;
pub const PROFILE_TIMESERIES: u8 = 1;
pub const PROFILE_SPATIAL: u8 = 2;

/// Writes a drained memtable into a complete segment byte buffer.
///
/// The segment is self-contained: header identifies the format, column
/// blocks store compressed data, and the footer enables random access to
/// any column without scanning the entire file.
pub struct SegmentWriter {
    profile_tag: u8,
}

impl SegmentWriter {
    /// Create a writer for the given profile.
    pub fn new(profile_tag: u8) -> Self {
        Self { profile_tag }
    }

    /// Create a writer for the plain (default) profile.
    pub fn plain() -> Self {
        Self::new(PROFILE_PLAIN)
    }

    /// Encode a drained memtable into a segment byte buffer.
    ///
    /// `schema` is the column schema, `columns` are the drained column data,
    /// `row_count` is the total number of rows.
    pub fn write_segment(
        &self,
        schema: &ColumnarSchema,
        columns: &[ColumnData],
        row_count: usize,
    ) -> Result<Vec<u8>, ColumnarError> {
        if row_count == 0 {
            return Err(ColumnarError::EmptyMemtable);
        }
        if columns.len() != schema.columns.len() {
            return Err(ColumnarError::SchemaMismatch {
                expected: schema.columns.len(),
                got: columns.len(),
            });
        }

        let mut buf = Vec::new();

        // 1. Write header.
        buf.extend_from_slice(&SegmentHeader::current().to_bytes());

        // 2. Encode each column's blocks.
        let mut column_metas = Vec::with_capacity(columns.len());

        for (i, (col_def, col_data)) in schema.columns.iter().zip(columns.iter()).enumerate() {
            let col_start = buf.len() as u64;

            // Select codec for this column type.
            let codec = select_codec_for_profile(&col_def.column_type, self.profile_tag);

            // Encode blocks.
            let block_stats =
                encode_column_blocks(&mut buf, col_data, &col_def.column_type, codec, row_count)?;

            let col_end = buf.len() as u64;

            // For DictEncoded columns, the codec stored in meta is DeltaFastLanesLz4 (IDs),
            // and the dictionary strings are stored in the meta for reader reconstruction.
            let (effective_codec, dictionary) = match col_data {
                ColumnData::DictEncoded { dictionary, .. } => {
                    (ColumnCodec::DeltaFastLanesLz4, Some(dictionary.clone()))
                }
                _ => (codec, None),
            };

            column_metas.push(ColumnMeta {
                name: col_def.name.clone(),
                offset: col_start - HEADER_SIZE as u64,
                length: col_end - col_start,
                codec: effective_codec,
                block_count: block_stats.len() as u32,
                block_stats,
                dictionary,
            });

            let _ = i; // Satisfy clippy about unused index.
        }

        // 3. Compute schema hash (simple hash of column names + types).
        let schema_hash = compute_schema_hash(schema);

        // 4. Write footer.
        let footer = SegmentFooter {
            schema_hash,
            column_count: schema.columns.len() as u32,
            row_count: row_count as u64,
            profile_tag: self.profile_tag,
            columns: column_metas,
        };
        let footer_bytes = footer.to_bytes()?;
        buf.extend_from_slice(&footer_bytes);

        Ok(buf)
    }
}

/// Select the best codec for a column type, with profile-aware overrides.
///
/// For timeseries profiles (tag=1), Float64 metric columns use Gorilla XOR
/// encoding when the data is monotonic/slowly-changing. For other profiles,
/// the standard auto-detection pipeline applies.
pub fn select_codec_for_profile(col_type: &ColumnType, profile_tag: u8) -> ColumnCodec {
    // Timeseries profile: prefer Gorilla for Float64 metrics.
    if profile_tag == PROFILE_TIMESERIES && matches!(col_type, ColumnType::Float64) {
        return ColumnCodec::Gorilla;
    }
    // Timeseries profile: delta-of-delta for timestamps.
    if profile_tag == PROFILE_TIMESERIES && matches!(col_type, ColumnType::Timestamp) {
        return ColumnCodec::DeltaFastLanesLz4;
    }
    select_codec(col_type)
}

/// Select the best codec for a column type using nodedb-codec's auto-detection.
fn select_codec(col_type: &ColumnType) -> ColumnCodec {
    let hint = match col_type {
        ColumnType::Int64 => ColumnTypeHint::Int64,
        ColumnType::Float64 => ColumnTypeHint::Float64,
        ColumnType::Timestamp => ColumnTypeHint::Timestamp,
        ColumnType::String | ColumnType::Geometry => ColumnTypeHint::String,
        ColumnType::Bool | ColumnType::Bytes | ColumnType::Decimal | ColumnType::Uuid => {
            return ColumnCodec::Lz4;
        }
        ColumnType::Vector(_) => {
            return ColumnCodec::Lz4; // Packed f32 — raw + LZ4.
        }
    };
    nodedb_codec::detect_codec(ColumnCodec::Auto, hint)
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;
    use crate::format::{SegmentFooter, SegmentHeader};
    use crate::memtable::ColumnarMemtable;

    fn analytics_schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("score", ColumnType::Float64),
        ])
        .expect("valid")
    }

    #[test]
    fn write_segment_roundtrip() {
        let schema = analytics_schema();
        let mut mt = ColumnarMemtable::new(&schema);

        for i in 0..100 {
            mt.append_row(&[
                Value::Integer(i),
                Value::String(format!("user_{i}")),
                if i % 3 == 0 {
                    Value::Null
                } else {
                    Value::Float(i as f64 * 0.25)
                },
            ])
            .expect("append");
        }

        let (schema, columns, row_count) = mt.drain();
        let writer = SegmentWriter::plain();
        let segment = writer
            .write_segment(&schema, &columns, row_count)
            .expect("write");

        // Verify header.
        let header = SegmentHeader::from_bytes(&segment).expect("valid header");
        assert_eq!(header.magic, *b"NDBS");
        assert_eq!(header.version_major, 1);

        // Verify footer.
        let footer = SegmentFooter::from_segment_tail(&segment).expect("valid footer");
        assert_eq!(footer.column_count, 3);
        assert_eq!(footer.row_count, 100);
        assert_eq!(footer.profile_tag, PROFILE_PLAIN);
        assert_eq!(footer.columns.len(), 3);

        // Verify column metadata.
        assert_eq!(footer.columns[0].name, "id");
        assert_eq!(footer.columns[1].name, "name");
        assert_eq!(footer.columns[2].name, "score");

        // Each column should have 1 block (100 rows < BLOCK_SIZE=1024).
        assert_eq!(footer.columns[0].block_count, 1);
        assert_eq!(footer.columns[0].block_stats[0].row_count, 100);

        // id: min=0, max=99.
        assert_eq!(footer.columns[0].block_stats[0].min, 0.0);
        assert_eq!(footer.columns[0].block_stats[0].max, 99.0);
        assert_eq!(footer.columns[0].block_stats[0].null_count, 0);

        // score: 34 nulls (every 3rd row), min=0.25 (row 1), max=99*0.25=24.75 (row 99).
        assert_eq!(footer.columns[2].block_stats[0].null_count, 34);
    }

    #[test]
    fn write_segment_multi_block() {
        let schema =
            ColumnarSchema::new(vec![ColumnDef::required("x", ColumnType::Int64)]).expect("valid");

        let mut mt = ColumnarMemtable::new(&schema);
        for i in 0..2500 {
            mt.append_row(&[Value::Integer(i)]).expect("append");
        }

        let (schema, columns, row_count) = mt.drain();
        let writer = SegmentWriter::plain();
        let segment = writer
            .write_segment(&schema, &columns, row_count)
            .expect("write");

        let footer = SegmentFooter::from_segment_tail(&segment).expect("valid footer");
        assert_eq!(footer.row_count, 2500);

        // 2500 rows / 1024 = 3 blocks (1024 + 1024 + 452).
        assert_eq!(footer.columns[0].block_count, 3);
        assert_eq!(footer.columns[0].block_stats[0].row_count, 1024);
        assert_eq!(footer.columns[0].block_stats[1].row_count, 1024);
        assert_eq!(footer.columns[0].block_stats[2].row_count, 452);

        // Block 0: min=0, max=1023.
        assert_eq!(footer.columns[0].block_stats[0].min, 0.0);
        assert_eq!(footer.columns[0].block_stats[0].max, 1023.0);
        // Block 2: min=2048, max=2499.
        assert_eq!(footer.columns[0].block_stats[2].min, 2048.0);
        assert_eq!(footer.columns[0].block_stats[2].max, 2499.0);
    }

    #[test]
    fn write_segment_empty_rejected() {
        let schema = analytics_schema();
        let mt = ColumnarMemtable::new(&schema);
        let (schema, columns, row_count) = {
            let mut m = mt;
            m.drain()
        };
        let writer = SegmentWriter::plain();
        assert!(matches!(
            writer.write_segment(&schema, &columns, row_count),
            Err(ColumnarError::EmptyMemtable)
        ));
    }

    #[test]
    fn block_stats_predicate_pushdown() {
        let schema = analytics_schema();
        let mut mt = ColumnarMemtable::new(&schema);

        for i in 0..50 {
            mt.append_row(&[
                Value::Integer(i + 100),
                Value::String(format!("item_{i}")),
                Value::Float(i as f64 + 10.0),
            ])
            .expect("append");
        }

        let (schema, columns, row_count) = mt.drain();
        let writer = SegmentWriter::plain();
        let segment = writer
            .write_segment(&schema, &columns, row_count)
            .expect("write");
        let footer = SegmentFooter::from_segment_tail(&segment).expect("valid");

        use crate::predicate::ScanPredicate;

        let id_stats = &footer.columns[0].block_stats[0];
        // id: min=100, max=149.
        assert!(ScanPredicate::gt(0, 200.0).can_skip_block(id_stats)); // WHERE id > 200 → skip.
        assert!(!ScanPredicate::gt(0, 120.0).can_skip_block(id_stats)); // WHERE id > 120 → cannot skip.
        assert!(ScanPredicate::lt(0, 50.0).can_skip_block(id_stats)); // WHERE id < 50 → skip.
        assert!(ScanPredicate::eq(0, 200.0).can_skip_block(id_stats)); // WHERE id = 200 → skip.
        assert!(!ScanPredicate::eq(0, 125.0).can_skip_block(id_stats)); // WHERE id = 125 → cannot skip.
    }

    #[test]
    fn string_block_stats_zone_map() {
        // Write a segment with known string values, then verify str_min/str_max.
        let schema = ColumnarSchema::new(vec![ColumnDef::required("tag", ColumnType::String)])
            .expect("valid");

        let mut mt = ColumnarMemtable::new(&schema);
        // Insert > 16 distinct values to trigger bloom filter construction.
        // Lexicographic order: apple < banana < cherry < date (first/last matter for zone map).
        let values: Vec<String> = (0..20).map(|i| format!("item_{i:02}")).collect();
        for name in &values {
            mt.append_row(&[Value::String(name.clone())])
                .expect("append");
        }
        // Add known boundary values for zone-map assertions.
        mt.append_row(&[Value::String("apple".into())])
            .expect("append");
        mt.append_row(&[Value::String("date".into())])
            .expect("append");

        let (schema, columns, row_count) = mt.drain();
        let writer = SegmentWriter::plain();
        let segment = writer
            .write_segment(&schema, &columns, row_count)
            .expect("write");
        let footer = SegmentFooter::from_segment_tail(&segment).expect("footer");

        let stats = &footer.columns[0].block_stats[0];
        assert!(stats.str_min.is_some(), "str_min should be populated");
        assert!(stats.str_max.is_some(), "str_max should be populated");
        // "apple" is lex smallest, "item_19" is lex largest (> "date").
        assert_eq!(stats.str_min.as_deref(), Some("apple"));
        assert_eq!(stats.str_max.as_deref(), Some("item_19"));

        // Bloom filter should be present (>16 distinct values).
        assert!(
            stats.bloom.is_some(),
            "bloom should be populated for >16 distinct values"
        );

        use crate::predicate::ScanPredicate;

        // WHERE tag = "aaa" → below "apple" → skip.
        assert!(ScanPredicate::str_eq(0, "aaa").can_skip_block(stats));
        // WHERE tag = "zzz" → above "item_19" → skip.
        assert!(ScanPredicate::str_eq(0, "zzz").can_skip_block(stats));
        // WHERE tag = "date" → in range [apple, item_19], inserted in bloom → cannot skip.
        assert!(!ScanPredicate::str_eq(0, "date").can_skip_block(stats));
        // WHERE tag > "item_19" → smax ≤ value → skip.
        assert!(ScanPredicate::str_gt(0, "item_19").can_skip_block(stats));
        // WHERE tag < "apple" → smin ≥ value → skip.
        assert!(ScanPredicate::str_lt(0, "apple").can_skip_block(stats));
    }

    #[test]
    fn string_block_stats_bloom_rejects_absent_value() {
        let schema = ColumnarSchema::new(vec![ColumnDef::required("label", ColumnType::String)])
            .expect("valid");

        let mut mt = ColumnarMemtable::new(&schema);
        // Insert > 16 distinct values to trigger bloom construction.
        let values: Vec<String> = (0..20).map(|i| format!("val_{i:02}")).collect();
        for name in &values {
            mt.append_row(&[Value::String(name.clone())])
                .expect("append");
        }
        // Add known values for bloom assertions.
        mt.append_row(&[Value::String("alpha".into())])
            .expect("append");
        mt.append_row(&[Value::String("beta".into())])
            .expect("append");
        mt.append_row(&[Value::String("gamma".into())])
            .expect("append");

        let (schema, columns, row_count) = mt.drain();
        let segment = SegmentWriter::plain()
            .write_segment(&schema, &columns, row_count)
            .expect("write");
        let footer = SegmentFooter::from_segment_tail(&segment).expect("footer");
        let stats = &footer.columns[0].block_stats[0];

        use crate::predicate::{ScanPredicate, bloom_may_contain};

        let bloom = stats
            .bloom
            .as_deref()
            .expect("bloom present for >16 distinct");
        assert!(bloom_may_contain(bloom, "alpha"));
        assert!(bloom_may_contain(bloom, "beta"));
        assert!(bloom_may_contain(bloom, "gamma"));

        // "delta" was not inserted. If bloom says absent, the predicate skips.
        let delta_absent = !bloom_may_contain(bloom, "delta");
        if delta_absent {
            // "delta" is in [alpha, val_19] range → only bloom can skip this.
            assert!(ScanPredicate::str_eq(0, "delta").can_skip_block(stats));
        }
    }
}
