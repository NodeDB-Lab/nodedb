//! Segment writer: drains a memtable into a compressed columnar segment.
//!
//! Encodes each column through the `nodedb-codec` pipeline in blocks of
//! BLOCK_SIZE rows. Computes per-block statistics for predicate pushdown.
//! Assembles the final segment: header + column blocks + footer with CRC32C.

use nodedb_codec::{ColumnCodec, ColumnTypeHint};
use nodedb_types::columnar::{ColumnType, ColumnarSchema};

use crate::error::ColumnarError;
use crate::format::{
    BLOCK_SIZE, BlockStats, ColumnMeta, HEADER_SIZE, SegmentFooter, SegmentHeader,
};
use crate::memtable::ColumnData;

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
            let codec = select_codec(&col_def.column_type);

            // Encode blocks.
            let block_stats =
                encode_column_blocks(&mut buf, col_data, &col_def.column_type, codec, row_count)?;

            let col_end = buf.len() as u64;

            column_metas.push(ColumnMeta {
                name: col_def.name.clone(),
                offset: col_start - HEADER_SIZE as u64,
                length: col_end - col_start,
                codec,
                block_count: block_stats.len() as u32,
                block_stats,
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

/// Encode all blocks for a single column, appending to `buf`.
/// Returns per-block statistics.
fn encode_column_blocks(
    buf: &mut Vec<u8>,
    col_data: &ColumnData,
    col_type: &ColumnType,
    codec: ColumnCodec,
    row_count: usize,
) -> Result<Vec<BlockStats>, ColumnarError> {
    let num_blocks = row_count.div_ceil(BLOCK_SIZE);
    let mut block_stats = Vec::with_capacity(num_blocks);

    for block_idx in 0..num_blocks {
        let start = block_idx * BLOCK_SIZE;
        let end = (start + BLOCK_SIZE).min(row_count);
        let block_row_count = end - start;

        let (compressed, stats) =
            encode_single_block(col_data, col_type, codec, start, end, block_row_count)?;

        // Write block: [compressed_len: u32 LE][compressed_data].
        let len = compressed.len() as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&compressed);

        block_stats.push(stats);
    }

    Ok(block_stats)
}

/// Encode a single block of rows for a column.
fn encode_single_block(
    col_data: &ColumnData,
    _col_type: &ColumnType,
    codec: ColumnCodec,
    start: usize,
    end: usize,
    block_row_count: usize,
) -> Result<(Vec<u8>, BlockStats), ColumnarError> {
    match col_data {
        ColumnData::Int64 { values, valid } => {
            let slice = &values[start..end];
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;

            let (min, max) = numeric_min_max_i64(slice, valid_slice);
            let stats = BlockStats {
                min: min as f64,
                max: max as f64,
                null_count,
                row_count: block_row_count as u32,
            };

            // Encode validity bitmap + values.
            let encoded = encode_i64_with_validity(slice, valid_slice, codec)?;
            Ok((encoded, stats))
        }
        ColumnData::Float64 { values, valid } => {
            let slice = &values[start..end];
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;

            let (min, max) = numeric_min_max_f64(slice, valid_slice);
            let stats = BlockStats {
                min,
                max,
                null_count,
                row_count: block_row_count as u32,
            };

            let encoded = encode_f64_with_validity(slice, valid_slice, codec)?;
            Ok((encoded, stats))
        }
        ColumnData::Timestamp { values, valid } => {
            let slice = &values[start..end];
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;

            let (min, max) = numeric_min_max_i64(slice, valid_slice);
            let stats = BlockStats {
                min: min as f64,
                max: max as f64,
                null_count,
                row_count: block_row_count as u32,
            };

            let encoded = encode_i64_with_validity(slice, valid_slice, codec)?;
            Ok((encoded, stats))
        }
        ColumnData::Bool { values, valid } => {
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;

            // Bit-pack booleans: 1 byte per 8 values.
            let bool_slice = &values[start..end];
            let mut packed = Vec::with_capacity(bool_slice.len().div_ceil(8));
            for chunk in bool_slice.chunks(8) {
                let mut byte = 0u8;
                for (j, &b) in chunk.iter().enumerate() {
                    if b {
                        byte |= 1 << j;
                    }
                }
                packed.push(byte);
            }
            let compressed = nodedb_codec::encode_bytes_pipeline(&packed, codec)?;

            let stats = BlockStats::non_numeric(null_count, block_row_count as u32);
            // Prepend validity bitmap.
            let encoded = prepend_validity(valid_slice, &compressed);
            Ok((encoded, stats))
        }
        ColumnData::String {
            data,
            offsets,
            valid,
        } => {
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;

            let byte_start = offsets[start] as usize;
            let byte_end = offsets[end] as usize;
            let string_bytes = &data[byte_start..byte_end];

            let compressed = nodedb_codec::encode_bytes_pipeline(string_bytes, codec)?;
            let stats = BlockStats::non_numeric(null_count, block_row_count as u32);

            // Encode offsets (relative to block start) as delta-encoded i64.
            let block_offsets: Vec<i64> = offsets[start..=end]
                .iter()
                .map(|&o| (o as i64) - (offsets[start] as i64))
                .collect();
            let offset_codec = ColumnCodec::DeltaFastLanesLz4;
            let compressed_offsets =
                nodedb_codec::encode_i64_pipeline(&block_offsets, offset_codec)?;

            // Block layout: [validity][offset_len: u32][offsets][data].
            let mut encoded = encode_validity_bitmap(valid_slice);
            encoded.extend_from_slice(&(compressed_offsets.len() as u32).to_le_bytes());
            encoded.extend_from_slice(&compressed_offsets);
            encoded.extend_from_slice(&compressed);
            Ok((encoded, stats))
        }
        ColumnData::Bytes {
            data,
            offsets,
            valid,
        }
        | ColumnData::Geometry {
            data,
            offsets,
            valid,
        } => {
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;

            let byte_start = offsets[start] as usize;
            let byte_end = offsets[end] as usize;
            let raw_bytes = &data[byte_start..byte_end];

            let compressed = nodedb_codec::encode_bytes_pipeline(raw_bytes, codec)?;
            let stats = BlockStats::non_numeric(null_count, block_row_count as u32);

            let block_offsets: Vec<i64> = offsets[start..=end]
                .iter()
                .map(|&o| (o as i64) - (offsets[start] as i64))
                .collect();
            let compressed_offsets =
                nodedb_codec::encode_i64_pipeline(&block_offsets, ColumnCodec::DeltaFastLanesLz4)?;

            let mut encoded = encode_validity_bitmap(valid_slice);
            encoded.extend_from_slice(&(compressed_offsets.len() as u32).to_le_bytes());
            encoded.extend_from_slice(&compressed_offsets);
            encoded.extend_from_slice(&compressed);
            Ok((encoded, stats))
        }
        ColumnData::Decimal { values, valid } | ColumnData::Uuid { values, valid } => {
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;

            // 16-byte fixed-size values → raw + LZ4.
            let slice = &values[start..end];
            let mut raw = Vec::with_capacity(slice.len() * 16);
            for v in slice {
                raw.extend_from_slice(v);
            }
            let compressed = nodedb_codec::encode_bytes_pipeline(&raw, codec)?;
            let stats = BlockStats::non_numeric(null_count, block_row_count as u32);
            let encoded = prepend_validity(valid_slice, &compressed);
            Ok((encoded, stats))
        }
        ColumnData::Vector { data, dim, valid } => {
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;
            let d = *dim as usize;

            // Packed f32 → raw bytes + LZ4.
            let float_start = start * d;
            let float_end = end * d;
            let float_slice = &data[float_start..float_end];
            let mut raw = Vec::with_capacity(float_slice.len() * 4);
            for f in float_slice {
                raw.extend_from_slice(&f.to_le_bytes());
            }
            let compressed = nodedb_codec::encode_bytes_pipeline(&raw, codec)?;
            let stats = BlockStats::non_numeric(null_count, block_row_count as u32);
            let encoded = prepend_validity(valid_slice, &compressed);
            Ok((encoded, stats))
        }
    }
}

/// Encode i64 values with a prepended validity bitmap.
fn encode_i64_with_validity(
    values: &[i64],
    valid: &[bool],
    codec: ColumnCodec,
) -> Result<Vec<u8>, ColumnarError> {
    let compressed = nodedb_codec::encode_i64_pipeline(values, codec)?;
    Ok(prepend_validity(valid, &compressed))
}

/// Encode f64 values with a prepended validity bitmap.
fn encode_f64_with_validity(
    values: &[f64],
    valid: &[bool],
    codec: ColumnCodec,
) -> Result<Vec<u8>, ColumnarError> {
    let compressed = nodedb_codec::encode_f64_pipeline(values, codec)?;
    Ok(prepend_validity(valid, &compressed))
}

/// Compute min/max for i64 values, skipping nulls.
fn numeric_min_max_i64(values: &[i64], valid: &[bool]) -> (i64, i64) {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for (v, &is_valid) in values.iter().zip(valid.iter()) {
        if is_valid {
            min = min.min(*v);
            max = max.max(*v);
        }
    }
    if min == i64::MAX {
        (0, 0) // All nulls.
    } else {
        (min, max)
    }
}

/// Compute min/max for f64 values, skipping nulls.
fn numeric_min_max_f64(values: &[f64], valid: &[bool]) -> (f64, f64) {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for (v, &is_valid) in values.iter().zip(valid.iter()) {
        if is_valid {
            if *v < min {
                min = *v;
            }
            if *v > max {
                max = *v;
            }
        }
    }
    if min == f64::INFINITY {
        (0.0, 0.0)
    } else {
        (min, max)
    }
}

/// Encode a validity bitmap: ceil(N/8) bytes, bit=0 means null, bit=1 means valid.
fn encode_validity_bitmap(valid: &[bool]) -> Vec<u8> {
    let byte_count = valid.len().div_ceil(8);
    let mut bitmap = vec![0u8; byte_count];
    for (i, &v) in valid.iter().enumerate() {
        if v {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    bitmap
}

/// Prepend a validity bitmap to compressed data.
fn prepend_validity(valid: &[bool], compressed: &[u8]) -> Vec<u8> {
    let bitmap = encode_validity_bitmap(valid);
    let mut result = Vec::with_capacity(bitmap.len() + compressed.len());
    result.extend_from_slice(&bitmap);
    result.extend_from_slice(compressed);
    result
}

/// Compute a simple schema hash for the footer.
fn compute_schema_hash(schema: &ColumnarSchema) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for col in &schema.columns {
        col.name.hash(&mut hasher);
        format!("{:?}", col.column_type).hash(&mut hasher);
    }
    hasher.finish()
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
}
