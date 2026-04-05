//! Column block encoding: `encode_column_blocks` and the per-block dispatch logic.

use nodedb_codec::ColumnCodec;
use nodedb_types::columnar::ColumnType;

use crate::error::ColumnarError;
use crate::format::{BLOCK_SIZE, BlockStats};
use crate::memtable::ColumnData;

use super::encode::{
    encode_f64_with_validity, encode_i64_with_validity, encode_validity_bitmap, prepend_validity,
};
use super::stats::{compute_string_block_stats, numeric_min_max_f64, numeric_min_max_i64};

/// Encode all blocks for a single column, appending to `buf`.
/// Returns per-block statistics.
pub(super) fn encode_column_blocks(
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
            let stats =
                BlockStats::numeric(min as f64, max as f64, null_count, block_row_count as u32);

            // Encode validity bitmap + values.
            let encoded = encode_i64_with_validity(slice, valid_slice, codec)?;
            Ok((encoded, stats))
        }
        ColumnData::Float64 { values, valid } => {
            let slice = &values[start..end];
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;

            let (min, max) = numeric_min_max_f64(slice, valid_slice);
            let stats = BlockStats::numeric(min, max, null_count, block_row_count as u32);

            let encoded = encode_f64_with_validity(slice, valid_slice, codec)?;
            Ok((encoded, stats))
        }
        ColumnData::Timestamp { values, valid } => {
            let slice = &values[start..end];
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;

            let (min, max) = numeric_min_max_i64(slice, valid_slice);
            let stats =
                BlockStats::numeric(min as f64, max as f64, null_count, block_row_count as u32);

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

            // Compute lexicographic min/max and bloom filter over non-null values.
            let stats = compute_string_block_stats(
                data,
                offsets,
                valid_slice,
                start,
                end,
                null_count,
                block_row_count,
            );

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
        ColumnData::DictEncoded { ids, valid, .. } => {
            let id_slice = &ids[start..end];
            let valid_slice = &valid[start..end];
            let null_count = valid_slice.iter().filter(|&&v| !v).count() as u32;

            // Cast u32 IDs to i64 for the delta+bitpack codec.
            let id_i64: Vec<i64> = id_slice.iter().map(|&id| id as i64).collect();
            let (min_id, max_id) = {
                let mut min = i64::MAX;
                let mut max = i64::MIN;
                for (&id, &v) in id_i64.iter().zip(valid_slice.iter()) {
                    if v {
                        min = min.min(id);
                        max = max.max(id);
                    }
                }
                if min == i64::MAX { (0, 0) } else { (min, max) }
            };
            let stats = BlockStats::numeric(
                min_id as f64,
                max_id as f64,
                null_count,
                block_row_count as u32,
            );
            // Encode IDs as integers: DeltaFastLanesLz4 compresses well for low-cardinality IDs.
            let encoded =
                encode_i64_with_validity(&id_i64, valid_slice, ColumnCodec::DeltaFastLanesLz4)?;
            Ok((encoded, stats))
        }
    }
}
