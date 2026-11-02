// SPDX-License-Identifier: BUSL-1.1

//! Per-column encoding and decoding with codec selection.

use nodedb_codec::{ColumnCodec, ColumnStatistics, ColumnTypeHint, ResolvedColumnCodec};

use super::super::columnar_memtable::{ColumnData, ColumnType, NULL_SYMBOL_ID};
use super::error::SegmentError;

/// Encode a single column using the appropriate codec pipeline.
///
/// Resolves `Auto` to a concrete codec via data-aware detection before
/// encoding. The returned codec is always a `ResolvedColumnCodec` — callers
/// can store it directly in partition metadata without a further resolve step.
pub(super) fn encode_column(
    col_data: &ColumnData,
    col_type: ColumnType,
    requested_codec: ColumnCodec,
) -> Result<(Vec<u8>, ResolvedColumnCodec, ColumnStatistics), SegmentError> {
    match col_type {
        ColumnType::Timestamp(_) => {
            let values = col_data.as_timestamps();
            let codec = if requested_codec == ColumnCodec::Auto {
                nodedb_codec::detect::detect_i64_codec(values)
            } else {
                requested_codec
            }
            .try_resolve()
            .map_err(|e| SegmentError::Io(format!("codec resolve ts: {e}")))?;
            let encoded = nodedb_codec::encode_i64_pipeline(values, codec.into_column_codec())
                .map_err(|e| SegmentError::Io(format!("encode ts: {e}")))?;
            let stats = ColumnStatistics::from_i64(values, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
        ColumnType::Float64 => {
            let values = col_data.as_f64();
            let codec = if requested_codec == ColumnCodec::Auto {
                nodedb_codec::detect::detect_f64_codec(values)
            } else {
                requested_codec
            }
            .try_resolve()
            .map_err(|e| SegmentError::Io(format!("codec resolve f64: {e}")))?;
            let encoded = nodedb_codec::encode_f64_pipeline(values, codec.into_column_codec())
                .map_err(|e| SegmentError::Io(format!("encode f64: {e}")))?;
            let stats = ColumnStatistics::from_f64(values, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
        ColumnType::Int64 => {
            let values = col_data.as_i64();
            let codec = if requested_codec == ColumnCodec::Auto {
                nodedb_codec::detect::detect_i64_codec(values)
            } else {
                requested_codec
            }
            .try_resolve()
            .map_err(|e| SegmentError::Io(format!("codec resolve i64: {e}")))?;
            let encoded = nodedb_codec::encode_i64_pipeline(values, codec.into_column_codec())
                .map_err(|e| SegmentError::Io(format!("encode i64: {e}")))?;
            let stats = ColumnStatistics::from_i64(values, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
        // A text column stores dictionary ids exactly as a symbol column does.
        ColumnType::Symbol | ColumnType::Text => {
            let values = col_data.as_symbols();
            let codec = if requested_codec == ColumnCodec::Auto {
                nodedb_codec::detect_codec(requested_codec, ColumnTypeHint::Symbol)
            } else {
                requested_codec
            }
            .try_resolve()
            .map_err(|e| SegmentError::Io(format!("codec resolve sym: {e}")))?;
            let raw: Vec<u8> = values.iter().flat_map(|id| id.to_le_bytes()).collect();
            let encoded = nodedb_codec::encode_bytes_pipeline(&raw, codec.into_column_codec())
                .map_err(|e| SegmentError::Io(format!("encode sym: {e}")))?;
            // A NULL cell stores the NULL symbol id, which names no
            // dictionary entry and does not count toward the cardinality.
            let cardinality = values
                .iter()
                .copied()
                .filter(|id| *id != NULL_SYMBOL_ID)
                .max()
                .map_or(0, |m| m + 1);
            let stats =
                ColumnStatistics::from_symbols(values, cardinality, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
    }
}

/// Decode a column file using the codec pipeline.
pub(super) fn decode_column(
    data: &[u8],
    col_type: ColumnType,
    codec: ResolvedColumnCodec,
) -> Result<ColumnData, SegmentError> {
    let map_err = |e: nodedb_codec::CodecError| SegmentError::Corrupt(format!("{codec}: {e}"));

    match col_type {
        ColumnType::Timestamp(_) => {
            let values = nodedb_codec::decode_i64_pipeline(data, codec.into_column_codec())
                .map_err(map_err)?;
            Ok(ColumnData::Timestamp(values))
        }
        ColumnType::Float64 => {
            let values = nodedb_codec::decode_f64_pipeline(data, codec.into_column_codec())
                .map_err(map_err)?;
            Ok(ColumnData::Float64(values))
        }
        ColumnType::Int64 => {
            let values = nodedb_codec::decode_i64_pipeline(data, codec.into_column_codec())
                .map_err(map_err)?;
            Ok(ColumnData::Int64(values))
        }
        ColumnType::Symbol | ColumnType::Text => {
            let raw = nodedb_codec::decode_bytes_pipeline(data, codec.into_column_codec())
                .map_err(map_err)?;
            let ids: Vec<u32> = raw
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| u32::from_le_bytes(*c))
                .collect();
            Ok(ColumnData::Symbol(ids))
        }
    }
}
