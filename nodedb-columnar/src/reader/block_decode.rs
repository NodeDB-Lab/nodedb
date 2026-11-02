// SPDX-License-Identifier: Apache-2.0

//! Block-level decode helpers: null-fill and compressed-block decoding by the
//! column's recorded [`BlockLayout`].

use nodedb_codec::{ColumnCodec, ResolvedColumnCodec};

use crate::error::ColumnarError;
use crate::format::BlockLayout;

use super::types::DecodedColumn;

/// Create an empty DecodedColumn for the given layout.
pub(super) fn empty_decoded(layout: BlockLayout) -> DecodedColumn {
    match layout {
        BlockLayout::Int64 => DecodedColumn::Int64 {
            values: Vec::new(),
            valid: Vec::new(),
        },
        BlockLayout::Float64 => DecodedColumn::Float64 {
            values: Vec::new(),
            valid: Vec::new(),
        },
        BlockLayout::PackedBool => DecodedColumn::Bool {
            values: Vec::new(),
            valid: Vec::new(),
        },
        BlockLayout::VarLen | BlockLayout::FixedWidth => DecodedColumn::Binary {
            data: Vec::new(),
            offsets: Vec::new(),
            valid: Vec::new(),
        },
        BlockLayout::DictIds => DecodedColumn::DictEncoded {
            ids: Vec::new(),
            dictionary: Vec::new(), // Populated during decode_block.
            valid: Vec::new(),
        },
    }
}

/// Append null-fill rows for a skipped block.
pub(super) fn append_null_fill(result: &mut DecodedColumn, row_count: usize) {
    match result {
        DecodedColumn::Int64 { values, valid } => {
            values.extend(std::iter::repeat_n(0i64, row_count));
            valid.extend(std::iter::repeat_n(false, row_count));
        }
        DecodedColumn::Float64 { values, valid } => {
            values.extend(std::iter::repeat_n(0.0f64, row_count));
            valid.extend(std::iter::repeat_n(false, row_count));
        }
        DecodedColumn::Timestamp { values, valid } => {
            values.extend(std::iter::repeat_n(0i64, row_count));
            valid.extend(std::iter::repeat_n(false, row_count));
        }
        DecodedColumn::Bool { values, valid } => {
            values.extend(std::iter::repeat_n(false, row_count));
            valid.extend(std::iter::repeat_n(false, row_count));
        }
        DecodedColumn::Binary {
            data: _,
            offsets,
            valid,
        } => {
            let last = *offsets.last().unwrap_or(&0);
            // For null-fill: each null row has zero-length data.
            // Need row_count + 1 offsets if this is the first block, else row_count.
            if offsets.is_empty() {
                offsets.push(last); // Initial sentinel for first block.
            }
            offsets.extend(std::iter::repeat_n(last, row_count));
            valid.extend(std::iter::repeat_n(false, row_count));
        }
        DecodedColumn::DictEncoded { ids, valid, .. } => {
            ids.extend(std::iter::repeat_n(0u32, row_count));
            valid.extend(std::iter::repeat_n(false, row_count));
        }
    }
}

/// Get the current length of the validity vector in a DecodedColumn.
pub(super) fn result_valid_len(result: &DecodedColumn) -> usize {
    match result {
        DecodedColumn::Int64 { valid, .. }
        | DecodedColumn::Float64 { valid, .. }
        | DecodedColumn::Timestamp { valid, .. }
        | DecodedColumn::Bool { valid, .. }
        | DecodedColumn::Binary { valid, .. }
        | DecodedColumn::DictEncoded { valid, .. } => valid.len(),
    }
}

/// Get a mutable slice of the validity vector starting from `offset`.
pub(super) fn result_valid_slice_mut(result: &mut DecodedColumn, offset: usize) -> &mut [bool] {
    match result {
        DecodedColumn::Int64 { valid, .. }
        | DecodedColumn::Float64 { valid, .. }
        | DecodedColumn::Timestamp { valid, .. }
        | DecodedColumn::Bool { valid, .. }
        | DecodedColumn::Binary { valid, .. }
        | DecodedColumn::DictEncoded { valid, .. } => &mut valid[offset..],
    }
}

#[cfg(test)]
thread_local! {
    /// Blocks `decode_block` decoded on this thread, for tests that prove a
    /// skipped block is never decoded.
    static DECODED_BLOCKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Blocks `decode_block` decoded on this thread so far.
#[cfg(test)]
pub(crate) fn decoded_block_count() -> usize {
    DECODED_BLOCKS.with(std::cell::Cell::get)
}

/// Decode a single block and append results to the DecodedColumn.
pub(super) fn decode_block(
    result: &mut DecodedColumn,
    block_data: &[u8],
    layout: BlockLayout,
    codec: ResolvedColumnCodec,
    row_count: usize,
    dictionary: Option<&[String]>,
) -> Result<(), ColumnarError> {
    #[cfg(test)]
    DECODED_BLOCKS.with(|count| count.set(count.get() + 1));
    let bitmap_size = row_count.div_ceil(8);

    if block_data.len() < bitmap_size {
        return Err(ColumnarError::TruncatedSegment {
            expected: bitmap_size,
            got: block_data.len(),
        });
    }

    let bitmap = &block_data[..bitmap_size];
    let payload = &block_data[bitmap_size..];

    // Extract validity from bitmap.
    let valid: Vec<bool> = (0..row_count)
        .map(|i| bitmap[i / 8] & (1 << (i % 8)) != 0)
        .collect();

    match layout {
        BlockLayout::Int64 => {
            let DecodedColumn::Int64 { values, valid: v } = result else {
                append_null_fill(result, row_count);
                return Ok(());
            };
            let decoded = nodedb_codec::decode_i64_pipeline(payload, codec.into_column_codec())?;
            values.extend_from_slice(&decoded[..row_count.min(decoded.len())]);
            while values.len() < v.len() + row_count {
                values.push(0);
            }
            v.extend_from_slice(&valid);
        }
        BlockLayout::Float64 => {
            let DecodedColumn::Float64 { values, valid: v } = result else {
                append_null_fill(result, row_count);
                return Ok(());
            };
            let decoded = nodedb_codec::decode_f64_pipeline(payload, codec.into_column_codec())?;
            values.extend_from_slice(&decoded[..row_count.min(decoded.len())]);
            while values.len() < v.len() + row_count {
                values.push(0.0);
            }
            v.extend_from_slice(&valid);
        }
        BlockLayout::PackedBool => {
            let DecodedColumn::Bool { values, valid: v } = result else {
                append_null_fill(result, row_count);
                return Ok(());
            };
            let packed = nodedb_codec::decode_bytes_pipeline(payload, codec.into_column_codec())?;
            if packed.len() != bitmap_size {
                return Err(block_corruption(format!(
                    "packed bool block holds {} bytes for {row_count} rows",
                    packed.len()
                )));
            }
            values.extend((0..row_count).map(|i| packed[i / 8] & (1 << (i % 8)) != 0));
            v.extend_from_slice(&valid);
        }
        BlockLayout::VarLen => {
            let DecodedColumn::Binary {
                data,
                offsets,
                valid: v,
            } = result
            else {
                append_null_fill(result, row_count);
                return Ok(());
            };
            // Variable-length layout: [offset_len: u32][compressed_offsets][compressed_data].
            if payload.len() < 4 {
                return Err(ColumnarError::TruncatedSegment {
                    expected: bitmap_size.checked_add(4).ok_or_else(|| {
                        ColumnarError::Corruption {
                            segment_id: None,
                            reason: "variable-length bitmap range overflow".into(),
                            offset: None,
                        }
                    })?,
                    got: block_data.len(),
                });
            }
            let offset_len = usize::try_from(u32::from_le_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ]))
            .map_err(|_| ColumnarError::Corruption {
                segment_id: None,
                reason: "variable-length offset table length does not fit usize".into(),
                offset: None,
            })?;
            let offset_end =
                4usize
                    .checked_add(offset_len)
                    .ok_or_else(|| ColumnarError::Corruption {
                        segment_id: None,
                        reason: "variable-length offset table range overflow".into(),
                        offset: None,
                    })?;
            let offset_data =
                payload
                    .get(4..offset_end)
                    .ok_or(ColumnarError::TruncatedSegment {
                        expected: bitmap_size.checked_add(offset_end).ok_or_else(|| {
                            ColumnarError::Corruption {
                                segment_id: None,
                                reason: "variable-length block length overflow".into(),
                                offset: None,
                            }
                        })?,
                        got: block_data.len(),
                    })?;
            let string_data =
                payload
                    .get(offset_end..)
                    .ok_or_else(|| ColumnarError::Corruption {
                        segment_id: None,
                        reason: "variable-length data range is invalid".into(),
                        offset: None,
                    })?;

            let decoded_offsets =
                nodedb_codec::decode_i64_pipeline(offset_data, ColumnCodec::DeltaFastLanesLz4)?;
            let decoded_bytes =
                nodedb_codec::decode_bytes_pipeline(string_data, codec.into_column_codec())?;

            // The block's offsets are relative to its first byte: row_count + 1
            // entries, the first 0, the last the block's byte length.
            if decoded_offsets.len() != row_count + 1
                || decoded_offsets.first() != Some(&0)
                || decoded_offsets.windows(2).any(|w| w[0] > w[1])
                || decoded_offsets
                    .last()
                    .is_some_and(|&end| usize::try_from(end) != Ok(decoded_bytes.len()))
            {
                return Err(block_corruption(format!(
                    "variable-length offset table of {} entries does not span {} bytes \
                     for {row_count} rows",
                    decoded_offsets.len(),
                    decoded_bytes.len()
                )));
            }
            append_block_offsets(data, offsets, &decoded_offsets[1..])?;
            data.extend_from_slice(&decoded_bytes);
            v.extend_from_slice(&valid);
        }
        BlockLayout::FixedWidth => {
            let DecodedColumn::Binary {
                data,
                offsets,
                valid: v,
            } = result
            else {
                append_null_fill(result, row_count);
                return Ok(());
            };
            let decoded_bytes =
                nodedb_codec::decode_bytes_pipeline(payload, codec.into_column_codec())?;
            // Every row holds a full cell, so the block divides evenly.
            let width = decoded_bytes.len().checked_div(row_count).unwrap_or(0);
            if width * row_count != decoded_bytes.len() {
                return Err(block_corruption(format!(
                    "fixed-width block of {} bytes does not divide into {row_count} rows",
                    decoded_bytes.len()
                )));
            }
            let ends: Vec<i64> = (1..=row_count)
                .map(|i| i64::try_from(i * width))
                .collect::<Result<_, _>>()
                .map_err(|_| block_corruption("fixed-width cell end exceeds i64".into()))?;
            append_block_offsets(data, offsets, &ends)?;
            data.extend_from_slice(&decoded_bytes);
            v.extend_from_slice(&valid);
        }
        BlockLayout::DictIds => {
            let DecodedColumn::DictEncoded {
                ids,
                dictionary: col_dict,
                valid: v,
            } = result
            else {
                append_null_fill(result, row_count);
                return Ok(());
            };

            // IDs are stored as i64 via DeltaFastLanesLz4.
            let decoded =
                nodedb_codec::decode_i64_pipeline(payload, ColumnCodec::DeltaFastLanesLz4)?;
            let id_slice = &decoded[..row_count.min(decoded.len())];
            ids.extend(id_slice.iter().map(|&id| id as u32));
            // Pad to row_count if decoded is shorter.
            while ids.len() < v.len() + row_count {
                ids.push(0);
            }
            v.extend_from_slice(&valid);

            // Populate the dictionary on the first block that provides it.
            if col_dict.is_empty()
                && let Some(dict) = dictionary
            {
                col_dict.extend_from_slice(dict);
            }
        }
    }

    Ok(())
}

/// Append one block's row end offsets, rebased onto the end of `data`.
///
/// `ends` holds each row's end relative to the block's first byte. The start
/// sentinel is pushed once, before the first row of the column, so `offsets`
/// always holds one more entry than the rows decoded so far.
fn append_block_offsets(
    data: &[u8],
    offsets: &mut Vec<u32>,
    ends: &[i64],
) -> Result<(), ColumnarError> {
    let base = data.len();
    if offsets.is_empty() {
        offsets.push(absolute_offset(base, 0)?);
    }
    for &end in ends {
        let relative = usize::try_from(end)
            .map_err(|_| block_corruption(format!("negative row end offset {end}")))?;
        offsets.push(absolute_offset(base, relative)?);
    }
    Ok(())
}

/// `base + relative` as a `u32` column offset.
fn absolute_offset(base: usize, relative: usize) -> Result<u32, ColumnarError> {
    base.checked_add(relative)
        .and_then(|offset| u32::try_from(offset).ok())
        .ok_or_else(|| block_corruption(format!("column offset {base} + {relative} exceeds u32")))
}

fn block_corruption(reason: String) -> ColumnarError {
    ColumnarError::Corruption {
        segment_id: None,
        reason,
        offset: None,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::super::segment_reader::SegmentReader;
    use super::super::types::DecodedColumn;
    use crate::memtable::ColumnarMemtable;
    use crate::predicate::ScanPredicate;
    use crate::test_support::test_memory;
    use crate::writer::{PROFILE_PLAIN, SegmentWriter};

    #[test]
    fn varlen_offset_table_range_must_fit_payload() {
        let mut result = DecodedColumn::Binary {
            data: Vec::new(),
            offsets: Vec::new(),
            valid: Vec::new(),
        };
        // One validity byte, then an offset-table length larger than the
        // remaining payload. The decoder must reject it before slicing.
        let block = [0x01, 0xff, 0xff, 0xff, 0x7f];
        assert!(matches!(
            super::decode_block(
                &mut result,
                &block,
                crate::format::BlockLayout::VarLen,
                nodedb_codec::ResolvedColumnCodec::FsstLz4,
                1,
                None,
            ),
            Err(crate::error::ColumnarError::TruncatedSegment { .. })
        ));
    }

    fn write_test_segment(rows: usize) -> Vec<u8> {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("score", ColumnType::Float64),
        ])
        .expect("valid");

        let mut mt = ColumnarMemtable::new(&schema);
        for i in 0..rows {
            mt.append_row(&[
                Value::Integer(i as i64),
                Value::String(format!("user_{i}")),
                if i % 5 == 0 {
                    Value::Null
                } else {
                    Value::Float(i as f64 * 0.5)
                },
            ])
            .expect("append");
        }

        let (schema, columns, row_count) = mt.drain();
        SegmentWriter::new(PROFILE_PLAIN, test_memory())
            .write_segment(&schema, &columns, row_count, None)
            .expect("write")
    }

    // ── ResolvedColumnCodec reader tests ──────────────────────────────────────

    /// The `ResolvedColumnCodec` type statically excludes `Auto` (discriminant 0).
    ///
    /// This test verifies the compile-time guarantee by confirming:
    /// 1. Serializing any `ResolvedColumnCodec` value never produces byte 0 (Auto discriminant).
    /// 2. The msgpack deserialization of a byte-0 value as `ResolvedColumnCodec` fails.
    ///
    /// Together these ensure that `Auto` can never appear in a segment footer,
    /// because `ResolvedColumnCodec` cannot represent it.
    #[test]
    fn resolved_codec_never_serializes_as_auto_discriminant() {
        use nodedb_codec::ResolvedColumnCodec;

        let all_concrete = [
            ResolvedColumnCodec::AlpFastLanesLz4,
            ResolvedColumnCodec::AlpRdLz4,
            ResolvedColumnCodec::PcodecLz4,
            ResolvedColumnCodec::DeltaFastLanesLz4,
            ResolvedColumnCodec::FastLanesLz4,
            ResolvedColumnCodec::FsstLz4,
            ResolvedColumnCodec::AlpFastLanesRans,
            ResolvedColumnCodec::DeltaFastLanesRans,
            ResolvedColumnCodec::FsstRans,
            ResolvedColumnCodec::Gorilla,
            ResolvedColumnCodec::DoubleDelta,
            ResolvedColumnCodec::Delta,
            ResolvedColumnCodec::Lz4,
            ResolvedColumnCodec::Zstd,
            ResolvedColumnCodec::Raw,
        ];

        for codec in all_concrete {
            let bytes = zerompk::to_msgpack_vec(&codec).expect("serialize");
            // The msgpack c_enum for small integers uses a fixint byte.
            // Byte 0 is the Auto discriminant — none of the resolved variants
            // should serialize to it.
            assert!(
                !bytes.contains(&0u8) || bytes.len() > 1,
                "codec {codec} serialized to bytes containing 0: {bytes:?}"
            );
            // More precisely: the last byte (discriminant byte) must not be 0.
            let disc = *bytes.last().unwrap();
            assert_ne!(disc, 0u8, "codec {codec} has discriminant byte 0 (Auto)");
        }

        // Attempt to deserialize byte 0 as ResolvedColumnCodec — must fail.
        // Encode byte 0 as a msgpack fixint (0x00 = msgpack positive fixint 0).
        let auto_byte: &[u8] = &[0x00];
        let result: Result<ResolvedColumnCodec, _> = zerompk::from_msgpack(auto_byte);
        assert!(
            result.is_err(),
            "deserializing byte 0 as ResolvedColumnCodec must fail (Auto is not a valid variant)"
        );
    }

    #[test]
    fn read_int64_column() {
        let segment = write_test_segment(100);
        let reader = SegmentReader::open(&segment).expect("open");

        assert_eq!(reader.row_count(), 100);
        assert_eq!(reader.column_count(), 3);

        let col = reader.read_column(0).expect("read id column");
        match col {
            DecodedColumn::Int64 { values, valid } => {
                assert_eq!(values.len(), 100);
                assert_eq!(valid.len(), 100);
                assert_eq!(values[0], 0);
                assert_eq!(values[99], 99);
                assert!(valid.iter().all(|&v| v)); // No nulls in id.
            }
            _ => panic!("expected Int64"),
        }
    }

    #[test]
    fn read_string_column() {
        let segment = write_test_segment(50);
        let reader = SegmentReader::open(&segment).expect("open");

        let col = reader.read_column(1).expect("read name column");
        match col {
            DecodedColumn::Binary {
                data,
                offsets,
                valid,
            } => {
                assert_eq!(valid.len(), 50);
                assert!(valid.iter().all(|&v| v));
                // Check first row.
                let start = offsets[0] as usize;
                let end = offsets[1] as usize;
                let first = std::str::from_utf8(&data[start..end]).expect("utf8");
                assert_eq!(first, "user_0");
                // Check last row.
                let start = offsets[49] as usize;
                let end = offsets[50] as usize;
                let last = std::str::from_utf8(&data[start..end]).expect("utf8");
                assert_eq!(last, "user_49");
            }
            _ => panic!("expected Binary (string)"),
        }
    }

    #[test]
    fn read_float64_with_nulls() {
        let segment = write_test_segment(100);
        let reader = SegmentReader::open(&segment).expect("open");

        let col = reader.read_column(2).expect("read score column");
        // Score column uses AlpFastLanesLz4 → decoded as Float64.
        let (values, valid) = match &col {
            DecodedColumn::Float64 { values, valid } => (values.as_slice(), valid.as_slice()),
            other => panic!("expected Float64, got {other:?}"),
        };

        // Float64 column: every 5th row is null (rows 0,5,10,...,95 = 20 nulls).
        assert_eq!(valid.len(), 100);
        let null_count = valid.iter().filter(|&&v| !v).count();
        assert_eq!(null_count, 20);

        // Row 1: score = 1 * 0.5 = 0.5
        assert!(valid[1]);
        assert!((values[1] - 0.5).abs() < 0.001);
    }

    #[test]
    fn predicate_pushdown_skips_blocks() {
        // Create a segment with multiple blocks (> 1024 rows).
        let segment = write_test_segment(2500);
        let reader = SegmentReader::open(&segment).expect("open");

        // id column has 3 blocks: [0..1023], [1024..2047], [2048..2499].
        let footer = reader.footer();
        assert_eq!(footer.columns[0].block_count, 3);

        // Predicate: id > 2100 → should skip blocks 0 and 1.
        let pred = ScanPredicate::gt(0, 2100.0);
        let col = reader
            .read_column_filtered(0, &[pred])
            .expect("filtered read");

        match col {
            DecodedColumn::Int64 { values, valid } => {
                assert_eq!(values.len(), 2500);
                // Blocks 0 and 1 should be null-filled (skipped).
                assert!(!valid[0]); // Block 0 row 0: skipped.
                assert!(!valid[1023]); // Block 0 last row: skipped.
                assert!(!valid[1024]); // Block 1 first row: skipped.
                assert!(!valid[2047]); // Block 1 last row: skipped.
                // Block 2 should be present.
                assert!(valid[2048]); // Block 2 first row: present.
                assert_eq!(values[2048], 2048);
                assert!(valid[2499]);
                assert_eq!(values[2499], 2499);
            }
            _ => panic!("expected Int64"),
        }
    }

    #[test]
    fn read_multiple_columns() {
        let segment = write_test_segment(50);
        let reader = SegmentReader::open(&segment).expect("open");

        let cols = reader.read_columns(&[0, 2], &[]).expect("read multi");
        assert_eq!(cols.len(), 2);

        // Column 0 (id): Int64.
        match &cols[0] {
            DecodedColumn::Int64 { values, .. } => {
                assert_eq!(values.len(), 50);
            }
            _ => panic!("expected Int64 for id"),
        }
    }

    #[test]
    fn column_out_of_range() {
        let segment = write_test_segment(10);
        let reader = SegmentReader::open(&segment).expect("open");
        assert!(matches!(
            reader.read_column(99),
            Err(crate::error::ColumnarError::ColumnOutOfRange { index: 99, .. })
        ));
    }

    #[test]
    fn write_read_roundtrip_multi_block() {
        let segment = write_test_segment(3000);
        let reader = SegmentReader::open(&segment).expect("open");

        let col = reader.read_column(0).expect("read id");
        match col {
            DecodedColumn::Int64 { values, valid } => {
                assert_eq!(values.len(), 3000);
                for i in 0..3000 {
                    assert!(valid[i], "row {i} should be valid");
                    assert_eq!(values[i], i as i64, "row {i} value mismatch");
                }
            }
            _ => panic!("expected Int64"),
        }
    }

    #[test]
    fn string_predicate_pushdown_skips_blocks() {
        use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
        use nodedb_types::value::Value;

        // Two-block segment:
        //   Block 0 (1024 rows): "aaaa_NNNN" → lexicographic range [aaaa_0000, aaaa_1023]
        //   Block 1 (476 rows):  "zzzz_NNNN" → range [zzzz_1024, zzzz_1499]
        let schema = ColumnarSchema::new(vec![ColumnDef::required("tag", ColumnType::String)])
            .expect("valid");

        let mut mt = crate::memtable::ColumnarMemtable::new(&schema);
        for i in 0..1024usize {
            mt.append_row(&[Value::String(format!("aaaa_{i:04}"))])
                .expect("append");
        }
        for i in 1024..1500usize {
            mt.append_row(&[Value::String(format!("zzzz_{i}"))])
                .expect("append");
        }

        let (schema, columns, row_count) = mt.drain();
        let segment = crate::writer::SegmentWriter::new(PROFILE_PLAIN, test_memory())
            .write_segment(&schema, &columns, row_count, None)
            .expect("write");

        let reader = SegmentReader::open(&segment).expect("open");
        let footer = reader.footer();

        // Confirm zone maps are populated on both blocks.
        let b0 = &footer.columns[0].block_stats[0];
        let b1 = &footer.columns[0].block_stats[1];
        assert!(b0.str_min.is_some(), "block 0 str_min missing");
        assert!(b1.str_min.is_some(), "block 1 str_min missing");

        // Predicate: tag >= "zzzz_0" → block 0 max ≈ "aaaa_..." < "zzzz_0" → skip block 0.
        let pred = ScanPredicate::str_gte(0, "zzzz_0");
        assert!(pred.can_skip_block(b0), "block 0 should be skippable");
        assert!(!pred.can_skip_block(b1), "block 1 should not be skipped");

        // End-to-end read: block 0 should be null-filled, block 1 decoded.
        let col = reader
            .read_column_filtered(0, &[pred])
            .expect("filtered read");
        match col {
            DecodedColumn::Binary { valid, .. } => {
                assert_eq!(valid.len(), 1500);
                // Block 0 (rows 0..1024) null-filled.
                assert!(!valid[0], "row 0 should be null-filled (skipped block)");
                assert!(!valid[1023], "row 1023 should be null-filled");
                // Block 1 (rows 1024..1500) present.
                assert!(valid[1024], "row 1024 should be valid");
                assert!(valid[1499], "row 1499 should be valid");
            }
            _ => panic!("expected Binary for string column"),
        }
    }

    /// Write a segment from a memtable that has been dict-encoded, then read back
    /// and verify the dictionary and IDs match the original values.
    #[test]
    fn dict_encoded_roundtrip() {
        use crate::memtable::{ColumnData, ColumnarMemtable, DICT_ENCODE_MAX_CARDINALITY};
        use crate::writer::SegmentWriter;

        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("qtype", ColumnType::String),
        ])
        .expect("valid");

        let qtypes = ["A", "AAAA", "MX", "NS", "SOA", "CNAME", "PTR", "TXT"];
        let mut mt = ColumnarMemtable::new(&schema);
        for (i, &q) in qtypes.iter().cycle().take(100).enumerate() {
            mt.append_row(&[Value::Integer(i as i64), Value::String(q.into())])
                .expect("append");
        }

        // Convert low-cardinality string column to dict-encoded.
        mt.try_dict_encode_columns(DICT_ENCODE_MAX_CARDINALITY);

        // Verify it converted.
        assert!(matches!(mt.columns()[1], ColumnData::DictEncoded { .. }));

        let (schema, columns, row_count) = mt.drain();
        let segment = SegmentWriter::new(PROFILE_PLAIN, test_memory())
            .write_segment(&schema, &columns, row_count, None)
            .expect("write segment");

        // Read back.
        let reader = SegmentReader::open(&segment).expect("open");
        assert_eq!(reader.row_count(), 100);

        // The footer should record the dictionary.
        let dict_in_meta = reader.footer().columns[1].dictionary.as_deref();
        assert!(dict_in_meta.is_some(), "dictionary should be in ColumnMeta");
        let meta_dict = dict_in_meta.expect("present");
        assert_eq!(meta_dict.len(), 8, "8 distinct qtypes");

        // Read the qtype column — should come back as DictEncoded.
        let col = reader.read_column(1).expect("read qtype column");
        match col {
            DecodedColumn::DictEncoded {
                ids,
                dictionary,
                valid,
            } => {
                assert_eq!(ids.len(), 100);
                assert_eq!(valid.len(), 100);
                assert!(valid.iter().all(|&v| v));
                assert_eq!(dictionary.len(), 8);

                // Verify round-trip: each row's ID resolves to the original qtype.
                for (i, &q) in qtypes.iter().cycle().take(100).enumerate() {
                    let resolved = &dictionary[ids[i] as usize];
                    assert_eq!(resolved, q, "row {i}: expected {q}, got {resolved}");
                }
            }
            _ => panic!("expected DictEncoded, got {col:?}"),
        }
    }

    /// Dict-encoded column with nulls must decode with valid=false for null rows.
    #[test]
    fn dict_encoded_roundtrip_with_nulls() {
        use crate::memtable::{ColumnarMemtable, DICT_ENCODE_MAX_CARDINALITY};
        use crate::writer::SegmentWriter;

        let schema = ColumnarSchema::new(vec![ColumnDef::nullable("rcode", ColumnType::String)])
            .expect("valid");

        let mut mt = ColumnarMemtable::new(&schema);
        mt.append_row(&[Value::String("NOERROR".into())])
            .expect("append");
        mt.append_row(&[Value::Null]).expect("null");
        mt.append_row(&[Value::String("NXDOMAIN".into())])
            .expect("append");
        mt.append_row(&[Value::Null]).expect("null");
        mt.append_row(&[Value::String("SERVFAIL".into())])
            .expect("append");

        mt.try_dict_encode_columns(DICT_ENCODE_MAX_CARDINALITY);

        let (schema, columns, row_count) = mt.drain();
        let segment = SegmentWriter::new(PROFILE_PLAIN, test_memory())
            .write_segment(&schema, &columns, row_count, None)
            .expect("write");

        let reader = SegmentReader::open(&segment).expect("open");
        let col = reader.read_column(0).expect("read");

        match col {
            DecodedColumn::DictEncoded {
                ids,
                dictionary,
                valid,
            } => {
                assert_eq!(ids.len(), 5);
                assert!(valid[0]);
                assert!(!valid[1]); // Null.
                assert!(valid[2]);
                assert!(!valid[3]); // Null.
                assert!(valid[4]);
                assert_eq!(dictionary.len(), 3);

                assert_eq!(&dictionary[ids[0] as usize], "NOERROR");
                assert_eq!(&dictionary[ids[2] as usize], "NXDOMAIN");
                assert_eq!(&dictionary[ids[4] as usize], "SERVFAIL");
            }
            _ => panic!("expected DictEncoded"),
        }
    }
}
