// SPDX-License-Identifier: Apache-2.0

//! Segment reader: decode compressed columns from a segment into typed vectors,
//! with column projection and block predicate pushdown.
//!
//! # Block Wire Format
//!
//! Each block in a column is stored as `[compressed_len: u32 LE][compressed_data]`.
//!
//! The compressed_data structure depends on the column type:
//! - **Int64/Float64/Timestamp**: `[validity_bitmap][codec_compressed_values]`
//! - **Bool/Decimal/Uuid/Vector**: `[validity_bitmap][codec_compressed_bytes]`
//! - **String/Bytes/Geometry**: `[validity_bitmap][offset_len: u32][compressed_offsets][compressed_data]`

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::format::{HEADER_SIZE, SegmentFooter, SegmentHeader};
use crate::predicate::ScanPredicate;

use super::block_decode::{
    append_null_fill, decode_block, empty_decoded, result_valid_len, result_valid_slice_mut,
};
use super::block_select::BlockSelection;
use super::types::DecodedColumn;

fn checked_slice(data: &[u8], start: usize, len: usize) -> Result<&[u8], ColumnarError> {
    let end = start
        .checked_add(len)
        .ok_or_else(|| ColumnarError::Corruption {
            segment_id: None,
            reason: "column block range overflow".into(),
            offset: None,
        })?;
    data.get(start..end).ok_or(ColumnarError::TruncatedSegment {
        expected: end,
        got: data.len(),
    })
}

/// Reads and decodes columns from a segment byte buffer.
///
/// Supports column projection (only decode requested columns) and block
/// predicate pushdown (skip blocks whose stats prove no match).
pub struct SegmentReader<'a> {
    pub(super) data: &'a [u8],
    pub(super) footer: SegmentFooter,
}

/// An owned segment reader that holds decrypted segment bytes.
///
/// Used when a segment was encrypted at rest: the decrypted plaintext is owned
/// by this struct, and `reader()` borrows from it for column decoding.
#[derive(Debug)]
pub struct OwnedSegmentReader {
    /// Decrypted plaintext segment bytes.
    plaintext: Vec<u8>,
    footer: SegmentFooter,
}

impl OwnedSegmentReader {
    fn from_plaintext(plaintext: Vec<u8>) -> Result<Self, ColumnarError> {
        SegmentHeader::from_bytes(&plaintext)?;
        let footer = SegmentFooter::from_segment_tail(&plaintext)?;
        Ok(Self { plaintext, footer })
    }

    /// Open a segment with optional at-rest decryption.
    ///
    /// - `kek = None` → requires a plaintext (`NDBS`) segment; returns
    ///   `Err(MissingKek)` if the blob starts with `SEGC`.
    /// - `kek = Some(key)` → requires an encrypted (`SEGC`) segment; decrypts
    ///   the blob, then parses the inner plaintext. Returns `Err(KekRequired)`
    ///   if the blob starts with `NDBS`.
    pub fn open_with_kek(
        blob: &[u8],
        kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    ) -> Result<Self, ColumnarError> {
        let is_encrypted = blob.len() >= 4 && blob[0..4] == crate::encrypt::SEGC_MAGIC;
        if is_encrypted {
            let key = kek.ok_or(ColumnarError::MissingKek)?;
            let plaintext = crate::encrypt::decrypt_segment(key, blob)?;
            Self::from_plaintext(plaintext)
        } else if kek.is_some() {
            Err(ColumnarError::KekRequired)
        } else {
            Self::from_plaintext(blob.to_vec())
        }
    }

    /// Borrow a `SegmentReader` over the owned plaintext bytes.
    pub fn reader(&self) -> SegmentReader<'_> {
        SegmentReader {
            data: &self.plaintext,
            footer: self.footer.clone(),
        }
    }

    /// Access the segment footer.
    pub fn footer(&self) -> &SegmentFooter {
        &self.footer
    }

    /// Total row count in the segment.
    pub fn row_count(&self) -> u64 {
        self.footer.row_count
    }
}

impl<'a> SegmentReader<'a> {
    /// Open a plaintext segment from a byte buffer.
    ///
    /// Validates the `NDBS` header and footer CRC. If `data` starts with `SEGC`
    /// (an encrypted envelope) and `kek` is `None`, returns `Err(MissingKek)`.
    /// If `data` starts with `NDBS` (plaintext) and `kek` is `Some`, returns
    /// `Err(KekRequired)` — refusing to load unencrypted data when encryption
    /// is configured.
    ///
    /// To read an encrypted segment, use [`OwnedSegmentReader::open_with_kek`].
    pub fn open(data: &'a [u8]) -> Result<Self, ColumnarError> {
        if data.len() >= 4 && data[0..4] == crate::encrypt::SEGC_MAGIC {
            return Err(ColumnarError::MissingKek);
        }
        SegmentHeader::from_bytes(data)?;
        let footer = SegmentFooter::from_segment_tail(data)?;
        Ok(Self { data, footer })
    }

    /// Access the footer metadata.
    pub fn footer(&self) -> &SegmentFooter {
        &self.footer
    }

    /// Total row count in the segment.
    pub fn row_count(&self) -> u64 {
        self.footer.row_count
    }

    /// Number of columns in the segment.
    pub fn column_count(&self) -> usize {
        self.footer.column_count as usize
    }

    /// Read a single column, decoding all blocks.
    ///
    /// `col_idx` is the column index in the footer's column metadata.
    pub fn read_column(&self, col_idx: usize) -> Result<DecodedColumn, ColumnarError> {
        self.read_column_filtered(col_idx, &[])
    }

    /// Read a single column with predicate pushdown.
    ///
    /// Blocks whose stats satisfy the predicates are skipped. For skipped
    /// blocks, null/zero-fill rows are emitted to preserve row alignment
    /// across projected columns.
    pub fn read_column_filtered(
        &self,
        col_idx: usize,
        predicates: &[ScanPredicate],
    ) -> Result<DecodedColumn, ColumnarError> {
        self.read_column_impl(col_idx, predicates, &DeleteBitmap::new(), None)
    }

    /// Read multiple columns with shared predicate pushdown.
    ///
    /// All columns share the same block skip decisions so row alignment
    /// is maintained across the result set.
    pub fn read_columns(
        &self,
        col_indices: &[usize],
        predicates: &[ScanPredicate],
    ) -> Result<Vec<DecodedColumn>, ColumnarError> {
        let selection = self.select_blocks(predicates);
        self.read_columns_selected(col_indices, &selection)
    }

    /// Read a column with both predicate pushdown and delete bitmap masking.
    ///
    /// Deleted rows have their validity set to false in the output.
    /// Fully deleted blocks are skipped entirely (no decompression).
    pub fn read_column_with_deletes(
        &self,
        col_idx: usize,
        predicates: &[ScanPredicate],
        deletes: &DeleteBitmap,
    ) -> Result<DecodedColumn, ColumnarError> {
        self.read_column_impl(col_idx, predicates, deletes, None)
    }

    /// Read multiple columns with predicate pushdown and delete bitmap. All
    /// columns share the same block skip decisions.
    pub fn read_columns_with_deletes(
        &self,
        col_indices: &[usize],
        predicates: &[ScanPredicate],
        deletes: &DeleteBitmap,
    ) -> Result<Vec<DecodedColumn>, ColumnarError> {
        let selection = self.select_blocks(predicates);
        col_indices
            .iter()
            .map(|&idx| self.read_column_impl(idx, &[], deletes, Some(&selection)))
            .collect()
    }

    /// Number of 1024-row blocks every column of the segment holds.
    pub fn block_count(&self) -> usize {
        self.footer
            .columns
            .first()
            .map_or(0, |col| col.block_stats.len())
    }

    /// The blocks whose statistics can match every predicate. A block one
    /// predicate's statistics rule out holds no row the conjunction admits,
    /// so no column of it is decoded. A predicate on a column the segment
    /// does not hold rules nothing out.
    pub fn select_blocks(&self, predicates: &[ScanPredicate]) -> BlockSelection {
        BlockSelection::from_fn(self.block_count(), |block| {
            !predicates.iter().any(|pred| {
                self.footer
                    .columns
                    .get(pred.col_idx)
                    .and_then(|col| col.block_stats.get(block))
                    .is_some_and(|stats| pred.can_skip_block(stats))
            })
        })
    }

    /// Read `col_indices`, decoding only the blocks `selection` selects.
    ///
    /// Every column keeps the segment's row alignment: an unselected block
    /// reads as null rows and is never decompressed.
    pub fn read_columns_selected(
        &self,
        col_indices: &[usize],
        selection: &BlockSelection,
    ) -> Result<Vec<DecodedColumn>, ColumnarError> {
        let no_deletes = DeleteBitmap::new();
        col_indices
            .iter()
            .map(|&idx| self.read_column_impl(idx, &[], &no_deletes, Some(selection)))
            .collect()
    }

    /// Shared implementation for column reading with predicate pushdown,
    /// optional delete bitmap masking, and an optional block selection.
    fn read_column_impl(
        &self,
        col_idx: usize,
        predicates: &[ScanPredicate],
        deletes: &DeleteBitmap,
        selection: Option<&BlockSelection>,
    ) -> Result<DecodedColumn, ColumnarError> {
        if col_idx >= self.footer.columns.len() {
            return Err(ColumnarError::ColumnOutOfRange {
                index: col_idx,
                count: self.footer.columns.len(),
            });
        }

        let col_meta = &self.footer.columns[col_idx];
        let my_preds: Vec<&ScanPredicate> =
            predicates.iter().filter(|p| p.col_idx == col_idx).collect();

        let col_offset =
            usize::try_from(col_meta.offset).map_err(|_| ColumnarError::Corruption {
                segment_id: None,
                reason: "column offset does not fit usize".into(),
                offset: None,
            })?;
        let col_start =
            HEADER_SIZE
                .checked_add(col_offset)
                .ok_or_else(|| ColumnarError::Corruption {
                    segment_id: None,
                    reason: "column offset range overflow".into(),
                    offset: None,
                })?;
        if col_start > self.data.len() {
            return Err(ColumnarError::TruncatedSegment {
                expected: col_start,
                got: self.data.len(),
            });
        }
        let mut cursor = col_start;
        let mut result = empty_decoded(col_meta.layout);
        let mut global_row: u32 = 0;

        for (block, block_stat) in col_meta.block_stats.iter().enumerate() {
            let block_row_count = block_stat.row_count;

            let header_end = cursor
                .checked_add(4)
                .ok_or_else(|| ColumnarError::Corruption {
                    segment_id: None,
                    reason: "column block header range overflow".into(),
                    offset: None,
                })?;
            let header =
                self.data
                    .get(cursor..header_end)
                    .ok_or(ColumnarError::TruncatedSegment {
                        expected: header_end,
                        got: self.data.len(),
                    })?;
            let block_len = usize::try_from(u32::from_le_bytes([
                header[0], header[1], header[2], header[3],
            ]))
            .map_err(|_| ColumnarError::Corruption {
                segment_id: None,
                reason: "column block length does not fit usize".into(),
                offset: None,
            })?;
            let block_data = checked_slice(self.data, header_end, block_len)?;
            cursor =
                header_end
                    .checked_add(block_len)
                    .ok_or_else(|| ColumnarError::Corruption {
                        segment_id: None,
                        reason: "column block range overflow".into(),
                        offset: None,
                    })?;

            // Skip via predicate pushdown.
            let pred_skip = my_preds.iter().any(|p| p.can_skip_block(block_stat));

            // Skip if entire block is deleted.
            let delete_skip =
                !deletes.is_empty() && deletes.is_block_fully_deleted(global_row, block_row_count);

            // Skip a block the caller's selection leaves out.
            let unselected = selection.is_some_and(|sel| !sel.is_selected(block));

            if pred_skip || delete_skip || unselected {
                append_null_fill(&mut result, block_row_count as usize);
                global_row += block_row_count;
                continue;
            }

            // Decode the block.
            let pre_len = result_valid_len(&result);
            decode_block(
                &mut result,
                block_data,
                col_meta.layout,
                col_meta.codec,
                block_row_count as usize,
                col_meta.dictionary.as_deref(),
            )?;

            // Apply delete bitmap to the newly decoded rows.
            if !deletes.is_empty() {
                let valid_slice = result_valid_slice_mut(&mut result, pre_len);
                deletes.apply_to_validity(valid_slice, global_row);
            }

            global_row += block_row_count;
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::checked_slice;
    use super::{BlockSelection, SegmentReader};
    use crate::error::ColumnarError;
    use crate::memtable::ColumnarMemtable;
    use crate::predicate::ScanPredicate;
    use crate::reader::block_decode::decoded_block_count;
    use crate::reader::decoded_cell_value;
    use crate::test_support::test_memory;
    use crate::writer::{PROFILE_PLAIN, SegmentWriter};

    /// A segment of `rows` rows over `(id, v = 2 * id)`, three blocks for
    /// 2500 rows.
    fn two_column_segment(rows: usize) -> Vec<u8> {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid");
        let mut mt = ColumnarMemtable::new(&schema);
        for i in 0..rows {
            let i = i64::try_from(i).expect("row fits i64");
            mt.append_row(&[Value::Integer(i), Value::Integer(2 * i)])
                .expect("append");
        }
        let (schema, columns, row_count) = mt.drain();
        SegmentWriter::new(PROFILE_PLAIN, test_memory())
            .write_segment(&schema, &columns, row_count, None)
            .expect("write")
    }

    /// A predicate on one column rules a block out for every column: the
    /// selected read decodes only the selected blocks, and each row of a
    /// selected block reads as the full decode reads it.
    #[test]
    fn a_selected_read_decodes_only_the_selected_blocks() {
        let segment = two_column_segment(2500);
        let reader = SegmentReader::open(&segment).expect("open");
        assert_eq!(reader.block_count(), 3);

        let before_full = decoded_block_count();
        let full = reader.read_columns(&[0, 1], &[]).expect("full read");
        assert_eq!(
            decoded_block_count() - before_full,
            6,
            "3 blocks x 2 columns"
        );

        // `v >= 4200` holds only in block 2 (ids 2100..).
        let selection = reader.select_blocks(&[ScanPredicate::gte_i64(1, 4200)]);
        assert_eq!(selection.selected_count(), 1);
        assert!(selection.is_selected(2));

        let before = decoded_block_count();
        let partial = reader
            .read_columns_selected(&[0, 1], &selection)
            .expect("selected read");
        assert_eq!(
            decoded_block_count() - before,
            2,
            "one block per column: blocks 0 and 1 are never decoded"
        );

        for row in 0..2500 {
            for (full_col, partial_col) in full.iter().zip(&partial) {
                let partial_cell =
                    decoded_cell_value(partial_col, row, &ColumnType::Int64).expect("cell");
                if selection.holds_row(row) {
                    let full_cell =
                        decoded_cell_value(full_col, row, &ColumnType::Int64).expect("cell");
                    assert_eq!(partial_cell, full_cell, "row {row}");
                } else {
                    assert_eq!(partial_cell, Value::Null, "row {row} is not decoded");
                }
            }
        }

        let none = BlockSelection::from_fn(reader.block_count(), |_| false);
        let before_none = decoded_block_count();
        reader
            .read_columns_selected(&[0, 1], &none)
            .expect("empty read");
        assert_eq!(decoded_block_count(), before_none);
    }

    #[test]
    fn block_range_rejects_truncated_and_overflowed_lengths() {
        assert!(matches!(
            checked_slice(&[0; 4], 2, 4),
            Err(ColumnarError::TruncatedSegment {
                expected: 6,
                got: 4
            })
        ));
        assert!(matches!(
            checked_slice(&[], usize::MAX, 1),
            Err(ColumnarError::Corruption { .. })
        ));
    }
}
