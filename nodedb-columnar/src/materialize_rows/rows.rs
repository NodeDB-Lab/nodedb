// SPDX-License-Identifier: Apache-2.0

//! Materialize the stored rows of a flushed segment.
//!
//! Used by the durable RESTORE re-issue path: a backed-up flushed segment is
//! decoded back into per-row values so the rows can be replayed as a normal
//! columnar `Insert` (WAL- or Raft-durable) rather than installed into
//! in-memory-only state. A row in `skip` is excluded. The restore passes the
//! purged rows: no read at any cut sees them, so reissuing one would
//! resurrect it.

use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::format::BLOCK_SIZE;
use crate::reader::{BlockSelection, OwnedSegmentReader, decoded_cell_value};

/// One stored row of a flushed segment.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentRow {
    /// The row's index in the segment.
    pub row: u32,
    /// The row's cells in schema column order.
    pub values: Vec<Value>,
    /// The row's cross-engine surrogate. `None` when the sidecar has no
    /// entry for the row.
    pub surrogate: Option<Surrogate>,
}

/// Decode every row of one flushed segment blob that `skip` does not mark.
///
/// - `blob` is a raw `NDBS` (plaintext) or `SEGC` (encrypted) segment.
///   Encryption is detected from the blob magic: a `SEGC` blob is decrypted
///   with `kek` (an error if `kek` is `None`). An `NDBS` blob is read directly.
/// - `skip` marks the rows to leave out. It is empty to read every row.
/// - `row_surrogates` is the per-row surrogate sidecar for this segment
///   (parallel to segment row order). It can be empty or shorter than the
///   row count. A missing entry reads as `None`.
///
/// Returns the kept rows in segment row order.
pub fn materialize_segment_rows(
    blob: &[u8],
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    schema: &ColumnarSchema,
    skip: &DeleteBitmap,
    row_surrogates: &[Option<Surrogate>],
) -> Result<Vec<SegmentRow>, ColumnarError> {
    // `open_with_kek` is strict: it refuses a plaintext blob when a kek is
    // supplied and refuses an encrypted blob when no kek is supplied. Pick the
    // kek by the blob's own magic so both encrypted and plaintext segments in a
    // single restore decode.
    let is_encrypted = blob.len() >= 4 && blob[0..4] == crate::encrypt::SEGC_MAGIC;
    let effective_kek = if is_encrypted { kek } else { None };
    let owned = OwnedSegmentReader::open_with_kek(blob, effective_kek)?;
    let reader = owned.reader();

    let total_rows = u32::try_from(reader.row_count()).map_err(|_| ColumnarError::Corruption {
        segment_id: None,
        reason: format!(
            "segment row count {} exceeds the u32 row index space",
            reader.row_count()
        ),
        offset: None,
    })?;
    let col_count = schema.columns.len();
    let col_indices: Vec<usize> = (0..col_count).collect();
    // A block every row of which `skip` marks is never decoded.
    let selection = BlockSelection::from_fn(reader.block_count(), |block| {
        let start = block.saturating_mul(BLOCK_SIZE);
        let len = (total_rows as usize).saturating_sub(start).min(BLOCK_SIZE);
        match (u32::try_from(start), u32::try_from(len)) {
            (Ok(start), Ok(len)) => !skip.is_block_fully_deleted(start, len),
            _ => true,
        }
    });
    let decoded_cols = reader.read_columns_selected(&col_indices, &selection)?;

    let kept = (total_rows as usize).saturating_sub(skip.deleted_count() as usize);
    let mut out = Vec::with_capacity(kept);
    for row in 0..total_rows {
        if skip.is_deleted(row) {
            continue;
        }
        let row_idx = row as usize;
        let values = decoded_cols
            .iter()
            .zip(&schema.columns)
            .map(|(decoded, col)| decoded_cell_value(decoded, row_idx, &col.column_type))
            .collect::<Result<Vec<Value>, ColumnarError>>()?;
        let surrogate = row_surrogates.get(row_idx).copied().flatten();
        out.push(SegmentRow {
            row,
            values,
            surrogate,
        });
    }
    Ok(out)
}
