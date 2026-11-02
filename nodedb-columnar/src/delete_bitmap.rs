// SPDX-License-Identifier: Apache-2.0

//! Per-segment delete bitmap for columnar UPDATE/DELETE.
//!
//! Uses roaring bitmaps for space-efficient tracking of deleted row indices.
//! Each segment has at most one associated DeleteBitmap, stored alongside
//! the segment as `{segment_id}.del`.
//!
//! The bitmap marks every row a write ended: overwritten, deleted, and
//! purged rows alike. A current read skips every marked row with one probe.
//! A read at an ordinal tells history from purged rows by the row's
//! `sys_to` (`crate::versions::SegmentVersions`).
//!
//! The bitmap is serialized to bytes for persistence and deserialized on
//! segment open.

use roaring::RoaringBitmap;

use crate::error::ColumnarError;

/// Per-segment bitmap of deleted row indices.
///
/// Wraps a roaring bitmap for O(1) membership checks and efficient
/// serialization. Row indices are 0-based within the segment.
#[derive(Debug, Clone)]
pub struct DeleteBitmap {
    inner: RoaringBitmap,
}

impl DeleteBitmap {
    /// Create an empty delete bitmap.
    pub fn new() -> Self {
        Self {
            inner: RoaringBitmap::new(),
        }
    }

    /// Mark a row as deleted. Returns true if the row was newly deleted.
    pub fn mark_deleted(&mut self, row_idx: u32) -> bool {
        self.inner.insert(row_idx)
    }

    /// Mark multiple rows as deleted.
    pub fn mark_deleted_batch(&mut self, row_indices: &[u32]) {
        for &idx in row_indices {
            self.inner.insert(idx);
        }
    }

    /// Check whether a row is deleted.
    pub fn is_deleted(&self, row_idx: u32) -> bool {
        self.inner.contains(row_idx)
    }

    /// Number of deleted rows.
    pub fn deleted_count(&self) -> u64 {
        self.inner.len()
    }

    /// Whether the bitmap is empty (no deletions).
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Delete ratio: deleted_count / total_rows.
    ///
    /// Reported as a segment-health statistic on Origin, where segments are
    /// never rewritten. Embedded deployments act on it via [`Self::should_compact`].
    pub fn delete_ratio(&self, total_rows: u64) -> f64 {
        if total_rows == 0 {
            return 0.0;
        }
        self.inner.len() as f64 / total_rows as f64
    }

    /// Whether this segment should be compacted (delete ratio > threshold).
    pub fn should_compact(&self, total_rows: u64, threshold: f64) -> bool {
        self.delete_ratio(total_rows) > threshold
    }

    /// Check whether an entire block is fully deleted.
    ///
    /// `block_start` is the first row index of the block, `block_len` is
    /// the number of rows. Returns true if every row in the range is deleted.
    pub fn is_block_fully_deleted(&self, block_start: u32, block_len: u32) -> bool {
        if block_len == 0 {
            return true;
        }
        // Count deleted rows in the range [block_start, block_start + block_len).
        let count = self
            .inner
            .range(block_start..block_start + block_len)
            .count() as u32;
        count == block_len
    }

    /// Apply the delete bitmap to a validity vector: set deleted rows to false.
    ///
    /// `global_offset` is the row index of the first element in `valid`
    /// (used when processing a block that starts at a non-zero row).
    pub fn apply_to_validity(&self, valid: &mut [bool], global_offset: u32) {
        for (i, v) in valid.iter_mut().enumerate() {
            if self.inner.contains(global_offset + i as u32) {
                *v = false;
            }
        }
    }

    /// Serialize the bitmap to bytes for persistence.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ColumnarError> {
        let mut buf = Vec::with_capacity(self.inner.serialized_size());
        self.inner
            .serialize_into(&mut buf)
            .map_err(|e| ColumnarError::Serialization(format!("delete bitmap: {e}")))?;
        Ok(buf)
    }

    /// Deserialize a bitmap from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, ColumnarError> {
        let inner = RoaringBitmap::deserialize_from(data)
            .map_err(|e| ColumnarError::Serialization(format!("delete bitmap: {e}")))?;
        Ok(Self { inner })
    }

    /// Iterate over all deleted row indices.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.inner.iter()
    }

    /// Remove a row from the deleted set (undo a tombstone).
    ///
    /// Used exclusively by transaction rollback to clear tombstones that
    /// were set by a preceding insert's upsert path. Returns true if the
    /// row was previously deleted, false if it was already live.
    pub fn unmark_deleted(&mut self, row_idx: u32) -> bool {
        self.inner.remove(row_idx)
    }

    /// Remove every row index at or above `start` from the deleted set.
    ///
    /// Used when the rows from `start` on are cut from a memtable: an index
    /// that later holds a new row must not inherit a tombstone.
    pub fn unmark_from(&mut self, start: u32) {
        self.inner.remove_range(start..);
    }

    /// Merge another bitmap into this one (union).
    ///
    /// Used when two views of the same segment's tombstones must be combined
    /// (for example a restored bitmap and one accumulated since the restore).
    pub fn merge(&mut self, other: &DeleteBitmap) {
        self.inner |= &other.inner;
    }
}

impl Default for DeleteBitmap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_and_check() {
        let mut bm = DeleteBitmap::new();
        assert!(!bm.is_deleted(5));
        assert!(bm.mark_deleted(5));
        assert!(bm.is_deleted(5));
        assert!(!bm.mark_deleted(5)); // Already deleted.
        assert_eq!(bm.deleted_count(), 1);
    }

    #[test]
    fn unmark_from_clears_the_tail_only() {
        let mut bm = DeleteBitmap::new();
        bm.mark_deleted_batch(&[1, 4, 5, 9]);
        bm.unmark_from(4);
        assert!(bm.is_deleted(1));
        assert!(!bm.is_deleted(4));
        assert!(!bm.is_deleted(5));
        assert!(!bm.is_deleted(9));
        assert_eq!(bm.deleted_count(), 1);
    }

    #[test]
    fn batch_delete() {
        let mut bm = DeleteBitmap::new();
        bm.mark_deleted_batch(&[1, 3, 5, 7, 9]);
        assert_eq!(bm.deleted_count(), 5);
        assert!(bm.is_deleted(3));
        assert!(!bm.is_deleted(4));
    }

    #[test]
    fn serialization_roundtrip() {
        let mut bm = DeleteBitmap::new();
        bm.mark_deleted_batch(&[0, 10, 100, 1000, 10000]);

        let bytes = bm.to_bytes().unwrap();
        let restored = DeleteBitmap::from_bytes(&bytes).expect("deserialize");

        assert_eq!(restored.deleted_count(), 5);
        assert!(restored.is_deleted(0));
        assert!(restored.is_deleted(10));
        assert!(restored.is_deleted(10000));
        assert!(!restored.is_deleted(50));
    }

    #[test]
    fn delete_ratio_reports_fraction_deleted() {
        let mut bm = DeleteBitmap::new();
        for i in 0..30 {
            bm.mark_deleted(i);
        }
        // 30 deleted out of 100 total = 0.3
        assert!((bm.delete_ratio(100) - 0.3).abs() < 0.001);
        // A zero-row segment must not divide by zero.
        assert_eq!(DeleteBitmap::new().delete_ratio(0), 0.0);
        // Default threshold 0.2 → should compact.
        assert!(bm.should_compact(100, 0.2));
        // Threshold 0.5 → should not compact.
        assert!(!bm.should_compact(100, 0.5));
    }

    #[test]
    fn block_fully_deleted() {
        let mut bm = DeleteBitmap::new();
        // Delete rows 10..20.
        for i in 10..20 {
            bm.mark_deleted(i);
        }
        assert!(bm.is_block_fully_deleted(10, 10));
        assert!(!bm.is_block_fully_deleted(10, 11)); // Row 20 is not deleted.
        assert!(!bm.is_block_fully_deleted(5, 10)); // Rows 5-9 are not deleted.
    }

    #[test]
    fn apply_to_validity() {
        let mut bm = DeleteBitmap::new();
        bm.mark_deleted_batch(&[2, 4]);

        let mut valid = vec![true, true, true, true, true];
        bm.apply_to_validity(&mut valid, 0);

        assert!(valid[0]);
        assert!(valid[1]);
        assert!(!valid[2]); // Deleted.
        assert!(valid[3]);
        assert!(!valid[4]); // Deleted.
    }

    #[test]
    fn apply_to_validity_with_offset() {
        let mut bm = DeleteBitmap::new();
        bm.mark_deleted(1025); // Row in second block.

        let mut valid = vec![true; 1024];
        bm.apply_to_validity(&mut valid, 1024); // Block starts at row 1024.

        assert!(valid[0]); // Global row 1024: not deleted.
        assert!(!valid[1]); // Global row 1025: deleted.
        assert!(valid[2]); // Global row 1026: not deleted.
    }

    #[test]
    fn merge_bitmaps() {
        let mut bm1 = DeleteBitmap::new();
        bm1.mark_deleted_batch(&[1, 2, 3]);

        let mut bm2 = DeleteBitmap::new();
        bm2.mark_deleted_batch(&[3, 4, 5]);

        bm1.merge(&bm2);
        assert_eq!(bm1.deleted_count(), 5); // Union: {1,2,3,4,5}.
    }

    #[test]
    fn empty_bitmap() {
        let bm = DeleteBitmap::new();
        assert!(bm.is_empty());
        assert_eq!(bm.deleted_count(), 0);
        assert!(!bm.is_deleted(0));

        // Serialization roundtrip of empty bitmap.
        let bytes = bm.to_bytes().unwrap();
        let restored = DeleteBitmap::from_bytes(&bytes).expect("deserialize");
        assert!(restored.is_empty());
    }
}
