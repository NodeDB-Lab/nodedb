// SPDX-License-Identifier: Apache-2.0

//! Primary key index for columnar collections.
//!
//! Maps PK values to (segment_id, row_index) locations. Used for:
//! - Point lookups by PK
//! - UPDATE/DELETE row resolution
//! - Duplicate detection on INSERT
//!
//! Uses a `BTreeMap` for sorted key order (enables range scans on PK).
//! PK values are encoded as sortable byte vectors for uniform comparison.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

use crate::error::ColumnarError;

/// Location of a row within the columnar segment store.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToMessagePack, FromMessagePack,
)]
pub struct RowLocation {
    /// Segment identifier (index in the segment list, or a unique segment ID).
    pub segment_id: u64,
    /// Row index within the segment (0-based).
    pub row_index: u32,
}

/// In-memory B-tree index mapping PK values to row locations.
///
/// PK values are stored as sortable byte vectors (`Vec<u8>`) produced by
/// `encode_pk`. This gives uniform ordering regardless of PK type
/// (Int64, String, Uuid, composite).
#[derive(Debug, Clone)]
pub struct PkIndex {
    inner: BTreeMap<Vec<u8>, RowLocation>,
}

impl PkIndex {
    /// Create an empty PK index.
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Insert a PK → location mapping. Returns error if PK already exists.
    pub fn insert(
        &mut self,
        pk_bytes: Vec<u8>,
        location: RowLocation,
    ) -> Result<(), ColumnarError> {
        if self.inner.contains_key(&pk_bytes) {
            return Err(ColumnarError::DuplicatePrimaryKey);
        }
        self.inner.insert(pk_bytes, location);
        Ok(())
    }

    /// Insert or overwrite a PK → location mapping (used during compaction remap).
    pub fn upsert(&mut self, pk_bytes: Vec<u8>, location: RowLocation) {
        self.inner.insert(pk_bytes, location);
    }

    /// Look up a row location by PK.
    pub fn get(&self, pk_bytes: &[u8]) -> Option<&RowLocation> {
        self.inner.get(pk_bytes)
    }

    /// Remove a PK entry (used when a row is deleted and compacted away).
    pub fn remove(&mut self, pk_bytes: &[u8]) -> Option<RowLocation> {
        self.inner.remove(pk_bytes)
    }

    /// Check whether a PK exists in the index.
    pub fn contains(&self, pk_bytes: &[u8]) -> bool {
        self.inner.contains_key(pk_bytes)
    }

    /// Remap all entries pointing to `old_segment_id` using a provided function.
    ///
    /// Used after compaction: old segment IDs are replaced with new ones,
    /// and row indices may shift. The `remap_fn` returns `None` to remove
    /// the entry (row was deleted during compaction) or `Some(new_location)`.
    pub fn remap_segment(
        &mut self,
        old_segment_id: u64,
        remap_fn: impl Fn(u32) -> Option<RowLocation>,
    ) {
        let keys_to_remap: Vec<Vec<u8>> = self
            .inner
            .iter()
            .filter(|(_, loc)| loc.segment_id == old_segment_id)
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remap {
            let old_loc = self.inner.remove(&key).expect("key exists from filter");
            if let Some(new_loc) = remap_fn(old_loc.row_index) {
                self.inner.insert(key, new_loc);
            }
            // If remap_fn returns None, the entry is dropped (row was deleted).
        }
    }

    /// Bulk insert entries for a newly flushed segment.
    ///
    /// `pk_bytes_list` contains the encoded PK bytes for each row in the segment
    /// (in order). `segment_id` is the new segment's ID.
    pub fn bulk_insert(
        &mut self,
        segment_id: u64,
        pk_bytes_list: &[Vec<u8>],
    ) -> Result<(), ColumnarError> {
        for (row_index, pk_bytes) in pk_bytes_list.iter().enumerate() {
            let location = RowLocation {
                segment_id,
                row_index: row_index as u32,
            };
            self.insert(pk_bytes.clone(), location)?;
        }
        Ok(())
    }

    /// Remove all entries pointing to a given segment (used when dropping a segment).
    pub fn remove_segment(&mut self, segment_id: u64) {
        self.inner.retain(|_, loc| loc.segment_id != segment_id);
    }

    /// Serialize the index to bytes for checkpoint persistence.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ColumnarError> {
        // Serialize as Vec<(key, location)> via MessagePack.
        let entries: Vec<(&Vec<u8>, &RowLocation)> = self.inner.iter().collect();
        zerompk::to_msgpack_vec(&entries).map_err(|e| ColumnarError::Serialization(e.to_string()))
    }

    /// Deserialize the index from a checkpoint.
    pub fn from_bytes(data: &[u8]) -> Result<Self, ColumnarError> {
        let entries: Vec<(Vec<u8>, RowLocation)> =
            zerompk::from_msgpack(data).map_err(|e| ColumnarError::Serialization(e.to_string()))?;
        let mut inner = BTreeMap::new();
        for (key, loc) in entries {
            inner.insert(key, loc);
        }
        Ok(Self { inner })
    }
}

impl Default for PkIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode a PK value into sortable bytes.
///
/// Uses big-endian XOR encoding for signed integers (preserves sort order),
/// raw UTF-8 for strings, raw bytes for UUIDs.
pub fn encode_pk(value: &nodedb_types::value::Value) -> Vec<u8> {
    use nodedb_types::value::Value;
    match value {
        Value::Integer(v) => {
            let sortable = (*v as u64) ^ (1u64 << 63);
            sortable.to_be_bytes().to_vec()
        }
        Value::String(s) => s.as_bytes().to_vec(),
        Value::Uuid(s) => s.as_bytes().to_vec(),
        Value::Decimal(d) => d.serialize().to_vec(),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => {
            let sortable = (dt.micros as u64) ^ (1u64 << 63);
            sortable.to_be_bytes().to_vec()
        }
        _ => format!("{value:?}").into_bytes(),
    }
}

/// Encode a composite PK from multiple values.
pub fn encode_composite_pk(values: &[&nodedb_types::value::Value]) -> Vec<u8> {
    let mut key = Vec::new();
    for (i, val) in values.iter().enumerate() {
        if i > 0 {
            key.push(0xFF); // Separator between composite PK parts.
        }
        key.extend_from_slice(&encode_pk(val));
    }
    key
}

#[cfg(test)]
mod tests {
    use nodedb_types::value::Value;

    use super::*;

    #[test]
    fn insert_and_lookup() {
        let mut idx = PkIndex::new();
        let pk = encode_pk(&Value::Integer(42));
        let loc = RowLocation {
            segment_id: 0,
            row_index: 5,
        };

        idx.insert(pk.clone(), loc).expect("insert");
        assert_eq!(idx.get(&pk), Some(&loc));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn duplicate_pk_rejected() {
        let mut idx = PkIndex::new();
        let pk = encode_pk(&Value::Integer(1));
        let loc = RowLocation {
            segment_id: 0,
            row_index: 0,
        };

        idx.insert(pk.clone(), loc).expect("first insert");
        assert!(matches!(
            idx.insert(pk, loc),
            Err(ColumnarError::DuplicatePrimaryKey)
        ));
    }

    #[test]
    fn remove_entry() {
        let mut idx = PkIndex::new();
        let pk = encode_pk(&Value::Integer(1));
        let loc = RowLocation {
            segment_id: 0,
            row_index: 0,
        };

        idx.insert(pk.clone(), loc).expect("insert");
        let removed = idx.remove(&pk);
        assert_eq!(removed, Some(loc));
        assert!(idx.is_empty());
    }

    #[test]
    fn bulk_insert() {
        let mut idx = PkIndex::new();
        let pks: Vec<Vec<u8>> = (0..10).map(|i| encode_pk(&Value::Integer(i))).collect();

        idx.bulk_insert(0, &pks).expect("bulk insert");
        assert_eq!(idx.len(), 10);

        // Verify row indices.
        let loc = idx.get(&pks[5]).expect("lookup");
        assert_eq!(loc.segment_id, 0);
        assert_eq!(loc.row_index, 5);
    }

    #[test]
    fn remap_segment() {
        let mut idx = PkIndex::new();
        let pks: Vec<Vec<u8>> = (0..5).map(|i| encode_pk(&Value::Integer(i))).collect();
        idx.bulk_insert(0, &pks).expect("bulk insert");

        // Remap segment 0 → segment 1, shifting row indices by +10.
        // Remove row 2 (deleted during compaction).
        idx.remap_segment(0, |old_row| {
            if old_row == 2 {
                None // Deleted.
            } else {
                Some(RowLocation {
                    segment_id: 1,
                    row_index: old_row + 10,
                })
            }
        });

        assert_eq!(idx.len(), 4); // Row 2 was removed.
        let loc = idx.get(&pks[0]).expect("row 0");
        assert_eq!(loc.segment_id, 1);
        assert_eq!(loc.row_index, 10);

        assert!(idx.get(&pks[2]).is_none()); // Row 2 was deleted.
    }

    #[test]
    fn remove_segment() {
        let mut idx = PkIndex::new();
        let pks: Vec<Vec<u8>> = (0..5).map(|i| encode_pk(&Value::Integer(i))).collect();
        idx.bulk_insert(0, &pks).expect("seg 0");

        let pks2: Vec<Vec<u8>> = (10..15).map(|i| encode_pk(&Value::Integer(i))).collect();
        idx.bulk_insert(1, &pks2).expect("seg 1");

        assert_eq!(idx.len(), 10);
        idx.remove_segment(0);
        assert_eq!(idx.len(), 5); // Only segment 1 remains.
    }

    #[test]
    fn serialization_roundtrip() {
        let mut idx = PkIndex::new();
        let pks: Vec<Vec<u8>> = (0..100).map(|i| encode_pk(&Value::Integer(i))).collect();
        idx.bulk_insert(0, &pks).expect("bulk insert");

        let bytes = idx.to_bytes().expect("serialize");
        let restored = PkIndex::from_bytes(&bytes).expect("deserialize");

        assert_eq!(restored.len(), 100);
        let loc = restored.get(&pks[50]).expect("lookup");
        assert_eq!(loc.segment_id, 0);
        assert_eq!(loc.row_index, 50);
    }

    #[test]
    fn int_sort_order() {
        // Verify that encoded integers sort correctly (including negatives).
        let values = [-100i64, -1, 0, 1, 100];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| encode_pk(&Value::Integer(*v)))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order broken: {} < {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn pk_index_roundtrip_segment_id_above_u32_max() {
        let mut idx = PkIndex::new();
        let pk = encode_pk(&Value::Integer(999));
        // Use a segment ID that cannot fit in u32.
        let large_seg_id: u64 = u32::MAX as u64 + 1;
        let loc = RowLocation {
            segment_id: large_seg_id,
            row_index: 7,
        };

        idx.insert(pk.clone(), loc).expect("insert");

        // Round-trip through serialization.
        let bytes = idx.to_bytes().expect("serialize");
        let restored = PkIndex::from_bytes(&bytes).expect("deserialize");

        let got = restored.get(&pk).expect("lookup after roundtrip");
        assert_eq!(got.segment_id, large_seg_id);
        assert_eq!(got.row_index, 7);

        // remap_segment must match on the large ID.
        idx.remap_segment(large_seg_id, |old_row| {
            Some(RowLocation {
                segment_id: large_seg_id + 1,
                row_index: old_row,
            })
        });
        let remapped = idx.get(&pk).expect("lookup after remap");
        assert_eq!(remapped.segment_id, large_seg_id + 1);

        // remove_segment must remove on the large ID.
        idx.remove_segment(large_seg_id + 1);
        assert!(idx.is_empty());
    }

    #[test]
    fn composite_pk() {
        let pk1 = encode_composite_pk(&[&Value::Integer(1), &Value::String("a".into())]);
        let pk2 = encode_composite_pk(&[&Value::Integer(1), &Value::String("b".into())]);
        let pk3 = encode_composite_pk(&[&Value::Integer(2), &Value::String("a".into())]);

        assert!(pk1 < pk2); // Same first part, "a" < "b".
        assert!(pk2 < pk3); // 1 < 2 in first part.
    }
}
