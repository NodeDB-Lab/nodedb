//! R-tree checkpoint/restore for durable persistence.
//!
//! Follows the same pattern as the vector engine's HNSW checkpoint:
//! serialize all entries to bytes (MessagePack) → write to storage →
//! restore via bulk_load on cold start.
//!
//! Storage key scheme (in redb under Namespace::Spatial):
//! - `{collection}\x00{field}\x00rtree` → serialized R-tree entries
//! - `{collection}\x00{field}\x00meta` → SpatialIndexMeta

use nodedb_types::BoundingBox;
use serde::{Deserialize, Serialize};

use crate::rtree::{RTree, RTreeEntry};

/// Metadata for a persisted spatial index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpatialIndexMeta {
    /// Collection this index belongs to.
    pub collection: String,
    /// Geometry field name being indexed.
    pub field: String,
    /// Index type.
    pub index_type: SpatialIndexType,
    /// Number of entries at last checkpoint.
    pub entry_count: usize,
    /// Bounding box of all indexed geometries (spatial extent).
    pub extent: Option<BoundingBox>,
}

/// Type of spatial index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpatialIndexType {
    RTree,
    Geohash,
}

impl SpatialIndexType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RTree => "rtree",
            Self::Geohash => "geohash",
        }
    }
}

impl std::fmt::Display for SpatialIndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Serialized R-tree snapshot for checkpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct RTreeSnapshot {
    pub entries: Vec<RTreeEntry>,
}

impl RTree {
    /// Serialize the R-tree to bytes for checkpointing.
    ///
    /// Collects all entries and serializes via MessagePack. On restore,
    /// `from_checkpoint` bulk-loads entries for optimal packing.
    pub fn checkpoint_to_bytes(&self) -> Result<Vec<u8>, RTreeCheckpointError> {
        let snapshot = RTreeSnapshot {
            entries: self.entries().into_iter().cloned().collect(),
        };
        rmp_serde::to_vec_named(&snapshot).map_err(RTreeCheckpointError::Serialize)
    }

    /// Restore an R-tree from checkpoint bytes.
    ///
    /// Uses bulk_load (STR packing) for better node packing than
    /// sequential insert. Returns None if bytes are corrupted.
    pub fn from_checkpoint(bytes: &[u8]) -> Result<Self, RTreeCheckpointError> {
        let snapshot: RTreeSnapshot =
            rmp_serde::from_slice(bytes).map_err(RTreeCheckpointError::Deserialize)?;
        Ok(RTree::bulk_load(snapshot.entries))
    }
}

/// Build the storage key for an R-tree checkpoint.
///
/// Format: `{collection}\0{field}\0rtree`
pub fn rtree_storage_key(collection: &str, field: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(collection.len() + field.len() + 8);
    key.extend_from_slice(collection.as_bytes());
    key.push(0);
    key.extend_from_slice(field.as_bytes());
    key.push(0);
    key.extend_from_slice(b"rtree");
    key
}

/// Build the storage key for spatial index metadata.
///
/// Format: `{collection}\0{field}\0meta`
pub fn meta_storage_key(collection: &str, field: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(collection.len() + field.len() + 7);
    key.extend_from_slice(collection.as_bytes());
    key.push(0);
    key.extend_from_slice(field.as_bytes());
    key.push(0);
    key.extend_from_slice(b"meta");
    key
}

/// Serialize index metadata to bytes.
pub fn serialize_meta(meta: &SpatialIndexMeta) -> Result<Vec<u8>, RTreeCheckpointError> {
    rmp_serde::to_vec_named(meta).map_err(RTreeCheckpointError::Serialize)
}

/// Deserialize index metadata from bytes.
pub fn deserialize_meta(bytes: &[u8]) -> Result<SpatialIndexMeta, RTreeCheckpointError> {
    rmp_serde::from_slice(bytes).map_err(RTreeCheckpointError::Deserialize)
}

/// Errors during R-tree checkpoint operations.
#[derive(Debug, thiserror::Error)]
pub enum RTreeCheckpointError {
    #[error("R-tree checkpoint serialization failed: {0}")]
    Serialize(rmp_serde::encode::Error),
    #[error("R-tree checkpoint deserialization failed: {0}")]
    Deserialize(rmp_serde::decode::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(id: u64, lng: f64, lat: f64) -> RTreeEntry {
        RTreeEntry {
            id,
            bbox: BoundingBox::from_point(lng, lat),
        }
    }

    #[test]
    fn checkpoint_roundtrip_empty() {
        let tree = RTree::new();
        let bytes = tree.checkpoint_to_bytes().unwrap();
        let restored = RTree::from_checkpoint(&bytes).unwrap();
        assert_eq!(restored.len(), 0);
    }

    #[test]
    fn checkpoint_roundtrip_entries() {
        let mut tree = RTree::new();
        for i in 0..100 {
            tree.insert(make_entry(i, (i as f64) * 0.5, (i as f64) * 0.3));
        }
        assert_eq!(tree.len(), 100);

        let bytes = tree.checkpoint_to_bytes().unwrap();
        let restored = RTree::from_checkpoint(&bytes).unwrap();
        assert_eq!(restored.len(), 100);

        // All entries should be searchable.
        let all = restored.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert_eq!(all.len(), 100);
    }

    #[test]
    fn checkpoint_preserves_ids() {
        let mut tree = RTree::new();
        tree.insert(make_entry(42, 10.0, 20.0));
        tree.insert(make_entry(99, 30.0, 40.0));

        let bytes = tree.checkpoint_to_bytes().unwrap();
        let restored = RTree::from_checkpoint(&bytes).unwrap();

        let results = restored.search(&BoundingBox::new(5.0, 15.0, 15.0, 25.0));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 42);
    }

    #[test]
    fn corrupted_bytes_returns_error() {
        assert!(RTree::from_checkpoint(&[0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn meta_roundtrip() {
        let meta = SpatialIndexMeta {
            collection: "buildings".to_string(),
            field: "geom".to_string(),
            index_type: SpatialIndexType::RTree,
            entry_count: 1000,
            extent: Some(BoundingBox::new(-180.0, -90.0, 180.0, 90.0)),
        };
        let bytes = serialize_meta(&meta).unwrap();
        let restored = deserialize_meta(&bytes).unwrap();
        assert_eq!(restored.collection, "buildings");
        assert_eq!(restored.entry_count, 1000);
        assert_eq!(restored.index_type, SpatialIndexType::RTree);
    }

    #[test]
    fn storage_key_format() {
        let key = rtree_storage_key("buildings", "geom");
        assert_eq!(key, b"buildings\0geom\0rtree");

        let meta_key = meta_storage_key("buildings", "geom");
        assert_eq!(meta_key, b"buildings\0geom\0meta");
    }

    #[test]
    fn checkpoint_size_reasonable() {
        let mut tree = RTree::new();
        for i in 0..1000 {
            tree.insert(make_entry(i, (i as f64) * 0.01, (i as f64) * 0.01));
        }
        let bytes = tree.checkpoint_to_bytes().unwrap();
        // Each entry: id(8) + 4 f64(32) = ~40 bytes + msgpack overhead.
        // 1000 entries ≈ 40-60KB is reasonable.
        assert!(
            bytes.len() < 100_000,
            "checkpoint too large: {} bytes",
            bytes.len()
        );
        assert!(
            bytes.len() > 10_000,
            "checkpoint too small: {} bytes",
            bytes.len()
        );
    }
}
