//! Per-collection spatial index manager for Lite.
//!
//! Wraps `nodedb_spatial::RTree` with:
//! - Incremental insert/delete on document put/delete
//! - Geometry extraction from document fields
//! - Checkpoint/restore via MessagePack + CRC32C (same pattern as HNSW/CSR)
//! - Spatial query execution (range search, nearest neighbor)

use std::collections::HashMap;

use nodedb_spatial::rtree::{RTree, RTreeEntry};
use nodedb_types::BoundingBox;
use nodedb_types::geometry::Geometry;

/// Manages per-collection R-tree spatial indexes.
///
/// Each collection that has geometry fields gets its own R-tree.
/// The field name is stored alongside so we know which document field
/// to extract geometry from.
pub struct SpatialIndexManager {
    /// (collection_name, field_name) → R-tree.
    indices: HashMap<(String, String), RTree>,
    /// Document ID → entry ID mapping for deletion.
    /// Key: (collection, doc_id), Value: entry_id in R-tree.
    doc_to_entry: HashMap<(String, String), u64>,
    /// Next entry ID (monotonically increasing).
    next_id: u64,
}

impl SpatialIndexManager {
    pub fn new() -> Self {
        Self {
            indices: HashMap::new(),
            doc_to_entry: HashMap::new(),
            next_id: 1,
        }
    }

    /// Index a geometry from a document. If the document already has an entry,
    /// it is removed first (upsert semantics).
    pub fn index_document(
        &mut self,
        collection: &str,
        field: &str,
        doc_id: &str,
        geometry: &Geometry,
    ) {
        let key = (collection.to_string(), field.to_string());
        let doc_key = (collection.to_string(), doc_id.to_string());

        // Remove old entry if this document was previously indexed.
        if let Some(old_id) = self.doc_to_entry.remove(&doc_key)
            && let Some(tree) = self.indices.get_mut(&key)
        {
            tree.delete(old_id);
        }

        let bbox = nodedb_types::geometry_bbox(geometry);
        let entry_id = self.next_id;
        self.next_id += 1;

        let tree = self.indices.entry(key).or_default();
        tree.insert(RTreeEntry { id: entry_id, bbox });
        self.doc_to_entry.insert(doc_key, entry_id);
    }

    /// Remove a document's geometry from the index.
    pub fn remove_document(&mut self, collection: &str, field: &str, doc_id: &str) {
        let key = (collection.to_string(), field.to_string());
        let doc_key = (collection.to_string(), doc_id.to_string());

        if let Some(entry_id) = self.doc_to_entry.remove(&doc_key)
            && let Some(tree) = self.indices.get_mut(&key)
        {
            tree.delete(entry_id);
        }
    }

    /// Range search: find all document entry IDs whose bbox intersects the query.
    pub fn search(&self, collection: &str, field: &str, query: &BoundingBox) -> Vec<&RTreeEntry> {
        let key = (collection.to_string(), field.to_string());
        match self.indices.get(&key) {
            Some(tree) => tree.search(query),
            None => Vec::new(),
        }
    }

    /// Nearest-neighbor search.
    pub fn nearest(
        &self,
        collection: &str,
        field: &str,
        lng: f64,
        lat: f64,
        k: usize,
    ) -> Vec<nodedb_spatial::rtree::NnResult> {
        let key = (collection.to_string(), field.to_string());
        match self.indices.get(&key) {
            Some(tree) => tree.nearest(lng, lat, k),
            None => Vec::new(),
        }
    }

    /// Number of indexed entries across all collections.
    /// Whether no spatial indices exist.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    pub fn total_entries(&self) -> usize {
        self.indices.values().map(|t| t.len()).sum()
    }

    /// Number of indexed collections.
    pub fn collection_count(&self) -> usize {
        self.indices.len()
    }

    /// Checkpoint all R-trees to bytes for persistence.
    ///
    /// Returns a vec of `(collection, field, rtree_bytes)`.
    pub fn checkpoint_all(&self) -> Vec<(String, String, Vec<u8>)> {
        let mut results = Vec::new();
        for ((collection, field), tree) in &self.indices {
            match tree.checkpoint_to_bytes() {
                Ok(bytes) => results.push((collection.clone(), field.clone(), bytes)),
                Err(e) => {
                    tracing::error!(
                        collection = %collection,
                        field = %field,
                        error = %e,
                        "spatial index checkpoint failed"
                    );
                }
            }
        }
        results
    }

    /// Restore R-trees from checkpoint data.
    ///
    /// Takes a vec of `(collection, field, rtree_bytes)`.
    pub fn restore_all(checkpoints: &[(String, String, Vec<u8>)]) -> Self {
        let mut manager = Self::new();
        for (collection, field, bytes) in checkpoints {
            match RTree::from_checkpoint(bytes) {
                Ok(tree) => {
                    // Rebuild doc_to_entry from restored entries.
                    // Entry IDs are opaque u64s; we reconstruct the mapping
                    // assuming entry.id was originally assigned to collection:doc_id.
                    // Since the R-tree doesn't store doc_ids, we record the
                    // entry_id → (collection, entry_id_as_string) mapping so that
                    // subsequent upserts can remove stale entries.
                    let max_id = tree.entries().iter().map(|e| e.id).max().unwrap_or(0);
                    if max_id >= manager.next_id {
                        manager.next_id = max_id + 1;
                    }

                    // Rebuild doc_to_entry: entry IDs map back to themselves
                    // as synthetic doc keys. The real doc_id mapping is rebuilt
                    // when rebuild_from_documents() is called on cold start.
                    for entry in tree.entries() {
                        let doc_key = (collection.clone(), format!("__entry_{}", entry.id));
                        manager.doc_to_entry.insert(doc_key, entry.id);
                    }

                    manager
                        .indices
                        .insert((collection.clone(), field.clone()), tree);
                }
                Err(e) => {
                    tracing::warn!(
                        collection = %collection,
                        field = %field,
                        error = %e,
                        "spatial index restore failed, will rebuild from documents"
                    );
                }
            }
        }
        manager
    }

    /// Rebuild spatial index from a collection of documents.
    ///
    /// Scans all documents, extracts geometry from the specified field,
    /// and builds the R-tree.
    pub fn rebuild_from_documents(
        &mut self,
        collection: &str,
        field: &str,
        documents: &[(String, Geometry)],
    ) {
        let entries: Vec<RTreeEntry> = documents
            .iter()
            .map(|(doc_id, geom)| {
                let id = self.next_id;
                self.next_id += 1;
                let doc_key = (collection.to_string(), doc_id.clone());
                self.doc_to_entry.insert(doc_key, id);
                RTreeEntry {
                    id,
                    bbox: nodedb_types::geometry_bbox(geom),
                }
            })
            .collect();

        let tree = RTree::bulk_load(entries);
        self.indices
            .insert((collection.to_string(), field.to_string()), tree);
    }
}

impl Default for SpatialIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_and_search() {
        let mut mgr = SpatialIndexManager::new();
        mgr.index_document("places", "location", "doc1", &Geometry::point(10.0, 20.0));
        mgr.index_document("places", "location", "doc2", &Geometry::point(11.0, 21.0));
        mgr.index_document("places", "location", "doc3", &Geometry::point(50.0, 50.0));

        let results = mgr.search(
            "places",
            "location",
            &BoundingBox::new(9.0, 19.0, 12.0, 22.0),
        );
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn upsert_replaces_old_entry() {
        let mut mgr = SpatialIndexManager::new();
        mgr.index_document("places", "loc", "doc1", &Geometry::point(10.0, 20.0));
        mgr.index_document("places", "loc", "doc1", &Geometry::point(50.0, 50.0));

        // Old location should not be found.
        let old = mgr.search("places", "loc", &BoundingBox::new(9.0, 19.0, 12.0, 22.0));
        assert!(old.is_empty());

        // New location should be found.
        let new = mgr.search("places", "loc", &BoundingBox::new(49.0, 49.0, 51.0, 51.0));
        assert_eq!(new.len(), 1);
    }

    #[test]
    fn remove_document() {
        let mut mgr = SpatialIndexManager::new();
        mgr.index_document("places", "loc", "doc1", &Geometry::point(10.0, 20.0));
        mgr.remove_document("places", "loc", "doc1");

        let results = mgr.search("places", "loc", &BoundingBox::new(0.0, 0.0, 180.0, 90.0));
        assert!(results.is_empty());
    }

    #[test]
    fn checkpoint_restore_roundtrip() {
        let mut mgr = SpatialIndexManager::new();
        for i in 0..50 {
            mgr.index_document(
                "buildings",
                "geom",
                &format!("b{i}"),
                &Geometry::point(i as f64 * 0.5, i as f64 * 0.3),
            );
        }

        let checkpoints = mgr.checkpoint_all();
        assert_eq!(checkpoints.len(), 1);

        let restored = SpatialIndexManager::restore_all(&checkpoints);
        assert_eq!(restored.total_entries(), 50);

        let results = restored.search(
            "buildings",
            "geom",
            &BoundingBox::new(-180.0, -90.0, 180.0, 90.0),
        );
        assert_eq!(results.len(), 50);
    }

    #[test]
    fn nearest_neighbor() {
        let mut mgr = SpatialIndexManager::new();
        mgr.index_document("pois", "loc", "a", &Geometry::point(0.0, 0.0));
        mgr.index_document("pois", "loc", "b", &Geometry::point(10.0, 10.0));
        mgr.index_document("pois", "loc", "c", &Geometry::point(1.0, 1.0));

        let nn = mgr.nearest("pois", "loc", 0.5, 0.5, 2);
        assert_eq!(nn.len(), 2);
    }

    #[test]
    fn rebuild_from_documents() {
        let mut mgr = SpatialIndexManager::new();
        let docs: Vec<(String, Geometry)> = (0..100)
            .map(|i| {
                (
                    format!("d{i}"),
                    Geometry::point(i as f64 * 0.1, i as f64 * 0.1),
                )
            })
            .collect();
        mgr.rebuild_from_documents("col", "geom", &docs);
        assert_eq!(mgr.total_entries(), 100);
    }

    #[test]
    fn multiple_collections() {
        let mut mgr = SpatialIndexManager::new();
        mgr.index_document("a", "loc", "d1", &Geometry::point(0.0, 0.0));
        mgr.index_document("b", "loc", "d1", &Geometry::point(50.0, 50.0));

        assert_eq!(mgr.collection_count(), 2);

        let a_results = mgr.search("a", "loc", &BoundingBox::new(-1.0, -1.0, 1.0, 1.0));
        assert_eq!(a_results.len(), 1);

        let b_results = mgr.search("b", "loc", &BoundingBox::new(-1.0, -1.0, 1.0, 1.0));
        assert!(b_results.is_empty());
    }
}
