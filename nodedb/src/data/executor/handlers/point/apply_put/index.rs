// SPDX-License-Identifier: BUSL-1.1

//! Spatial R-tree + columnar ingest side-effect for `apply_point_put`:
//! geometry-field detection, per-field R-tree insert, reverse entry→doc map,
//! and columnar-memtable ingest. HNSW vector indexing lives in the sibling
//! `vector` module. Split out of `apply_put.rs` to keep that file focused on
//! the core document-write transaction.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::spatial_key::SpatialIndexKey;
use crate::engine::document::store::StorageKey;

/// Entry id of one row in a spatial R-tree: `fnv1a_hash` of the row's identity text.
///
/// A document row hashes its rendered storage key; a columnar row hashes its
/// `id` column value. The put side and the remove side both construct this
/// through the matching constructor, so they can never hash the row's
/// identity differently and silently miss each other's entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::data::executor) struct SpatialEntryId(u64);

impl SpatialEntryId {
    /// A document row: the rendered storage key.
    pub fn from_storage_key(key: StorageKey) -> Self {
        Self::from_rendered(&key.to_string())
    }

    /// A document row whose storage key is already rendered — avoids a
    /// second `to_string()` when the caller already holds the text.
    pub fn from_rendered(rendered_key: &str) -> Self {
        Self(crate::util::fnv1a_hash(rendered_key.as_bytes()))
    }

    /// A columnar row: its `id` column value.
    pub fn from_user_id(id: &str) -> Self {
        Self(crate::util::fnv1a_hash(id.as_bytes()))
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl CoreLoop {
    /// Spatial R-tree + columnar ingest side-effect: parse geometry fields,
    /// insert into the per-field R-tree, maintain the reverse entry→doc map,
    /// and (when geometry present) ingest into the columnar memtable so bare
    /// scans/aggregates over spatial collections work.
    ///
    /// Returns the `(spatial_index_key, entry_id)` pairs inserted so a
    /// transactional caller can push `UndoEntry::SpatialInsert` reversals. The
    /// spatial writes are in-memory (an aborted redb txn does not reverse them),
    /// so explicit undo is required. Empty when no geometry fields are present.
    pub(in crate::data::executor) fn apply_point_put_spatial(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        storage_key: StorageKey,
        value: &[u8],
    ) -> Vec<(
        (
            nodedb_types::DatabaseId,
            crate::types::TenantId,
            String,
            String,
        ),
        u64,
    )> {
        // Rendered once here; every reverse-map / hash use below shares it.
        let document_id = storage_key.to_string();
        let document_id = document_id.as_str();
        let spatial_entry_id = SpatialEntryId::from_rendered(document_id);
        let mut inserts = Vec::new();
        // Re-indexing a document must REPLACE, not append: `RTree::insert`
        // blindly pushes a fresh entry even when one with this `entry_id`
        // already exists, so a live geometry UPDATE, a WAL replay, or the
        // crash-recovery rebuild would otherwise leave stale duplicate bbox
        // entries scoring alongside the new one. Clear any prior geometry for
        // this document first (idempotent — a no-op on a genuine first insert).
        // The removed tuples are discarded here, mirroring the vector put path:
        // only the new inserts are captured for transactional undo.
        let _ =
            self.remove_document_spatial_indexes(database_id, tid, collection, spatial_entry_id);
        // Spatial index: detect geometry fields and insert into R-tree.
        // Tries to parse each field as a GeoJSON Geometry — either a native
        // JSON object (schemaless document writes, e.g.
        // `{"type":"Point","coordinates":[...]}`) or a JSON string containing
        // GeoJSON (SQL `ST_Point(...)` inserts, which serialize geometry to a
        // string). See `nodedb_types::geometry::from_geojson_str` — shared
        // with the read path (`extract_geometry` in spatial.rs) and the
        // columnar index path (`geometry_index.rs`); keep all three in sync.
        // If successful, computes bbox and inserts into the per-field R-tree.
        // Also writes the document to columnar_memtables so that bare table scans
        // and aggregates on spatial collections read from columnar (spatial extends columnar).
        //
        // `value` is `apply_point_put`'s incoming body, and the invariant on
        // that function applies unchanged here: geometry is detected by walking
        // a decoded document's fields, so a body that is not one carries no
        // geometry to index and nothing to ingest. This would be wrong if
        // `value` were ever the STORED row instead — a stored geometry that
        // failed to decode would silently drop out of the R-tree while the row
        // stayed queryable, which is the desync the delete-then-insert above
        // exists to prevent.
        if let Ok(doc) = doc_format::decode_document(value)
            && let Some(obj) = doc.as_object()
        {
            let mut has_geometry = false;
            for (field_name, field_value) in obj {
                let parsed_geom = match field_value {
                    serde_json::Value::String(s) => nodedb_types::geometry::from_geojson_str(s),
                    _ => serde_json::from_value::<nodedb_types::geometry::Geometry>(
                        field_value.clone(),
                    )
                    .ok(),
                };
                if let Some(geom) = parsed_geom {
                    has_geometry = true;
                    let bbox = nodedb_types::bbox::geometry_bbox(&geom);
                    let db_id = nodedb_types::DatabaseId::new(database_id);
                    let tid_id = crate::types::TenantId::new(tid);
                    let spatial_key = (db_id, tid_id, collection.to_string(), field_name.clone());
                    let entry_id = spatial_entry_id.as_u64();
                    let memory = nodedb_mem::ScopedMemory::new(
                        self.governor.clone(),
                        db_id,
                        tid_id,
                        nodedb_mem::EngineId::Spatial,
                    );
                    let rtree = self
                        .spatial_indexes
                        .entry(spatial_key.clone())
                        .or_insert_with(|| crate::engine::spatial::RTree::new(memory));
                    rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
                    // Maintain reverse map: entry_id → document_id.
                    self.spatial_doc_map.insert(
                        (
                            db_id,
                            tid_id,
                            collection.to_string(),
                            field_name.clone(),
                            entry_id,
                        ),
                        document_id.to_string(),
                    );
                    inserts.push((spatial_key, entry_id));
                }
            }

            // If document has geometry, also write to columnar memtable.
            // This ensures bare scans + aggregates work via columnar path.
            if has_geometry {
                self.ingest_doc_to_columnar(database_id, tid, collection, obj);
            }
        }

        inserts
    }

    /// Remove every R-tree entry (and its paired `spatial_doc_map` reverse
    /// entry) this document produced across all of the collection's per-field
    /// spatial indexes, keyed by `entry_id` — the same [`SpatialEntryId`] the
    /// insert path constructs. Shared by the PointDelete cascade (which
    /// orphans the geometry of a removed row) and `apply_point_put_spatial`
    /// (which must clear a document's prior geometry before re-inserting,
    /// since `RTree::insert` appends rather than replaces).
    ///
    /// The bbox is read BEFORE the R-tree `delete` (which does not return the
    /// removed geometry) so a transactional caller can push
    /// `UndoEntry::SpatialDelete` re-insert reversals — the reverse
    /// `spatial_doc_map` stores only the doc id. Returns the removed
    /// `(spatial_index_key, entry_id, bbox, document_id)` tuples; empty when the
    /// document had no spatial fields.
    pub(in crate::data::executor) fn remove_document_spatial_indexes(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        entry_id: SpatialEntryId,
    ) -> Vec<(SpatialIndexKey, u64, nodedb_types::BoundingBox, String)> {
        let mut spatial_deletes = Vec::new();
        let entry_id = entry_id.as_u64();
        let db_id = nodedb_types::DatabaseId::new(database_id);
        let tid_id = crate::types::TenantId::new(tid);
        let spatial_fields: Vec<String> = self
            .spatial_indexes
            .keys()
            .filter(|(d, t, c, _)| *d == db_id && *t == tid_id && c == collection)
            .map(|(_, _, _, f)| f.clone())
            .collect();
        for field in spatial_fields {
            let skey = (db_id, tid_id, collection.to_string(), field.clone());
            // Read the bbox BEFORE deleting — the R-tree `delete` does not
            // return the removed geometry, so a reversible undo must capture
            // it here (the reverse `spatial_doc_map` stores only the doc id).
            let bbox = self
                .spatial_indexes
                .get(&skey)
                .and_then(|rtree| rtree.entries().into_iter().find(|e| e.id == entry_id))
                .map(|e| e.bbox);
            if let Some(rtree) = self.spatial_indexes.get_mut(&skey) {
                rtree.delete(entry_id);
            }
            let removed_doc = self.spatial_doc_map.remove(&(
                db_id,
                tid_id,
                collection.to_string(),
                field,
                entry_id,
            ));
            if let (Some(bbox), Some(doc)) = (bbox, removed_doc) {
                spatial_deletes.push((skey, entry_id, bbox, doc));
            }
        }
        spatial_deletes
    }
}

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::{Priority, Request, Status};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::point::put::PointPutExec;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::Surrogate;
    use std::time::{Duration, Instant};

    /// An `ExecutionTask` for a `PointPut` of `document_id` with a raw JSON
    /// document body (`value`) into `collection`, tenant 1 / database DEFAULT.
    fn point_put_task(collection: &str, document_id: &str, value: &[u8]) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
                document_id: document_id.into(),
                value: value.to_vec(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Admitted,
        })
    }

    /// A DOCUMENT-collection insert whose geometry field is a JSON **string**
    /// containing GeoJSON — the exact shape SQL `ST_Point(...)` inserts
    /// produce, as opposed to a GeoJSON **object**. `apply_point_put_spatial`
    /// must detect both shapes, or this insert never populates
    /// `spatial_indexes` (O(n) full-scan fallback instead of
    /// the R-tree). This is a raw JSON document body (not msgpack) so
    /// `doc_format::decode_document`'s JSON fallback path is exercised, same
    /// as documents freshly inserted via SQL before any msgpack re-encode.
    #[test]
    fn sql_geometry_string_field_is_rtree_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let doc = br#"{"loc":"{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}"}"#;
        let task = point_put_task("docs", "d1", doc);
        let resp = core.execute_point_put(
            &task,
            PointPutExec {
                tid: 1,
                collection: "docs",
                document_id: "d1",
                surrogate: Surrogate::ZERO,
                value: doc,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(resp.status, Status::Ok);

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "docs".to_string(),
            "loc".to_string(),
        );
        assert!(
            core.spatial_indexes.contains_key(&key),
            "SQL-inserted (string-form) geometry must be R-tree-indexed, \
             not left to O(n) full-scan; spatial_indexes keys: {:?}",
            core.spatial_indexes.keys().collect::<Vec<_>>()
        );
        let rtree = core.spatial_indexes.get(&key).unwrap();
        assert_eq!(
            rtree.entries().len(),
            1,
            "exactly one R-tree entry expected for the single inserted document"
        );
    }

    /// Parity: an object-form GeoJSON field (schemaless doc write) is indexed
    /// identically to the string form above — same key, one entry.
    #[test]
    fn object_geometry_field_is_rtree_indexed_parity() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let doc = br#"{"loc":{"type":"Point","coordinates":[1.0,2.0]}}"#;
        let task = point_put_task("docs_obj", "d1", doc);
        let resp = core.execute_point_put(
            &task,
            PointPutExec {
                tid: 1,
                collection: "docs_obj",
                document_id: "d1",
                surrogate: Surrogate::ZERO,
                value: doc,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(resp.status, Status::Ok);

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "docs_obj".to_string(),
            "loc".to_string(),
        );
        assert!(core.spatial_indexes.contains_key(&key));
        assert_eq!(core.spatial_indexes.get(&key).unwrap().entries().len(), 1);
    }
}
