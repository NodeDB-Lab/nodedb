// SPDX-License-Identifier: BUSL-1.1

//! Spatial R-tree side-effect for `apply_point_put`: geometry-field
//! detection, per-field R-tree insert, and the reverse entry→doc map. HNSW
//! vector indexing lives in the sibling `vector` module.
//!
//! A document collection keeps its rows in the sparse store only. Its
//! scans, aggregates and joins read them there, so every document is visible
//! and every delete is honored. The R-tree is the one derived structure a
//! geometry field adds.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
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
    /// Spatial R-tree side-effect: parse geometry fields, insert into the
    /// per-field R-tree, and maintain the reverse entry→doc map.
    ///
    /// The spatial writes are in memory, so an aborted redb txn does not
    /// reverse them. Pushes onto `undo` a `SpatialDelete` for each prior entry
    /// of the document it removes, then a `SpatialInsert` for each entry it
    /// inserts. Rollback runs the log in reverse, so it removes the new
    /// entries before it puts the prior ones back.
    pub(in crate::data::executor) fn apply_point_put_spatial(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        storage_key: StorageKey,
        value: &[u8],
        undo: &mut Vec<UndoEntry>,
    ) {
        // Rendered once here; every reverse-map / hash use below shares it.
        let document_id = storage_key.to_string();
        let document_id = document_id.as_str();
        let spatial_entry_id = SpatialEntryId::from_rendered(document_id);
        // Spatial index: detect geometry fields. Tries to parse each field as
        // a GeoJSON Geometry — either a native JSON object (schemaless
        // document writes, e.g. `{"type":"Point","coordinates":[...]}`) or a
        // JSON string containing GeoJSON (SQL `ST_Point(...)` inserts, which
        // serialize geometry to a string). See
        // `nodedb_types::geometry::from_geojson_str` — shared with the read
        // path (`extract_geometry` in spatial.rs) and the columnar index path
        // (`geometry_index.rs`); keep all three in sync.
        //
        // `value` is `apply_point_put`'s incoming body, and the invariant on
        // that function applies unchanged here: geometry is detected by walking
        // a decoded document's fields, so a body that is not one carries no
        // geometry to index. This would be wrong if
        // `value` were ever the STORED row instead — a stored geometry that
        // failed to decode would silently drop out of the R-tree while the row
        // stayed queryable, which is the desync the delete-then-insert below
        // exists to prevent.
        let mut geometries = Vec::new();
        if let Ok(doc) = doc_format::decode_document(value)
            && let Some(obj) = doc.as_object()
        {
            for (field_name, field_value) in obj {
                let parsed_geom = match field_value {
                    serde_json::Value::String(s) => nodedb_types::geometry::from_geojson_str(s),
                    _ => serde_json::from_value::<nodedb_types::geometry::Geometry>(
                        field_value.clone(),
                    )
                    .ok(),
                };
                if let Some(geom) = parsed_geom {
                    geometries.push((field_name.clone(), geom));
                }
            }
        }

        // Re-indexing a document must REPLACE, not append: `RTree::insert`
        // blindly pushes a fresh entry even when one with this `entry_id`
        // already exists, so a live geometry UPDATE, a WAL replay, or the
        // crash-recovery rebuild would otherwise leave stale duplicate bbox
        // entries scoring alongside the new one. Clear any prior geometry for
        // this document first (idempotent — a no-op on a genuine first insert).
        self.remove_document_spatial_indexes_with_undo(
            database_id,
            tid,
            collection,
            spatial_entry_id,
            undo,
        );
        let db_id = nodedb_types::DatabaseId::new(database_id);
        let tid_id = crate::types::TenantId::new(tid);
        let entry_id = spatial_entry_id.as_u64();
        for (field_name, geom) in geometries {
            let bbox = nodedb_types::bbox::geometry_bbox(&geom);
            let spatial_key = (db_id, tid_id, collection.to_string(), field_name.clone());
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
                (db_id, tid_id, collection.to_string(), field_name, entry_id),
                document_id.to_string(),
            );
            undo.push(UndoEntry::SpatialInsert {
                key: spatial_key,
                entry_id,
            });
        }
    }

    /// [`Self::remove_document_spatial_indexes`], pushing a `SpatialDelete`
    /// undo entry onto `undo` for each entry it removes.
    pub(in crate::data::executor) fn remove_document_spatial_indexes_with_undo(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        entry_id: SpatialEntryId,
        undo: &mut Vec<UndoEntry>,
    ) {
        let removed = self.remove_document_spatial_indexes(database_id, tid, collection, entry_id);
        for (key, entry_id, bbox, document_id) in removed {
            undo.push(UndoEntry::SpatialDelete {
                key,
                entry_id,
                bbox,
                document_id,
            });
        }
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
                surrogate: Surrogate::new(1),
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
            commit_hlc: None,
            entry_version: None,
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
                surrogate: Surrogate::new(1),
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
                surrogate: Surrogate::new(1),
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

    /// A schemaless collection whose geometry documents carry different value
    /// types for the same field, or no `id` field, accepts every write. A
    /// collection scan returns every document, including one with no
    /// geometry, from the sparse store.
    #[test]
    fn schemaless_geometry_docs_with_mixed_field_types_are_all_scanned() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let coll = "geo_mixed";
        let docs: [(&str, u32, &[u8]); 4] = [
            (
                "a",
                1,
                br#"{"id":"a","loc":{"type":"Point","coordinates":[1.0,2.0]},"n":1}"#,
            ),
            (
                "b",
                2,
                br#"{"id":"b","loc":{"type":"Point","coordinates":[3.0,4.0]},"n":1.5}"#,
            ),
            (
                "c",
                3,
                br#"{"loc":{"type":"Point","coordinates":[5.0,6.0]},"n":"text"}"#,
            ),
            ("d", 4, br#"{"id":"d","n":2}"#),
        ];
        for (document_id, surrogate, doc) in docs {
            let task = point_put_task(coll, document_id, doc);
            let resp = core.execute_point_put(
                &task,
                PointPutExec {
                    tid: 1,
                    collection: coll,
                    document_id,
                    surrogate: Surrogate::new(surrogate),
                    value: doc,
                    returning: None,
                    rls_filters: &[],
                    resolved_sum_targets: &[],
                },
            );
            assert_eq!(resp.status, Status::Ok, "put of '{document_id}' refused");
        }

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            coll.to_string(),
            "loc".to_string(),
        );
        assert_eq!(core.spatial_indexes.get(&key).unwrap().entries().len(), 3);
        assert!(
            !core.columnar_engines.contains_key(&(
                DatabaseId::DEFAULT,
                TenantId::new(1),
                coll.to_string()
            )),
            "a document collection keeps no columnar copy of its rows"
        );

        let scanned = core
            .scan_collection(DatabaseId::DEFAULT.as_u64(), 1, coll, usize::MAX)
            .unwrap();
        assert_eq!(scanned.len(), 4, "every document must be scanned");
    }

    /// A put that overwrites a geometry row and then aborts puts the row's
    /// prior R-tree entry and its `spatial_doc_map` record back.
    #[test]
    fn an_aborted_geometry_overwrite_restores_the_prior_entry() {
        use crate::data::executor::enforcement::chain_guard::{AbandonedWrite, abandon_write};
        use crate::data::executor::enforcement::unique::UniqueJudge;
        use crate::data::executor::handlers::point::apply_put::{PointPutParams, SpatialEntryId};
        use crate::engine::document::store::StorageKey;

        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let coll = "geo_abort";
        let db = DatabaseId::DEFAULT.as_u64();
        let surrogate = Surrogate::new(1);
        let storage_key = StorageKey::for_surrogate(surrogate);
        let entry_id = SpatialEntryId::from_storage_key(storage_key).as_u64();
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            coll.to_string(),
            "loc".to_string(),
        );
        let map_key = (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            coll.to_string(),
            "loc".to_string(),
            entry_id,
        );

        let old = br#"{"loc":{"type":"Point","coordinates":[1.0,2.0]}}"#;
        let task = point_put_task(coll, "d1", old);
        let resp = core.execute_point_put(
            &task,
            PointPutExec {
                tid: 1,
                collection: coll,
                document_id: "d1",
                surrogate,
                value: old,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(resp.status, Status::Ok);
        let old_entries = core.spatial_indexes.get(&key).unwrap().entries();
        assert_eq!(old_entries.len(), 1);
        let old_bbox = old_entries[0].bbox;
        let old_doc = core.spatial_doc_map.get(&map_key).cloned();
        assert!(old_doc.is_some());

        let new = br#"{"loc":{"type":"Point","coordinates":[30.0,40.0]}}"#;
        let txn = core.sparse.begin_write().unwrap();
        let outcome = core
            .apply_point_put(
                &txn,
                PointPutParams {
                    database_id: db,
                    tid: 1,
                    collection: coll,
                    storage_key,
                    surrogate,
                    value: new,
                    index_text: true,
                    user_roles: &[],
                    enforce: true,
                    unique: UniqueJudge::Row,
                    wal_lsn: None,
                    resolved_targets: &[],
                },
            )
            .unwrap();
        let new_entries = core.spatial_indexes.get(&key).unwrap().entries();
        assert_eq!(new_entries.len(), 1);
        assert_ne!(
            new_entries[0].bbox, old_bbox,
            "the overwrite replaced the entry"
        );

        drop(txn);
        let error = abandon_write(
            &mut core,
            AbandonedWrite::row(db, 1, coll, &storage_key).undo(outcome.memory_undo),
            crate::Error::Storage {
                engine: "sparse".into(),
                detail: "commit refused".into(),
            },
        );
        assert!(
            matches!(error, crate::Error::Storage { .. }),
            "a clean undo reports the original error, got {error:?}"
        );

        let restored = core.spatial_indexes.get(&key).unwrap().entries();
        assert_eq!(restored.len(), 1, "exactly the prior entry is back");
        assert_eq!(restored[0].id, entry_id);
        assert_eq!(restored[0].bbox, old_bbox);
        assert_eq!(core.spatial_doc_map.get(&map_key).cloned(), old_doc);
    }
}
