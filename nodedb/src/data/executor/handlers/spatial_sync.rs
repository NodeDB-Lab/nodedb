// SPDX-License-Identifier: BUSL-1.1

//! Spatial sync ingest handlers: insert/delete geometries into the R-tree
//! on behalf of a Lite sync client.
//!
//! Called by `dispatch_spatial` when the plan variant is
//! `SpatialOp::Insert` or `SpatialOp::Delete`.
//!
//! ## Surrogate-keyed storage
//!
//! Both the R-tree entry and the sparse document body are keyed by the
//! hex-encoded surrogate (assigned on the Control Plane before dispatch).
//! This matches the direct `INSERT INTO ... VALUES (ST_GeomFromText(...))`
//! path so cross-engine prefilter (roaring-bitmap intersect against the
//! surrogate space) just works — `spatial_doc_map` stores the same
//! 8-char hex string that `surrogate_to_doc_id(surrogate)` produces, which
//! is what the scan path parses via `u32::from_str_radix(doc_id, 16)`.
//!
//! ## Document-store parity
//!
//! Origin spatial scans read document bytes from the sparse store to apply
//! exact OGC predicates. When syncing from Lite, we therefore also write a
//! minimal geometry document (`{field: <geometry>, "id": surrogate_hex}`)
//! to the sparse engine in addition to the R-tree entry. Any failure to
//! materialise that body is a hard error (no silent partial-success); the
//! whole op fails and the Control Plane converts it to a rejection ACK.

use tracing::{debug, error};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::sync_gate::{SyncAdmit, ack_status_from_admit};
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use crate::engine::spatial::{RTree, RTreeEntry};
use crate::types::TenantId;
use crate::util::fnv1a_hash;
use nodedb_types::Surrogate;
use nodedb_types::bbox::geometry_bbox;
use nodedb_types::geometry::Geometry;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

/// Parameters for a spatial geometry insert on the Data Plane.
///
/// Bundles the non-`self` arguments to `execute_spatial_insert` so the
/// method stays within the argument-count limit.
pub(in crate::data::executor) struct SpatialInsertExec<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub surrogate: Surrogate,
    pub geometry: &'a Geometry,
    pub provenance: Option<&'a SyncProvenance>,
}

impl CoreLoop {
    /// Insert a geometry into the per-field R-tree and the sparse document store
    /// on behalf of a Lite client.
    ///
    /// Both writes use the hex-encoded surrogate as the storage key so
    /// cross-engine surrogate prefilters resolve without translation.
    ///
    /// Fails fast (returns a Response::Error) if either:
    /// - the geometry document cannot be msgpack-serialised, or
    /// - the sparse document write fails.
    ///
    /// If an entry with the same surrogate already exists, the R-tree entry is
    /// replaced (upsert semantics).
    pub(in crate::data::executor) fn execute_spatial_insert(
        &mut self,
        args: SpatialInsertExec<'_>,
    ) -> Response {
        let SpatialInsertExec {
            task,
            tid,
            collection,
            field,
            surrogate,
            geometry,
            provenance,
        } = args;
        let doc_id = surrogate_to_doc_id(surrogate);

        debug!(
            core = self.core_id,
            %collection,
            %field,
            doc_id = %doc_id,
            surrogate = surrogate.as_u32(),
            "spatial sync: insert geometry"
        );

        // ── Idempotency gate ────────────────────────────────────────────────
        if let Some(prov) = provenance {
            match self.sync_admit(prov) {
                SyncAdmit::Apply => {
                    // Fall through to engine write below; commit after.
                }
                admit @ (SyncAdmit::Duplicate | SyncAdmit::Fenced | SyncAdmit::Gap { .. }) => {
                    let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&admit),
                        applied_seq,
                    );
                }
            }
        }

        // ── 1. Serialise + write minimal geometry document to sparse store ──
        let geom_value = geometry_to_value(geometry);
        let mut doc_map = std::collections::HashMap::new();
        doc_map.insert(field.to_string(), geom_value);
        doc_map.insert(
            "id".to_string(),
            nodedb_types::Value::String(doc_id.clone()),
        );
        let doc_value = nodedb_types::Value::Object(doc_map);
        // Use standard msgpack map format so scan_collection / decode_document_value
        // can read it back. zerompk::to_msgpack_vec encodes Value in a tagged array
        // format the document scan path does not understand.
        let msgpack = match nodedb_types::value_to_msgpack(&doc_value) {
            Ok(b) => b,
            Err(e) => {
                error!(
                    core = self.core_id,
                    %collection,
                    %field,
                    doc_id = %doc_id,
                    error = %e,
                    "spatial sync: geometry document msgpack serialisation failed"
                );
                return self.response_error(
                    task,
                    crate::Error::Internal {
                        detail: format!("spatial sync: serialise geometry document: {e}"),
                    },
                );
            }
        };

        let storage_key = nodedb_types::StorageKey::for_surrogate(surrogate);
        if let Err(e) = self.sparse.put(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &storage_key,
            &msgpack,
        ) {
            error!(
                core = self.core_id,
                %collection,
                doc_id = %doc_id,
                error = %e,
                "spatial sync: sparse document write failed"
            );
            return self.response_error(task, e);
        }

        // ── 2. Update R-tree ────────────────────────────────────────────────
        let db_id = task.request.database_id;
        let tenant_id = TenantId::new(tid);
        let spatial_key = (db_id, tenant_id, collection.to_string(), field.to_string());
        let entry_id = fnv1a_hash(doc_id.as_bytes());
        let doc_map_key = (
            db_id,
            tenant_id,
            collection.to_string(),
            field.to_string(),
            entry_id,
        );

        // If the surrogate was previously inserted, remove the stale entry
        // first so the R-tree doesn't accumulate duplicate bboxes.
        if let Some(rtree) = self.spatial_indexes.get_mut(&spatial_key)
            && self.spatial_doc_map.contains_key(&doc_map_key)
        {
            rtree.delete(entry_id);
        }

        let bbox = geometry_bbox(geometry);
        let memory = nodedb_mem::ScopedMemory::new(
            self.governor.clone(),
            db_id,
            tenant_id,
            nodedb_mem::EngineId::Spatial,
        );
        let rtree = self
            .spatial_indexes
            .entry(spatial_key)
            .or_insert_with(|| RTree::new(memory));
        rtree.insert(RTreeEntry { id: entry_id, bbox });
        self.spatial_doc_map.insert(doc_map_key, doc_id);

        // Advance the collection floor for this committed spatial write.
        self.note_collection_write_lsn(task, collection);

        // Advance HWM and return gate result payload when provenance is present.
        if let Some(prov) = provenance {
            self.sync_commit(prov);
            return self.sync_ack_response(task, AckStatus::Applied, prov.seq);
        }

        self.response_ok(task)
    }

    /// Remove a document's geometry from the per-field R-tree and the sparse
    /// document store on behalf of a Lite client.
    ///
    /// Keyed by the same hex-encoded surrogate used at insert time. A delete
    /// of a non-existent surrogate is a no-op (idempotent).
    ///
    /// Fails fast if the sparse store delete returns an error (other than
    /// "not found", which the sparse engine surfaces as `Ok`).
    pub(in crate::data::executor) fn execute_spatial_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field: &str,
        surrogate: Surrogate,
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        let doc_id = surrogate_to_doc_id(surrogate);

        debug!(
            core = self.core_id,
            %collection,
            %field,
            doc_id = %doc_id,
            surrogate = surrogate.as_u32(),
            "spatial sync: delete geometry"
        );

        // ── Idempotency gate ────────────────────────────────────────────────
        if let Some(prov) = provenance {
            match self.sync_admit(prov) {
                SyncAdmit::Apply => {}
                admit @ (SyncAdmit::Duplicate | SyncAdmit::Fenced | SyncAdmit::Gap { .. }) => {
                    let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&admit),
                        applied_seq,
                    );
                }
            }
        }

        let storage_key = nodedb_types::StorageKey::for_surrogate(surrogate);
        if let Err(e) = self.sparse.delete(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &storage_key,
        ) {
            error!(
                core = self.core_id,
                %collection,
                doc_id = %doc_id,
                error = %e,
                "spatial sync: sparse document delete failed"
            );
            return self.response_error(task, e);
        }

        let db_id = task.request.database_id;
        let tenant_id = TenantId::new(tid);
        let spatial_key = (db_id, tenant_id, collection.to_string(), field.to_string());
        let entry_id = fnv1a_hash(doc_id.as_bytes());
        let doc_map_key = (
            db_id,
            tenant_id,
            collection.to_string(),
            field.to_string(),
            entry_id,
        );

        if let Some(rtree) = self.spatial_indexes.get_mut(&spatial_key) {
            rtree.delete(entry_id);
        }
        self.spatial_doc_map.remove(&doc_map_key);

        // Advance the collection floor for this committed spatial delete.
        self.note_collection_write_lsn(task, collection);

        if let Some(prov) = provenance {
            self.sync_commit(prov);
            return self.sync_ack_response(task, AckStatus::Applied, prov.seq);
        }

        self.response_ok(task)
    }
}

/// Convert a `Geometry` into a `nodedb_types::Value` for msgpack serialisation.
///
/// Produces a JSON-compatible nested structure matching what a pgwire INSERT
/// with a GeoJSON geometry value would produce.
///
/// Shared with the in-transaction staging path
/// (`transaction::stage_write::stage_spatial`), which stages the identical
/// `{field: geometry, "id": surrogate_hex}` document shape as an overlay
/// `Put` body.
pub(in crate::data::executor) fn geometry_to_value(geometry: &Geometry) -> nodedb_types::Value {
    use nodedb_types::Value;
    use nodedb_types::geometry::Geometry::*;

    match geometry {
        Point { coordinates } => {
            let mut m = std::collections::HashMap::new();
            m.insert("type".to_string(), Value::String("Point".to_string()));
            m.insert(
                "coordinates".to_string(),
                Value::Array(vec![
                    Value::Float(coordinates[0]),
                    Value::Float(coordinates[1]),
                ]),
            );
            Value::Object(m)
        }
        LineString { coordinates } => {
            let mut m = std::collections::HashMap::new();
            m.insert("type".to_string(), Value::String("LineString".to_string()));
            m.insert(
                "coordinates".to_string(),
                Value::Array(
                    coordinates
                        .iter()
                        .map(|c| Value::Array(vec![Value::Float(c[0]), Value::Float(c[1])]))
                        .collect(),
                ),
            );
            Value::Object(m)
        }
        Polygon { coordinates } => {
            let mut m = std::collections::HashMap::new();
            m.insert("type".to_string(), Value::String("Polygon".to_string()));
            m.insert(
                "coordinates".to_string(),
                Value::Array(
                    coordinates
                        .iter()
                        .map(|ring| {
                            Value::Array(
                                ring.iter()
                                    .map(|c| {
                                        Value::Array(vec![Value::Float(c[0]), Value::Float(c[1])])
                                    })
                                    .collect(),
                            )
                        })
                        .collect(),
                ),
            );
            Value::Object(m)
        }
        // For other geometry types, fall back to JSON serialisation then
        // parse as Value. These are less common in sync workloads.
        other => match sonic_rs::to_string(other) {
            Ok(json) => match sonic_rs::from_str::<serde_json::Value>(&json) {
                Ok(v) => nodedb_types::conversion::json_to_value(v),
                Err(_) => Value::Null,
            },
            Err(_) => Value::Null,
        },
    }
}
