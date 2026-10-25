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
//! 8-char hex string that `StorageKey::for_surrogate(surrogate)` produces,
//! which is what the scan path parses via `u32::from_str_radix(doc_id, 16)`.
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
use crate::engine::document::store::StorageKey;
use crate::engine::spatial::{RTree, RTreeEntry, TreeSrid};
use crate::types::TenantId;
use crate::util::fnv1a_hash;
use nodedb_types::Surrogate;
use nodedb_types::geometry::SridGeometry;
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
    pub geometry: &'a SridGeometry,
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
        if let Some(refusal) =
            super::unbound_surrogate::refuse_unbound("spatial", collection, surrogate)
        {
            return self.response_error(task, refusal);
        }
        let doc_id = StorageKey::for_surrogate(surrogate).to_string();

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

        // ── SRID check, before any write ────────────────────────────────────
        let db_id = task.request.database_id;
        let tenant_id = TenantId::new(tid);
        let spatial_key = (db_id, tenant_id, collection.to_string(), field.to_string());
        if let Some(rtree) = self.spatial_indexes.get(&spatial_key)
            && !rtree.accepts_srid(geometry.srid)
        {
            let mismatch = crate::engine::spatial::SridMismatch {
                index: rtree.srid(),
                entry: geometry.srid,
            };
            return self.response_error(
                task,
                super::spatial_refine::mixed_srid_error(collection, field, mismatch),
            );
        }

        // The geometry's SRID resolves before any write: an SRID no CRS
        // defines fails the op with nothing stored.
        let crs = self.crs_context();
        let resolved = TreeSrid::resolve(geometry.srid, &crs)
            .and_then(|tree_srid| geometry.bbox(&crs).map(|bbox| (tree_srid, bbox)));
        let (tree_srid, bbox) = match resolved {
            Ok(resolved) => resolved,
            Err(e) => return self.response_error(task, crate::Error::from(e)),
        };

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

        let memory = nodedb_mem::ScopedMemory::new(
            self.governor.clone(),
            db_id,
            tenant_id,
            nodedb_mem::EngineId::Spatial,
        );
        let rtree = self
            .spatial_indexes
            .entry(spatial_key)
            .or_insert_with(|| RTree::new(memory, tree_srid));
        // The SRID check above admitted this SRID; the stale-entry delete can
        // only empty the tree.
        if let Err(mismatch) = rtree.bind_srid(tree_srid) {
            return self.response_error(
                task,
                super::spatial_refine::mixed_srid_error(collection, field, mismatch),
            );
        }
        // An EMPTY geometry has no extent: its row is stored, not indexed.
        if !geometry.is_empty() {
            rtree.insert(RTreeEntry { id: entry_id, bbox });
            self.spatial_doc_map.insert(doc_map_key, doc_id);
        }

        // Advance the collection floor for this committed spatial write.
        self.note_collection_write(task, collection);

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
    /// of a non-existent surrogate is a no-op (idempotent). `None` names a
    /// key its home never bound: the delete removes nothing and still
    /// commits the producer's sequence.
    ///
    /// Fails fast if the sparse store delete returns an error (other than
    /// "not found", which the sparse engine surfaces as `Ok`).
    pub(in crate::data::executor) fn execute_spatial_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field: &str,
        surrogate: Option<Surrogate>,
        provenance: Option<&SyncProvenance>,
    ) -> Response {
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

        let Some(surrogate) = surrogate else {
            if let Some(prov) = provenance {
                self.sync_commit(prov);
                return self.sync_ack_response(task, AckStatus::Applied, prov.seq);
            }
            return self.response_ok(task);
        };

        let doc_id = StorageKey::for_surrogate(surrogate).to_string();
        debug!(
            core = self.core_id,
            %collection,
            %field,
            doc_id = %doc_id,
            surrogate = surrogate.as_u32(),
            "spatial sync: delete geometry"
        );

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
        self.note_collection_write(task, collection);

        if let Some(prov) = provenance {
            self.sync_commit(prov);
            return self.sync_ack_response(task, AckStatus::Applied, prov.seq);
        }

        self.response_ok(task)
    }
}

/// The document field value of a synced geometry: the native geometry,
/// which the standard-msgpack document stores as the geometry ext (SRID,
/// Z and M kept).
///
/// Shared with the in-transaction staging path
/// (`transaction::stage_write::stage_spatial`), which stages the identical
/// `{field: geometry, "id": surrogate_hex}` document shape as an overlay
/// `Put` body.
pub(in crate::data::executor) fn geometry_to_value(geometry: &SridGeometry) -> nodedb_types::Value {
    nodedb_types::Value::Geometry(geometry.clone())
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::{SyncAckResult, SyncOutcome};

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use nodedb_types::geometry::Geometry;

    const TID: u64 = 1;

    fn prov(seq: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id: 7,
            epoch: 1,
            stream_id: 42,
            seq,
        }
    }

    fn assert_applied(response: &Response, seq: u64) {
        assert_eq!(response.status, Status::Ok);
        let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).expect("sync ack");
        assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Applied));
        assert_eq!(ack.applied_seq, seq);
    }

    fn insert(core: &mut CoreLoop, task: &ExecutionTask, surrogate: Surrogate) -> Response {
        core.execute_spatial_insert(SpatialInsertExec {
            task,
            tid: TID,
            collection: "places",
            field: "loc",
            surrogate,
            geometry: &SridGeometry::wgs84(Geometry::point(10.0, 20.0)),
            provenance: None,
        })
    }

    /// One geometry under a bound surrogate.
    fn insert_geometries(core: &mut CoreLoop, task: &ExecutionTask) {
        assert_eq!(insert(core, task, Surrogate::new(5)).status, Status::Ok);
    }

    /// The indexed entry count and the doc-map size of `places.loc`.
    fn spatial_state(core: &CoreLoop) -> (usize, usize) {
        let key = (
            nodedb_types::DatabaseId::DEFAULT,
            TenantId::new(TID),
            "places".to_string(),
            "loc".to_string(),
        );
        let indexed = core.spatial_indexes.get(&key).map_or(0, RTree::len);
        (indexed, core.spatial_doc_map.len())
    }

    #[test]
    fn an_insert_without_a_surrogate_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();

        let response = insert(&mut core, &task, Surrogate::ZERO);
        assert!(matches!(
            response.error_code.as_deref(),
            Some(crate::bridge::envelope::ErrorCode::RejectedPrevalidation { .. })
        ));
        assert_eq!(spatial_state(&core), (0, 0), "nothing is stored");
    }

    #[test]
    fn unbound_delete_with_provenance_commits_the_sequence_and_removes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        insert_geometries(&mut core, &task);
        let before = spatial_state(&core);

        let response =
            core.execute_spatial_delete(&task, TID, "places", "loc", None, Some(&prov(1)));
        assert_applied(&response, 1);
        assert_eq!(core.sync_hwm_value(7, 42), 1, "the sequence commits");
        assert_eq!(spatial_state(&core), before, "no geometry is removed");
    }

    #[test]
    fn unbound_delete_without_provenance_is_ok_and_removes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        insert_geometries(&mut core, &task);
        let before = spatial_state(&core);

        let response = core.execute_spatial_delete(&task, TID, "places", "loc", None, None);
        assert_eq!(response.status, Status::Ok);
        assert!(response.payload.is_empty());
        assert_eq!(spatial_state(&core), before);
    }
}
