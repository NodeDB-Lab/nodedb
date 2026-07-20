// SPDX-License-Identifier: BUSL-1.1

//! Spatial query handler: R-tree index scan with predicate refinement.
//!
//! Documents with geometry fields are auto-indexed into per-field R-trees
//! on insert (see `handlers/point.rs`). Spatial queries use the R-tree for
//! fast bbox candidate selection, then refine with exact predicates.
//!
//! Internal document representation: `nodedb_types::Value` (no JSON intermediary).

use tracing::debug;

use super::super::response_codec;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::SpatialPredicate;
use nodedb_types::{Surrogate, SurrogateBitmap};

use super::spatial_refine::{apply_predicate, expand_bbox, extract_geometry, project_doc};

/// Parameters for [`CoreLoop::execute_spatial_scan`].
pub(in crate::data::executor) struct SpatialScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub predicate: &'a SpatialPredicate,
    pub query_geometry: &'a nodedb_types::geometry::Geometry,
    pub distance_meters: f64,
    pub attribute_filters: &'a [u8],
    pub limit: usize,
    pub projection: &'a [String],
    pub rls_filters: &'a [u8],
    pub prefilter: Option<&'a SurrogateBitmap>,
}

/// Parameters for [`CoreLoop::spatial_full_scan`].
struct SpatialFullScanParams<'a> {
    task: &'a ExecutionTask,
    tid: u64,
    collection: &'a str,
    field: &'a str,
    predicate: &'a SpatialPredicate,
    query_geom: &'a nodedb_types::geometry::Geometry,
    distance_meters: f64,
    limit: usize,
    projection: &'a [String],
    attr_filters: &'a [ScanFilter],
    rls_filters: &'a [ScanFilter],
    prefilter: Option<&'a SurrogateBitmap>,
}

impl CoreLoop {
    /// Execute a spatial scan using the R-tree index.
    ///
    /// 1. Parse query geometry from GeoJSON bytes
    /// 2. R-tree range search for bbox candidates
    /// 3. Exact predicate refinement (extract geometry, apply ST_*)
    /// 4. Return matching documents up to limit
    pub(in crate::data::executor) fn execute_spatial_scan(
        &mut self,
        params: SpatialScanParams<'_>,
    ) -> Response {
        let SpatialScanParams {
            task,
            tid,
            collection,
            field,
            predicate,
            query_geometry,
            distance_meters,
            attribute_filters,
            limit,
            projection,
            rls_filters,
            prefilter,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %field,
            predicate = ?predicate,
            "spatial scan"
        );

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        // The query geometry was parsed and validated on the Control Plane.
        let query_geom = query_geometry;

        // 2. Deserialize attribute and RLS filters.
        let attr_filters: Vec<ScanFilter> = if attribute_filters.is_empty() {
            Vec::new()
        } else {
            zerompk::from_msgpack(attribute_filters).unwrap_or_default()
        };
        let row_level_filters: Vec<ScanFilter> = if rls_filters.is_empty() {
            Vec::new()
        } else {
            zerompk::from_msgpack(rls_filters).unwrap_or_default()
        };

        // 3. Compute search bbox (expand by distance for ST_DWithin).
        let query_bbox = nodedb_types::bbox::geometry_bbox(query_geom);
        let search_bbox = if distance_meters > 0.0 {
            expand_bbox(&query_bbox, distance_meters)
        } else {
            query_bbox
        };

        let db_id = task.request.database_id;
        let tid_id = crate::types::TenantId::new(tid);
        let spatial_key = (db_id, tid_id, collection.to_string(), field.to_string());
        let has_index = self.spatial_indexes.contains_key(&spatial_key);
        let limit = if limit == 0 { 1000 } else { limit };

        // No R-tree: full scan with predicate post-filter.
        if !has_index {
            return self.spatial_full_scan(SpatialFullScanParams {
                task,
                tid,
                collection,
                field,
                predicate,
                query_geom,
                distance_meters,
                limit,
                projection,
                attr_filters: &attr_filters,
                rls_filters: &row_level_filters,
                prefilter,
            });
        }

        let coll_key = (db_id, tid_id, collection.to_string());

        let rtree = match self.spatial_indexes.get(&spatial_key) {
            Some(rt) => rt,
            None => {
                return match response_codec::encode_value_vec(&[]) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        };

        // 4. R-tree range search → candidate entry IDs.
        let candidates = rtree.search(&search_bbox);
        debug!(
            core = self.core_id,
            candidates = candidates.len(),
            "spatial R-tree candidates"
        );

        // Candidate documents are fetched per-candidate in an ENGINE-AWARE way
        // below. `spatial_doc_map` keys candidates differently per engine:
        //   * Document collections (schemaless/strict) keep their rows in the
        //     sparse engine keyed by the hex surrogate, and the doc-map records
        //     that same hex surrogate (see `apply_point_put_spatial`).
        //   * The `spatial` / columnar-family engine keeps its rows in columnar
        //     keyed by the user `id` column, and the doc-map records that user
        //     id (see `index_columnar_geometry_columns`).
        // The refinement fetch below tries sparse first (the document-collection
        // case) and, on a miss, resolves the candidate from a columnar id → doc
        // map built once via `scan_collection` (the columnar-family case). This
        // mirrors `scan_collection`'s own presence-based (columnar → sparse)
        // routing and is correct regardless of which engine backs the row.
        let database_id = db_id.as_u64();
        let strict_schema = self.strict_schema_for(db_id, tid_id, collection);

        // Lazily-built columnar id → document map, populated on the first
        // candidate that is absent from the sparse store (i.e. a columnar-family
        // collection). Never built for pure document collections.
        let mut columnar_docs: Option<std::collections::HashMap<String, Vec<u8>>> = None;

        // 5. Exact predicate refinement.
        let mut results = Vec::new();

        for entry in &candidates {
            if results.len() >= limit {
                break;
            }

            let doc_id = match self.spatial_doc_map.get(&(
                db_id,
                tid_id,
                collection.to_string(),
                field.to_string(),
                entry.id,
            )) {
                Some(id) => id.clone(),
                None => continue,
            };

            // Prefilter: skip candidates not in the surrogate bitmap before
            // any geometry evaluation. The doc_id is a hex-encoded surrogate.
            if let Some(bitmap) = prefilter {
                match u32::from_str_radix(&doc_id, 16) {
                    Ok(raw) => {
                        if !bitmap.contains(Surrogate(raw)) {
                            continue;
                        }
                    }
                    Err(_) => continue,
                }
            }

            // Fetch the candidate's document in an engine-aware way. A sparse
            // hit is the document-collection case (doc-map id is the hex
            // surrogate). A sparse miss means the row lives in columnar under
            // the user `id` column (the `spatial` / columnar-family case), so it
            // is resolved from the columnar id → doc map. Both forms normalise
            // to standard msgpack maps that `decode_document_value` and
            // `extract_geometry` read identically.
            let doc = match self.sparse.get(database_id, tid, collection, &doc_id) {
                Ok(Some(raw)) => {
                    let (_, doc_mp) = crate::data::executor::scan_normalize::sparse_row_to_doc(
                        &doc_id,
                        &raw,
                        strict_schema.as_ref(),
                    );
                    match super::super::doc_format::decode_document_value(&doc_mp) {
                        Some(d) => d,
                        None => continue,
                    }
                }
                Ok(None) => {
                    // Columnar-family (e.g. `engine='spatial'`) collection: the
                    // doc-map id is the user `id` column and the row lives in
                    // columnar, not sparse. Build the id → document map once and
                    // reuse it across every remaining candidate.
                    if columnar_docs.is_none() {
                        match self.scan_collection(database_id, tid, collection, usize::MAX) {
                            Ok(rows) => {
                                columnar_docs = Some(rows.into_iter().collect());
                            }
                            Err(e) => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: e.to_string(),
                                    },
                                );
                            }
                        }
                    }
                    let Some(doc_mp) = columnar_docs.as_ref().and_then(|m| m.get(&doc_id)) else {
                        continue;
                    };
                    match super::super::doc_format::decode_document_value(doc_mp) {
                        Some(d) => d,
                        None => continue,
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        err = %e,
                        %doc_id,
                        %collection,
                        "sparse get failed during spatial candidate hydration; skipping row"
                    );
                    continue;
                }
            };

            let doc_geom = match extract_geometry(&doc, field) {
                Some(g) => g,
                None => continue,
            };

            if !apply_predicate(predicate, query_geom, &doc_geom, distance_meters) {
                continue;
            }

            if !attr_filters.iter().all(|f| f.matches_value(&doc)) {
                continue;
            }
            if !row_level_filters.iter().all(|f| f.matches_value(&doc)) {
                continue;
            }

            results.push(project_doc(&doc, &doc_id, projection));
        }

        if let Some(txn_id) = task.request.txn_id {
            self.merge_overlay_into_spatial_scan(
                super::transaction::overlay::SpatialOverlayMergeParams {
                    txn_id,
                    coll_key: &coll_key,
                    field,
                    predicate,
                    query_geom,
                    distance_meters,
                    projection,
                    attr_filters: &attr_filters,
                    row_level_filters: &row_level_filters,
                },
                &mut results,
            );
        }

        match response_codec::encode_value_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Full scan when no R-tree exists for the field.
    fn spatial_full_scan(&self, params: SpatialFullScanParams<'_>) -> Response {
        let SpatialFullScanParams {
            task,
            tid,
            collection,
            field,
            predicate,
            query_geom,
            distance_meters,
            limit,
            projection,
            attr_filters,
            rls_filters,
            prefilter,
        } = params;
        debug!(core = self.core_id, %collection, "spatial full scan (no R-tree index yet)");

        let scan_limit = limit * 10;
        let entries = match self.scan_collection(
            task.request.database_id.as_u64(),
            tid,
            collection,
            scan_limit,
        ) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let mut results = Vec::new();
        for (doc_id, doc_bytes) in &entries {
            if results.len() >= limit {
                break;
            }

            // Prefilter: skip non-members before geometry evaluation.
            if let Some(bitmap) = prefilter {
                match u32::from_str_radix(doc_id, 16) {
                    Ok(raw) => {
                        if !bitmap.contains(Surrogate(raw)) {
                            continue;
                        }
                    }
                    Err(_) => continue,
                }
            }

            let doc = match super::super::doc_format::decode_document_value(doc_bytes) {
                Some(d) => d,
                None => continue,
            };

            let doc_geom = match extract_geometry(&doc, field) {
                Some(g) => g,
                None => continue,
            };

            if !apply_predicate(predicate, query_geom, &doc_geom, distance_meters) {
                continue;
            }

            if !attr_filters.iter().all(|f| f.matches_value(&doc)) {
                continue;
            }
            if !rls_filters.iter().all(|f| f.matches_value(&doc)) {
                continue;
            }

            results.push(project_doc(&doc, doc_id, projection));
        }

        if let Some(txn_id) = task.request.txn_id {
            let coll_key = (
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );
            self.merge_overlay_into_spatial_scan(
                super::transaction::overlay::SpatialOverlayMergeParams {
                    txn_id,
                    coll_key: &coll_key,
                    field,
                    predicate,
                    query_geom,
                    distance_meters,
                    projection,
                    attr_filters,
                    row_level_filters: rls_filters,
                },
                &mut results,
            );
        }

        match response_codec::encode_value_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SpatialScanParams;
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::spatial::RTreeEntry;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use crate::util::fnv1a_hash;
    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::SpatialOp;
    use nodedb_types::{Surrogate, SurrogateBitmap};
    use std::time::{Duration, Instant};

    fn make_core() -> (
        crate::data::executor::core_loop::CoreLoop,
        nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (req_tx, req_rx) = RingBuffer::channel::<crate::bridge::dispatch::BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<crate::bridge::dispatch::BridgeResponse>(64);
        drop(req_tx);
        let core = crate::data::executor::core_loop::CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .unwrap();
        (core, resp_rx, dir)
    }

    fn make_task(plan: PhysicalPlan) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
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
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    /// Insert a document directly into sparse storage and the R-tree.
    /// Returns the hex doc_id.
    fn insert_spatial_doc(
        core: &mut crate::data::executor::core_loop::CoreLoop,
        tid: u64,
        collection: &str,
        field: &str,
        surrogate: Surrogate,
        lng: f64,
        lat: f64,
    ) -> String {
        let doc_id = crate::engine::document::store::surrogate_to_doc_id(surrogate);

        // Build a minimal msgpack document with a GeoJSON Point field.
        let geojson = serde_json::json!({
            field: { "type": "Point", "coordinates": [lng, lat] },
            "id": &doc_id
        });
        let msgpack = nodedb_types::json_to_msgpack(&geojson).unwrap();

        core.sparse
            .put(0, tid, collection, &doc_id, &msgpack)
            .unwrap();

        // Manually populate the R-tree and the doc-map.
        let geom: nodedb_types::geometry::Geometry =
            serde_json::from_value(serde_json::json!({"type":"Point","coordinates":[lng,lat]}))
                .unwrap();
        let bbox = nodedb_types::bbox::geometry_bbox(&geom);
        let db_id = DatabaseId::DEFAULT;
        let tid_id = TenantId::new(tid);
        let spatial_key = (db_id, tid_id, collection.to_string(), field.to_string());
        let entry_id = fnv1a_hash(doc_id.as_bytes());
        let rtree = core.spatial_indexes.entry(spatial_key.clone()).or_default();
        rtree.insert(RTreeEntry { id: entry_id, bbox });
        core.spatial_doc_map.insert(
            (
                db_id,
                tid_id,
                collection.to_string(),
                field.to_string(),
                entry_id,
            ),
            doc_id.clone(),
        );

        doc_id
    }

    /// GeoJSON Point query centred on (0.0, 0.0) for DWithin.
    fn origin_point() -> nodedb_types::geometry::Geometry {
        nodedb_types::geometry::Geometry::point(0.0, 0.0)
    }

    fn dummy_spatial_plan() -> PhysicalPlan {
        PhysicalPlan::Spatial(SpatialOp::Scan {
            collection: "places".into(),
            field: "loc".into(),
            predicate: nodedb_physical::physical_plan::SpatialPredicate::DWithin,
            query_geometry: origin_point(),
            distance_meters: 1_000_000.0,
            attribute_filters: Vec::new(),
            limit: 100,
            projection: Vec::new(),
            rls_filters: Vec::new(),
            prefilter: None,
        })
    }

    #[test]
    fn prefilter_skips_non_member_doc_ids() {
        // Direct unit on the prefilter check: the candidate-loop logic
        // parses doc_id as hex Surrogate and skips non-members.
        let mut bitmap = SurrogateBitmap::new();
        bitmap.insert(Surrogate(2));

        let candidate_doc_ids = [
            crate::engine::document::store::surrogate_to_doc_id(Surrogate(1)),
            crate::engine::document::store::surrogate_to_doc_id(Surrogate(2)),
            crate::engine::document::store::surrogate_to_doc_id(Surrogate(3)),
        ];

        let kept: Vec<_> = candidate_doc_ids
            .iter()
            .filter(|doc_id| match u32::from_str_radix(doc_id, 16) {
                Ok(raw) => bitmap.contains(Surrogate(raw)),
                Err(_) => false,
            })
            .cloned()
            .collect();

        assert_eq!(kept.len(), 1);
        assert_eq!(
            kept[0],
            crate::engine::document::store::surrogate_to_doc_id(Surrogate(2))
        );
    }

    // Note: the R-tree-branch by-surrogate candidate hydration
    // (execute_spatial_scan → sparse.get → sparse_row_to_doc) is covered
    // end-to-end by `tests/engine_surface_spatial.rs::
    // sql_geometry_insert_into_document_collection_matches_spatial_predicate`,
    // which drives a real SQL INSERT + ST_DWithin. That test went FAIL→PASS
    // across the read-path fix, so it genuinely exercises the R-tree branch.
    // An isolated `make_core` unit test cannot faithfully reproduce the full
    // write+register+scan setup (surrogate allocation / collection
    // registration the hydrate/decode path depends on), so the e2e test is
    // the authoritative coverage here.

    #[test]
    fn empty_prefilter_returns_nothing() {
        let (mut core, _resp_rx, _dir) = make_core();
        let tid = 1u64;
        let collection = "geo";
        let field = "loc";

        insert_spatial_doc(&mut core, tid, collection, field, Surrogate(10), 0.0, 0.0);

        let task = make_task(dummy_spatial_plan());
        let empty_bitmap = SurrogateBitmap::new();

        let resp = core.execute_spatial_scan(SpatialScanParams {
            task: &task,
            tid,
            collection,
            field,
            predicate: &nodedb_physical::physical_plan::SpatialPredicate::DWithin,
            query_geometry: &origin_point(),
            distance_meters: 1_000_000.0,
            attribute_filters: &[],
            limit: 100,
            projection: &[],
            rls_filters: &[],
            prefilter: Some(&empty_bitmap),
        });
        assert_eq!(resp.status, Status::Ok);
        let decoded: Vec<nodedb_types::Value> =
            zerompk::from_msgpack(resp.payload.as_bytes()).unwrap_or_default();
        assert!(decoded.is_empty(), "empty prefilter must return no results");
    }
}
