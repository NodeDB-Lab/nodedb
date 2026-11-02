// SPDX-License-Identifier: BUSL-1.1

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::columnar_read::filter::decode_rls_filters;
use crate::data::executor::handlers::spatial_refine::{
    SpatialHit, apply_predicate, extract_geometry, hit_rows, mixed_srid_error, project_doc,
    search_bbox,
};
use crate::data::executor::handlers::transaction::overlay::SpatialOverlayMergeParams;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::SpatialPredicate;
use nodedb_spatial::Metric;
use nodedb_types::SurrogateBitmap;

use super::cut_rows::CandidateBodies;
use super::full_scan::SpatialFullScanParams;
use super::prefilter::prefilter_admits;
use crate::data::executor::handlers::search_cut::SearchCut;
use nodedb_spatial::CutCandidate;

/// Parameters for [`CoreLoop::execute_spatial_scan`].
pub(in crate::data::executor) struct SpatialScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub predicate: &'a SpatialPredicate,
    pub query_geometry: &'a nodedb_types::geometry::SridGeometry,
    pub distance_meters: f64,
    pub attribute_filters: &'a [u8],
    pub limit: usize,
    pub projection: &'a [String],
    pub rls_filters: &'a [u8],
    pub prefilter: Option<&'a SurrogateBitmap>,
    /// The system-time scope the read sees.
    pub system_time: nodedb_types::SystemTimeScope,
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
        let crs = self.crs_context();
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
            system_time,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %field,
            predicate = ?predicate,
            "spatial scan"
        );

        let cut = match SearchCut::of(system_time) {
            Ok(cut) => cut,
            Err(e) => return self.response_error(task, e),
        };

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        // The query geometry was parsed and validated on the Control Plane.
        let query_geom = query_geometry;

        // 2. Deserialize attribute and RLS filters. A payload that does not
        // decode fails the scan: an unreadable predicate never admits the
        // rows it would have excluded.
        let attr_filters: Vec<ScanFilter> = if attribute_filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(attribute_filters) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("malformed spatial attribute filters: {e}"),
                        },
                    );
                }
            }
        };
        let row_level_filters: Vec<ScanFilter> = match decode_rls_filters(rls_filters) {
            Ok(f) => f,
            Err(e) => return self.response_error(task, e),
        };

        // 3. Compute the search bbox in the query SRID's space, widened by
        // the distance predicates' reach.
        let metric = match Metric::resolve(&crs, query_geom.srid) {
            Ok(metric) => metric,
            Err(e) => return self.response_error(task, crate::Error::from(e)),
        };
        let query_bbox = match query_geom.bbox(&crs) {
            Ok(bbox) => bbox,
            Err(e) => return self.response_error(task, crate::Error::from(e)),
        };
        let search_bbox = search_bbox(predicate, query_bbox, distance_meters, &metric);

        let db_id = task.request.database_id;
        let tid_id = crate::types::TenantId::new(tid);
        let spatial_key = (db_id, tid_id, collection.to_string(), field.to_string());
        let has_index = self.spatial_indexes.contains_key(&spatial_key);

        // A query in an SRID other than the index's is refused before the
        // scan. No implicit transform runs.
        if let Some(rtree) = self.spatial_indexes.get(&spatial_key)
            && !rtree.accepts_srid(query_geom.srid)
        {
            let mismatch = nodedb_spatial::SridMismatch {
                index: rtree.srid(),
                entry: query_geom.srid,
            };
            return self.response_error(task, mixed_srid_error(collection, field, mismatch));
        }
        // `limit` is the statement's LIMIT, `usize::MAX` without one. A
        // `LIMIT 0` returns no rows.
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
                cut,
            });
        }

        let coll_key = (db_id, tid_id, collection.to_string());

        let rtree = match self.spatial_indexes.get(&spatial_key) {
            Some(rt) => rt,
            None => {
                return match response_codec::encode_value_vec(&[]) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::from(e)),
                };
            }
        };

        // 4. R-tree range search → the candidates' document ids. A read at a
        // cut finds the live entries inserted at or below the cut and the
        // ended entries visible there. The reverse map keys a candidate per
        // engine: a document collection's row by its hex surrogate (see
        // `apply_point_put_spatial`), a columnar-family row by its user `id`
        // (see `index_columnar_geometry_columns`).
        let doc_map_id = |entry_id: u64| {
            self.spatial_doc_map
                .get(&(
                    db_id,
                    tid_id,
                    collection.to_string(),
                    field.to_string(),
                    entry_id,
                ))
                .cloned()
        };
        let candidates: Vec<String> = match cut.at {
            None => rtree
                .search(&search_bbox)
                .into_iter()
                .filter_map(|entry| doc_map_id(entry.id))
                .collect(),
            Some(at) => rtree
                .search_at(&search_bbox, at)
                .into_iter()
                .filter_map(|candidate| match candidate {
                    CutCandidate::Live(entry_id) => doc_map_id(entry_id),
                    CutCandidate::Ended(ended) => Some(ended.document_id.clone()),
                })
                .collect(),
        };
        debug!(
            core = self.core_id,
            candidates = candidates.len(),
            "spatial R-tree candidates"
        );

        let identity_column = self.identity_column(db_id.as_u64(), tid, collection);
        let mut bodies = CandidateBodies {
            database_id: db_id.as_u64(),
            tid,
            collection,
            at: cut.at,
            columnar: None,
        };

        // 5. Exact predicate refinement on each candidate's body at the
        // read's cut.
        let mut results = Vec::new();
        for doc_id in &candidates {
            if results.len() >= limit {
                break;
            }
            // Prefilter: skip candidates not in the surrogate bitmap before
            // any geometry evaluation.
            if let Some(bitmap) = prefilter
                && !prefilter_admits(bitmap, doc_id)
            {
                continue;
            }
            // A candidate with no version the read sees is no row of it. A
            // body that does not read fails the scan: skipped, it would read
            // as "no row matched the geometry".
            let doc_mp = match self.spatial_candidate_body(&mut bodies, doc_id) {
                Ok(Some(doc_mp)) => doc_mp,
                Ok(None) => continue,
                Err(e) => return self.response_error(task, e),
            };
            let doc = match doc_format::decode_document_value(&doc_mp) {
                Ok(d) => d,
                Err(e) => return self.response_error(task, e),
            };

            let doc_geom = match extract_geometry(&doc, field, &crs) {
                Ok(Some(g)) => g,
                Ok(None) => continue,
                Err(e) => return self.response_error(task, crate::Error::from(e)),
            };

            match apply_predicate(predicate, query_geom, &doc_geom, distance_meters, &metric) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(mismatch) => {
                    return self
                        .response_error(task, mixed_srid_error(collection, field, mismatch));
                }
            }

            match ScanFilter::all_match_value(&attr_filters, &doc, &crs) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    return self.response_error(task, ErrorCode::from(e));
                }
            }
            match ScanFilter::all_match_value(&row_level_filters, &doc, &crs) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    return self.response_error(task, ErrorCode::from(e));
                }
            }

            results.push(SpatialHit::new(
                doc_id,
                project_doc(&doc, doc_id, projection, &identity_column),
            ));
        }

        // The issuing transaction's staged writes merge into a current read
        // and a read at its own snapshot.
        if let Some(txn_id) = task.request.txn_id.filter(|_| cut.merge_overlay)
            && let Err(e) = self.merge_overlay_into_spatial_scan(
                SpatialOverlayMergeParams {
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
            )
        {
            return self.response_error(task, e);
        }

        match response_codec::encode_value_vec(&hit_rows(results)) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
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
            crate::data::executor::core_loop::test_governor(),
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
            commit_hlc: None,
            system_ordinal: None,
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
        let storage_key = nodedb_types::StorageKey::for_surrogate(surrogate);
        let doc_id = storage_key.to_string();

        // Build a minimal msgpack document with a GeoJSON Point field.
        let geojson = serde_json::json!({
            field: { "type": "Point", "coordinates": [lng, lat] },
            "id": &doc_id
        });
        let msgpack = nodedb_types::json_to_msgpack(&geojson).unwrap();

        core.sparse
            .put(
                0,
                tid,
                collection,
                &storage_key,
                &msgpack,
                core.bitemporal_ordinal(),
            )
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
        let memory = nodedb_mem::ScopedMemory::new(
            core.governor.clone(),
            db_id,
            tid_id,
            nodedb_mem::EngineId::Spatial,
        );
        let rtree = core
            .spatial_indexes
            .entry(spatial_key.clone())
            .or_insert_with(|| {
                crate::engine::spatial::RTree::new(memory, crate::engine::spatial::TreeSrid::WGS84)
            });
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
    fn origin_point() -> nodedb_types::geometry::SridGeometry {
        nodedb_types::geometry::SridGeometry::wgs84(nodedb_types::geometry::Geometry::point(
            0.0, 0.0,
        ))
    }

    fn dummy_spatial_plan() -> PhysicalPlan {
        PhysicalPlan::Spatial(SpatialOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "places",
            ),
            field: "loc".into(),
            predicate: nodedb_physical::physical_plan::SpatialPredicate::DWithin,
            query_geometry: origin_point(),
            distance_meters: 1_000_000.0,
            attribute_filters: Vec::new(),
            limit: 100,
            projection: Vec::new(),
            rls_filters: Vec::new(),
            prefilter: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        })
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
            system_time: nodedb_types::SystemTimeScope::Current,
        });
        assert_eq!(resp.status, Status::Ok);
        let decoded: Vec<nodedb_types::Value> =
            zerompk::from_msgpack(resp.payload.as_bytes()).unwrap_or_default();
        assert!(decoded.is_empty(), "empty prefilter must return no results");
    }

    /// A query in an SRID other than the index's is refused before the scan.
    #[test]
    fn query_in_another_srid_is_refused() {
        let (mut core, _resp_rx, _dir) = make_core();
        let tid = 1u64;
        insert_spatial_doc(&mut core, tid, "geo", "loc", Surrogate(10), 0.0, 0.0);

        let query = nodedb_types::geometry::SridGeometry::new(
            nodedb_types::geometry::Srid::new(32648).expect("srid"),
            nodedb_types::geometry::Geometry::point(500_000.0, 0.0),
        );
        let task = make_task(dummy_spatial_plan());
        let resp = core.execute_spatial_scan(SpatialScanParams {
            task: &task,
            tid,
            collection: "geo",
            field: "loc",
            predicate: &nodedb_physical::physical_plan::SpatialPredicate::DWithin,
            query_geometry: &query,
            distance_meters: 1_000.0,
            attribute_filters: &[],
            limit: 100,
            projection: &[],
            rls_filters: &[],
            prefilter: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        });
        assert_eq!(resp.status, Status::Error);
    }
}
