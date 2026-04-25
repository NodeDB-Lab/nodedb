//! Spatial query handler: R-tree index scan with predicate refinement.
//!
//! Documents with geometry fields are auto-indexed into per-field R-trees
//! on insert (see `handlers/point.rs`). Spatial queries use the R-tree for
//! fast bbox candidate selection, then refine with exact predicates.
//!
//! Internal document representation: `nodedb_types::Value` (no JSON intermediary).

use sonic_rs;
use tracing::debug;

use super::super::response_codec;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::SpatialPredicate;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_types::{Surrogate, SurrogateBitmap, Value};

impl CoreLoop {
    /// Execute a spatial scan using the R-tree index.
    ///
    /// 1. Parse query geometry from GeoJSON bytes
    /// 2. R-tree range search for bbox candidates
    /// 3. Exact predicate refinement (extract geometry, apply ST_*)
    /// 4. Return matching documents up to limit
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_spatial_scan(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
        predicate: &SpatialPredicate,
        query_geometry_bytes: &[u8],
        distance_meters: f64,
        attribute_filters: &[u8],
        limit: usize,
        projection: &[String],
        rls_filters: &[u8],
        prefilter: Option<&SurrogateBitmap>,
    ) -> Response {
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

        // 1. Parse query geometry.
        let query_geom: nodedb_types::geometry::Geometry =
            match sonic_rs::from_slice(query_geometry_bytes) {
                Ok(g) => g,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("invalid query geometry GeoJSON: {e}"),
                        },
                    );
                }
            };

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
        let query_bbox = nodedb_types::bbox::geometry_bbox(&query_geom);
        let search_bbox = if distance_meters > 0.0 {
            expand_bbox(&query_bbox, distance_meters)
        } else {
            query_bbox
        };

        let tid_id = crate::types::TenantId::new(tid);
        let spatial_key = (tid_id, collection.to_string(), field.to_string());
        let has_index = self.spatial_indexes.contains_key(&spatial_key);
        let limit = if limit == 0 { 1000 } else { limit };

        // No R-tree: full scan with predicate post-filter.
        if !has_index {
            return self.spatial_full_scan(
                task,
                tid,
                collection,
                field,
                predicate,
                &query_geom,
                distance_meters,
                limit,
                projection,
                &attr_filters,
                &row_level_filters,
                prefilter,
            );
        }

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

        // Pre-load all docs from scan_collection (handles both sparse and columnar).
        let all_docs: std::collections::HashMap<String, Vec<u8>> = self
            .scan_collection(tid, collection, 10_000)
            .unwrap_or_default()
            .into_iter()
            .collect();

        // 5. Exact predicate refinement.
        let mut results = Vec::new();

        for entry in &candidates {
            if results.len() >= limit {
                break;
            }

            let doc_id = match self.spatial_doc_map.get(&(
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

            let doc_bytes = match all_docs.get(&doc_id) {
                Some(b) => b,
                None => continue,
            };

            let doc = match super::super::doc_format::decode_document_value(doc_bytes) {
                Some(d) => d,
                None => continue,
            };

            let doc_geom = match extract_geometry(&doc, field) {
                Some(g) => g,
                None => continue,
            };

            if !apply_predicate(predicate, &query_geom, &doc_geom, distance_meters) {
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
    #[allow(clippy::too_many_arguments)]
    fn spatial_full_scan(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
        predicate: &SpatialPredicate,
        query_geom: &nodedb_types::geometry::Geometry,
        distance_meters: f64,
        limit: usize,
        projection: &[String],
        attr_filters: &[ScanFilter],
        rls_filters: &[ScanFilter],
        prefilter: Option<&SurrogateBitmap>,
    ) -> Response {
        debug!(core = self.core_id, %collection, "spatial full scan (no R-tree index yet)");

        let scan_limit = limit * 10;
        let entries = match self.scan_collection(tid, collection, scan_limit) {
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

/// Extract geometry from a document field.
///
/// Handles three storage forms:
/// - `Value::Geometry(g)` — native geometry (columnar path preserves type)
/// - `Value::String(s)` — GeoJSON string (from SQL ST_Point → serialized)
/// - `Value::Object(_)` — GeoJSON object (from schemaless doc storage)
fn extract_geometry(doc: &Value, field: &str) -> Option<nodedb_types::geometry::Geometry> {
    let field_val = doc.get(field)?;
    match field_val {
        Value::Geometry(g) => Some(g.clone()),
        Value::String(s) => sonic_rs::from_str(s).ok(),
        Value::Object(map) => {
            // GeoJSON object stored as Value::Object — serialize to JSON then parse.
            let json = serde_json::Value::from(Value::Object(map.clone()));
            serde_json::from_value(json).ok()
        }
        _ => None,
    }
}

/// Apply the spatial predicate.
fn apply_predicate(
    predicate: &SpatialPredicate,
    query: &nodedb_types::geometry::Geometry,
    doc: &nodedb_types::geometry::Geometry,
    distance_meters: f64,
) -> bool {
    match predicate {
        SpatialPredicate::DWithin => {
            crate::engine::spatial::st_dwithin(query, doc, distance_meters)
        }
        SpatialPredicate::Contains => crate::engine::spatial::st_contains(query, doc),
        SpatialPredicate::Intersects => crate::engine::spatial::st_intersects(query, doc),
        SpatialPredicate::Within => crate::engine::spatial::st_within(doc, query),
    }
}

/// Apply projection to a document, returning `nodedb_types::Value`.
fn project_doc(doc: &Value, doc_id: &str, projection: &[String]) -> Value {
    if projection.is_empty() {
        // Add id if not present.
        if let Value::Object(mut map) = doc.clone() {
            map.entry("id".to_string())
                .or_insert(Value::String(doc_id.to_string()));
            Value::Object(map)
        } else {
            doc.clone()
        }
    } else {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), Value::String(doc_id.to_string()));
        for col in projection {
            if let Some(v) = doc.get(col) {
                map.insert(col.clone(), v.clone());
            }
        }
        Value::Object(map)
    }
}

/// Expand a bounding box by a distance in meters.
fn expand_bbox(bbox: &nodedb_types::BoundingBox, meters: f64) -> nodedb_types::BoundingBox {
    let lat_delta = meters / 111_320.0;
    let avg_lat = ((bbox.min_lat + bbox.max_lat) / 2.0).to_radians();
    let lng_delta = meters / (111_320.0 * avg_lat.cos().max(0.001));

    nodedb_types::BoundingBox::new(
        bbox.min_lng - lng_delta,
        bbox.min_lat - lat_delta,
        bbox.max_lng + lng_delta,
        bbox.max_lat + lat_delta,
    )
}

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::bridge::physical_plan::SpatialOp;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::spatial::RTreeEntry;
    use crate::types::{ReadConsistency, RequestId, TenantId, VShardId};
    use crate::util::fnv1a_hash;
    use nodedb_bridge::buffer::RingBuffer;
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
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
        })
    }

    /// Insert a document directly into sparse storage and the R-tree.
    /// Returns the hex doc_id.
    fn insert_spatial_doc(
        core: &mut crate::data::executor::core_loop::CoreLoop,
        tid: u32,
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

        core.sparse.put(tid, collection, &doc_id, &msgpack).unwrap();

        // Manually populate the R-tree and the doc-map.
        let geom: nodedb_types::geometry::Geometry =
            serde_json::from_value(serde_json::json!({"type":"Point","coordinates":[lng,lat]}))
                .unwrap();
        let bbox = nodedb_types::bbox::geometry_bbox(&geom);
        let tid_id = TenantId::new(tid);
        let spatial_key = (tid_id, collection.to_string(), field.to_string());
        let entry_id = fnv1a_hash(doc_id.as_bytes());
        let rtree = core.spatial_indexes.entry(spatial_key.clone()).or_default();
        rtree.insert(RTreeEntry { id: entry_id, bbox });
        core.spatial_doc_map.insert(
            (tid_id, collection.to_string(), field.to_string(), entry_id),
            doc_id.clone(),
        );

        doc_id
    }

    /// GeoJSON Point query centred on (0.0, 0.0) as bytes for DWithin.
    fn origin_point_bytes() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"type":"Point","coordinates":[0.0,0.0]})).unwrap()
    }

    fn dummy_spatial_plan() -> PhysicalPlan {
        PhysicalPlan::Spatial(SpatialOp::Scan {
            collection: "places".into(),
            field: "loc".into(),
            predicate: crate::bridge::physical_plan::SpatialPredicate::DWithin,
            query_geometry: origin_point_bytes(),
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

    #[test]
    fn empty_prefilter_returns_nothing() {
        let (mut core, _resp_rx, _dir) = make_core();
        let tid = 1u32;
        let collection = "geo";
        let field = "loc";

        insert_spatial_doc(&mut core, tid, collection, field, Surrogate(10), 0.0, 0.0);

        let task = make_task(dummy_spatial_plan());
        let empty_bitmap = SurrogateBitmap::new();

        let resp = core.execute_spatial_scan(
            &task,
            tid,
            collection,
            field,
            &crate::bridge::physical_plan::SpatialPredicate::DWithin,
            &origin_point_bytes(),
            1_000_000.0,
            &[],
            100,
            &[],
            &[],
            Some(&empty_bitmap),
        );
        assert_eq!(resp.status, Status::Ok);
        let decoded: Vec<nodedb_types::Value> =
            zerompk::from_msgpack(resp.payload.as_bytes()).unwrap_or_default();
        assert!(decoded.is_empty(), "empty prefilter must return no results");
    }
}
