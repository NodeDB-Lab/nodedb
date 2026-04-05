//! Spatial query handler: R-tree index scan with predicate refinement.
//!
//! Documents with geometry fields are auto-indexed into per-field R-trees
//! on insert (see `handlers/point.rs`). Spatial queries use the R-tree for
//! fast bbox candidate selection, then refine with exact predicates.

use sonic_rs;
use tracing::debug;

use super::super::response_codec;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::SpatialPredicate;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a spatial scan using the R-tree index.
    ///
    /// 1. Parse query geometry from GeoJSON bytes
    /// 2. Get or lazily create R-tree for `collection:field`
    /// 3. R-tree range search for bbox candidates
    /// 4. Exact predicate refinement (load document geometry, apply ST_*)
    /// 5. Return matching documents up to limit
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
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field,
            predicate = ?predicate,
            "spatial scan"
        );

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

        // 3. Get R-tree index for this collection:field.
        let index_key = format!("{tid}:{collection}:{field}");
        let has_index = self.spatial_indexes.contains_key(&index_key);

        let limit = if limit == 0 { 1000 } else { limit };

        // If no R-tree exists yet, do a full document scan with predicate post-filter.
        // This handles cold-start (no inserts yet with geometry) and ensures correctness.
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
            );
        }

        let rtree = match self.spatial_indexes.get(&index_key) {
            Some(rt) => rt,
            None => {
                // Should not happen (has_index was true), but handle gracefully.
                match response_codec::encode_json_vec(&[]) {
                    Ok(payload) => return self.response_with_payload(task, payload),
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
        };

        // 4. R-tree range search → candidate entry IDs.
        let candidates = rtree.search(&search_bbox);

        debug!(
            core = self.core_id,
            candidates = candidates.len(),
            "spatial R-tree candidates"
        );

        // 5. Exact predicate refinement: for each candidate, load document,
        //    extract geometry, apply spatial predicate.
        let mut results = Vec::new();

        for entry in &candidates {
            if results.len() >= limit {
                break;
            }

            // Resolve entry_id → doc_id via the reverse map populated at insert time.
            let doc_id = match self.spatial_doc_map.get(&(index_key.clone(), entry.id)) {
                Some(id) => id.as_str(),
                None => continue,
            };

            let doc_bytes = match self.sparse.get(tid, collection, doc_id) {
                Ok(Some(b)) => b,
                _ => continue,
            };

            let doc = match super::super::doc_format::decode_document(&doc_bytes) {
                Some(d) => d,
                None => continue,
            };

            let geom_value = match doc.get(field) {
                Some(v) => v,
                None => continue,
            };
            let doc_geom: nodedb_types::geometry::Geometry =
                match serde_json::from_value(geom_value.clone()) {
                    Ok(g) => g,
                    Err(_) => continue,
                };

            if !apply_predicate(predicate, &query_geom, &doc_geom, distance_meters) {
                continue;
            }

            // Apply attribute and RLS post-filters.
            if !attr_filters.iter().all(|f| f.matches(&doc)) {
                continue;
            }
            if !row_level_filters.iter().all(|f| f.matches(&doc)) {
                continue;
            }

            results.push(project_doc(&doc, doc_id, projection));
        }

        match response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Full scan fallback when no R-tree exists for the field.
    ///
    /// Scans all documents in the collection, extracts geometry, applies predicate.
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
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            "spatial full scan (no R-tree index yet)"
        );

        let scan_limit = limit * 10;
        let entries = match self.sparse.scan_documents(tid, collection, scan_limit) {
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

            let doc = match super::super::doc_format::decode_document(doc_bytes) {
                Some(d) => d,
                None => continue,
            };

            let geom_value = match doc.get(field) {
                Some(v) => v,
                None => continue,
            };
            let doc_geom: nodedb_types::geometry::Geometry =
                match serde_json::from_value(geom_value.clone()) {
                    Ok(g) => g,
                    Err(_) => continue,
                };

            if !apply_predicate(predicate, query_geom, &doc_geom, distance_meters) {
                continue;
            }

            // Apply attribute and RLS post-filters.
            if !attr_filters.iter().all(|f| f.matches(&doc)) {
                continue;
            }
            if !rls_filters.iter().all(|f| f.matches(&doc)) {
                continue;
            }

            results.push(project_doc(&doc, doc_id, projection));
        }

        match response_codec::encode_json_vec(&results) {
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

/// Apply the spatial predicate.
fn apply_predicate(
    predicate: &SpatialPredicate,
    query: &nodedb_types::geometry::Geometry,
    doc: &nodedb_types::geometry::Geometry,
    distance_meters: f64,
) -> bool {
    match predicate {
        SpatialPredicate::DWithin => nodedb_spatial::st_dwithin(query, doc, distance_meters),
        SpatialPredicate::Contains => nodedb_spatial::st_contains(query, doc),
        SpatialPredicate::Intersects => nodedb_spatial::st_intersects(query, doc),
        SpatialPredicate::Within => nodedb_spatial::st_within(doc, query),
    }
}

/// Apply projection to a document.
fn project_doc(doc: &serde_json::Value, doc_id: &str, projection: &[String]) -> serde_json::Value {
    if projection.is_empty() {
        doc.clone()
    } else {
        let mut projected = serde_json::Map::new();
        projected.insert(
            "id".to_string(),
            serde_json::Value::String(doc_id.to_string()),
        );
        for col in projection {
            if let Some(v) = doc.get(col) {
                projected.insert(col.clone(), v.clone());
            }
        }
        serde_json::Value::Object(projected)
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
