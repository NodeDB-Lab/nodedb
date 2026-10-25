// SPDX-License-Identifier: BUSL-1.1

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::spatial_refine::{
    SpatialHit, apply_predicate, extract_geometry, hit_rows, mixed_srid_error, project_doc,
};
use crate::data::executor::handlers::transaction::overlay::SpatialOverlayMergeParams;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::SpatialPredicate;
use nodedb_spatial::Metric;
use nodedb_types::SurrogateBitmap;

use super::prefilter::prefilter_admits;

/// Parameters for [`CoreLoop::spatial_full_scan`].
pub(super) struct SpatialFullScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub predicate: &'a SpatialPredicate,
    pub query_geom: &'a nodedb_types::geometry::SridGeometry,
    pub distance_meters: f64,
    pub limit: usize,
    pub projection: &'a [String],
    pub attr_filters: &'a [ScanFilter],
    pub rls_filters: &'a [ScanFilter],
    pub prefilter: Option<&'a SurrogateBitmap>,
}

impl CoreLoop {
    /// Full scan when no R-tree exists for the field.
    pub(super) fn spatial_full_scan(&self, params: SpatialFullScanParams<'_>) -> Response {
        let crs = self.crs_context();
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
        let metric = match Metric::resolve(&crs, query_geom.srid) {
            Ok(metric) => metric,
            Err(e) => return self.response_error(task, crate::Error::from(e)),
        };

        // The predicate is evaluated after the read, so the read cannot stop
        // at a multiple of the limit: a match past that prefix would be
        // silently missed. Every row is read under the scan byte budget, and
        // a collection past it fails loudly.
        let budget = self.query_tuning.max_scan_result_bytes;
        let scan_limit =
            crate::data::executor::handlers::scan_budget::fetch_limit_for(usize::MAX, 0, budget);
        let entries = match self.scan_collection(
            task.request.database_id.as_u64(),
            tid,
            collection,
            scan_limit,
        ) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(task, ErrorCode::from(e));
            }
        };
        if crate::data::executor::handlers::scan_budget::scan_bytes_exceeded(&entries, budget) {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let identity_column =
            self.identity_column(task.request.database_id.as_u64(), tid, collection);
        let mut results = Vec::new();
        for (doc_id, doc_bytes) in &entries {
            if results.len() >= limit {
                break;
            }

            // Prefilter: skip non-members before geometry evaluation.
            if let Some(bitmap) = prefilter
                && !prefilter_admits(bitmap, doc_id)
            {
                continue;
            }

            // A row skipped here silently drops out of the spatial result set,
            // which reads as "no row matched the geometry" rather than "a row
            // could not be read".
            let doc = match doc_format::decode_document_value(doc_bytes) {
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

            match ScanFilter::all_match_value(attr_filters, &doc, &crs) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    return self.response_error(task, ErrorCode::from(e));
                }
            }
            match ScanFilter::all_match_value(rls_filters, &doc, &crs) {
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

        if let Some(txn_id) = task.request.txn_id {
            let coll_key = (
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );
            if let Err(e) = self.merge_overlay_into_spatial_scan(
                SpatialOverlayMergeParams {
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
            ) {
                return self.response_error(task, e);
            }
        }

        match response_codec::encode_value_vec(&hit_rows(results)) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
