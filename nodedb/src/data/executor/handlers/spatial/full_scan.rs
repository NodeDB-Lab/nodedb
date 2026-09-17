// SPDX-License-Identifier: BUSL-1.1

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::spatial_refine::{
    apply_predicate, extract_geometry, project_doc,
};
use crate::data::executor::handlers::transaction::overlay::SpatialOverlayMergeParams;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::SpatialPredicate;
use nodedb_types::SurrogateBitmap;

use super::prefilter::prefilter_admits;

/// Parameters for [`CoreLoop::spatial_full_scan`].
pub(super) struct SpatialFullScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub predicate: &'a SpatialPredicate,
    pub query_geom: &'a nodedb_types::geometry::Geometry,
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

            let doc_geom = match extract_geometry(&doc, field) {
                Some(g) => g,
                None => continue,
            };

            if !apply_predicate(predicate, query_geom, &doc_geom, distance_meters) {
                continue;
            }

            match ScanFilter::all_match_value(attr_filters, &doc) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_e) => {
                    return self.response_error(task, ErrorCode::DivisionByZero);
                }
            }
            match ScanFilter::all_match_value(rls_filters, &doc) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_e) => {
                    return self.response_error(task, ErrorCode::DivisionByZero);
                }
            }

            results.push(project_doc(&doc, doc_id, projection));
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
