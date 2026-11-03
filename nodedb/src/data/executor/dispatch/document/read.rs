// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for the multi-row document reads: scans, index reads, the clone
//! materializer's page scan, and the row-count estimate.

use nodedb_physical::physical_plan::DocumentOp;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::dispatch::document_admit::{
    all_versions_refused, doc_scan_mode, point_cut,
};
use crate::data::executor::handlers::control::snapshot::RangeScanArgs;
use crate::data::executor::handlers::document::index_fetch::IndexedFetchParams;
use crate::data::executor::handlers::document::read::materialize_scan::DocumentMaterializeScan;
use crate::data::executor::handlers::document::read::scan::DocumentScanParams;
use crate::data::executor::task::ExecutionTask;

use super::entry::misrouted;

impl CoreLoop {
    /// Dispatch one multi-row read. Any other op is a routing error.
    pub(super) fn dispatch_document_read(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &DocumentOp,
    ) -> Response {
        match op {
            DocumentOp::Scan {
                collection,
                limit,
                offset,
                sort_keys,
                filters,
                distinct,
                projection,
                computed_columns,
                window_functions,
                system_time,
                valid_at_ms,
                prefilter,
            } => {
                let mode = doc_scan_mode(system_time, *valid_at_ms);
                self.execute_document_scan(
                    task,
                    DocumentScanParams {
                        tid,
                        collection: collection.as_str(),
                        limit: *limit,
                        offset: *offset,
                        sort_keys,
                        filters,
                        distinct: *distinct,
                        projection,
                        computed_columns_bytes: computed_columns,
                        window_functions_bytes: window_functions,
                        mode,
                        prefilter: prefilter.as_ref(),
                    },
                )
            }

            DocumentOp::RangeScan {
                collection,
                field,
                lower,
                upper,
                limit,
                rls_filters,
            } => self.execute_range_scan(
                task,
                RangeScanArgs {
                    tid,
                    collection: collection.as_str(),
                    field: field.as_str(),
                    lower: lower.as_deref(),
                    upper: upper.as_deref(),
                    limit: *limit,
                    rls_filters,
                },
            ),

            DocumentOp::EstimateCount { collection, field } => {
                self.execute_estimate_count(task, tid, collection.as_str(), field)
            }

            DocumentOp::IndexLookup {
                collection,
                path,
                value,
                system_time,
            } => self.execute_document_index_lookup(
                task,
                tid,
                collection.as_str(),
                path,
                value,
                system_time,
            ),

            DocumentOp::IndexedFetch {
                collection,
                path,
                value,
                also,
                filters,
                projection,
                limit,
                offset,
                sort_keys,
                system_time,
            } => {
                let Some(system_as_of) = point_cut(system_time) else {
                    return self.response_error(task, all_versions_refused("indexed fetches"));
                };
                self.execute_document_indexed_fetch(
                    task,
                    IndexedFetchParams {
                        tid,
                        collection: collection.as_str(),
                        path,
                        value,
                        also,
                        filters,
                        projection,
                        limit: *limit,
                        offset: *offset,
                        sort_keys,
                        system_as_of,
                        merge_overlay: system_time.merges_txn_overlay(),
                    },
                )
            }

            DocumentOp::MaterializeScan {
                collection,
                cursor,
                count,
                system_time,
                raw_bodies,
                only_row,
            } => {
                let Some(system_as_of) = point_cut(system_time) else {
                    return self.response_error(task, all_versions_refused("materialize scans"));
                };
                self.execute_document_materialize_scan(
                    task,
                    DocumentMaterializeScan {
                        tid,
                        collection: collection.as_str(),
                        cursor,
                        count: *count,
                        system_as_of,
                        merge_overlay: system_time.merges_txn_overlay(),
                        raw_bodies: *raw_bodies,
                        only_row: *only_row,
                    },
                )
            }

            DocumentOp::PointGet { .. }
            | DocumentOp::PointPut { .. }
            | DocumentOp::PointInsert { .. }
            | DocumentOp::PointDelete { .. }
            | DocumentOp::PointUpdate { .. }
            | DocumentOp::BatchInsert { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::BulkUpdate { .. }
            | DocumentOp::BulkDelete { .. }
            | DocumentOp::Upsert { .. }
            | DocumentOp::BatchUpsert { .. }
            | DocumentOp::Truncate { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::ResolveWrite(_)
            | DocumentOp::ResolvedWrite { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::ApplyBalanceDelta { .. } => {
                self.response_error(task, misrouted("a multi-row read"))
            }
        }
    }
}
