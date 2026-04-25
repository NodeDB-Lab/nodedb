//! Document operation dispatch.

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::DocumentOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_document(&mut self, task: &ExecutionTask, op: &DocumentOp) -> Response {
        let tid = task.request.tenant_id.as_u32();
        match op {
            DocumentOp::PointGet {
                collection,
                document_id,
                surrogate,
                pk_bytes: _,
                rls_filters,
                system_as_of_ms,
                valid_at_ms,
            } => self.execute_point_get(
                task,
                super::super::handlers::point::get::PointGetParams {
                    tid,
                    collection,
                    document_id,
                    surrogate: *surrogate,
                    rls_filters,
                    system_as_of_ms: *system_as_of_ms,
                    valid_at_ms: *valid_at_ms,
                },
            ),

            DocumentOp::PointPut {
                collection,
                document_id,
                value,
                surrogate,
                pk_bytes: _,
            } => self.execute_point_put(task, tid, collection, document_id, *surrogate, value),

            DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
            } => self.execute_point_insert(
                task,
                tid,
                collection,
                document_id,
                *surrogate,
                value,
                *if_absent,
            ),

            DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                pk_bytes: _,
            } => self.execute_point_delete(task, tid, collection, document_id, *surrogate),

            DocumentOp::PointUpdate {
                collection,
                document_id,
                surrogate,
                pk_bytes: _,
                updates,
                returning,
            } => self.execute_point_update(
                task,
                tid,
                collection,
                document_id,
                *surrogate,
                updates,
                *returning,
            ),

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
                system_as_of_ms,
                valid_at_ms,
                prefilter,
            } => {
                if system_as_of_ms.is_some() || valid_at_ms.is_some() {
                    self.execute_document_scan_as_of(
                        task,
                        tid,
                        collection,
                        *limit,
                        *offset,
                        filters,
                        projection,
                        *system_as_of_ms,
                        *valid_at_ms,
                    )
                } else {
                    self.execute_document_scan(
                        task,
                        tid,
                        collection,
                        *limit,
                        *offset,
                        sort_keys,
                        filters,
                        *distinct,
                        projection,
                        computed_columns,
                        window_functions,
                        prefilter.as_ref(),
                    )
                }
            }

            DocumentOp::BatchInsert {
                collection,
                documents,
            } => self.execute_document_batch_insert(task, tid, collection, documents),

            DocumentOp::RangeScan {
                collection,
                field,
                lower,
                upper,
                limit,
            } => self.execute_range_scan(
                task,
                tid,
                collection,
                field,
                lower.as_deref(),
                upper.as_deref(),
                *limit,
            ),

            DocumentOp::BulkUpdate {
                collection,
                filters,
                updates,
                returning,
            } => self.execute_bulk_update(task, tid, collection, filters, updates, *returning),

            DocumentOp::BulkDelete {
                collection,
                filters,
            } => self.execute_bulk_delete(task, tid, collection, filters),

            DocumentOp::Upsert {
                collection,
                document_id,
                value,
                on_conflict_updates,
                surrogate,
            } => self.execute_upsert(
                task,
                tid,
                collection,
                document_id,
                *surrogate,
                value,
                on_conflict_updates,
            ),

            DocumentOp::Truncate { collection, .. } => self.execute_truncate(task, tid, collection),

            DocumentOp::EstimateCount { collection, field } => {
                self.execute_estimate_count(task, tid, collection, field)
            }

            DocumentOp::InsertSelect {
                target_collection,
                source_collection,
                source_filters,
                source_limit,
            } => self.execute_insert_select(
                task,
                tid,
                target_collection,
                source_collection,
                source_filters,
                *source_limit,
            ),

            DocumentOp::Register {
                collection,
                indexes,
                crdt_enabled,
                storage_mode,
                enforcement,
                bitemporal,
            } => self.execute_register_document_collection(
                task,
                tid,
                collection,
                indexes,
                *crdt_enabled,
                storage_mode,
                enforcement,
                *bitemporal,
            ),

            DocumentOp::IndexLookup {
                collection,
                path,
                value,
            } => self.execute_document_index_lookup(task, tid, collection, path, value),

            DocumentOp::IndexedFetch {
                collection,
                path,
                value,
                filters,
                projection,
                limit,
                offset,
            } => self.execute_document_indexed_fetch(
                task, tid, collection, path, value, filters, projection, *limit, *offset,
            ),

            DocumentOp::DropIndex { collection, field } => {
                self.execute_drop_document_index(task, tid, collection, field)
            }

            DocumentOp::BackfillIndex {
                collection,
                path,
                is_array,
                unique,
                case_insensitive,
                predicate,
            } => self.execute_backfill_index(
                task,
                tid,
                collection,
                path,
                *is_array,
                *unique,
                *case_insensitive,
                predicate.as_deref(),
            ),
        }
    }
}
