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
                rls_filters,
            } => self.execute_point_get(task, tid, collection, document_id, rls_filters),

            DocumentOp::PointPut {
                collection,
                document_id,
                value,
            } => self.execute_point_put(task, tid, collection, document_id, value),

            DocumentOp::PointDelete {
                collection,
                document_id,
            } => self.execute_point_delete(task, tid, collection, document_id),

            DocumentOp::PointUpdate {
                collection,
                document_id,
                updates,
                returning,
            } => self.execute_point_update(task, tid, collection, document_id, updates, *returning),

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
            } => self.execute_document_scan(
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
            ),

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
            } => self.execute_upsert(
                task,
                tid,
                collection,
                document_id,
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
                index_paths,
                crdt_enabled,
                storage_mode,
                enforcement,
            } => self.execute_register_document_collection(
                task,
                tid,
                collection,
                index_paths,
                *crdt_enabled,
                storage_mode,
                enforcement,
            ),

            DocumentOp::IndexLookup {
                collection,
                path,
                value,
            } => self.execute_document_index_lookup(task, tid, collection, path, value),

            DocumentOp::DropIndex { collection, field } => {
                self.execute_drop_document_index(task, tid, collection, field)
            }
        }
    }
}
