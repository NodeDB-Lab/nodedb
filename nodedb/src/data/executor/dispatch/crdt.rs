//! CRDT operation dispatch.

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::CrdtOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_crdt(&mut self, task: &ExecutionTask, op: &CrdtOp) -> Response {
        match op {
            CrdtOp::Read {
                collection,
                document_id,
            } => self.execute_crdt_read(task, collection, document_id),

            CrdtOp::Apply {
                collection: _,
                document_id: _,
                delta,
                peer_id: _,
                mutation_id: _,
                surrogate: _,
            } => self.execute_crdt_apply(task, delta),

            CrdtOp::SetPolicy {
                collection,
                policy_json,
            } => self.execute_set_collection_policy(task, collection, policy_json),

            CrdtOp::ReadAtVersion {
                collection,
                document_id,
                version_vector_json,
            } => self.execute_crdt_read_at_version(
                task,
                collection,
                document_id,
                version_vector_json,
            ),

            CrdtOp::GetVersionVector => self.execute_crdt_get_version_vector(task),

            CrdtOp::ExportDelta { from_version_json } => {
                self.execute_crdt_export_delta(task, from_version_json)
            }

            CrdtOp::RestoreToVersion {
                collection,
                document_id,
                target_version_json,
                surrogate: _,
            } => self.execute_crdt_restore(task, collection, document_id, target_version_json),

            CrdtOp::CompactAtVersion {
                target_version_json,
            } => self.execute_crdt_compact(task, target_version_json),

            CrdtOp::ListInsert {
                collection,
                document_id,
                list_path,
                index,
                fields_json,
                surrogate: _,
            } => self.execute_crdt_list_insert(
                task,
                collection,
                document_id,
                list_path,
                *index,
                fields_json,
            ),

            CrdtOp::ListDelete {
                collection,
                document_id,
                list_path,
                index,
                surrogate: _,
            } => self.execute_crdt_list_delete(task, collection, document_id, list_path, *index),

            CrdtOp::ListMove {
                collection,
                document_id,
                list_path,
                from_index,
                to_index,
                surrogate: _,
            } => self.execute_crdt_list_move(
                task,
                collection,
                document_id,
                list_path,
                *from_index,
                *to_index,
            ),
        }
    }
}
