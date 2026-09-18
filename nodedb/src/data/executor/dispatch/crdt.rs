// SPDX-License-Identifier: BUSL-1.1

//! CRDT operation dispatch.

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::CrdtOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_crdt(&mut self, task: &ExecutionTask, op: &CrdtOp) -> Response {
        match op {
            CrdtOp::Read {
                collection,
                document_id,
            } => self.execute_crdt_read(task, collection.as_str(), document_id),

            CrdtOp::PreviewApply {
                collection,
                document_id,
                delta,
            } => self.execute_crdt_preview_apply(task, collection.as_str(), document_id, delta),

            CrdtOp::Apply {
                collection,
                document_id,
                delta,
                peer_id,
                mutation_id: _,
                surrogate,
                provenance,
                constraint_version_required,
                expected_frontier_digest,
            } => self.execute_crdt_apply(
                task,
                crate::data::executor::handlers::control::crdt_apply::CrdtApplyParams {
                    collection: collection.as_str(),
                    document_id,
                    delta,
                    surrogate: *surrogate,
                    peer_id: *peer_id,
                    provenance: provenance.as_ref(),
                    constraint_version_required: *constraint_version_required,
                    expected_frontier_digest: *expected_frontier_digest,
                    auth_user_id: 0,
                    auth_device_id: 0,
                    auth_seq_no: 0,
                    delta_signature: [0; 32],
                    signing_required: false,
                },
            ),

            CrdtOp::ApplyAuthenticated {
                collection,
                document_id,
                delta,
                peer_id,
                mutation_id: _,
                surrogate,
                provenance,
                constraint_version_required,
                expected_frontier_digest,
                auth_user_id,
                auth_device_id,
                auth_seq_no,
                delta_signature,
                signing_required,
            } => self.execute_crdt_apply(
                task,
                crate::data::executor::handlers::control::crdt_apply::CrdtApplyParams {
                    collection: collection.as_str(),
                    document_id,
                    delta,
                    surrogate: *surrogate,
                    peer_id: *peer_id,
                    provenance: Some(provenance),
                    constraint_version_required: *constraint_version_required,
                    expected_frontier_digest: *expected_frontier_digest,
                    auth_user_id: *auth_user_id,
                    auth_device_id: *auth_device_id,
                    auth_seq_no: *auth_seq_no,
                    delta_signature: *delta_signature,
                    signing_required: *signing_required,
                },
            ),

            CrdtOp::ImportSnapshot {
                tenant_id,
                collection,
                bytes,
            } => self.execute_crdt_import_snapshot(task, *tenant_id, collection.as_str(), bytes),

            CrdtOp::SetPolicy {
                collection,
                policy_json,
            } => self.execute_set_collection_policy(task, collection.as_str(), policy_json),

            CrdtOp::GetPolicy { collection } => {
                self.execute_get_collection_policy(task, collection.as_str())
            }

            CrdtOp::SetConstraints {
                collection,
                constraint_version,
                constraints,
            } => self.execute_crdt_set_constraints(
                task,
                collection.as_str(),
                *constraint_version,
                constraints,
            ),

            CrdtOp::DropConstraints {
                collection,
                constraint_version,
            } => self.execute_crdt_drop_constraints(task, collection.as_str(), *constraint_version),

            CrdtOp::ReadConstraints { collection } => {
                self.execute_crdt_read_constraints(task, collection.as_str())
            }

            CrdtOp::ReadAtVersion {
                collection,
                document_id,
                version_vector_json,
            } => self.execute_crdt_read_at_version(
                task,
                collection.as_str(),
                document_id,
                version_vector_json,
            ),

            CrdtOp::GetVersionVector { collection } => {
                self.execute_crdt_get_version_vector(task, collection.as_str())
            }

            CrdtOp::ExportDelta {
                collection,
                from_version_json,
            } => self.execute_crdt_export_delta(task, collection.as_str(), from_version_json),

            CrdtOp::RestoreToVersion {
                collection,
                document_id,
                target_version_json,
                surrogate: _,
            } => self.execute_crdt_restore(
                task,
                collection.as_str(),
                document_id,
                target_version_json,
            ),

            CrdtOp::CompactAtVersion {
                collection,
                target_version_json,
            } => self.execute_crdt_compact(task, collection.as_str(), target_version_json),

            CrdtOp::ListInsert {
                collection,
                document_id,
                list_path,
                index,
                fields_json,
                surrogate: _,
            } => self.execute_crdt_list_insert(
                task,
                collection.as_str(),
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
            } => self.execute_crdt_list_delete(
                task,
                collection.as_str(),
                document_id,
                list_path,
                *index,
            ),

            CrdtOp::ListMove {
                collection,
                document_id,
                list_path,
                from_index,
                to_index,
                surrogate: _,
            } => self.execute_crdt_list_move(
                task,
                collection.as_str(),
                document_id,
                list_path,
                *from_index,
                *to_index,
            ),

            CrdtOp::DocUpsert {
                collection,
                document_id,
                fields_json,
                surrogate,
                partial,
                // Decides the client's command tag only; the write is the same.
                verb: _,
                returning,
                rls_filters,
            } => self.execute_crdt_doc_upsert(
                task,
                crate::data::executor::handlers::control::crdt_doc::CrdtDocUpsert {
                    collection: collection.as_str(),
                    document_id,
                    fields_json,
                    surrogate: *surrogate,
                    partial: *partial,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),

            CrdtOp::DocDelete {
                collection,
                document_id,
                surrogate,
                returning,
                rls_filters,
            } => self.execute_crdt_doc_delete(
                task,
                crate::data::executor::handlers::control::crdt_doc::CrdtDocDelete {
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
        }
    }
}
