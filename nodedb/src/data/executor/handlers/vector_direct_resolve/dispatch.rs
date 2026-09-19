// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `VectorOp::ResolveDirectWrite`. Reports what the
//! wrapped vector-primary write would apply and reply, mutating nothing —
//! run before proposing a governed `DELETE` / `UPDATE` / conflict-patching
//! `UPSERT`, since a follower has no writing identity to decide the policy
//! and re-deriving after commit risks a different answer per replica.
//! Read-only, so this takes `&self`.

use nodedb_physical::physical_plan::VectorOp;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_direct_delete::VectorDirectDeleteParams;
use crate::data::executor::handlers::vector_direct_update::VectorDirectUpdateParams;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

use super::resolve_upsert::VectorResolveUpsertParams;

impl CoreLoop {
    /// Handle `VectorOp::ResolveDirectWrite`: resolve `inner` against current
    /// state, decide the policy per row, and report the mutation list plus
    /// the reply. Mutates nothing.
    pub(in crate::data::executor) fn execute_vector_resolve_direct_write(
        &self,
        task: &ExecutionTask,
        tid: u64,
        inner: &VectorOp,
    ) -> Response {
        debug!(core = self.core_id, "vector resolve direct write");
        let outcome = match inner {
            VectorOp::DirectDelete {
                collection,
                field,
                targets,
                returning,
                rls_filters,
                rls_write_check,
            } => self.resolve_vector_direct_delete(VectorDirectDeleteParams {
                task,
                tid,
                collection: collection.as_str(),
                field,
                targets,
                returning: returning.as_ref(),
                rls_filters,
                rls_write_check,
            }),
            VectorOp::DirectUpdate {
                collection,
                field,
                targets,
                new_vector,
                payload_patch,
                quantization,
                storage_dtype,
                payload_indexes,
                returning,
                rls_filters,
                rls_write_check,
            } => self.resolve_vector_direct_update(VectorDirectUpdateParams {
                task,
                tid,
                collection: collection.as_str(),
                field,
                targets,
                new_vector: new_vector.as_deref(),
                payload_patch,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
                returning: returning.as_ref(),
                rls_filters,
                rls_write_check,
            }),
            VectorOp::DirectUpsert {
                collection,
                field,
                surrogate,
                pk_bytes,
                vector,
                payload,
                quantization,
                storage_dtype,
                payload_indexes,
                returning,
                rls_filters,
                on_conflict_updates,
                rls_write_check,
            } => self.resolve_vector_direct_upsert(VectorResolveUpsertParams {
                task,
                tid,
                collection: collection.as_str(),
                field,
                surrogate: *surrogate,
                pk_bytes,
                vector,
                payload,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
                on_conflict_updates,
                rls_write_check,
                returning: returning.as_ref(),
                rls_filters,
            }),
            // Every other op is unwrapped by `resolver_for_plan` already.
            VectorOp::Search { .. }
            | VectorOp::Insert { .. }
            | VectorOp::BatchInsert { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::Delete { .. }
            | VectorOp::DeleteBySurrogate { .. }
            | VectorOp::SetParams { .. }
            | VectorOp::DropIndex { .. }
            | VectorOp::QueryStats { .. }
            | VectorOp::Seal { .. }
            | VectorOp::CompactIndex { .. }
            | VectorOp::Rebuild { .. }
            | VectorOp::SparseInsert { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::SparseDelete { .. }
            | VectorOp::MultiVectorInsert { .. }
            | VectorOp::MultiVectorDelete { .. }
            | VectorOp::MultiVectorScoreSearch { .. }
            | VectorOp::DirectInsert { .. }
            | VectorOp::DirectInsertIfAbsent { .. }
            | VectorOp::ResolveDirectWrite(_)
            | VectorOp::ResolvedDirectWrite { .. } => Err(ErrorCode::Internal {
                detail: "vector resolve-direct-write wraps an op with no governed row image; \
                         only DirectDelete, DirectUpdate, and DirectUpsert resolve"
                    .to_owned(),
            }),
        };

        match outcome {
            Ok(resolved) => match response_codec::encode(&resolved) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, e),
            },
            Err(code) => self.response_error(task, code),
        }
    }
}
