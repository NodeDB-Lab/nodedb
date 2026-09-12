// SPDX-License-Identifier: BUSL-1.1

//! Resolver for `Upsert`. Probes for the row exactly as `execute_upsert` does
//! and takes the same two branches. The merge branch's precondition is the
//! present row's stored bytes; the insert branch's is `None`, requiring the
//! row to still be gone at apply.

use nodedb_physical::physical_plan::{
    DocumentResolveOutcome, ResolvedSumTarget, ReturningSpec, UpdateValue,
};
use nodedb_types::{RlsWriteCheck, Surrogate};

use super::context::{
    ResolveResult, ResolvedPut, put_mutation, resolved_response_payload, row_key_of,
};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::stored_body::StoredBodyInput;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::handlers::upsert::merge::{apply_on_conflict_updates, merge_values};
use crate::data::executor::strict_format;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::RowIdentity;

/// Borrowed arguments for [`CoreLoop::resolve_upsert`].
pub(super) struct ResolveUpsert<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub rls_write_check: &'a RlsWriteCheck,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Resolve an `Upsert` to the one row write it would apply.
    pub(super) fn resolve_upsert(
        &self,
        task: &ExecutionTask,
        args: ResolveUpsert<'_>,
    ) -> ResolveResult {
        let ResolveUpsert {
            tid,
            collection,
            document_id,
            surrogate,
            value,
            on_conflict_updates,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
        } = args;
        let ctx = self.doc_resolve_ctx(task, tid, collection);
        let row_key = row_key_of(surrogate);
        let row_identity =
            crate::engine::document::store::StorageKey::for_surrogate(surrogate).to_identity();
        let document_identity = RowIdentity::from_user_key(document_id);

        let existing = self.doc_resolve_read(&ctx, collection, &row_key)?;
        let (body, precondition) = match existing {
            Some(current_bytes) => {
                let merged = self.merge_upsert_body(
                    &current_bytes,
                    value,
                    on_conflict_updates,
                    ctx.strict_schema.as_ref(),
                )?;
                (merged, Some(current_bytes))
            }
            // No merge on the insert branch: the incoming body IS the
            // post-image, exactly as `execute_upsert_insert` treats it.
            None => (value.to_vec(), None),
        };

        // Both live branches gate the MessagePack body, no schema passed.
        rls_write_gate::admit_stored_row(
            rls_write_check,
            &body,
            &row_identity,
            None,
            tid,
            collection,
        )
        .map_err(ErrorCode::from)?;

        // `RETURNING` projects the stored image via the same `build_stored_body`
        // the apply runs, so the resolve reports the row that will actually land.
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let stored_image = self
            .build_stored_body(StoredBodyInput {
                config_key: &config_key,
                surrogate,
                value: &body,
                bitemporal: ctx.bitemporal,
                sys_from_ms: self.bitemporal_now_ms(),
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
            })
            .map_err(ErrorCode::from)?
            .stored;
        let response_payload = resolved_response_payload(
            returning,
            rls_filters,
            ctx.strict_schema.as_ref(),
            &[(&document_identity, stored_image.as_slice())],
        )?;

        Ok(DocumentResolveOutcome {
            mutations: vec![put_mutation(ResolvedPut {
                collection,
                document_id,
                surrogate,
                value: body,
                precondition,
                resolved_sum_targets,
            })],
            response_payload,
        })
    }

    /// The merged body an upsert's conflict branch stores, as MessagePack —
    /// the same decode → merge → encode `execute_upsert_overwrite` performs.
    fn merge_upsert_body(
        &self,
        current_bytes: &[u8],
        value: &[u8],
        on_conflict_updates: &[(String, UpdateValue)],
        strict_schema: Option<&nodedb_types::columnar::StrictSchema>,
    ) -> Result<Vec<u8>, ErrorCode> {
        let existing_val = match strict_schema {
            Some(schema) => match strict_format::binary_tuple_to_value(current_bytes, schema) {
                Some(v) => v,
                // Migration case: a pre-strict row is still MessagePack.
                None => nodedb_types::value_from_msgpack(current_bytes).map_err(|_| {
                    ErrorCode::Internal {
                        detail: "failed to decode document for upsert".into(),
                    }
                })?,
            },
            None => nodedb_types::value_from_msgpack(current_bytes).map_err(|_| {
                ErrorCode::Internal {
                    detail: "failed to decode document for upsert".into(),
                }
            })?,
        };
        let new_val = nodedb_types::value_from_msgpack(value).map_err(|_| ErrorCode::Internal {
            detail: "failed to decode upsert value from msgpack".into(),
        })?;

        let merged = if on_conflict_updates.is_empty() {
            merge_values(existing_val, new_val)
        } else {
            apply_on_conflict_updates(existing_val, &new_val, on_conflict_updates)
                .map_err(ErrorCode::from)?
        };
        nodedb_types::value_to_msgpack(&merged).map_err(|_| ErrorCode::Internal {
            detail: "failed to encode merged upsert value".into(),
        })
    }
}
