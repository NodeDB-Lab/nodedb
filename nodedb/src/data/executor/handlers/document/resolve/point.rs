// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the keyed document writes: `PointUpdate`, `PointDelete`.
//! Each reads the row and computes the post-image via the same functions the
//! live handler calls, then reports the mutation instead of applying it —
//! reuse is what stops resolve and apply diverging on the admitted image.

use nodedb_physical::physical_plan::{
    DocumentResolveOutcome, ResolvedSumTarget, ReturningSpec, UpdateValue,
};
use nodedb_types::{RlsWriteCheck, Surrogate};

use super::context::{
    ResolveResult, ResolvedPut, affected_payload, delete_mutation, put_mutation,
    resolved_response_payload, row_key_of,
};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::update::post_image::{
    PointUpdateImage, point_update_body_to_msgpack,
};
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};

/// Borrowed arguments for [`CoreLoop::resolve_point_update`].
pub(super) struct ResolvePointUpdate<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub updates: &'a [(String, UpdateValue)],
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub rls_write_check: &'a RlsWriteCheck,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// Declared `PRIMARY KEY` column of a schemaless collection, `None`
    /// otherwise — see `PointUpdateImage::declared_primary_key`.
    pub declared_primary_key: Option<&'a str>,
}

/// Borrowed arguments for [`CoreLoop::resolve_point_delete`].
pub(super) struct ResolvePointDelete<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub rls_write_check: &'a RlsWriteCheck,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Resolve a `PointUpdate` to the one row write it would apply.
    ///
    /// A row that is already gone resolves to no mutation and the
    /// `{"affected": 0}` reply the live handler returns for the same input.
    pub(super) fn resolve_point_update(
        &self,
        task: &ExecutionTask,
        args: ResolvePointUpdate<'_>,
    ) -> ResolveResult {
        let ResolvePointUpdate {
            tid,
            collection,
            document_id,
            surrogate,
            updates,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            declared_primary_key,
        } = args;
        let ctx = self.doc_resolve_ctx(task, tid, collection);
        let row_key = row_key_of(surrogate);
        let row_identity = StorageKey::for_surrogate(surrogate).to_identity();
        let document_identity = RowIdentity::from_user_key(document_id);

        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        // Same refusals as `execute_point_update`, raised here so a refused
        // statement never reaches Raft (apply-time refusal is post-commit).
        if let Some(config) = self.doc_configs.get(&config_key) {
            crate::data::executor::handlers::generated::check_generated_readonly(
                updates,
                &config.enforcement.generated_columns,
            )?;
            crate::data::executor::enforcement::append_only::check_point_update(
                collection,
                &config.enforcement,
            )?;
        }

        // A gone row reports `{"affected": 0}`, same as `execute_point_update`.
        let Some(current_bytes) = self.doc_resolve_read(&ctx, collection, &row_key)? else {
            return Ok(DocumentResolveOutcome {
                mutations: Vec::new(),
                response_payload: affected_payload(0),
            });
        };

        let is_strict = ctx.strict_schema.is_some();
        let has_expr = updates
            .iter()
            .any(|(_, v)| matches!(v, UpdateValue::Expr(_)));
        let has_generated = self.doc_configs.get(&config_key).is_some_and(|c| {
            !c.enforcement.generated_columns.is_empty()
                && crate::data::executor::handlers::generated::needs_recomputation(
                    updates,
                    &c.enforcement.generated_columns,
                )
        });
        // Stamped as the live handler stamps it; apply mints its own on write.
        let sys_from_ms = if ctx.bitemporal {
            self.bitemporal_now_ms()
        } else {
            0
        };
        let image_params = PointUpdateImage {
            config_key: &config_key,
            current_bytes: &current_bytes,
            updates,
            is_strict,
            has_generated,
            has_expr,
            bitemporal: ctx.bitemporal,
            sys_from_ms,
            declared_primary_key,
        };
        let body = self.compute_point_update_body(image_params)?;
        // The STORED image the policy decides against and `RETURNING` projects,
        // and the pre-encode body the apply writes — both from ONE computation.
        let stored_image = self.encode_point_update_body(image_params, &body)?;
        let value = point_update_body_to_msgpack(&body);

        rls_write_gate::admit_stored_row(
            rls_write_check,
            &stored_image,
            &row_identity,
            ctx.strict_schema.as_ref(),
            tid,
            collection,
        )
        .map_err(ErrorCode::from)?;

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
                value,
                precondition: Some(current_bytes),
                resolved_sum_targets,
            })],
            response_payload,
        })
    }

    /// Resolve a `PointDelete` to the one row removal it would apply. The
    /// pre-image is the only image a delete has, so it is both what the
    /// policy decides and what `RETURNING` projects.
    pub(super) fn resolve_point_delete(
        &self,
        task: &ExecutionTask,
        args: ResolvePointDelete<'_>,
    ) -> ResolveResult {
        let ResolvePointDelete {
            tid,
            collection,
            document_id,
            surrogate,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
        } = args;
        let ctx = self.doc_resolve_ctx(task, tid, collection);
        let row_key = row_key_of(surrogate);
        let row_identity = StorageKey::for_surrogate(surrogate).to_identity();
        let document_identity = RowIdentity::from_user_key(document_id);

        // A row that is already absent removes nothing, so there is no image for
        // the policy to restrict — the same admission `gate_point_delete` makes.
        let Some(prior) = self.doc_resolve_read(&ctx, collection, &row_key)? else {
            return Ok(DocumentResolveOutcome {
                mutations: Vec::new(),
                response_payload: resolved_response_payload(
                    returning,
                    rls_filters,
                    ctx.strict_schema.as_ref(),
                    &[],
                )?,
            });
        };

        rls_write_gate::admit_stored_row(
            rls_write_check,
            &prior,
            &row_identity,
            ctx.strict_schema.as_ref(),
            tid,
            collection,
        )
        .map_err(ErrorCode::from)?;

        let response_payload = resolved_response_payload(
            returning,
            rls_filters,
            ctx.strict_schema.as_ref(),
            &[(&document_identity, prior.as_slice())],
        )?;
        Ok(DocumentResolveOutcome {
            mutations: vec![delete_mutation(
                collection,
                document_id,
                surrogate,
                Some(prior),
                resolved_sum_targets,
            )],
            response_payload,
        })
    }
}
