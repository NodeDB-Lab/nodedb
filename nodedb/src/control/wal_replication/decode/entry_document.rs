// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Document`, delegated from `decode/entry.rs`'s grouped match
//! arm to stay under the file size limit. `write` is guaranteed to be one of
//! these variants.

use super::super::decode_sync_engines::decode_returning;
use super::super::types::{ReplicatedSumTarget, ReplicatedWrite};
use super::ctx::DecodeCtx;
use super::document;
use super::document::{PointInsertOptions, ReturningFields, UpsertExtras, WireSumResolution};
use crate::bridge::envelope::PhysicalPlan;

/// Pair a record's two materialized-sum resolution slots so the decoder, not
/// each call site, decides which answers — see [`WireSumResolution`].
fn sums<'a>(
    bindings: &'a [ReplicatedSumTarget],
    legacy: &'a [(String, u32)],
) -> WireSumResolution<'a> {
    WireSumResolution { bindings, legacy }
}

pub(super) fn decode_arm(ctx: &DecodeCtx, write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
            surrogate,
            resolved_sum_targets,
            resolved_sum_target_bindings,
            returning,
            rls_filters,
        } => document::point_put(
            ctx,
            collection,
            document_id,
            value,
            *surrogate,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
        ),
        ReplicatedWrite::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
            resolved_sum_targets,
            deferred_sum_targets,
            resolved_sum_target_bindings,
            returning,
            rls_filters,
        } => document::point_insert(
            ctx,
            collection,
            document_id,
            value,
            *if_absent,
            *surrogate,
            PointInsertOptions {
                sums: document::SumDecisions {
                    resolved: sums(resolved_sum_target_bindings, resolved_sum_targets),
                    deferred: deferred_sum_targets,
                },
                returning: ReturningFields {
                    returning: decode_returning(returning)?,
                    rls_filters,
                },
            },
        ),
        ReplicatedWrite::PointDelete {
            collection,
            document_id,
            surrogate,
            resolved_sum_targets,
            resolved_sum_target_bindings,
            returning,
            rls_filters,
        } => document::point_delete(
            ctx,
            collection,
            document_id,
            *surrogate,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
        ),
        ReplicatedWrite::PointUpdate {
            collection,
            document_id,
            updates,
            surrogate,
            resolved_sum_targets,
            resolved_sum_target_bindings,
            returning,
            rls_filters,
            declared_primary_key,
        } => document::point_update(
            ctx,
            collection,
            document_id,
            updates,
            *surrogate,
            document::PointUpdateExtras {
                resolved_sum_targets: &sums(resolved_sum_target_bindings, resolved_sum_targets),
                returning: ReturningFields {
                    returning: decode_returning(returning)?,
                    rls_filters,
                },
                declared_primary_key: declared_primary_key.clone(),
            },
        ),
        ReplicatedWrite::DocUpsert {
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate,
            resolved_sum_targets,
            resolved_sum_target_bindings,
            returning,
            rls_filters,
        } => document::doc_upsert(
            ctx,
            collection,
            document_id,
            value,
            on_conflict_updates,
            *surrogate,
            UpsertExtras {
                resolved_sum_targets: &sums(resolved_sum_target_bindings, resolved_sum_targets),
                returning: ReturningFields {
                    returning: decode_returning(returning)?,
                    rls_filters,
                },
            },
        ),
        ReplicatedWrite::DocBatchInsert {
            collection,
            documents,
            surrogates,
            resolved_sum_targets,
            deferred_sum_targets,
            resolved_sum_target_bindings,
            returning,
            rls_filters,
        } => document::batch_insert(
            ctx,
            collection,
            documents,
            surrogates,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
            deferred_sum_targets,
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
        ),
        ReplicatedWrite::DocTruncate {
            collection,
            restart_identity,
            resolved_sum_targets,
            resolved_sum_target_bindings,
        } => Ok(document::truncate(
            collection,
            *restart_identity,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
        )),
        ReplicatedWrite::BulkDml {
            collection,
            filters,
            is_update,
            updates,
            resolved_sum_targets,
            resolved_sum_target_bindings,
            returning,
            rls_filters,
            declared_primary_key,
        } => Ok(document::bulk_dml(
            collection,
            filters,
            *is_update,
            updates,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
            declared_primary_key.clone(),
        )),
        ReplicatedWrite::InsertSelect {
            target_collection,
            source_collection,
            source_filters,
            source_limit,
            column_map,
        } => Ok(document::insert_select(
            target_collection,
            source_collection,
            source_filters,
            *source_limit,
            column_map,
        )),
        ReplicatedWrite::ApplyBalanceDelta {
            collection,
            document_id,
            surrogate,
            column,
            delta,
            join_column,
            join_value,
        } => Ok(document::apply_balance_delta(
            collection,
            document_id,
            *surrogate,
            column,
            delta,
            join_column,
            join_value,
        )),
        ReplicatedWrite::DocumentResolvedWrite {
            mutations,
            response_payload,
        } => document::resolved_write(ctx, mutations, response_payload),
        _ => Err(crate::Error::Internal {
            detail: "entry_document::decode_arm called with a non-Document ReplicatedWrite \
                variant (dispatch bug in decode/entry.rs's grouped Document match arm)"
                .into(),
        }),
    }
}
