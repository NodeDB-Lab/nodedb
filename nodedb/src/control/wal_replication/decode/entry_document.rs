// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Document`, delegated from `decode/entry.rs`'s grouped match
//! arm to stay under the file size limit. `write` is guaranteed to be one of
//! these variants.

use super::super::decode_sync_engines::decode_returning;
use super::super::types::{BalanceDeltaFields, ReplicatedWrite};
use super::ctx::DecodeCtx;
use super::document;
use super::document::{PointInsertOptions, ReturningFields, UpsertExtras};
use super::document_join;
use crate::bridge::envelope::PhysicalPlan;

pub(super) fn decode_arm(ctx: &DecodeCtx, write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    let database_id = ctx.database_id;
    match write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
            surrogate,
            resolved_sum_targets,
            returning,
            rls_filters,
        } => document::point_put(
            database_id,
            collection,
            document_id,
            value,
            *surrogate,
            resolved_sum_targets,
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
            returning,
            rls_filters,
        } => document::point_insert(
            database_id,
            collection,
            document_id,
            value,
            *if_absent,
            *surrogate,
            PointInsertOptions {
                sums: document::SumDecisions {
                    resolved: resolved_sum_targets,
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
            returning,
            rls_filters,
        } => document::point_delete(
            database_id,
            collection,
            document_id,
            *surrogate,
            resolved_sum_targets,
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
            returning,
            rls_filters,
            declared_primary_key,
        } => document::point_update(
            database_id,
            collection,
            document_id,
            updates,
            *surrogate,
            document::PointUpdateExtras {
                resolved_sum_targets,
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
            returning,
            rls_filters,
        } => document::doc_upsert(
            database_id,
            collection,
            document_id,
            value,
            on_conflict_updates,
            *surrogate,
            UpsertExtras {
                resolved_sum_targets,
                returning: ReturningFields {
                    returning: decode_returning(returning)?,
                    rls_filters,
                },
            },
        ),
        ReplicatedWrite::DocBatchUpsert {
            collection,
            documents,
            surrogates,
            on_conflict_updates,
            resolved_sum_targets,
            returning,
            rls_filters,
        } => document::batch_upsert(
            database_id,
            collection,
            documents,
            surrogates,
            on_conflict_updates,
            UpsertExtras {
                resolved_sum_targets,
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
            if_absent,
            resolved_sum_targets,
            deferred_sum_targets,
            returning,
            rls_filters,
        } => document::batch_insert(
            database_id,
            collection,
            documents,
            surrogates,
            *if_absent,
            document::BatchSumTargets {
                resolved: resolved_sum_targets,
                deferred: deferred_sum_targets,
            },
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
        ),
        ReplicatedWrite::DocTruncate {
            collection,
            restart_identity,
            resolved_sum_targets,
            declared_primary_key,
        } => document::truncate(
            database_id,
            collection,
            *restart_identity,
            resolved_sum_targets,
            declared_primary_key.clone(),
        ),
        ReplicatedWrite::BulkDml {
            collection,
            filters,
            is_update,
            updates,
            resolved_sum_targets,
            returning,
            rls_filters,
            declared_primary_key,
        } => document::bulk_dml(
            database_id,
            collection,
            filters,
            *is_update,
            updates,
            document::BulkDmlExtras {
                resolved_sum_targets,
                returning: ReturningFields {
                    returning: decode_returning(returning)?,
                    rls_filters,
                },
                declared_primary_key: declared_primary_key.clone(),
            },
        ),
        ReplicatedWrite::ApplyBalanceDelta {
            collection,
            document_id,
            surrogate,
            column,
            delta,
            join_column,
            join_value,
            declared_primary_key,
        } => document::apply_balance_delta(
            database_id,
            BalanceDeltaFields {
                collection,
                document_id,
                surrogate: *surrogate,
                column,
                delta,
                join_column,
                join_value,
                declared_primary_key: declared_primary_key.as_deref(),
            },
        ),
        ReplicatedWrite::DocumentResolvedWrite {
            mutations,
            response_payload,
        } => document::resolved_write(database_id, mutations, response_payload),
        ReplicatedWrite::MergeApply {
            target_collection,
            source,
            source_alias,
            target_join_col,
            source_join_col,
            clauses,
            returning,
            resolved_inserts,
            resolved_insert_identities,
            source_rows,
            rls_filters,
            resolved_sum_targets,
            declared_primary_key,
        } => document_join::merge_apply(
            database_id,
            document_join::JoinFields {
                target_collection,
                source,
                source_alias,
                target_join_col,
                source_join_col,
                source_rows,
                resolved_sum_targets,
                declared_primary_key: declared_primary_key.clone(),
            },
            document_join::MergeFields {
                clauses,
                resolved_inserts,
                resolved_insert_identities,
            },
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
        ),
        ReplicatedWrite::UpdateFromJoinApply {
            target_collection,
            source,
            source_alias,
            target_join_col,
            source_join_col,
            updates,
            target_filters,
            returning,
            source_rows,
            rls_filters,
            resolved_sum_targets,
            declared_primary_key,
        } => document_join::update_from_join_apply(
            database_id,
            document_join::JoinFields {
                target_collection,
                source,
                source_alias,
                target_join_col,
                source_join_col,
                source_rows,
                resolved_sum_targets,
                declared_primary_key: declared_primary_key.clone(),
            },
            updates,
            target_filters,
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
        ),
        _ => Err(crate::Error::Internal {
            detail: "entry_document::decode_arm called with a non-Document ReplicatedWrite \
                variant (dispatch bug in decode/entry.rs's grouped Document match arm)"
                .into(),
        }),
    }
}
