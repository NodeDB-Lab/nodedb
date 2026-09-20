// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Document`, delegated from `decode/entry.rs`'s grouped match
//! arm to stay under the file size limit. `write` is guaranteed to be one of
//! these variants.

use super::super::decode_sync_engines::decode_returning;
use super::super::types::{BalanceDeltaFields, ReplicatedSumTarget, ReplicatedWrite};
use super::document;
use super::document::{PointInsertOptions, ReturningFields, UpsertExtras, WireSumResolution};
use super::document_join;
use crate::bridge::envelope::PhysicalPlan;

/// Pair a record's two materialized-sum resolution slots so the decoder, not
/// each call site, decides which answers — see [`WireSumResolution`].
fn sums<'a>(
    bindings: &'a [ReplicatedSumTarget],
    legacy: &'a [(String, u32)],
) -> WireSumResolution<'a> {
    WireSumResolution { bindings, legacy }
}

pub(super) fn decode_arm(write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
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
        } => Ok(document::point_put(
            collection,
            document_id,
            value,
            *surrogate,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
        )),
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
        } => Ok(document::point_insert(
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
        )),
        ReplicatedWrite::PointDelete {
            collection,
            document_id,
            surrogate,
            resolved_sum_targets,
            resolved_sum_target_bindings,
            returning,
            rls_filters,
        } => Ok(document::point_delete(
            collection,
            document_id,
            *surrogate,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
        )),
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
        } => Ok(document::point_update(
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
        )),
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
        } => Ok(document::doc_upsert(
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
        )),
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
            declared_primary_key,
        } => Ok(document::truncate(
            collection,
            *restart_identity,
            &sums(resolved_sum_target_bindings, resolved_sum_targets),
            declared_primary_key.clone(),
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
            declared_primary_key,
        } => Ok(document::apply_balance_delta(BalanceDeltaFields {
            collection,
            document_id,
            surrogate: *surrogate,
            column,
            delta,
            join_column,
            join_value,
            declared_primary_key: declared_primary_key.as_deref(),
        })),
        ReplicatedWrite::DocumentResolvedWrite {
            mutations,
            response_payload,
        } => Ok(document::resolved_write(mutations, response_payload)),
        ReplicatedWrite::MergeApply {
            target_collection,
            source_collection,
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
            resolved_sum_target_bindings,
            declared_primary_key,
        } => Ok(document_join::merge_apply(
            document_join::JoinFields {
                target_collection,
                source_collection,
                source_alias,
                target_join_col,
                source_join_col,
                source_rows,
                resolved_sum_targets: &sums(resolved_sum_target_bindings, resolved_sum_targets),
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
        )),
        ReplicatedWrite::UpdateFromJoinApply {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
            updates,
            target_filters,
            returning,
            source_rows,
            rls_filters,
            resolved_sum_targets,
            resolved_sum_target_bindings,
            declared_primary_key,
        } => Ok(document_join::update_from_join_apply(
            document_join::JoinFields {
                target_collection,
                source_collection,
                source_alias,
                target_join_col,
                source_join_col,
                source_rows,
                resolved_sum_targets: &sums(resolved_sum_target_bindings, resolved_sum_targets),
                declared_primary_key: declared_primary_key.clone(),
            },
            updates,
            target_filters,
            ReturningFields {
                returning: decode_returning(returning)?,
                rls_filters,
            },
        )),
        _ => Err(crate::Error::Internal {
            detail: "entry_document::decode_arm called with a non-Document ReplicatedWrite \
                variant (dispatch bug in decode/entry.rs's grouped Document match arm)"
                .into(),
        }),
    }
}
