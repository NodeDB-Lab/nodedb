// SPDX-License-Identifier: BUSL-1.1

//! Classify a `DocumentOp` into an optional `ReplicatedWrite`.
//!
//! Exhaustive over `DocumentOp` (not a catch-all): a new variant is a compile
//! error here, so no future document write is silently left un-replicated.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::document;
use super::document::{SumFields, WireReturning};
use super::entry::encode_returning;
use nodedb_physical::physical_plan::DocumentOp;

/// Encode a `DocumentOp` write variant into its `ReplicatedWrite` wire shape,
/// or `None` when the op is not a single-shard replicated write.
pub(super) fn document_write(op: &DocumentOp) -> Option<ReplicatedWrite> {
    Some(match op {
        DocumentOp::PointPut {
            collection,
            document_id,
            value,
            surrogate,
            // Decode re-derives this from `document_id.as_bytes()`, same value.
            pk_bytes: _,
            returning,
            rls_filters,
            // Resolved at plan time and copied onto the record: the applier can't
            // resolve the target row's identity itself — see `document`'s module doc.
            resolved_sum_targets,
        } => document::point_put(
            collection.as_str(),
            document_id,
            value,
            surrogate.as_u32(),
            resolved_sum_targets,
            encode_returning(returning),
            rls_filters,
        ),
        DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
            returning,
            rls_filters,
            // See `PointPut`.
            resolved_sum_targets,
            deferred_sum_targets,
        } => document::point_insert(
            collection.as_str(),
            document_id,
            value,
            *if_absent,
            surrogate.as_u32(),
            SumFields {
                resolved: resolved_sum_targets,
                deferred: deferred_sum_targets,
            },
            WireReturning {
                returning: encode_returning(returning),
                rls_filters,
            },
        ),
        DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            // Decode re-derives this from `document_id.as_bytes()`, same value.
            pk_bytes: _,
            returning,
            rls_filters,
            // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
            rls_write_check: _,
            // See `PointPut`.
            resolved_sum_targets,
        } => document::point_delete(
            collection.as_str(),
            document_id,
            surrogate.as_u32(),
            resolved_sum_targets,
            encode_returning(returning),
            rls_filters,
        ),
        DocumentOp::PointUpdate {
            collection,
            document_id,
            surrogate,
            // Decode re-derives this from `document_id.as_bytes()`, same value.
            pk_bytes: _,
            updates,
            returning,
            rls_filters,
            // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
            rls_write_check: _,
            // See `PointPut`.
            resolved_sum_targets,
            // Carried on the record so an applier can enforce NOT NULL on the
            // computed post-image — see `decode/document.rs`.
            declared_primary_key,
        } => document::point_update(
            collection.as_str(),
            document_id,
            updates,
            surrogate.as_u32(),
            resolved_sum_targets,
            WireReturning {
                returning: encode_returning(returning),
                rls_filters,
            },
            declared_primary_key.as_deref(),
        ),
        DocumentOp::Upsert {
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate,
            // Leader already decided this row; the record carries the row, not the policy.
            rls_write_check: _,
            returning,
            rls_filters,
            // See `PointPut`.
            resolved_sum_targets,
        } => document::upsert(
            collection.as_str(),
            document_id,
            value,
            on_conflict_updates,
            surrogate.as_u32(),
            resolved_sum_targets,
            WireReturning {
                returning: encode_returning(returning),
                rls_filters,
            },
        ),
        DocumentOp::BulkDelete {
            collection,
            filters,
            returning,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters,
            // Leader already decided every matched row; the record carries the predicate.
            rls_write_check: _,
            // See `PointPut`. Matches are re-derived by every replica; target identity is not.
            resolved_sum_targets,
            // Carried on the record so a staged replay keys each removed row
            // by its identity — see `decode/document.rs`.
            declared_primary_key,
        } => document::bulk_delete(
            collection.as_str(),
            filters,
            resolved_sum_targets,
            encode_returning(returning),
            rls_filters,
            declared_primary_key.as_deref(),
        ),
        DocumentOp::BulkUpdate {
            collection,
            filters,
            updates,
            returning,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters,
            // See `BulkDelete`.
            rls_write_check: _,
            // See `BulkDelete`.
            resolved_sum_targets,
            // Carried on the record so an applier can enforce NOT NULL on the
            // computed post-image — see `decode/document.rs`.
            declared_primary_key,
        } => document::bulk_update(
            collection.as_str(),
            filters,
            updates,
            resolved_sum_targets,
            encode_returning(returning),
            rls_filters,
            declared_primary_key.as_deref(),
        ),
        DocumentOp::InsertSelect {
            target_collection,
            source_collection,
            source_filters,
            source_limit,
            column_map,
        } => document::insert_select(
            target_collection.as_str(),
            source_collection.as_str(),
            source_filters,
            *source_limit,
            column_map,
        ),

        DocumentOp::BatchInsert {
            collection,
            documents,
            surrogates,
            returning,
            rls_filters,
            // See `PointPut`.
            resolved_sum_targets,
            deferred_sum_targets,
        } => document::batch_insert(
            collection.as_str(),
            documents,
            surrogates,
            resolved_sum_targets,
            deferred_sum_targets,
            encode_returning(returning),
            rls_filters,
        ),

        // Known gap: cross-collection writes whose source/target co-location is
        // not enforced (`Unroutable` in `plan_vshard`); no ReplicatedWrite shape yet.
        DocumentOp::Merge { .. } | DocumentOp::UpdateFromJoin { .. } => return None,
        DocumentOp::Truncate {
            collection,
            restart_identity,
            // See `PointPut`.
            resolved_sum_targets,
        } => document::truncate(collection.as_str(), *restart_identity, resolved_sum_targets),
        // OLLP-prepared bulk plans route via the cross-shard Calvin path, not
        // single-shard Raft proposal.
        DocumentOp::BulkDelete { .. } | DocumentOp::BulkUpdate { .. } => return None,

        // Verdict is already on the plan (`RlsWriteCheck::DecidedEarlierInRequest`),
        // so nothing to re-decide here.
        DocumentOp::ResolvedWrite {
            mutations,
            response_payload,
            // Decode stamps `decided_earlier_in_request()`.
            rls_write_check: _,
        } => document::resolved_write(mutations, response_payload),

        DocumentOp::ApplyBalanceDelta {
            collection,
            document_id,
            surrogate,
            column,
            delta,
            join_column,
            join_value,
        } => document::apply_balance_delta(
            collection.as_str(),
            document_id,
            surrogate.as_u32(),
            column,
            delta,
            join_column,
            join_value,
        ),

        // Not a write — reads / scans / index DDL-metadata / system ops.
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => return None,
    })
}
