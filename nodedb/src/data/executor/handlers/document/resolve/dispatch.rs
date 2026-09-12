// SPDX-License-Identifier: BUSL-1.1

//! Fill the resolvers' params from the wrapped point/bulk write op.
//!
//! Called from `DocumentOp::ResolveWrite`'s dispatch for the five ops whose
//! resolution is a mutation list rather than a classification: `PointUpdate`,
//! `PointDelete`, `Upsert`, `BulkUpdate`, `BulkDelete`.

use nodedb_physical::physical_plan::{DocumentOp, DocumentResolveOutcome};

use super::bulk::{ResolveBulkDelete, ResolveBulkUpdate};
use super::point::{ResolvePointDelete, ResolvePointUpdate};
use super::upsert::ResolveUpsert;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Resolve `inner` to the mutations it would apply, or `None` when it is
    /// not one of the five point/bulk write ops. `None` is not a refusal —
    /// the two join-driven ops resolve to a classification instead.
    pub(in crate::data::executor) fn resolve_document_point_write(
        &self,
        task: &ExecutionTask,
        tid: u64,
        inner: &DocumentOp,
    ) -> Option<Result<DocumentResolveOutcome, ErrorCode>> {
        Some(match inner {
            DocumentOp::PointUpdate {
                collection,
                document_id,
                surrogate,
                updates,
                returning,
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key,
                // Decode re-derives this from `document_id.as_bytes()`.
                pk_bytes: _,
            } => self.resolve_point_update(
                task,
                ResolvePointUpdate {
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    updates,
                    returning: returning.as_ref(),
                    rls_filters,
                    rls_write_check,
                    resolved_sum_targets,
                    declared_primary_key: declared_primary_key.as_deref(),
                },
            ),
            DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                returning,
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                pk_bytes: _,
            } => self.resolve_point_delete(
                task,
                ResolvePointDelete {
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    returning: returning.as_ref(),
                    rls_filters,
                    rls_write_check,
                    resolved_sum_targets,
                },
            ),
            DocumentOp::Upsert {
                collection,
                document_id,
                value,
                on_conflict_updates,
                surrogate,
                rls_write_check,
                returning,
                rls_filters,
                resolved_sum_targets,
            } => self.resolve_upsert(
                task,
                ResolveUpsert {
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    value,
                    on_conflict_updates,
                    returning: returning.as_ref(),
                    rls_filters,
                    rls_write_check,
                    resolved_sum_targets,
                },
            ),
            // OLLP prediction ignored: resolving uses a per-row content
            // precondition instead, proposed single-shard, not via Calvin.
            DocumentOp::BulkUpdate {
                collection,
                filters,
                updates,
                returning,
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key,
                ollp_predicted_surrogates: _,
                ollp_predicted_edges: _,
            } => self.resolve_bulk_update(
                task,
                ResolveBulkUpdate {
                    tid,
                    collection: collection.as_str(),
                    filter_bytes: filters,
                    updates,
                    returning: returning.as_ref(),
                    rls_filters,
                    rls_write_check,
                    resolved_sum_targets,
                    declared_primary_key: declared_primary_key.as_deref(),
                },
            ),
            DocumentOp::BulkDelete {
                collection,
                filters,
                returning,
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                ollp_predicted_surrogates: _,
                ollp_predicted_edges: _,
                // Read only by overlay staging; a resolved delete removes rows
                // by storage key.
                declared_primary_key: _,
            } => self.resolve_bulk_delete(
                task,
                ResolveBulkDelete {
                    tid,
                    collection: collection.as_str(),
                    filter_bytes: filters,
                    returning: returning.as_ref(),
                    rls_filters,
                    rls_write_check,
                    resolved_sum_targets,
                },
            ),
            DocumentOp::Merge { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::ResolveWrite(_)
            | DocumentOp::ResolvedWrite { .. }
            | DocumentOp::PointGet { .. }
            | DocumentOp::PointPut { .. }
            | DocumentOp::PointInsert { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::BatchInsert { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::Truncate { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::MaterializeScan { .. }
            | DocumentOp::ApplyBalanceDelta { .. } => return None,
        })
    }
}
