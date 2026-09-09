// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for the two join-driven document DML ops, `Merge` and
//! `UpdateFromJoin`.
//!
//! Each runs in one of two passes: the write pass (the bare op) and the
//! read-only RESOLVE pass (the same op wrapped in `DocumentOp::ResolveWrite`).
//! Both passes fill the SAME handler params, so the classifier they share
//! cannot diverge between them — only `resolve_only` differs.

use crate::bridge::envelope::{ErrorCode, Response};
use nodedb_physical::physical_plan::DocumentOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Run an `UpdateFromJoin` as the write pass or the RESOLVE pass.
    pub(super) fn dispatch_update_from_join(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &DocumentOp,
        resolve_only: bool,
    ) -> Response {
        let DocumentOp::UpdateFromJoin {
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
            rls_write_check,
            resolved_sum_targets,
            declared_primary_key,
        } = op
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "dispatch_update_from_join: plan is not UpdateFromJoin".into(),
                },
            );
        };
        self.execute_update_from_join(
            task,
            tid,
            super::super::handlers::update_from_join::UpdateFromJoinParams {
                target_collection: target_collection.as_str(),
                source_collection: source_collection.as_str(),
                source_alias,
                target_join_col,
                source_join_col,
                updates,
                target_filter_bytes: target_filters,
                returning: returning.as_ref(),
                resolve_only,
                source_rows: source_rows.as_deref(),
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key: declared_primary_key.as_deref(),
            },
        )
    }

    /// Run a `Merge` as the write pass or the RESOLVE pass.
    pub(super) fn dispatch_merge(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &DocumentOp,
        resolve_only: bool,
    ) -> Response {
        let DocumentOp::Merge {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
            clauses,
            returning,
            resolved_inserts,
            source_rows,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            declared_primary_key,
        } = op
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "dispatch_merge: plan is not Merge".into(),
                },
            );
        };
        self.execute_merge(
            task,
            tid,
            super::super::handlers::merge::MergeParams {
                target_collection: target_collection.as_str(),
                source_collection: source_collection.as_str(),
                source_alias,
                target_join_col,
                source_join_col,
                clauses,
                resolve_only,
                resolved_inserts: resolved_inserts.as_deref(),
                source_rows: source_rows.as_deref(),
                returning: returning.as_ref(),
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key: declared_primary_key.as_deref(),
            },
        )
    }

    /// Run the read-only RESOLVE pass over the op `ResolveWrite` wraps. The
    /// join-driven ops report a classification; the five point/bulk ops
    /// report a `DocumentResolveOutcome`. Anything else is a construction bug.
    pub(super) fn dispatch_document_resolve_write(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        inner: &DocumentOp,
    ) -> Response {
        match inner {
            DocumentOp::UpdateFromJoin { .. } => {
                self.dispatch_update_from_join(task, tid, inner, true)
            }
            DocumentOp::Merge { .. } => self.dispatch_merge(task, tid, inner, true),
            other => match self.resolve_document_point_write(task, tid, other) {
                Some(Ok(outcome)) => match response_codec::encode(&outcome) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, e),
                },
                Some(Err(code)) => self.response_error(task, code),
                None => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "ResolveWrite wraps an op with no resolve pass".into(),
                    },
                ),
            },
        }
    }
}
