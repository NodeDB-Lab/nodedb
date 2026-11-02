// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for the two join-driven document DML ops, `Merge` and
//! `UpdateFromJoin`.
//!
//! Each runs in one of two passes: the write pass (the bare op) and the
//! read-only RESOLVE pass (the same op wrapped in `DocumentOp::ResolveWrite`).
//! Both passes fill the SAME handler params, so the classifier they share
//! cannot diverge between them — only `resolve_only` differs.

use crate::bridge::envelope::{ErrorCode, Response};
use nodedb_physical::physical_plan::{DocumentOp, RowSource};

use nodedb_types::SystemTimeScope;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::write_match_cut::{autocommit_match_cut, write_match_cut};
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
            source,
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
            target_system_time,
        } = op
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "dispatch_update_from_join: plan is not UpdateFromJoin".into(),
                },
            );
        };
        let Some(source_rows) = source_rows.as_deref() else {
            return self.response_error(task, unresolved_source("UPDATE ... FROM", source));
        };
        let target_cut = match join_target_cut(target_system_time, resolve_only, "UPDATE ... FROM")
        {
            Ok(cut) => cut,
            Err(code) => return self.response_error(task, code),
        };
        self.execute_update_from_join(
            task,
            tid,
            super::super::handlers::update_from_join::UpdateFromJoinParams {
                target_collection: target_collection.as_str(),
                source_label: source.label(),
                source_alias,
                target_join_col,
                source_join_col,
                updates,
                target_filter_bytes: target_filters,
                returning: returning.as_ref(),
                resolve_only,
                source_rows,
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key: declared_primary_key.as_deref(),
                target_cut,
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
            source,
            source_alias,
            target_join_col,
            source_join_col,
            clauses,
            returning,
            resolved_inserts,
            // Bound on the Control Plane before dispatch; the handler keys
            // inserts by `resolved_inserts` alone.
            resolved_insert_identities: _,
            source_rows,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            declared_primary_key,
            target_system_time,
        } = op
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "dispatch_merge: plan is not Merge".into(),
                },
            );
        };
        let Some(source_rows) = source_rows.as_deref() else {
            return self.response_error(task, unresolved_source("MERGE", source));
        };
        let target_cut = match join_target_cut(target_system_time, resolve_only, "MERGE") {
            Ok(cut) => cut,
            Err(code) => return self.response_error(task, code),
        };
        self.execute_merge(
            task,
            tid,
            super::super::handlers::merge::MergeParams {
                target_collection: target_collection.as_str(),
                source_label: source.label(),
                source_alias,
                target_join_col,
                source_join_col,
                clauses,
                resolve_only,
                resolved_inserts: resolved_inserts.as_deref(),
                source_rows,
                returning: returning.as_ref(),
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key: declared_primary_key.as_deref(),
                target_cut,
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

/// The ordinal a join write matches its target rows at. The RESOLVE pass of
/// a transaction block reads at the block's snapshot. The write pass applies
/// an autocommit statement, so it matches the current rows only.
fn join_target_cut(
    target_system_time: &SystemTimeScope,
    resolve_only: bool,
    statement: &str,
) -> Result<Option<i64>, ErrorCode> {
    if resolve_only {
        write_match_cut(target_system_time, statement)
    } else {
        autocommit_match_cut(target_system_time, statement).map(|()| None)
    }
}

/// The error for a join write that reached the Data Plane without its source
/// rows. The Control Plane reads the source and ships it on every entry
/// point; the Data Plane holds no read path for a source relation.
fn unresolved_source(statement: &str, source: &RowSource) -> ErrorCode {
    ErrorCode::Internal {
        detail: format!(
            "{statement}: source '{}' reached the Data Plane without its rows; the Control \
             Plane must read the source and ship it",
            source.label()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_resolve_pass_matches_the_target_at_the_block_snapshot() {
        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: 42,
            cut_marker: 1,
        };
        assert_eq!(join_target_cut(&snapshot, true, "MERGE"), Ok(Some(42)));
        assert_eq!(
            join_target_cut(&SystemTimeScope::Current, true, "MERGE"),
            Ok(None)
        );
    }

    #[test]
    fn a_write_pass_matches_the_current_target_only() {
        assert_eq!(
            join_target_cut(&SystemTimeScope::Current, false, "UPDATE ... FROM"),
            Ok(None)
        );
        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: 42,
            cut_marker: 1,
        };
        assert!(join_target_cut(&snapshot, false, "UPDATE ... FROM").is_err());
        assert!(join_target_cut(&SystemTimeScope::AsOf(7), true, "MERGE").is_err());
    }
}
