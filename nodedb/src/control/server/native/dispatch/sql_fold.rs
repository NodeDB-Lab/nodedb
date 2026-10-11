// SPDX-License-Identifier: BUSL-1.1

//! Rendering one native SQL statement's answer: its columns and rows, its
//! warnings, and its one command tag, or the error frame it stopped with.

use nodedb_types::protocol::NativeResponse;

use crate::control::server::native::sqlstate_code::sqlstate_error;
use crate::control::server::response_shape::types::FoldedTag;
use crate::control::server::shared::statement_exec::{StatementAnswer, StatementError};

use super::{
    apply_dml_outcome, dml_fold_error_to_native, error_code_to_native, error_response_to_native,
    error_to_native, error_to_native_with_sqlstate, shape_error_to_native, to_native_columns_rows,
};

/// The native frame for a statement's answer or error.
pub(super) fn statement_result_to_native(
    seq: u64,
    result: Result<StatementAnswer, StatementError>,
) -> NativeResponse {
    match result {
        Ok(answer) => answer_to_native(seq, answer),
        Err(error) => statement_error_to_native(seq, error),
    }
}

/// The native frame for a statement's answer. The columns are the first
/// non-empty set a task's rows named.
pub(super) fn answer_to_native(seq: u64, answer: StatementAnswer) -> NativeResponse {
    let mut r = NativeResponse::ok(seq);
    r.watermark_lsn = answer.last_lsn;
    r.warnings = answer.warnings;
    let mut columns = None;
    let mut rows = Vec::new();
    for shaped in &answer.rows {
        let (task_columns, task_rows) = to_native_columns_rows(shaped);
        if !task_columns.is_empty() && columns.is_none() {
            columns = Some(task_columns);
        }
        rows.extend(task_rows);
    }
    if !rows.is_empty() {
        r.columns = columns;
        r.rows = Some(rows);
    }
    match answer.tag {
        Some(FoldedTag::Dml(outcome)) => apply_dml_outcome(&mut r, outcome),
        Some(FoldedTag::Opaque) | None => {}
    }
    r
}

/// The native error frame for why a statement stopped.
pub(super) fn statement_error_to_native(seq: u64, error: StatementError) -> NativeResponse {
    match error {
        StatementError::Error(error) => error_to_native(seq, &error),
        StatementError::Quota(error) => error_to_native_with_sqlstate(seq, "53400", &error),
        StatementError::TenantIsolation => {
            sqlstate_error(seq, "42501", "tenant isolation violation")
        }
        StatementError::Rejected(code) => error_code_to_native(seq, code.as_ref()),
        StatementError::Response(response) => error_response_to_native(seq, &response),
        StatementError::DmlFold(error) => dml_fold_error_to_native(seq, &error),
        StatementError::Shape(error) => shape_error_to_native(seq, &error),
    }
}
