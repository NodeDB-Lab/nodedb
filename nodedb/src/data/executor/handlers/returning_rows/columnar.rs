// SPDX-License-Identifier: BUSL-1.1

//! `RETURNING` for columnar writes.

use super::project::build_rows_payload;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_write;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_types::Value;

impl CoreLoop {
    /// Build a columnar write's `RETURNING` response from the rows it stored.
    /// A columnar row is `Vec<Value>` in schema order, assembled by
    /// `row_values_to_object` — the same builder the WHERE evaluator and RLS
    /// write gate use, so output can't drift from what `SELECT` reports.
    pub(in crate::data::executor) fn columnar_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        schema: &nodedb_types::columnar::ColumnarSchema,
        rows: &[Vec<nodedb_types::Value>],
    ) -> Response {
        let docs: Vec<Value> = rows
            .iter()
            .map(|row| columnar_write::row_values_to_user_object(schema, row))
            .collect();
        match build_rows_payload(spec, rls_filters, &docs, &self.crs_context()) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}
