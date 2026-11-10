// SPDX-License-Identifier: BUSL-1.1

//! The per-outer-row step both LATERAL handlers share: run the correlated
//! inner scan and append the joined rows.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::JoinProjection;

use super::shared::{MAX_RESULT_ROWS, build_row, inner_rows};

/// One outer row of a LATERAL join and the inner scan it correlates with.
pub(super) struct OuterRowJoin<'a> {
    /// The outer row's user-visible fields as a flat map.
    pub flat_outer: &'a [u8],
    /// The correlated inner scan. `None` when a correlation value is NULL or
    /// missing, so no inner row can match.
    pub inner_plan: Option<PhysicalPlan>,
    pub outer_alias: &'a str,
    pub lateral_alias: &'a str,
    pub projection: &'a [JoinProjection],
    pub left_join: bool,
}

/// Encode the inner scan's filter set.
pub(super) fn encode_inner_filters(filters: &[ScanFilter]) -> Result<Vec<u8>, ErrorCode> {
    zerompk::to_msgpack_vec(&filters).map_err(|e| ErrorCode::Internal {
        detail: format!("LATERAL inner filter serialization: {e}"),
    })
}

impl CoreLoop {
    /// Run `row`'s inner scan and append its joined rows to `result`. A LEFT
    /// join with no inner match appends the outer row once with no inner
    /// fields.
    ///
    /// Each of these ends the join with an error response:
    /// - the inner scan responds with an error;
    /// - the inner payload does not decode;
    /// - `result` would pass `MAX_RESULT_ROWS`.
    pub(super) fn lateral_join_outer_row(
        &mut self,
        task: &ExecutionTask,
        row: OuterRowJoin<'_>,
        result: &mut Vec<Vec<u8>>,
    ) -> Result<(), Response> {
        let inner_docs = match &row.inner_plan {
            None => Vec::new(),
            Some(plan) => {
                let resp = self.execute_plan(task, plan);
                if matches!(resp.status, Status::Error) {
                    return Err(resp);
                }
                inner_rows(resp.payload.as_ref(), plan)
                    .map_err(|error| self.response_error(task, ErrorCode::from(error)))?
            }
        };

        let mut push = |joined: Vec<u8>| -> Result<(), Response> {
            if result.len() >= MAX_RESULT_ROWS {
                return Err(self.response_error(
                    task,
                    ErrorCode::Unsupported {
                        detail: format!(
                            "LATERAL query exceeded the result cap of {MAX_RESULT_ROWS} rows; \
                             add a LIMIT or a more selective filter"
                        ),
                    },
                ));
            }
            result.push(joined);
            Ok(())
        };

        if inner_docs.is_empty() {
            if row.left_join {
                push(
                    build_row(
                        row.flat_outer,
                        None,
                        row.outer_alias,
                        row.lateral_alias,
                        row.projection,
                    )
                    .map_err(|error| self.response_error(task, ErrorCode::from(error)))?,
                )?;
            }
            return Ok(());
        }
        for inner_body in &inner_docs {
            push(
                build_row(
                    row.flat_outer,
                    Some(inner_body.as_slice()),
                    row.outer_alias,
                    row.lateral_alias,
                    row.projection,
                )
                .map_err(|error| self.response_error(task, ErrorCode::from(error)))?,
            )?;
        }
        Ok(())
    }
}
