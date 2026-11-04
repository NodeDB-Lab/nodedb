// SPDX-License-Identifier: BUSL-1.1

//! `LateralLoop` handler: general nested-loop LATERAL for non-equi correlations.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::JoinProjection;

use super::inner::{OuterRowJoin, encode_inner_filters};
use super::shared::{bind_outer_values, build_scan_plan, correlation_value, flatten_outer_row};
use crate::bridge::scan_filter::{FilterOp, ScanFilter, decode_scan_filters};

/// Parameters for `LateralLoop` execution.
pub struct LateralLoopParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub outer_plan: &'a PhysicalPlan,
    pub outer_alias: &'a str,
    pub inner_collection: &'a str,
    pub inner_filters: &'a [u8],
    pub correlation_predicates: &'a [(String, String)],
    pub lateral_alias: &'a str,
    pub projection: &'a [JoinProjection],
    pub left_join: bool,
    pub outer_row_cap: usize,
    /// The scope every inner scan reads at.
    pub inner_system_time: nodedb_types::SystemTimeScope,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_lateral_loop(
        &mut self,
        p: LateralLoopParams<'_>,
    ) -> Response {
        debug!(
            core = self.core_id,
            inner = %p.inner_collection,
            cap = p.outer_row_cap,
            "lateral loop"
        );

        let outer_resp = self.execute_plan(p.task, p.outer_plan);
        let outer_docs = match response_codec::decode_response_to_docs(&outer_resp) {
            Some(docs) => docs,
            None => return outer_resp,
        };

        if outer_docs.len() > p.outer_row_cap {
            return self.response_error(
                p.task,
                ErrorCode::Unsupported {
                    detail: format!(
                        "LATERAL query exceeded outer-row cap of {} rows; \
                         use a more selective outer filter",
                        p.outer_row_cap
                    ),
                },
            );
        }

        // A malformed filter payload is an error. Reading it as "no filter"
        // would join every inner row.
        let base_inner_filters = match decode_scan_filters(p.inner_filters, "LATERAL inner filter")
        {
            Ok(filters) => filters,
            Err(e) => return self.response_error(p.task, e),
        };

        let mut result: Vec<Vec<u8>> = Vec::new();

        for (_outer_id, outer_bytes) in &outer_docs {
            // Produce a flat outer map that includes wrapper-level fields (e.g.
            // the primary-key `id`) alongside the document payload fields.
            let flat_outer = flatten_outer_row(outer_bytes);

            // Bind any *Column filter ops (e.g. GtColumn for non-equi correlations)
            // that reference the outer row by substituting the actual runtime value.
            let mut corr_filters =
                match bind_outer_values(base_inner_filters.clone(), outer_bytes, p.outer_alias) {
                    Ok(filters) => filters,
                    Err(e) => return self.response_error(p.task, e),
                };

            let mut satisfiable = true;
            for (inner_col, outer_col) in p.correlation_predicates {
                match correlation_value(outer_bytes, outer_col) {
                    Ok(Some(value)) => corr_filters.push(ScanFilter {
                        field: inner_col.clone(),
                        op: FilterOp::Eq,
                        value,
                        clauses: Vec::new(),
                        expr: None,
                    }),
                    Ok(None) => {
                        satisfiable = false;
                        break;
                    }
                    Err(e) => return self.response_error(p.task, e),
                }
            }

            let inner_plan = if satisfiable {
                match encode_inner_filters(&corr_filters) {
                    Ok(filter_bytes) => match build_scan_plan(
                        p.task.request.database_id,
                        p.inner_collection,
                        filter_bytes,
                        &[],
                        usize::MAX,
                        p.inner_system_time,
                    ) {
                        Ok(plan) => Some(plan),
                        Err(code) => return self.response_error(p.task, code),
                    },
                    Err(code) => return self.response_error(p.task, code),
                }
            } else {
                None
            };

            let row = OuterRowJoin {
                flat_outer: &flat_outer,
                inner_plan,
                outer_alias: p.outer_alias,
                lateral_alias: p.lateral_alias,
                projection: p.projection,
                left_join: p.left_join,
            };
            if let Err(resp) = self.lateral_join_outer_row(p.task, row, &mut result) {
                return resp;
            }
        }

        let payload = response_codec::encode_binary_rows(&result);
        self.response_with_payload(p.task, payload)
    }
}
