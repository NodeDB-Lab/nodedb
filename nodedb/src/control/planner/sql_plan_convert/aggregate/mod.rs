// SPDX-License-Identifier: BUSL-1.1

//! Aggregate plan conversion and projection/window helpers.
//!
//! Split by concern so each file stays under the project's hard size limit:
//! `plan` (the `convert_aggregate` entry point and its join / catalog /
//! timeseries lowering), `spec` (aggregate-spec + collection/alias helpers and
//! join-side embedding), and `projection` (projection / computed-column /
//! window-function serialization).

mod cost;
mod plan;
mod projection;
mod spec;

pub(in crate::control::planner::sql_plan_convert) use plan::{
    ConvertAggregateParams, convert_aggregate,
};
pub(in crate::control::planner::sql_plan_convert) use projection::{
    extract_computed_columns, extract_join_projection_specs, extract_projection_names,
    serialize_join_computed_projection, serialize_window_functions,
};
pub(in crate::control::planner::sql_plan_convert) use spec::{
    agg_expr_to_pair, extract_collection_name, extract_scan_alias, inline_join_side,
    join_side_collection,
};
