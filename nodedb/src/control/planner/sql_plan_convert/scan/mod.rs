// SPDX-License-Identifier: BUSL-1.1

//! Scan and search plan conversions (read-only query paths).
//!
//! Split by concern so each file stays under the project's hard size limit.

mod core;
mod helpers;
mod join;
mod join_cost;
mod recursive;
mod search;
mod spatial;
mod text_score_bound;
mod text_search;
mod timeseries;

pub(in crate::control::planner::sql_plan_convert) use core::{
    DocumentIndexLookupArgs, convert_document_index_lookup, convert_point_get, convert_scan,
};
pub(in crate::control::planner::sql_plan_convert) use join::convert_join;
pub(in crate::control::planner::sql_plan_convert) use recursive::{
    convert_recursive_scan, convert_recursive_value,
};
pub(in crate::control::planner::sql_plan_convert) use search::{
    convert_hybrid_search, convert_hybrid_search_triple, convert_sparse_search,
    convert_vector_search,
};
pub(in crate::control::planner::sql_plan_convert) use spatial::convert_spatial_scan;
pub(in crate::control::planner::sql_plan_convert) use text_score_bound::{
    ScoreScanTail, bound_score_scan,
};
pub(in crate::control::planner::sql_plan_convert) use text_search::convert_text_search;
pub(in crate::control::planner::sql_plan_convert) use timeseries::{
    convert_timeseries_ingest, convert_timeseries_scan,
};
