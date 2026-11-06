// SPDX-License-Identifier: Apache-2.0

//! SELECT query planning: FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT.
//!
//! This is the main entry point for SELECT statement conversion. It detects
//! search patterns (vector, text, hybrid, spatial) directly from the AST
//! instead of reverse-engineering an optimizer's output.

mod array_cell_geom;
mod array_from;
mod comma_lateral;
mod cte_catalog;
mod derived_from;
mod derived_output;
mod entry;
mod entry_ann;
pub mod helpers;
mod limit;
mod order_by;
mod post_process;
mod query_tail;
mod relation_leaf;
mod select_stmt;
mod text_call;
mod text_options;
mod where_search;

pub(crate) use cte_catalog::CteCatalog;
pub(crate) use derived_output::rekey_join_output;
pub use entry::{plan_query, plan_statement_query};
pub use helpers::{
    convert_projection, convert_where_to_filters, extract_float, extract_func_args,
    extract_string_literal, qualified_name,
};
pub(crate) use order_by::select_output_aliases;
pub(crate) use select_stmt::has_aggregation;
