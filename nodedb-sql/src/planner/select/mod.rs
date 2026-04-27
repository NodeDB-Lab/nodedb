//! SELECT query planning: FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT.
//!
//! This is the main entry point for SELECT statement conversion. It detects
//! search patterns (vector, text, hybrid, spatial) directly from the AST
//! instead of reverse-engineering an optimizer's output.

mod entry;
mod entry_ann;
mod helpers;
mod select_stmt;
mod where_search;

#[cfg(test)]
mod tests;

pub use entry::plan_query;
pub use helpers::{
    convert_projection, convert_where_to_filters, extract_float, extract_func_args,
    extract_string_literal, qualified_name,
};
