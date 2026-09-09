// SPDX-License-Identifier: Apache-2.0

//! DML planning helpers, split by concern:
//! - [`value_convert`] — `ast::Expr` -> `SqlValue` conversion
//! - [`range_check`] — declared-width coercion + range validation
//! - [`insert_columns`] — positional-insert column resolution
//! - [`ast_extract`] — table-name / primary-key point-lookup extraction
//! - [`vector_primary_insert`] — vector-primary collection insert plans
//! - [`kv_insert`] — KV engine insert plans
//! - [`insert_select_bind`] — `INSERT ... SELECT` target-column binding
//! - [`params`] — parameter structs for the helpers above

mod ast_extract;
mod insert_columns;
mod insert_select_bind;
mod kv_insert;
mod params;
mod range_check;
mod value_convert;
mod vector_primary_insert;

pub use ast_extract::extract_point_keys;
pub(super) use ast_extract::extract_table_name_from_table_with_joins;
pub(super) use insert_columns::resolve_insert_columns;
pub(super) use insert_select_bind::bind_insert_select_columns;
pub(super) use kv_insert::build_kv_insert_plan;
pub(super) use params::KvInsertParams;
pub(super) use range_check::{
    check_declared_float_ranges_in_assignments, check_declared_int_ranges_in_assignments,
    coerce_and_check_rows,
};
pub(super) use value_convert::convert_value_rows;
pub(super) use vector_primary_insert::build_vector_primary_insert_plan;
