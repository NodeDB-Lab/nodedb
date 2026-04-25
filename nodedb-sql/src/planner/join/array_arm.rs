//! NDARRAY_* table-valued function as a JOIN source.
//!
//! When a JOIN's left or right relation is `NDARRAY_SLICE(...)` (or
//! sibling read TVFs), the planner lowers it to the corresponding
//! `SqlPlan::NdArray*` variant directly — bypassing catalog lookup.
//! The TVF's output rows participate in the join key resolution like
//! any other relation; the dispatch handler emits `(coords, attrs)`
//! tuples that include the array's surrogate-equivalent identity
//! columns so equi-join keys work naturally.

use sqlparser::ast;

use crate::error::Result;
use crate::types::{SqlCatalog, SqlPlan};

/// If `rel` is a `NDARRAY_*(...)` TVF call, return the corresponding
/// `SqlPlan::NdArray*` node. Otherwise return `Ok(None)` so the caller
/// falls through to the named-table scan path.
pub(super) fn try_plan_relation(
    rel: &ast::TableFactor,
    catalog: &dyn SqlCatalog,
) -> Result<Option<SqlPlan>> {
    let name = match rel {
        ast::TableFactor::Table {
            name,
            args: Some(_),
            ..
        } => name,
        _ => return Ok(None),
    };
    let fn_name = crate::parser::normalize::normalize_object_name(name);
    if !is_array_tvf(&fn_name) {
        return Ok(None);
    }

    // Reuse the SELECT * FROM NDARRAY_*(...) planner by wrapping the
    // single relation into a one-element TableWithJoins. Both call
    // sites need exactly the same validation (arg arity, schema
    // resolution, dim/attr name checks) — re-deriving it here would
    // duplicate that logic and let the two paths drift.
    let twj = ast::TableWithJoins {
        relation: rel.clone(),
        joins: Vec::new(),
    };
    super::super::array_fn::try_plan_array_table_fn(std::slice::from_ref(&twj), catalog)
}

fn is_array_tvf(name: &str) -> bool {
    matches!(
        name,
        "ndarray_slice" | "ndarray_project" | "ndarray_agg" | "ndarray_elementwise"
    )
}
