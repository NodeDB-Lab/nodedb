// SPDX-License-Identifier: BUSL-1.1

//! Shared GROUP BY key naming rule.
//!
//! A computed (non-column) GROUP BY key needs one canonical name shared by the
//! planner spec (the name the Data-Plane executor emits the evaluated value
//! under) and the response shaper (the `lookup_key` it reads that value back
//! by). Deriving both from this single helper guarantees they can never
//! diverge — a mismatch would surface the key value as a NULL cell.
//!
//! The name is index-based (`group_{index}`), not the SELECT alias: it is a
//! purely internal executor↔shaper handshake. The alias only ever reaches the
//! shaper's `display_name`, derived separately from `SqlPlan::Aggregate`'s
//! `group_by_aliases`.
//!
//! The rule itself lives in `nodedb_sql::planner::agg_naming`, because the
//! planner projects these names when it wraps an aggregate in a
//! Control-Plane-computed projection. Both sides call the same function.

use nodedb_sql::types_expr::SqlExpr;

/// The canonical internal name for a computed GROUP BY key: a stable
/// `group_{index}` placeholder. Used as both the executor output name and the
/// shaper lookup key for a non-column key.
pub(in crate::control::planner::sql_plan_convert) fn computed_group_key_name(
    index: usize,
) -> String {
    nodedb_sql::planner::agg_naming::computed_group_key_name(index)
}

/// The output/lookup name for one GROUP BY key. A bare column keeps its own
/// (unqualified) column name so bare-column output stays byte-identical to the
/// string-keyed form; a computed expression uses [`computed_group_key_name`].
pub(in crate::control::planner::sql_plan_convert) fn group_key_output_name(
    expr: &SqlExpr,
    index: usize,
) -> String {
    nodedb_sql::planner::agg_naming::group_key_row_name(expr, index)
}
