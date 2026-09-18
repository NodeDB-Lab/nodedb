// SPDX-License-Identifier: BUSL-1.1

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_sql::types::SortKey;

use super::bridge_expr::sql_expr_to_bridge_expr;

/// Lower planner sort keys to their physical form.
///
/// Every key is carried, expression and all. Dropping a key the Data Plane
/// cannot name as a stored column would silently answer
/// `ORDER BY 100 / weight` with rows in storage order.
pub(in crate::control::planner::sql_plan_convert) fn convert_sort_keys(
    keys: &[SortKey],
) -> Vec<SortKeySpec> {
    keys.iter()
        .map(|k| SortKeySpec {
            expr: sql_expr_to_bridge_expr(&k.expr),
            ascending: k.ascending,
            nulls_first: k.nulls_first,
        })
        .collect()
}
