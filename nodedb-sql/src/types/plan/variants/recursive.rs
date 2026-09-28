// SPDX-License-Identifier: Apache-2.0

//! Recursive CTE plan payloads.

use crate::types::filter::Filter;
use crate::types::query::Projection;

/// Payload of [`SqlPlan::RecursiveScan`](crate::types::SqlPlan::RecursiveScan).
#[derive(Debug, Clone)]
pub struct RecursiveScanPlan {
    pub collection: String,
    pub base_filters: Vec<Filter>,
    pub recursive_filters: Vec<Filter>,
    /// Equi-join link for tree-traversal recursion:
    /// `(collection_field, working_table_field)`.
    /// e.g. `("parent_id", "id")` means each iteration finds rows
    /// where `collection.parent_id` matches a `working_table.id`.
    pub join_link: Option<(String, String)>,
    pub max_iterations: usize,
    pub distinct: bool,
    pub limit: usize,
    /// Resolved SELECT target list, for output-schema derivation.
    pub projection: Vec<Projection>,
}

/// Payload of [`SqlPlan::RecursiveValue`](crate::types::SqlPlan::RecursiveValue).
#[derive(Debug, Clone)]
pub struct RecursiveValuePlan {
    /// CTE name (used in error messages).
    pub cte_name: String,
    /// Column names declared on the CTE (e.g. `(n)` in `c(n) AS ...`).
    pub columns: Vec<String>,
    /// Anchor SELECT expressions as raw SQL text (one per column).
    pub init_exprs: Vec<String>,
    /// Recursive step SELECT expressions as raw SQL text (one per column).
    /// May reference column names from `columns`.
    pub step_exprs: Vec<String>,
    /// Optional WHERE condition as raw SQL text applied to each new row
    /// to decide whether to continue.  `None` → run until fixed point.
    pub condition: Option<String>,
    /// Maximum iterations before a `RecursionDepthExceeded` error is raised.
    pub max_depth: usize,
    /// `false` → UNION ALL (keep duplicates); `true` → UNION (deduplicate).
    pub distinct: bool,
}
