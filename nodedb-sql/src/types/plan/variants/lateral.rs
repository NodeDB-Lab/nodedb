// SPDX-License-Identifier: Apache-2.0

//! LATERAL join plan payloads.

use crate::types::filter::Filter;
use crate::types::query::{Projection, SortKey};

use super::plan::SqlPlan;

/// Payload of [`SqlPlan::LateralTopK`](crate::types::SqlPlan::LateralTopK).
#[derive(Debug, Clone)]
pub struct LateralTopKPlan {
    /// Plan producing outer rows.
    pub outer: Box<SqlPlan>,
    /// Alias used to qualify outer table columns (e.g. `"u"`).
    pub outer_alias: Option<String>,
    /// Inner collection to scan.
    pub inner_collection: String,
    /// Pre-filter applied to inner rows (non-correlated filters).
    pub inner_filters: Vec<Filter>,
    /// Sort keys for the inner per-outer-row result.
    pub inner_order_by: Vec<SortKey>,
    /// Maximum number of inner rows per outer row.
    pub inner_limit: usize,
    /// Equi-join pairs `(outer_col, inner_col)`.
    pub correlation_keys: Vec<(String, String)>,
    /// Alias under which inner rows are presented.
    pub lateral_alias: String,
    /// Post-lateral projection.
    pub projection: Vec<Projection>,
    /// LEFT join semantics: preserve outer rows even when inner is empty.
    pub left_join: bool,
}

/// Payload of [`SqlPlan::LateralLoop`](crate::types::SqlPlan::LateralLoop).
#[derive(Debug, Clone)]
pub struct LateralLoopPlan {
    /// Plan producing outer rows.
    pub outer: Box<SqlPlan>,
    /// Alias used to qualify outer table columns.
    pub outer_alias: Option<String>,
    /// Inner subquery plan (correlation predicates are injected at runtime).
    pub inner: Box<SqlPlan>,
    /// Correlated predicates extracted from the inner WHERE that reference
    /// outer columns.  Each entry is `(inner_field, outer_field)`.
    pub correlation_predicates: Vec<(String, String)>,
    /// Alias under which inner rows are presented.
    pub lateral_alias: String,
    /// Post-lateral projection.
    pub projection: Vec<Projection>,
    /// Maximum outer rows allowed. Queries exceeding this return an error.
    pub outer_row_cap: usize,
    /// LEFT join semantics: preserve outer rows even when inner is empty.
    pub left_join: bool,
}
