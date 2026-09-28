// SPDX-License-Identifier: Apache-2.0

//! Non-recursive CTE plan payload.

use super::plan::SqlPlan;

/// Payload of [`SqlPlan::Cte`](crate::types::SqlPlan::Cte).
#[derive(Debug, Clone)]
pub struct CtePlan {
    /// CTE definitions: `(name, subquery_plan)`.
    pub definitions: Vec<(String, SqlPlan)>,
    /// The outer query that references CTE names.
    pub outer: Box<SqlPlan>,
}
