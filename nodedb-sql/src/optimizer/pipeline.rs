// SPDX-License-Identifier: Apache-2.0

//! The optimization pass pipeline.

use crate::catalog::SqlCatalog;
use crate::types::SqlPlan;

use super::{point_get, predicate_pushdown};

/// Apply all optimization passes to a plan.
pub fn optimize(plan: SqlPlan, catalog: &dyn SqlCatalog) -> crate::Result<SqlPlan> {
    let plan = point_get::optimize(plan, catalog)?;
    Ok(predicate_pushdown::optimize(plan))
}
