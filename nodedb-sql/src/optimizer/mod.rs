pub mod constant_fold;
pub mod point_get;
pub mod predicate_pushdown;

use crate::types::SqlPlan;

/// Apply all optimization passes to a plan.
pub fn optimize(plan: SqlPlan) -> SqlPlan {
    let plan = point_get::optimize(plan);
    predicate_pushdown::optimize(plan)
}
