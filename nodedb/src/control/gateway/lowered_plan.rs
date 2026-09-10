// SPDX-License-Identifier: BUSL-1.1

//! The lowered plan the gateway executes, paired with its plan-cache verdict.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_sql::types::PlanCacheEligibility;

/// A `SqlPlan` lowered to a `PhysicalPlan`, carrying the verdict that decides
/// admission to the gateway plan cache.
///
/// The verdict rides beside the plan instead of inside it. `PhysicalPlan` is
/// the Data Plane wire enum; cacheability is a Control Plane planning property
/// the Data Plane never reads. A field on the enum would widen the wire shape
/// and every match on it for no execution benefit.
#[derive(Debug, Clone)]
pub struct LoweredPlan {
    /// The physical operation to execute.
    pub plan: PhysicalPlan,
    /// Whether the gateway can admit this plan to its cache.
    pub cache_eligibility: PlanCacheEligibility,
}

impl LoweredPlan {
    /// Pair a lowered plan with the verdict its `SqlPlan` batch produced.
    ///
    /// Pass the value returned by
    /// `crate::control::planner::sql_plan_convert::batch_cache_eligibility`.
    /// Never derive a second volatility verdict here.
    pub fn new(plan: PhysicalPlan, cache_eligibility: PlanCacheEligibility) -> Self {
        Self {
            plan,
            cache_eligibility,
        }
    }

    /// Pair a plan built directly, with no `SqlPlan` behind it.
    ///
    /// A directly constructed operation carries no expression tree, so it
    /// carries no volatile call.
    pub fn cacheable(plan: PhysicalPlan) -> Self {
        Self::new(plan, PlanCacheEligibility::Cacheable)
    }

    /// Whether the gateway can admit this plan to its cache.
    pub fn is_cacheable(&self) -> bool {
        self.cache_eligibility.is_cacheable()
    }
}
