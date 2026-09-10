// SPDX-License-Identifier: BUSL-1.1

//! Plan-cache verdict for a batch of logical plans.

use nodedb_sql::types::{PlanCacheEligibility, SqlPlan};

/// Fold one statement batch into a single plan-cache verdict.
///
/// The batch is cacheable only when every plan in it is cacheable.
/// `SqlPlan::cache_eligibility` is the one source of truth for volatile calls
/// and for row identity bound while lowering. Never derive a second verdict.
pub fn batch_cache_eligibility(plans: &[SqlPlan]) -> PlanCacheEligibility {
    if plans
        .iter()
        .all(|plan| plan.cache_eligibility().is_cacheable())
    {
        PlanCacheEligibility::Cacheable
    } else {
        PlanCacheEligibility::DataDependent
    }
}
