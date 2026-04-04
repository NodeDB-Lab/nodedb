//! Auto-wiring: create/remove continuous aggregates from retention policy tiers.
//!
//! When a retention policy is created, this module builds a `ContinuousAggregateDef`
//! for each downsample tier and dispatches it to the Data Plane via `MetaOp`.
//! Tiers are chained: tier N aggregates from tier N-1 (cascading).

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::MetaOp;
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::{ContinuousAggregateDef, RefreshPolicy};
use crate::engine::timeseries::retention_policy::types::RetentionPolicyDef;
use crate::types::TenantId;

/// Register continuous aggregates for all downsample tiers in a retention policy.
///
/// For each non-RAW tier, creates a `ContinuousAggregateDef` with:
/// - Name: `_policy_{policy_name}_tier{N}`
/// - Source: collection (tier 1) or previous tier's aggregate (tier 2+)
/// - Refresh: OnFlush for tier 1, OnSeal for higher tiers
///
/// Dispatches each to the Data Plane via `MetaOp::RegisterContinuousAggregate`.
pub async fn register_tiers(
    state: &SharedState,
    def: &RetentionPolicyDef,
) -> Result<(), crate::Error> {
    let tenant_id = TenantId::new(def.tenant_id);

    for tier in def.downsample_tiers() {
        let agg_name = def.aggregate_name(tier.tier_index);

        // Source: for tier 1, aggregate from the raw collection.
        // For tier 2+, cascade from the previous tier's aggregate.
        let source = if tier.tier_index == 1 {
            def.collection.clone()
        } else {
            def.aggregate_name(tier.tier_index - 1)
        };

        // Refresh policy: OnFlush for tier 1 (lowest latency),
        // OnSeal for higher tiers (lower CPU cost — cascaded data is less urgent).
        let refresh_policy = if tier.tier_index == 1 {
            RefreshPolicy::OnFlush
        } else {
            RefreshPolicy::OnSeal
        };

        let interval_str = format_interval_ms(tier.resolution_ms);

        let agg_def = ContinuousAggregateDef {
            name: agg_name.clone(),
            source: source.clone(),
            bucket_interval: interval_str,
            bucket_interval_ms: tier.resolution_ms as i64,
            group_by: Vec::new(),
            aggregates: tier.aggregates.clone(),
            refresh_policy,
            retention_period_ms: tier.retain_ms,
            stale: false,
        };

        let plan = PhysicalPlan::Meta(MetaOp::RegisterContinuousAggregate {
            def: agg_def.clone(),
        });

        crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
            state,
            tenant_id,
            &source,
            plan,
            Duration::from_secs(5),
        )
        .await?;

        tracing::info!(
            aggregate = agg_name,
            source,
            tier = tier.tier_index,
            resolution_ms = tier.resolution_ms,
            "auto-wired continuous aggregate for retention policy"
        );
    }

    Ok(())
}

/// Unregister all continuous aggregates created by a retention policy.
///
/// Called on `DROP RETENTION POLICY` to clean up auto-created aggregates.
pub async fn unregister_tiers(
    state: &SharedState,
    def: &RetentionPolicyDef,
) -> Result<(), crate::Error> {
    let tenant_id = TenantId::new(def.tenant_id);

    // Unregister in reverse order (highest tier first) to avoid
    // cascading source-not-found issues.
    for tier in def.downsample_tiers().iter().rev() {
        let agg_name = def.aggregate_name(tier.tier_index);

        let plan = PhysicalPlan::Meta(MetaOp::UnregisterContinuousAggregate {
            name: agg_name.clone(),
        });

        // All aggregates routed via the base collection for vShard affinity.
        let route_collection = &def.collection;

        crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
            state,
            tenant_id,
            route_collection,
            plan,
            Duration::from_secs(5),
        )
        .await?;

        tracing::info!(
            aggregate = agg_name,
            tier = tier.tier_index,
            "removed continuous aggregate for retention policy"
        );
    }

    Ok(())
}

/// Format milliseconds as a compact interval string for ContinuousAggregateDef.
fn format_interval_ms(ms: u64) -> String {
    const MINUTE: u64 = 60_000;
    const HOUR: u64 = 3_600_000;
    const DAY: u64 = 86_400_000;

    if ms.is_multiple_of(DAY) {
        format!("{}d", ms / DAY)
    } else if ms.is_multiple_of(HOUR) {
        format!("{}h", ms / HOUR)
    } else if ms.is_multiple_of(MINUTE) {
        format!("{}m", ms / MINUTE)
    } else if ms.is_multiple_of(1_000) {
        format!("{}s", ms / 1_000)
    } else {
        format!("{ms}ms")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_interval() {
        assert_eq!(format_interval_ms(60_000), "1m");
        assert_eq!(format_interval_ms(3_600_000), "1h");
        assert_eq!(format_interval_ms(86_400_000), "1d");
        assert_eq!(format_interval_ms(30_000), "30s");
        assert_eq!(format_interval_ms(1_500), "1500ms");
    }
}
