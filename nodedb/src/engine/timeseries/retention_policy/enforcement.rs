//! Retention policy enforcement background loop.
//!
//! Spawned on the Event Plane alongside the cron scheduler. On each evaluation
//! cycle, iterates all enabled retention policies and for each:
//!
//! 1. Dispatches `EnforceTimeseriesRetention` to drop expired raw partitions
//!    (only if the next tier's aggregate covers that time range).
//! 2. Dispatches `ApplyContinuousAggRetention` to drop expired aggregate buckets.
//! 3. Archives partitions to S3 for tiers with `ARCHIVE TO` (future: not yet wired).
//!
//! Runs on the Event Plane (Send + Sync, Tokio). NEVER does storage I/O directly —
//! all enforcement is dispatched to the Data Plane via MetaOp.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{info, warn};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::MetaOp;
use crate::control::state::SharedState;
use crate::engine::timeseries::retention_policy::RetentionPolicyRegistry;
use crate::types::TenantId;

/// Spawn the retention policy enforcement loop as a background Tokio task.
///
/// Returns a `JoinHandle` that can be used for shutdown coordination.
pub fn spawn_enforcement_loop(
    shared_state: Arc<SharedState>,
    registry: Arc<RetentionPolicyRegistry>,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        enforcement_loop(shared_state, registry, shutdown).await;
    })
}

async fn enforcement_loop(
    state: Arc<SharedState>,
    registry: Arc<RetentionPolicyRegistry>,
    mut shutdown: watch::Receiver<bool>,
) {
    // Start with a short initial delay to let the system warm up.
    tokio::time::sleep(Duration::from_secs(10)).await;

    loop {
        // Find the shortest eval interval among all enabled policies.
        let sleep_ms = next_sleep_ms(&registry);

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("retention enforcement loop shutting down");
                    return;
                }
            }
        }

        let policies = registry.list_all_enabled();
        if policies.is_empty() {
            continue;
        }

        for policy in &policies {
            let tenant_id = TenantId::new(policy.tenant_id);

            // Enforce raw tier retention — only if downsample tier covers it.
            if let Some(raw_tier) = policy.raw_tier()
                && raw_tier.retain_ms > 0
            {
                // Safety check: if there are downsample tiers, verify tier1's
                // watermark covers the data we're about to drop.
                let safe_to_drop = if !policy.downsample_tiers().is_empty() {
                    check_watermark_coverage(&state, tenant_id, policy).await
                } else {
                    true // No downsample tiers — safe to drop raw unconditionally.
                };

                if safe_to_drop {
                    let plan = PhysicalPlan::Meta(MetaOp::EnforceTimeseriesRetention {
                        collection: policy.collection.clone(),
                        max_age_ms: raw_tier.retain_ms as i64,
                    });

                    if let Err(e) =
                        crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
                            &state,
                            tenant_id,
                            &policy.collection,
                            plan,
                            Duration::from_secs(30),
                        )
                        .await
                    {
                        warn!(
                            policy = policy.name,
                            collection = policy.collection,
                            error = %e,
                            "failed to enforce raw tier retention"
                        );
                    }
                } else {
                    warn!(
                        policy = policy.name,
                        collection = policy.collection,
                        "skipping raw retention: tier1 watermark does not cover cutoff"
                    );
                }
            }

            // Apply retention to continuous aggregate buckets.
            let plan = PhysicalPlan::Meta(MetaOp::ApplyContinuousAggRetention);
            if let Err(e) = crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
                &state,
                tenant_id,
                &policy.collection,
                plan,
                Duration::from_secs(30),
            )
            .await
            {
                warn!(
                    policy = policy.name,
                    error = %e,
                    "failed to apply continuous aggregate retention"
                );
            }
        }
    }
}

/// Determine the sleep duration based on the shortest eval interval
/// among all enabled policies. Falls back to 1 hour if no policies exist.
fn next_sleep_ms(registry: &RetentionPolicyRegistry) -> u64 {
    let policies = registry.list_all_enabled();
    policies
        .iter()
        .map(|p| p.eval_interval_ms)
        .filter(|&ms| ms > 0)
        .min()
        .unwrap_or(3_600_000) // Default: 1 hour
}

/// Check whether the first downsample tier's watermark covers the raw
/// retention cutoff. Raw data is only safe to drop if the aggregate has
/// already processed (aggregated) all data up to that cutoff.
async fn check_watermark_coverage(
    state: &std::sync::Arc<SharedState>,
    tenant_id: TenantId,
    policy: &crate::engine::timeseries::retention_policy::RetentionPolicyDef,
) -> bool {
    let raw_retain_ms = policy.raw_tier().map(|t| t.retain_ms).unwrap_or(0);
    if raw_retain_ms == 0 {
        return false; // Forever retention — never drop.
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let cutoff = now_ms - raw_retain_ms as i64;

    // Query tier1's watermark via MetaOp.
    let tier1_name = policy.aggregate_name(1);
    let plan = PhysicalPlan::Meta(MetaOp::QueryAggregateWatermark {
        aggregate_name: tier1_name.clone(),
    });

    match crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        &policy.collection,
        plan,
        Duration::from_secs(10),
    )
    .await
    {
        Ok(payload) => {
            if let Ok(wm) = sonic_rs::from_slice::<
                crate::engine::timeseries::continuous_agg::WatermarkState,
            >(&payload)
            {
                // Safe if tier1 has aggregated data beyond the cutoff.
                wm.watermark_ts >= cutoff
            } else {
                false // Can't parse watermark — not safe.
            }
        }
        Err(e) => {
            tracing::warn!(
                policy = policy.name,
                tier1 = tier1_name,
                error = %e,
                "failed to query tier1 watermark for safety check"
            );
            false // Query failed — not safe to drop.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::retention_policy::types::{RetentionPolicyDef, TierDef};

    fn make_policy(eval_ms: u64) -> RetentionPolicyDef {
        RetentionPolicyDef {
            tenant_id: 1,
            name: "test".into(),
            collection: "metrics".into(),
            tiers: vec![TierDef {
                tier_index: 0,
                resolution_ms: 0,
                aggregates: Vec::new(),
                retain_ms: 604_800_000,
                archive: None,
            }],
            auto_tier: false,
            enabled: true,
            eval_interval_ms: eval_ms,
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn next_sleep_uses_shortest_interval() {
        let reg = RetentionPolicyRegistry::new();
        reg.register(make_policy(3_600_000)); // 1h
        reg.register({
            let mut p = make_policy(1_800_000); // 30m
            p.name = "fast".into();
            p.collection = "fast_metrics".into();
            p
        });
        assert_eq!(next_sleep_ms(&reg), 1_800_000);
    }

    #[test]
    fn next_sleep_defaults_to_1h() {
        let reg = RetentionPolicyRegistry::new();
        assert_eq!(next_sleep_ms(&reg), 3_600_000);
    }
}
