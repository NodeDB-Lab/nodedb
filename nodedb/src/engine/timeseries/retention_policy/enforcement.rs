// SPDX-License-Identifier: BUSL-1.1

//! Retention policy enforcement background loop.
//!
//! Spawned on the Event Plane alongside the cron scheduler. On each evaluation
//! cycle, iterates all enabled retention policies and for each:
//!
//! 1. Drops expired raw partitions (only if the next tier's aggregate covers that
//!    time range).
//! 2. Drops expired aggregate buckets.
//! 3. Archives partitions to S3 for tiers with `ARCHIVE TO` (future: not yet wired).
//!
//! The metadata leader decides, on one clock read per pass. Each decision enters
//! every data group's log, and every replica and mirror drops at that position.
//!
//! Runs on the Event Plane (Send + Sync, Tokio). NEVER does storage I/O directly.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{info, warn};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::retention_log::{decides_retention, enact_retention};
use crate::control::state::SharedState;
use crate::control::wal_replication::types::RetentionOp;
use crate::engine::timeseries::retention_policy::RetentionPolicyRegistry;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::MetaOp;

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
    // Start with a short initial delay to let the system warm up. Shutdown
    // ends the delay: a server stopped within it must not wait it out.
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(10)) => {}
        _ = shutdown.wait_for(|stopping| *stopping) => {
            info!("retention enforcement loop shutting down");
            return;
        }
    }

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
        if policies.is_empty() || !decides_retention(&state) {
            continue;
        }

        let decided_ms = nodedb_types::ordinal_to_ms(nodedb_types::wall_now_ns_i64());
        let mut bucket_passes: BTreeSet<(u64, u64)> = BTreeSet::new();
        for policy in &policies {
            // A mirror decides nothing: its drops arrive through the log. The
            // policy runs from the database's PROMOTE.
            let database = DatabaseId::new(policy.database_id);
            if state.mirror.fence.is_read_only(database) {
                continue;
            }
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
                    let op = RetentionOp::TimeseriesPartitions {
                        collection: policy.collection.clone(),
                        max_age_ms: raw_tier.retain_ms as i64,
                        decided_ms,
                    };
                    if let Err(e) = enact_retention(&state, tenant_id, database, op).await {
                        warn!(
                            policy = policy.name,
                            collection = policy.collection,
                            error = %e,
                            "raw tier retention did not land; the next pass decides again"
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

            // Apply retention to the tenant's continuous aggregate buckets, once
            // per pass however many policies it holds.
            if bucket_passes.insert((policy.database_id, policy.tenant_id)) {
                let op = RetentionOp::ContinuousAggregates { decided_ms };
                if let Err(e) = enact_retention(&state, tenant_id, database, op).await {
                    warn!(
                        policy = policy.name,
                        error = %e,
                        "continuous aggregate retention did not land; the next pass decides again"
                    );
                }
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

    match crate::control::server::shared::ddl::sync_dispatch::dispatch_system(
        state,
        crate::control::server::shared::ddl::sync_dispatch::SystemTask::new(
            crate::control::server::shared::ddl::sync_dispatch::SystemReason::RetentionEnforcement,
            tenant_id,
            nodedb_types::CollectionKey::from_bare(
                DatabaseId::new(policy.database_id),
                &policy.collection,
            ),
            plan,
        ),
        Duration::from_secs(10),
    )
    .await
    {
        // The core answers with `encode_serde`, which is MessagePack.
        Ok(payload) => match crate::data::executor::response_codec::decode_payload::<
            crate::engine::timeseries::continuous_agg::WatermarkState,
        >(&payload)
        {
            // Safe if tier1 has aggregated data beyond the cutoff.
            Ok(wm) => wm.watermark_ts >= cutoff,
            Err(e) => {
                tracing::warn!(
                    policy = policy.name,
                    tier1 = tier1_name,
                    error = %e,
                    "tier1 watermark payload does not decode; raw partitions are kept"
                );
                crate::diag::continuous_aggregate_state_fault(
                    &e,
                    "watermark_decode",
                    policy.database_id,
                    tenant_id.as_u64(),
                    &tier1_name,
                );
                false
            }
        },
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
            database_id: 0,
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
