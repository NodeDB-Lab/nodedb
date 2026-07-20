// SPDX-License-Identifier: BUSL-1.1

//! Collection-GC sweeper loop.
//!
//! One long-lived Tokio task per node. On each eval tick (default 60s)
//! it loads every soft-deleted collection from the local catalog and,
//! for those past their retention window, proposes
//! `CatalogEntry::PurgeCollection` through the normal metadata-raft
//! path. The post_apply async dispatch does the rest (per-node
//! tombstone persist + WAL append + quiesce + Data Plane reclaim).
//!
//! Runs everywhere — proposing is idempotent (the leader accepts,
//! followers' proposals forward or race-lose with no harm).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::state::SharedState;

use super::policy::{PurgeDecision, resolve_retention};

/// Owning handle for the sweeper task. Dropping it drops the task's
/// `JoinHandle`; callers that need to shut down cleanly should hold
/// this and `.abort()` explicitly.
#[derive(Debug)]
pub struct CollectionGcSweeper {
    pub handle: JoinHandle<()>,
}

/// Spawn the sweeper on the Tokio runtime.
///
/// Reads retention + sweep-interval from `shared.retention_settings`
/// on every tick so `ALTER SYSTEM SET deactivated_collection_retention_days`
/// takes effect without a restart.
pub fn spawn_collection_gc(shared: Arc<SharedState>) -> CollectionGcSweeper {
    let handle = tokio::spawn(async move { run_loop(shared).await });
    CollectionGcSweeper { handle }
}

fn load_settings(shared: &SharedState) -> (Duration, Duration, u32) {
    let guard = shared
        .retention_settings
        .read()
        .unwrap_or_else(|p| p.into_inner());
    (
        guard.sweep_interval(),
        guard.retention_window(),
        guard.deactivated_collection_retention_days,
    )
}

async fn run_loop(shared: Arc<SharedState>) {
    let (initial_interval, _, initial_days) = load_settings(&shared);
    info!(
        sweep_interval_secs = initial_interval.as_secs(),
        retention_days = initial_days,
        "collection-gc sweeper started"
    );

    tokio::time::sleep(initial_interval).await;

    loop {
        // Reload every tick so `ALTER SYSTEM SET ...` propagates
        // without a restart. `retention` is the *system-wide default*;
        // per-tenant overrides are resolved inside `sweep_once` via
        // `effective_retention_for_tenant`.
        let (interval, retention, _) = load_settings(&shared);
        if let Err(e) = sweep_once(&shared, retention) {
            warn!(error = %e, "collection-gc sweep failed; will retry next tick");
        }
        tokio::time::sleep(interval).await;
    }
}

/// Resolve the effective retention window for a single tenant.
///
/// Precedence:
/// 1. Per-tenant override on the tenant's `TenantQuota`
///    (`deactivated_collection_retention_days = Some(n)`) — set via
///    `ALTER TENANT <id> SET QUOTA deactivated_collection_retention_days = <n>`.
/// 2. System-wide default (`fallback`).
///
/// `None` on the quota field means "inherit the system default".
fn effective_retention_for_tenant(
    shared: &SharedState,
    tenant_id: u64,
    fallback: Duration,
) -> Duration {
    let tenants = match shared.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };
    let quota = tenants.quota(crate::types::TenantId::new(tenant_id));
    match quota.deactivated_collection_retention_days {
        Some(days) => Duration::from_secs(u64::from(days) * 24 * 60 * 60),
        None => fallback,
    }
}

/// Single sweep pass. Public for testability — callers can drive the
/// sweeper synchronously in tests without waiting on the interval.
pub fn sweep_once(shared: &SharedState, retention: Duration) -> crate::Result<()> {
    let catalog = shared.credentials.catalog();

    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let dropped: Vec<_> = catalog
        .load_all_collections_across_databases()?
        .into_iter()
        .filter(|collection| !collection.is_active)
        .collect();

    // Refresh the pending-purge gauge every pass, even when empty —
    // tenants whose pending count just went to zero need the gauge
    // to zero out, not stay pinned at the previous value.
    if let Some(metrics) = shared.system_metrics.as_ref() {
        let mut pending: HashMap<u64, u64> = HashMap::new();
        for coll in &dropped {
            *pending.entry(coll.tenant_id).or_insert(0) += 1;
        }
        metrics.purge.set_pending_by_tenant(pending);
    }

    if dropped.is_empty() {
        return Ok(());
    }

    let mut proposed = 0usize;
    let mut waiting = 0usize;
    for coll in &dropped {
        let effective = effective_retention_for_tenant(shared, coll.tenant_id, retention);
        match resolve_retention(coll, now_ns, effective) {
            PurgeDecision::Purge => {
                let entry = CatalogEntry::PurgeCollection {
                    database_id: coll.database_id.as_u64(),
                    tenant_id: coll.tenant_id,
                    name: coll.name.clone(),
                };
                match crate::control::metadata_proposer::propose_catalog_entry(shared, &entry) {
                    Ok(_) => {
                        proposed += 1;
                        debug!(
                            tenant = coll.tenant_id,
                            collection = %coll.name,
                            "collection-gc: proposed PurgeCollection"
                        );
                    }
                    Err(e) => {
                        // Likely not-leader on this node — that's fine;
                        // the leader's own sweeper will handle it. Log
                        // at debug, not warn, to avoid alerting on
                        // expected behavior.
                        debug!(
                            tenant = coll.tenant_id,
                            collection = %coll.name,
                            error = %e,
                            "collection-gc: propose failed (expected on follower)"
                        );
                    }
                }
            }
            PurgeDecision::Wait { .. } => waiting += 1,
            PurgeDecision::NotDeactivated => {}
        }
    }

    if proposed > 0 || waiting > 0 {
        info!(
            proposed,
            waiting,
            total = dropped.len(),
            "collection-gc sweep complete"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! End-to-end exercise of the sweep pass against an in-memory
    //! catalog is deferred to
    //! `tests/collection_gc_retention.rs` (needs full SharedState
    //! fixture). The pure policy logic is covered in
    //! `super::policy::tests`.
}
