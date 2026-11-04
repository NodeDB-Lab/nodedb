// SPDX-License-Identifier: BUSL-1.1

//! Publish one round's cluster totals to every reader.
//!
//! The totals reach the lock-free store admission reads, the per-database
//! storage gauge and each tenant's storage usage. Tenant usage keys by tenant
//! alone, so each tenant reads its bytes summed over every database.

use crate::control::state::SharedState;

use super::shares::TenantTotals;

/// Publish `totals` as the cluster's stored bytes. `complete` marks a round
/// with a report from every active member.
///
/// A database the totals do not name reads zero. A tenant the prior round
/// named and this one does not drops to zero.
pub fn publish_cluster_totals(state: &SharedState, totals: TenantTotals, complete: bool) {
    let store = &state.cluster_storage_totals;
    let prior = store.publish(totals, complete);
    let current = store.snapshot();

    let registry = &state.database_metrics;
    for id in registry.database_ids() {
        registry.set_storage_bytes(id, current.by_database.get(&id).copied().unwrap_or(0));
    }

    let mut isolation = state.tenants.lock().unwrap_or_else(|p| p.into_inner());
    for (tenant_id, bytes) in &current.by_tenant {
        isolation.update_storage(*tenant_id, *bytes);
    }
    for tenant_id in prior.by_tenant.keys() {
        if !current.by_tenant.contains_key(tenant_id) {
            isolation.update_storage(*tenant_id, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn totals(rows: &[(u64, u64, u64)]) -> TenantTotals {
        rows.iter()
            .map(|(d, t, b)| ((DatabaseId::new(*d), TenantId::new(*t)), *b))
            .collect()
    }

    /// The totals reach the store, the database gauge and tenant usage. A
    /// tenant in two databases sums both, and one that leaves reads zero.
    #[test]
    fn totals_reach_the_store_gauges_and_tenant_usage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("p.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let (a, b) = (DatabaseId::new(41), DatabaseId::new(42));
        state.database_metrics.ensure_database(a);
        state.database_metrics.ensure_database(b);
        state.database_metrics.set_storage_bytes(b, 99);
        let usage = |t: u64| {
            state
                .tenants
                .lock()
                .expect("tenants")
                .usage(TenantId::new(t))
                .map_or(0, |u| u.storage_bytes)
        };

        publish_cluster_totals(
            &state,
            totals(&[(41, 1, 100), (41, 2, 20), (43, 1, 5)]),
            true,
        );
        assert!(state.cluster_storage_totals.has_total());
        assert_eq!(state.cluster_storage_totals.bytes(a, TenantId::new(2)), 20);
        assert_eq!(state.database_metrics.storage_bytes(a), 120);
        assert_eq!(state.database_metrics.storage_bytes(b), 0);
        assert_eq!(usage(1), 105);
        assert_eq!(usage(2), 20);

        publish_cluster_totals(&state, totals(&[(41, 1, 40)]), false);
        assert_eq!(state.database_metrics.storage_bytes(a), 40);
        assert_eq!(usage(1), 40);
        assert_eq!(usage(2), 0);
    }
}
