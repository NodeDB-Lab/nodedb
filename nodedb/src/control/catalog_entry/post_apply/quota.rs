// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for resource-quota catalog entries.
//!
//! Pushes the applied record into the live enforcement components on every
//! node: the admission registry's connection cap, the memory governor's byte
//! ceiling, (database scope only) the maintenance CPU budget, and (tenant
//! scope only) the tenant isolation view. A zero dimension clears the
//! corresponding cap.
//!
//! A database or tenant drop releases the caps of that scope on every node.
//! The matching row deletion is durable state and lives in apply.

use nodedb_types::{DatabaseId, QuotaRecord, TenantId};

use crate::control::security::tenant::TenantQuota;
use crate::control::state::SharedState;

/// Install a database quota into live enforcement.
pub fn put_database(db_id: DatabaseId, record: &QuotaRecord, shared: &SharedState) {
    if let Ok(Some(name)) = shared.credentials.catalog().get_database_name_by_id(db_id) {
        shared
            .maintenance_budget
            .set_cap_named(db_id, name, record.maintenance_cpu_pct);
    } else {
        shared
            .maintenance_budget
            .set_cap(db_id, record.maintenance_cpu_pct);
    }
    if record.max_memory_bytes > 0 {
        shared
            .governor
            .set_database_budget(db_id, record.max_memory_bytes as usize);
    } else {
        shared.governor.clear_database_budget(db_id);
    }
    // `max_connections == 0` clears the cap inside the registry.
    shared
        .admission_registry
        .set_database_limit(db_id, record.max_connections);
}

/// Drop a database quota from live enforcement, restoring defaults.
pub fn delete_database(db_id: DatabaseId, shared: &SharedState) {
    put_database(db_id, &QuotaRecord::DEFAULT, shared);
}

/// Install a tenant quota into live enforcement.
///
/// The record drives three consumers: the admission registry's connection cap,
/// the memory governor's byte ceiling, and the tenant isolation view that the
/// planner, the graph depth gate, and the collection-GC sweeper read.
pub fn put_tenant(
    db_id: DatabaseId,
    tenant_id: TenantId,
    record: &QuotaRecord,
    shared: &SharedState,
) {
    set_tenant_caps(db_id, tenant_id, record, shared);
    let mut tenants = match shared.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };
    tenants.set_quota(tenant_id, TenantQuota::from(record));
}

/// Drop a tenant quota from live enforcement, restoring defaults.
pub fn delete_tenant(db_id: DatabaseId, tenant_id: TenantId, shared: &SharedState) {
    set_tenant_caps(db_id, tenant_id, &QuotaRecord::DEFAULT, shared);
    let mut tenants = match shared.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };
    tenants.clear_quota(tenant_id);
}

/// Point the admission registry and the memory governor at `record`.
fn set_tenant_caps(
    db_id: DatabaseId,
    tenant_id: TenantId,
    record: &QuotaRecord,
    shared: &SharedState,
) {
    shared
        .admission_registry
        .set_tenant_limit(db_id, tenant_id, record.max_connections);
    if record.max_memory_bytes > 0 {
        shared
            .governor
            .set_tenant_budget(db_id, tenant_id, record.max_memory_bytes as usize);
    } else {
        shared.governor.clear_tenant_budget(db_id, tenant_id);
    }
}

/// Release a dropped database's live caps, its tenants' caps included.
/// Row deletion belongs to apply; this frees in-memory enforcement only.
pub fn release_database_scope(db_id: DatabaseId, shared: &SharedState) {
    shared.maintenance_budget.set_cap(db_id, 0);
    shared.admission_registry.clear_database_scope(db_id);
    shared.governor.clear_database_scope(db_id);
}

/// Release a dropped tenant's live caps in every database.
/// Row deletion belongs to apply; this frees in-memory enforcement only.
pub fn release_tenant_scope(tenant_id: TenantId, shared: &SharedState) {
    shared.admission_registry.clear_tenant_scope(tenant_id);
    shared.governor.clear_tenant_scope(tenant_id);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    /// Shared state with a live admission registry.
    fn make_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"));
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("shared state");
        (shared, dir)
    }

    fn capped(max_connections: u32) -> QuotaRecord {
        QuotaRecord {
            max_connections,
            ..QuotaRecord::DEFAULT
        }
    }

    #[test]
    fn release_database_scope_clears_database_and_tenant_caps() {
        let (shared, _dir) = make_state();
        let db = DatabaseId::new(9);
        let other = DatabaseId::new(10);
        let tenant = TenantId::new(4);
        put_database(db, &capped(2), &shared);
        put_tenant(db, tenant, &capped(2), &shared);
        put_database(other, &capped(2), &shared);

        release_database_scope(db, &shared);

        let registry = &shared.admission_registry;
        assert_eq!(registry.database_live_connections(db), None);
        assert_eq!(registry.tenant_live_connections(db, tenant), None);
        assert_eq!(registry.database_live_connections(other), Some(0));
    }

    #[test]
    fn put_tenant_installs_the_record_into_the_isolation_view() {
        let (shared, _dir) = make_state();
        let db = DatabaseId::new(3);
        let tenant = TenantId::new(11);
        let record = QuotaRecord {
            max_vector_dim: 1536,
            max_graph_depth: 8,
            max_concurrent_requests: 64,
            deactivated_collection_retention_days: Some(14),
            ..QuotaRecord::DEFAULT
        };

        put_tenant(db, tenant, &record, &shared);

        let tenants = shared.tenants.lock().unwrap_or_else(|p| p.into_inner());
        let live = tenants.quota(tenant);
        assert_eq!(live.max_vector_dim, 1536);
        assert_eq!(live.max_graph_depth, 8);
        assert_eq!(live.max_concurrent_requests, 64);
        assert_eq!(live.deactivated_collection_retention_days, Some(14));
    }

    #[test]
    fn delete_tenant_returns_the_isolation_view_to_the_default() {
        let (shared, _dir) = make_state();
        let db = DatabaseId::new(3);
        let tenant = TenantId::new(12);
        put_tenant(
            db,
            tenant,
            &QuotaRecord {
                max_graph_depth: 8,
                ..QuotaRecord::DEFAULT
            },
            &shared,
        );

        delete_tenant(db, tenant, &shared);

        let tenants = shared.tenants.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(
            tenants.quota(tenant).max_graph_depth,
            crate::control::security::tenant::TenantQuota::default().max_graph_depth,
            "dropping the row restores the default quota"
        );
    }

    #[test]
    fn release_tenant_scope_clears_that_tenant_in_every_database() {
        let (shared, _dir) = make_state();
        let first = DatabaseId::new(1);
        let second = DatabaseId::new(2);
        let dropped = TenantId::new(7);
        let kept = TenantId::new(8);
        put_tenant(first, dropped, &capped(2), &shared);
        put_tenant(second, dropped, &capped(2), &shared);
        put_tenant(first, kept, &capped(2), &shared);

        release_tenant_scope(dropped, &shared);

        let registry = &shared.admission_registry;
        assert_eq!(registry.tenant_live_connections(first, dropped), None);
        assert_eq!(registry.tenant_live_connections(second, dropped), None);
        assert_eq!(registry.tenant_live_connections(first, kept), Some(0));
    }
}
